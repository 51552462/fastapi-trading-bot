import os, time, json, hmac, hashlib, base64, requests, math
from typing import Dict, List, Optional

BASE_URL = "https://api.bitget.com"

API_KEY        = os.getenv("BITGET_API_KEY", "")
API_SECRET     = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE = os.getenv("BITGET_API_PASSWORD", "")

# ── Auth (Bitget: HMAC-SHA256 → base64) ───────────────────────
def _ts() -> str:
    return str(int(time.time() * 1000))

def _sign(ts: str, method: str, path_with_query: str, body: str = "") -> str:
    prehash = ts + method.upper() + path_with_query + body
    digest  = hmac.new(API_SECRET.encode(), prehash.encode(), hashlib.sha256).digest()
    return base64.b64encode(digest).decode()

def _headers(method: str, path_with_query: str, body: str = "") -> Dict[str, str]:
    ts = _ts()
    return {"ACCESS-KEY": API_KEY,"ACCESS-SIGN": _sign(ts, method, path_with_query, body),"ACCESS-TIMESTAMP": ts,"ACCESS-PASSPHRASE": API_PASSPHRASE,"Content-Type": "application/json","locale": "en-US",}

# ── Symbol helpers ─────────────────────────────────────────────
def convert_symbol(sym: str) -> str:
    s = (sym or "").upper().replace("/", "").replace("-", "").replace("_", "")
    if s.endswith("PERP"):
        s = s[:-4]
    return s

def _mix_symbol(sym: str) -> str:
    return f"{convert_symbol(sym)}_UMCBL"

# ── Market: last price ────────────────────────────────────────
def get_last_price(symbol: str, retries: int = 3, sleep_base: float = 0.15) -> Optional[float]:
    url = f"{BASE_URL}/api/mix/v1/market/ticker?symbol={_mix_symbol(symbol)}"
    for i in range(retries):
        try:
            r = requests.get(url, timeout=10)
            j = r.json()
            if j and j.get("data") and j["data"].get("last") is not None:
                return float(j["data"]["last"])
        except Exception:
            pass
        time.sleep(sleep_base * (2 ** i))
    print(f"❌ Ticker 실패: {_mix_symbol(symbol)}")
    return None

# ── Symbol spec cache (sizeStep / minQty) ─────────────────────
_SYMBOLS_CACHE = {"ts": 0.0, "data": {}}

def _refresh_symbols_cache():
    path = "/api/mix/v1/public/symbols"
    q    = "productType=umcbl"
    try:
        r = requests.get(f"{BASE_URL}{path}?{q}", headers=_headers("GET", f"{path}?{q}", ""), timeout=12)
        j = r.json()
        arr = j.get("data") or []
        m = {}
        for it in arr:
            sym_full = (it.get("symbol") or "")
            if not sym_full.endswith("_UMCBL"):
                continue
            sym_core   = sym_full.replace("_UMCBL", "")
            size_scale = int(it.get("sizeScale") or 0)
            size_step  = 10 ** (-size_scale) if size_scale >= 0 else 0.001
            min_qty    = float(it.get("minTradeNum") or it.get("minOrderSize") or 0.0)
            m[sym_core] = {"sizeStep": size_step, "minQty": min_qty}
        _SYMBOLS_CACHE["data"] = m
        _SYMBOLS_CACHE["ts"]   = time.time()
    except Exception as e:
        print("❌ 심볼 캐시 갱신 실패:", e)

def get_symbol_spec(symbol: str) -> Dict[str, float]:
    now = time.time()
    if now - _SYMBOLS_CACHE["ts"] > 600 or not _SYMBOLS_CACHE["data"]:
        _refresh_symbols_cache()
    sym  = convert_symbol(symbol)
    spec = _SYMBOLS_CACHE["data"].get(sym)
    if not spec:
        spec = {"sizeStep": 0.001, "minQty": 0.001}
        _SYMBOLS_CACHE["data"][sym] = spec
    return spec

def round_down_step(qty: float, step: float) -> float:
    if step <= 0:
        return round(qty, 6)
    k = math.floor(qty / step)
    return round(k * step, 6)

# ── Orders ────────────────────────────────────────────────────
def place_market_order(symbol: str, usdt_amount: float, side: str, leverage: float = 5, reduce_only: bool = False) -> Dict:
    """usdt_amount → 시장가 수량 환산 후 주문. side='buy'|'sell'"""
    last = get_last_price(symbol)
    if not last:
        return {"code": "LOCAL_TICKER_FAIL", "msg": "ticker_none"}

    spec = get_symbol_spec(symbol)
    qty  = round_down_step(usdt_amount / last, float(spec.get("sizeStep", 0.001)))
    if qty <= 0:
        return {"code": "LOCAL_BAD_QTY", "msg": f"qty {qty}"}
    if qty < float(spec.get("minQty", 0.0)):
        need = float(spec.get("minQty")) * last
        return {"code": "LOCAL_MIN_QTY", "msg": f"need≈{need:.6f}USDT", "qty": qty}

    path = "/api/mix/v1/order/placeOrder"
    body = {"symbol":     _mix_symbol(symbol),"marginCoin": "USDT","size":       str(qty),"side":       "buy_single" if side == "buy" else "sell_single","orderType":  "market","leverage":   str(leverage),"reduceOnly": bool(reduce_only),}
    bj = json.dumps(body)
    try:
        res = requests.post(BASE_URL + path, headers=_headers("POST", path, bj), data=bj, timeout=15)
        if res.status_code != 200:
            return {"code": f"HTTP_{res.status_code}", "msg": res.text}
        return res.json()
    except Exception as e:
        return {"code": "LOCAL_EXCEPTION", "msg": str(e)}

def place_reduce_by_size(symbol: str, size: float, side: str) -> Dict:
    """size 계약 수량을 reduceOnly 시장가로 청산. side='long'→sell_single, 'short'→buy_single"""
    size = float(size)
    if size <= 0:
        return {"code": "LOCAL_BAD_QTY", "msg": "size<=0"}

    step = float(get_symbol_spec(symbol).get("sizeStep", 0.001))
    size = round_down_step(size, step)
    if size <= 0:
        return {"code": "LOCAL_STEP_ZERO", "msg": "after_step=0"}

    path = "/api/mix/v1/order/placeOrder"
    body = {"symbol":     _mix_symbol(symbol),"marginCoin": "USDT","size":       str(size),"side":       "sell_single" if side.lower() == "long" else "buy_single","orderType":  "market","reduceOnly": True,}
    bj = json.dumps(body)
    try:
        res = requests.post(BASE_URL + path, headers=_headers("POST", path, bj), data=bj, timeout=15)
        if res.status_code != 200:
            return {"code": f"HTTP_{res.status_code}", "msg": res.text}
        return res.json()
    except Exception as e:
        return {"code": "LOCAL_EXCEPTION", "msg": str(e)}

# ── Positions ────────────────────────────────────────────────
_POS_CACHE = {"data": [], "ts": 0.0, "cooldown_until": 0.0}

def _fetch_positions() -> List[Dict]:
    path = "/api/mix/v1/position/allPosition"
    q    = "productType=umcbl"
    try:
        res = requests.get(f"{BASE_URL}{path}?{q}", headers=_headers("GET", f"{path}?{q}", ""), timeout=12)
        j = res.json()
    except Exception as e:
        print("❌ position fetch 예외:", e)
        return []

    if not j or j.get("code") not in ("00000", "0"):
        print("❌ position 응답 이상:", j)
        return []

    raw = j.get("data") or []
    if isinstance(raw, dict):
        raw = raw.get("positions") or raw.get("list") or []

    out: List[Dict] = []
    def ffloat(x):
        try: return float(x)
        except: return 0.0

    for it in raw:
        sym_full = it.get("symbol") or ""
        if not sym_full.endswith("_UMCBL"):
            continue
        sym_core = sym_full.replace("_UMCBL", "")
        hold     = (it.get("holdSide") or it.get("side") or "").lower()  # long | short
        sz       = ffloat(it.get("total") or it.get("available") or it.get("size"))
        avg      = ffloat(it.get("averageOpenPrice") or it.get("avgOpenPrice") or it.get("entryPrice"))
        if sz > 0 and hold in ("long", "short"):
            out.append({"symbol": sym_core, "side": hold, "size": sz, "entry_price": avg})
    return out

def get_open_positions() -> List[Dict]:
    now = time.time()
    if now < _POS_CACHE["cooldown_until"] and _POS_CACHE["data"]:
        return _POS_CACHE["data"]
    res = _fetch_positions()
    if res:
        _POS_CACHE["data"] = res
        _POS_CACHE["ts"] = now
        _POS_CACHE["cooldown_until"] = 0.0
        return res
    # 실패 시 캐시 반환 + 쿨다운
    if _POS_CACHE["data"]:
        _POS_CACHE["cooldown_until"] = now + 90
        print("⚠️ position 새 조회 실패 → 캐시 반환(90s 쿨다운)")
    return _POS_CACHE["data"]
