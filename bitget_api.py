# bitget_api.py — Bitget UMCBL (USDT-M) 최소 구현 안정판
import os, time, json, hmac, hashlib, base64, requests, math
from typing import Dict, List, Optional

BASE_URL = "https://api.bitget.com"

API_KEY        = os.getenv("BITGET_API_KEY", "")
API_SECRET     = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE = os.getenv("BITGET_API_PASSWORD", "")

# ── rate-limit (best-effort) ───────────────────────────────────
_last_call: Dict[str, float] = {}
def _rl(key: str, min_interval: float = 0.10):
    now = time.time()
    prev = _last_call.get(key, 0.0)
    wait = min_interval - (now - prev)
    if wait > 0: time.sleep(wait)
    _last_call[key] = time.time()

def _ts() -> str:
    # Bitget: millisecond precision ISO8601
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + f".{int((time.time()%1)*1000):03d}Z"

def _sign(ts: str, method: str, path_with_query: str, body: str) -> str:
    pre = f"{ts}{method}{path_with_query}{body}"
    digest = hmac.new(API_SECRET.encode(), pre.encode(), hashlib.sha256).digest()
    return base64.b64encode(digest).decode()

def _headers(method: str, path_with_query: str, body: str = "") -> Dict[str, str]:
    ts   = _ts()
    sign = _sign(ts, method.upper(), path_with_query, body or "")
    return {
        "ACCESS-KEY": API_KEY,
        "ACCESS-PASSPHRASE": API_PASSPHRASE,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-SIGN": sign,
        "Content-Type": "application/json",
        "X-CHANNEL-API-CODE": "python",
    }

def _get(path: str, query: str = "") -> Dict:
    _rl("GET"+path, 0.10)
    url = f"{BASE_URL}{path}" + (f"?{query}" if query else "")
    r = requests.get(url, headers=_headers("GET", f"{path}{'?' + query if query else ''}"), timeout=12)
    try: return r.json()
    except Exception: return {"code": "99999", "msg":"json_error", "raw": r.text}

def _post(path: str, body: Dict) -> Dict:
    _rl("POST"+path, 0.12)
    jb = json.dumps(body or {})
    r = requests.post(f"{BASE_URL}{path}", headers=_headers("POST", path, jb), data=jb, timeout=12)
    try: return r.json()
    except Exception: return {"code": "99999", "msg":"json_error", "raw": r.text}

# ── symbol helpers ─────────────────────────────────────────────
ALIASES = {
    # 필요 시 심볼 치환 추가
    # "BTCUSDT.P": "BTCUSDT",
}
def convert_symbol(sym: str) -> str:
    s = (sym or "").upper().replace("/", "").replace("-", "").replace("_", "")
    if s.endswith("PERP"): s = s[:-4]
    return ALIASES.get(s, s)

def _mix_symbol(sym: str) -> str:
    return f"{convert_symbol(sym)}_UMCBL"

# ── ticker cache ───────────────────────────────────────────────
_TICKER_CACHE: Dict[str, tuple] = {}  # { "BTCUSDT": (ts, price) }
TICKER_TTL = float(os.getenv("TICKER_TTL", "2.0"))

def get_last_price(sym: str) -> Optional[float]:
    s = convert_symbol(sym)
    now = time.time()
    ts, px = _TICKER_CACHE.get(s, (0.0, None))
    if now - ts < TICKER_TTL and px:
        return px
    j = _get("/api/mix/v1/market/ticker", f"symbol={_mix_symbol(s)}")
    p = None
    try:
        p = float(j.get("data", {}).get("last", 0) or 0)
    except Exception:
        p = None
    if p and p > 0:
        _TICKER_CACHE[s] = (now, p)
        return p
    return None

# ── symbols spec cache (sizeStep/minQty) ───────────────────────
_SYMBOLS_CACHE = {"ts": 0.0, "data": {}}  # { "BTCUSDT": {"sizeStep":0.001,"minQty":0.001} }

def _refresh_symbols_cache():
    j = _get("/api/mix/v1/public/symbols", "productType=umcbl")
    arr = j.get("data") or []
    m = {}
    for it in arr:
        sym = (it.get("symbol") or "").replace("_UMCBL","")
        step = float(it.get("sizePlace", 3))
        # Bitget는 sizePlace(소수자릿수)로 오기도 함
        size_step = 10 ** (-int(step))
        min_qty = float(it.get("minTradeNum", 0.0) or it.get("minTradeAmount", 0.0) or 0.0)
        m[sym] = {"sizeStep": size_step, "minQty": min_qty}
    _SYMBOLS_CACHE["ts"] = time.time()
    _SYMBOLS_CACHE["data"] = m

def get_symbol_spec(sym: str) -> Dict[str, float]:
    s = convert_symbol(sym)
    if time.time() - _SYMBOLS_CACHE["ts"] > 300 or not _SYMBOLS_CACHE["data"]:
        _refresh_symbols_cache()
    return _SYMBOLS_CACHE["data"].get(s, {"sizeStep": 0.001, "minQty": 0.001})

def round_down_step(qty: float, step: float) -> float:
    if step <= 0: return qty
    k = math.floor(qty / step)
    return round(k * step, 6)

# ── orders ─────────────────────────────────────────────────────
def place_market_order(symbol: str, usdt_amount: float, side: str, leverage: float = 5, reduce_only: bool = False) -> Dict:
    """
    usdt_amount → 시장가 수량 환산 후 주문.
    side: 'buy' | 'sell'
    """
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

    body = {
        "symbol":     _mix_symbol(symbol),
        "marginCoin": "USDT",
        "size":       str(qty),
        "side":       "buy_single" if side == "buy" else "sell_single",
        "orderType":  "market",
        "leverage":   str(leverage),
        "reduceOnly": bool(reduce_only),
        "timeInForceValue": "normal",
    }
    return _post("/api/mix/v1/order/placeOrder", body)

def place_reduce_by_size(symbol: str, size: float, side: str) -> Dict:
    # side는 포지션 방향('long'|'short'). long 청산→ sell, short 청산→ buy
    body = {
        "symbol":     _mix_symbol(symbol),
        "marginCoin": "USDT",
        "size":       str(max(size, 0)),
        "side":       "sell_single" if side == "long" else "buy_single",
        "orderType":  "market",
        "reduceOnly": True,
        "timeInForceValue": "normal",
    }
    return _post("/api/mix/v1/order/placeOrder", body)

# ── positions ──────────────────────────────────────────────────
def _fetch_positions() -> List[Dict]:
    j = _get("/api/mix/v1/position/allPosition", "productType=umcbl")
    arr = j.get("data") or []
    out = []
    for it in arr:
        sym  = (it.get("symbol") or "").replace("_UMCBL", "")
        long_sz  = float(it.get("longQty", 0) or it.get("holdVol", 0) or 0)
        short_sz = float(it.get("shortQty", 0) or 0)
        long_price  = float(it.get("longAvgCost", 0) or it.get("avgOpenPrice", 0) or 0)
        short_price = float(it.get("shortAvgCost", 0) or 0)

        if long_sz > 0:
            out.append({"symbol": sym, "side": "long",  "size": long_sz,  "entry_price": long_price})
        if short_sz > 0:
            out.append({"symbol": sym, "side": "short", "size": short_sz, "entry_price": short_price})
    return out

_POS_CACHE = {"ts": 0.0, "data": [], "cooldown_until": 0.0}

def get_open_positions() -> List[Dict]:
    now = time.time()
    # 쿨다운 중이면 캐시 반환
    if now < _POS_CACHE["cooldown_until"] and _POS_CACHE["data"]:
        return _POS_CACHE["data"]

    # 2초 이내 재호출이면 캐시
    if now - _POS_CACHE["ts"] < 2.0 and _POS_CACHE["data"]:
        return _POS_CACHE["data"]

    res = _fetch_positions()
    if res is not None:
        _POS_CACHE["data"] = res
        _POS_CACHE["ts"] = now
        _POS_CACHE["cooldown_until"] = 0.0
        return res

    # 실패 시 캐시 반환 + 쿨다운
    _POS_CACHE["cooldown_until"] = now + 90
    return _POS_CACHE["data"]
