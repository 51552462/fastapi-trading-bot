import os, time, json, hmac, hashlib, base64, requests, re, math
from typing import List, Dict

BASE_URL = "https://api.bitget.com"

API_KEY        = os.getenv("BITGET_API_KEY", "")
API_SECRET     = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE = os.getenv("BITGET_API_PASSWORD", "")

def _ts() -> str:
    return str(int(time.time() * 1000))

def _sign(timestamp: str, method: str, path_with_query: str, body: str = "") -> str:
    raw = timestamp + method.upper() + path_with_query + body
    digest = hmac.new(API_SECRET.encode(), raw.encode(), hashlib.sha256).digest()
    return base64.b64encode(digest).decode()

def _headers(method: str, path_with_query: str, body: str = "") -> Dict[str, str]:
    ts = _ts()
    return {
        "ACCESS-KEY": API_KEY,
        "ACCESS-SIGN": _sign(ts, method, path_with_query, body),
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": API_PASSPHRASE,
        "Content-Type": "application/json",
        "locale": "en-US",
    }

# ── symbol normalize ─────────────────────────────────────────────────────────
def convert_symbol(sym: str) -> str:
    s = re.sub(r'[^A-Za-z0-9]', '', str(sym or "").upper())
    s = re.sub(r'(UMCBL|CMCBL|DMCBL)$', '', s)
    return s

# ── public ticker (retries) ─────────────────────────────────────────────────
def _safe_last_price(symbol: str):
    symbol_conv = convert_symbol(symbol) + "_UMCBL"
    url = f"{BASE_URL}/api/mix/v1/market/ticker?symbol={symbol_conv}"
    r = requests.get(url, timeout=10)
    j = r.json()
    if j and j.get("data") and "last" in j["data"]:
        return float(j["data"]["last"])
    return None

def get_last_price(symbol: str, retries: int = 3, sleep_base: float = 0.15):
    last = None
    for i in range(retries):
        try:
            last = _safe_last_price(symbol)
            if last is not None:
                return last
        except Exception:
            pass
        time.sleep(sleep_base * (2 ** i))
    print(f"❌ Ticker 실패 {convert_symbol(symbol)}_UMCBL")
    return None

# ── symbol spec cache (minQty/step) ─────────────────────────────────────────
_SYMBOLS_CACHE = {"ts": 0, "data": {}}

def _refresh_symbols_cache():
    path = "/api/mix/v1/public/symbols"
    url  = f"{BASE_URL}{path}?productType=umcbl"
    try:
        r = requests.get(url, headers=_headers("GET", f"{path}?productType=umcbl", ""), timeout=10)
        j = r.json()
        data = j.get("data") or []
        m = {}
        for it in data:
            sym = convert_symbol(it.get("symbol") or (it.get("baseCoin","")+it.get("quoteCoin","")))
            if not sym:
                continue
            min_qty = float(it.get("minTradeNum") or it.get("minTradeAmount") or 0)
            step    = float(it.get("sizeStep")    or it.get("lotSize")        or 0)
            m[sym]  = {"min_qty": min_qty, "step": step}
        _SYMBOLS_CACHE["ts"] = time.time()
        _SYMBOLS_CACHE["data"] = m
    except Exception as e:
        print("⚠️ symbols cache refresh fail:", e)

def get_symbol_spec(symbol: str):
    if time.time() - _SYMBOLS_CACHE["ts"] > 600:
        _refresh_symbols_cache()
    return _SYMBOLS_CACHE["data"].get(convert_symbol(symbol), {"min_qty": 0.0, "step": 0.0})

def round_down_step(qty: float, step: float) -> float:
    if not step or step <= 0:
        return round(qty, 6)
    k = math.floor(qty / step)
    return round(k * step, 6)

# ── place orders ─────────────────────────────────────────────────────────────
def place_market_order(symbol, usdt_amount, side, leverage=5, reduce_only=False):
    symbol_conv = convert_symbol(symbol) + "_UMCBL"
    last = get_last_price(symbol)
    if not last:
        return {"code": "LOCAL_TICKER_FAIL", "msg": "ticker_none"}

    spec = get_symbol_spec(symbol)
    qty  = round_down_step(usdt_amount / last, float(spec.get("step", 0.0)))
    if qty <= 0:
        return {"code": "LOCAL_BAD_QTY", "msg": f"qty {qty}"}

    path = "/api/mix/v1/order/placeOrder"
    body = {
        "symbol":     symbol_conv,
        "marginCoin": "USDT",
        "size":       str(qty),
        "side":       "buy_single" if side == "buy" else "sell_single",
        "orderType":  "market",
        "leverage":   str(leverage),
        "reduceOnly": bool(reduce_only),
    }
    bj = json.dumps(body)
    try:
        res = requests.post(BASE_URL + path, headers=_headers("POST", path, bj), data=bj, timeout=15)
        print(f"📥 Bitget 응답 {res.status_code}: {res.text}")
        return res.json()
    except Exception as e:
        print("❌ Bitget 예외:", e)
        return {"code": "LOCAL_EXCEPTION", "msg": str(e)}

def place_reduce_by_size(symbol, size, pos_side, leverage=5):
    symbol_conv = convert_symbol(symbol) + "_UMCBL"
    order_side = "sell_single" if pos_side == "long" else "buy_single"
    path = "/api/mix/v1/order/placeOrder"
    body = {
        "symbol":     symbol_conv,
        "marginCoin": "USDT",
        "size":       str(size),
        "side":       order_side,
        "orderType":  "market",
        "leverage":   str(leverage),
        "reduceOnly": True,
    }
    bj = json.dumps(body)
    try:
        res = requests.post(BASE_URL + path, headers=_headers("POST", path, bj), data=bj, timeout=15)
        print(f"📥 Bitget 응답 {res.status_code}: {res.text}")
        return res.json()
    except Exception as e:
        print("❌ Bitget 예외:", e)
        return {"code": "LOCAL_EXCEPTION", "msg": str(e)}

# ── positions (robust) ──────────────────────────────────────────────────────
def _fetch_positions(query: str) -> List[Dict]:
    path = "/api/mix/v1/position/allPosition"
    url  = f"{BASE_URL}{path}?{query}"
    try:
        res = requests.get(url, headers=_headers("GET", f"{path}?{query}", ""), timeout=10)
        j = res.json()
    except Exception as e:
        print("❌ get_open_positions 예외:", e)
        return []

    if not j or j.get("code") not in ("00000", "0"):
        print(f"❌ get_open_positions 응답 이상: {j}")
        return []

    raw = j.get("data") or []
    if isinstance(raw, dict):
        raw = raw.get("positions") or raw.get("list") or []

    out: List[Dict] = []
    def _f(x): 
        try: return float(x)
        except: return 0.0

    for pos in raw:
        sym  = convert_symbol(pos.get("symbol") or pos.get("instId") or "")
        side = (pos.get("holdSide") or pos.get("side") or pos.get("position") or "").lower()
        size = _f(pos.get("total") or pos.get("available") or pos.get("holdAmount") or pos.get("availableAmount") or pos.get("size") or pos.get("contracts"))
        entry= _f(pos.get("openAvgPrice") or pos.get("averageOpenPrice") or pos.get("avgOpenPrice") or pos.get("entryPrice") or pos.get("avgPrice"))
        if sym and side in ("long", "short") and size>0 and entry>0:
            out.append({"symbol": sym, "side": side, "size": size, "entry_price": entry})
        for k in ("long","short"):
            if isinstance(pos.get(k), dict):
                sub=pos[k]
                s=_f(sub.get("total") or sub.get("available") or sub.get("size") or sub.get("contracts"))
                e=_f(sub.get("openAvgPrice") or sub.get("averageOpenPrice") or sub.get("avgOpenPrice") or sub.get("entryPrice") or entry)
                if sym and s>0 and e>0:
                    out.append({"symbol": sym, "side": k, "size": s, "entry_price": e})
    return out

def get_open_positions() -> List[Dict]:
    merged = {}
    for q in ("productType=umcbl&marginCoin=USDT", "productType=umcbl"):
        for p in _fetch_positions(q):
            merged[f"{p['symbol']}_{p['side']}"] = p
    return list(merged.values())
