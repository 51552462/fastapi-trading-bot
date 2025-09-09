# bitget_api.py — Bitget wrapper
# - v2 ticker 우선 + v1 폴백
# - 심볼 자동 동기화(+_UMCBL/코어심볼 매핑)
# - POSITION_MODE(hedge|oneway) 지원 → 40774 해결
import os, time, math, hmac, hashlib, base64, json
from typing import Dict, Any
import requests

BITGET_BASE = os.getenv("BITGET_BASE_URL", "https://api.bitget.com")
PRODUCT_TYPE = os.getenv("BITGET_PRODUCT_TYPE", "UMCBL")  # USDT-M
USE_V2 = os.getenv("BITGET_USE_V2", "1") == "1"
POSITION_MODE = os.getenv("BITGET_POSITION_MODE", "hedge").lower().strip()  # hedge | oneway
MARGIN_COIN = os.getenv("MARGIN_COIN", "USDT")

# ===== symbol cache =====
_SYMBOL_CACHE: Dict[str, Dict[str, Any]] = {}
_SYMBOL_TS = 0
_SYMBOL_TTL = int(os.getenv("SYMBOL_CACHE_TTL", "1800"))

def convert_symbol(sym: str) -> str:
    s = (sym or "").upper().replace("/", "").replace("-", "")
    for suf in ("_PERP", "PERP", f"_{PRODUCT_TYPE}"):
        if s.endswith(suf): s = s[: -len(suf)]
    return s

def _public_get(path: str, params: Dict[str, Any] | None = None):
    r = requests.get(BITGET_BASE + path, params=params or {}, timeout=10)
    r.raise_for_status()
    j = r.json()
    if str(j.get("code")) != "00000":
        raise RuntimeError(f"bitget public err: {j}")
    return j.get("data") or []

def _refresh_symbols(force: bool = False):
    global _SYMBOL_CACHE, _SYMBOL_TS
    now = time.time()
    if not force and _SYMBOL_CACHE and (now - _SYMBOL_TS) < _SYMBOL_TTL:
        return
    cache: Dict[str, Dict[str, Any]] = {}
    data = []
    try:
        data = _public_get("/api/v2/mix/market/contracts", {"productType": PRODUCT_TYPE})
    except Exception:
        try:
            data = _public_get("/api/mix/v1/market/contracts", {"productType": PRODUCT_TYPE})
        except Exception as e:
            print("symbol refresh failed:", e); data = []
    for it in data:
        sym_full = str(it.get("symbol") or it.get("contract") or "").upper()
        core = convert_symbol(sym_full)
        size_step = float(it.get("sizeTick") or it.get("sizeStep") or 0.001)
        price_prec = int(it.get("pricePlace") or it.get("pricePrecision") or 4)
        spec = {"sizeStep": size_step, "pricePrecision": price_prec}
        cache[sym_full] = spec
        cache[core] = spec
    if cache:
        _SYMBOL_CACHE = cache
        _SYMBOL_TS = now
        print(f"[bitget] symbols cached: {len(cache)}")

def symbol_exists(symbol: str) -> bool:
    s = convert_symbol(symbol)
    if s in _SYMBOL_CACHE: return True
    _refresh_symbols(force=True)
    return s in _SYMBOL_CACHE or f"{s}_{PRODUCT_TYPE}" in _SYMBOL_CACHE

def get_symbol_spec(symbol: str) -> Dict[str, Any]:
    s = convert_symbol(symbol)
    sp = _SYMBOL_CACHE.get(s) or _SYMBOL_CACHE.get(f"{s}_{PRODUCT_TYPE}")
    if sp: return sp
    _refresh_symbols(force=True)
    sp = _SYMBOL_CACHE.get(s) or _SYMBOL_CACHE.get(f"{s}_{PRODUCT_TYPE}")
    return sp or {"sizeStep": 0.001, "pricePrecision": 4}

def round_down_step(qty: float, step: float) -> float:
    if step <= 0: return qty
    return math.floor(qty / step) * step

# ===== signing =====
_API_KEY    = os.getenv("BITGET_API_KEY", "")
_API_SECRET = os.getenv("BITGET_API_SECRET", "")
_API_PASS   = os.getenv("BITGET_API_PASSWORD", "")

def _signed_headers(method: str, path: str, body: str = "") -> Dict[str, str]:
    ts = str(int(time.time() * 1000))
    msg = ts + method.upper() + path + body
    sign = base64.b64encode(hmac.new(_API_SECRET.encode(), msg.encode(), hashlib.sha256).digest()).decode()
    return {
        "ACCESS-KEY": _API_KEY,
        "ACCESS-SIGN": sign,
        "ACCESS-PASSPHRASE": _API_PASS,
        "ACCESS-TIMESTAMP": ts,
        "Content-Type": "application/json",
    }

# ===== market data =====
def get_last_price(symbol: str) -> float:
    core = convert_symbol(symbol)
    if USE_V2:
        try:
            d = _public_get("/api/v2/mix/market/ticker", {"symbol": core})
            last = d.get("last") or d.get("close")
            if last not in (None, "", "null"):
                return float(last)
        except Exception as e:
            print("v2 ticker err:", e)
    try:
        d = _public_get("/api/mix/v1/market/ticker", {"symbol": f"{core}_{PRODUCT_TYPE}"})
        last = d.get("last") or d.get("lastPrice")
        return float(last or 0.0)
    except Exception as e:
        print("v1 ticker err:", e); return 0.0

# ===== private: positions/orders =====
def get_open_positions():
    path = "/api/mix/v1/position/allPosition"
    try:
        r = requests.get(BITGET_BASE + path, headers=_signed_headers("GET", path), timeout=10)
        j = r.json()
        if str(j.get("code")) != "00000": return []
        return j.get("data") or []
    except Exception:
        return []

def _place_order(symbol: str, side: str, size: float, leverage: float) -> Dict[str, Any]:
    core = convert_symbol(symbol)
    path = "/api/mix/v1/order/placeOrder"
    if POSITION_MODE == "oneway":
        # one-way: buy/sell
        side_field = "buy" if side=="long" else "sell"
    else:
        # hedge: open_long/open_short
        side_field = "open_long" if side=="long" else "open_short"
    body = {
        "symbol": f"{core}_{PRODUCT_TYPE}",
        "marginCoin": MARGIN_COIN,
        "side": side_field,
        "orderType": "market",
        "size": str(size),
        "leverage": str(leverage),
    }
    try:
        r = requests.post(BITGET_BASE + path, headers=_signed_headers("POST", path, json.dumps(body)), json=body, timeout=10)
        return r.json()
    except Exception as e:
        return {"code": "HTTP_FAIL", "msg": str(e)}

def place_reduce_by_size(symbol: str, size: float, side: str) -> Dict[str, Any]:
    core = convert_symbol(symbol)
    path = "/api/mix/v1/order/placeOrder"
    if POSITION_MODE == "oneway":
        # one-way: reduceOnly=true + opposite direction
        side_field = "sell" if side=="long" else "buy"
    else:
        # hedge: close_long/close_short
        side_field = "close_long" if side=="long" else "close_short"
    body = {
        "symbol": f"{core}_{PRODUCT_TYPE}",
        "marginCoin": MARGIN_COIN,
        "side": side_field,
        "orderType": "market",
        "size": str(size),
        "reduceOnly": "true",
    }
    try:
        r = requests.post(BITGET_BASE + path, headers=_signed_headers("POST", path, json.dumps(body)), json=body, timeout=10)
        return r.json()
    except Exception as e:
        return {"code": "HTTP_FAIL", "msg": str(e)}

def place_market_order(symbol: str, usdt_amount: float, side: str, leverage: float) -> Dict[str, Any]:
    if not symbol_exists(symbol):
        return {"code": "LOCAL_TICKER_FAIL", "msg": "ticker_none"}
    price = float(get_last_price(symbol) or 0.0)
    if price <= 0:
        return {"code": "MARK_PRICE_FAIL", "msg": "price_zero"}
    spec = get_symbol_spec(symbol)
    step = float(spec.get("sizeStep", 0.001))
    qty = usdt_amount / price
    qty = round_down_step(qty, step)
    if qty <= 0: qty = step
    return _place_order(symbol, side, qty, leverage)
