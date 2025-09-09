# bitget_api.py — Bitget UMCBL one-way 전용 경량 래퍼 (v2 우선, 다중 폴백)
import time
import hmac
import json
import hashlib
import base64
import os
from typing import Dict, Any, List
import requests

BASE = os.getenv("BITGET_BASE_URL", "https://api.bitget.com")
TIMEOUT = float(os.getenv("BITGET_HTTP_TIMEOUT", "10"))

API_KEY    = os.getenv("BITGET_API_KEY", "")
API_SECRET = os.getenv("BITGET_API_SECRET", "")
API_PASS   = os.getenv("BITGET_API_PASS", "")

PRODUCT_TYPE = os.getenv("BITGET_PRODUCT_TYPE", "UMCBL")  # 선물(USDT)
MARGIN_COIN  = os.getenv("MARGIN_COIN", "USDT")
USE_V2       = os.getenv("BITGET_USE_V2", "1") == "1"
POSITION_MODE= os.getenv("BITGET_POSITION_MODE", "oneway").lower().strip()

_sess = requests.Session()

# ---------------- low-level ----------------
def _ts() -> str:
    return str(int(time.time() * 1000))  # v2는 ms 문자열

def _sign(ts: str, method: str, path: str, body_str: str = "") -> str:
    msg = ts + method.upper() + path + body_str
    mac = hmac.new(API_SECRET.encode(), msg.encode(), hashlib.sha256).digest()
    return base64.b64encode(mac).decode()

def _headers(ts: str, sign: str) -> Dict[str, str]:
    return {
        "ACCESS-KEY": API_KEY,
        "ACCESS-SIGN": sign,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": API_PASS,
        "Content-Type": "application/json",
        "locale": "en-US",
    }

def _request(method: str, path: str, params: Dict[str, Any] = None, body: Dict[str, Any] = None, auth: bool = False):
    params = params or {}
    body = body or {}
    url = BASE + path

    if method.upper() == "GET":
        # v2는 서명 시 쿼리를 path에 포함
        query = "&".join([f"{k}={params[k]}" for k in sorted(params.keys())]) if params else ""
        path_sig = path + (("?" + query) if query else "")
        ts = _ts()
        hdr = _headers(ts, _sign(ts, "GET", path_sig, ""))
        r = _sess.get(url, params=params, headers=(hdr if auth else None), timeout=TIMEOUT)
    else:
        body_str = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
        ts = _ts()
        hdr = _headers(ts, _sign(ts, method, path, body_str))
        r = _sess.request(method.upper(), url, data=body_str, headers=(hdr if auth else None), timeout=TIMEOUT)

    try:
        data = r.json()
    except Exception:
        data = {"code": f"HTTP_{r.status_code}", "raw": r.text}

    if r.status_code >= 400:
        return {"code": f"HTTP_{r.status_code}", "msg": data}
    return data

# ---------------- utils ----------------
def convert_symbol(sym: str) -> str:
    """내부/트뷰 입력 → Bitget 표준 심볼(BTCUSDT). 접미사는 제거."""
    s = (sym or "").upper().replace("-", "").replace("_", "")
    for suf in ("UMCBL", "CMCBL", "UMCML", "DMCBL", "UMCBLUSDT"):
        s = s.replace(suf, "")
    if not s.endswith("USDT"):
        s = s + "USDT"
    return s

def round_down_step(x: float, step: float) -> float:
    if step <= 0: return float(x)
    return (int(float(x) / float(step))) * float(step)

# ---------------- public meta ----------------
_symbol_spec_cache: Dict[str, Dict[str, Any]] = {}
_symbol_exist_cache: Dict[str, float] = {}

def _fetch_contracts_v2() -> List[Dict[str, Any]]:
    res = _request("GET", "/api/v2/mix/market/contracts", params={"productType": PRODUCT_TYPE.lower()}, auth=False)
    return res.get("data", []) if isinstance(res, dict) else []

def _fetch_contracts_v1() -> List[Dict[str, Any]]:
    res = _request("GET", "/api/mix/v1/market/contracts", params={"productType": PRODUCT_TYPE}, auth=False)
    return res.get("data", []) if isinstance(res, dict) else []

def get_symbol_spec(symbol: str) -> Dict[str, Any]:
    sym = convert_symbol(symbol)
    now = time.time()
    spec = _symbol_spec_cache.get(sym)
    if spec and now - spec.get("_ts", 0) < 3600:
        return spec

    rows = _fetch_contracts_v2() if USE_V2 else _fetch_contracts_v1()
    found = None
    for r in rows:
        if (r.get("symbol") or "").upper() == sym:
            found = r; break
    if not found:
        for r in rows:
            if sym in (r.get("symbol") or "").upper():
                found = r; break

    size_step = float(found.get("sizeStep") or found.get("minTradeNum") or 0.001) if found else 0.001
    price_prec= int(found.get("pricePlace") or found.get("pricePrecision") or 4) if found else 4
    lot = float(found.get("minTradeNum") or 0.001) if found else 0.001
    spec = {"sizeStep": size_step, "pricePrecision": price_prec, "minTradeNum": lot, "_ts": now}
    _symbol_spec_cache[sym] = spec
    return spec

# ---------------- ticker (다중 폴백) ----------------
def _get_last_v2_single(sym: str) -> float:
    # /api/v2/mix/market/ticker?symbol=BTCUSDT
    res = _request("GET", "/api/v2/mix/market/ticker", params={"symbol": sym}, auth=False)
    try:
        d = res.get("data", {})
        v = float(d.get("last") or d.get("close") or 0)
        return v if v > 0 else 0.0
    except Exception:
        return 0.0

def _get_last_v2_list(sym: str) -> float:
    # /api/v2/mix/market/tickers?productType=umcbl
    res = _request("GET", "/api/v2/mix/market/tickers", params={"productType": PRODUCT_TYPE.lower()}, auth=False)
    try:
        for row in res.get("data", []):
            if (row.get("symbol") or "").upper() == sym:
                v = float(row.get("last") or row.get("close") or 0)
                if v > 0: return v
    except Exception:
        pass
    return 0.0

def _get_last_v1_single(sym: str) -> float:
    res = _request("GET", "/api/mix/v1/market/ticker", params={"symbol": sym}, auth=False)
    try:
        d = res.get("data", {})
        v = float(d.get("last") or d.get("close") or 0)
        return v if v > 0 else 0.0
    except Exception:
        return 0.0

def _get_last_v1_list(sym: str) -> float:
    res = _request("GET", "/api/mix/v1/market/tickers", params={"productType": PRODUCT_TYPE}, auth=False)
    try:
        for row in res.get("data", []):
            if (row.get("symbol") or "").upper() == sym:
                v = float(row.get("last") or row.get("close") or 0)
                if v > 0: return v
    except Exception:
        pass
    return 0.0

def get_last_price(symbol: str) -> float:
    """v2 단건 → v2 리스트 → v1 단건 → v1 리스트 순서 폴백."""
    sym = convert_symbol(symbol)
    v = 0.0
    if USE_V2:
        v = _get_last_v2_single(sym)
        if v <= 0: v = _get_last_v2_list(sym)
        if v <= 0: v = _get_last_v1_single(sym)
        if v <= 0: v = _get_last_v1_list(sym)
    else:
        v = _get_last_v1_single(sym)
        if v <= 0: v = _get_last_v1_list(sym)
        if v <= 0: v = _get_last_v2_single(sym)
        if v <= 0: v = _get_last_v2_list(sym)
    return v if v > 0 else 0.0

def symbol_exists(symbol: str) -> bool:
    sym = convert_symbol(symbol)
    now = time.time()
    if sym in _symbol_exist_cache and now - _symbol_exist_cache[sym] < 300:
        return True
    ok = get_last_price(sym) > 0
    if ok:
        _symbol_exist_cache[sym] = now
    return ok

# ---------------- positions ----------------
def _get_positions_v2() -> List[Dict[str, Any]]:
    res = _request("GET", "/api/v2/mix/position/all-position",
                   params={"productType": PRODUCT_TYPE.lower(), "marginCoin": MARGIN_COIN}, auth=True)
    arr = res.get("data", []) if isinstance(res, dict) else []
    out = []
    for p in arr:
        sz = float(p.get("total") or p.get("holdSideTotal") or p.get("available") or p.get("size") or 0.0)
        side = p.get("holdSide", "")
        if not side:
            if float(p.get("longQty") or 0) > 0:  side, sz = "long", float(p.get("longQty"))
            if float(p.get("shortQty") or 0) > 0: side, sz = "short", float(p.get("shortQty"))
        out.append({
            "symbol": p.get("symbol"),
            "side": ("long" if str(side).lower() in ("long","buy") else "short"),
            "size": abs(sz),
            "entryPrice": float(p.get("averageOpenPrice") or p.get("avgEntryPrice") or p.get("openPriceAvg") or 0.0),
        })
    return [x for x in out if x["size"] > 0]

def _get_positions_v1() -> List[Dict[str, Any]]:
    res = _request("GET", "/api/mix/v1/position/allPosition",
                   params={"productType": PRODUCT_TYPE, "marginCoin": MARGIN_COIN}, auth=True)
    arr = res.get("data", []) if isinstance(res, dict) else []
    out = []
    for p in arr:
        out.append({
            "symbol": p.get("symbol"),
            "side": ("long" if (p.get("holdSide") or "").lower() in ("long","buy") else "short"),
            "size": float(p.get("total") or p.get("available") or p.get("size") or 0.0),
            "entryPrice": float(p.get("averageOpenPrice") or p.get("avgEntryPrice") or 0.0),
        })
    return [x for x in out if x["size"] > 0]

def get_open_positions() -> List[Dict[str, Any]]:
    try:
        return _get_positions_v2() if USE_V2 else _get_positions_v1()
    except Exception:
        return []

# ---------------- orders ----------------
def _calc_size_from_usdt(symbol: str, usdt_amount: float) -> float:
    px = get_last_price(symbol)
    if px is None or px <= 0:
        return 0.0
    size = float(usdt_amount) / float(px)
    step = float(get_symbol_spec(symbol).get("sizeStep", 0.001))
    return round_down_step(size, step)

def place_market_order(symbol: str, usdt_amount: float, side: str, leverage: float = 5.0) -> Dict[str, Any]:
    sym = convert_symbol(symbol)
    size = _calc_size_from_usdt(sym, usdt_amount)
    if size <= 0:
        return {"code": "LOCAL_TICKER_FAIL", "msg": "ticker_none or size<=0"}

    body_v2 = {
        "symbol": sym, "marginCoin": MARGIN_COIN, "size": str(size),
        "side": "buy" if side.lower().startswith("l") else "sell",
        "orderType": "market", "reduceOnly": False, "timeInForceValue": "normal",
    }
    path_v2 = "/api/v2/mix/order/place-order"

    body_v1 = {
        "symbol": sym, "marginCoin": MARGIN_COIN, "size": str(size),
        "side": body_v2["side"], "orderType": "market", "reduceOnly": False
    }
    path_v1 = "/api/mix/v1/order/placeOrder"

    res = _request("POST", path_v2 if USE_V2 else path_v1, body=(body_v2 if USE_V2 else body_v1), auth=True)
    code = str(res.get("code", ""))
    if code in ("00000", "0", "success"):
        return {"code": "00000", "data": res.get("data")}
    return res

def place_reduce_by_size(symbol: str, size: float, side: str) -> Dict[str, Any]:
    sym = convert_symbol(symbol)
    step = float(get_symbol_spec(sym).get("sizeStep", 0.001))
    qty  = round_down_step(float(size), step)
    if qty <= 0:
        return {"code": "LOCAL_SIZE_TOO_SMALL", "msg": "size<=0"}

    is_long = side.lower().startswith("l")
    body_v2 = {
        "symbol": sym, "marginCoin": MARGIN_COIN, "size": str(qty),
        "side": "sell" if is_long else "buy",
        "orderType": "market", "reduceOnly": True, "timeInForceValue": "normal",
    }
    body_v1 = {
        "symbol": sym, "marginCoin": MARGIN_COIN, "size": str(qty),
        "side": body_v2["side"], "orderType": "market", "reduceOnly": True
    }
    path_v2 = "/api/v2/mix/order/place-order"
    path_v1 = "/api/mix/v1/order/placeOrder"

    res = _request("POST", path_v2 if USE_V2 else path_v1, body=(body_v2 if USE_V2 else body_v1), auth=True)
    code = str(res.get("code", ""))
    if code in ("00000", "0", "success"):
        return {"code": "00000", "data": res.get("data")}
    return res
