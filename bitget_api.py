# bitget_api.py — Bitget UMCBL one-way 전용 경량 래퍼 (v2 우선, v1 폴백)
import time
import hmac
import json
import hashlib
import base64
import os
from typing import Dict, Any, List, Tuple
import requests

BASE = os.getenv("BITGET_BASE_URL", "https://api.bitget.com")
TIMEOUT = float(os.getenv("BITGET_HTTP_TIMEOUT", "10"))

API_KEY    = os.getenv("BITGET_API_KEY", "")
API_SECRET = os.getenv("BITGET_API_SECRET", "")
API_PASS   = os.getenv("BITGET_API_PASS", "")

PRODUCT_TYPE = os.getenv("BITGET_PRODUCT_TYPE", "UMCBL")  # 선물(USDT) — umcbl
MARGIN_COIN  = os.getenv("MARGIN_COIN", "USDT")
USE_V2       = os.getenv("BITGET_USE_V2", "1") == "1"
POSITION_MODE= os.getenv("BITGET_POSITION_MODE", "oneway").lower().strip()

_sess = requests.Session()

# ---------- helpers ----------
def _ts() -> str:
    # Bitget v2: ms 문자열
    return str(int(time.time() * 1000))

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
    if method.upper() == "GET" and params:
        # Bitget는 서명 시 쿼리가 path에 포함되어야 함
        query = "&".join([f"{k}={params[k]}" for k in sorted(params.keys())])
        path_sig = path + ("?" + query if query else "")
        ts = _ts()
        hdr = _headers(ts, _sign(ts, "GET", path_sig, ""))
        r = _sess.get(url, params=params, headers=hdr if auth else None, timeout=TIMEOUT)
    else:
        body_str = json.dumps(body, separators=(",", ":"), ensure_ascii=False)
        ts = _ts()
        hdr = _headers(ts, _sign(ts, method, path, body_str))
        if method.upper() == "POST":
            r = _sess.post(url, data=body_str, headers=hdr if auth else None, timeout=TIMEOUT)
        else:
            r = _sess.request(method.upper(), url, data=body_str, headers=hdr if auth else None, timeout=TIMEOUT)

    try:
        data = r.json()
    except Exception:
        data = {"code": f"HTTP_{r.status_code}", "raw": r.text}

    if r.status_code >= 400:
        return {"code": f"HTTP_{r.status_code}", "msg": data if isinstance(data, str) else data}
    return data

# ---------- symbol helpers ----------
def convert_symbol(sym: str) -> str:
    """TradingView/내부에서 들어오는 다양한 표기 → Bitget v2 심볼 표준화(BTCUSDT)."""
    s = (sym or "").upper().replace("-", "").replace("_", "")
    for suf in ("UMCBL", "CMCBL", "UMCML", "UMCBLUSDT"):
        s = s.replace(suf, "")
    if not s.endswith("USDT"):
        if "USDT" in s:
            # already ok
            pass
        else:
            s = s + "USDT"
    return s

# 단순 소수점 보정
def round_down_step(x: float, step: float) -> float:
    if step <= 0: return float(x)
    return (int(x / step)) * step

# ---------- public ----------
_symbol_spec_cache: Dict[str, Dict[str, Any]] = {}
_symbol_exist_cache: Dict[str, float] = {}

def _fetch_contracts_v2() -> List[Dict[str, Any]]:
    path = "/api/v2/mix/market/contracts"
    params = {"productType": PRODUCT_TYPE.lower()}  # umcbl/ dmcbl
    res = _request("GET", path, params=params, auth=False)
    return res.get("data", []) if isinstance(res, dict) else []

def _fetch_contracts_v1() -> List[Dict[str, Any]]:
    path = "/api/mix/v1/market/contracts"
    params = {"productType": PRODUCT_TYPE}
    res = _request("GET", path, params=params, auth=False)
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
        # v2: r["symbol"] like "BTCUSDT"; v1: similar
        if (r.get("symbol") or "").upper() == sym:
            found = r
            break
    if not found:
        # fallback: first match containing sym
        for r in rows:
            if sym in (r.get("symbol") or "").upper():
                found = r; break

    # defaults
    size_step = float(found.get("sizeStep") or found.get("minTradeNum") or 0.001) if found else 0.001
    price_prec= int(found.get("pricePlace") or found.get("pricePrecision") or 4) if found else 4
    lot = float(found.get("minTradeNum") or 0.001) if found else 0.001

    spec = {"sizeStep": size_step, "pricePrecision": price_prec, "minTradeNum": lot, "_ts": now}
    _symbol_spec_cache[sym] = spec
    return spec

def symbol_exists(symbol: str) -> bool:
    sym = convert_symbol(symbol)
    now = time.time()
    if sym in _symbol_exist_cache and now - _symbol_exist_cache[sym] < 300:
        return True
    last = get_last_price(sym)
    ok = last is not None and last > 0
    if ok:
        _symbol_exist_cache[sym] = now
    return ok

def get_last_price(symbol: str) -> float:
    sym = convert_symbol(symbol)
    if USE_V2:
        path = "/api/v2/mix/market/ticker"
        res = _request("GET", path, params={"symbol": sym}, auth=False)
        # v2 spec: {"data":{"symbol":"BTCUSDT","last":"67890"...}}
        try:
            d = res.get("data", {})
            last = float(d.get("last") or d.get("close") or 0)
            return last if last > 0 else 0.0
        except Exception:
            return 0.0
    else:
        path = "/api/mix/v1/market/ticker"
        res = _request("GET", path, params={"symbol": sym}, auth=False)
        try:
            d = res.get("data", {})
            return float(d.get("last") or d.get("close") or 0)
        except Exception:
            return 0.0

# ---------- private: positions ----------
def _get_positions_v2() -> List[Dict[str, Any]]:
    # all positions
    path = "/api/v2/mix/position/all-position"
    params = {"productType": PRODUCT_TYPE.lower(), "marginCoin": MARGIN_COIN}
    res = _request("GET", path, params=params, auth=True)
    arr = res.get("data", []) if isinstance(res, dict) else []
    out = []
    for p in arr:
        # normalize
        sz = float(p.get("total") or p.get("holdSideTotal") or p.get("available") or p.get("size") or 0.0)
        side = "long" if (p.get("holdSide") or "").lower() in ("long","buy","openlong") or float(p.get("total") or 0)>0 else "short" if float(p.get("total") or 0)<0 else p.get("holdSide","")
        if "holdSide" not in p:  # sometimes separate fields long/short
            if float(p.get("longQty") or 0) > 0: side, sz = "long", float(p.get("longQty"))
            if float(p.get("shortQty") or 0) > 0: side, sz = "short", float(p.get("shortQty"))
        out.append({
            "symbol": p.get("symbol"),
            "side": side,
            "size": abs(sz),
            "entryPrice": float(p.get("averageOpenPrice") or p.get("avgEntryPrice") or p.get("openPriceAvg") or 0.0),
        })
    return [x for x in out if x["size"] > 0]

def _get_positions_v1() -> List[Dict[str, Any]]:
    path = "/api/mix/v1/position/allPosition"
    params = {"productType": PRODUCT_TYPE, "marginCoin": MARGIN_COIN}
    res = _request("GET", path, params=params, auth=True)
    arr = res.get("data", []) if isinstance(res, dict) else []
    out = []
    for p in arr:
        out.append({
            "symbol": p.get("symbol"),
            "side": "long" if (p.get("holdSide") or "").lower() in ("long","buy") else "short",
            "size": float(p.get("total") or p.get("available") or p.get("size") or 0.0),
            "entryPrice": float(p.get("averageOpenPrice") or p.get("avgEntryPrice") or 0.0),
        })
    return [x for x in out if x["size"] > 0]

def get_open_positions() -> List[Dict[str, Any]]:
    try:
        return _get_positions_v2() if USE_V2 else _get_positions_v1()
    except Exception as e:
        return []

# ---------- private: orders ----------
def _calc_size_from_usdt(symbol: str, usdt_amount: float) -> float:
    """
    USDT 금액 → 계약 수량(size) 근사.
    UMCBL(선물)은 1 계약이 1 코인 기준인 심볼이 많아 단순화: size ≈ USDT / last.
    """
    px = get_last_price(symbol) or 0.0
    if px <= 0:
        return 0.0
    size = float(usdt_amount) / float(px)
    step = float(get_symbol_spec(symbol).get("sizeStep", 0.001))
    return round_down_step(size, step)

def place_market_order(symbol: str, usdt_amount: float, side: str, leverage: float = 5.0) -> Dict[str, Any]:
    """
    시장가 진입(단방향 one-way). reduceOnly=False
    v2: /api/v2/mix/order/place-order
    body: symbol, marginCoin, size, side(buy/sell), orderType(market), reduceOnly(false), timeInForceValue("normal")
    """
    sym = convert_symbol(symbol)
    size = _calc_size_from_usdt(sym, usdt_amount)
    if size <= 0:
        return {"code": "LOCAL_TICKER_FAIL", "msg": "ticker_none or size<=0"}

    body = {
        "symbol": sym,
        "marginCoin": MARGIN_COIN,
        "size": str(size),
        "side": "buy" if side.lower().startswith("l") else "sell",
        "orderType": "market",
        "reduceOnly": False,
        "timeInForceValue": "normal",
    }
    path = "/api/v2/mix/order/place-order" if USE_V2 else "/api/mix/v1/order/placeOrder"
    res = _request("POST", path, body=body if USE_V2 else {
        "symbol": sym, "marginCoin": MARGIN_COIN, "size": str(size),
        "side": body["side"], "orderType": "market", "reduceOnly": False
    }, auth=True)

    # Bitget 성공코드 통일
    code = str(res.get("code", ""))
    if code in ("00000", "0", "success"):
        return {"code": "00000", "data": res.get("data")}
    return res

def place_reduce_by_size(symbol: str, size: float, side: str) -> Dict[str, Any]:
    """
    시장가 reduceOnly 청산(부분/전체).
    - LONG 청산 → side='sell'
    - SHORT 청산 → side='buy'
    """
    sym = convert_symbol(symbol)
    size = round_down_step(float(size), float(get_symbol_spec(sym).get("sizeStep", 0.001)))
    if size <= 0:
        return {"code": "LOCAL_SIZE_TOO_SMALL", "msg": "size<=0"}

    is_long = side.lower().startswith("l")
    body = {
        "symbol": sym,
        "marginCoin": MARGIN_COIN,
        "size": str(size),
        "side": "sell" if is_long else "buy",
        "orderType": "market",
        "reduceOnly": True,
        "timeInForceValue": "normal",
    }
    path = "/api/v2/mix/order/place-order" if USE_V2 else "/api/mix/v1/order/placeOrder"
    res = _request("POST", path, body=body if USE_V2 else {
        "symbol": sym, "marginCoin": MARGIN_COIN, "size": str(size),
        "side": body["side"], "orderType": "market", "reduceOnly": True
    }, auth=True)

    code = str(res.get("code", ""))
    if code in ("00000", "0", "success"):
        return {"code": "00000", "data": res.get("data")}
    return res
