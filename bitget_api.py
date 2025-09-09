# -*- coding: utf-8 -*-
import os, time, hmac, hashlib, base64, json
from typing import Any, Dict, List, Optional
import urllib.parse, urllib.request

BITGET_HOST = os.getenv("BITGET_HOST", "https://api.bitget.com")
API_KEY     = os.getenv("BITGET_API_KEY", "")
API_SECRET  = os.getenv("BITGET_API_SECRET", "")
API_PASS    = os.getenv("BITGET_API_PASS", "")

HTTP_TIMEOUT = int(float(os.getenv("HTTP_TIMEOUT","12")))
DEFAULT_SIZE_STEP  = float(os.getenv("DEFAULT_SIZE_STEP","0.1"))
DEFAULT_PRICE_STEP = float(os.getenv("DEFAULT_PRICE_STEP","0.1"))
UMCBL_SUFFIX = "_UMCBL"

def convert_symbol(s: str) -> str:
    if not s: return ""
    t = str(s).upper().strip()
    if ":" in t:
        t = t.split(":")[-1]
    for sep in ["_", "-", "."]:
        t = t.replace(sep, "")
    t = t.replace("UMCBL","").replace("PERP","")
    return t  # e.g. BTCUSDT

def _core_to_umcbl(core: str) -> str:
    c = convert_symbol(core)
    if c.endswith("USDT"):
        return c + UMCBL_SUFFIX
    return c + "USDT" + UMCBL_SUFFIX

def _ts_ms() -> str: return str(int(time.time() * 1000))

def _sign(ts: str, method: str, path: str, query: Optional[Dict[str,Any]], body: Optional[Dict[str,Any]]) -> str:
    qs = "" if not query else "?" + urllib.parse.urlencode(query)
    b  = "" if not body else json.dumps(body, separators=(",", ":"), ensure_ascii=False)
    s  = ts + method.upper() + path + qs + b
    mac = hmac.new(API_SECRET.encode("utf-8"), s.encode("utf-8"), digestmod=hashlib.sha256).digest()
    return base64.b64encode(mac).decode()

def _headers(ts: str, sign: str) -> Dict[str,str]:
    return {
        "ACCESS-KEY": API_KEY,
        "ACCESS-SIGN": sign,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": API_PASS,
        "Content-Type": "application/json",
        "locale": "en-US",
    }

def _http(method: str, path: str, query: Optional[Dict[str,Any]]=None, body: Optional[Dict[str,Any]]=None) -> Dict[str,Any]:
    url = BITGET_HOST + path
    if query: url += "?" + urllib.parse.urlencode(query)
    ts = _ts_ms(); sign = _sign(ts, method, path, query, body)
    data = None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method.upper(), headers=_headers(ts, sign))
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            raw = r.read().decode("utf-8")
        return json.loads(raw or "{}")
    except Exception as e:
        return {"code":"HTTP_ERR", "msg": f"{type(e).__name__}: {e}"}

def _req_public(method, path, params=None):  return _http(method, path, params, None)
def _req_private(method, path, body=None, query=None): return _http(method, path, query, body)

def get_last_price(core: str) -> Optional[float]:
    sym = _core_to_umcbl(core)
    j = _req_public("GET", "/api/v2/mix/market/ticker", {"symbol": sym})
    try:
        data = j.get("data") or {}
        last = data.get("last")
        if last is not None: return float(last)
    except Exception: pass
    j1 = _req_public("GET", "/api/mix/v1/market/ticker", {"symbol": sym})
    try:
        data = j1.get("data") or {}
        last = data.get("last")
        if last is not None: return float(last)
    except Exception: pass
    return None

def get_symbol_spec(core: str) -> Dict[str, float]:
    sym = _core_to_umcbl(core)
    size_step, price_step = DEFAULT_SIZE_STEP, DEFAULT_PRICE_STEP
    j = _req_public("GET", "/api/v2/mix/market/contracts", {})
    try:
        for it in (j.get("data") or []):
            if (it.get("symbol") or "").upper() == sym:
                ps = it.get("priceTick"); ss = it.get("sizeTick")
                if ps: price_step = float(ps)
                if ss: size_step  = float(ss)
                return {"sizeStep": size_step, "priceStep": price_step}
    except Exception: pass
    j1 = _req_public("GET", "/api/mix/v1/market/contracts", {})
    try:
        for it in (j1.get("data") or []):
            if (it.get("symbol") or "").upper() == sym:
                ps = it.get("pricePrecision"); ss = it.get("sizeIncrement")
                if ps: price_step = float(ps)
                if ss: size_step  = float(ss)
                return {"sizeStep": size_step, "priceStep": price_step}
    except Exception: pass
    return {"sizeStep": size_step, "priceStep": price_step}

def round_down_step(x: float, step: float) -> float:
    if step <= 0: return float(x)
    k = int(float(x) / step)
    return float(f"{k * step:.12f}")

def set_leverage(core: str, leverage: float) -> Dict[str,Any]:
    sym = _core_to_umcbl(core)
    for hold in ("long", "short"):
        body = {"symbol": sym, "marginCoin": "USDT", "leverage": str(int(leverage)), "holdSide": hold}
        _req_private("POST", "/api/mix/v1/account/setLeverage", body)
    return {"code":"00000"}

def _compute_size(core: str, notional_usdt: float, leverage: float) -> float:
    last = get_last_price(core)
    if not last or last <= 0: return 0.0
    size = (notional_usdt * leverage) / last
    step = get_symbol_spec(core)["sizeStep"]
    return round_down_step(size, step)

def place_market_order(core: str, amount_usdt: float, side: str, leverage: float) -> Dict[str,Any]:
    sym = _core_to_umcbl(core)
    size = _compute_size(core, amount_usdt, leverage)
    if size <= 0: return {"code":"LOCAL_TICKER_FAIL", "msg":"ticker_none or size<=0"}
    try: set_leverage(core, leverage)
    except Exception: pass
    body = {
        "symbol": sym, "marginCoin": "USDT", "productType": "USDT-FUTURES",
        "size": f"{size}",
        "side": "buy" if side.lower()=="long" else "sell",
        "orderType": "market", "force": "gtc", "reduceOnly": False,
    }
    j = _req_private("POST", "/api/v2/mix/order/place-order", body)
    ok = str(j.get("code","")) in ("00000","0","200")
    return j if ok else {"code": j.get("code","HTTP_ERR"), "msg": j.get("msg") or j}

def place_reduce_by_size(core: str, size: float, side: str) -> Dict[str,Any]:
    sym = _core_to_umcbl(core)
    size = round_down_step(size, get_symbol_spec(core)["sizeStep"])
    if size <= 0: return {"code":"LOCAL_BAD_SIZE","msg":"size<=0"}
    body = {
        "symbol": sym, "marginCoin": "USDT", "productType": "USDT-FUTURES",
        "size": f"{size}",
        "side": "buy" if side.lower()=="short" else "sell",
        "orderType": "market", "force": "gtc", "reduceOnly": True,
    }
    j = _req_private("POST", "/api/v2/mix/order/place-order", body)
    ok = str(j.get("code","")) in ("00000","0","200")
    return j if ok else {"code": j.get("code","HTTP_ERR"), "msg": j.get("msg") or j}

def close_all_for_symbol(core: str) -> Dict[str,Any]:
    # v2 일괄 종료 API가 상황에 따라 잔량 남길 수 있어 감축 주문 루프로 처리하는 쪽을 권장
    return {"code":"DEPRECATED"}

def get_open_positions(symbol: Optional[str]=None) -> List[Dict[str,Any]]:
    j = _req_private("GET", "/api/v2/mix/position/all-position", query={"productType":"USDT-FUTURES"})
    out = []
    try:
        for it in (j.get("data") or []):
            if symbol and (it.get("symbol") or "") != _core_to_umcbl(symbol): continue
            sz = float(it.get("total") or it.get("holdVolume") or 0.0)
            sd = (it.get("holdSide") or it.get("side") or "").lower()
            out.append({
                "symbol": it.get("symbol"),
                "size": sz,
                "side": sd,
                "entryPrice": float(it.get("avgOpenPrice") or it.get("avgOpenPri") or 0.0),
                "unrealizedPnl": float(it.get("unrealizedPL") or 0.0),
                "leverage": float(it.get("leverage") or 0.0),
            })
    except Exception: return []
    return out
