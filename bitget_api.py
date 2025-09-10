# bitget_api.py — Bitget USDT-M Perp v2 adapter (강화본)
# -*- coding: utf-8 -*-
from __future__ import annotations
import os, time, json, hmac, hashlib, base64
from typing import Any, Dict, Optional, Tuple, List
import requests
from urllib.parse import urlencode

BITGET_HOST    = os.getenv("BITGET_HOST", "https://api.bitget.com")
API_KEY        = os.getenv("BITGET_API_KEY", "")
API_SECRET     = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE = os.getenv("BITGET_API_PASSPHRASE", "")

PRODUCT_TYPE    = os.getenv("BITGET_PRODUCT_TYPE", "USDT-FUTURES")
POSITION_MODE   = os.getenv("BITGET_POSITION_MODE", "oneway").lower().strip()
MARGIN_MODE_ENV = os.getenv("BITGET_MARGIN_MODE", "cross").lower().strip()

HTTP_TIMEOUT    = int(float(os.getenv("HTTP_TIMEOUT", "8")))
BITGET_DEBUG    = os.getenv("BITGET_DEBUG", "0") == "1"

AMOUNT_MODE     = os.getenv("AMOUNT_MODE", "notional").lower().strip()

DEFAULT_SIZE_STEP  = float(os.getenv("DEFAULT_SIZE_STEP", "0.001"))
DEFAULT_PRICE_STEP = float(os.getenv("DEFAULT_PRICE_STEP", "0.01"))

_SYMBOL_CACHE: Dict[str, Tuple[str, float]] = {}
_SYMBOL_CACHE_TTL = float(os.getenv("SYMBOL_CACHE_TTL", "300"))

def _dbg(*a):
    if BITGET_DEBUG: print("[bitget]", *a)

def convert_symbol(s: str) -> str:
    if not s: return ""
    t = str(s).upper().strip()
    if ":" in t: t = t.split(":")[-1]
    for sep in [" ","/","-",".","_"]:
        t = t.replace(sep, "")
    for suf in ["UMCBL","DMCBL","CMCBL","PERP"]:
        if t.endswith(suf):
            t = t[:-len(suf)]
    if not t.endswith("USDT"):
        t += "USDT"
    return t

def round_down_step(x: float, step: float) -> float:
    try:
        x = float(x); step = float(step)
    except: return float(x or 0.0)
    if step <= 0: return float(x)
    return int(x / step) * step

def _ts_ms() -> str:
    return str(int(time.time() * 1000))

def _headers(ts: str, sign: str) -> Dict[str, str]:
    return {
        "ACCESS-KEY": API_KEY,
        "ACCESS-SIGN": sign,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": API_PASSPHRASE,
        "Content-Type": "application/json"
    }

def _sign(ts: str, method: str, path_with_query: str, body: str = "") -> str:
    pre = f"{ts}{method.upper()}{path_with_query}{body}"
    mac = hmac.new(API_SECRET.encode(), pre.encode(), hashlib.sha256).digest()
    return base64.b64encode(mac).decode()

def _req_public(method: str, path: str, params: Optional[dict] = None) -> dict:
    url = BITGET_HOST + path
    try:
        if method == "GET":
            r = requests.get(url, params=params or {}, timeout=HTTP_TIMEOUT)
        else:
            r = requests.post(url, json=params or {}, timeout=HTTP_TIMEOUT)
        return r.json()
    except Exception as e:
        return {"code": "HTTP_ERR", "msg": f"{type(e).__name__}:{e}"}

def _req_private(method: str, path: str,
                 body: Optional[dict] = None,
                 query: Optional[dict] = None) -> dict:
    ts = _ts_ms()
    qstr = ""
    if query:
        qstr = "?" + urlencode(sorted([(str(k), str(v)) for k, v in query.items()]))
    path_for_sign = path + qstr
    body_str = json.dumps(body or {}, separators=(",", ":")) if method != "GET" else ""
    sign = _sign(ts, method, path_for_sign, body_str)
    url = BITGET_HOST + path
    try:
        if method == "GET":
            r = requests.get(url, params=query or {}, headers=_headers(ts, sign), timeout=HTTP_TIMEOUT)
        else:
            r = requests.post(url, params=query or {}, data=body_str,
                              headers=_headers(ts, sign), timeout=HTTP_TIMEOUT)
        return r.json()
    except Exception as e:
        return {"code": "HTTP_ERR", "msg": f"{type(e).__name__}:{e}"}

def _margin_mode() -> str:
    return "crossed" if (MARGIN_MODE_ENV or "cross").startswith("cross") else "isolated"

# ───────── Symbol/Spec
def _load_symbol_map() -> Dict[str, str]:
    out: Dict[str, str] = {}
    j = _req_public("GET", "/api/v2/mix/market/contracts", {"productType": PRODUCT_TYPE})
    try:
        for it in j.get("data") or []:
            ex = (it.get("symbol") or "").upper()
            core = convert_symbol(ex)
            if core: out[core] = ex
    except: pass
    return out

def _resolve_exchange_symbol_for_v1(core: str) -> str:
    core = convert_symbol(core); now = time.time()
    if core in _SYMBOL_CACHE and now - _SYMBOL_CACHE[core][1] < _SYMBOL_CACHE_TTL:
        return _SYMBOL_CACHE[core][0]
    m = _load_symbol_map()
    ex = m.get(core) or (core + "_UMCBL")
    _SYMBOL_CACHE[core] = (ex, now)
    return ex

def get_symbol_spec(core: str) -> Dict[str, Any]:
    base = convert_symbol(core)
    size_step = DEFAULT_SIZE_STEP; price_step = DEFAULT_PRICE_STEP
    j = _req_public("GET", "/api/v2/mix/market/contracts", {"productType": PRODUCT_TYPE})
    try:
        for it in j.get("data") or []:
            if convert_symbol(it.get("symbol") or "") == base:
                if it.get("priceTick"): price_step = float(it["priceTick"])
                if it.get("sizeTick"):  size_step  = float(it["sizeTick"])
                return {"sizeStep": size_step, "priceStep": price_step}
    except: pass
    ex = _resolve_exchange_symbol_for_v1(core)
    j1 = _req_public("GET", "/api/mix/v1/market/contracts", {})
    try:
        for it in j1.get("data") or []:
            if (it.get("symbol") or "").upper() == ex:
                ps = it.get("priceEndStep") or it.get("priceTick")
                ss = it.get("sizeTick") or it.get("volumePlace")
                if ps: price_step = float(ps)
                if ss is not None:
                    try: size_step = float(ss)
                    except: size_step = 10 ** (-int(ss))
                return {"sizeStep": size_step, "priceStep": price_step}
    except: pass
    return {"sizeStep": size_step, "priceStep": price_step}

# ───────── Price
def get_last_price(core: str) -> Optional[float]:
    base = convert_symbol(core)
    ex_v1 = _resolve_exchange_symbol_for_v1(core)

    def _f(x):
        try: v = float(x); return v if v > 0 else None
        except: return None

    for params in (
        ("/api/v2/mix/market/ticker", {"symbol": base, "productType": PRODUCT_TYPE}),
        ("/api/v2/mix/market/ticker", {"symbol": base}),
    ):
        j = _req_public("GET", *params)
        try:
            d = j.get("data") or {}
            p = _f(d.get("last") or d.get("close"))
            if p: return p
        except: pass

    for params in (
        ("/api/v2/mix/market/tickers", {"productType": PRODUCT_TYPE}),
        ("/api/v2/mix/market/tickers", {}),
    ):
        j = _req_public("GET", *params)
        try:
            for it in j.get("data") or []:
                if (it.get("symbol") or "").upper() == base:
                    p = _f(it.get("last") or it.get("close"))
                    if p: return p
        except: pass

    j = _req_public("GET", "/api/v2/mix/market/candles", {"symbol": base, "granularity": "60"})
    try:
        arr = j.get("data") or []
        if arr:
            p = _f(arr[0][4])
            if p: return p
    except: pass

    j = _req_public("GET", "/api/mix/v1/market/ticker", {"symbol": ex_v1})
    try:
        d = j.get("data") or {}
        p = _f(d.get("last") or d.get("close"))
        if p: return p
    except: pass
    j = _req_public("GET", "/api/mix/v1/market/depth", {"symbol": ex_v1, "limit": 1})
    try:
        d = j.get("data") or {}
        bids, asks = d.get("bids") or [], d.get("asks") or []
        if bids and asks:
            mid = (float(bids[0][0]) + float(asks[0][0])) / 2.0
            return mid if mid > 0 else None
    except: pass
    _dbg("no price", core)
    return None

# ───────── Account/Position
def set_position_mode(mode: str = "oneway") -> Dict[str, Any]:
    m = (mode or "oneway").lower()
    if m not in ("oneway","hedge"): m = "oneway"
    return _req_private("POST","/api/v2/mix/account/set-position-mode",
                        {"productType":PRODUCT_TYPE,"posMode":"one_way" if m=="oneway" else "hedge"})

def set_leverage(core: str, lev: float) -> Dict[str, Any]:
    return _req_private("POST","/api/v2/mix/account/set-leverage",{
        "symbol": convert_symbol(core), "productType": PRODUCT_TYPE, "marginCoin": "USDT",
        "leverage": str(int(lev or 1)), "holdSide": "long", "marginMode": ("crossed" if _margin_mode()=="crossed" else "isolated")
    })

def get_open_positions(symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    def _parse_v2(j):
        tmp=[]
        try:
            for it in (j or {}).get("data") or []:
                sym = (it.get("symbol") or "").upper()
                base = convert_symbol(sym)
                sd   = (it.get("holdSide") or it.get("side") or "").lower()
                sz   = float(it.get("total") or it.get("holdVolume") or it.get("available") or 0.0)
                ep   = float(it.get("avgOpenPrice") or it.get("openPrice") or 0.0)
                if base and sd in ("long","short") and sz>0:
                    tmp.append({"symbol": base, "size": sz, "side": sd, "entryPrice": ep})
        except Exception as e:
            _dbg("v2 parse err:", e)
        return tmp

    v2_queries = [
        {"productType": PRODUCT_TYPE},
        {"productType": PRODUCT_TYPE, "marginCoin": "USDT"},
        {"marginCoin": "USDT"},
        {},
    ]
    for q in v2_queries:
        try:
            j = _req_private("GET","/api/v2/mix/position/all-position", query=q)
            res = _parse_v2(j)
            if BITGET_DEBUG: _dbg("v2 all-position", q, "=>", len(res))
            if res:
                out = res
                break
        except Exception as e:
            _dbg("v2 all-position err:", e)

    base_filter = convert_symbol(symbol or "") if symbol else None
    if out and base_filter:
        out = [p for p in out if convert_symbol(p.get("symbol") or "") == base_filter]

    if not out:
        try:
            contracts = _load_symbol_map()
            cores = [base_filter] if base_filter else list(contracts.keys())
            found=[]
            for core in cores:
                if not core: continue
                ex = contracts.get(core) or _resolve_exchange_symbol_for_v1(core)
                j1 = _req_private("GET","/api/mix/v1/position/singlePosition", query={"symbol": ex})
                d  = (j1 or {}).get("data") or {}
                sz = float(d.get("total") or d.get("holdVolume") or 0.0)
                if sz>0:
                    sd = (d.get("holdSide") or d.get("side") or "").lower()
                    ep = float(d.get("avgOpenPrice") or d.get("openPrice") or 0.0)
                    found.append({"symbol": convert_symbol(core), "size": sz, "side": sd, "entryPrice": ep})
            if BITGET_DEBUG: _dbg("v1 scan found:", len(found))
            out = found
        except Exception as e:
            _dbg("v1 scan err:", e)
    return out

# ───────── Order/Reduce/Clear
def _compute_size(core: str, amount_usdt: float, leverage: float) -> float:
    price = float(get_last_price(core) or 0.0)
    if price <= 0: return 0.0
    step = get_symbol_spec(core).get("sizeStep", DEFAULT_SIZE_STEP)
    notional = (float(amount_usdt) * float(leverage or 1.0)) if AMOUNT_MODE == "margin" else float(amount_usdt)
    return round_down_step(notional / price, float(step))

def _normalize_side_for_oneway(s: str) -> str:
    s = (s or "").lower()
    return "buy" if s == "long" else ("sell" if s == "short" else "buy")

def place_market_order(core: str, amount_usdt: float, side: str, leverage: float) -> Dict[str, Any]:
    base = convert_symbol(core)
    size = _compute_size(core, amount_usdt, leverage)
    if size <= 0:
        return {"code": "LOCAL_TICKER_FAIL", "msg": "ticker_none or size<=0"}
    try:
        if leverage and leverage > 0:
            _ = set_leverage(core, leverage)
    except Exception as e:
        _dbg("set_leverage err:", e)
    body = {
        "symbol": base, "productType": PRODUCT_TYPE, "marginCoin": "USDT",
        "size": f"{size}", "side": _normalize_side_for_oneway(side),
        "orderType": "market", "timeInForceValue": "normal",
        "marginMode": _margin_mode()
    }
    return _req_private("POST","/api/v2/mix/order/place-order", body)

def place_reduce_by_size(core: str, contracts: float, side: str) -> Dict[str, Any]:
    base = convert_symbol(core)
    req_side = "sell" if (side or "").lower() == "long" else "buy"
    body = {
        "symbol": base, "productType": PRODUCT_TYPE, "marginCoin": "USDT",
        "size": f"{contracts}", "side": req_side,
        "orderType": "market", "timeInForceValue": "normal",
        "reduceOnly": True, "marginMode": _margin_mode()
    }
    return _req_private("POST","/api/v2/mix/order/place-order", body)

def close_all_for_symbol(core: str) -> Dict[str, Any]:
    return _req_private("POST","/api/v2/mix/order/close-positions", {
        "symbol": convert_symbol(core), "marginCoin": "USDT", "productType": PRODUCT_TYPE
    })
