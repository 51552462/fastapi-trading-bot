# -*- coding: utf-8 -*-
"""
bitget_api.py — Bitget REST adapter (USDT-M perpetual, one-way)

What changed (fix pack):
- V2 market endpoints (ticker/candles) **must** receive the base symbol without `_UMCBL`.
  This was returning `last: null`, which then cascaded into `LOCAL_TICKER_FAIL`.
  We now convert to base (e.g., "DOGEUSDT") for V2 market endpoints only.
- Symbol resolution keeps using the exchange contract id with suffix (e.g., "DOGEUSDT_UMCBL")
  for trading/account endpoints.
- Added stronger fallbacks for price retrieval and robust symbol cache.
"""

from __future__ import annotations

import os, time, json, hmac, hashlib, base64
from typing import Any, Dict, Optional, Tuple, List

import requests

# ---------- Env ----------
BITGET_HOST   = os.getenv("BITGET_HOST", "https://api.bitget.com")
API_KEY       = os.getenv("BITGET_API_KEY", "")
API_SECRET    = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE= os.getenv("BITGET_API_PASSPHRASE", "")
HTTP_TIMEOUT  = int(float(os.getenv("HTTP_TIMEOUT", "8")))
BITGET_DEBUG  = os.getenv("BITGET_DEBUG", "0") == "1"

# One-way only (the rest of the code assumes one-way)
POSITION_MODE = os.getenv("BITGET_POSITION_MODE", "oneway").lower().strip()

# margin mode: cross / isolated  → Bitget v2 expects crossed/isolated
MARGIN_MODE_ENV = os.getenv("BITGET_MARGIN_MODE", "cross").lower().strip()

# V2 requires productType for USDT-M perpetuals
PRODUCT_TYPE  = "USDT-FUTURES"

# Amount mode: "notional" (default) or "margin" (rare)
AMOUNT_MODE   = os.getenv("AMOUNT_MODE", "notional").lower().strip()

DEFAULT_SIZE_STEP  = float(os.getenv("DEFAULT_SIZE_STEP", "0.001"))
DEFAULT_PRICE_STEP = float(os.getenv("DEFAULT_PRICE_STEP", "0.01"))

# Contract symbol cache
_SYMBOL_CACHE: Dict[str, Tuple[str, float]] = {}  # core->(exchange_symbol, ts)
_SYMBOL_CACHE_TTL = float(os.getenv("SYMBOL_CACHE_TTL", "300"))  # 5 min

def _dbg(*a):
    if BITGET_DEBUG: print("[bitget]", *a)

# -------- Normalizers --------
def convert_symbol(s: str) -> str:
    """
    Normalize any variant to base 'BTCUSDT' style.
    E.g. BINANCE:BTCUSDT, BTC-USDT, BTCUSDT_UMCBL, BTCUSDT.PERP → BTCUSDT
    """
    if not s: return ""
    t = str(s).upper().strip()
    if ":" in t: t = t.split(":")[-1]
    for sep in ["_", "-", ".", "/"]:
        t = t.replace(sep, "")
    for suf in ["UMCBL", "DMCBL", "CMCBL", "PERP"]:
        if t.endswith(suf):
            t = t[: -len(suf)]
    if not t.endswith("USDT"):
        t = t + "USDT"
    return t

def _v2_market_symbol(sym_or_core: str) -> str:
    """
    V2 market endpoints (ticker/candles) expect base symbol WITHOUT suffix.
    """
    return convert_symbol(sym_or_core)

def round_down_step(x: float, step: float) -> float:
    try:
        x = float(x); step = float(step)
    except Exception:
        return float(x or 0.0)
    if step <= 0: return float(x)
    return (int(x / step)) * step

# -------- Signing / HTTP --------
def _ts_ms() -> str:
    return str(int(time.time() * 1000))

def _headers(ts: str, sign: str) -> Dict[str, str]:
    return {
        "ACCESS-KEY":        API_KEY,
        "ACCESS-SIGN":       sign,
        "ACCESS-TIMESTAMP":  ts,
        "ACCESS-PASSPHRASE": API_PASSPHRASE,
        "Content-Type":      "application/json",
    }

def _sign(ts: str, method: str, path: str, body: str = "") -> str:
    # Bitget v2: prehash = timestamp + method + requestPath + body
    prehash = f"{ts}{method.upper()}{path}{body}"
    mac = hmac.new(API_SECRET.encode(), prehash.encode(), hashlib.sha256).digest()
    return base64.b64encode(mac).decode()

def _req_public(method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = BITGET_HOST + path
    try:
        if method.upper() == "GET":
            r = requests.get(url, params=params or {}, timeout=HTTP_TIMEOUT)
        else:
            r = requests.post(url, json=params or {}, timeout=HTTP_TIMEOUT)
        return r.json()
    except Exception as e:
        return {"code": "HTTP_ERR", "msg": f"{type(e).__name__}: {e}"}

def _req_private(method: str, path: str, body: Optional[Dict[str, Any]] = None, query: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = BITGET_HOST + path
    ts = _ts_ms()
    body_str = json.dumps(body or {}, separators=(",", ":"))
    sign = _sign(ts, method, path, body_str if method.upper() != "GET" else "")
    try:
        if method.upper() == "GET":
            r = requests.get(url, params=query or {}, headers=_headers(ts, sign), timeout=HTTP_TIMEOUT)
        elif method.upper() == "POST":
            r = requests.post(url, params=query or {}, data=body_str, headers=_headers(ts, sign), timeout=HTTP_TIMEOUT)
        else:
            r = requests.request(method.upper(), url, params=query or {}, data=body_str, headers=_headers(ts, sign), timeout=HTTP_TIMEOUT)
        return r.json()
    except Exception as e:
        return {"code": "HTTP_ERR", "msg": f"{type(e).__name__}: {e}"}

def _margin_mode_v2() -> str:
    m = (MARGIN_MODE_ENV or "cross").lower()
    return "crossed" if m.startswith("cross") else "isolated"

# -------- Contract discovery --------
def _load_symbol_map() -> Dict[str, str]:
    """
    Build map: core (BTCUSDT) -> exchange contract symbol (BTCUSDT_UMCBL)
    """
    out: Dict[str, str] = {}
    j = _req_public("GET", "/api/v2/mix/market/contracts", {"productType": PRODUCT_TYPE})
    try:
        for it in j.get("data") or []:
            ex_sym = (it.get("symbol") or "").upper()
            core = convert_symbol(ex_sym)
            if core:
                out[core] = ex_sym
    except Exception:
        pass
    _dbg("symbol_map size:", len(out))
    return out

def _resolve_exchange_symbol(core: str) -> str:
    core = convert_symbol(core)
    now = time.time()
    cached = _SYMBOL_CACHE.get(core)
    if cached and now - cached[1] < _SYMBOL_CACHE_TTL:
        return cached[0]
    m = _load_symbol_map()
    ex = m.get(core) or (core + "_UMCBL")  # last resort
    _SYMBOL_CACHE[core] = (ex, now)
    return ex

# -------- Market / Specs --------
def get_last_price(core: str) -> Optional[float]:
    """
    Solid price retrieval:
    1) v2 market ticker (base symbol, +productType)
    2) v2 market tickers (list) then match either base or exchange id
    3) v2 candles (1m, base symbol)
    4) v1 ticker (exchange id)
    5) v1 depth → mid price
    """
    ex_sym = _resolve_exchange_symbol(core)         # e.g., DOGEUSDT_UMCBL
    base   = _v2_market_symbol(ex_sym)              # e.g., DOGEUSDT

    # 1) v2 ticker
    j1 = _req_public("GET", "/api/v2/mix/market/ticker", {"symbol": base, "productType": PRODUCT_TYPE})
    try:
        data = j1.get("data") or {}
        last = data.get("last") or data.get("close")
        if last is not None and float(last) > 0:
            return float(last)
    except Exception:
        pass

    # 2) v2 tickers (list)
    j2 = _req_public("GET", "/api/v2/mix/market/tickers", {"productType": PRODUCT_TYPE})
    try:
        for it in j2.get("data") or []:
            sym_field = (it.get("symbol") or "").upper()
            if sym_field in (base, ex_sym):
                last = it.get("last") or it.get("close")
                if last is not None and float(last) > 0:
                    return float(last)
    except Exception:
        pass

    # 3) v2 candles (1m)
    j3 = _req_public("GET", "/api/v2/mix/market/candles", {"symbol": base, "granularity": "60"})
    try:
        arr = j3.get("data") or []
        if arr:
            close_px = float(arr[0][4])
            if close_px > 0:
                return close_px
    except Exception:
        pass

    # 4) v1 ticker (needs exchange id)
    j4 = _req_public("GET", "/api/mix/v1/market/ticker", {"symbol": ex_sym})
    try:
        data = j4.get("data") or {}
        last = data.get("last")
        if last is not None and float(last) > 0:
            return float(last)
    except Exception:
        pass

    # 5) v1 depth → mid
    j5 = _req_public("GET", "/api/mix/v1/market/depth", {"symbol": ex_sym, "limit": 1})
    try:
        data = j5.get("data") or {}
        bids = data.get("bids") or []
        asks = data.get("asks") or []
        if bids and asks:
            bid = float(bids[0][0]); ask = float(asks[0][0])
            mid = (bid + ask) / 2.0
            if mid > 0:
                return mid
    except Exception:
        pass
    return None

def get_symbol_spec(core: str) -> Dict[str, Any]:
    ex_sym = _resolve_exchange_symbol(core)
    size_step, price_step = DEFAULT_SIZE_STEP, DEFAULT_PRICE_STEP

    # v2 instruments
    j = _req_public("GET", "/api/v2/mix/market/contracts", {"productType": PRODUCT_TYPE})
    try:
        for it in j.get("data") or []:
            if (it.get("symbol") or "").upper() == ex_sym:
                ps = it.get("priceTick"); ss = it.get("sizeTick")
                if ps: price_step = float(ps)
                if ss: size_step  = float(ss)
                return {"sizeStep": size_step, "priceStep": price_step}
    except Exception:
        pass

    # v1 fallback
    j1 = _req_public("GET", "/api/mix/v1/market/contracts", {})
    try:
        for it in j1.get("data") or []:
            if (it.get("symbol") or "").upper() == ex_sym:
                ps = it.get("priceEndStep") or it.get("priceTick")
                ss = it.get("sizeTick") or it.get("volumePlace")
                if ps: price_step = float(ps)
                if ss is not None:
                    try: size_step = float(ss)
                    except Exception: size_step = 10 ** (-int(ss))
                return {"sizeStep": size_step, "priceStep": price_step}
    except Exception:
        pass

    return {"sizeStep": size_step, "priceStep": price_step}

def symbol_exists(core: str) -> bool:
    # Use base symbol for v2 market ping
    base = _v2_market_symbol(core)
    j = _req_public("GET", "/api/v2/mix/market/ticker", {"symbol": base, "productType": PRODUCT_TYPE})
    return bool(j.get("data"))

# -------- Account / Positions --------
def set_position_mode(mode: str = "oneway") -> Dict[str, Any]:
    m = (mode or "oneway").lower()
    if m not in ("oneway", "hedge"): m = "oneway"
    body = {"productType": PRODUCT_TYPE, "posMode": "one_way" if m=="oneway" else "hedge"}
    return _req_private("POST", "/api/v2/mix/account/set-position-mode", body)

def _margin_mode() -> str:
    return _margin_mode_v2()

def set_leverage(core: str, leverage: float) -> Dict[str, Any]:
    ex_sym = _resolve_exchange_symbol(core)
    body = {
        "symbol": ex_sym,
        "productType": PRODUCT_TYPE,
        "marginCoin": "USDT",
        "leverage": str(int(leverage or 1)),
        "holdSide": "long",
        "marginMode": _margin_mode(),
    }
    return _req_private("POST", "/api/v2/mix/account/set-leverage", body)

def get_open_positions(symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    q = {"productType": PRODUCT_TYPE}
    j = _req_private("GET", "/api/v2/mix/position/all-position", query=q)
    arr: List[Dict[str, Any]] = []
    try:
        data = j.get("data") or []
        for it in data:
            if symbol:
                target = _resolve_exchange_symbol(symbol).upper()
                if (it.get("symbol") or "").upper() != target:
                    continue
            sz = float(it.get("total") or it.get("holdVolume") or 0.0)
            sd = (it.get("holdSide") or it.get("side") or "").lower()
            arr.append({
                "symbol": it.get("symbol"),
                "size": sz,
                "side": sd,  # long/short
                "entryPrice": float(it.get("avgOpenPrice") or it.get("openPrice") or 0.0),
                "unrealizedPnl": float(it.get("unrealizedPL") or 0.0),
            })
    except Exception:
        pass
    return arr

# -------- Orders --------
def _normalize_side_for_oneway(side: str) -> str:
    s = (side or "").lower()
    if s == "long": return "buy"
    if s == "short": return "sell"
    return "buy"

def _compute_size(core: str, amount_usdt: float, leverage: float) -> float:
    price = float(get_last_price(core) or 0.0)
    if price <= 0: 
        return 0.0
    spec = get_symbol_spec(core)
    if AMOUNT_MODE == "margin":
        notional = float(amount_usdt) * float(leverage or 1.0)
    else:
        notional = float(amount_usdt)
    size = notional / price
    size = round_down_step(size, float(spec.get("sizeStep", DEFAULT_SIZE_STEP)))
    return size

def place_market_order(core: str, amount_usdt: float, side: str, leverage: float) -> Dict[str, Any]:
    ex_sym = _resolve_exchange_symbol(core)
    size = _compute_size(core, amount_usdt, leverage)
    if size <= 0:
        return {"code": "LOCAL_TICKER_FAIL", "msg": "ticker_none or size<=0"}

    # best-effort set leverage (ignore failures)
    try:
        if leverage and leverage > 0:
            _ = set_leverage(core, leverage)
    except Exception as e:
        _dbg("set_leverage err:", e)

    req_side = _normalize_side_for_oneway(side)
    body = {
        "symbol": ex_sym,
        "productType": PRODUCT_TYPE,
        "marginCoin": "USDT",
        "size": f"{size}",
        "side": req_side,             # buy/sell
        "orderType": "market",
        "timeInForceValue": "normal",
        "reduceOnly": False,
        "marginMode": _margin_mode(),
    }
    j = _req_private("POST", "/api/v2/mix/order/place-order", body)
    return j

def place_reduce_by_size(core: str, contracts: float, side: str) -> Dict[str, Any]:
    ex_sym = _resolve_exchange_symbol(core)
    req_side = "sell" if (side or "").lower() == "long" else "buy"  # reduce opposite
    body = {
        "symbol": ex_sym,
        "productType": PRODUCT_TYPE,
        "marginCoin": "USDT",
        "size": f"{contracts}",
        "side": req_side,
        "orderType": "market",
        "timeInForceValue": "normal",
        "reduceOnly": True,
        "marginMode": _margin_mode(),
    }
    j = _req_private("POST", "/api/v2/mix/order/place-order", body)
    return j

def close_all_for_symbol(core: str) -> Dict[str, Any]:
    ex_sym = _resolve_exchange_symbol(core)
    body = {"symbol": ex_sym, "marginCoin": "USDT", "productType": PRODUCT_TYPE}
    j = _req_private("POST", "/api/v2/mix/order/close-positions", body)
    return j
