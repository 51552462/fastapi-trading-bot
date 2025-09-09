# -*- coding: utf-8 -*-
"""
bitget_api.py — Bitget REST adapter (USDT-M perpetual, one-way)

요약
- V2 모든 엔드포인트(마켓/주문/계정/포지션)는 기본 심볼(ex: BTCUSDT) + productType 사용
- V1 폴백 엔드포인트만 거래소 심볼(ex: BTCUSDT_UMCBL) 사용
- get_last_price(): v2 단건(+/-productType) → v2 목록(+/-productType) → v2 캔들 → v1 티커 → v1 호가(mid)
- 주문:
  * 신규 진입(place_market_order): reduceOnly 필드 '아예 미포함' (40017 방지)
  * 부분청산(place_reduce_by_size): reduceOnly=True
  * 전량종료(close_all_for_symbol): v2 close-positions
"""

from __future__ import annotations
import os, time, json, hmac, hashlib, base64
from typing import Any, Dict, Optional, Tuple, List
import requests

# ---------------- Env ----------------
BITGET_HOST    = os.getenv("BITGET_HOST", "https://api.bitget.com")
API_KEY        = os.getenv("BITGET_API_KEY", "")
API_SECRET     = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE = os.getenv("BITGET_API_PASSPHRASE", "")
HTTP_TIMEOUT   = int(float(os.getenv("HTTP_TIMEOUT", "8")))
BITGET_DEBUG   = os.getenv("BITGET_DEBUG", "0") == "1"

POSITION_MODE   = os.getenv("BITGET_POSITION_MODE", "oneway").lower().strip()
MARGIN_MODE_ENV = os.getenv("BITGET_MARGIN_MODE", "cross").lower().strip()

PRODUCT_TYPE    = os.getenv("BITGET_PRODUCT_TYPE", "USDT-FUTURES")  # USDT-M Perpetual
AMOUNT_MODE     = os.getenv("AMOUNT_MODE", "notional").lower().strip()  # notional | margin

DEFAULT_SIZE_STEP  = float(os.getenv("DEFAULT_SIZE_STEP", "0.001"))
DEFAULT_PRICE_STEP = float(os.getenv("DEFAULT_PRICE_STEP", "0.01"))

# 심볼 캐시: core(BTCUSDT) -> (exchange_symbol, ts)  (v1 폴백용)
_SYMBOL_CACHE: Dict[str, Tuple[str, float]] = {}
_SYMBOL_CACHE_TTL = float(os.getenv("SYMBOL_CACHE_TTL", "300"))

def _dbg(*a):
    if BITGET_DEBUG:
        print("[bitget]", *a)

# ---------------- Helpers ----------------
def convert_symbol(s: str) -> str:
    """어떤 형태든 'BTCUSDT' 코어 형태로."""
    if not s:
        return ""
    t = str(s).upper().strip()
    if ":" in t:
        t = t.split(":")[-1]
    for sep in [" ", "/", "-", ".", "_"]:
        t = t.replace(sep, "")
    for suf in ["UMCBL", "DMCBL", "CMCBL", "PERP"]:
        if t.endswith(suf):
            t = t[: -len(suf)]
    if not t.endswith("USDT"):
        t = t + "USDT"
    return t

def _base_symbol(sym_or_core: str) -> str:
    """V2 모든 엔드포인트에 넣을 기본 심볼."""
    return convert_symbol(sym_or_core)

def round_down_step(x: float, step: float) -> float:
    try:
        x = float(x); step = float(step)
    except Exception:
        return float(x or 0.0)
    if step <= 0:
        return float(x)
    return (int(x / step)) * step

# ---------------- HTTP & Signing ----------------
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
    return "crossed" if (MARGIN_MODE_ENV or "cross").lower().startswith("cross") else "isolated"

# ---------------- Contract map (v1 폴백용) ----------------
def _load_symbol_map() -> Dict[str, str]:
    """코어(BTCUSDT) → 거래소 심볼(BTCUSDT_UMCBL)"""
    out: Dict[str, str] = {}
    j = _req_public("GET", "/api/v2/mix/market/contracts", {"productType": PRODUCT_TYPE})
    try:
        for it in j.get("data") or []:
            ex_sym = (it.get("symbol") or "").upper()
            core   = convert_symbol(ex_sym)
            if core:
                out[core] = ex_sym
    except Exception:
        pass
    _dbg("symbol_map size:", len(out))
    return out

def _resolve_exchange_symbol_for_v1(core: str) -> str:
    """v1 폴백에서만 사용. 거래소 심볼 반환."""
    core = convert_symbol(core)
    now = time.time()
    cached = _SYMBOL_CACHE.get(core)
    if cached and now - cached[1] < _SYMBOL_CACHE_TTL:
        return cached[0]
    m = _load_symbol_map()
    ex = m.get(core) or (core + "_UMCBL")
    _SYMBOL_CACHE[core] = (ex, now)
    return ex

# ---------------- Market / Specs ----------------
def get_last_price(core: str) -> Optional[float]:
    """
    Ultra-robust price retrieval:
    1) v2 /ticker (+productType)
    1b) v2 /ticker (no productType)
    2) v2 /tickers (+productType)
    2b) v2 /tickers (no productType)
    3) v2 /candles (1m)
    4) v1 /market/ticker (exchange id)
    5) v1 /market/depth  (mid price)
    """
    base = _base_symbol(core)
    ex_v1 = _resolve_exchange_symbol_for_v1(core)

    def _as_float(x):
        try:
            v = float(x)
            return v if v > 0 else None
        except Exception:
            return None

    # 1) v2 ticker (+productType)
    j1 = _req_public("GET", "/api/v2/mix/market/ticker", {"symbol": base, "productType": PRODUCT_TYPE})
    try:
        d = j1.get("data") or {}
        p = _as_float(d.get("last") or d.get("close"))
        if p: return p
    except Exception: pass

    # 1b) v2 ticker (no productType)
    j1b = _req_public("GET", "/api/v2/mix/market/ticker", {"symbol": base})
    try:
        d = j1b.get("data") or {}
        p = _as_float(d.get("last") or d.get("close"))
        if p: return p
    except Exception: pass

    # 2) v2 tickers (+productType)
    j2 = _req_public("GET", "/api/v2/mix/market/tickers", {"productType": PRODUCT_TYPE})
    try:
        for it in j2.get("data") or []:
            if (it.get("symbol") or "").upper() == base:
                p = _as_float(it.get("last") or it.get("close"))
                if p: return p
    except Exception: pass

    # 2b) v2 tickers (no productType)
    j2b = _req_public("GET", "/api/v2/mix/market/tickers", {})
    try:
        for it in j2b.get("data") or []:
            if (it.get("symbol") or "").upper() == base:
                p = _as_float(it.get("last") or it.get("close"))
                if p: return p
    except Exception: pass

    # 3) v2 candles (1m)
    j3 = _req_public("GET", "/api/v2/mix/market/candles", {"symbol": base, "granularity": "60"})
    try:
        arr = j3.get("data") or []
        if arr:
            p = _as_float(arr[0][4])  # close
            if p: return p
    except Exception: pass

    # 4) v1 ticker (exchange id)
    j4 = _req_public("GET", "/api/mix/v1/market/ticker", {"symbol": ex_v1})
    try:
        d = j4.get("data") or {}
        p = _as_float(d.get("last") or d.get("close"))
        if p: return p
    except Exception: pass

    # 5) v1 depth (mid)
    j5 = _req_public("GET", "/api/mix/v1/market/depth", {"symbol": ex_v1, "limit": 1})
    try:
        d = j5.get("data") or {}
        bids = d.get("bids") or []
        asks = d.get("asks") or []
        if bids and asks:
            bid = float(bids[0][0]); ask = float(asks[0][0])
            mid = (bid + ask) / 2.0
            if mid > 0: return mid
    except Exception: pass

    _dbg("price not found for", core, "base=", base, "ex(v1)=", ex_v1)
    return None

def get_symbol_spec(core: str) -> Dict[str, Any]:
    base = _base_symbol(core)
    size_step, price_step = DEFAULT_SIZE_STEP, DEFAULT_PRICE_STEP

    # v2 contracts
    j = _req_public("GET", "/api/v2/mix/market/contracts", {"productType": PRODUCT_TYPE})
    try:
        for it in j.get("data") or []:
            if convert_symbol(it.get("symbol") or "") == base:
                ps = it.get("priceTick"); ss = it.get("sizeTick")
                if ps: price_step = float(ps)
                if ss: size_step  = float(ss)
                return {"sizeStep": size_step, "priceStep": price_step}
    except Exception: pass

    # v1 fallback
    ex_v1 = _resolve_exchange_symbol_for_v1(core)
    j1 = _req_public("GET", "/api/mix/v1/market/contracts", {})
    try:
        for it in j1.get("data") or []:
            if (it.get("symbol") or "").upper() == ex_v1:
                ps = it.get("priceEndStep") or it.get("priceTick")
                ss = it.get("sizeTick") or it.get("volumePlace")
                if ps: price_step = float(ps)
                if ss is not None:
                    try: size_step = float(ss)
                    except Exception: size_step = 10 ** (-int(ss))
                return {"sizeStep": size_step, "priceStep": price_step}
    except Exception: pass

    return {"sizeStep": size_step, "priceStep": price_step}

def symbol_exists(core: str) -> bool:
    base = _base_symbol(core)
    j = _req_public("GET", "/api/v2/mix/market/ticker", {"symbol": base, "productType": PRODUCT_TYPE})
    if j.get("data"):
        return True
    j2 = _req_public("GET", "/api/v2/mix/market/ticker", {"symbol": base})
    return bool(j2.get("data"))

# ---------------- Account / Positions (V2) ----------------
def set_position_mode(mode: str = "oneway") -> Dict[str, Any]:
    m = (mode or "oneway").lower()
    if m not in ("oneway", "hedge"):
        m = "oneway"
    body = {"productType": PRODUCT_TYPE, "posMode": "one_way" if m == "oneway" else "hedge"}
    return _req_private("POST", "/api/v2/mix/account/set-position-mode", body)

def _margin_mode() -> str:
    return _margin_mode_v2()

def set_leverage(core: str, leverage: float) -> Dict[str, Any]:
    base = _base_symbol(core)
    body = {
        "symbol": base,
        "productType": PRODUCT_TYPE,
        "marginCoin": "USDT",
        "leverage": str(int(leverage or 1)),
        "holdSide": "long",           # one-way
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
            if symbol and _base_symbol(it.get("symbol") or "") != _base_symbol(symbol):
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

# ---------------- Orders (V2) ----------------
def _normalize_side_for_oneway(side: str) -> str:
    s = (side or "").lower()
    if s == "long":  return "buy"
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
    base = _base_symbol(core)
    size = _compute_size(core, amount_usdt, leverage)
    if size <= 0:
        return {"code": "LOCAL_TICKER_FAIL", "msg": "ticker_none or size<=0"}

    # leverage best-effort
    try:
        if leverage and leverage > 0:
            _ = set_leverage(core, leverage)
    except Exception as e:
        _dbg("set_leverage error:", e)

    body = {
        "symbol": base,
        "productType": PRODUCT_TYPE,
        "marginCoin": "USDT",
        "size": f"{size}",
        "side": _normalize_side_for_oneway(side),  # buy/sell
        "orderType": "market",
        "timeInForceValue": "normal",
        # 신규 진입에서 reduceOnly는 '아예 미포함'
        "marginMode": _margin_mode(),
    }
    return _req_private("POST", "/api/v2/mix/order/place-order", body)

def place_reduce_by_size(core: str, contracts: float, side: str) -> Dict[str, Any]:
    """contracts 만큼 시장가 reduceOnly (분할청산용). side=현재포지션(long/short)"""
    base = _base_symbol(core)
    req_side = "sell" if (side or "").lower() == "long" else "buy"  # 반대 방향
    body = {
        "symbol": base,
        "productType": PRODUCT_TYPE,
        "marginCoin": "USDT",
        "size": f"{contracts}",
        "side": req_side,
        "orderType": "market",
        "timeInForceValue": "normal",
        "reduceOnly": True,   # 청산에만 사용
        "marginMode": _margin_mode(),
    }
    return _req_private("POST", "/api/v2/mix/order/place-order", body)

def close_all_for_symbol(core: str) -> Dict[str, Any]:
    """심볼 전량 종료."""
    base = _base_symbol(core)
    body = {"symbol": base, "marginCoin": "USDT", "productType": PRODUCT_TYPE}
    return _req_private("POST", "/api/v2/mix/order/close-positions", body)
