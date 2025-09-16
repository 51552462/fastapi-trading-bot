# -*- coding: utf-8 -*-
"""
Bitget REST API helper for USDT-M perpetual (UMCBL) — v2 우선, v1 폴백
요구 인터페이스(트레이더와의 호환):
  convert_symbol, get_last_price, get_open_positions,
  place_market_order, place_reduce_by_size, get_symbol_spec, round_down_step
"""

from __future__ import annotations
import os
import time
import math
import json
import hmac
import hashlib
import base64
import requests
from typing import Any, Dict, Optional, Tuple, List
from urllib.parse import urlencode

# ─────────────────────────────────────────────────────────
# ENV
# ─────────────────────────────────────────────────────────
BASE_URL  = os.getenv("BITGET_BASE_URL", "https://api.bitget.com")

API_KEY   = os.getenv("BITGET_API_KEY", "")
API_SEC   = os.getenv("BITGET_API_SECRET", "")
API_PASS  = os.getenv("BITGET_API_PASSWORD", "")

# v2 경로
USE_V2               = os.getenv("BITGET_USE_V2", "1") == "1"
V2_TICKER_PATH       = os.getenv("BITGET_V2_TICKER_PATH", "/api/v2/mix/market/ticker")
V2_MARK_PATH         = os.getenv("BITGET_V2_MARK_PATH", "/api/v2/mix/market/mark-price")
V2_DEPTH_PATH        = os.getenv("BITGET_V2_DEPTH_PATH", "/api/v2/mix/market/orderbook")
V2_PLACE_ORDER_PATH  = os.getenv("BITGET_V2_PLACE_ORDER_PATH", "/api/v2/mix/order/place-order")
V2_POSITIONS_PATH    = os.getenv("BITGET_V2_POSITIONS_PATH", "/api/v2/mix/position/all-position")

# v1 폴백
V1_TICKER_PATH       = os.getenv("BITGET_V1_TICKER_PATH", "/api/mix/market/ticker")
V1_PLACE_ORDER_PATH  = os.getenv("BITGET_V1_PLACE_ORDER_PATH", "/api/mix/v1/order/placeOrder")
V1_POSITIONS_PATH    = os.getenv("BITGET_V1_POSITIONS_PATH", "/api/mix/v1/position/allPosition")

# productType — 지원팀이 문서에서 두 표기를 모두 언급(umcbl / USDT-FUTURES)
V2_PRODUCT_TYPE      = os.getenv("BITGET_V2_PRODUCT_TYPE", "umcbl")
V2_PRODUCT_TYPE_ALT  = os.getenv("BITGET_V2_PRODUCT_TYPE_ALT", "USDT-FUTURES")  # fallback try
MARGIN_COIN          = os.getenv("BITGET_MARGIN_COIN", "USDT")

# 티커 품질/캐시
STRICT_TICKER        = os.getenv("STRICT_TICKER", "0") == "1"
ALLOW_DEPTH_FALLBACK = os.getenv("ALLOW_DEPTH_FALLBACK", "1") == "1"
TICKER_TTL           = float(os.getenv("TICKER_TTL", "3"))

# 심볼 별칭
try:
    SYMBOL_ALIASES = json.loads(os.getenv("SYMBOL_ALIASES_JSON", "") or "{}")
except Exception:
    SYMBOL_ALIASES = {}

TRACE = os.getenv("TRACE_LOG", "0") == "1"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "auto-trader/1.0"})

# ─────────────────────────────────────────────────────────
# 로그/서명/HTTP
# ─────────────────────────────────────────────────────────
def _log(msg: str):
    if TRACE:
        print(msg, flush=True)

def _ts_ms() -> str:
    return str(int(time.time() * 1000))

def _sign(message: str) -> str:
    mac = hmac.new(API_SEC.encode("utf-8"), msg=message.encode("utf-8"), digestmod=hashlib.sha256)
    return base64.b64encode(mac.digest()).decode()

def _auth_headers(method: str, path: str, query: Dict[str, Any] | None, body: str = "") -> Dict[str, str]:
    """
    Signature = base64(HmacSHA256(secret, timestamp + method + requestPath + '?' + queryString + body))
    - GET: queryString는 urlencode로 직렬화한 것을 사용
    """
    ts = _ts_ms()
    qstr = urlencode(query or {})
    pre = ts + method.upper() + path + (f"?{qstr}" if qstr else "") + body
    sign = _sign(pre)
    return {
        "ACCESS-KEY": API_KEY,
        "ACCESS-SIGN": sign,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": API_PASS,
        "Content-Type": "application/json",
    }

def _http_get(path: str, params: Dict[str, Any], auth: bool = False, timeout: int = 8) -> Dict[str, Any]:
    url = BASE_URL + path
    if auth:
        headers = _auth_headers("GET", path, params, "")
        r = SESSION.get(url, params=params, headers=headers, timeout=timeout)
    else:
        r = SESSION.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def _http_post(path: str, payload: Dict[str, Any], auth: bool = True, timeout: int = 8) -> Dict[str, Any]:
    url = BASE_URL + path
    body = json.dumps(payload or {})
    headers = _auth_headers("POST", path, None, body) if auth else {"Content-Type": "application/json"}
    r = SESSION.post(url, data=body, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()

# ─────────────────────────────────────────────────────────
# 심볼 정규화 / 스펙
# ─────────────────────────────────────────────────────────
def convert_symbol(sym: str) -> str:
    s = (sym or "").upper().strip()
    s = SYMBOL_ALIASES.get(s, s)
    for suf in ("_UMCBL", "-UMCBL", "UMCBL", "_CMCBL", "-CMCBL", "CMCBL"):
        if s.endswith(suf):
            s = s.replace(suf, "")
    return s.replace("-", "").replace("_", "")

def _v2_product_types() -> List[str]:
    # umcbl(기본) → USDT-FUTURES(대체) 순서로 시도
    seen, out = set(), []
    for x in (V2_PRODUCT_TYPE, V2_PRODUCT_TYPE_ALT):
        x = (x or "").strip()
        if x and x not in seen:
            seen.add(x); out.append(x)
    return out if out else ["umcbl"]

_spec_cache: Dict[str, Dict[str, Any]] = {}
def get_symbol_spec(symbol: str) -> Dict[str, Any]:
    sym = convert_symbol(symbol)
    sp = _spec_cache.get(sym)
    if sp: return sp
    sp = {"sizeStep": 0.001, "priceStep": 0.01}
    _spec_cache[sym] = sp
    return sp

def round_down_step(v: float, step: float) -> float:
    if step <= 0:
        return v
    return math.floor(float(v) / float(step)) * float(step)

# ─────────────────────────────────────────────────────────
# Ticker (v2→mark→orderbook→v1) + 캐시
# ─────────────────────────────────────────────────────────
_ticker_cache: Dict[str, Tuple[float, float]] = {}

def _cache_get(sym: str) -> Optional[float]:
    row = _ticker_cache.get(sym)
    if not row:
        return None
    ts, px = row
    return px if (time.time() - ts) <= TICKER_TTL else None

def _cache_set(sym: str, px: float):
    _ticker_cache[sym] = (time.time(), float(px))

def _parse_ticker_v2(js: Dict[str, Any]) -> Optional[float]:
    d = js.get("data") if isinstance(js, dict) else None
    if isinstance(d, dict):
        for k in ("last", "close", "price"):
            v = d.get(k)
            if v not in (None, "", "null"):
                try:
                    px = float(v)
                    if px > 0: return px
                except Exception: pass
        bid, ask = d.get("bestBid"), d.get("bestAsk")
        try:
            if bid not in (None, "") and ask not in (None, ""):
                b = float(bid); a = float(ask)
                if b > 0 and a > 0: return (a + b) / 2.0
        except Exception: pass
    return None

def _get_ticker_v2(sym: str, product: str) -> Optional[float]:
    try:
        return _parse_ticker_v2(_http_get(V2_TICKER_PATH, {"productType": product, "symbol": sym}))
    except Exception as e:
        _log(f"ticker v2 fail {sym}/{product}: {e}")
        return None

def _get_mark_v2(sym: str, product: str) -> Optional[float]:
    try:
        js = _http_get(V2_MARK_PATH, {"productType": product, "symbol": sym})
        d = js.get("data") or {}
        v = d.get("markPrice") or d.get("price")
        if v:
            px = float(v)
            return px if px > 0 else None
    except Exception as e:
        _log(f"mark v2 fail {sym}/{product}: {e}")
    return None

def _get_depth_mid_v2(sym: str, product: str) -> Optional[float]:
    try:
        js = _http_get(V2_DEPTH_PATH, {"productType": product, "symbol": sym, "priceLevel": 1})
        d = js.get("data") or {}
        bids, asks = d.get("bids") or [], d.get("asks") or []
        if bids and asks:
            b = float(bids[0][0]); a = float(asks[0][0])
            if b > 0 and a > 0: return (a + b) / 2.0
    except Exception as e:
        _log(f"depth v2 fail {sym}/{product}: {e}")
    return None

def _get_ticker_v1(sym: str) -> Optional[float]:
    for s in (sym, f"{sym}_UMCBL", f"{sym}-UMCBL"):
        try:
            js = _http_get(V1_TICKER_PATH, {"symbol": s})
            d = js.get("data") or {}
            for k in ("last", "close", "price"):
                v = d.get(k)
                if v:
                    px = float(v)
                    if px > 0: return px
        except Exception:
            pass
    return None

def get_last_price(sym: str) -> Optional[float]:
    symbol = convert_symbol(sym)
    cached = _cache_get(symbol)
    if cached: return cached

    if USE_V2:
        s = symbol
        products = _v2_product_types()
        for product in products:
            px = _get_ticker_v2(s, product)
            if px: _cache_set(symbol, px); return px
            px = _get_mark_v2(s, product)
            if px: _cache_set(symbol, px); return px
            if ALLOW_DEPTH_FALLBACK:
                px = _get_depth_mid_v2(s, product)
                if px: _cache_set(symbol, px); return px

        if not STRICT_TICKER:
            px = _get_ticker_v1(symbol)
            if px: _cache_set(symbol, px); return px

        _log(f"❌ Ticker 실패(최종): {symbol} v2=True")
        return None

    px = _get_ticker_v1(symbol)
    if px: _cache_set(symbol, px); return px
    _log(f"❌ Ticker 실패(최종): {symbol} v2=False")
    return None

# ─────────────────────────────────────────────────────────
# 주문/감축
# ─────────────────────────────────────────────────────────
def _api_side(side: str, reduce_only: bool) -> str:
    s = (side or "").lower()
    if s in ("buy", "long"):
        return "close_short" if reduce_only else "open_long"
    else:
        return "close_long" if reduce_only else "open_short"

def _order_size_from_usdt(symbol: str, usdt_amount: float) -> float:
    last = get_last_price(symbol)
    if not last or last <= 0: return 0.0
    step = float(get_symbol_spec(symbol).get("sizeStep", 0.001))
    size = float(usdt_amount) / float(last)
    return round_down_step(size, step)

def place_market_order(symbol: str, usdt_amount: float, side: str, leverage: float, reduce_only: bool=False) -> Dict[str, Any]:
    sym = convert_symbol(symbol)
    size = _order_size_from_usdt(sym, usdt_amount)
    if size <= 0:
        return {"code": "LOCAL_MIN_QTY", "msg": "size below step"}

    side_api = _api_side(side, reduce_only)
    # v2 우선 — productType 다중 시도
    if USE_V2:
        for product in _v2_product_types():
            payload_v2 = {
                "symbol": sym,
                "productType": product,
                "marginCoin": MARGIN_COIN,
                "size": str(size),
                "price": "",
                "side": side_api,
                "orderType": "market",
                "reduceOnly": reduce_only,
                "force": "gtc",
                "leverage": str(leverage),
            }
            try:
                js = _http_post(V2_PLACE_ORDER_PATH, payload_v2, auth=True)
                code = str(js.get("code", ""))
                if code == "00000":
                    return js
                _log(f"place v2 fail {sym}/{product}: {js}")
            except Exception as e:
                _log(f"place v2 error {sym}/{product}: {e}")

    # v1 폴백
    try:
        payload_v1 = {
            "symbol": f"{sym}_UMCBL",
            "marginCoin": MARGIN_COIN,
            "size": str(size),
            "orderType": "market",
            "side": side_api,
            "timeInForceValue": "normal",
            "reduceOnly": reduce_only,
        }
        js = _http_post(V1_PLACE_ORDER_PATH, payload_v1, auth=True)
        return js
    except Exception as e:
        _log(f"place v1 error {sym}: {e}")
        return {"code": "LOCAL_ORDER_FAIL", "msg": str(e)}

def place_reduce_by_size(symbol: str, size: float, side: str) -> Dict[str, Any]:
    sym = convert_symbol(symbol)
    size = round_down_step(float(size), float(get_symbol_spec(sym).get("sizeStep", 0.001)))
    if size <= 0:
        return {"code": "LOCAL_BAD_QTY", "msg": "size<=0"}
    # reduceOnly=True — 시장가 감축
    return place_market_order(
        sym,
        usdt_amount=float(size) * (get_last_price(sym) or 0.0),
        side=("buy" if (side or "long").lower() == "long" else "sell"),
        leverage=1,
        reduce_only=True,
    )

# ─────────────────────────────────────────────────────────
# 포지션 조회 (v2: productType 다중 시도 + marginCoin 옵션, v1 파라미터 보강)
# ─────────────────────────────────────────────────────────
def _parse_positions_v2(js: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = js.get("data") or []
    out: List[Dict[str, Any]] = []
    for row in data:
        try:
            sym = convert_symbol(row.get("symbol", ""))
            side = (row.get("holdSide") or "").lower()     # long / short
            size = float(row.get("total", 0) or row.get("available", 0) or 0)
            entry = float(row.get("averageOpenPrice", 0) or 0)
            if size > 0 and side in ("long", "short"):
                out.append({"symbol": sym, "side": side, "size": size, "entry_price": entry})
        except Exception:
            pass
    return out

def _parse_positions_v1(js: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = js.get("data") or []
    out: List[Dict[str, Any]] = []
    for row in data:
        try:
            sym = convert_symbol(row.get("symbol", ""))
            for pos in row.get("positions") or []:
                side = (pos.get("holdSide") or "").lower()
                size = float(pos.get("total", 0) or 0)
                entry = float(pos.get("averageOpenPrice", 0) or 0)
                if size > 0 and side in ("long", "short"):
                    out.append({"symbol": sym, "side": side, "size": size, "entry_price": entry})
        except Exception:
            pass
    return out

def get_open_positions() -> List[Dict[str, Any]]:
    # v2 — productType 다중 시도 (+ marginCoin 함께 시도)
    if USE_V2:
        for product in _v2_product_types():
            for params in (
                {"productType": product},
                {"productType": product, "marginCoin": MARGIN_COIN},
            ):
                try:
                    js = _http_get(V2_POSITIONS_PATH, params, auth=True)
                    out = _parse_positions_v2(js)
                    return out
                except requests.HTTPError as e:
                    _log(f"positions v2 error: {e} url: {BASE_URL}{V2_POSITIONS_PATH}?{urlencode(params)}")
                except Exception as e:
                    _log(f"positions v2 error: {e}")

    # v1 폴백 — 문서/계정 설정에 따라 productType, marginCoin이 필요한 경우가 있어 함께 시도
    for params in (
        {},  # 일부 계정은 파라미터 없이도 동작
        {"productType": "umcbl"},
        {"productType": "umcbl", "marginCoin": MARGIN_COIN},
    ):
        try:
            js = _http_get(V1_POSITIONS_PATH, params, auth=True)
            out = _parse_positions_v1(js)
            return out
        except requests.HTTPError as e:
            _log(f"positions v1 error: {e} url: {BASE_URL}{V1_POSITIONS_PATH}?{urlencode(params)}")
        except Exception as e:
            _log(f"positions v1 error: {e}")

    return []
