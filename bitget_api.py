# -*- coding: utf-8 -*-
"""
Bitget REST API helper (USDT-M Perpetual)

외부에서 사용하는 함수(트레이더와 호환):
  - convert_symbol(symbol) -> str
  - get_last_price(symbol) -> Optional[float]
  - get_open_positions() -> List[Dict]
  - place_market_order(symbol, usdt_amount, side, leverage, reduce_only=False) -> Dict
  - place_reduce_by_size(symbol, size, side) -> Dict
  - get_symbol_spec(symbol) -> Dict
  - round_down_step(value, step) -> float

필요 ENV (필수 ★ / 권장 ◇):
★ BITGET_API_KEY
★ BITGET_API_SECRET
★ BITGET_API_PASSWORD
◇ BITGET_BASE_URL (default: https://api.bitget.com)
◇ BITGET_USE_V2=1
◇ BITGET_V2_PRODUCT_TYPE=USDT-FUTURES
◇ BITGET_V2_PRODUCT_TYPE_ALTS="COIN-FUTURES,USDC-FUTURES"
◇ BITGET_MARGIN_COIN=USDT
◇ BITGET_V2_TICKER_PATH=/api/v2/mix/market/ticker
◇ BITGET_V2_MARK_PATH=/api/v2/mix/market/mark-price
◇ BITGET_V2_DEPTH_PATH=/api/v2/mix/market/orderbook
◇ BITGET_V2_PLACE_ORDER_PATH=/api/v2/mix/order/place-order
◇ BITGET_V2_POSITIONS_PATH=/api/v2/mix/position/all-position
◇ BITGET_V2_SINGLE_POSITION_PATH=/api/v2/mix/position/single-position
◇ BITGET_V1_TICKER_PATH=/api/mix/market/ticker
◇ BITGET_V1_PLACE_ORDER_PATH=/api/mix/v1/order/placeOrder
◇ BITGET_V1_POSITIONS_PATH=/api/mix/v1/position/allPosition
◇ POSITION_SYMBOLS_HINT="BTCUSDT,ETHUSDT,..."     # v2 single-position 폴백용
◇ STRICT_TICKER=0, ALLOW_DEPTH_FALLBACK=1, TICKER_TTL=3
◇ SYMBOL_ALIASES_JSON='{"KAITOUSDT":"KAITOUSDT"}'
◇ TRACE_LOG=0
"""

from __future__ import annotations
import os
import time
import math
import json
import hmac
import hashlib
import base64
import random
from typing import Any, Dict, Optional, Tuple, List
from urllib.parse import urlencode

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# ─────────────────────────────────────────────────────────
# ENV
# ─────────────────────────────────────────────────────────
BASE_URL  = os.getenv("BITGET_BASE_URL", "https://api.bitget.com")

API_KEY   = os.getenv("BITGET_API_KEY", "")
API_SEC   = os.getenv("BITGET_API_SECRET", "")
API_PASS  = os.getenv("BITGET_API_PASSWORD", "")

USE_V2               = os.getenv("BITGET_USE_V2", "1") == "1"
V2_TICKER_PATH       = os.getenv("BITGET_V2_TICKER_PATH", "/api/v2/mix/market/ticker")
V2_MARK_PATH         = os.getenv("BITGET_V2_MARK_PATH", "/api/v2/mix/market/mark-price")
V2_DEPTH_PATH        = os.getenv("BITGET_V2_DEPTH_PATH", "/api/v2/mix/market/orderbook")
V2_PLACE_ORDER_PATH  = os.getenv("BITGET_V2_PLACE_ORDER_PATH", "/api/v2/mix/order/place-order")
V2_POSITIONS_PATH    = os.getenv("BITGET_V2_POSITIONS_PATH", "/api/v2/mix/position/all-position")
V2_SINGLE_POSITION_PATH = os.getenv("BITGET_V2_SINGLE_POSITION_PATH", "/api/v2/mix/position/single-position")

V1_TICKER_PATH       = os.getenv("BITGET_V1_TICKER_PATH", "/api/mix/market/ticker")
V1_PLACE_ORDER_PATH  = os.getenv("BITGET_V1_PLACE_ORDER_PATH", "/api/mix/v1/order/placeOrder")
V1_POSITIONS_PATH    = os.getenv("BITGET_V1_POSITIONS_PATH", "/api/mix/v1/position/allPosition")

# 공식값: USDT-FUTURES / COIN-FUTURES / USDC-FUTURES
V2_PRODUCT_TYPE      = os.getenv("BITGET_V2_PRODUCT_TYPE", "USDT-FUTURES")
V2_PRODUCT_TYPE_ALTS = os.getenv("BITGET_V2_PRODUCT_TYPE_ALTS", "COIN-FUTURES,USDC-FUTURES")

MARGIN_COIN          = os.getenv("BITGET_MARGIN_COIN", "USDT")
POSITION_SYMBOLS_HINT = os.getenv("POSITION_SYMBOLS_HINT", "")

STRICT_TICKER        = os.getenv("STRICT_TICKER", "0") == "1"
ALLOW_DEPTH_FALLBACK = os.getenv("ALLOW_DEPTH_FALLBACK", "1") == "1"
TICKER_TTL           = float(os.getenv("TICKER_TTL", "3"))

try:
    SYMBOL_ALIASES = json.loads(os.getenv("SYMBOL_ALIASES_JSON", "") or "{}")
except Exception:
    SYMBOL_ALIASES = {}

TRACE = os.getenv("TRACE_LOG", "0") == "1"

# ─────────────────────────────────────────────────────────
# HTTP 세션 + 재시도
# ─────────────────────────────────────────────────────────
SESSION = requests.Session()
# 네트워크/5xx/429 재시도
_retry = Retry(
    total=5, read=5, connect=5,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods={"GET", "POST"},
    raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=50, pool_maxsize=100)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)
SESSION.headers.update({"User-Agent": "auto-trader/1.0", "Connection": "keep-alive"})

DEFAULT_TIMEOUT = 12  # 초

def _log(msg: str):
    if TRACE:
        print(msg, flush=True)

def _ts_ms() -> str:
    return str(int(time.time() * 1000))

def _sign(message: str) -> str:
    mac = hmac.new(API_SEC.encode("utf-8"), msg=message.encode("utf-8"), digestmod=hashlib.sha256)
    return base64.b64encode(mac.digest()).decode()

def _auth_headers(method: str, path: str, query: Dict[str, Any] | None, body: str = "") -> Dict[str, str]:
    # Bitget: ts + METHOD + path + '?' + queryString + body
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

def _with_retry(fn, *args, **kwargs):
    attempts = 3
    for i in range(1, attempts + 1):
        try:
            return fn(*args, **kwargs)
        except (requests.exceptions.ConnectionError,
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.SSLError) as e:
            if i == attempts:
                raise
            sleep = (0.4 * (2 ** (i - 1))) + random.uniform(0, 0.3)
            _log(f"network retry {i}/{attempts} after error: {e}")
            time.sleep(sleep)

def _http_get(path: str, params: Dict[str, Any], auth: bool = False, timeout: int = DEFAULT_TIMEOUT) -> Dict[str, Any]:
    url = BASE_URL + path
    if auth:
        headers = _auth_headers("GET", path, params, "")
        r = SESSION.get(url, params=params, headers=headers, timeout=timeout)
    else:
        r = SESSION.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def _http_get_raw(path: str, params: Dict[str, Any], auth: bool = False, timeout: int = DEFAULT_TIMEOUT) -> requests.Response:
    url = BASE_URL + path
    if auth:
        headers = _auth_headers("GET", path, params, "")
        res = SESSION.get(url, params=params, headers=headers, timeout=timeout)
    else:
        res = SESSION.get(url, params=params, timeout=timeout)
    return res

def _http_post(path: str, payload: Dict[str, Any], auth: bool = True, timeout: int = DEFAULT_TIMEOUT) -> Dict[str, Any]:
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
    out: List[str] = []
    seen = set()
    for x in [V2_PRODUCT_TYPE] + [v.strip() for v in V2_PRODUCT_TYPE_ALTS.split(",") if v.strip()]:
        x = x.strip()
        if x and x not in seen:
            seen.add(x); out.append(x)
    return out or ["USDT-FUTURES"]

_spec_cache: Dict[str, Dict[str, Any]] = {}
def get_symbol_spec(symbol: str) -> Dict[str, Any]:
    sym = convert_symbol(symbol)
    sp = _spec_cache.get(sym)
    if sp: return sp
    sp = {"sizeStep": 0.001, "priceStep": 0.01}  # 필요시 거래소 메타를 붙여서 보강 가능
    _spec_cache[sym] = sp
    return sp

def round_down_step(v: float, step: float) -> float:
    if step <= 0:
        return v
    return math.floor(float(v) / float(step)) * float(step)

# ─────────────────────────────────────────────────────────
# Ticker (v2 -> mark -> orderbook(mid) -> v1) + 캐시
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
                except Exception:
                    pass
        bid, ask = d.get("bestBid"), d.get("bestAsk")
        try:
            if bid not in (None, "") and ask not in (None, ""):
                b = float(bid); a = float(ask)
                if b > 0 and a > 0: return (a + b) / 2.0
        except Exception:
            pass
    return None

def _get_ticker_v2(sym: str, product: str) -> Optional[float]:
    try:
        return _with_retry(_http_get, V2_TICKER_PATH, {"productType": product, "symbol": sym}, False)
    except Exception as e:
        _log(f"ticker v2 fail {sym}/{product}: {e}")
        return None

def _get_mark_v2(sym: str, product: str) -> Optional[float]:
    try:
        js = _with_retry(_http_get, V2_MARK_PATH, {"productType": product, "symbol": sym}, False)
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
        js = _with_retry(_http_get, V2_DEPTH_PATH, {"productType": product, "symbol": sym, "priceLevel": 1}, False)
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
            js = _with_retry(_http_get, V1_TICKER_PATH, {"symbol": s}, False)
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
        for product in _v2_product_types():
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
                js = _with_retry(_http_post, V2_PLACE_ORDER_PATH, payload_v2, True)
                if str(js.get("code", "")) == "00000":
                    return js
                _log(f"place v2 fail {sym}/{product}: {js}")
            except Exception as e:
                _log(f"place v2 error {sym}/{product}: {e}")

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
        js = _with_retry(_http_post, V1_PLACE_ORDER_PATH, payload_v1, True)
        return js
    except Exception as e:
        _log(f"place v1 error {sym}: {e}")
        return {"code": "LOCAL_ORDER_FAIL", "msg": str(e)}

def place_reduce_by_size(symbol: str, size: float, side: str) -> Dict[str, Any]:
    sym = convert_symbol(symbol)
    size = round_down_step(float(size), float(get_symbol_spec(sym).get("sizeStep", 0.001)))
    if size <= 0:
        return {"code": "LOCAL_BAD_QTY", "msg": "size<=0"}
    return place_market_order(
        sym,
        usdt_amount=float(size) * (get_last_price(sym) or 0.0),
        side=("buy" if (side or "long").lower() == "long" else "sell"),
        leverage=1,
        reduce_only=True,
    )

# ─────────────────────────────────────────────────────────
# 포지션 조회
# ─────────────────────────────────────────────────────────
def _parse_positions_v2(js: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = js.get("data") or []
    out: List[Dict[str, Any]] = []
    for row in data:
        try:
            if not row:
                continue
            sym = convert_symbol(row.get("symbol", ""))
            side = (row.get("holdSide") or "").lower()  # long/short
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
    if USE_V2:
        for product in _v2_product_types():
            for params in (
                {"productType": product},
                {"productType": product, "marginCoin": MARGIN_COIN},
            ):
                try:
                    res = _with_retry(_http_get_raw, V2_POSITIONS_PATH, params, True)
                    if res.status_code == 200:
                        js = res.json()
                        return _parse_positions_v2(js)
                    else:
                        _log(f"positions v2 {res.status_code} url: {BASE_URL}{V2_POSITIONS_PATH}?{urlencode(params)} body: {res.text}")
                except Exception as e:
                    _log(f"positions v2 error: {e} url: {BASE_URL}{V2_POSITIONS_PATH}?{urlencode(params)}")

        # (옵션) 힌트 심볼로 single-position 폴백
        hint_syms = [s.strip().upper() for s in POSITION_SYMBOLS_HINT.split(",") if s.strip()]
        if hint_syms:
            collected: List[Dict[str, Any]] = []
            for product in _v2_product_types():
                for sym in hint_syms:
                    try:
                        res = _with_retry(_http_get_raw, V2_SINGLE_POSITION_PATH,
                                          {"productType": product, "symbol": convert_symbol(sym)}, True)
                        if res.status_code == 200:
                            js = res.json()
                            one = _parse_positions_v2({"data": [js.get("data", {})]})
                            if one: collected.extend(one)
                        else:
                            _log(f"single-position v2 {res.status_code} url: {BASE_URL}{V2_SINGLE_POSITION_PATH}?productType={product}&symbol={convert_symbol(sym)} body: {res.text}")
                    except Exception as e:
                        _log(f"single-position v2 error: {e}")
            if collected:
                return collected

    # v1 폴백(본문 로깅까지)
    for params in (
        {},
        {"productType": "umcbl"},
        {"productType": "umcbl", "marginCoin": MARGIN_COIN},
    ):
        try:
            res = _with_retry(_http_get_raw, V1_POSITIONS_PATH, params, True)
            if res.status_code == 200:
                js = res.json()
                return _parse_positions_v1(js)
            else:
                _log(f"positions v1 {res.status_code} url: {BASE_URL}{V1_POSITIONS_PATH}?{urlencode(params)} body: {res.text}")
        except Exception as e:
            _log(f"positions v1 error: {e} url: {BASE_URL}{V1_POSITIONS_PATH}?{urlencode(params)}")

    return []
