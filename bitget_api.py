# -*- coding: utf-8 -*-
"""
Bitget REST API helper for UMCBL (USDT-M Perpetual)

- Trader/main 이 기대하는 인터페이스 유지:
  convert_symbol, get_last_price, get_open_positions,
  place_market_order, place_reduce_by_size, get_symbol_spec, round_down_step

- 티커 안정화:
  v2 ticker (plain symbol + productType=umcbl)
    -> v2 mark price
    -> v2 orderbook (mid)
    -> (옵션) v1 ticker
  + TTL 캐시

- 주문/감축/포지션:
  v2 우선, v1 폴백

ENV는 render.yaml 템플릿과 일치(티커 v2 경로/옵션 포함).  # see: render.yaml
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

# ─────────────────────────────────────────────────────────
# 공통 설정/ENV
# ─────────────────────────────────────────────────────────

BASE_URL  = os.getenv("BITGET_BASE_URL", "https://api.bitget.com")

API_KEY   = os.getenv("BITGET_API_KEY", "")
API_SEC   = os.getenv("BITGET_API_SECRET", "")
API_PASS  = os.getenv("BITGET_API_PASSWORD", "")

# v2 사용 및 경로
USE_V2               = os.getenv("BITGET_USE_V2", "1") == "1"
V2_TICKER_PATH       = os.getenv("BITGET_V2_TICKER_PATH", "/api/v2/mix/market/ticker")
V2_MARK_PATH         = os.getenv("BITGET_V2_MARK_PATH", "/api/v2/mix/market/mark-price")
V2_DEPTH_PATH        = os.getenv("BITGET_V2_DEPTH_PATH", "/api/v2/mix/market/orderbook")

# v2 주문/포지션 (공식 문서 기준)
V2_PLACE_ORDER_PATH  = os.getenv("BITGET_V2_PLACE_ORDER_PATH",  "/api/v2/mix/order/place-order")
V2_POSITIONS_PATH    = os.getenv("BITGET_V2_POSITIONS_PATH",    "/api/v2/mix/position/all-position")

# v1 폴백 경로
V1_TICKER_PATH       = os.getenv("BITGET_V1_TICKER_PATH",       "/api/mix/market/ticker")
V1_PLACE_ORDER_PATH  = os.getenv("BITGET_V1_PLACE_ORDER_PATH",  "/api/mix/v1/order/placeOrder")
V1_POSITIONS_PATH    = os.getenv("BITGET_V1_POSITIONS_PATH",    "/api/mix/v1/position/allPosition")

# 마켓 품질 옵션
STRICT_TICKER        = os.getenv("STRICT_TICKER", "0") == "1"   # True면 v1폴백/오더북 폴백 금지
ALLOW_DEPTH_FALLBACK = os.getenv("ALLOW_DEPTH_FALLBACK", "1") == "1"
TICKER_TTL           = float(os.getenv("TICKER_TTL", "3"))

# 심볼 별칭
try:
    SYMBOL_ALIASES = json.loads(os.getenv("SYMBOL_ALIASES_JSON", "") or "{}")
except Exception:
    SYMBOL_ALIASES = {}

TRACE = os.getenv("TRACE_LOG", "0") == "1"

MARGIN_COIN = os.getenv("BITGET_MARGIN_COIN", "USDT")  # mix 선물 마진코인

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "auto-trader/1.0"})

# ─────────────────────────────────────────────────────────
# 유틸/로그
# ─────────────────────────────────────────────────────────

def _log(msg: str):
    if TRACE:
        print(msg, flush=True)

def _ts_ms() -> str:
    return str(int(time.time() * 1000))

def _sign(message: str) -> str:
    mac = hmac.new(API_SEC.encode("utf-8"), msg=message.encode("utf-8"), digestmod=hashlib.sha256)
    return base64.b64encode(mac.digest()).decode()

def _auth_headers(method: str, path: str, query: str = "", body: str = "") -> Dict[str, str]:
    """
    Bitget 서명 규칙:
    sign = base64(HmacSHA256(secret, timestamp + method + requestPath + queryString + body))
    """
    ts = _ts_ms()
    pre = ts + method.upper() + path + (f"?{query}" if query else "") + body
    sign = _sign(pre)
    return {
        "ACCESS-KEY": API_KEY,
        "ACCESS-SIGN": sign,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": API_PASS,
        "Content-Type": "application/json",
    }

def _http_get(path: str, params: Dict[str, Any], auth: bool=False, timeout: int=8) -> Dict[str, Any]:
    url = BASE_URL + path
    if auth:
        # GET 서명 시 querystring 포함
        from urllib.parse import urlencode
        q = urlencode(params or {})
        headers = _auth_headers("GET", path, q, "")
        r = SESSION.get(url, params=params, headers=headers, timeout=timeout)
    else:
        r = SESSION.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def _http_post(path: str, payload: Dict[str, Any], auth: bool=True, timeout: int=8) -> Dict[str, Any]:
    url = BASE_URL + path
    body = json.dumps(payload or {})
    headers = _auth_headers("POST", path, "", body) if auth else {"Content-Type": "application/json"}
    r = SESSION.post(url, data=body, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()

# ─────────────────────────────────────────────────────────
# 심볼 정규화
# ─────────────────────────────────────────────────────────

def convert_symbol(sym: str) -> str:
    """
    외부 입력(시그널/환경변수 등)을 Bitget 호환 심볼로 정규화.
    - v2 ticker는 접미사를 쓰지 않고 `productType=umcbl`로 구분
    - v1은 내부에서 접미사 변형을 시도
    """
    s = (sym or "").upper().strip()
    s = SYMBOL_ALIASES.get(s, s)
    for suf in ("_UMCBL", "-UMCBL", "UMCBL", "_CMCBL", "-CMCBL", "CMCBL"):
        if s.endswith(suf):
            s = s.replace(suf, "")
    return s.replace("-", "").replace("_", "")

def _clean_symbol_for_v2(sym: str) -> Tuple[str, str]:
    """(symbol, productType) 반환. U-Perp = umcbl"""
    return convert_symbol(sym), "umcbl"

# ─────────────────────────────────────────────────────────
# 스펙/반올림
# ─────────────────────────────────────────────────────────

_spec_cache: Dict[str, Dict[str, Any]] = {}

def get_symbol_spec(symbol: str) -> Dict[str, Any]:
    """
    계약 단위/라운딩 스텝을 제공. (없으면 안전한 기본값)
    필요시 v2 컨트랙트 메타 API로 확장 가능.
    """
    sym = convert_symbol(symbol)
    spec = _spec_cache.get(sym)
    if spec:
        return spec
    # 기본값 (Bitget U-Perp 다수 종목과 호환)
    spec = {"sizeStep": 0.001, "priceStep": 0.01}
    _spec_cache[sym] = spec
    return spec

def round_down_step(v: float, step: float) -> float:
    if step <= 0:
        return v
    return math.floor(float(v) / float(step)) * float(step)

# ─────────────────────────────────────────────────────────
# Ticker / Mark / Orderbook 폴백 + 캐시
# ─────────────────────────────────────────────────────────

_ticker_cache: Dict[str, Tuple[float, float]] = {}  # symbol -> (ts, price)

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
                    if px > 0:
                        return px
                except Exception:
                    pass
        # bid/ask 중간값
        bid, ask = d.get("bestBid"), d.get("bestAsk")
        try:
            if bid not in (None, "") and ask not in (None, ""):
                b = float(bid); a = float(ask)
                if b > 0 and a > 0:
                    return (a + b) / 2.0
        except Exception:
            pass
    return None

def _get_ticker_v2(sym: str, product: str) -> Optional[float]:
    try:
        js = _http_get(V2_TICKER_PATH, {"productType": product, "symbol": sym}, auth=False)
        return _parse_ticker_v2(js)
    except Exception as e:
        _log(f"ticker v2 fail {sym}: {e}")
        return None

def _get_mark_v2(sym: str, product: str) -> Optional[float]:
    try:
        js = _http_get(V2_MARK_PATH, {"productType": product, "symbol": sym}, auth=False)
        d = js.get("data") or {}
        v = d.get("markPrice") or d.get("price")
        if v:
            px = float(v)
            return px if px > 0 else None
    except Exception as e:
        _log(f"mark v2 fail {sym}: {e}")
    return None

def _get_depth_mid_v2(sym: str, product: str) -> Optional[float]:
    try:
        js = _http_get(V2_DEPTH_PATH, {"productType": product, "symbol": sym, "priceLevel": 1}, auth=False)
        d = js.get("data") or {}
        bids, asks = d.get("bids") or [], d.get("asks") or []
        if bids and asks:
            b = float(bids[0][0]); a = float(asks[0][0])
            if b > 0 and a > 0:
                return (a + b) / 2.0
    except Exception as e:
        _log(f"depth v2 fail {sym}: {e}")
    return None

def _get_ticker_v1(sym: str) -> Optional[float]:
    # v1은 심볼 변형이 필요한 경우가 있어 여러 형태를 시도.
    for s in (sym, f"{sym}_UMCBL", f"{sym}-UMCBL"):
        try:
            js = _http_get(V1_TICKER_PATH, {"symbol": s}, auth=False)
            d = js.get("data") or {}
            for k in ("last", "close", "price"):
                v = d.get(k)
                if v:
                    px = float(v)
                    if px > 0:
                        return px
        except Exception:
            pass
    return None

def get_last_price(sym: str) -> Optional[float]:
    """
    최종 last price:
    cache → v2 ticker → v2 mark → v2 orderbook(mid) → (옵션) v1 → 실패
    """
    symbol = convert_symbol(sym)
    cached = _cache_get(symbol)
    if cached:
        return cached

    if USE_V2:
        s, product = _clean_symbol_for_v2(symbol)

        px = _get_ticker_v2(s, product)
        if px:
            _cache_set(symbol, px); return px

        px = _get_mark_v2(s, product)
        if px:
            _cache_set(symbol, px); return px

        if ALLOW_DEPTH_FALLBACK:
            px = _get_depth_mid_v2(s, product)
            if px:
                _cache_set(symbol, px); return px

        if not STRICT_TICKER:
            px = _get_ticker_v1(symbol)
            if px:
                _cache_set(symbol, px); return px

        _log(f"❌ Ticker 실패(최종): {symbol} v2=True")
        return None

    # v1 우선 모드
    px = _get_ticker_v1(symbol)
    if px:
        _cache_set(symbol, px); return px
    _log(f"❌ Ticker 실패(최종): {symbol} v2=False")
    return None

# ─────────────────────────────────────────────────────────
# 주문/감축/포지션
# ─────────────────────────────────────────────────────────

def _api_side(side: str, reduce_only: bool) -> str:
    """
    Bitget mix 주문 side 값 매핑:
      - open_long / open_short / close_long / close_short
    """
    s = (side or "").lower()
    if s in ("buy", "long"):
        return "close_short" if reduce_only else "open_long"
    else:
        return "close_long" if reduce_only else "open_short"

def _order_size_from_usdt(symbol: str, usdt_amount: float) -> float:
    last = get_last_price(symbol)
    if not last or last <= 0:
        return 0.0
    step = float(get_symbol_spec(symbol).get("sizeStep", 0.001))
    size = float(usdt_amount) / float(last)
    return round_down_step(size, step)

def place_market_order(symbol: str, usdt_amount: float, side: str, leverage: float, reduce_only: bool=False) -> Dict[str, Any]:
    """
    시장가 주문(개시/감축 모두 지원): v2 → v1 폴백
    - trader는 code=="00000"을 성공으로 해석
    """
    sym = convert_symbol(symbol)
    size = _order_size_from_usdt(sym, usdt_amount)
    if size <= 0:
        return {"code": "LOCAL_MIN_QTY", "msg": "size below step"}

    side_api = _api_side(side, reduce_only)
    payload_v2 = {
        "symbol": sym,
        "productType": "umcbl",
        "marginCoin": MARGIN_COIN,
        "size": str(size),
        "price": "",                # market
        "side": side_api,
        "orderType": "market",
        "reduceOnly": reduce_only,
        "force": "gtc",
        "leverage": str(leverage),
    }

    try:
        if USE_V2:
            js = _http_post(V2_PLACE_ORDER_PATH, payload_v2, auth=True)
            code = str(js.get("code", ""))
            if code == "00000":
                return js
            _log(f"place v2 fail {sym}: {js}")
    except Exception as e:
        _log(f"place v2 error {sym}: {e}")

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
    """
    포지션 감축(시장가). side는 현재 포지션 방향(long/short)과 동일하게 넘김.
    """
    sym = convert_symbol(symbol)
    size = round_down_step(float(size), float(get_symbol_spec(sym).get("sizeStep", 0.001)))
    if size <= 0:
        return {"code": "LOCAL_BAD_QTY", "msg": "size<=0"}

    # reduceOnly=True로 호출
    return place_market_order(sym, usdt_amount=float(size) * (get_last_price(sym) or 0.0),
                              side=("buy" if (side or "long").lower() == "long" else "sell"),
                              leverage=1, reduce_only=True)

def _parse_positions_v2(js: Dict[str, Any]) -> List[Dict[str, Any]]:
    data = js.get("data") or []
    out: List[Dict[str, Any]] = []
    for row in data:
        try:
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
            # v1은 "positions": [{"holdSide":"long","total":"0.001",...}, ...] 형태일 수 있음
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
    """
    열린 포지션 목록(양 사이드 분리). trader는
      [{"symbol":"BTCUSDT","side":"long","size":0.01,"entry_price":27000.0}, ...]
    같은 구조를 기대.
    """
    # v2
    if USE_V2:
        try:
            js = _http_get(V2_POSITIONS_PATH, {"productType": "umcbl"}, auth=True)
            out = _parse_positions_v2(js)
            return out
        except Exception as e:
            _log(f"positions v2 error: {e}")

    # v1 폴백
    try:
        js = _http_get(V1_POSITIONS_PATH, {}, auth=True)
        out = _parse_positions_v1(js)
        return out
    except Exception as e:
        _log(f"positions v1 error: {e}")
        return []
