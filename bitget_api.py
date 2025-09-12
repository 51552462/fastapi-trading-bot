# -*- coding: utf-8 -*-
"""
Bitget USDT-FUTURES (v2) 전용 경량 래퍼
- positions:  single-position / all-position
- ticker:     /mix/market/ticker (lastPr)
- contracts:  /mix/market/contracts (사이즈 정책)
- order:      /mix/order/placeOrder (reduceOnly = "YES"/"NO")
"""

import os
import time
import hmac
import json
import math
import hashlib
from typing import Dict, Any, Optional, List, Tuple
import requests

BITGET_HOST = os.getenv("BITGET_HOST", "https://api.bitget.com")
API_KEY = os.getenv("BITGET_API_KEY", "")
API_SECRET = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE = os.getenv("BITGET_API_PASSPHRASE", "")

PRODUCT_TYPE = os.getenv("PRODUCT_TYPE", "USDT-FUTURES")
MARGIN_COIN = os.getenv("MARGIN_COIN", "USDT")

# ---- utilities --------------------------------------------------------------

class BgError(Exception):
    pass

def _now_ms() -> str:
    return str(int(time.time() * 1000))

def _sign_v2(ts: str, method: str, path: str, query: str = "", body: str = "") -> str:
    """
    v2 사인 규격
    prehash = timestamp + method + path + (queryString) + body
    - queryString은 '?' 포함해서 붙인다 (없으면 빈 문자열)
    """
    prehash = ts + method.upper() + path + (f"?{query}" if query else "") + body
    mac = hmac.new(API_SECRET.encode(), prehash.encode(), hashlib.sha256).digest()
    return base64_b64encode(mac)

def base64_b64encode(b: bytes) -> str:
    import base64
    return base64.b64encode(b).decode()

def _headers(ts: str, sign: str) -> Dict[str, str]:
    # ACCESS-TYPE=2 (현물=1, 선물/마진=2)
    return {
        "ACCESS-KEY": API_KEY,
        "ACCESS-SIGN": sign,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": API_PASSPHRASE,
        "ACCESS-TYPE": "2",
        "Content-Type": "application/json",
    }

def _req(
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    timeout: int = 15,
) -> Dict[str, Any]:
    """
    공통 요청기.
    - v2 서명
    - 성공코드 '00000' 확인
    """
    if not (API_KEY and API_SECRET and API_PASSPHRASE):
        raise BgError("Bitget API env missing (BITGET_API_KEY/SECRET/PASSPHRASE)")

    params = params or {}
    body = body or {}

    query = "&".join([f"{k}={params[k]}" for k in sorted(params)]) if params else ""
    body_str = json.dumps(body, separators=(",", ":"), ensure_ascii=False) if body else ""

    ts = _now_ms()
    url = BITGET_HOST + path + (f"?{query}" if query else "")
    sign = _sign_v2(ts, method, path, query, body_str)
    headers = _headers(ts, sign)

    fn = requests.get if method.upper() == "GET" else requests.post
    resp = fn(url, headers=headers, data=body_str if body else None, timeout=timeout)
    try:
        js = resp.json()
    except Exception:
        raise BgError(f"HTTP {resp.status_code} non-JSON: {resp.text[:200]}")

    if js.get("code") != "00000":
        raise BgError(f"{js.get('code')}: {js.get('msg')} (data={js.get('data')})")
    return js

def _symbol(s: str) -> str:
    """
    변형된 입력 심볼을 표준 선물 심볼로 보정
    예) DOGEUSDT, DOGEUSDT_UMCBL, dogeusdt → DOGEUSDT
    """
    s = (s or "").upper()
    s = s.replace("_UMCBL", "").replace("-UMCBL", "")
    s = s.replace("_USDT", "USDT").replace("-USDT", "USDT")
    return s

# ---- market / contracts -----------------------------------------------------

# contracts 캐시 (사이즈 규칙)
_CONTRACTS_CACHE: Tuple[float, List[Dict[str, Any]]] = (0, [])

def _get_contracts() -> List[Dict[str, Any]]:
    """
    /api/v2/mix/market/contracts?productType=USDT-FUTURES
    """
    global _CONTRACTS_CACHE
    ts, data = _CONTRACTS_CACHE
    if time.time() - ts < 60 and data:
        return data

    js = _req(
        "GET",
        "/api/v2/mix/market/contracts",
        params={"productType": PRODUCT_TYPE},
    )
    arr = js.get("data") or []
    _CONTRACTS_CACHE = (time.time(), arr)
    return arr

def _contract_info(symbol: str) -> Dict[str, Any]:
    symbol = _symbol(symbol)
    for it in _get_contracts():
        if (it.get("symbol") or "").upper() == symbol:
            return it
    raise BgError(f"contracts not found for {symbol} (productType={PRODUCT_TYPE})")

def _round_size(symbol: str, size: float) -> float:
    """
    Bitget 가이드: floor to sizeStep (또는 sizeMulti) & >= minTradeNum
    """
    info = _contract_info(symbol)
    # 가능한 키들 (계정마다 스펙 필드명이 다를 수 있어 다 받아줌)
    step = float(info.get("sizeStep") or info.get("sizePrecision") or info.get("sizeTick") or 0.0) or 0.001
    mult = float(info.get("sizeMult") or info.get("sizeMultiplier") or 1.0)
    min_qty = float(info.get("minTradeNum") or info.get("minOrderSize") or step)

    # sizeMult가 있으면 해당 배수로 맞추기
    if mult and mult > 1.0:
        size = math.floor(size / mult) * mult

    # step 배수로 내림
    size = math.floor(size / step) * step

    if size < min_qty:
        # 최소 주문수량을 만족하도록 보정 (내림 후 0되면 최소로 올림)
        size = math.floor(min_qty / step) * step
        if size < min_qty:
            size = min_qty
    return float(f"{size:.10f}".rstrip("0").rstrip("."))

# ---- market/ticker ----------------------------------------------------------

def get_last_price(symbol: str) -> float:
    """
    /api/v2/mix/market/ticker?productType=USDT-FUTURES&symbol=CETUSUSDT
    return: lastPr (float)
    """
    symbol = _symbol(symbol)
    js = _req(
        "GET",
        "/api/v2/mix/market/ticker",
        params={"productType": PRODUCT_TYPE, "symbol": symbol},
    )
    data = js.get("data") or {}
    last = data.get("lastPr")
    if last in (None, "", "null"):
        # 지원팀 코멘트: lastPr는 보통 null이 아님 → 방어적으로 예외 처리
        raise BgError(f"ticker.lastPr null for {symbol}")
    return float(last)

# ---- positions --------------------------------------------------------------

def get_positions_all() -> List[Dict[str, Any]]:
    """
    /api/v2/mix/position/all-position?marginCoin=USDT&productType=USDT-FUTURES
    """
    js = _req(
        "GET",
        "/api/v2/mix/position/all-position",
        params={"marginCoin": MARGIN_COIN, "productType": PRODUCT_TYPE},
    )
    return js.get("data") or []

def get_position_single(symbol: str) -> Dict[str, Any]:
    """
    /api/v2/mix/position/single-position?marginCoin=USDT&productType=USDT-FUTURES&symbol=DOGEUSDT
    """
    symbol = _symbol(symbol)
    js = _req(
        "GET",
        "/api/v2/mix/position/single-position",
        params={"marginCoin": MARGIN_COIN, "productType": PRODUCT_TYPE, "symbol": symbol},
    )
    # 없으면 빈 dict 반환
    return (js.get("data") or {}) if isinstance(js.get("data"), dict) else {}

# ---- orders -----------------------------------------------------------------

def place_market_order(
    symbol: str,
    side: str,             # 'buy' or 'sell'
    size: float,
    reduce_only: bool = False,
) -> Dict[str, Any]:
    """
    /api/v2/mix/order/placeOrder
    필수 파라미터:
      - symbol, productType, marginCoin
      - side: 'buy'|'sell'
      - orderType: 'market'
      - size: string (sizeStep/sizeMult에 맞춘 내림)
      - reduceOnly: 'YES'|'NO'
    """
    symbol = _symbol(symbol)
    size_adj = _round_size(symbol, float(size))
    body = {
        "symbol": symbol,
        "productType": PRODUCT_TYPE,
        "marginCoin": MARGIN_COIN,
        "side": side,
        "orderType": "market",
        "size": str(size_adj),
        "reduceOnly": "YES" if reduce_only else "NO",
    }
    js = _req("POST", "/api/v2/mix/order/placeOrder", body=body)
    return js.get("data") or {}

# ---- helpers for bot --------------------------------------------------------

def open_position_by_usdt(symbol: str, side: str, usdt_amount: float, leverage: float = 1.0) -> Dict[str, Any]:
    """
    '금액(USDT)' 기반 시장가 진입 → 계약 수량으로 변환
    (대략) size = (usdt * leverage) / last_price
    이후 sizeStep/Mult 규칙에 맞춰 내림
    """
    px = get_last_price(symbol)
    raw_size = (float(usdt_amount) * float(leverage)) / px
    size_adj = _round_size(symbol, raw_size)
    return place_market_order(symbol, side=side, size=size_adj, reduce_only=False)

def close_position_all(symbol: str, side_was_long: bool) -> Dict[str, Any]:
    """
    포지션 즉시 청산(전량):
      - 롱 보유 → sell with reduceOnly YES
      - 숏 보유 → buy  with reduceOnly YES
    남은 수량은 single-position에서 available 로 추정.
    """
    sym = _symbol(symbol)
    pos = get_position_single(sym)
    size = float(pos.get("available", 0) or pos.get("total", 0) or 0)
    if size <= 0:
        return {"skipped": True, "reason": "no_position"}

    close_side = "sell" if side_was_long else "buy"
    size_adj = _round_size(sym, size)
    return place_market_order(sym, side=close_side, size=size_adj, reduce_only=True)
