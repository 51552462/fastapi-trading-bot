# -*- coding: utf-8 -*-
"""
bitget_api.py — Bitget REST 어댑터 (USDT Perp / ONEWAY)

업데이트 요약
- 심볼 자동 해석(동적 조회 + 캐시): /api/v2/mix/market/contracts(productType=USDT-FUTURES)
- 주문/레버리지/청산에 productType, marginMode 항상 포함 (v2 엄격검증 대응)
- 가격 조회(get_last_price) 강화: v2 ticker(+productType) → v2 tickers 목록 → v2 candles → v1 ticker → v1 depth 순서 폴백
- 스텝 내림 반올림/명목가·증거금 해석/oneway reduceOnly 등 기존 기능 유지
"""

import os
import time
import hmac
import hashlib
import base64
import json
from typing import Any, Dict, List, Optional, Tuple

import requests

# =========================
# 환경변수
# =========================
BITGET_HOST = os.getenv("BITGET_HOST", "https://api.bitget.com")

API_KEY    = os.getenv("BITGET_API_KEY", "")
API_SECRET = os.getenv("BITGET_API_SECRET", "")
API_PASS   = os.getenv("BITGET_API_PASS", "")

# 수량/가격 기본 스텝 (심볼 스펙 조회 실패 시 사용)
DEFAULT_SIZE_STEP  = float(os.getenv("DEFAULT_SIZE_STEP", "0.001"))
DEFAULT_PRICE_STEP = float(os.getenv("DEFAULT_PRICE_STEP", "0.0001"))

# amount 해석 모드: notional(명목가) / margin(증거금)
AMOUNT_MODE = os.getenv("AMOUNT_MODE", "notional").lower().strip()

# 포지션 모드 기본: oneway (hedge 미지원)
POSITION_MODE = os.getenv("BITGET_POSITION_MODE", "oneway").lower().strip()

# 마진 모드: cross / isolated (Bitget v2 표기: crossed / isolated)
MARGIN_MODE_ENV = os.getenv("BITGET_MARGIN_MODE", "cross").lower().strip()

# HTTP 타임아웃(초)
HTTP_TIMEOUT = 8

# v2에서 주문/레버리지/포지션 조회 시 요구됨
PRODUCT_TYPE = "USDT-FUTURES"

# 디버그 로그 (서버 콘솔에만)
BITGET_DEBUG = os.getenv("BITGET_DEBUG", "0") == "1"

# 계약심볼 캐시(실심볼 ↔ 코어심볼)
_SYMBOL_CACHE: Dict[str, Tuple[str, float]] = {}  # core -> (exchange_symbol, ts)
_SYMBOL_CACHE_TTL = float(os.getenv("SYMBOL_CACHE_TTL", "300"))  # 5분

# =========================
# 내부 유틸
# =========================
def _dbg(*a):
    if BITGET_DEBUG:
        print("[bitget]", *a)

def convert_symbol(s: str) -> str:
    """
    다양한 표기(BINANCE:BTCUSDT, BTC-USDT, BTCUSDT_UMCBL, BTCUSDT.PERP)를
    내부 표준 'BTCUSDT'로 정규화.
    """
    if not s:
        return ""
    t = str(s).upper().strip()
    if ":" in t:
        t = t.split(":")[-1]
    for sep in ["_", "-", ".", "/"]:
        t = t.replace(sep, "")
    for suf in ["UMCBL", "DMCBL", "CMCBL", "PERP"]:
        if t.endswith(suf):
            t = t[: -len(suf)]
    if not t.endswith("USDT"):
        t = t + "USDT"
    return t


def round_down_step(x: float, step: float) -> float:
    """거래소가 요구하는 최소 호가/수량 스텝에 맞춰 내림 반올림."""
    try:
        x = float(x)
        step = float(step)
    except Exception:
        return float(x)
    if step <= 0:
        return float(x)
    k = int(x / step)
    return float(f"{k * step:.12f}")


def _ts_ms() -> str:
    return str(int(time.time() * 1000))


def _sign(ts: str, method: str, path: str, body: str) -> str:
    raw = f"{ts}{method.upper()}{path}{body}"
    mac = hmac.new(API_SECRET.encode(), raw.encode(), hashlib.sha256).digest()
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
    url_path = path
    url = BITGET_HOST + url_path
    ts = _ts_ms()
    body_str = json.dumps(body or {}, separators=(",", ":"))
    sign = _sign(ts, method, url_path, body_str if method.upper() != "GET" else "")
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
    """환경변수 → Bitget v2 표기로 매핑"""
    m = (MARGIN_MODE_ENV or "cross").lower()
    if m in ("cross", "crossed", "cross_margin"):
        return "crossed"
    if m in ("isolated", "fixed", "isolate"):
        return "isolated"
    return "crossed"

# =========================
# 심볼 해석기 (동적 조회 + 캐시)
# =========================
def _load_symbol_map() -> Dict[str, str]:
    """
    /api/v2/mix/market/contracts 에서 productType=USDT-FUTURES 목록을 받아
    '코어심볼(BTCUSDT)' → '거래소심볼(BTCUSDT_UMCBL 또는 환경에 맞는 문자열)' 매핑 생성
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
    """core(예: DOGEUSDT) → 거래소가 요구하는 심볼 문자열로 변환(캐시 사용)."""
    core = convert_symbol(core)
    now = time.time()
    cached = _SYMBOL_CACHE.get(core)
    if cached and now - cached[1] < _SYMBOL_CACHE_TTL:
        return cached[0]
    # 캐시 미스 → 계약목록 조회
    m = _load_symbol_map()
    ex = m.get(core)
    if not ex:
        # 마지막 폴백: 알려진 접미사로 시도
        ex = core + "_UMCBL"
    _SYMBOL_CACHE[core] = (ex, now)
    return ex

# =========================
# 마켓/스펙
# =========================
def get_last_price(core: str) -> Optional[float]:
    """
    가격 조회(튼튼한 폴백 체인)
    1) v2 ticker (symbol, productType)
    2) v2 tickers 목록(productType)에서 심볼 매칭
    3) v2 candles(1m) 마지막 종가
    4) v1 ticker
    5) v1 depth 최상단 호가(mid)
    """
    # 1) v2 ticker
    sym = _resolve_exchange_symbol(core)
    j = _req_public("GET", "/api/v2/mix/market/ticker", {"symbol": sym, "productType": PRODUCT_TYPE})
    try:
        data = j.get("data") or {}
        last = data.get("last")
        if last is not None:
            px = float(last)
            if px > 0:
                return px
    except Exception:
        pass

    # 2) v2 tickers 목록
    j2 = _req_public("GET", "/api/v2/mix/market/tickers", {"productType": PRODUCT_TYPE})
    try:
        arr = j2.get("data") or []
        for it in arr:
            if (it.get("symbol") or "").upper() == sym:
                last = it.get("last")
                if last is not None and float(last) > 0:
                    return float(last)
    except Exception:
        pass

    # 3) v2 candles 1m
    j3 = _req_public("GET", "/api/v2/mix/market/candles", {"symbol": sym, "granularity": "60"})
    try:
        arr = j3.get("data") or []
        if arr:
            # [ts, open, high, low, close, volume, ...] 형태
            close_px = float(arr[0][4])
            if close_px > 0:
                return close_px
    except Exception:
        pass

    # 4) v1 ticker
    j4 = _req_public("GET", "/api/mix/v1/market/ticker", {"symbol": sym})
    try:
        data = j4.get("data") or {}
        last = data.get("last")
        if last is not None and float(last) > 0:
            return float(last)
    except Exception:
        pass

    # 5) v1 depth → mid
    j5 = _req_public("GET", "/api/mix/v1/market/depth", {"symbol": sym, "limit": 1})
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

    _dbg("get_last_price FAIL for", core, "->", sym, "responses:", j)
    return None


def get_symbol_spec(core: str) -> Dict[str, Any]:
    sym = _resolve_exchange_symbol(core)
    size_step, price_step = DEFAULT_SIZE_STEP, DEFAULT_PRICE_STEP

    # v2 instruments
    j = _req_public("GET", "/api/v2/mix/market/contracts", {"productType": PRODUCT_TYPE})
    try:
        for it in j.get("data") or []:
            if (it.get("symbol") or "").upper() == sym:
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
            if (it.get("symbol") or "").upper() == sym:
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
    sym = _resolve_exchange_symbol(core)
    j = _req_public("GET", "/api/v2/mix/market/ticker", {"symbol": sym, "productType": PRODUCT_TYPE})
    return bool(j.get("data"))

# =========================
# 계정/포지션
# =========================
def set_position_mode(mode: str = "oneway") -> Dict[str, Any]:
    m = (mode or "oneway").lower()
    if m not in ("oneway", "hedge"): m = "oneway"
    body = {"productType": PRODUCT_TYPE, "posMode": "one_way" if m=="oneway" else "hedge"}
    return _req_private("POST", "/api/v2/mix/account/set-position-mode", body)


def set_leverage(core: str, leverage: float) -> Dict[str, Any]:
    sym = _resolve_exchange_symbol(core)
    body = {
        "symbol": sym,
        "productType": PRODUCT_TYPE,
        "marginCoin": "USDT",
        "leverage": str(int(leverage or 1)),
        "holdSide": "long",
        "marginMode": _margin_mode_v2(),
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
                # 비교 대상도 해석 필요
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

# =========================
# 주문
# =========================
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
    sym = _resolve_exchange_symbol(core)
    size = _compute_size(core, amount_usdt, leverage)
    if size <= 0:
        return {"code": "LOCAL_TICKER_FAIL", "msg": "ticker_none or size<=0"}

    # (선택) 레버리지 선반영 (실패해도 주문은 진행)
    try:
        if leverage and leverage > 0:
            _ = set_leverage(core, leverage)
    except Exception as e:
        _dbg("set_leverage err:", e)

    req_side = _normalize_side_for_oneway(side)
    body = {
        "symbol": sym,
        "productType": PRODUCT_TYPE,
        "marginCoin": "USDT",
        "size": f"{size}",
        "side": req_side,             # buy/sell
        "orderType": "market",
        "force": "gtc",
        "reduceOnly": False,
        "marginMode": _margin_mode_v2(),  # crossed / isolated
    }
    j = _req_private("POST", "/api/v2/mix/order/place-order", body)
    ok = str(j.get("code", "")) in ("00000", "0", "200")
    return j if ok else {"code": j.get("code", "HTTP_ERR"), "msg": j.get("msg") or j}


def place_reduce_by_size(core: str, size: float, side: str) -> Dict[str, Any]:
    sym = _resolve_exchange_symbol(core)
    spec = get_symbol_spec(core)
    size = round_down_step(float(size), float(spec.get("sizeStep", DEFAULT_SIZE_STEP)))
    if size <= 0:
        return {"code": "LOCAL_SIZE_ZERO", "msg": "size<=0"}

    req_side = _normalize_side_for_oneway(side)
    body = {
        "symbol": sym,
        "productType": PRODUCT_TYPE,
        "marginCoin": "USDT",
        "size": f"{size}",
        "side": req_side,            # buy/sell
        "orderType": "market",
        "force": "gtc",
        "reduceOnly": True,
        "marginMode": _margin_mode_v2(),
    }
    j = _req_private("POST", "/api/v2/mix/order/place-order", body)
    ok = str(j.get("code", "")) in ("00000", "0", "200")
    return j if ok else {"code": j.get("code", "HTTP_ERR"), "msg": j.get("msg") or j}


def close_all_for_symbol(core: str) -> Dict[str, Any]:
    sym = _resolve_exchange_symbol(core)
    body = {"symbol": sym, "marginCoin": "USDT", "productType": PRODUCT_TYPE}
    j = _req_private("POST", "/api/v2/mix/order/close-positions", body)
    ok = str(j.get("code", "")) in ("00000", "0", "200")
    return j if ok else {"code": j.get("code", "HTTP_ERR"), "msg": j.get("msg") or j}
