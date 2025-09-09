# -*- coding: utf-8 -*-
"""
bitget_api.py — Bitget REST 어댑터 (USDT Perp / ONEWAY 전제)

핵심 포인트
- v2 엔드포인트 사용 (주요: place-order / set-leverage / ticker / contracts / all-position)
- 심볼 정규화: BINANCE:BTCUSDT, BTC-USDT, BTCUSDT_UMCBL 등 → 'BTCUSDT'
- 수량/가격 스텝 맞춤: 거래소 스펙 조회 후 반올림(내림)
- 주문:
  • 진입: 시장가, reduceOnly=False
  • 감축/청산: 시장가, reduceOnly=True
- 마진 모드: cross(=crossed) / isolated — v2에서 필수 → '400172' 방지
- 포지션 모드: oneway 기본 (필요 시 set-position-mode 제공)
- AMOUNT_MODE:
  • notional (기본): amount=명목가(USDT)
  • margin: amount=증거금(USDT) → 실명목가 = amount * leverage
- 실패 시 JSON(code/msg) 그대로 반환하여 상위 레이어에서 원인 파악 용이
"""

import os
import time
import hmac
import hashlib
import base64
import json
from typing import Any, Dict, List, Optional

import requests

# =========================
# 환경변수
# =========================
BITGET_HOST = os.getenv("BITGET_HOST", "https://api.bitget.com")

API_KEY   = os.getenv("BITGET_API_KEY", "")
API_SECRET= os.getenv("BITGET_API_SECRET", "")
API_PASS  = os.getenv("BITGET_API_PASS", "")

# 선물(USDT Perp) 심볼 접미사 (예: BTCUSDT_UMCBL)
UMCBL_SUFFIX = os.getenv("BITGET_UMCBL_SUFFIX", "_UMCBL")

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


# =========================
# 내부 유틸
# =========================
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
    # 접미사 제거
    for suf in ["UMCBL", "PERP"]:
        if t.endswith(suf):
            t = t[: -len(suf)]
    # 쿼트 기본 USDT 가정
    if not t.endswith("USDT"):
        t = t + "USDT"
    return t


def _core_to_umcbl(core: str) -> str:
    core = convert_symbol(core)
    if core.endswith("USDT"):
        return core + UMCBL_SUFFIX
    return core + "USDT" + UMCBL_SUFFIX


def round_down_step(x: float, step: float) -> float:
    """
    거래소가 요구하는 최소 호가/수량 스텝에 맞춰 내림 반올림.
    (부적합 수치로 주문 리젝트되는 것을 방지)
    """
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
    """
    Bitget v2: SIGN = base64(hmac_sha256(secret, ts + method + path + body))
    GET인 경우 body는 빈 문자열.
    """
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


def _req_private(
    method: str,
    path: str,
    body: Optional[Dict[str, Any]] = None,
    query: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    v2 사양에 맞춰 서명/요청.
    """
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
            r = requests.request(
                method.upper(), url, params=query or {}, data=body_str, headers=_headers(ts, sign), timeout=HTTP_TIMEOUT
            )
        return r.json()
    except Exception as e:
        return {"code": "HTTP_ERR", "msg": f"{type(e).__name__}: {e}"}


def _margin_mode_v2() -> str:
    """
    환경변수 → Bitget v2 표기로 매핑
    cross, crossed, cross_margin → "crossed"
    isolated, fixed, isolate      → "isolated"
    """
    m = (MARGIN_MODE_ENV or "cross").lower()
    if m in ("cross", "crossed", "cross_margin"):
        return "crossed"
    if m in ("isolated", "fixed", "isolate"):
        return "isolated"
    return "crossed"


# =========================
# 마켓/스펙
# =========================
def get_last_price(core: str) -> Optional[float]:
    """
    v2 ticker 우선 사용. 실패 시 v1로 폴백.
    """
    sym_v2 = _core_to_umcbl(core)
    j = _req_public("GET", "/api/v2/mix/market/ticker", {"symbol": sym_v2})
    try:
        data = j.get("data") or {}
        last = data.get("last")
        if last is not None:
            return float(last)
    except Exception:
        pass

    # v1 fallback
    j1 = _req_public("GET", "/api/mix/v1/market/ticker", {"symbol": sym_v2})
    try:
        data = j1.get("data") or {}
        last = data.get("last")
        if last is not None:
            return float(last)
    except Exception:
        pass
    return None


def get_symbol_spec(core: str) -> Dict[str, Any]:
    """
    심볼 사양 조회(sizeStep/priceStep). 실패 시 디폴트 반환.
    """
    sym = _core_to_umcbl(core)
    size_step, price_step = DEFAULT_SIZE_STEP, DEFAULT_PRICE_STEP

    # v2 instruments
    j = _req_public("GET", "/api/v2/mix/market/contracts", {})
    try:
        arr = j.get("data") or []
        for it in arr:
            if (it.get("symbol") or "").upper() == sym:
                ps = it.get("priceTick")
                ss = it.get("sizeTick")
                if ps:
                    price_step = float(ps)
                if ss:
                    size_step = float(ss)
                return {"sizeStep": size_step, "priceStep": price_step}
    except Exception:
        pass

    # v1 fallback
    j1 = _req_public("GET", "/api/mix/v1/market/contracts", {})
    try:
        arr = j1.get("data") or []
        for it in arr:
            if (it.get("symbol") or "").upper() == sym:
                ps = it.get("priceEndStep") or it.get("priceTick")
                ss = it.get("sizeTick") or it.get("volumePlace")
                if ps:
                    price_step = float(ps)
                if ss is not None:
                    try:
                        size_step = float(ss)
                    except Exception:
                        # volumePlace가 '3'처럼 소수 자리수인 경우 → 10^-3
                        size_step = 10 ** (-int(ss))
                return {"sizeStep": size_step, "priceStep": price_step}
    except Exception:
        pass

    return {"sizeStep": size_step, "priceStep": price_step}


def symbol_exists(core: str) -> bool:
    """
    빠른 존재 여부 확인 (v2 ticker)
    """
    sym = _core_to_umcbl(core)
    j = _req_public("GET", "/api/v2/mix/market/ticker", {"symbol": sym})
    return bool(j.get("data"))


# =========================
# 계정/포지션
# =========================
def set_position_mode(mode: str = "oneway") -> Dict[str, Any]:
    """
    ONEWAY / HEDGE 설정 (기본 oneway)
    """
    m = (mode or "oneway").lower()
    if m not in ("oneway", "hedge"):
        m = "oneway"
    body = {
        "productType": "USDT-FUTURES",
        "posMode": "one_way" if m == "oneway" else "hedge",
    }
    return _req_private("POST", "/api/v2/mix/account/set-position-mode", body)


def set_leverage(core: str, leverage: float) -> Dict[str, Any]:
    """
    v2 set-leverage (ONEWAY 기준, marginMode 필수)
    """
    sym = _core_to_umcbl(core)
    body = {
        "symbol": sym,
        "marginCoin": "USDT",
        "leverage": str(int(leverage or 1)),
        "holdSide": "long",              # oneway에선 단일; long만 보내도 반영
        "marginMode": _margin_mode_v2(), # crossed / isolated
    }
    return _req_private("POST", "/api/v2/mix/account/set-leverage", body)


def get_open_positions(symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    현재 보유 포지션 목록. symbol=None이면 전체를 반환.
    """
    j = _req_private("GET", "/api/v2/mix/position/all-position", query={"productType": "USDT-FUTURES"})
    arr: List[Dict[str, Any]] = []
    try:
        data = j.get("data") or []
        for it in data:
            if symbol and (it.get("symbol") or "") != _core_to_umcbl(symbol):
                continue
            sz = float(it.get("total") or it.get("holdVolume") or 0.0)
            sd = (it.get("holdSide") or it.get("side") or "").lower()
            arr.append(
                {
                    "symbol": it.get("symbol"),
                    "size": sz,
                    "side": sd,  # "long" / "short"
                    "entryPrice": float(it.get("avgOpenPrice") or it.get("openPrice") or 0.0),
                    "unrealizedPnl": float(it.get("unrealizedPL") or 0.0),
                }
            )
    except Exception:
        pass
    return arr


# =========================
# 주문
# =========================
def _normalize_side_for_oneway(side: str) -> str:
    """
    ONEWAY에서 진입/감축 모두 buy/sell로 보낸다 (reduceOnly로 감축 구분).
    """
    s = (side or "").lower()
    if s == "long":
        return "buy"
    if s == "short":
        return "sell"
    return "buy"


def _compute_size(core: str, amount_usdt: float, leverage: float) -> float:
    """
    AMOUNT_MODE에 따라 실주문 계약수 계산.
    - notional: amount_usdt = 명목가 → size = notional / price
    - margin  : amount_usdt = 증거금 → size = (amount * leverage) / price
    """
    price = float(get_last_price(core) or 0.0)
    if price <= 0:
        return 0.0
    spec = get_symbol_spec(core)
    # amount 해석
    if AMOUNT_MODE == "margin":
        notional = float(amount_usdt) * float(leverage or 1.0)
    else:
        notional = float(amount_usdt)
    size = notional / price
    size = round_down_step(size, float(spec.get("sizeStep", DEFAULT_SIZE_STEP)))
    return size


def place_market_order(core: str, amount_usdt: float, side: str, leverage: float) -> Dict[str, Any]:
    """
    시장가 진입 (reduceOnly=False)
    """
    sym = _core_to_umcbl(core)
    size = _compute_size(core, amount_usdt, leverage)
    if size <= 0:
        return {"code": "LOCAL_TICKER_FAIL", "msg": "ticker_none or size<=0"}

    # (선택) 레버리지 선반영
    try:
        if leverage and leverage > 0:
            _ = set_leverage(core, leverage)
    except Exception:
        pass

    req_side = _normalize_side_for_oneway(side)
    body = {
        "symbol": sym,
        "marginCoin": "USDT",
        "size": f"{size}",
        "side": req_side,             # buy/sell
        "orderType": "market",
        "force": "gtc",               # 또는 "ioc"
        "reduceOnly": False,
        "marginMode": _margin_mode_v2(),  # crossed or isolated (★ 400172 방지)
    }
    j = _req_private("POST", "/api/v2/mix/order/place-order", body)
    ok = str(j.get("code", "")) in ("00000", "0", "200")
    return j if ok else {"code": j.get("code", "HTTP_ERR"), "msg": j.get("msg") or j}


def place_reduce_by_size(core: str, size: float, side: str) -> Dict[str, Any]:
    """
    보유 포지션을 size만큼 감축(시장가, reduceOnly=True)
    """
    sym = _core_to_umcbl(core)
    spec = get_symbol_spec(core)
    size = round_down_step(float(size), float(spec.get("sizeStep", DEFAULT_SIZE_STEP)))
    if size <= 0:
        return {"code": "LOCAL_SIZE_ZERO", "msg": "size<=0"}

    req_side = _normalize_side_for_oneway(side)
    body = {
        "symbol": sym,
        "marginCoin": "USDT",
        "size": f"{size}",
        "side": req_side,            # buy/sell
        "orderType": "market",
        "force": "gtc",
        "reduceOnly": True,
        "marginMode": _margin_mode_v2(),  # crossed or isolated
    }
    j = _req_private("POST", "/api/v2/mix/order/place-order", body)
    ok = str(j.get("code", "")) in ("00000", "0", "200")
    return j if ok else {"code": j.get("code", "HTTP_ERR"), "msg": j.get("msg") or j}


def close_all_for_symbol(core: str) -> Dict[str, Any]:
    """
    심볼 전체 포지션 시장가 강제 청산(원클릭)
    """
    sym = _core_to_umcbl(core)
    body = {
        "symbol": sym,
        "marginCoin": "USDT",
        "productType": "USDT-FUTURES",
    }
    j = _req_private("POST", "/api/v2/mix/order/close-positions", body)
    ok = str(j.get("code", "")) in ("00000", "0", "200")
    return j if ok else {"code": j.get("code", "HTTP_ERR"), "msg": j.get("msg") or j}
