# -*- coding: utf-8 -*-
"""
Bitget REST 어댑터 (ONEWAY 전제)
- main.py / trader.py 가 기대하는 함수 시그니처를 1:1 맞춤
- 레버리지/사이즈/사이드/심볼/포지션모드 정합성 보장
- AMOUNT_MODE 지원: notional(기본) / margin
"""

import os, time, hmac, hashlib, base64, json
from typing import Any, Dict, List, Optional
import requests

BITGET_HOST = os.getenv("BITGET_HOST", "https://api.bitget.com")
API_KEY     = os.getenv("BITGET_API_KEY", "")
API_SECRET  = os.getenv("BITGET_API_SECRET", "")
API_PASS    = os.getenv("BITGET_API_PASS", "")

# 선물(USDT Perp) 심볼 접미사
UMCBL_SUFFIX = os.getenv("BITGET_UMCBL_SUFFIX", "_UMCBL")

# 수량/가격 스텝 기본값(심볼 스펙 조회 실패시 사용)
DEFAULT_SIZE_STEP  = float(os.getenv("DEFAULT_SIZE_STEP", "0.001"))
DEFAULT_PRICE_STEP = float(os.getenv("DEFAULT_PRICE_STEP", "0.0001"))

# amount 해석 모드: notional(명목가) / margin(증거금)
AMOUNT_MODE = os.getenv("AMOUNT_MODE", "notional").lower().strip()

# 포지션 모드 (oneway만 사용)
POSITION_MODE = os.getenv("BITGET_POSITION_MODE", "oneway").lower().strip()

# 타임아웃/재시도
HTTP_TIMEOUT = 8

# =============== 공통 유틸 ===============

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

def _req_public(method: str, path: str, params: Optional[Dict[str, Any]]=None) -> Dict[str, Any]:
    url = BITGET_HOST + path
    try:
        if method.upper()=="GET":
            r = requests.get(url, params=params or {}, timeout=HTTP_TIMEOUT)
        else:
            r = requests.post(url, json=params or {}, timeout=HTTP_TIMEOUT)
        return r.json()
    except Exception as e:
        return {"code":"HTTP_ERR", "msg": f"{type(e).__name__}: {e}"}

def _req_private(method: str, path: str, body: Optional[Dict[str, Any]]=None, query: Optional[Dict[str, Any]]=None) -> Dict[str, Any]:
    url_path = path
    url = BITGET_HOST + url_path
    ts = _ts_ms()
    q = ""
    if query:
        # 정렬 없이 그대로 보냄(비트겟은 보통 쿼리스트링 포함해 서명하지 않음 - v2는 path만)
        pass
    body_str = json.dumps(body or {}, separators=(",", ":"))
    sign = _sign(ts, method, url_path, body_str if method.upper()!="GET" else "")

    try:
        if method.upper()=="GET":
            r = requests.get(url, params=query or {}, headers=_headers(ts, sign), timeout=HTTP_TIMEOUT)
        elif method.upper()=="POST":
            r = requests.post(url, params=query or {}, data=body_str, headers=_headers(ts, sign), timeout=HTTP_TIMEOUT)
        else:
            r = requests.request(method.upper(), url, params=query or {}, data=body_str, headers=_headers(ts, sign), timeout=HTTP_TIMEOUT)
        return r.json()
    except Exception as e:
        return {"code":"HTTP_ERR", "msg": f"{type(e).__name__}: {e}"}

def _core_to_umcbl(core: str) -> str:
    core = core.upper().replace("PERP","").replace("_","")
    if core.endswith("USDT"):
        return core + UMCBL_SUFFIX
    return core + "USDT" + UMCBL_SUFFIX

def round_down_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    k = int(x / step)
    return float(f"{k * step:.12f}")

# =============== 마켓/스펙 ===============

def get_last_price(core: str) -> Optional[float]:
    """
    v2 ticker 우선 사용. v2에서 last가 비는 종목은 드물지만, 비면 v1로 폴백.
    """
    sym_v2 = _core_to_umcbl(core)  # v2도 보통 _UMCBL 필요
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
    심볼 사양 조회: sizeStep/priceStep 반환. 실패시 디폴트.
    """
    sym = _core_to_umcbl(core)
    # v2 instruments
    j = _req_public("GET", "/api/v2/mix/market/contracts", {})
    size_step, price_step = DEFAULT_SIZE_STEP, DEFAULT_PRICE_STEP
    try:
        arr = j.get("data") or []
        for it in arr:
            if (it.get("symbol") or "").upper()==sym:
                # v2 필드명: priceTick, sizeTick
                ps = it.get("priceTick"); ss = it.get("sizeTick")
                if ps: price_step = float(ps)
                if ss: size_step  = float(ss)
                return {"sizeStep": size_step, "priceStep": price_step}
    except Exception:
        pass
    # v1 fallback
    j1 = _req_public("GET", "/api/mix/v1/market/contracts", {})
    try:
        arr = j1.get("data") or []
        for it in arr:
            if (it.get("symbol") or "").upper()==sym:
                ps = it.get("priceEndStep") or it.get("priceTick")
                ss = it.get("volumePlace") or it.get("sizeTick")
                if ps: price_step = float(ps)
                if ss:
                    try:
                        size_step = float(ss)
                    except:
                        # volumePlace가 소수 자리수인 경우(예: 3) → 10^-3
                        size_step = 10 ** (-int(ss))
                return {"sizeStep": size_step, "priceStep": price_step}
    except Exception:
        pass
    return {"sizeStep": size_step, "priceStep": price_step}

# =============== 계정/포지션 ===============

def set_position_mode(mode: str="oneway") -> Dict[str, Any]:
    """
    ONEWAY 고정. (Bitget v2: /api/v2/mix/account/set-position-mode)
    """
    m = mode.lower()
    if m not in ("oneway", "hedge"):
        m = "oneway"
    body = {
        "productType": "USDT-FUTURES",  # Bitget v2 표기
        "posMode": "one_way" if m=="oneway" else "hedge"
    }
    return _req_private("POST", "/api/v2/mix/account/set-position-mode", body)

def set_leverage(core: str, leverage: float) -> Dict[str, Any]:
    """
    v2 set-leverage (oneway 이므로 longShortMode='cross'와 별개로 leverage 단일)
    """
    sym = _core_to_umcbl(core)
    body = {
        "symbol": sym,
        "marginCoin": "USDT",
        "leverage": str(int(leverage)),
        "holdSide": "long"  # v2는 long/short 각각 보낼 수 있으나 oneway는 합의적으로 long만 보내도 반영됨
    }
    return _req_private("POST", "/api/v2/mix/account/set-leverage", body)

def get_open_positions(symbol: Optional[str]=None) -> List[Dict[str, Any]]:
    """
    현재 보유 포지션(리스트). symbol=None이면 전체.
    v2 positions
    """
    j = _req_private("GET", "/api/v2/mix/position/all-position", query={"productType":"USDT-FUTURES"})
    arr = []
    try:
        data = j.get("data") or []
        for it in data:
            if symbol and (it.get("symbol") or "") != _core_to_umcbl(symbol):
                continue
            # 통일된 형태로 정규화
            sz = float(it.get("total") or it.get("holdVolume") or 0.0)
            sd = (it.get("holdSide") or it.get("side") or "").lower()
            arr.append({
                "symbol": it.get("symbol"),
                "size": sz,
                "side": sd,  # "long" / "short"
                "entryPrice": float(it.get("avgOpenPrice") or it.get("openPrice") or 0.0),
                "unrealizedPnl": float(it.get("unrealizedPL") or 0.0),
            })
    except Exception:
        pass
    return arr

# =============== 주문 ===============

def _normalize_side_for_oneway(side: str) -> str:
    """
    ONEWAY 에서 롱/숏 개시의 주문 사이드
    - 롱 진입: buy
    - 숏 진입: sell
    - 감축/청산도 같은 매핑 유지 (Bitget는 reduceOnly로 구분)
    """
    s = side.lower()
    if s == "long":
        return "buy"
    if s == "short":
        return "sell"
    # fall-back
    return "buy"

def _compute_size(core: str, amount_usdt: float, leverage: float) -> float:
    price = float(get_last_price(core) or 0.0)
    if price <= 0:
        return 0.0
    spec = get_symbol_spec(core)
    # amount 해석
    if AMOUNT_MODE == "margin":
        notional = float(amount_usdt) * float(leverage)
    else:
        notional = float(amount_usdt)
    size = notional / price
    size = round_down_step(size, float(spec.get("sizeStep", DEFAULT_SIZE_STEP)))
    return size

def place_market_order(core: str, amount_usdt: float, side: str, leverage: float) -> Dict[str, Any]:
    """
    시장가 진입(ONEWAY)
    - AMOUNT_MODE 적용
    - set-leverage 선반영(옵션)
    """
    sym = _core_to_umcbl(core)
    size = _compute_size(core, amount_usdt, leverage)
    if size <= 0:
        return {"code":"LOCAL_TICKER_FAIL", "msg":"ticker_none or size<=0"}

    # 레버리지(선택적으로 보정)
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
        "side": req_side,            # buy/sell
        "orderType": "market",
        "force": "gtc",              # or "ioc"
        "reduceOnly": False,
    }
    j = _req_private("POST", "/api/v2/mix/order/place-order", body)
    ok = str(j.get("code","")) in ("00000","0","200")
    return j if ok else {"code": j.get("code","HTTP_ERR"), "msg": j.get("msg") or j}

def place_reduce_by_size(core: str, size: float, side: str) -> Dict[str, Any]:
    """
    보유 포지션을 size만큼 감축(시장가)
    - oneway: reduceOnly=True
    - side 매핑은 진입과 동일한 buy/sell (비트겟은 reduceOnly 플래그로 감축 처리)
    """
    sym = _core_to_umcbl(core)
    spec = get_symbol_spec(core)
    size = round_down_step(float(size), float(spec.get("sizeStep", DEFAULT_SIZE_STEP)))
    if size <= 0:
        return {"code":"LOCAL_SIZE_ZERO", "msg":"size<=0"}

    req_side = _normalize_side_for_oneway(side)
    body = {
        "symbol": sym,
        "marginCoin": "USDT",
        "size": f"{size}",
        "side": req_side,           # buy/sell
        "orderType": "market",
        "force": "gtc",
        "reduceOnly": True,
    }
    j = _req_private("POST", "/api/v2/mix/order/place-order", body)
    ok = str(j.get("code","")) in ("00000","0","200")
    return j if ok else {"code": j.get("code","HTTP_ERR"), "msg": j.get("msg") or j}

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
    # v2 close-all
    j = _req_private("POST", "/api/v2/mix/order/close-positions", body)
    ok = str(j.get("code","")) in ("00000","0","200")
    return j if ok else {"code": j.get("code","HTTP_ERR"), "msg": j.get("msg") or j}
