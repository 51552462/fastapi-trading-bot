# -*- coding: utf-8 -*-
"""
bitget_api.py  (FULL)

- v2 엔드포인트 우선, 실패 시 v1 폴백
- oneway(단일) 포지션/hedge(양방향) 모두 지원 (env POSITION_MODE)
- 롱/숏 side 매핑 보강: 'long/buy' -> buy_single, 'short/sell' -> sell_single
- AMOUNT_MODE 지원: 'notional'(기본, 명목가) / 'margin'(마진기준)
- 심볼 자동 매핑: 'DOGEUSDT' -> 'DOGEUSDT_UMCBL' (선물 UMCBL)
- reduceOnly 청산 지원
- 안전한 수량 라운딩(step)
- 에러 메시지/로깅 보강
"""
import os
import time
import json
import hmac
import hashlib
import base64
import requests
from typing import Dict, Any, Optional

# ============ ENV ============

API_KEY       = os.getenv("BITGET_API_KEY", "")
API_SECRET    = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE= os.getenv("BITGET_API_PASSPHRASE", "")

# v2 엔드포인트 기본 권장
USE_V2        = os.getenv("BITGET_USE_V2", "1") == "1"

BASE_V2 = "https://api.bitget.com/api/v2"
BASE_V1 = "https://api.bitget.com/api/mix/v1"

# 선물 심볼 접미사 (UMCBL=USDT 무기한)
MIX_SUFFIX    = os.getenv("BITGET_MIX_SUFFIX", "UMCBL")

# 포지션 모드: oneway | hedge
POSITION_MODE = os.getenv("POSITION_MODE", "oneway").lower()

# 마진 모드: cross | isolated
MARGIN_MODE   = os.getenv("MARGIN_MODE", "isolated").lower()

# 수량 계산 모드: notional(명목가=기본) | margin(마진 기준)
AMOUNT_MODE   = os.getenv("AMOUNT_MODE", "notional").lower()

# 기본 레버리지
LEVERAGE_DEFAULT = float(os.getenv("LEVERAGE_DEFAULT", "5"))

# 타임아웃(초)
HTTP_TIMEOUT  = float(os.getenv("HTTP_TIMEOUT", "10"))

# ============ 공통 유틸 ============

def _ts_ms() -> str:
    return str(int(time.time() * 1000))

def _sign_v2(timestamp_ms: str, method: str, path_with_qs: str, body: str) -> str:
    # Bitget v2: prehash = timestamp + method + path + body
    prehash = f"{timestamp_ms}{method.upper()}{path_with_qs}{body}"
    mac = hmac.new(API_SECRET.encode(), prehash.encode(), hashlib.sha256).digest()
    return base64.b64encode(mac).decode()

def _headers_v2(timestamp_ms: str, sign: str) -> Dict[str, str]:
    return {
        "ACCESS-KEY": API_KEY,
        "ACCESS-PASSPHRASE": API_PASSPHRASE,
        "ACCESS-TIMESTAMP": timestamp_ms,
        "ACCESS-SIGN": sign,
        "Content-Type": "application/json",
        "X-CHANNEL-API-CODE": "bitget.openapi"
    }

def _req_v2(method: str, path: str, params: Optional[Dict]=None, body: Optional[Dict]=None) -> Dict:
    """
    path: '/mix/order/place-order' 처럼 /api/v2 이후 경로
    """
    url = BASE_V2 + path
    qs = ""
    if params:
        from urllib.parse import urlencode
        qs = "?" + urlencode(params, doseq=True)
        url += qs
    payload = json.dumps(body or {}, separators=(",", ":"), ensure_ascii=False)

    ts = _ts_ms()
    sign = _sign_v2(ts, method, path + (qs or ""), payload if method.upper() != "GET" else "")
    headers = _headers_v2(ts, sign)
    r = requests.request(method.upper(), url, headers=headers,
                         data=(payload if method.upper() != "GET" else None),
                         timeout=HTTP_TIMEOUT)
    try:
        return r.json()
    except Exception:
        return {"code":"HTTP_"+str(r.status_code), "msg": r.text}

def _req_v1(method: str, path: str, params: Optional[Dict]=None, body: Optional[Dict]=None) -> Dict:
    """
    v1 일부 폴백용.
    path: '/order/placeOrder' 처럼 /api/mix/v1 이후 경로
    """
    url = BASE_V1 + path
    # v1 사인 규칙(참고용): timestamp+method+requestPath+body
    qs = ""
    if params:
        from urllib.parse import urlencode
        qs = "?" + urlencode(params, doseq=True)
        url += qs
    payload = json.dumps(body or {}, separators=(",", ":"), ensure_ascii=False)

    ts = _ts_ms()
    prehash = f"{ts}{method.upper()}{path + (qs or '')}{payload if method.upper() != 'GET' else ''}"
    sign = base64.b64encode(hmac.new(API_SECRET.encode(), prehash.encode(), hashlib.sha256).digest()).decode()

    headers = {
        "ACCESS-KEY": API_KEY,
        "ACCESS-PASSPHRASE": API_PASSPHRASE,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-SIGN": sign,
        "Content-Type": "application/json",
    }
    r = requests.request(method.upper(), url, headers=headers,
                         data=(payload if method.upper() != "GET" else None),
                         timeout=HTTP_TIMEOUT)
    try:
        return r.json()
    except Exception:
        return {"code":"HTTP_"+str(r.status_code), "msg": r.text}

def _ok(res: Dict) -> bool:
    # v2: {"code":"00000", "msg":"success", "data":...}
    code = str(res.get("code", ""))
    return code in ("00000", "0", "success")

def round_down_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    n = int(x / step)
    return float(n * step)

# ============ 심볼/스펙 ============

def _spot_symbol(symbol: str) -> str:
    return symbol.upper()

def _mix_symbol(symbol: str) -> str:
    s = symbol.upper()
    if s.endswith("_UMCBL") or s.endswith("_USDT") or s.endswith("_UMFUTURE"):
        return s
    return f"{s}_{MIX_SUFFIX}"

def get_symbol_spec(symbol: str) -> Dict[str, Any]:
    """
    size step, minSz 등을 얻기 위해 시도. 실패하면 안전한 기본치 반환.
    """
    # v2 instruments
    try:
        if USE_V2:
            res = _req_v2("GET", "/mix/market/contracts")
            if _ok(res):
                for it in res.get("data", []):
                    if it.get("symbol") == _mix_symbol(symbol):
                        # v2는 lotSz 대신 sizeScale/priceScale 제공일 수 있음
                        # sizeStep 추정: 10^(-sizeScale)
                        size_scale = it.get("sizeScale")
                        size_step = 0.001
                        if size_scale is not None:
                            try:
                                size_step = float(f"1e-{int(size_scale)}")
                            except Exception:
                                pass
                        return {
                            "sizeStep": float(size_step),
                            "minSz": float(it.get("minSz", 0.001))
                        }
    except Exception:
        pass

    # v1 fallback
    try:
        res = _req_v1("GET", "/market/contracts")
        if _ok(res):
            for it in res.get("data", []):
                if it.get("symbol") == _mix_symbol(symbol):
                    step = float(it.get("sizeStep", 0.001))
                    return {"sizeStep": step, "minSz": float(it.get("minSz", 0.001))}
    except Exception:
        pass

    # 기본값(안전)
    return {"sizeStep": 0.001, "minSz": 0.001}

# ============ 티커/가격 ============

def get_last_price(symbol: str) -> Optional[float]:
    """
    v2 권장: /api/v2/contract/market/ticker?symbol=DOGEUSDT  (UMCBL 붙이지 않음)
    v1 폴백: /api/mix/v1/market/ticker?symbol=DOGEUSDT_UMCBL
    """
    # v2
    try:
        if USE_V2:
            res = _req_v2("GET", "/contract/market/ticker", params={"symbol": _spot_symbol(symbol)})
            if _ok(res):
                data = res.get("data") or {}
                last = data.get("last") or data.get("close")
                if last is not None:
                    return float(last)
    except Exception:
        pass

    # v1 fallback
    try:
        res = _req_v1("GET", "/market/ticker", params={"symbol": _mix_symbol(symbol)})
        if _ok(res):
            data = res.get("data") or {}
            p = data.get("last") or data.get("close")
            if p is not None:
                return float(p)
    except Exception:
        pass

    return None

# ============ 계정/세팅(레버리지/포지션/마진모드) ============

def set_leverage(symbol: str, leverage: float) -> Dict:
    """
    v2: POST /api/v2/mix/account/set-leverage
    v1: POST /api/mix/v1/account/setLeverage
    """
    lev = str(int(leverage))
    # v2
    if USE_V2:
        body = {
            "symbol": _mix_symbol(symbol),
            "marginCoin": "USDT",
            "leverage": lev,
            "holdSide": "long_short" if POSITION_MODE == "hedge" else "net",
        }
        res = _req_v2("POST", "/mix/account/set-leverage", body=body)
        if _ok(res):
            return res
    # v1
    body = {
        "symbol": _mix_symbol(symbol),
        "marginCoin": "USDT",
        "leverage": lev,
        "holdSide": "long_short" if POSITION_MODE == "hedge" else "net",
    }
    return _req_v1("POST", "/account/setLeverage", body=body)

def set_position_mode() -> Dict:
    """
    v2: POST /api/v2/mix/account/set-position-mode  (net/long_short)
    """
    mode = "long_short" if POSITION_MODE == "hedge" else "net"
    if USE_V2:
        body = {"productType": MIX_SUFFIX.lower(), "positionMode": mode}
        return _req_v2("POST", "/mix/account/set-position-mode", body=body)
    # v1 폴백(없을 수 있음): 무시
    return {"code":"00000","msg":"success","data":{"positionMode":mode}}

def set_margin_mode(symbol: str) -> Dict:
    """
    v2: POST /api/v2/mix/account/set-margin-mode  (cross/isolated)
    """
    mm = "cross" if MARGIN_MODE == "cross" else "isolated"
    if USE_V2:
        body = {"symbol": _mix_symbol(symbol), "marginMode": mm}
        return _req_v2("POST", "/mix/account/set-margin-mode", body=body)
    return {"code":"00000","msg":"success","data":{"marginMode":mm}}

# ============ 주문/청산 ============

def _resolve_side_tag(side: str) -> Optional[str]:
    s = (side or "").lower()
    if s in ("buy", "long", "open_long"):
        return "buy_single"
    if s in ("sell", "short", "open_short"):
        return "sell_single"
    return None

def _calc_qty(symbol: str, usdt_amount: float, leverage: float) -> float:
    """
    수량 계산 (step 라운딩)
    - AMOUNT_MODE = 'notional' → notional(=amount) / price
    - AMOUNT_MODE = 'margin'   → (amount * leverage) / price
    """
    last = get_last_price(symbol)
    if not last:
        return 0.0
    spec = get_symbol_spec(symbol)
    step = float(spec.get("sizeStep", 0.001))
    if AMOUNT_MODE == "margin":
        notional = float(usdt_amount) * float(leverage or 1)
    else:
        notional = float(usdt_amount)
    qty = notional / float(last)
    qty = round_down_step(qty, step)
    return max(qty, float(spec.get("minSz", 0.0)))

def place_market_order(symbol: str, usdt_amount: float, side: str, leverage: float = None,
                       reduce_only: bool = False) -> Dict:
    """
    시장가 오더
    - side: 'long/buy' → buy_single, 'short/sell' → sell_single
    - reduce_only=True → 줄이기만(청산용)
    """
    try:
        lv = leverage or LEVERAGE_DEFAULT
        # 사전 세팅(안전)
        set_position_mode()
        set_margin_mode(symbol)
        set_leverage(symbol, lv)

        side_tag = _resolve_side_tag(side)
        if not side_tag:
            return {"code":"LOCAL_BAD_SIDE","msg":f"unknown side {side}"}

        qty = _calc_qty(symbol, usdt_amount, lv)
        if qty <= 0:
            return {"code":"LOCAL_TICKER_FAIL","msg":"ticker_none or size<=0"}

        body = {
            "symbol":     _mix_symbol(symbol),
            "marginCoin": "USDT",
            "size":       str(qty),
            "side":       side_tag,
            "orderType":  "market",
            "leverage":   str(int(lv)),
            "reduceOnly": bool(reduce_only),
            "clientOid":  f"cli-{int(time.time()*1000)}"
        }

        if USE_V2:
            res = _req_v2("POST", "/mix/order/place-order", body=body)
            if _ok(res):
                return res
        # v1 fallback
        # v1은 필드명이 약간 다를 수 있으나 대부분 호환
        return _req_v1("POST", "/order/placeOrder", body=body)

    except Exception as e:
        return {"code":"LOCAL_EXCEPTION","msg":str(e)}

def close_position(symbol: str, side: str, usdt_amount: float = None) -> Dict:
    """
    포지션 청산(시장가, reduceOnly)
    - side는 현재 보유 방향과 '반대 side_tag'를 쓰지 않습니다.
      Bitget는 reduceOnly + 해당 방향 side_single 으로 수량만큼 줄이면 됩니다.
      여기서는 side에 따라 동일 side_tag로 reduceOnly=True 로 전송.
    - usdt_amount 없으면 큰 값으로 전송(=남은 수량 전부 청산)
    """
    try:
        # 잔여 전량 청산을 위해 아주 큰 notional로 계산
        amount = usdt_amount if usdt_amount and usdt_amount > 0 else 1e12
        lv = LEVERAGE_DEFAULT
        qty = _calc_qty(symbol, amount, lv)
        side_tag = _resolve_side_tag(side)
        if not side_tag:
            # 모를 경우 두 방향 시도 (마켓 reduceOnly)
            r1 = place_market_order(symbol, amount, "long", lv, reduce_only=True)
            r2 = place_market_order(symbol, amount, "short", lv, reduce_only=True)
            return {"code": "00000" if _ok(r1) or _ok(r2) else "LOCAL_CLOSE_FAIL",
                    "msg": "attempted both", "data": {"long": r1, "short": r2}}

        body = {
            "symbol":     _mix_symbol(symbol),
            "marginCoin": "USDT",
            "size":       str(qty),
            "side":       side_tag,      # 같은 방향이지만 reduceOnly=True
            "orderType":  "market",
            "leverage":   str(int(lv)),
            "reduceOnly": True,
            "clientOid":  f"close-{int(time.time()*1000)}"
        }

        if USE_V2:
            res = _req_v2("POST", "/mix/order/place-order", body=body)
            if _ok(res):
                return res
        return _req_v1("POST", "/order/placeOrder", body=body)

    except Exception as e:
        return {"code":"LOCAL_EXCEPTION","msg":str(e)}

# ============ 고수준 헬퍼 ============

def open_long(symbol: str, usdt_amount: float, leverage: float = None) -> Dict:
    return place_market_order(symbol, usdt_amount, "long", leverage or LEVERAGE_DEFAULT, reduce_only=False)

def open_short(symbol: str, usdt_amount: float, leverage: float = None) -> Dict:
    return place_market_order(symbol, usdt_amount, "short", leverage or LEVERAGE_DEFAULT, reduce_only=False)

def close_long(symbol: str) -> Dict:
    # long 포지션 청산(리듀스온리로 같은 buy_single side 전송)
    return close_position(symbol, "long")

def close_short(symbol: str) -> Dict:
    return close_position(symbol, "short")
