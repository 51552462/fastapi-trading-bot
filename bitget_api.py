# -*- coding: utf-8 -*-
"""
bitget_api.py  (USDT Perpetual Futures)
- v2 우선, 실패시 v1 폴백
- 호스트 강제 교정 가드
- positions 조회는 v2/v1 모두 POST
- reduceOnly: "YES"/"NO"
- sizeStep / minTradeNum 반올림
- 심볼 변환 v2('BTCUSDT') <-> v1('BTCUSDT_UMCBL')
"""

from __future__ import annotations

import os
import hmac
import time
import json
import base64
import hashlib
from typing import Any, Dict, List, Optional, Tuple

import requests

# =========================
# 환경변수 & 상수
# =========================

API_KEY = os.getenv("BITGET_API_KEY", "").strip()
API_SECRET = os.getenv("BITGET_API_SECRET", "").strip()
API_PASSPHRASE = os.getenv("BITGET_API_PASSPHRASE", "").strip()

PRODUCT_TYPE_V2 = "USDT-FUTURES"   # v2 명시
PRODUCT_TYPE_V1 = "umcbl"          # v1 명시(USDT Perp)

TIMEOUT = (8, 15)  # (connect, read)

# ── Host resolver: 환경변수가 잘못되어도 Bitget로 강제 교정
def _resolve_host() -> str:
    raw = os.getenv("BITGET_HOST", "https://api.bitget.com").strip()
    low = raw.lower()
    if "bitget.com" not in low:
        print(f"[FATAL] BITGET_HOST looks wrong: {raw} -> forcing https://api.bitget.com")
        return "https://api.bitget.com"
    return raw.rstrip("/")

BITGET_HOST = _resolve_host()
print(f"[bitget] host={BITGET_HOST}")

_session = requests.Session()


# =========================
# 유틸
# =========================

def _now_ms() -> str:
    return str(int(time.time() * 1000))

def _b64_hmac_sha256(msg: str, secret: str) -> str:
    mac = hmac.new(secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256)
    return base64.b64encode(mac.digest()).decode()

def _headers(ts: str, sign: str) -> Dict[str, str]:
    return {
        "ACCESS-KEY": API_KEY,
        "ACCESS-SIGN": sign,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": API_PASSPHRASE,
        "Content-Type": "application/json",
        # "X-CHANNEL-API-CODE": "bitget-python",  # 선택
    }

def _sign(ts: str, method: str, path: str, body: str) -> str:
    # Bitget: signText = timestamp + method + requestPath + body
    return _b64_hmac_sha256(ts + method + path + body, API_SECRET)

def _request(method: str, path: str, body: Optional[Dict[str, Any]] = None, auth: bool = False) -> Dict[str, Any]:
    url = BITGET_HOST + path
    data = "" if body is None else json.dumps(body, separators=(",", ":"), ensure_ascii=False)
    hdrs = {"Content-Type": "application/json"}
    if auth:
        ts = _now_ms()
        sign = _sign(ts, method.upper(), path, data)
        hdrs = _headers(ts, sign)

    try:
        if method.upper() == "GET":
            resp = _session.get(url, headers=hdrs, timeout=TIMEOUT)
        elif method.upper() == "POST":
            resp = _session.post(url, headers=hdrs, data=data, timeout=TIMEOUT)
        else:
            raise ValueError(f"Unsupported method: {method}")
    except requests.RequestException as e:
        return {"code": "HTTP_ERR", "msg": str(e), "data": None}

    try:
        return resp.json()
    except Exception:
        return {"code": f"HTTP_{resp.status_code}", "msg": resp.text, "data": None}

def _get_v2(path: str, auth: bool = False) -> Dict[str, Any]:
    return _request("GET", path, None, auth=auth)

def _post_v2(path: str, body: Dict[str, Any], auth: bool = False) -> Dict[str, Any]:
    return _request("POST", path, body, auth=auth)

def _get_v1(path: str, auth: bool = False) -> Dict[str, Any]:
    return _request("GET", path, None, auth=auth)

def _post_v1(path: str, body: Dict[str, Any], auth: bool = False) -> Dict[str, Any]:
    return _request("POST", path, body, auth=auth)


# =========================
# 심볼 변환
# =========================

def convert_symbol_v2(v1_symbol: str) -> str:
    """v1: BTCUSDT_UMCBL -> v2: BTCUSDT"""
    if not v1_symbol:
        return ""
    if v1_symbol.endswith("_UMCBL"):
        return v1_symbol[:-6]
    return v1_symbol

def convert_symbol_v1(v2_symbol: str) -> str:
    """v2: BTCUSDT -> v1: BTCUSDT_UMCBL"""
    if not v2_symbol:
        return ""
    if v2_symbol.endswith("_UMCBL"):
        return v2_symbol
    return f"{v2_symbol}_UMCBL"


# =========================
# 계약/틱/스텝 유틸
# =========================

_contract_cache: Dict[str, Dict[str, Any]] = {}

def get_contract_v2(symbol_v2: str) -> Dict[str, Any]:
    """v2 계약 정보 조회 (캐시)"""
    s = symbol_v2.upper()
    if s in _contract_cache:
        return _contract_cache[s]

    # v2: Get All Symbols - Contracts
    r = _get_v2("/api/v2/mix/market/contracts")
    if str(r.get("code")) != "00000":
        print("[contracts v2] ERROR:", r)
        return {}

    for it in r.get("data") or []:
        if it.get("productType") != PRODUCT_TYPE_V2:
            continue
        if it.get("symbol", "").upper() == s:
            _contract_cache[s] = it
            return it
    return {}

def get_size_step(symbol_v2: str) -> Tuple[float, float]:
    """(sizeStep, minTradeNum)"""
    c = get_contract_v2(symbol_v2)
    size_step = float(c.get("sizeStep") or 0.0)
    min_trade = float(c.get("minTradeNum") or 0.0)
    if size_step <= 0:
        size_step = 0.001
    if min_trade <= 0:
        min_trade = size_step
    return size_step, min_trade

def round_size(symbol_v2: str, size: float) -> float:
    step, min_num = get_size_step(symbol_v2)
    if step <= 0:
        return max(size, 0.0)
    # step 단위 내림
    k = int(size / step)
    adj = k * step
    if adj < min_num:
        adj = min_num
    return round(adj, 10)

def get_ticker_last(symbol_v2: str) -> float:
    """v2 ticker.lastPr, fallback v1"""
    s = symbol_v2.upper()
    r = _get_v2(f"/api/v2/mix/market/ticker?symbol={s}")
    if str(r.get("code")) == "00000":
        d = r.get("data") or {}
        lp = d.get("lastPr")
        try:
            v = float(lp)
            if v > 0:
                return v
        except Exception:
            pass

    # fallback v1
    v1s = convert_symbol_v1(s)
    r1 = _get_v1(f"/api/mix/v1/market/ticker?symbol={v1s}")
    if str(r1.get("code")) == "00000":
        d = r1.get("data") or {}
        for key in ("last", "close"):
            try:
                v = float(d.get(key))
                if v > 0:
                    return v
            except Exception:
                pass
    raise RuntimeError(f"tickerNone: {symbol_v2}")


# =========================
# 포지션 조회 (PATCH 적용)
# =========================

def _positions_v2(symbol_v2: Optional[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if symbol_v2:
        payload = {"symbol": symbol_v2, "productType": PRODUCT_TYPE_V2}
        resp = _post_v2("/api/v2/mix/position/singlePosition", payload, auth=True)
    else:
        payload = {"productType": PRODUCT_TYPE_V2}
        resp = _post_v2("/api/v2/mix/position/allPosition", payload, auth=True)

    if str(resp.get("code")) != "00000":
        print("[positions v2] ERROR:", resp)
        return out

    rows = resp.get("data") or []
    for it in rows:
        sym = convert_symbol_v2(it.get("symbol") or "")
        hold = (it.get("holdSide") or "").lower()
        if hold not in ("long", "short"):
            continue
        size = float(it.get("total") or it.get("available") or it.get("holdAmount") or 0)
        if size <= 0:
            continue
        out.append({
            "symbol": sym,
            "side": hold,
            "size": size,
            "entryPrice": float(it.get("avgPrice") or it.get("openAvgPrice") or 0.0),
            "leverage": float(it.get("leverage") or 0.0),
        })
    return out

def _positions_v1(symbol_v2: Optional[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if symbol_v2:
        sym_v1 = convert_symbol_v1(symbol_v2)
        payload = {"symbol": sym_v1, "productType": PRODUCT_TYPE_V1}
        resp = _post_v1("/api/mix/v1/position/singlePosition", payload, auth=True)
    else:
        payload = {"productType": PRODUCT_TYPE_V1}
        resp = _post_v1("/api/mix/v1/position/allPosition", payload, auth=True)

    if str(resp.get("code")) != "00000":
        print("[positions v1] ERROR:", resp)
        return out

    rows = resp.get("data") or []
    for it in rows:
        sym_v1 = it.get("symbol") or ""
        sym = convert_symbol_v2(sym_v1)
        # v1 포맷: {"long":{...}, "short":{...}}
        for side_key in ("long", "short"):
            sub = it.get(side_key) or {}
            size = float(sub.get("total") or sub.get("available") or 0)
            if size <= 0:
                continue
            out.append({
                "symbol": sym,
                "side": side_key,
                "size": size,
                "entryPrice": float(sub.get("avgOpenPrice") or sub.get("openPriceAvg") or 0.0),
                "leverage": float(sub.get("leverage") or 0.0),
            })
    return out

def get_positions(symbol_v2: Optional[str] = None) -> List[Dict[str, Any]]:
    """심볼 지정 시 해당 심볼만, 없으면 전체"""
    v2 = _positions_v2(symbol_v2)
    if v2:
        return v2
    print("[positions] fallback v1 used")
    return _positions_v1(symbol_v2)


# =========================
# 주문
# =========================

def _reduce_only_to_str(flag: bool) -> str:
    # v2는 YES/NO 여야 함 (Bitget가 확인해준 사항)
    return "YES" if flag else "NO"

def place_order_market(symbol_v2: str, side: str, size: float, reduce_only: bool = False) -> Dict[str, Any]:
    """
    side: 'buy' (long open / short close) | 'sell' (short open / long close)
    size: contracts (코인 수량)
    """
    size_adj = round_size(symbol_v2, float(size))
    if size_adj <= 0:
        return {"code": "SIZE_ERR", "msg": "size<=0", "data": None}

    payload = {
        "symbol": symbol_v2.upper(),
        "productType": PRODUCT_TYPE_V2,
        "marginCoin": "USDT",
        "orderType": "market",
        "side": side,  # 'buy' or 'sell'
        "size": f"{size_adj}",
        "reduceOnly": _reduce_only_to_str(reduce_only),
    }
    r = _post_v2("/api/v2/mix/order/placeOrder", payload, auth=True)
    if str(r.get("code")) != "00000":
        print("[placeOrder v2] ERROR:", r)
    return r

def close_position_full(symbol_v2: str, side: str) -> Dict[str, Any]:
    """
    side: 'long' or 'short'  (닫을 포지션 방향)
    내부적으로 반대 side로 reduceOnly=YES 시장가 발주
    """
    pos_list = get_positions(symbol_v2)
    total = 0.0
    for p in pos_list:
        if p["symbol"].upper() == symbol_v2.upper() and p["side"] == side:
            total += float(p["size"])
    if total <= 0:
        return {"code": "NO_POSITION", "msg": "no position", "data": None}

    opp = "sell" if side == "long" else "buy"
    return place_order_market(symbol_v2, opp, total, reduce_only=True)


# =========================
# 금액 → 수량 변환(시장가 진입용)
# =========================

def quote_to_size(symbol_v2: str, usdt_amount: float, leverage: float = 1.0) -> float:
    """
    usdt 금액을 수량(코인)으로 변환. Bitget USDT-Perp 기준.
    """
    px = get_ticker_last(symbol_v2)
    if px <= 0:
        raise RuntimeError("price<=0")
    raw = (usdt_amount * leverage) / px
    return round_size(symbol_v2, raw)


# =========================
# 테스트 헬퍼
# =========================

def resume_positions_message() -> str:
    items = get_positions(None)
    if not items:
        return "Resumed 0 open positions: -"
    tags = [f"{it['symbol']}_{it['side']}" for it in items]
    return f"Resumed {len(items)} open positions: " + ", ".join(tags)


# =========================
# 모듈 직접 실행 테스트
# =========================

if __name__ == "__main__":
    # 간단 점검 루틴
    try:
        print(resume_positions_message())
    except Exception as e:
        print("resume error:", e)
