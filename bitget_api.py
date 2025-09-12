# -*- coding: utf-8 -*-
"""
Bitget v2 REST helper (USDT-FUTURES 전용)
- 심볼 변환, 계약정보 캐시(contracts), 사이즈 라운딩(sizeStep/minTradeNum/sizeMult)
- 포지션 조회 (single-position / all-position)
- 시세 조회 (ticker)
- 마켓 주문(placeOrder), 포지션 마켓 청산(현재 포지션 사이즈 자동 탐지)
- reduceOnly: "YES"/"NO" (Bitget v2 요구)
"""

from __future__ import annotations
import os, time, hmac, hashlib, json, math, threading
from typing import Dict, Any, List, Optional, Tuple
import requests

BITGET_HOST = "https://api.bitget.com"  # v2 고정
PRODUCT_TYPE = "USDT-FUTURES"
MARGIN_COIN  = "USDT"

_API_KEY       = os.getenv("BITGET_API_KEY", "")
_API_SECRET    = os.getenv("BITGET_API_SECRET", "")
_API_PASSPHRASE= os.getenv("BITGET_API_PASSPHRASE", "")

# ---- Simple logger ---------------------------------------------------------
def _log(*args):
    print("[bitget]", *args, flush=True)

# ---- HTTP / sign -----------------------------------------------------------
def _ts_ms() -> str:
    return str(int(time.time() * 1000))

def _sign(pre_hash: str) -> str:
    return hmac.new(_API_SECRET.encode(), pre_hash.encode(), hashlib.sha256).hexdigest()

def _headers(ts: str, body: str) -> Dict[str, str]:
    pre_hash = ts + "application/json" + body
    sign = _sign(pre_hash)
    return {
        "ACCESS-KEY": _API_KEY,
        "ACCESS-SIGN": sign,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": _API_PASSPHRASE,
        "Content-Type": "application/json"
    }

def _get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = BITGET_HOST + path
    ts = _ts_ms()
    # v2 GET은 body가 비어있는 형태로 sign (문서 예시 동일)
    headers = _headers(ts, "")
    r = requests.get(url, headers=headers, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def _post(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = BITGET_HOST + path
    ts = _ts_ms()
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    headers = _headers(ts, body)
    r = requests.post(url, headers=headers, data=body, timeout=10)
    r.raise_for_status()
    return r.json()

# ---- Symbol helpers --------------------------------------------------------
def convert_symbol(sym: str) -> str:
    """
    TradingView 등에서 오는 'DOGEUSDT' → Bitget v2 'DOGEUSDT'
    (지금은 동일 포맷이지만, 확장성 위해 함수로 유지)
    """
    return sym.replace("_", "").upper()

# ---- Contracts cache (sizeStep/minTradeNum/sizeMult) ----------------------
_contracts_cache: Dict[str, Dict[str, Any]] = {}
_contracts_lock  = threading.Lock()
_contracts_last  = 0
_CONTRACTS_TTL   = 60 * 5  # 5분

def refresh_contracts_cache(force: bool = False) -> None:
    global _contracts_last
    now = time.time()
    with _contracts_lock:
        if not force and (now - _contracts_last) < _CONTRACTS_TTL and _contracts_cache:
            return
        res = _get("/api/v2/mix/market/contracts", {"productType": PRODUCT_TYPE})
        if str(res.get("code")) != "00000":
            raise RuntimeError(f"contracts fail: {res}")
        data = res.get("data", [])
        _contracts_cache.clear()
        for c in data:
            sym = c.get("symbol")
            _contracts_cache[sym] = c
        _contracts_last = now
        _log(f"contracts cached: {len(_contracts_cache)}")

def _get_contract(sym: str) -> Dict[str, Any]:
    refresh_contracts_cache()
    c = _contracts_cache.get(sym)
    if not c:
        # 강제 갱신 후 한번 더 시도
        refresh_contracts_cache(force=True)
        c = _contracts_cache.get(sym)
    if not c:
        raise RuntimeError(f"contract not found: {sym}")
    return c

def _round_size(sym: str, size: float) -> float:
    c = _get_contract(sym)
    step = float(c.get("sizeStep", 0))
    min_num = float(c.get("minTradeNum", 0))
    mult = float(c.get("sizeMultiplier", c.get("sizeMult", 1)))  # 호환 필드
    if step <= 0:
        return size
    # floor to step
    q = math.floor(size / step) * step
    if q < min_num:
        q = min_num
    # 미래 호환성 (사이즈 배수 제약)
    if mult and mult > 0:
        q = math.floor(q / mult) * mult
        if q < mult:
            q = mult
    # 반올림 소수 제한(실수 오차 정리)
    prec = max(0, -int(math.log10(step))) if step < 1 else 0
    return float(f"{q:.{prec}f}")

# ---- Public market ---------------------------------------------------------
def get_last_price(sym: str) -> float:
    """
    v2 ticker: /api/v2/mix/market/ticker
    lastPr == None 이면 markPrice 또는 bestAsk 사용 (Bitget팀 안내에 따라 주로 lastPr 정상)
    """
    s = convert_symbol(sym)
    res = _get("/api/v2/mix/market/ticker", {"symbol": s})
    if str(res.get("code")) != "00000":
        raise RuntimeError(f"ticker fail: {res}")
    d = res.get("data") or {}
    # Bitget팀 피드백: lastPr가 정상. 만약 None이면 보조가격 사용
    price = d.get("lastPr")
    if price is None:
        price = d.get("markPrice") or d.get("bestAsk") or d.get("bestBid")
    if price is None:
        raise RuntimeError(f"ticker no price: {res}")
    return float(price)

# ---- Positions -------------------------------------------------------------
def get_positions_by_symbol(sym: str) -> Dict[str, Any]:
    """
    /api/v2/mix/position/single-position
    """
    s = convert_symbol(sym)
    res = _get("/api/v2/mix/position/single-position", {
        "symbol": s, "marginCoin": MARGIN_COIN
    })
    if str(res.get("code")) != "00000":
        raise RuntimeError(f"single-position fail: {res}")
    return res.get("data") or {}

def get_positions_all() -> List[Dict[str, Any]]:
    """
    /api/v2/mix/position/all-position
    """
    res = _get("/api/v2/mix/position/all-position", {
        "productType": PRODUCT_TYPE, "marginCoin": MARGIN_COIN
    })
    if str(res.get("code")) != "00000":
        raise RuntimeError(f"all-position fail: {res}")
    return res.get("data") or []

# ---- Trading (market) ------------------------------------------------------
def place_market_order(
    sym: str,
    side: str,            # "buy" | "sell"
    size: float,          # 계약 수량(코인 단위, Futures size 규칙 적용)
    reduce_only: bool = False,
    client_order_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    v2 placeOrder (market)
    reduceOnly: "YES"/"NO"
    size는 sizeStep/minTradeNum/sizeMult에 맞춰 floor 라운딩
    """
    s = convert_symbol(sym)
    q = _round_size(s, float(size))
    payload = {
        "symbol": s,
        "productType": PRODUCT_TYPE,
        "marginCoin": MARGIN_COIN,
        "orderType": "market",
        "side": "buy" if side.lower().startswith("b") else "sell",
        "size": f"{q}",
        "reduceOnly": "YES" if reduce_only else "NO",
    }
    if client_order_id:
        payload["clientOid"] = client_order_id

    res = _post("/api/v2/mix/order/placeOrder", payload)
    if str(res.get("code")) != "00000":
        raise RuntimeError(f"placeOrder fail: {res}")
    return res

def _current_net_size(sym: str) -> float:
    d = get_positions_by_symbol(sym)
    # one-way 모드 기준: holdSide=long/short 각각 available/total 등 존재
    # 간단화: longSize - shortSize (없으면 0)
    long_sz  = float(d.get("long", {}).get("total", 0.0)) if isinstance(d.get("long"), dict) else 0.0
    short_sz = float(d.get("short", {}).get("total", 0.0)) if isinstance(d.get("short"), dict) else 0.0
    return long_sz - short_sz  # >0 long 보유, <0 short 보유

def close_position_market(sym: str, reason: str = "manual") -> Dict[str, Any]:
    """
    현재 보유 포지션을 반대 사이드 market 주문으로 청산
    """
    s = convert_symbol(sym)
    net = _current_net_size(s)
    if abs(net) <= 0:
        return {"ok": True, "skipped": True, "reason": "no_position"}

    side = "sell" if net > 0 else "buy"  # long -> sell, short -> buy
    res = place_market_order(s, side=side, size=abs(net), reduce_only=True,
                             client_order_id=f"close_{int(time.time())}")
    return {"ok": True, "res": res, "closed": abs(net), "side": side, "reason": reason}

# ---- export ---------------------------------------------------------------
__all__ = [
    "convert_symbol",
    "refresh_contracts_cache",
    "get_last_price",
    "get_positions_all",
    "get_positions_by_symbol",
    "place_market_order",
    "close_position_market",
]
