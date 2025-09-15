# -*- coding: utf-8 -*-
"""
Bitget v2 REST helper (USDT-FUTURES 전용) - 400 오류 수정 버전
- API 파라미터 수정 및 에러 핸들링 강화
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

# API 키 검증
if not _API_KEY or not _API_SECRET or not _API_PASSPHRASE:
    raise RuntimeError("BITGET_API_KEY, BITGET_API_SECRET, BITGET_API_PASSPHRASE 환경변수가 필요합니다")

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
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        _log(f"GET {path} failed: {e}")
        raise RuntimeError(f"API request failed: {e}")
    except Exception as e:
        _log(f"GET {path} error: {e}")
        raise RuntimeError(f"API error: {e}")

def _post(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = BITGET_HOST + path
    ts = _ts_ms()
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    headers = _headers(ts, body)
    try:
        r = requests.post(url, headers=headers, data=body, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        _log(f"POST {path} failed: {e}")
        raise RuntimeError(f"API request failed: {e}")
    except Exception as e:
        _log(f"POST {path} error: {e}")
        raise RuntimeError(f"API error: {e}")

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

# ---- Positions (수정된 버전) ----------------------------------------------
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
    /api/v2/mix/position/all-position - 400 오류 수정 버전
    """
    try:
        # 먼저 단순한 파라미터로 시도
        res = _get("/api/v2/mix/position/all-position", {
            "productType": PRODUCT_TYPE
        })
        if str(res.get("code")) != "00000":
            _log(f"all-position with productType failed: {res}")
            # 대안: marginCoin만으로 시도
            res = _get("/api/v2/mix/position/all-position", {
                "marginCoin": MARGIN_COIN
            })
            if str(res.get("code")) != "00000":
                _log(f"all-position with marginCoin failed: {res}")
                # 최후: 파라미터 없이 시도
                res = _get("/api/v2/mix/position/all-position", {})
                if str(res.get("code")) != "00000":
                    raise RuntimeError(f"all-position fail: {res}")
        return res.get("data") or []
    except Exception as e:
        _log(f"get_positions_all error: {e}")
        # 폴백: 빈 리스트 반환
        return []

# ---- Trading (market) ------------------------------------------------------
def _place_market_order_internal(
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
    res = _place_market_order_internal(s, side=side, size=abs(net), reduce_only=True,
                             client_order_id=f"close_{int(time.time())}")
    return {"ok": True, "res": res, "closed": abs(net), "side": side, "reason": reason}

# ---- Additional functions for trader.py compatibility ----------------------
def get_open_positions(symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    trader.py에서 사용하는 get_open_positions 함수 - 400 오류 수정 버전
    """
    try:
        if symbol:
            pos = get_positions_by_symbol(symbol)
            # 단일 포지션을 리스트 형태로 변환
            positions = []
            if pos.get("long", {}).get("total", 0) > 0:
                positions.append({
                    "symbol": symbol,
                    "side": "long",
                    "size": pos["long"]["total"],
                    "entryPrice": pos["long"].get("averageOpenPrice", 0),
                    "liq_price": pos["long"].get("liquidationPrice", 0)
                })
            if pos.get("short", {}).get("total", 0) > 0:
                positions.append({
                    "symbol": symbol,
                    "side": "short", 
                    "size": pos["short"]["total"],
                    "entryPrice": pos["short"].get("averageOpenPrice", 0),
                    "liq_price": pos["short"].get("liquidationPrice", 0)
                })
            return positions
        else:
            # 모든 포지션 조회 - 수정된 버전 사용
            all_positions = get_positions_all()
            positions = []
            for pos in all_positions:
                symbol = pos.get("symbol")
                if pos.get("long", {}).get("total", 0) > 0:
                    positions.append({
                        "symbol": symbol,
                        "side": "long",
                        "size": pos["long"]["total"],
                        "entryPrice": pos["long"].get("averageOpenPrice", 0),
                        "liq_price": pos["long"].get("liquidationPrice", 0)
                    })
                if pos.get("short", {}).get("total", 0) > 0:
                    positions.append({
                        "symbol": symbol,
                        "side": "short",
                        "size": pos["short"]["total"], 
                        "entryPrice": pos["short"].get("averageOpenPrice", 0),
                        "liq_price": pos["short"].get("liquidationPrice", 0)
                    })
            return positions
    except Exception as e:
        _log(f"get_open_positions error: {e}")
        return []

def place_market_order_with_amount(symbol: str, amount: float, side: str, leverage: float = 1.0) -> Dict[str, Any]:
    """
    trader.py에서 사용하는 place_market_order 함수 (amount 기반)
    """
    try:
        # amount를 USDT 기준으로 받아서 계약 수량으로 변환
        price = get_last_price(symbol)
        if price <= 0:
            raise RuntimeError(f"Invalid price for {symbol}: {price}")
        
        # USDT amount를 계약 수량으로 변환
        size = amount / price
        
        # 레버리지 적용
        if leverage > 1:
            size = size * leverage
            
        return _place_market_order_internal(symbol, side, size)
    except Exception as e:
        _log(f"place_market_order_with_amount error: {e}")
        return {"code": "50000", "msg": str(e)}

def place_reduce_by_size(symbol: str, size: float, side: str) -> Dict[str, Any]:
    """
    trader.py에서 사용하는 place_reduce_by_size 함수
    """
    try:
        return _place_market_order_internal(symbol, side, size, reduce_only=True)
    except Exception as e:
        _log(f"place_reduce_by_size error: {e}")
        return {"code": "50000", "msg": str(e)}

# trader.py 호환성을 위한 별칭
def place_market_order(symbol: str, amount: float, side: str, leverage: float = 1.0) -> Dict[str, Any]:
    """
    trader.py 호환성을 위한 place_market_order 함수
    """
    return place_market_order_with_amount(symbol, amount, side, leverage)

def get_symbol_spec(symbol: str) -> Dict[str, Any]:
    """
    trader.py에서 사용하는 get_symbol_spec 함수
    """
    try:
        return _get_contract(symbol)
    except Exception as e:
        _log(f"get_symbol_spec error: {e}")
        return {"sizeStep": 0.001, "minTradeNum": 0.001, "sizeMultiplier": 1}

def round_down_step(size: float, step: float) -> float:
    """
    trader.py에서 사용하는 round_down_step 함수
    """
    if step <= 0:
        return size
    return math.floor(size / step) * step

def get_account_equity() -> Optional[float]:
    """
    계정 자본 조회 (risk_guard.py에서 사용)
    """
    try:
        # 계정 정보 조회
        res = _get("/api/v2/mix/account/accounts", {
            "productType": PRODUCT_TYPE,
            "marginCoin": MARGIN_COIN
        })
        if str(res.get("code")) != "00000":
            return None
        data = res.get("data", [])
        if data:
            return float(data[0].get("equity", 0))
        return None
    except Exception as e:
        _log(f"get_account_equity error: {e}")
        return None

def get_wallet_balance(coin: str = "USDT") -> Optional[Dict[str, Any]]:
    """
    월렛 잔고 조회 (risk_guard.py에서 사용)
    """
    try:
        res = _get("/api/v2/spot/wallet/account-assets", {"coin": coin})
        if str(res.get("code")) != "00000":
            return None
        data = res.get("data", [])
        if data:
            return data[0]
        return None
    except Exception as e:
        _log(f"get_wallet_balance error: {e}")
        return None

# ---- export ---------------------------------------------------------------
__all__ = [
    "convert_symbol",
    "refresh_contracts_cache", 
    "get_last_price",
    "get_positions_all",
    "get_positions_by_symbol",
    "place_market_order",
    "close_position_market",
    "get_open_positions",
    "place_reduce_by_size", 
    "get_symbol_spec",
    "round_down_step",
    "get_account_equity",
    "get_wallet_balance",
]
