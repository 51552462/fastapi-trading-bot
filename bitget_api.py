# bitget_api.py — Bitget USDT-Futures(v2) helper
# - Symbol cache(contracts) + ticker + precise size rounding
# - place market / reduceOnly (with safe clamp & retry)
from __future__ import annotations

import os
import hmac
import time
import json
import math
import base64
import hashlib
import threading
from typing import Dict, Any, Optional, List, Tuple

import requests

# ────────────────────────────────────────────────────────────
# ENV / CONST
BITGET_HOST = os.getenv("BITGET_HOST", "https://api.bitget.com").rstrip("/")
API_KEY = os.getenv("BITGET_API_KEY", "")
API_SECRET = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE = os.getenv("BITGET_PASSPHRASE", "")

# Product type mapping (USDT-Futures = umcbl)
PRODUCT_TYPE_HUMAN = os.getenv("BITGET_PRODUCT_TYPE", "USDT-FUTURES").upper()
PRODUCT_TYPE = {"USDT-FUTURES": "umcbl", "COIN-FUTURES": "dmcbl"}.get(PRODUCT_TYPE_HUMAN, "umcbl")

MARGIN_COIN = os.getenv("BITGET_MARGIN_COIN", "USDT")
TIMEOUT = (7, 15)

# NOTE: Bitget v2 mix endpoints use symbol WITHOUT "_UMCBL" in most places.
USE_V2_SYMBOL = True

# ────────────────────────────────────────────────────────────
# HTTP + SIGN
def _ts_ms() -> str:
    return str(int(time.time() * 1000))

def _sign(timestamp: str, method: str, path: str, body: str = "") -> str:
    msg = f"{timestamp}{method.upper()}{path}{body}"
    mac = hmac.new(API_SECRET.encode(), msg.encode(), hashlib.sha256)
    return base64.b64encode(mac.digest()).decode()

def _headers(method: str, path: str, body: str = "") -> Dict[str, str]:
    ts = _ts_ms()
    sign = _sign(ts, method, path, body)
    return {
        "ACCESS-KEY": API_KEY,
        "ACCESS-SIGN": sign,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": API_PASSPHRASE,
        "Content-Type": "application/json",
        "locale": "en-US",
    }

def _get(path: str, params: Optional[Dict[str, Any]] = None, auth: bool = False) -> Dict[str, Any]:
    url = BITGET_HOST + path
    if not auth:
        r = requests.get(url, params=params or {}, timeout=TIMEOUT)
        return r.json()
    # auth GET는 쿼리를 path에 붙여 사인
    q = ""
    if params:
        from urllib.parse import urlencode
        q = "?" + urlencode(params)
    h = _headers("GET", path + q)
    r = requests.get(url, params=params or {}, headers=h, timeout=TIMEOUT)
    return r.json()

def _post(path: str, payload: Dict[str, Any], auth: bool = True) -> Dict[str, Any]:
    url = BITGET_HOST + path
    body = json.dumps(payload, separators=(",", ":"))
    if not auth:
        r = requests.post(url, data=body, timeout=TIMEOUT, headers={"Content-Type": "application/json"})
        return r.json()
    h = _headers("POST", path, body)
    r = requests.post(url, data=body, headers=h, timeout=TIMEOUT)
    return r.json()

# ────────────────────────────────────────────────────────────
# SYMBOL CONTRACT CACHE (sizeStep/minSize/pricePrecision …)
_CONTRACTS_LOCK = threading.RLock()
_CONTRACTS: Dict[str, Dict[str, Any]] = {}
_LAST_LOAD = 0.0
_LOAD_INTERVAL = 60.0  # 60s마다 최대 1회 갱신

def _product_type_to_api() -> str:
    # umcbl = USDT margined perpetual; dmcbl = coin margined
    return PRODUCT_TYPE

def _load_contracts(force: bool = False) -> None:
    global _LAST_LOAD, _CONTRACTS
    now = time.time()
    if not force and (now - _LAST_LOAD) < _LOAD_INTERVAL and _CONTRACTS:
        return
    try:
        pt = _product_type_to_api()
        data = _get("/api/v2/mix/market/contracts", {"productType": pt})
        lst = data.get("data") or []
        tmp: Dict[str, Dict[str, Any]] = {}
        for it in lst:
            # v2 symbol: e.g. "DOGEUSDT"
            sym = (it.get("symbol") or "").upper()
            if not sym:
                continue
            tmp[sym] = {
                "symbol": sym,
                "baseCoin": it.get("baseCoin"),
                "quoteCoin": it.get("quoteCoin"),
                "pricePlace": int(it.get("pricePlace") or 4),
                "priceEndStep": float(it.get("priceEndStep") or 0.0001),
                "sizePlace": int(it.get("sizePlace") or 3),
                "sizeMultiplier": float(it.get("sizeMultiplier") or 1),
                "sizeStep": float(it.get("minTradeNum") or it.get("sizeStep") or 0.001),
                "minSize": float(it.get("minTradeNum") or 0.001),
            }
        with _CONTRACTS_LOCK:
            _CONTRACTS = tmp
            _LAST_LOAD = now
    except Exception as e:
        print("contract load err:", e)

def _ensure_contract(sym: str) -> Optional[Dict[str, Any]]:
    _load_contracts(force=False)
    s = sym.upper()
    with _CONTRACTS_LOCK:
        if s in _CONTRACTS:
            return _CONTRACTS[s]
    # 한번 더 강제 리프레시 (JASMYUSDT 같은 신규/희귀티커)
    _load_contracts(force=True)
    with _CONTRACTS_LOCK:
        return _CONTRACTS.get(s)

# ────────────────────────────────────────────────────────────
# SYMBOL & PRICE
def convert_symbol(s: str) -> str:
    """입력 심볼을 v2 표기(UMCBL 접미사 없이)로 통일"""
    s = (s or "").upper().replace(" ", "")
    if s.endswith("_UMCBL") or s.endswith("_DMCBL"):
        s = s.split("_")[0]
    if not s.endswith("USDT"):
        # TradingView에서 "DOGEUSDT"로 보통 오므로 default로 USDT 붙임
        if s.endswith("USD"):
            s = s + "T"
        elif "USDT" not in s:
            s = s + "USDT"
    return s

def get_symbol_spec(symbol: str) -> Dict[str, Any]:
    sym = convert_symbol(symbol)
    spec = _ensure_contract(sym)
    if not spec:
        raise ValueError(f"unknown symbol {sym} (contract not found)")
    return spec

def get_last_price(symbol: str) -> Optional[float]:
    sym = convert_symbol(symbol)
    try:
        # v2: /api/v2/mix/market/ticker?symbol=BTCUSDT
        data = _get("/api/v2/mix/market/ticker", {"symbol": sym})
        d = (data.get("data") or {})
        px = float(d.get("lastPr") or d.get("last", 0.0))
        return px if px > 0 else None
    except Exception as e:
        print("ticker err:", e)
        return None

# ────────────────────────────────────────────────────────────
# POSITION (AUTH)
def get_open_positions(symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    """모든 열린 포지션(또는 특정 심볼)을 반환. 사이드/사이즈/엔트리 포함."""
    pt = _product_type_to_api()
    out: List[Dict[str, Any]] = []
    try:
        if symbol:
            sym = convert_symbol(symbol)
            data = _get("/api/v2/mix/position/singlePosition", {"symbol": sym, "productType": pt}, auth=True)
            rows = data.get("data") or []
        else:
            data = _get("/api/v2/mix/position/allPosition", {"productType": pt}, auth=True)
            rows = data.get("data") or []
        for it in rows:
            # v2는 long/short가 각각 entry/size로 오거나, list가 있을 수 있음
            sym = convert_symbol(it.get("symbol") or "")
            # unify both long/short lines if provided
            for side_key in ("long", "short"):
                pos = it.get(side_key) or {}
                size = float(pos.get("total", 0) or pos.get("available", 0) or 0)
                if size <= 0:
                    continue
                out.append({
                    "symbol": sym,
                    "side": side_key,
                    "size": size,
                    "entryPrice": float(pos.get("avgPrice") or 0.0),
                    "leverage": float(pos.get("leverage") or 0.0),
                })
    except Exception as e:
        print("get_open_positions err:", e)
    return out

# ────────────────────────────────────────────────────────────
# SIZE UTILS
def round_down_step(x: float, step: float) -> float:
    if step <= 0:
        return float(x)
    return math.floor(x / step) * step

def _usdt_to_size(usdt: float, price: float, size_step: float, min_size: float) -> float:
    if price <= 0:
        return 0.0
    size = usdt / price
    size = round_down_step(size, size_step)
    if size < min_size:
        # 최소 수량 미만이면 0으로 취급 (다음 로직에서 fail 처리)
        return 0.0
    return size

# ────────────────────────────────────────────────────────────
# ORDERS
def place_market_order(symbol: str, usdt_amount: float, side: str, leverage: Optional[float] = None) -> Dict[str, Any]:
    """시장가 진입 (reduceOnly=False). usdt_amount 기준으로 수량 산출."""
    sym = convert_symbol(symbol)
    spec = get_symbol_spec(sym)
    price = get_last_price(sym)
    if price is None or price <= 0:
        return {"code": "LOCAL_TICKER_FAIL", "msg": "ticker_none"}

    size = _usdt_to_size(float(usdt_amount or 0), price, float(spec["sizeStep"]), float(spec["minSize"]))
    if size <= 0:
        return {"code": "LOCAL_TICKER_FAIL", "msg": "ticker_none or size<=0"}

    # Bitget side: 'buy' for long, 'sell' for short
    dir_side = "buy" if side.lower().startswith("l") else "sell"

    payload = {
        "symbol": sym,
        "productType": _product_type_to_api(),
        "marginCoin": MARGIN_COIN,
        "orderType": "market",
        "side": dir_side,
        "size": str(size),
        "reduceOnly": False,
    }
    if leverage:
        payload["leverage"] = str(leverage)

    resp = _post("/api/v2/mix/order/placeOrder", payload, auth=True)
    return resp

def place_reduce_by_size(symbol: str, contracts: float, side: str) -> Dict[str, Any]:
    """시장가 reduceOnly (감축). 원격 보유수 대비 초과분 자동 조정 + 40017 1회 재시도."""
    sym = convert_symbol(symbol)
    spec = get_symbol_spec(sym)

    # 현재 보유 수량 파악 & 초과 감축 방지
    held = 0.0
    for p in get_open_positions(sym):
        if convert_symbol(p.get("symbol")) == sym and (p.get("side") or "") == side:
            held = float(p.get("size") or 0.0)
            break
    if held <= 0:
        return {"code": "LOCAL_NO_POS", "msg": "no_position"}

    # 요청 수량 → 스텝 반올림 ↓ → 보유수 초과 시 clamp
    step = float(spec["sizeStep"])
    qty = round_down_step(float(contracts or 0), step)
    if qty <= 0:
        return {"code": "LOCAL_BAD_QTY", "msg": "qty<=0"}
    if qty > held:
        # 보유수량-1step로 클램프 (reduceOnly 검증 실패 방지)
        qty = round_down_step(held, step)
        if qty <= 0:
            return {"code": "LOCAL_CLAMP_ZERO", "msg": "clamped_to_zero"}

    dir_side = "sell" if side == "long" else "buy"  # 감축은 반대 방향 체결
    payload = {
        "symbol": sym,
        "productType": _product_type_to_api(),
        "marginCoin": MARGIN_COIN,
        "orderType": "market",
        "side": dir_side,
        "size": str(qty),
        "reduceOnly": True,
    }

    resp = _post("/api/v2/mix/order/placeOrder", payload, auth=True)
    code = str(resp.get("code", ""))

    # 40017 재시도: 1 step 줄여서 한 번 더
    if code == "40017":
        retry_qty = max(round_down_step(qty - step, step), 0.0)
        if retry_qty > 0:
            payload["size"] = str(retry_qty)
            resp2 = _post("/api/v2/mix/order/placeOrder", payload, auth=True)
            # 재시도 응답도 함께 전달
            return {"first": resp, "retry": resp2}
    return resp
