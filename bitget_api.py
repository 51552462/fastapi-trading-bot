# bitget_api.py — Bitget USDT-Futures (v2) helper (완성본)
# - productType=USDT-FUTURES
# - contracts cache (sizeStep/minSize/pricePlace …)
# - ticker (lastPr만 신뢰), positions(holdSide), market order open/close
# - USDT→수량 sizeStep 내림, reduceOnly 안전 감축 + 40017 1회 재시도
from __future__ import annotations

import os
import hmac
import time
import json
import math
import base64
import hashlib
import threading
from typing import Dict, Any, Optional, List

import requests

# ────────────────────────────────────────────────────────────
# ENV / CONST
BITGET_HOST = os.getenv("BITGET_HOST", "https://api.bitget.com").rstrip("/")
API_KEY = os.getenv("BITGET_API_KEY", "")
API_SECRET = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE = os.getenv("BITGET_PASSPHRASE", "")

# v2 공식 productType
PRODUCT_TYPE = "USDT-FUTURES"
MARGIN_COIN = os.getenv("BITGET_MARGIN_COIN", "USDT")

# HTTP timeout (connect, read)
TIMEOUT = (7, 15)

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
    # auth GET: query까지 포함해 사인
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

def _product_type() -> str:
    return PRODUCT_TYPE  # "USDT-FUTURES"

# ────────────────────────────────────────────────────────────
# CONTRACTS CACHE
_CONTRACTS_LOCK = threading.RLock()
_CONTRACTS: Dict[str, Dict[str, Any]] = {}
_LAST_LOAD = 0.0
_LOAD_INTERVAL = 60.0  # seconds

def _load_contracts(force: bool = False) -> None:
    global _LAST_LOAD, _CONTRACTS
    now = time.time()
    if not force and (now - _LAST_LOAD) < _LOAD_INTERVAL and _CONTRACTS:
        return
    try:
        data = _get("/api/v2/mix/market/contracts", {"productType": _product_type()})
        rows = data.get("data") or []
        tmp: Dict[str, Dict[str, Any]] = {}
        for it in rows:
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
                # v2: minTradeNum을 최소/스텝으로 사용
                "sizeStep": float(it.get("minTradeNum") or 0.001),
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
    _load_contracts(force=True)
    with _CONTRACTS_LOCK:
        return _CONTRACTS.get(s)

# ────────────────────────────────────────────────────────────
# SYMBOL/TICKER
def convert_symbol(s: str) -> str:
    """v2 표기(예: DOGEUSDT)로 정규화; 구형 접미사 제거"""
    s = (s or "").upper().replace(" ", "")
    if s.endswith("_UMCBL") or s.endswith("_DMCBL"):
        s = s.split("_")[0]
    if not s.endswith("USDT"):
        if s.endswith("USD"):
            s += "T"
        elif "USDT" not in s:
            s += "USDT"
    return s

def get_symbol_spec(symbol: str) -> Dict[str, Any]:
    sym = convert_symbol(symbol)
    spec = _ensure_contract(sym)
    if not spec:
        raise ValueError(f"unknown symbol {sym} (contract not found)")
    return spec

def get_last_price(symbol: str) -> Optional[float]:
    """
    Bitget 공식 안내: lastPr는 최신 체결가이며 일반적으로 null이 아니다.
    → lastPr만 신뢰. 없으면 None 반환(상위 레이어에서 재시도/스킵).
    """
    sym = convert_symbol(symbol)
    try:
        data = _get("/api/v2/mix/market/ticker", {"symbol": sym})
        d = data.get("data") or {}
        px = d.get("lastPr")
        if px in (None, "", "null"):
            print(f"[ticker] lastPr missing for {sym}. raw={d}")
            return None
        px = float(px)
        return px if px > 0 else None
    except Exception as e:
        print("ticker err:", e)
        return None

# ────────────────────────────────────────────────────────────
# POSITIONS (v2 canonical: one row per side with holdSide)
def get_open_positions(symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    try:
        if symbol:
            sym = convert_symbol(symbol)
            data = _get(
                "/api/v2/mix/position/singlePosition",
                {"symbol": sym, "productType": _product_type()},
                auth=True,
            )
            rows = data.get("data") or []
        else:
            data = _get(
                "/api/v2/mix/position/allPosition",
                {"productType": _product_type()},
                auth=True,
            )
            rows = data.get("data") or []

        for it in rows:
            sym = convert_symbol(it.get("symbol") or "")
            hold_side = (it.get("holdSide") or "").lower()
            if hold_side not in ("long", "short"):
                continue
            size = float(it.get("total") or it.get("available") or it.get("holdAmount") or 0)
            if size <= 0:
                continue
            out.append({
                "symbol": sym,
                "side": hold_side,
                "size": size,
                "entryPrice": float(it.get("avgPrice") or it.get("openAvgPrice") or 0.0),
                "leverage": float(it.get("leverage") or 0.0),
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
        return 0.0
    return size

# ────────────────────────────────────────────────────────────
# ORDERS
def place_market_order(symbol: str, usdt_amount: float, side: str, leverage: Optional[float] = None) -> Dict[str, Any]:
    """
    시장가 OPEN (reduceOnly="NO").
    usdt_amount → contracts using last price with sizeStep/minSize rounding.
    """
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
        "productType": _product_type(),
        "marginCoin": MARGIN_COIN,
        "orderType": "market",
        "side": dir_side,
        "size": str(size),     # string
        "reduceOnly": "NO",    # v2: YES/NO
    }
    if leverage:
        payload["leverage"] = str(leverage)

    resp = _post("/api/v2/mix/order/placeOrder", payload, auth=True)
    return resp

def place_reduce_by_size(symbol: str, contracts: float, side: str) -> Dict[str, Any]:
    """
    시장가 reduceOnly CLOSE.
    - 원격 보유수량 조회 후 초과분 clamp
    - 40017 시 1 step 줄여 1회 재시도
    """
    sym = convert_symbol(symbol)
    spec = get_symbol_spec(sym)

    # 현재 보유 size (해당 side)
    held = 0.0
    for p in get_open_positions(sym):
        if convert_symbol(p.get("symbol")) == sym and (p.get("side") or "") == side:
            held = float(p.get("size") or 0.0)
            break
    if held <= 0:
        return {"code": "LOCAL_NO_POS", "msg": "no_position"}

    step = float(spec["sizeStep"])
    qty = round_down_step(float(contracts or 0), step)
    if qty <= 0:
        return {"code": "LOCAL_BAD_QTY", "msg": "qty<=0"}

    # 보유 이하로 clamp
    max_qty = round_down_step(held, step)
    if qty > max_qty:
        qty = max_qty
    if qty <= 0:
        return {"code": "LOCAL_CLAMP_ZERO", "msg": "clamped_to_zero"}

    # reduceOnly는 반대 방향
    dir_side = "sell" if side == "long" else "buy"
    payload = {
        "symbol": sym,
        "productType": _product_type(),
        "marginCoin": MARGIN_COIN,
        "orderType": "market",
        "side": dir_side,
        "size": str(qty),
        "reduceOnly": "YES",  # v2: YES/NO
    }

    resp = _post("/api/v2/mix/order/placeOrder", payload, auth=True)
    code = str(resp.get("code", ""))

    # 40017이면 1 step 줄여 재시도
    if code == "40017":
        retry_qty = round_down_step(qty - step, step)
        if retry_qty > 0:
            payload["size"] = str(retry_qty)
            resp2 = _post("/api/v2/mix/order/placeOrder", payload, auth=True)
            return {"first": resp, "retry": resp2}
    return resp
