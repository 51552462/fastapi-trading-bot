# bitget_api.py  — Bitget USDT-Futures helper (v2 우선 + v1 폴백)
# 기능 요약
# - v2 계약/티커/포지션/주문을 우선 시도 → 실패(code != "00000", lastPr 누락 등) 시 v1로 자동 폴백
# - v1과 v2의 차이(심볼 표기, productType, reduceOnly 등) 자동 변환
# - 계약 캐시(/contracts), sizeStep/minSize/pricePlace 유지
# - 시장가 OPEN: reduceOnly="NO"(v2) / "false"(v1)
# - 시장가 CLOSE: reduceOnly="YES"(v2) / "true"(v1) + 40017일 때 1 step 줄여 재시도
# - 포지션: v2(holdSide 1row)와 v1(long/short nested) 모두 공통 포맷으로 반환
# - 상세 로깅으로 원인 추적([ticker] ERROR, [positions] ERROR 등)

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
PRODUCT_TYPE_V2 = "USDT-FUTURES"
# v1 productType (USDT Perp. Linear)
PRODUCT_TYPE_V1 = "umcbl"

MARGIN_COIN = os.getenv("BITGET_MARGIN_COIN", "USDT")
TIMEOUT = (7, 15)

# ────────────────────────────────────────────────────────────
# HTTP + SIGN (v1/v2 동일 사인룰)
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

def _req_get(path: str, params: Optional[Dict[str, Any]] = None, auth: bool = False) -> Dict[str, Any]:
    url = BITGET_HOST + path
    if not auth:
        r = requests.get(url, params=params or {}, timeout=TIMEOUT)
        return r.json()
    q = ""
    if params:
        from urllib.parse import urlencode
        q = "?" + urlencode(params)
    h = _headers("GET", path + q)
    r = requests.get(url, params=params or {}, headers=h, timeout=TIMEOUT)
    return r.json()

def _req_post(path: str, payload: Dict[str, Any], auth: bool = True) -> Dict[str, Any]:
    url = BITGET_HOST + path
    body = json.dumps(payload, separators=(",", ":"))
    if not auth:
        r = requests.post(url, data=body, timeout=TIMEOUT, headers={"Content-Type": "application/json"})
        return r.json()
    h = _headers("POST", path, body)
    r = requests.post(url, data=body, headers=h, timeout=TIMEOUT)
    return r.json()

# v2 helpers
def _get_v2(path: str, params: Optional[Dict[str, Any]] = None, auth: bool = False) -> Dict[str, Any]:
    return _req_get(path, params, auth)

def _post_v2(path: str, payload: Dict[str, Any], auth: bool = True) -> Dict[str, Any]:
    return _req_post(path, payload, auth)

# v1 helpers
def _get_v1(path: str, params: Optional[Dict[str, Any]] = None, auth: bool = False) -> Dict[str, Any]:
    return _req_get(path, params, auth)

def _post_v1(path: str, payload: Dict[str, Any], auth: bool = True) -> Dict[str, Any]:
    return _req_post(path, payload, auth)

# ────────────────────────────────────────────────────────────
# SYMBOL CONVERT
def convert_symbol_v2(s: str) -> str:
    """DOGEUSDT 형태로 정규화 (v2)"""
    s = (s or "").upper().replace(" ", "")
    if s.endswith("_UMCBL") or s.endswith("_DMCBL"):
        s = s.split("_")[0]
    if not s.endswith("USDT"):
        if s.endswith("USD"):
            s += "T"
        elif "USDT" not in s:
            s += "USDT"
    return s

def convert_symbol_v1(s: str) -> str:
    """v1 심볼(DOGEUSDT_UMCBL)로 변환"""
    s2 = convert_symbol_v2(s)
    if not s2.endswith("_UMCBL"):
        s2 = f"{s2}_UMCBL"
    return s2

# 외부에서 공통 사용
def convert_symbol(s: str) -> str:
    return convert_symbol_v2(s)

# ────────────────────────────────────────────────────────────
# CONTRACTS CACHE (v2 → 실패시 v1로 폴백)
_CONTRACTS_LOCK = threading.RLock()
_CONTRACTS: Dict[str, Dict[str, Any]] = {}
_LAST_LOAD = 0.0
_LOAD_INTERVAL = 60.0

def _load_contracts_v2() -> Optional[Dict[str, Dict[str, Any]]]:
    resp = _get_v2("/api/v2/mix/market/contracts", {"productType": PRODUCT_TYPE_V2})
    if str(resp.get("code")) != "00000":
        print("[contracts v2] ERROR:", resp)
        return None
    rows = resp.get("data") or []
    tmp: Dict[str, Dict[str, Any]] = {}
    for it in rows:
        sym = (it.get("symbol") or "").upper()
        if not sym:
            continue
        tmp[sym] = {
            "symbol": sym,
            "pricePlace": int(it.get("pricePlace") or 4),
            "priceEndStep": float(it.get("priceEndStep") or 0.0001),
            "sizePlace": int(it.get("sizePlace") or 3),
            "sizeMultiplier": float(it.get("sizeMultiplier") or 1),
            "sizeStep": float(it.get("minTradeNum") or 0.001),
            "minSize": float(it.get("minTradeNum") or 0.001),
        }
    return tmp

def _load_contracts_v1() -> Optional[Dict[str, Dict[str, Any]]]:
    resp = _get_v1("/api/mix/v1/market/contracts", {"productType": PRODUCT_TYPE_V1})
    if str(resp.get("code")) != "00000":
        print("[contracts v1] ERROR:", resp)
        return None
    rows = resp.get("data") or []
    tmp: Dict[str, Dict[str, Any]] = {}
    for it in rows:
        # v1은 symbol이 DOGEUSDT_UMCBL 형식. v2키로 저장
        sym_v1 = (it.get("symbol") or "").upper()
        if not sym_v1:
            continue
        sym_v2 = convert_symbol_v2(sym_v1)
        tmp[sym_v2] = {
            "symbol": sym_v2,
            "pricePlace": int(it.get("pricePlace") or 4),
            "priceEndStep": float(it.get("priceEndStep") or 0.0001),
            "sizePlace": int(it.get("sizePlace") or 3),
            "sizeMultiplier": float(it.get("sizeMultiplier") or 1),
            "sizeStep": float(it.get("minTradeNum") or 0.001),
            "minSize": float(it.get("minTradeNum") or 0.001),
        }
    return tmp

def _load_contracts(force: bool = False) -> None:
    global _LAST_LOAD, _CONTRACTS
    now = time.time()
    if not force and (now - _LAST_LOAD) < _LOAD_INTERVAL and _CONTRACTS:
        return
    try:
        tmp = _load_contracts_v2()
        if tmp is None or not tmp:
            tmp = _load_contracts_v1()
        if tmp:
            with _CONTRACTS_LOCK:
                _CONTRACTS = tmp
                _LAST_LOAD = now
    except Exception as e:
        print("contract load err:", e)

def _ensure_contract(sym_v2: str) -> Optional[Dict[str, Any]]:
    _load_contracts(force=False)
    s = sym_v2.upper()
    with _CONTRACTS_LOCK:
        if s in _CONTRACTS:
            return _CONTRACTS[s]
    _load_contracts(force=True)
    with _CONTRACTS_LOCK:
        return _CONTRACTS.get(s)

def get_symbol_spec(symbol: str) -> Dict[str, Any]:
    sym = convert_symbol_v2(symbol)
    spec = _ensure_contract(sym)
    if not spec:
        raise ValueError(f"LOCAL_CONTRACT_FAIL: {sym} not found in /contracts(v2,v1)")
    return spec

# ────────────────────────────────────────────────────────────
# TICKER (v2 → v1 폴백)
def get_last_price(symbol: str) -> Optional[float]:
    sym_v2 = convert_symbol_v2(symbol)
    # v2
    try:
        for _ in range(2):
            r2 = _get_v2("/api/v2/mix/market/ticker", {"symbol": sym_v2})
            c2 = str(r2.get("code"))
            if c2 == "00000":
                d = r2.get("data") or {}
                px = d.get("lastPr")
                if px not in (None, "", "null"):
                    px = float(px)
                    if px > 0:
                        return px
                print(f"[ticker v2] MISSING lastPr sym={sym_v2} raw={d}")
            else:
                print(f"[ticker v2] ERROR code={c2} sym={sym_v2} resp={r2}")
            time.sleep(0.15)
    except Exception as e:
        print("ticker v2 err:", e)

    # v1
    try:
        sym_v1 = convert_symbol_v1(sym_v2)
        for _ in range(2):
            r1 = _get_v1("/api/mix/v1/market/ticker", {"symbol": sym_v1})
            c1 = str(r1.get("code"))
            if c1 == "00000":
                d = r1.get("data") or {}
                px = d.get("last") or d.get("close") or d.get("lastPrice")
                if px not in (None, "", "null"):
                    px = float(px)
                    if px > 0:
                        print(f"[ticker] fallback v1 used sym={sym_v1}")
                        return px
                print(f"[ticker v1] MISSING last sym={sym_v1} raw={d}")
            else:
                print(f"[ticker v1] ERROR code={c1} sym={sym_v1} resp={r1}")
            time.sleep(0.15)
    except Exception as e:
        print("ticker v1 err:", e)
    return None

# ────────────────────────────────────────────────────────────
# POSITIONS (v2 → v1 폴백, 공통 포맷: symbol/side/size/entryPrice/leverage)
def _positions_v2(symbol_v2: Optional[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if symbol_v2:
        resp = _get_v2("/api/v2/mix/position/singlePosition",
                       {"symbol": symbol_v2, "productType": PRODUCT_TYPE_V2}, auth=True)
    else:
        resp = _get_v2("/api/v2/mix/position/allPosition",
                       {"productType": PRODUCT_TYPE_V2}, auth=True)
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
        resp = _get_v1("/api/mix/v1/position/singlePosition",
                       {"symbol": sym_v1, "productType": PRODUCT_TYPE_V1}, auth=True)
    else:
        resp = _get_v1("/api/mix/v1/position/allPosition",
                       {"productType": PRODUCT_TYPE_V1}, auth=True)
    if str(resp.get("code")) != "00000":
        print("[positions v1] ERROR:", resp)
        return out
    rows = resp.get("data") or []
    for it in rows:
        sym_v1 = it.get("symbol") or ""
        sym = convert_symbol_v2(sym_v1)
        # v1은 long/short 하위객체
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

def get_open_positions(symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    sym_v2 = convert_symbol_v2(symbol) if symbol else None
    try:
        out = _positions_v2(sym_v2)
        if out:
            return out
    except Exception as e:
        print("positions v2 err:", e)
    try:
        out = _positions_v1(sym_v2)
        if out:
            print("[positions] fallback v1 used")
        return out
    except Exception as e:
        print("positions v1 err:", e)
        return []

# ────────────────────────────────────────────────────────────
# SIZE UTILS
def round_down_step(x: float, step: float) -> float:
    if step <= 0:
        return float(x)
    return math.floor(x / step) * step

def _usdt_to_size(usdt: float, price: float, size_step: float, min_size: float) -> float:
    if price <= 0:
        return 0.0
    size = float(usdt) / float(price)
    size = round_down_step(size, size_step)
    if size < min_size:
        return 0.0
    return size

# ────────────────────────────────────────────────────────────
# ORDERS (v2 → v1 폴백)
def place_market_order(symbol: str, usdt_amount: float, side: str, leverage: Optional[float] = None) -> Dict[str, Any]:
    """시장가 OPEN (reduceOnly NO)."""
    try:
        sym_v2 = convert_symbol_v2(symbol)
        spec = get_symbol_spec(sym_v2)
    except Exception as e:
        return {"code": "LOCAL_CONTRACT_FAIL", "msg": str(e)}

    price = get_last_price(sym_v2)
    if price is None or price <= 0:
        return {"code": "LOCAL_TICKER_FAIL", "msg": "ticker_none"}

    size = _usdt_to_size(float(usdt_amount or 0), price, float(spec["sizeStep"]), float(spec["minSize"]))
    if size <= 0:
        return {"code": "LOCAL_TICKER_FAIL", "msg": "ticker_none or size<=0"}

    # 방향
    dir_side = "buy" if side.lower().startswith("l") else "sell"

    # v2 시도
    payload_v2 = {
        "symbol": sym_v2,
        "productType": PRODUCT_TYPE_V2,
        "marginCoin": MARGIN_COIN,
        "orderType": "market",
        "side": dir_side,
        "size": str(size),
        "reduceOnly": "NO",     # v2: YES/NO
    }
    if leverage:
        payload_v2["leverage"] = str(leverage)

    try:
        r2 = _post_v2("/api/v2/mix/order/placeOrder", payload_v2, auth=True)
        if str(r2.get("code")) == "00000":
            return r2
        print("[order v2 OPEN] ERROR:", r2)
    except Exception as e:
        print("order v2 open err:", e)

    # v1 폴백
    try:
        sym_v1 = convert_symbol_v1(sym_v2)
        payload_v1 = {
            "symbol": sym_v1,
            "productType": PRODUCT_TYPE_V1,
            "marginCoin": MARGIN_COIN,
            "orderType": "market",
            "side": dir_side,
            "size": str(size),
            "reduceOnly": "false",  # v1: true/false
        }
        if leverage:
            payload_v1["leverage"] = str(leverage)
        r1 = _post_v1("/api/mix/v1/order/placeOrder", payload_v1, auth=True)
        if str(r1.get("code")) != "00000":
            print("[order v1 OPEN] ERROR:", r1)
        else:
            print("[order] fallback v1 used OPEN")
        return r1
    except Exception as e:
        print("order v1 open err:", e)
        return {"code": "LOCAL_ORDER_FAIL", "msg": str(e)}

def place_reduce_by_size(symbol: str, contracts: float, side: str) -> Dict[str, Any]:
    """시장가 reduceOnly CLOSE. 보유수량 초과 clamp + 40017 1회 재시도."""
    sym_v2 = convert_symbol_v2(symbol)
    spec = get_symbol_spec(sym_v2)

    # 현재 보유(해당 side) 수량
    held = 0.0
    for p in get_open_positions(sym_v2):
        if convert_symbol_v2(p.get("symbol")) == sym_v2 and (p.get("side") or "") == side:
            held = float(p.get("size") or 0.0)
            break
    if held <= 0:
        return {"code": "LOCAL_NO_POS", "msg": "no_position"}

    step = float(spec["sizeStep"])
    qty = round_down_step(float(contracts or 0), step)
    if qty <= 0:
        return {"code": "LOCAL_BAD_QTY", "msg": "qty<=0"}

    max_qty = round_down_step(held, step)
    if qty > max_qty:
        qty = max_qty
    if qty <= 0:
        return {"code": "LOCAL_CLAMP_ZERO", "msg": "clamped_to_zero"}

    # reduceOnly는 반대 방향
    dir_side = "sell" if side == "long" else "buy"

    # v2 우선
    payload_v2 = {
        "symbol": sym_v2,
        "productType": PRODUCT_TYPE_V2,
        "marginCoin": MARGIN_COIN,
        "orderType": "market",
        "side": dir_side,
        "size": str(qty),
        "reduceOnly": "YES",
    }
    try:
        r2 = _post_v2("/api/v2/mix/order/placeOrder", payload_v2, auth=True)
        c2 = str(r2.get("code"))
        if c2 == "00000":
            return r2
        if c2 == "40017":
            retry_qty = round_down_step(qty - step, step)
            if retry_qty > 0:
                payload_v2["size"] = str(retry_qty)
                r2b = _post_v2("/api/v2/mix/order/placeOrder", payload_v2, auth=True)
                return {"first": r2, "retry": r2b}
        print("[order v2 CLOSE] ERROR:", r2)
    except Exception as e:
        print("order v2 close err:", e)

    # v1 폴백
    try:
        sym_v1 = convert_symbol_v1(sym_v2)
        payload_v1 = {
            "symbol": sym_v1,
            "productType": PRODUCT_TYPE_V1,
            "marginCoin": MARGIN_COIN,
            "orderType": "market",
            "side": dir_side,
            "size": str(qty),
            "reduceOnly": "true",
        }
        r1 = _post_v1("/api/mix/v1/order/placeOrder", payload_v1, auth=True)
        c1 = str(r1.get("code"))
        if c1 != "00000":
            if c1 == "40017":
                retry_qty = round_down_step(qty - step, step)
                if retry_qty > 0:
                    payload_v1["size"] = str(retry_qty)
                    r1b = _post_v1("/api/mix/v1/order/placeOrder", payload_v1, auth=True)
                    print("[order] fallback v1 used CLOSE with retry")
                    return {"first": r1, "retry": r1b}
            print("[order v1 CLOSE] ERROR:", r1)
        else:
            print("[order] fallback v1 used CLOSE")
        return r1
    except Exception as e:
        print("order v1 close err:", e)
        return {"code": "LOCAL_ORDER_FAIL", "msg": str(e)}

# ────────────────────────────────────────────────────────────
# 공개 (디버그용)
def _get(path: str, params: Optional[Dict[str, Any]] = None, auth: bool = False) -> Dict[str, Any]:
    """server의 /debug에서 사용하기 위한 공개 함수(v2 기본)."""
    return _get_v2(path, params, auth)

PRODUCT_TYPE = PRODUCT_TYPE_V2

__all__ = [
    "PRODUCT_TYPE",
    "convert_symbol",
    "get_symbol_spec",
    "get_last_price",
    "get_open_positions",
    "place_market_order",
    "place_reduce_by_size",
    "_get",
]
