# -*- coding: utf-8 -*-
"""
Bitget REST API helper (USDT-M Perpetual)

외부에서 사용하는 함수(트레이더와 호환):
  - convert_symbol(symbol) -> str
  - get_last_price(symbol) -> Optional[float]
  - get_open_positions() -> List[Dict]
  - place_market_order(symbol, usdt_amount, side, leverage, reduce_only=False) -> Dict
  - place_reduce_by_size(symbol, size, side) -> Dict
  - get_symbol_spec(symbol) -> Dict
  - round_down_step(value, step) -> float

필요 ENV (필수 ★ / 권장 ◇):
★ BITGET_API_KEY
★ BITGET_API_SECRET
★ BITGET_API_PASSPHRASE
◇ BITGET_USE_V2=1 → v2 엔드포인트 사용 (권장)
◇ BITGET_PRODUCT_TYPE=USDT-FUTURES  ← v2용 productType(공식 가이드)
"""

import os, time, hmac, hashlib, json, math, random
from typing import Any, Dict, Optional, Tuple, List
from urllib.parse import urlencode

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# ─────────────────────────────────────────────────────────
# ENV
# ─────────────────────────────────────────────────────────
BASE_URL  = os.getenv("BITGET_BASE_URL", "https://api.bitget.com")

API_KEY   = os.getenv("BITGET_API_KEY", "")
API_SEC   = os.getenv("BITGET_API_SECRET", "")
API_PASS  = os.getenv("BITGET_API_PASSPHRASE", "")

USE_V2    = os.getenv("BITGET_USE_V2", "1") == "1"
PRODUCT_TYPE = os.getenv("BITGET_PRODUCT_TYPE", "USDT-FUTURES")  # v2 권장값

SESSION = requests.Session()
RETRY   = Retry(total=3, backoff_factor=0.3, status_forcelist=(429, 500, 502, 503, 504))
ADAPTER = HTTPAdapter(max_retries=RETRY, pool_connections=50, pool_maxsize=50)
SESSION.mount("https://", ADAPTER)
SESSION.mount("http://", ADAPTER)
SESSION.headers.update({"Content-Type": "application/json"})

def _ts_ms() -> str:
    return str(int(time.time() * 1000))

def _sign_v2(ts: str, method: str, path: str, query: str, body: str) -> str:
    msg = f"{ts}{method.upper()}{path}{query}{body}"
    return hmac.new(API_SEC.encode(), msg.encode(), hashlib.sha256).hexdigest()

def _auth_headers_v2(ts: str, sign: str) -> Dict[str, str]:
    return {
        "ACCESS-KEY": API_KEY,
        "ACCESS-SIGN": sign,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": API_PASS,
        "Content-Type": "application/json",
    }

def convert_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if s.endswith("_UMCBL") or s.endswith("_UMCBL"):
        s = s.replace("_UMCBL", "").replace("_UMCBL", "")
    return s

def round_down_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return math.floor(value / step) * step

def _request_json(method: str, path: str, params: Optional[Dict]=None, body: Optional[Dict]=None) -> Dict:
    url = BASE_URL + path
    params = params or {}
    body   = body or {}
    q = ""
    if params:
        q = "?" + urlencode(params)
    ts = _ts_ms()
    payload = json.dumps(body, separators=(",", ":")) if body else ""
    sign = _sign_v2(ts, method, path, q, payload)
    headers = _auth_headers_v2(ts, sign)
    resp = SESSION.request(method.upper(), url + q, data=payload, headers=headers, timeout=10)
    if resp.status_code >= 400:
        return {"ok": False, "status": resp.status_code, "url": url+q, "body": resp.text}
    try:
        j = resp.json()
    except Exception:
        return {"ok": False, "status": resp.status_code, "url": url+q, "body": resp.text}
    return {"ok": True, "json": j, "url": url+q}

# ─────────────────────────────────────────────────────────
# 시세/포지션
# ─────────────────────────────────────────────────────────
def get_last_price(symbol: str) -> Optional[float]:
    s = convert_symbol(symbol)
    # v2 ticker
    r = _request_json("GET", "/api/v2/mix/market/ticker", params={"symbol": f"{s}_UMCBL"})
    if not r.get("ok"):
        return None
    try:
        data = r["json"]["data"]
        # 일부 심볼은 last(또는 close)가 null일 수 있음 → bestBid/Ask로 보정
        last = data.get("last") or data.get("close")
        if last in (None, "", "null"):
            bid = data.get("bestBid") or "0"
            ask = data.get("bestAsk") or "0"
            px = (float(bid) + float(ask)) / 2.0 if (bid and ask) else float(bid or ask or 0)
            return px if px > 0 else None
        return float(last)
    except Exception:
        return None

def get_open_positions() -> List[Dict]:
    if USE_V2:
        r = _request_json("GET", "/api/v2/mix/position/all-position", params={"productType": PRODUCT_TYPE})
        if not r.get("ok"):
            return []
        try:
            arr = r["json"]["data"] or []
            out = []
            for row in arr:
                sym = row.get("symbol", "")
                side = row.get("holdSide", "").lower()
                total = float(row.get("total", 0) or row.get("available", 0) or 0)
                aop   = float(row.get("averageOpenPrice", 0) or 0)
                out.append({"symbol": sym.replace("_UMCBL",""), "side": side, "total": total, "averageOpenPrice": aop})
            return out
        except Exception:
            return []
    # v1 fallback
    r = _request_json("GET", "/api/mix/v1/position/allPosition", params={"productType": "umcbl"})
    if not r.get("ok"):
        return []
    try:
        arr = r["json"]["data"] or []
        out = []
        for row in arr:
            sym = row.get("symbol", "")
            side = row.get("holdSide", "").lower()
            total = float(row.get("total", 0) or row.get("available", 0) or 0)
            aop   = float(row.get("averageOpenPrice", 0) or 0)
            out.append({"symbol": sym.replace("_UMCBL",""), "side": side, "total": total, "averageOpenPrice": aop})
        return out
    except Exception:
        return []

# ─────────────────────────────────────────────────────────
# 주문
# ─────────────────────────────────────────────────────────
def get_symbol_spec(symbol: str) -> Dict:
    # 최소 수량/스텝 조회(간단화; 필요 시 캐시)
    s = convert_symbol(symbol)
    r = _request_json("GET", "/api/v2/mix/market/contracts", params={"productType": PRODUCT_TYPE})
    if r.get("ok"):
        for it in (r["json"]["data"] or []):
            if (it.get("symbol","") or "").upper().startswith(s+"_UMCBL"):
                try:
                    return {
                        "sizeStep": float(it.get("sizeTick", 0.001) or 0.001),
                        "priceStep": float(it.get("priceEndStep", 0.01) or 0.01),
                        "minSz": float(it.get("minSz", 0.001) or 0.001),
                    }
                except Exception:
                    pass
    return {"sizeStep": 0.001, "priceStep": 0.01, "minSz": 0.001}

def place_market_order(symbol: str, usdt_amount: float, side: str, leverage: float, reduce_only: bool=False) -> Tuple[bool, Dict]:
    s = convert_symbol(symbol)
    side = side.lower()
    # 수량 계산 — 단순 시장가 추정
    last = get_last_price(s) or 0.0
    if last <= 0:
        return (False, {"err": "no_ticker"})
    spec = get_symbol_spec(s)
    sz = float(usdt_amount) / float(last)
    sz = max(round_down_step(sz, spec.get("sizeStep", 0.001)), spec.get("minSz", 0.001))
    body = {
        "symbol": f"{s}_UMCBL",
        "marginCoin": "USDT",
        "size": str(sz),
        "side": "open_short" if side=="short" else "open_long",
        "orderType": "market",
        "force": "gtc",
        "reduceOnly": reduce_only,
        "leverage": str(int(leverage)),
    }
    r = _request_json("POST", "/api/v2/mix/order/place-order", body=body)
    if not r.get("ok"):
        return (False, r)
    try:
        code = r["json"].get("code")
        if str(code) not in ("00000", "0"):
            return (False, r["json"])
        return (True, r["json"])
    except Exception:
        return (False, r)

def place_reduce_by_size(symbol: str, size: float, side: str) -> Dict:
    s = convert_symbol(symbol)
    side = side.lower()
    body = {
        "symbol": f"{s}_UMCBL",
        "marginCoin": "USDT",
        "size": str(float(size)),
        "side": "close_short" if side=="short" else "close_long",
        "orderType": "market",
        "force": "gtc",
        "reduceOnly": True,
    }
    r = _request_json("POST", "/api/v2/mix/order/place-order", body=body)
    return r

# ─────────────────────────────────────────────────────────
# 기타
# ─────────────────────────────────────────────────────────
