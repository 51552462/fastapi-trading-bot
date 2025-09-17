# -*- coding: utf-8 -*-
"""
Bitget REST API helper (USDT-M Futures, v2 우선)
"""
from __future__ import annotations
import os, time, hmac, hashlib, base64, json, math
from typing import Optional, Dict, Any, List
import requests
from urllib.parse import urlencode

# ── ENV ──────────────────────────────────────────────────────────────────────
BASE_URL       = os.getenv("BITGET_BASE_URL", "https://api.bitget.com")
API_KEY        = os.getenv("BITGET_API_KEY", "")
API_SECRET     = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE = os.getenv("BITGET_API_PASSPHRASE", "")
PRODUCT_TYPE   = os.getenv("BITGET_PRODUCT_TYPE", "USDT-FUTURES")  # v2 권고
MARGIN_COIN    = os.getenv("BITGET_MARGIN_COIN", "USDT")
USE_V2         = os.getenv("BITGET_USE_V2", "1") == "1"

TICKER_TTL     = float(os.getenv("TICKER_TTL", "2.5"))      # ← float
HTTP_TIMEOUT   = float(os.getenv("HTTP_TIMEOUT", "5"))
RETRY_TOTAL    = int(os.getenv("HTTP_RETRY_TOTAL", "3"))
TRACE          = os.getenv("TRACE_LOG", "0") == "1"

# 별칭
SYMBOL_ALIASES: Dict[str, str] = {}
try:
    raw = os.getenv("SYMBOL_ALIASES_JSON", "") or os.getenv("SYMBOL_ALIASES_FILE", "")
    if raw and raw.strip().startswith("{"):
        SYMBOL_ALIASES = json.loads(raw)
    elif raw and os.path.isfile(raw):
        SYMBOL_ALIASES = json.loads(open(raw, "r", encoding="utf-8").read())
except Exception:
    SYMBOL_ALIASES = {}

def _log(*a): 
    if TRACE: print(*a, flush=True)

# ── HTTP ─────────────────────────────────────────────────────────────────────
SESSION = requests.Session()

def _http(method: str, path: str, *, params: dict | None=None, body: dict | None=None, auth=False):
    url = BASE_URL + path
    headers = {"Content-Type": "application/json"}
    query = "?" + urlencode(params, doseq=True) if params else ""
    data  = json.dumps(body, separators=(",", ":")) if body else ""

    if auth:
        ts = str(int(time.time() * 1000))
        prehash = ts + method.upper() + path + query + data
        sign = base64.b64encode(hmac.new(API_SECRET.encode(), prehash.encode(), hashlib.sha256).digest()).decode()
        headers.update({
            "ACCESS-KEY": API_KEY,
            "ACCESS-PASSPHRASE": API_PASSPHRASE,
            "ACCESS-TIMESTAMP": ts,
            "ACCESS-SIGN": sign,
        })

    _log(method, url+query, data if data else "")
    return SESSION.request(method, url+query, data=data or None, headers=headers, timeout=HTTP_TIMEOUT)

def _http_json(method: str, path: str, **kw) -> dict:
    r = _http(method, path, **kw)
    try:
        j = r.json()
    except Exception:
        j = {"code": str(r.status_code), "msg": r.text}
    if r.status_code >= 400 or j.get("code") not in (None, "00000", 0, "0"):
        _log("HTTP ERR", r.status_code, j)
    return j

# ── 유틸 ─────────────────────────────────────────────────────────────────────
def convert_symbol(symbol: str) -> str:
    s = (symbol or "").upper().strip()
    s = SYMBOL_ALIASES.get(s, s)
    if s.endswith("_UMCBL"):  # v2는 접미사 없이
        s = s[:-6]
    return s

def round_down_step(v: float, step: float) -> float:
    if step <= 0: return v
    return math.floor(v / step) * step

# 심볼 스펙 캐시
_SPEC: dict[str, dict] = {}
def get_symbol_spec(symbol: str) -> dict:
    key = convert_symbol(symbol)
    if key in _SPEC: return _SPEC[key]
    js = _http_json("GET", "/api/v2/mix/market/get-all-symbols")
    spec = {}
    for it in (js.get("data") or []):
        if (it.get("symbol") or "").upper() == key:
            spec = {
                "symbol": key,
                "minSz": float(it.get("minSz", "0.001") or 0.001),
                "sizePlace": int(it.get("sizePlace", "4") or 4),
            }
            break
    _SPEC[key] = spec or {"symbol": key, "minSz": 0.001, "sizePlace": 4}
    return _SPEC[key]

# ── 시세: v2 ticker → mark → candles ─────────────────────────────────────────
_cache: dict[str, tuple[float, float]] = {}
def _cache_get(s: str) -> Optional[float]:
    p = _cache.get(s)
    if not p: return None
    price, exp = p
    return price if time.time() < exp else None

def _cache_put(s: str, price: float):
    _cache[s] = (price, time.time() + TICKER_TTL)

def get_last_price(symbol: str) -> Optional[float]:
    sym = convert_symbol(symbol)
    c = _cache_get(sym)
    if c is not None: return c

    # 1) ticker
    try:
        js = _http_json("GET", "/api/v2/mix/market/ticker", params={"symbol": sym})
        d = js.get("data") or {}
        v = d.get("last") or d.get("close")
        if v is not None:
            price = float(v); _cache_put(sym, price); return price
    except Exception as e: _log("ticker v2 err", e)

    # 2) mark price
    try:
        js = _http_json("GET", "/api/v2/mix/market/mark-price", params={"symbol": sym})
        d = js.get("data") or {}
        v = d.get("markPrice")
        if v is not None:
            price = float(v); _cache_put(sym, price); return price
    except Exception as e: _log("mark err", e)

    # 3) candles(마지막 종가)
    try:
        js = _http_json("GET", "/api/v2/mix/market/candles", params={"symbol": sym, "granularity": "60", "limit": 1})
        arr = js.get("data") or []
        if arr:
            price = float(arr[0][4]); _cache_put(sym, price); return price
    except Exception as e: _log("candles err", e)

    return None

# ── 포지션 ───────────────────────────────────────────────────────────────────
def _parse_pos_v2(js: dict) -> List[Dict[str, Any]]:
    out = []
    for it in (js.get("data") or []):
        try:
            out.append({
                "symbol": (it.get("symbol") or "").upper(),
                "holdSide": (it.get("holdSide") or "").lower(),
                "total": float(it.get("total") or 0),
                "available": float(it.get("available") or 0),
                "averageOpenPrice": float(it.get("averageOpenPrice") or 0),
            })
        except Exception:
            pass
    return out

def get_open_positions() -> List[Dict[str, Any]]:
    try:
        js = _http_json("GET", "/api/v2/mix/position/all-position", params={"productType": PRODUCT_TYPE}, auth=True)
        if js.get("data") is not None:
            return _parse_pos_v2(js)
    except Exception as e:
        _log("positions v2 err", e)
    return []

# ── 주문 ─────────────────────────────────────────────────────────────────────
def place_market_order(symbol: str, usdt_amount: float, side: str, leverage: int, reduce_only: bool=False) -> dict:
    sym = convert_symbol(symbol)
    price = get_last_price(sym)
    if not price or price <= 0:
        return {"code": "ticker_fail", "msg": f"ticker_fail {sym}"}

    spec = get_symbol_spec(sym)
    size_dec = 10 ** -spec.get("sizePlace", 4)
    size = round_down_step(usdt_amount / price * leverage, size_dec)
    side = side.lower()

    body = {
        "symbol": sym,
        "productType": PRODUCT_TYPE,
        "marginCoin": MARGIN_COIN,
        "size": f"{size:.{spec.get('sizePlace',4)}f}",
        "price": "",
        "side": "buy" if side == "long" else "sell",
        "orderType": "market",
        "force": "gtc",
        "reduceOnly": reduce_only,
        "leverage": str(leverage),
        "clientOid": f"mkt-{int(time.time()*1000)}",
    }
    return _http_json("POST", "/api/v2/mix/order/place-order", body=body, auth=True)

def place_reduce_by_size(symbol: str, size: float, side: str) -> dict:
    sym = convert_symbol(symbol)
    spec = get_symbol_spec(sym)
    size_dec = 10 ** -spec.get("sizePlace", 4)
    size = round_down_step(float(size), size_dec)
    body = {
        "symbol": sym,
        "productType": PRODUCT_TYPE,
        "marginCoin": MARGIN_COIN,
        "size": f"{size:.{spec.get('sizePlace',4)}f}",
        "price": "",
        "side": "sell" if side.lower()=="long" else "buy",
        "orderType": "market",
        "force": "gtc",
        "reduceOnly": True,
        "clientOid": f"red-{int(time.time()*1000)}",
    }
    return _http_json("POST", "/api/v2/mix/order/place-order", body=body, auth=True)
