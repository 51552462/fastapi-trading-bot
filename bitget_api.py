# bitget_api.py — Bitget REST 래퍼 (long/short 정확 매핑, usdt_amount→qty 변환은 여기서 1회만)
import os, time, hmac, json, hashlib
from typing import Dict, Any, Optional

import requests

BASE_URL = os.getenv("BITGET_BASE_URL", "https://api.bitget.com")

API_KEY    = os.getenv("BITGET_API_KEY", "")
API_SECRET = os.getenv("BITGET_API_SECRET", "")
API_PASS   = os.getenv("BITGET_API_PASSWORD", "")

SESSION = requests.Session()
SESSION.headers.update({"Content-Type": "application/json"})

# ---- 공통 ----
def _ts_ms() -> str:
    return str(int(time.time() * 1000))

def _sign(ts: str, method: str, path: str, body: str) -> str:
    msg = ts + method.upper() + path + (body or "")
    return hmac.new(API_SECRET.encode(), msg.encode(), hashlib.sha256).hexdigest()

def _headers(method: str, path: str, body: Optional[str] = None) -> Dict[str, str]:
    ts = _ts_ms()
    return {
        "ACCESS-KEY": API_KEY,
        "ACCESS-SIGN": _sign(ts, method, path, body or ""),
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": API_PASS,
        "Content-Type": "application/json",
        "X-CHANNEL-API-CODE": "fastapi-bot",
    }

def _mix_symbol(symbol: str) -> str:
    # 내부 심볼(BTCUSDT) → Bitget(MCBTCUSDT)형식은 거래쌍에 따라 다름.
    # 대부분 USDT 무기한은 그대로 통과 가능.
    return symbol

def _rl(key: str, sleep: float = 0.12):
    # 간단한 rate limit
    time.sleep(sleep)

# ---- 메타 ----
_symbol_cache: Dict[str, Dict[str, Any]] = {}

def get_symbol_spec(symbol: str) -> Dict[str, Any]:
    """sizeStep, minQty 등 심볼 정보 조회(캐시)"""
    s = _symbol_cache.get(symbol)
    if s:
        return s
    path = "/api/mix/v1/market/contracts"
    try:
        _rl("meta", 0.2)
        r = SESSION.get(BASE_URL + path, timeout=10)
        if r.status_code != 200:
            return {"sizeStep": 0.001, "minQty": 0.0}
        data = r.json().get("data", [])
        for d in data:
            if (d.get("symbol") or "").upper() == symbol.upper():
                step = float(d.get("sizePlace", 3))
                size_step = 10 ** (-step)
                _symbol_cache[symbol] = {"sizeStep": size_step, "minQty": float(d.get("minTradeNum") or 0)}
                return _symbol_cache[symbol]
    except Exception:
        pass
    return {"sizeStep": 0.001, "minQty": 0.0}

def round_down_step(x: float, step: float) -> float:
    if step <= 0:
        return float(x)
    return (float(x) // step) * step

# ---- 시세 ----
def get_last_price(symbol: str) -> Optional[float]:
    path = f"/api/mix/v1/market/ticker?symbol={_mix_symbol(symbol)}"
    try:
        _rl("ticker", 0.08)
        r = SESSION.get(BASE_URL + path, timeout=8)
        if r.status_code != 200:
            return None
        d = r.json().get("data") or {}
        return float(d.get("last", 0) or 0) or None
    except Exception:
        return None

# ---- 포지션 ----
def get_open_positions():
    """간단 포지션 리스트 반환: [{'symbol':..., 'side': 'long'|'short', 'size': float, 'entryPrice': float}]"""
    path = "/api/mix/v1/position/allPosition"
    body = ""
    try:
        _rl("positions", 0.2)
        r = SESSION.get(BASE_URL + path, headers=_headers("GET", path, body), timeout=12)
        if r.status_code != 200:
            return []
        arr = []
        for p in r.json().get("data", []):
            # Bitget은 long/short 별도로 내려줌
            for leg in ("long", "short"):
                sz = float(p.get(f"{leg}Qty") or 0)
                if sz > 0:
                    arr.append({
                        "symbol": p.get("symbol"),
                        "side": leg,
                        "size": sz,
                        "entryPrice": float(p.get(f"{leg}AvgOpenPrice") or 0),
                    })
        return arr
    except Exception:
        return []

# ---- 주문 ----
def place_market_order(symbol: str, usdt_amount: float, side: str,
                       leverage: float = 5, reduce_only: bool = False) -> Dict:
    """usdt_amount(명목가)를 단 한 번만 가격으로 나눠 qty 계산 → 시장가 주문.
       side: long|short|buy|sell 모두 허용."""
    last = get_last_price(symbol)
    if not last:
        return {"code": "LOCAL_TICKER_FAIL", "msg": "ticker_none"}

    spec = get_symbol_spec(symbol)
    qty = round_down_step(float(usdt_amount) / float(last), float(spec.get("sizeStep", 0.001)))
    if qty <= 0:
        return {"code": "LOCAL_BAD_QTY", "msg": f"qty {qty}"}
    if qty < float(spec.get("minQty", 0.0)):
        need = float(spec.get("minQty")) * last
        return {"code": "LOCAL_MIN_QTY", "msg": f"need≈{need:.6f}USDT", "qty": qty}

    s = (side or "").strip().lower()
    if s in ("long", "buy"):
        api_side = "buy_single"
    elif s in ("short", "sell"):
        api_side = "sell_single"
    else:
        return {"code": "LOCAL_BAD_SIDE", "msg": f"side={side}"}

    path = "/api/mix/v1/order/placeOrder"
    body = {
        "symbol":     _mix_symbol(symbol),
        "marginCoin": "USDT",
        "size":       str(qty),
        "side":       api_side,
        "orderType":  "market",
        "leverage":   str(leverage),
        "reduceOnly": bool(reduce_only),
        "clientOid":  f"cli-{int(time.time()*1000)}"
    }
    bj = json.dumps(body)
    try:
        _rl("order", 0.12)
        res = SESSION.post(BASE_URL + path, headers=_headers("POST", path, bj), data=bj, timeout=15)
        if res.status_code != 200:
            return {"code": f"HTTP_{res.status_code}", "msg": res.text}
        return res.json()
    except Exception as e:
        return {"code": "LOCAL_EXCEPTION", "msg": str(e)}

def place_reduce_by_size(symbol: str, size: float, side: str) -> Dict:
    """보유수량만큼 시장가 감소(부분청산)"""
    # held size 초과 주문 방지
    held = 0.0
    for p in get_open_positions():
        if p.get("symbol") == symbol and (p.get("side") or "").lower() == side:
            held = float(p.get("size") or 0.0)
            break
    size = min(max(0.0, float(size)), held)
    if size <= 0:
        return {"code": "LOCAL_ZERO", "msg": "no held"}

    s = (side or "").strip().lower()
    api_side = "close_long" if s == "long" else "close_short"

    path = "/api/mix/v1/order/placeOrder"
    body = {
        "symbol":     _mix_symbol(symbol),
        "marginCoin": "USDT",
        "size":       str(size),
        "side":       api_side,
        "orderType":  "market",
        "reduceOnly": True,
        "clientOid":  f"cli-close-{int(time.time()*1000)}"
    }
    bj = json.dumps(body)
    try:
        _rl("order", 0.12)
        res = SESSION.post(BASE_URL + path, headers=_headers("POST", path, bj), data=bj, timeout=15)
        if res.status_code != 200:
            return {"code": f"HTTP_{res.status_code}", "msg": res.text}
        return res.json()
    except Exception as e:
        return {"code": "LOCAL_EXCEPTION", "msg": str(e)}

# 편의
def convert_symbol(sym: str) -> str:
    return (sym or "").replace(":", "").upper()
