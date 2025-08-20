# bitget_api.py – Bitget USDT‑M Perp (UMCBL) 안정 클라이언트
import os, time, json, hmac, hashlib, base64, requests, math, threading
from typing import Dict, List, Optional

BASE_URL = os.getenv("BITGET_BASE_URL", "https://api.bitget.com")
API_KEY        = os.getenv("BITGET_API_KEY", "")
API_SECRET     = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE = os.getenv("BITGET_API_PASSWORD", "")

PRODUCT_TYPE = "umcbl"   # USDT-M perpetual
MARGIN_COIN  = os.getenv("MARGIN_COIN", "USDT")

# ── 간단 레이트리미트 ─────────────────────────────────────────
_last_call: Dict[str, float] = {}
def _rl(key: str, min_interval: float = 0.08):
    now = time.time()
    prev = _last_call.get(key, 0.0)
    wait = min_interval - (now - prev)
    if wait > 0:
        time.sleep(wait)
    _last_call[key] = time.time()

def _ts() -> str:
    return str(int(time.time() * 1000))

def _sign(ts: str, method: str, path_with_query: str, body: str = "") -> str:
    prehash = ts + method.upper() + path_with_query + body
    digest  = hmac.new(API_SECRET.encode(), prehash.encode(), hashlib.sha256).digest()
    return base64.b64encode(digest).decode()

def _headers(method: str, path_with_query: str, body: str = "") -> Dict[str, str]:
    ts = _ts()
    return {
        "ACCESS-KEY": API_KEY,
        "ACCESS-SIGN": _sign(ts, method, path_with_query, body),
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": API_PASSPHRASE,
        "Content-Type": "application/json",
        "locale": "en-US",
    }

def _req(method: str, path: str, params: Optional[Dict] = None, body: Optional[Dict] = None, auth: bool = False):
    params = params or {}
    body   = body or {}
    if method.upper() == "GET":
        q = "&".join([f"{k}={v}" for k, v in params.items()]) if params else ""
        path_with_query = path + (("?" + q) if q else "")
        headers = _headers(method, path_with_query, "") if auth else {"Content-Type":"application/json"}
        _rl(path, 0.08)
        r = requests.get(BASE_URL + path_with_query, headers=headers, timeout=10)
    else:
        q = "&".join([f"{k}={v}" for k, v in params.items()]) if params else ""
        path_with_query = path + (("?" + q) if q else "")
        payload = json.dumps(body) if body else ""
        headers = _headers(method, path_with_query, payload) if auth else {"Content-Type":"application/json"}
        _rl(path, 0.08)
        r = requests.post(BASE_URL + path_with_query, data=payload, headers=headers, timeout=10)
    r.raise_for_status()
    return r.json()

# ── 심볼 정규화 ────────────────────────────────────────────────
ALIASES: Dict[str, str] = {}

def convert_symbol(sym: str) -> str:
    s = (sym or "").upper().strip()
    if ":" in s:  # 예: BINANCE:IMXUSDT.P
        s = s.split(":", 1)[1]
    s = s.replace("/", "").replace("-", "").replace("_", "")
    if s.endswith(".P"):
        s = s[:-2]
    if s.endswith("PERP"):
        s = s[:-4]
    return ALIASES.get(s, s)

def _mix_symbol(sym: str) -> str:
    return f"{convert_symbol(sym)}_UMCBL"

# ── Ticker / Depth(mid) 캐시 ──────────────────────────────────
_TICKER_CACHE: Dict[str, tuple] = {}  # sym -> (ts, price)
TICKER_TTL    = float(os.getenv("TICKER_TTL", "1.2"))
STRICT_TICKER = os.getenv("STRICT_TICKER", "0") == "1"

def _depth_midprice(sym: str) -> Optional[float]:
    try:
        r = _req("GET", "/api/mix/v1/market/depth", {"symbol": _mix_symbol(sym), "limit": 5})
        if r.get("msg") == "success":
            asks = r["data"].get("asks") or []
            bids = r["data"].get("bids") or []
            if asks and bids:
                return (float(asks[0][0]) + float(bids[0][0])) / 2.0
    except Exception:
        pass
    return None

def get_last_price(sym: str) -> Optional[float]:
    sym = convert_symbol(sym)
    c = _TICKER_CACHE.get(sym)
    if c and (time.time() - c[0] <= TICKER_TTL):
        return float(c[1])

    for i in range(2):
        try:
            r = _req("GET", "/api/mix/v1/market/ticker", {"symbol": _mix_symbol(sym)})
            if r.get("msg") == "success" and r.get("data"):
                px = float(r["data"]["last"])
                if px > 0:
                    _TICKER_CACHE[sym] = (time.time(), px)
                    return px
        except Exception:
            time.sleep(0.2 * (i + 1))

    alt = _depth_midprice(sym)
    if alt and alt > 0:
        _TICKER_CACHE[sym] = (time.time(), alt)
        return alt

    if not STRICT_TICKER and c:
        return float(c[1])
    return None

# ── 컨트랙트 스펙 ─────────────────────────────────────────────
_SPEC_CACHE: Dict[str, Dict] = {}
_SPEC_LOCK = threading.Lock()
_SPEC_TS   = 0.0
SPEC_TTL   = 60.0

def _refresh_specs():
    global _SPEC_TS
    try:
        r = _req("GET", "/api/mix/v1/market/contracts", {"productType": PRODUCT_TYPE})
        if r.get("msg") == "success":
            data = r.get("data") or []
            with _SPEC_LOCK:
                _SPEC_CACHE.clear()
                for it in data:
                    if it.get("symbol","").endswith("_UMCBL"):
                        sym = it["symbol"].replace("_UMCBL","")
                        _SPEC_CACHE[sym] = {
                            "symbol": sym,
                            "sizeStep": float(it.get("lotSize") or it.get("minTradeNum") or 0.001),
                            "priceStep": float(it.get("priceEndStep") or it.get("minPricePrecision") or 0.01),
                            "minSize": float(it.get("minTradeNum") or 0.001),
                        }
                _SPEC_TS = time.time()
    except Exception:
        pass

def get_symbol_spec(sym: str) -> Dict:
    sym = convert_symbol(sym)
    global _SPEC_TS
    if time.time() - _SPEC_TS > SPEC_TTL or sym not in _SPEC_CACHE:
        _refresh_specs()
    with _SPEC_LOCK:
        return _SPEC_CACHE.get(sym, {"symbol": sym, "sizeStep": 0.001, "priceStep": 0.01, "minSize": 0.001})

def round_down_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return math.floor(float(value) / step) * step

# ── 포지션 캐시 (멈춤 방지) ───────────────────────────────────
_POS_CACHE = {"data": [], "ts": 0.0, "cooldown_until": 0.0}
POS_FAIL_COOLDOWN_SEC = float(os.getenv("POS_FAIL_COOLDOWN_SEC", "6"))
POS_MAX_STALE_SEC     = float(os.getenv("POS_MAX_STALE_SEC", "20"))

def _fetch_positions() -> List[Dict]:
    try:
        r = _req("GET", "/api/mix/v1/position/allPosition", {"productType": PRODUCT_TYPE}, auth=True)
        if r.get("msg") == "success":
            arr = []
            for it in r.get("data") or []:
                size = float(it.get("total","0"))
                if size <= 0:
                    continue
                arr.append({
                    "symbol": it["symbol"].replace("_UMCBL",""),
                    "side": "long" if it.get("holdSide","") == "long" else "short",
                    "size": size,
                    "entry_price": float(it.get("averageOpenPrice") or 0),
                })
            return arr
    except Exception:
        return []
    return []

def get_open_positions() -> List[Dict]:
    now = time.time()
    if now < _POS_CACHE["cooldown_until"] and _POS_CACHE["data"]:
        if now - _POS_CACHE["ts"] > POS_MAX_STALE_SEC:
            return []
        return _POS_CACHE["data"]

    res = _fetch_positions()
    if res:
        _POS_CACHE["data"] = res
        _POS_CACHE["ts"] = now
        _POS_CACHE["cooldown_until"] = 0.0
        return res

    if _POS_CACHE["data"]:
        _POS_CACHE["cooldown_until"] = now + POS_FAIL_COOLDOWN_SEC
        if now - _POS_CACHE["ts"] > POS_MAX_STALE_SEC:
            return []
    return _POS_CACHE["data"]

# ── 주문 ──────────────────────────────────────────────────────
def _calc_size_from_notional(symbol: str, usdt: float, price: float) -> float:
    spec = get_symbol_spec(symbol)
    step = float(spec.get("sizeStep", 0.001))
    size = usdt / max(price, 1e-9)
    return max(step, round_down_step(size, step))

def place_market_order(symbol: str, usdt_amount: float, side: str = "buy", leverage: float = 5.0, reduce_only: bool=False) -> Dict:
    symbol = convert_symbol(symbol)
    mix = _mix_symbol(symbol)
    price = get_last_price(symbol)
    if not price:
        return {"code":"TICKER_FAIL","msg":"ticker fail"}
    size = _calc_size_from_notional(symbol, usdt_amount, price)

    if reduce_only:
        side_map = {"buy":"close_short", "sell":"close_long"}
    else:
        side_map = {"buy":"open_long", "sell":"open_short"}

    body = {
        "symbol": mix,
        "marginCoin": MARGIN_COIN,
        "size": str(size),
        "side": side_map.get(side.lower(), "open_long"),
        "orderType": "market",
        "reduceOnly": reduce_only,
        "presetTakeProfitPrice": "",
        "presetStopLossPrice": "",
    }
    try:
        r = _req("POST", "/api/mix/v1/order/placeOrder", body=body, auth=True)
        return {"code": r.get("code",""), "data": r.get("data"), "msg": r.get("msg","")}
    except Exception as e:
        return {"code":"HTTP_ERR","msg":str(e)}

def place_reduce_by_size(symbol: str, size: float, side: str = "long") -> Dict:
    symbol = convert_symbol(symbol)
    mix = _mix_symbol(symbol)
    step = float(get_symbol_spec(symbol).get("sizeStep", 0.001))
    qty = round_down_step(size, step)
    if qty <= 0:
        return {"code":"LOCAL_MIN_QTY"}

    side_tag = "close_long" if (side or "long").lower() == "long" else "close_short"

    body = {
        "symbol": mix,
        "marginCoin": MARGIN_COIN,
        "size": str(qty),
        "side": side_tag,
        "orderType": "market",
        "reduceOnly": True,
    }
    try:
        r = _req("POST", "/api/mix/v1/order/placeOrder", body=body, auth=True)
        return {"code": r.get("code",""), "data": r.get("data"), "msg": r.get("msg","")}
    except Exception as e:
        return {"code":"HTTP_ERR","msg":str(e)}
