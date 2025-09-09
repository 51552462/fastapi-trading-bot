# bitget_api.py  — base: your old file, + mark-price fallback only
import os, time, json, hmac, hashlib, base64, requests, math, random
from typing import Dict, List, Optional

BASE_URL = os.getenv("BITGET_BASE_URL", "https://api.bitget.com")

API_KEY        = os.getenv("BITGET_API_KEY", "")
API_SECRET     = os.getenv("BITGET_API_SECRET", "")
# NOTE: 네 예전 코드 네이밍을 유지합니다.
API_PASSPHRASE = os.getenv("BITGET_API_PASSWORD", "")  # ← Render 환경변수도 이 이름으로

# ── Simple rate-limit guard ───────────────────────────────────
_last_call = {}
def _rl(key: str, min_interval: float = 0.08):
    now = time.time()
    prev = _last_call.get(key, 0.0)
    wait = min_interval - (now - prev)
    if wait > 0:
        time.sleep(wait)
    _last_call[key] = time.time()

# ── Auth (HMAC-SHA256 → base64) ───────────────────────────────
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

# ── Optional symbol aliases (TV ↔ Bitget) ─────────────────────
ALIASES: Dict[str, str] = {}
_alias_env = os.getenv("SYMBOL_ALIASES_JSON", "")
if _alias_env:
    try:
        ALIASES.update(json.loads(_alias_env))
    except Exception:
        pass

def convert_symbol(sym: str) -> str:
    s = (sym or "").upper().replace("/", "").replace("-", "").replace("_", "")
    if s.endswith("PERP"):
        s = s[:-4]
    return ALIASES.get(s, s)

def _mix_symbol(sym: str) -> str:
    return f"{convert_symbol(sym)}_UMCBL"

# ── Ticker cache & fallbacks ──────────────────────────────────
_TICKER_CACHE: Dict[str, tuple] = {}  # { "BTCUSDT": (ts, price) }
TICKER_TTL     = float(os.getenv("TICKER_TTL", "2.5"))      # seconds
STRICT_TICKER  = os.getenv("STRICT_TICKER", "0") == "1"     # 1 → 캐시 폴백 금지
USE_MARK_PRICE = os.getenv("BITGET_TICKER_USE_MARK", "1") == "1"  # mark-price 폴백 ON

def _depth_midprice(symbol: str) -> Optional[float]:
    """ticker 비거나 에러일 때 오더북 최우선 호가로 미드프라이스 산출."""
    path = "/api/mix/v1/market/depth"
    q = f"symbol={_mix_symbol(symbol)}&limit=5"
    try:
        _rl("depth", 0.08)
        r = requests.get(f"{BASE_URL}{path}?{q}", timeout=10)
        j = r.json()
        d = j.get("data") or {}
        asks = d.get("asks") or d.get("ask") or []
        bids = d.get("bids") or d.get("bid") or []
        if asks and bids:
            best_ask = float(asks[0][0])
            best_bid = float(bids[0][0])
            if best_ask > 0 and best_bid > 0:
                return (best_ask + best_bid) / 2.0
    except Exception:
        pass
    return None

def _mark_price(symbol: str) -> Optional[float]:
    """Bitget mark price (v1). last가 비거나 0일 때 폴백."""
    path = "/api/mix/v1/market/mark-price"
    q = f"symbol={_mix_symbol(symbol)}"
    try:
        _rl("mark", 0.08)
        r = requests.get(f"{BASE_URL}{path}?{q}", timeout=10)
        j = r.json()
        d = j.get("data") or {}
        v = d.get("markPrice") or d.get("price")
        if v is not None:
            v = float(v)
            if v > 0:
                return v
    except Exception:
        pass
    return None

def get_last_price(symbol: str, retries: int = 6, sleep_base: float = 0.20) -> Optional[float]:
    """USDT-M 선물(UMCBL) last 가격.
       순서: 캐시 → v1 ticker → (오더북 mid) → (mark price) → 캐시 폴백(STRICT_TICKER=0일 때)"""
    sym = convert_symbol(symbol)

    # 1) 캐시 히트
    c = _TICKER_CACHE.get(sym)
    now = time.time()
    if c and now - c[0] <= TICKER_TTL:
        return float(c[1])

    url = f"{BASE_URL}/api/mix/v1/market/ticker?symbol={_mix_symbol(sym)}"

    for i in range(retries):
        try:
            _rl("ticker", 0.06)
            r = requests.get(url, timeout=10)
            if r.status_code != 200:
                time.sleep(sleep_base * (2 ** i) + random.uniform(0, 0.1))
                continue
            j = r.json()
            data = j.get("data")
            if data and data.get("last") is not None:
                px = float(data["last"])
                if px > 0:
                    _TICKER_CACHE[sym] = (time.time(), px)
                    return px
            # 1차 폴백: 오더북 미드프라이스
            alt = _depth_midprice(sym)
            if alt and alt > 0:
                _TICKER_CACHE[sym] = (time.time(), alt)
                return alt
            # 2차 폴백: mark price
            if USE_MARK_PRICE:
                mp = _mark_price(sym)
                if mp and mp > 0:
                    _TICKER_CACHE[sym] = (time.time(), mp)
                    return mp
        except Exception:
            pass
        time.sleep(sleep_base * (2 ** i) + random.uniform(0, 0.1))

    # 2) 최종 실패 → 캐시 폴백(엄격모드가 아니면)
    if not STRICT_TICKER:
        c = _TICKER_CACHE.get(sym)
        if c:
            return float(c[1])

    print(f"❌ Ticker 실패(최종): {_mix_symbol(sym)}")
    return None

# ── Symbol spec cache (sizeStep / minQty) ─────────────────────
_SYMBOLS_CACHE = {"ts": 0.0, "data": {}}

def _refresh_symbols_cache():
    path = "/api/mix/v1/public/symbols"
    q    = "productType=umcbl"
    try:
        _rl("symbols", 0.15)
        r = requests.get(f"{BASE_URL}{path}?{q}", headers=_headers("GET", f"{path}?{q}", ""), timeout=12)
        j = r.json()
        arr = j.get("data") or []
        m = {}
        for it in arr:
            sym_full = (it.get("symbol") or "")
            if not sym_full.endswith("_UMCBL"):
                continue
            sym_core   = sym_full.replace("_UMCBL", "")
            size_scale = int(it.get("sizeScale") or 0)
            size_step  = 10 ** (-size_scale) if size_scale >= 0 else 0.001
            min_qty    = float(it.get("minTradeNum") or it.get("minOrderSize") or 0.0)
            m[sym_core] = {"sizeStep": size_step, "minQty": min_qty}
        _SYMBOLS_CACHE["data"] = m
        _SYMBOLS_CACHE["ts"]   = time.time()
    except Exception as e:
        print("❌ 심볼 캐시 갱신 실패:", e)

def get_symbol_spec(symbol: str) -> Dict[str, float]:
    now = time.time()
    if now - _SYMBOLS_CACHE["ts"] > 600 or not _SYMBOLS_CACHE["data"]:
        _refresh_symbols_cache()
    sym  = convert_symbol(symbol)
    spec = _SYMBOLS_CACHE["data"].get(sym)
    if not spec:
        spec = {"sizeStep": 0.001, "minQty": 0.001}
        _SYMBOLS_CACHE["data"][sym] = spec
    return spec

def round_down_step(qty: float, step: float) -> float:
    if step <= 0:
        return round(qty, 6)
    k = math.floor(qty / step)
    return round(k * step, 6)

# ── Orders ────────────────────────────────────────────────────
def place_market_order(symbol: str, usdt_amount: float, side: str, leverage: float = 5, reduce_only: bool = False) -> Dict:
    """usdt_amount → 시장가 수량 환산 후 주문. side='buy'|'sell' (oneway: buy_single/sell_single)"""
    last = get_last_price(symbol)
    if not last:
        return {"code": "LOCAL_TICKER_FAIL", "msg": "ticker_none"}

    spec = get_symbol_spec(symbol)
    qty  = round_down_step(usdt_amount / last, float(spec.get("sizeStep", 0.001)))
    if qty <= 0:
        return {"code": "LOCAL_BAD_QTY", "msg": f"qty {qty}"}
    if qty < float(spec.get("minQty", 0.0)):
        need = float(spec.get("minQty")) * last
        return {"code": "LOCAL_MIN_QTY", "msg": f"need≈{need:.6f}USDT", "qty": qty}

    path = "/api/mix/v1/order/placeOrder"
    body = {
        "symbol":     _mix_symbol(symbol),
        "marginCoin": "USDT",
        "size":       str(qty),
        "side":       "buy_single" if side == "buy" else "sell_single",
        "orderType":  "market",
        "leverage":   str(leverage),
        "reduceOnly": bool(reduce_only),
        "clientOid":  f"cli-{int(time.time()*1000)}"
    }
    bj = json.dumps(body)
    try:
        _rl("order", 0.12)
        res = requests.post(BASE_URL + path, headers=_headers("POST", path, bj), data=bj, timeout=15)
        if res.status_code != 200:
            print("❌ order HTTP", res.status_code, res.text[:200])
            return {"code": f"HTTP_{res.status_code}", "msg": res.text}
        return res.json()
    except Exception as e:
        print("❌ order EXC", str(e))
        return {"code": "LOCAL_EXCEPTION", "msg": str(e)}

def place_reduce_by_size(symbol: str, size: float, side: str) -> Dict:
    """size 계약 수량을 reduceOnly 시장가로 청산. side='long'→sell_single, 'short'→buy_single"""
    size = float(size)
    if size <= 0:
        return {"code": "LOCAL_BAD_QTY", "msg": "size<=0"}

    step = float(get_symbol_spec(symbol).get("sizeStep", 0.001))
    size = round_down_step(size, step)
    if size <= 0:
        return {"code": "LOCAL_STEP_ZERO", "msg": "after_step=0"}

    path = "/api/mix/v1/order/placeOrder"
    body = {
        "symbol":     _mix_symbol(symbol),
        "marginCoin": "USDT",
        "size":       str(size),
        "side":       "sell_single" if side.lower() == "long" else "buy_single",
        "orderType":  "market",
        "reduceOnly": True,
        "clientOid":  f"cli-red-{int(time.time()*1000)}"
    }
    bj = json.dumps(body)
    try:
        _rl("order", 0.12)
        res = requests.post(BASE_URL + path, headers=_headers("POST", path, bj), data=bj, timeout=15)
        if res.status_code != 200:
            print("❌ reduce HTTP", res.status_code, res.text[:200])
            return {"code": f"HTTP_{res.status_code}", "msg": res.text}
        return res.json()
    except Exception as e:
        print("❌ reduce EXC", str(e))
        return {"code": "LOCAL_EXCEPTION", "msg": str(e)}

# ── Positions ────────────────────────────────────────────────
_POS_CACHE = {"data": [], "ts": 0.0, "cooldown_until": 0.0}

def _ffloat(x):
    try: return float(x)
    except: return 0.0

def _fetch_positions() -> List[Dict]:
    path = "/api/mix/v1/position/allPosition"
    q    = "productType=umcbl"
    try:
        _rl("positions", 0.10)
        res = requests.get(f"{BASE_URL}{path}?{q}", headers=_headers("GET", f"{path}?{q}", ""), timeout=12)
        j = res.json()
    except Exception as e:
        print("❌ position fetch 예외:", e)
        return []

    if not j or j.get("code") not in ("00000", "0"):
        print("❌ position 응답 이상:", j)
        return []

    raw = j.get("data") or []
    if isinstance(raw, dict):
        raw = raw.get("positions") or raw.get("list") or []

    out: List[Dict] = []
    for it in raw:
        sym_full = it.get("symbol") or ""
        if not sym_full.endswith("_UMCBL"):
            continue
        sym_core = sym_full.replace("_UMCBL", "")
        hold     = (it.get("holdSide") or it.get("side") or "").lower()  # long | short
        sz       = _ffloat(it.get("total") or it.get("available") or it.get("size"))
        avg      = _ffloat(it.get("averageOpenPrice") or it.get("avgOpenPrice") or it.get("entryPrice"))
        if sz > 0 and hold in ("long", "short"):
            out.append({"symbol": sym_core, "side": hold, "size": sz, "entry_price": avg})
    return out

def get_open_positions() -> List[Dict]:
    now = time.time()
    if now < _POS_CACHE["cooldown_until"] and _POS_CACHE["data"]:
        return _POS_CACHE["data"]
    res = _fetch_positions()
    if res:
        _POS_CACHE["data"] = res
        _POS_CACHE["ts"] = now
        _POS_CACHE["cooldown_until"] = 0.0
        return res
    # 실패 시 캐시 반환 + 쿨다운
    if _POS_CACHE["data"]:
        _POS_CACHE["cooldown_until"] = now + 90
        print("⚠️ position 새 조회 실패 → 캐시 반환(90s 쿨다운)")
    return _POS_CACHE["data"]
