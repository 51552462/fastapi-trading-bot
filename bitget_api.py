# bitget_api.py — V2는 suffix 없이(core 심볼만), V1은 기존대로 _UMCBL 사용
import os, time, json, hmac, hashlib, base64, requests, math, random
from typing import Dict, List, Optional, Tuple

BASE_URL = "https://api.bitget.com"

API_KEY        = os.getenv("BITGET_API_KEY", "")
API_SECRET     = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE = os.getenv("BITGET_API_PASSWORD", "")

_last_call = {}
def _rl(key: str, min_interval: float = 0.08):
    now = time.time(); prev = _last_call.get(key, 0.0)
    wait = min_interval - (now - prev)
    if wait > 0: time.sleep(wait)
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

# ──────────────────────────────────────────────────────────────
# TradingView ↔ Bitget symbol alias
# ──────────────────────────────────────────────────────────────
ALIASES: Dict[str, str] = {}
_alias_env = os.getenv("SYMBOL_ALIASES_JSON", "")
if _alias_env:
    try: ALIASES.update(json.loads(_alias_env))
    except: pass

def convert_symbol(sym: str) -> str:
    s = (sym or "").upper().strip()
    for ch in ("/", "-", "_", " "):
        s = s.replace(ch, "")
    if s.endswith("PERP"):
        s = s[:-4]
    if s.endswith("USDT.P"):
        s = s[:-6] + "USDT"
    if s.endswith("USDTP"):
        s = s[:-5] + "USDT"
    return ALIASES.get(s, s)

def _mix_symbol(sym: str) -> str:
    return f"{convert_symbol(sym)}_UMCBL"

# ──────────────────────────────────────────────────────────────
# ticker/price cache
# ──────────────────────────────────────────────────────────────
_TICKER_CACHE: Dict[str, tuple] = {}
TICKER_TTL    = float(os.getenv("TICKER_TTL", "2.5"))
STRICT_TICKER = os.getenv("STRICT_TICKER", "0") == "1"
ALLOW_DEPTH_FALLBACK = os.getenv("ALLOW_DEPTH_FALLBACK", "1") == "1"

def _depth_midprice(symbol: str) -> Optional[float]:
    try:
        _rl("depth", 0.08)
        r = requests.get(f"{BASE_URL}/api/mix/v1/market/depth?symbol={_mix_symbol(symbol)}&limit=5", timeout=10)
        j = r.json(); d = j.get("data") or {}
        asks = d.get("asks") or d.get("ask") or []; bids = d.get("bids") or d.get("bid") or []
        if asks and bids:
            a = float(asks[0][0]); b = float(bids[0][0])
            if a > 0 and b > 0: return (a + b) / 2.0
    except: pass
    return None

_SYMBOLS_CACHE = {"ts": 0.0, "data": {}}
def _refresh_symbols_cache():
    try:
        _rl("symbols", 0.15)
        path = "/api/mix/v1/public/symbols"; q = "productType=umcbl"
        r = requests.get(f"{BASE_URL}{path}?{q}", headers=_headers("GET", f"{path}?{q}", ""), timeout=12)
        j = r.json(); arr = j.get("data") or []
        m = {}
        for it in arr:
            sym_full = it.get("symbol") or ""
            if not sym_full.endswith("_UMCBL"): continue
            sym_core = sym_full.replace("_UMCBL", "")
            size_scale = int(it.get("sizeScale") or 0)
            size_step  = 10 ** (-size_scale) if size_scale >= 0 else 0.001
            min_qty    = float(it.get("minTradeNum") or it.get("minOrderSize") or 0.0)
            m[sym_core] = {"sizeStep": size_step, "minQty": min_qty}
        _SYMBOLS_CACHE["data"] = m; _SYMBOLS_CACHE["ts"] = time.time()
    except Exception as e:
        print("❌ 심볼 캐시 갱신 실패:", e)

def get_symbol_spec(symbol: str) -> Dict[str, float]:
    now = time.time()
    if now - _SYMBOLS_CACHE["ts"] > 600 or not _SYMBOLS_CACHE["data"]:
        _refresh_symbols_cache()
    sym = convert_symbol(symbol)
    spec = _SYMBOLS_CACHE["data"].get(sym)
    if not spec:
        print(f"⚠️ {sym} 심볼 스펙 없음 → fallback(sizeStep=0.001,minQty=0.001)")
        spec = {"sizeStep": 0.001, "minQty": 0.001}
        _SYMBOLS_CACHE["data"][sym] = spec
    return spec

def round_down_step(qty: float, step: float) -> float:
    if step <= 0: return round(qty, 6)
    k = math.floor(qty / step); return round(k * step, 6)

# ──────────────────────────────────────────────────────────────
# ====== V2 endpoints (env로 경로 주입 가능) + V1 폴백 ======
# ──────────────────────────────────────────────────────────────
_BITGET_USE_V2 = os.getenv("BITGET_USE_V2", "1") == "1"
_V2_TICKER_PATH = os.getenv("BITGET_V2_TICKER_PATH", "/api/v2/mix/market/ticker")
_V2_MARK_PATH   = os.getenv("BITGET_V2_MARK_PATH",   "/api/v2/mix/market/mark-price")
_V2_DEPTH_PATH  = os.getenv("BITGET_V2_DEPTH_PATH",  "/api/v2/mix/market/orderbook")

def _http_get(path: str, params: dict, timeout: float = 1.2) -> Tuple[Optional[dict], Optional[str]]:
    if not path:
        return None, "EMPTY_PATH"
    url = BASE_URL + path
    try:
        r = requests.get(url, params=params, timeout=timeout)
        if r.status_code != 200:
            return None, f"HTTP_{r.status_code}:{r.text[:160]}"
        try:
            return r.json(), None
        except Exception as e:
            return None, f"JSON_ERR:{str(e)}"
    except Exception as e:
        return None, f"REQ_ERR:{str(e)}"

# ★ 변경 포인트 1 : V2는 suffix 없이 core 심볼만
def _v2_ticker(sym: str) -> Optional[float]:
    if not _BITGET_USE_V2 or not _V2_TICKER_PATH: return None
    core = convert_symbol(sym)
    j, err = _http_get(_V2_TICKER_PATH, {"symbol": core})
    if err or not j: 
        return None
    d = j.get("data") or {}
    last = d.get("last") or d.get("close")
    try:
        return float(last) if last not in (None, "", "0", 0, "0.0") else None
    except:
        return None

# ★ 변경 포인트 2 : V2 mark-price도 core 심볼만
def _v2_mark_price(sym: str) -> Optional[float]:
    if not _BITGET_USE_V2 or not _V2_MARK_PATH: return None
    core = convert_symbol(sym)
    j, err = _http_get(_V2_MARK_PATH, {"symbol": core})
    if err or not j:
        return None
    d = j.get("data") or {}
    mp = d.get("markPrice") or d.get("price")
    try:
        return float(mp) if mp not in (None, "", "0", 0, "0.0") else None
    except:
        return None

# ★ 변경 포인트 3 : V2 orderbook도 core 심볼만
def _v2_orderbook_mid(sym: str) -> Optional[float]:
    if not _BITGET_USE_V2 or not _V2_DEPTH_PATH: return None
    core = convert_symbol(sym)
    j, err = _http_get(_V2_DEPTH_PATH, {"symbol": core, "limit": 1})
    if err or not j:
        return None
    d = j.get("data") or {}
    bids = d.get("bids") or []
    asks = d.get("asks") or []
    try:
        bid = float(bids[0][0]) if bids and bids[0] else None
        ask = float(asks[0][0]) if asks and asks[0] else None
        if bid and ask: 
            return (bid + ask) / 2.0
    except:
        return None
    return None

# ──────────────────────────────────────────────────────────────
# get_last_price: V2 → V1 순서 (짧은 재시도 + 캐시)
# ──────────────────────────────────────────────────────────────
def get_last_price(symbol: str, retries: int = 6, base: float = 0.20) -> Optional[float]:
    sym = convert_symbol(symbol)
    c = _TICKER_CACHE.get(sym); now = time.time()
    if c and now - c[0] <= TICKER_TTL:
        return float(c[1])

    url_ticker = f"{BASE_URL}/api/mix/v1/market/ticker?symbol={_mix_symbol(sym)}"
    url_mark   = f"{BASE_URL}/api/mix/v1/market/mark-price?symbol={_mix_symbol(sym)}"

    def _try_once() -> Optional[float]:
        # v2
        px = _v2_ticker(sym)
        if px: return px
        px = _v2_mark_price(sym)
        if px: return px
        px = _v2_orderbook_mid(sym)
        if px: return px

        # v1
        try:
            _rl("ticker", 0.06)
            r = requests.get(url_ticker, timeout=10)
            if r.status_code == 200:
                j = r.json(); data = j.get("data")
                if data and data.get("last") not in (None, "", "0", 0, "0.0"):
                    px = float(data["last"])
                    if px > 0: return px
        except: pass
        try:
            _rl("mark", 0.06)
            rm = requests.get(url_mark, timeout=10)
            if rm.status_code == 200:
                jm = rm.json(); dm = jm.get("data") or {}
                mp = dm.get("markPrice") or dm.get("mark") or dm.get("price")
                if mp not in (None, "", "0", 0, "0.0"):
                    px = float(mp)
                    if px > 0: return px
        except: pass

        if ALLOW_DEPTH_FALLBACK:
            alt = _depth_midprice(sym)
            if alt and alt > 0:
                return alt
        return None

    for i in range(retries):
        px = _try_once()
        if px and px > 0:
            _TICKER_CACHE[sym] = (time.time(), px)
            return px
        time.sleep(base * (2 ** i) + random.uniform(0, 0.1))

    if not STRICT_TICKER:
        c = _TICKER_CACHE.get(sym)
        if c: return float(c[1])

    print(f"❌ Ticker 실패(최종): {_mix_symbol(sym)}")
    return None

# ──────────────────────────────────────────────────────────────
# Positions helpers (NEW)
# ──────────────────────────────────────────────────────────────
_POS_CACHE = {"data": [], "ts": 0.0, "cooldown_until": 0.0}
def _ffloat(x):
    try: return float(x)
    except: return 0.0

def _fetch_positions() -> List[Dict]:
    path = "/api/mix/v1/position/allPosition"; q = "productType=umcbl"
    try:
        _rl("positions", 0.10)
        res = requests.get(f"{BASE_URL}{path}?{q}", headers=_headers("GET", f"{path}?{q}", ""), timeout=12)
        j = res.json()
    except Exception as e:
        print("❌ position fetch 예외:", e); return []
    if not j or j.get("code") not in ("00000","0"):
        print("❌ position 응답 이상:", j); return []
    raw = j.get("data") or []
    if isinstance(raw, dict): raw = raw.get("positions") or raw.get("list") or []
    out: List[Dict] = []
    for it in raw:
        sym_full = it.get("symbol") or ""
        if not sym_full.endswith("_UMCBL"): continue
        sym_core = sym_full.replace("_UMCBL","")
        hold     = (it.get("holdSide") or it.get("side") or "").lower()
        sz       = _ffloat(it.get("total") or it.get("available") or it.get("size"))
        avg      = _ffloat(it.get("averageOpenPrice") or it.get("avgOpenPrice") or it.get("entryPrice"))
        liq      = _ffloat(it.get("liquidationPrice") or it.get("liqPx") or 0.0)
        lev      = _ffloat(it.get("fixedLeverage") or it.get("crossLeverage") or it.get("leverage") or 0.0)
        if sz > 0 and hold in ("long","short"):
            out.append({
                "symbol": sym_core,
                "side": hold,
                "size": sz,
                "entry_price": avg,
                "liq_price": liq,
                "leverage": lev,
            })
    return out

def get_open_positions() -> List[Dict]:
    now = time.time()
    if now < _POS_CACHE["cooldown_until"] and _POS_CACHE["data"]:
        return _POS_CACHE["data"]
    res = _fetch_positions()
    if res:
        _POS_CACHE["data"] = res; _POS_CACHE["ts"] = now; _POS_CACHE["cooldown_until"] = 0.0
        return res
    if _POS_CACHE["data"]:
        _POS_CACHE["cooldown_until"] = now + 90
        print("⚠️ position 새 조회 실패 → 캐시 반환(90s 쿨다운)")
    return _POS_CACHE["data"]

def get_position_size(symbol: str, side: str) -> float:
    """현재 보유 수량(long/short) 반환. 없으면 0."""
    sym = convert_symbol(symbol)
    side = side.lower()
    for p in get_open_positions():
        if p.get("symbol") == sym and p.get("side") == side:
            try:
                return float(p.get("size") or 0.0)
            except:
                return 0.0
    return 0.0

# ──────────────────────────────────────────────────────────────
# Orders
# ──────────────────────────────────────────────────────────────
def place_market_order(symbol: str, usdt_amount: float, side: str, leverage: float = 5, reduce_only: bool = False) -> Dict:
    last = get_last_price(symbol)
    if not last: return {"code": "LOCAL_TICKER_FAIL", "msg": "ticker_none"}
    spec = get_symbol_spec(symbol)
    qty  = round_down_step(usdt_amount / last, float(spec.get("sizeStep", 0.001)))
    if qty <= 0: return {"code": "LOCAL_BAD_QTY", "msg": f"qty {qty}"}
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
    """
    reduceOnly 마켓주문. 요청 사이즈를 현재 보유 사이즈와 스텝/최소수량에 맞춰 자동 클램핑.
    side: "long"을 줄이면 sell_single, "short"를 줄이면 buy_single
    """
    # 1) 현재 보유 수량 점검 & 클램핑 (NEW)
    held = get_position_size(symbol, side)
    if held <= 0:
        return {"code": "LOCAL_NO_POSITION", "msg": "held=0"}

    step = float(get_symbol_spec(symbol).get("sizeStep", 0.001))
    minq = float(get_symbol_spec(symbol).get("minQty", 0.0))

    size_req = max(0.0, float(size))
    size_req = min(size_req, held)                        # 보유 수량 초과 방지
    size_req = round_down_step(size_req, step)            # 스텝 반올림
    if size_req < max(minq, step):
        return {"code": "LOCAL_STEP_ZERO", "msg": f"after_clamp={size_req}"}

    path = "/api/mix/v1/order/placeOrder"
    body = {
        "symbol":     _mix_symbol(symbol),
        "marginCoin": "USDT",
        "size":       str(size_req),
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
            # 40804 방지 로직을 넣었지만, 혹시 겹치면 여기서도 방어
            if "40804" in res.text:
                return {"code": "LOCAL_CLAMPED_40804", "msg": "exceed_held_blocked"}
            return {"code": f"HTTP_{res.status_code}", "msg": res.text}
        return res.json()
    except Exception as e:
        print("❌ reduce EXC", str(e))
        return {"code": "LOCAL_EXCEPTION", "msg": str(e)}

def reduce_by_ratio(symbol: str, side: str, ratio: float) -> Dict:
    """
    현재 보유 수량의 비율만큼 안전 감축(중복 안전).
    ratio=0.5 → 50% 감축
    """
    held = get_position_size(symbol, side)
    if held <= 0:
        return {"code": "LOCAL_NO_POSITION", "msg": "held=0"}
    size = max(0.0, held * float(ratio))
    return place_reduce_by_size(symbol, size, side)

# ──────────────────────────────────────────────────────────────
# 계좌/마진/잔고 조회
# ──────────────────────────────────────────────────────────────
def _private_get(path: str, query: str = "", timeout: float = 10.0) -> Dict:
    q = f"?{query}" if query else ""
    try:
        _rl(path, 0.10)
        r = requests.get(BASE_URL + path + q, headers=_headers("GET", path + q, ""), timeout=timeout)
        return r.json() if r is not None else {}
    except Exception as e:
        print("❌ private_get 예외:", e)
        return {}

def get_account_equity() -> Optional[float]:
    j = _private_get("/api/mix/v1/account/accounts", "productType=umcbl", timeout=12)
    data = j.get("data")
    if not data: return None
    def _pick(d):
        for k in ("usdtEquity","equity","totalEquity","accountEquity"):
            v = d.get(k)
            if v not in (None, "", "0", 0):
                try: return float(v)
                except: pass
        return None
    if isinstance(data, list):
        for d in data:
            v = _pick(d)
            if v and v > 0: return v
    elif isinstance(data, dict):
        v = _pick(data)
        if v and v > 0: return v
    return None

def get_wallet_balance(coin: str = "USDT") -> Dict[str, float]:
    j = _private_get("/api/mix/v1/account/accounts", "productType=umcbl", timeout=12)
    data = j.get("data")
    out = {"available": 0.0, "total": 0.0}
    if not data: return out
    arr = data if isinstance(data, list) else [data]
    for d in arr:
        try:
            eq = float(d.get("usdtEquity") or d.get("equity") or d.get("totalEquity") or 0.0)
            av = float(d.get("available") or d.get("availableMargin") or d.get("cashBal") or 0.0)
            out["total"] = max(out["total"], eq)
            out["available"] = max(out["available"], av)
        except:
            continue
    return out

def get_margin_snapshot() -> Dict[str, float]:
    bal = get_wallet_balance("USDT")
    eq  = get_account_equity() or float(bal.get("total") or 0.0)
    av  = float(bal.get("available") or 0.0)
    used = max(0.0, eq - av)
    return {"margin_used": used, "available": av, "equity": eq}
