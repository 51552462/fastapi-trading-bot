# bitget_api_spot.py  (Spot symbols from v2, rest kept the same)
import os, time, json, hmac, hashlib, base64, requests, math, re
from typing import Dict, Optional

BASE_URL = "https://api.bitget.com"

API_KEY        = os.getenv("BITGET_API_KEY", "")
API_SECRET     = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE = os.getenv("BITGET_API_PASSWORD", "")

# how long to block a symbol after 40309 (removed) – default 12h
REMOVED_BLOCK_TTL = int(os.getenv("REMOVED_BLOCK_TTL", "43200"))

# ---------- small rate-limit helper ----------
_last_call: Dict[str, float] = {}
def _rl(key: str, min_interval: float = 0.08):
    now = time.time()
    prev = _last_call.get(key, 0.0)
    wait = min_interval - (now - prev)
    if wait > 0:
        time.sleep(wait)
    _last_call[key] = time.time()

# ---------- auth ----------
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

# ---------- symbol helpers ----------
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

def _spot_symbol(sym: str) -> str:
    """
    Orders/tickers on v1 endpoints still commonly expect *_SPBL.
    We normalize to that form for order/ticker calls.
    """
    base = convert_symbol(sym)
    return base if base.endswith("_SPBL") else f"{base}_SPBL"

# ---------- removed symbol cache ----------
_REMOVED: Dict[str, float] = {}  # base -> until_ts

def mark_symbol_removed(symbol: str):
    base = convert_symbol(symbol)
    _REMOVED[base] = time.time() + REMOVED_BLOCK_TTL

def is_symbol_removed(symbol: str) -> bool:
    base = convert_symbol(symbol)
    until = _REMOVED.get(base, 0.0)
    if until <= 0:
        return False
    if time.time() > until:
        _REMOVED.pop(base, None)
        return False
    return True

# ---------- symbol/spec cache (from v2 endpoint) ----------
_PROD_CACHE = {"ts": 0.0, "data": {}}

def _to_float(x, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if s == "" or s.lower() == "null":
            return default
        return float(s)
    except Exception:
        return default

def _refresh_products_cache_v2():
    """
    Use v2 symbols endpoint:
      GET /api/v2/spot/public/symbols
    Returns entries like:
      {
        "symbol":"PRIMEUSDT",
        "status":"online",
        "pricePrecision":"5",
        "quantityPrecision":"2",
        "quotePrecision":"5",
        "minTradeUSDT":"1",
        "minTradeAmount":"0",
        ...
      }
    """
    path = "/api/v2/spot/public/symbols"
    try:
        _rl("products_v2", 0.15)
        r = requests.get(BASE_URL + path, timeout=12)
        j = r.json()
        arr = j.get("data") or []
        m: Dict[str, Dict] = {}
        for it in arr:
            sym_base = (it.get("symbol") or "").upper()  # e.g. PRIMEUSDT  (no _SPBL in v2)
            if not sym_base:
                continue
            qty_prec   = int(_to_float(it.get("quantityPrecision"), 6))
            price_prec = int(_to_float(it.get("pricePrecision"), 6))
            # prefer minTradeUSDT, fallback to minTradeAmount
            min_quote  = _to_float(it.get("minTradeUSDT"), _to_float(it.get("minTradeAmount"), 1.0))
            status_raw = it.get("status")
            tradable   = True
            if isinstance(status_raw, str):
                tradable = status_raw.lower() in ("online", "enable", "enabled", "true", "tradable")
            elif isinstance(status_raw, bool):
                tradable = bool(status_raw)

            spec = {
                "qtyStep":   10 ** (-qty_prec),
                "priceStep": 10 ** (-price_prec),
                "minQuote":  min_quote if min_quote > 0 else 1.0,
                "tradable":  tradable,
            }
            # store under multiple keys to avoid mismatches
            m[sym_base] = spec
            m[f"{sym_base}_SPBL"] = spec
            # legacy keys without slash/dash also map to same spec
            m[sym_base.replace("/", "").replace("-", "")] = spec

        _PROD_CACHE["data"] = m
        _PROD_CACHE["ts"] = time.time()
    except Exception as e:
        print("spot products v2 refresh fail:", e)

def get_symbol_spec_spot(symbol: str) -> Dict[str, float]:
    now = time.time()
    if now - _PROD_CACHE["ts"] > 600 or not _PROD_CACHE["data"]:
        _refresh_products_cache_v2()
    base = convert_symbol(symbol)
    key1 = _spot_symbol(symbol)  # e.g. PRIMEUSDT_SPBL
    key2 = base                  # e.g. PRIMEUSDT
    spec = _PROD_CACHE["data"].get(key1) or _PROD_CACHE["data"].get(key2)
    if not spec:
        # safe default
        spec = {"qtyStep": 0.000001, "priceStep": 0.000001, "minQuote": 1.0, "tradable": True}
        _PROD_CACHE["data"][key1] = spec
    return spec

def is_tradable(symbol: str) -> bool:
    if is_symbol_removed(symbol):
        return False
    spec = get_symbol_spec_spot(symbol)
    return bool(spec.get("tradable", True))

def round_down_step(x: float, step: float) -> float:
    if step <= 0:
        return round(float(x), 8)
    k = math.floor(float(x) / step)
    return round(k * step, 12)

def _step_to_scale(step: float) -> int:
    if step <= 0:
        return 6
    p = round(-math.log10(step))
    return max(0, int(p))

# ---------- ticker (keep v1; it accepts *_SPBL) ----------
_SPOT_TICKER_CACHE: Dict[str, tuple] = {}
SPOT_TICKER_TTL = float(os.getenv("SPOT_TICKER_TTL", "2.5"))

def get_last_price_spot(symbol: str, retries: int = 5, sleep_base: float = 0.18) -> Optional[float]:
    sym_spbl = _spot_symbol(symbol)
    c = _SPOT_TICKER_CACHE.get(sym_spbl)
    now = time.time()
    if c and now - c[0] <= SPOT_TICKER_TTL:
        return float(c[1])

    path = f"/api/spot/v1/market/ticker?symbol={sym_spbl}"
    for i in range(retries):
        try:
            _rl("spot_ticker", 0.06)
            r = requests.get(BASE_URL + path, timeout=10)
            if r.status_code != 200:
                time.sleep(sleep_base * (2 ** i))
                continue
            j = r.json()
            d = j.get("data")
            px = None
            if isinstance(d, dict):
                px = d.get("close") or d.get("last")
            elif isinstance(d, list) and d and isinstance(d[0], dict):
                px = d[0].get("close") or d[0].get("last")
            if px:
                v = float(px)
                if v > 0:
                    _SPOT_TICKER_CACHE[sym_spbl] = (time.time(), v)
                    return v
        except Exception:
            pass
        time.sleep(sleep_base * (2 ** i))
    return None

# ---------- balances (v2 fresh, v1 fallback) ----------
_BAL_CACHE = {"ts": 0.0, "data": {}}

def _fetch_assets_v2(coin: Optional[str] = None) -> Dict[str, float]:
    path = "/api/v2/spot/account/assets"
    if coin:
        path += f"?coin={coin}"
    _rl("spot_bal_v2", 0.15)
    r = requests.get(BASE_URL + path, headers=_headers("GET", path, ""), timeout=12)
    j = r.json()
    arr = j.get("data") or []
    m: Dict[str, float] = {}
    for it in arr:
        c = (it.get("coin") or "").upper()
        if not c:
            continue
        avail = _to_float(it.get("available"), 0.0)
        m[c] = avail
    return m

def _fetch_assets_v1() -> Dict[str, float]:
    path = "/api/spot/v1/account/assets"
    _rl("spot_bal_v1", 0.15)
    r = requests.get(BASE_URL + path, headers=_headers("GET", path, ""), timeout=12)
    j = r.json()
    arr = j.get("data") or []
    m: Dict[str, float] = {}
    for it in arr:
        c = (it.get("coin") or "").upper()
        if not c:
            continue
        avail = _to_float(it.get("available"), 0.0)
        m[c] = avail
    return m

def get_spot_balances(force: bool = False, coin: Optional[str] = None) -> Dict[str, float]:
    now = time.time()
    if not force and not coin:
        if now - _BAL_CACHE["ts"] < 5.0 and _BAL_CACHE["data"]:
            return _BAL_CACHE["data"]

    try:
        if coin:
            return _fetch_assets_v2(coin)
        m = _fetch_assets_v2(None)
        if not m:
            m = _fetch_assets_v1()
        if m:
            _BAL_CACHE["ts"] = now
            _BAL_CACHE["data"] = m
        return m or (_BAL_CACHE["data"] or {})
    except Exception as e:
        print("spot balances error:", e)
        return _BAL_CACHE["data"] or {}

def get_spot_free_qty(symbol: str, fresh: bool = False) -> float:
    base = convert_symbol(symbol).replace("USDT", "")
    if fresh:
        m = get_spot_balances(force=True, coin=base)
        return float(m.get(base, 0.0))
    m = get_spot_balances()
    return float(m.get(base, 0.0))

# ---------- order helpers ----------
def _extract_code_text(resp_text: str) -> Dict[str, str]:
    try:
        j = json.loads(resp_text)
        if isinstance(j, dict):
            return {"code": str(j.get("code", "")), "msg": str(j.get("msg", ""))}
    except Exception:
        pass
    m = re.search(r'"code"\s*:\s*"?(?P<code>\d+)"?', resp_text or "")
    n = re.search(r'"msg"\s*:\s*"(?P<msg>[^"]+)"', resp_text or "")
    return {"code": m.group("code") if m else "", "msg": n.group("msg") if n else ""}

# ---------- orders (keep v1 place-order; symbol = *_SPBL) ----------
def place_spot_market_buy(symbol: str, usdt_amount: float) -> Dict:
    if not is_tradable(symbol):
        mark_symbol_removed(symbol)
        return {"code": "LOCAL_SYMBOL_REMOVED", "msg": "symbol not tradable/removed"}

    spec = get_symbol_spec_spot(symbol)
    min_quote = float(spec.get("minQuote", 1.0))
    if usdt_amount < min_quote:
        return {"code": "LOCAL_MIN_QUOTE", "msg": f"need>={min_quote}USDT"}

    amt_str = f"{float(usdt_amount):.6f}".rstrip("0").rstrip(".")
    path = "/api/spot/v1/trade/orders"
    body = {
        "symbol": _spot_symbol(symbol),
        "side": "buy",
        "orderType": "market",
        "force": "gtc",
        "quantity": amt_str  # market-buy expects quote amount
    }
    bj = json.dumps(body)
    try:
        _rl("spot_order", 0.12)
        res = requests.post(BASE_URL + path, headers=_headers("POST", path, bj), data=bj, timeout=15)
        if res.status_code != 200:
            info = _extract_code_text(res.text or "")
            if info.get("code") == "40309" or "symbol has been removed" in (info.get("msg","").lower()):
                mark_symbol_removed(symbol)
            return {"code": f"HTTP_{res.status_code}", "msg": res.text}
        return res.json()
    except Exception as e:
        return {"code": "LOCAL_EXCEPTION", "msg": str(e)}

def place_spot_market_sell_qty(symbol: str, qty: float) -> Dict:
    qty = float(qty)
    if qty <= 0:
        return {"code": "LOCAL_BAD_QTY", "msg": "qty<=0"}

    if not is_tradable(symbol):
        mark_symbol_removed(symbol)
        return {"code": "LOCAL_SYMBOL_REMOVED", "msg": "symbol not tradable/removed"}

    spec = get_symbol_spec_spot(symbol)
    step = float(spec.get("qtyStep", 1e-6))

    def _scale_from_step(s: float) -> int:
        if s <= 0:
            return 6
        p = round(-math.log10(s))
        return max(0, int(p))

    def _fmt(q: float, s: float) -> str:
        q = round_down_step(q, s)
        scale = _scale_from_step(s)
        return (f"{q:.{scale}f}").rstrip("0").rstrip(".") if scale > 0 else str(int(q))

    qty_str = _fmt(qty, step)
    path = "/api/spot/v1/trade/orders"
    body = {
        "symbol": _spot_symbol(symbol),
        "side": "sell",
        "orderType": "market",
        "force": "gtc",
        "quantity": qty_str
    }
    bj = json.dumps(body)

    try:
        _rl("spot_order", 0.12)
        res = requests.post(BASE_URL + path, headers=_headers("POST", path, bj), data=bj, timeout=15)
        if res.status_code == 200:
            return res.json()

        txt  = res.text or ""
        info = _extract_code_text(txt)
        if info.get("code") == "40309" or "symbol has been removed" in (info.get("msg","").lower()):
            mark_symbol_removed(symbol)
            return {"code": f"HTTP_{res.status_code}", "msg": txt}

        # 40808 → precision resubmit
        m = re.search(r"(?:checkBDScale|checkScale)[\"']?\s*[:=]\s*([0-9]+)", txt)
        if res.status_code == 400 and m:
            chk = int(m.group(1))
            step2 = 10 ** (-chk)
            qty_str2 = _fmt(qty, step2)
            body2 = dict(body, quantity=qty_str2)
            bj2 = json.dumps(body2)
            try:
                from telegram_bot import send_telegram
                send_telegram(f"[SPOT] retry sell {symbol} scale-> {chk} qty={qty_str2}")
            except Exception:
                pass
            _rl("spot_order", 0.12)
            res2 = requests.post(BASE_URL + path, headers=_headers("POST", path, bj2), data=bj2, timeout=15)
            if res2.status_code == 200:
                return res2.json()
            return {"code": f"HTTP_{res2.status_code}", "msg": res2.text, "retry_with_scale": chk}

        return {"code": f"HTTP_{res.status_code}", "msg": txt}
    except Exception as e:
        return {"code": "LOCAL_EXCEPTION", "msg": str(e)}
