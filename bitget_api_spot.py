# bitget_api_spot.py
import os, time, json, hmac, hashlib, base64, requests, math, random
from typing import Dict, List, Optional

BASE_URL = "https://api.bitget.com"

API_KEY        = os.getenv("BITGET_API_KEY", "")
API_SECRET     = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE = os.getenv("BITGET_API_PASSWORD", "")

# ── Rate limit best-effort ────────────────────────────────────
_last_call = {}
def _rl(key: str, min_interval: float = 0.08):
    now = time.time()
    prev = _last_call.get(key, 0.0)
    wait = min_interval - (now - prev)
    if wait > 0:
        time.sleep(wait)
    _last_call[key] = time.time()

# ── Auth (Bitget: HMAC-SHA256 → base64) ───────────────────────
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

# ── Symbol helpers ─────────────────────────────────────────────
ALIASES: Dict[str, str] = {}
_alias_env = os.getenv("SYMBOL_ALIASES_JSON", "")
if _alias_env:
    try: ALIASES.update(json.loads(_alias_env))
    except: pass

def convert_symbol(sym: str) -> str:
    s = (sym or "").upper().replace("/", "").replace("-", "").replace("_", "")
    if s.endswith("PERP"):  # 트뷰가 가끔 붙여보내는 표기 방어
        s = s[:-4]
    return ALIASES.get(s, s)  # ex) KAIAUSDT→KLAYUSDT 매핑 필요시

def _spot_symbol(sym: str) -> str:
    return convert_symbol(sym)  # BTCUSDT

# ── Product(spec) cache ───────────────────────────────────────
_PROD_CACHE = {"ts": 0.0, "data": {}}

def _refresh_products_cache():
    # https://www.bitget.com/api-doc/spot/market/Get-Symbols
    path = "/api/spot/v1/public/products"
    try:
        _rl("products", 0.15)
        r = requests.get(BASE_URL + path, timeout=12)
        j = r.json()
        arr = j.get("data") or []
        m = {}
        for it in arr:
            sym = (it.get("symbol") or "").upper()
            if not sym:
                continue
            # 주요 파라미터: quantityPrecision, minTradeAmount(= 최소 주문 금액, quote 기준)
            qty_prec = int(it.get("quantityPrecision") or 6)
            price_prec = int(it.get("pricePrecision") or 6)
            min_amt = float(it.get("minTradeAmount") or 0.0)  # quote(=USDT) 최소주문금액
            m[sym] = {
                "qtyStep": 10 ** (-qty_prec),
                "priceStep": 10 ** (-price_prec),
                "minQuote": min_amt
            }
        _PROD_CACHE["data"] = m
        _PROD_CACHE["ts"] = time.time()
    except Exception as e:
        print("❌ spot products refresh fail:", e)

def get_symbol_spec_spot(symbol: str) -> Dict[str, float]:
    now = time.time()
    if now - _PROD_CACHE["ts"] > 600 or not _PROD_CACHE["data"]:
        _refresh_products_cache()
    sym = convert_symbol(symbol)
    spec = _PROD_CACHE["data"].get(sym)
    if not spec:
        spec = {"qtyStep": 0.000001, "priceStep": 0.000001, "minQuote": 5.0}
        _PROD_CACHE["data"][sym] = spec
    return spec

def round_down_step(x: float, step: float) -> float:
    if step <= 0:
        return round(x, 8)
    k = math.floor(float(x) / step)
    return round(k * step, 8)

# ── Ticker ────────────────────────────────────────────────────
_SPOT_TICKER_CACHE: Dict[str, tuple] = {}
SPOT_TICKER_TTL = float(os.getenv("SPOT_TICKER_TTL", "2.5"))

def get_last_price_spot(symbol: str, retries: int = 5, sleep_base: float = 0.18) -> Optional[float]:
    sym = convert_symbol(symbol)
    c = _SPOT_TICKER_CACHE.get(sym)
    now = time.time()
    if c and now - c[0] <= SPOT_TICKER_TTL:
        return float(c[1])
    path = f"/api/spot/v1/market/ticker?symbol={_spot_symbol(sym)}"
    for i in range(retries):
        try:
            _rl("spot_ticker", 0.06)
            r = requests.get(BASE_URL + path, timeout=10)
            if r.status_code != 200:
                time.sleep(sleep_base * (2 ** i) + random.uniform(0, 0.1))
                continue
            j = r.json()
            d = j.get("data") or {}
            px = d.get("close") or d.get("last")
            if px:
                v = float(px)
                if v > 0:
                    _SPOT_TICKER_CACHE[sym] = (time.time(), v)
                    return v
        except Exception:
            pass
        time.sleep(sleep_base * (2 ** i) + random.uniform(0, 0.1))
    return None

# ── Balances (현물 잔고) ──────────────────────────────────────
_BAL_CACHE = {"ts": 0.0, "data": {}}

def get_spot_balances() -> Dict[str, float]:
    # https://www.bitget.com/api-doc/spot/account/Get-Account-Assets
    global _BAL_CACHE
    now = time.time()
    if now - _BAL_CACHE["ts"] < 5.0 and _BAL_CACHE["data"]:
        return _BAL_CACHE["data"]
    path = "/api/spot/v1/account/assets"
    try:
        _rl("spot_bal", 0.15)
        r = requests.get(BASE_URL + path, headers=_headers("GET", path, ""), timeout=12)
        j = r.json()
        arr = j.get("data") or []
        m = {}
        for it in arr:
            coin = (it.get("coin") or "").upper()
            avail = float(it.get("available") or 0.0)
            if coin:
                m[coin] = avail
        _BAL_CACHE = {"ts": now, "data": m}
        return m
    except Exception as e:
        print("❌ spot balances err:", e)
        return _BAL_CACHE["data"] or {}

def get_spot_free_qty(symbol: str) -> float:
    sym = convert_symbol(symbol)
    base = sym.replace("USDT", "")  # BTCUSDT → BTC
    bals = get_spot_balances()
    return float(bals.get(base, 0.0))

# ── Orders (현물 시장가 주문) ────────────────────────────────
def place_spot_market_buy(symbol: str, usdt_amount: float) -> Dict:
    last = get_last_price_spot(symbol)
    if not last:
        return {"code":"LOCAL_TICKER_FAIL","msg":"no_spot_ticker"}
    spec = get_symbol_spec_spot(symbol)
    if usdt_amount < float(spec.get("minQuote", 5.0)):
        return {"code":"LOCAL_MIN_QUOTE","msg":f"need≥{spec.get('minQuote',5.0)}USDT"}
    qty = round_down_step(usdt_amount / last, float(spec.get("qtyStep", 1e-6)))
    if qty <= 0:
        return {"code":"LOCAL_BAD_QTY","msg":f"qty={qty}"}
    path = "/api/spot/v1/trade/orders"
    body = {
        "symbol": _spot_symbol(symbol),
        "side": "buy",
        "orderType": "market",
        "force": "gtc",
        "quantity": str(qty)  # 시장가 매수는 quantity 지정
    }
    bj = json.dumps(body)
    try:
        _rl("spot_order", 0.12)
        res = requests.post(BASE_URL + path, headers=_headers("POST", path, bj), data=bj, timeout=15)
        if res.status_code != 200:
            return {"code": f"HTTP_{res.status_code}", "msg": res.text}
        return res.json()
    except Exception as e:
        return {"code":"LOCAL_EXCEPTION","msg":str(e)}

def place_spot_market_sell_qty(symbol: str, qty: float) -> Dict:
    qty = float(qty)
    if qty <= 0:
        return {"code":"LOCAL_BAD_QTY","msg":"qty<=0"}
    spec = get_symbol_spec_spot(symbol)
    qty = round_down_step(qty, float(spec.get("qtyStep", 1e-6)))
    if qty <= 0:
        return {"code":"LOCAL_STEP_ZERO","msg":"after_step=0"}
    path = "/api/spot/v1/trade/orders"
    body = {
        "symbol": _spot_symbol(symbol),
        "side": "sell",
        "orderType": "market",
        "force": "gtc",
        "quantity": str(qty)
    }
    bj = json.dumps(body)
    try:
        _rl("spot_order", 0.12)
        res = requests.post(BASE_URL + path, headers=_headers("POST", path, bj), data=bj, timeout=15)
        if res.status_code != 200:
            return {"code": f"HTTP_{res.status_code}", "msg": res.text}
        return res.json()
    except Exception as e:
        return {"code":"LOCAL_EXCEPTION","msg":str(e)}
