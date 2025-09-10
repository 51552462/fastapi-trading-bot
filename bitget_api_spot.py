# bitget_api_spot.py
import os, time, json, hmac, hashlib, base64, requests, math, re
from typing import Dict, Optional, Tuple

BASE_URL = "https://api.bitget.com"

API_KEY        = os.getenv("BITGET_API_KEY", "")
API_SECRET     = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE = os.getenv("BITGET_API_PASSWORD", "")

REMOVED_BLOCK_TTL = int(os.getenv("REMOVED_BLOCK_TTL", "43200"))
SPOT_TICKER_TTL   = float(os.getenv("SPOT_TICKER_TTL", "2.5"))

# 옵션: 캐시에 없는 심볼일 때 v2 목록에서 비슷한 심볼 자동대응 (MOEW -> MOODENG 등)
AUTO_FUZZY_SYMBOL = os.getenv("AUTO_FUZZY_SYMBOL", "1") == "1"

# 텔레그램
try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(_): pass

# ------------------------------------------------
# small rate-limit
_last_call: Dict[str, float] = {}
def _rl(key: str, min_interval: float = 0.08):
    now = time.time()
    prev = _last_call.get(key, 0.0)
    wait = min_interval - (now - prev)
    if wait > 0: time.sleep(wait)
    _last_call[key] = time.time()

# auth/sign
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

# ------------------------------------------------
# alias / base-only set
ALIASES: Dict[str, str] = {}
_alias_env = os.getenv("SYMBOL_ALIASES_JSON", "")
if _alias_env:
    try: ALIASES.update(json.loads(_alias_env))
    except Exception: pass

BASE_ONLY = set()
_bo = os.getenv("BASE_ONLY_SYMBOLS", "")
if _bo: BASE_ONLY = {s.strip().upper() for s in _bo.split(",") if s.strip()}

def _norm(s: str) -> str:
    return (s or "").upper().replace("/", "").replace("-", "").replace("_", "")

def convert_symbol(sym: str) -> str:
    s = _norm(sym)
    if s.endswith("PERP"): s = s[:-4]
    return ALIASES.get(s, s)

# removed cache
_REMOVED: Dict[str, float] = {}
def mark_symbol_removed(symbol: str):
    _REMOVED[convert_symbol(symbol)] = time.time() + REMOVED_BLOCK_TTL
def is_symbol_removed(symbol: str) -> bool:
    b = convert_symbol(symbol)
    u = _REMOVED.get(b, 0.0)
    if not u: return False
    if time.time() > u: _REMOVED.pop(b, None); return False
    return True

# ------------------------------------------------
# v2 symbol/spec cache
_PROD_TS = 0.0
# key -> spec; spec = {qtyStep, priceStep, minQuote, tradable, baseCoin, quoteCoin}
_PROD: Dict[str, Dict] = {}

def _to_float(x, d=0.0):
    try:
        if x is None: return d
        if isinstance(x, (int, float)): return float(x)
        s = str(x).strip()
        if s in ("", "null"): return d
        return float(s)
    except Exception:
        return d

def _refresh_products_cache_v2():
    global _PROD_TS, _PROD
    path = "/api/v2/spot/public/symbols"
    try:
        _rl("products_v2", 0.15)
        r = requests.get(BASE_URL + path, timeout=12)
        j = r.json()
        arr = j.get("data") or []
        m: Dict[str, Dict] = {}
        for it in arr:
            sym_base = _norm(it.get("symbol") or "")      # PRIMEUSDT
            if not sym_base: continue
            qty_p   = int(_to_float(it.get("quantityPrecision"), 6))
            price_p = int(_to_float(it.get("pricePrecision"), 6))
            min_qt  = _to_float(it.get("minTradeUSDT"), _to_float(it.get("minTradeAmount"), 1.0))
            status  = it.get("status")
            tradable= (str(status).lower() in ("online", "enable", "enabled", "true", "tradable"))
            spec = {
                "qtyStep":   10**(-qty_p),
                "priceStep": 10**(-price_p),
                "minQuote":  min_qt if min_qt > 0 else 1.0,
                "tradable":  bool(tradable),
                "baseCoin":  _norm(it.get("baseCoin") or ""),
                "quoteCoin": _norm(it.get("quoteCoin") or ""),
            }
            # 저장: base, base_SPBL 모두
            m[sym_base] = spec
            m[f"{sym_base}_SPBL"] = spec
        _PROD = m
        _PROD_TS = time.time()
    except Exception as e:
        print("spot products v2 refresh fail:", e)

def _ensure_products():
    if not _PROD or time.time() - _PROD_TS > 600:
        _refresh_products_cache_v2()

def _closest_symbol_guess(base: str) -> Optional[str]:
    """
    base가 없을 때 유사 후보를 v2 목록에서 찾아준다.
    규칙:
      1) 같은 quote(USDT) & 심볼이 같은 접두(3~4자)로 시작
      2) 가장 짧은 편차 우선
    """
    if not AUTO_FUZZY_SYMBOL: return None
    pref = base.replace("USDT", "")
    if len(pref) < 3: return None
    cand: list[Tuple[int, str]] = []
    for k in _PROD.keys():
        if not k.endswith("USDT"): continue
        if _PROD[k].get("quoteCoin") != "USDT": continue
        if k.startswith(pref): 
            cand.append((0, k))
        elif k.startswith(pref[:4]): 
            cand.append((1, k))
        elif k.startswith(pref[:3]): 
            cand.append((2, k))
    if not cand: return None
    cand.sort(key=lambda x: x[0])
    best = cand[0][1]
    if best and best != base:
        try: send_telegram(f"[SPOT] alias auto map {base} -> {best}")
        except Exception: pass
    return best

def get_symbol_spec_spot(symbol: str) -> Dict[str, float]:
    _ensure_products()
    base = convert_symbol(symbol)
    spec = _PROD.get(base) or _PROD.get(f"{base}_SPBL")
    if not spec:
        guess = _closest_symbol_guess(base)
        if guess:
            ALIASES[base] = guess  # 캐시에 올려 재사용
            spec = _PROD.get(guess) or _PROD.get(f"{guess}_SPBL")
    if not spec:
        spec = {"qtyStep":1e-6, "priceStep":1e-6, "minQuote":1.0, "tradable":True, "baseCoin":"", "quoteCoin":"USDT"}
        _PROD[base] = spec
    return spec

def is_tradable(symbol: str) -> bool:
    if is_symbol_removed(symbol): return False
    return bool(get_symbol_spec_spot(symbol).get("tradable", True))

# 주문·틱커에서 사용할 문자열 결정
def _spot_symbol(sym: str) -> str:
    """
    원칙:
      - BASE_ONLY_SYMBOLS에 있으면 base 그대로 (PRIMEUSDT 등)
      - 아니면 *_SPBL
      - 단, base가 v2 목록에 존재하면 base 그대로 사용해도 OK (호환성)
    """
    _ensure_products()
    base = convert_symbol(sym)
    if base in BASE_ONLY or base in _PROD:
        return base
    return base if base.endswith("_SPBL") else f"{base}_SPBL"

# ------------------------------------------------
# ticker
_SPOT_TICKER_CACHE: Dict[str, Tuple[float, float]] = {}

def get_last_price_spot(symbol: str, retries: int = 5, sleep_base: float = 0.18) -> Optional[float]:
    sym = _spot_symbol(symbol)
    c = _SPOT_TICKER_CACHE.get(sym)
    now = time.time()
    if c and now - c[0] <= SPOT_TICKER_TTL: return float(c[1])
    path = f"/api/spot/v1/market/ticker?symbol={sym}"
    for i in range(retries):
        try:
            _rl("spot_ticker", 0.06)
            r = requests.get(BASE_URL + path, timeout=10)
            if r.status_code != 200: time.sleep(sleep_base*(2**i)); continue
            j = r.json()
            d = j.get("data")
            px = None
            if isinstance(d, dict): px = d.get("close") or d.get("last")
            elif isinstance(d, list) and d and isinstance(d[0], dict): px = d[0].get("close") or d[0].get("last")
            if px:
                v = float(px); 
                if v>0: _SPOT_TICKER_CACHE[sym]=(time.time(), v); return v
        except Exception: pass
        time.sleep(sleep_base*(2**i))
    return None

# ------------------------------------------------
# balances
_BAL_TS=0.0
_BAL: Dict[str, float] = {}

def _fetch_assets_v2(coin: Optional[str]=None) -> Dict[str, float]:
    path = "/api/v2/spot/account/assets"
    if coin: path += f"?coin={coin}"
    _rl("spot_bal_v2", 0.15)
    r = requests.get(BASE_URL + path, headers=_headers("GET", path, ""), timeout=12)
    j = r.json()
    arr = j.get("data") or []
    m: Dict[str, float] = {}
    for it in arr:
        c = _norm(it.get("coin") or "")
        if not c: continue
        m[c] = _to_float(it.get("available"), 0.0)
    return m

def _fetch_assets_v1() -> Dict[str, float]:
    path = "/api/spot/v1/account/assets"
    _rl("spot_bal_v1", 0.15)
    r = requests.get(BASE_URL + path, headers=_headers("GET", path, ""), timeout=12)
    j = r.json(); arr = j.get("data") or []
    m={}
    for it in arr:
        c=_norm(it.get("coin") or ""); 
        if not c: continue
        m[c]=_to_float(it.get("available"),0.0)
    return m

def get_spot_balances(force=False, coin: Optional[str]=None) -> Dict[str, float]:
    global _BAL_TS, _BAL
    now = time.time()
    if not force and not coin and _BAL and now - _BAL_TS < 5.0: return _BAL
    try:
        if coin:
            return _fetch_assets_v2(coin)
        m = _fetch_assets_v2(None) or _fetch_assets_v1()
        if m: _BAL, _BAL_TS = m, now
        return m or _BAL
    except Exception:
        return _BAL

def get_spot_free_qty(symbol: str, fresh=False) -> float:
    base = convert_symbol(symbol).replace("USDT","")
    if fresh:
        m = get_spot_balances(force=True, coin=base); return float(m.get(base, 0.0))
    m = get_spot_balances(); return float(m.get(base, 0.0))

# ------------------------------------------------
# orders
def _extract_code_text(resp_text: str) -> Dict[str, str]:
    try:
        j = json.loads(resp_text)
        if isinstance(j, dict): return {"code": str(j.get("code","")), "msg": str(j.get("msg",""))}
    except Exception: pass
    m = re.search(r'"code"\s*:\s*"?(?P<code>\d+)"?', resp_text or "")
    n = re.search(r'"msg"\s*:\s*"(?P<msg>[^"]+)"', resp_text or "")
    return {"code": m.group("code") if m else "", "msg": n.group("msg") if n else ""}

def _fmt_by_step(v: float, step: float) -> str:
    if step<=0: return f"{v:.6f}".rstrip("0").rstrip(".")
    k = math.floor(v/step)*step
    scale = max(0, int(round(-math.log10(step))))
    s = f"{k:.{scale}f}"
    return s.rstrip("0").rstrip(".") if "." in s else s

def place_spot_market_buy(symbol: str, usdt_amount: float) -> Dict:
    if not is_tradable(symbol): mark_symbol_removed(symbol); return {"code":"LOCAL_SYMBOL_REMOVED","msg":"symbol not tradable/removed"}
    spec = get_symbol_spec_spot(symbol)
    if float(usdt_amount) < float(spec.get("minQuote",1.0)):
        return {"code":"LOCAL_MIN_QUOTE","msg":f"need>={spec.get('minQuote',1.0)}USDT"}
    sym = _spot_symbol(symbol)
    path="/api/spot/v1/trade/orders"
    body={"symbol":sym,"side":"buy","orderType":"market","force":"gtc","quantity":_fmt_by_step(float(usdt_amount), 0.000001)}
    bj=json.dumps(body)
    try:
        _rl("spot_order",0.12)
        res = requests.post(BASE_URL+path, headers=_headers("POST",path,bj), data=bj, timeout=15)
        if res.status_code!=200:
            info=_extract_code_text(res.text or "")
            if info.get("code") in ("40309","40034"): mark_symbol_removed(symbol)
            return {"code":f"HTTP_{res.status_code}","msg":res.text}
        return res.json()
    except Exception as e:
        return {"code":"LOCAL_EXCEPTION","msg":str(e)}

def place_spot_market_sell_qty(symbol: str, qty: float) -> Dict:
    if qty<=0: return {"code":"LOCAL_BAD_QTY","msg":"qty<=0"}
    if not is_tradable(symbol): mark_symbol_removed(symbol); return {"code":"LOCAL_SYMBOL_REMOVED","msg":"symbol not tradable/removed"}
    step = float(get_symbol_spec_spot(symbol).get("qtyStep", 1e-6))
    qty_str=_fmt_by_step(float(qty), step)
    sym=_spot_symbol(symbol)
    path="/api/spot/v1/trade/orders"
    body={"symbol":sym,"side":"sell","orderType":"market","force":"gtc","quantity":qty_str}
    bj=json.dumps(body)
    try:
        _rl("spot_order",0.12)
        res=requests.post(BASE_URL+path, headers=_headers("POST",path,bj), data=bj, timeout=15)
        if res.status_code==200: return res.json()
        txt=res.text or ""; info=_extract_code_text(txt)
        if info.get("code") in ("40309","40034"): mark_symbol_removed(symbol); return {"code":f"HTTP_{res.status_code}","msg":txt}
        m=re.search(r"(?:checkBDScale|checkScale)[\"']?\s*[:=]\s*([0-9]+)", txt)
        if res.status_code==400 and m:
            chk=int(m.group(1)); step2=10**(-chk)
            qty_str2=_fmt_by_step(float(qty), step2)
            bj2=json.dumps({**body,"quantity":qty_str2})
            try: send_telegram(f"[SPOT] retry sell {symbol} scale->{chk} qty={qty_str2}")
            except Exception: pass
            _rl("spot_order",0.12)
            res2=requests.post(BASE_URL+path, headers=_headers("POST",path,bj2), data=bj2, timeout=15)
            if res2.status_code==200: return res2.json()
            return {"code":f"HTTP_{res2.status_code}","msg":res2.text,"retry_with_scale":chk}
        return {"code":f"HTTP_{res.status_code}","msg":txt}
    except Exception as e:
        return {"code":"LOCAL_EXCEPTION","msg":str(e)}
