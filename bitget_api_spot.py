# bitget_api_spot.py
import os, time, json, hmac, hashlib, base64, requests, math, re
from typing import Dict, Optional, Tuple

BASE_URL = "https://api.bitget.com"

API_KEY        = os.getenv("BITGET_API_KEY", "")
API_SECRET     = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE = os.getenv("BITGET_API_PASSWORD", "")

REMOVED_BLOCK_TTL = int(os.getenv("REMOVED_BLOCK_TTL", "43200"))
SPOT_TICKER_TTL   = float(os.getenv("SPOT_TICKER_TTL", "2.5"))

# alias + base-only
ALIASES: Dict[str, str] = {}
_alias_env = os.getenv("SYMBOL_ALIASES_JSON", "")
if _alias_env:
    try: ALIASES.update(json.loads(_alias_env))
    except Exception: pass

BASE_ONLY = set()
_bo = os.getenv("BASE_ONLY_SYMBOLS", "")
if _bo: BASE_ONLY = {s.strip().upper() for s in _bo.split(",") if s.strip()}

# telegram
try:
    from telegram_spot_bot import send_telegram
except Exception:
    def send_telegram(_): pass

# -------------------------
# small rate-limit
_last_call: Dict[str, float] = {}
def _rl(key: str, min_interval: float = 0.08):
    now = time.time()
    prev = _last_call.get(key, 0.0)
    wait = min_interval - (now - prev)
    if wait > 0: time.sleep(wait)
    _last_call[key] = time.time()

# auth
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

# -------------------------
# normalize
def _norm(s: str) -> str:
    return (s or "").upper().replace("/", "").replace("-", "").replace("_", "")

def convert_symbol(sym: str) -> str:
    s = _norm(sym)
    if s.endswith("PERP"): s = s[:-4]
    return ALIASES.get(s, s)

# -------------------------
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

# -------------------------
# v2 products cache
_PROD_TS = 0.0
_PROD: Dict[str, Dict] = {}

def _to_float(x, d=0.0):
    try:
        if x is None: return d
        if isinstance(x,(int,float)): return float(x)
        s = str(x).strip()
        if s in ("","null"): return d
        return float(s)
    except: return d

def _refresh_products_cache_v2():
    global _PROD_TS, _PROD
    path = "/api/v2/spot/public/symbols"
    try:
        _rl("products_v2",0.15)
        r = requests.get(BASE_URL+path, timeout=12)
        j = r.json()
        arr = j.get("data") or []
        m={}
        for it in arr:
            sym = _norm(it.get("symbol") or "")
            if not sym: continue
            qty_p   = int(_to_float(it.get("quantityPrecision"),6))
            price_p = int(_to_float(it.get("pricePrecision"),6))
            min_qt  = _to_float(it.get("minTradeUSDT"), _to_float(it.get("minTradeAmount"),1.0))
            tradable= (str(it.get("status")).lower() in ("online","enable","enabled","true","tradable"))
            m[sym] = {
                "qtyStep":10**(-qty_p),
                "priceStep":10**(-price_p),
                "minQuote":min_qt if min_qt>0 else 1.0,
                "tradable":bool(tradable)
            }
            m[f"{sym}_SPBL"]=m[sym]
        _PROD = m; _PROD_TS = time.time()
    except Exception as e:
        print("spot products v2 refresh fail:",e)

def _ensure_products():
    if not _PROD or time.time()-_PROD_TS>600:
        _refresh_products_cache_v2()

def get_symbol_spec_spot(symbol: str) -> Dict[str,float]:
    _ensure_products()
    base = convert_symbol(symbol)
    return _PROD.get(base) or _PROD.get(f"{base}_SPBL") or {"qtyStep":1e-6,"priceStep":1e-6,"minQuote":1.0,"tradable":True}

def is_tradable(symbol: str) -> bool:
    if is_symbol_removed(symbol): return False
    return bool(get_symbol_spec_spot(symbol).get("tradable",True))

# -------------------------
# order/ticker symbol formatting
def _spot_symbol(sym: str) -> str:
    base = convert_symbol(sym)
    _ensure_products()
    if base in BASE_ONLY or base in _PROD:
        return base
    return f"{base}_SPBL"

# -------------------------
# ticker
_SPOT_TICKER_CACHE: Dict[str,Tuple[float,float]] = {}
def get_last_price_spot(symbol: str) -> Optional[float]:
    sym=_spot_symbol(symbol)
    c=_SPOT_TICKER_CACHE.get(sym); now=time.time()
    if c and now-c[0]<=SPOT_TICKER_TTL: return c[1]
    path=f"/api/spot/v1/market/ticker?symbol={sym}"
    try:
        _rl("spot_ticker",0.06)
        r=requests.get(BASE_URL+path,timeout=10)
        if r.status_code!=200: return None
        j=r.json(); d=j.get("data")
        px=None
        if isinstance(d,dict): px=d.get("close") or d.get("last")
        elif isinstance(d,list) and d and isinstance(d[0],dict): px=d[0].get("close") or d[0].get("last")
        if px: 
            v=float(px); _SPOT_TICKER_CACHE[sym]=(time.time(),v); return v
    except: pass
    return None

# -------------------------
# balances
_BAL_TS=0.0; _BAL:Dict[str,float]={}
def _fetch_assets_v2() -> Dict[str,float]:
    path="/api/v2/spot/account/assets"
    _rl("spot_bal_v2",0.15)
    r=requests.get(BASE_URL+path,headers=_headers("GET",path,""),timeout=12)
    j=r.json(); arr=j.get("data") or []
    m={}
    for it in arr:
        c=_norm(it.get("coin") or "")
        m[c]=_to_float(it.get("available"),0.0)
    return m

def get_spot_balances(force=False) -> Dict[str,float]:
    global _BAL,_BAL_TS
    now=time.time()
    if not force and _BAL and now-_BAL_TS<5: return _BAL
    try:
        m=_fetch_assets_v2()
        if m: _BAL,_BAL_TS=m,now
        return m or _BAL
    except: return _BAL

def get_spot_free_qty(symbol: str, fresh=False) -> float:
    base=convert_symbol(symbol).replace("USDT","")
    m=get_spot_balances(force=fresh)
    return float(m.get(base,0.0))

# -------------------------
# error helper
def _extract_code_text(txt:str)->Dict[str,str]:
    try:
        j=json.loads(txt); 
        if isinstance(j,dict): return {"code":str(j.get("code","")),"msg":str(j.get("msg",""))}
    except: pass
    return {"code":"","msg":txt}

def _fmt_by_step(v:float, step:float)->str:
    if step<=0: return f"{v:.6f}".rstrip("0").rstrip(".")
    k=math.floor(v/step)*step
    scale=max(0,int(round(-math.log10(step))))
    s=f"{k:.{scale}f}"
    return s.rstrip("0").rstrip(".") if "." in s else s

# -------------------------
# orders
def place_spot_market_buy(symbol:str,usdt_amount:float)->Dict:
    if not is_tradable(symbol): mark_symbol_removed(symbol); return {"code":"LOCAL_SYMBOL_REMOVED","msg":"symbol not tradable/removed"}
    spec=get_symbol_spec_spot(symbol)
    if usdt_amount<spec.get("minQuote",1.0): return {"code":"LOCAL_MIN_QUOTE","msg":f"need>={spec.get('minQuote',1.0)}USDT"}
    sym=_spot_symbol(symbol)
    path="/api/spot/v1/trade/orders"
    body={"symbol":sym,"side":"buy","orderType":"market","force":"gtc","quantity":_fmt_by_step(usdt_amount,1e-6)}
    bj=json.dumps(body)
    try:
        _rl("spot_order",0.12)
        r=requests.post(BASE_URL+path,headers=_headers("POST",path,bj),data=bj,timeout=15)
        if r.status_code!=200: return {"code":f"HTTP_{r.status_code}","msg":r.text}
        return r.json()
    except Exception as e:
        return {"code":"LOCAL_EXCEPTION","msg":str(e)}

def place_spot_market_sell_qty(symbol:str,qty:float)->Dict:
    if qty<=0: return {"code":"LOCAL_BAD_QTY","msg":"qty<=0"}
    if not is_tradable(symbol): mark_symbol_removed(symbol); return {"code":"LOCAL_SYMBOL_REMOVED","msg":"symbol not tradable/removed"}
    step=get_symbol_spec_spot(symbol).get("qtyStep",1e-6)
    qty_str=_fmt_by_step(qty,step)
    sym=_spot_symbol(symbol)
    path="/api/spot/v1/trade/orders"
    body={"symbol":sym,"side":"sell","orderType":"market","force":"gtc","quantity":qty_str}
    bj=json.dumps(body)
    try:
        _rl("spot_order",0.12)
        r=requests.post(BASE_URL+path,headers=_headers("POST",path,bj),data=bj,timeout=15)
        if r.status_code==200: return r.json()
        return {"code":f"HTTP_{r.status_code}","msg":r.text}
    except Exception as e:
        return {"code":"LOCAL_EXCEPTION","msg":str(e)}
