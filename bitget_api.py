# -*- coding: utf-8 -*-
"""
Bitget REST API helper (USDT-M Perpetual)

공용 인터페이스(트레이더/메인과 호환):
  - convert_symbol(symbol) -> str
  - get_last_price(symbol) -> Optional[float]
  - get_open_positions() -> List[Dict]
  - place_market_order(symbol, usdt_amount, side, leverage, reduce_only=False) -> Dict
  - place_reduce_by_size(symbol, size, side) -> Dict  # (필요 시 사용)
  - get_symbol_spec(symbol) -> Dict
  - round_down_step(value, step) -> float

ENV (필수★ / 권장◇):
★ BITGET_API_KEY, BITGET_API_SECRET, BITGET_API_PASSWORD
◇ BITGET_BASE_URL=https://api.bitget.com
◇ BITGET_USE_V2=1
◇ BITGET_V2_PRODUCT_TYPE=USDT-FUTURES
◇ BITGET_V2_PRODUCT_TYPE_ALTS="COIN-FUTURES,USDC-FUTURES"
◇ BITGET_MARGIN_COIN=USDT
◇ BITGET_V2_TICKER_PATH=/api/v2/mix/market/ticker
◇ BITGET_V2_MARK_PATH=/api/v2/mix/market/mark-price
◇ BITGET_V2_DEPTH_PATH=/api/v2/mix/market/orderbook
◇ BITGET_V2_CANDLES_PATH=/api/v2/mix/market/candles
◇ BITGET_V2_INDEX_CANDLES_PATH=/api/v2/mix/market/index-candles
◇ BITGET_CANDLE_GRANULARITY=60

◇ BITGET_V2_PLACE_ORDER_PATH=/api/v2/mix/order/place-order
◇ BITGET_V2_POSITIONS_PATH=/api/v2/mix/position/get-all-position
◇ BITGET_V1_TICKER_PATH=/api/mix/v1/market/ticker
◇ BITGET_V1_PLACE_ORDER_PATH=/api/mix/v1/order/placeOrder
◇ BITGET_V1_POSITIONS_PATH=/api/mix/v1/position/allPosition
◇ POSITION_SYMBOLS_HINT="BTCUSDT,ETHUSDT,..."
◇ STRICT_TICKER=0, ALLOW_DEPTH_FALLBACK=1, TICKER_TTL=3
◇ SYMBOL_ALIASES_JSON='{"KAITOUSDT":"KAITOUSDT"}'
◇ TRACE_LOG=0
"""

from __future__ import annotations
import os, time, math, json, hmac, hashlib, base64
from typing import Any, Dict, Optional, Tuple, List
from urllib.parse import urlencode
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# ────────────────────────────────────────────────────────
# ENV
# ────────────────────────────────────────────────────────
BASE_URL  = os.getenv("BITGET_BASE_URL", "https://api.bitget.com")
API_KEY   = os.getenv("BITGET_API_KEY", "")
API_SEC   = os.getenv("BITGET_API_SECRET", "")
API_PASS  = os.getenv("BITGET_API_PASSWORD", "")

USE_V2               = os.getenv("BITGET_USE_V2", "1") == "1"
V2_TICKER_PATH       = os.getenv("BITGET_V2_TICKER_PATH", "/api/v2/mix/market/ticker")
V2_MARK_PATH         = os.getenv("BITGET_V2_MARK_PATH", "/api/v2/mix/market/mark-price")
V2_DEPTH_PATH        = os.getenv("BITGET_V2_DEPTH_PATH", "/api/v2/mix/market/orderbook")
V2_CANDLES_PATH      = os.getenv("BITGET_V2_CANDLES_PATH", "/api/v2/mix/market/candles")
V2_INDEX_CANDLES_PATH= os.getenv("BITGET_V2_INDEX_CANDLES_PATH", "/api/v2/mix/market/index-candles")
CANDLE_GRANULARITY   = int(os.getenv("BITGET_CANDLE_GRANULARITY", "60"))

V2_PLACE_ORDER_PATH  = os.getenv("BITGET_V2_PLACE_ORDER_PATH", "/api/v2/mix/order/place-order")
# 최신 문서 기준
V2_POSITIONS_PATH    = os.getenv("BITGET_V2_POSITIONS_PATH", "/api/v2/mix/position/get-all-position")
# 구 문서/계정 호환 폴백
V2_POSITIONS_PATH_FALLBACK = "/api/v2/mix/position/all-position"

# [PATCH] env가 잘못 들어와도 자동 보정: /api/mix/market/... → /api/mix/v1/market/...
def _ensure_v1_path(p: str) -> str:
    try:
        return p if "/v1/" in p else p.replace("/api/mix/", "/api/mix/v1/")
    except Exception:
        return p

# v1 경로들(정확 경로)  ※ 환경변수 값이 틀려도 보정
V1_TICKER_PATH       = _ensure_v1_path(os.getenv("BITGET_V1_TICKER_PATH", "/api/mix/v1/market/ticker"))
V1_MARK_PATH         = _ensure_v1_path(os.getenv("BITGET_V1_MARK_PATH",   "/api/mix/v1/market/mark-price"))
V1_DEPTH_PATH        = _ensure_v1_path(os.getenv("BITGET_V1_DEPTH_PATH",  "/api/mix/v1/market/depth"))
V1_CANDLES_PATH      = _ensure_v1_path(os.getenv("BITGET_V1_CANDLES_PATH","/api/mix/v1/market/candles"))
# [PATCH] v2 mark 대체(목록형 data)
V2_MARK_PATH_ALT     = "/api/v2/mix/market/mark-prices"

V1_PLACE_ORDER_PATH  = _ensure_v1_path(os.getenv("BITGET_V1_PLACE_ORDER_PATH", "/api/mix/v1/order/placeOrder"))
V1_POSITIONS_PATH    = _ensure_v1_path(os.getenv("BITGET_V1_POSITIONS_PATH", "/api/mix/v1/position/allPosition"))

V2_PRODUCT_TYPE      = os.getenv("BITGET_V2_PRODUCT_TYPE", "USDT-FUTURES")  # USDT-FUTURES / COIN-FUTURES / USDC-FUTURES
V2_PRODUCT_TYPE_ALTS = os.getenv("BITGET_V2_PRODUCT_TYPE_ALTS", "COIN-FUTURES,USDC-FUTURES")
MARGIN_COIN          = os.getenv("BITGET_MARGIN_COIN", "USDT")

STRICT_TICKER        = os.getenv("STRICT_TICKER", "0") == "1"
ALLOW_DEPTH_FALLBACK = os.getenv("ALLOW_DEPTH_FALLBACK", "1") == "1"
TICKER_TTL           = int(os.getenv("TICKER_TTL", "3"))

# 심볼 alias
try:
    SYMBOL_ALIASES = json.loads(os.getenv("SYMBOL_ALIASES_JSON", "") or "{}")
except Exception:
    SYMBOL_ALIASES = {}

TRACE = os.getenv("TRACE_LOG", "0") == "1"

# 유지보수 에러코드
MAINTENANCE_ERRORS = {"45001", "40725", "40808", "40015"}

# ────────────────────────────────────────────────────────
# HTTP 세션
# ────────────────────────────────────────────────────────
SESSION = requests.Session()
_retry = Retry(
    total=5, read=5, connect=5,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods={"GET","POST"},
    raise_on_status=False,
)
_adapter = HTTPAdapter(max_retries=_retry, pool_connections=50, pool_maxsize=100)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)
SESSION.headers.update({"User-Agent":"auto-trader/1.0","Connection":"keep-alive"})
DEFAULT_TIMEOUT = 12

def _log(msg: str):
    if TRACE: print(msg, flush=True)

def _ts_ms() -> str: return str(int(time.time()*1000))

def _sign(ts: str, method: str, path: str, query: str, body: str) -> str:
    prehash = f"{ts}{method}{path}{query}{body}"
    mac = hmac.new(API_SEC.encode(), prehash.encode(), hashlib.sha256).digest()
    return base64.b64encode(mac).decode()

def _headers(ts: str, sign: str) -> Dict[str,str]:
    return {
        "ACCESS-KEY": API_KEY,
        "ACCESS-SIGN": sign,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": API_PASS,
        "Content-Type": "application/json",
        "Locale":"en-US",
    }

# ── 유지보수 백오프 래퍼 ───────────────────────────────
def _is_maintenance(js_or_text) -> bool:
    try:
        js = js_or_text if isinstance(js_or_text, dict) else json.loads(js_or_text)
        code = str(js.get("code",""))
        return code in MAINTENANCE_ERRORS
    except Exception:
        return False

def _with_retry_maintenance(callable_fn, *args, **kwargs):
    max_try = 3
    for i in range(max_try):
        try:
            res = callable_fn(*args, **kwargs)
        except Exception as e:
            # [PATCH] 네트워크/HTTP 예외도 재시도
            time.sleep(0.5 + i*0.5)
            last_err = e
            continue
        # Response 객체인 경우
        if hasattr(res, "status_code"):
            if res.status_code == 200: return res
            if _is_maintenance(getattr(res,"text","") or "{}"):
                time.sleep(3 + i*2); continue
            return res
        # dict(JSON)인 경우
        if isinstance(res, dict) and _is_maintenance(res):
            time.sleep(3 + i*2); continue
        return res
    # 마지막 시도 결과/예외는 상위에서 처리
    return res if "res" in locals() else {"code":"-1","msg":str(last_err)}

# ── HTTP 래퍼 ─────────────────────────────────────────
def _http_get_raw(path: str, params: Dict[str,Any], need_auth: bool=False, timeout: float=DEFAULT_TIMEOUT):
    url = f"{BASE_URL}{path}"
    if params: url = f"{url}?{urlencode(params)}"
    if need_auth:
        ts = _ts_ms()
        sign = _sign(ts, "GET", path, f"?{urlencode(params)}", "")
        headers = _headers(ts, sign)
        r = SESSION.get(url, headers=headers, timeout=timeout)
    else:
        r = SESSION.get(url, timeout=timeout)
    return r

def _http_get(path: str, params: Dict[str,Any], need_auth: bool=False, timeout: float=DEFAULT_TIMEOUT) -> Dict[str,Any]:
    r = _http_get_raw(path, params, need_auth, timeout)
    r.raise_for_status()
    return r.json()

# [PATCH] 예외 없이 status와 JSON을 돌려주는 soft GET/POST
def _http_get_soft(path: str, params: Dict[str,Any], need_auth: bool=False, timeout: float=DEFAULT_TIMEOUT):
    r = _http_get_raw(path, params, need_auth, timeout)
    try:
        js = r.json()
    except Exception:
        js = {}
    return r.status_code, js, r.text

def _http_post(path: str, body: Dict[str,Any], need_auth: bool=True, timeout: float=DEFAULT_TIMEOUT) -> Dict[str,Any]:
    url = f"{BASE_URL}{path}"
    data = json.dumps(body, separators=(",",":"))
    if need_auth:
        ts = _ts_ms()
        sign = _sign(ts, "POST", path, "", data)
        headers = _headers(ts, sign)
    else:
        headers = {"Content-Type":"application/json"}
    r = SESSION.post(url, data=data, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()

def _http_post_soft(path: str, body: Dict[str,Any], need_auth: bool=True, timeout: float=DEFAULT_TIMEOUT):
    url = f"{BASE_URL}{path}"
    data = json.dumps(body, separators=(",",":"))
    if need_auth:
        ts = _ts_ms()
        sign = _sign(ts, "POST", path, "", data)
        headers = _headers(ts, sign)
    else:
        headers = {"Content-Type":"application/json"}
    r = SESSION.post(url, data=data, headers=headers, timeout=timeout)
    try:
        js = r.json()
    except Exception:
        js = {}
    return r.status_code, js, r.text

# ────────────────────────────────────────────────────────
# 심볼/스펙
# ────────────────────────────────────────────────────────
def convert_symbol(sym: str) -> str:
    s = (sym or "").upper().strip()
    s = SYMBOL_ALIASES.get(s, s)
    for suf in ("_UMCBL","-UMCBL","UMCBL","_CMCBL","-CMCBL","CMCBL"):
        if s.endswith(suf): s = s.replace(suf,"")
    return s

def _v2_product_types() -> List[str]:
    out, seen = [], set()
    for x in [V2_PRODUCT_TYPE] + [y.strip() for y in (V2_PRODUCT_TYPE_ALTS or "").split(",") if y.strip()]:
        if x and x not in seen: seen.add(x); out.append(x)
    return out or ["USDT-FUTURES"]

_spec_cache: Dict[str,Dict[str,Any]] = {}
def get_symbol_spec(symbol: str) -> Dict[str,Any]:
    sym = convert_symbol(symbol)
    sp = _spec_cache.get(sym)
    if sp: return sp
    sp = {"sizeStep":0.001, "priceStep":0.01}  # 필요시 거래소 메타 보강 가능
    _spec_cache[sym] = sp
    return sp

def round_down_step(v: float, step: float) -> float:
    if step <= 0: return v
    return math.floor(float(v)/float(step)) * float(step)

# ── 상장 심볼 캐시(옵션) ───────────────────────────────
CONTRACTS_PATH = os.getenv("BITGET_V2_CONTRACTS_PATH", "/api/v2/mix/market/contracts")
_contract_cache: Dict[str,set[str]] = {}
_contract_cache_ts = 0

def refresh_contracts_cache(ttl_sec: int = 600):
    global _contract_cache_ts
    now = time.time()
    if (now - _contract_cache_ts) < ttl_sec: return
    newmap: Dict[str,set[str]] = {}
    for pt in _v2_product_types():
        try:
            js = _http_get(CONTRACTS_PATH, {"productType": pt}, False)
            bag = {convert_symbol(row.get("symbol","")) for row in (js.get("data") or [])}
            if bag: newmap[pt] = bag
        except Exception as e:
            _log(f"contracts fetch fail {pt}: {e}")
    if newmap:
        _contract_cache.clear(); _contract_cache.update(newmap); _contract_cache_ts = now

def is_symbol_listed(symbol: str) -> bool:
    refresh_contracts_cache()
    s = convert_symbol(symbol)
    for bag in _contract_cache.values():
        if s in bag: return True
    return False

# ── Ticker Fallback 체인 ───────────────────────────────
_ticker_cache: Dict[str, Tuple[float,float]] = {}

def _cache_get(sym: str) -> Optional[float]:
    row = _ticker_cache.get(sym); 
    if not row: return None
    ts, px = row
    return px if (time.time() - ts) <= TICKER_TTL else None

def _cache_set(sym: str, px: float):
    _ticker_cache[sym] = (time.time(), float(px))

def _parse_px(js: Dict[str,Any]) -> Optional[float]:
    d = js.get("data") if isinstance(js, dict) else None
    if isinstance(d, dict):
        # [PATCH] v2 단일 티커의 lastPr 키까지 파싱
        for k in ("lastPr", "last", "close", "price"):
            v = d.get(k)
            if v not in (None,"","null"):
                try:
                    px = float(v)
                    if px > 0: return px
                except Exception: pass
        bid, ask = d.get("bestBid"), d.get("bestAsk")
        try:
            if bid not in (None,"") and ask not in (None,""):
                b, a = float(bid), float(ask)
                if b>0 and a>0: return (a+b)/2.0
        except Exception: pass
    return None

def _get_ticker_v2(sym: str, product: str) -> Optional[float]:
    # [PATCH] soft 호출로 400/404에도 다음 시도 진행
    sc, js, _ = _http_get_soft(V2_TICKER_PATH, {"symbol": sym}, False)
    if sc == 200:
        px = _parse_px(js)
        if px: return px
    sc, js, _ = _http_get_soft(V2_TICKER_PATH, {"productType": product, "symbol": sym}, False)
    if sc == 200:
        return _parse_px(js)
    _log(f"ticker v2 fail {sym}/{product}: {sc}")
    return None

def _get_mark_v2(sym: str, product: str) -> Optional[float]:
    sc, js, _ = _http_get_soft(V2_MARK_PATH, {"symbol": sym}, False)
    if sc == 200:
        d = js.get("data") or {}
        v = d.get("markPrice") or d.get("price")
        if v not in (None,"","null"):
            return float(v)
    # [PATCH] 대체 경로(목록). productType만 허용 → 심볼 매칭
    sc, js, _ = _http_get_soft(V2_MARK_PATH_ALT, {"productType": product}, False)
    if sc == 200:
        d = js.get("data") or []
        if isinstance(d, list):
            for row in d:
                if str(row.get("symbol","")).upper() == sym.upper():
                    v = row.get("markPrice") or row.get("price")
                    if v not in (None,"","null"):
                        return float(v)
    _log(f"mark v2/alt fail {sym}/{product}: {sc}")
    return None

def _get_depth_mid_v2(sym: str, product: str) -> Optional[float]:
    sc, js, _ = _http_get_soft(V2_DEPTH_PATH, {"symbol": sym}, False)
    if sc == 200:
        d = js.get("data") or {}
        if isinstance(d, list) and d:
            d = d[0] if isinstance(d[0], dict) else {}
        if isinstance(d, dict):
            best_ask = d.get("bestAsk")
            best_bid = d.get("bestBid")
            if not best_ask or not best_bid:
                asks = d.get("asks") or []
                bids = d.get("bids") or []
                if asks and isinstance(asks, list):
                    best_ask = asks[0][0] if isinstance(asks[0], (list,tuple)) else asks[0].get("price")
                if bids and isinstance(bids, list):
                    best_bid = bids[0][0] if isinstance(bids[0], (list,tuple)) else bids[0].get("price")
            if best_ask and best_bid:
                try:
                    return (float(best_ask)+float(best_bid))/2.0
                except Exception:
                    pass
    # productType 포함 재시도
    sc, js, _ = _http_get_soft(V2_DEPTH_PATH, {"productType": product, "symbol": sym}, False)
    if sc == 200:
        d = js.get("data") or {}
        best_ask = d.get("bestAsk") or (d.get("asks") or [{}])[0].get("price")
        best_bid = d.get("bestBid") or (d.get("bids") or [{}])[0].get("price")
        if best_ask and best_bid:
            try:
                return (float(best_ask)+float(best_bid))/2.0
            except Exception:
                pass
    _log(f"depth v2 fail {sym}/{product}: {sc}")
    return None

def _get_candle_close_v2(sym: str, product: str) -> Optional[float]:
    sc, js, _ = _http_get_soft(V2_CANDLES_PATH, {"symbol": sym, "granularity": CANDLE_GRANULARITY, "limit": 2}, False)
    if sc == 200:
        data = js.get("data") or []
        if data:
            row = data[-2] if len(data)>=2 else data[-1]
            close = (row[4] if isinstance(row,(list,tuple)) and len(row)>=5 else (row.get("close") if isinstance(row,dict) else None))
            if close not in (None,"","null"):
                return float(close)
    _log(f"candles v2 fail {sym}/{product}: {sc}")
    return None

def _get_index_candle_close_v2(sym: str, product: str) -> Optional[float]:
    sc, js, _ = _http_get_soft(V2_INDEX_CANDLES_PATH, {"symbol": sym, "granularity": CANDLE_GRANULARITY, "limit": 2}, False)
    if sc == 200:
        data = js.get("data") or []
        if data:
            row = data[-2] if len(data)>=2 else data[-1]
            close = (row[4] if isinstance(row,(list,tuple)) and len(row)>=5 else (row.get("close") if isinstance(row,dict) else None))
            if close not in (None,"","null"):
                return float(close)
    _log(f"index-candles v2 fail {sym}/{product}: {sc}")
    return None

def _get_ticker_v1(sym: str) -> Optional[float]:
    sc, js, _ = _http_get_soft(V1_TICKER_PATH, {"symbol": f"{sym}_UMCBL"}, False)  # _UMCBL 필수
    if sc == 200:
        return _parse_px(js)
    _log(f"ticker v1 fail {sym}: {sc}")
    return None

# v1 폴백(마크/딥스/캔들)
def _get_mark_v1(sym: str) -> Optional[float]:
    sc, js, _ = _http_get_soft(V1_MARK_PATH, {"symbol": f"{sym}_UMCBL"}, False)
    if sc == 200:
        d = js.get("data") or {}
        v = d.get("markPrice") or d.get("price")
        if v not in (None,"","null"): 
            return float(v)
    _log(f"mark v1 fail {sym}: {sc}")
    return None

def _get_depth_mid_v1(sym: str) -> Optional[float]:
    sc, js, _ = _http_get_soft(V1_DEPTH_PATH, {"symbol": f"{sym}_UMCBL", "limit": 1}, False)
    if sc == 200:
        d = js.get("data") or {}
        bids = d.get("bids") or []
        asks = d.get("asks") or []
        if bids and asks:
            try:
                b = float(bids[0][0]); a = float(asks[0][0])
                if b>0 and a>0: return (a+b)/2.0
            except Exception:
                pass
    _log(f"depth v1 fail {sym}: {sc}")
    return None

def _get_candle_close_v1(sym: str, granularity: int) -> Optional[float]:
    sc, js, _ = _http_get_soft(V1_CANDLES_PATH, {"symbol": f"{sym}_UMCBL","granularity": str(granularity),"limit":"2"}, False)
    if sc == 200:
        data = js.get("data") or []
        if data:
            row = data[-2] if len(data)>=2 else data[-1]
            close = row[4] if isinstance(row,(list,tuple)) and len(row)>=5 else (row.get("close") if isinstance(row,dict) else None)
            if close not in (None,"","null"): 
                return float(close)
    _log(f"candles v1 fail {sym}: {sc}")
    return None

def get_last_price(symbol: str) -> Optional[float]:
    symbol = convert_symbol(symbol)
    if not is_symbol_listed(symbol):
        _log(f"⚠️ {symbol} not in contracts cache (목록 캐시 기준)")

    cached = _cache_get(symbol)
    if cached: return cached

    if USE_V2:
        s = symbol
        for product in _v2_product_types():
            px = _get_ticker_v2(s, product)
            if px: _cache_set(symbol, px); return px
            px = _get_mark_v2(s, product)
            if px: _cache_set(symbol, px); return px
            if ALLOW_DEPTH_FALLBACK:
                px = _get_depth_mid_v2(s, product)
                if px: _cache_set(symbol, px); return px
            px = _get_candle_close_v2(s, product)
            if px: _cache_set(symbol, px); return px
            px = _get_index_candle_close_v2(s, product)
            if px: _cache_set(symbol, px); return px

        if not STRICT_TICKER:
            px = _get_ticker_v1(symbol)
            if px: _cache_set(symbol, px); return px
            px = _get_mark_v1(symbol)
            if px: _cache_set(symbol, px); return px
            if ALLOW_DEPTH_FALLBACK:
                px = _get_depth_mid_v1(symbol)
                if px: _cache_set(symbol, px); return px
            px = _get_candle_close_v1(symbol, CANDLE_GRANULARITY)
            if px: _cache_set(symbol, px); return px
        _log(f"❌ Ticker 실패(최종): {symbol} v2=True"); return None

    px = _get_ticker_v1(symbol)
    if px: _cache_set(symbol, px); return px
    _log(f"❌ Ticker 실패(최종): {symbol} v2=False"); return None

# ── 주문/감축 ──────────────────────────────────────────
def _api_side(side: str, reduce_only: bool) -> str:
    s = (side or "").lower()
    if s in ("buy","long"):  return "close_short" if reduce_only else "open_long"
    else:                    return "close_long" if reduce_only else "open_short"

def _order_size_from_usdt(symbol: str, usdt_amount: float) -> float:
    last = get_last_price(symbol)
    if not last or last<=0: return 0.0
    step = float(get_symbol_spec(symbol).get("sizeStep",0.001))
    size = float(usdt_amount) / float(last)
    return round_down_step(size, step)

def place_market_order(symbol: str, usdt_amount: float, side: str, leverage: float, reduce_only: bool=False) -> Dict[str,Any]:
    sym  = convert_symbol(symbol)
    size = _order_size_from_usdt(sym, float(usdt_amount))
    if size <= 0: raise RuntimeError(f"size_calc_fail {sym} amt={usdt_amount}")

    # (1) 기존 v2 포맷(네 로직 유지)
    body_v2_legacy = {
        "symbol": sym,
        "marginCoin": MARGIN_COIN,
        "side": _api_side(side, reduce_only),
        "orderType": "market",
        "timeInForceValue": "normal",
        "size": str(size),
        "price": "",
        "force": "gtc",
        "reduceOnly": reduce_only,
        "marginMode": "cross",
        "leverage": str(leverage),
    }
    sc, js, txt = _http_post_soft(V2_PLACE_ORDER_PATH, body_v2_legacy, True)
    if sc == 200 and (js.get("code") in ("00000","0",0,None) or js.get("data")):
        return js
    _log(f"place_order v2 legacy fail {sym}: {sc} {txt}")

    # (2) v2 신규 스펙(일부 계정)
    body_v2_new = {
        "symbol": sym,
        "marginCoin": MARGIN_COIN,
        "size": str(size),
        "side": ("buy" if str(side).lower() in ("buy","long") else "sell"),
        "tradeSide": ("close" if reduce_only else "open"),
        "orderType": "market",
        "force": "gtc",
        "marginMode": "cross",
        "leverage": str(leverage),
    }
    sc, js, txt = _http_post_soft(V2_PLACE_ORDER_PATH, body_v2_new, True)
    if sc == 200 and (js.get("code") in ("00000","0",0,None) or js.get("data")):
        return js
    _log(f"place_order v2 new fail {sym}: {sc} {txt}")

    # (3) v1 폴백
    body_v1 = {
        "symbol": f"{sym}_UMCBL",
        "marginCoin": MARGIN_COIN,
        "size": str(size),
        "side": ("buy" if str(side).lower() in ("buy","long") else "sell"),
        "orderType": "market",
        "timeInForceValue": "normal",
        "reduceOnly": reduce_only
    }
    sc, js, txt = _http_post_soft(V1_PLACE_ORDER_PATH, body_v1, True)
    if sc == 200 and (js.get("code") in ("00000","0",0,None) or js.get("data")):
        return js
    _log(f"place_order v1 fail {sym}: {sc} {txt}")
    # 모두 실패 시 그대로 응답 반환
    return {"code": str(sc), "msg": txt or "place_order_failed", "data": js}

# (선택) 사이즈로 감축 주문
def place_reduce_by_size(symbol: str, size: float, side: str) -> Dict[str,Any]:
    sym = convert_symbol(symbol)
    body = {
        "symbol": sym,
        "marginCoin": MARGIN_COIN,
        "side": _api_side(side, True),
        "orderType": "market",
        "timeInForceValue": "normal",
        "size": str(size),
        "price": "",
        "reduceOnly": True,
        "marginMode": "cross",
    }
    sc, js, txt = _http_post_soft(V2_PLACE_ORDER_PATH, body, True)
    if sc == 200 and (js.get("code") in ("00000","0",0,None) or js.get("data")):
        return js
    _log(f"reduce_by_size v2 fail {sym}: {sc} {txt}")
    return js or {"code":str(sc),"msg":txt}

# ── 포지션 조회 ────────────────────────────────────────
def _parse_positions_v2(js: Dict[str,Any]) -> List[Dict[str,Any]]:
    data = js.get("data") or []
    out: List[Dict[str,Any]] = []
    for row in data:
        try:
            sym  = convert_symbol(row.get("symbol",""))
            side = (row.get("holdSide") or "").lower()
            size = float(row.get("total",0) or 0)
            entry= float(row.get("averageOpenPrice",0) or 0)
            if size>0 and side in ("long","short"):
                out.append({"symbol":sym,"side":side,"size":size,"entry_price":entry})
        except Exception: pass
    return out

def _parse_positions_v1(js: Dict[str,Any]) -> List[Dict[str,Any]]:
    data = js.get("data") or []
    out: List[Dict[str,Any]] = []
    for row in data:
        try:
            sym = convert_symbol(row.get("symbol",""))
            for pos in row.get("positions") or []:
                side = (pos.get("holdSide") or "").lower()
                size = float(pos.get("total",0) or 0)
                entry= float(pos.get("averageOpenPrice",0) or 0)
                if size>0 and side in ("long","short"):
                    out.append({"symbol":sym,"side":side,"size":size,"entry_price":entry})
        except Exception: pass
    return out

def _get_positions_v2(params) -> Optional[Dict[str,Any]]:
    # 1차: 최신 경로
    res = _with_retry_maintenance(_http_get_raw, V2_POSITIONS_PATH, params, True)
    if res.status_code == 200:
        return res.json()
    if res.status_code in (400,404,405):
        # 2차: 구경로 폴백
        res2 = _with_retry_maintenance(_http_get_raw, V2_POSITIONS_PATH_FALLBACK, params, True)
        if res2.status_code == 200:
            return res2.json()
        _log(f"positions v2 fallback {res2.status_code} url: {BASE_URL}{V2_POSITIONS_PATH_FALLBACK}?{urlencode(params)} body: {res2.text}")
    else:
        _log(f"positions v2 {res.status_code} url: {BASE_URL}{V2_POSITIONS_PATH}?{urlencode(params)} body: {res.text}")
    return None

def get_open_positions() -> List[Dict[str,Any]]:
    if USE_V2:
        for product in _v2_product_types():
            for params in ({"productType":product}, {"productType":product, "marginCoin":MARGIN_COIN}):
                try:
                    js = _get_positions_v2(params)
                    if js: return _parse_positions_v2(js)
                except Exception as e:
                    _log(f"positions v2 error: {e} url: {BASE_URL}{V2_POSITIONS_PATH}?{urlencode(params)}")
        # (옵션) 단일 심볼 폴백
        hint = (os.getenv("POSITION_SYMBOLS_HINT") or "").strip()
        if hint:
            out: List[Dict[str,Any]] = []
            symbols = [convert_symbol(x) for x in hint.split(",") if x.strip()]
            for sym in symbols:
                for product in _v2_product_types():
                    try:
                        js = _http_get("/api/v2/mix/position/single-position",
                                       {"productType":product,"symbol":sym,"marginCoin":MARGIN_COIN}, True)
                        d = js.get("data") or {}
                        side = (d.get("holdSide") or "").lower()
                        size = float(d.get("total",0) or 0)
                        entry= float(d.get("averageOpenPrice",0) or 0)
                        if size>0 and side in ("long","short"):
                            out.append({"symbol":sym,"side":side,"size":size,"entry_price":entry})
                    except Exception: pass
            if out: return out

    # v1 폴백
    for params in ({"productType":"umcbl"}, {"productType":"umcbl","marginCoin":MARGIN_COIN}):
        try:
            res = _with_retry_maintenance(_http_get_raw, V1_POSITIONS_PATH, params, True)
            if res.status_code == 200:
                return _parse_positions_v1(res.json())
            _log(f"positions v1 {res.status_code} url: {BASE_URL}{V1_POSITIONS_PATH}?{urlencode(params)} body: {res.text}")
        except Exception as e:
            _log(f"positions v1 error: {e}")
    return []
