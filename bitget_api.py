# -*- coding: utf-8 -*-
"""
bitget.py  (USDT-M Perp 전용)
- v2 우선, v1 폴백
- 강력한 심볼 정규화
- 티커 체인: v2 ticker(단일) → v2 tickers(목록) → v1 ticker → v2/v1 mark → depth mid → 1m candle
- 원웨이 모드/레버리지/수량 반올림
환경변수:
  BITGET_API_KEY, BITGET_API_SECRET, BITGET_API_PASSWORD  (필수)
  BITGET_USE_V2=1
  BITGET_V2_PRODUCT_TYPE=USDT-FUTURES
  STRICT_TICKER=0
  ALLOW_DEPTH_FALLBACK=1
  SYMBOL_ALIASES_JSON={"HUSDT":"HFTUSDT"}   # (선택)
  TRACE_LOG=1
"""
import os, time, hmac, hashlib, base64, json, math, re, threading
from typing import Dict, Any, Optional, Tuple
import requests

BITGET_HOST = "https://api.bitget.com"

API_KEY     = os.getenv("BITGET_API_KEY", "")
API_SECRET  = os.getenv("BITGET_API_SECRET", "")
API_PASS    = os.getenv("BITGET_API_PASSWORD", "")

USE_V2      = os.getenv("BITGET_USE_V2", "1") == "1"
PRODUCT_V2  = os.getenv("BITGET_V2_PRODUCT_TYPE", "USDT-FUTURES")
STRICT_TICKER = os.getenv("STRICT_TICKER", "0") == "1"
ALLOW_DEPTH_FALLBACK = os.getenv("ALLOW_DEPTH_FALLBACK", "1") == "1"
TRACE_LOG   = os.getenv("TRACE_LOG", "0") == "1"

try:
    SYMBOL_ALIASES = json.loads(os.getenv("SYMBOL_ALIASES_JSON", "{}"))
    if not isinstance(SYMBOL_ALIASES, dict):
        SYMBOL_ALIASES = {}
except Exception:
    SYMBOL_ALIASES = {}

_contract_lock = threading.Lock()
_contract_cache: Dict[str, Dict[str, Any]] = {}
_contract_ttl = 60 * 30
_contract_last_ts = 0.0

def _ts() -> str:
    return str(int(time.time() * 1000))

def _log(*a):
    if TRACE_LOG:
        print("[bitget]", *a, flush=True)

def convert_symbol(sym: str) -> str:
    s = (sym or "").upper().strip()
    if not s:
        return s
    if s in SYMBOL_ALIASES:
        return SYMBOL_ALIASES[s].upper().strip()
    s = re.sub(r'^(BINANCE|BITGET|BYBIT|OKX|HUOBI|KUCOIN|MEXC|GATE|DERIBIT|FTX)[:/._-]+', '', s)
    s = re.sub(r'(_|-)?(U|C)MCBL$', '', s)
    s = re.sub(r'(\.P|_PERP|-PERP|PERP|_SWAP|-SWAP|SWAP)$', '', s)
    s = re.sub(r'[:/._-]+', '', s)
    if s.endswith("USD"):
        s = s + "T"
    s = re.sub(r'USDT(P|PERP|PS)?$', 'USDT', s)
    m = re.search(r'([A-Z0-9]{2,})USDT$', s)
    if m:
        return m.group(1) + "USDT"
    if re.fullmatch(r'[A-Z0-9]{2,10}', s):
        return s + "USDT"
    return s

def _sign(ts: str, method: str, path: str, body: str = "") -> str:
    pre = ts + method.upper() + path + body
    mac = hmac.new(API_SECRET.encode(), pre.encode(), hashlib.sha256).digest()
    return base64.b64encode(mac).decode()

def _q(params: Optional[dict]) -> str:
    if not params: return ""
    return "&".join(f"{k}={params[k]}" for k in sorted(params.keys()))

def _req(method: str, path: str, params: Optional[dict]=None, body: Optional[dict]=None,
         auth: bool=False, v2: bool=True, timeout: int=10):
    url = BITGET_HOST + path
    headers = {"Content-Type": "application/json"}
    data = ""
    if body: data = json.dumps(body, separators=(",", ":"))
    if auth:
        ts = _ts()
        sig = _sign(ts, method, path + (("?" + _q(params)) if params else ""), data)
        headers.update({
            "ACCESS-KEY": API_KEY,
            "ACCESS-SIGN": sig,
            "ACCESS-TIMESTAMP": ts,
            "ACCESS-PASSPHRASE": API_PASS
        })
    try:
        if method.upper() == "GET":
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
        else:
            r = requests.post(url, params=params, data=data or None, headers=headers, timeout=timeout)
        try:
            return r.status_code, r.json()
        except Exception:
            return r.status_code, {"raw": r.text}
    except requests.RequestException as e:
        return 599, {"error": str(e)}

def refresh_contracts_cache(force: bool=False):
    global _contract_last_ts, _contract_cache
    now = time.time()
    with _contract_lock:
        if not force and (now - _contract_last_ts) < _contract_ttl and _contract_cache:
            return
        if USE_V2:
            sc, js = _req("GET", "/api/v2/mix/market/contracts",
                          params={"productType": PRODUCT_V2}, v2=True)
            if sc == 200 and js.get("code") == "00000":
                bag = {}
                for it in js.get("data", []):
                    sym = it.get("symbol")
                    if sym: bag[sym] = it
                _contract_cache = {"UMCBL": bag}
                _contract_last_ts = now
                _log("contracts v2 cached:", len(bag))
                return
        sc, js = _req("GET", "/api/mix/v1/market/contracts",
                      params={"productType": "UMCBL"}, v2=False)
        if sc == 200 and js.get("code") == "00000":
            bag = {}
            for it in js.get("data", []):
                full = it.get("symbol", "")
                if full.endswith("_UMCBL"):
                    bag[full.replace("_UMCBL", "")] = it
            _contract_cache = {"UMCBL": bag}
            _contract_last_ts = now
            _log("contracts v1 cached:", len(bag))

def _contract(sym: str) -> Optional[dict]:
    if not _contract_cache:
        refresh_contracts_cache()
    return _contract_cache.get("UMCBL", {}).get(sym)

def _size_tick(sym: str) -> float:
    c = _contract(sym)
    if not c: return 0.0001
    tick = c.get("sizeTick")
    if tick: 
        try: return float(tick)
        except: pass
    if "sizePlace" in c:
        return float(f"1e-{int(c['sizePlace'])}")
    if "minTradeNum" in c:
        try: return float(c["minTradeNum"])
        except: pass
    return 0.0001

def _price_tick(sym: str) -> float:
    c = _contract(sym)
    if not c: return 0.01
    tick = c.get("priceTick")
    if tick:
        try: return float(tick)
        except: pass
    if "pricePlace" in c:
        return float(f"1e-{int(c['pricePlace'])}")
    return 0.01

def round_size(sym: str, qty: float) -> float:
    tick = _size_tick(sym) or 0.0001
    return math.floor(float(qty) / tick) * tick

def round_price(sym: str, px: float) -> float:
    tick = _price_tick(sym) or 0.01
    return round(math.floor(float(px) / tick) * tick, 10)

def _sym_v1(sym: str) -> str:
    return f"{sym}_UMCBL"

def _ok(js: Any) -> bool:
    try: return js.get("code") == "00000"
    except: return False

class Bitget:
    def __init__(self):
        if not API_KEY or not API_SECRET or not API_PASS:
            raise RuntimeError("Bitget API credentials missing")
        refresh_contracts_cache(force=True)

    # ---------- Market ----------
    def last_price(self, sym: str) -> Optional[float]:
        """티커 체인(강화판)"""
        # v2 single ticker
        if USE_V2 and not STRICT_TICKER:
            sc, js = _req("GET", "/api/v2/mix/market/ticker",
                          params={"symbol": sym}, v2=True)
            if sc == 200 and _ok(js) and js.get("data"):
                try:
                    return float(js["data"]["lastPr"])
                except Exception:
                    pass
            # 일부 환경에서는 productType을 요구
            sc, js = _req("GET", "/api/v2/mix/market/ticker",
                          params={"productType": PRODUCT_V2, "symbol": sym}, v2=True)
            if sc == 200 and _ok(js) and js.get("data"):
                try:
                    return float(js["data"]["lastPr"])
                except Exception:
                    pass
            # v2 tickers (목록) 폴백
            sc, js = _req("GET", "/api/v2/mix/market/tickers",
                          params={"productType": PRODUCT_V2}, v2=True)
            if sc == 200 and _ok(js) and js.get("data"):
                try:
                    for row in js["data"]:
                        if row.get("symbol") == sym:
                            return float(row["lastPr"])
                except Exception:
                    pass

        # v1 ticker
        sc, js = _req("GET", "/api/mix/v1/market/ticker",
                      params={"symbol": _sym_v1(sym)}, v2=False)
        if sc == 200 and _ok(js) and js.get("data"):
            try:
                return float(js["data"]["last"])
            except Exception:
                pass

        # v2/v1 mark price
        if USE_V2:
            sc, js = _req("GET", "/api/v2/mix/market/mark-price",
                          params={"symbol": sym}, v2=True)
            if sc == 200 and _ok(js) and js.get("data"):
                try:
                    return float(js["data"]["markPrice"])
                except Exception:
                    pass
        sc, js = _req("GET", "/api/mix/v1/market/mark-price",
                      params={"symbol": _sym_v1(sym)}, v2=False)
        if sc == 200 and _ok(js) and js.get("data"):
            try:
                return float(js["data"]["markPrice"])
            except Exception:
                pass

        # depth mid
        if ALLOW_DEPTH_FALLBACK:
            if USE_V2:
                sc, js = _req("GET", "/api/v2/mix/market/merged-depth",
                              params={"symbol": sym, "limit": 1}, v2=True)
                if sc == 200 and _ok(js) and js.get("data"):
                    try:
                        b = float(js["data"]["bids"][0][0])
                        a = float(js["data"]["asks"][0][0])
                        return (b + a) / 2.0
                    except Exception:
                        pass
            sc, js = _req("GET", "/api/mix/v1/market/depth",
                          params={"symbol": _sym_v1(sym), "limit": 1}, v2=False)
            if sc == 200 and _ok(js) and js.get("data"):
                try:
                    b = float(js["data"]["bids"][0][0]); a = float(js["data"]["asks"][0][0])
                    return (b + a) / 2.0
                except Exception:
                    pass

        # 1m candle close
        if USE_V2:
            sc, js = _req("GET", "/api/v2/mix/market/candles",
                          params={"symbol": sym, "granularity": "60", "limit": "1"}, v2=True)
            if sc == 200 and _ok(js) and js.get("data"):
                try:
                    return float(js["data"][0][4])
                except Exception:
                    pass
        sc, js = _req("GET", "/api/mix/v1/market/candles",
                      params={"symbol": _sym_v1(sym), "granularity": "60", "limit": "1"}, v2=False)
        if sc == 200 and _ok(js) and js.get("data"):
            try:
                return float(js["data"][0][4])
            except Exception:
                pass
        return None

    # ---------- Account / Mode ----------
    def ensure_one_way(self) -> bool:
        try:
            if USE_V2:
                sc, js = _req("POST", "/api/v2/mix/account/set-position-mode",
                              body={"productType": PRODUCT_V2, "positionMode": "one_way"},
                              auth=True, v2=True)
                if sc == 200 and _ok(js): return True
            sc, js = _req("POST", "/api/mix/v1/account/setPositionMode",
                          body={"productType": "UMCBL", "marginCoin": "USDT", "positionMode": "one_way"},
                          auth=True, v2=False)
            return sc == 200 and _ok(js)
        except Exception as e:
            _log("ensure_one_way err:", e)
            return False

    def set_leverage(self, sym: str, leverage: int = 5) -> bool:
        ok = False
        if USE_V2:
            try:
                for hold in ("long", "short"):
                    sc, js = _req("POST", "/api/v2/mix/account/set-leverage",
                                  body={"symbol": sym, "leverage": str(leverage), "holdSide": hold},
                                  auth=True, v2=True)
                    ok = ok or (sc == 200 and _ok(js))
            except Exception:
                pass
        try:
            for hold in ("long", "short"):
                sc, js = _req("POST", "/api/mix/v1/account/setLeverage",
                              body={"symbol": _sym_v1(sym), "marginCoin": "USDT",
                                    "leverage": str(leverage), "holdSide": hold},
                              auth=True, v2=False)
                ok = ok or (sc == 200 and _ok(js))
        except Exception:
            pass
        return ok

    # ---------- Positions ----------
    def position_size(self, sym: str) -> Tuple[float, float]:
        if USE_V2:
            sc, js = _req("GET", "/api/v2/mix/position/single-position",
                          params={"symbol": sym}, auth=True, v2=True)
            if sc == 200 and _ok(js) and js.get("data"):
                try:
                    d = js["data"]
                    ls = float(d.get("total", {}).get("longQty", "0"))
                    ss = float(d.get("total", {}).get("shortQty", "0"))
                    return ls, ss
                except Exception:
                    pass
        sc, js = _req("GET", "/api/mix/v1/position/singlePosition",
                      params={"symbol": _sym_v1(sym), "marginCoin": "USDT"},
                      auth=True, v2=False)
        if sc == 200 and _ok(js) and js.get("data"):
            try:
                d = js["data"]
                ls = float(d.get("long", {}).get("total", "0") or 0)
                ss = float(d.get("short", {}).get("total", "0") or 0)
                return ls, ss
            except Exception:
                pass
        return 0.0, 0.0

    # ---------- Orders ----------
    def place_market(self, sym: str, side: str, size: float, reduce_only: bool=False):
        size = float(size)
        if size <= 0: return False, {"error": "size<=0"}
        if USE_V2:
            try:
                trade_side = "open" if not reduce_only else "close"
                order_side = "buy" if side.lower().startswith("b") else "sell"
                body = {
                    "symbol": sym,
                    "marginCoin": "USDT",
                    "size": str(size),
                    "side": order_side,
                    "tradeSide": trade_side,
                    "orderType": "market",
                    "force": "gtc"
                }
                sc, js = _req("POST", "/api/v2/mix/order/place-order",
                              body=body, auth=True, v2=True)
                if sc == 200 and _ok(js):
                    return True, js
            except Exception as e:
                _log("place_market v2 err:", e)
        body = {
            "symbol": _sym_v1(sym),
            "marginCoin": "USDT",
            "size": str(size),
            "side": "buy" if side.lower().startswith("b") else "sell",
            "orderType": "market",
            "timeInForceValue": "normal",
            "reduceOnly": reduce_only
        }
        sc, js = _req("POST", "/api/mix/v1/order/placeOrder", body=body, auth=True, v2=False)
        return (sc == 200 and _ok(js)), js
