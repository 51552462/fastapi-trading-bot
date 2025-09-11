# bitget_api_spot.py
# ------------------------------------------------------------
# Bitget Spot API helper (V2)
# - V2 symbols cache (/api/v2/spot/public/symbols)
# - V2 place order (/api/v2/spot/trade/place-order)
# - V2 tickers (/api/v2/spot/market/tickers)
# - Assets V2 (/api/v2/spot/account/assets)
# - Aliases/Fuzzy symbol normalization
# - Min notional guard, scale retry, light rate-limit, Telegram notify
# ------------------------------------------------------------
import os
import re
import time
import json
import hmac
import math
import base64
import hashlib
from typing import Dict, Optional, Tuple, Any

import requests

BASE_URL = "https://api.bitget.com"

API_KEY        = os.getenv("BITGET_API_KEY", "")
API_SECRET     = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE = os.getenv("BITGET_API_PASSWORD", "")

REMOVED_BLOCK_TTL = int(os.getenv("REMOVED_BLOCK_TTL", "43200"))  # 12h
TICKER_TTL        = float(os.getenv("SPOT_TICKER_TTL", "2.5"))    # seconds
AUTO_FUZZY_SYMBOL = os.getenv("AUTO_FUZZY_SYMBOL", "1") == "1"

ALIASES: Dict[str, str] = {}
_alias_env = os.getenv("SYMBOL_ALIASES_JSON", "")
if _alias_env:
    try:
        ALIASES.update(json.loads(_alias_env))
    except Exception:
        pass

# Telegram (spot)
try:
    from telegram_spot_bot import send_telegram
except Exception:
    def send_telegram(_msg: str):
        pass

# ------------------------- light rate limiter -------------------------
_last_call: Dict[str, float] = {}
def _rl(key: str, min_interval: float = 0.08):
    now = time.time()
    prev = _last_call.get(key, 0.0)
    wait = min_interval - (now - prev)
    if wait > 0:
        time.sleep(wait)
    _last_call[key] = time.time()

# ----------------------------- auth -----------------------------
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

# --------------------------- normalize ---------------------------
def _norm(s: str) -> str:
    return (s or "").upper().replace("/", "").replace("-", "").replace("_", "")

def convert_symbol(sym: str) -> str:
    s = _norm(sym)
    if s.endswith("PERP"):
        s = s[:-4]
    return ALIASES.get(s, s)

# ---------------------- removed symbol cache ----------------------
_REMOVED: Dict[str, float] = {}

def mark_symbol_removed(symbol: str):
    _REMOVED[convert_symbol(symbol)] = time.time() + REMOVED_BLOCK_TTL

def is_symbol_removed(symbol: str) -> bool:
    b = convert_symbol(symbol)
    u = _REMOVED.get(b, 0.0)
    if not u:
        return False
    if time.time() > u:
        _REMOVED.pop(b, None)
        return False
    return True

# ----------------------- v2 products (spec) -----------------------
_PROD_TS = 0.0
_PROD: Dict[str, Dict[str, Any]] = {}  # symbol -> spec dict

def _to_float(x, d=0.0):
    try:
        if x is None:
            return d
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        if s in ("", "null"):
            return d
        return float(s)
    except Exception:
        return d

def _refresh_products_cache_v2():
    """GET /api/v2/spot/public/symbols : 전체 스펙 캐시"""
    global _PROD_TS, _PROD
    path = "/api/v2/spot/public/symbols"
    try:
        _rl("products_v2", 0.15)
        r = requests.get(BASE_URL + path, timeout=12)
        j = r.json()
        arr = j.get("data") or []
        m: Dict[str, Dict[str, Any]] = {}
        for it in arr:
            sym_base = _norm(it.get("symbol") or "")
            if not sym_base:
                continue
            qty_p   = int(_to_float(it.get("quantityPrecision"), 6))
            price_p = int(_to_float(it.get("pricePrecision"),  6))
            min_qt  = _to_float(it.get("minTradeUSDT"), _to_float(it.get("minTradeAmount"), 1.0))
            status  = str(it.get("status") or "").lower()
            tradable = status in ("online", "enable", "enabled", "true", "tradable")
            spec = {
                "qtyStep":   10 ** (-qty_p),
                "priceStep": 10 ** (-price_p),
                "minQuote":  min_qt if min_qt > 0 else 1.0,
                "tradable":  bool(tradable),
                "baseCoin":  _norm(it.get("baseCoin")  or ""),
                "quoteCoin": _norm(it.get("quoteCoin") or ""),
            }
            m[sym_base] = spec
        _PROD = m
        _PROD_TS = time.time()
    except Exception as e:
        print("spot products v2 refresh fail:", e)

def _ensure_products():
    if not _PROD or time.time() - _PROD_TS > 600:
        _refresh_products_cache_v2()

def _closest_symbol_guess(base: str) -> Optional[str]:
    """유사 심볼 추정(옵션) ex) MOEWUSDT → MOODENGUSDT"""
    if not AUTO_FUZZY_SYMBOL:
        return None
    pref = base.replace("USDT", "")
    if len(pref) < 3:
        return None
    best: Optional[str] = None
    best_score = 99
    for k in _PROD.keys():
        if not k.endswith("USDT"):
            continue
        if k.startswith(pref):
            score = 0
        elif k.startswith(pref[:4]):
            score = 1
        elif k.startswith(pref[:3]):
            score = 2
        else:
            continue
        if score < best_score:
            best_score = score
            best = k
            if best_score == 0:
                break
    if best and best != base:
        try:
            send_telegram(f"[SPOT] alias auto map {base} -> {best}")
        except Exception:
            pass
    return best

def get_symbol_spec_spot(symbol: str) -> Dict[str, float]:
    _ensure_products()
    base = convert_symbol(symbol)
    spec = _PROD.get(base)
    if not spec:
        guess = _closest_symbol_guess(base)
        if guess:
            ALIASES[base] = guess
            spec = _PROD.get(guess)
    if not spec:
        spec = {
            "qtyStep": 1e-6,
            "priceStep": 1e-6,
            "minQuote": 1.0,
            "tradable": True,
            "baseCoin": "",
            "quoteCoin": "USDT",
        }
        _PROD[base] = spec
    return spec

def is_tradable(symbol: str) -> bool:
    if is_symbol_removed(symbol):
        return False
    return bool(get_symbol_spec_spot(symbol).get("tradable", True))

# --------------------------- ticker (V2) ---------------------------
_TICKER_CACHE: Dict[str, Tuple[float, float]] = {}

def get_last_price_spot(symbol: str, retries: int = 4, sleep_base: float = 0.15) -> Optional[float]:
    base = convert_symbol(symbol)
    c = _TICKER_CACHE.get(base)
    now = time.time()
    if c and now - c[0] <= TICKER_TTL:
        return float(c[1])
    path = f"/api/v2/spot/market/tickers?symbol={base}"
    for i in range(retries):
        try:
            _rl("spot_ticker_v2", 0.06)
            r = requests.get(BASE_URL + path, timeout=10)
            if r.status_code != 200:
                time.sleep(sleep_base * (2 ** i))
                continue
            j = r.json()
            arr = j.get("data") or []
            px = None
            if isinstance(arr, list) and arr:
                d = arr[0]
                px = d.get("close") or d.get("last") or d.get("price")
            if px:
                v = float(px)
                if v > 0:
                    _TICKER_CACHE[base] = (time.time(), v)
                    return v
        except Exception:
            pass
        time.sleep(sleep_base * (2 ** i))
    return None

# -------------------------- balances (V2) --------------------------
_BAL_TS = 0.0
_BAL: Dict[str, float] = {}

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
        c = _norm(it.get("coin") or "")
        if not c:
            continue
        m[c] = _to_float(it.get("available"), 0.0)
    return m

def get_spot_balances(force: bool = False, coin: Optional[str] = None) -> Dict[str, float]:
    global _BAL_TS, _BAL
    now = time.time()
    if not force and not coin and _BAL and now - _BAL_TS < 5.0:
        return _BAL
    try:
        if coin:
            return _fetch_assets_v2(coin)
        m = _fetch_assets_v2(None)
        if m:
            _BAL, _BAL_TS = m, now
        return m or _BAL
    except Exception:
        return _BAL

def get_spot_free_qty(symbol: str, fresh: bool = False) -> float:
    base = convert_symbol(symbol).replace("USDT", "")
    if fresh:
        m = get_spot_balances(force=True, coin=base)
        return float(m.get(base, 0.0))
    m = get_spot_balances()
    return float(m.get(base, 0.0))

# ------------------------ helpers (fmt/err) ------------------------
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

def _fmt_by_step(v: float, step: float) -> str:
    if step <= 0:
        return f"{v:.6f}".rstrip("0").rstrip(".")
    k = math.floor(float(v) / float(step)) * float(step)
    scale = max(0, int(round(-math.log10(step))))
    s = f"{k:.{scale}f}"
    return s.rstrip("0").rstrip(".") if "." in s else s

def round_down_step(v: float, step: float) -> float:
    if step is None or step <= 0:
        return float(v)
    return math.floor(float(v) / float(step)) * float(step)

# ------------------------------ orders (V2) ------------------------------
def _post_v2_place_order(body: Dict[str, Any]) -> Dict[str, Any]:
    path = "/api/v2/spot/trade/place-order"
    bj = json.dumps(body)
    _rl("spot_order_v2", 0.12)
    r = requests.post(BASE_URL + path, headers=_headers("POST", path, bj), data=bj, timeout=15)
    if r.status_code != 200:
        return {"http": r.status_code, "text": r.text}
    return r.json()

def place_spot_market_buy(symbol: str, usdt_amount: float) -> Dict[str, Any]:
    """
    시장가 매수 (quote USDT 금액 기반)
    - 먼저 quoteQty 필드로 전송
    - 실패 시 quantity 로 1회 폴백
    - minQuote 미만이면 로컬 스킵
    """
    base = convert_symbol(symbol)
    if not is_tradable(base):
        mark_symbol_removed(base)
        return {"code": "LOCAL_SYMBOL_REMOVED", "msg": "symbol not tradable/removed"}

    spec = get_symbol_spec_spot(base)
    min_quote = float(spec.get("minQuote", 1.0))
    if float(usdt_amount) < min_quote:
        return {"code": "LOCAL_MIN_QUOTE", "msg": f"need>={min_quote}USDT"}

    # 1) quoteQty 로 우선 시도
    body1 = {
        "symbol": base,
        "side": "buy",
        "orderType": "market",
        "force": "gtc",
        "quoteQty": _fmt_by_step(float(usdt_amount), 1e-6),
    }
    res = _post_v2_place_order(body1)
    if isinstance(res, dict) and ("code" in res) and str(res.get("code")) in ("00000", "0"):
        return res

    # 2) 실패 시 quantity 로 폴백
    body2 = {
        "symbol": base,
        "side": "buy",
        "orderType": "market",
        "force": "gtc",
        "quantity": _fmt_by_step(float(usdt_amount), 1e-6),
    }
    res2 = _post_v2_place_order(body2)
    if isinstance(res2, dict) and ("code" in res2) and str(res2.get("code")) in ("00000", "0"):
        try:
            send_telegram(f"[SPOT] BUY {base} via quantity fallback {usdt_amount}")
        except Exception:
            pass
        return res2

    # HTTP 에러 포맷 or API 코드 에러
    if isinstance(res2, dict) and "http" in res2:
        info = _extract_code_text(res2.get("text", "") or "")
        if info.get("code") in ("40309", "40034"):
            mark_symbol_removed(base)
        return {"code": f"HTTP_{res2.get('http')}", "msg": res2.get("text")}
    return res2

def place_spot_market_sell_qty(symbol: str, qty: float) -> Dict[str, Any]:
    """
    시장가 매도(기초코인 수량 기준).
    - 현재가 × 수량 < minQuote 이면 로컬 스킵(too small)
    - scale 오류 감지 시 스텝 재계산으로 1회 재시도
    """
    if qty <= 0:
        return {"code": "LOCAL_BAD_QTY", "msg": "qty<=0"}

    base = convert_symbol(symbol)
    if not is_tradable(base):
        mark_symbol_removed(base)
        return {"code": "LOCAL_SYMBOL_REMOVED", "msg": "symbol not tradable/removed"}

    spec = get_symbol_spec_spot(base)
    step = float(spec.get("qtyStep", 1e-6))
    min_quote = float(spec.get("minQuote", 1.0))

    # 최소 주문금액 가드
    last_px = get_last_price_spot(base) or 0.0
    if last_px > 0:
        notional = float(qty) * last_px
        if notional < min_quote:
            return {"code": "LOCAL_TOO_SMALL", "msg": f"order notional {notional:.4f} < {min_quote} USDT"}

    qty_str = _fmt_by_step(float(qty), step)

    body1 = {
        "symbol": base,
        "side": "sell",
        "orderType": "market",
        "force": "gtc",
        "quantity": qty_str,
    }
    res = _post_v2_place_order(body1)
    if isinstance(res, dict) and ("code" in res) and str(res.get("code")) in ("00000", "0"):
        return res

    # HTTP 에러 → scale 또는 심볼 문제 처리
    if isinstance(res, dict) and "http" in res:
        txt = res.get("text") or ""
        info = _extract_code_text(txt)

        # scale 재시도 (checkBDScale= or checkScale=)
        m = re.search(r"(?:checkBDScale|checkScale)[\"']?\s*[:=]\s*([0-9]+)", txt)
        if res.get("http") == 400 and m:
            chk = int(m.group(1))
            step2 = 10 ** (-chk)
            qty2 = round_down_step(float(qty), step2)
            body2 = {"symbol": base, "side": "sell", "orderType": "market", "force": "gtc",
                     "quantity": _fmt_by_step(qty2, step2)}
            res2 = _post_v2_place_order(body2)
            if isinstance(res2, dict) and ("code" in res2) and str(res2.get("code")) in ("00000", "0"):
                try:
                    send_telegram(f"[SPOT] retry sell {base} scale->{chk} qty={qty2}")
                except Exception:
                    pass
                return res2
            if "http" in res2:
                return {"code": f"HTTP_{res2['http']}", "msg": res2["text"], "retry_scale": chk}
            return res2

        if info.get("code") in ("40309", "40034"):
            mark_symbol_removed(base)
        return {"code": f"HTTP_{res.get('http')}", "msg": txt}

    # API 코드 에러 그대로 리턴
    return res
