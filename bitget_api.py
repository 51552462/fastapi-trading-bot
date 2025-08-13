import os, time, json, hmac, hashlib, base64, requests, re
from typing import List, Dict

BASE_URL = "https://api.bitget.com"

API_KEY        = os.getenv("BITGET_API_KEY", "")
API_SECRET     = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE = os.getenv("BITGET_API_PASSWORD", "")

# ── auth/sign ────────────────────────────────────────────────────────────────
def _ts() -> str:
    return str(int(time.time() * 1000))

def _sign(timestamp: str, method: str, path_with_query: str, body: str = "") -> str:
    raw = timestamp + method.upper() + path_with_query + body
    digest = hmac.new(API_SECRET.encode(), raw.encode(), hashlib.sha256).digest()
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

# ── symbol normalize ─────────────────────────────────────────────────────────
def convert_symbol(sym: str) -> str:
    """
    임의의 표기(예: BTC/USDT, btcusdt_umcbl, BTCUSDTUMCBL)를 깔끔히 'BTCUSDT'로.
    """
    s = re.sub(r'[^A-Za-z0-9]', '', str(sym or "").upper())
    s = re.sub(r'(UMCBL|CMCBL|DMCBL)$', '', s)  # 접미사 제거
    return s

# ── public ticker ────────────────────────────────────────────────────────────
def _safe_last_price(symbol: str):
    symbol_conv = convert_symbol(symbol) + "_UMCBL"
    url = f"{BASE_URL}/api/mix/v1/market/ticker?symbol={symbol_conv}"
    try:
        r = requests.get(url, timeout=10)
        j = r.json()
        if j and j.get("data") and "last" in j["data"]:
            return float(j["data"]["last"])
        print(f"❌ Ticker 실패 {symbol_conv}: {j}")
        return None
    except Exception as e:
        print(f"❌ Ticker 예외 {symbol}: {e}")
        return None

def get_last_price(symbol: str):
    return _safe_last_price(symbol)

# ── place orders ─────────────────────────────────────────────────────────────
def place_market_order(symbol, usdt_amount, side, leverage=5, reduce_only=False):
    """
    USDT 명목금액 기준 시장가 주문.
    side: 'buy' | 'sell'
    """
    symbol_conv = convert_symbol(symbol) + "_UMCBL"
    last = _safe_last_price(symbol)
    if not last:
        return {"code": "LOCAL_TICKER_FAIL", "msg": "ticker_none"}

    qty = round(usdt_amount / last, 6)
    if qty <= 0:
        return {"code": "LOCAL_BAD_QTY", "msg": f"qty {qty}"}

    path = "/api/mix/v1/order/placeOrder"
    body = {
        "symbol":     symbol_conv,
        "marginCoin": "USDT",
        "size":       str(qty),
        "side":       "buy_single" if side == "buy" else "sell_single",
        "orderType":  "market",
        "leverage":   str(leverage),
        "reduceOnly": bool(reduce_only),
    }
    bj = json.dumps(body)
    try:
        res = requests.post(BASE_URL + path, headers=_headers("POST", path, bj), data=bj, timeout=15)
        print(f"📥 Bitget 응답 {res.status_code}: {res.text}")
        return res.json()
    except Exception as e:
        print("❌ Bitget 예외:", e)
        return {"code": "LOCAL_EXCEPTION", "msg": str(e)}

def place_reduce_by_size(symbol, size, pos_side, leverage=5):
    """
    현재 '수량(size)' 그대로 감소(=청산) 주문.
    pos_side: 포지션 방향 'long'|'short'
    → long 닫기: sell_single / short 닫기: buy_single
    """
    symbol_conv = convert_symbol(symbol) + "_UMCBL"
    order_side = "sell_single" if pos_side == "long" else "buy_single"
    path = "/api/mix/v1/order/placeOrder"
    body = {
        "symbol":     symbol_conv,
        "marginCoin": "USDT",
        "size":       str(size),
        "side":       order_side,
        "orderType":  "market",
        "leverage":   str(leverage),
        "reduceOnly": True,
    }
    bj = json.dumps(body)
    try:
        res = requests.post(BASE_URL + path, headers=_headers("POST", path, bj), data=bj, timeout=15)
        print(f"📥 Bitget 응답 {res.status_code}: {res.text}")
        return res.json()
    except Exception as e:
        print("❌ Bitget 예외:", e)
        return {"code": "LOCAL_EXCEPTION", "msg": str(e)}

# ── positions ────────────────────────────────────────────────────────────────
def get_open_positions() -> List[Dict]:
    """
    USDT-M Perp(UMCBL) 오픈 포지션 목록을 표준화해서 반환.
    [{symbol:'BTCUSDT', side:'long'|'short', size:float, entry_price:float}, ...]
    """
    query = "productType=umcbl&marginCoin=USDT"
    path = "/api/mix/v1/position/allPosition"
    url  = f"{BASE_URL}{path}?{query}"
    try:
        res = requests.get(url, headers=_headers("GET", f"{path}?{query}", ""), timeout=10)
        j = res.json()
        out = []
        if not j or j.get("code") not in ("00000", "0"):
            print(f"❌ get_open_positions 응답 이상: {j}")
            return out

        for pos in (j.get("data") or []):
            try:
                sym = convert_symbol(pos.get("symbol", ""))  # BTCUSDT_UMCBL → BTCUSDT
                side = (pos.get("holdSide") or "").lower()   # long/short
                size = float(pos.get("total") or pos.get("available") or 0)
                entry_price = float(pos.get("openAvgPrice") or pos.get("averageOpenPrice") or pos.get("avgOpenPrice") or 0)
                if sym and side in ("long", "short") and size > 0 and entry_price > 0:
                    out.append({"symbol": sym, "side": side, "size": size, "entry_price": entry_price})
            except Exception as e:
                print("get_open_positions parse err:", e, pos)
        return out
    except Exception as e:
        print("❌ get_open_positions 예외:", e)
        return []
