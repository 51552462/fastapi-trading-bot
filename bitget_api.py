import os, time, json, hmac, hashlib, base64, requests, re
from typing import List, Dict

BASE_URL = "https://api.bitget.com"

API_KEY        = os.getenv("BITGET_API_KEY", "")
API_SECRET     = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE = os.getenv("BITGET_API_PASSWORD", "")

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

def convert_symbol(sym: str) -> str:
    # "BTCUSDT.P", "BTC/USDT", "btc_usdt" 등 → "BTCUSDT"
    s = re.sub(r'[^A-Za-z0-9]', '', sym).upper()
    # Bitget 심볼 뒤에 붙는 _UMCBL 제거
    s = s.replace("_UMCBL", "")
    return s

# ───────────────────────────
# Public Ticker (미서명)
# ───────────────────────────
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

# ───────────────────────────
# Private: Market Order
# ───────────────────────────
def place_market_order(symbol, usdt_amount, side, leverage=5, reduce_only=False):
    """side: 'buy' or 'sell'  (reduce_only=True면 감소주문)"""
    symbol_conv = convert_symbol(symbol) + "_UMCBL"
    last_price = _safe_last_price(symbol)
    if not last_price:
        return {"code": "LOCAL_TICKER_FAIL", "msg": "ticker_none"}

    qty = round(usdt_amount / last_price, 6)
    if qty <= 0:
        return {"code": "LOCAL_BAD_QTY", "msg": f"qty {qty}"}

    path = "/api/mix/v1/order/placeOrder"
    path_with_query = path
    order_side = "buy_single" if side == "buy" else "sell_single"

    body = {
        "symbol":     symbol_conv,
        "marginCoin": "USDT",
        "size":       str(qty),
        "side":       order_side,
        "orderType":  "market",
        "leverage":   str(leverage),
        "reduceOnly": True if reduce_only else False
    }
    body_json = json.dumps(body)

    print("📤 Bitget 요청:", body)
    try:
        res = requests.post(BASE_URL + path, headers=_headers("POST", path_with_query, body_json), data=body_json, timeout=15)
        print(f"📥 Bitget 응답 {res.status_code}: {res.text}")
        return res.json()
    except Exception as e:
        print(f"❌ Bitget 예외: {e}")
        return {"code": "LOCAL_EXCEPTION", "msg": str(e)}

# ───────────────────────────
# Private: Open Positions Sync
# ───────────────────────────
def get_open_positions() -> List[Dict]:
    """
    Bitget Perp(UMCBL) 전체 오픈 포지션 반환.
    결과 예시: [{"symbol":"BTCUSDT","side":"long","size":0.05,"entry_price":64000.0}, ...]
    """
    query = "productType=umcbl&marginCoin=USDT"
    path = "/api/mix/v1/position/allPosition"
    path_with_query = f"{path}?{query}"
    url = f"{BASE_URL}{path}?{query}"

    try:
        res = requests.get(url, headers=_headers("GET", path_with_query, ""), timeout=10)
        j = res.json()
        out = []
        if not j or j.get("code") not in ("00000", "0"):
            print(f"❌ get_open_positions 응답 이상: {j}")
            return out

        data = j.get("data") or []
        for pos in data:
            try:
                sym = convert_symbol(pos.get("symbol", ""))
                # holdSide: "long" or "short"
                side = (pos.get("holdSide") or "").lower()
                # 총 수량(계약 사이즈)
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
