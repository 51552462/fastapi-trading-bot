import os, time, hmac, hashlib, base64, requests, json
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.bitget.com"
API_KEY = os.getenv("BITGET_API_KEY")
API_SECRET = os.getenv("BITGET_API_SECRET")
API_PASSPHRASE = os.getenv("BITGET_API_PASSWORD")

def convert_symbol(symbol: str) -> str:
    return symbol.upper().replace("/", "").replace("_", "") + "_UMCBL"

def _timestamp():
    return str(int(time.time() * 1000))

def _sign(method, path, timestamp, body=""):
    message = f"{timestamp}{method.upper()}{path}{body}"
    signature = hmac.new(API_SECRET.encode(), message.encode(), hashlib.sha256).digest()
    return base64.b64encode(signature).decode()

def _headers(method, path, body=""):
    ts = _timestamp()
    sign = _sign(method, path, ts, body)
    return {
        "ACCESS-KEY": API_KEY,
        "ACCESS-SIGN": sign,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": API_PASSPHRASE,
        "Content-Type": "application/json"
    }

def place_market_order(symbol, usdt_amount, side, leverage=5):
    path = "/api/mix/v1/order/placeOrder"
    url = BASE_URL + path
    symbol_conv = convert_symbol(symbol)

    # 시세 조회 → 수량 계산
    price_url = f"{BASE_URL}/api/mix/v1/market/ticker?symbol={symbol_conv}"
    price_res = requests.get(price_url).json()
    last_price = float(price_res["data"]["last"])
    qty = round(usdt_amount / last_price, 6)

    if qty < 0.001:
        print(f"⚠️ 최소 주문 수량 미달 → {qty}, 주문 생략")
        return {"code": "SKIP", "msg": "below min qty"}

    body = {
        "symbol": symbol_conv,
        "marginCoin": "USDT",
        "size": str(qty),
        "side": "buy" if side == "buy" else "sell",   # ✅ Bitget 공식 방식
        "orderType": "market",
        "leverage": str(leverage)
        # ❌ holdMode 제거
    }

    body_json = json.dumps(body)
    print("📤 Bitget 최종 주문 요청:", body)
    headers = _headers("POST", path, body_json)
    res = requests.post(url, headers=headers, data=body_json)
    return res.json()

def close_all(symbol):
    path = "/api/mix/v1/order/close-position"
    url = BASE_URL + path
    symbol_conv = convert_symbol(symbol)
    body = {
        "symbol": symbol_conv,
        "marginCoin": "USDT"
    }
    body_json = json.dumps(body)
    headers = _headers("POST", path, body_json)
    res = requests.post(url, headers=headers, data=body_json)
    return res.json()

def get_last_price(symbol):
    symbol_conv = convert_symbol(symbol)
    url = f"{BASE_URL}/api/mix/v1/market/ticker?symbol={symbol_conv}"
    res = requests.get(url)
    return float(res.json()["data"]["last"])
