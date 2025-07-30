# bitget_client.py

import os, time, random, ccxt
from dotenv import load_dotenv

load_dotenv()
DRY_RUN     = os.getenv("DRY_RUN", "false").lower() == "true"
USE_TESTNET = os.getenv("USE_TESTNET", "false").lower() == "true"

config = {
    "apiKey":       os.getenv("BITGET_API_KEY"),
    "secret":       os.getenv("BITGET_API_SECRET"),
    "password":     os.getenv("BITGET_API_PASSWORD"),
    "enableRateLimit": True,
    "timeout":      30000,
    "options": {
        "defaultType": "swap",
        "adjustForTimeDifference": True,
    },
}
if USE_TESTNET:
    # bitget 테스트넷 엔드포인트 설정
    config["urls"] = {"api": "https://api-testnet.bitget.com"}

exchange = ccxt.bitget(config)
exchange.load_markets()

def get_market_id(symbol: str) -> str:
    base, quote = symbol[:-4], symbol[-4:]
    return f"{base}/{quote}:USDT"

def place_order(order_type, symbol, amount_usdt=1):
    mid = get_market_id(symbol)
    if mid not in exchange.markets:
        exchange.load_markets()
    if mid not in exchange.markets:
        raise ValueError(f"Unknown market: {mid}")

    # Dry‐Run 모드면 실제 주문 건너뛰고 가짜 가격 리턴
    ticker = exchange.fetch_ticker(mid)
    mark_price = ticker["last"]
    if DRY_RUN:
        print(f"[DRY_RUN] {order_type}@{mid}, amount_usdt={amount_usdt}, price={mark_price}")
        return mark_price

    # 실거래 모드: 5배 레버리지 + 시장가 주문
    qty = round(amount_usdt / mark_price, 4)
    exchange.set_leverage(5, mid)
    side = "buy" if order_type == "long" else "sell"
    order = exchange.create_order(symbol=mid, type="market", side=side, amount=qty)
    print(f"✅ {order_type.upper()} {mid} @ {mark_price} (qty={qty})")
    return mark_price
