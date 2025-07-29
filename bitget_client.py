import os
import ccxt
import math
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("BITGET_API_KEY")
secret  = os.getenv("BITGET_API_SECRET")
password= os.getenv("BITGET_API_PASSWORD")

exchange = ccxt.bitget({
    "apiKey": api_key,
    "secret": secret,
    "password": password,
    "enableRateLimit": True,
    "options": {"defaultType": "swap"}
})
exchange.load_markets()

def get_market_id(symbol: str) -> str:
    """
    symbol: 'BTCUSDT', 'ETHUSDT', 'FLOKIUSDT', ...
    returns: 'BTC/USDT:USDT', 'ETH/USDT:USDT', 'FLOKI/USDT:USDT', ...
    """
    base  = symbol[:-4]
    quote = symbol[-4:]
    return f"{base}/{quote}:USDT"

def place_order(order_type, symbol, amount_usdt=1):
    market_id = get_market_id(symbol)
    if market_id not in exchange.markets:
        raise ValueError(f"Invalid market symbol: {market_id}")

    # 1) 현재가 조회
    ticker     = exchange.fetch_ticker(market_id)
    mark_price = ticker["last"]

    # 2) 진입 수량 계산
    qty = round(amount_usdt / mark_price, 4)

    # 3) 레버리지(5배) 설정
    exchange.set_leverage(5, market_id)

    # 4) 주문 방향
    side = "buy" if order_type == "long" else "sell"

    # 5) 시장가 주문
    order = exchange.create_order(
        symbol=market_id,
        type="market",
        side=side,
        amount=qty
    )

    print(f"✅ {symbol} {order_type.upper()} 진입 완료 @ {mark_price} → Qty: {qty}")
    return mark_price
