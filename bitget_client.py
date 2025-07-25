import os
import ccxt
import math
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("BITGET_API_KEY")
secret = os.getenv("BITGET_API_SECRET")
password = os.getenv("BITGET_API_PASSWORD")

exchange = ccxt.bitget({
    "apiKey": api_key,
    "secret": secret,
    "password": password,
    "enableRateLimit": True,
    "options": {
        "defaultType": "swap"
    }
})

def place_order(order_type, symbol):
    symbol_pair = f"{symbol}_UMCBL"
    amount = 1 / 5

    ticker = exchange.fetch_ticker(symbol_pair)
    mark_price = ticker['last']
    qty = round(amount / mark_price, 4)

    exchange.set_leverage(5, symbol_pair)

    side = "buy" if order_type == "long" else "sell"

    order = exchange.create_order(
        symbol=symbol_pair,
        type="market",
        side=side,
        amount=qty
    )

    print(f"✅ {symbol} {order_type.upper()} 진입 완료: {order}")
    return mark_price  # 진입가 반환

