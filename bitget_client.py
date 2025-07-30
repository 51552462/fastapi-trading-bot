# bitget_client.py

import os, time, random
import ccxt
from dotenv import load_dotenv

load_dotenv()
exchange = ccxt.bitget({
    "apiKey":       os.getenv("BITGET_API_KEY"),
    "secret":       os.getenv("BITGET_API_SECRET"),
    "password":     os.getenv("BITGET_API_PASSWORD"),
    "enableRateLimit": True,
    "timeout":      30000,
})
exchange.options["adjustForTimeDifference"] = True

def get_market_id(symbol: str) -> str:
    return f"{symbol[:-4]}/{symbol[-4:]}:USDT"

def retry_on_network(fn):
    def wrapper(*args, **kwargs):
        delay = 1.0
        for attempt in range(1, 4):
            try:
                return fn(*args, **kwargs)
            except ccxt.NetworkError as e:
                print(f"⚠️ 네트워크 에러 {attempt}/3: {e}, {delay:.1f}s 후 재시도")
                time.sleep(delay + random.uniform(0, 0.5))
                delay *= 2  # 지수 백오프
        # 3회 모두 실패 시 예외 그대로 던지기
        return fn(*args, **kwargs)
    return wrapper

@retry_on_network
def place_order(order_type, symbol, amount_usdt=1):
    mid = get_market_id(symbol)
    if mid not in exchange.markets:
        exchange.load_markets()

    # 1) 마켓 티커로 현재가 조회
    ticker = exchange.fetch_ticker(mid)
    mark_price = ticker["last"]
    qty = round(amount_usdt / mark_price, 4)

    # 2) 레버리지 고정 5배
    exchange.set_leverage(5, mid)

    # 3) 시장가 주문
    side = "buy" if order_type == "long" else "sell"
    order = exchange.create_order(symbol=mid, type="market", side=side, amount=qty)
    print(f"✅ [{symbol}] {order_type} 주문 체결 @ {mark_price} (qty={qty})")
    return mark_price
