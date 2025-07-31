# bitget_client.py
import os
import ccxt
from dotenv import load_dotenv

load_dotenv()

exchange = ccxt.bitget({
    "apiKey": os.getenv("BITGET_API_KEY"),
    "secret": os.getenv("BITGET_API_SECRET"),
    "password": os.getenv("BITGET_API_PASSWORD"),
    "enableRateLimit": True,
    "options": {"defaultType": "swap"}
})

exchange.set_sandbox_mode(False)  # 실계정

def place_order(side, symbol, amount_usdt=15, leverage=20):
    exchange.load_markets()
    market = exchange.market(symbol)
    ticker = exchange.fetch_ticker(symbol)
    price = ticker["last"]

    # ✅ 정수형 precision 적용
    precision = int(market['precision']['amount'])
    quantity = round((amount_usdt * leverage) / price, precision)

    # ✅ 레버리지 설정 시 정수 변환
    exchange.set_leverage(int(leverage), symbol)

    # ✅ 시장가 주문 실행
    order = exchange.create_market_order(symbol, side, quantity)
    print(f"📈 {symbol} {side.upper()} 주문 완료: {quantity} @ {price}")
    return order


def close_position(symbol):
    exchange.load_markets()
    pos = exchange.fetch_position(symbol)
    amt = float(pos["contracts"])
    if amt > 0:
        exchange.create_order(symbol, type="market", side="sell", amount=amt)
        print(f"🔻 전체 종료: {symbol}")
    else:
        print(f"ℹ️ 포지션 없음: {symbol}")


def close_partial(symbol, ratio):
    exchange.load_markets()
    pos = exchange.fetch_position(symbol)
    amt = float(pos["contracts"])
    if amt > 0:
        close_amt = round(amt * ratio, 4)
        exchange.create_order(symbol, type="market", side="sell", amount=close_amt)
        print(f"💠 분할 종료: {symbol} {ratio*100:.1f}%")
    else:
        print(f"ℹ️ 포지션 없음: {symbol}")
