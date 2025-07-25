import time
import threading
import ccxt

from dotenv import load_dotenv
import os

load_dotenv()

api_key = os.getenv("BITGET_API_KEY")
secret = os.getenv("BITGET_API_SECRET")
password = os.getenv("BITGET_API_PASSWORD")

exchange = ccxt.bitget({
    'apiKey': api_key,
    'secret': secret,
    'password': password,
    'enableRateLimit': True,
    'options': {'defaultType': 'swap'}
})

# 상태 변수
positions = {}

def start_tracker(symbol, side, entry_price):
    print(f"📡 트래커 시작: {symbol}, 진입가: {entry_price}")
    positions[symbol] = {
        "side": side,
        "entry": entry_price,
        "exit_stage": 0
    }
    t = threading.Thread(target=track_position, args=(symbol,))
    t.start()

def track_position(symbol):
    while symbol in positions:
        try:
            ticker = exchange.fetch_ticker(f"{symbol}_UMCBL")
            price = ticker['last']
            pos = positions[symbol]
            entry = pos["entry"]
            side = pos["side"]
            stage = pos["exit_stage"]

            pnl_pct = ((price - entry) / entry) * 100 if side == "long" else ((entry - price) / entry) * 100

            # 손절
            if pnl_pct <= -10:
                close_position(symbol)
                print(f"❌ -10% 손절 실행: {symbol}")
                del positions[symbol]
                break

            # 익절 분할 30/40/30
            if stage == 0 and pnl_pct >= 1.5:
                close_partial(symbol, 0.3)
                positions[symbol]["exit_stage"] += 1
            elif stage == 1 and pnl_pct >= 3.5:
                close_partial(symbol, 0.4)
                positions[symbol]["exit_stage"] += 1
            elif stage == 2 and pnl_pct >= 5.5:
                close_partial(symbol, 0.3)
                del positions[symbol]
                print(f"🎯 전체 청산 완료: {symbol}")
                break

            time.sleep(5)

        except Exception as e:
            print("❗ 트래커 오류:", e)
            time.sleep(5)

def close_partial(symbol, ratio):
    print(f"💰 {symbol} {int(ratio*100)}% 익절")
    balance = exchange.fetch_position(symbol=f"{symbol}_UMCBL")
    amt = float(balance['contracts']) * ratio
    side = "sell" if balance['side'] == "long" else "buy"

    exchange.create_order(
        symbol=f"{symbol}_UMCBL",
        type="market",
        side=side,
        amount=round(amt, 4)
    )

def close_position(symbol):
    balance = exchange.fetch_position(symbol=f"{symbol}_UMCBL")
    amt = float(balance['contracts'])
    side = "sell" if balance['side'] == "long" else "buy"
    exchange.create_order(
        symbol=f"{symbol}_UMCBL",
        type="market",
        side=side,
        amount=round(amt, 4)
    )
