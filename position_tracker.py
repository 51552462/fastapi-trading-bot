import time, threading
import ccxt
from dotenv import load_dotenv
import os

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

# 동일하게 market_id 매핑 함수
def get_market_id(symbol: str) -> str:
    base  = symbol[:-4]
    quote = symbol[-4:]
    return f"{base}/{quote}:USDT"

positions = {}

def start_tracker(symbol, side, entry_price):
    print(f"📡 트래커 시작: {symbol}, 진입가: {entry_price}")
    positions[symbol] = {"side": side, "entry": entry_price, "stage": 0}
    threading.Thread(target=track_position, args=(symbol,), daemon=True).start()

def track_position(symbol):
    while symbol in positions:
        try:
            market_id = get_market_id(symbol)
            ticker    = exchange.fetch_ticker(market_id)
            price     = ticker["last"]
            pos       = positions[symbol]
            entry     = pos["entry"]
            side      = pos["side"]
            stage     = pos["stage"]

            pnl_pct = ((price - entry) / entry)*100 if side=="long" else ((entry - price)/entry)*100

            # 손절 -10%
            if pnl_pct <= -10:
                close_position(symbol)
                print(f"❌ -10% 손절: {symbol}")
                del positions[symbol]
                break

            # 익절 30/40/30
            if stage==0 and pnl_pct>=1.5:
                close_partial(symbol, 0.3)
                positions[symbol]["stage"]+=1
            elif stage==1 and pnl_pct>=3.5:
                close_partial(symbol, 0.4)
                positions[symbol]["stage"]+=1
            elif stage==2 and pnl_pct>=5.5:
                close_partial(symbol, 0.3)
                print(f"🎯 전체 청산 완료: {symbol}")
                del positions[symbol]
                break

            time.sleep(5)
        except Exception as e:
            print("❗ 트래커 오류:", e)
            time.sleep(5)

def close_partial(symbol, ratio):
    market_id = get_market_id(symbol)
    balance   = exchange.fetch_position(symbol=market_id)
    amt       = float(balance["contracts"]) * ratio
    side      = "sell" if balance["side"]=="long" else "buy"
    exchange.create_order(symbol=market_id, type="market", side=side, amount=round(amt,4))
    print(f"💰 {symbol} {int(ratio*100)}% 익절")

def close_position(symbol):
    market_id = get_market_id(symbol)
    balance   = exchange.fetch_position(symbol=market_id)
    amt       = float(balance["contracts"])
    side      = "sell" if balance["side"]=="long" else "buy"
    exchange.create_order(symbol=market_id, type="market", side=side, amount=round(amt,4))
