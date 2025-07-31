# main.py
import os
import uvicorn
import asyncio
from fastapi import FastAPI, Request
from dotenv import load_dotenv
from bitget_client import place_order, close_position, close_partial, exchange
from position_tracker import set_entry, update_exit_stage, reset_position, get_entry_price, get_all_positions

load_dotenv()

app = FastAPI()

@app.post("/signal")
async def receive_signal(request: Request):
    data = await request.json()
    print(f"📩 시그널 수신: {data}")

    try:
        signal_type = data.get("type")
        symbol = data.get("symbol", "").upper()
        leverage = int(data.get("leverage", 20))
        amount_usdt = float(data.get("amount", 15))

        if signal_type == "entry":
            order = place_order("buy", symbol, amount_usdt, leverage)
            entry_price = order['average'] or order['price']
            set_entry(symbol, entry_price)
            return {"status": "ok", "event": "entry"}

        elif signal_type in ["takeprofit1", "takeprofit2", "takeprofit3"]:
            pct = int(data.get("pct", 33))
            close_partial(symbol, pct / 100)
            update_exit_stage(symbol)
            return {"status": "ok", "event": signal_type}

        elif signal_type in ["stoploss", "liquidation"]:
            close_position(symbol)
            reset_position(symbol)
            return {"status": "ok", "event": signal_type}

        else:
            return {"status": "error", "detail": "Unknown signal type"}

    except Exception as e:
        print("🚨 예외 발생:", e)
        return {"status": "error", "detail": str(e)}


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(monitor_loss())

async def monitor_loss():
    while True:
        try:
            positions = get_all_positions()
            for symbol, info in positions.items():
                entry_price = info.get("entry_price")
                if not entry_price:
                    continue
                ticker = exchange.fetch_ticker(symbol)
                current_price = ticker["last"]
                loss_rate = (current_price - entry_price) / entry_price

                if loss_rate <= -0.10:
                    print(f"🚨 {symbol} -10% 손실 감지 → 포지션 강제 종료")
                    close_position(symbol)
                    reset_position(symbol)

        except Exception as e:
            print("❌ 실시간 손실 감시 오류:", e)

        await asyncio.sleep(5)  # 5초 간격 체크


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=10000)
