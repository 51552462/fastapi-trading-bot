import os
import uvicorn
from fastapi import FastAPI, Request
from bitget_client import place_order
from position_tracker import close_position, close_partial

app = FastAPI()

@app.post("/signal")
async def receive_signal(request: Request):
    data = await request.json()
    print(f"📩 Signal received: {data}")

    try:
        signal_type = data.get("type")
        symbol = data.get("symbol")
        leverage = int(data.get("leverage", 5))
        amount_usdt = float(data.get("amount", 15))  # 고정 진입 금액

        if signal_type == "entry":
            price = place_order("long", symbol, amount_usdt=amount_usdt, leverage=leverage)
            print(f"✅ Entry Order Placed at {price}")
            return {"status": "ok", "event": "entry"}

        elif signal_type in ["takeprofit1", "takeprofit2", "takeprofit3"]:
            pct = int(data.get("pct", 33))  # 분할 익절 비율
            close_partial(symbol, pct / 100)
            return {"status": "ok", "event": signal_type}

        elif signal_type in ["stoploss", "liquidation"]:
            close_position(symbol)
            return {"status": "ok", "event": signal_type}

        else:
            return {"status": "error", "detail": "Unknown signal type"}

    except Exception as e:
        print("🚨 처리 중 예외:", e)
        return {"status": "error", "detail": str(e)}


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=10000)
