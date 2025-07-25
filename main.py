from fastapi import FastAPI, Request
from bitget_client import place_order
from position_tracker import start_tracker
import uvicorn

app = FastAPI()

@app.post("/signal")
async def receive_signal(req: Request):
    data = await req.json()
    print("📩 시그널 수신:", data)

    order_type = data.get("type")
    symbol = data.get("symbol")

    if order_type in ["long", "short"] and symbol:
        entry_price = place_order(order_type, symbol)
        start_tracker(symbol, order_type, entry_price)
        return {"status": "executed"}
    return {"status": "ignored"}
