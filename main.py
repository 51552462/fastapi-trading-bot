from fastapi import FastAPI, Request
from bitget_client import place_order
import uvicorn

app = FastAPI()

@app.post("/signal")
async def receive_signal(req: Request):
    data = await req.json()
    print("📩 시그널 수신:", data)

    order_type = data.get("type")
    symbol = data.get("symbol")

    if order_type in ["long", "short"] and symbol:
        result = place_order(order_type, symbol)
        return {"status": "executed", "result": result}
    return {"status": "ignored"}
