import uvicorn
import asyncio
from fastapi import FastAPI, Request
from trader import enter_position, take_partial_profit, stoploss, check_loss_and_exit
from bitget_api import set_one_way_mode  # ✅ 단일모드 설정 함수 가져오기

app = FastAPI()

@app.post("/signal")
async def receive_signal(request: Request):
    data = await request.json()
    print(f"📩 시그널 수신: {data}")
    try:
        signal_type = data.get("type")
        symbol = data.get("symbol", "").upper()
        amount = float(data.get("amount", 15))
        if signal_type == "entry":
            price = enter_position(symbol, amount)
            if price is not None:
                return {"status": "ok", "entry_price": price}
            return {"status": "error", "detail": "order_failed"}
        elif signal_type in ["takeprofit1", "takeprofit2", "takeprofit3"]:
            pct = int(data.get("pct", 33)) / 100
            take_partial_profit(symbol, pct)
            return {"status": "ok", "event": signal_type}
        elif signal_type in ["stoploss", "liquidation"]:
            stoploss(symbol)
            return {"status": "ok", "event": signal_type}
        return {"status": "error", "message": "Unknown signal type"}
    except Exception as e:
        print(f"❌ 예외 발생: {e}")
        return {"status": "error", "detail": str(e)}

@app.on_event("startup")
async def startup_event():
    set_one_way_mode()  # ✅ 서버 시작 시 단 1회만 호출
    asyncio.create_task(loss_monitor_loop())

async def loss_monitor_loop():
    while True:
        try:
            check_loss_and_exit()
        except Exception as e:
            print(f"❌ 손실 감시 중 오류: {e}")
        await asyncio.sleep(5)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=10000)
