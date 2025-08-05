import uvicorn
import asyncio
from fastapi import FastAPI, Request
from trader import enter_position, take_partial_profit, stoploss, check_loss_and_exit

app = FastAPI()

@app.post("/signal")
async def receive_signal(request: Request):
    data = await request.json()
    print(f"\n📩 시그널 수신: {data}")
    try:
        signal_type = data.get("type")
        symbol      = data.get("symbol", "").upper()
        amount      = float(data.get("amount", 0))
        pct         = int(data.get("pct", 0)) / 100

        if signal_type == "entry":
            price = enter_position(symbol, amount)
            return {"status": "ok", "entry_price": price}

        elif signal_type in ["takeprofit1", "takeprofit2", "takeprofit3"]:
            take_partial_profit(symbol, pct)
            return {"status": "ok", "event": signal_type}

        elif signal_type in ["stoploss", "liquidation"]:
            stoploss(symbol)
            return {"status": "ok", "event": signal_type}

        else:
            return {"status": "error", "message": f"Unknown signal type: {signal_type}"}

    except Exception as e:
        print(f"❌ 예외 발생: {e}")
        return {"status": "error", "detail": str(e)}

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(loss_monitor_loop())

async def loss_monitor_loop():
    """
    1초마다 모든 포지션에 대해 현재가를 조회해
    진입가 대비 90% 이하이면 stoploss() 호출
    """
    while True:
        try:
            check_loss_and_exit()
        except Exception as e:
            print(f"❌ 손절 감시 중 오류: {e}")
        await asyncio.sleep(1)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=10000)
