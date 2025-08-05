# main.py

import uvicorn, asyncio
from fastapi import FastAPI, Request
from json import JSONDecodeError
from trader import enter_position, take_partial_profit, stoploss, check_loss_and_exit

app = FastAPI()

@app.post("/signal")
async def receive_signal(request: Request):
    # 1) 빈 바디나 잘못된 JSON 무시
    try:
        data = await request.json()
    except JSONDecodeError:
        print("⚠️ /signal: 빈 또는 잘못된 JSON 바디 수신 → 무시")
        return {"status": "ok", "detail": "no payload"}

    print(f"\n📩 시그널 수신: {data}")
    try:
        signal_type = data.get("type")
        symbol      = data.get("symbol", "").upper()
        amount      = float(data.get("amount", 0))
        pct         = float(data.get("pct", 0)) / 100

        if signal_type == "entry":
            enter_position(symbol, amount)

        elif signal_type in ["stoploss", "liquidation"]:
            stoploss(symbol)

        elif signal_type == "takeprofit1":
            take_partial_profit(symbol, 0.30)

        elif signal_type == "takeprofit2":
            take_partial_profit(symbol, 0.40)

        elif signal_type == "takeprofit3":
            take_partial_profit(symbol, 0.30)

        elif signal_type == "takeprofit_full":
            take_partial_profit(symbol, 1.00)

        else:
            print(f"❓ 알 수 없는 시그널 타입: {signal_type}")

        return {"status": "ok"}

    except Exception as e:
        print(f"❌ /signal 처리 예외: {e}")
        return {"status": "error", "detail": str(e)}


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(loss_monitor_loop())

async def loss_monitor_loop():
    """
    1초마다 실시간 가격 체크 → 진입가 대비 90% 이하 시 stoploss()
    """
    while True:
        try:
            check_loss_and_exit()
        except Exception as e:
            print(f"❌ 손절 감시 오류: {e}")
        await asyncio.sleep(1)


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=10000)
