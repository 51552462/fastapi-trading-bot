# main.py

import uvicorn, asyncio
from fastapi import FastAPI, Request
from json import JSONDecodeError
from trader import (
    enter_position,
    take_partial_profit,
    close_position,
    check_loss_and_exit,
    position_data,
)

app = FastAPI()

@app.post("/signal")
async def receive_signal(request: Request):
    try:
        data = await request.json()
    except JSONDecodeError:
        return {"status":"ok","detail":"no payload"}

    print(f"📩 시그널 수신: {data}")
    signal_type = data.get("type")
    symbol      = data.get("symbol","").upper()
    amount      = float(data.get("amount",0))
    side        = data.get("side","long").lower()
    key         = f"{symbol}_{side}"

    # 1) 진입
    if signal_type == "entry":
        if key not in position_data:
            enter_position(symbol, amount, side)
        else:
            print(f"⚠️ 중복 진입 스킵: {key}")
        return {"status":"ok"}

    # 2) 부분 익절
    if signal_type in {"tp1","tp2"}:
        take_partial_profit(symbol, 0.30 if signal_type=="tp1" else 0.40, side)
        return {"status":"ok"}

    # 3) 나머지 모두 완전 종료
    if signal_type in {
        "tp3","emaExit","failCut",
        "sl1","sl2","stoploss","liquidation"
    }:
        close_position(symbol, side, signal_type)
        return {"status":"ok"}

    # 4) 꼬리 터치 only 알림
    if signal_type == "tailTouch":
        print(f"📎 꼬리터치 알림: {key}")
        return {"status":"ok"}

    print(f"❓ 알 수 없는 시그널: {signal_type}")
    return {"status":"ok"}

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(loss_monitor_loop())

async def loss_monitor_loop():
    while True:
        try:
            check_loss_and_exit()
        except Exception as e:
            print(f"❌ 손절 감시 오류: {e}")
        await asyncio.sleep(1)

if __name__=="__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=10000)
