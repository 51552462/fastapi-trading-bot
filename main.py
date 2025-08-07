# main.py

import uvicorn, asyncio
from fastapi import FastAPI, Request
from json import JSONDecodeError
from trader import (
    enter_position,
    take_partial_profit,
    stoploss,
    check_loss_and_exit,
    position_data,
)

app = FastAPI()

@app.post("/signal")
async def receive_signal(request: Request):
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
        side        = data.get("side", "long").lower()

        pos_key = f"{symbol}_{side}"

        if signal_type == "entry":
            if pos_key in position_data:
                print(f"⚠️ 중복 진입 스킵: {pos_key} 이미 포지션 보유 중")
            else:
                enter_position(symbol, amount, side)
            return {"status": "ok"}

        pct_map = {
            "tp1": 0.30,
            "tp2": 0.40,
            "tp3": 1.00,
            "takeprofit1": 0.30,
            "takeprofit2": 0.40,
            "takeprofit3": 1.00,
            "takeprofit_full": 1.00,
            "emaExit":  1.00
        }
        if signal_type in pct_map:
            take_partial_profit(symbol, pct_map[signal_type], side)
            return {"status": "ok"}

        stoploss_set = {"stoploss", "sl1", "sl2", "liquidation", "liq", "failCut"}
        if signal_type in stoploss_set:
            stoploss(symbol, side)
            return {"status": "ok"}

        if signal_type == "tailTouch":
            print(f"📎 꼬리터치 알림 수신: {symbol} ({side}) → 알림만 처리")
            return {"status": "ok"}

        print(f"❓ 알 수 없는 시그널 타입: {signal_type}")
        return {"status": "ok"}

    except Exception as e:
        print(f"❌ /signal 처리 예외: {e}")
        return {"status": "error", "detail": str(e)}

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

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=10000)
