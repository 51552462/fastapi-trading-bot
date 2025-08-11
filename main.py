import uvicorn, asyncio
from fastapi import FastAPI, Request
from json import JSONDecodeError
from datetime import datetime
from zoneinfo import ZoneInfo
from trader import (
    enter_position,
    take_partial_profit,
    close_position,
    check_loss_and_exit,
    position_data,
    send_daily_summary_and_reset,
)

app = FastAPI()
KST = ZoneInfo("Asia/Seoul")

@app.post("/signal")
async def receive_signal(request: Request):
    try:
        data = await request.json()
    except JSONDecodeError:
        return {"status":"ok","detail":"no payload"}

    print("📩 시그널 수신:", data)
    t    = data.get("type", "")
    sym  = data.get("symbol", "").upper()
    amt  = float(data.get("amount", 0))
    side = data.get("side", "long").lower()
    key  = f"{sym}_{side}"

    if t == "entry":
        if key not in position_data:
            enter_position(sym, amt, side)
        else:
            print("⚠️ 중복 진입 스킵:", key)
        return {"status":"ok"}

    if t in {"tp1","tp2"}:
        pct = 0.30 if t=="tp1" else 0.40
        take_partial_profit(sym, pct, side)
        return {"status":"ok"}

    # tp3나 각종 손절/종료 시그널은 전부 최종 청산
    if t in {"tp3","sl1","sl2","failCut","emaExit","stoploss","liquidation"}:
        close_position(sym, side, t)
        return {"status":"ok"}

    if t == "tailTouch":
        print("📎 꼬리터치 (no action):", key)
        return {"status":"ok"}

    if t == "dailySummaryNow":
        # 수동으로 즉시 요약 전송하고 리셋 (원하면 사용)
        send_daily_summary_and_reset()
        return {"status":"ok"}

    print("❓ 알 수 없는 시그널:", t)
    return {"status":"ok"}

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(loss_monitor_loop())
    asyncio.create_task(daily_summary_loop())

async def loss_monitor_loop():
    while True:
        try:
            check_loss_and_exit()
        except Exception as e:
            print("❌ 손절 감시 오류:", e)
        await asyncio.sleep(1)

async def daily_summary_loop():
    # KST 23:59에 일일 요약 전송
    while True:
        try:
            now = datetime.now(KST)
            if now.hour == 23 and now.minute == 59:
                send_daily_summary_and_reset()
                # 같은 분 중복 전송 방지
                await asyncio.sleep(60)
        except Exception as e:
            print("❌ 일일 요약 루프 오류:", e)
        await asyncio.sleep(1)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=10000)
