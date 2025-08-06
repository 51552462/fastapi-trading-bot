# main.py

import uvicorn, asyncio
from fastapi import FastAPI, Request
from json import JSONDecodeError
from trader import (
    enter_position,
    take_partial_profit,
    stoploss,
    check_loss_and_exit,
    position_data,    # 중복 진입 방지를 위해 import
)

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

        # ——————————————————————————————
        # 1) 진입 중복 방지
        if signal_type == "entry":
            if symbol in position_data:
                print(f"⚠️ 중복 진입 스킵: {symbol} 이미 포지션 보유 중")
            else:
                enter_position(symbol, amount)
            return {"status": "ok"}

        # ——————————————————————————————
        # 2) 분할 익절 매핑 (tp3·emaExit은 100% 종료)
        pct_map = {
            "tp1": 0.30,
            "tp2": 0.40,
            "tp3": 1.00,
            "takeprofit1": 0.30,
            "takeprofit2": 0.40,
            "takeprofit3": 1.00,
            "takeprofit_full": 1.00,
            "emaExit":  1.00    # EMA 기준 전체 종료 시그널
        }
        if signal_type in pct_map:
            take_partial_profit(symbol, pct_map[signal_type])
            return {"status": "ok"}

        # ——————————————————————————————
        # 3) 손절 시그널 확장
        stoploss_set = {"stoploss", "sl1", "sl2", "liquidation", "liq"}
        if signal_type in stoploss_set:
            stoploss(symbol)
            return {"status": "ok"}

        # ——————————————————————————————
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
    1초마다 실시간 가격 체크 → 진입가 대비 90% 이하(롱) 혹은 10% 이상(숏) 시 손절()
    """
    while True:
        try:
            check_loss_and_exit()
        except Exception as e:
            print(f"❌ 손절 감시 오류: {e}")
        await asyncio.sleep(1)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=10000)
