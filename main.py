# main.py

import traceback
from fastapi import FastAPI, Request
from bitget_client import place_order
from position_tracker import start_tracker, close_position, close_partial

app = FastAPI()

@app.get("/")
def health():
    return {"status": "ok"}

@app.post("/signal")
async def receive_signal(req: Request):
    # 1) JSON 파싱
    try:
        data = await req.json()
    except Exception as e:
        print("🚨 JSON 파싱 실패:", e)
        return {"status": "error", "detail": "invalid JSON"}

    # 2) 필드 추출
    ev       = data.get("type")
    sym      = data.get("symbol")
    amount   = data.get("amount", 1)
    leverage = data.get("leverage", 1)
    pct      = data.get("pct", None)

    print("📩 Signal received:", data)

    # 3) 이벤트별 분기 처리
    try:
        if ev == "entry":
            # 진입: long 포지션, 고정 1 USD + 5x 레버리지 (파인스크립트에서 이미 설정)
            entry_price = place_order("long", sym, amount=amount, leverage=leverage)
            start_tracker(sym, "long", entry_price)

        elif ev in ["stoploss1", "stoploss2", "liquidation", "fail", "entry_fail"]:
            # 손절·강제청산·진입실패: 전량 청산
            close_position(sym)

        elif ev in ["takeprofit1", "takeprofit2", "takeprofit3", "exitByEMA", "takeprofit_base"]:
            # 분할 익절: pct 필드로 비중 계산 후 부분 청산
            fraction = (pct or 100) / 100
            close_partial(sym, fraction)

        else:
            # 알 수 없는 이벤트 무시
            return {"status": "ignored", "event": ev}

        return {"status": "ok", "event": ev}

    except Exception as e:
        tb = traceback.format_exc()
        print("🚨 처리 중 예외 발생:\n", tb)
        return {"status": "error", "detail": str(e)}
