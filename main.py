# main.py

import json, traceback
from fastapi import FastAPI, Request, HTTPException
from bitget_client import place_order
from position_tracker import start_tracker, close_position, close_partial

app = FastAPI()

@app.get("/")
def health():
    return {"status": "ok"}

@app.post("/signal")
async def receive_signal(req: Request):
    body = await req.body()
    try:
        data = json.loads(body)
    except json.JSONDecodeError as e:
        print("🚨 JSON 파싱 에러:", e, "| raw:", body)
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    ev  = data.get("type")
    sym = data.get("symbol")
    amt = data.get("amount", 1)
    pct = data.get("pct", None)

    print("📩 Signal received:", data)

    try:
        if ev == "entry":
            price = place_order("long", sym, amount_usdt=amt)
            start_tracker(sym, "long", price)

        elif ev in ["stoploss1", "stoploss2", "liquidation", "fail", "entry_fail"]:
            close_position(sym)

        elif ev in ["takeprofit1", "takeprofit2", "takeprofit3", "exitByEMA", "takeprofit_base"]:
            frac = (pct or 100) / 100
            close_partial(sym, frac)

        else:
            return {"status": "ignored", "event": ev}

        return {"status": "ok", "event": ev}

    except Exception as e:
        tb = traceback.format_exc()
        print("🚨 처리 중 예외 발생:\n", tb)
        # 에러 발생해도 500이 아닌 200으로 응답해서 트레이딩뷰 알림 루프를 막지 않음
        return {"status": "error", "event": ev, "detail": str(e)}
