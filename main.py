import json, traceback
from fastapi import FastAPI, Request
from bitget_client import place_order
from position_tracker import start_tracker, close_position, close_partial

app = FastAPI()

@app.get("/")
def health():
    return {"status": "ok"}

@app.post("/signal")
async def receive_signal(req: Request):
    body = await req.body()

    # ── 1) 빈 바디(줄바꿈만 있는)라면 그냥 무시
    if not body or body.strip() == b"":
        print("⚠️ 빈 페이로드 무시")
        return {"status": "ignored", "reason": "empty body"}

    # ── 2) JSON 파싱
    try:
        data = json.loads(body)
    except json.JSONDecodeError as e:
        print("🚨 JSON 파싱 에러:", e, "| raw:", body)
        return {"status": "ignored", "reason": "invalid JSON"}

    ev  = data.get("type")
    sym = data.get("symbol")
    amt = data.get("amount", 1)
    pct = data.get("pct")

    print("📩 Signal received:", data)

    # ── 3) 이벤트별 분기
    try:
        if ev == "entry":
            price = place_order("long", sym, amount_usdt=amt)
            start_tracker(sym, "long", price)

        elif ev in ["stoploss1", "stoploss2", "liquidation", "fail", "entry_fail"]:
            close_position(sym)

        elif ev in ["takeprofit1", "takeprofit2", "takeprofit3", "exitByEMA", "takeprofit_base"]:
            fraction = (pct or 100) / 100
            close_partial(sym, fraction)

        else:
            return {"status": "ignored", "event": ev}

        return {"status": "ok", "event": ev}

    except Exception as e:
        tb = traceback.format_exc()
        print("🚨 처리 중 예외 발생:\n", tb)
        # 200 OK로 응답해 트레이딩뷰 Alert 전송 루프가 멈추도록
        return {"status": "error", "event": ev, "detail": str(e)}
