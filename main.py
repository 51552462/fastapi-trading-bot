import json
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
    body = await req.body()
    if not body or body.strip() == b"":
        return {"status": "ignored", "reason": "empty body"}

    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return {"status": "ignored", "reason": "invalid JSON"}

    ev  = data.get("type")
    sym = data.get("symbol")
    pct = data.get("pct")

    print("📩 Signal received:", data)

    try:
        if ev == "entry":
            # 10 USD + 5× 레버리지, Dual Mode Hedge 진입
            price = place_order("long", sym, amount_usdt=10)
            start_tracker(sym, "long", price)

        elif ev in ["stoploss1","stoploss2","liquidation","fail","entry_fail"]:
            close_position(sym)

        elif ev in ["takeprofit1","takeprofit2","takeprofit3","exitByEMA","takeprofit_base"]:
            frac = (pct or 100) / 100
            close_partial(sym, frac)

        else:
            return {"status": "ignored", "event": ev}

        return {"status": "ok", "event": ev}

    except Exception as e:
        tb = traceback.format_exc()
        print(f"🚨 처리 중 예외:\n{tb}")
        return {"status": "error", "event": ev, "detail": str(e)}
