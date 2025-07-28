# main.py
import traceback
from fastapi import FastAPI, Request
from bitget_client import place_order
from position_tracker import start_tracker

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

    print("📩 시그널 수신:", data)

    order_type = data.get("type")
    symbol     = data.get("symbol")
    # amount 인자를 place_order에서 쓰도록 바꾸셨다면 여기서도 꺼내야 합니다
    # amount     = data.get("amount", 1)

    if order_type in ["long", "short"] and symbol:
        # 2) 주문 로직 감싸기
        try:
            entry_price = place_order(order_type, symbol)
            start_tracker(symbol, order_type, entry_price)
            return {"status": "executed"}
        except Exception as e:
            tb = traceback.format_exc()
            print("🚨 주문 처리 중 예외 발생:\n", tb)
            return {"status": "error", "detail": str(e)}
    else:
        return {"status": "ignored", "detail": "missing type or symbol"}
