# main.py
from fastapi import FastAPI, Request
from bitget_client import place_order
from position_tracker import start_tracker

app = FastAPI()

# ── 헬스체크용 (브라우저로도 확인 가능)
@app.get("/")
def health():
    return {"status": "ok"}

# ── 두 가지 엔드포인트 모두 여기로 라우팅
@app.post("/signal")
@app.post("/webhook")
async def receive_signal(req: Request):
    data = await req.json()
    print("📩 시그널 수신:", data)

    # payload 검사
    order_type = data.get("type")
    symbol     = data.get("symbol")
    amount     = data.get("amount", 1)   # amount 키도 받아오도록

    if order_type in ["long", "short"] and symbol:
        # 실제 주문 실행
        entry_price = place_order(order_type, symbol, amount)
        start_tracker(symbol, order_type, entry_price)
        return {"status": "executed"}

    return {"status": "ignored"}
