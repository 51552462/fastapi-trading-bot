import uvicorn, asyncio, json, re
from fastapi import FastAPI, Request
from json import JSONDecodeError
from trader import (
    enter_position,
    take_partial_profit,
    close_position,
    check_loss_and_exit,
    position_data,
)
from telegram_bot import send_telegram  # 선택: 초기 디버깅용

app = FastAPI()

def _robust_parse(raw: bytes):
    """
    TradingView는 종종 text/plain으로 JSON 문자열을 보냄.
    1) 우선 json.loads 시도
    2) 실패하면 양끝 큰따옴표 제거 후 재시도
    3) 실패하면 중괄호 {...} 부분만 추출해서 재시도
    """
    txt = raw.decode("utf-8", errors="ignore").strip()
    # 1) 그대로 시도
    try:
        return json.loads(txt)
    except Exception:
        pass
    # 2) 감싸진 따옴표 제거 (예: "\"{...}\"")
    if (txt.startswith('"') and txt.endswith('"')) or (txt.startswith("'") and txt.endswith("'")):
        thin = txt[1:-1]
        try:
            return json.loads(thin)
        except Exception:
            txt = thin  # 다음 단계에서 활용
    # 3) 중괄호 블록 추출
    m = re.search(r'\{.*\}', txt, flags=re.S)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            pass
    # 실패 시 None
    return None

@app.post("/signal")
async def receive_signal(request: Request):
    raw = await request.body()
    print("🔎 RAW:", raw.decode("utf-8", errors="ignore"))  # 반드시 확인

    data = _robust_parse(raw)
    if not data:
        print("⚠️ 페이로드 파싱 실패 (headers:", dict(request.headers), ")")
        # 초기 디버깅용 푸시(원하면 주석처리)
        # send_telegram("⚠️ TV Webhook parse fail\n" + raw.decode("utf-8","ignore")[:500])
        return {"status":"ok","detail":"parse_fail"}

    print("📩 시그널 수신:", data)
    t    = str(data.get("type", "")).strip()
    sym  = str(data.get("symbol", "")).upper().replace("PERP","").replace("_","")
    amt  = float(data.get("amount", 0) or 0)
    side = str(data.get("side", "long")).lower()
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

    if t in {"tp3","sl1","sl2","failCut","emaExit","stoploss","liquidation"}:
        close_position(sym, side, t)
        return {"status":"ok"}

    if t == "tailTouch":
        print("📎 꼬리터치 (no action):", key)
        return {"status":"ok"}

    print("❓ 알 수 없는 시그널:", t)
    return {"status":"ok"}

@app.get("/ping")
async def ping():
    return {"ok": True}

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(loss_monitor_loop())

async def loss_monitor_loop():
    while True:
        try:
            check_loss_and_exit()
        except Exception as e:
            print("❌ 손절 감시 오류:", e)
        await asyncio.sleep(1)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=10000)
