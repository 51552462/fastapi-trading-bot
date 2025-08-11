import os, uvicorn, asyncio, json, re
from fastapi import FastAPI, Request
from datetime import datetime
from zoneinfo import ZoneInfo
from trader import (
    enter_position,
    take_partial_profit,
    close_position,
    check_loss_and_exit,
    position_data,
    send_daily_summary_and_reset,  # 일일 요약 사용 안 하면 trader에서 제거하고 이 라인도 지우세요
)
# from telegram_bot import send_telegram  # 필요시 임시 디버깅용

app = FastAPI()
KST = ZoneInfo("Asia/Seoul")
DEBUG = os.getenv("DEBUG", "0") == "1"

def _robust_parse(raw: bytes):
    """text/plain, 따옴표로 감싼 JSON, 문자열 내부 {...}까지 최대한 파싱"""
    txt = raw.decode("utf-8", errors="ignore").strip()
    # 1) 그대로
    try:
        return json.loads(txt)
    except Exception:
        pass
    # 2) 양끝 따옴표 제거 후
    if (txt.startswith('"') and txt.endswith('"')) or (txt.startswith("'") and txt.endswith("'")):
        thin = txt[1:-1]
        try:
            return json.loads(thin)
        except Exception:
            txt = thin
    # 3) 본문에서 중괄호 블록만 추출
    m = re.search(r'\{.*\}', txt, flags=re.S)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            pass
    return None

async def _handle_signal(request: Request):
    raw = await request.body()
    if DEBUG:
        print("🔎 RAW:", raw.decode("utf-8", errors="ignore"))

    data = _robust_parse(raw)
    if not data:
        if DEBUG:
            print("⚠️ 페이로드 파싱 실패 (headers:", dict(request.headers), ")")
        return {"status":"ok","detail":"parse_fail"}

    if DEBUG:
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
            if DEBUG: print("⚠️ 중복 진입 스킵:", key)
        return {"status":"ok"}

    if t in {"tp1","tp2"}:
        pct = 0.30 if t=="tp1" else 0.40
        take_partial_profit(sym, pct, side)
        return {"status":"ok"}

    if t in {"tp3","sl1","sl2","failCut","emaExit","stoploss","liquidation"}:
        close_position(sym, side, t)
        return {"status":"ok"}

    if t == "tailTouch":
        if DEBUG: print("📎 꼬리터치 (no action):", key)
        return {"status":"ok"}

    if DEBUG: print("❓ 알 수 없는 시그널:", t)
    return {"status":"ok"}

# TV가 루트로 보내도 처리되도록 허용
@app.post("/")
async def receive_root(request: Request):
    return await _handle_signal(request)

@app.post("/signal")
async def receive_signal(request: Request):
    return await _handle_signal(request)

# 헬스체크/브라우저 확인
@app.get("/")
async def root_ok():
    return {"ok": True, "msg": "fastapi-trading-bot alive", "endpoint": "/signal"}

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
    # KST 23:59에 일일 요약 전송 (trader.send_daily_summary_and_reset 사용)
    while True:
        try:
            now = datetime.now(KST)
            if now.hour == 23 and now.minute == 59:
                try:
                    send_daily_summary_and_reset()
                except Exception as e:
                    print("❌ 일일 요약 전송 오류:", e)
                await asyncio.sleep(60)  # 중복 방지
        except Exception as e:
            print("❌ 일일 요약 루프 오류:", e)
        await asyncio.sleep(1)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=10000)
