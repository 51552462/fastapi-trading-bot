from fastapi import FastAPI, Request
from trader import (enter_position, take_partial_profit, close_position,check_loss_and_exit, position_data,sync_open_positions, send_daily_summary)
import threading, time, re

app = FastAPI()

DEFAULT_AMOUNT = 15.0             # Pine에서 amount 주면 그 값 사용
TP_PCT = {"tp1": 0.30, "tp2": 0.40}  # tp3는 전체 종료

def _norm_symbol(s: str) -> str:
    return re.sub(r'[^A-Za-z0-9]', '', (s or "")).upper()

def _infer_side(symbol: str, side_in: str):
    side = (side_in or "").lower()
    if side in {"long", "short"}:
        return side
    has_long  = f"{symbol}_long"  in position_data
    has_short = f"{symbol}_short" in position_data
    if has_long ^ has_short:
        return "long" if has_long else "short"
    return "long"  # 보수적으로

@app.post("/signal")
async def signal(request: Request):
    data = await request.json()
    typ = (data.get("type") or "").strip()
    sym = _norm_symbol(data.get("symbol", ""))
    if not sym:
        return {"ok": False, "msg": "symbol missing"}

    side = _infer_side(sym, data.get("side"))
    amount = float(data.get("amount", DEFAULT_AMOUNT))

    if typ == "entry":
        enter_position(sym, amount, side)
        return {"ok": True, "msg": f"entry {sym} {side} {amount}"}

    if typ in ("tp1", "tp2"):
        take_partial_profit(sym, TP_PCT[typ], side)
        return {"ok": True, "msg": f"{typ} {sym} {side}"}

    if typ in ("tp3", "sl1", "sl2", "emaExit", "failCut", "liquidation", "stoploss", "roeStop"):
        close_position(sym, side, typ)
        return {"ok": True, "msg": f"close {typ} {sym} {side}"}

    return {"ok": False, "msg": f"unknown type {typ}"}

@app.get("/health")
def health():
    return {"ok": True, "positions": list(position_data.keys())}

# ──────────────────────────────────────────
# 백그라운드 루프들 (재시작 자동복구 + ROE 감시 + 일일 리포트)
# ──────────────────────────────────────────
def _watchdog_roe():
    while True:
        try:
            check_loss_and_exit()
        except Exception as e:
            print("watchdog_roe error:", e)
        time.sleep(5)

def _sync_loop():
    # 시작 직후 1회 즉시 동기화
    try:
        sync_open_positions()
    except Exception as e:
        print("initial sync error:", e)
    # 이후 주기적 동기화
    while True:
        try:
            sync_open_positions()
        except Exception as e:
            print("sync error:", e)
        time.sleep(60)

def _seconds_until_kst(hour: int, minute: int) -> int:
    # 현재 UTC epoch +9h → KST 기준 다음 hour:minute 까지 남은 초
    now_utc = int(time.time())
    now_kst = now_utc + 9*3600
    tm = time.gmtime(now_kst)
    target_kst = int(time.mktime((tm.tm_year, tm.tm_mon, tm.tm_mday, hour, minute, 0, 0, 0, -1)))  # this treats as local; we already shifted
    # 위 mktime은 로컬기준이라 오차 → 그냥 직접 계산
    today_kst = (now_kst // 86400) * 86400
    target_kst = today_kst + hour*3600 + minute*60
    if target_kst <= now_kst:
        target_kst += 86400
    return (target_kst - now_kst)

def _daily_report_loop():
    # 매일 23:59(KST) 전송
    while True:
        try:
            sec = _seconds_until_kst(23, 59)
            time.sleep(max(5, sec))
            send_daily_summary()
            # 다음 날까지 대기
            time.sleep(65)  # 1분 버퍼
        except Exception as e:
            print("daily_report error:", e)
            time.sleep(60)

threading.Thread(target=_watchdog_roe,   daemon=True).start()
threading.Thread(target=_sync_loop,      daemon=True).start()
threading.Thread(target=_daily_report_loop, daemon=True).start()

