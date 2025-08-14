from fastapi import FastAPI, Request
from trader import (
    enter_position, take_partial_profit, close_position,
    check_loss_and_exit, position_data,
    sync_open_positions, send_daily_summary
)
from bitget_api import convert_symbol, get_open_positions
import threading, time, os, random, hashlib, json

app = FastAPI()

DEFAULT_AMOUNT = 15.0
TP_PCT = {"tp1": 0.30, "tp2": 0.40}
WATCHDOG_SEC = float(os.getenv("WATCHDOG_SEC", "1"))

# 최근 수신 신호 중복 차단(15초 TTL)
_DEDUP = {}

def _dedup_key(payload: dict) -> str:
    blob = json.dumps(payload, sort_keys=True)
    return hashlib.sha1(blob.encode()).hexdigest()

def _dedup_prune(ttl=15):
    now = time.time()
    for k, t in list(_DEDUP.items()):
        if now - t > ttl:
            del _DEDUP[k]

def _norm_symbol(s: str) -> str:
    return convert_symbol(s)

def _infer_side(symbol: str, side_in: str):
    """
    우선순위: payload side → 거래소 열린 포지션(단일) → 로컬 상태(단일) → 'long'
    """
    side = (side_in or "").lower()
    if side in {"long", "short"}:
        return side

    # 거래소에서 단일 방향만 열려 있으면 그쪽
    opens = [p["side"] for p in get_open_positions() if p["symbol"] == symbol]
    opens = list(set(opens))
    if len(opens) == 1:
        return opens[0]

    # 로컬 상태에서 단일 방향만 있으면 그쪽
    has_long  = f"{symbol}_long"  in position_data
    has_short = f"{symbol}_short" in position_data
    if has_long ^ has_short:
        return "long" if has_long else "short"

    return "long"

@app.post("/signal")
async def signal(request: Request):
    data = await request.json()
    _dedup_prune()
    dk = _dedup_key(data)
    if dk in _DEDUP:
        return {"ok": True, "msg": "duplicate skipped"}
    _DEDUP[dk] = time.time()

    typ = (data.get("type") or "").strip()
    sym = _norm_symbol(data.get("symbol", ""))
    if not sym:
        return {"ok": False, "msg": "symbol missing"}

    side = _infer_side(sym, data.get("side"))
    amount = float(data.get("amount", DEFAULT_AMOUNT))

    # 레이트리밋 완화: 0~200ms 지터
    time.sleep(random.uniform(0, 0.2))

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

@app.get("/positions")
def positions():
    return {"local": list(position_data.keys()), "remote": get_open_positions()}

def _watchdog_roe():
    while True:
        try:
            check_loss_and_exit()
        except Exception as e:
            print("watchdog_roe error:", e)
        time.sleep(max(0.5, WATCHDOG_SEC))

def _sync_loop():
    try:
        sync_open_positions()
    except Exception as e:
        print("initial sync error:", e)
    while True:
        try:
            sync_open_positions()
        except Exception as e:
            print("sync error:", e)
        time.sleep(60)

def _seconds_until_kst(hour: int, minute: int) -> int:
    now_utc = int(time.time()); now_kst = now_utc + 9*3600
    today_kst = (now_kst // 86400) * 86400
    target_kst = today_kst + hour*3600 + minute*60
    if target_kst <= now_kst: target_kst += 86400
    return target_kst - now_kst

def _daily_report_loop():
    while True:
        try:
            time.sleep(max(5, _seconds_until_kst(23, 59)))
            send_daily_summary()
            time.sleep(65)
        except Exception as e:
            print("daily_report error:", e)
            time.sleep(60)

threading.Thread(target=_watchdog_roe,     daemon=True).start()
threading.Thread(target=_sync_loop,        daemon=True).start()
threading.Thread(target=_daily_report_loop,daemon=True).start()
