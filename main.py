from fastapi import FastAPI, Request
from trader import (
    enter_position, take_partial_profit, close_position, reduce_by_contracts,
    check_loss_and_exit, position_data, sync_open_positions, send_daily_summary
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
    """우선순위: payload → 거래소 열린 포지션(단일) → 로컬(단일) → long"""
    side = (side_in or "").lower()
    if side in {"long","short"}:
        return side
    opens = [p["side"] for p in get_open_positions() if p["symbol"] == symbol]
    opens = list(set(opens))
    if len(opens) == 1:
        return opens[0]
    has_long  = f"{symbol}_long"  in position_data
    has_short = f"{symbol}_short" in position_data
    if has_long ^ has_short:
        return "long" if has_long else "short"
    return "long"

# ── TradingView webhook ────────────────────────────────────────────────────
@app.post("/signal")
async def signal(request: Request):
    data = await request.json()
    _dedup_prune()
    dk = _dedup_key(data)
    if dk in _DEDUP:
        return {"ok": True, "msg": "duplicate skipped"}
    _DEDUP[dk] = time.time()

    # 공통 파라미터
    typ = (data.get("type") or "").strip()         # (수정된 Pine이면 alert_message로 넘어옴)
    sym = _norm_symbol(data.get("symbol", ""))
    side = _infer_side(sym, data.get("side"))
    amount = float(data.get("amount", DEFAULT_AMOUNT))

    # 레이트리밋 피크 완화
    time.sleep(random.uniform(0, 0.2))

    # ① 새 포맷(type 존재) → 기존 로직
    if typ:
        if typ == "entry":
            enter_position(sym, amount, side)
            return {"ok": True}
        if typ in ("tp1","tp2"):
            take_partial_profit(sym, TP_PCT[typ], side)
            return {"ok": True}
        if typ in ("tp3","sl1","sl2","emaExit","failCut","liquidation","stoploss","roeStop"):
            close_position(sym, side, typ)
            return {"ok": True}
        return {"ok": False, "msg": f"unknown type {typ}"}

    # ② 구 포맷(메시지에 type 없이 {{strategy.*}}만 있는 경우) 지원
    action    = (data.get("action") or "").lower()      # {{strategy.order.action}} → buy/sell
    comment   = (data.get("comment") or "").lower()     # {{strategy.order.comment}}
    contracts = float(str(data.get("contracts", 0) or 0) or 0)  # {{strategy.order.contracts}}

    # comment에 tp/sl 키워드가 있으면 전체 종료로 맵핑
    CMAP = {
        "tp3": "tp3", "sl1": "sl1", "sl2": "sl2",
        "emaexit": "emaExit", "fa ilcut": "failCut", "failcut": "failCut",
        "liquidation": "liquidation", "stoploss": "stoploss"
    }
    for k, v in CMAP.items():
        if k in comment.replace(" ", ""):
            close_position(sym, side, v)
            return {"ok": True}

    # 분할(contracts 제공) → 해당 수량만 reduceOnly
    if action == "sell" and contracts > 0:
        reduce_by_contracts(sym, side, contracts)
        return {"ok": True}

    # 엔트리 추정: buy 이고 현재 그 방향 포지션이 없으면
    if action == "buy":
        enter_position(sym, amount, side)
        return {"ok": True}

    return {"ok": False, "msg": "unhandled legacy payload"}

# ── 헬스 & 모니터 ─────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"ok": True, "positions": list(position_data.keys())}

@app.get("/positions")
def positions():
    return {"local": list(position_data.keys()), "remote": get_open_positions()}

# ── 백그라운드 루프 ───────────────────────────────────────────────────────
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

import threading
from trader import send_telegram  # for daily report thread
threading.Thread(target=_watchdog_roe,     daemon=True).start()
threading.Thread(target=_sync_loop,        daemon=True).start()
threading.Thread(target=_daily_report_loop,daemon=True).start()
