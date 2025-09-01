# main_spot.py
import os, time, json, hashlib, threading, queue, re
from collections import deque
from typing import Dict, Any
from fastapi import FastAPI, Request

from trader_spot import (
    enter_spot, take_partial_spot, close_spot,
    start_capacity_guard
)
from telegram_bot import send_telegram
from bitget_api_spot import convert_symbol, get_spot_balances

# ===== 설정값 =====
DEFAULT_AMOUNT = float(os.getenv("DEFAULT_AMOUNT", "15"))   # TV에 amount 없을 때 기본 진입 USDT
DEDUP_TTL      = float(os.getenv("DEDUP_TTL", "15"))        # 동일 payload 중복 차단 TTL
BIZDEDUP_TTL   = float(os.getenv("BIZDEDUP_TTL", "3"))      # 같은 유형/심볼/사이드 연속 차단 TTL

WORKERS        = int(os.getenv("WORKERS", "4"))
QUEUE_MAX      = int(os.getenv("QUEUE_MAX", "1000"))

LOG_INGRESS    = os.getenv("LOG_INGRESS", "0") == "1"       # 수신 로그를 TG로 보낼지
FORCE_DEFAULT_AMOUNT = os.getenv("FORCE_DEFAULT_AMOUNT", "0") == "1"

# 심볼별 강제 금액 (선택)
SYMBOL_AMOUNT_JSON = os.getenv("SYMBOL_AMOUNT_JSON", "")
try:
    SYMBOL_AMOUNT = json.loads(SYMBOL_AMOUNT_JSON) if SYMBOL_AMOUNT_JSON else {}
except Exception:
    SYMBOL_AMOUNT = {}

# TP 분할 비율(참고: sl1/sl2는 코드에서 전량 종료로 처리하므로 여기선 미사용)
TP1_PCT = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT = float(os.getenv("TP2_PCT", "0.40"))
TP3_PCT = float(os.getenv("TP3_PCT", "0.30"))

# ===== 앱/큐/중복 =====
app = FastAPI()

INGRESS_LOG: deque = deque(maxlen=200)
_DEDUP: Dict[str, float] = {}       # payload 해시 → 마지막 처리 시각
_BIZDEDUP: Dict[str, float] = {}    # (type,symbol,side) key → 마지막 시각
_task_q: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=QUEUE_MAX)

# ===== 유틸 =====
def _dedup_key(d: Dict[str, Any]) -> str:
    return hashlib.sha1(json.dumps(d, sort_keys=True).encode()).hexdigest()

def _biz_key(typ: str, symbol: str, side: str) -> str:
    return f"{typ}:{symbol}:{side}"

def _infer_side(side: str, default: str = "long") -> str:
    s = (side or "").strip().lower()
    return s if s in ("long", "short") else default

def _norm_symbol(sym: str) -> str:
    return convert_symbol(sym)

async def _parse_any(req: Request) -> Dict[str, Any]:
    # 1) JSON
    try:
        return await req.json()
    except Exception:
        pass
    # 2) raw body → JSON 시도
    try:
        raw = (await req.body()).decode(errors="ignore").strip()
        if raw:
            try:
                return json.loads(raw)
            except Exception:
                fixed = raw.replace("'", '"')
                return json.loads(fixed)
    except Exception:
        pass
    # 3) form payload
    try:
        form = await req.form()
        payload = form.get("payload") or form.get("data")
        if payload:
            return json.loads(payload)
    except Exception:
        pass
    raise ValueError("cannot parse request")

# ===== 핵심 처리 =====
def _handle_signal(data: Dict[str, Any]):
    typ    = (data.get("type") or "").strip()
    symbol = _norm_symbol(data.get("symbol", ""))
    side   = _infer_side(data.get("side"), "long")
    amount = float(data.get("amount", DEFAULT_AMOUNT))
    resolved_amount = float(amount)

    # 심볼별 강제 금액 → 최우선
    if (symbol in SYMBOL_AMOUNT) and (str(SYMBOL_AMOUNT[symbol]).strip() != ""):
        try:
            resolved_amount = float(SYMBOL_AMOUNT[symbol])
        except Exception:
            resolved_amount = float(DEFAULT_AMOUNT)
    # FORCE_DEFAULT_AMOUNT=1이면 TV amount 무시
    elif FORCE_DEFAULT_AMOUNT:
        resolved_amount = float(DEFAULT_AMOUNT)

    if not symbol:
        send_telegram("[SPOT] symbol missing: " + json.dumps(data))
        return

    # 레거시 명칭 호환
    legacy = {
        "tp_1":"tp1","tp_2":"tp2","tp_3":"tp3",
        "sl_1":"sl1","sl_2":"sl2",
        "ema_exit":"emaExit","failcut":"failCut"
    }
    typ = legacy.get(typ.lower(), typ)

    # 비즈 중복 차단
    now = time.time()
    bk = _biz_key(typ, symbol, side)
    tprev = _BIZDEDUP.get(bk, 0.0)
    if now - tprev < BIZDEDUP_TTL:
        return
    _BIZDEDUP[bk] = now

    # 수신 로그 (entry일 때만 amt 표기)
    if LOG_INGRESS:
        msg = f"[SPOT] {typ} {symbol} {side}"
        if typ == "entry":
            msg += f" amt={resolved_amount}"
        try:
            send_telegram(msg)
        except Exception:
            pass

    # 라우팅
    if typ == "entry":
        enter_spot(symbol, resolved_amount); return

    if typ in ("tp1","tp2","tp3"):
        pct = TP1_PCT if typ == "tp1" else (TP2_PCT if typ == "tp2" else TP3_PCT)
        take_partial_spot(symbol, pct); return

    # === 손절은 sl1/sl2 모두 전량 종료 ===
    if typ in ("sl1","sl2"):
        close_spot(symbol, reason=typ); return

    # 전량 종료 계열
    if typ in ("failCut","emaExit","liquidation","fullExit","close","exit"):
        close_spot(symbol, reason=typ); return

    if typ in ("tailTouch","info","debug"):
        return

    send_telegram("[SPOT] unknown signal: " + json.dumps(data))

# ===== 워커/엔드포인트 =====
def _worker_loop(idx: int):
    while True:
        try:
            data = _task_q.get()
            if data is None:
                continue
            _handle_signal(data)
        except Exception as e:
            print(f"[spot-worker-{idx}] error:", e)
        finally:
            _task_q.task_done()

async def _ingest(req: Request):
    now = time.time()
    try:
        data = await _parse_any(req)
    except Exception as e:
        return {"ok": False, "error": f"bad_payload: {e}"}
    dk = _dedup_key(data)
    if dk in _DEDUP and now - _DEDUP[dk] < DEDUP_TTL:
        return {"ok": True, "dedup": True}
    _DEDUP[dk] = now

    INGRESS_LOG.append({"ts": now, "ip": (req.client.host if req and req.client else "?"), "data": data})
    try:
        _task_q.put_nowait(data)
    except queue.Full:
        send_telegram("[SPOT] queue full drop: " + json.dumps(data))
        return {"ok": False, "queued": False, "reason": "queue_full"}
    return {"ok": True, "queued": True, "qsize": _task_q.qsize()}

app = FastAPI()

@app.get("/")
def root():
    return {"ok": True, "service": "spot"}

@app.post("/signal")
async def signal(req: Request):
    return await _ingest(req)

@app.post("/webhook")
async def webhook(req: Request):
    return await _ingest(req)

@app.post("/alert")
async def alert(req: Request):
    return await _ingest(req)

@app.get("/health")
def health():
    return {"ok": True, "ingress": len(INGRESS_LOG), "queue": _task_q.qsize(), "workers": WORKERS}

@app.get("/ingress")
def ingress():
    return list(INGRESS_LOG)[-30:]

@app.get("/balances")
def balances():
    return {"balances": get_spot_balances(force=True)}  # 수동 점검시 신선 조회

@app.get("/config")
def config():
    return {
        "DEFAULT_AMOUNT": DEFAULT_AMOUNT,
        "DEDUP_TTL": DEDUP_TTL, "BIZDEDUP_TTL": BIZDEDUP_TTL,
        "WORKERS": WORKERS, "QUEUE_MAX": QUEUE_MAX,
        "LOG_INGRESS": LOG_INGRESS,
        "FORCE_DEFAULT_AMOUNT": FORCE_DEFAULT_AMOUNT,
        "SYMBOL_AMOUNT": SYMBOL_AMOUNT,
        "TP1_PCT": TP1_PCT, "TP2_PCT": TP2_PCT, "TP3_PCT": TP3_PCT,
        "SL_MODE": "sl1/sl2 -> FULL CLOSE"
    }

@app.on_event("startup")
def on_startup():
    for i in range(WORKERS):
        t = threading.Thread(target=_worker_loop, args=(i,), daemon=True, name=f"spot-worker-{i}")
        t.start()
    start_capacity_guard()
    try:
        threading.Thread(target=send_telegram, args=("[SPOT] FastAPI up",), daemon=True).start()
    except Exception:
        pass
