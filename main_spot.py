# main_spot.py
# ------------------------------------------------------------
# TradingView → Render(FastAPI) → Bitget(Spot) 자동매매 엔진 (메인 엔드포인트)
# - 중복/업무중복 방지(DEDUP/BIZ DEDUP)
# - 워커 큐 기반 비동기 처리
# - 심볼별 금액 설정/기본금액/강제기본금액
# - TP/SL/FailCut/Close 라우팅
# - 헬스/로그/밸런스/설정 조회
# - 용량가드 + 자동 손절(-3% 등) 감시 스레드 시작
# ------------------------------------------------------------
import os
import time
import json
import hashlib
import threading
import queue
from collections import deque
from typing import Dict, Any

from fastapi import FastAPI, Request

# Telegram (spot 전용 모듈 우선)
try:
    from telegram_spot_bot import send_telegram
except Exception:
    try:
        from telegram_bot import send_telegram  # 폴백(있으면 사용)
    except Exception:
        def send_telegram(msg: str):
            print("[TG]", msg)

# Bitget Spot 헬퍼
from bitget_api_spot import convert_symbol, get_spot_balances

# 트레이더(실거래 동작)
from trader_spot import (
    enter_spot, take_partial_spot, close_spot,
    start_capacity_guard, start_auto_stoploss
)

# ----------------------- 환경변수 -----------------------
DEFAULT_AMOUNT = float(os.getenv("DEFAULT_AMOUNT", "15"))  # 기본 진입 USDT
DEDUP_TTL      = float(os.getenv("DEDUP_TTL", "15"))       # 동일 페이로드 중복 잠금
BIZDEDUP_TTL   = float(os.getenv("BIZDEDUP_TTL", "3"))     # 같은 작업(타입/심볼/사이드) 잠금

WORKERS        = int(os.getenv("WORKERS", "4"))
QUEUE_MAX      = int(os.getenv("QUEUE_MAX", "1000"))

LOG_INGRESS    = os.getenv("LOG_INGRESS", "0") == "1"      # 들어오는 시그널 로그 텔레그램 알림
FORCE_DEFAULT_AMOUNT = os.getenv("FORCE_DEFAULT_AMOUNT", "0") == "1"  # 금액 강제 기본값

# 부분익절 비율(메인에서 pct를 정해서 trader에 전달)
TP1_PCT = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT = float(os.getenv("TP2_PCT", "0.40"))
TP3_PCT = float(os.getenv("TP3_PCT", "0.30"))

# 심볼별 고정 금액 매핑(JSON 문자열)
SYMBOL_AMOUNT_JSON = os.getenv("SYMBOL_AMOUNT_JSON", "")
try:
    SYMBOL_AMOUNT = json.loads(SYMBOL_AMOUNT_JSON) if SYMBOL_AMOUNT_JSON else {}
except Exception:
    SYMBOL_AMOUNT = {}

# 자동 손절(트레이더 내부 스레드가 수행; 메인에선 설정 표기만)
AUTO_SL_ENABLE    = os.getenv("AUTO_SL_ENABLE", "1") == "1"
_auto_sl_pct_env  = float(os.getenv("AUTO_SL_PCT", "-3"))
AUTO_SL_PCT       = _auto_sl_pct_env if _auto_sl_pct_env < 0 else -abs(_auto_sl_pct_env)
AUTO_SL_POLL_SEC  = float(os.getenv("AUTO_SL_POLL_SEC", "3"))
AUTO_SL_GRACE_SEC = float(os.getenv("AUTO_SL_GRACE_SEC", "5"))

# ----------------------- 앱 상태 -----------------------
app = FastAPI()

INGRESS_LOG: deque = deque(maxlen=200)    # 최근 유입 시그널 기록
_DEDUP: Dict[str, float] = {}             # 페이로드 중복 키
_BIZDEDUP: Dict[str, float] = {}          # 업무중복 키(type+symbol+side)

_task_q: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=QUEUE_MAX)


# ----------------------- 유틸 -----------------------
def _dedup_key(d: Dict[str, Any]) -> str:
    """페이로드 전체로 해시 생성"""
    return hashlib.sha1(json.dumps(d, sort_keys=True).encode()).hexdigest()

def _biz_key(typ: str, symbol: str, side: str) -> str:
    """업무중복(같은 명령) 키"""
    return f"{typ}:{symbol}:{side}"

def _infer_side(side: str, default: str = "long") -> str:
    s = (side or "").strip().lower()
    return s if s in ("long", "short") else default

def _norm_symbol(sym: str) -> str:
    return convert_symbol(sym)


async def _parse_any(req: Request) -> Dict[str, Any]:
    """JSON/Raw/Form 모두 파싱(TradingView/수동 curl 호환)"""
    # 1) JSON
    try:
        return await req.json()
    except Exception:
        pass
    # 2) Raw text(JSON 문자열 가정)
    try:
        raw = (await req.body()).decode(errors="ignore").strip()
        if raw:
            try:
                return json.loads(raw)
            except Exception:
                # 작은따옴표 → 큰따옴표 보정
                fixed = raw.replace("'", '"')
                return json.loads(fixed)
    except Exception:
        pass
    # 3) Form(payload|data 필드 JSON)
    try:
        form = await req.form()
        payload = form.get("payload") or form.get("data")
        if payload:
            return json.loads(payload)
    except Exception:
        pass
    raise ValueError("cannot parse request")


# ----------------------- 시그널 처리 -----------------------
def _handle_signal(data: Dict[str, Any]):
    """
    표준 입력:
      { "type": "entry|tp1|tp2|tp3|sl1|sl2|close|failCut|emaExit|...", "symbol": "DOGEUSDT", "side": "long", "amount": 50 }
    """
    typ    = (data.get("type") or "").strip()
    symbol = _norm_symbol(data.get("symbol", ""))
    side   = _infer_side(data.get("side"), "long")
    amount = float(data.get("amount", DEFAULT_AMOUNT))
    resolved_amount = float(amount)

    # 심볼별 금액 우선
    if (symbol in SYMBOL_AMOUNT) and (str(SYMBOL_AMOUNT[symbol]).strip() != ""):
        try:
            resolved_amount = float(SYMBOL_AMOUNT[symbol])
        except Exception:
            resolved_amount = float(DEFAULT_AMOUNT)
    elif FORCE_DEFAULT_AMOUNT:
        resolved_amount = float(DEFAULT_AMOUNT)

    if not symbol:
        send_telegram("[SPOT] symbol missing: " + json.dumps(data))
        return

    # 레거시 키워드 매핑
    legacy = {
        "tp_1": "tp1", "tp_2": "tp2", "tp_3": "tp3",
        "sl_1": "sl1", "sl_2": "sl2",
        "ema_exit": "emaExit", "failcut": "failCut"
    }
    typ = legacy.get(typ.lower(), typ)

    now = time.time()
    bk = _biz_key(typ, symbol, side)
    tprev = _BIZDEDUP.get(bk, 0.0)
    if now - tprev < BIZDEDUP_TTL:
        return
    _BIZDEDUP[bk] = now

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

    if typ in ("tp1", "tp2", "tp3"):
        pct = TP1_PCT if typ == "tp1" else (TP2_PCT if typ == "tp2" else TP3_PCT)
        take_partial_spot(symbol, pct); return

    # sl1/sl2 → 전량 종료(트레이더에서 예쁜 포맷으로 텔레그램)
    if typ in ("sl1", "sl2"):
        close_spot(symbol, reason=typ); return

    if typ in ("failCut", "emaExit", "liquidation", "fullExit", "close", "exit"):
        close_spot(symbol, reason=typ); return

    if typ in ("tailTouch", "info", "debug"):
        return

    send_telegram("[SPOT] unknown signal: " + json.dumps(data))


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
    """공통 인입 처리(+중복 방지, 큐 적재)"""
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


# ----------------------- FastAPI 엔드포인트 -----------------------
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
    """최근 유입 30건 확인용(디버깅)"""
    return list(INGRESS_LOG)[-30:]

@app.get("/balances")
def balances():
    """현물 잔고 스냅샷(가용)"""
    return {"balances": get_spot_balances(force=True)}

@app.get("/config")
def config():
    """현재 설정값 확인"""
    return {
        "DEFAULT_AMOUNT": DEFAULT_AMOUNT,
        "DEDUP_TTL": DEDUP_TTL, "BIZDEDUP_TTL": BIZDEDUP_TTL,
        "WORKERS": WORKERS, "QUEUE_MAX": QUEUE_MAX,
        "LOG_INGRESS": LOG_INGRESS,
        "FORCE_DEFAULT_AMOUNT": FORCE_DEFAULT_AMOUNT,
        "SYMBOL_AMOUNT": SYMBOL_AMOUNT,
        "TP1_PCT": TP1_PCT, "TP2_PCT": TP2_PCT, "TP3_PCT": TP3_PCT,
        "AUTO_SL_ENABLE": AUTO_SL_ENABLE,
        "AUTO_SL_PCT": AUTO_SL_PCT,
        "AUTO_SL_POLL_SEC": AUTO_SL_POLL_SEC,
        "AUTO_SL_GRACE_SEC": AUTO_SL_GRACE_SEC,
        "SL_MODE": "sl1/sl2 → FULL CLOSE (autoSL thread active if enabled)"
    }


# ----------------------- 스타트업 -----------------------
@app.on_event("startup")
def on_startup():
    # 워커 시작
    for i in range(WORKERS):
        t = threading.Thread(target=_worker_loop, args=(i,), daemon=True, name=f"spot-worker-{i}")
        t.start()

    # 용량가드 + 자동손절 감시 스레드 시작
    start_capacity_guard()
    start_auto_stoploss()

    # 기동 알림
    try:
        threading.Thread(target=send_telegram, args=("[SPOT] FastAPI up",), daemon=True).start()
    except Exception:
        pass
