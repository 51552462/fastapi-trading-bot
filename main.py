# main.py — FastAPI: 시그널 처리/정상화, 전략 손절 reason 기록, 워커/리포트, 부트스트랩
import os, sys, time, json, hashlib, threading, queue, re
from collections import deque
from typing import Dict, Any, Optional
from fastapi import FastAPI, Request, HTTPException

# --- import path guard ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

\1
# (AI Expert는 선택적으로 import)
try:
    from ai_expert import start_ai_expert
except Exception:
    def start_ai_expert():
        pass
from trader import (
    enter_position, take_partial_profit, reduce_by_contracts, close_position,
    start_watchdogs, start_reconciler, start_capacity_guard, get_pending_snapshot
)

# 텔레그램
try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str): print("[TG]", msg)

# Bitget 유틸
from bitget_api import convert_symbol, get_open_positions

# 정책/AI(없어도 안전하게 동작)
try:
    from policy.tf_policy import ingest_signal, start_policy_manager
except Exception:
    def ingest_signal(*a, **kw): pass
    def start_policy_manager(): pass

# =========================
# 환경변수
# =========================
DEFAULT_AMOUNT        = float(os.getenv("DEFAULT_AMOUNT", "80"))  # 기본 진입금액($)
LEVERAGE              = float(os.getenv("LEVERAGE", "5"))         # 기본 레버리지
DEDUP_TTL             = float(os.getenv("DEDUP_TTL", "15"))
ENTRY_DUP_TTL_SEC     = float(os.getenv("ENTRY_DUP_TTL_SEC", "3"))  # 동일 비즈 이벤트 윈도
WORKERS               = int(os.getenv("WORKERS", "6"))
QUEUE_MAX             = int(os.getenv("QUEUE_MAX", "2000"))
LOG_INGRESS           = os.getenv("LOG_INGRESS", "0") == "1"

# “진짜 $80 고정” 강제 스위치 + 심볼별 금액 매핑
FORCE_DEFAULT_AMOUNT  = os.getenv("FORCE_DEFAULT_AMOUNT", "1") == "1"
SYMBOL_AMOUNT_JSON    = os.getenv("SYMBOL_AMOUNT_JSON", "")
try:
    SYMBOL_AMOUNT = json.loads(SYMBOL_AMOUNT_JSON) if SYMBOL_AMOUNT_JSON else {}
except Exception:
    SYMBOL_AMOUNT = {}

LOGS_API_TOKEN = os.getenv("LOGS_API_TOKEN", "")

# =========================
# 런타임 상태
# =========================
app = FastAPI(title="fastapi-trading-bot", version="1.0.0")
INGRESS_LOG: deque = deque(maxlen=200)
_DEDUP: Dict[str, float]   = {}
_BIZDEDUP: Dict[str, float] = {}
_task_q: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=QUEUE_MAX)

# =========================
# 헬퍼
# =========================
def _dedup_key(d: Dict[str, Any]) -> str:
    return hashlib.sha1(json.dumps(d, sort_keys=True).encode()).hexdigest()

def _norm_symbol(sym: str) -> str:
    return convert_symbol(str(sym or ""))

def _infer_side(side: str, default: str = "long") -> str:
    s = (side or "").strip().lower()
    return s if s in ("long", "short") else default

def _norm_type(typ: str) -> str:
    t = (typ or "").strip().lower()
    t = re.sub(r"[\s_\-]+", "", t)
    aliases = {
        "tp_1":"tp1","tp_2":"tp2","tp_3":"tp3",
        "takeprofit1":"tp1","takeprofit2":"tp2","takeprofit3":"tp3",
        "sl_1":"sl1","sl_2":"sl2",
        "stopfull":"stoploss","stopall":"stoploss","stop":"stoploss",
        "fullexit":"stoploss","exitall":"stoploss",
        "emaexit":"emaexit","failcut":"failcut",
        "closeposition":"close","closeall":"close",
        "reducecontracts":"reducebycontracts","reduce_by_contracts":"reducebycontracts",
        "entrybuy":"entry","entrysell":"entry",
    }
    return aliases.get(t, t)

def _canon_tf(s: Optional[str]) -> Optional[str]:
    s = (s or "").strip().lower()
    if not s: return None
    m = {"1m":"1m","3m":"3m","5m":"5m","15m":"15m","30m":"30m",
         "1h":"1h","2h":"2h","3h":"3h","4h":"4h","1d":"1d","d":"1d"}
    return m.get(s, s)

def _resolve_amount(symbol: str, default: float) -> float:
    """심볼 매핑 > FORCE_DEFAULT > 기본값 순서로 결정. (USDT 금액 그대로 유지)"""
    if symbol in SYMBOL_AMOUNT:
        try: return float(SYMBOL_AMOUNT[symbol])
        except Exception: pass
    return float(default) if FORCE_DEFAULT_AMOUNT else float(default)

# =========================
# 시그널 처리
# =========================
def _handle_signal(data: Dict[str, Any]):
    symbol  = _norm_symbol(data.get("symbol") or data.get("ticker"))
    side    = _infer_side(data.get("side"), "long")
    typ_raw = str(data.get("type") or data.get("event") or data.get("reason") or "")
    tf      = _canon_tf(str(data.get("timeframe") or ""))

    if not symbol or not typ_raw:
        return

    t = _norm_type(typ_raw)
    amount   = _resolve_amount(symbol, DEFAULT_AMOUNT)
    leverage = float(data.get("leverage", LEVERAGE))

    # 비즈니스 디듀프(ENTRY/TP/SL 스팸 방지)
    now = time.time()
    bizkey = f"{t}:{symbol}:{side}"
    if now - _BIZDEDUP.get(bizkey, 0.0) < ENTRY_DUP_TTL_SEC:
        return
    _BIZDEDUP[bizkey] = now

    # 진입/청산/분할
    if LOG_INGRESS:
        try: send_telegram(f"📥 {t} {symbol} {side} amt={amount}")
        except Exception: pass

    try: ingest_signal({"type": t, "symbol": symbol, "side": side, "tf": tf})
    except Exception: pass

    if t == "entry":
        enter_position(symbol=symbol, usdt_amount=amount, side=side, leverage=leverage)
        return

    if t in ("tp1","tp2","tp3"):
        pct = {"tp1": float(os.getenv("TP1_PCT","0.30")),
               "tp2": float(os.getenv("TP2_PCT","0.40")),
               "tp3": float(os.getenv("TP3_PCT","0.30"))}[t]
        take_partial_profit(symbol=symbol, ratio=pct, side=side)
        return

    if t in {"stoploss","emaexit","failcut","fullexit","close","exit","sl1","sl2"}:
        close_position(symbol=symbol, side=side, reason=t)
        return

    if t == "reducebycontracts":
        try:
            contracts = float(data.get("contracts", 0))
        except Exception:
            contracts = 0.0
        if contracts > 0:
            reduce_by_contracts(symbol=symbol, contracts=contracts, side=side)
        return

    # 모르는 타입은 알림만
    send_telegram(f"❓ unknown: {json.dumps(data, ensure_ascii=False)}")

def _worker():
    while True:
        data = _task_q.get()
        try:
            _handle_signal(data)
        except Exception as e:
            try: send_telegram(f"❗worker error: {e}")
            except Exception: pass
        finally:
            _task_q.task_done()

def _boot_workers():
    for _ in range(WORKERS):
        threading.Thread(target=_worker, daemon=True).start()

# =========================
# FastAPI 엔드포인트
# =========================
async def _parse_any(req: Request) -> Dict[str, Any]:
    # JSON
    try: return await req.json()
    except Exception: pass
    # Raw
    try:
        raw = (await req.body()).decode(errors="ignore")
        if raw:
            try: return json.loads(raw)
            except Exception:
                fixed = raw.replace("'", '"')
                return json.loads(fixed)
    except Exception: pass
    # Form
    try:
        form = await req.form()
        p = form.get("payload") or form.get("data") or ""
        if p: return json.loads(p)
    except Exception: pass
    raise HTTPException(status_code=400, detail="cannot parse payload")

@app.get("/health")
def health():
    try: pos = list(get_open_positions())
    except Exception: pos = []
    return {
        "DEFAULT_AMOUNT": DEFAULT_AMOUNT, "LEVERAGE": LEVERAGE,
        "FORCE_DEFAULT_AMOUNT": FORCE_DEFAULT_AMOUNT,
        "SYMBOL_AMOUNT": SYMBOL_AMOUNT,
        "qsize": _task_q.qsize(), "workers": WORKERS,
        "positions": pos,
    }

@app.get("/pending")
def pending():
    return get_pending_snapshot()

@app.post("/signal")
async def signal(req: Request):
    payload: Dict[str, Any] = await _parse_any(req)
    dk = _dedup_key(payload)
    now = time.time()
    if dk in _DEDUP and now - _DEDUP[dk] < DEDUP_TTL:
        return {"ok": True, "dedup": True}
    _DEDUP[dk] = now

    INGRESS_LOG.append({"ts": now, "data": payload})
    try:
        _task_q.put_nowait(payload)
    except queue.Full:
        send_telegram("⚠️ queue full → drop signal")
        return {"ok": False, "queued": False}
    return {"ok": True, "queued": True}

@app.get("/")
def root():
    return {"ok": True}

# =========================
# 스타트업
# =========================
_boot_workers()
start_watchdogs()       # -10% 즉시컷 + 2% 급반전 연속확인 컷
start_reconciler()
start_capacity_guard()
try:
    start_policy_manager()  # AI 튜너(있을 때만)
    send_telegram("🧠 Policy manager started")
except Exception:
    pass
send_telegram("✅ FastAPI up (workers + watchdog + reconciler + capacity-guard + policy + ai)")
