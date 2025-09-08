# main.py — FastAPI: 시그널 처리/정상화, 전략 손절 reason 기록, 워커/리포트, 부트스트랩
import os, sys, time, json, hashlib, threading, queue, re, glob, subprocess
from collections import deque
from typing import Dict, Any, Optional
from fastapi import FastAPI, Request, Query, HTTPException

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from trader import (
    enter_position, take_partial_profit, close_position,
    start_watchdogs, start_reconciler, get_pending_snapshot, start_capacity_guard
)
from telegram_bot import send_telegram
from bitget_api import convert_symbol, get_open_positions

# ---- 옵션(있으면 포함) ----
try:
    from policy.tf_policy import ingest_signal, start_policy_manager
except Exception:
    def ingest_signal(*a, **kw): pass
    def start_policy_manager(): pass

DEFAULT_AMOUNT = float(os.getenv("DEFAULT_AMOUNT", "80"))
LEVERAGE       = float(os.getenv("LEVERAGE", "5"))
DEDUP_TTL      = float(os.getenv("DEDUP_TTL", "15"))
BIZDEDUP_TTL   = float(os.getenv("ENTRY_DUP_TTL_SEC", "3"))

WORKERS        = int(os.getenv("WORKERS", "6"))
QUEUE_MAX      = int(os.getenv("QUEUE_MAX", "2000"))
LOG_INGRESS    = os.getenv("LOG_INGRESS", "0") == "1"

FORCE_DEFAULT_AMOUNT = os.getenv("FORCE_DEFAULT_AMOUNT", "1") == "1"  # 기본 $80 강제
SYMBOL_AMOUNT_JSON = os.getenv("SYMBOL_AMOUNT_JSON", "")
try:
    SYMBOL_AMOUNT = json.loads(SYMBOL_AMOUNT_JSON) if SYMBOL_AMOUNT_JSON else {}
except Exception:
    SYMBOL_AMOUNT = {}

TRADE_LOG_DIR  = os.getenv("TRADE_LOG_DIR", "./trade_logs")
REPORT_DIR     = os.getenv("REPORT_DIR", "./reports")
LOGS_API_TOKEN = os.getenv("LOGS_API_TOKEN", "")

INGRESS_LOG: deque = deque(maxlen=200)
_DEDUP: Dict[str, float] = {}
_BIZDEDUP: Dict[str, float] = {}

_task_q: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=QUEUE_MAX)

app = FastAPI(title="fastapi-trading-bot", version="1.0.0")

# ---- utils ----
def _dedup_key(d: Dict[str, Any]) -> str:
    return hashlib.sha1(json.dumps(d, sort_keys=True).encode()).hexdigest()

def _norm_symbol(sym: str) -> str:
    return convert_symbol(sym)

def _infer_side(side: str, default: str = "long") -> str:
    s = (side or "").strip().lower()
    return s if s in ("long", "short") else default

def _norm_type(typ: str) -> str:
    t = (typ or "").strip().lower()
    t = re.sub(r"[\s_\-]+", "", t)
    aliases = {
        "tp_1": "tp1", "tp_2": "tp2", "tp_3": "tp3",
        "takeprofit1": "tp1", "takeprofit2": "tp2", "takeprofit3": "tp3",
        "sl_1": "sl1", "sl_2": "sl2",
        "stopfull": "stoploss", "stopall": "stoploss", "stop": "stoploss",
        "fullexit": "stoploss", "exitall": "stoploss",
        "emaexit": "emaexit", "failcut": "failcut",
        "closeposition": "close", "closeall": "close",
        "reducecontracts": "reducebycontracts",
        "reduce_by_contracts": "reducebycontracts",
        "entrybuy": "entry", "entrysell": "entry",
    }
    return aliases.get(t, t)

def _canon_tf(s: Optional[str]) -> Optional[str]:
    s = (s or "").strip().lower()
    if not s: return None
    s = s.replace(" ", "").replace("_", "")
    m = {"1m":"1m","3m":"3m","5m":"5m","15m":"15m","30m":"30m","1h":"1h","2h":"2h","4h":"4h","1d":"1d","d":"1d"}
    return m.get(s, s)

def _resolve_amount(symbol: str, default: float) -> float:
    """금액 결정: FORCE_DEFAULT_AMOUNT=1 이면 default 강제, 아니면 심볼 매핑 우선"""
    if FORCE_DEFAULT_AMOUNT:
        return default
    try:
        amt = SYMBOL_AMOUNT.get(symbol) or SYMBOL_AMOUNT.get(symbol.replace("USDT",""))
        return float(amt) if amt is not None else default
    except Exception:
        return default

# ---- worker ----
def _handle_signal(data: Dict[str, Any]):
    symbol = _norm_symbol(str(data.get("symbol") or data.get("ticker") or ""))
    typ_raw = str(data.get("type") or data.get("event") or data.get("reason") or "")
    side    = _infer_side(str(data.get("side") or data.get("direction") or "long"))
    tf      = _canon_tf(str(data.get("timeframe") or ""))

    if not symbol or not typ_raw:
        return

    resolved_amount = _resolve_amount(symbol, DEFAULT_AMOUNT)
    t = _norm_type(typ_raw)

    now = time.time()
    bizkey = f"{t}:{symbol}:{side}"
    if now - _BIZDEDUP.get(bizkey, 0.0) < BIZDEDUP_TTL:
        return
    _BIZDEDUP[bizkey] = now

    if LOG_INGRESS:
        try:
            send_telegram(f"📥 {t} {symbol} {side} amt={resolved_amount}")
        except Exception:
            pass

    if t == "entry":
        ingest_signal({"type":"entry","symbol":symbol,"side":side,"tf":tf})
        enter_position(symbol=symbol, side=side, usdt_amount=resolved_amount, timeframe=tf, leverage=LEVERAGE)

    elif t in ("tp1","tp2","tp3"):
        ratio = {"tp1":TP1_PCT,"tp2":TP2_PCT,"tp3":TP3_PCT}[t]
        ingest_signal({"type":t,"symbol":symbol,"side":side,"tf":tf})
        take_partial_profit(symbol=symbol, side=side, ratio=ratio)

    elif t in ("reducebycontracts",):
        # 필요 시 구현. (원본 구조 유지)
        pass

    # 전략 손절성 종료 — reason 남김
    elif t in ("stoploss","failcut","emaexit"):
        ingest_signal({"type":t,"symbol":symbol,"side":side,"tf":tf})
        close_position(symbol=symbol, side=side, reason=t)

    else:
        ingest_signal({"type":t,"symbol":symbol,"side":side,"tf":tf})
        close_position(symbol=symbol, side=side)

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

def _worker_boot():
    for _ in range(WORKERS):
        threading.Thread(target=_worker, daemon=True).start()

# ---- FastAPI ----
@app.get("/health")
def health():
    try:
        pos = list(get_open_positions())
    except Exception:
        pos = []
    return {
        "DEFAULT_AMOUNT": DEFAULT_AMOUNT, "LEVERAGE": LEVERAGE,
        "DEDUP_TTL": DEDUP_TTL, "BIZDEDUP_TTL": BIZDEDUP_TTL,
        "WORKERS": WORKERS, "QUEUE_MAX": QUEUE_MAX,
        "FORCE_DEFAULT_AMOUNT": FORCE_DEFAULT_AMOUNT,
        "SYMBOL_AMOUNT": SYMBOL_AMOUNT,
        "positions": pos
    }

@app.get("/pending")
def pending():
    return get_pending_snapshot()

@app.post("/signal")
async def signal_root(request: Request):
    payload: Dict[str, Any] = {}
    try:
        payload = await request.json()
    except Exception:
        try:
            form = await request.form()
            payload = json.loads(form.get("payload", "{}"))
        except Exception:
            payload = {}
    now = time.time()
    dk = _dedup_key(payload)
    if dk in _DEDUP and now - _DEDUP[dk] < DEDUP_TTL:
        return {"ok": True, "dedup": True}
    _DEDUP[dk] = now

    INGRESS_LOG.append({"ts": now, "ip": request.client.host if request.client else "?", "data": payload})
    try:
        _task_q.put_nowait(payload)
    except queue.Full:
        send_telegram("⚠️ queue full → drop signal: " + json.dumps(payload))
        return {"ok": False, "queued": False, "reason": "queue_full"}
    return {"ok": True, "queued": True}

@app.get("/ingress")
def ingress():
    return {"items": list(INGRESS_LOG)}

# ---- 리포트(API 토큰 보호) ----
def _auth_or_raise(token: Optional[str]):
    if LOGS_API_TOKEN and token != LOGS_API_TOKEN:
        raise HTTPException(status_code=401, detail="bad token")

@app.post("/reports/run")
def run_summary(days: Optional[int] = None, token: Optional[str] = Query(None)):
    _auth_or_raise(token)
    cmd = ["python3", "summarize_logs.py"]
    if days: cmd += ["--days", str(days)]
    try:
        os.makedirs(REPORT_DIR, exist_ok=True)
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        return {
            "ok": (res.returncode == 0),
            "stdout": (res.stdout or "")[-4000:],
            "stderr": (res.stderr or "")[-2000:],
            "reports": sorted([os.path.basename(p) for p in glob.glob(os.path.join(REPORT_DIR, "*"))])
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/reports/kpis")
def get_kpis(token: Optional[str] = Query(None)):
    _auth_or_raise(token)
    path = os.path.join(REPORT_DIR, "kpis.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="kpis.json not found (먼저 /reports/run 호출)")

# ---- 부트 ----
def _boot():
    _worker_boot()
    start_capacity_guard()
    start_watchdogs()
    start_reconciler()
    try:
        start_policy_manager()
    except Exception:
        pass
    try:
        threading.Thread(target=send_telegram, args=("✅ FastAPI up",), daemon=True).start()
    except Exception:
        pass

_boot()
