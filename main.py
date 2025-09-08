# main.py — FastAPI entrypoint (ADD-ONLY)
import os, sys, time, json, hashlib, threading, queue, re, glob, subprocess
from collections import deque
from typing import Dict, Any, Optional
from fastapi import FastAPI, Request, Query, HTTPException

# --- path guard (import 경로 안전) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from trader import (
    enter_position, take_partial_profit, close_position, reduce_by_contracts,
    start_watchdogs, start_reconciler, get_pending_snapshot, start_capacity_guard
)
from telegram_bot import send_telegram
from bitget_api import convert_symbol, get_open_positions

# 정책/텔레메트리/리스크가드 (비침투적 추가)
from tf_policy import ingest_signal, start_policy_manager      # ← 경로: 레포에 tf_policy.py가 루트면 이렇게
# from policy.tf_policy import ingest_signal, start_policy_manager  # policy/ 하위면 이 줄로 바꾸세요
from risk_guard import can_open                                # (있으면 동작, 없으면 주석처리 가능)

try:
    from telemetry.logger import log_event                     # [OPT]
except Exception:
    def log_event(*a, **kw): pass

DEFAULT_AMOUNT = float(os.getenv("DEFAULT_AMOUNT", "15"))
LEVERAGE       = float(os.getenv("LEVERAGE", "5"))
DEDUP_TTL      = float(os.getenv("DEDUP_TTL", "15"))
BIZDEDUP_TTL   = float(os.getenv("BIZDEDUP_TTL", "3"))

WORKERS        = int(os.getenv("WORKERS", "6"))
QUEUE_MAX      = int(os.getenv("QUEUE_MAX", "2000"))
LOG_INGRESS    = os.getenv("LOG_INGRESS", "0") == "1"

FORCE_DEFAULT_AMOUNT = os.getenv("FORCE_DEFAULT_AMOUNT", "0") == "1"
SYMBOL_AMOUNT_JSON = os.getenv("SYMBOL_AMOUNT_JSON", "")
try:
    SYMBOL_AMOUNT = json.loads(SYMBOL_AMOUNT_JSON) if SYMBOL_AMOUNT_JSON else {}
except Exception:
    SYMBOL_AMOUNT = {}

LOG_DIR   = os.getenv("TRADE_LOG_DIR", "./logs")
REPORT_DIR = "./reports"
LOGS_API_TOKEN = os.getenv("LOGS_API_TOKEN", "")

app = FastAPI()

INGRESS_LOG: deque = deque(maxlen=200)
_DEDUP: Dict[str, float] = {}
_BIZDEDUP: Dict[str, float] = {}
_task_q: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=QUEUE_MAX)

# ──────────────────────────────────────────────────────────────
# 유틸
# ──────────────────────────────────────────────────────────────
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
        "emaexit": "emaexit",
        "failcut": "failcut",
        "closeposition": "close", "closeall": "close",
        "reducecontracts": "reducebycontracts",
        "reduce_by_contracts": "reducebycontracts",
    }
    return aliases.get(t, t)

def _canon_tf(s: Optional[str]) -> Optional[str]:
    if not s: return None
    s = s.strip().lower()
    return s if s in ("1h","2h","3h","4h","d") else None

def _auth_or_raise(token: Optional[str]):
    if LOGS_API_TOKEN and token != LOGS_API_TOKEN:
        raise HTTPException(status_code=401, detail="invalid token")

# ──────────────────────────────────────────────────────────────
# Payload 파서(느슨하게)
# ──────────────────────────────────────────────────────────────
async def _parse_any(req: Request) -> Dict[str, Any]:
    # JSON
    try:
        return await req.json()
    except Exception:
        pass
    # Raw body(JSON-like)
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
    # Form
    try:
        form = await req.form()
        payload = form.get("payload") or form.get("data")
        if payload:
            return json.loads(payload)
    except Exception:
        pass
    # key:value lines
    try:
        txt = (await req.body()).decode(errors="ignore")
        d: Dict[str, Any] = {}
        for part in re.split(r"[\n,]+", txt):
            if ":" in part:
                k, v = part.split(":", 1)
                d[k.strip()] = v.strip()
        if d:
            return d
    except Exception:
        pass
    raise ValueError("cannot parse request")

# ──────────────────────────────────────────────────────────────
# 시그널 라우팅
# ──────────────────────────────────────────────────────────────
def _handle_signal(data: Dict[str, Any]):
    typ_raw = (data.get("type") or "")
    symbol  = _norm_symbol(data.get("symbol", ""))
    side    = _infer_side(data.get("side"), "long")

    # 원본 로그(선택)
    try: log_event(data, stage="ingress")
    except: pass

    # TF 힌트 수집 + 정책엔진
    try: ingest_signal(data)
    except: pass

    amount   = float(data.get("amount", DEFAULT_AMOUNT))
    leverage = float(data.get("leverage", LEVERAGE))

    # 심볼별 금액 우선
    resolved_amount = float(amount)
    if (symbol in SYMBOL_AMOUNT) and (str(SYMBOL_AMOUNT[symbol]).strip() != ""):
        try: resolved_amount = float(SYMBOL_AMOUNT[symbol])
        except Exception: resolved_amount = float(DEFAULT_AMOUNT)
    elif FORCE_DEFAULT_AMOUNT:
        resolved_amount = float(DEFAULT_AMOUNT)

    if not symbol:
        send_telegram("⚠️ symbol 없음: " + json.dumps(data))
        return

    t = _norm_type(typ_raw)

    # 너무 잦은 동일 비즈니스 이벤트 차단
    now = time.time()
    bizkey = f"{t}:{symbol}:{side}"
    last = _BIZDEDUP.get(bizkey, 0.0)
    if now - last < BIZDEDUP_TTL:
        return
    _BIZDEDUP[bizkey] = now

    if LOG_INGRESS:
        try:
            send_telegram(f"📥 {t} {symbol} {side} amt={resolved_amount}")
        except Exception:
            pass

    # 라우팅
    if t == "entry":
        # === 리스크 예산 체크 (있는 경우에만 사용) ===
        try:
            allowed = can_open({
                "symbol": symbol, "side": side,
                "entry_price": data.get("entry_price") or 0,
                "size": resolved_amount
            })
        except Exception:
            allowed = True
        if not allowed:
            try: send_telegram(f"⛔ RiskGuard block {symbol} {side} amt={resolved_amount} tf={_canon_tf(str(data.get('timeframe') or ''))}")
            except Exception: pass
            try: log_event({"event":"guard_block","symbol":symbol,"side":side,"amount":resolved_amount}, stage="guard")
            except Exception: pass
            return
        enter_position(symbol, resolved_amount, side=side, leverage=leverage)
        return

    if t in ("tp1", "tp2", "tp3"):
        pct = float(os.getenv("TP1_PCT", "0.30")) if t == "tp1" else \
              float(os.getenv("TP2_PCT", "0.40")) if t == "tp2" else \
              float(os.getenv("TP3_PCT", "0.30"))
        take_partial_profit(symbol, pct, side=side)
        return

    CLOSE_KEYS = {"stoploss","emaexit","failcut","fullexit","close","exit","liquidation","sl1","sl2"}
    if t in CLOSE_KEYS:
        close_position(symbol, side=side, reason=t)
        return

    if t == "reducebycontracts":
        contracts = float(data.get("contracts", 0))
        if contracts > 0:
            reduce_by_contracts(symbol, contracts, side=side)
        return

    if t in ("tailtouch", "info", "debug"):
        return

    send_telegram("❓ 알 수 없는 신호: " + json.dumps(data))

# ──────────────────────────────────────────────────────────────
# 워커/엔드포인트/스타트업
# ──────────────────────────────────────────────────────────────
def _worker_loop(idx: int):
    while True:
        try:
            data = _task_q.get()
            if data is None:
                continue
            _handle_signal(data)
        except Exception as e:
            print(f"[worker-{idx}] error:", e)
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
        send_telegram("⚠️ queue full → drop signal: " + json.dumps(data))
        return {"ok": False, "queued": False, "reason": "queue_full"}

    return {"ok": True, "queued": True, "qsize": _task_q.qsize()}

@app.get("/")
def root():
    return {"ok": True}

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

@app.get("/positions")
def positions():
    return {"positions": get_open_positions()}

@app.get("/queue")
def queue_size():
    return {"size": _task_q.qsize(), "max": QUEUE_MAX}

@app.get("/config")
def config():
    return {
        "DEFAULT_AMOUNT": DEFAULT_AMOUNT, "LEVERAGE": LEVERAGE,
        "DEDUP_TTL": DEDUP_TTL, "BIZDEDUP_TTL": BIZDEDUP_TTL,
        "WORKERS": WORKERS, "QUEUE_MAX": QUEUE_MAX,
        "LOG_INGRESS": LOG_INGRESS,
        "FORCE_DEFAULT_AMOUNT": FORCE_DEFAULT_AMOUNT,
        "SYMBOL_AMOUNT": SYMBOL_AMOUNT,
    }

@app.get("/pending")
def pending():
    return get_pending_snapshot()

# === 시간봉 강제 태깅 웹훅 (Pine 수정 없이 URL만 바꿈) ===
def _ingest_with_tf_override(data: Dict[str, Any], tf: str):
    now = time.time()
    d = dict(data or {}); d["timeframe"] = tf
    dk = _dedup_key(d)
    if dk in _DEDUP and now - _DEDUP[dk] < DEDUP_TTL:
        return {"ok": True, "dedup": True, "tf": tf}
    _DEDUP[dk] = now
    INGRESS_LOG.append({"ts": now, "ip": "tf-override", "data": d})
    try:
        _task_q.put_nowait(d)
    except queue.Full:
        send_telegram("⚠️ queue full → drop signal(tf): " + json.dumps(d))
        return {"ok": False, "queued": False, "reason": "queue_full"}
    return {"ok": True, "queued": True, "qsize": _task_q.qsize(), "tf": tf}

@app.post("/signal/1h")  async def signal_1h(req: Request):  d = await _parse_any(req); return _ingest_with_tf_override(d, "1H")
@app.post("/signal/2h")  async def signal_2h(req: Request):  d = await _parse_any(req); return _ingest_with_tf_override(d, "2H")
@app.post("/signal/3h")  async def signal_3h(req: Request):  d = await _parse_any(req); return _ingest_with_tf_override(d, "3H")
@app.post("/signal/4h")  async def signal_4h(req: Request):  d = await _parse_any(req); return _ingest_with_tf_override(d, "4H")
@app.post("/signal/1h/") async def signal_1h_slash(req: Request): d = await _parse_any(req); return _ingest_with_tf_override(d, "1H")
@app.post("/signal/2h/") async def signal_2h_slash(req: Request): d = await _parse_any(req); return _ingest_with_tf_override(d, "2H")
@app.post("/signal/3h/") async def signal_3h_slash(req: Request): d = await _parse_any(req); return _ingest_with_tf_override(d, "3H")
@app.post("/signal/4h/") async def signal_4h_slash(req: Request): d = await _parse_any(req); return _ingest_with_tf_override(d, "4H")

# ──────────────────────────────────────────────────────────────
# 파일 로그/리포트(옵션)
# ──────────────────────────────────────────────────────────────
@app.get("/logs/list")
def list_logs(token: Optional[str] = Query(None)):
    _auth_or_raise(token)
    try:
        files = sorted(glob.glob(os.path.join(LOG_DIR, "*")))
        return {"dir": LOG_DIR, "files": [os.path.basename(f) for f in files]}
    except Exception as e:
        return {"error": str(e), "dir": LOG_DIR}

@app.get("/logs/tail")
def tail_log(file: str, lines: int = 50, token: Optional[str] = Query(None)):
    _auth_or_raise(token)
    path = os.path.join(LOG_DIR, file)
    try:
        with open(path, "r", encoding="utf-8") as f:
            buf = f.readlines()[-max(1, min(lines, 1000)):]
        return {"file": file, "lines": buf}
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"not found: {file}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/logs/selftest")
def logs_selftest(token: Optional[str] = Query(None)):
    _auth_or_raise(token)
    try:
        log_event({"event": "selftest", "msg": "hello", "stage": "self"}, stage="selftest")
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/reports/run")
def run_summary(days: Optional[int] = None, token: Optional[str] = Query(None)):
    _auth_or_raise(token)
    cmd = ["python3", "tools/summarize_logs.py"]
    if days is not None:
        cmd += ["--days", str(days)]
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

@app.get("/reports/run")
def run_summary_get(days: Optional[int] = None, token: Optional[str] = Query(None)):
    return run_summary(days=days, token=token)

@app.get("/reports/kpis")
def get_kpis(token: Optional[str] = Query(None)):
    _auth_or_raise(token)
    path = os.path.join(REPORT_DIR, "kpis.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="kpis.json not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/reports/download")
def download_report(name: str, token: Optional[str] = Query(None)):
    _auth_or_raise(token)
    path = os.path.join(REPORT_DIR, name)
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail=f"report not found: {name}")
    with open(path, "r", encoding="utf-8") as f:
        return {"name": name, "content": f.read()}

# ──────────────────────────────────────────────────────────────
# 스타트업 훅
# ──────────────────────────────────────────────────────────────
def _worker_boot():
    for i in range(WORKERS):
        t = threading.Thread(target=_worker_loop, args=(i,), daemon=True, name=f"signal-worker-{i}")
        t.start()

@app.on_event("startup")
def on_startup():
    _worker_boot()
    start_capacity_guard()
    start_watchdogs()
    start_reconciler()
    start_policy_manager()   # 정책/AI 매니저 시작
    try:
        threading.Thread(
            target=send_telegram,
            args=("✅ FastAPI up (workers + watchdog + reconciler + capacity-guard + policy + ai)",),
            daemon=True
        ).start()
    except Exception:
        pass
