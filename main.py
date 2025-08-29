import os, time, json, hashlib, threading, queue, re
from collections import deque
from typing import Dict, Any
from fastapi import FastAPI, Request

from trader import (
    enter_position, take_partial_profit, close_position, reduce_by_contracts,
    start_watchdogs, start_reconciler, get_pending_snapshot, start_capacity_guard
)
from telegram_bot import send_telegram
from bitget_api import convert_symbol, get_open_positions

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

app = FastAPI()

INGRESS_LOG: deque = deque(maxlen=200)
_DEDUP: Dict[str, float] = {}
_BIZDEDUP: Dict[str, float] = {}
_task_q: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=QUEUE_MAX)

def _biz_key(typ: str, symbol: str, side: str) -> str:
    return f"{typ}:{symbol}:{side}"

def _infer_side(side: str, default: str = "long") -> str:
    s = (side or "").strip().lower()
    return s if s in ("long", "short") else default

def _norm_symbol(sym: str) -> str:
    return convert_symbol(sym)

async def _parse_any(req: Request) -> Dict[str, Any]:
    try:
        return await req.json()
    except Exception:
        pass
    try:
        raw = (await req.body()).decode(errors="ignore").strip()
        if raw:
            try: return json.loads(raw)
            except Exception:
                fixed = raw.replace("'", '"'); return json.loads(fixed)
    except Exception:
        pass
    try:
        form = await req.form()
        payload = form.get("payload") or form.get("data")
        if payload: return json.loads(payload)
    except Exception:
        pass
    try:
        txt = (await req.body()).decode(errors="ignore")
        d: Dict[str, Any] = {}
        for part in re.split(r"[\n,]+", txt):
            if ":" in part:
                k, v = part.split(":", 1); d[k].strip(); d[k.strip()] = v.strip()
        if d: return d
    except Exception:
        pass
    raise ValueError("cannot parse request")

def _handle_signal(data: Dict[str, Any]):
    typ0   = (data.get("type") or "").strip()
    symbol = _norm_symbol(data.get("symbol", ""))
    side   = _infer_side(data.get("side"), "long")

    amount   = float(data.get("amount", DEFAULT_AMOUNT))
    leverage = float(data.get("leverage", LEVERAGE))

    resolved_amount = float(amount)
    if (symbol in SYMBOL_AMOUNT) and (str(SYMBOL_AMOUNT[symbol]).strip() != ""):
        try: resolved_amount = float(SYMBOL_AMOUNT[symbol])
        except Exception: resolved_amount = float(DEFAULT_AMOUNT)
    elif FORCE_DEFAULT_AMOUNT:
        resolved_amount = float(DEFAULT_AMOUNT)

    if not symbol:
        send_telegram("⚠️ symbol 없음: " + json.dumps(data)); return

    t = typ0.lower().replace(" ", "")
    legacy_map = {
        "tp_1": "tp1", "tp_2": "tp2", "tp_3": "tp3",
        "sl_1": "sl1", "sl_2": "sl2",
        "ema_exit": "emaExit", "failcut": "failCut",
    }
    t = legacy_map.get(t, t)

    # stoploss 동의어 → 즉시 전체 청산
    STOP_KEYS = {"stoploss", "stop_loss", "sl", "stop", "stopall", "stopfull"}
    if t in STOP_KEYS:
        t = "stoploss"

    now = time.time()
    bk = _biz_key(t, symbol, side)
    if bk in _BIZDEDUP and now - _BIZDEDUP[bk] < BIZDEDUP_TTL:
        return
    _BIZDEDUP[bk] = now

    if LOG_INGRESS:
        try: send_telegram(f"📥 {t} {symbol} {side} amt={resolved_amount}")
        except: pass

    if t == "entry":
        enter_position(symbol, resolved_amount, side=side, leverage=leverage); return

    if t in ("tp1","tp2","tp3"):
        pct = float(os.getenv("TP1_PCT","0.30")) if t=="tp1" else float(os.getenv("TP2_PCT","0.40")) if t=="tp2" else float(os.getenv("TP3_PCT","0.30"))
        take_partial_profit(symbol, pct, side=side); return

    if t in ("sl1","sl2","failCut","emaExit","liquidation","fullExit","close","exit","stoploss"):
        close_position(symbol, side=side, reason=t); return

    if t == "reduceByContracts":
        contracts = float(data.get("contracts", 0))
        if contracts > 0: reduce_by_contracts(symbol, contracts, side=side)
        return

    if t in ("tailTouch","info","debug"): return

    send_telegram("❓ 알 수 없는 신호: " + json.dumps(data))

def _worker_loop(idx: int):
    while True:
        try:
            data = _task_q.get()
            if data is None: continue
            _handle_signal(data)
        except Exception as e:
            print(f"[worker-{idx}] error:", e)
        finally:
            _task_q.task_done()

def _dedup_key(d: Dict[str, Any]) -> str:
    return hashlib.sha1(json.dumps(d, sort_keys=True).encode()).hexdigest()

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

app = FastAPI()

app.get("/")
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

@app.on_event("startup")
def on_startup():
    for i in range(WORKERS):
        t = threading.Thread(target=_worker_loop, args=(i,), daemon=True, name=f"signal-worker-{i}")
        t.start()
    start_capacity_guard()
    start_watchdogs()
    start_reconciler()
    try:
        threading.Thread(target=send_telegram,
                         args=("✅ FastAPI up (workers + watchdog + reconciler + capacity-guard)",),
                         daemon=True).start()
    except:
        pass
