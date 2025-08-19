import os, time, json, hashlib, threading, queue, re
from collections import deque
from typing import Dict, Any
from fastapi import FastAPI, Request

from trader import (
    enter_position, take_partial_profit, close_position, reduce_by_contracts,
    start_watchdogs, start_reconciler, get_pending_snapshot
)
from telegram_bot import send_telegram
from bitget_api import convert_symbol, get_open_positions

DEFAULT_AMOUNT = float(os.getenv("DEFAULT_AMOUNT", "15"))
LEVERAGE       = float(os.getenv("LEVERAGE", "5"))
DEDUP_TTL      = float(os.getenv("DEDUP_TTL", "15"))
BIZDEDUP_TTL   = float(os.getenv("BIZDEDUP_TTL", "3"))

WORKERS        = int(os.getenv("WORKERS", "4"))
QUEUE_MAX      = int(os.getenv("QUEUE_MAX", "2000"))

LOG_INGRESS    = os.getenv("LOG_INGRESS", "0") == "1"

app = FastAPI()

INGRESS_LOG: deque = deque(maxlen=200)
_DEDUP: Dict[str, float] = {}
_BIZDEDUP: Dict[str, float] = {}
_task_q: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=QUEUE_MAX)

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
        j = await req.json()
        if isinstance(j, dict): return j
    except Exception: pass
    # 2) raw → json
    try:
        raw = (await req.body()).decode(errors="ignore").strip()
        if raw:
            try: return json.loads(raw)
            except Exception:
                return json.loads(raw.replace("'", '"'))
    except Exception: pass
    # 3) form(payload=...)
    try:
        form = await req.form()
        payload = form.get("payload") or form.get("data")
        if payload: return json.loads(payload)
    except Exception: pass
    # 4) key:value lines
    try:
        txt = (await req.body()).decode(errors="ignore")
        d: Dict[str, Any] = {}
        for part in re.split(r"[\n,]+", txt):
            if ":" in part:
                k, v = part.split(":", 1)
                d[k.strip()] = v.strip()
        if d: return d
    except Exception: pass
    raise ValueError("cannot parse request")

def _handle_signal(data: Dict[str, Any]):
    typ    = (data.get("type") or "").strip()
    symbol = _norm_symbol(data.get("symbol", ""))
    side   = _infer_side(data.get("side"), "long")

    amount   = float(data.get("amount", DEFAULT_AMOUNT))
    leverage = float(data.get("leverage", LEVERAGE))

    if not symbol:
        send_telegram("⚠️ symbol 없음: " + json.dumps(data)); return

    # legacy mapping
    legacy = {
        "tp_1": "tp1", "tp_2": "tp2", "tp_3": "tp3",
        "sl_1": "sl1", "sl_2": "sl2",
        "ema_exit": "emaExit", "failcut": "failCut",
        "stoploss": "close",  # ← 중요
    }
    typ = legacy.get(typ.lower(), typ)

    # short TTL biz-dedup
    now = time.time()
    bk = _biz_key(typ, symbol, side)
    if now - _BIZDEDUP.get(bk, 0.0) < BIZDEDUP_TTL:
        return
    _BIZDEDUP[bk] = now

    if LOG_INGRESS:
        try: send_telegram(f"📥 {typ} {symbol} {side} amt={amount}")
        except Exception: pass

    if typ == "entry":
        enter_position(symbol, amount, side=side, leverage=leverage); return

    if typ in ("tp1", "tp2", "tp3"):
        pct = float(os.getenv("TP1_PCT", "0.30")) if typ == "tp1" else \
              float(os.getenv("TP2_PCT", "0.40")) if typ == "tp2" else \
              float(os.getenv("TP3_PCT", "0.30"))
        take_partial_profit(symbol, pct, side=side); return

    if typ in ("sl1", "sl2", "failCut", "emaExit", "liquidation", "fullExit", "close", "exit", "stoploss"):
        close_position(symbol, side=side, reason=typ); return

    if typ == "reduceByContracts":
        c = float(data.get("contracts", 0))
        if c > 0: reduce_by_contracts(symbol, c, side=side); return

    if typ in ("tailTouch", "info", "debug"): return
    send_telegram("❓ 알 수 없는 신호: " + json.dumps(data))

def _worker_loop(i: int):
    while True:
        try:
            item = _task_q.get()
            if item is None: continue
            _handle_signal(item)
        except Exception as e:
            print(f"[worker-{i}] error:", e)
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

    INGRESS_LOG.append({"ts": now, "raw": data})
    try: _task_q.put_nowait(data)
    except queue.Full: return {"ok": False, "error": "queue_full"}
    return {"ok": True}

@app.post("/signal")
async def signal(req: Request): return await _ingest(req)

@app.get("/health")
def health():
    try: return {"ok": True, "positions": get_open_positions()}
    except Exception as e: return {"ok": False, "error": str(e)}

@app.get("/ingress")
def ingress(): return list(INGRESS_LOG)

@app.get("/pending")
def pending(): return get_pending_snapshot()

@app.on_event("startup")
def on_startup():
    for i in range(WORKERS):
        t = threading.Thread(target=_worker_loop, args=(i,), daemon=True)
        t.start()
    start_watchdogs()
    start_reconciler()
    try:
        threading.Thread(target=send_telegram,
            args=("✅ FastAPI up (workers+watchdog+reconciler)",), daemon=True).start()
    except Exception: pass
