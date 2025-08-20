# main.py – FastAPI ingress (큐/중복제거/워커/조용한 시작)
import os, time, json, hashlib, threading, queue
from collections import deque
from typing import Dict, Any
from fastapi import FastAPI, Request

from trader import (
    enter_position, close_position, take_partial_profit,
    reduce_by_contracts, start_watchdogs, start_reconciler,
    start_supervisor, get_pending_snapshot,
)
from bitget_api import convert_symbol, get_open_positions

try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str):
        print("[TG]", msg)

DEFAULT_AMOUNT = float(os.getenv("DEFAULT_AMOUNT", "15"))
LEVERAGE       = float(os.getenv("LEVERAGE", "5"))
WORKERS        = int(os.getenv("WORKERS", "4"))
QUEUE_MAX      = int(os.getenv("QUEUE_MAX", "2000"))
DEDUP_TTL      = float(os.getenv("DEDUP_TTL", "15"))
BIZDEDUP_TTL   = float(os.getenv("BIZDEDUP_TTL", "3"))
LOG_INGRESS    = os.getenv("LOG_INGRESS", "0") == "1"
STARTUP_NOTIFY = os.getenv("STARTUP_NOTIFY", "0") == "1"   # 기본 끔

app = FastAPI()
INGRESS_LOG: deque = deque(maxlen=200)
_DEDUP: Dict[str, float] = {}
_BIZDEDUP: Dict[str, float] = {}
_task_q: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=QUEUE_MAX)

def _dedup_key(d: Dict[str, Any]) -> str:
    return hashlib.sha1(json.dumps(d, sort_keys=True).encode()).hexdigest()

def _biz_key(typ: str, symbol: str, side: str) -> str:
    return f"{typ}:{symbol}:{side}"

def _infer_side(v: str, default="long") -> str:
    s = (v or "").strip().lower()
    return s if s in ("long","short") else default

async def _parse_any(req: Request) -> Dict[str, Any]:
    # 1) JSON
    try: return await req.json()
    except Exception: pass
    # 2) raw body
    try:
        raw = (await req.body()).decode(errors="ignore").strip()
        if raw:
            try: return json.loads(raw)
            except Exception: return json.loads(raw.replace("'", '"'))
    except Exception: pass
    # 3) form(payload)
    try:
        form = await req.form()
        p = form.get("payload") or form.get("data")
        if p: return json.loads(p)
    except Exception: pass
    return {}

def _route(sig: Dict[str, Any]):
    typ = (sig.get("type") or "").strip()
    sym = convert_symbol(sig.get("symbol", ""))
    side = _infer_side(sig.get("side"), "long")
    amount   = float(sig.get("amount", DEFAULT_AMOUNT))
    leverage = float(sig.get("leverage", LEVERAGE))

    if not sym: 
        send_telegram("⚠️ symbol 없음: " + json.dumps(sig))
        return

    legacy = {
        "tp_1":"tp1","tp_2":"tp2","tp_3":"tp3",
        "sl_1":"sl1","sl_2":"sl2",
        "ema_exit":"emaExit","failcut":"failCut",
        "stoploss":"close",
    }
    typ = legacy.get(typ.lower(), typ)

    if LOG_INGRESS:
        try: send_telegram(f"📥 {typ} {sym} {side} amt={amount}")
        except Exception: pass

    if typ == "entry":
        enter_position(sym, amount, side=side, leverage=leverage)
    elif typ in ("tp1","tp_1"):
        from trader import TP1_PCT; take_partial_profit(sym, TP1_PCT, side=side)
    elif typ in ("tp2","tp_2"):
        from trader import TP2_PCT; take_partial_profit(sym, TP2_PCT, side=side)
    elif typ in ("tp3","tp_3"):
        from trader import TP3_PCT; take_partial_profit(sym, TP3_PCT, side=side)
    elif typ in ("sl1","sl_1","sl2","sl_2","failCut","emaExit","liquidation","close","exit"):
        close_position(sym, side=side, reason=typ)
    elif typ.startswith("reduce:"):
        try:
            qty = float(typ.split(":",1)[1])
            reduce_by_contracts(sym, qty, side=side)
        except Exception:
            pass

def _worker_loop(i: int):
    while True:
        try:
            data = _task_q.get()
            if data is not None:
                _route(data)
        except Exception as e:
            print(f"[worker-{i}] error:", e)
        finally:
            _task_q.task_done()

async def _ingest(req: Request):
    now = time.time()
    data = {}
    try: data = await _parse_any(req)
    except Exception: pass

    key = _dedup_key(data)
    last = _DEDUP.get(key, 0.0)
    if now - last < DEDUP_TTL:
        return {"ok": False, "duplicate": True}
    _DEDUP[key] = now

    # 업무 dedup
    typ = (data.get("type") or "").lower()
    sym = convert_symbol(data.get("symbol",""))
    side = _infer_side(data.get("side"), "long")
    bk = _biz_key(typ, sym, side)
    if now - _BIZDEDUP.get(bk, 0.0) < BIZDEDUP_TTL:
        return {"ok": False, "queued": False, "reason": "biz_dedup"}
    _BIZDEDUP[bk] = now

    INGRESS_LOG.append({"ts": now, "data": data})
    try:
        _task_q.put_nowait({"type": typ, "symbol": sym, "side": side,
                            "amount": float(data.get("amount", DEFAULT_AMOUNT)),
                            "leverage": float(data.get("leverage", LEVERAGE))})
        return {"ok": True, "queued": True, "qsize": _task_q.qsize()}
    except queue.Full:
        send_telegram("⚠️ queue full → drop signal: " + json.dumps(data))
        return {"ok": False, "queued": False, "reason": "queue_full"}

@app.post("/signal")
async def signal(req: Request):  return await _ingest(req)
@app.post("/webhook")
async def webhook(req: Request): return await _ingest(req)
@app.post("/alert")
async def alert(req: Request):   return await _ingest(req)

@app.get("/health")
def health():    return {"ok": True, "workers": WORKERS}
@app.get("/positions")
def positions(): return {"positions": get_open_positions()}
@app.get("/pending")
def pending():   return get_pending_snapshot()
@app.get("/ingress")
def ingress():   return list(INGRESS_LOG)[-30:]
@app.get("/queue")
def queue_size():return {"size": _task_q.qsize()}

@app.on_event("startup")
async def on_startup():
    for i in range(WORKERS):
        t = threading.Thread(target=_worker_loop, args=(i,), name=f"worker-{i}", daemon=True)
        t.start()
    start_watchdogs(); start_reconciler(); start_supervisor()
    if STARTUP_NOTIFY:
        try: send_telegram("✅ FastAPI up (workers + watchdog + reconciler + supervisor)")
        except Exception: pass
