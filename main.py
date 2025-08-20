# main.py – minimal FastAPI server hooking TradingView → trader
import os, time
from typing import Dict, Any
from fastapi import FastAPI, Request
from pydantic import BaseModel

from trader import (
    enter_position, close_position, take_partial_profit,
    start_watchdogs, start_reconciler, start_supervisor,
    get_pending_snapshot,
)
from bitget_api import get_open_positions

try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str):
        print("[TG]", msg)

ENTRY_USDT  = float(os.getenv("ENTRY_USDT", "15"))
TP1_PCT     = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT     = float(os.getenv("TP2_PCT", "0.40"))
TP3_PCT     = float(os.getenv("TP3_PCT", "0.30"))
LOG_INGRESS = os.getenv("LOG_INGRESS", "0") == "1"

app = FastAPI()
_recent_ingress = []

class Signal(BaseModel):
    type: str
    symbol: str
    side: str | None = None
    amount: float | None = None

def _record_ingress(payload: Dict[str, Any]):
    if not LOG_INGRESS: 
        return
    ts = time.strftime("%H:%M:%S")
    _recent_ingress.append({"ts": ts, "payload": payload})
    if len(_recent_ingress) > 50:
        _recent_ingress.pop(0)

def _handle_signal(sig: Dict[str, Any]) -> Dict[str, Any]:
    typ = (sig.get("type") or "").strip()
    sym = sig.get("symbol")
    side = (sig.get("side") or "").lower() or "long"
    amount = float(sig.get("amount") or ENTRY_USDT)

    # legacy key map
    legacy = {
        "tp_1": "tp1", "tp_2": "tp2", "tp_3": "tp3",
        "sl_1": "sl1", "sl_2": "sl2",
        "ema_exit": "emaExit", "failcut": "failCut",
        "stoploss": "close",   # some strategies use stoploss
    }
    typ = legacy.get(typ.lower(), typ)

    if typ == "entry":
        enter_position(sym, amount, side=side)
        return {"ok": True}
    elif typ in ("tp1","tp_1"):
        take_partial_profit(sym, TP1_PCT, side=side)
        return {"ok": True}
    elif typ in ("tp2","tp_2"):
        take_partial_profit(sym, TP2_PCT, side=side)
        return {"ok": True}
    elif typ in ("tp3","tp_3"):
        take_partial_profit(sym, TP3_PCT, side=side)
        return {"ok": True}
    elif typ in ("sl1","sl_1","sl2","sl_2","failCut","emaExit","liquidation","close","exit"):
        close_position(sym, side=side, reason=typ)
        return {"ok": True}
    else:
        send_telegram(f"❓ unknown signal: {sig}")
        return {"ok": False, "error": "unknown type"}

@app.post("/signal")
async def signal(sig: Signal):
    payload = sig.dict()
    _record_ingress(payload)
    return _handle_signal(payload)

@app.get("/health")
def health():
    return {"ok": True, "time": time.time()}

@app.get("/positions")
def positions():
    return {"positions": get_open_positions()}

@app.get("/pending")
def pending():
    return get_pending_snapshot()

@app.get("/ingress")
def ingress():
    return {"recent": list(_recent_ingress)}

@app.on_event("startup")
async def on_startup():
    start_watchdogs()
    start_reconciler()
    start_supervisor()
    try:
        send_telegram("✅ FastAPI up (workers + watchdog + reconciler + supervisor)")
    except Exception:
        pass
