# -*- coding: utf-8 -*-
import os, time, json, threading
from typing import Any, Dict, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from logger import info
from telegram_bot import send_telegram
from trader import (
    enter_position, close_position, take_partial_profit,
    start_watchdogs, start_reconciler, start_capacity_guard,
    get_pending_snapshot
)
from kpi_pipeline import start_kpi_pipeline, aggregate_and_save, list_trades
from bitget_api import convert_symbol

APP_NAME = os.getenv("APP_NAME","AutoTrader")
APP_VER  = os.getenv("APP_VER","2025-09-09")

TP1_PCT = float(os.getenv("TP1_PCT","0.30"))
TP2_PCT = float(os.getenv("TP2_PCT","0.5714286"))
TP3_PCT = float(os.getenv("TP3_PCT","1.0"))

app = FastAPI(title=APP_NAME, version=APP_VER)

class SignalReq(BaseModel):
    type: str
    symbol: str
    side: Optional[str] = None
    amount: Optional[float] = None
    timeframe: Optional[str] = None

def _boot():
    try:
        start_kpi_pipeline()
    except Exception as e:
        info("kpi start error", err=str(e))
    start_watchdogs(); start_reconciler(); start_capacity_guard()
    send_telegram("✅ FastAPI up (workers + watchdog + reconciler + guards + AI)")

@app.on_event("startup")
def on_startup(): _boot()

@app.get("/")
def root(): return {"ok": True, "name": APP_NAME, "version": APP_VER}

@app.get("/health")
def health(): return {"ok": True, "ts": int(time.time())}

@app.get("/version")
def version(): return {"ok": True, "version": APP_VER}

@app.post("/signal")
def signal(req: SignalReq):
    t = (req.type or "").lower().strip()
    sym = convert_symbol(req.symbol)
    side = (req.side or "").lower().strip()
    amt = req.amount
    tf  = req.timeframe
    try:
        if t in ("entry","open"):
            if side not in ("long","short"): raise HTTPException(400, "side must be long/short")
            r = enter_position(sym, side=side, usdt_amount=amt, timeframe=tf); return {"ok": True, "res": r}
        if t in ("close","exit"):
            if side not in ("long","short"): raise HTTPException(400, "side must be long/short")
            r = close_position(sym, side=side, reason="signal_close"); return {"ok": True, "res": r}
        if t in ("tp1","tp_1","takeprofit1"):
            r = take_partial_profit(sym, ratio=TP1_PCT, side=side, reason="tp1"); return {"ok": True, "res": r}
        if t in ("tp2","tp_2","takeprofit2"):
            r = take_partial_profit(sym, ratio=TP2_PCT, side=side, reason="tp2"); return {"ok": True, "res": r}
        if t in ("tp3","tp_3","takeprofit3"):
            r = take_partial_profit(sym, ratio=TP3_PCT, side=side, reason="tp3"); return {"ok": True, "res": r}
    except HTTPException:
        raise
    except Exception as e:
        return {"ok": False, "error": str(e)}
    raise HTTPException(400, "unknown type")

@app.get("/debug/symbol/{symbol}")
def debug_symbol(symbol: str):
    from bitget_api import get_symbol_spec, get_last_price
    core = convert_symbol(symbol)
    return {"core": core, "spec": get_symbol_spec(core), "last": get_last_price(core)}

@app.get("/debug/positions")
def debug_positions():
    from bitget_api import get_open_positions
    return {"positions": get_open_positions(None)}

@app.get("/snapshot")
def snapshot(): return get_pending_snapshot()

@app.post("/reports/kpis")
def force_kpis():
    aggregate_and_save(); return {"ok": True}

@app.get("/reports/kpis")
def get_kpis():
    p = os.path.join(os.getenv("REPORT_DIR","./reports"), "kpis.json")
    try:
        with open(p,"r",encoding="utf-8") as f: return json.load(f)
    except Exception:
        return {"updated": time.time(), "winrate":0.0, "avg_roe":0.0, "n":0}

@app.get("/reports/trades")
def get_trades_csv():
    return {"csv": list_trades()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT","8080")), reload=False)
