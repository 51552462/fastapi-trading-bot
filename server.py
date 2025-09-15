# server.py — FastAPI trading-bot gateway (health OK + non-blocking startup)
from __future__ import annotations

import os
import json
import time
import threading
from collections import deque
from typing import Any, Dict, Tuple

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

# 내부 모듈
from trader import (
    start_all_backgrounds,
    enter_position,
    take_partial_profit,
    reduce_by_contracts,
    close_position,
)
from bitget_api import get_open_positions, get_last_price

app = FastAPI()

# ───── Health  Root
@app.get("/health")
async def health():
    return {"ok": True}

@app.get("/")
async def root():
    return {"ok": True, "name": "trading-bot"}

# ───── 진단
@app.get("/diagenv")
async def diag_env():
    keys = [
        "BITGET_PRODUCT_TYPE",
        "BITGET_POSITION_MODE", 
        "BITGET_MARGIN_MODE",
        "AMOUNT_MODE",
        "BITGET_HOST",
        "WEB_CONCURRENCY",
        "BITGET_DEBUG",
        "PUBLIC_BASE_URL",
    ]
    return {k: os.getenv(k) for k in keys}

@app.get("/diagpositions")
async def diag_positions(symbol: str = None):
    try:
        pos = get_open_positions(symbol)
        return {"ok": True, "count": len(pos), "data": pos}
    except Exception as e:
        return {"ok": False, "err": f"{type(e).__name__}: {e}"}

@app.get("/diagticker")
async def diag_ticker(symbol: str):
    try:
        p = get_last_price(symbol)
        return {"ok": p is not None, "price": p}
    except Exception as e:
        return {"ok": False, "err": f"{type(e).__name__}: {e}"}

# ───── 최근 수신에러 로그(간단)
_LAST_SIGNALS = deque(maxlen=50)
_LAST_ERRORS = deque(maxlen=50)

def _record_signal(j: dict, note: str = "") -> None:
    try:
        _LAST_SIGNALS.appendleft({"ts": time.time(), "note": note, "json": j})
    except Exception:
        pass

def _record_error(where: str, err: str) -> None:
    try:
        _LAST_ERRORS.appendleft({"ts": time.time(), "where": where, "err": err})
    except Exception:
        pass

@app.get("/diagsignals")
async def diag_signals():
    return {"ok": True, "recent": list(_LAST_SIGNALS)}

@app.get("/diagerrors")
async def diag_errors():
    return {"ok": True, "recent": list(_LAST_ERRORS)}

# ───── Webhook dedupe (3s window)
_DEDUPE_WIN = 3.0
_LAST_BODIES = deque(maxlen=200)

async def _read_json_safely(req: Request) -> Tuple[Dict[str, Any], str]:
    raw = await req.body()
    raw_s = raw.decode("utf-8", "ignore")
    try:
        j = json.loads(raw_s or "{}")
        if not isinstance(j, dict):
            j = {}
    except Exception:
        j = {}
    return j, raw_s

def _dedupe_check(raw: str) -> bool:
    now = time.time()
    try:
        for ts, body in list(_LAST_BODIES):
            if (now - ts) <= _DEDUPE_WIN and body == raw:
                return True
        _LAST_BODIES.append((now, raw))
    except Exception:
        pass
    return False

def _should_bypass_dedupe(j: dict, req: Request) -> bool:
    try:
        if isinstance(j, dict) and j.get("dedupe") is False:
            return True
        return (req.query_params.get("nd") == "1")
    except Exception:
        return False

# ───── 시그널 라우팅
def _handle_signal(j: Dict[str, Any]) -> Dict[str, Any]:
    t = (j.get("type") or "").strip()
    sym = (j.get("symbol") or j.get("ticker") or "").upper().replace(" ", "")
    side = (j.get("side") or "").lower()
    amount = j.get("amount")
    lev = j.get("leverage")

    if not t or not sym:
        return {"ok": False, "msg": "missing type/symbol"}

    if t == "entry":
        if side not in ("long", "short"):
            return {"ok": False, "msg": "bad side"}
        return {"ok": True, "res": enter_position(sym, side=side, usdt_amount=amount, leverage=lev)}

    if t in ("tp1", "tp2", "tp3"):
        ratio = 1.0
        if t == "tp1":
            ratio = float(os.getenv("TP1_PCT", "0.30"))
        if t == "tp2":
            ratio = float(os.getenv("TP2_PCT", "0.5714286"))
        if t == "tp3":
            ratio = 1.0
        return {"ok": True, "res": take_partial_profit(sym, ratio=ratio, side=side or "long", reason=t)}

    if t == "tp_qty":
        qty = float(j.get("qty") or 0.0)
        return {"ok": True, "res": take_partial_profit(sym, ratio=0.0, side=side or "long", reason=f"tp_qty:{qty}")}

    if t in ("sl1", "sl2", "failCut", "emaExit", "stoploss", "close", "policyClose"):
        return {"ok": True, "res": close_position(sym, side=side or "long", reason=t)}

    if t == "reduce":
        qty = float(j.get("qty") or 0.0)
        return {"ok": True, "res": reduce_by_contracts(sym, qty, side or "long")}

    return {"ok": False, "msg": f"unknown type {t}"}

@app.post("/signal")
async def signal(req: Request):
    j, raw = await _read_json_safely(req)
    _record_signal(j, note="signal")
    if not _should_bypass_dedupe(j, req) and _dedupe_check(raw):
        return JSONResponse({"ok": True, "deduped": True})
    try:
        res = _handle_signal(j)
        return JSONResponse(res)
    except Exception as e:
        _record_error("signal", f"{type(e).__name__}: {e}")
        return JSONResponse({"ok": False, "msg": f"server_err: {e}"}, status_code=500)

@app.post("/signal/{tail:path}")
async def signal_tail(tail: str, req: Request):
    j, raw = await _read_json_safely(req)
    _record_signal(j, note=f"signal/{tail}")
    if not _should_bypass_dedupe(j, req) and _dedupe_check(raw):
        return JSONResponse({"ok": True, "deduped": True})
    try:
        res = _handle_signal(j)
        return JSONResponse(res)
    except Exception as e:
        _record_error("signal_tail", f"{type(e).__name__}: {e}")
        return JSONResponse({"ok": False, "msg": f"server_err: {e}"}, status_code=500)

# ───── 논블로킹 스타트업 (헬스 먼저 OK)
def _late_start() -> None:
    time.sleep(2)
    try:
        start_all_backgrounds()
    except Exception as e:
        print("late_start err:", e)

@app.on_event("startup")
async def _on_startup():
    print("[ENV] PRODUCT_TYPE   =", os.getenv("BITGET_PRODUCT_TYPE"))
    print("[ENV] POSITION_MODE  =", os.getenv("BITGET_POSITION_MODE"))
    print("[ENV] MARGIN_MODE    =", os.getenv("BITGET_MARGIN_MODE"))
    print("[ENV] AMOUNT_MODE    =", os.getenv("AMOUNT_MODE"))
    print("[ENV] WEB_CONCURRENCY=", os.getenv("WEB_CONCURRENCY"))
    threading.Thread(target=_late_start, daemon=True).start()
