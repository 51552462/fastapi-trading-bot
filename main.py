
# main.py — FastAPI entrypoint
# - TradingView webhook ingestion (generic + 1H/2H/3H/4H/D)
# - Admin runtime overrides
# - Boots watchdogs/reconciler/capacity guard + policy manager + optional AI expert
# - Telegram status pings

import os, sys, json, time
from typing import Any, Dict, Optional
from fastapi import FastAPI, Request, HTTPException

# ===== import path guard =====
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

# ===== internal modules =====
from trader import (
    enter_position, take_partial_profit, reduce_by_contracts, close_position,
    start_watchdogs, start_reconciler, start_capacity_guard,
    runtime_overrides as trader_runtime_overrides,
    get_pending_snapshot
)

# Telegram (optional)
try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str):
        print("[TG]", msg)

# Policy / AI (optional)
try:
    from tf_policy import ingest_signal, start_policy_manager
except Exception:
    def ingest_signal(*args, **kwargs): pass
    def start_policy_manager(): pass

# Optional AI expert (user custom)
try:
    from ai_expert import start_ai_expert
except Exception:
    def start_ai_expert(): pass

# =========================
# helpers
# =========================
def _infer_side(s: Optional[str]) -> Optional[str]:
    if not s: return None
    s = s.lower()
    if s in ("buy","long","l"): return "long"
    if s in ("sell","short","s"): return "short"
    return s

async def _parse_any(req: Request) -> Dict[str, Any]:
    try:
        data = await req.json()
    except Exception:
        body = await req.body()
        try:
            data = json.loads(body.decode("utf-8"))
        except Exception:
            raise HTTPException(400, "invalid payload")

    # Normalize common fields
    symbol = (data.get("symbol") or data.get("ticker") or "").upper()
    side   = _infer_side(data.get("side"))
    type_  = (data.get("type") or data.get("action") or "").lower()
    amount = data.get("amount")  # usdt notional
    leverage = data.get("leverage")
    ratio  = data.get("ratio")
    contracts = data.get("contracts")
    meta   = data

    return {
        "symbol": symbol, "side": side, "type": type_,
        "amount": amount, "leverage": leverage,
        "ratio": ratio, "contracts": contracts,
        "meta": meta,
    }

def _ingest_with_tf_override(d: Dict[str, Any], tf_hint: Optional[str] = None) -> Dict[str, Any]:
    try:
        ingest_signal(d.get("symbol"), d.get("side"), tf_hint, d.get("meta"))
    except Exception:
        pass
    return _route_signal(d, tf_hint=tf_hint)

def _route_signal(d: Dict[str, Any], tf_hint: Optional[str] = None) -> Dict[str, Any]:
    symbol = d["symbol"]; side = d["side"]; type_ = d["type"]
    amount = d["amount"]; leverage = d["leverage"]
    ratio  = d["ratio"];  contracts = d["contracts"]

    if not symbol:
        raise HTTPException(400, "symbol required")

    # OPEN
    if type_ in ("open","entry","enter"):
        if side not in ("long","short"):
            raise HTTPException(400, "side required for open")
        ok = enter_position(symbol, side, usdt_amount=amount, leverage=leverage)
        return {"ok": bool(ok), "r": ok}

    # CLOSE
    if type_ in ("close","exit","flatten"):
        if side not in ("long","short"):
            raise HTTPException(400, "side required for close")
        ok = close_position(symbol, side, reason="signal")
        return {"ok": bool(ok), "r": ok}

    # TP (ratio 0~1)
    if type_ in ("tp","takeprofit","partial"):
        if ratio is None:
            raise HTTPException(400, "ratio required for tp")
        if side not in ("long","short"):
            raise HTTPException(400, "side required for tp")
        ok = take_partial_profit(symbol, float(ratio), side, reason="tp")
        return {"ok": bool(ok), "r": ok}

    # REDUCE by contracts (size)
    if type_ in ("reduce","reduceonly","reduce_by_contracts","reduce_by_size"):
        if contracts is None:
            raise HTTPException(400, "contracts required for reduce")
        if side not in ("long","short"):
            raise HTTPException(400, "side required for reduce")
        ok = reduce_by_contracts(symbol, float(contracts), side)
        return {"ok": bool(ok), "r": ok}

    raise HTTPException(400, f"unsupported type: {type_}")

# =========================
# FastAPI app & routes
# =========================
app = FastAPI(title="fastapi-trading-bot")

@app.get("/health")
def health():
    return {"ok": True, "ts": int(time.time())}

@app.get("/pending")
def pending():
    try:
        snap = get_pending_snapshot()
    except Exception:
        snap = {}
    return {"ok": True, "snapshot": snap}

@app.post("/signal")
async def signal_generic(req: Request):
    d = await _parse_any(req)
    return _ingest_with_tf_override(d, None)

@app.post("/signal/1h")
async def signal_1h(req: Request):
    d = await _parse_any(req)
    return _ingest_with_tf_override(d, "1H")

@app.post("/signal/2h")
async def signal_2h(req: Request):
    d = await _parse_any(req)
    return _ingest_with_tf_override(d, "2H")

@app.post("/signal/3h")
async def signal_3h(req: Request):
    d = await _parse_any(req)
    return _ingest_with_tf_override(d, "3H")

@app.post("/signal/4h")
async def signal_4h(req: Request):
    d = await _parse_any(req)
    return _ingest_with_tf_override(d, "4H")

@app.post("/signal/d")
async def signal_d(req: Request):
    d = await _parse_any(req)
    return _ingest_with_tf_override(d, "D")

@app.post("/admin/runtime")
async def admin_runtime(req: Request):
    tok = req.headers.get("x-admin-token") or req.headers.get("X-Admin-Token")
    expect = os.getenv("ADMIN_TOKEN")
    if not expect or tok != expect:
        raise HTTPException(401, "bad token")
    data = await req.json()
    try:
        trader_runtime_overrides(data or {})
    except Exception as e:
        raise HTTPException(400, f"apply failed: {e}")
    return {"ok": True}

# =========================
# bootstrap
# =========================
def _boot():
    start_watchdogs()
    start_reconciler()
    start_capacity_guard()
    # AI policy manager & expert (if available)
    try:
        start_policy_manager()
        send_telegram("🧠 Policy manager started")
    except Exception:
        pass
    try:
        start_ai_expert()
        send_telegram("🤖 AI expert started")
    except Exception:
        pass
    send_telegram("✅ FastAPI up (workers + watchdog + reconciler + guards + AI)")

_boot()
