# main.py — FastAPI entrypoint (workers + watchdog + reconciler + guards + KPI + AI)
import os, time, json, threading
from typing import Any, Dict, Optional
from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from trader import (
    enter_position, close_position, take_partial_profit,
    start_watchdogs, start_reconciler, start_capacity_guard,
    apply_runtime_overrides, get_pending_snapshot
)

try:
    from kpi_pipeline import start_kpi_pipeline, aggregate_and_save, list_trades
except Exception:
    def start_kpi_pipeline(): ...
    def aggregate_and_save(): return {}
    def list_trades(limit: int = 200): return []

try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str): print("[TG]", msg)

try:
    from bitget_api import symbol_exists, get_last_price, convert_symbol, get_open_positions
except Exception:
    def symbol_exists(symbol: str) -> bool: return True
    def get_last_price(symbol: str) -> float: return 0.0
    def convert_symbol(s: str) -> str: return (s or "").upper()
    def get_open_positions() -> list: return []

ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")
POLICY_ENABLE = os.getenv("POLICY_ENABLE", "1") == "1"
AI_ORCH_APPLY_MODE = os.getenv("AI_ORCH_APPLY_MODE", "live").lower().strip()
POLICY_CLOSE_ENABLE = os.getenv("POLICY_CLOSE_ENABLE", "0") == "1"  # 기본 OFF
REPORT_DIR = os.getenv("REPORT_DIR", "./reports")
KPIS_JSON = os.path.join(REPORT_DIR, "kpis.json")
APP_NAME = os.getenv("APP_NAME", "fastapi-trading-bot")
APP_VER = os.getenv("APP_VER", "2025-09-09")
app = FastAPI(title=APP_NAME, version=APP_VER)

class SignalReq(BaseModel):
    type: str
    symbol: str
    side: Optional[str] = None
    amount: Optional[float] = None
    timeframe: Optional[str] = None

class AdminRuntimeReq(BaseModel):
    STOP_ROE: Optional[float] = None
    STOP_PRICE_MOVE: Optional[float] = None
    RECON_INTERVAL_SEC: Optional[float] = None
    TP1_PCT: Optional[float] = None
    TP2_PCT: Optional[float] = None
    TP3_PCT: Optional[float] = None
    REOPEN_COOLDOWN_SEC: Optional[float] = None

class KPIReq(BaseModel):
    win_rate: Optional[float] = None
    avg_r: Optional[float] = None
    roi_per_hour: Optional[float] = None
    max_dd: Optional[float] = None
    n_trades: Optional[int] = None
    streak_win: Optional[int] = None
    streak_loss: Optional[int] = None
    avg_hold_sec: Optional[int] = None

def _load_kpis() -> Dict[str, Any]:
    try:
        if not os.path.exists(KPIS_JSON): return {}
        with open(KPIS_JSON, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_kpis(obj: Dict[str, Any]):
    os.makedirs(REPORT_DIR, exist_ok=True)
    tmp = KPIS_JSON + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, KPIS_JSON)

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
    tf = req.timeframe
    try:
        if t in ("entry", "open"):
            if side not in ("long", "short"): raise HTTPException(400, "side must be long/short")
            r = enter_position(sym, side=side, usdt_amount=amt, timeframe=tf)
            return {"ok": True, "res": r}
        if t in ("close", "exit"):
            if side not in ("long", "short"): raise HTTPException(400, "side must be long/short")
            r = close_position(sym, side=side, reason="signal_close")
            return {"ok": True, "res": r}
        if t in ("tp1", "tp_1", "takeprofit1"):
            r = take_partial_profit(sym, ratio=float(os.getenv("TP1_PCT", "0.30")), side=side, reason="tp1"); return {"ok": True, "res": r}
        if t in ("tp2", "tp_2", "takeprofit2"):
            r = take_partial_profit(sym, ratio=float(os.getenv("TP2_PCT", "0.70")), side=side, reason="tp2"); return {"ok": True, "res": r}
        if t in ("tp3", "tp_3", "takeprofit3"):
            r = take_partial_profit(sym, ratio=float(os.getenv("TP3_PCT", "0.30")), side=side, reason="tp3"); return {"ok": True, "res": r}
        if t in ("stop", "sl", "cut", "failcut", "be", "breakeven"):
            if side not in ("long", "short"): raise HTTPException(400, "side must be long/short")
            reason = "breakeven" if t in ("be","breakeven") else ("failcut" if t in ("failcut",) else "stop")
            r = close_position(sym, side=side, reason=reason); return {"ok": True, "res": r}
        raise HTTPException(400, f"unknown type: {t}")
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.post("/admin/runtime")
def admin_runtime(req: AdminRuntimeReq, request: Request, x_admin_token: str = Header(default="")):
    if not ADMIN_TOKEN or x_admin_token != ADMIN_TOKEN:
        raise HTTPException(401, "invalid admin token")
    changed: Dict[str, Any] = {k:v for k,v in req.dict().items() if v is not None}
    if not changed: return {"ok": True, "changed": {}}
    apply_runtime_overrides(changed)
    send_telegram(f"🧠 AI 튜너 조정\n{', '.join([f'{k}={v}' for k, v in changed.items()])}")
    return {"ok": True, "changed": changed}

@app.post("/reports/kpis")
def post_kpis(req: KPIReq):
    cur = _load_kpis()
    for k, v in req.dict().items():
        if v is not None: cur[k] = v
    cur["updated_ts"] = int(time.time())
    _save_kpis(cur)
    return {"ok": True, "kpis": cur}

@app.get("/reports/kpis")
def get_kpis(): return {"ok": True, "kpis": _load_kpis()}
@app.get("/reports/trades")
def get_trades(limit: int = 200): return {"ok": True, "trades": list_trades(limit=limit)}
@app.get("/debug/symbol/{symbol}")
def debug_symbol(symbol: str):
    core = convert_symbol(symbol)
    return {"ok": True, "symbol": core, "exists": symbol_exists(core), "last": get_last_price(core)}
@app.get("/debug/positions")
def debug_positions():
    try: return {"ok": True, "positions": get_open_positions(None)}
    except Exception as e: return {"ok": False, "error": str(e)}
@app.get("/snapshot")
def snapshot(): return {"ok": True, "snapshot": get_pending_snapshot()}

def _orch_logic_from_kpi(kpi: Dict[str, Any]) -> Dict[str, Any]:
    changed: Dict[str, Any] = {}
    win = float(kpi.get("win_rate", 0.0) or 0.0)
    avg_r = float(kpi.get("avg_r", 0.0) or 0.0)
    roi_h = float(kpi.get("roi_per_hour", 0.0) or 0.0)
    mdd = float(kpi.get("max_dd", 0.0) or 0.0)
    if roi_h < 0.0 or mdd < -0.15:
        changed["STOP_PRICE_MOVE"] = 0.025; changed["STOP_ROE"] = 0.08; changed["REOPEN_COOLDOWN_SEC"] = 120
    elif win > 0.50 and avg_r > 0.25:
        changed["STOP_PRICE_MOVE"] = 0.018; changed["STOP_ROE"] = 0.10; changed["REOPEN_COOLDOWN_SEC"] = 90
    return changed

def _orchestrator_loop():
    if not POLICY_ENABLE: 
        print("[orch] disabled (POLICY_ENABLE=0)"); 
        return
    send_telegram("🧠 Policy manager started")
    send_telegram("🤖 AI expert started")
    send_telegram("🧠 Orchestrator started")
    first_announce = True
    while True:
        try:
            kpi = _load_kpis()
            win = float(kpi.get("win_rate", 0.0) or 0.0)
            avg_r = float(kpi.get("avg_r", 0.0) or 0.0)
            n = int(kpi.get("n_trades", 0) or 0)
            if first_announce:
                send_telegram(f"🤖 AI 튜너 조정\n- WinRate={win*100:.1f}% AvgR={avg_r:.2f} N={n}\n• 신호: worst=0.0% (버킷Top=0.0%, 24hTop=0.0%), state.stable_seq=0")
                first_announce = False
            changed = _orch_logic_from_kpi(kpi)
            if changed and AI_ORCH_APPLY_MODE == "live":
                apply_runtime_overrides(changed)
                send_telegram("🤖 AI 튜너 조정\n" + ", ".join([f"{k}={v}" for k, v in changed.items()]))
            # policy 강제종료 기본 OFF
        except Exception as e:
            print("orchestrator err:", e)
        time.sleep(30)

def _boot():
    try: start_kpi_pipeline()
    except Exception as e: print("kpi pipeline start err:", e)
    start_watchdogs(); start_reconciler(); start_capacity_guard()
    threading.Thread(target=_orchestrator_loop, name="ai-orchestrator", daemon=True).start()
    send_telegram("✅ FastAPI up (workers + watchdog + reconciler + guards + AI)")

@app.on_event("startup")
def on_startup(): _boot()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8080")), reload=False)
