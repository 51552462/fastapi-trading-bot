# main.py — FastAPI entrypoint
# - 기존 기능 유지 + 추가: TV 전략 alert(type) 완전 매핑, 심볼 워밍업, KPI/AI/디버그

import os, sys, json, time, threading
from typing import Any, Dict, Optional
from fastapi import FastAPI, Request, HTTPException

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from trader import (
    enter_position, take_partial_profit, reduce_by_contracts, close_position,
    start_watchdogs, start_reconciler, start_capacity_guard,
    runtime_overrides as trader_runtime_overrides,
    get_pending_snapshot,
)

# Telegram (모듈 없으면 print로 대체)
try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str):
        print("[TG]", msg)

# Policy/AI (없으면 무시)
POLICY_ENABLE = os.getenv("POLICY_ENABLE", "0") == "1"
try:
    from tf_policy import ingest_signal, start_policy_manager
except Exception:
    def ingest_signal(*args, **kwargs): pass
    def start_policy_manager(): pass
try:
    from ai_expert import start_ai_expert
except Exception:
    def start_ai_expert(): pass
try:
    from ai_orchestrator import loop as _orch_loop
    def start_ai_orchestrator():
        threading.Thread(target=_orch_loop, name="ai-orchestrator", daemon=True).start()
except Exception:
    def start_ai_orchestrator(): pass

# ---------------- helpers ----------------
def _infer_side(s: Optional[str]) -> Optional[str]:
    if not s: return None
    s = s.lower()
    if s in ("buy","long","l"):  return "long"
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

    return {
        "symbol": (data.get("symbol") or data.get("ticker") or "").upper(),
        "side": _infer_side(data.get("side")),
        "type": (data.get("type") or data.get("action") or "").lower(),
        "amount": data.get("amount"),
        "leverage": data.get("leverage"),
        "ratio": data.get("ratio"),
        "contracts": data.get("contracts"),
        "meta": data,
    }

def _ingest_with_tf_override(d: Dict[str, Any], tf_hint: Optional[str] = None) -> Dict[str, Any]:
    try:
        if POLICY_ENABLE:
            ingest_signal(d.get("symbol"), d.get("side"), tf_hint, d.get("meta"))
    except Exception:
        pass
    return _route_signal(d, tf_hint=tf_hint)

# ---------- TradingView 전략 타입 매핑(롱/숏 2세트 포함) ----------
TV_CLOSE_TYPES = {
    # 즉시 전체 종료(손절/실패/EMA/청산)
    "sl": "stop", "sl1": "stop1", "sl2": "stop2",
    "failcut": "failcut", "fail": "failcut",
    "emaexit": "emaexit", "ema160_exit": "emaexit", "exit_ema160": "emaexit",
    "stoploss": "stoploss", "liquidation": "liquidation",
    "exit": "signal",
}
TV_TP_RATIOS = {  # 현재 보유수량 기준
    "tp1": 0.30,
    "tp2": 0.70,
    # tp3는 전체 종료로 처리(아래)
}

def _route_signal(d: Dict[str, Any], tf_hint: Optional[str] = None) -> Dict[str, Any]:
    symbol = d["symbol"]; side = d["side"]; type_ = d["type"]
    amount = d["amount"]; leverage = d["leverage"]
    ratio  = d["ratio"];  contracts = d["contracts"]

    if not symbol:
        raise HTTPException(400, "symbol required")

    # --- 공용 기본 타입 ---
    if type_ in ("open","entry","enter"):
        if side not in ("long","short"):
            raise HTTPException(400, "side required for open/entry")
        ok = enter_position(symbol, side, usdt_amount=amount, leverage=leverage)
        return {"ok": bool(ok), "r": ok}

    if type_ in ("close","exit","flatten"):
        if side not in ("long","short"):
            raise HTTPException(400, "side required for close")
        ok = close_position(symbol, side, reason="signal")
        return {"ok": bool(ok), "r": ok}

    if type_ in ("reduce","reduceonly","reduce_by_contracts","reduce_by_size"):
        if contracts is None:
            raise HTTPException(400, "contracts required for reduce")
        if side not in ("long","short"):
            raise HTTPException(400, "side required for reduce")
        ok = reduce_by_contracts(symbol, float(contracts), side)
        return {"ok": bool(ok), "r": ok}

    if type_ in ("tp","takeprofit","partial"):
        if ratio is None or side not in ("long","short"):
            raise HTTPException(400, "ratio/side required for tp")
        ok = take_partial_profit(symbol, float(ratio), side, reason="tp")
        return {"ok": bool(ok), "r": ok}

    # --- TV 전략 전용 타입(롱/숏 2세트) ---
    # 분할 익절
    if type_ in ("tp1","tp2"):
        if side not in ("long","short"):
            raise HTTPException(400, "side required for tpX")
        r = TV_TP_RATIOS[type_]
        ok = take_partial_profit(symbol, r, side, reason=type_)
        return {"ok": bool(ok), "r": ok}

    # tp3 → 전체 종료(텔레그램 'CLOSE' 포맷으로)
    if type_ in ("tp3","final","fullclose"):
        if side not in ("long","short"):
            raise HTTPException(400, "side required for tp3")
        ok = close_position(symbol, side, reason="tp3")
        return {"ok": bool(ok), "r": ok}

    # 손절/실패/EMA/강제청산 → 전체 종료
    if type_ in TV_CLOSE_TYPES:
        if side not in ("long","short"):
            raise HTTPException(400, "side required for stop/fail/ema/liq")
        ok = close_position(symbol, side, reason=TV_CLOSE_TYPES[type_])
        return {"ok": bool(ok), "r": ok}

    # 경고성 알림(숏2 'tailTouch' 등) → 서버 동작 없이 통과
    if type_ in ("tailtouch","notice","warn"):
        send_telegram(f"⚠️ {type_} {side or ''} {symbol}")
        return {"ok": True, "r": "noted"}

    raise HTTPException(400, f"unsupported type: {type_}")

# --------------- FastAPI ---------------
app = FastAPI(title="fastapi-trading-bot")

@app.get("/health")
def health(): return {"ok": True, "ts": int(time.time())}

@app.get("/pending")
def pending():
    try:
        snap = get_pending_snapshot()
    except Exception:
        snap = {}
    return {"ok": True, "snapshot": snap}

# KPI in/out
@app.post("/reports/kpis")
async def reports_kpis(req: Request):
    data = await req.json()
    report_dir = os.getenv("REPORT_DIR", "./reports")
    os.makedirs(report_dir, exist_ok=True)
    path = os.path.join(report_dir, "kpis.json")
    tmp  = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)
    return {"ok": True, "path": path}

@app.get("/reports/kpis")
def get_kpis():
    report_dir = os.getenv("REPORT_DIR", "./reports")
    path = os.path.join(report_dir, "kpis.json")
    if not os.path.exists(path):
        return {"ok": False, "error": "no kpis.json"}
    return {"ok": True, "kpis": json.load(open(path, "r", encoding="utf-8"))}

# 트뷰 신호(타임프레임 힌트 버전 유지)
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

# 관리자 런타임 패치
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

# 디버그
@app.get("/debug/symbol/{sym}")
def dbg_symbol(sym: str):
    try:
        from bitget_api import get_symbol_spec, convert_symbol, symbol_exists
        s = convert_symbol(sym)
        return {"sym": s, "exists": symbol_exists(s), "spec": get_symbol_spec(s)}
    except Exception as e:
        return {"ok": False, "err": str(e)}

# --------------- bootstrap ---------------
def _boot():
    # 심볼 캐시 워밍업(v2/v1 자동 동기화)
    try:
        from bitget_api import _refresh_symbols
        _refresh_symbols(force=True)
    except Exception:
        pass

    start_watchdogs()
    start_reconciler()
    start_capacity_guard()

    if POLICY_ENABLE:
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
    try:
        start_ai_orchestrator()
        send_telegram("🧠 Orchestrator started")
    except Exception:
        pass

    send_telegram("✅ FastAPI up (workers + watchdog + reconciler + guards + AI)")

_boot()
