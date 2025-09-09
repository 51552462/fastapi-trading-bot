# main.py — FastAPI entrypoint (workers + watchdog + reconciler + guards + KPI + AI 튜너)
# - 축약/생략 없음 (완전체)
import os
import time
import json
import threading
from typing import Any, Dict, Optional

from fastapi import FastAPI, Request, Header, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# --- local modules ---
from trader import (
    enter_position, close_position, take_partial_profit,
    start_watchdogs, start_reconciler, start_capacity_guard,
    apply_runtime_overrides, get_pending_snapshot
)

# KPI 파이프라인(없어도 구동 가능하도록 안전장치)
try:
    from kpi_pipeline import start_kpi_pipeline, aggregate_and_save, list_trades
except Exception:
    def start_kpi_pipeline(): ...
    def aggregate_and_save(): return {}
    def list_trades(limit: int = 200): return []

# 텔레그램 (없으면 콘솔로 대체)
try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str):
        print("[TG]", msg)

# Bitget 심볼 존재여부/가격 등 디버그용
try:
    from bitget_api import symbol_exists, get_last_price, convert_symbol, get_open_positions
except Exception:
    def symbol_exists(symbol: str) -> bool: return True
    def get_last_price(symbol: str) -> float: return 0.0
    def convert_symbol(s: str) -> str: return (s or "").upper()
    def get_open_positions() -> list: return []


# ---------------- ENV ----------------
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "")

# AI/오케스트레이터 관련
POLICY_ENABLE = os.getenv("POLICY_ENABLE", "1") == "1"
AI_ORCH_APPLY_MODE = os.getenv("AI_ORCH_APPLY_MODE", "live").lower().strip()  # live|dry
# 🔒 요청사항 반영: 기본값은 '강제 종료 비활성화'
POLICY_CLOSE_ENABLE = os.getenv("POLICY_CLOSE_ENABLE", "0") == "1"

# KPI 파일 경로
REPORT_DIR = os.getenv("REPORT_DIR", "./reports")
KPIS_JSON = os.path.join(REPORT_DIR, "kpis.json")

APP_NAME = os.getenv("APP_NAME", "fastapi-trading-bot")
APP_VER = os.getenv("APP_VER", "2025-09-09")

# ------------- FastAPI -------------
app = FastAPI(title=APP_NAME, version=APP_VER)


# =============== MODELS ===============
class SignalReq(BaseModel):
    type: str                    # entry|close|tp1|tp2|tp3|stop|failcut|be (등)
    symbol: str
    side: Optional[str] = None   # long|short
    amount: Optional[float] = None
    timeframe: Optional[str] = None


class AdminRuntimeReq(BaseModel):
    # 허용되는 키만 필터링해서 사용
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


# =============== UTILS ===============
def _load_kpis() -> Dict[str, Any]:
    try:
        if not os.path.exists(KPIS_JSON):
            return {}
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


# =============== ROUTES ===============
@app.get("/")
def root():
    return {"ok": True, "name": APP_NAME, "version": APP_VER}


@app.get("/health")
def health():
    return {"ok": True, "ts": int(time.time())}


@app.get("/version")
def version():
    return {"ok": True, "version": APP_VER}


# ---------- signals (TradingView) ----------
@app.post("/signal")
def signal(req: SignalReq):
    t = (req.type or "").lower().strip()
    sym = convert_symbol(req.symbol)
    side = (req.side or "").lower().strip()
    amt = req.amount
    tf = req.timeframe

    try:
        # ENTRY
        if t in ("entry", "open"):
            if side not in ("long", "short"):
                raise HTTPException(400, "side must be long/short")
            r = enter_position(sym, side=side, usdt_amount=amt, timeframe=tf)
            return {"ok": True, "res": r}

        # CLOSE (전략 시그널로 전체 종료)
        if t in ("close", "exit"):
            if side not in ("long", "short"):
                raise HTTPException(400, "side must be long/short")
            r = close_position(sym, side=side, reason="signal_close")
            return {"ok": True, "res": r}

        # 부분익절 — 반드시 "전략 시그널"로만 수행 (AI는 실행 안 함)
        if t in ("tp1", "tp_1", "takeprofit1"):
            r = take_partial_profit(sym, ratio=float(os.getenv("TP1_PCT", "0.30")), side=side, reason="tp1")
            return {"ok": True, "res": r}
        if t in ("tp2", "tp_2", "takeprofit2"):
            r = take_partial_profit(sym, ratio=float(os.getenv("TP2_PCT", "0.70")), side=side, reason="tp2")
            return {"ok": True, "res": r}
        if t in ("tp3", "tp_3", "takeprofit3"):
            r = take_partial_profit(sym, ratio=float(os.getenv("TP3_PCT", "0.30")), side=side, reason="tp3")
            return {"ok": True, "res": r}

        # STOP/FAILCUT/BE — 전략이 명시적으로 지시했을 때만
        if t in ("stop", "sl", "cut", "failcut", "be", "breakeven"):
            if side not in ("long", "short"):
                raise HTTPException(400, "side must be long/short")
            reason = "breakeven" if t in ("be", "breakeven") else ("failcut" if t in ("failcut",) else "stop")
            r = close_position(sym, side=side, reason=reason)
            return {"ok": True, "res": r}

        raise HTTPException(400, f"unknown type: {t}")
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ---------- admin (runtime patch) ----------
@app.post("/admin/runtime")
def admin_runtime(req: AdminRuntimeReq, request: Request, x_admin_token: str = Header(default="")):
    if not ADMIN_TOKEN or x_admin_token != ADMIN_TOKEN:
        raise HTTPException(401, "invalid admin token")

    changed: Dict[str, Any] = {}
    for k, v in req.dict().items():
        if v is not None:
            changed[k] = v

    if not changed:
        return {"ok": True, "changed": {}}

    apply_runtime_overrides(changed)
    send_telegram(f"🧠 AI 튜너 조정\n{', '.join([f'{k}={v}' for k, v in changed.items()])}")
    return {"ok": True, "changed": changed}


# ---------- reports (KPI) ----------
@app.post("/reports/kpis")
def post_kpis(req: KPIReq):
    # 외부(리포터/애널라이저)에서 보내주는 집계 KPI를 그대로 저장
    cur = _load_kpis()
    for k, v in req.dict().items():
        if v is not None:
            cur[k] = v
    cur["updated_ts"] = int(time.time())
    _save_kpis(cur)
    return {"ok": True, "kpis": cur}


@app.get("/reports/kpis")
def get_kpis():
    # 내부 KPI 파이프라인이 돌아가면 aggregate_and_save()가 최신화
    k = _load_kpis()
    return {"ok": True, "kpis": k}


@app.get("/reports/trades")
def get_trades(limit: int = 200):
    return {"ok": True, "trades": list_trades(limit=limit)}


# ---------- debug ----------
@app.get("/debug/symbol/{symbol}")
def debug_symbol(symbol: str):
    core = convert_symbol(symbol)
    return {
        "ok": True,
        "symbol": core,
        "exists": symbol_exists(core),
        "last": get_last_price(core)
    }


@app.get("/debug/positions")
def debug_positions():
    try:
        return {"ok": True, "positions": get_open_positions()}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/snapshot")
def snapshot():
    return {"ok": True, "snapshot": get_pending_snapshot()}


# =============== BOOTSTRAP ===============
def _orch_logic_from_kpi(kpi: Dict[str, Any]) -> Dict[str, Any]:
    """
    KPI → 런타임 파라미터 맵핑 (예시)
    - 주의: 여기서는 '분할 TP'는 조정하지 않고, '강제 종료'도 트리거하지 않음.
    - 목적: 추세를 오래 태우되, 손실 국면에서는 리스크 축소.
    """
    changed: Dict[str, Any] = {}
    win = float(kpi.get("win_rate", 0.0) or 0.0)
    avg_r = float(kpi.get("avg_r", 0.0) or 0.0)
    roi_h = float(kpi.get("roi_per_hour", 0.0) or 0.0)
    mdd = float(kpi.get("max_dd", 0.0) or 0.0)
    n = int(kpi.get("n_trades", 0) or 0)

    # 보수화 조건: ROI/h < 0 이거나, MDD < -0.15
    if roi_h < 0.0 or mdd < -0.15:
        changed["STOP_PRICE_MOVE"] = 0.020 + 0.005  # +0.5%p
        changed["STOP_ROE"] = 0.08                  # -8%로 타이트
        changed["REOPEN_COOLDOWN_SEC"] = 120
    # 완화 조건: 성과 양호
    elif win > 0.50 and avg_r > 0.25:
        changed["STOP_PRICE_MOVE"] = 0.018          # -0.2%p
        changed["STOP_ROE"] = 0.10
        changed["REOPEN_COOLDOWN_SEC"] = 90
    else:
        # 중립 유지
        pass

    return changed


def _orchestrator_loop():
    """
    LIVE 자동 보정 (분할 TP/강제종료는 절대 여기서 실행하지 않음)
    - 요청사항 반영: '수익 잘 나오는 추세 포지션'은 AI가 끊지 않음
      → 강제 종료는 기본 비활성(POLICY_CLOSE_ENABLE=0)
    """
    if not POLICY_ENABLE:
        print("[orch] disabled (POLICY_ENABLE=0)")
        return

    send_telegram("🧠 Policy manager started")
    while True:
        try:
            kpi = _load_kpis()
            if not kpi:
                time.sleep(10)
                continue

            changed = _orch_logic_from_kpi(kpi)
            if changed and AI_ORCH_APPLY_MODE == "live":
                apply_runtime_overrides(changed)
                send_telegram("🤖 AI 튜너 조정\n" + ", ".join([f"{k}={v}" for k, v in changed.items()]))

            # 요청 시에만 강제 종료 로직을 여기에 넣을 수 있지만, 기본값은 미사용
            if POLICY_CLOSE_ENABLE:
                # 예시: 매우 나쁜 단기 ROI면 정책 종료 검토 (trader에 보호가드 있음)
                # disabled by default — keep empty
                pass

        except Exception as e:
            print("orchestrator err:", e)
        time.sleep(30)


def _boot():
    try:
        # KPI 파이프라인(체결로그 → 주기 집계)
        start_kpi_pipeline()
    except Exception as e:
        print("kpi pipeline start err:", e)

    # 워치독(손절/BE 감시), 리컨실, 용량가드
    start_watchdogs()
    start_reconciler()
    start_capacity_guard()

    # 오케스트레이터(라이브 자동 보정)
    threading.Thread(target=_orchestrator_loop, name="ai-orchestrator", daemon=True).start()

    # 부팅 메시지
    send_telegram("✅ FastAPI up (workers + watchdog + reconciler + guards + AI)")


@app.on_event("startup")
def on_startup():
    _boot()


# ---- local dev run ----
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8080")), reload=False)
