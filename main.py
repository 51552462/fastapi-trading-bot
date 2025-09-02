# main.py — FastAPI entrypoint (ADD-ONLY philosophy)
import os
import json
import time
import traceback
from typing import Optional, Dict, Any

from fastapi import FastAPI, Request, Query, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

# ──────────────────────────────────────────────────────────────
# Optional deps: 텔레그램/로거는 없으면 안전 폴백
# ──────────────────────────────────────────────────────────────
try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str):
        print("[TG]", msg)

try:
    from telemetry.logger import log_event
except Exception:
    def log_event(payload: dict, stage: str = "event"):
        print("[LOG]", stage, payload)

# ──────────────────────────────────────────────────────────────
# Policy / Trader / Guards
# ──────────────────────────────────────────────────────────────
# tf_policy는 정책/시간봉 힌트 & 시그널 인입
try:
    # 권장 경로: policy/tf_policy.py
    from policy.tf_policy import ingest_signal, start_policy_manager  # type: ignore
except Exception:
    # 루트에 tf_policy.py 가 있을 수도 있음
    try:
        from tf_policy import ingest_signal, start_policy_manager  # type: ignore
    except Exception:
        ingest_signal = None
        start_policy_manager = None
        print("⚠️ tf_policy 모듈을 찾지 못했습니다. (ingest_signal, start_policy_manager 비활성)")

# trader: 실제 주문 처리
from trader import (
    enter_position, take_partial_profit, close_position,
    start_watchdogs, start_reconciler, get_pending_snapshot, start_capacity_guard
)

# guards는 내부에서 bitget_api를 사용
try:
    from risk_guard import check_entry_allowed  # bool 반환 가정(허용/차단)
except Exception:
    def check_entry_allowed(symbol: str, side: str, usdt_amount: float, timeframe: Optional[str] = None) -> bool:
        # 위험 가드 모듈이 없을 때는 항상 허용 (안전 폴백)
        return True

# ──────────────────────────────────────────────────────────────
# ENV
# ──────────────────────────────────────────────────────────────
DEFAULT_AMOUNT = float(os.getenv("DEFAULT_AMOUNT", "80"))
FORCE_DEFAULT_AMOUNT = os.getenv("FORCE_DEFAULT_AMOUNT", "1") == "1"

# 무료 플랜에서 로그/리포트 확인 대응
LOG_DIR = os.getenv("TRADE_LOG_DIR", "./logs")
REPORT_DIR = "./reports"
LOG_API_TOKEN = os.getenv("LOGS_API_TOKEN", "")  # 없으면 무제한 접근

# ──────────────────────────────────────────────────────────────
# FastAPI
# ──────────────────────────────────────────────────────────────
app = FastAPI(title="FastAPI Trading Bot", version="1.0.0")

# CORS (필요시 열어둠)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ──────────────────────────────────────────────────────────────
# 모델
# ──────────────────────────────────────────────────────────────
class SignalPayload(BaseModel):
    symbol: str
    side: str               # "long"|"short" 또는 "buy"|"sell"
    amount: Optional[float] = None
    timeframe: Optional[str] = None   # "1H"/"2H"/"3H"/"4H"/"D" 등 (트뷰 경로에서도 주입)

class TPRequest(BaseModel):
    symbol: str
    side: str = "long"
    pct: float = 0.25

class CloseRequest(BaseModel):
    symbol: str
    side: str = "long"
    reason: Optional[str] = "manual"

# ──────────────────────────────────────────────────────────────
# 유틸
# ──────────────────────────────────────────────────────────────
def _canon_side(s: str) -> str:
    s = (s or "").lower()
    if s in ("buy", "long", "1", "l"):  return "long"
    if s in ("sell", "short", "-1", "s"): return "short"
    return s or "long"

def _canon_tf(s: Optional[str]) -> Optional[str]:
    if not s: return None
    s = s.strip().lower()
    if s in ("1h","2h","3h","4h","d"):
        return s
    if s in ("1","1h ","01h"): return "1h"
    if s in ("2","2h ","02h"): return "2h"
    if s in ("3","3h ","03h"): return "3h"
    if s in ("4","4h ","04h"): return "4h"
    if s in ("day","1d","d1","d"): return "d"
    return s

def _auth_or_raise(token: Optional[str]):
    if LOG_API_TOKEN and token != LOG_API_TOKEN:
        raise HTTPException(status_code=401, detail="invalid token")

# ──────────────────────────────────────────────────────────────
# 기본 라우트
# ──────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"ok": True, "ts": int(time.time())}

# TradingView 웹훅: /signal/{tf} (예: /signal/3h)
@app.post("/signal/{tf}")
async def signal_with_tf(tf: str, req: Request):
    """
    TradingView 웹훅 규격:
    - JSON body 예시: {"symbol":"BTCUSDT","side":"long","amount":80}
    - 경로의 {tf} 로 timeframe 힌트 주입 (1h/2h/3h/4h/d)
    """
    try:
        data: Dict[str, Any] = {}
        try:
            data = await req.json()
        except Exception:
            # TV가 form으로 보낼 수도 있음
            form = await req.form()
            if "payload" in form:
                data = json.loads(form["payload"])
            else:
                data = dict(form)

        # 정규화
        symbol = (data.get("symbol") or data.get("ticker") or "").upper()
        side   = _canon_side(str(data.get("side") or "long"))
        amount = float(data.get("amount") or 0.0)
        tf_str = _canon_tf(tf)

        if not symbol:
            raise HTTPException(status_code=400, detail="symbol required")

        # 기본 금액 강제 옵션
        if FORCE_DEFAULT_AMOUNT or amount <= 0:
            amount = DEFAULT_AMOUNT

        payload = {
            "event": "entry",
            "symbol": symbol,
            "side": side,
            "amount": amount,
            "timeframe": tf_str,
            "source": "tradingview"
        }

        # 로깅 (인입)
        log_event(payload, stage="ingress")

        # 시간봉 힌트 등록(있으면)
        if start_policy_manager is not None and ingest_signal is not None:
            # tf_policy가 시간봉 힌트를 내부 state에 저장/활용
            try:
                ingest_signal(payload)
            except Exception as e:
                print("⚠️ ingest_signal error:", e)

        # 리스크/마진 가드 체크
        allowed = True
        try:
            allowed = check_entry_allowed(symbol, side, amount, timeframe=tf_str)
        except Exception as e:
            print("⚠️ risk_guard check error:", e)

        if not allowed:
            send_telegram(f"⛔ RiskGuard blocked {symbol} {side} {amount} (tf={tf_str})")
            return {"ok": False, "blocked": True}

        # 진입
        enter_position(symbol, amount, side=side)

        return {"ok": True, "symbol": symbol, "side": side, "amount": amount, "tf": tf_str}
    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        send_telegram(f"❌ /signal error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# 수동 TP/청산 (필요 시)
@app.post("/tp")
def api_tp(req: TPRequest):
    take_partial_profit(req.symbol, req.pct, side=_canon_side(req.side))
    return {"ok": True}

@app.post("/close")
def api_close(req: CloseRequest):
    close_position(req.symbol, side=_canon_side(req.side), reason=req.reason or "manual")
    return {"ok": True}

# 대기 상태/용량 스냅샷
@app.get("/pending")
def api_pending():
    try:
        return get_pending_snapshot()
    except Exception as e:
        return {"error": str(e)}

# ──────────────────────────────────────────────────────────────
# [ADD] 무료 플랜 전용: 파일 로그/리포트 조회 API
# ──────────────────────────────────────────────────────────────
import glob
import subprocess

@app.get("/logs/list")
def list_logs(token: Optional[str] = Query(None)):
    _auth_or_raise(token)
    try:
        files = sorted(glob.glob(os.path.join(LOG_DIR, "*")))
        return {"dir": LOG_DIR, "files": [os.path.basename(f) for f in files]}
    except Exception as e:
        return {"error": str(e), "dir": LOG_DIR}

@app.get("/logs/tail")
def tail_log(file: str, lines: int = 50, token: Optional[str] = Query(None)):
    _auth_or_raise(token)
    path = os.path.join(LOG_DIR, file)
    try:
        with open(path, "r", encoding="utf-8") as f:
            buf = f.readlines()[-max(1, min(lines, 1000)):]
        return {"file": file, "lines": buf}
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"not found: {file}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/reports/run")
def run_summary(days: Optional[int] = None, token: Optional[str] = Query(None)):
    _auth_or_raise(token)
    cmd = ["python3", "tools/summarize_logs.py"]
    if days is not None:
        cmd += ["--days", str(days)]
    try:
        os.makedirs(REPORT_DIR, exist_ok=True)
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        return {
            "ok": (res.returncode == 0),
            "stdout": (res.stdout or "")[-4000:],
            "stderr": (res.stderr or "")[-2000:],
            "reports": sorted([os.path.basename(p) for p in glob.glob(os.path.join(REPORT_DIR, "*"))])
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/reports/kpis")
def get_kpis(token: Optional[str] = Query(None)):
    _auth_or_raise(token)
    path = os.path.join(REPORT_DIR, "kpis.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="kpis.json not found (먼저 /reports/run 호출)")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# (원하면 CSV 다운로드도 추가 가능)
@app.get("/reports/download")
def download_report(name: str, token: Optional[str] = Query(None)):
    _auth_or_raise(token)
    path = os.path.join(REPORT_DIR, name)
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail=f"report not found: {name}")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return {"name": name, "content": f.read()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ──────────────────────────────────────────────────────────────
# 스타트업 훅: 백그라운드 루프 시작
# ──────────────────────────────────────────────────────────────
@app.on_event("startup")
def _boot():
    try:
        start_watchdogs()
    except Exception as e:
        print("⚠️ start_watchdogs err:", e)
    try:
        start_reconciler()
    except Exception as e:
        print("⚠️ start_reconciler err:", e)
    try:
        start_capacity_guard()
    except Exception as e:
        print("⚠️ start_capacity_guard err:", e)
    try:
        if start_policy_manager is not None:
            start_policy_manager()
            send_telegram("🟢 Policy manager started")
    except Exception as e:
        print("⚠️ start_policy_manager err:", e)

# ──────────────────────────────────────────────────────────────
# 로컬 실행용
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
