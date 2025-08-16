import os, time, json, hashlib
from collections import deque
from typing import Dict, Any
from fastapi import FastAPI, Request, BackgroundTasks

from trader import (enter_position,take_partial_profit,close_position,reduce_by_contracts,)
from telegram_bot import send_telegram
from bitget_api import convert_symbol, get_open_positions

# ── Config ─────────────────────────────────────────────────────
DEFAULT_AMOUNT = float(os.getenv("DEFAULT_AMOUNT", "15"))
LEVERAGE       = float(os.getenv("LEVERAGE", "5"))
DEDUP_TTL      = float(os.getenv("DEDUP_TTL", "15"))  # seconds
TP1_PCT        = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT        = float(os.getenv("TP2_PCT", "0.40"))
TP3_PCT        = float(os.getenv("TP3_PCT", "0.30"))

# ── App/Infra ──────────────────────────────────────────────────
app = FastAPI()

INGRESS_LOG: deque = deque(maxlen=200)   # 최근 수신 로그
_DEDUP: Dict[str, float] = {}            # payload 해시 → ts


def _dedup_key(d: Dict[str, Any]) -> str:
    # JSON 정규화 후 해시
    return hashlib.sha1(json.dumps(d, sort_keys=True).encode()).hexdigest()


def _infer_side(side: str, default: str = "long") -> str:
    s = (side or "").strip().lower()
    if s in ("long", "short"):
        return s
    return default


def _norm_symbol(sym: str) -> str:
    return convert_symbol(sym)

# ── Core handler (백그라운드 실행) ─────────────────────────────
def _handle_signal(data: Dict[str, Any]):
    typ    = (data.get("type") or "").strip()
    symbol = _norm_symbol(data.get("symbol", ""))
    side   = _infer_side(data.get("side"), "long")

    amount   = float(data.get("amount", DEFAULT_AMOUNT))
    leverage = float(data.get("leverage", LEVERAGE))

    if not symbol:
        send_telegram("⚠️ symbol 없음: " + json.dumps(data))
        return

    # 레거시 키 보정
    legacy = {"tp_1": "tp1", "tp_2": "tp2", "tp_3": "tp3","sl_1": "sl1", "sl_2": "sl2","ema_exit": "emaExit", "failcut": "failCut",}
    typ = legacy.get(typ.lower(), typ)

    if typ == "entry":
        enter_position(symbol, amount, side=side, leverage=leverage); return

    if typ in ("tp1", "tp2", "tp3"):
        pct = TP1_PCT if typ == "tp1" else TP2_PCT if typ == "tp2" else TP3_PCT
        take_partial_profit(symbol, pct, side=side); return

    if typ in ("sl1", "sl2", "failCut", "emaExit", "liquidation", "fullExit", "close", "exit"):
        close_position(symbol, side=side, reason=typ); return

    if typ == "reduceByContracts":
        contracts = float(data.get("contracts", 0))
        if contracts > 0:
            reduce_by_contracts(symbol, contracts, side=side)
        return

    # 주문이 아닌 정보성 알림은 무시
    if typ in ("tailTouch", "info", "debug"):
        return

    send_telegram("❓ 알 수 없는 신호: " + json.dumps(data))

# ── Endpoints ──────────────────────────────────────────────────
@app.post("/signal")
async def signal(req: Request, background_tasks: BackgroundTasks):
    data = await req.json()
    now  = time.time()

    # 1) Dedup within TTL
    dk = _dedup_key(data)
    if dk in _DEDUP and now - _DEDUP[dk] < DEDUP_TTL:
        return {"ok": True, "dedup": True}
    _DEDUP[dk] = now

    # 2) 수신 로그
    INGRESS_LOG.append({"ts": now,"ip": (req.client.host if req and req.client else "?"),"data": data})

    # 3) 백그라운드 처리
    background_tasks.add_task(_handle_signal, data)

    # 4) 즉시 ACK
    return {"ok": True, "queued": True}

@app.get("/health")
def health():
    return {"ok": True, "ingress": len(INGRESS_LOG)}

@app.get("/ingress")
def ingress():
    # 최근 30건만 노출
    return list(INGRESS_LOG)[-30:]

@app.get("/positions")
def positions():
    return {"positions": get_open_positions()}

@app.on_event("startup")
def on_startup():
    try:
        send_telegram("✅ FastAPI up (background handler ready)")
    except Exception:
        pass

