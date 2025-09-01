import os, time, json, hashlib, threading, queue, re
from collections import deque
from typing import Dict, Any
from fastapi import FastAPI, Request

from trader import (
    enter_position, take_partial_profit, close_position, reduce_by_contracts,
    start_watchdogs, start_reconciler, get_pending_snapshot, start_capacity_guard
)
from telegram_bot import send_telegram
from bitget_api import convert_symbol, get_open_positions

# [ADD] 정책/텔레메트리 추가 (비침투적)
from policy.tf_policy import ingest_signal, start_policy_manager   # [ADD]
try:
    from telemetry.logger import log_event                        # [ADD]
except Exception:
    def log_event(*a, **kw): pass                                 # [ADD]

DEFAULT_AMOUNT = float(os.getenv("DEFAULT_AMOUNT", "15"))
LEVERAGE       = float(os.getenv("LEVERAGE", "5"))
DEDUP_TTL      = float(os.getenv("DEDUP_TTL", "15"))
BIZDEDUP_TTL   = float(os.getenv("BIZDEDUP_TTL", "3"))

WORKERS        = int(os.getenv("WORKERS", "6"))
QUEUE_MAX      = int(os.getenv("QUEUE_MAX", "2000"))
LOG_INGRESS    = os.getenv("LOG_INGRESS", "0") == "1"

FORCE_DEFAULT_AMOUNT = os.getenv("FORCE_DEFAULT_AMOUNT", "0") == "1"
SYMBOL_AMOUNT_JSON = os.getenv("SYMBOL_AMOUNT_JSON", "")
try:
    SYMBOL_AMOUNT = json.loads(SYMBOL_AMOUNT_JSON) if SYMBOL_AMOUNT_JSON else {}
except Exception:
    SYMBOL_AMOUNT = {}

app = FastAPI()

INGRESS_LOG: deque = deque(maxlen=200)
_DEDUP: Dict[str, float] = {}
_BIZDEDUP: Dict[str, float] = {}
_task_q: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=QUEUE_MAX)

# ──────────────────────────────────────────────────────────────
# 유틸
# ──────────────────────────────────────────────────────────────
def _dedup_key(d: Dict[str, Any]) -> str:
    return hashlib.sha1(json.dumps(d, sort_keys=True).encode()).hexdigest()

def _norm_symbol(sym: str) -> str:
    return convert_symbol(sym)

def _infer_side(side: str, default: str = "long") -> str:
    s = (side or "").strip().lower()
    return s if s in ("long", "short") else default

def _norm_type(typ: str) -> str:
    """
    type 문자열을 소문자로 만들고, 공백/언더스코어/대시 제거해 표준화.
    예) 'emaExit'/'ema_exit'/'EMA-EXIT' -> 'emaexit'
    """
    t = (typ or "").strip().lower()
    t = re.sub(r"[\s_\-]+", "", t)
    aliases = {
        "tp_1": "tp1", "tp_2": "tp2", "tp_3": "tp3",
        "takeprofit1": "tp1", "takeprofit2": "tp2", "takeprofit3": "tp3",
        "sl_1": "sl1", "sl_2": "sl2",
        "stopfull": "stoploss", "stopall": "stoploss", "stop": "stoploss",
        "fullexit": "stoploss", "exitall": "stoploss",
        "emaexit": "emaexit",
        "failcut": "failcut",
        "closeposition": "close", "closeall": "close",
        "reducecontracts": "reducebycontracts",
        "reduce_by_contracts": "reducebycontracts",
    }
    return aliases.get(t, t)

# ──────────────────────────────────────────────────────────────
# Payload 파서(느슨하게)
# ──────────────────────────────────────────────────────────────
async def _parse_any(req: Request) -> Dict[str, Any]:
    # JSON
    try:
        return await req.json()
    except Exception:
        pass
    # Raw body
    try:
        raw = (await req.body()).decode(errors="ignore").strip()
        if raw:
            try:
                return json.loads(raw)
            except Exception:
                fixed = raw.replace("'", '"')
                return json.loads(fixed)
    except Exception:
        pass
    # Form
    try:
        form = await req.form()
        payload = form.get("payload") or form.get("data")
        if payload:
            return json.loads(payload)
    except Exception:
        pass
    # Key:Value 텍스트
    try:
        txt = (await req.body()).decode(errors="ignore")
        d: Dict[str, Any] = {}
        for part in re.split(r"[\n,]+", txt):
            if ":" in part:
                k, v = part.split(":", 1)
                d[k.strip()] = v.strip()    # ← FIX: 잘못된 d[k].strip() 제거 반영
        if d:
            return d
    except Exception:
        pass
    raise ValueError("cannot parse request")

# ──────────────────────────────────────────────────────────────
# 시그널 라우팅
# ──────────────────────────────────────────────────────────────
def _handle_signal(data: Dict[str, Any]):
    typ_raw = (data.get("type") or "")
    symbol  = _norm_symbol(data.get("symbol", ""))
    side    = _infer_side(data.get("side"), "long")

    # [ADD] 텔레메트리: 원본 신호 로그(선택)
    try: log_event(data, stage="ingress")
    except: pass

    # [ADD] TF 힌트 수집 (실행 흐름 영향 없음)
    try: ingest_signal(data)
    except: pass

    amount   = float(data.get("amount", DEFAULT_AMOUNT))
    leverage = float(data.get("leverage", LEVERAGE))

    # 심볼별 금액 우선
    resolved_amount = float(amount)
    if (symbol in SYMBOL_AMOUNT) and (str(SYMBOL_AMOUNT[symbol]).strip() != ""):
        try:
            resolved_amount = float(SYMBOL_AMOUNT[symbol])
        except Exception:
            resolved_amount = float(DEFAULT_AMOUNT)
    elif FORCE_DEFAULT_AMOUNT:
        resolved_amount = float(DEFAULT_AMOUNT)

    if not symbol:
        send_telegram("⚠️ symbol 없음: " + json.dumps(data))
        return

    t = _norm_type(typ_raw)

    # 너무 잦은 동일 비즈니스 이벤트 차단
    now = time.time()
    bizkey = f"{t}:{symbol}:{side}"
    last = _BIZDEDUP.get(bizkey, 0.0)
    if now - last < BIZDEDUP_TTL:
        return
    _BIZDEDUP[bizkey] = now

    if LOG_INGRESS:
        try:
            send_telegram(f"📥 {t} {symbol} {side} amt={resolved_amount}")
        except Exception:
            pass

    # 라우팅
    if t == "entry":
        enter_position(symbol, resolved_amount, side=side, leverage=leverage)
        return

    if t in ("tp1", "tp2", "tp3"):
        pct = float(os.getenv("TP1_PCT", "0.30")) if t == "tp1" else \
              float(os.getenv("TP2_PCT", "0.40")) if t == "tp2" else \
              float(os.getenv("TP3_PCT", "0.30"))
        take_partial_profit(symbol, pct, side=side)
        return

    # 즉시 전체 종료 키들
    CLOSE_KEYS = {
        "stoploss", "emaexit", "failcut", "fullexit", "close", "exit", "liquidation",
        "sl1", "sl2"
    }
    if t in CLOSE_KEYS:
        close_position(symbol, side=side, reason=t)
        return

    if t == "reducebycontracts":
        contracts = float(data.get("contracts", 0))
        if contracts > 0:
            reduce_by_contracts(symbol, contracts, side=side)
        return

    if t in ("tailtouch", "info", "debug"):
        return

    send_telegram("❓ 알 수 없는 신호: " + json.dumps(data))

# ──────────────────────────────────────────────────────────────
# 워커/엔드포인트/스타트업
# ──────────────────────────────────────────────────────────────
def _worker_loop(idx: int):
    while True:
        try:
            data = _task_q.get()
            if data is None:
                continue
            _handle_signal(data)
        except Exception as e:
            print(f"[worker-{idx}] error:", e)
        finally:
            _task_q.task_done()

async def _ingest(req: Request):
    now = time.time()
    try:
        data = await _parse_any(req)
    except Exception as e:
        return {"ok": False, "error": f"bad_payload: {e}"}

    dk = _dedup_key(data)
    if dk in _DEDUP and now - _DEDUP[dk] < DEDUP_TTL:
        return {"ok": True, "dedup": True}
    _DEDUP[dk] = now

    INGRESS_LOG.append({"ts": now, "ip": (req.client.host if req and req.client else "?"), "data": data})
    try:
        _task_q.put_nowait(data)
    except queue.Full:
        send_telegram("⚠️ queue full → drop signal: " + json.dumps(data))
        return {"ok": False, "queued": False, "reason": "queue_full"}

    return {"ok": True, "queued": True, "qsize": _task_q.qsize()}

app = FastAPI()

@app.get("/")
def root():
    return {"ok": True}

@app.post("/signal")
async def signal(req: Request):
    return await _ingest(req)

@app.post("/webhook")
async def webhook(req: Request):
    return await _ingest(req)

@app.post("/alert")
async def alert(req: Request):
    return await _ingest(req)

@app.get("/health")
def health():
    return {"ok": True, "ingress": len(INGRESS_LOG), "queue": _task_q.qsize(), "workers": WORKERS}

@app.get("/ingress")
def ingress():
    return list(INGRESS_LOG)[-30:]

@app.get("/positions")
def positions():
    return {"positions": get_open_positions()}

@app.get("/queue")
def queue_size():
    return {"size": _task_q.qsize(), "max": QUEUE_MAX}

@app.get("/config")
def config():
    return {
        "DEFAULT_AMOUNT": DEFAULT_AMOUNT, "LEVERAGE": LEVERAGE,
        "DEDUP_TTL": DEDUP_TTL, "BIZDEDUP_TTL": BIZDEDUP_TTL,
        "WORKERS": WORKERS, "QUEUE_MAX": QUEUE_MAX,
        "LOG_INGRESS": LOG_INGRESS,
        "FORCE_DEFAULT_AMOUNT": FORCE_DEFAULT_AMOUNT,
        "SYMBOL_AMOUNT": SYMBOL_AMOUNT,
    }

@app.get("/pending")
def pending():
    return get_pending_snapshot()

# [ADD] — 시간봉 강제 태깅 전용 엔드포인트 (Pine 수정 없이 URL만 변경)
def _ingest_with_tf_override(data: Dict[str, Any], tf: str):
    now = time.time()
    d = dict(data or {})
    d["timeframe"] = tf  # 핵심: 서버가 TF를 주입

    dk = _dedup_key(d)
    if dk in _DEDUP and now - _DEDUP[dk] < DEDUP_TTL:
        return {"ok": True, "dedup": True, "tf": tf}
    _DEDUP[dk] = now

    INGRESS_LOG.append({"ts": now, "ip": "tf-override", "data": d})
    try:
        _task_q.put_nowait(d)
    except queue.Full:
        send_telegram("⚠️ queue full → drop signal(tf): " + json.dumps(d))
        return {"ok": False, "queued": False, "reason": "queue_full"}

    return {"ok": True, "queued": True, "qsize": _task_q.qsize(), "tf": tf}

@app.post("/signal/1h")
async def signal_1h(req: Request):
    data = await _parse_any(req)
    return _ingest_with_tf_override(data, "1H")

@app.post("/signal/2h")
async def signal_2h(req: Request):
    data = await _parse_any(req)
    return _ingest_with_tf_override(data, "2H")

@app.post("/signal/3h")
async def signal_3h(req: Request):
    data = await _parse_any(req)
    return _ingest_with_tf_override(data, "3H")

@app.post("/signal/4h")
async def signal_4h(req: Request):
    data = await _parse_any(req)
    return _ingest_with_tf_override(data, "4H")

@app.on_event("startup")
def on_startup():
    for i in range(WORKERS):
        t = threading.Thread(target=_worker_loop, args=(i,), daemon=True, name=f"signal-worker-{i}")
        t.start()
    start_capacity_guard()
    start_watchdogs()
    start_reconciler()
    start_policy_manager()   # [ADD] 정책 매니저 시작

    try:
        threading.Thread(
            target=send_telegram,
            args=("✅ FastAPI up (workers + watchdog + reconciler + capacity-guard + policy)",),
            daemon=True
        ).start()
    except Exception:
        pass
