# server.py
# -*- coding: utf-8 -*-
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import os, json, time, hashlib, threading
from trader import (
    start_all_backgrounds, enter_position, take_partial_profit,
    close_position, reduce_by_contracts
)
from bitget_api import convert_symbol

app = FastAPI(title="Trading Signal Bridge")

# ---- idempotency (중복 방지) ----
_DEDUPE_LOCK = threading.Lock()
_RECENT_SIGS = {}  # hash -> ts

def _dedupe_check(raw: bytes, window_sec: float = 3.0) -> bool:
    """같은 payload가 짧은 시간 내 두 번 오면 True(중복)"""
    h = hashlib.sha256(raw).hexdigest()
    now = time.time()
    with _DEDUPE_LOCK:
        # cleanup old
        for k, ts in list(_RECENT_SIGS.items()):
            if now - ts > window_sec:
                _RECENT_SIGS.pop(k, None)
        if h in _RECENT_SIGS and now - _RECENT_SIGS[h] <= window_sec:
            return True
        _RECENT_SIGS[h] = now
        return False

@app.on_event("startup")
async def _on_startup():
    # 중요 환경변수 로그(중복/잘못된 로딩 점검용)
    print("[ENV] PRODUCT_TYPE =", os.getenv("BITGET_PRODUCT_TYPE"))
    print("[ENV] POSITION_MODE=", os.getenv("BITGET_POSITION_MODE"))
    print("[ENV] MARGIN_MODE  =", os.getenv("BITGET_MARGIN_MODE"))
    print("[ENV] WEB_CONCURRENCY =", os.getenv("WEB_CONCURRENCY"))
    # 부팅 직후: 이어받기 + 백그라운드 시작
    start_all_backgrounds()

def _handle_signal(j: dict) -> dict:
    t   = (j.get("type") or "").strip()
    sym = convert_symbol(j.get("symbol") or j.get("ticker") or "")
    side= (j.get("side") or "long").lower()
    amt = float(j.get("amount") or 0)
    lev = float(j.get("leverage") or 0)

    if not sym:
        return {"ok": False, "msg": "symbol missing"}

    if t == "entry":
        return {"ok": True, "res": enter_position(
            sym, side=side,
            usdt_amount=amt if amt>0 else None,
            leverage=lev if lev>0 else None
        )}

    if t == "tp1":
        r=float(os.getenv("TP1_PCT","0.30"))
        return {"ok": True, "res": take_partial_profit(sym, ratio=r, side=side, reason="tp1")}

    if t == "tp2":
        r=float(os.getenv("TP2_PCT","0.5714286"))
        return {"ok": True, "res": take_partial_profit(sym, ratio=r, side=side, reason="tp2")}

    if t == "tp3":
        return {"ok": True, "res": close_position(sym, side=side, reason="tp3")}

    if t in ("sl1","sl2","failCut","emaExit","stoploss"):
        return {"ok": True, "res": close_position(sym, side=side, reason=t)}

    if t == "reduce":
        if "reduce_pct" in j:
            pct = float(j.get("reduce_pct") or 0)/100.0
            return {"ok": True, "res": take_partial_profit(sym, ratio=pct, side=side, reason="tp_pct_api")}
        qty = float(j.get("contracts") or 0)
        return {"ok": True, "res": reduce_by_contracts(sym, qty, side)}

    return {"ok": False, "msg": f"unknown type {t}"}

def _read_json_safely(req: Request) -> (dict, bytes):
    try:
        raw = await_bytes = None
        # body를 먼저 읽고, 파싱 실패시를 대비해 raw도 반환
        await_bytes = None
        try:
            await_bytes = (await req.body()) or b"{}"
        except Exception:
            pass
        j = {}
        try:
            j = json.loads((await_bytes or b"{}").decode() or "{}")
        except Exception:
            try:
                j = (await req.json())  # fallback
            except Exception:
                j = {}
        return j, (await_bytes or json.dumps(j).encode())
    except Exception:
        return {}, b"{}"

@app.post("/signal")
async def signal(req: Request):
    j, raw = _read_json_safely.__wrapped__(req) if hasattr(_read_json_safely, "__wrapped__") else await _read_json_safely(req)
    # 중복 차단
    if _dedupe_check(raw):
        return JSONResponse({"ok": True, "deduped": True})
    return JSONResponse(_handle_signal(j))

# 트뷰에서 /signal/3h 같은 꼬리 경로도 허용
@app.post("/signal/{tail:path}")
async def signal_any(tail: str, req: Request):
    j, raw = _read_json_safely.__wrapped__(req) if hasattr(_read_json_safely, "__wrapped__") else await _read_json_safely(req)
    if _dedupe_check(raw):
        return JSONResponse({"ok": True, "deduped": True})
    return JSONResponse(_handle_signal(j))

@app.get("/healthz")
async def healthz():
    return {"ok": True}
