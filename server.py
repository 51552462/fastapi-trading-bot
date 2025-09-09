# -*- coding: utf-8 -*-
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import os, json
from trader import (
    start_all_backgrounds, enter_position, take_partial_profit,
    close_position, reduce_by_contracts
)
from bitget_api import convert_symbol

app = FastAPI(title="Trading Signal Bridge")

@app.on_event("startup")
async def _on_startup():
    # 부팅 직후: 원격 포지션 즉시 이어받기 + 가드/리컨/워치독 시작
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
        return {"ok": True, "res": enter_position(sym, side=side,
                    usdt_amount=amt if amt>0 else None,
                    leverage=lev if lev>0 else None)}

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
        # 분할을 퍼센트로 줄 수도, 계약수로 줄 수도 있게 두 모드 지원
        if "reduce_pct" in j:
            pct = float(j.get("reduce_pct") or 0)/100.0
            return {"ok": True, "res": take_partial_profit(sym, ratio=pct, side=side, reason="tp_pct_api")}
        qty = float(j.get("contracts") or 0)
        return {"ok": True, "res": reduce_by_contracts(sym, qty, side)}

    return {"ok": False, "msg": f"unknown type {t}"}

# 표준 경로
@app.post("/signal")
async def signal(req: Request):
    try:
        j = await req.json()
    except Exception:
        body = await req.body()
        try:
            j = json.loads(body.decode() or "{}")
        except Exception:
            j = {}
    return JSONResponse(_handle_signal(j))

# 트뷰에서 /signal/3h 같은 꼬리 경로도 허용
@app.post("/signal/{tail:path}")
async def signal_any(tail: str, req: Request):
    try:
        j = await req.json()
    except Exception:
        body = await req.body()
        try:
            j = json.loads(body.decode() or "{}")
        except Exception:
            j = {}
    return JSONResponse(_handle_signal(j))

# 상태 점검용(선택)
@app.get("/healthz")
async def healthz():
    return {"ok": True}
