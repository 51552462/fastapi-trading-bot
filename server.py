# -*- coding: utf-8 -*-
from fastapi import FastAPI, Request
from trader import start_all_backgrounds, enter_position, take_partial_profit, close_position, reduce_by_contracts
from bitget_api import convert_symbol

app = FastAPI()

@app.on_event("startup")
async def _boot():
    # 부팅 직후 원격 포지션 이어받기 + 백그라운드 루프 시작
    start_all_backgrounds()

@app.post("/signal")
async def signal(req: Request):
    j = await req.json()
    t = (j.get("type") or "").strip()
    sym = convert_symbol(j.get("symbol") or j.get("ticker") or "")
    side = (j.get("side") or "long").lower()
    amt  = float(j.get("amount") or 0)
    lev  = float(j.get("leverage") or 0)
    res = {"ok": True}
    try:
        if t == "entry":
            res["res"] = enter_position(sym, side=side, usdt_amount=amt if amt>0 else None, leverage=lev if lev>0 else None)
        elif t == "tp1":
            res["res"] = take_partial_profit(sym, ratio=float(os.getenv("TP1_PCT","0.30")), side=side, reason="tp1")
        elif t == "tp2":
            res["res"] = take_partial_profit(sym, ratio=float(os.getenv("TP2_PCT","0.5714286")), side=side, reason="tp2")
        elif t == "tp3":
            res["res"] = close_position(sym, side=side, reason="tp3")
        elif t in ("sl1","sl2","failCut","emaExit","stoploss"):
            res["res"] = close_position(sym, side=side, reason=t)
        elif t == "reduce":
            # 테스트/수동: 특정 계약수로 감축
            qty = float(j.get("contracts") or 0)
            if qty<=0 and "reduce_pct" in j:
                # 잔량 pct → 계약수로 변환(서버 내부에서 계산 가능)
                res["res"] = take_partial_profit(sym, ratio=float(j["reduce_pct"])/100.0, side=side, reason="tp_pct_api")
            else:
                res["res"] = reduce_by_contracts(sym, qty, side)
        else:
            res = {"ok": False, "msg": f"unknown type {t}"}
    except Exception as e:
        res = {"ok": False, "msg": f"{type(e).__name__}: {e}"}
    return res
