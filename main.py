# -*- coding: utf-8 -*-
"""
main.py (TV/기타 → Bitget)
- /ping, /health
- /webhook, /signal  (둘 다 동일 동작)
- /diag/last_price?symbol=BTCUSDT  (진단용)
- 분할익절(30/40/30), 전체종료(sl/failCut/emaExit/liquidation/close)
"""
import os, json
from typing import Optional, Dict, Any
from fastapi import FastAPI, Request, Query
from pydantic import BaseModel
import uvicorn

from bitget import Bitget, convert_symbol, round_size

APP = FastAPI(title="TV→Bitget Bot", version="2.1")

DEFAULT_AMOUNT = float(os.getenv("DEFAULT_AMOUNT", "15"))
FORCE_DEFAULT_AMOUNT = os.getenv("FORCE_DEFAULT_AMOUNT", "1") == "1"
LEVERAGE = int(os.getenv("LEVERAGE", "5"))
STRICT_TICKER = os.getenv("STRICT_TICKER", "0") == "1"

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")

def send_telegram(msg: str):
    if not TG_TOKEN or not TG_CHAT:
        return
    try:
        import requests
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"},
            timeout=5
        )
    except Exception:
        pass

try:
    BG = Bitget()
except Exception as e:
    BG = None
    send_telegram(f"❌ Bitget init failed: {e}")

class Webhook(BaseModel):
    type: Optional[str] = None
    symbol: Optional[str] = None
    ticker: Optional[str] = None
    side: Optional[str] = None
    direction: Optional[str] = None
    amount: Optional[float] = None
    note: Optional[str] = None
    class Config:
        extra = "allow"

@APP.get("/ping")
def ping(): return {"ok": True}

@APP.get("/health")
def health(): return {"ok": BG is not None}

@APP.get("/diag/last_price")
def diag_last_price(symbol: str = Query(...)):
    sym = convert_symbol(symbol)
    if BG is None:
        return {"ok": False, "reason": "bitget_client_not_ready"}
    px = BG.last_price(sym)
    return {"ok": px is not None, "symbol": sym, "price": px}

def _infer_side(raw: Optional[str], default_long: str="long") -> str:
    s = (raw or "").lower()
    if s in ("long","buy","b","l","open_long"): return "long"
    if s in ("short","sell","s","sh","open_short"): return "short"
    return default_long

def _base_side(side: str) -> str:
    return "buy" if side == "long" else "sell"

def _malformed(sym: str) -> bool:
    return (not sym) or (len(sym) < 6) or (not sym.endswith("USDT"))

def _calc_entry_qty(sym: str, px: float, payload_amount: Optional[float]) -> float:
    amt = DEFAULT_AMOUNT if (FORCE_DEFAULT_AMOUNT or not payload_amount or float(payload_amount) <= 0) else float(payload_amount)
    q = round_size(sym, amt / max(px, 1e-12))
    return q

def _ensure_ready(sym: str):
    try: BG.ensure_one_way()
    except Exception: pass
    try: BG.set_leverage(sym, leverage=LEVERAGE)
    except Exception: pass

def _close_all(sym: str):
    ls, ss = BG.position_size(sym)
    ok = True; res = {}
    if ls > 0:
        _ok, r = BG.place_market(sym, "sell", round_size(sym, ls), reduce_only=True); ok &= _ok; res["close_long"] = r
    if ss > 0:
        _ok, r = BG.place_market(sym, "buy", round_size(sym, ss), reduce_only=True); ok &= _ok; res["close_short"] = r
    return ok, res

def _partial_close(sym: str, pct: float):
    ls, ss = BG.position_size(sym)
    ok = True; res = {}
    if ls > 0:
        take = round_size(sym, ls * pct)
        if take > 0:
            _ok, r = BG.place_market(sym, "sell", take, reduce_only=True); ok &= _ok; res["reduce_long"] = r
    if ss > 0:
        take = round_size(sym, ss * pct)
        if take > 0:
            _ok, r = BG.place_market(sym, "buy", take, reduce_only=True); ok &= _ok; res["reduce_short"] = r
    return ok, res

async def _handle_payload(data: Dict[str, Any]):
    symbol = convert_symbol(str(data.get("symbol") or data.get("ticker") or ""))
    side   = _infer_side(data.get("side") or data.get("direction"))
    typ    = (data.get("type") or "").lower().strip()

    if _malformed(symbol):
        send_telegram("⚠️ malformed_symbol drop: " + json.dumps({"raw": data.get("symbol") or data.get("ticker"), "norm": symbol}, ensure_ascii=False))
        return {"ok": False, "reason": "malformed_symbol", "symbol": symbol}

    if BG is None:
        send_telegram("❌ Bitget client not ready")
        return {"ok": False, "reason": "bitget_client_not_ready"}

    _ensure_ready(symbol)

    if typ in ("entry","open","open_entry"):
        px = BG.last_price(symbol)
        if not px or px <= 0:
            send_telegram(f"❗ ticker_fail {symbol} trace=")
            return {"ok": False, "reason": "ticker_fail", "symbol": symbol}
        qty = _calc_entry_qty(symbol, px, data.get("amount"))
        if qty <= 0:
            return {"ok": False, "reason": "qty_le_0", "symbol": symbol, "price": px}
        ok, res = BG.place_market(symbol, _base_side(side), qty, reduce_only=False)
        if ok:
            send_telegram(f"✅ ENTRY {symbol} {side} q={qty}")
            return {"ok": True, "symbol": symbol, "qty": qty, "side": side, "price": px, "res": res}
        send_telegram(f"❌ ENTRY_FAIL {symbol} {side} q={qty} :: {res}")
        return {"ok": False, "reason": "order_fail", "res": res}

    if typ in ("tp1","take1","t1"):
        ok, res = _partial_close(symbol, 0.30); send_telegram(("✅ TP1 " if ok else "❌ TP1_FAIL ")+symbol); return {"ok": ok, "action":"tp1","res":res}
    if typ in ("tp2","take2","t2"):
        ok, res = _partial_close(symbol, 0.40); send_telegram(("✅ TP2 " if ok else "❌ TP2_FAIL ")+symbol); return {"ok": ok, "action":"tp2","res":res}
    if typ in ("tp3","take3","t3"):
        ok, res = _partial_close(symbol, 0.30); send_telegram(("✅ TP3 " if ok else "❌ TP3_FAIL ")+symbol); return {"ok": ok, "action":"tp3","res":res}

    if typ in ("sl","stop","failcut","emaexit","liquidation","close","exit"):
        ok, res = _close_all(symbol); send_telegram(("✅ EXIT_ALL " if ok else "❌ EXIT_ALL_FAIL ")+symbol); return {"ok": ok,"action":"close_all","res":res}

    send_telegram(f"ℹ️ ignored type: {typ} / {symbol}")
    return {"ok": True, "ignored": True, "type": typ, "symbol": symbol}

@APP.post("/webhook")
async def webhook(req: Request):
    try: data = await req.json()
    except Exception: data = {}
    return await _handle_payload(dict(data))

@APP.post("/signal")   # 트뷰가 /signal로 보내는 경우 호환
async def signal(req: Request):
    try: data = await req.json()
    except Exception: data = {}
    return await _handle_payload(dict(data))

if __name__ == "__main__":
    uvicorn.run(APP, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
