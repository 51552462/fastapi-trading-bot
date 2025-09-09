# -*- coding: utf-8 -*-
import os, time, threading, inspect
from typing import Dict, Any, Optional, List
from logger import info, warn, error, debug
from telegram_bot import send_telegram
from risk_guard import can_open
from bitget_api import (
    convert_symbol, get_last_price, get_open_positions as _raw_get_positions,
    place_market_order, place_reduce_by_size, get_symbol_spec, round_down_step,
)

# ---- ENV ----
DEFAULT_AMOUNT = float(os.getenv("DEFAULT_AMOUNT","80"))
LEVERAGE       = float(os.getenv("LEVERAGE","5"))
FORCE_DEFAULT_AMOUNT = os.getenv("FORCE_DEFAULT_AMOUNT","0") == "1"
SYMBOL_AMOUNT_JSON = os.getenv("SYMBOL_AMOUNT_JSON","").strip()

TP1_PCT = float(os.getenv("TP1_PCT","0.30"))        # 30% of current position
TP2_PCT = float(os.getenv("TP2_PCT","0.5714286"))   # 40% of remaining (0.4/0.7)
TP3_PCT = float(os.getenv("TP3_PCT","1.0"))         # 100% of remaining

STOP_ROE          = float(os.getenv("STOP_ROE","0.10"))       # -10% ROE triggers (we compare <= -STOP_ROE)
STOP_PRICE_MOVE   = float(os.getenv("STOP_PRICE_MOVE","0.20"))# fallback absolute price move (%)
STOP_CHECK_SEC    = float(os.getenv("STOP_CHECK_SEC","2"))
STOP_CONFIRM_N    = int(float(os.getenv("STOP_CONFIRM_N","1")))
STOP_DEBOUNCE_SEC = float(os.getenv("STOP_DEBOUNCE_SEC","2"))
STOP_COOLDOWN_SEC = float(os.getenv("STOP_COOLDOWN_SEC","3"))

RECON_INTERVAL_SEC = float(os.getenv("RECON_INTERVAL_SEC","3"))
REOPEN_COOLDOWN_SEC = float(os.getenv("REOPEN_COOLDOWN_SEC","30"))

MAX_OPEN_POSITIONS = int(float(os.getenv("MAX_OPEN_POSITIONS","120")))
LONG_BYPASS_CAP    = (os.getenv("LONG_BYPASS_CAP","0")=="1")
SHORT_BYPASS_CAP   = (os.getenv("SHORT_BYPASS_CAP","0")=="1")

# ---- State ----
position_data: Dict[str, Dict[str, Any]] = {}
_POS_LOCK = threading.RLock()
_CAP_LOCK = threading.RLock()
_CAPACITY = {"last_count":0, "long_count":0, "short_count":0, "ts":0.0}
LAST_EXIT_TS: Dict[str, float] = {}

def _norm_side(s: str) -> str:
    s = (s or "").lower().strip()
    return "long" if s in ("long","buy") else "short"

def _choose_amount(symbol: str, usdt_amount: Optional[float]) -> float:
    if FORCE_DEFAULT_AMOUNT or not SYMBOL_AMOUNT_JSON:
        return float(usdt_amount or DEFAULT_AMOUNT)
    try:
        m = json.loads(SYMBOL_AMOUNT_JSON)
        return float(m.get(symbol, usdt_amount or DEFAULT_AMOUNT))
    except Exception:
        return float(usdt_amount or DEFAULT_AMOUNT)

# ---- Safe wrapper for open positions ----
def _safe_get_positions() -> List[Dict[str,Any]]:
    try:
        if len(inspect.signature(_raw_get_positions).parameters) >= 1:
            return _raw_get_positions(None)
        return _raw_get_positions()
    except TypeError:
        try:
            return _raw_get_positions(None)
        except Exception:
            return []
    except Exception:
        return []

def _update_local_state_from_exchange():
    opens = _safe_get_positions()
    seen = set()
    with _POS_LOCK:
        for p in opens:
            sym = convert_symbol(p.get("symbol") or "")
            side = _norm_side(p.get("side"))
            size = float(p.get("size") or 0.0)
            seen.add(f"{sym}:{side}")
            d = position_data.setdefault(f"{sym}:{side}", {"size":0.0, "entry":float(p.get("entryPrice") or 0.0), "leverage":float(p.get("leverage") or 0.0)})
            d.update({"size": size, "entry": float(p.get("entryPrice") or 0.0), "leverage": float(p.get("leverage") or 0.0)})
        # cleanup zeroed entries
        for k in list(position_data.keys()):
            if k not in seen:
                if position_data.get(k,{}).get("size",0) <= 0:
                    position_data.pop(k, None)

# ---- Capacity guard loop ----
def _capacity_loop():
    while True:
        try:
            opens = _safe_get_positions()
            long_c = sum(1 for p in opens if _norm_side(p.get("side"))=="long" and float(p.get("size") or 0)>0)
            short_c= sum(1 for p in opens if _norm_side(p.get("side"))=="short" and float(p.get("size") or 0)>0)
            with _CAP_LOCK:
                _CAPACITY.update({"last_count": long_c + short_c, "long_count": long_c, "short_count": short_c, "ts": time.time()})
        except Exception:
            pass
        time.sleep(5)

def start_capacity_guard():
    threading.Thread(target=_capacity_loop, name="cap-guard", daemon=True).start()

# ---- Reconciler ----
def _recon_loop():
    while True:
        try:
            _update_local_state_from_exchange()
        except Exception:
            pass
        time.sleep(RECON_INTERVAL_SEC)

def start_reconciler():
    threading.Thread(target=_recon_loop, name="reconciler", daemon=True).start()

# ---- Watchdogs (stop loss) ----
def _entry_price(symbol: str, side: str) -> float:
    with _POS_LOCK:
        d = position_data.get(f"{symbol}:{side}")
        return float(d.get("entry", 0.0)) if d else 0.0

def _held_size(symbol: str, side: str) -> float:
    with _POS_LOCK:
        d = position_data.get(f"{symbol}:{side}")
        return float(d.get("size", 0.0)) if d else 0.0

def _roe_now(symbol: str, side: str) -> float:
    entry = _entry_price(symbol, side)
    last  = get_last_price(symbol) or 0.0
    if entry <= 0 or last <= 0: return 0.0
    change = (last - entry) / entry
    if side == "short": change = -change
    return change * LEVERAGE

def _watch_loop():
    last_fire = {}
    while True:
        try:
            _update_local_state_from_exchange()
            for k in list(position_data.keys()):
                symbol, side = k.split(":")
                size = _held_size(symbol, side)
                if size <= 0: 
                    continue
                roe = _roe_now(symbol, side)
                # -STOP_ROE or adverse price move
                if roe <= -abs(STOP_ROE):
                    now = time.time()
                    if now - last_fire.get(k, 0) >= STOP_DEBOUNCE_SEC:
                        take = min(size, size)  # full remaining
                        place_reduce_by_size(symbol, take, side)
                        send_telegram(f"🛑 failCut: {symbol}_{side} ROE={roe:.2%}")
                        LAST_EXIT_TS[k] = now
                        last_fire[k] = now
        except Exception:
            pass
        time.sleep(max(1.0, STOP_CHECK_SEC))

def start_watchdogs():
    threading.Thread(target=_watch_loop, name="watchdogs", daemon=True).start()

# ---- Public API ----
def start_workers():
    start_watchdogs(); start_reconciler(); start_capacity_guard()

def get_pending_snapshot():
    with _POS_LOCK, _CAP_LOCK:
        return {
            "positions": position_data.copy(),
            "capacity": _CAPACITY.copy(),
        }

def enter_position(symbol: str, side: str, usdt_amount: Optional[float]=None, timeframe: Optional[str]=None):
    symbol = convert_symbol(symbol); side = _norm_side(side)
    _update_local_state_from_exchange()
    # capacity & duplicate checks
    opens = _safe_get_positions()
    has_same = any(convert_symbol(p.get("symbol"))==symbol and _norm_side(p.get("side"))==side and float(p.get("size") or 0)>0 for p in opens)
    with _CAP_LOCK:
        total_open = _CAPACITY.get("last_count",0)
    ok, reason = can_open(symbol, total_open, has_same)
    if not ok:
        warn("open-blocked", symbol=symbol, side=side, reason=reason)
        send_telegram(f"⚠️ OPEN BLOCKED: {symbol}_{side} reason={reason}")
        return {"ok": False, "reason": reason}
    amt = _choose_amount(symbol, usdt_amount)
    r = place_market_order(symbol, amt, side, LEVERAGE)
    info("open", symbol=symbol, side=side, amount=amt, res=str(r)[:240])
    if str(r.get("code","")) in ("00000","0","200"):
        send_telegram(f"✅ ENTRY {symbol}_{side} ${amt}")
        return {"ok": True, "res": r}
    send_telegram(f"❌ ENTRY FAIL {symbol}_{side} {r}")
    return {"ok": False, "res": r}

def close_position(symbol: str, side: str, reason: str="manual"):
    symbol = convert_symbol(symbol); side = _norm_side(side)
    _update_local_state_from_exchange()
    held = _held_size(symbol, side)
    if held <= 0:
        return {"ok": False, "reason": "no_position"}
    r = place_reduce_by_size(symbol, held, side)
    info("close", symbol=symbol, side=side, reason=reason, res=str(r)[:240])
    send_telegram(f"🚪 CLOSE {symbol}_{side} reason={reason}")
    return {"ok": True, "res": r}

def take_partial_profit(symbol: str, ratio: float, side: str="long", reason: str="tp"):
    symbol = convert_symbol(symbol); side = _norm_side(side)
    _update_local_state_from_exchange()
    held = _held_size(symbol, side)
    if held <= 0:
        return {"ok": False, "reason": "no_position"}
    close_sz = round_down_step(held * max(0.0, min(1.0, ratio)), get_symbol_spec(symbol)["sizeStep"])
    if close_sz <= 0: 
        return {"ok": False, "reason":"bad_ratio"}
    r = place_reduce_by_size(symbol, close_sz, side)
    info("tp", symbol=symbol, side=side, ratio=ratio, size=close_sz, res=str(r)[:240])
    send_telegram(f"🎯 TP {symbol}_{side} ×{ratio:.2f}")
    return {"ok": True, "res": r}
