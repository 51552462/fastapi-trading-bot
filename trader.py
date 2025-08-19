# trader.py — 중복 진입 가드 + 리컨실 보강 + -10% 워치독
import os, time, threading, math
from typing import Dict, Optional

from bitget_api import (
    convert_symbol, get_last_price, get_open_positions,
    place_market_order, place_reduce_by_size, get_symbol_spec, round_down_step,
)

try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str): print("[TG]", msg)

LEVERAGE = float(os.getenv("LEVERAGE", "5"))
TP1_PCT = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT = float(os.getenv("TP2_PCT", "0.40"))
TP3_PCT = float(os.getenv("TP3_PCT", "0.30"))

STOP_PCT           = float(os.getenv("STOP_PCT", "0.10"))   # -10%
STOP_CHECK_SEC     = float(os.getenv("STOP_CHECK_SEC", "1.0"))
STOP_COOLDOWN_SEC  = float(os.getenv("STOP_COOLDOWN_SEC", "5.0"))

RECON_INTERVAL_SEC = float(os.getenv("RECON_INTERVAL_SEC", "60"))
RECON_DEBUG        = os.getenv("RECON_DEBUG", "0") == "1"

ENTRY_GUARD_SEC = float(os.getenv("ENTRY_GUARD_SEC", "75"))
_ENTRY_GUARD: Dict[str, float] = {}
_ENTRY_GUARD_LOCK = threading.RLock()

def _entry_guard_active(key: str) -> bool:
    with _ENTRY_GUARD_LOCK:
        return time.time() < _ENTRY_GUARD.get(key, 0.0)
def _arm_entry_guard(key: str, sec: float = None):
    with _ENTRY_GUARD_LOCK:
        _ENTRY_GUARD[key] = time.time() + float(sec or ENTRY_GUARD_SEC)

# ── 상태/락 ────────────────────────────────────────────────────
position_data: Dict[str, dict] = {}
_POS_LOCK = threading.RLock()

_KEY_LOCKS: Dict[str, threading.RLock] = {}
_KEY_LOCKS_LOCK = threading.RLock()
def _key(symbol: str, side: str) -> str: return f"{convert_symbol(symbol)}_{side}"
def _lock_for(key: str):
    with _KEY_LOCKS_LOCK:
        if key not in _KEY_LOCKS:
            _KEY_LOCKS[key] = threading.RLock()
    return _KEY_LOCKS[key]

_PENDING = {"entry": {}, "close": {}, "tp": {}}
_PENDING_LOCK = threading.RLock()
def _pending_key(prefix: str, symbol: str, side: str) -> str:
    return f"{prefix}:{convert_symbol(symbol)}:{side}:{int(time.time()*1000)}"
def _mark_done(kind: str, pkey: str, note: str = ""):
    with _PENDING_LOCK:
        _PENDING.get(kind, {}).pop(pkey, None)
    if RECON_DEBUG and note:
        send_telegram(f"✅ pending done [{kind}] {pkey} {note}")

def get_pending_snapshot() -> Dict[str, Dict]:
    with _PENDING_LOCK:
        return {k: list(v.keys()) for k, v in _PENDING.items()}

# ── 원격 포지션 조회 헬퍼 ─────────────────────────────────────
def _get_remote(symbol: str, side: Optional[str] = None):
    sym = convert_symbol(symbol)
    for p in get_open_positions():
        if p.get("symbol") == sym and (side is None or p.get("side") == side):
            return p
    return None
def _get_remote_any(symbol: str):
    sym = convert_symbol(symbol)
    for p in get_open_positions():
        if p.get("symbol") == sym and float(p.get("size", 0)) > 0:
            return p
    return None

# ── 주문 로직 ─────────────────────────────────────────────────
def enter_position(symbol: str, usdt_amount: float, side: str = "long", leverage: float = None):
    symbol = convert_symbol(symbol); side = (side or "long").lower()
    key = _key(symbol, side); lev = float(leverage or LEVERAGE)
    pkey = _pending_key("entry", symbol, side)

    if _entry_guard_active(key):
        if RECON_DEBUG: send_telegram(f"⏳ ENTRY guard skip {side.upper()} {symbol}")
        return

    with _PENDING_LOCK:
        _PENDING["entry"][pkey] = {"symbol": symbol, "side": side, "amount": usdt_amount,
                                   "leverage": lev, "created": time.time(), "last_try": 0.0}

    with _lock_for(key):
        with _POS_LOCK:
            if position_data.get(key):
                _mark_done("entry", pkey, "(local-exists)"); return
        if _get_remote_any(symbol):
            _mark_done("entry", pkey, "(exists)"); return

        last = get_last_price(symbol)
        if not last: return  # 다음 리컨실

        resp = place_market_order(symbol, usdt_amount,
                                  side=("buy" if side == "long" else "sell"),
                                  leverage=lev, reduce_only=False)
        if str(resp.get("code", "")) == "00000":
            with _POS_LOCK: position_data[key] = {"symbol": symbol, "side": side, "ts": time.time()}
            _mark_done("entry", pkey); _arm_entry_guard(key)
            send_telegram(f"🚀 ENTRY {side.upper()} {symbol}\n• Notional≈ {usdt_amount} USDT\n• Lvg: {lev}x")
        else:
            # minQty/수량 오류는 즉시 소거
            code = str(resp.get("code", ""))
            if code.startswith("LOCAL_MIN_QTY") or code.startswith("LOCAL_BAD_QTY"):
                _mark_done("entry", pkey, f"({code})")
                send_telegram(f"⛔ ENTRY 스킵 {symbol} {side} → {resp}")

def _sweep_full_close(symbol: str, side: str, max_retry: int = 5, sleep_s: float = 0.4):
    for _ in range(max_retry):
        p = _get_remote(symbol, side)
        if not p or float(p.get("size", 0)) <= 0: return True
        size = float(p["size"])
        r = place_reduce_by_size(symbol, size, side)
        if str(r.get("code", "")) == "00000":
            time.sleep(sleep_s)  # 체결 반영 대기
            continue
        time.sleep(sleep_s)
    return False

def close_position(symbol: str, side: str = "long", reason: str = "manual"):
    symbol = convert_symbol(symbol); side = (side or "long").lower()
    key = _key(symbol, side); pkey = _pending_key("close", symbol, side)

    with _PENDING_LOCK:
        _PENDING["close"][pkey] = {"symbol": symbol, "side": side, "reason": reason,
                                   "created": time.time(), "last_try": 0.0}

    with _lock_for(key):
        ok = _sweep_full_close(symbol, side)
        if ok:
            with _POS_LOCK: position_data.pop(key, None)
            _mark_done("close", pkey)
            with _ENTRY_GUARD_LOCK: _ENTRY_GUARD.pop(key, None)
            send_telegram(f"✅ CLOSE {side.upper()} {symbol} ({reason})")

def take_partial_profit(symbol: str, pct: float, side: str = "long"):
    symbol = convert_symbol(symbol); side = (side or "long").lower()
    key = _key(symbol, side)
    with _lock_for(key):
        p = _get_remote(symbol, side)
        if not p or float(p.get("size", 0)) <= 0:
            send_telegram(f"⚠️ TP 스킵(원격 없음) {side.upper()} {symbol}")
            return
        size_step = float(get_symbol_spec(symbol).get("sizeStep", 0.001))
        cut = round_down_step(float(p["size"]) * float(pct), size_step)
        if cut <= 0: return
        r = place_reduce_by_size(symbol, cut, side)
        if str(r.get("code", "")) == "00000":
            send_telegram(f"✂️ TP {int(pct*100)}% {side.upper()} {symbol}")
        else:
            send_telegram(f"⛔ TP 실패 {side.upper()} {symbol} → {r}")

def reduce_by_contracts(symbol: str, contracts: float, side: str = "long"):
    symbol = convert_symbol(symbol); side = (side or "long").lower()
    if contracts <= 0: return
    r = place_reduce_by_size(symbol, contracts, side)
    if str(r.get("code", "")) == "00000":
        send_telegram(f"✂️ REDUCE {contracts}c {side.upper()} {symbol}")
    else:
        send_telegram(f"⛔ REDUCE 실패 {side.upper()} {symbol} → {r}")

# ── -10% failCut 워치독 ────────────────────────────────────────
_STOP_FIRED: Dict[str, float] = {}
_STOP_LOCK = threading.RLock()

def _est_loss_ratio(p: Dict) -> float:
    # 평균가/현재가로 근사 (마진/레버리지는 단순화)
    last = get_last_price(p["symbol"]) or 0
    e = float(p.get("entry_price", 0))
    if last <= 0 or e <= 0: return 0.0
    if p.get("side") == "long":  return (last - e) / e
    else:                        return (e - last) / e

def _watchdog_loop():
    while True:
        time.sleep(max(0.2, STOP_CHECK_SEC))
        try:
            now = time.time()
            for p in get_open_positions():
                key = _key(p["symbol"], p["side"])
                loss = _est_loss_ratio(p)
                if loss <= -float(STOP_PCT):
                    with _STOP_LOCK:
                        if now - _STOP_FIRED.get(key, 0.0) < STOP_COOLDOWN_SEC: continue
                        _STOP_FIRED[key] = now
                    close_position(p["symbol"], side=p["side"], reason="failCut")
        except Exception: pass

# ── 리컨실러 ───────────────────────────────────────────────────
def _reconciler_loop():
    while True:
        time.sleep(RECON_INTERVAL_SEC)
        try:
            # ENTRY
            with _PENDING_LOCK:
                es = list(_PENDING["entry"].items())
            for pkey, item in es:
                sym, side, amt, lev = item["symbol"], item["side"], item["amount"], item["leverage"]
                key = _key(sym, side)
                if _entry_guard_active(key): _mark_done("entry", pkey, "(guard)"); continue
                with _POS_LOCK:
                    if position_data.get(key): _mark_done("entry", pkey, "(local-exists)"); continue
                if _get_remote_any(sym): _mark_done("entry", pkey, "(exists)"); continue
                r = place_market_order(sym, amt,
                                       side=("buy" if side=="long" else "sell"),
                                       leverage=lev, reduce_only=False)
                if str(r.get("code","")) == "00000":
                    _mark_done("entry", pkey); _arm_entry_guard(key)
                    send_telegram(f"🔁 ENTRY 재시도 성공 {side.upper()} {sym}")
                elif str(r.get("code","")).startswith("LOCAL_MIN_QTY"):
                    _mark_done("entry", pkey, "(minQty)")

            # CLOSE
            with _PENDING_LOCK:
                cs = list(_PENDING["close"].items())
            for pkey, item in cs:
                sym, side = item["symbol"], item["side"]
                key = _key(sym, side)
                p = _get_remote(sym, side)
                if not p or float(p.get("size",0))<=0:
                    _mark_done("close", pkey, "(no-remote)"); continue
                ok = _sweep_full_close(sym, side)
                if ok:
                    with _POS_LOCK: position_data.pop(key, None)
                    _mark_done("close", pkey); 
                    with _ENTRY_GUARD_LOCK: _ENTRY_GUARD.pop(key, None)
                    send_telegram(f"🔁 CLOSE 재시도 성공 {side.upper()} {sym}")
        except Exception as e:
            print("reconciler error:", e)

def start_watchdogs():
    threading.Thread(target=_watchdog_loop, daemon=True).start()
def start_reconciler():
    threading.Thread(target=_reconciler_loop, daemon=True).start()
