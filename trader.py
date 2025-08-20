# trader.py – 실행/복구 레이어 (patched: 실패 사유 텔레그램 알림 강화)
import os, time, threading
from typing import Dict, Optional

from bitget_api import (
    convert_symbol, get_last_price, get_open_positions,
    place_market_order, place_reduce_by_size, get_symbol_spec, round_down_step,
)

try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str):
        print("[TG]", msg)

LEVERAGE = float(os.getenv("LEVERAGE", "5"))

TP1_PCT = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT = float(os.getenv("TP2_PCT", "0.40"))
TP3_PCT = float(os.getenv("TP3_PCT", "0.30"))

STOP_PCT           = float(os.getenv("STOP_PCT", "0.10"))
STOP_CHECK_SEC     = float(os.getenv("STOP_CHECK_SEC", "1.0"))
STOP_COOLDOWN_SEC  = float(os.getenv("STOP_COOLDOWN_SEC", "5.0"))

RECON_INTERVAL_SEC = float(os.getenv("RECON_INTERVAL_SEC", "60"))
TP_EPSILON_RATIO   = float(os.getenv("TP_EPSILON_RATIO", "0.001"))
RECON_DEBUG        = os.getenv("RECON_DEBUG", "0") == "1"

ENTRY_GUARD_SEC = float(os.getenv("ENTRY_GUARD_SEC", "45"))

# ── 슈퍼바이저 알림 ON/OFF ───────────────────────────────────
SUP_NOTIFY = os.getenv("SUP_NOTIFY", "0") == "1"
SUP_NOTIFY_MIN_INTERVAL = float(os.getenv("SUP_NOTIFY_MIN_INTERVAL", "600"))  # 10분

# ── state/locks ───────────────────────────────────────────────
position_data: Dict[str, dict] = {}
_POS_LOCK = threading.RLock()

_KEY_LOCKS: Dict[str, threading.RLock] = {}
_KEY_LOCKS_LOCK = threading.Lock()

def _key(symbol: str, side: str) -> str:
    return f"{symbol}_{side}"

def _lock_for(key: str):
    with _KEY_LOCKS_LOCK:
        if key not in _KEY_LOCKS:
            _KEY_LOCKS[key] = threading.RLock()
    return _KEY_LOCKS[key]

_STOP_FIRED: Dict[str, float] = {}
_STOP_LOCK = threading.Lock()

def _should_fire_stop(key: str) -> bool:
    now = time.time()
    with _STOP_LOCK:
        last = _STOP_FIRED.get(key, 0.0)
        if now - last < STOP_COOLDOWN_SEC:
            return False
        _STOP_FIRED[key] = now
        return True

_PENDING = {"entry": {}, "close": {}, "tp": {}}
_PENDING_LOCK = threading.RLock()

def _pending_key_entry(symbol: str, side: str) -> str: return f"{_key(symbol, side)}:entry"
def _pending_key_close(symbol: str, side: str) -> str: return f"{_key(symbol, side)}:close"
def _pending_key_tp3(symbol: str, side: str) -> str:   return f"{_key(symbol, side)}:tp3"

def _mark_done(typ: str, pkey: str, note: str = ""):
    with _PENDING_LOCK:
        _PENDING[typ].pop(pkey, None)
    if RECON_DEBUG and note:
        send_telegram(f"✅ pending done [{typ}] {pkey} {note}")

def get_pending_snapshot() -> Dict[str, Dict]:
    with _PENDING_LOCK:
        return {
            "counts": {k: len(v) for k, v in _PENDING.items()},
            "entry_keys": list(_PENDING["entry"].keys()),
            "close_keys": list(_PENDING["close"].keys()),
            "tp_keys": list(_PENDING["tp"].keys()),
            "interval": RECON_INTERVAL_SEC,
            "debug": RECON_DEBUG,
        }

# ── helpers ───────────────────────────────────────────────────
def _get_remote(symbol: str, side: Optional[str] = None):
    symbol = convert_symbol(symbol)
    arr = get_open_positions()
    for p in arr:
        if p.get("symbol") == symbol and (side is None or p.get("side") == side):
            return p
    return None

def _get_remote_any_side(symbol: str):
    symbol = convert_symbol(symbol)
    arr = get_open_positions()
    for p in arr:
        if p.get("symbol") == symbol and float(p.get("size") or 0) > 0:
            return p
    return None

def _pnl_usdt(entry: float, exit: float, notional: float, side: str) -> float:
    pct = (exit - entry) / entry if side == "long" else (entry - exit) / entry
    return notional * pct

def _loss_ratio_on_margin(entry: float, last: float, size: float, side: str, leverage: float) -> float:
    notional = entry * size
    pnl = _pnl_usdt(entry, last, notional, side)
    margin = max(1e-9, notional / max(1.0, leverage))
    return max(0.0, -pnl) / margin

# ── entry guard & local hint ──────────────────────────────────
_ENTRY_GUARD: Dict[str, float] = {}
_ENTRY_GUARD_LOCK = threading.Lock()

def _recent_entry_guard_hit(key: str) -> bool:
    now = time.time()
    with _ENTRY_GUARD_LOCK:
        ts = _ENTRY_GUARD.get(key, 0.0)
        return (now - ts) < ENTRY_GUARD_SEC

def _recent_entry_guard_set(key: str):
    with _ENTRY_GUARD_LOCK:
        _ENTRY_GUARD[key] = time.time()

def _has_local_position(symbol: str, within_sec: float = 180.0) -> bool:
    symbol = convert_symbol(symbol)
    now = time.time()
    with _POS_LOCK:
        for v in position_data.values():
            if v.get("symbol") == symbol and (now - float(v.get("ts", 0))) <= within_sec:
                return True
    return False

# ── trading ops ───────────────────────────────────────────────
def enter_position(symbol: str, usdt_amount: float, side: str = "long", leverage: float = None):
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    key    = _key(symbol, side)
    lev    = float(leverage or LEVERAGE)
    pkey   = _pending_key_entry(symbol, side)

    with _PENDING_LOCK:
        _PENDING["entry"][pkey] = {"symbol": symbol, "side": side, "amount": usdt_amount,
                                   "leverage": lev, "created": time.time(), "last_try": 0.0, "attempts": 0}
    if RECON_DEBUG:
        send_telegram(f"📌 pending add [entry] {pkey}")

    with _lock_for(key):
        if _recent_entry_guard_hit(key) or _has_local_position(symbol):
            _mark_done("entry", pkey, "(local/guard)")
            if RECON_DEBUG:
                send_telegram(f"⏭️ ENTRY 스킵(guard/local) {key}")
            return

        if _get_remote_any_side(symbol):
            _mark_done("entry", pkey, "(exists)")
            return

        last = get_last_price(symbol)
        if not last or last <= 0:
            send_telegram(f"❌ ENTRY 실패 {symbol} {side} → ticker unavailable")
            return

        resp = place_market_order(symbol, usdt_amount,
                                  side=("buy" if side == "long" else "sell"),
                                  leverage=lev, reduce_only=False)
        code = str(resp.get("code", ""))
        if code == "00000":
            with _POS_LOCK:
                position_data[key] = {"symbol": symbol, "side": side, "entry_usd": usdt_amount, "ts": time.time()}
            with _STOP_LOCK:
                _STOP_FIRED.pop(key, None)
            _recent_entry_guard_set(key)
            _mark_done("entry", pkey)
            send_telegram(f"🚀 ENTRY {side.upper()} {symbol}\n• Notional≈ {usdt_amount} USDT\n• Lvg: {lev}x")
        else:
            # 실패 이유를 즉시 노출
            send_telegram(f"❌ ENTRY 실패 {symbol} {side} → {resp}")

def _sweep_full_close(symbol: str, side: str, reason: str, max_retry: int = 5, sleep_s: float = 0.3):
    for _ in range(max_retry):
        p = _get_remote(symbol, side)
        size = float(p["size"]) if p and p.get("size") else 0.0
        if size <= 0:
            return True
        place_reduce_by_size(symbol, size, side)
        time.sleep(sleep_s)
    p = _get_remote(symbol, side)
    return (not p) or float(p.get("size", 0)) <= 0

def take_partial_profit(symbol: str, pct: float, side: str = "long"):
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    key    = _key(symbol, side)

    with _lock_for(key):
        p = _get_remote(symbol, side)
        if not p or float(p.get("size", 0)) <= 0:
            send_telegram(f"⚠️ TP 스킵: 원격 포지션 없음 {_key(symbol, side)}")
            return

        size_step = float(get_symbol_spec(symbol).get("sizeStep", 0.001))
        cur_size  = float(p["size"])
        cut_size  = round_down_step(cur_size * float(pct), size_step)
        if cut_size <= 0:
            send_telegram(f"⚠️ TP 스킵: 계산된 사이즈=0 ({_key(symbol, side)})")
            return

        # TP3 보정 페일오버 큐
        if abs(float(pct) - TP3_PCT) <= 1e-6:
            with _PENDING_LOCK:
                pk = _pending_key_tp3(symbol, side)
                _PENDING["tp"][pk] = {
                    "symbol": symbol, "side": side, "stage": 3, "pct": float(pct),
                    "init_size": cur_size, "cut_size": cut_size, "size_step": size_step,
                    "created": time.time(), "last_try": 0.0, "attempts": 0,
                }
            if RECON_DEBUG:
                send_telegram(f"📌 pending add [tp] {_pending_key_tp3(symbol, side)}")

        resp = place_reduce_by_size(symbol, cut_size, side)
        exit_price = get_last_price(symbol) or float(p.get("entry_price", 0))
        if str(resp.get("code", "")) == "00000":
            entry = float(p.get("entry_price", 0))
            realized = _pnl_usdt(entry, exit_price, entry * cut_size, side)
            send_telegram(
                f"🤑 TP {int(pct*100)}% {side.upper()} {symbol}\n"
                f"• Exit: {exit_price}\n• Cut size: {cut_size}\n• Realized≈ {realized:+.2f} USDT"
            )
        else:
            send_telegram(f"❌ TP 실패 {side.upper()} {symbol} → {resp}")

def close_position(symbol: str, side: str = "long", reason: str = "manual"):
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    key    = _key(symbol, side)
    pkey   = _pending_key_close(symbol, side)

    with _PENDING_LOCK:
        _PENDING["close"][pkey] = {"symbol": symbol, "side": side, "reason": reason,
                                   "created": time.time(), "last_try": 0.0, "attempts": 0}
    if RECON_DEBUG:
        send_telegram(f"📌 pending add [close] {pkey}")

    with _lock_for(key):
        p = None
        for _ in range(3):
            p = _get_remote(symbol, side)
            if p and float(p.get("size", 0)) > 0:
                break
            time.sleep(0.15)

        if not p or float(p.get("size", 0)) <= 0:
            with _POS_LOCK:
                position_data.pop(key, None)
            _mark_done("close", pkey, "(no-remote)")
            send_telegram(f"⚠️ CLOSE 스킵: 원격 포지션 없음 {key} ({reason})")
            return

        size = float(p["size"])
        resp = place_reduce_by_size(symbol, size, side)
        exit_price = get_last_price(symbol) or float(p.get("entry_price", 0))
        success = str(resp.get("code", "")) == "00000"
        ok = _sweep_full_close(symbol, side, reason) if success else False

        if success or ok:
            entry = float(p.get("entry_price", 0))
            realized = _pnl_usdt(entry, exit_price, entry * size, side)
            with _POS_LOCK:
                position_data.pop(key, None)
            _mark_done("close", pkey)
            send_telegram(
                f"✅ CLOSE {side.upper()} {symbol} ({reason})\n"
                f"• Exit: {exit_price}\n• Size: {size}\n• Realized≈ {realized:+.2f} USDT"
            )
        else:
            send_telegram(f"❌ CLOSE 실패 {side.upper()} {symbol}({reason}) → {resp}")

def reduce_by_contracts(symbol: str, contracts: float, side: str = "long"):
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    key    = _key(symbol, side)
    with _lock_for(key):
        step = float(get_symbol_spec(symbol).get("sizeStep", 0.001))
        qty  = round_down_step(float(contracts), step)
        if qty <= 0:
            send_telegram(f"⚠️ reduceByContracts 스킵: step 미달 {key}")
            return
        resp = place_reduce_by_size(symbol, qty, side)
        if str(resp.get("code", "")) == "00000":
            send_telegram(f"🔻 Reduce {qty} {side.upper()} {symbol}")
        else:
            send_telegram(f"❌ Reduce 실패 {key} → {resp}")

# ── 워치독 / 리컨실러 / 슈퍼바이저 ────────────────────────────
HEARTBEAT = {"watchdog": 0.0, "reconciler": 0.0}
_THREAD   = {"watchdog": None, "reconciler": None}
_STUCK_CNT = {"watchdog": 0, "reconciler": 0}
_LAST_NOTIFY: Dict[str, float] = {"watchdog": 0.0, "reconciler": 0.0}

def _maybe_notify(kind: str, msg: str):
    if not SUP_NOTIFY:
        return
    now = time.time()
    if now - _LAST_NOTIFY.get(kind, 0.0) >= SUP_NOTIFY_MIN_INTERVAL:
        _LAST_NOTIFY[kind] = now
        try: send_telegram(msg)
        except Exception: pass

def _watchdog_loop():
    while True:
        HEARTBEAT["watchdog"] = time.time()
        try:
            for p in get_open_positions():
                symbol = p.get("symbol"); side = p.get("side")
                entry  = float(p.get("entry_price") or 0)
                size   = float(p.get("size") or 0)
                if not symbol or not side or entry <= 0 or size <= 0:
                    continue
                last = get_last_price(symbol)
                if not last:
                    continue
                loss_ratio = _loss_ratio_on_margin(entry, last, size, side, leverage=LEVERAGE)
                if loss_ratio >= STOP_PCT:
                    k = _key(symbol, side)
                    if _should_fire_stop(k):
                        send_telegram(f"⛔ {symbol} {side.upper()} emergencyStop PnL≤-{int(STOP_PCT*100)}%")
                        close_position(symbol, side=side, reason="emergencyStop")
        except Exception as e:
            print("watchdog error:", e)
        time.sleep(STOP_CHECK_SEC)

def _reconciler_loop():
    while True:
        HEARTBEAT["reconciler"] = time.time()
        time.sleep(RECON_INTERVAL_SEC)
        try:
            # ENTRY 재시도
            with _PENDING_LOCK:
                entry_items = list(_PENDING["entry"].items())
            for pkey, item in entry_items:
                sym, side = item["symbol"], item["side"]
                key = _key(sym, side)

                if _recent_entry_guard_hit(key) or _has_local_position(sym):
                    _mark_done("entry", pkey, "(guard)")
                    if RECON_DEBUG:
                        send_telegram(f"⏭️ retry skip [entry] {pkey} (recent guard/local)")
                    continue

                if _get_remote_any_side(sym):
                    _mark_done("entry", pkey, "(exists)")
                    continue

                with _lock_for(key):
                    now = time.time()
                    if now - item.get("last_try", 0.0) < RECON_INTERVAL_SEC - 1:
                        continue
                    amt, lev = item["amount"], item["leverage"]
                    if RECON_DEBUG:
                        send_telegram(f"🔁 retry [entry] {pkey}")
                    resp = place_market_order(sym, amt,
                                              side=("buy" if side == "long" else "sell"),
                                              leverage=lev, reduce_only=False)
                    item["last_try"] = now
                    item["attempts"] = item.get("attempts", 0) + 1
                    code = str(resp.get("code", ""))
                    if code == "00000":
                        _recent_entry_guard_set(key)
                        _mark_done("entry", pkey)
                        send_telegram(f"🔁 ENTRY 재시도 성공 {side.upper()} {sym}")
                    else:
                        send_telegram(f"❌ ENTRY 재시도 실패 {side.upper()} {sym} → {resp}")

            # CLOSE 재시도
            with _PENDING_LOCK:
                close_items = list(_PENDING["close"].items())
            for pkey, item in close_items:
                sym, side = item["symbol"], item["side"]
                key = _key(sym, side)
                p = _get_remote(sym, side)
                if not p or float(p.get("size", 0)) <= 0:
                    _mark_done("close", pkey, "(no-remote)")
                    continue
                with _lock_for(key):
                    now = time.time()
                    if now - item.get("last_try", 0.0) < RECON_INTERVAL_SEC - 1:
                        continue
                    if RECON_DEBUG:
                        send_telegram(f"🔁 retry [close] {pkey}")
                    size = float(p["size"])
                    resp = place_reduce_by_size(sym, size, side)
                    item["last_try"] = now
                    item["attempts"] = item.get("attempts", 0) + 1
                    if str(resp.get("code", "")) == "00000":
                        ok = _sweep_full_close(sym, side, "reconcile")
                        if ok:
                            _mark_done("close", pkey)
                            send_telegram(f"🔁 CLOSE 재시도 성공 {side.upper()} {sym}")
                    else:
                        send_telegram(f"❌ CLOSE 재시도 실패 {side.upper()} {sym} → {resp}")

            # TP3 재시도
            with _PENDING_LOCK:
                tp_items = list(_PENDING["tp"].items())
            for pkey, item in tp_items:
                sym, side = item["symbol"], item["side"]
                key = _key(sym, side)
                p = _get_remote(sym, side)
                if not p or float(p.get("size", 0)) <= 0:
                    _mark_done("tp", pkey, "(no-remote)")
                    continue

                cur_size  = float(p["size"])
                init_size = float(item.get("init_size") or cur_size)
                cut_size  = float(item["cut_size"])
                size_step = float(item.get("size_step", 0.001))
                achieved  = max(0.0, init_size - cur_size)
                eps = max(size_step * 2.0, init_size * TP_EPSILON_RATIO)
                if achieved + eps >= cut_size:
                    _mark_done("tp", pkey)
                    continue
                remain = round_down_step(cut_size - achieved, size_step)
                if remain <= 0:
                    _mark_done("tp", pkey)
                    continue

                with _lock_for(key):
                    now = time.time()
                    if now - item.get("last_try", 0.0) < RECON_INTERVAL_SEC - 1:
                        continue
                    if RECON_DEBUG:
                        send_telegram(f"🔁 retry [tp3] {pkey} remain≈{remain}")
                    resp = place_reduce_by_size(sym, remain, side)
                    item["last_try"] = now
                    item["attempts"] = item.get("attempts", 0) + 1
                    if str(resp.get("code", "")) == "00000":
                        send_telegram(f"🔁 TP3 재시도 감축 {side.upper()} {sym} remain≈{remain}")
                    else:
                        send_telegram(f"❌ TP3 재시도 실패 {side.upper()} {sym} → {resp}")
        except Exception as e:
            print("reconciler error:", e)

# ── 슈퍼바이저 ────────────────────────────────────────────────
SUP_INTERVAL_SEC = float(os.getenv("SUP_INTERVAL_SEC", "5"))
WD_STUCK_SEC     = float(os.getenv("WD_STUCK_SEC", str(max(15.0, STOP_CHECK_SEC*30))))
REC_STUCK_SEC    = float(os.getenv("REC_STUCK_SEC", str(max(60.0, RECON_INTERVAL_SEC*3))))

def _supervisor_loop():
    while True:
        try:
            now = time.time()
            tw = _THREAD.get("watchdog")
            w_stale = (now - HEARTBEAT["watchdog"]) > WD_STUCK_SEC
            if (tw is None) or (not tw.is_alive()) or w_stale:
                _STUCK_CNT["watchdog"] += 1 if w_stale else 0
                if (tw is None) or (not tw.is_alive()) or _STUCK_CNT["watchdog"] >= 2:
                    _maybe_notify("watchdog", "♻️ restarting watchdog")
                    start_watchdogs()
                    _STUCK_CNT["watchdog"] = 0
            else:
                _STUCK_CNT["watchdog"] = 0

            tr = _THREAD.get("reconciler")
            r_stale = (now - HEARTBEAT["reconciler"]) > REC_STUCK_SEC
            if (tr is None) or (not tr.is_alive()) or r_stale:
                _STUCK_CNT["reconciler"] += 1 if r_stale else 0
                if (tr is None) or (not tr.is_alive()) or _STUCK_CNT["reconciler"] >= 2:
                    _maybe_notify("reconciler", "♻️ restarting reconciler")
                    start_reconciler()
                    _STUCK_CNT["reconciler"] = 0
            else:
                _STUCK_CNT["reconciler"] = 0

        except Exception as e:
            print("supervisor error:", e)
        time.sleep(SUP_INTERVAL_SEC)

def start_watchdogs():
    t = threading.Thread(target=_watchdog_loop, name="emergency-stop-watchdog", daemon=True)
    t.start()
    _THREAD["watchdog"] = t
    HEARTBEAT["watchdog"] = time.time()

def start_reconciler():
    t = threading.Thread(target=_reconciler_loop, name="reconciler", daemon=True)
    t.start()
    _THREAD["reconciler"] = t
    HEARTBEAT["reconciler"] = time.time()

def start_supervisor():
    threading.Thread(target=_supervisor_loop, name="supervisor", daemon=True).start()
