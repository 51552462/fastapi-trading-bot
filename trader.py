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
TRACE_LOG = os.getenv("TRACE_LOG", "0") == "1"

TP1_PCT = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT = float(os.getenv("TP2_PCT", "0.40"))
TP3_PCT = float(os.getenv("TP3_PCT", "0.30"))

STOP_PCT           = float(os.getenv("STOP_PCT", "0.10"))
STOP_CHECK_SEC     = float(os.getenv("STOP_CHECK_SEC", "1.0"))
STOP_COOLDOWN_SEC  = float(os.getenv("STOP_COOLDOWN_SEC", "5.0"))

RECON_INTERVAL_SEC = float(os.getenv("RECON_INTERVAL_SEC", "40"))
TP_EPSILON_RATIO   = float(os.getenv("TP_EPSILON_RATIO", "0.001"))
RECON_DEBUG        = os.getenv("RECON_DEBUG", "0") == "1"

MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "40"))  # 전체 포지션 한도(숏 제한 판단에 사용)
CAP_CHECK_SEC      = float(os.getenv("CAP_CHECK_SEC", "10"))
LONG_BYPASS_CAP    = os.getenv("LONG_BYPASS_CAP", "1") == "1"    # 롱은 cap 우회(무제한)

ENTRY_INFLIGHT_TTL_SEC = float(os.getenv("ENTRY_INFLIGHT_TTL_SEC", "30"))
ENTRY_DUP_TTL_SEC      = float(os.getenv("ENTRY_DUP_TTL_SEC", "60"))

# ── capacity(state) : 의도대로 "전체 카운트로 숏만 제한" ─────────────────
_CAPACITY = {
    "blocked": False,        # 하위호환
    "last_count": 0,         # 전체 포지션 수
    "short_blocked": False,  # total>=cap 이면 True
    "short_count": 0,        # 표시용(=total)
    "ts": 0.0
}
_CAP_LOCK = threading.Lock()

# ── local state & locks ───────────────────────────────────────
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

def _local_open_count() -> int:
    with _POS_LOCK:
        return len(position_data)

def _local_has_any(symbol: str) -> bool:
    symbol = convert_symbol(symbol)
    with _POS_LOCK:
        for k in position_data.keys():
            if k.startswith(symbol + "_"):
                return True
    return False

# ── stop 쿨다운 ───────────────────────────────────────────────
_STOP_FIRED: Dict[str, float] = {}
_STOP_LOCK = threading.Lock()
def _should_fire_stop(key: str) -> bool:
    now = time.time()
    with _STOP_LOCK:
        last = _STOP_FIRED.get(key, 0.0)
        if now - last < STOP_COOLDOWN_SEC: return False
        _STOP_FIRED[key] = now; return True

# ── pending registry ──────────────────────────────────────────
_PENDING = {"entry": {}, "close": {}, "tp": {}}
_PENDING_LOCK = threading.RLock()

def _pending_key_entry(symbol: str, side: str) -> str: return f"{_key(symbol, side)}:entry"
def _pending_key_close(symbol: str, side: str) -> str: return f"{_key(symbol, side)}:close"
def _pending_key_tp3(symbol: str, side: str)   -> str: return f"{_key(symbol, side)}:tp3"

def _mark_done(typ: str, pkey: str, note: str = ""):
    with _PENDING_LOCK:
        _PENDING.get(typ, {}).pop(pkey, None)
    if RECON_DEBUG and note: send_telegram(f"✅ pending done [{typ}] {pkey} {note}")

def get_pending_snapshot() -> Dict[str, Dict]:
    with _PENDING_LOCK, _CAP_LOCK, _POS_LOCK:
        return {
            "counts": {k: len(v) for k, v in _PENDING.items()},
            "entry_keys": list(_PENDING["entry"].keys()),
            "close_keys": list(_PENDING["close"].keys()),
            "tp_keys": list(_PENDING["tp"].keys()),
            "interval": RECON_INTERVAL_SEC,
            "debug": RECON_DEBUG,
            "capacity": {
                "blocked": _CAPACITY["blocked"],
                "last_count": _CAPACITY["last_count"],
                "short_blocked": _CAPACITY["short_blocked"],
                "short_count": _CAPACITY["short_count"],
                "max": MAX_OPEN_POSITIONS,
                "interval": CAP_CHECK_SEC,
                "ts": _CAPACITY["ts"],
            },
            "local_keys": list(position_data.keys()),
        }

# ── remote helpers ────────────────────────────────────────────
def _get_remote(symbol: str, side: Optional[str] = None):
    symbol = convert_symbol(symbol)
    for p in get_open_positions():
        if p.get("symbol") == symbol and (side is None or p.get("side") == side):
            return p
    return None

def _get_remote_any_side(symbol: str):
    symbol = convert_symbol(symbol)
    for p in get_open_positions():
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

# ── Capacity guard: "전체 카운트로 숏만 제한" ──────────────────
def _total_open_positions_now() -> int:
    try:
        return len(get_open_positions()) + _local_open_count()
    except:
        return _local_open_count()

def capacity_status():
    with _CAP_LOCK:
        return {
            "blocked": _CAPACITY["blocked"],           # 하위호환
            "last_count": _CAPACITY["last_count"],     # total
            "short_blocked": _CAPACITY["short_blocked"],
            "short_count": _CAPACITY["short_count"],   # 표시용(=total)
            "max": MAX_OPEN_POSITIONS,
            "interval": CAP_CHECK_SEC,
            "ts": _CAPACITY["ts"],
        }

def can_enter_now(side: str) -> bool:
    if side == "long" and LONG_BYPASS_CAP:
        return True
    with _CAP_LOCK:
        return not _CAPACITY["short_blocked"]

def _capacity_loop():
    prev_blocked = None
    while True:
        try:
            total_count = _total_open_positions_now()
            short_blocked = total_count >= MAX_OPEN_POSITIONS
            now = time.time()
            with _CAP_LOCK:
                _CAPACITY["short_blocked"] = short_blocked
                _CAPACITY["short_count"]   = total_count  # 표시용
                _CAPACITY["last_count"]    = total_count
                _CAPACITY["blocked"]       = short_blocked
                _CAPACITY["ts"]            = now
            if prev_blocked is None or prev_blocked != short_blocked:
                state = "BLOCKED (total>=cap)" if short_blocked else "UNBLOCKED (total<cap)"
                try: send_telegram(f"ℹ️ Capacity {state} | {total_count}/{MAX_OPEN_POSITIONS}")
                except: pass
                prev_blocked = short_blocked
        except Exception as e:
            print("capacity guard error:", e)
        time.sleep(CAP_CHECK_SEC)

def start_capacity_guard():
    threading.Thread(target=_capacity_loop, name="capacity-guard", daemon=True).start()

# ── STRICT gate(실시간 예약) : 숏만 차단, 기준은 "전체 카운트" ─────────
_RESERVE = {"short": 0}
_RES_LOCK = threading.Lock()
def _strict_try_reserve(side: str) -> bool:
    if side == "long" and LONG_BYPASS_CAP:
        return True
    total = _total_open_positions_now()
    with _RES_LOCK:
        effective = total + _RESERVE["short"]
        if effective >= MAX_OPEN_POSITIONS:
            return False
        _RESERVE["short"] += 1
        return True

def _strict_release(side: str):
    if side == "long" and LONG_BYPASS_CAP:
        return
    with _RES_LOCK:
        if _RESERVE["short"] > 0:
            _RESERVE["short"] -= 1

# ── 중복 진입 가드 ────────────────────────────────────────────
_ENTRY_BUSY: Dict[str, float] = {}
_RECENT_OK: Dict[str, float]  = {}
_ENTRY_G_LOCK = threading.Lock()
def _set_busy(key: str):
    with _ENTRY_G_LOCK: _ENTRY_BUSY[key] = time.time()
def _clear_busy(key: str):
    with _ENTRY_G_LOCK: _ENTRY_BUSY.pop(key, None)
def _is_busy(key: str) -> bool:
    with _ENTRY_G_LOCK:
        ts = _ENTRY_BUSY.get(key, 0.0)
    return (time.time() - ts) < ENTRY_INFLIGHT_TTL_SEC
def _mark_recent_ok(key: str):
    with _ENTRY_G_LOCK: _RECENT_OK[key] = time.time()
def _recent_ok(key: str) -> bool:
    with _ENTRY_G_LOCK:
        ts = _RECENT_OK.get(key, 0.0)
    return (time.time() - ts) < ENTRY_DUP_TTL_SEC

# ── trading ops ───────────────────────────────────────────────
def enter_position(symbol: str, usdt_amount: float, side: str = "long", leverage: float = None):
    symbol = convert_symbol(symbol); side = (side or "long").lower()
    key = _key(symbol, side); lev = float(leverage or LEVERAGE)
    pkey = _pending_key_entry(symbol, side)
    trace = os.getenv("CURRENT_TRACE_ID", "")

    if TRACE_LOG:
        send_telegram(f"🔎 ENTRY request trace={trace} {symbol} {side} amt={usdt_amount}")

    if _is_busy(key) or _recent_ok(key):
        if RECON_DEBUG: send_telegram(f"⏸️ skip entry (busy/recent) {key}")
        return

    if not _strict_try_reserve(side):
        st = capacity_status()
        send_telegram(f"🧱 STRICT HOLD {symbol} {side} {st['last_count']}/{MAX_OPEN_POSITIONS}")
        return
    try:
        if not can_enter_now(side):
            st = capacity_status()
            send_telegram(f"⏳ ENTRY HOLD (periodic) {symbol} {side} {st['last_count']}/{MAX_OPEN_POSITIONS}")
            return

        with _PENDING_LOCK:
            _PENDING["entry"][pkey] = {"symbol": symbol, "side": side, "amount": usdt_amount,
                                       "leverage": lev, "created": time.time(), "last_try": 0.0, "attempts": 0}
        if RECON_DEBUG: send_telegram(f"📌 pending add [entry] {pkey}")

        with _lock_for(key):
            if _local_has_any(symbol) or _get_remote_any_side(symbol) or _recent_ok(key):
                _mark_done("entry", pkey, "(exists/recent)"); return

            _set_busy(key)

            last = get_last_price(symbol)
            if not last or last <= 0:
                if TRACE_LOG: send_telegram(f"❗ ticker_fail {symbol} trace={trace}")
                return  # 리컨실러가 재시도

            resp = place_market_order(symbol, usdt_amount,
                                      side=("buy" if side == "long" else "sell"),
                                      leverage=lev, reduce_only=False)
            code = str(resp.get("code", ""))
            if TRACE_LOG: send_telegram(f"📦 order_resp code={code} {symbol} {side} trace={trace}")

            if code == "00000":
                with _POS_LOCK:
                    position_data[key] = {"symbol": symbol, "side": side, "entry_usd": usdt_amount, "ts": time.time()}
                with _STOP_LOCK: _STOP_FIRED.pop(key, None)
                _mark_done("entry", pkey)
                _mark_recent_ok(key)
                send_telegram(f"🚀 ENTRY {side.upper()} {symbol}\n• Notional≈ {usdt_amount} USDT\n• Lvg: {lev}x")
            elif code.startswith("LOCAL_MIN_QTY") or code.startswith("LOCAL_BAD_QTY"):
                _mark_done("entry", pkey, "(minQty/badQty)")
                send_telegram(f"⛔ ENTRY 스킵 {symbol} {side} → {resp}")
            else:
                if TRACE_LOG: send_telegram(f"❌ order_fail resp={resp} trace={trace}")
    finally:
        _clear_busy(key)
        _strict_release(side)

def _sweep_full_close(symbol: str, side: str, reason: str, max_retry: int = 5, sleep_s: float = 0.3):
    for _ in range(max_retry):
        p = _get_remote(symbol, side); size = float(p["size"]) if p and p.get("size") else 0.0
        if size <= 0: return True
        place_reduce_by_size(symbol, size, side); time.sleep(sleep_s)
    p = _get_remote(symbol, side)
    return (not p) or float(p.get("size", 0)) <= 0

# ── Breakeven (TP1/TP2 이후 본절 도달 시 전체 종료) ────────────
BE_ENABLE        = os.getenv("BE_ENABLE", "1") == "1"
BE_AFTER_STAGE   = int(os.getenv("BE_AFTER_STAGE", "1"))
BE_EPSILON_RATIO = float(os.getenv("BE_EPSILON_RATIO", "0.0005"))

def take_partial_profit(symbol: str, pct: float, side: str = "long"):
    symbol = convert_symbol(symbol); side = (side or "long").lower()
    key = _key(symbol, side)

    with _lock_for(key):
        p = _get_remote(symbol, side)
        if not p or float(p.get("size", 0)) <= 0:
            send_telegram(f"⚠️ TP 스킵: 원격 포지션 없음 {_key(symbol, side)}"); return

        size_step = float(get_symbol_spec(symbol).get("sizeStep", 0.001))
        cur_size  = float(p["size"])
        cut_size  = round_down_step(cur_size * float(pct), size_step)
        if cut_size <= 0:
            send_telegram(f"⚠️ TP 스킵: 계산된 사이즈=0 ({_key(symbol, side)})"); return

        if abs(float(pct) - TP3_PCT) <= 1e-6:
            with _PENDING_LOCK:
                pk = _pending_key_tp3(symbol, side)
                _PENDING["tp"][pk] = {
                    "symbol": symbol, "side": side, "stage": 3, "pct": float(pct),
                    "init_size": cur_size, "cut_size": cut_size, "size_step": size_step,
                    "created": time.time(), "last_try": 0.0, "attempts": 0,
                }
            if RECON_DEBUG: send_telegram(f"📌 pending add [tp] {pk}")

        resp = place_reduce_by_size(symbol, cut_size, side)
        exit_price = get_last_price(symbol) or float(p.get("entry_price", 0))
        if str(resp.get("code", "")) == "00000":
            entry = float(p.get("entry_price", 0))
            realized = _pnl_usdt(entry, exit_price, entry * cut_size, side)
            send_telegram(
                f"🤑 TP {int(pct*100)}% {side.upper()} {symbol}\n"
                f"• Exit: {exit_price}\n• Cut size: {cut_size}\n• Realized≈ {realized:+.2f} USDT"
            )
            try:
                stage = 1 if abs(float(pct) - TP1_PCT) <= 1e-6 else (2 if abs(float(pct) - TP2_PCT) <= 1e-6 else 0)
                if BE_ENABLE and stage in (1, 2) and stage >= BE_AFTER_STAGE:
                    profited = (exit_price > entry) if side == "long" else (exit_price < entry)
                    if profited:
                        with _POS_LOCK:
                            st = position_data.get(key, {}) or {}
                            st.update({"be_armed": True, "be_entry": entry, "be_from_stage": stage})
                            position_data[key] = st
                        send_telegram(f"🧷 Breakeven ARMED at entry≈{entry} ({symbol} {side}, from TP{stage})")
            except: pass

def close_position(symbol: str, side: str = "long", reason: str = "manual"):
    symbol = convert_symbol(symbol); side = (side or "long").lower()
    key = _key(symbol, side); pkey = _pending_key_close(symbol, side)

    with _PENDING_LOCK:
        _PENDING["close"][pkey] = {"symbol": symbol, "side": side, "reason": reason,
                                   "created": time.time(), "last_try": 0.0, "attempts": 0}
    if RECON_DEBUG: send_telegram(f"📌 pending add [close] {pkey}")

    with _lock_for(key):
        p = None
        for _ in range(3):
            p = _get_remote(symbol, side)
            if p and float(p.get("size", 0)) > 0: break
            time.sleep(0.15)

        if not p or float(p.get("size", 0)) <= 0:
            with _POS_LOCK: position_data.pop(key, None)
            _mark_done("close", pkey, "(no-remote)")
            send_telegram(f"⚠️ CLOSE 스킵: 원격 포지션 없음 {key} ({reason})")
            return

        size = float(p["size"])
        resp = place_reduce_by_size(symbol, size, side)
        exit_price = get_last_price(symbol) or float(p.get("entry_price", 0))
        success = str(resp.get("code", "")) == "00000"
        ok = _sweep_full_close(symbol, side, "reconcile") if success else False

        if success or ok:
            entry = float(p.get("entry_price", 0))
            realized = _pnl_usdt(entry, exit_price, entry * size, side)
            with _POS_LOCK: position_data.pop(key, None)
            _mark_done("close", pkey)
            send_telegram(
                f"✅ CLOSE {side.upper()} {symbol} ({reason})\n"
                f"• Exit: {exit_price}\n• Size: {size}\n• Realized≈ {realized:+.2f} USDT"
            )
            _mark_recent_ok(key)  # 직후 중복 재진입 방지(짧게)

def reduce_by_contracts(symbol: str, contracts: float, side: str = "long"):
    symbol = convert_symbol(symbol); side = (side or "long").lower()
    key = _key(symbol, side)
    with _lock_for(key):
        step = float(get_symbol_spec(symbol).get("sizeStep", 0.001))
        qty  = round_down_step(float(contracts), step)
        if qty <= 0:
            send_telegram(f"⚠️ reduceByContracts 스킵: step 미달 {key}"); return
        resp = place_reduce_by_size(symbol, qty, side)
        if str(resp.get("code", "")) == "00000":
            send_telegram(f"🔻 Reduce {qty} {side.upper()} {symbol}")
        else:
            send_telegram(f"❌ Reduce 실패 {key} → {resp}")

# ── watchdogs ─────────────────────────────────────────────────
def _watchdog_loop():
    while True:
        try:
            for p in get_open_positions():
                symbol = p.get("symbol"); side = (p.get("side") or "").lower()
                entry  = float(p.get("entry_price") or 0); size = float(p.get("size") or 0)
                if not symbol or side not in ("long","short") or entry <= 0 or size <= 0: continue
                last = get_last_price(symbol)
                if not last: continue
                loss_ratio = _loss_ratio_on_margin(entry, last, size, side, leverage=LEVERAGE)
                if loss_ratio >= STOP_PCT:
                    k = _key(symbol, side)
                    if _should_fire_stop(k):
                        send_telegram(f"⛔ {symbol} {side.upper()} emergencyStop PnL≤{-int(STOP_PCT*100)}%")
                        close_position(symbol, side=side, reason="emergencyStop")
        except Exception as e:
            print("watchdog error:", e)
        time.sleep(STOP_CHECK_SEC)

def _breakeven_watchdog():
    if not BE_ENABLE: return
    while True:
        try:
            for p in get_open_positions():
                symbol = p.get("symbol"); side = (p.get("side") or "").lower()
                entry  = float(p.get("entry_price") or 0); size = float(p.get("size") or 0)
                if not symbol or side not in ("long","short") or entry <= 0 or size <= 0: continue
                key = _key(symbol, side)
                with _POS_LOCK:
                    st = position_data.get(key, {}) or {}
                    be_armed = bool(st.get("be_armed")); be_entry = float(st.get("be_entry") or 0.0)
                if not (be_armed and be_entry > 0): continue
                last = get_last_price(symbol)
                if not last: continue
                eps = max(be_entry * BE_EPSILON_RATIO, 0.0)
                trigger = (last <= be_entry - eps) if side == "long" else (last >= be_entry + eps)
                if trigger:
                    send_telegram(f"🧷 Breakeven stop → CLOSE {side.upper()} {symbol} @≈{last} (entry≈{be_entry})")
                    close_position(symbol, side=side, reason="breakeven")
        except Exception as e:
            print("breakeven watchdog error:", e)
        time.sleep(0.8)

def _reconciler_loop():
    while True:
        time.sleep(RECON_INTERVAL_SEC)
        try:
            # ENTRY 재시도
            with _PENDING_LOCK:
                entry_items = list(_PENDING["entry"].items())
            for pkey, item in entry_items:
                sym, side = item["symbol"], item["side"]
                key = _key(sym, side)

                if _local_has_any(sym) or _get_remote_any_side(sym) or _recent_ok(key):
                    _mark_done("entry", pkey, "(exists/recent)"); continue

                if _is_busy(key): continue

                if not _strict_try_reserve(side):
                    if TRACE_LOG:
                        st = capacity_status()
                        send_telegram(f"⏸️ retry_hold STRICT {sym} {side} {st['last_count']}/{MAX_OPEN_POSITIONS}")
                    continue

                try:
                    if not can_enter_now(side): continue

                    with _lock_for(key):
                        now = time.time()
                        if now - item.get("last_try", 0.0) < RECON_INTERVAL_SEC - 1: continue

                        _set_busy(key)
                        amt, lev = item["amount"], item["leverage"]
                        if RECON_DEBUG or TRACE_LOG:
                            send_telegram(f"🔁 retry_entry {sym} {side} attempt={item.get('attempts',0)+1}")

                        resp = place_market_order(sym, amt,
                                                  side=("buy" if side == "long" else "sell"),
                                                  leverage=lev, reduce_only=False)
                        item["last_try"] = now; item["attempts"] = item.get("attempts", 0) + 1
                        code = str(resp.get("code", ""))
                        if code == "00000":
                            _mark_done("entry", pkey)
                            with _POS_LOCK:
                                position_data[key] = {"symbol": sym, "side": side, "entry_usd": amt, "ts": time.time()}
                            _mark_recent_ok(key)
                            send_telegram(f"🔁 ENTRY 재시도 성공 {side.upper()} {sym}")
                        elif code.startswith("LOCAL_MIN_QTY") or code.startswith("LOCAL_BAD_QTY"):
                            _mark_done("entry", pkey, "(minQty/badQty)")
                            send_telegram(f"⛔ ENTRY 재시도 스킵 {sym} {side} → {resp}")
                finally:
                    _clear_busy(key)
                    _strict_release(side)

            # CLOSE 재시도
            with _PENDING_LOCK:
                close_items = list(_PENDING["close"].items())
            for pkey, item in close_items:
                sym, side = item["symbol"], item["side"]
                key = _key(sym, side)
                p = _get_remote(sym, side)
                if not p or float(p.get("size", 0)) <= 0:
                    _mark_done("close", pkey, "(no-remote)")
                    with _POS_LOCK: position_data.pop(key, None)
                    continue
                with _lock_for(key):
                    now = time.time()
                    if now - item.get("last_try", 0.0) < RECON_INTERVAL_SEC - 1: continue
                    if RECON_DEBUG: send_telegram(f"🔁 retry [close] {pkey}")
                    size = float(p["size"])
                    resp = place_reduce_by_size(sym, size, side)
                    item["last_try"] = now; item["attempts"] = item.get("attempts", 0) + 1
                    if str(resp.get("code", "")) == "00000":
                        ok = _sweep_full_close(sym, side, "reconcile")
                        if ok:
                            _mark_done("close", pkey)
                            with _POS_LOCK: position_data.pop(key, None)
                            send_telegram(f"🔁 CLOSE 재시도 성공 {side.upper()} {sym}")

            # TP3 재시도
            with _PENDING_LOCK:
                tp_items = list(_PENDING["tp"].items())
            for pkey, item in tp_items:
                sym, side = item["symbol"], item["side"]
                key = _key(sym, side)
                p = _get_remote(sym, side)
                if not p or float(p.get("size", 0)) <= 0:
                    _mark_done("tp", pkey, "(no-remote)"); continue

                cur_size  = float(p["size"])
                init_size = float(item.get("init_size") or cur_size)
                cut_size  = float(item["cut_size"])
                size_step = float(item.get("size_step", 0.001))
                achieved  = max(0.0, init_size - cur_size)
                eps = max(size_step * 2.0, init_size * TP_EPSILON_RATIO)
                if achieved + eps >= cut_size:
                    _mark_done("tp", pkey); continue
                remain = round_down_step(cut_size - achieved, size_step)
                if remain <= 0:
                    _mark_done("tp", pkey); continue

                with _lock_for(key):
                    now = time.time()
                    if now - item.get("last_try", 0.0) < RECON_INTERVAL_SEC - 1: continue
                    if RECON_DEBUG: send_telegram(f"🔁 retry [tp3] {pkey} remain≈{remain}")
                    resp = place_reduce_by_size(sym, remain, side)
                    item["last_try"] = now; item["attempts"] = item.get("attempts", 0) + 1
                    if str(resp.get("code", "")) == "00000":
                        send_telegram(f"🔁 TP3 재시도 감축 {side.upper()} {sym} remain≈{remain}")
        except Exception as e:
            print("reconciler error:", e)

def start_capacity_guard():
    threading.Thread(target=_capacity_loop, name="capacity-guard", daemon=True).start()

def start_watchdogs():
    threading.Thread(target=_watchdog_loop, name="emergency-stop-watchdog", daemon=True).start()
    if BE_ENABLE:
        threading.Thread(target=_breakeven_watchdog, name="breakeven-watchdog", daemon=True).start()

def start_reconciler():
    threading.Thread(target=_reconciler_loop, name="reconciler", daemon=True).start()
