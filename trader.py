import os, time, threading
from typing import Dict, Optional

from bitget_api import (
    convert_symbol, get_last_price, get_open_positions,
    place_market_order, place_reduce_by_size, get_symbol_spec, round_down_step,
)

# --- Telegram ---
try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str):
        print("[TG]", msg)

# --- Adaptive filters (graceful fallback) ---
try:
    from filters.runtime_filters import evaluate_position, evaluate_position_adaptive
except Exception:
    def evaluate_position(**kwargs): return ("hold","keep")
    def evaluate_position_adaptive(**kwargs): return ("hold","keep")
try:
    from filters.adaptive_thresholds import compute as compute_adaptive
except Exception:
    def compute_adaptive(open_positions, meta_map, last_price_fn): return {}

# ===== ENV =====
LEVERAGE = float(os.getenv("LEVERAGE", "5"))
TRACE_LOG = os.getenv("TRACE_LOG", "0") == "1"

TP1_PCT = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT = float(os.getenv("TP2_PCT", "0.40"))
TP3_PCT = float(os.getenv("TP3_PCT", "0.30"))
TP_EPSILON_RATIO = float(os.getenv("TP_EPSILON_RATIO", "0.0005"))

STOP_PCT           = float(os.getenv("STOP_PCT", os.getenv("DEFAULT_STOP_PCT", "0.10")))
STOP_CHECK_SEC     = float(os.getenv("STOP_CHECK_SEC", "1.0"))
STOP_DEBOUNCE_SEC  = float(os.getenv("STOP_DEBOUNCE_SEC", "0.8"))

MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "40"))
CAP_CHECK_SEC      = float(os.getenv("CAP_CHECK_SEC", "10"))
LONG_BYPASS_CAP    = os.getenv("LONG_BYPASS_CAP", "1") == "1"
STRICT_RESERVE_DISABLE = os.getenv("STRICT_RESERVE_DISABLE", "0") == "1"

ENTRY_INFLIGHT_TTL_SEC = float(os.getenv("ENTRY_INFLIGHT_TTL_SEC", "30"))
ENTRY_DUP_TTL_SEC      = float(os.getenv("ENTRY_DUP_TTL_SEC", "60"))

BE_ENABLE        = os.getenv("BE_ENABLE", "1") == "1"
BE_AFTER_STAGE   = int(os.getenv("BE_AFTER_STAGE", "1"))
BE_EPSILON_RATIO = float(os.getenv("BE_EPSILON_RATIO", "0.0005"))

RECON_INTERVAL_SEC = float(os.getenv("RECON_INTERVAL_SEC", "25"))
RECON_DEBUG        = os.getenv("RECON_DEBUG", "0") == "1"

# ===== Capacity cache =====
_CAPACITY = {"blocked": False, "short_blocked": False, "last_count": 0, "short_count": 0, "ts": 0.0}
_CAP_LOCK = threading.Lock()

# ===== Local state =====
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
    with _POS_LOCK: return len(position_data)

def _local_has_any(symbol: str) -> bool:
    symbol = convert_symbol(symbol)
    with _POS_LOCK:
        return any(k.startswith(symbol + "_") for k in position_data.keys())

# ===== Utils =====
def _pnl_usdt(entry: float, exitp: float, notion: float, side: str) -> float:
    if entry <= 0 or notion <= 0: return 0.0
    if side == "long": return (exitp - entry) / entry * notion
    else:              return (entry - exitp) / entry * notion

def _price_move_pct(entry: float, last: float, side: str) -> float:
    """가격 변화율(손익 방향 기준, +면 이익, -면 손실)."""
    if entry <= 0: return 0.0
    raw = (last - entry) / entry
    return raw if side == "long" else -raw

def _loss_ratio_on_margin(entry: float, last: float, side: str, leverage: float) -> float:
    """
    레버리지 대비 손실률(양수=손실). 예)
      5배 레버리지, 가격 -2% 이동 → 손실률 ≈ 0.10(=10%)
    STOP_PCT(예: 0.10)와 비교해 응급정지 트리거로 사용.
    """
    move = _price_move_pct(entry, last, side)  # 이익이면 +, 손실이면 -
    loss_on_price = max(0.0, -move)           # 손실만 양수화
    return loss_on_price * float(leverage)

# ===== Capacity helpers =====
def _total_open_positions_now() -> int:
    try:
        return sum(1 for _ in get_open_positions())
    except Exception:
        return 0

def capacity_status() -> Dict[str, any]:
    with _CAP_LOCK: return dict(_CAPACITY)

def can_enter_now(side: str) -> bool:
    if STRICT_RESERVE_DISABLE: return True
    if side == "long" and LONG_BYPASS_CAP: return True
    with _CAP_LOCK: return not _CAPACITY["short_blocked"]

def _capacity_loop():
    prev = None
    while True:
        try:
            total_count = _total_open_positions_now()
            short_blocked = total_count >= MAX_OPEN_POSITIONS
            now = time.time()
            with _CAP_LOCK:
                _CAPACITY.update({
                    "blocked": short_blocked,
                    "short_blocked": short_blocked,
                    "last_count": total_count,
                    "short_count": total_count,
                    "ts": now,
                })
            if prev != short_blocked and TRACE_LOG:
                send_telegram(f"ℹ️ capacity short_blocked={short_blocked} count={total_count}/{MAX_OPEN_POSITIONS}")
            prev = short_blocked
        except Exception as e:
            print("capacity error:", e)
        time.sleep(CAP_CHECK_SEC)

def start_capacity_guard():
    threading.Thread(target=_capacity_loop, name="capacity-guard", daemon=True).start()

# ===== Strict in-flight gate (short only) =====
_RESERVE = {"short": 0}
_RES_LOCK = threading.Lock()
def _strict_try_reserve(side: str) -> bool:
    if side == "long" and LONG_BYPASS_CAP: return True
    total = _total_open_positions_now()
    with _RES_LOCK:
        effective = total + _RESERVE["short"]
        if effective >= MAX_OPEN_POSITIONS: return False
        _RESERVE["short"] += 1
        return True
def _strict_release(side: str):
    if side == "long" and LONG_BYPASS_CAP: return
    with _RES_LOCK:
        if _RESERVE["short"] > 0: _RESERVE["short"] -= 1

# ===== Entry de-dup =====
_ENTRY_BUSY: Dict[str, float] = {}
_RECENT_OK: Dict[str, float]  = {}
_ENTRY_G_LOCK = threading.Lock()
def _set_busy(key: str):  with _ENTRY_G_LOCK: _ENTRY_BUSY[key] = time.time()
def _clear_busy(key: str):  with _ENTRY_G_LOCK: _ENTRY_BUSY.pop(key, None)
def _is_busy(key: str) -> bool:
    with _ENTRY_G_LOCK: ts = _ENTRY_BUSY.get(key, 0.0)
    return (time.time() - ts) < ENTRY_INFLIGHT_TTL_SEC
def _mark_recent_ok(key: str):  with _ENTRY_G_LOCK: _RECENT_OK[key] = time.time()
def _recent_ok(key: str) -> bool:
    with _ENTRY_G_LOCK: ts = _RECENT_OK.get(key, 0.0)
    return (time.time() - ts) < ENTRY_DUP_TTL_SEC

# ===== Trading ops =====
def _pending_key_entry(symbol: str, side: str) -> str: return f"e:{symbol}:{side}:{int(time.time()*1000)}"
def _pending_key_close(symbol: str, side: str) -> str: return f"c:{symbol}:{side}:{int(time.time()*1000)}"
def _pending_key_tp(symbol: str, side: str) -> str:    return f"t:{symbol}:{side}:{int(time.time()*1000)}"

_PENDING: Dict[str, Dict[str, dict]] = {"entry": {}, "close": {}, "tp": {}}
_PENDING_LOCK = threading.RLock()
def _mark_done(kind: str, key: str, note: str = ""):
    with _PENDING_LOCK:
        _PENDING[kind].pop(key, None)

def _get_remote(symbol: str, side: Optional[str] = None):
    symbol = convert_symbol(symbol)
    for p in get_open_positions():
        if p.get("symbol") == symbol and (side is None or p.get("side") == side):
            return p
    return None
def _get_remote_any_side(symbol: str):
    symbol = convert_symbol(symbol)
    for p in get_open_positions():
        if p.get("symbol") == symbol:
            return p
    return None

def enter_position(symbol: str, usdt_amount: float, side: str = "long", leverage: float = None):
    symbol = convert_symbol(symbol); side = (side or "long").lower()
    key = _key(symbol, side); lev = float(leverage or LEVERAGE)
    pkey = _pending_key_entry(symbol, side)
    trace = os.getenv("CURRENT_TRACE_ID", "")

    if TRACE_LOG: send_telegram(f"🔎 ENTRY request trace={trace} {symbol} {side} amt={usdt_amount}")
    if _is_busy(key) or _recent_ok(key):
        if RECON_DEBUG: send_telegram(f"⏸️ skip entry (busy/recent) {key}")
        return

    # Strict in-flight gate (우회 토글)
    if not STRICT_RESERVE_DISABLE and not _strict_try_reserve(side):
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
                    position_data[key] = {
                        "symbol": symbol, "side": side, "entry_usd": usdt_amount,
                        "ts": time.time(), "entry_ts": time.time(),
                        "tf": "1h", "mfe_price": float(last), "mfe_ts": time.time()
                    }
                send_telegram(f"✅ OPEN {side.upper()} {symbol} amt≈{usdt_amount} lev={lev} last≈{last}")
                _mark_done("entry", pkey); _mark_recent_ok(key)
            else:
                send_telegram(f"❌ OPEN FAIL {side.upper()} {symbol} code={code}")
    except Exception as e:
        send_telegram(f"🔥 ENTRY ERR {symbol} {side} {e}")
    finally:
        _clear_busy(key); _strict_release(side)

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

        resp = place_reduce_by_size(symbol, cut_size, side)
        if str(resp.get("code", "")) == "00000":
            send_telegram(f"✅ TP {pct*100:.0f}% {side.upper()} {symbol} qty≈{cut_size}")
        else:
            send_telegram(f"❌ TP 실패 {side.upper()} {symbol} code={resp.get('code')}")

def _sweep_full_close(symbol: str, side: str, reason: str, max_retry: int = 5, sleep_s: float = 0.3):
    for _ in range(max_retry):
        p = _get_remote(symbol, side)
        size = float(p["size"]) if p and p.get("size") else 0.0
        if size <= 0: return True
        place_reduce_by_size(symbol, size, side); time.sleep(sleep_s)
    p = _get_remote(symbol, side)
    return (not p) or float(p.get("size", 0)) <= 0

def close_position(symbol: str, side: str = "long", reason: str = "manual"):
    symbol = convert_symbol(symbol); side = (side or "long").lower()
    key = _key(symbol, side)
    with _lock_for(key):
        p = _get_remote(symbol, side)
        if not p or float(p.get("size", 0)) <= 0:
            send_telegram(f"⚠️ CLOSE 스킵: 원격 포지션 없음 {key}")
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
            send_telegram(
                f"✅ CLOSE {side.upper()} {symbol} ({reason})\n"
                f"• Exit: {exit_price}\n• Size: {size}\n• Realized≈ {realized:+.2f} USDT"
            )
            _mark_recent_ok(key)

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

# ===== Watchdogs =====
_STOP_FIRE_TS: Dict[str, float] = {}
def _should_fire_stop(key: str) -> bool:
    ts = _STOP_FIRE_TS.get(key, 0.0)
    now = time.time()
    if now - ts < STOP_DEBOUNCE_SEC: return False
    _STOP_FIRE_TS[key] = now
    return True

def _watchdog_loop():
    while True:
        try:
            for p in get_open_positions():
                symbol = p.get("symbol"); side = (p.get("side") or "").lower()
                entry  = float(p.get("entry_price") or 0); size = float(p.get("size") or 0)
                if not symbol or side not in ("long","short") or entry <= 0 or size <= 0: continue
                last = get_last_price(symbol)
                if not last: continue

                # Update MFE
                try:
                    k = _key(symbol, side)
                    with _POS_LOCK:
                        meta = position_data.get(k, {}) or {}
                        mfe = float(meta.get("mfe_price") or 0.0)
                        better = (last > mfe) if side == "long" else (mfe == 0 or last < mfe)
                        if better:
                            meta["mfe_price"] = float(last)
                            meta["mfe_ts"] = time.time()
                            position_data[k] = meta
                except Exception:
                    pass

                # Emergency stop by leverage-adjusted loss
                loss_ratio = _loss_ratio_on_margin(entry, last, side, leverage=LEVERAGE)
                if loss_ratio >= STOP_PCT:
                    k = _key(symbol, side)
                    if _should_fire_stop(k):
                        send_telegram(f"⛔ {symbol} {side.upper()} emergencyStop loss≈-{loss_ratio*100:.1f}% (th={STOP_PCT*100:.0f}%)")
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

# ===== Curation loop (adaptive hold/reduce/close) =====
def _curation_loop():
    while True:
        try:
            pos = get_open_positions() or []
            with _POS_LOCK: meta_map = dict(position_data)
            def _price(sym):
                try: return float(get_last_price(sym))
                except Exception: return None
            thresholds = compute_adaptive(pos, meta_map, _price)

            for p in pos:
                symbol = p.get("symbol"); side = (p.get("side") or "").lower()
                entry  = float(p.get("entry_price") or 0); size = float(p.get("size") or 0)
                if not symbol or side not in ("long","short") or entry <= 0 or size <= 0: continue
                last = _price(symbol); if not last: continue
                key = _key(symbol, side)
                with _POS_LOCK: meta = meta_map.get(key, {}) or {}
                tf    = (meta.get("tf") or "1h").lower()
                ets   = float(meta.get("entry_ts") or time.time())
                age_h = max(0.0, (time.time() - ets)/3600.0)
                mfe_p = float(meta.get("mfe_price") or entry)
                mfe_t = float(meta.get("mfe_ts") or ets)
                th = thresholds.get(key)

                if th:
                    action, reason = evaluate_position_adaptive(
                        tf=tf, side=side, entry=entry, last=float(last),
                        age_h=age_h, mfe_price=mfe_p, mfe_ts=mfe_t,
                        roi_th=th.get("roi_th", 0.01),
                        plateau_bars=th.get("plateau_bars", 24),
                        mfe_bp=th.get("mfe_bp", 30),
                        trail_scale=th.get("trail_scale", 1.0),
                    )
                else:
                    action, reason = evaluate_position(
                        tf=tf, side=side, entry=entry, last=float(last),
                        age_h=age_h, mfe_price=mfe_p, mfe_ts=mfe_t
                    )

                if action == "close":
                    try: send_telegram(f"✂️ AUTO CLOSE {side.upper()} {symbol} [{reason}]")
                    except: pass
                    close_position(symbol, side=side, reason=reason)
                elif action == "reduce":
                    try: send_telegram(f"➖ AUTO REDUCE {side.upper()} {symbol} 30% [{reason}]")
                    except: pass
                    take_partial_profit(symbol, 0.30, side=side)
        except Exception as e:
            print("curation error:", e)
        time.sleep(20)

# ===== Reconciler =====
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

def _reconciler_loop():
    while True:
        try:
            # ENTRY 재시도
            with _PENDING_LOCK: entry_items = list(_PENDING["entry"].items())
            for pkey, item in entry_items:
                sym, side = item["symbol"], item["side"]
                key = _key(sym, side)
                if _local_has_any(sym) or _get_remote_any_side(sym) or _recent_ok(key):
                    _mark_done("entry", pkey, "(exists/recent)"); continue
                if _is_busy(key): continue

                if not STRICT_RESERVE_DISABLE and not _strict_try_reserve(side):
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
                            with _POS_LOCK:
                                position_data[key] = {
                                    "symbol": sym, "side": side, "entry_usd": amt,
                                    "ts": time.time(), "entry_ts": time.time(),
                                    "tf": "1h", "mfe_price": float(get_last_price(sym) or 0.0),
                                    "mfe_ts": time.time()
                                }
                            send_telegram(f"✅ RETRY OPEN {side.upper()} {sym} amt≈{amt}")
                            _mark_done("entry", pkey); _mark_recent_ok(key)
                        else:
                            if RECON_DEBUG:
                                send_telegram(f"❌ RETRY OPEN FAIL {side.upper()} {sym} code={code}")
                except Exception as e:
                    print("recon entry err:", e)
                finally:
                    _clear_busy(key); _strict_release(side)

            # CLOSE 재시도
            with _PENDING_LOCK: close_items = list(_PENDING["close"].items())
            for pkey, item in close_items:
                sym, side = item["symbol"], item["side"]
                try:
                    ok = _sweep_full_close(sym, side, "reconcile")
                    if ok: _mark_done("close", pkey)
                except Exception as e:
                    print("recon close err:", e)

            # TP 재시도
            with _PENDING_LOCK: tp_items = list(_PENDING["tp"].items())
            for pkey, item in tp_items:
                try:
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
                    print("recon tp err:", e)
        except Exception as e:
            print("reconciler error:", e)

# ===== Starters =====
def start_watchdogs():
    threading.Thread(target=_watchdog_loop, name="emergency-stop-watchdog", daemon=True).start()
    if BE_ENABLE:
        threading.Thread(target=_breakeven_watchdog, name="breakeven-watchdog", daemon=True).start()
    threading.Thread(target=_curation_loop, name="curation-loop", daemon=True).start()

def start_reconciler():
    threading.Thread(target=_reconciler_loop, name="reconciler", daemon=True).start()
