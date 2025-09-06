# === trader.py (네 버전 + TF/그레이스 보강; 기존 함수/흐름 보존, 생략 없음) ===
import os, time, threading
from typing import Dict, Optional
from bitget_api import (
    convert_symbol, get_last_price, get_open_positions,
    place_market_order, place_reduce_by_size, get_symbol_spec, round_down_step,
)

try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str): print("[TG]", msg)

try:
    from telemetry.logger import log_event, log_trade
except Exception:
    def log_event(payload: dict, stage: str = "event"): print("[LOG]", stage, payload)
    def log_trade(event: str, symbol: str, side: str, amount: float,
                  reason: Optional[str] = None, extra: Optional[Dict] = None):
        d = {"event": event, "symbol": symbol, "side": side, "amount": amount}
        if reason: d["reason"] = reason
        if extra: d.update(extra)
        log_event(d, stage="trade")

LEVERAGE = float(os.getenv("LEVERAGE", "5"))
TRACE_LOG = os.getenv("TRACE_LOG", "0") == "1"

TP1_PCT = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT = float(os.getenv("TP2_PCT", "0.40"))
TP3_PCT = float(os.getenv("TP3_PCT", "0.30"))
TP_EPSILON_RATIO = float(os.getenv("TP_EPSILON_RATIO", "0.001"))

STOP_PRICE_MOVE   = float(os.getenv("STOP_PRICE_MOVE", "0.02"))
STOP_CHECK_SEC    = float(os.getenv("STOP_CHECK_SEC", "1.0"))
STOP_COOLDOWN_SEC = float(os.getenv("STOP_COOLDOWN_SEC", "5.0"))
STOP_CONFIRM_N    = int(os.getenv("STOP_CONFIRM_N", "3"))
STOP_DEBOUNCE_SEC = float(os.getenv("STOP_DEBOUNCE_SEC", "1.2"))

RECON_INTERVAL_SEC = float(os.getenv("RECON_INTERVAL_SEC", "40"))
RECON_DEBUG        = os.getenv("RECON_DEBUG", "0") == "1"

MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "180"))
CAP_CHECK_SEC      = float(os.getenv("CAP_CHECK_SEC", "10"))

LONG_BYPASS_CAP    = os.getenv("LONG_BYPASS_CAP", "1") == "1"
SHORT_BYPASS_CAP   = os.getenv("SHORT_BYPASS_CAP", "1") == "1"

ENTRY_INFLIGHT_TTL_SEC = float(os.getenv("ENTRY_INFLIGHT_TTL_SEC", "30"))
ENTRY_DUP_TTL_SEC      = float(os.getenv("ENTRY_DUP_TTL_SEC", "60"))

# ── 내부 상태
position_data: Dict[str, dict] = {}
_POS_LOCK = threading.RLock()
def _key(symbol: str, side: str) -> str: return f"{convert_symbol(symbol)}_{(side or '').lower()}"
def _set_local(symbol: str, side: str, size: float, entry: float):
    with _POS_LOCK: position_data[_key(symbol, side)] = {"size": size, "entry": entry, "ts": time.time()}
def _rm_local(symbol: str, side: str):
    with _POS_LOCK: position_data.pop(_key(symbol, side), None)
def _local_has_any(symbol: str) -> bool:
    sym = convert_symbol(symbol)
    with _POS_LOCK:
        for k in position_data.keys():
            if k.startswith(sym + "_"): return True
    return False

_BUSY: Dict[str, float] = {}; _RECENT: Dict[str, float] = {}
_BUSY_LOCK = threading.RLock(); _RECENT_LOCK = threading.RLock()
def _is_busy(key: str, within: float = None) -> bool:
    within = within or ENTRY_INFLIGHT_TTL_SEC
    with _BUSY_LOCK:
        t = _BUSY.get(key, 0.0); return time.time() - t < within
def _set_busy(key: str): 
    with _BUSY_LOCK: _BUSY[key] = time.time()
def _recent_ok(key: str, within: float = None) -> bool:
    within = within or ENTRY_DUP_TTL_SEC
    with _RECENT_LOCK:
        t = _RECENT.get(key, 0.0); return time.time() - t < within
def _mark_recent_ok(key: str):
    with _RECENT_LOCK: _RECENT[key] = time.time()

_KEY_LOCKS: Dict[str, threading.RLock] = {}; _KEY_LOCKS_LOCK = threading.RLock()
def _lock_for(key: str) -> threading.RLock:
    with _KEY_LOCKS_LOCK:
        if key not in _KEY_LOCKS: _KEY_LOCKS[key] = threading.RLock()
    return _KEY_LOCKS[key]

# ── Capacity Guard
_CAPACITY = {"blocked": False, "last_count": 0, "short_blocked": False, "short_count": 0, "ts": 0.0}
_CAP_LOCK = threading.Lock(); _cap_thread: Optional[threading.Thread] = None
def _update_capacity():
    with _CAP_LOCK:
        _CAPACITY["ts"] = time.time()
        count = 0; scount = 0
        for p in get_open_positions():
            try:
                sz = float(p.get("size") or 0)
                if sz > 0:
                    count += 1
                    if (p.get("side") or "").lower() == "short": scount += 1
            except: continue
        _CAPACITY["last_count"] = count; _CAPACITY["short_count"] = scount
        blocked = count >= MAX_OPEN_POSITIONS
        _CAPACITY["blocked"] = blocked; _CAPACITY["short_blocked"] = blocked
def capacity_status() -> Dict:
    with _CAP_LOCK:
        d = dict(_CAPACITY); d["max"] = MAX_OPEN_POSITIONS; return d
def _capacity_loop():
    last_block, last_short = None, None
    while True:
        try:
            _update_capacity(); st = capacity_status()
            if last_block != st["blocked"]:
                last_block = st["blocked"]
                send_telegram(f"ℹ️ Capacity {'BLOCKED' if st['blocked'] else 'UNBLOCKED'} {st['last_count']}/{st['max']}")
            if last_short != st["short_blocked"]:
                last_short = st["short_blocked"]
        except Exception as e:
            print("capacity error:", e)
        time.sleep(CAP_CHECK_SEC)
def start_capacity_guard():
    global _cap_thread
    if _cap_thread and _cap_thread.is_alive(): return
    _cap_thread = threading.Thread(target=_capacity_loop, name="capacity-guard", daemon=True); _cap_thread.start()

# ── Pending/Recon
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
            "entry_keys": list(_PENDING["entry"].keys()),
            "close_keys": list(_PENDING["close"].keys()),
            "tp_keys": list(_PENDING["tp"].keys()),
            "interval": RECON_INTERVAL_SEC,
            "debug": RECON_DEBUG,
            "capacity": {
                "blocked": _CAPACITY["blocked"], "last_count": _CAPACITY["last_count"],
                "short_blocked": _CAPACITY["short_blocked"], "short_count": _CAPACITY["short_count"],
                "max": MAX_OPEN_POSITIONS, "interval": CAP_CHECK_SEC, "ts": _CAPACITY["ts"],
            },
            "local_keys": list(position_data.keys()),
        }

# ── 긴급 Stop 카운터
_STOP_RECENT: Dict[str, float] = {}; _STOP_CNT: Dict[str, int] = {}; _STOP_LOCK = threading.RLock()
def _stop_recently_fired(symbol: str, side: str) -> bool:
    with _STOP_LOCK:
        t = _STOP_RECENT.get(_key(symbol, side), 0.0); return time.time() - t < STOP_COOLDOWN_SEC
def _mark_stop_fired(symbol: str, side: str):
    with _STOP_LOCK: _STOP_RECENT[_key(symbol, side)] = time.time()
def _bump_stop_cnt(symbol: str, side: str, hit: bool) -> int:
    k = _key(symbol, side)
    with _STOP_LOCK:
        if not hit: _STOP_CNT[k] = 0; return 0
        _STOP_CNT[k] = _STOP_CNT.get(k, 0) + 1; return _STOP_CNT[k]

# === [추가] TF 판별 & TF별 ENV 읽기 / 진입 타임스탬프/홀드누적 ===
_ENTRY_TS: Dict[str, float] = {}
_STOP_HOLD_ACCUM: Dict[str, float] = {}

def _entry_age_sec(symbol: str, side: str) -> float:
    return time.time() - _ENTRY_TS.get(_key(symbol, side), 0.0)

def _accum_hit_time(symbol: str, side: str, hit: bool, tick_dt: float, min_hold: float):
    k = _key(symbol, side)
    cur = _STOP_HOLD_ACCUM.get(k, 0.0)
    if hit: cur += tick_dt
    else:   cur = max(0.0, cur - tick_dt*0.5)
    _STOP_HOLD_ACCUM[k] = min(cur, min_hold*3)
    return cur

def _tf_of(symbol: str) -> str:
    try:
        from policy.tf_policy import get_timeframe
        tf = (get_timeframe(symbol) or "1h").lower()
        return tf if tf in ("1h","2h","3h","4h","d") else "1h"
    except Exception:
        return "1h"

def _env_tf_f(base: str, tf: str, default: float) -> float:
    # 예: STOP_PRICE_MOVE_1H
    key_tf = f"{base}_{tf.upper()}"
    try:
        if key_tf in os.environ: return float(os.getenv(key_tf))
        return float(os.getenv(base, default))
    except: return default

def _env_tf_i(base: str, tf: str, default: int) -> int:
    key_tf = f"{base}_{tf.upper()}"
    try:
        if key_tf in os.environ: return int(os.getenv(key_tf))
        return int(os.getenv(base, default))
    except: return default

# (선택) 핫스왑 적용 알림 훅
def apply_runtime_overrides(changed: dict):
    try:
        if changed:
            pretty = ", ".join([f"{k}={v}" for k,v in changed.items()])
            send_telegram(f"🛠 Runtime overrides: {pretty}")
    except Exception: pass

# ── 외부 API
def enter_position(symbol: str, usdt_amount: float, side: str = "long",
                   leverage: Optional[float] = None):
    side = (side or "").lower().strip()
    if side not in ("long","short"): return {"ok": False, "reason": "bad_side"}
    key = _key(symbol, side)
    if _is_busy(key): return {"ok": False, "reason": "busy"}
    _set_busy(key)

    st = capacity_status()
    if st["blocked"] and not LONG_BYPASS_CAP and side == "long":
        send_telegram(f"🧱 STRICT HOLD {convert_symbol(symbol)} LONG cap {st['last_count']}/{st['max']}")
        return {"ok": False, "reason": "cap_blocked"}
    if st["short_blocked"] and not SHORT_BYPASS_CAP and side == "short":
        send_telegram(f"🧱 STRICT HOLD {convert_symbol(symbol)} SHORT cap {st['last_count']}/{st['max']}")
        return {"ok": False, "reason": "cap_blocked"}

    if _recent_ok(key): return {"ok": False, "reason": "recent"}
    if _local_has_any(symbol): return {"ok": False, "reason": "local_exists"}

    lev = float(leverage or LEVERAGE)
    try:
        resp = place_market_order(symbol, usdt_amount, "buy" if side == "long" else "sell", leverage=lev)
    except Exception as e:
        send_telegram(f"❌ ENTRY EXC {convert_symbol(symbol)} {side}: {e}")
        return {"ok": False, "reason": "exception", "error": str(e)}

    if str(resp.get("code", "")) == "00000":
        _mark_recent_ok(key)
        # [추가] 진입 시각 기록 → 그레이스/첫바 무시 계산
        _ENTRY_TS[key] = time.time()
        send_telegram(f"🚀 ENTRY {side.upper()} {convert_symbol(symbol)} amt≈{usdt_amount} lev={lev}x")
        log_trade("entry", convert_symbol(symbol), side, usdt_amount, extra={"lev": lev})
        return {"ok": True}
    else:
        send_telegram(f"❌ ENTRY FAIL {convert_symbol(symbol)} {side}: {resp}")
        return {"ok": False, "reason": "api_fail", "resp": resp}

def _tp_targets(entry: float, side: str):
    eps = TP_EPSILON_RATIO
    if side == "long":
        return (entry * (1 + TP1_PCT), entry * (1 + TP2_PCT), entry * (1 + TP3_PCT), entry * (1 + eps))
    else:
        return (entry * (1 - TP1_PCT), entry * (1 - TP2_PCT), entry * (1 - TP3_PCT), entry * (1 - eps))

def take_partial_profit(symbol: str, ratio: float, side: str = "long", reason: str = "tp"):
    symbol = convert_symbol(symbol); side = (side or "").lower().strip()
    if ratio <= 0 or ratio > 1: return {"ok": False, "reason": "bad_ratio"}
    for p in get_open_positions():
        if (p.get("symbol") == symbol) and ((p.get("side") or "").lower() == side):
            size = float(p.get("size") or 0.0)
            if size <= 0: break
            cut = size * float(ratio)
            try:
                spec = get_symbol_spec(symbol); cut = round_down_step(cut, spec.get("sizeStep"))
            except Exception: pass
            if cut <= 0: return {"ok": False, "reason": "too_small"}
            resp = place_reduce_by_size(symbol, cut, side)
            if str(resp.get("code", "")) == "00000":
                send_telegram(f"✂️ TP {side.UPPER()} {symbol} ratio={ratio:.2f} size≈{cut}")
                log_trade("tp", symbol, side, cut, reason=reason); return {"ok": True, "reduced": cut}
            else:
                send_telegram(f"❌ TP FAIL {side.upper()} {symbol} → {resp}")
                return {"ok": False, "reason": "api_fail", "resp": resp}
    return {"ok": False, "reason": "no_position"}

def reduce_by_contracts(symbol: str, contracts: float, side: str = "long",
                        reason: str = "reduceByContracts") -> dict:
    try:
        sym = convert_symbol(symbol); s = (side or "").lower().strip()
        if s not in ("long","short"): return {"ok": False, "reason": "bad_side"}
        cur_size = 0.0
        for p in get_open_positions():
            if p.get("symbol") == sym and (p.get("side") or "").lower() == s:
                cur_size = float(p.get("size") or 0.0); break
        if cur_size <= 0: return {"ok": False, "reason": "no_position"}
        qty = float(contracts or 0.0)
        if qty <= 0: return {"ok": False, "reason": "zero_qty"}
        try:
            spec = get_symbol_spec(sym); qty = min(cur_size, qty); qty = round_down_step(qty, spec.get("sizeStep"))
        except Exception: qty = min(cur_size, qty)
        if qty <= 0: return {"ok": False, "reason": "too_small_after_step"}
        resp = place_reduce_by_size(sym, qty, s)
        if str(resp.get("code", "")) == "00000":
            send_telegram(f"✂️ REDUCE {s.upper()} {sym} -{qty:.6f} ({reason})")
            log_trade("reduce", sym, s, qty, reason=reason); return {"ok": True, "reduced": qty}
        else:
            send_telegram(f"❌ REDUCE FAIL {s.upper()} {sym} {qty:.6f} → {resp}"); return {"ok": False, "reason": "api_fail", "resp": resp}
    except Exception as e:
        send_telegram(f"❌ REDUCE EXC {side.upper()} {symbol}: {e}"); return {"ok": False, "reason": "exception", "error": str(e)}

def close_position(symbol: str, side: str, reason: str = "manual"):
    symbol = convert_symbol(symbol); side = (side or "").lower().strip()
    for p in get_open_positions():
        if p.get("symbol") == symbol and (p.get("side") or "").lower() == side:
            size = float(p.get("size") or 0.0); if size <= 0: continue
            try:
                spec = get_symbol_spec(symbol); size = round_down_step(size, spec.get("sizeStep"))
            except Exception: pass
            if size <= 0: send_telegram(f"⚠️ CLOSE SKIP {side.upper()} {symbol} size≈0"); return
            try:
                resp = place_reduce_by_size(symbol, size, side)
                if str(resp.get("code", "")) == "00000":
                    _rm_local(symbol, side); _mark_recent_ok(_key(symbol, side))
                    send_telegram(f"✅ CLOSE ALL {side.upper()} {symbol} ({reason})")
                    log_trade("close", symbol, side, size, reason=reason)
                else:
                    send_telegram(f"❌ CLOSE FAIL {side.upper()} {symbol} → {resp}")
            except Exception as e:
                send_telegram(f"❌ CLOSE EXC {side.upper()} {symbol}: {e}")

# ── Watchdogs
def _watchdog_loop():
    last_tick = 0.0
    while True:
        try:
            now = time.time()
            if now - last_tick < STOP_DEBOUNCE_SEC: time.sleep(0.05); continue
            tick_dt = STOP_CHECK_SEC
            last_tick = now

            for p in get_open_positions():
                symbol = p.get("symbol"); side = (p.get("side") or "").lower()
                entry  = float(p.get("entry_price") or 0); size = float(p.get("size") or 0)
                if not symbol or side not in ("long","short") or entry <= 0 or size <= 0: continue
                last = get_last_price(symbol); if not last: continue

                tf = _tf_of(symbol)  # '1h'|'2h'|'3h'|'4h'
                px_move   = _env_tf_f("STOP_PRICE_MOVE", tf, STOP_PRICE_MOVE)
                n_confirm = _env_tf_i("STOP_CONFIRM_N", tf, STOP_CONFIRM_N)
                min_hold  = _env_tf_f("STOP_CONFIRM_MIN_HOLD_SEC", tf, float(os.getenv("STOP_CONFIRM_MIN_HOLD_SEC","0") or 0.0))
                grace     = _env_tf_f("ENTRY_GRACE_SEC", tf, float(os.getenv("ENTRY_GRACE_SEC","0") or 0.0))
                first_bar = _env_tf_f("FIRST_BAR_IGNORE_SEC", tf, float(os.getenv("FIRST_BAR_IGNORE_SEC","0") or 0.0))

                # 1) 진입 직후 보호
                age = _entry_age_sec(symbol, side)
                if age < max(grace, first_bar):
                    _bump_stop_cnt(symbol, side, False)
                    continue

                # 2) 불리 이동 hit
                move = ((entry - last) / entry) if side == "long" else ((last - entry) / entry)
                hit = bool(move >= px_move)
                cnt = _bump_stop_cnt(symbol, side, hit)
                acc = _accum_hit_time(symbol, side, hit, tick_dt, min_hold)

                # 3) 최종 컷: 연속 N회 + 누적 홀드
                if hit and cnt >= max(1, n_confirm) and acc >= max(0.0, min_hold) and not _stop_recently_fired(symbol, side):
                    _mark_stop_fired(symbol, side); _bump_stop_cnt(symbol, side, False)
                    send_telegram(f"⛔ {symbol} {side.upper()} emergencyStop ≥{px_move*100:.2f}% (x{n_confirm}, hold {acc:.1f}s, tf={tf})")
                    close_position(symbol, side=side, reason="emergencyStop")

        except Exception as e:
            print("watchdog error:", e)
        time.sleep(STOP_CHECK_SEC)

def _breakeven_watchdog():
    while True:
        try:
            for p in get_open_positions():
                symbol = p.get("symbol"); side = (p.get("side") or "").lower()
                entry  = float(p.get("entry_price") or 0)
                if not symbol or side not in ("long","short") or entry <= 0: continue
                last = get_last_price(symbol); if not last: continue
                _ = _tp_targets(entry, side)  # 확장 포인트
        except Exception as e:
            print("breakeven watchdog error:", e)
        time.sleep(0.8)

def start_watchdogs():
    threading.Thread(target=_watchdog_loop, name="emergency-stop-watchdog", daemon=True).start()
    threading.Thread(target=_breakeven_watchdog, name="breakeven-watchdog", daemon=True).start()

def _strict_try_reserve(side: str) -> bool: return True
def can_enter_now(side: str) -> bool: return True

def _reconciler_loop():
    while True:
        time.sleep(RECON_INTERVAL_SEC)
        try:
            with _PENDING_LOCK:
                entry_items = list(_PENDING["entry"].items())
            for pkey, item in entry_items:
                sym, side = item["symbol"], item["side"]; key = _key(sym, side)
                if _local_has_any(sym): _mark_done("entry", pkey, "(local_exists)"); continue
                if _is_busy(key): continue
                if not _strict_try_reserve(side): continue
                try:
                    now = time.time()
                    if now - item.get("last_try", 0.0) < RECON_INTERVAL_SEC - 1: continue
                    _set_busy(key)
                    amt, lev = float(item["amount"]), float(item["leverage"])
                    resp = place_market_order(sym, amt, "buy" if side == "long" else "sell", leverage=lev)
                    item["last_try"] = now; item["attempts"] = item.get("attempts", 0) + 1
                    if str(resp.get("code", "")) == "00000":
                        _mark_done("entry", pkey, "(success)"); send_telegram(f"🔁 ENTRY 재시도 성공 {side.upper()} {sym}")
                    else:
                        if RECON_DEBUG: send_telegram(f"🔁 ENTRY 재시도 실패 {side.upper()} {sym} → {resp}")
                except Exception as e: print("recon entry err:", e)

            with _PENDING_LOCK:
                close_items = list(_PENDING["close"].items())
            for pkey, item in close_items:
                sym, side = item["symbol"], item["side"]; key = _key(sym, side)
                if _is_busy(key): continue
                with _lock_for(key):
                    try:
                        now = time.time()
                        if now - item.get("last_try", 0.0) < RECON_INTERVAL_SEC - 1: continue
                        _set_busy(key)
                        resp = place_reduce_by_size(sym, float(item.get("size") or 0.0) or 0.0, side)
                        item["last_try"] = now; item["attempts"] = item.get("attempts", 0) + 1
                        if str(resp.get("code", "")) == "00000":
                            _mark_done("close", pkey, "(success)"); send_telegram(f"🔁 CLOSE 재시도 성공 {side.upper()} {sym}")
                        else:
                            if RECON_DEBUG: send_telegram(f"🔁 CLOSE 재시도 실패 {side.upper()} {sym} → {resp}")
                    except Exception as e: print("recon close err:", e)

            with _PENDING_LOCK:
                tp_items = list(_PENDING["tp"].items())
            for pkey, item in tp_items:
                sym, side = item["symbol"], item["side"]; remain = float(item.get("remain") or 0.0)
                if remain <= 0: _mark_done("tp", pkey, "(zero)"); continue
                with _lock_for(_key(sym, side)):
                    try:
                        now = time.time()
                        if now - item.get("last_try", 0.0) < RECON_INTERVAL_SEC - 1: continue
                        resp = place_reduce_by_size(sym, remain, side)
                        item["last_try"] = now; item["attempts"] = item.get("attempts", 0) + 1
                        if str(resp.get("code", "")) == "00000":
                            send_telegram(f"🔁 TP3 재시도 감축 {side.upper()} {sym} remain≈{remain}")
                    except Exception as e: print("recon tp err:", e)
        except Exception as e:
            print("reconciler error:", e)

def start_reconciler():
    threading.Thread(target=_reconciler_loop, name="reconciler", daemon=True).start()
