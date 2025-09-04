# trader.py — 기존 로직 유지 + (-2% 실시간 전체 종료) + (TP1/TP2 후 본절 도달 시 전체 종료)
import os, time, threading
from typing import Dict, Optional

from bitget_api import (
    convert_symbol, get_last_price, get_open_positions,
    place_market_order, place_reduce_by_size, get_symbol_spec, round_down_step,
)

# 텔레그램
try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str):
        print("[TG]", msg)

# 파일 로깅 (telemetry/logger.py가 없으면 콘솔로 대체)
try:
    from telemetry.logger import log_event, log_trade
except Exception:
    def log_event(payload: dict, stage: str = "event"):
        print("[LOG]", stage, payload)
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

STOP_PCT           = float(os.getenv("STOP_PCT", "0.10"))      # (기존값 유지)
STOP_PRICE_MOVE    = float(os.getenv("STOP_PRICE_MOVE", "0.02"))  # ✅ 진입가 대비 -2% 기본
STOP_CHECK_SEC     = float(os.getenv("STOP_CHECK_SEC", "1.0"))
STOP_COOLDOWN_SEC  = float(os.getenv("STOP_COOLDOWN_SEC", "5.0"))

RECON_INTERVAL_SEC = float(os.getenv("RECON_INTERVAL_SEC", "40"))
TP_EPSILON_RATIO   = float(os.getenv("TP_EPSILON_RATIO", "0.001"))
RECON_DEBUG        = os.getenv("RECON_DEBUG", "0") == "1"

MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "40"))
CAP_CHECK_SEC      = float(os.getenv("CAP_CHECK_SEC", "10"))
LONG_BYPASS_CAP    = os.getenv("LONG_BYPASS_CAP", "1") == "1"

ENTRY_INFLIGHT_TTL_SEC = float(os.getenv("ENTRY_INFLIGHT_TTL_SEC", "30"))
ENTRY_DUP_TTL_SEC      = float(os.getenv("ENTRY_DUP_TTL_SEC", "60"))

# ── capacity(state)
_CAPACITY = {"blocked": False, "last_count": 0, "short_blocked": False, "short_count": 0, "ts": 0.0}
_CAP_LOCK = threading.Lock()

# ── local state & locks
position_data: Dict[str, dict] = {}
_POS_LOCK = threading.RLock()

_KEY_LOCKS: Dict[str, threading.RLock] = {}
_KEY_LOCKS_LOCK = threading.RLock()
def _lock_for(key: str) -> threading.RLock:
    with _KEY_LOCKS_LOCK:
        if key not in _KEY_LOCKS:
            _KEY_LOCKS[key] = threading.RLock()
    return _KEY_LOCKS[key]

def _local_open_count() -> int:
    with _POS_LOCK: return len(position_data)

def _local_has_any(symbol: str) -> bool:
    symbol = convert_symbol(symbol)
    with _POS_LOCK:
        for k in position_data.keys():
            if k.startswith(symbol + "_"): return True
    return False

def _set_local(symbol: str, side: str, size: float, entry: float):
    with _POS_LOCK:
        position_data[f"{convert_symbol(symbol)}_{side}"] = {"size": size, "entry": entry, "ts": time.time()}

def _rm_local(symbol: str, side: str):
    with _POS_LOCK:
        position_data.pop(f"{convert_symbol(symbol)}_{side}", None)

def _get_remote_any_side(symbol: str) -> bool:
    for p in get_open_positions():
        if p.get("symbol") == convert_symbol(symbol) and float(p.get("size") or 0) > 0:
            return True
    return False

# ── busy/recent
_BUSY: Dict[str, float] = {}
_RECENT: Dict[str, float] = {}
_BUSY_LOCK = threading.RLock()
_RECENT_LOCK = threading.RLock()

def _set_busy(key: str): 
    with _BUSY_LOCK: _BUSY[key] = time.time()
def _is_busy(key: str, within: float = 12.0) -> bool:
    with _BUSY_LOCK:
        t = _BUSY.get(key, 0.0)
        return time.time() - t < within

def _mark_ok(key: str):
    with _RECENT_LOCK: _RECENT[key] = time.time()
def _recent_ok(key: str, within: float = 35.0) -> bool:
    with _RECENT_LOCK:
        t = _RECENT.get(key, 0.0)
        return time.time() - t < within

def _key(symbol: str, side: str) -> str:
    return f"{convert_symbol(symbol)}_{side.lower()}"

# ── capacity guard
def capacity_status() -> Dict:
    with _CAP_LOCK:
        return dict(_CAPACITY)

def _update_capacity():
    with _CAP_LOCK:
        _CAPACITY["ts"] = time.time()
        count = 0; scount = 0
        for p in get_open_positions():
            sz = float(p.get("size") or 0)
            if sz > 0:
                count += 1
                if (p.get("side") or "").lower() == "short":
                    scount += 1
        _CAPACITY["last_count"] = count
        _CAPACITY["short_count"] = scount
        blocked = count >= MAX_OPEN_POSITIONS
        _CAPACITY["blocked"] = blocked
        _CAPACITY["short_blocked"] = blocked

def _capacity_loop():
    last_blocked = None; last_short = None
    while True:
        try:
            _update_capacity()
            st = capacity_status()
            if last_blocked != st["blocked"]:
                last_blocked = st["blocked"]
                if st["blocked"]:
                    send_telegram(f"ℹ️ Capacity BLOCKED {st['last_count']}/{st['max'] if 'max' in st else MAX_OPEN_POSITIONS}")
                else:
                    send_telegram("ℹ️ Capacity UNBLOCKED")
            if last_short != st["short_blocked"]:
                last_short = st["short_blocked"]
        except Exception as e:
            print("capacity error:", e)
        time.sleep(CAP_CHECK_SEC)

# ── pending registry
_PENDING = {"entry": {}, "close": {}, "tp": {}}
_PENDING_LOCK = threading.RLock()
def _pending_key_entry(symbol: str, side: str) -> str: return f"{_key(symbol, side)}:entry"
def _pending_key_close(symbol: str, side: str) -> str: return f"{_key(symbol, side)}:close"
def _pending_key_tp3(symbol: str, side: str)   -> str: return f"{_key(symbol, side)}:tp3"
def _mark_done(typ: str, pkey: str, note: str = ""):
    with _PENDING_LOCK: _PENDING.get(typ, {}).pop(pkey, None)
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

# ── stop cooldown
_STOP_RECENT: Dict[str, float] = {}
_STOP_LOCK = threading.RLock()
def _stop_recently_fired(symbol: str, side: str) -> bool:
    with _STOP_LOCK:
        t = _STOP_RECENT.get(_key(symbol, side), 0.0)
        return time.time() - t < STOP_COOLDOWN_SEC
def _mark_stop_fired(symbol: str, side: str):
    with _STOP_LOCK:
        _STOP_RECENT[_key(symbol, side)] = time.time()

# ── entry APIs (외부에서 호출)
def enter_position(symbol: str, side: str, usdt_amount: float, leverage: Optional[float] = None):
    side = side.lower().strip(); key = _key(symbol, side)
    if side not in ("long","short"): return {"ok": False, "reason": "bad_side"}

    if _is_busy(key): return {"ok": False, "reason": "busy"}
    _set_busy(key)

    # 엄격 슬롯(숏 보호) – 실패시 보류 없이 return (기존 동작 유지)
    st = capacity_status()
    if st["short_blocked"] and side == "short":
        send_telegram(f"🧱 STRICT HOLD {convert_symbol(symbol)} {side} {st['short_count']}/{st['max'] if 'max' in st else MAX_OPEN_POSITIONS}")
        return {"ok": False, "reason": "strict_hold"}

    if st["blocked"] and not LONG_BYPASS_CAP and side == "short":
        send_telegram(f"🧱 STRICT HOLD {convert_symbol(symbol)} {side} {st['last_count']}/{st['max'] if 'max' in st else MAX_OPEN_POSITIONS}")
        return {"ok": False, "reason": "cap_blocked"}

    if _recent_ok(key): return {"ok": False, "reason": "recent"}
    if _local_has_any(symbol): return {"ok": False, "reason": "local_exists"}
    if _get_remote_any_side(symbol): return {"ok": False, "reason": "remote_exists"}

    lev = float(leverage or LEVERAGE)
    try:
        resp = place_market_order(symbol, usdt_amount, "buy" if side == "long" else "sell", leverage=lev)
    except Exception as e:
        send_telegram(f"❌ ENTRY EXC {convert_symbol(symbol)} {side}: {e}")
        return {"ok": False, "reason": "exception"}

    if str(resp.get("code","")) == "00000":
        _mark_ok(key)
        send_telegram(f"🚀 ENTRY {side.upper()} {convert_symbol(symbol)} amt≈{usdt_amount} lev={lev}x")
        log_trade("entry", convert_symbol(symbol), side, usdt_amount, extra={"lev": lev})
        return {"ok": True}
    else:
        send_telegram(f"❌ ENTRY FAIL {convert_symbol(symbol)} {side}: {resp}")
        return {"ok": False, "reason": "api_fail", "resp": resp}

def close_position(symbol: str, side: str, reason: str = "manual"):
    symbol = convert_symbol(symbol); side = side.lower().strip()
    for p in get_open_positions():
        if p.get("symbol") == symbol and (p.get("side") or "").lower() == side:
            size = float(p.get("size") or 0.0)
            if size <= 0: continue
            try:
                resp = place_reduce_by_size(symbol, size, side)
                if str(resp.get("code", "")) == "00000":
                    _rm_local(symbol, side)
                    _mark_ok(_key(symbol, side))
                    send_telegram(f"✅ CLOSE ALL {side.upper()} {symbol} ({reason})")
                    log_trade("close", symbol, side, size, reason=reason)
                else:
                    send_telegram(f"❌ CLOSE FAIL {side.upper()} {symbol} → {resp}")
            except Exception as e:
                send_telegram(f"❌ CLOSE EXC {side.upper()} {symbol}: {e}")

# ── TP/BE 계산 헬퍼
def _tp_targets(entry: float, side: str):
    eps = TP_EPSILON_RATIO
    if side == "long":
        return (entry*(1+TP1_PCT), entry*(1+TP2_PCT), entry*(1+TP3_PCT), entry*(1+eps))
    else:
        return (entry*(1-TP1_PCT), entry*(1-TP2_PCT), entry*(1-TP3_PCT), entry*(1-eps))

# ── ✅ BE(본절) 트리거 상태 (TP1/TP2 달성 기록)
_BE_FLAGS: Dict[str, dict] = {}
_BE_LOCK = threading.RLock()
def _be_key(symbol: str, side: str) -> str:
    return f"{convert_symbol(symbol)}_{side.lower()}"

# ── watchdogs
def _watchdog_loop():
    # ✅ 진입가 대비 -2% 손실(기본) 시 즉시 전체 종료 (롱/숏 공통)
    while True:
        try:
            for p in get_open_positions():
                symbol = p.get("symbol"); side = (p.get("side") or "").lower()
                entry  = float(p.get("entry_price") or 0); size = float(p.get("size") or 0)
                if not symbol or side not in ("long","short") or entry <= 0 or size <= 0: continue
                last = get_last_price(symbol)
                if not last: continue
                # notional, margin 계산은 기존 유지
                notional = entry * size
                margin = notional / max(1.0, LEVERAGE)

                # 가격 변동률이 STOP_PRICE_MOVE 이상(예: -2%)이면 즉시 종료
                loss_ratio = ((entry-last)/entry if side=="long" else (last-entry)/entry)
                if loss_ratio >= STOP_PRICE_MOVE:
                    if not _stop_recently_fired(symbol, side):
                        _mark_stop_fired(symbol, side)
                        send_telegram(f"⛔ {symbol} {side.upper()} emergencyStop ≥{STOP_PRICE_MOVE*100:.2f}% (Δ≈{loss_ratio*100:.2f}%)")
                        close_position(symbol, side=side, reason="emergencyStop")
        except Exception as e:
            print("watchdog error:", e)
        time.sleep(STOP_CHECK_SEC)

def _breakeven_watchdog():
    """
    ✅ 기능 추가: TP1 또는 TP2를 한 번이라도 달성했다가 다시 본절(be_px)에 닿으면 즉시 전체 종료.
    - 롱: last ≥ tp1/tp2 달성 기록 후 last ≤ be_px → 전량 종료
    - 숏: last ≤ tp1/tp2 달성 기록 후 last ≥ be_px → 전량 종료
    기존 구조/함수는 유지, 추가 상태만 사용.
    """
    while True:
        try:
            positions = get_open_positions()
            for p in positions:
                symbol = p.get("symbol"); side = (p.get("side") or "").lower()
                entry  = float(p.get("entry_price") or 0.0)
                size   = float(p.get("size") or 0.0)
                if not symbol or side not in ("long","short") or entry <= 0 or size <= 0:
                    continue

                last = get_last_price(symbol)
                if not last or last <= 0:
                    continue

                tp1, tp2, tp3, be_px = _tp_targets(entry, side)
                k = _be_key(symbol, side)

                # 1) TP1/TP2 달성 플래그 갱신 (조용히 내부 상태만 기록)
                stage_reached = 0
                if side == "long":
                    if last >= tp1: stage_reached = max(stage_reached, 1)
                    if last >= tp2: stage_reached = max(stage_reached, 2)
                else:  # short
                    if last <= tp1: stage_reached = max(stage_reached, 1)
                    if last <= tp2: stage_reached = max(stage_reached, 2)

                with _BE_LOCK:
                    st = _BE_FLAGS.get(k, {"stage": 0})
                    if stage_reached > st["stage"]:
                        st["stage"] = stage_reached
                        _BE_FLAGS[k] = st

                # 2) 본절 도달 시 전량 종료 (단, TP1 이상 달성 기록이 있어야 함)
                with _BE_LOCK:
                    reached = _BE_FLAGS.get(k, {}).get("stage", 0) >= 1

                if reached:
                    trigger = False
                    if side == "long" and last <= be_px:
                        trigger = True
                    if side == "short" and last >= be_px:
                        trigger = True

                    if trigger and not _stop_recently_fired(symbol, side):
                        _mark_stop_fired(symbol, side)  # 쿨다운 공용
                        try:
                            send_telegram(
                                f"⛔ {symbol} {side.upper()} BE-close: "
                                f"TP≥1 hit & back to BE (px≈{last:.6f}, be≈{be_px:.6f})"
                            )
                        except Exception:
                            pass
                        try:
                            close_position(symbol, side=side, reason="breakevenAfterTP")
                        except Exception as e:
                            print("breakeven close error:", e)

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
                if not _strict_try_reserve(side): continue

                try:
                    if not can_enter_now(side): continue
                    with _lock_for(key):
                        now = time.time()
                        if now - item.get("last_try", 0.0) < RECON_INTERVAL_SEC - 1: continue
                        _set_busy(key)
                        amt, lev = item["amount"], item["leverage"]
                        resp = place_market_order(sym, amt,
                                                  "buy" if side == "long" else "sell",
                                                  leverage=lev)
                        item["last_try"] = now; item["attempts"] = item.get("attempts", 0) + 1
                        if str(resp.get("code","")) == "00000":
                            _mark_ok(key)
                            _mark_done("entry", pkey, "(success)")
                            send_telegram(f"🔁 ENTRY 재시도 성공 {side.upper()} {sym}")
                        else:
                            if RECON_DEBUG:
                                send_telegram(f"🔁 ENTRY 재시도 실패 {side.upper()} {sym} → {resp}")
                except Exception as e:
                    print("recon entry err:", e)

            # CLOSE 재시도
            with _PENDING_LOCK:
                close_items = list(_PENDING["close"].items())
            for pkey, item in close_items:
                sym, side = item["symbol"], item["side"]
                key = _key(sym, side)
                if _is_busy(key): continue
                with _lock_for(key):
                    try:
                        now = time.time()
                        if now - item.get("last_try", 0.0) < RECON_INTERVAL_SEC - 1: continue
                        _set_busy(key)
                        resp = place_reduce_by_size(sym, float(item.get("size") or 0.0) or 0.0, side)
                        item["last_try"] = now; item["attempts"] = item.get("attempts", 0) + 1
                        if str(resp.get("code", "")) == "00000":
                            _mark_ok(key)
                            _mark_done("close", pkey, "(success)")
                            send_telegram(f"🔁 CLOSE 재시도 성공 {side.upper()} {sym}")
                        else:
                            if RECON_DEBUG:
                                send_telegram(f"🔁 CLOSE 재시도 실패 {side.upper()} {sym} → {resp}")
                    except Exception as e:
                        print("recon close err:", e)

            # TP3(감축) 재시도 — 기존 유지
            with _PENDING_LOCK:
                tp_items = list(_PENDING["tp"].items())
            for pkey, item in tp_items:
                sym, side = item["symbol"], item["side"]
                remain = float(item.get("remain") or 0.0)
                if remain <= 0: 
                    _mark_done("tp", pkey, "(zero)")
                    continue
                with _lock_for(_key(sym, side)):
                    try:
                        now = time.time()
                        if now - item.get("last_try", 0.0) < RECON_INTERVAL_SEC - 1: continue
                        resp = place_reduce_by_size(sym, remain, side)
                        item["last_try"] = now; item["attempts"] = item.get("attempts", 0) + 1
                        if str(resp.get("code", "")) == "00000":
                            send_telegram(f"🔁 TP3 재시도 감축 {side.upper()} {sym} remain≈{remain}")
                    except Exception as e:
                        print("recon tp err:", e)
        except Exception as e:
            print("reconciler error:", e)

# (원본에 있던 보조 함수 — 유지)
def can_enter_now(side: str) -> bool:
    st = capacity_status()
    if st["blocked"] and side == "short" and not LONG_BYPASS_CAP:
        return False
    return True

def _strict_try_reserve(side: str) -> bool:
    # (원본 정책 유지: 필요 시 동시 숏 제한을 구현하는 자리)
    return True

def start_watchdogs():
    threading.Thread(target=_watchdog_loop, name="emergency-stop-watchdog", daemon=True).start()
    threading.Thread(target=_breakeven_watchdog, name="breakeven-watchdog", daemon=True).start()

def start_reconciler():
    threading.Thread(target=_reconciler_loop, name="reconciler", daemon=True).start()
