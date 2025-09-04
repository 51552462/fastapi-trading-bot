# trader.py — 기존 로직 유지 + (-2% 실시간 전체 종료) + (TP1/TP2 후 본절 도달 시 전량 종료) + 분할종료 API 복원
import os, time, threading
from typing import Dict, Optional

from bitget_api import (
    convert_symbol, get_last_price, get_open_positions,
    place_market_order, place_reduce_by_size, get_symbol_spec, round_down_step,
)

# ──────────────────────────────────────────────────────────────
# Telegram (없으면 콘솔 대체)
# ──────────────────────────────────────────────────────────────
try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str):
        print("[TG]", msg)

# ──────────────────────────────────────────────────────────────
# 파일 로깅 (telemetry/logger.py 없으면 콘솔)
# ──────────────────────────────────────────────────────────────
try:
    from telemetry.logger import log_event, log_trade
except Exception:
    def log_event(payload: dict, stage: str = "event"):
        print("[LOG]", stage, payload)

    def log_trade(event: str, symbol: str, side: str, amount: float,
                  reason: Optional[str] = None, extra: Optional[Dict] = None):
        d = {"event": event, "symbol": symbol, "side": side, "amount": amount}
        if reason:
            d["reason"] = reason
        if extra:
            d.update(extra)
        log_event(d, stage="trade")

# ──────────────────────────────────────────────────────────────
# 환경변수 (기존 값 유지)
# ──────────────────────────────────────────────────────────────
LEVERAGE = float(os.getenv("LEVERAGE", "5"))
TRACE_LOG = os.getenv("TRACE_LOG", "0") == "1"

TP1_PCT = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT = float(os.getenv("TP2_PCT", "0.40"))
TP3_PCT = float(os.getenv("TP3_PCT", "0.30"))
TP_EPSILON_RATIO = float(os.getenv("TP_EPSILON_RATIO", "0.001"))

STOP_PCT = float(os.getenv("STOP_PCT", "0.10"))  # (예전 마진기반 손절 값, 유지)
STOP_PRICE_MOVE = float(os.getenv("STOP_PRICE_MOVE", "0.02"))  # ✅ 진입가 대비 -2%
STOP_CHECK_SEC = float(os.getenv("STOP_CHECK_SEC", "1.0"))
STOP_COOLDOWN_SEC = float(os.getenv("STOP_COOLDOWN_SEC", "5.0"))

RECON_INTERVAL_SEC = float(os.getenv("RECON_INTERVAL_SEC", "40"))
RECON_DEBUG = os.getenv("RECON_DEBUG", "0") == "1"

MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "40"))
CAP_CHECK_SEC = float(os.getenv("CAP_CHECK_SEC", "10"))
LONG_BYPASS_CAP = os.getenv("LONG_BYPASS_CAP", "1") == "1"

ENTRY_INFLIGHT_TTL_SEC = float(os.getenv("ENTRY_INFLIGHT_TTL_SEC", "30"))
ENTRY_DUP_TTL_SEC = float(os.getenv("ENTRY_DUP_TTL_SEC", "60"))

# ──────────────────────────────────────────────────────────────
# 용량/현황 상태
# ──────────────────────────────────────────────────────────────
_CAPACITY = {"blocked": False, "last_count": 0, "short_blocked": False, "short_count": 0, "ts": 0.0}
_CAP_LOCK = threading.RLock()

def capacity_status() -> Dict:
    with _CAP_LOCK:
        out = dict(_CAPACITY)
        out.setdefault("max", MAX_OPEN_POSITIONS)
        return out

def _update_capacity():
    with _CAP_LOCK:
        _CAPACITY["ts"] = time.time()
        ct = 0; sct = 0
        for p in get_open_positions():
            sz = float(p.get("size") or 0.0)
            if sz > 0:
                ct += 1
                if (p.get("side") or "").lower() == "short":
                    sct += 1
        _CAPACITY["last_count"] = ct
        _CAPACITY["short_count"] = sct
        blocked = ct >= MAX_OPEN_POSITIONS
        _CAPACITY["blocked"] = blocked
        _CAPACITY["short_blocked"] = blocked

def _capacity_loop():
    last_b = None; last_s = None
    while True:
        try:
            _update_capacity()
            st = capacity_status()
            if last_b != st["blocked"]:
                last_b = st["blocked"]
                if st["blocked"]:
                    send_telegram(f"ℹ️ Capacity BLOCKED {st['last_count']}/{st['max']}")
                else:
                    send_telegram("ℹ️ Capacity UNBLOCKED")
            if last_s != st["short_blocked"]:
                last_s = st["short_blocked"]
        except Exception as e:
            print("capacity err:", e)
        time.sleep(CAP_CHECK_SEC)

# ──────────────────────────────────────────────────────────────
# 로컬 포지션/락
# ──────────────────────────────────────────────────────────────
position_data: Dict[str, dict] = {}
_POS_LOCK = threading.RLock()
_KEY_LOCKS: Dict[str, threading.RLock] = {}
_KEY_LOCKS_LOCK = threading.RLock()

def _lock_for(key: str) -> threading.RLock:
    with _KEY_LOCKS_LOCK:
        if key not in _KEY_LOCKS:
            _KEY_LOCKS[key] = threading.RLock()
        return _KEY_LOCKS[key]

def _key(symbol: str, side: str) -> str:
    return f"{convert_symbol(symbol)}_{side.lower()}"

def _local_has_any(symbol: str) -> bool:
    s = convert_symbol(symbol)
    with _POS_LOCK:
        for k in position_data.keys():
            if k.startswith(s + "_"):
                return True
    return False

def _set_local(symbol: str, side: str, size: float, entry: float):
    with _POS_LOCK:
        position_data[_key(symbol, side)] = {"size": size, "entry": entry, "ts": time.time()}

def _rm_local(symbol: str, side: str):
    with _POS_LOCK:
        position_data.pop(_key(symbol, side), None)

def _get_remote_any_side(symbol: str) -> bool:
    core = convert_symbol(symbol)
    for p in get_open_positions():
        if p.get("symbol") == core and float(p.get("size") or 0) > 0:
            return True
    return False

# ──────────────────────────────────────────────────────────────
# busy/recent 가드
# ──────────────────────────────────────────────────────────────
_BUSY: Dict[str, float] = {}
_RECENT: Dict[str, float] = {}
_BUSY_LOCK = threading.RLock()
_RECENT_LOCK = threading.RLock()

def _set_busy(key: str):
    with _BUSY_LOCK:
        _BUSY[key] = time.time()

def _is_busy(key: str, within: float = 12.0) -> bool:
    with _BUSY_LOCK:
        t = _BUSY.get(key, 0.0)
        return time.time() - t < within

def _mark_ok(key: str):
    with _RECENT_LOCK:
        _RECENT[key] = time.time()

def _recent_ok(key: str, within: float = 35.0) -> bool:
    with _RECENT_LOCK:
        t = _RECENT.get(key, 0.0)
        return time.time() - t < within

# ──────────────────────────────────────────────────────────────
# pending 레지스트리
# ──────────────────────────────────────────────────────────────
_PENDING = {"entry": {}, "close": {}, "tp": {}}
_PENDING_LOCK = threading.RLock()

def _pending_key_entry(symbol: str, side: str) -> str: return f"{_key(symbol, side)}:entry"
def _pending_key_close(symbol: str, side: str) -> str: return f"{_key(symbol, side)}:close"
def _pending_key_tp3(symbol: str, side: str)   -> str: return f"{_key(symbol, side)}:tp3"

def _mark_done(typ: str, pkey: str, note: str = ""):
    with _PENDING_LOCK:
        _PENDING.get(typ, {}).pop(pkey, None)
    if RECON_DEBUG and note:
        send_telegram(f"✅ pending done [{typ}] {pkey} {note}")

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

# ──────────────────────────────────────────────────────────────
# stop 쿨다운 & BE 상태
# ──────────────────────────────────────────────────────────────
_STOP_RECENT: Dict[str, float] = {}
_STOP_LOCK = threading.RLock()

def _stop_recently_fired(symbol: str, side: str) -> bool:
    with _STOP_LOCK:
        t = _STOP_RECENT.get(_key(symbol, side), 0.0)
        return time.time() - t < STOP_COOLDOWN_SEC

def _mark_stop_fired(symbol: str, side: str):
    with _STOP_LOCK:
        _STOP_RECENT[_key(symbol, side)] = time.time()

_BE_FLAGS: Dict[str, dict] = {}
_BE_LOCK = threading.RLock()

def _be_key(symbol: str, side: str) -> str:
    return _key(symbol, side)

# ──────────────────────────────────────────────────────────────
# 엔트리 / 종료 / 분할종료 API (외부에서 main.py가 import)
# ──────────────────────────────────────────────────────────────
def can_enter_now(side: str) -> bool:
    st = capacity_status()
    if st["blocked"] and side == "short" and not LONG_BYPASS_CAP:
        return False
    return True

def _strict_try_reserve(side: str) -> bool:
    # 필요 시 동시 숏 제한 등 추가할 자리 (현재는 허용)
    return True

def enter_position(symbol: str, side: str, usdt_amount: float, leverage: Optional[float] = None):
    side = side.lower().strip()
    if side not in ("long", "short"):
        return {"ok": False, "reason": "bad_side"}

    key = _key(symbol, side)
    if _is_busy(key):
        return {"ok": False, "reason": "busy"}
    _set_busy(key)

    st = capacity_status()
    if st["short_blocked"] and side == "short":
        send_telegram(f"🧱 STRICT HOLD {convert_symbol(symbol)} {side} {st['short_count']}/{st['max']}")
        return {"ok": False, "reason": "strict_hold"}
    if st["blocked"] and side == "short" and not LONG_BYPASS_CAP:
        send_telegram(f"🧱 STRICT HOLD {convert_symbol(symbol)} {side} {st['last_count']}/{st['max']}")
        return {"ok": False, "reason": "cap_blocked"}

    if _recent_ok(key):
        return {"ok": False, "reason": "recent"}
    if _local_has_any(symbol):
        return {"ok": False, "reason": "local_exists"}
    if _get_remote_any_side(symbol):
        return {"ok": False, "reason": "remote_exists"}

    lev = float(leverage or LEVERAGE)
    try:
        resp = place_market_order(symbol, usdt_amount, "buy" if side == "long" else "sell", leverage=lev)
    except Exception as e:
        send_telegram(f"❌ ENTRY EXC {convert_symbol(symbol)} {side}: {e}")
        return {"ok": False, "reason": "exception"}

    if str(resp.get("code", "")) == "00000":
        _mark_ok(key)
        send_telegram(f"🚀 ENTRY {side.upper()} {convert_symbol(symbol)} amt≈{usdt_amount} lev={lev}x")
        log_trade("entry", convert_symbol(symbol), side, usdt_amount, extra={"lev": lev})
        return {"ok": True}
    else:
        send_telegram(f"❌ ENTRY FAIL {convert_symbol(symbol)} {side}: {resp}")
        return {"ok": False, "reason": "api_fail", "resp": resp}

def close_position(symbol: str, side: str, reason: str = "manual"):
    core = convert_symbol(symbol)
    s = side.lower().strip()
    for p in get_open_positions():
        if p.get("symbol") == core and (p.get("side") or "").lower() == s:
            size = float(p.get("size") or 0.0)
            if size <= 0:
                continue
            try:
                resp = place_reduce_by_size(core, size, s)
                if str(resp.get("code", "")) == "00000":
                    _rm_local(core, s)
                    _mark_ok(_key(core, s))
                    send_telegram(f"✅ CLOSE ALL {s.upper()} {core} ({reason})")
                    log_trade("close", core, s, size, reason=reason)
                else:
                    send_telegram(f"❌ CLOSE FAIL {s.upper()} {core} → {resp}")
            except Exception as e:
                send_telegram(f"❌ CLOSE EXC {s.upper()} {core}: {e}")

def take_partial_profit(symbol: str, side: str, ratio: float, reason: str = "partialTP") -> dict:
    """
    분할 종료: 현재 열린 포지션의 ratio(0~1) 만큼 수량을 줄인다.
    main.py에서 import 하는 API — 누락되면 ImportError 발생하므로 반드시 존재해야 함.
    """
    try:
        sym = convert_symbol(symbol)
        s = side.lower().strip()
        if s not in ("long", "short"):
            return {"ok": False, "reason": "bad_side"}

        # 현재 원격 포지션 사이즈 조회
        size = 0.0
        for p in get_open_positions():
            if p.get("symbol") == sym and (p.get("side") or "").lower() == s:
                size = float(p.get("size") or 0.0)
                break
        if size <= 0:
            return {"ok": False, "reason": "no_position"}

        cut = max(0.0, min(1.0, float(ratio))) * size
        if cut <= 0:
            return {"ok": False, "reason": "zero_cut"}

        # 거래소 스텝 보정(가능한 경우)
        try:
            spec = get_symbol_spec(sym)
            cut = round_down_step(cut, spec.get("sizeStep"))
        except Exception:
            pass

        resp = place_reduce_by_size(sym, cut, s)
        if str(resp.get("code", "")) == "00000":
            send_telegram(f"✂️ PARTIAL {s.upper()} {sym} -{cut:.6f} ({ratio*100:.1f}%) {reason}")
            log_trade("partial", sym, s, cut, reason=reason)
            return {"ok": True, "reduced": cut}
        else:
            send_telegram(f"❌ PARTIAL FAIL {s.upper()} {sym} {ratio*100:.1f}% → {resp}")
            return {"ok": False, "reason": "api_fail", "resp": resp}
    except Exception as e:
        send_telegram(f"❌ PARTIAL EXC {side.upper()} {symbol}: {e}")
        return {"ok": False, "reason": "exception", "error": str(e)}

# ──────────────────────────────────────────────────────────────
# TP/BE 계산 헬퍼
# ──────────────────────────────────────────────────────────────
def _tp_targets(entry: float, side: str):
    eps = TP_EPSILON_RATIO
    if side == "long":
        return (entry * (1 + TP1_PCT), entry * (1 + TP2_PCT), entry * (1 + TP3_PCT), entry * (1 + eps))
    else:
        return (entry * (1 - TP1_PCT), entry * (1 - TP2_PCT), entry * (1 - TP3_PCT), entry * (1 - eps))

# ──────────────────────────────────────────────────────────────
# 워치독: (-2%)/본절 종료
# ──────────────────────────────────────────────────────────────
def _watchdog_loop():
    """
    ✅ 진입가 대비 -2%(기본 STOP_PRICE_MOVE) 손실이면 즉시 전량 종료.
    """
    while True:
        try:
            for p in get_open_positions():
                symbol = p.get("symbol")
                side = (p.get("side") or "").lower()
                entry = float(p.get("entry_price") or 0.0)
                size = float(p.get("size") or 0.0)
                if not symbol or side not in ("long", "short") or entry <= 0 or size <= 0:
                    continue

                last = get_last_price(symbol)
                if not last or last <= 0:
                    continue

                loss_ratio = ((entry - last) / entry) if side == "long" else ((last - entry) / entry)
                if loss_ratio >= STOP_PRICE_MOVE:
                    if not _stop_recently_fired(symbol, side):
                        _mark_stop_fired(symbol, side)
                        send_telegram(
                            f"⛔ {symbol} {side.upper()} emergencyStop "
                            f"(Δ≈{loss_ratio*100:.2f}% ≥ {STOP_PRICE_MOVE*100:.2f}%)"
                        )
                        close_position(symbol, side=side, reason="emergencyStop")
        except Exception as e:
            print("watchdog error:", e)
        time.sleep(STOP_CHECK_SEC)

def _breakeven_watchdog():
    """
    ✅ TP1 또는 TP2를 한 번이라도 달성했다가 다시 '본절(be_px)'로 되돌아오면 즉시 전량 종료.
    - 롱: last ≥ tp1/tp2 기록 후 last ≤ be_px → 종료
    - 숏: last ≤ tp1/tp2 기록 후 last ≥ be_px → 종료
    """
    while True:
        try:
            for p in get_open_positions():
                symbol = p.get("symbol")
                side = (p.get("side") or "").lower()
                entry = float(p.get("entry_price") or 0.0)
                size = float(p.get("size") or 0.0)
                if not symbol or side not in ("long", "short") or entry <= 0 or size <= 0:
                    continue

                last = get_last_price(symbol)
                if not last or last <= 0:
                    continue

                tp1, tp2, tp3, be_px = _tp_targets(entry, side)
                k = _be_key(symbol, side)

                # TP1/TP2 달성 기록
                stage_reached = 0
                if side == "long":
                    if last >= tp1: stage_reached = max(stage_reached, 1)
                    if last >= tp2: stage_reached = max(stage_reached, 2)
                else:
                    if last <= tp1: stage_reached = max(stage_reached, 1)
                    if last <= tp2: stage_reached = max(stage_reached, 2)

                with _BE_LOCK:
                    st = _BE_FLAGS.get(k, {"stage": 0})
                    if stage_reached > st["stage"]:
                        st["stage"] = stage_reached
                        _BE_FLAGS[k] = st

                # 본절 도달 시 종료 (TP1 이상 달성한 경우에만)
                with _BE_LOCK:
                    reached = _BE_FLAGS.get(k, {}).get("stage", 0) >= 1

                trigger = False
                if reached:
                    if side == "long" and last <= be_px:
                        trigger = True
                    if side == "short" and last >= be_px:
                        trigger = True

                if trigger and not _stop_recently_fired(symbol, side):
                    _mark_stop_fired(symbol, side)
                    send_telegram(
                        f"⛔ {symbol} {side.upper()} BE-close: "
                        f"TP≥1 hit & back to BE (px≈{last:.6f}, be≈{be_px:.6f})"
                    )
                    close_position(symbol, side=side, reason="breakevenAfterTP")

        except Exception as e:
            print("breakeven watchdog error:", e)
        time.sleep(0.8)

# ──────────────────────────────────────────────────────────────
# 리컨실러: entry/close/tp 재시도
# ──────────────────────────────────────────────────────────────
def _reconciler_loop():
    while True:
        time.sleep(RECON_INTERVAL_SEC)
        try:
            # ENTRY 재시도
            with _PENDING_LOCK:
                items = list(_PENDING["entry"].items())
            for pkey, item in items:
                sym = item.get("symbol")
                side = (item.get("side") or "").lower()
                amt = float(item.get("amount") or 0.0)
                lev = float(item.get("leverage") or LEVERAGE)
                if not sym or side not in ("long", "short") or amt <= 0:
                    _mark_done("entry", pkey, "(invalid)")
                    continue

                key = _key(sym, side)
                if _local_has_any(sym) or _get_remote_any_side(sym) or _recent_ok(key):
                    _mark_done("entry", pkey, "(exists/recent)")
                    continue
                if _is_busy(key):
                    continue
                if not _strict_try_reserve(side):
                    continue
                if not can_enter_now(side):
                    continue

                with _lock_for(key):
                    try:
                        now = time.time()
                        if now - item.get("last_try", 0.0) < RECON_INTERVAL_SEC - 1:
                            continue
                        _set_busy(key)
                        resp = place_market_order(sym, amt, "buy" if side == "long" else "sell", leverage=lev)
                        item["last_try"] = now
                        item["attempts"] = item.get("attempts", 0) + 1
                        if str(resp.get("code", "")) == "00000":
                            _mark_ok(key)
                            _mark_done("entry", pkey, "(success)")
                            send_telegram(f"🔁 ENTRY 재시도 성공 {side.upper()} {convert_symbol(sym)}")
                        else:
                            if RECON_DEBUG:
                                send_telegram(f"🔁 ENTRY 재시도 실패 {side.upper()} {convert_symbol(sym)} → {resp}")
                    except Exception as e:
                        print("recon entry err:", e)

            # CLOSE 재시도
            with _PENDING_LOCK:
                citems = list(_PENDING["close"].items())
            for pkey, item in citems:
                sym = item.get("symbol")
                side = (item.get("side") or "").lower()
                if not sym or side not in ("long", "short"):
                    _mark_done("close", pkey, "(invalid)")
                    continue

                key = _key(sym, side)
                if _is_busy(key):
                    continue

                with _lock_for(key):
                    try:
                        now = time.time()
                        if now - item.get("last_try", 0.0) < RECON_INTERVAL_SEC - 1:
                            continue
                        _set_busy(key)
                        # 사이즈는 place_reduce_by_size 내부 스텝에서 처리
                        size = 0.0
                        for p in get_open_positions():
                            if p.get("symbol") == convert_symbol(sym) and (p.get("side") or "").lower() == side:
                                size = float(p.get("size") or 0.0)
                                break
                        if size <= 0:
                            _mark_done("close", pkey, "(no-size)")
                            continue

                        resp = place_reduce_by_size(sym, size, side)
                        item["last_try"] = now
                        item["attempts"] = item.get("attempts", 0) + 1
                        if str(resp.get("code", "")) == "00000":
                            _mark_ok(key)
                            _mark_done("close", pkey, "(success)")
                            send_telegram(f"🔁 CLOSE 재시도 성공 {side.upper()} {convert_symbol(sym)}")
                        else:
                            if RECON_DEBUG:
                                send_telegram(f"🔁 CLOSE 재시도 실패 {side.upper()} {convert_symbol(sym)} → {resp}")
                    except Exception as e:
                        print("recon close err:", e)

            # TP3 재시도(남은 수량 감축 등) – 기존 구조 유지
            with _PENDING_LOCK:
                titems = list(_PENDING["tp"].items())
            for pkey, item in titems:
                sym = item.get("symbol")
                side = (item.get("side") or "").lower()
                remain = float(item.get("remain") or 0.0)
                if remain <= 0:
                    _mark_done("tp", pkey, "(zero)")
                    continue
                with _lock_for(_key(sym, side)):
                    try:
                        now = time.time()
                        if now - item.get("last_try", 0.0) < RECON_INTERVAL_SEC - 1:
                            continue
                        resp = place_reduce_by_size(sym, remain, side)
                        item["last_try"] = now
                        item["attempts"] = item.get("attempts", 0) + 1
                        if str(resp.get("code", "")) == "00000":
                            send_telegram(f"🔁 TP3 재시도 감축 {side.upper()} {convert_symbol(sym)} remain≈{remain}")
                    except Exception as e:
                        print("recon tp err:", e)

        except Exception as e:
            print("reconciler error:", e)

# ──────────────────────────────────────────────────────────────
# 스레드 시작 진입점
# ──────────────────────────────────────────────────────────────
def start_watchdogs():
    threading.Thread(target=_watchdog_loop, name="emergency-stop-watchdog", daemon=True).start()
    threading.Thread(target=_breakeven_watchdog, name="breakeven-watchdog", daemon=True).start()

def start_reconciler():
    threading.Thread(target=_reconciler_loop, name="reconciler", daemon=True).start()

def start_capacity_guard():
    threading.Thread(target=_capacity_loop, name="capacity-guard", daemon=True).start()
