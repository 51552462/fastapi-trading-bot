# trader.py
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

# ── TP 비율 (환경변수 반영) ───────────────────────────────────
TP1_PCT = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT = float(os.getenv("TP2_PCT", "0.40"))
TP3_PCT = float(os.getenv("TP3_PCT", "0.30"))

# ── Emergency stop (PnL 기준 고정) ────────────────────────────
STOP_PCT           = float(os.getenv("STOP_PCT", "0.10"))   # -10% 손실률
STOP_CHECK_SEC     = float(os.getenv("STOP_CHECK_SEC", "1.0"))
STOP_COOLDOWN_SEC  = float(os.getenv("STOP_COOLDOWN_SEC", "5.0"))

# ── Reconciler ────────────────────────────────────────────────
RECON_INTERVAL_SEC = float(os.getenv("RECON_INTERVAL_SEC", "40"))
TP_EPSILON_RATIO   = float(os.getenv("TP_EPSILON_RATIO", "0.001"))
RECON_DEBUG        = os.getenv("RECON_DEBUG", "0") == "1"

# ── Capacity guard (주기적 모니터) ────────────────────────────
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "60"))
CAP_CHECK_SEC      = float(os.getenv("CAP_CHECK_SEC", "10"))

_CAPACITY = {"blocked": False, "last_count": 0, "ts": 0.0}
_CAP_LOCK = threading.Lock()

def capacity_status():
    with _CAP_LOCK, _RES_LOCK:
        return {
            "blocked": _CAPACITY["blocked"],
            "last_count": _CAPACITY["last_count"],
            "reserved": _RESERVE["count"],
            "effective_now": _CAPACITY["last_count"] + _RESERVE["count"],
            "max": MAX_OPEN_POSITIONS,
            "interval": CAP_CHECK_SEC,
            "ts": _CAPACITY["ts"],
        }

def can_enter_now() -> bool:
    with _CAP_LOCK:
        return not _CAPACITY["blocked"]

def _capacity_loop():
    prev_blocked = None
    while True:
        try:
            count = len(get_open_positions())  # long/short 합산
            now = time.time()
            blocked = count >= MAX_OPEN_POSITIONS
            with _CAP_LOCK:
                _CAPACITY["blocked"] = blocked
                _CAPACITY["last_count"] = count
                _CAPACITY["ts"] = now
            if prev_blocked is None or prev_blocked != blocked:
                state = "BLOCKED (>= cap)" if blocked else "UNBLOCKED (< cap)"
                try:
                    send_telegram(f"ℹ️ Position capacity {state} | {count}/{MAX_OPEN_POSITIONS}")
                except Exception:
                    pass
                prev_blocked = blocked
        except Exception as e:
            print("capacity guard error:", e)
        time.sleep(CAP_CHECK_SEC)

def start_capacity_guard():
    t = threading.Thread(target=_capacity_loop, name="capacity-guard", daemon=True)
    t.start()

# ── STRICT admission (실시간 예약 토큰 게이트) ────────────────
_RESERVE = {"count": 0}
_RES_LOCK = threading.Lock()

def _strict_try_reserve() -> bool:
    """원격 오픈수 + 로컬 예약수 기준으로 원자적으로 슬롯 선점."""
    try:
        open_now = len(get_open_positions())
    except Exception:
        # 원격 조회 실패 시 마지막 카운트 사용
        with _CAP_LOCK:
            open_now = _CAPACITY["last_count"]
    with _RES_LOCK:
        effective = open_now + _RESERVE["count"]
        if effective >= MAX_OPEN_POSITIONS:
            return False
        _RESERVE["count"] += 1
        return True

def _strict_release():
    with _RES_LOCK:
        if _RESERVE["count"] > 0:
            _RESERVE["count"] -= 1

# ── Local state & locks ───────────────────────────────────────
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

# ── Stop fire 쿨다운 ──────────────────────────────────────────
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

# ── Pending registry ──────────────────────────────────────────
_PENDING = {
    "entry": {},  # key -> {amount, leverage, created, last_try, attempts}
    "close": {},  # key -> {reason,  created, last_try, attempts}
    "tp":    {}   # key(stage3 only) -> {stage, pct, init_size, cut_size, size_step, created, last_try, attempts}
}
_PENDING_LOCK = threading.RLock()

def _pending_key_entry(symbol: str, side: str) -> str:
    return f"{_key(symbol, side)}:entry"

def _pending_key_close(symbol: str, side: str) -> str:
    return f"{_key(symbol, side)}:close"

def _pending_key_tp3(symbol: str, side: str) -> str:
    return f"{_key(symbol, side)}:tp3"

def _mark_done(typ: str, pkey: str, note: str = ""):
    with _PENDING_LOCK:
        if pkey in _PENDING.get(typ, {}):
            _PENDING[typ].pop(pkey, None)
    if RECON_DEBUG and note:
        send_telegram(f"✅ pending done [{typ}] {pkey} {note}")

def get_pending_snapshot() -> Dict[str, Dict]:
    """/pending 조회용(메인에서 노출)"""
    with _PENDING_LOCK, _RES_LOCK, _CAP_LOCK:
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
                "reserved": _RESERVE["count"],
                "effective_now": _CAPACITY["last_count"] + _RESERVE["count"],
                "max": MAX_OPEN_POSITIONS,
                "interval": CAP_CHECK_SEC,
                "ts": _CAPACITY["ts"],
            },
        }

# ── Helpers ───────────────────────────────────────────────────
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
    return max(0.0, -pnl) / margin  # 양수 = 손실

# ── Trading ops ───────────────────────────────────────────────
def enter_position(symbol: str, usdt_amount: float, side: str = "long", leverage: float = None):
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    key    = _key(symbol, side)
    lev    = float(leverage or LEVERAGE)
    pkey   = _pending_key_entry(symbol, side)

    trace = os.getenv("CURRENT_TRACE_ID", "")

    if TRACE_LOG:
        send_telegram(f"🔎 ENTRY request trace={trace} {symbol} {side} amt={usdt_amount}")

    # ── 실시간 엄격 게이트: 슬롯 선점 실패 시 즉시 스킵
    if not _strict_try_reserve():
        st = capacity_status()
        send_telegram(f"🧱 STRICT HOLD {symbol} {side} {st['effective_now']}/{st['max']} trace={trace}")
        return
    try:
        # (선택) 주기적 가드도 병행: blocked면 조기 스킵
        if not can_enter_now():
            st = capacity_status()
            send_telegram(f"⏳ ENTRY HOLD (periodic) {symbol} {side} {st['last_count']}/{st['max']} trace={trace}")
            return

        # pending 등록
        with _PENDING_LOCK:
            _PENDING["entry"][pkey] = {"symbol": symbol, "side": side, "amount": usdt_amount,
                                       "leverage": lev, "created": time.time(), "last_try": 0.0, "attempts": 0}
        if RECON_DEBUG:
            send_telegram(f"📌 pending add [entry] {pkey}")

        with _lock_for(key):
            if _get_remote_any_side(symbol):
                _mark_done("entry", pkey, "(exists)")
                return

            last = get_last_price(symbol)
            if not last or last <= 0:
                if TRACE_LOG:
                    send_telegram(f"❗ ticker_fail {symbol} trace={trace}")
                return  # 리컨실러가 재시도

            resp = place_market_order(symbol, usdt_amount,
                                      side=("buy" if side == "long" else "sell"),
                                      leverage=lev, reduce_only=False)
            code = str(resp.get("code", ""))
            if TRACE_LOG:
                send_telegram(f"📦 order_resp code={code} {symbol} {side} trace={trace}")

            if code == "00000":
                with _POS_LOCK:
                    position_data[key] = {"symbol": symbol, "side": side, "entry_usd": usdt_amount, "ts": time.time()}
                with _STOP_LOCK:
                    _STOP_FIRED.pop(key, None)
                _mark_done("entry", pkey)
                send_telegram(f"🚀 ENTRY {side.upper()} {symbol}\n• Notional≈ {usdt_amount} USDT\n• Lvg: {lev}x")
            elif code.startswith("LOCAL_MIN_QTY") or code.startswith("LOCAL_BAD_QTY"):
                _mark_done("entry", pkey, "(minQty/badQty)")
                send_telegram(f"⛔ ENTRY 스킵 {symbol} {side} → {resp}")
            else:
                if TRACE_LOG:
                    send_telegram(f"❌ order_fail resp={resp} trace={trace}")
                # 실패는 리컨실러 재시도
    finally:
        _strict_release()

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

        # TP3면 펜딩 등록
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

def reduce_by_contracts(symbol: str, contracts: float, side: str = "long"):
    """고정 계약수만큼 reduceOnly 시장가로 즉시 감축."""
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

# ── Emergency watchdog ────────────────────────────────────────
def _watchdog_loop():
    while True:
        try:
            positions = get_open_positions()
            for p in positions:
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
                        send_telegram(
                            f"⛔ {symbol} {side.upper()} emergencyStop PnL≤{-int(STOP_PCT*100)}%"
                        )
                        close_position(symbol, side=side, reason="emergencyStop")
        except Exception as e:
            print("watchdog error:", e)
        time.sleep(STOP_CHECK_SEC)

# ── Reconciler (1분 주기 재시도) ──────────────────────────────
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
                if _get_remote_any_side(sym):
                    _mark_done("entry", pkey, "(exists)")
                    continue

                # 실시간 엄격 게이트: 슬롯 선점 실패 시 이번 사이클 스킵
                if not _strict_try_reserve():
                    if TRACE_LOG:
                        st = capacity_status()
                        send_telegram(f"⏸️ retry_hold STRICT {sym} {side} {st['effective_now']}/{st['max']}")
                    continue

                try:
                    # (보조) 주기 가드 체크
                    if not can_enter_now():
                        continue

                    with _lock_for(key):
                        now = time.time()
                        if now - item.get("last_try", 0.0) < RECON_INTERVAL_SEC - 1:
                            continue
                        amt, lev = item["amount"], item["leverage"]
                        if RECON_DEBUG or TRACE_LOG:
                            send_telegram(f"🔁 retry_entry {sym} {side} attempt={item.get('attempts',0)+1}")
                        resp = place_market_order(sym, amt,
                                                  side=("buy" if side == "long" else "sell"),
                                                  leverage=lev, reduce_only=False)
                        item["last_try"] = now
                        item["attempts"] = item.get("attempts", 0) + 1
                        code = str(resp.get("code", ""))
                        if code == "00000":
                            _mark_done("entry", pkey)
                            send_telegram(f"🔁 ENTRY 재시도 성공 {side.upper()} {sym}")
                        elif code.startswith("LOCAL_MIN_QTY") or code.startswith("LOCAL_BAD_QTY"):
                            _mark_done("entry", pkey, "(minQty/badQty)")
                            send_telegram(f"⛔ ENTRY 재시도 스킵 {sym} {side} → {resp}")
                finally:
                    _strict_release()

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
        except Exception as e:
            print("reconciler error:", e)

# ── Starters ──────────────────────────────────────────────────
def start_watchdogs():
    t = threading.Thread(target=_watchdog_loop, name="emergency-stop-watchdog", daemon=True)
    t.start()

def start_reconciler():
    t = threading.Thread(target=_reconciler_loop, name="reconciler", daemon=True)
    t.start()




