# trader.py
import os, time, threading, requests
from typing import Dict, Optional, Any, Tuple, List

from bitget_api import (
    convert_symbol, get_last_price, get_open_positions,
    place_market_order, place_reduce_by_size, get_symbol_spec, round_down_step,
)

# ──────────────────────────────────────────────────────────────
# Optional: Telegram logger (없으면 콘솔로 대체)
try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str):
        print("[TG]", msg)

# ──────────────────────────────────────────────────────────────
# 공통 설정 (ENV)
LEVERAGE = float(os.getenv("LEVERAGE", "5"))
TRACE_LOG = os.getenv("TRACE_LOG", "0") == "1"

# 분할 비율 (트뷰/파이썬 공통)
TP1_PCT = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT = float(os.getenv("TP2_PCT", "0.40"))   # 0.5714286 사용 시 30/70 누적 일치
TP3_PCT = float(os.getenv("TP3_PCT", "0.30"))

# Emergency stop (PnL 손실률 기준)
STOP_PCT           = float(os.getenv("STOP_PCT", "0.10"))   # 0.10 = -10%
STOP_CHECK_SEC     = float(os.getenv("STOP_CHECK_SEC", "1.0"))
STOP_COOLDOWN_SEC  = float(os.getenv("STOP_COOLDOWN_SEC", "5.0"))

# Reconciler
RECON_INTERVAL_SEC = float(os.getenv("RECON_INTERVAL_SEC", "60"))
TP_EPSILON_RATIO   = float(os.getenv("TP_EPSILON_RATIO", "0.001"))
RECON_DEBUG        = os.getenv("RECON_DEBUG", "0") == "1"

# Capacity guard
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "50"))
CAP_CHECK_SEC      = float(os.getenv("CAP_CHECK_SEC", "10"))

# 수익률 기반 보조 TP 워치독 (옵션)
PY_TP_ENABLE        = os.getenv("PY_TP_ENABLE", "0") == "1"
PY_TP1_TRIG         = float(os.getenv("PY_TP1_TRIG", "0.015"))   # +1.5%
PY_TP2_TRIG         = float(os.getenv("PY_TP2_TRIG", "0.030"))   # +3.0%
PY_TP3_TRIG         = float(os.getenv("PY_TP3_TRIG", "0.050"))   # +5.0%
PY_TP_COOLDOWN_SEC  = float(os.getenv("PY_TP_COOLDOWN_SEC", "6"))
PY_TP_ALLOW = {s.strip().upper() for s in os.getenv("PY_TP_ALLOW","").split(",") if s.strip()}

# EMA 기반 보조 TP 워치독 (트뷰 전략 동일 조건)
PY_TP_EMA_ENABLE        = os.getenv("PY_TP_EMA_ENABLE", "0") == "1"
PY_TP_EMA_TF_DEFAULT    = os.getenv("PY_TP_EMA_TF_DEFAULT", "4h")
PY_TP_EMA_LIMIT         = int(os.getenv("PY_TP_EMA_LIMIT", "240"))  # 최소 170 이상 권장
PY_TP_EMA_POLL_SEC      = float(os.getenv("PY_TP_EMA_POLL_SEC", "10"))
_raw_map = os.getenv("PY_TP_EMA_TF_MAP", "")
_TF_MAP: Dict[str, str] = {}
for pair in _raw_map.split(","):
    if ":" in pair:
        s, t = pair.split(":", 1)
        _TF_MAP[convert_symbol(s.strip())] = t.strip()

# 본절(Break-even) 스톱
BE_ENABLE        = os.getenv("BE_ENABLE", "1") == "1"
BE_AFTER_STAGE   = int(os.getenv("BE_AFTER_STAGE", "1"))           # 1 or 2
BE_EPSILON_RATIO = float(os.getenv("BE_EPSILON_RATIO", "0.0005"))  # 0.05%

# ──────────────────────────────────────────────────────────────
# 내부 상태/락
position_data: Dict[str, dict] = {}
_POS_LOCK = threading.RLock()

_KEY_LOCKS: Dict[str, threading.RLock] = {}
_KEY_LOCKS_LOCK = threading.Lock()

def _key(symbol: str, side: str) -> str:
    return f"{convert_symbol(symbol)}:{(side or 'long').lower()}"

def _lock_for(key: str):
    with _KEY_LOCKS_LOCK:
        if key not in _KEY_LOCKS:
            _KEY_LOCKS[key] = threading.RLock()
    return _KEY_LOCKS[key]

# Stop watchdog 쿨다운
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

# Pending 레지스트리
_PENDING = {
    "entry": {},  # key -> {...}
    "close": {},  # key -> {...}
    "tp":    {}   # key(stage3 only) -> {...}
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

def get_pending_snapshot() -> Dict[str, Any]:
    with _PENDING_LOCK:
        return {
            "counts": {k: len(v) for k, v in _PENDING.items()},
            "entry_keys": list(_PENDING["entry"].keys()),
            "close_keys": list(_PENDING["close"].keys()),
            "tp_keys": list(_PENDING["tp"].keys()),
            "interval": RECON_INTERVAL_SEC,
            "debug": RECON_DEBUG,
            "capacity": capacity_status(),
        }

# 용량 가드
_CAPACITY = {"blocked": False, "last_count": 0, "ts": 0.0}
_CAP_LOCK = threading.Lock()

def capacity_status():
    with _CAP_LOCK:
        return {
            "blocked": _CAPACITY["blocked"],
            "last_count": _CAPACITY["last_count"],
            "ts": _CAPACITY["ts"],
            "max": MAX_OPEN_POSITIONS,
            "interval": CAP_CHECK_SEC,
        }

def can_enter_now() -> bool:
    with _CAP_LOCK:
        return not _CAPACITY["blocked"]

def _capacity_loop():
    prev_blocked = None
    while True:
        try:
            count = len(get_open_positions())
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
    threading.Thread(target=_capacity_loop, name="capacity-guard", daemon=True).start()

# ──────────────────────────────────────────────────────────────
# 원격 포지션/호가 도우미
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

# ──────────────────────────────────────────────────────────────
# 진입/감축/청산
def enter_position(symbol: str, usdt_amount: float, side: str = "long", leverage: float = None):
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    key    = _key(symbol, side)
    lev    = float(leverage or LEVERAGE)
    pkey   = _pending_key_entry(symbol, side)
    trace  = os.getenv("CURRENT_TRACE_ID", "")

    if TRACE_LOG:
        send_telegram(f"🔎 ENTRY request trace={trace} {symbol} {side} amt={usdt_amount}")

    if not can_enter_now():
        try:
            st = capacity_status()
            send_telegram(f"🧱 capacity BLOCKED {symbol} {side} {st.get('last_count')}/{st.get('max')} trace={trace}")
        except Exception:
            pass
        return

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
            return

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
            # 실패는 리컨실러가 재시도

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
    """트뷰/파이썬 공통 출구. 스테이지 가드 + reduceOnly + TP3 펜딩."""
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    key    = _key(symbol, side)

    with _lock_for(key):
        p = _get_remote(symbol, side)
        if not p or float(p.get("size", 0)) <= 0:
            send_telegram(f"⚠️ TP 스킵: 원격 포지션 없음 {key}")
            return

        size_step = float(get_symbol_spec(symbol).get("sizeStep", 0.001))
        cur_size  = float(p["size"])

        stage = 1 if abs(float(pct) - TP1_PCT) <= 1e-6 else \
                2 if abs(float(pct) - TP2_PCT) <= 1e-6 else \
                3 if abs(float(pct) - TP3_PCT) <= 1e-6 else 0

        with _POS_LOCK:
            st = position_data.get(key, {}) or {}
            if "init_size" not in st or st.get("init_size", 0) <= 0:
                st["init_size"] = cur_size
            done_stage = int(st.get("tp_stage", 0))
            if stage and done_stage >= stage:
                send_telegram(f"⏭️ TP stage{stage} 이미 처리됨 → 스킵 {key}")
                return
            position_data[key] = st

        cut_size  = round_down_step(cur_size * float(pct), size_step)
        if cut_size <= 0:
            send_telegram(f"⚠️ TP 스킵: 계산된 사이즈=0 ({key})")
            return

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
            if stage:
                with _POS_LOCK:
                    st = position_data.get(key, {}) or {}
                    st["tp_stage"] = max(int(st.get("tp_stage", 0)), stage)
                    if "init_size" not in st or st.get("init_size", 0) <= 0:
                        st["init_size"] = cur_size
                    # --- 본절 스톱 무장(수익 체결 시)
                    if BE_ENABLE and stage in (1, 2) and stage >= BE_AFTER_STAGE:
                        profited = (exit_price > entry) if side == "long" else (exit_price < entry)
                        if profited:
                            st["be_armed"] = True
                            st["be_entry"] = entry
                            st["be_from_stage"] = stage
                    position_data[key] = st
        # 실패는 리컨실러가 재시도

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
                f"• Exit: {exit_price}\n"
                f"• Size: {size}\n"
                f"• Realized≈ {realized:+.2f} USDT"
            )
        # 실패는 리컨실러 재시도

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

# ──────────────────────────────────────────────────────────────
# Emergency watchdog (-STOP_PCT 이상 손실시 전량 종료)
def _watchdog_loop():
    while True:
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
                        send_telegram(f"⛔ {symbol} {side.upper()} emergencyStop PnL≤{-int(STOP_PCT*100)}%")
                        close_position(symbol, side=side, reason="emergencyStop")
        except Exception as e:
            print("watchdog error:", e)
        time.sleep(STOP_CHECK_SEC)

# ──────────────────────────────────────────────────────────────
# Reconciler (주기적 재시도)
def _reconciler_loop():
    while True:
        time.sleep(RECON_INTERVAL_SEC)
        try:
            # ENTRY 재시도
            with _PENDING_LOCK:
                entry_items = list(_PENDING["entry"].items())
            for pkey, item in entry_items:
                sym, side = item["symbol"], item["side"]
                if _get_remote_any_side(sym):
                    _mark_done("entry", pkey, "(exists)")
                    continue
                if not can_enter_now():
                    if TRACE_LOG:
                        st = capacity_status()
                        send_telegram(f"⏸️ retry_hold cap {sym} {side} {st['last_count']}/{st['max']}")
                    continue
                key = _key(sym, side)
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

# ──────────────────────────────────────────────────────────────
# (옵션) 수익률 기반 보조 TP 워치독
_LAST_TP_AT: Dict[str, float] = {}

def _cum_thresholds() -> Tuple[float, float, float]:
    return (TP1_PCT, TP1_PCT + TP2_PCT, 1.0)

def _stage_from_fraction(frac: float, eps: float) -> int:
    t1, t2, t3 = _cum_thresholds()
    if frac + eps >= t3: return 3
    if frac + eps >= t2: return 2
    if frac + eps >= t1: return 1
    return 0

def _profit_ratio(entry: float, last: float, side: str) -> float:
    return (last - entry) / entry if side == "long" else (entry - last) / entry

def _py_tp_watchdog():
    if not PY_TP_ENABLE:
        return
    while True:
        try:
            for p in get_open_positions():
                symbol = p.get("symbol"); side = (p.get("side") or "").lower()
                if not symbol or side not in ("long", "short"):
                    continue
                if PY_TP_ALLOW and symbol.upper() not in PY_TP_ALLOW:
                    continue

                entry = float(p.get("entry_price") or 0)
                cur   = float(p.get("size") or 0)
                if entry <= 0 or cur <= 0:
                    continue
                last = get_last_price(symbol)
                if not last:
                    continue

                key = _key(symbol, side)
                with _POS_LOCK:
                    st = position_data.get(key, {}) or {}
                    init = float(st.get("init_size") or 0.0)
                    if init <= 0:
                        init = cur
                        st["init_size"] = init
                    done_stage = int(st.get("tp_stage", 0))
                    position_data[key] = st

                frac = 0.0 if init <= 0 else max(0.0, (init - cur) / init)
                eps_qty = max(float(get_symbol_spec(symbol).get("sizeStep", 0.001)) * 2.0,
                              init * TP_EPSILON_RATIO)
                stage_done = _stage_from_fraction(frac, eps_qty)

                pr = _profit_ratio(entry, last, side)
                want = None
                if stage_done < 1 and pr >= PY_TP1_TRIG:
                    want = 1
                elif stage_done < 2 and pr >= PY_TP2_TRIG:
                    want = 2
                elif stage_done < 3 and pr >= PY_TP3_TRIG:
                    want = 3
                if not want or done_stage >= want:
                    continue

                now = time.time()
                if now - _LAST_TP_AT.get(key, 0.0) < PY_TP_COOLDOWN_SEC:
                    continue

                pct = TP1_PCT if want == 1 else (TP2_PCT if want == 2 else TP3_PCT)
                take_partial_profit(symbol, pct, side=side)
                _LAST_TP_AT[key] = time.time()
        except Exception as e:
            print("py-tp watchdog error:", e)
        time.sleep(1.5)

# ──────────────────────────────────────────────────────────────
# (EMA 동등 조건) 트뷰 전략과 동일한 분할 종료 판정

_TF_TO_SEC = {"1m":60, "3m":180, "5m":300, "15m":900, "30m":1800, "1h":3600, "2h":7200, "4h":14400, "6h":21600, "12h":43200, "1d":86400}

# 포지션별 TF 기억(웹훅 "tf" 힌트로 저장)
_position_tf: Dict[str, str] = {}

def set_position_tf(symbol: str, side: str, tf: str):
    if not symbol or not side or not tf:
        return
    _position_tf[_key(symbol, side)] = tf

def get_position_tf(symbol: str, side: str, default_tf: str) -> str:
    return _position_tf.get(_key(symbol, side), default_tf)

def _fallback_tf(symbol: str) -> str:
    return _TF_MAP.get(convert_symbol(symbol), PY_TP_EMA_TF_DEFAULT)

def _ema(vals: List[float], period: int) -> float:
    k = 2.0 / (period + 1.0)
    ema = None
    for v in vals:
        ema = v if ema is None else (v - ema) * k + ema
    return float(ema or 0.0)

def _fetch_candles_close_low(symbol: str, tf: str, limit: int) -> Tuple[List[float], List[float]]:
    gran = _TF_TO_SEC.get(tf, 14400)
    sym  = convert_symbol(symbol)
    url  = f"https://api.bitget.com/api/mix/v1/market/candles?symbol={sym}_UMCBL&granularity={gran}&limit={limit}"
    try:
        r = requests.get(url, timeout=10)
        j = r.json()
        rows = j if isinstance(j, list) else j.get("data") or []
        rows = list(reversed(rows))  # 과거→현재 순서로
        closes = [float(x[4]) for x in rows]
        lows   = [float(x[3]) for x in rows]
        return closes, lows
    except Exception as e:
        print("candle fetch err:", e)
        return [], []

def _ema_tp_watchdog():
    if not PY_TP_EMA_ENABLE:
        return
    while True:
        try:
            for p in get_open_positions():
                symbol = p.get("symbol"); side = (p.get("side") or "").lower()
                if not symbol or side not in ("long","short"):
                    continue

                entry  = float(p.get("entry_price") or 0)
                size   = float(p.get("size") or 0)
                if entry <= 0 or size <= 0:
                    continue

                tf = get_position_tf(symbol, side, _fallback_tf(symbol))
                closes, lows = _fetch_candles_close_low(symbol, tf, PY_TP_EMA_LIMIT)
                if len(closes) < 170:
                    continue  # EMA160 계산 여유 필요

                # 직전 봉 기준(롱/숏 모두)
                c_prev = closes[-2] if len(closes) >= 2 else closes[-1]
                c_last = closes[-1]
                l_last = lows[-1]

                ema20  = _ema(closes[:-1], 20)
                ema34  = _ema(closes[:-1], 34)
                ema60  = _ema(closes[:-1], 60)
                ema75  = _ema(closes[:-1], 75)
                ema160 = _ema(closes[:-1], 160)  # 필요 시 활용

                key = _key(symbol, side)
                with _POS_LOCK:
                    st = position_data.get(key, {}) or {}
                    init = float(st.get("init_size") or 0.0)
                    if init <= 0:
                        init = size
                        st["init_size"] = init
                    done_stage = int(st.get("tp_stage", 0))
                    position_data[key] = st

                eps_qty = max(float(get_symbol_spec(symbol).get("sizeStep", 0.001)) * 2.0,
                              init * TP_EPSILON_RATIO)
                frac = 0.0 if init <= 0 else max(0.0, (init - size) / init)
                stage_done = _stage_from_fraction(frac, eps_qty)

                # 롱: 트뷰와 동일 조건
                if side == "long":
                    want = None
                    if stage_done < 1 and (c_last < ema20 and c_last > entry):
                        want = 1
                    elif stage_done < 2 and (c_last < ema34 and c_last > entry):
                        want = 2
                    elif stage_done < 3 and (l_last < ema75 and c_last > entry):
                        want = 3
                    if want and done_stage < want:
                        pct = TP1_PCT if want == 1 else (TP2_PCT if want == 2 else TP3_PCT)
                        take_partial_profit(symbol, pct, side="long")
                        continue

                # 숏: '직전 봉 종가' 기준 + 손실이면 분할 스킵(트뷰는 종종 SL 처리)
                else:
                    want = None
                    pnl_ok = (entry - c_last) > 0  # 이익 여부
                    if stage_done < 1 and (c_prev > ema34) and pnl_ok:
                        want = 1
                    elif stage_done < 2 and (c_prev > ema60) and pnl_ok:
                        want = 2
                    elif stage_done < 3 and (c_prev > ema75) and pnl_ok:
                        want = 3
                    if want and done_stage < want:
                        pct = TP1_PCT if want == 1 else (TP2_PCT if want == 2 else TP3_PCT)
                        take_partial_profit(symbol, pct, side="short")
                        continue
        except Exception as e:
            print("ema-tp watchdog error:", e)
        time.sleep(PY_TP_EMA_POLL_SEC)

# ──────────────────────────────────────────────────────────────
# 본절(BE) 워치독: TP1/2 수익 체결 후 본절 닿으면 전량 종료
def _breakeven_watchdog():
    if not BE_ENABLE:
        return
    while True:
        try:
            for p in get_open_positions():
                symbol = p.get("symbol"); side = (p.get("side") or "").lower()
                entry  = float(p.get("entry_price") or 0)
                size   = float(p.get("size") or 0)
                if not symbol or side not in ("long","short") or entry <= 0 or size <= 0:
                    continue

                key = _key(symbol, side)
                with _POS_LOCK:
                    st = position_data.get(key, {}) or {}
                    be_armed = bool(st.get("be_armed"))
                    be_entry = float(st.get("be_entry") or 0)
                    stage_ok = int(st.get("tp_stage", 0)) >= BE_AFTER_STAGE

                if not (be_armed and stage_ok and be_entry > 0):
                    continue

                last = get_last_price(symbol)
                if not last:
                    continue

                eps = max(be_entry * BE_EPSILON_RATIO, 0.0)
                trigger = (last <= be_entry - eps) if side == "long" else (last >= be_entry + eps)
                if trigger:
                    send_telegram(f"🧷 Breakeven stop → CLOSE {side.upper()} {symbol} @≈{last} (entry≈{be_entry})")
                    close_position(symbol, side=side, reason="breakeven")
        except Exception as e:
            print("breakeven watchdog error:", e)
        time.sleep(0.8)

# ──────────────────────────────────────────────────────────────
# 스타터
def start_watchdogs():
    threading.Thread(target=_watchdog_loop, name="emergency-stop-watchdog", daemon=True).start()
    if PY_TP_ENABLE:
        threading.Thread(target=_py_tp_watchdog, name="py-tp-watchdog", daemon=True).start()
    if PY_TP_EMA_ENABLE:
        threading.Thread(target=_ema_tp_watchdog, name="ema-tp-watchdog", daemon=True).start()
    if BE_ENABLE:
        threading.Thread(target=_breakeven_watchdog, name="breakeven-watchdog", daemon=True).start()

def start_reconciler():
    threading.Thread(target=_reconciler_loop, name="reconciler", daemon=True).start()

# ──────────────────────────────────────────────────────────────
# (참고) main.py에서 웹훅으로 받은 TF 저장하려면 set_position_tf(symbol, side, tf) 호출
# 예) set_position_tf("BTCUSDT","long","4h")
