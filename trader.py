# trader.py
# -*- coding: utf-8 -*-
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

# ============================================================================
# ENV 설정 (기본값)
# ============================================================================

LEVERAGE   = float(os.getenv("LEVERAGE", "5"))
TRACE_LOG  = os.getenv("TRACE_LOG", "0") == "1"
RECON_DEBUG= os.getenv("RECON_DEBUG", "0") == "1"

TP1_PCT = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT = float(os.getenv("TP2_PCT", "0.40"))
TP3_PCT = float(os.getenv("TP3_PCT", "0.30"))

STOP_PCT           = float(os.getenv("STOP_PCT", "0.10"))
STOP_CHECK_SEC     = float(os.getenv("STOP_CHECK_SEC", "1.0"))
STOP_COOLDOWN_SEC  = float(os.getenv("STOP_COOLDOWN_SEC", "5.0"))

PX_STOP_DROP_LONG  = float(os.getenv("PX_STOP_DROP_LONG",  "0.02"))
PX_STOP_DROP_SHORT = float(os.getenv("PX_STOP_DROP_SHORT", "0.015"))

STOP_USE_ROE        = os.getenv("STOP_USE_ROE", "1") == "1"
STOP_ROE_LONG       = float(os.getenv("STOP_ROE_LONG", "-10"))
STOP_ROE_SHORT      = float(os.getenv("STOP_ROE_SHORT", "-8"))
STOP_ROE_COOLDOWN   = float(os.getenv("STOP_ROE_COOLDOWN", "20"))

RECON_INTERVAL_SEC = float(os.getenv("RECON_INTERVAL_SEC", "40"))
TP_EPSILON_RATIO   = float(os.getenv("TP_EPSILON_RATIO", "0.001"))

MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "40"))
CAP_CHECK_SEC      = float(os.getenv("CAP_CHECK_SEC", "10"))
LONG_BYPASS_CAP    = os.getenv("LONG_BYPASS_CAP", "1") == "1"

ENTRY_INFLIGHT_TTL_SEC = float(os.getenv("ENTRY_INFLIGHT_TTL_SEC", "30"))
ENTRY_DUP_TTL_SEC      = float(os.getenv("ENTRY_DUP_TTL_SEC", "60"))

BE_ENABLE        = os.getenv("BE_ENABLE", "1") == "1"
BE_AFTER_STAGE   = int(os.getenv("BE_AFTER_STAGE", "1"))
BE_EPSILON_RATIO = float(os.getenv("BE_EPSILON_RATIO", "0.0005"))

CLOSE_IMMEDIATE     = os.getenv("CLOSE_IMMEDIATE", "1") == "1"
TP3_CLOSE_IMMEDIATE = os.getenv("TP3_CLOSE_IMMEDIATE", "1") == "1"

# ============================================================================
# 유틸: ENV를 루프마다 재평가(재배포 없이 즉시 반영)
# ============================================================================
def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return float(default)

def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name, "1" if default else "0").strip().lower()
    return v in ("1", "true", "yes", "on")

# ============================================================================
# 상태/락
# ============================================================================
_CAPACITY = {"blocked": False, "last_count": 0, "short_blocked": False, "short_count": 0, "ts": 0.0}
_CAP_LOCK = threading.Lock()

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

# STOP 쿨다운(동일 포지션 반복 발동 방지)
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

# ROE close 최근 성공 시각(성공시에만 쿨다운)
_last_roe_close_ts: Dict[str, float] = {}

# ============================================================================
# Pending 레지스트리 (재시도/조정용)
# ============================================================================
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

# ============================================================================
# 숫자 파싱 보강
# ============================================================================
def _to_float(x) -> float:
    try:
        if isinstance(x, (int, float)): return float(x)
        if isinstance(x, str):
            xs = x.strip()
            if xs == "" or xs.lower() == "null": return 0.0
            return float(xs)
        return 0.0
    except Exception:
        return 0.0

# ============================================================================
# 원격 포지션 조회
# ============================================================================
def _get_remote(symbol: str, side: Optional[str] = None):
    symbol = convert_symbol(symbol)
    for p in get_open_positions():
        s = (p.get("side") or p.get("holdSide") or p.get("positionSide") or "").lower()
        if p.get("symbol") == symbol and (side is None or s == side):
            return p
    return None

def _get_remote_any_side(symbol: str):
    symbol = convert_symbol(symbol)
    for p in get_open_positions():
        sz = _to_float(p.get("size"))
        if p.get("symbol") == symbol and sz > 0:
            return p
    return None

# ============================================================================
# 손익/리스크 계산
# ============================================================================
def _pnl_usdt(entry: float, exit: float, notional: float, side: str) -> float:
    pct = (exit - entry) / entry if side == "long" else (entry - exit) / entry
    return notional * pct

def _loss_ratio_on_margin(entry: float, last: float, size: float, side: str, leverage: float) -> float:
    notional = entry * size
    pnl = _pnl_usdt(entry, last, notional, side)
    margin = max(1e-9, notional / max(1.0, leverage))
    return max(0.0, -pnl) / margin

def _adverse_move_ratio(entry: float, last: float, side: str) -> float:
    if entry <= 0 or last <= 0: return 0.0
    side = (side or "long").lower()
    if side == "long":
        return max(0.0, (entry - last) / entry)
    else:
        return max(0.0, (last - entry) / entry)

def _calc_roe_pct(entry_price: float, mark_price: float, side: str, leverage: float) -> float:
    """엔트리/마크/레버리지로 계산식 ROE%"""
    try:
        if not entry_price or not mark_price or leverage <= 0:
            return 0.0
        s = (side or "").lower()
        dir_sign = 1.0 if s in ("long", "buy") else -1.0
        pnl_rate = (mark_price - entry_price) / entry_price * dir_sign
        return pnl_rate * float(leverage) * 100.0
    except Exception:
        return 0.0

def _calc_roe_from_exchange_fields(p: dict, entry: float, last: float, side: str, fallback_lev: float) -> float:
    """
    거래소가 포지션에 'unrealizedPnl'과 'margin'을 제공하면 그것으로 ROE% = PnL/margin*100
    없으면 _calc_roe_pct로 폴백
    """
    lev_pos = _to_float(p.get("leverage") or p.get("marginLeverage") or 0.0)
    margin  = _to_float(p.get("margin") or p.get("marginSize") or 0.0)
    upnl    = _to_float(p.get("unrealizedPnl") or 0.0)

    lev = lev_pos if lev_pos > 0 else fallback_lev
    if margin > 0:
        return (upnl / margin) * 100.0
    return _calc_roe_pct(entry, last, side, lev)

# ============================================================================
# 용량(상한) 가드 — 숏만 제한, 롱은 무제한
# ============================================================================
def _total_open_positions_now() -> int:
    try:
        return len(get_open_positions()) + _local_open_count()
    except:
        return _local_open_count()

def capacity_status():
    with _CAP_LOCK:
        return {
            "blocked": _CAPACITY["blocked"],
            "last_count": _CAPACITY["last_count"],
            "short_blocked": _CAPACITY["short_blocked"],
            "short_count": _CAPACITY["short_count"],
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
            total_count   = _total_open_positions_now()
            short_blocked = total_count >= MAX_OPEN_POSITIONS
            now = time.time()
            with _CAP_LOCK:
                _CAPACITY["short_blocked"] = short_blocked
                _CAPACITY["short_count"]   = total_count
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

# ============================================================================
# 진입 중복/인플라이트 가드
# ============================================================================
_ENTRY_BUSY: Dict[str, float] = {}
_RECENT_OK: Dict[str, float]  = {}
_ENTRY_G_LOCK = threading.Lock()

def _set_busy(key: str):
    with _ENTRY_G_LOCK:
        _ENTRY_BUSY[key] = time.time()

def _clear_busy(key: str):
    with _ENTRY_G_LOCK:
        _ENTRY_BUSY.pop(key, None)

def _is_busy(key: str) -> bool:
    with _ENTRY_G_LOCK:
        ts = _ENTRY_BUSY.get(key, 0.0)
    return (time.time() - ts) < ENTRY_INFLIGHT_TTL_SEC

def _mark_recent_ok(key: str):
    with _ENTRY_G_LOCK:
        _RECENT_OK[key] = time.time()

def _recent_ok(key: str) -> bool:
    with _ENTRY_G_LOCK:
        ts = _RECENT_OK.get(key, 0.0)
    return (time.time() - ts) < ENTRY_DUP_TTL_SEC

# ============================================================================
# Trading Ops
# ============================================================================
def enter_position(symbol: str, usdt_amount: float, side: str = "long", leverage: float = None):
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    key    = _key(symbol, side)
    lev    = float(leverage or _env_float("LEVERAGE", LEVERAGE))
    pkey   = _pending_key_entry(symbol, side)
    trace  = os.getenv("CURRENT_TRACE_ID", "")

    if TRACE_LOG:
        send_telegram(f"🔎 ENTRY request trace={trace} {symbol} {side} amt={usdt_amount}")

    if _is_busy(key) or _recent_ok(key):
        if RECON_DEBUG:
            send_telegram(f"⏸️ skip entry (busy/recent) {key}")
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
            _PENDING["entry"][pkey] = {
                "symbol": symbol, "side": side, "amount": usdt_amount,
                "leverage": lev, "created": time.time(), "last_try": 0.0, "attempts": 0
            }
        if RECON_DEBUG:
            send_telegram(f"📌 pending add [entry] {pkey}")

        with _lock_for(key):
            if _local_has_any(symbol) or _get_remote_any_side(symbol) or _recent_ok(key):
                _mark_done("entry", pkey, "(exists/recent)")
                return

            _set_busy(key)

            last = _to_float(get_last_price(symbol))
            if last <= 0:
                if TRACE_LOG:
                    send_telegram(f"❗ ticker_fail {symbol} trace={trace}")
                return

            resp = place_market_order(
                symbol, usdt_amount,
                side=("buy" if side == "long" else "sell"),
                leverage=lev, reduce_only=False
            )
            code = str(resp.get("code", "")) if isinstance(resp, dict) else ""
            if TRACE_LOG:
                send_telegram(f"📦 order_resp code={code} {symbol} {side} trace={trace}")

            if code == "00000":
                with _POS_LOCK:
                    position_data[key] = {
                        "symbol": symbol, "side": side,
                        "entry_usd": usdt_amount, "ts": time.time(),
                        "entry_price": last
                    }
                with _STOP_LOCK:
                    _STOP_FIRED.pop(key, None)
                _mark_done("entry", pkey)
                _mark_recent_ok(key)
                send_telegram(
                    f"🚀 ENTRY {side.upper()} {symbol}\n"
                    f"• Notional≈ {usdt_amount} USDT\n• Lvg: {lev}x"
                )
            elif code.startswith("LOCAL_MIN_QTY") or code.startswith("LOCAL_BAD_QTY"):
                _mark_done("entry", pkey, "(minQty/badQty)")
                send_telegram(f"⛔ ENTRY 스킵 {symbol} {side} → {resp}")
            else:
                if TRACE_LOG:
                    send_telegram(f"❌ order_fail resp={resp} trace={trace}")
    finally:
        _clear_busy(key)
        _strict_release(side)

def take_partial_profit(symbol: str, pct: float, side: str = "long"):
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    key    = _key(symbol, side)

    with _lock_for(key):
        p = _get_remote(symbol, side)
        if not p or _to_float(p.get("size")) <= 0:
            send_telegram(f"⚠️ TP 스킵: 원격 포지션 없음 {_key(symbol, side)}")
            return

        size_step = _to_float(get_symbol_spec(symbol).get("sizeStep", 0.001))
        cur_size  = _to_float(p.get("size"))
        pct       = max(0.0, min(1.0, float(pct)))
        cut_size  = round_down_step(cur_size * pct, size_step)
        if cut_size <= 0:
            send_telegram(f"⚠️ TP 스킵: 계산된 사이즈=0 ({_key(symbol, side)})")
            return

        if abs(pct - 1.0) < 1e-9 and TP3_CLOSE_IMMEDIATE:
            resp = place_reduce_by_size(symbol, cur_size, side)
            if str(resp.get("code", "")) == "00000":
                exit_price = _to_float(get_last_price(symbol)) or _to_float(p.get("entry_price"))
                entry = _to_float(p.get("entry_price"))
                realized = _pnl_usdt(entry, exit_price, entry * cur_size, side)
                send_telegram(
                    f"🤑 TP3 FULL CLOSE {side.upper()} {symbol}\n"
                    f"• Exit: {exit_price}\n• Size: {cur_size}\n• Realized≈ {realized:+.2f} USDT"
                )
            else:
                send_telegram(f"❌ TP3 즉시 종료 실패 {symbol} {side} → {resp}")
            return

        resp = place_reduce_by_size(symbol, cut_size, side)
        if str(resp.get("code", "")) == "00000":
            send_telegram(f"🤑 TP {int(pct*100)}% {side.upper()} {symbol} cut={cut_size}")
        else:
            send_telegram(f"❌ TP 실패 {symbol} {side} → {resp}")

def close_position(symbol: str, side: str = "long", reason: str = "manual"):
    symbol = convert_symbol(symbol)
    req_side = (side or "long").lower()
    key_req  = _key(symbol, req_side)
    pkey     = _pending_key_close(symbol, req_side)

    with _PENDING_LOCK:
        _PENDING["close"][pkey] = {
            "symbol": symbol, "side": req_side, "reason": reason,
            "created": time.time(), "last_try": 0.0, "attempts": 0
        }
    if RECON_DEBUG:
        send_telegram(f"📌 pending add [close] {pkey}")

    if CLOSE_IMMEDIATE:
        p = _get_remote(symbol, req_side) or _get_remote_any_side(symbol)
        if not p or _to_float(p.get("size")) <= 0:
            with _POS_LOCK:
                position_data.pop(key_req, None)
            _mark_done("close", pkey, "(no-remote)")
            send_telegram(f"⚠️ CLOSE 스킵: 원격 포지션 없음 {key_req} ({reason})")
            return

        pos_side = (p.get("side") or p.get("holdSide") or p.get("positionSide") or "").lower()
        key_real = _key(symbol, pos_side)
        with _lock_for(key_real):
            size = _to_float(p.get("size"))
            resp = place_reduce_by_size(symbol, size, pos_side)
            exit_price = _to_float(get_last_price(symbol)) or _to_float(p.get("entry_price"))
            success = str(resp.get("code", "")) == "00000"
            if success:
                entry = _to_float(p.get("entry_price"))
                realized = _pnl_usdt(entry, exit_price, entry * size, pos_side)
                with _POS_LOCK:
                    position_data.pop(key_real, None)
                _mark_done("close", pkey)
                _mark_recent_ok(key_real)
                _last_roe_close_ts[key_real] = time.time()  # 성공시에만 쿨다운
                send_telegram(
                    f"✅ CLOSE {pos_side.upper()} {symbol} ({reason})\n"
                    f"• Exit: {exit_price}\n• Size: {size}\n• Realized≈ {realized:+.2f} USDT"
                )
            else:
                send_telegram(f"❌ CLOSE 실패 {symbol} {pos_side} → {resp}")

def reduce_by_contracts(symbol: str, contracts: float, side: str = "long"):
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    key    = _key(symbol, side)
    with _lock_for(key):
        step = _to_float(get_symbol_spec(symbol).get("sizeStep", 0.001))
        qty  = round_down_step(_to_float(contracts), step)
        if qty <= 0:
            send_telegram(f"⚠️ reduceByContracts 스킵: step 미달 {key}")
            return
        resp = place_reduce_by_size(symbol, qty, side)
        if str(resp.get("code", "")) == "00000":
            send_telegram(f"🔻 Reduce {qty} {side.upper()} {symbol}")
        else:
            send_telegram(f"❌ Reduce 실패 {key} → {resp}")

# ============================================================================
# 보조 루틴
# ============================================================================
def _sweep_full_close(symbol: str, side: str, reason: str, max_retry: int = 5, sleep_s: float = 0.3):
    for _ in range(max_retry):
        p = _get_remote(symbol, side)
        size = _to_float(p.get("size")) if p else 0.0
        if size <= 0:
            return True
        place_reduce_by_size(symbol, size, side)
        time.sleep(sleep_s)
    p = _get_remote(symbol, side)
    return (not p) or _to_float(p.get("size")) <= 0

# ============================================================================
# 워치독: ROE 기반 → 가격 기반 → 마진 기반 순서로 즉시 종료 평가
# ============================================================================
def _watchdog_loop():
    while True:
        try:
            # 동적 ENV 반영
            use_roe      = _env_bool("STOP_USE_ROE", STOP_USE_ROE)
            roe_thr_long = _env_float("STOP_ROE_LONG", STOP_ROE_LONG)
            roe_thr_short= _env_float("STOP_ROE_SHORT", STOP_ROE_SHORT)
            roe_cooldown = _env_float("STOP_ROE_COOLDOWN", STOP_ROE_COOLDOWN)
            lev_env      = _env_float("DEFAULT_LEVERAGE", _env_float("LEVERAGE", LEVERAGE))

            for p in get_open_positions():
                symbol = p.get("symbol")
                side   = (p.get("side") or p.get("holdSide") or p.get("positionSide") or "").lower()
                entry  = _to_float(p.get("entry_price"))
                size   = _to_float(p.get("size"))
                if not symbol or side not in ("long", "short") or entry <= 0 or size <= 0:
                    continue

                last = _to_float(get_last_price(symbol))
                if not last:
                    continue

                # ---- 1) ROE 기반 긴급 손절 ----
                if use_roe:
                    roe = _calc_roe_from_exchange_fields(p, entry, last, side, lev_env)
                    thr = roe_thr_long if side == "long" else roe_thr_short
                    k   = _key(symbol, side)
                    now = time.time()
                    last_ok_ts = _last_roe_close_ts.get(k, 0.0)
                    if RECON_DEBUG:
                        send_telegram(f"🧪 ROE dbg {symbol} {side} ROE={roe:.2f}% thr={thr:.2f}% lev={_to_float(p.get('leverage') or p.get('marginLeverage') or lev_env)}x")
                    if roe <= thr and (now - last_ok_ts) >= roe_cooldown:
                        send_telegram(f"⛔ ROE STOP {side.upper()} {symbol} (ROE {roe:.2f}% ≤ {thr:.2f}%)")
                        close_position(symbol, side=side, reason="roeStop")
                        # close 성공 시점에 _last_roe_close_ts 갱신됨
                        continue  # 동일 루프 내 다른 스톱은 스킵

                # ---- 2) 가격 기반 즉시 종료 ----
                adverse      = _adverse_move_ratio(entry, last, side)
                px_threshold = PX_STOP_DROP_LONG if side == "long" else PX_STOP_DROP_SHORT
                if adverse >= px_threshold:
                    k = _key(symbol, side)
                    if _should_fire_stop(k):
                        send_telegram(
                            f"⛔ PRICE STOP {side.upper()} {symbol} "
                            f"(adverse {adverse*100:.2f}% ≥ {px_threshold*100:.2f}%)"
                        )
                        close_position(symbol, side=side, reason="priceStop")
                    continue

                # ---- 3) 마진 기반 긴급 정지 ----
                loss_ratio = _loss_ratio_on_margin(entry, last, size, side, leverage=lev_env)
                if loss_ratio >= STOP_PCT:
                    k = _key(symbol, side)
                    if _should_fire_stop(k):
                        send_telegram(
                            f"⛔ MARGIN STOP {symbol} {side.upper()} (loss/margin ≥ {int(STOP_PCT*100)}%)"
                        )
                        close_position(symbol, side=side, reason="emergencyStop")
        except Exception as e:
            print("watchdog error:", e)
        time.sleep(STOP_CHECK_SEC)

# ============================================================================
# 브레이크이븐 워치독
# ============================================================================
def _breakeven_watchdog():
    if not BE_ENABLE:
        return
    while True:
        try:
            for p in get_open_positions():
                symbol = p.get("symbol")
                side   = (p.get("side") or p.get("holdSide") or p.get("positionSide") or "").lower()
                entry  = _to_float(p.get("entry_price"))
                size   = _to_float(p.get("size"))
                if not symbol or side not in ("long", "short") or entry <= 0 or size <= 0:
                    continue

                key = _key(symbol, side)
                with _POS_LOCK:
                    st = position_data.get(key, {}) or {}
                    be_armed = bool(st.get("be_armed"))
                    be_entry = _to_float(st.get("be_entry"))

                if not (be_armed and be_entry > 0):
                    continue

                last = _to_float(get_last_price(symbol))
                if not last:
                    continue

                eps = max(be_entry * BE_EPSILON_RATIO, 0.0)
                trigger = (last <= be_entry - eps) if side == "long" else (last >= be_entry + eps)
                if trigger:
                    send_telegram(
                        f"🧷 Breakeven stop → CLOSE {side.upper()} {symbol} @≈{last} (entry≈{be_entry})"
                    )
                    close_position(symbol, side=side, reason="breakeven")
        except Exception as e:
            print("breakeven watchdog error:", e)
        time.sleep(0.8)

# ============================================================================
# 재조정 루프(엔트리/클로즈/TP3 재시도)
# ============================================================================
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
                    _mark_done("entry", pkey, "(exists/recent)")
                    continue

                if _is_busy(key):
                    continue

                if not _strict_try_reserve(side):
                    if TRACE_LOG:
                        st = capacity_status()
                        send_telegram(f"⏸️ retry_hold STRICT {sym} {side} {st['last_count']}/{MAX_OPEN_POSITIONS}")
                    continue

                try:
                    if not can_enter_now(side):
                        continue
                    with _lock_for(key):
                        now = time.time()
                        if now - item.get("last_try", 0.0) < RECON_INTERVAL_SEC - 1:
                            continue

                        _set_busy(key)
                        amt, lev = item["amount"], item["leverage"]
                        if RECON_DEBUG or TRACE_LOG:
                            send_telegram(f"🔁 retry_entry {sym} {side} attempt={item.get('attempts', 0) + 1}")

                        resp = place_market_order(
                            sym, amt, side=("buy" if side == "long" else "sell"),
                            leverage=lev, reduce_only=False
                        )
                        item["last_try"] = now
                        item["attempts"] = item.get("attempts", 0) + 1
                        code = str(resp.get("code", "")) if isinstance(resp, dict) else ""

                        if code == "00000":
                            _mark_done("entry", pkey)
                            with _POS_LOCK:
                                position_data[key] = {
                                    "symbol": sym, "side": side, "entry_usd": amt,
                                    "ts": time.time(), "entry_price": _to_float(get_last_price(sym)) or 0.0
                                }
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
                p = _get_remote(sym, side) or _get_remote_any_side(sym)
                if not p or _to_float(p.get("size")) <= 0:
                    _mark_done("close", pkey, "(no-remote)")
                    with _POS_LOCK:
                        position_data.pop(key, None)
                    continue

                with _lock_for(key):
                    now = time.time()
                    if now - item.get("last_try", 0.0) < RECON_INTERVAL_SEC - 1:
                        continue
                    if RECON_DEBUG:
                        send_telegram(f"🔁 retry [close] {pkey}")

                    size = _to_float(p.get("size"))
                    side_real = (p.get("side") or p.get("holdSide") or p.get("positionSide") or "").lower()
                    resp = place_reduce_by_size(sym, size, side_real)
                    item["last_try"] = now
                    item["attempts"] = item.get("attempts", 0) + 1
                    if str(resp.get("code", "")) == "00000":
                        ok = _sweep_full_close(sym, side_real, "reconcile")
                        if ok:
                            _mark_done("close", pkey)
                            with _POS_LOCK:
                                position_data.pop(_key(sym, side_real), None)
                            send_telegram(f"🔁 CLOSE 재시도 성공 {side_real.upper()} {sym}")

            # TP3 재시도(달성 보장)
            with _PENDING_LOCK:
                tp_items = list(_PENDING["tp"].items())
            for pkey, item in tp_items:
                sym, side = item["symbol"], item["side"]
                key = _key(sym, side)
                p = _get_remote(sym, side)
                if not p or _to_float(p.get("size")) <= 0:
                    _mark_done("tp", pkey, "(no-remote)")
                    continue

                cur_size  = _to_float(p.get("size"))
                init_size = _to_float(item.get("init_size") or cur_size)
                cut_size  = _to_float(item.get("cut_size") or cur_size)
                size_step = _to_float(item.get("size_step", 0.001))

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

# ============================================================================
# STRICT(상한) 예약/해제 — 숏만 대상
# ============================================================================
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

# ============================================================================
# 외부에서 호출
# ============================================================================
def start_watchdogs():
    threading.Thread(target=_watchdog_loop, name="emergency-stop-watchdog", daemon=True).start()
    if BE_ENABLE:
        threading.Thread(target=_breakeven_watchdog, name="breakeven-watchdog", daemon=True).start()

def start_reconciler():
    threading.Thread(target=_reconciler_loop, name="reconciler", daemon=True).start()
