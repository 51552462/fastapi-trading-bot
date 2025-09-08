# trader.py — full, no omissions (syntax-safe)
# 기능 요약:
# - enter_position / take_partial_profit / reduce_by_contracts / close_position
# - capacity guard (롱/숏 분리 허용 스위치)
# - emergency cut ① 가격 급락(STOP_PRICE_MOVE) + ② 언리얼 PnL(EMERGENCY_UNREAL_PNL_PCT, 선택)
# - sizeStep rounding for reduce/TP (exchange min step 400 오류 방지)
# - reconciler(재시도), pending snapshot, watchdogs
# - main.py와 인터페이스 100% 호환
# - /admin/params 런타임 오버라이드 수용(apply_runtime_overrides)

import os, time, threading
from typing import Dict, Optional

from bitget_api import (
    convert_symbol, get_last_price, get_open_positions,
    place_market_order, place_reduce_by_size, get_symbol_spec, round_down_step,
)

# ─────────────────────────────────────────────────────────────
# Telegram 채널 (없으면 print로 대체)
# ─────────────────────────────────────────────────────────────
try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str):
        print("[TG]", msg)

# ─────────────────────────────────────────────────────────────
# Telemetry logger (없으면 콘솔로 대체)
# ─────────────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────────────
# (선택) 언리얼 PnL 컷
# ─────────────────────────────────────────────────────────────
try:
    from pnl_guard import should_pnl_cut    # EMERGENCY_UNREAL_PNL_PCT(음수) 일 때만 트리거
except Exception:
    def should_pnl_cut(*args, **kwargs):    # 폴백: 비활성
        return False

# ─────────────────────────────────────────────────────────────
# ENV
# ─────────────────────────────────────────────────────────────
LEVERAGE = float(os.getenv("LEVERAGE", "5"))
TRACE_LOG = os.getenv("TRACE_LOG", "0") == "1"

TP1_PCT = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT = float(os.getenv("TP2_PCT", "0.40"))
TP3_PCT = float(os.getenv("TP3_PCT", "0.30"))

DEFAULT_AMOUNT = float(os.getenv("DEFAULT_AMOUNT", "15"))

# 긴급 stop 파라미터(가격)
STOP_CONFIRM_N    = int(float(os.getenv("STOP_CONFIRM_N", "3")))
STOP_DEBOUNCE_SEC = float(os.getenv("STOP_DEBOUNCE_SEC", "2"))
STOP_COOLDOWN_SEC = float(os.getenv("STOP_COOLDOWN_SEC", "3"))
STOP_CHECK_SEC    = float(os.getenv("STOP_CHECK_SEC", "2"))
STOP_PCT          = float(os.getenv("STOP_PRICE_MOVE", "0.02"))  # 예: 0.10 = -10%

# Reconciler/Watchdog
RECON_INTERVAL_SEC = float(os.getenv("RECON_INTERVAL_SEC", "2"))
RECON_DEBUG        = os.getenv("RECON_DEBUG", "0") == "1"

# capacity guard
MAX_OPEN_POSITIONS = int(float(os.getenv("MAX_OPEN_POSITIONS", "120")))
CAP_CHECK_SEC      = float(os.getenv("CAP_CHECK_SEC", "5"))
LONG_BYPASS_CAP    = (os.getenv("LONG_BYPASS_CAP", "0") == "1")

# ─────────────────────────────────────────────────────────────
# Local position cache (성능/호환)
# ─────────────────────────────────────────────────────────────
position_data: Dict[str, Dict] = {}   # key: f"{sym}_{side}" → {size, entry, opened_ts}

_POS_LOCK = threading.RLock()
_CAP_LOCK = threading.RLock()

_CAPACITY = {
    "blocked": False,
    "last_count": 0,
    "short_blocked": False,
    "long_blocked": False,
    "short_count": 0,
    "long_count": 0,
    "ts": 0.0,
}

_PENDING = {"entry": {}, "close": {}, "tp": {}}
_PENDING_LOCK = threading.RLock()

def _key(symbol: str, side: str) -> str:
    side = (side or "").strip().lower()
    if side.startswith("l"): side = "long"
    if side.startswith("s"): side = "short"
    return f"{symbol}_{side}"

def _has_local_position(sym: str) -> bool:
    with _POS_LOCK:
        for k in position_data.keys():
            if k.startswith(sym + "_"):
                return True
    return False

# busy / recent (진입 중복 방지)
_BUSY: Dict[str, float] = {}
_RECENT: Dict[str, float] = {}
_BUSY_LOCK = threading.RLock()
_RECENT_LOCK = threading.RLock()

def _is_busy(key: str, within: float = None) -> bool:
    within = within or ENTRY_INFLIGHT_TTL_SEC
    with _BUSY_LOCK:
        t = _BUSY.get(key, 0.0)
        return time.time() - t < within

def _set_busy(key: str):
    with _BUSY_LOCK:
        _BUSY[key] = time.time()

def _recent_ok(key: str, within: float = None) -> bool:
    within = within or ENTRY_DUP_TTL_SEC
    with _RECENT_LOCK:
        t = _RECENT.get(key, 0.0)
        return time.time() - t < within

def _mark_recent_ok(key: str):
    with _RECENT_LOCK:
        _RECENT[key] = time.time()

ENTRY_INFLIGHT_TTL_SEC = float(os.getenv("ENTRY_INFLIGHT_TTL_SEC", "10"))
ENTRY_DUP_TTL_SEC      = float(os.getenv("ENTRY_DUP_TTL_SEC", "3"))

# ─────────────────────────────────────────────────────────────
# Capacity guard
# ─────────────────────────────────────────────────────────────
def _count_positions() -> Dict[str, int]:
    long_c = short_c = 0
    for p in get_open_positions():
        side = (p.get("side") or "").lower()
        if side == "long": long_c += 1
        elif side == "short": short_c += 1
    return {"long": long_c, "short": short_c}

def _cap_guard_tick():
    with _CAP_LOCK:
        c = _count_positions()
        _CAPACITY["last_count"] = c["long"] + c["short"]
        _CAPACITY["long_count"] = c["long"]
        _CAPACITY["short_count"] = c["short"]
        _CAPACITY["long_blocked"] = (c["long"] >= MAX_OPEN_POSITIONS) and not LONG_BYPASS_CAP
        _CAPACITY["short_blocked"] = (c["short"] >= MAX_OPEN_POSITIONS)
        _CAPACITY["blocked"] = _CAPACITY["last_count"] >= MAX_OPEN_POSITIONS
        _CAPACITY["ts"] = time.time()

def _cap_guard_loop():
    while True:
        try:
            _cap_guard_tick()
        except Exception as e:
            print("cap_guard err:", e)
        time.sleep(CAP_CHECK_SEC)

def start_capacity_guard():
    threading.Thread(target=_cap_guard_loop, name="cap-guard", daemon=True).start()

# ─────────────────────────────────────────────────────────────
# Local position helpers
# ─────────────────────────────────────────────────────────────
def _set_local(symbol: str, side: str, size: float, entry: float):
    with _POS_LOCK:
        position_data[_key(symbol, side)] = {
            "size": size, "entry": entry, "opened_ts": time.time()
        }

def _rm_local(symbol: str, side: str):
    with _POS_LOCK:
        position_data.pop(_key(symbol, side), None)

def _upd_local_qty(symbol: str, side: str, new_size: float):
    with _POS_LOCK:
        k = _key(symbol, side)
        if k in position_data:
            position_data[k]["size"] = new_size

# ─────────────────────────────────────────────────────────────
# 진입/TP/감축/종료
# ─────────────────────────────────────────────────────────────
def enter_position(symbol: str, side: str = "long", usdt_amount: Optional[float] = None,
                   leverage: float = None, timeframe: Optional[str] = None):
    """
    간단한 시장가 진입 (금액 기반 → 수량 추정)
    """
    symbol = convert_symbol(symbol)
    side = (side or "").lower().strip()
    amount = float(usdt_amount or DEFAULT_AMOUNT)
    price = float(get_last_price(symbol) or 0.0)
    if price <= 0:
        send_telegram(f"⚠️ enter skip {side.upper()} {symbol} no price")
        return {"ok": False, "reason": "no_price"}

    qty = amount / price
    try:
        spec = get_symbol_spec(symbol)
        qty = round_down_step(qty, spec.get("sizeStep"))
    except Exception:
        pass
    if qty <= 0:
        return {"ok": False, "reason": "too_small"}

    if (_CAPACITY["blocked"]) or (_CAPACITY["long_blocked"] and side=="long") or (_CAPACITY["short_blocked"] and side=="short"):
        send_telegram(f"⛔ capacity block {side.upper()} {symbol}")
        return {"ok": False, "reason": "cap_block"}

    resp = place_market_order(symbol, qty, side)
    if str(resp.get("code","")) == "00000":
        send_telegram(f"✅ ENTER {side.upper()} {symbol} amt≈{amount} qty≈{qty}")
        _set_local(symbol, side, qty, price)
        log_trade("entry", symbol, side, qty, extra={"tf": timeframe or ""})
        return {"ok": True}
    else:
        send_telegram(f"❌ ENTER FAIL {side.upper()} {symbol} → {resp}")
        return {"ok": False, "reason": "exchange"}

def reduce_by_contracts(symbol: str, contracts: float, side: str = "long"):
    symbol = convert_symbol(symbol)
    side = (side or "").lower().strip()
    if contracts <= 0:
        return {"ok": False, "reason": "bad_contracts"}
    # 'by contracts' 요청은 내부적으로 ratio처럼 처리(거래소 sizeStep 맞춤)
    for p in get_open_positions():
        if (p.get("symbol") == symbol) and ((p.get("side") or "").lower() == side):
            size = float(p.get("size") or 0.0)
            if size <= 0:
                break
            ratio = contracts / size
            ratio = max(0.0, min(1.0, ratio))
            return take_partial_profit(symbol, ratio=ratio, side=side, reason="by_contracts")
    return {"ok": False, "reason": "no_position"}

def take_partial_profit(symbol: str, ratio: float, side: str = "long", reason: str = "tp"):
    """
    현재 포지션 사이즈의 ratio 만큼 감축 (0<ratio<=1)
    거래소 sizeStep에 맞게 round_down_step 적용해서 400 방지
    """
    symbol = convert_symbol(symbol)
    side = (side or "").lower().strip()
    if ratio <= 0 or ratio > 1:
        return {"ok": False, "reason": "bad_ratio"}

    for p in get_open_positions():
        if (p.get("symbol") == symbol) and ((p.get("side") or "").lower() == side):
            size = float(p.get("size") or 0.0)
            if size <= 0:
                break
            cut = size * float(ratio)
            try:
                spec = get_symbol_spec(symbol)
                cut = round_down_step(cut, spec.get("sizeStep"))
            except Exception:
                pass
            if cut <= 0:
                return {"ok": False, "reason": "too_small"}

            resp = place_reduce_by_size(symbol, cut, side)
            if str(resp.get("code", "")) == "00000":
                send_telegram(f"✂️ TP {side.upper()} {symbol} ratio={ratio:.2f} size≈{cut}")
                log_trade("tp", symbol, side, cut, reason=reason)
                # 로컬 사이즈 갱신
                with _POS_LOCK:
                    k = _key(symbol, side)
                    if k in position_data:
                        position_data[k]["size"] = max(0.0, float(position_data[k]["size"]) - cut)
                        if position_data[k]["size"] <= 1e-9:
                            position_data.pop(k, None)
                return {"ok": True}
            else:
                send_telegram(f"❌ TP FAIL {side.upper()} {symbol} → {resp}")
                return {"ok": False, "reason": "exchange"}
    return {"ok": False, "reason": "no_position"}

def close_position(symbol: str, side: str = "long", reason: str = "close"):
    symbol = convert_symbol(symbol)
    side = (side or "").lower().strip()
    # 거래소 포지션 조회
    for p in get_open_positions():
        if (p.get("symbol") == symbol) and ((p.get("side") or "").lower() == side):
            size = float(p.get("size") or 0.0)
            if size <= 0:
                break
            try:
                spec = get_symbol_spec(symbol)
                size = round_down_step(size, spec.get("sizeStep"))
            except Exception:
                pass
            if size <= 0:
                send_telegram(f"⚠️ CLOSE SKIP {side.upper()} {symbol} size≈0 after step")
                return
            try:
                resp = place_reduce_by_size(symbol, size, side)
                if str(resp.get("code", "")) == "00000":
                    _rm_local(symbol, side)
                    _mark_recent_ok(_key(symbol, side))
                    send_telegram(f"✅ CLOSE ALL {side.upper()} {symbol} ({reason})")
                    log_trade("close", symbol, side, size, reason=reason)
                else:
                    send_telegram(f"❌ CLOSE FAIL {side.upper()} {symbol} → {resp}")
            except Exception as e:
                send_telegram(f"❌ CLOSE EXC {side.upper()} {symbol}: {e}")

# ─────────────────────────────────────────────────────────────
# Watchdogs (긴급 컷)
# ─────────────────────────────────────────────────────────────
_STOP_RECENT: Dict[str, float] = {}
_STOP_CNT: Dict[str, int] = {}
_STOP_LOCK = threading.RLock()

def _stop_recently_fired(symbol: str, side: str) -> bool:
    with _STOP_LOCK:
        t = _STOP_RECENT.get(_key(symbol, side), 0.0)
        return (time.time() - t) < STOP_COOLDOWN_SEC

def _mark_stop_fired(symbol: str, side: str):
    with _STOP_LOCK:
        _STOP_RECENT[_key(symbol, side)] = time.time()

def _inc_stop_confirm(symbol: str, side: str) -> int:
    with _STOP_LOCK:
        k = _key(symbol, side)
        _STOP_CNT[k] = _STOP_CNT.get(k, 0) + 1
        return _STOP_CNT[k]

def _reset_stop_confirm(symbol: str, side: str):
    with _STOP_LOCK:
        _STOP_CNT[_key(symbol, side)] = 0

def _watchdog_loop():
    """
    - 긴급 가격 컷(STOP_PRICE_MOVE): '연속 STOP_CONFIRM_N회' 확인 후 발동(휩쏘 방지)
    - 언리얼 PnL 컷(should_pnl_cut): 설정 시 즉시 발동(보수적 사용 권장)
    - 컷 후 STOP_COOLDOWN_SEC 동안 동일 포지션 재발 방지
    """
    last_tick = 0.0
    while True:
        try:
            now = time.time()
            if now - last_tick < STOP_CHECK_SEC:
                time.sleep(0.1); continue
            last_tick = now

            with _POS_LOCK:
                items = list(position_data.items())

            for k, p in items:
                sym, side = k.split("_", 1)
                entry = float(p.get("entry") or 0.0)
                size  = float(p.get("size") or 0.0)
                if entry <= 0 or size <= 0:
                    continue

                mark = float(get_last_price(sym) or 0.0)
                if mark <= 0:
                    continue

                # ---- (1) 언리얼 PnL 컷: 설정 시 가장 먼저 체크(더 보수적) ----
                if should_pnl_cut(side, mark, entry):
                    send_telegram(f"🛑 E-STOP(PnL) {side.upper()} {sym}")
                    close_position(sym, side, reason="pnl_guard")
                    _mark_stop_fired(sym, side)
                    continue

                # ---- (2) 가격 변동 기반 긴급 컷 ----
                if side == "long":
                    moved = (entry - mark) / max(1e-9, entry)
                else:
                    moved = (mark - entry) / max(1e-9, entry)

                if _stop_recently_fired(sym, side):
                    continue

                if moved >= STOP_PCT:
                    cnt = _inc_stop_confirm(sym, side)
                    if cnt >= STOP_CONFIRM_N:
                        send_telegram(f"🛑 E-STOP(PRICE) {side.upper()} {sym} moved={moved:.4f}")
                        close_position(sym, side, reason="price_guard")
                        _mark_stop_fired(sym, side)
                else:
                    _reset_stop_confirm(sym, side)

        except Exception as e:
            print("watchdog error:", e)
        time.sleep(0.05)

def start_watchdogs():
    threading.Thread(target=_watchdog_loop, name="watchdog", daemon=True).start()

# ─────────────────────────────────────────────────────────────
# Reconciler
# ─────────────────────────────────────────────────────────────
def _reconciler_loop():
    last_try = 0.0
    while True:
        try:
            now = time.time()
            if now - last_try < RECON_INTERVAL_SEC:
                time.sleep(0.1); continue
            last_try = now

            # pending 재시도
            with _PENDING_LOCK:
                pend = dict(_PENDING)

            # close 재시도
            for pkey, item in list(pend["close"].items()):
                sym, side = pkey.split("_", 1)
                remain = float(item.get("remain") or 0.0)
                if remain <= 0:
                    with _PENDING_LOCK: _PENDING["close"].pop(pkey, None)
                    continue
                if now - item.get("last_try", 0.0) < RECON_INTERVAL_SEC:
                    continue
                try:
                    resp = place_reduce_by_size(sym, remain, side)
                    item["last_try"] = now
                    item["attempts"] = item.get("attempts", 0) + 1
                    if str(resp.get("code", "")) == "00000":
                        send_telegram(f"🔁 CLOSE 재시도 {side.upper()} {sym} remain≈{remain}")
                        with _PENDING_LOCK: _PENDING["close"].pop(pkey, None)
                except Exception as e:
                    print("recon close err:", e)

            # tp 재시도 (TP3 잔량 등)
            for pkey, item in list(pend["tp"].items()):
                sym, side = pkey.split("_", 1)
                remain = float(item.get("remain") or 0.0)
                if remain <= 0:
                    with _PENDING_LOCK: _PENDING["tp"].pop(pkey, None)
                    continue
                if now - item.get("last_try", 0.0) < RECON_INTERVAL_SEC:
                    continue
                try:
                    resp = place_reduce_by_size(sym, remain, side)
                    item["last_try"] = now
                    item["attempts"] = item.get("attempts", 0) + 1
                    if str(resp.get("code", "")) == "00000":
                        send_telegram(f"🔁 TP 재시도 감축 {side.upper()} {sym} remain≈{remain}")
                except Exception as e:
                    print("recon tp err:", e)

        except Exception as e:
            print("reconciler error:", e)

def start_reconciler():
    threading.Thread(target=_reconciler_loop, name="reconciler", daemon=True).start()

# ─────────────────────────────────────────────────────────────
# Snapshot
# ─────────────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────────────
# 런타임 오버라이드 (/admin/params)
# ─────────────────────────────────────────────────────────────
def _to_float(x, default):
    try: return float(x)
    except: return default

def _to_int(x, default):
    try: return int(float(x))
    except: return default

def apply_runtime_overrides(changed: dict):
    global STOP_PCT, STOP_CHECK_SEC, STOP_COOLDOWN_SEC, STOP_CONFIRM_N, STOP_DEBOUNCE_SEC
    global TP1_PCT, TP2_PCT, TP3_PCT
    global RECON_INTERVAL_SEC, MAX_OPEN_POSITIONS, CAP_CHECK_SEC, LONG_BYPASS_CAP

    if "STOP_PRICE_MOVE"   in changed: STOP_PCT          = _to_float(changed["STOP_PRICE_MOVE"], STOP_PCT)
    if "STOP_CHECK_SEC"    in changed: STOP_CHECK_SEC    = _to_float(changed["STOP_CHECK_SEC"], STOP_CHECK_SEC)
    if "STOP_COOLDOWN_SEC" in changed: STOP_COOLDOWN_SEC = _to_float(changed["STOP_COOLDOWN_SEC"], STOP_COOLDOWN_SEC)
    if "STOP_CONFIRM_N"    in changed: STOP_CONFIRM_N    = _to_int(  changed["STOP_CONFIRM_N"], STOP_CONFIRM_N)
    if "STOP_DEBOUNCE_SEC" in changed: STOP_DEBOUNCE_SEC = _to_float(changed["STOP_DEBOUNCE_SEC"], STOP_DEBOUNCE_SEC)

    if "TP1_PCT" in changed: TP1_PCT = _to_float(changed["TP1_PCT"], TP1_PCT)
    if "TP2_PCT" in changed: TP2_PCT = _to_float(changed["TP2_PCT"], TP2_PCT)
    if "TP3_PCT" in changed: TP3_PCT = _to_float(changed["TP3_PCT"], TP3_PCT)

    if "RECON_INTERVAL_SEC" in changed: RECON_INTERVAL_SEC = _to_float(changed["RECON_INTERVAL_SEC"], RECON_INTERVAL_SEC)
    if "MAX_OPEN_POSITIONS" in changed: MAX_OPEN_POSITIONS = _to_int(  changed["MAX_OPEN_POSITIONS"], MAX_OPEN_POSITIONS)
    if "CAP_CHECK_SEC" in changed: CAP_CHECK_SEC = _to_float(changed["CAP_CHECK_SEC"], CAP_CHECK_SEC)
    if "LONG_BYPASS_CAP" in changed: LONG_BYPASS_CAP = True if str(changed["LONG_BYPASS_CAP"])=="1" else False
