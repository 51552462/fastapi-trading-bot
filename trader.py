# trader.py — 전체본 (main.py와 인터페이스 100% 호환, 기존 로직 유지)
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
# 파일 로깅 (telemetry/logger.py가 없으면 콘솔 대체)
# ──────────────────────────────────────────────────────────────
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

# ──────────────────────────────────────────────────────────────
# 환경 변수
# ──────────────────────────────────────────────────────────────
LEVERAGE = float(os.getenv("LEVERAGE", "5"))
TRACE_LOG = os.getenv("TRACE_LOG", "0") == "1"

TP1_PCT = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT = float(os.getenv("TP2_PCT", "0.40"))
TP3_PCT = float(os.getenv("TP3_PCT", "0.30"))

# 레버리지 5배 기준 진입가 대비 -2% 가격 이동이면 ≒ 손실 -10% → 즉시 종료
STOP_PRICE_MOVE   = float(os.getenv("STOP_PRICE_MOVE", "0.02"))   # 0.02 = 2%
STOP_CHECK_SEC    = float(os.getenv("STOP_CHECK_SEC", "1.0"))
STOP_COOLDOWN_SEC = float(os.getenv("STOP_COOLDOWN_SEC", "5.0"))

RECON_INTERVAL_SEC = float(os.getenv("RECON_INTERVAL_SEC", "40"))
TP_EPSILON_RATIO   = float(os.getenv("TP_EPSILON_RATIO", "0.001"))
RECON_DEBUG        = os.getenv("RECON_DEBUG", "0") == "1"

MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "40"))
CAP_CHECK_SEC      = float(os.getenv("CAP_CHECK_SEC", "10"))
LONG_BYPASS_CAP    = os.getenv("LONG_BYPASS_CAP", "1") == "1"

ENTRY_INFLIGHT_TTL_SEC = float(os.getenv("ENTRY_INFLIGHT_TTL_SEC", "30"))
ENTRY_DUP_TTL_SEC      = float(os.getenv("ENTRY_DUP_TTL_SEC", "60"))

# ──────────────────────────────────────────────────────────────
# 내부 상태
# ──────────────────────────────────────────────────────────────
position_data: Dict[str, dict] = {}
_POS_LOCK = threading.RLock()

def _key(symbol: str, side: str) -> str:
    return f"{convert_symbol(symbol)}_{(side or '').lower()}"

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
        position_data[_key(symbol, side)] = {"size": size, "entry": entry, "ts": time.time()}

def _rm_local(symbol: str, side: str):
    with _POS_LOCK:
        position_data.pop(_key(symbol, side), None)

def _get_remote_any_side(symbol: str) -> bool:
    for p in get_open_positions():
        if p.get("symbol") == convert_symbol(symbol) and float(p.get("size") or 0) > 0:
            return True
    return False

# busy / recent
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

# per-key lock
_KEY_LOCKS: Dict[str, threading.RLock] = {}
_KEY_LOCKS_LOCK = threading.RLock()
def _lock_for(key: str) -> threading.RLock:
    with _KEY_LOCKS_LOCK:
        if key not in _KEY_LOCKS:
            _KEY_LOCKS[key] = threading.RLock()
    return _KEY_LOCKS[key]

# ──────────────────────────────────────────────────────────────
# Capacity Guard
# ──────────────────────────────────────────────────────────────
_CAPACITY = {"blocked": False, "last_count": 0, "short_blocked": False, "short_count": 0, "ts": 0.0}
_CAP_LOCK = threading.Lock()
_cap_thread: Optional[threading.Thread] = None

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
        _CAPACITY["short_blocked"] = blocked  # 보수적으로 동일 적용

def capacity_status() -> Dict:
    with _CAP_LOCK:
        d = dict(_CAPACITY)
        d["max"] = MAX_OPEN_POSITIONS
        return d

def _capacity_loop():
    last_blocked = None; last_short = None
    while True:
        try:
            _update_capacity()
            st = capacity_status()
            if last_blocked != st["blocked"]:
                last_blocked = st["blocked"]
                if st["blocked"]:
                    send_telegram(f"ℹ️ Capacity BLOCKED {st['last_count']}/{st['max']}")
                else:
                    send_telegram("ℹ️ Capacity UNBLOCKED")
            if last_short != st["short_blocked"]:
                last_short = st["short_blocked"]
        except Exception as e:
            print("capacity error:", e)
        time.sleep(CAP_CHECK_SEC)

def start_capacity_guard():
    global _cap_thread
    if _cap_thread and _cap_thread.is_alive():
        return
    _cap_thread = threading.Thread(target=_capacity_loop, name="capacity-guard", daemon=True)
    _cap_thread.start()

# ──────────────────────────────────────────────────────────────
# Pending registry & snapshot (reconciler에서 사용)
# ──────────────────────────────────────────────────────────────
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

# ──────────────────────────────────────────────────────────────
# Stop(긴급 -2%) 연속 트리거 방지
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

# ──────────────────────────────────────────────────────────────
# 외부에서 호출하는 API들 (main.py가 import)
# enter_position, take_partial_profit, close_position, reduce_by_contracts
# ──────────────────────────────────────────────────────────────
def enter_position(symbol: str, usdt_amount: float, side: str = "long",
                   leverage: Optional[float] = None):
    """
    NOTE: main.py가 enter_position(symbol, amount, side=..., leverage=...) 형태로 호출하므로
          두 번째 인자는 반드시 amount 입니다. (시그니처 주의)  :contentReference[oaicite:1]{index=1}
    """
    side = (side or "").lower().strip(); key = _key(symbol, side)
    if side not in ("long","short"): return {"ok": False, "reason": "bad_side"}

    if _is_busy(key): return {"ok": False, "reason": "busy"}
    _set_busy(key)

    # 엄격 슬롯(숏 보호) – 실패시 조용히 거절(기존 동작 유지)
    st = capacity_status()
    if st["short_blocked"] and side == "short":
        send_telegram(f"🧱 STRICT HOLD {convert_symbol(symbol)} {side} {st['short_count']}/{st['max']}")
        return {"ok": False, "reason": "strict_hold"}

    if st["blocked"] and not LONG_BYPASS_CAP and side == "short":
        send_telegram(f"🧱 STRICT HOLD {convert_symbol(symbol)} {side} {st['last_count']}/{st['max']}")
        return {"ok": False, "reason": "cap_blocked"}

    if _recent_ok(key): return {"ok": False, "reason": "recent"}
    if _local_has_any(symbol): return {"ok": False, "reason": "local_exists"}
    if _get_remote_any_side(symbol): return {"ok": False, "reason": "remote_exists"}

    lev = float(leverage or LEVERAGE)
    try:
        resp = place_market_order(symbol, usdt_amount, "buy" if side == "long" else "sell", leverage=lev)
    except Exception as e:
        send_telegram(f"❌ ENTRY EXC {convert_symbol(symbol)} {side}: {e}")
        return {"ok": False, "reason": "exception", "error": str(e)}

    if str(resp.get("code","")) == "00000":
        _mark_ok(key)
        send_telegram(f"🚀 ENTRY {side.upper()} {convert_symbol(symbol)} amt≈{usdt_amount} lev={lev}x")
        log_trade("entry", convert_symbol(symbol), side, usdt_amount, extra={"lev": lev})
        return {"ok": True}
    else:
        send_telegram(f"❌ ENTRY FAIL {convert_symbol(symbol)} {side}: {resp}")
        return {"ok": False, "reason": "api_fail", "resp": resp}

def take_partial_profit(symbol: str, ratio: float, side: str = "long", reason: str = "tp"):
    """
    부분청산: 현재 포지션 사이즈의 ratio 만큼 감축.
    - ratio는 0~1 사이 (예: 0.3 = 30%)
    """
    symbol = convert_symbol(symbol); side = (side or "").lower().strip()
    if ratio <= 0: return {"ok": False, "reason": "zero_ratio"}
    for p in get_open_positions():
        if p.get("symbol") == symbol and (p.get("side") or "").lower() == side:
            size = float(p.get("size") or 0.0)
            if size <= 0: break
            cut = size * float(ratio)
            try:
                spec = get_symbol_spec(symbol)  # sizeStep 보정
                cut = round_down_step(cut, spec.get("sizeStep"))
            except Exception:
                pass
            if cut <= 0: return {"ok": False, "reason": "too_small"}

            resp = place_reduce_by_size(symbol, cut, side)
            if str(resp.get("code", "")) == "00000":
                send_telegram(f"✂️ TP {side.upper()} {symbol} ratio={ratio:.2f} size≈{cut}")
                log_trade("tp", symbol, side, cut, reason=reason)
                return {"ok": True, "reduced": cut}
            else:
                send_telegram(f"❌ TP FAIL {side.upper()} {symbol} → {resp}")
                return {"ok": False, "reason": "api_fail", "resp": resp}
    return {"ok": False, "reason": "no_position"}

def reduce_by_contracts(symbol: str, contracts: float, side: str = "long",
                         reason: str = "reduceByContracts") -> dict:
    """
    계약 수(=사이즈) 기준으로 포지션을 줄이는 래퍼.
    main.py에서 `reduce_by_contracts(symbol, contracts, side=...)`로 호출.  :contentReference[oaicite:2]{index=2}
    """
    try:
        sym = convert_symbol(symbol)
        s = (side or "").lower().strip()
        if s not in ("long", "short"):
            return {"ok": False, "reason": "bad_side"}

        # 현재 보유 사이즈 확인
        cur_size = 0.0
        for p in get_open_positions():
            if p.get("symbol") == sym and (p.get("side") or "").lower() == s:
                cur_size = float(p.get("size") or 0.0)
                break
        if cur_size <= 0:
            return {"ok": False, "reason": "no_position"}

        qty = float(contracts or 0.0)
        if qty <= 0:
            return {"ok": False, "reason": "zero_qty"}

        # 보유 수량 초과 방지 + 거래소 step 보정
        try:
            spec = get_symbol_spec(sym)  # {'sizeStep': ..., ...}
            qty = min(cur_size, qty)
            qty = round_down_step(qty, spec.get("sizeStep"))
        except Exception:
            qty = min(cur_size, qty)

        if qty <= 0:
            return {"ok": False, "reason": "too_small_after_step"}

        resp = place_reduce_by_size(sym, qty, s)
        if str(resp.get("code", "")) == "00000":
            try:
                send_telegram(f"✂️ REDUCE {s.upper()} {sym} -{qty:.6f} ({reason})")
            except Exception:
                pass
            log_trade("reduce", sym, s, qty, reason=reason)
            return {"ok": True, "reduced": qty}
        else:
            try:
                send_telegram(f"❌ REDUCE FAIL {s.upper()} {sym} {qty:.6f} → {resp}")
            except Exception:
                pass
            return {"ok": False, "reason": "api_fail", "resp": resp}

    except Exception as e:
        try:
            send_telegram(f"❌ REDUCE EXC {side.upper()} {symbol}: {e}")
        except Exception:
            pass
        return {"ok": False, "reason": "exception", "error": str(e)}

def close_position(symbol: str, side: str, reason: str = "manual"):
    symbol = convert_symbol(symbol); side = (side or "").lower().strip()
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

# ──────────────────────────────────────────────────────────────
# TP/BE 계산 헬퍼
# ──────────────────────────────────────────────────────────────
def _tp_targets(entry: float, side: str):
    eps = TP_EPSILON_RATIO
    if side == "long":
        return (entry*(1+TP1_PCT), entry*(1+TP2_PCT), entry*(1+TP3_PCT), entry*(1+eps))
    else:
        return (entry*(1-TP1_PCT), entry*(1-TP2_PCT), entry*(1-TP3_PCT), entry*(1-eps))

# ──────────────────────────────────────────────────────────────
# Watchdogs (긴급 -2% 손절 + BE 감시 훅)
# ──────────────────────────────────────────────────────────────
def _watchdog_loop():
    while True:
        try:
            for p in get_open_positions():
                symbol = p.get("symbol"); side = (p.get("side") or "").lower()
                entry  = float(p.get("entry_price") or 0); size = float(p.get("size") or 0)
                if not symbol or side not in ("long","short") or entry <= 0 or size <= 0: continue
                last = get_last_price(symbol)
                if not last: continue

                # 가격 변동률이 STOP_PRICE_MOVE 이상(예: -2%)이면 즉시 종료
                move = ((entry-last)/entry if side=="long" else (last-entry)/entry)
                if move >= STOP_PRICE_MOVE:
                    if not _stop_recently_fired(symbol, side):
                        _mark_stop_fired(symbol, side)
                        send_telegram(f"⛔ {symbol} {side.upper()} emergencyStop ≥{STOP_PRICE_MOVE*100:.2f}%")
                        close_position(symbol, side=side, reason="emergencyStop")
        except Exception as e:
            print("watchdog error:", e)
        time.sleep(STOP_CHECK_SEC)

def _breakeven_watchdog():
    # 기존 로직 유지(필요 시 BE 이동 훅 추가 가능)
    while True:
        try:
            for p in get_open_positions():
                symbol = p.get("symbol"); side = (p.get("side") or "").lower()
                entry  = float(p.get("entry_price") or 0)
                if not symbol or side not in ("long","short") or entry <= 0: continue
                last = get_last_price(symbol)
                if not last: continue
                tp1, tp2, tp3, be_px = _tp_targets(entry, side)
                # BE 이동/조건 훅 — 현재는 유지 (요청 시 세부 로직 넣을 수 있음)
                if side == "long":
                    if last >= be_px: pass
                else:
                    if last <= be_px: pass
        except Exception as e:
            print("breakeven watchdog error:", e)
        time.sleep(0.8)

def start_watchdogs():
    threading.Thread(target=_watchdog_loop, name="emergency-stop-watchdog", daemon=True).start()
    threading.Thread(target=_breakeven_watchdog, name="breakeven-watchdog", daemon=True).start()

# ──────────────────────────────────────────────────────────────
# Reconciler (재시도 루프) — 기존 동작 유지, 보조 훅은 no-op로 안전하게 정의
# ──────────────────────────────────────────────────────────────
def _strict_try_reserve(side: str) -> bool:
    # 슬롯 예약 등의 강화 로직을 쓸 수 있으나, 기본은 통과
    return True

def can_enter_now(side: str) -> bool:
    # 시간/슬롯 제약을 걸고 싶으면 여기서 제어. 기본은 통과
    return True

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
                        if str(resp.get("code","")) == "00000":
                            _mark_ok(key)
                            _mark_done("close", pkey, "(success)")
                            send_telegram(f"🔁 CLOSE 재시도 성공 {side.upper()} {sym}")
                        else:
                            if RECON_DEBUG:
                                send_telegram(f"🔁 CLOSE 재시도 실패 {side.upper()} {sym} → {resp}")
                    except Exception as e:
                        print("recon close err:", e)

            # TP3(감축) 재시도
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

def start_reconciler():
    threading.Thread(target=_reconciler_loop, name="reconciler", daemon=True).start()
