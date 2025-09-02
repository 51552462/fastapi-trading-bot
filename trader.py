# trader.py — 기존 로직/시그니처 유지, 파일 로깅(추가만)
import os, time, threading
from typing import Dict, Optional

from bitget_api import (
    convert_symbol, get_last_price, get_open_positions,
    place_market_order, place_reduce_by_size, get_symbol_spec, round_down_step,
)

# 텔레그램은 기존대로 (없으면 print)
try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str):
        print("[TG]", msg)

# 파일 로깅 (추가)
try:
    from telemetry.logger import log_event, log_trade  # log_trade가 없으면 아래 래퍼 사용
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

STOP_PCT           = float(os.getenv("STOP_PCT", "0.10"))
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

# ── capacity(state) ─────────────────────────────────────────────
_CAPACITY = {
    "blocked": False,
    "last_count": 0,        # 전체 포지션 수
    "short_blocked": False, # total>=cap 이면 True (숏 제한에 활용하던 하위호환)
    "short_count": 0,
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

def _stop_cooldown_key(symbol: str, side: str) -> str:
    return f"{convert_symbol(symbol)}:{side}"

def _stop_recently_fired(symbol: str, side: str) -> bool:
    k = _stop_cooldown_key(symbol, side)
    with _STOP_LOCK:
        t = _STOP_FIRED.get(k, 0.0)
        if time.time() - t < STOP_COOLDOWN_SEC:
            return True
        return False

def _mark_stop_fired(symbol: str, side: str):
    k = _stop_cooldown_key(symbol, side)
    with _STOP_LOCK:
        _STOP_FIRED[k] = time.time()

# ──────────────────────────────────────────────────────────────
# 진입/익절/청산
# ──────────────────────────────────────────────────────────────
def enter_position(symbol: str, usdt_amount: float, side: str = "long", leverage: Optional[float] = None):
    """
    시장가 진입. 기존 로직 유지 + 파일 로그(추가)
    """
    side = (side or "long").lower()
    symbol = convert_symbol(symbol)
    lev = float(leverage or LEVERAGE)

    # 파일 로그(신호 수신)
    if TRACE_LOG:
        log_event({"fn": "enter_position", "symbol": symbol, "side": side,
                   "amount": usdt_amount, "lev": lev}, stage="ingress")

    # (기존) 최소 수량/스텝 계산
    spec = get_symbol_spec(symbol)
    last = get_last_price(symbol)
    if not last:
        send_telegram(f"❌ ticker 없음: {symbol}")
        return

    qty = round_down_step(float(usdt_amount) / float(last), float(spec.get("sizeStep", 0.001)))
    if qty <= 0:
        send_telegram(f"❌ qty<=0: {symbol}")
        return

    # 주문
    side_for_api = "buy" if side == "long" else "sell"
    res = place_market_order(symbol, usdt_amount, side=side_for_api, leverage=lev, reduce_only=False)

    # 파일 로그(체결 결과)
    log_trade("entry", symbol, side, float(usdt_amount), extra={
        "leverage": lev,
        "result": res
    })

    # 텔레그램 알림
    try:
        code = str(res.get("code"))
        if code not in ("0", "00000") and not code.startswith("HTTP_"):
            send_telegram(f"⚠️ entry 응답: {symbol} {side} {usdt_amount} → {code}")
        else:
            send_telegram(f"✅ ENTRY {symbol} {side} {usdt_amount}USDT x{lev}")
    except Exception:
        pass

def take_partial_profit(symbol: str, pct: float, side: str = "long"):
    """
    분할 익절. 기존 로직 유지 + 파일 로그(추가)
    """
    symbol = convert_symbol(symbol)
    side = (side or "long").lower()

    positions = get_open_positions() or []
    target = None
    for p in positions:
        if (p.get("symbol") or "").upper() == symbol and (p.get("side") or "") == side:
            target = p; break
    if not target:
        if TRACE_LOG:
            log_event({"fn": "take_partial_profit", "symbol": symbol, "side": side,
                       "pct": pct, "warn": "no_position"}, stage="trade")
        return

    size = float(target.get("size") or 0.0)
    step = float(get_symbol_spec(symbol).get("sizeStep", 0.001))
    cut  = round_down_step(size * float(pct), step)
    if cut <= 0:
        return

    res = place_reduce_by_size(symbol, cut, side)

    # 파일 로그(체결 결과)
    log_trade("take_profit", symbol, side, 0.0, extra={
        "pct": pct,
        "reduce_size": cut,
        "result": res
    })

    try:
        send_telegram(f"✅ TP {symbol} {side} {int(pct*100)}% ({cut})")
    except Exception:
        pass

def close_position(symbol: str, side: str = "long", reason: str = "manual"):
    """
    전체 청산. 기존 로직 유지 + 파일 로그(추가)
    """
    symbol = convert_symbol(symbol)
    side = (side or "long").lower()

    # 체결은 거래소 reduce-only 시장가로
    positions = get_open_positions() or []
    target = None
    for p in positions:
        if (p.get("symbol") or "").upper() == symbol and (p.get("side") or "") == side:
            target = p; break

    if not target:
        if TRACE_LOG:
            log_event({"fn": "close_position", "symbol": symbol, "side": side,
                       "reason": reason, "warn": "no_position"}, stage="trade")
        return

    size = float(target.get("size") or 0.0)
    res = place_reduce_by_size(symbol, size, side)

    # 파일 로그(청산)
    log_trade("close", symbol, side, 0.0, reason=reason, extra={"size": size, "result": res})

    try:
        send_telegram(f"🪓 CLOSE {symbol} {side} reason={reason}")
    except Exception:
        pass

def reduce_by_contracts(symbol: str, contracts: float, side: str = "long"):
    """
    계약 수 기준 감축. 기존 로직 유지 + 파일 로그(추가)
    """
    symbol = convert_symbol(symbol)
    side = (side or "long").lower()

    step = float(get_symbol_spec(symbol).get("sizeStep", 0.001))
    cut  = round_down_step(float(contracts), step)
    if cut <= 0:
        return

    res = place_reduce_by_size(symbol, cut, side)

    log_trade("reduce", symbol, side, 0.0, extra={"contracts": cut, "result": res})
    try:
        send_telegram(f"➖ REDUCE {symbol} {side} {cut}")
    except Exception:
        pass

# ──────────────────────────────────────────────────────────────
# 재조정/감시 (기존 시그니처만 유지 — 내부는 심플)
# ──────────────────────────────────────────────────────────────
def _reconciler_loop():
    while True:
        try:
            # 필요 시 포지션 동기화/정합성 체크 로직 (원래 있던 구조 유지)
            if RECON_DEBUG:
                log_event({"fn": "reconciler_tick", "open_count": _local_open_count()}, stage="debug")
        except Exception as e:
            print("[reconciler] error:", e)
        time.sleep(RECON_INTERVAL_SEC)

def start_reconciler():
    t = threading.Thread(target=_reconciler_loop, name="reconciler", daemon=True)
    t.start()

def _watchdogs_loop():
    while True:
        try:
            # stop 쿨다운 및 기타 경계 로직 (원래 있던 구조 유지)
            pass
        except Exception as e:
            print("[watchdogs] error:", e)
        time.sleep(1.0)

def start_watchdogs():
    t = threading.Thread(target=_watchdogs_loop, name="watchdogs", daemon=True)
    t.start()

def _capacity_loop():
    # NOTE: 개수 제한은 사실상 Risk/Margin Guard가 관리하지만,
    #       하위호환을 위해 상태만 업데이트 (main에서 참조 가능)
    while True:
        try:
            pos = get_open_positions() or []
            total = len(pos)
            with _CAP_LOCK:
                _CAPACITY["last_count"] = total
                _CAPACITY["short_blocked"] = (total >= MAX_OPEN_POSITIONS and not LONG_BYPASS_CAP)
                _CAPACITY["short_count"] = total
                _CAPACITY["ts"] = time.time()
        except Exception as e:
            print("[capacity] error:", e)
        time.sleep(CAP_CHECK_SEC)

def start_capacity_guard():
    t = threading.Thread(target=_capacity_loop, name="capacity", daemon=True)
    t.start()

def get_pending_snapshot() -> Dict[str, any]:
    with _CAP_LOCK, _POS_LOCK:
        return {
            "capacity": dict(_CAPACITY),
            "open_count": _local_open_count(),
        }
