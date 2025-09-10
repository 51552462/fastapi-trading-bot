# trader_spot.py
import os
import time
import threading
from typing import Dict

from bitget_api_spot import (
    convert_symbol,
    get_spot_free_qty,
    place_spot_market_buy,
    place_spot_market_sell_qty,
    get_symbol_spec_spot,
    round_down_step,
)

try:
    from telegram_spot_bot import send_telegram
except Exception:
    def send_telegram(msg: str):
        print("[TG]", msg)

TRACE_LOG = os.getenv("TRACE_LOG", "0") == "1"

TP1_PCT = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT = float(os.getenv("TP2_PCT", "0.40"))
TP3_PCT = float(os.getenv("TP3_PCT", "0.30"))
# SL은 main에서 sl1/sl2 모두 전량 종료로 라우팅

MAX_OPEN_COINS = int(os.getenv("MAX_OPEN_COINS", "60"))
CAP_CHECK_SEC  = float(os.getenv("CAP_CHECK_SEC", "10"))

BALANCE_RETRY       = int(os.getenv("BALANCE_RETRY", "10"))
BALANCE_RETRY_DELAY = float(os.getenv("BALANCE_RETRY_DELAY", "2"))

_POS_LOCK = threading.RLock()
held_marks_ts: Dict[str, float] = {}   # symbol -> last_buy_ts
held_marks_qty: Dict[str, float] = {}  # symbol -> cached base qty

_CAP = {"blocked": False, "last_count": 0, "ts": 0.0}
_CAP_LOCK = threading.Lock()


def _count_open_coins() -> int:
    with _POS_LOCK:
        return sum(1 for _, q in held_marks_qty.items() if q > 0)


def start_capacity_guard():
    def _loop():
        prev_blocked = None
        while True:
            try:
                cnt = _count_open_coins()
                blocked = cnt >= MAX_OPEN_COINS
                now = time.time()
                with _CAP_LOCK:
                    _CAP["blocked"] = blocked
                    _CAP["last_count"] = cnt
                    _CAP["ts"] = now
                if prev_blocked is None or prev_blocked != blocked:
                    state = "BLOCKED" if blocked else "OK"
                    send_telegram(f"[SPOT] Capacity {state} {cnt}/{MAX_OPEN_COINS}")
                    prev_blocked = blocked
            except Exception as e:
                print("[spot] capacity guard error:", e)
            time.sleep(CAP_CHECK_SEC)

    threading.Thread(target=_loop, daemon=True, name="spot-capacity").start()


def capacity_status():
    with _CAP_LOCK:
        return dict(_CAP)


def _cache_qty(symbol: str, qty: float):
    with _POS_LOCK:
        held_marks_ts[symbol] = time.time()
        held_marks_qty[symbol] = max(0.0, float(qty))


def _clear_cache(symbol: str):
    with _POS_LOCK:
        held_marks_ts.pop(symbol, None)
        held_marks_qty.pop(symbol, None)


def _refresh_free_qty(symbol: str) -> float:
    """fresh 잔고 API를 리트라이로 재조회"""
    free = 0.0
    tries = max(1, BALANCE_RETRY)
    for i in range(tries):
        free = get_spot_free_qty(symbol, fresh=True)
        if free > 0:
            break
        if i < tries - 1:
            time.sleep(BALANCE_RETRY_DELAY)
    return float(free)


# ===== trading =====
def enter_spot(symbol: str, usdt_amount: float):
    symbol = convert_symbol(symbol)
    st = capacity_status()
    if st.get("blocked"):
        send_telegram(f"[SPOT] ENTRY HOLD {symbol} {st['last_count']}/{MAX_OPEN_COINS}")
        return

    if TRACE_LOG:
        send_telegram(f"[SPOT] ENTRY req {symbol} amt={usdt_amount}")

    resp = place_spot_market_buy(symbol, usdt_amount)
    code = str(resp.get("code", ""))
    if code in ("00000", "0"):
        free_after = _refresh_free_qty(symbol)
        _cache_qty(symbol, free_after)
        send_telegram(f"[SPOT] BUY {symbol} approx {usdt_amount} USDT (qty~{free_after})")
    elif code in ("LOCAL_SYMBOL_REMOVED",):
        send_telegram(f"[SPOT] BUY skip (removed) {symbol}")
        _clear_cache(symbol)
    else:
        send_telegram(f"[SPOT] BUY fail {symbol} -> {resp}")


def _sell_pct(symbol: str, pct: float, tag: str):
    symbol = convert_symbol(symbol)

    cached = float(held_marks_qty.get(symbol, 0.0))
    free   = _refresh_free_qty(symbol)

    base_qty = max(0.0, min(cached if cached > 0 else float("inf"), free))
    if base_qty <= 0:
        send_telegram(f"[SPOT] {tag} skip (no free balance) {symbol}")
        return

    step = float(get_symbol_spec_spot(symbol).get("qtyStep", 1e-6))
    qty  = round_down_step(base_qty * pct, step)
    if qty <= 0:
        send_telegram(f"[SPOT] {tag} qty=0 after step {symbol}")
        return

    resp = place_spot_market_sell_qty(symbol, qty)
    code = str(resp.get("code", ""))
    if code in ("00000", "0"):
        remaining = max(0.0, cached - qty) if cached > 0 else max(0.0, free - qty)
        _cache_qty(symbol, remaining)
        send_telegram(f"[SPOT] {tag} {symbol} qty~{qty} ({int(pct*100)}%)")
    elif code in ("LOCAL_SYMBOL_REMOVED",):
        send_telegram(f"[SPOT] {tag} skip (removed) {symbol}")
        _clear_cache(symbol)
    else:
        send_telegram(f"[SPOT] {tag} fail {symbol} -> {resp}")


def take_partial_spot(symbol: str, pct: float):
    _sell_pct(symbol, pct, tag="SELL")


def stop_partial_spot(symbol: str, pct: float):
    _sell_pct(symbol, pct, tag="STOP")


def close_spot(symbol: str, reason: str = "manual"):
    symbol = convert_symbol(symbol)

    cached = float(held_marks_qty.get(symbol, 0.0))
    free   = _refresh_free_qty(symbol)
    base_qty = max(0.0, max(cached, free))  # 전량 종료는 더 큰 쪽 시도

    if base_qty <= 0:
        _clear_cache(symbol)
        send_telegram(f"[SPOT] CLOSE skip (no free balance) {symbol} ({reason})")
        return

    resp = place_spot_market_sell_qty(symbol, base_qty)
    code = str(resp.get("code", ""))
    if code in ("00000", "0"):
        _clear_cache(symbol)
        send_telegram(f"[SPOT] CLOSE {symbol} ({reason})")
    elif code in ("LOCAL_SYMBOL_REMOVED",):
        _clear_cache(symbol)
        send_telegram(f"[SPOT] CLOSE skip (removed) {symbol} ({reason})")
    else:
        send_telegram(f"[SPOT] CLOSE fail {symbol} -> {resp}")
