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
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str):
        print("[TG]", msg)

TRACE_LOG = os.getenv("TRACE_LOG", "0") == "1"

TP1_PCT = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT = float(os.getenv("TP2_PCT", "0.40"))
TP3_PCT = float(os.getenv("TP3_PCT", "0.30"))

MAX_OPEN_COINS = int(os.getenv("MAX_OPEN_COINS", "60"))
CAP_CHECK_SEC  = float(os.getenv("CAP_CHECK_SEC", "10"))

# 잔고 리트라이(환경변수로 조절 가능)
BALANCE_RETRY       = int(os.getenv("BALANCE_RETRY", "3"))
BALANCE_RETRY_DELAY = float(os.getenv("BALANCE_RETRY_DELAY", "1.5"))

_POS_LOCK = threading.RLock()
held_marks: Dict[str, float] = {}  # symbol -> last_buy_ts

_CAP = {"blocked": False, "last_count": 0, "ts": 0.0}
_CAP_LOCK = threading.Lock()

def _count_open_coins() -> int:
    with _POS_LOCK:
        return len(held_marks)

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

def _mark_hold(symbol: str):
    with _POS_LOCK:
        held_marks[symbol] = time.time()

def _unmark_hold(symbol: str):
    with _POS_LOCK:
        held_marks.pop(symbol, None)

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
        _mark_hold(symbol)
        send_telegram(f"[SPOT] BUY {symbol} approx {usdt_amount} USDT")
    else:
        send_telegram(f"[SPOT] BUY fail {symbol} -> {resp}")

def _sell_pct(symbol: str, pct: float):
    symbol = convert_symbol(symbol)

    # --- 잔고 재확인 리트라이 ---
    free = 0.0
    tries = max(1, BALANCE_RETRY)
    for i in range(tries):
        free = get_spot_free_qty(symbol)
        if free > 0:
            break
        if i < tries - 1:
            time.sleep(BALANCE_RETRY_DELAY)

    if free <= 0:
        send_telegram(f"[SPOT] SELL skip (no free balance) {symbol}")
        return

    step = float(get_symbol_spec_spot(symbol).get("qtyStep", 1e-6))
    qty  = round_down_step(free * pct, step)
    if qty <= 0:
        send_telegram(f"[SPOT] SELL qty=0 after step {symbol}")
        return

    resp = place_spot_market_sell_qty(symbol, qty)
    if str(resp.get("code", "")) in ("00000", "0"):
        send_telegram(f"[SPOT] SELL {symbol} qty approx {qty} ({int(pct * 100)}%)")
    else:
        send_telegram(f"[SPOT] SELL fail {symbol} -> {resp}")

def take_partial_spot(symbol: str, pct: float):
    _sell_pct(symbol, pct)

def close_spot(symbol: str, reason: str = "manual"):
    symbol = convert_symbol(symbol)

    # --- 잔고 재확인 리트라이 ---
    free = 0.0
    tries = max(1, BALANCE_RETRY)
    for i in range(tries):
        free = get_spot_free_qty(symbol)
        if free > 0:
            break
        if i < tries - 1:
            time.sleep(BALANCE_RETRY_DELAY)

    if free <= 0:
        _unmark_hold(symbol)
        send_telegram(f"[SPOT] CLOSE skip (no free balance) {symbol} ({reason})")
        return

    resp = place_spot_market_sell_qty(symbol, free)
    if str(resp.get("code", "")) in ("00000", "0"):
        _unmark_hold(symbol)
        send_telegram(f"[SPOT] CLOSE {symbol} ({reason})")
    else:
        send_telegram(f"[SPOT] CLOSE fail {symbol} -> {resp}")
