# trader_spot.py
import os, time, threading
from typing import Dict

from bitget_api_spot import (
    convert_symbol, get_last_price_spot, get_spot_free_qty,
    place_spot_market_buy, place_spot_market_sell_qty,
    get_symbol_spec_spot, round_down_step
)

try
    from telegram_bot import send_telegram
except Exception
    def send_telegram(msg str)
        print([TG], msg)

TRACE_LOG = os.getenv(TRACE_LOG, 0) == 1

TP1_PCT = float(os.getenv(TP1_PCT, 0.30))
TP2_PCT = float(os.getenv(TP2_PCT, 0.40))
TP3_PCT = float(os.getenv(TP3_PCT, 0.30))

# 용량 가드(“보유 중인 코인 수” 기준)
MAX_OPEN_COINS   = int(os.getenv(MAX_OPEN_COINS, 60))
CAP_CHECK_SEC    = float(os.getenv(CAP_CHECK_SEC, 10))

_POS_LOCK = threading.RLock()
# 로컬 보유 추적(심플) 마지막 매수 발생시 심볼 마킹. 실제 잔고는 API에서 확인
held_marks Dict[str, float] = {}  # symbol - last_buy_ts

_CAP = {blocked False, last_count 0, ts 0.0}
_CAP_LOCK = threading.Lock()

def _count_open_coins() - int
    with _POS_LOCK
        return len(held_marks)

def start_capacity_guard()
    def _loop()
        prev = None
        while True
            try
                cnt = _count_open_coins()
                blocked = cnt = MAX_OPEN_COINS
                now = time.time()
                with _CAP_LOCK
                    _CAP[blocked] = blocked
                    _CAP[last_count] = cnt
                    _CAP[ts] = now
                if prev is None or prev != blocked
                    send_telegram(fℹ️ [SPOT] Capacity {'BLOCKED' if blocked else 'OK'} {cnt}{MAX_OPEN_COINS})
                    prev = blocked
            except Exception as e
                print([spot] cap err, e)
            time.sleep(CAP_CHECK_SEC)
    threading.Thread(target=_loop, daemon=True, name=spot-capacity).start()

def capacity_status()
    with _CAP_LOCK
        return dict(_CAP)

def _has_mark(symbol str) - bool
    with _POS_LOCK
        return symbol in held_marks

def _mark_hold(symbol str)
    with _POS_LOCK
        held_marks[symbol] = time.time()

def _unmark_hold(symbol str)
    with _POS_LOCK
        held_marks.pop(symbol, None)

# ── Trading ops ───────────────────────────────────────────────
def enter_spot(symbol str, usdt_amount float)
    symbol = convert_symbol(symbol)
    if capacity_status()[blocked]
        st = capacity_status()
        send_telegram(f🧱 [SPOT] ENTRY HOLD {symbol} {st['last_count']}{MAX_OPEN_COINS})
        return
    if TRACE_LOG
        send_telegram(f🔎 [SPOT] ENTRY req {symbol} amt={usdt_amount})
    resp = place_spot_market_buy(symbol, usdt_amount)
    code = str(resp.get(code, ))
    if code in (00000, 0)
        _mark_hold(symbol)
        send_telegram(f🛒 [SPOT] BUY {symbol} ≈ {usdt_amount}USDT)
    else
        send_telegram(f❌ [SPOT] BUY fail {symbol} → {resp})

def _sell_pct(symbol str, pct float)
    symbol = convert_symbol(symbol)
    free = get_spot_free_qty(symbol)
    if free = 0
        send_telegram(f⚠️ [SPOT] SELL skip(no bal) {symbol})
        return
    step = float(get_symbol_spec_spot(symbol).get(qtyStep, 1e-6))
    qty  = round_down_step(free  pct, step)
    if qty = 0
        send_telegram(f⚠️ [SPOT] SELL qty=0 {symbol})
        return
    resp = place_spot_market_sell_qty(symbol, qty)
    if str(resp.get(code,)) in (00000,0)
        send_telegram(f💸 [SPOT] SELL {symbol} qty≈{qty} ({int(pct100)}%))
    else
        send_telegram(f❌ [SPOT] SELL fail {symbol} → {resp})

def take_partial_spot(symbol str, pct float)
    # pct(0~1) 보유 수량 기준 분할 매도
    _sell_pct(symbol, pct)

def close_spot(symbol str, reason str = manual)
    symbol = convert_symbol(symbol)
    free = get_spot_free_qty(symbol)
    if free = 0
        _unmark_hold(symbol)
        send_telegram(f⚠️ [SPOT] CLOSE skip(no bal) {symbol} ({reason}))
        return
    resp = place_spot_market_sell_qty(symbol, free)
    if str(resp.get(code,)) in (00000,0)
        _unmark_hold(symbol)
        send_telegram(f✅ [SPOT] CLOSE {symbol} ({reason}))
    else
        send_telegram(f❌ [SPOT] CLOSE fail {symbol} → {resp})
