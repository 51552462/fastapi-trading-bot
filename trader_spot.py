# trader_spot.py
import os
import time
import threading
from typing import Dict, Optional

from bitget_api_spot import (
    convert_symbol,
    get_spot_free_qty,
    place_spot_market_buy,
    place_spot_market_sell_qty,
    get_symbol_spec_spot,
    round_down_step,
    get_last_price_spot,
)

# Telegram
try:
    from telegram_spot_bot import send_telegram
except Exception:
    def send_telegram(msg: str):
        print("[TG]", msg)

# --------------------- ENV / Params ---------------------
TRACE_LOG = os.getenv("TRACE_LOG", "0") == "1"

# 부분익절 비율(엔진은 main에서 tp1/tp2/tp3로 pct만 넘겨줌)
TP1_PCT = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT = float(os.getenv("TP2_PCT", "0.40"))
TP3_PCT = float(os.getenv("TP3_PCT", "0.30"))

# 동시 오픈 코인 제한
MAX_OPEN_COINS = int(os.getenv("MAX_OPEN_COINS", "60"))
CAP_CHECK_SEC  = float(os.getenv("CAP_CHECK_SEC", "10"))

# 잔고 fresh 재시도
BALANCE_RETRY       = int(os.getenv("BALANCE_RETRY", "10"))
BALANCE_RETRY_DELAY = float(os.getenv("BALANCE_RETRY_DELAY", "2"))

# === 자동 손절(실시간 PnL %) ===
AUTO_SL_ENABLE    = os.getenv("AUTO_SL_ENABLE", "1") == "1"
# 음수로 주면 그대로 임계값 사용(-3 = -3%), 양수로 주면 하락폭으로 해석(3 = -3%)
_auto_sl_pct_env   = float(os.getenv("AUTO_SL_PCT", "-3"))
AUTO_SL_PCT       = _auto_sl_pct_env if _auto_sl_pct_env < 0 else -abs(_auto_sl_pct_env)
AUTO_SL_POLL_SEC  = float(os.getenv("AUTO_SL_POLL_SEC", "3"))   # 가격 폴링 주기
AUTO_SL_GRACE_SEC = float(os.getenv("AUTO_SL_GRACE_SEC", "5"))  # 진입 직후 유예

# --------------------- State / Locks ---------------------
_POS_LOCK = threading.RLock()

# 최근 체결/보유 캐시
held_marks_ts:  Dict[str, float] = {}  # symbol -> last buy ts
held_marks_qty: Dict[str, float] = {}  # symbol -> cached base qty

# 엔트리 기준가(평단) 추정용
entry_px:   Dict[str, float] = {}       # symbol -> avg entry price (USDT)
entry_qty:  Dict[str, float] = {}       # symbol -> qty accumulated after last entry
entry_time: Dict[str, float] = {}       # symbol -> timestamp of last entry
_sl_armed:  Dict[str, bool]  = {}       # symbol -> autoSL 가능 상태(유예후 True)

# 용량가드
_CAP = {"blocked": False, "last_count": 0, "ts": 0.0}
_CAP_LOCK = threading.Lock()

# 자동 손절 스레드 제어
_ASL_ON   = False
_ASL_THRD: Optional[threading.Thread] = None


# --------------------- Capacity Guard ---------------------
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


# --------------------- Cache helpers ---------------------
def _cache_qty(symbol: str, qty: float):
    with _POS_LOCK:
        held_marks_ts[symbol] = time.time()
        held_marks_qty[symbol] = max(0.0, float(qty))

def _clear_cache(symbol: str):
    with _POS_LOCK:
        held_marks_ts.pop(symbol, None)
        held_marks_qty.pop(symbol, None)
        entry_px.pop(symbol, None)
        entry_qty.pop(symbol, None)
        entry_time.pop(symbol, None)
        _sl_armed.pop(symbol, None)

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


# --------------------- Trading (Entry / Sell / Close) ---------------------
def enter_spot(symbol: str, usdt_amount: float):
    symbol = convert_symbol(symbol)
    st = capacity_status()
    if st.get("blocked"):
        send_telegram(f"[SPOT] ENTRY HOLD {symbol} {st['last_count']}/{MAX_OPEN_COINS}")
        return

    if TRACE_LOG:
        send_telegram(f"[SPOT] ENTRY req {symbol} amt={usdt_amount}")

    # 엔트리 전 free 잔고
    before = _refresh_free_qty(symbol)

    resp = place_spot_market_buy(symbol, usdt_amount)
    code = str(resp.get("code", ""))
    if code in ("00000", "0"):
        # 엔트리 후 free 잔고
        after = _refresh_free_qty(symbol)
        _cache_qty(symbol, after)

        # 매수된 수량 추정
        bought = max(0.0, after - before)
        px_now = get_last_price_spot(symbol) or 0.0
        avg_px = 0.0
        if bought > 0:
            # 금액/수량으로 평단 추정
            avg_px = max(0.0, float(usdt_amount) / bought)
        elif px_now > 0:
            avg_px = px_now

        with _POS_LOCK:
            # 누적 진입을 고려해 가중평균 업데이트
            prev_qty = entry_qty.get(symbol, 0.0)
            prev_px  = entry_px.get(symbol, 0.0)
            if prev_qty > 0 and avg_px > 0 and bought > 0:
                new_qty = prev_qty + bought
                entry_px[symbol]  = (prev_px * prev_qty + avg_px * bought) / new_qty
                entry_qty[symbol] = new_qty
            else:
                if avg_px > 0:
                    entry_px[symbol]  = avg_px
                    entry_qty[symbol] = bought if bought > 0 else after
            entry_time[symbol] = time.time()
            _sl_armed[symbol]  = False  # 유예기간 후 True로 전환

        send_telegram(f"[SPOT] BUY {symbol} approx {usdt_amount} USDT (qty~{bought or after})")
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
    """
    전량 종료. 텔레그램 메시지는 예쁘게:
    ✅ CLOSE LONG SYMBOL\n• Exit: {px}\n• Size: {qty}\n• Realized≈ {pnl} USDT
    """
    symbol = convert_symbol(symbol)

    cached = float(held_marks_qty.get(symbol, 0.0))
    free   = _refresh_free_qty(symbol)
    base_qty = max(0.0, max(cached, free))  # 전량 종료는 더 큰 쪽

    if base_qty <= 0:
        _clear_cache(symbol)
        send_telegram(f"[SPOT] CLOSE skip (no free balance) {symbol} ({reason})")
        return

    # 청산 전 가격 스냅샷 (시장가이므로 근사치로 표기)
    exit_px = get_last_price_spot(symbol) or 0.0
    ent_px  = float(entry_px.get(symbol, 0.0))
    realized = (exit_px - ent_px) * base_qty if (exit_px > 0 and ent_px > 0) else 0.0

    resp = place_spot_market_sell_qty(symbol, base_qty)
    code = str(resp.get("code", ""))
    if code in ("00000", "0"):
        _clear_cache(symbol)
        # 예쁜 메시지
        lines = [
            f"✅ CLOSE LONG {symbol}",
            f"• Exit: {exit_px:.6g}" if exit_px > 0 else "• Exit: market",
            f"• Size: {base_qty:.6g}",
            f"• Realized≈ {realized:.2f} USDT"
        ]
        send_telegram("\n".join(lines))
    elif code in ("LOCAL_SYMBOL_REMOVED",):
        _clear_cache(symbol)
        send_telegram(f"[SPOT] CLOSE skip (removed) {symbol} ({reason})")
    else:
        send_telegram(f"[SPOT] CLOSE fail {symbol} -> {resp}")


# --------------------- Auto Stop-Loss thread ---------------------
def _auto_sl_loop():
    # –3% 이하 떨어지면 즉시 전량 종료
    while _ASL_ON:
        try:
            now = time.time()
            with _POS_LOCK:
                symbols = [s for s, q in held_marks_qty.items() if q > 0]

            for s in symbols:
                try:
                    ent_ts = entry_time.get(s, 0.0)
                    if ent_ts <= 0:
                        continue
                    # 유예기간
                    if not _sl_armed.get(s, False):
                        if (now - ent_ts) >= AUTO_SL_GRACE_SEC:
                            _sl_armed[s] = True
                        else:
                            continue

                    ent_px = entry_px.get(s, 0.0)
                    if ent_px <= 0:
                        continue
                    px = get_last_price_spot(s) or 0.0
                    if px <= 0:
                        continue
                    pnl_pct = (px / ent_px - 1.0) * 100.0
                    if pnl_pct <= AUTO_SL_PCT:
                        # 즉시 전량 종료
                        send_telegram(f"[SPOT] autoSL trigger {s} pnl≈{pnl_pct:.2f}% (th={AUTO_SL_PCT}%)")
                        close_spot(s, reason="autoSL")
                except Exception:
                    pass

        except Exception as e:
            print("[spot] autoSL error:", e)
        time.sleep(max(0.5, AUTO_SL_POLL_SEC))


def start_auto_stoploss():
    """자동 손절 감시 스레드 시작 (idempotent)"""
    global _ASL_ON, _ASL_THRD
    if not AUTO_SL_ENABLE:
        return
    if _ASL_ON:
        return
    _ASL_ON = True
    _ASL_THRD = threading.Thread(target=_auto_sl_loop, daemon=True, name="spot-autoSL")
    _ASL_THRD.start()


# --------------------- (호환) pos_store 어댑터 ---------------------
class _CompatPosStore:
    """예전 main 코드가 `from trader_spot import pos_store` 를 임포트할 때 깨지지 않게 하는 호환 레이어"""
    def size(self, symbol: str) -> float:
        sym = convert_symbol(symbol)
        return float(held_marks_qty.get(sym, 0.0))

    def entry(self, symbol: str):
        """(평단, 누적수량) 튜플 반환. 없으면 (0.0, 0.0)"""
        sym = convert_symbol(symbol)
        return float(entry_px.get(sym, 0.0)), float(entry_qty.get(sym, 0.0))

pos_store = _CompatPosStore()
