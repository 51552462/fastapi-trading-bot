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

# ── Emergency stop params (PnL 기준으로 고정) ─────────────────
STOP_MODE          = "pnl"                                   # 고정
STOP_PCT           = float(os.getenv("STOP_PCT", "0.10"))    # 0.10 → 손실률 -10%
STOP_CHECK_SEC     = float(os.getenv("STOP_CHECK_SEC", "1.0"))
STOP_COOLDOWN_SEC  = float(os.getenv("STOP_COOLDOWN_SEC", "5.0"))

# ── Local state ───────────────────────────────────────────────
position_data: Dict[str, dict] = {}
_POS_LOCK = threading.RLock()

# 심볼·사이드별 직렬 락 (BTCUSDT_long / BTCUSDT_short)
_KEY_LOCKS: Dict[str, threading.RLock] = {}
_KEY_LOCKS_LOCK = threading.Lock()

# 최근 스탑 발동 기록(중복 방지)
_STOP_FIRED: Dict[str, float] = {}
_STOP_LOCK = threading.Lock()

def _key(symbol: str, side: str) -> str:
    return f"{symbol}_{side}"

def _lock_for(key: str):
    with _KEY_LOCKS_LOCK:
        if key not in _KEY_LOCKS:
            _KEY_LOCKS[key] = threading.RLock()
    return _KEY_LOCKS[key]

# ── Helpers ───────────────────────────────────────────────────
def _get_remote(symbol: str, side: Optional[str] = None):
    symbol = convert_symbol(symbol)
    arr = get_open_positions()
    for p in arr:
        if p.get("symbol") == symbol and (side is None or p.get("side") == side):
            return p
    return None

def _pnl_usdt(entry: float, exit: float, notional: float, side: str) -> float:
    if side == "long":
        pct = (exit - entry) / entry
    else:
        pct = (entry - exit) / entry
    return notional * pct

def _adverse_move_pct(entry: float, last: float, side: str) -> float:
    if side == "long":
        return max(0.0, (entry - last) / entry)
    else:
        return max(0.0, (last - entry) / entry)

def _loss_ratio_on_margin(entry: float, last: float, size: float, side: str, leverage: float) -> float:
    """
    마진 기준 손실률(양수=손실):
      loss_ratio = max(0, -PnL_USDT) / (Notional/Leverage)
    """
    notional = entry * size
    pnl = _pnl_usdt(entry, last, notional, side)   # 손실이면 음수
    margin = max(1e-9, notional / max(1.0, leverage))
    loss_ratio = max(0.0, -pnl) / margin
    return loss_ratio

# ── Trading ops ───────────────────────────────────────────────
def enter_position(symbol: str, usdt_amount: float, side: str = "long", leverage: float = None):
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    key    = _key(symbol, side)
    lev    = float(leverage or LEVERAGE)

    with _lock_for(key):
        last = get_last_price(symbol)
        if not last or last <= 0:
            send_telegram(f"❌ ENTRY 실패: {symbol} 현재가 조회 실패")
            return

        resp = place_market_order(symbol, usdt_amount,
                                  side=("buy" if side == "long" else "sell"),
                                  leverage=lev, reduce_only=False)
        if str(resp.get("code", "")) == "00000":
            with _POS_LOCK:
                position_data[key] = {"symbol": symbol, "side": side, "entry_usd": usdt_amount, "ts": time.time()}
            # 스탑 중복 초기화
            with _STOP_LOCK:
                _STOP_FIRED.pop(key, None)
            send_telegram(f"🚀 ENTRY {side.UPPER()} {symbol}\n• Notional≈ {usdt_amount} USDT\n• Lvg: {lev}x")
        else:
            send_telegram(f"❌ ENTRY 실패 {symbol} {side} → {resp}")

def _sweep_full_close(symbol: str, side: str, reason: str, max_retry: int = 5, sleep_s: float = 0.3):
    for _ in range(max_retry):
        p = _get_remote(symbol, side)
        size = float(p["size"]) if p and p.get("size") else 0.0
        if size <= 0:
            return True
        resp = place_reduce_by_size(symbol, size, side)
        time.sleep(sleep_s)
    p = _get_remote(symbol, side)
    if not p or float(p.get("size", 0)) <= 0:
        return True
    send_telegram(f"⚠️ CLOSE 잔량 남음 {symbol} {side} ({reason}) size≈{p.get('size')}")
    return False

def take_partial_profit(symbol: str, pct: float, side: str = "long"):
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    key    = _key(symbol, side)

    with _lock_for(key):
        p = _get_remote(symbol, side)
        if not p or float(p.get("size", 0)) <= 0:
            send_telegram(f"⚠️ TP 스킵: 원격 포지션 없음 {key}")
            return

        size_step = float(get_symbol_spec(symbol).get("sizeStep", 0.001))
        cut_size  = round_down_step(float(p["size"]) * float(pct), size_step)
        if cut_size <= 0:
            send_telegram(f"⚠️ TP 스킵: 계산된 사이즈=0 ({key})")
            return

        resp = place_reduce_by_size(symbol, cut_size, side)
        exit_price = get_last_price(symbol) or float(p.get("entry_price", 0))
        if str(resp.get("code", "")) == "00000":
            entry = float(p.get("entry_price", 0))
            pnl   = _pnl_usdt(entry, exit_price, entry * cut_size, side)
            send_telegram(
                f"🤑 TP {int(pct*100)}% {side.UPPER()} {symbol}\n"
                f"• Exit: {exit_price}\n• Cut size: {cut_size}\n• Realized≈ {pnl:+.2f} USDT"
            )
        else:
            send_telegram(f"❌ TP 실패 {key} → {resp}")

def close_position(symbol: str, side: str = "long", reason: str = "manual"):
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    key    = _key(symbol, side)

    with _lock_for(key):
        p = None
        for _ in range(3):
            p = _get_remote(symbol, side)
            if p and float(p.get("size", 0)) > 0:
                break
            time.sleep(0.15)

        if not p or float(p.get("size", 0)) <= 0:
            send_telegram(f"⚠️ CLOSE 스킵: 원격 포지션 없음 {key} ({reason})")
            with _POS_LOCK:
                position_data.pop(key, None)
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
            send_telegram(
                f"✅ CLOSE {side.UPPER()} {symbol} ({reason})\n"
                f"• Exit: {exit_price}\n• Size: {size}\n• Realized≈ {realized:+.2f} USDT"
            )
        else:
            send_telegram(f"❌ CLOSE 실패/잔량 {key} ({reason}) → {resp}")

def reduce_by_contracts(symbol: str, contracts: float, side: str = "long"):
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    key    = _key(symbol, side)

    with _lock_for(key):
        step = float(get_symbol_spec(symbol).get("sizeStep", 0.001))
        contracts = round_down_step(float(contracts), step)
        if contracts <= 0:
            send_telegram(f"⚠️ reduceByContracts 스킵: step 미달 {key}")
            return
        resp = place_reduce_by_size(symbol, contracts, side)
        if str(resp.get("code", "")) == "00000":
            send_telegram(f"🔻 Reduce {contracts} {side.UPPER()} {symbol}")
        else:
            send_telegram(f"❌ Reduce 실패 {key} → {resp}")

# ── Emergency watchdog (PnL 손실률 기준) ──────────────────────
def _should_fire_stop(key: str) -> bool:
    now = time.time()
    with _STOP_LOCK:
        last = _STOP_FIRED.get(key, 0.0)
        if now - last < STOP_COOLDOWN_SEC:
            return False
        _STOP_FIRED[key] = now
        return True

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

                # 마진 기준 손실률 계산
                loss_ratio = _loss_ratio_on_margin(entry, last, size, side, leverage=LEVERAGE)

                if loss_ratio >= STOP_PCT:
                    key = _key(symbol, side)
                    if _should_fire_stop(key):
                        send_telegram(
                            f"⛔ {symbol} {side.UPPER()} emergencyStop PnL≤{-int(STOP_PCT*100)}%\n"
                            f"• entry={entry}, last={last}, loss≈{-loss_ratio*100:.2f}%"
                        )
                        close_position(symbol, side=side, reason="emergencyStop")
        except Exception as e:
            print("watchdog error:", e)
        time.sleep(STOP_CHECK_SEC)

def start_watchdogs():
    t = threading.Thread(target=_watchdog_loop, name="emergency-stop-watchdog", daemon=True)
    t.start()
