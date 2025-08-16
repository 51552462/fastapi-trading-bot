import os, time, threading
from typing import Dict, Optional

from bitget_api import (convert_symbol,get_last_price,get_open_positions,place_market_order,place_reduce_by_size,get_symbol_spec,round_down_step,)

try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str):
        print("[TG]", msg)

LEVERAGE = float(os.getenv("LEVERAGE", "5"))

# ── Local state ───────────────────────────────────────────────
position_data: Dict[str, dict] = {}
_POS_LOCK = threading.RLock()

# 심볼·사이드별 직렬 락 (BTCUSDT_long / BTCUSDT_short)
_KEY_LOCKS: Dict[str, threading.RLock] = {}
_KEY_LOCKS_LOCK = threading.Lock()

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

        # 주문 (usdt→수량 변환은 API에서 처리 + step/minQty 체크)
        resp = place_market_order(symbol, usdt_amount,side=("buy" if side == "long" else "sell"),leverage=lev, reduce_only=False)
        if str(resp.get("code", "")) == "00000":
            with _POS_LOCK:
                position_data[key] = {"symbol": symbol, "side": side, "entry_usd": usdt_amount, "ts": time.time()}
            send_telegram(f"🚀 ENTRY {side.upper()} {symbol}\n• Notional≈ {usdt_amount} USDT\n• Lvg: {lev}x")
        else:
            send_telegram(f"❌ ENTRY 실패 {symbol} {side} → {resp}")

def take_partial_profit(symbol: str, pct: float, side: str = "long"):
    """항상 수량 기반 분할청산 (원격 사이즈×비율)."""
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
            send_telegram(f"🤑 TP {int(pct*100)}% {side.upper()} {symbol}\n"f"• Exit: {exit_price}\n• Cut size: {cut_size}\n• Realized≈ {pnl:+.2f} USDT")
        else:
            send_telegram(f"❌ TP 실패 {key} → {resp}")

def close_position(symbol: str, side: str = "long", reason: str = "manual"):
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    key    = _key(symbol, side)

    with _lock_for(key):
        # 최대 3회 재조회
        p = None
        for _ in range(3):
            p = _get_remote(symbol, side)
            if p and float(p.get("size", 0)) > 0:
                break
            time.sleep(0.2)

        if not p or float(p.get("size", 0)) <= 0:
            send_telegram(f"⚠️ CLOSE 스킵: 원격 포지션 없음 {key} ({reason})")
            with _POS_LOCK:
                position_data.pop(key, None)
            return

        size = float(p["size"])  # 전량 청산
        resp = place_reduce_by_size(symbol, size, side)
        exit_price = get_last_price(symbol) or float(p.get("entry_price", 0))

        if str(resp.get("code", "")) == "00000":
            entry = float(p.get("entry_price", 0))
            pnl   = _pnl_usdt(entry, exit_price, entry * size, side)
            with _POS_LOCK:
                position_data.pop(key, None)
            send_telegram(f"✅ CLOSE {side.upper()} {symbol} ({reason})\n"f"• Exit: {exit_price}\n• Size: {size}\n• Realized≈ {pnl:+.2f} USDT")
        else:
            send_telegram(f"❌ CLOSE 실패 {key} ({reason}) → {resp}")

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
            send_telegram(f"🔻 Reduce {contracts} {side.upper()} {symbol}")
        else:
            send_telegram(f"❌ Reduce 실패 {key} → {resp}")
