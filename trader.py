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

# ── TP 비율 (환경변수와 동일하게) ─────────────────────────────
TP1_PCT = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT = float(os.getenv("TP2_PCT", "0.40"))
TP3_PCT = float(os.getenv("TP3_PCT", "0.30"))

# ── Emergency stop params (PnL 기준 고정) ─────────────────────
STOP_MODE          = "pnl"
STOP_PCT           = float(os.getenv("STOP_PCT", "0.10"))   # -10% 손실률
STOP_CHECK_SEC     = float(os.getenv("STOP_CHECK_SEC", "1.0"))
STOP_COOLDOWN_SEC  = float(os.getenv("STOP_COOLDOWN_SEC", "5.0"))

# ── Reconciler(1분 주기 재시도) ───────────────────────────────
RECON_INTERVAL_SEC = float(os.getenv("RECON_INTERVAL_SEC", "60"))
# TP3 달성 판정 여유 (초기 size의 0.1% 또는 2*sizeStep 중 큰 값)
TP_EPSILON_RATIO   = float(os.getenv("TP_EPSILON_RATIO", "0.001"))

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

# ── 최근 스탑 발동 기록(중복 방지) ───────────────────────────
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

# ── Pending 작업 레지스트리(리컨실러 대상) ───────────────────
#  entry:  key -> {amount, leverage, created, last_try, attempts}
#  close:  key -> {reason,  created, last_try, attempts}
#  tp3:    key -> {stage=3, pct, init_size, cut_size, size_step, created, last_try, attempts}
_PENDING = {
    "entry": {},  # key -> dict
    "close": {},  # key -> dict
    "tp":    {}   # key(stage3 only) -> dict
}
_PENDING_LOCK = threading.RLock()

def _pending_key_entry(symbol: str, side: str) -> str:
    return f"{_key(symbol, side)}:entry"

def _pending_key_close(symbol: str, side: str) -> str:
    return f"{_key(symbol, side)}:close"

def _pending_key_tp3(symbol: str, side: str) -> str:
    return f"{_key(symbol, side)}:tp3"

def _mark_done(typ: str, pkey: str):
    with _PENDING_LOCK:
        if pkey in _PENDING.get(typ, {}):
            _PENDING[typ].pop(pkey, None)

# ── Helpers ───────────────────────────────────────────────────
def _get_remote(symbol: str, side: Optional[str] = None):
    symbol = convert_symbol(symbol)
    arr = get_open_positions()
    for p in arr:
        if p.get("symbol") == symbol and (side is None or p.get("side") == side):
            return p
    return None

def _get_remote_any_side(symbol: str):
    symbol = convert_symbol(symbol)
    arr = get_open_positions()
    for p in arr:
        if p.get("symbol") == symbol and float(p.get("size") or 0) > 0:
            return p
    return None

def _pnl_usdt(entry: float, exit: float, notional: float, side: str) -> float:
    if side == "long":
        pct = (exit - entry) / entry
    else:
        pct = (entry - exit) / entry
    return notional * pct

def _loss_ratio_on_margin(entry: float, last: float, size: float, side: str, leverage: float) -> float:
    """loss_ratio = max(0, -PnL_USDT) / (Notional / Leverage)"""
    notional = entry * size
    pnl = _pnl_usdt(entry, last, notional, side)
    margin = max(1e-9, notional / max(1.0, leverage))
    loss_ratio = max(0.0, -pnl) / margin
    return loss_ratio

# ── Trading ops ───────────────────────────────────────────────
def enter_position(symbol: str, usdt_amount: float, side: str = "long", leverage: float = None):
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    key    = _key(symbol, side)
    lev    = float(leverage or LEVERAGE)

    # pending 등록 (성공 시 즉시 완료 처리)
    with _PENDING_LOCK:
        _PENDING["entry"][_pending_key_entry(symbol, side)] = {
            "symbol": symbol, "side": side, "amount": usdt_amount, "leverage": lev,
            "created": time.time(), "last_try": 0.0, "attempts": 0,
        }

    with _lock_for(key):
        # 진입 전에, 동일 심볼의 어떤 사이드든 이미 포지션 있으면 재진입 안 함(뒤집힘 방지)
        if _get_remote_any_side(symbol):
            return

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
            # 중복 스탑 초기화
            with _STOP_LOCK:
                _STOP_FIRED.pop(key, None)
            # pending 완료
            _mark_done("entry", _pending_key_entry(symbol, side))
            send_telegram(f"🚀 ENTRY {side.upper()} {symbol}\n• Notional≈ {usdt_amount} USDT\n• Lvg: {lev}x")
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
            send_telegram(f"⚠️ TP 스킵: 원격 포지션 없음 {_key(symbol, side)}")
            return

        size_step = float(get_symbol_spec(symbol).get("sizeStep", 0.001))
        cur_size  = float(p["size"])
        cut_size  = round_down_step(cur_size * float(pct), size_step)
        if cut_size <= 0:
            send_telegram(f"⚠️ TP 스킵: 계산된 사이즈=0 ({_key(symbol, side)})")
            return

        # TP3라면 pending 등록 (요청 당시 기준으로 달성 여부 판단)
        if abs(float(pct) - TP3_PCT) <= 1e-6:
            with _PENDING_LOCK:
                _PENDING["tp"][_pending_key_tp3(symbol, side)] = {
                    "symbol": symbol, "side": side, "stage": 3, "pct": float(pct),
                    "init_size": cur_size, "cut_size": cut_size, "size_step": size_step,
                    "created": time.time(), "last_try": 0.0, "attempts": 0,
                }

        resp = place_reduce_by_size(symbol, cut_size, side)
        exit_price = get_last_price(symbol) or float(p.get("entry_price", 0))
        if str(resp.get("code", "")) == "00000":
            entry = float(p.get("entry_price", 0))
            realized = _pnl_usdt(entry, exit_price, entry * cut_size, side)
            send_telegram(
                f"🤑 TP {int(pct*100)}% {side.upper()} {symbol}\n"
                f"• Exit: {exit_price}\n• Cut size: {cut_size}\n• Realized≈ {realized:+.2f} USDT"
            )
        else:
            send_telegram(f"❌ TP 실패 {_key(symbol, side)} → {resp}")

def close_position(symbol: str, side: str = "long", reason: str = "manual"):
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    key    = _key(symbol, side)

    # pending 등록 (성공 시 즉시 완료 처리)
    with _PENDING_LOCK:
        _PENDING["close"][_pending_key_close(symbol, side)] = {
            "symbol": symbol, "side": side, "reason": reason,
            "created": time.time(), "last_try": 0.0, "attempts": 0,
        }

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
            _mark_done("close", _pending_key_close(symbol, side))
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
            _mark_done("close", _pending_key_close(symbol, side))
            send_telegram(
                f"✅ CLOSE {side.upper()} {symbol} ({reason})\n"
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
            send_telegram(f"🔻 Reduce {contracts} {side.upper()} {symbol}")
        else:
            send_telegram(f"❌ Reduce 실패 {key} → {resp}")

# ── Emergency watchdog (PnL 손실률 기준) ──────────────────────
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

                loss_ratio = _loss_ratio_on_margin(entry, last, size, side, leverage=LEVERAGE)
                if loss_ratio >= STOP_PCT:
                    key = _key(symbol, side)
                    if _should_fire_stop(key):
                        send_telegram(
                            f"⛔ {symbol} {side.upper()} emergencyStop PnL≤{-int(STOP_PCT*100)}%\n"
                            f"• entry={entry}, last={last}, loss≈{-loss_ratio*100:.2f}%"
                        )
                        close_position(symbol, side=side, reason="emergencyStop")
        except Exception as e:
            print("watchdog error:", e)
        time.sleep(STOP_CHECK_SEC)

# ── Reconciler loop (1분마다 미이행 작업 재시도) ───────────────
def _reconciler_loop():
    while True:
        time.sleep(RECON_INTERVAL_SEC)
        try:
            # ENTRY 재시도
            with _PENDING_LOCK:
                entry_items = list(_PENDING["entry"].items())
            for pkey, item in entry_items:
                sym = item["symbol"]; side = item["side"]
                key = _key(sym, side)
                # 이미 어떤 사이드든 포지션 있으면 진입 안 함(뒤집힘 방지)
                if _get_remote_any_side(sym):
                    # 포지션이 존재한다면 entry pending은 완료 처리
                    _mark_done("entry", pkey)
                    continue
                # 재시도
                with _lock_for(key):
                    now = time.time()
                    if now - item.get("last_try", 0.0) < RECON_INTERVAL_SEC - 1:
                        continue
                    amt = item["amount"]; lev = item["leverage"]
                    resp = place_market_order(sym, amt,
                                              side=("buy" if side == "long" else "sell"),
                                              leverage=lev, reduce_only=False)
                    item["last_try"] = now
                    item["attempts"] = item.get("attempts", 0) + 1
                    if str(resp.get("code", "")) == "00000":
                        _mark_done("entry", pkey)
                        send_telegram(f"🔁 ENTRY 재시도 성공 {side.upper()} {sym}")

            # CLOSE 재시도
            with _PENDING_LOCK:
                close_items = list(_PENDING["close"].items())
            for pkey, item in close_items:
                sym = item["symbol"]; side = item["side"]
                key = _key(sym, side)
                p = _get_remote(sym, side)
                if not p or float(p.get("size", 0)) <= 0:
                    _mark_done("close", pkey)
                    continue
                with _lock_for(key):
                    now = time.time()
                    if now - item.get("last_try", 0.0) < RECON_INTERVAL_SEC - 1:
                        continue
                    size = float(p["size"])
                    resp = place_reduce_by_size(sym, size, side)
                    item["last_try"] = now
                    item["attempts"] = item.get("attempts", 0) + 1
                    if str(resp.get("code", "")) == "00000":
                        ok = _sweep_full_close(sym, side, "reconcile")
                        if ok:
                            _mark_done("close", pkey)
                            send_telegram(f"🔁 CLOSE 재시도 성공 {side.upper()} {sym}")

            # TP3 재시도 (요청 당시 대비 남은 물량만 재감축)
            with _PENDING_LOCK:
                tp_items = list(_PENDING["tp"].items())
            for pkey, item in tp_items:
                sym = item["symbol"]; side = item["side"]
                key = _key(sym, side)

                p = _get_remote(sym, side)
                if not p or float(p.get("size", 0)) <= 0:
                    _mark_done("tp", pkey)
                    continue

                cur_size  = float(p["size"])
                init_size = float(item.get("init_size") or cur_size)  # 보정
                cut_size  = float(item["cut_size"])
                size_step = float(item.get("size_step", 0.001))

                # 달성량 = 초기 - 현재
                achieved = max(0.0, init_size - cur_size)
                # 여유 허용치
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
                    resp = place_reduce_by_size(sym, remain, side)
                    item["last_try"] = now
                    item["attempts"] = item.get("attempts", 0) + 1
                    if str(resp.get("code", "")) == "00000":
                        send_telegram(f"🔁 TP3 재시도 감축 {side.upper()} {sym} remain≈{remain}")
        except Exception as e:
            print("reconciler error:", e)

# ── Starters ──────────────────────────────────────────────────
def start_watchdogs():
    t = threading.Thread(target=_watchdog_loop, name="emergency-stop-watchdog", daemon=True)
    t.start()

def start_reconciler():
    t = threading.Thread(target=_reconciler_loop, name="reconciler", daemon=True)
    t.start()
