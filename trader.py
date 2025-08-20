# trader.py — 중복 진입 가드 + 리컨실 보강 + -10% PnL 워치독 안정판
import os, time, threading, math
from typing import Dict, Optional

from bitget_api import (
    convert_symbol, get_last_price, get_open_positions,
    place_market_order, place_reduce_by_size, get_symbol_spec, round_down_step,
)

# ── Telegram(없으면 print) ─────────────────────────────────────
try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str): print("[TG]", msg)

# ── 환경변수 ───────────────────────────────────────────────────
LEVERAGE = float(os.getenv("LEVERAGE", "5"))
TP1_PCT  = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT  = float(os.getenv("TP2_PCT", "0.40"))
TP3_PCT  = float(os.getenv("TP3_PCT", "0.30"))

# -10% 즉시 종료(증거금 대비 PnL 기준)
STOP_PCT           = float(os.getenv("STOP_PCT", "0.10"))   # 예: 0.10 => -10%
STOP_CHECK_SEC     = float(os.getenv("STOP_CHECK_SEC", "1.0"))
STOP_COOLDOWN_SEC  = float(os.getenv("STOP_COOLDOWN_SEC", "5.0"))
WATCHDOG_LEV       = float(os.getenv("WATCHDOG_LEV", os.getenv("LEVERAGE", "5")))
STOP_DEBUG         = os.getenv("STOP_DEBUG", "0") == "1"

# 리컨실러
RECON_INTERVAL_SEC = float(os.getenv("RECON_INTERVAL_SEC", "60"))
RECON_DEBUG        = os.getenv("RECON_DEBUG", "0") == "1"

# 중복 진입 가드
ENTRY_GUARD_SEC    = float(os.getenv("ENTRY_GUARD_SEC", "75"))

# ── 로컬 상태/락 ───────────────────────────────────────────────
position_data: Dict[str, dict] = {}
_POS_LOCK = threading.RLock()

_KEY_LOCKS: Dict[str, threading.RLock] = {}
_KEY_LOCKS_LOCK = threading.RLock()

def _key(symbol: str, side: str) -> str:
    return f"{convert_symbol(symbol)}_{side}"

def _lock_for(key: str):
    with _KEY_LOCKS_LOCK:
        if key not in _KEY_LOCKS:
            _KEY_LOCKS[key] = threading.RLock()
    return _KEY_LOCKS[key]

# ── Entry Guard ────────────────────────────────────────────────
_ENTRY_GUARD: Dict[str, float] = {}
_ENTRY_GUARD_LOCK = threading.RLock()

def _entry_guard_active(key: str) -> bool:
    with _ENTRY_GUARD_LOCK:
        return time.time() < _ENTRY_GUARD.get(key, 0.0)

def _arm_entry_guard(key: str, sec: float = None):
    with _ENTRY_GUARD_LOCK:
        _ENTRY_GUARD[key] = time.time() + float(sec or ENTRY_GUARD_SEC)

# ── Pending 관리 ──────────────────────────────────────────────
_PENDING = {"entry": {}, "close": {}, "tp": {}}
_PENDING_LOCK = threading.RLock()

def _pending_key(prefix: str, symbol: str, side: str) -> str:
    return f"{prefix}:{convert_symbol(symbol)}:{side}:{int(time.time()*1000)}"

def _mark_done(kind: str, pkey: str, note: str = ""):
    with _PENDING_LOCK:
        _PENDING.get(kind, {}).pop(pkey, None)
    if RECON_DEBUG and note:
        send_telegram(f"✅ pending done [{kind}] {pkey} {note}")

def get_pending_snapshot() -> Dict[str, Dict]:
    with _PENDING_LOCK:
        return {
            "counts": {k: len(v) for k, v in _PENDING.items()},
            "entry_keys": list(_PENDING["entry"].keys()),
            "close_keys": list(_PENDING["close"].keys()),
            "tp_keys": list(_PENDING["tp"].keys()),
            "interval": RECON_INTERVAL_SEC,
            "debug": RECON_DEBUG,
        }

# ── 원격 포지션 조회 헬퍼 ─────────────────────────────────────
def _get_remote(symbol: str, side: Optional[str] = None):
    sym = convert_symbol(symbol)
    for p in get_open_positions():
        if p.get("symbol") == sym and (side is None or p.get("side") == side):
            return p
    return None

def _get_remote_any(symbol: str):
    sym = convert_symbol(symbol)
    for p in get_open_positions():
        if p.get("symbol") == sym and float(p.get("size", 0)) > 0:
            return p
    return None

# ── 진입 ───────────────────────────────────────────────────────
def enter_position(symbol: str, usdt_amount: float, side: str = "long", leverage: float = None):
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    key    = _key(symbol, side)
    lev    = float(leverage or LEVERAGE)
    pkey   = _pending_key("entry", symbol, side)

    # guard: 최근 성공 직후 빠른 재진입 차단
    if _entry_guard_active(key):
        if RECON_DEBUG: send_telegram(f"⏳ ENTRY guard skip {side.upper()} {symbol}")
        return

    with _PENDING_LOCK:
        _PENDING["entry"][pkey] = {"symbol": symbol, "side": side, "amount": usdt_amount,
                                   "leverage": lev, "created": time.time(), "last_try": 0.0}

    with _lock_for(key):
        # 로컬/원격 모두 확인 → 중복 진입 차단
        with _POS_LOCK:
            if position_data.get(key):
                _mark_done("entry", pkey, "(local-exists)")
                return
        if _get_remote_any(symbol):
            _mark_done("entry", pkey, "(exists)")
            return

        last = get_last_price(symbol)
        if not last:
            return  # 리컨실러가 재시도

        resp = place_market_order(symbol, usdt_amount,
                                  side=("buy" if side == "long" else "sell"),
                                  leverage=lev, reduce_only=False)
        code = str(resp.get("code", ""))
        if code == "00000":
            with _POS_LOCK:
                position_data[key] = {"symbol": symbol, "side": side, "ts": time.time()}
            _mark_done("entry", pkey)
            _arm_entry_guard(key)
            send_telegram(f"🚀 ENTRY {side.upper()} {symbol}\n• Notional≈ {usdt_amount} USDT\n• Lvg: {lev}x")
        elif code.startswith("LOCAL_MIN_QTY") or code.startswith("LOCAL_BAD_QTY"):
            _mark_done("entry", pkey, f"({code})")
            send_telegram(f"⛔ ENTRY 스킵 {symbol} {side} → {resp}")
        else:
            # 실패는 리컨실러 재시도
            pass

# ── 전량 종료 ─────────────────────────────────────────────────
def _sweep_full_close(symbol: str, side: str, max_retry: int = 5, sleep_s: float = 0.35):
    for _ in range(max_retry):
        p = _get_remote(symbol, side)
        if not p or float(p.get("size", 0)) <= 0:
            return True
        size = float(p["size"])
        r = place_reduce_by_size(symbol, size, side)
        if str(r.get("code", "")) == "00000":
            time.sleep(sleep_s)
            continue
        time.sleep(sleep_s)
    return False

def close_position(symbol: str, side: str = "long", reason: str = "manual"):
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    key    = _key(symbol, side)
    pkey   = _pending_key("close", symbol, side)

    with _PENDING_LOCK:
        _PENDING["close"][pkey] = {"symbol": symbol, "side": side, "reason": reason,
                                   "created": time.time(), "last_try": 0.0}

    with _lock_for(key):
        ok = _sweep_full_close(symbol, side)
        if ok:
            with _POS_LOCK:
                position_data.pop(key, None)
            _mark_done("close", pkey)
            # 종료 성공 시 가드 해제 → 재진입 허용
            with _ENTRY_GUARD_LOCK:
                _ENTRY_GUARD.pop(key, None)
            send_telegram(f"✅ CLOSE {side.upper()} {symbol} ({reason})")
        else:
            # 리컨실러 재시도
            pass

# ── 분할 익절/감축 ────────────────────────────────────────────
def take_partial_profit(symbol: str, pct: float, side: str = "long"):
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    key    = _key(symbol, side)

    with _lock_for(key):
        p = _get_remote(symbol, side)
        if not p or float(p.get("size", 0)) <= 0:
            send_telegram(f"⚠️ TP 스킵(원격 없음) {side.upper()} {symbol}")
            return
        size_step = float(get_symbol_spec(symbol).get("sizeStep", 0.001))
        cut = round_down_step(float(p["size"]) * float(pct), size_step)
        if cut <= 0:
            return
        r = place_reduce_by_size(symbol, cut, side)
        if str(r.get("code", "")) == "00000":
            send_telegram(f"✂️ TP {int(pct*100)}% {side.upper()} {symbol}")
        else:
            send_telegram(f"⛔ TP 실패 {side.upper()} {symbol} → {r}")

def reduce_by_contracts(symbol: str, contracts: float, side: str = "long"):
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    if contracts <= 0:
        return
    r = place_reduce_by_size(symbol, contracts, side)
    if str(r.get("code", "")) == "00000":
        send_telegram(f"✂️ REDUCE {contracts}c {side.upper()} {symbol}")
    else:
        send_telegram(f"⛔ REDUCE 실패 {side.upper()} {symbol} → {r}")

# ── -10% 즉시 종료 워치독(PnL 기준) ───────────────────────────
_STOP_FIRED: Dict[str, float] = {}
_STOP_LOCK = threading.RLock()

def _pnl_ratio_by_price(entry_price: float, last_price: float, side: str, lev: float) -> float:
    """가격변화율 × 레버리지로 PnL% 근사"""
    if entry_price <= 0 or last_price <= 0:
        return 0.0
    if side == "long":
        return ((last_price - entry_price) / entry_price) * lev
    else:
        return ((entry_price - last_price) / entry_price) * lev

def _get_pnl_ratio(p: dict) -> float:
    # 1) 거래소 제공 비율이 있으면 우선 사용
    try:
        r = float(p.get("unrealizedPnlRatio"))
        if math.isfinite(r) and r != 0:
            return r
    except Exception:
        pass
    # 2) entry_price & 현재가로 근사
    entry = float(p.get("entry_price", 0) or 0)
    last  = float(get_last_price(p["symbol"]) or 0)
    lev   = float(p.get("leverage") or WATCHDOG_LEV)
    return _pnl_ratio_by_price(entry, last, p.get("side", "long"), lev)

def _watchdog_loop():
    while True:
        time.sleep(max(0.2, float(STOP_CHECK_SEC)))
        try:
            now = time.time()
            for p in get_open_positions():
                sym  = p["symbol"]
                side = p["side"]
                key  = _key(sym, side)

                pnl_ratio = _get_pnl_ratio(p)  # 음수면 손실
                if STOP_DEBUG:
                    try:
                        send_telegram(f"🧪WDG {sym} {side} PnL={pnl_ratio*100:.2f}% "
                                      f"(STOP={-STOP_PCT*100:.1f}%)")
                    except Exception:
                        pass

                if pnl_ratio <= -float(STOP_PCT):
                    with _STOP_LOCK:
                        fired_at = _STOP_FIRED.get(key, 0.0)
                        if now - fired_at < float(STOP_COOLDOWN_SEC):
                            continue
                        _STOP_FIRED[key] = now
                    close_position(sym, side=side, reason="failCut")
        except Exception:
            # 워치독은 절대 죽지 않도록
            pass

def start_watchdogs():
    threading.Thread(target=_watchdog_loop, daemon=True).start()

# ── 리컨실러 ───────────────────────────────────────────────────
def _reconciler_loop():
    while True:
        time.sleep(RECON_INTERVAL_SEC)
        try:
            # ENTRY 재시도
            with _PENDING_LOCK:
                es = list(_PENDING["entry"].items())
            for pkey, item in es:
                sym, side, amt, lev = item["symbol"], item["side"], item["amount"], item["leverage"]
                key = _key(sym, side)

                # guard / local / remote 순으로 소거
                if _entry_guard_active(key):
                    _mark_done("entry", pkey, "(guard)")
                    continue
                with _POS_LOCK:
                    if position_data.get(key):
                        _mark_done("entry", pkey, "(local-exists)")
                        continue
                if _get_remote_any(sym):
                    _mark_done("entry", pkey, "(exists)")
                    continue

                # 재주문
                r = place_market_order(sym, amt,
                                       side=("buy" if side == "long" else "sell"),
                                       leverage=lev, reduce_only=False)
                code = str(r.get("code", ""))
                if code == "00000":
                    _mark_done("entry", pkey)
                    _arm_entry_guard(key)
                    send_telegram(f"🔁 ENTRY 재시도 성공 {side.upper()} {sym}")
                elif code.startswith("LOCAL_MIN_QTY") or code.startswith("LOCAL_BAD_QTY"):
                    _mark_done("entry", pkey, "(minQty/badQty)")
                    send_telegram(f"⛔ ENTRY 재시도 스킵 {sym} {side} → {r}")

            # CLOSE 재시도
            with _PENDING_LOCK:
                cs = list(_PENDING["close"].items())
            for pkey, item in cs:
                sym, side = item["symbol"], item["side"]
                key = _key(sym, side)

                p = _get_remote(sym, side)
                if not p or float(p.get("size", 0)) <= 0:
                    _mark_done("close", pkey, "(no-remote)")
                    continue

                ok = _sweep_full_close(sym, side)
                if ok:
                    with _POS_LOCK:
                        position_data.pop(key, None)
                    _mark_done("close", pkey)
                    with _ENTRY_GUARD_LOCK:
                        _ENTRY_GUARD.pop(key, None)
                    send_telegram(f"🔁 CLOSE 재시도 성공 {side.upper()} {sym}")
                else:
                    # 다음 라운드에서 다시 시도
                    pass
        except Exception as e:
            print("reconciler error:", e)

def start_reconciler():
    threading.Thread(target=_reconciler_loop, daemon=True).start()
