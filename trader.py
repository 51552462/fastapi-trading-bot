# trader.py
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

# ── 기본 환경 ──────────────────────────────────────────────────
LEVERAGE  = float(os.getenv("LEVERAGE", "5"))
TP1_PCT = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT = float(os.getenv("TP2_PCT", "0.40"))  # 초기 40%와 동일 효과 원하면 0.5714286 사용
TP3_PCT = float(os.getenv("TP3_PCT", "0.30"))

# ── Emergency stop (PnL 기준 고정) ────────────────────────────
STOP_PCT           = float(os.getenv("STOP_PCT", "0.10"))   # -10% 손실률
STOP_CHECK_SEC     = float(os.getenv("STOP_CHECK_SEC", "1.0"))
STOP_COOLDOWN_SEC  = float(os.getenv("STOP_COOLDOWN_SEC", "5.0"))

# ── Reconciler ────────────────────────────────────────────────
RECON_INTERVAL_SEC = float(os.getenv("RECON_INTERVAL_SEC", "60"))
TP_EPSILON_RATIO   = float(os.getenv("TP_EPSILON_RATIO", "0.001"))
RECON_DEBUG        = os.getenv("RECON_DEBUG", "0") == "1"   # 재시도 로그 on/off

# ── Entry Guard (중복 진입 방지) ───────────────────────────────
ENTRY_GUARD_SEC = float(os.getenv("ENTRY_GUARD_SEC", "75"))
_ENTRY_GUARD = {}
_ENTRY_GUARD_LOCK = threading.Lock()

def _entry_guard_active(key: str) -> bool:
    with _ENTRY_GUARD_LOCK:
        return time.time() < _ENTRY_GUARD.get(key, 0.0)

def _arm_entry_guard(key: str, sec: float = None):
    with _ENTRY_GUARD_LOCK:
        _ENTRY_GUARD[key] = time.time() + float(sec or ENTRY_GUARD_SEC)

# ── Local state & locks ───────────────────────────────────────
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

# ── Pending 관리 ──────────────────────────────────────────────
_PENDING = {
    "entry": {},  # { pkey: {...} }
    "close": {},
    "tp": {},
}
_PENDING_LOCK = threading.RLock()

def _pending_key_entry(symbol: str, side: str) -> str:
    return f"{symbol}:{side}:{int(time.time()*1000)}"

def _pending_key_close(symbol: str, side: str) -> str:
    return f"{symbol}:{side}:{int(time.time()*1000)}"

def _mark_done(typ: str, pkey: str, note: str = ""):
    with _PENDING_LOCK:
        if pkey in _PENDING.get(typ, {}):
            _PENDING[typ].pop(pkey, None)
    if RECON_DEBUG and note:
        send_telegram(f"✅ pending done [{typ}] {pkey} {note}")

def get_pending_snapshot() -> Dict[str, Dict]:
    """/pending 조회용(메인에서 노출)"""
    with _PENDING_LOCK:
        return {
            "counts": {k: len(v) for k, v in _PENDING.items()},
            "entry_keys": list(_PENDING["entry"].keys()),
            "close_keys": list(_PENDING["close"].keys()),
            "tp_keys": list(_PENDING["tp"].keys()),
            "interval": RECON_INTERVAL_SEC,
            "debug": RECON_DEBUG,
        }

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
        if p.get("symbol") == symbol and float(p.get("size", 0)) > 0:
            return p
    return None

def get_last_price_safe(symbol: str) -> float:
    try:
        return float(get_last_price(symbol) or 0)
    except Exception:
        return 0.0

# ── 주문/체결 ──────────────────────────────────────────────────
def enter_position(symbol: str, usdt_amount: float, side: str = "long", leverage: float = None):
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    key    = _key(symbol, side)
    lev    = float(leverage or LEVERAGE)
    pkey   = _pending_key_entry(symbol, side)

    # [NEW] guard: 최근 진입 성공 직후 일정 시간 재진입 금지
    if _entry_guard_active(key):
        if RECON_DEBUG:
            send_telegram(f"⏳ ENTRY guard skip {side.upper()} {symbol}")
        return

    # pending 등록
    with _PENDING_LOCK:
        _PENDING["entry"][pkey] = {"symbol": symbol, "side": side, "amount": usdt_amount,
                                   "leverage": lev, "created": time.time(), "last_try": 0.0, "attempts": 0}
    if RECON_DEBUG:
        send_telegram(f"📌 pending add [entry] {pkey}")

    with _lock_for(key):
        # 이미 포지션 있으면 재진입 금지 + pending 종료
        with _POS_LOCK:
            if position_data.get(key):
                _mark_done("entry", pkey, "(local-exists)")
                return
        if _get_remote_any_side(symbol):
            _mark_done("entry", pkey, "(exists)")
            return

        last = get_last_price_safe(symbol)
        if not last or last <= 0:
            # 실패 → 리컨실러가 재시도
            return

        resp = place_market_order(symbol, usdt_amount,
                                  side=("buy" if side == "long" else "sell"),
                                  leverage=lev, reduce_only=False)
        code = str(resp.get("code", ""))
        if code == "00000":
            with _POS_LOCK:
                position_data[key] = {"symbol": symbol, "side": side, "entry_usd": usdt_amount, "ts": time.time()}
            with _STOP_LOCK:
                _STOP_FIRED.pop(key, None)
            _mark_done("entry", pkey)
            _arm_entry_guard(key)  # [NEW] 성공 시 가드 장착
            send_telegram(f"🚀 ENTRY {side.upper()} {symbol}\n• Notional≈ {usdt_amount} USDT\n• Lvg: {lev}x")
        elif code.startswith("LOCAL_MIN_QTY") or code.startswith("LOCAL_BAD_QTY"):
            _mark_done("entry", pkey, "(minQty/badQty)")
            send_telegram(f"⛔ ENTRY 스킵 {symbol} {side} → {resp}")
        else:
            # 네트워크/호출 실패 등은 리컨실러가 재시도
            pass

def _sweep_full_close(symbol: str, side: str, reason: str, max_retry: int = 5, sleep_s: float = 0.3):
    for _ in range(max_retry):
        p = _get_remote(symbol, side)
        if not p or float(p.get("size", 0)) <= 0:
            return True
        size = float(p["size"])
        resp = place_reduce_by_size(symbol, size, side)
        code = str(resp.get("code", ""))
        if code == "00000":
            time.sleep(sleep_s)
            continue
        time.sleep(sleep_s)
    return False

def close_position(symbol: str, side: str = "long", reason: str = "manual"):
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    key    = _key(symbol, side)
    pkey   = _pending_key_close(symbol, side)

    with _PENDING_LOCK:
        _PENDING["close"][pkey] = {"symbol": symbol, "side": side, "reason": reason,
                                   "created": time.time(), "last_try": 0.0, "attempts": 0}
    if RECON_DEBUG:
        send_telegram(f"📌 pending add [close] {pkey}")

    with _lock_for(key):
        ok = _sweep_full_close(symbol, side, reason)
        if ok:
            with _POS_LOCK:
                position_data.pop(key, None)
            _mark_done("close", pkey)
            # [NEW] close 성공 → guard 해제
            with _ENTRY_GUARD_LOCK:
                _ENTRY_GUARD.pop(key, None)
            send_telegram(
                f"✅ CLOSE {side.upper()} {symbol} ({reason})"
            )
        else:
            # 실패 → 리컨실러가 재시도
            pass

def take_partial_profit(symbol: str, pct: float, side: str = "long"):
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    key    = _key(symbol, side)

    with _lock_for(key):
        p = _get_remote(symbol, side)
        if not p or float(p.get("size", 0)) <= 0:
            return
        size = float(p["size"]) * float(pct)
        if size <= 0:
            return
        resp = place_reduce_by_size(symbol, size, side)
        code = str(resp.get("code", ""))
        if code == "00000":
            send_telegram(f"✂️ TP {int(pct*100)}% {side.upper()} {symbol}")
        elif code.startswith("LOCAL_MIN_QTY") or code.startswith("LOCAL_BAD_QTY"):
            send_telegram(f"⛔ TP 스킵 {symbol} {side} → {resp}")

def reduce_by_contracts(symbol: str, contracts: float, side: str = "long"):
    symbol = convert_symbol(symbol)
    side   = (side or "long").lower()
    key    = _key(symbol, side)
    if contracts <= 0:
        return
    with _lock_for(key):
        resp = place_reduce_by_size(symbol, contracts, side)
        code = str(resp.get("code", ""))
        if code == "00000":
            send_telegram(f"✂️ REDUCE {contracts}c {side.upper()} {symbol}")
        elif code.startswith("LOCAL_MIN_QTY") or code.startswith("LOCAL_BAD_QTY"):
            send_telegram(f"⛔ REDUCE 스킵 {symbol} {side} → {resp}")

# ── Watchdog: -10% 손절 감시 ──────────────────────────────────
_STOP_FIRED: Dict[str, float] = {}
_STOP_LOCK = threading.RLock()

def _watchdog_loop():
    while True:
        time.sleep(max(0.2, float(STOP_CHECK_SEC)))
        try:
            arr = get_open_positions()
            now = time.time()
            for p in arr:
                sym  = p["symbol"]
                side = p["side"]
                key  = _key(sym, side)
                # 여기서는 거래소 PnL/liq 기준으로 -10% 감지한다고 가정
                loss_ratio = float(p.get("unrealizedPnlRatio", 0.0))  # -0.1 이면 -10%
                if loss_ratio <= -float(STOP_PCT):
                    with _STOP_LOCK:
                        fired_at = _STOP_FIRED.get(key, 0.0)
                        if now - fired_at < STOP_COOLDOWN_SEC:
                            continue
                        _STOP_FIRED[key] = now
                    close_position(sym, side=side, reason="failCut")
        except Exception:
            pass

def start_watchdogs():
    t = threading.Thread(target=_watchdog_loop, daemon=True)
    t.start()

# ── Reconciler ─────────────────────────────────────────────────
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
                # [NEW] guard / local / remote 순으로 소거
                if _entry_guard_active(key):
                    _mark_done("entry", pkey, "(guard)")
                    continue
                with _POS_LOCK:
                    if position_data.get(key):
                        _mark_done("entry", pkey, "(local-exists)")
                        continue
                if _get_remote_any_side(sym):
                    _mark_done("entry", pkey, "(exists)")
                    continue
                with _lock_for(key):
                    now = time.time()
                    if now - item.get("last_try", 0.0) < RECON_INTERVAL_SEC - 1:
                        continue
                    amt, lev = item["amount"], item["leverage"]
                    if RECON_DEBUG:
                        send_telegram(f"🔁 retry [entry] {pkey}")
                    resp = place_market_order(sym, amt,
                                              side=("buy" if side == "long" else "sell"),
                                              leverage=lev, reduce_only=False)
                    item["last_try"] = now
                    item["attempts"] = item.get("attempts", 0) + 1
                    code = str(resp.get("code", ""))
                    if code == "00000":
                        _mark_done("entry", pkey)
                        send_telegram(f"🔁 ENTRY 재시도 성공 {side.upper()} {sym}")
                    elif code.startswith("LOCAL_MIN_QTY") or code.startswith("LOCAL_BAD_QTY"):
                        _mark_done("entry", pkey, "(minQty/badQty)")
                        send_telegram(f"⛔ ENTRY 재시도 스킵 {sym} {side} → {resp}")

            # CLOSE 재시도
            with _PENDING_LOCK:
                close_items = list(_PENDING["close"].items())
            for pkey, item in close_items:
                sym, side = item["symbol"], item["side"]
                key = _key(sym, side)
                p = _get_remote(sym, side)
                if not p or float(p.get("size", 0)) <= 0:
                    _mark_done("close", pkey, "(no-remote)")
                    continue
                with _lock_for(key):
                    now = time.time()
                    if now - item.get("last_try", 0.0) < RECON_INTERVAL_SEC - 1:
                        continue
                    if RECON_DEBUG:
                        send_telegram(f"🔁 retry [close] {pkey}")
                    size = float(p["size"])
                    resp = place_reduce_by_size(sym, size, side)
                    item["last_try"] = now
                    item["attempts"] = item.get("attempts", 0) + 1
                    code = str(resp.get("code", ""))
                    if code == "00000":
                        _mark_done("close", pkey)
                        with _POS_LOCK:
                            position_data.pop(key, None)
                        # close 성공 → guard 해제
                        with _ENTRY_GUARD_LOCK:
                            _ENTRY_GUARD.pop(key, None)
                        send_telegram(f"🔁 CLOSE 재시도 성공 {side.upper()} {sym}")
                    elif code.startswith("LOCAL_MIN_QTY") or code.startswith("LOCAL_BAD_QTY"):
                        _mark_done("close", pkey, "(minQty/badQty)")
                        send_telegram(f"⛔ CLOSE 재시도 스킵 {sym} {side} → {resp}")
        except Exception:
            pass

def start_reconciler():
    t = threading.Thread(target=_reconciler_loop, daemon=True)
    t.start()
