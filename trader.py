# trader.py — 즉시손절/TP 후 본절/용량가드/알림 통합
import os, time, threading
from typing import Dict, Any, Optional

from bitget_api import (
    convert_symbol, get_last_price, get_open_positions,
    place_market_order, place_reduce_by_size, get_symbol_spec, round_down_step,
)

try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str): print("[TG]", msg)

# ========= ENV =========
DEFAULT_AMOUNT = float(os.getenv("DEFAULT_AMOUNT", "80"))
LEVERAGE       = float(os.getenv("LEVERAGE", "5"))

# 분할 비율(총 100%)
TP1_PCT = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT = float(os.getenv("TP2_PCT", "0.40"))
TP3_PCT = float(os.getenv("TP3_PCT", "0.30"))

# 가격 기준 급락 컷(가격 하락폭%), 5배면 2%≈ROE -10%
STOP_PCT          = float(os.getenv("STOP_PRICE_MOVE", "0.02"))
# **레버리지 반영 ROE 컷** (예: 0.10 == -10% ROE에서 컷)
STOP_ROE          = float(os.getenv("STOP_ROE", "0.10"))

STOP_CHECK_SEC    = float(os.getenv("STOP_CHECK_SEC", "2"))
STOP_CONFIRM_N    = int(float(os.getenv("STOP_CONFIRM_N", "1")))
STOP_DEBOUNCE_SEC = float(os.getenv("STOP_DEBOUNCE_SEC", "2"))
STOP_COOLDOWN_SEC = float(os.getenv("STOP_COOLDOWN_SEC", "3"))

RECON_INTERVAL_SEC = float(os.getenv("RECON_INTERVAL_SEC", "2"))
RECON_DEBUG        = os.getenv("RECON_DEBUG", "0") == "1"

MAX_OPEN_POSITIONS = int(float(os.getenv("MAX_OPEN_POSITIONS", "120")))
CAP_CHECK_SEC      = float(os.getenv("CAP_CHECK_SEC", "5"))
LONG_BYPASS_CAP    = (os.getenv("LONG_BYPASS_CAP", "0") == "1")
SHORT_BYPASS_CAP   = (os.getenv("SHORT_BYPASS_CAP", "0") == "1")

# === 추세 친화 BE 토글 ===
BE_AFTER_TP1 = os.getenv("BE_AFTER_TP1", "1") == "1"   # TP1 후 본절 무기 장전
BE_AFTER_TP2 = os.getenv("BE_AFTER_TP2", "1") == "1"   # TP2 후 본절 무기 장전

# ========= STATE =========
position_data: Dict[str, Dict[str, Any]] = {}
_POS_LOCK = threading.RLock()
_CAP_LOCK = threading.RLock()

_CAPACITY = {"blocked": False, "last_count": 0,
             "short_blocked": False, "long_blocked": False,
             "short_count": 0, "long_count": 0, "ts": 0.0}

def _key(symbol, side):
    s = (side or "").lower()
    if s.startswith("l"): s = "long"
    if s.startswith("s"): s = "short"
    return f"{symbol}_{s}"

def _norm_side(s: str) -> str:
    s = (s or "").lower().strip()
    if s in ("buy","long","l"):  return "long"
    if s in ("sell","short","s"): return "short"
    return s

def _signed_change_pct(side: str, mark: float, entry: float) -> float:
    raw = (mark - entry) / entry if entry > 0 else 0.0
    return raw if side == "long" else -raw

def _price_drawdown_pct(side: str, mark: float, entry: float) -> float:
    chg = _signed_change_pct("short" if side=="long" else "long", mark, entry)
    return abs(chg)

def should_pnl_cut(side: str, mark: float, entry: float, lev: float = None) -> bool:
    lev = float(lev or LEVERAGE or 1.0)
    if entry <= 0 or lev <= 0: 
        return False
    roe = _signed_change_pct(side, mark, entry) * lev
    return roe <= -abs(STOP_ROE)

def _update_local_state_from_exchange():
    opens = get_open_positions()
    with _POS_LOCK:
        seen = set()
        for p in opens:
            sym = convert_symbol(p.get("symbol") or "")
            side = _norm_side(p.get("side"))
            if not sym or side not in ("long","short"): 
                continue
            k = _key(sym, side)
            seen.add(k)
            d = position_data.setdefault(k, {})
            d["size"]  = float(p.get("size") or 0.0)
            d["entry"] = float(p.get("entryPrice") or 0.0)
            if d["size"] <= 0:
                position_data.pop(k, None)
        for k in list(position_data.keys()):
            if k not in seen and position_data.get(k,{}).get("size",0) <= 0:
                position_data.pop(k, None)

# ========= CAPACITY GUARD =========
def _capacity_loop():
    while True:
        try:
            opens = get_open_positions()
            long_c = sum(1 for p in opens if _norm_side(p.get("side"))=="long" and float(p.get("size") or 0)>0)
            short_c= sum(1 for p in opens if _norm_side(p.get("side"))=="short"and float(p.get("size") or 0)>0)
            with _CAP_LOCK:
                _CAPACITY["last_count"] = long_c + short_c
                _CAPACITY["long_count"] = long_c
                _CAPACITY["short_count"]= short_c
                _CAPACITY["blocked"] = (_CAPACITY["last_count"] >= MAX_OPEN_POSITIONS)
                _CAPACITY["long_blocked"]  = (not LONG_BYPASS_CAP)  and _CAPACITY["blocked"]
                _CAPACITY["short_blocked"] = (not SHORT_BYPASS_CAP) and _CAPACITY["blocked"]
        except Exception as e:
            print("capacity err:", e)
        time.sleep(CAP_CHECK_SEC)

def start_capacity_guard():
    threading.Thread(target=_capacity_loop, name="capacity-guard", daemon=True).start()

# ========= TRADING OPS =========
def enter_position(symbol: str, side: str = "long", usdt_amount: Optional[float] = None,
                   leverage: Optional[float] = None, timeframe: Optional[str] = None):
    symbol = convert_symbol(symbol)
    side   = _norm_side(side)
    amount = float(usdt_amount or DEFAULT_AMOUNT)

    with _CAP_LOCK:
        if _CAPACITY["blocked"]:
            if side=="long" and _CAPACITY["long_blocked"]:
                send_telegram(f"⛔ capacity block LONG {symbol} (count={_CAPACITY['last_count']})")
                return {"ok": False, "reason": "cap_block"}
            if side=="short" and _CAPACITY["short_blocked"]:
                send_telegram(f"⛔ capacity block SHORT {symbol} (count={_CAPACITY['last_count']})")
                return {"ok": False, "reason": "cap_block"}

    resp = place_market_order(symbol, amount, side, leverage or LEVERAGE)
    code = str(resp.get("code",""))
    if code != "00000":
        send_telegram(f"❌ OPEN {side.upper()} {symbol} {amount}USDT fail: {resp}")
        return {"ok": False, "resp": resp}

    send_telegram(f"✅ OPEN {side.upper()} {symbol} {amount:.2f}USDT @ {leverage or LEVERAGE}x")
    _update_local_state_from_exchange()
    return {"ok": True}

def reduce_by_contracts(symbol: str, contracts: float, side: str):
    symbol = convert_symbol(symbol)
    side   = _norm_side(side)
    if contracts <= 0:
        return {"ok": False, "reason": "bad_contracts"}
    spec = get_symbol_spec(symbol)
    qty  = round_down_step(float(contracts), float(spec.get("sizeStep", 0.001)))
    if qty <= 0:
        return {"ok": False, "reason": "too_small"}
    resp = place_reduce_by_size(symbol, qty, side)
    code = str(resp.get("code",""))
    if code != "00000":
        send_telegram(f"❌ REDUCE {side.upper()} {symbol} {qty} fail: {resp}")
        return {"ok": False, "resp": resp}
    send_telegram(f"✂️ REDUCE {side.upper()} {symbol} {qty}")
    _update_local_state_from_exchange()
    return {"ok": True}

def take_partial_profit(symbol: str, ratio: float, side: str = "long", reason: str = "tp"):
    symbol = convert_symbol(symbol)
    side   = _norm_side(side)
    if ratio <= 0 or ratio > 1: 
        return {"ok": False, "reason": "bad_ratio"}

    held = 0.0
    for p in get_open_positions():
        if convert_symbol(p.get("symbol"))==symbol and _norm_side(p.get("side"))==side:
            held = float(p.get("size") or 0.0); break
    if held <= 0:
        send_telegram(f"⚠️ TP SKIP: 원격 포지션 없음 {symbol}_{side}")
        return {"ok": False, "reason": "no_position"}

    cut = held * float(ratio)
    spec = get_symbol_spec(symbol)
    cut = round_down_step(cut, float(spec.get("sizeStep", 0.001)))
    if cut <= 0:
        return {"ok": False, "reason": "too_small"}

    resp = place_reduce_by_size(symbol, cut, side)
    if str(resp.get("code","")) != "00000":
        send_telegram(f"❌ TP fail {symbol}_{side} ratio={ratio}: {resp}")
        return {"ok": False, "resp": resp}

    send_telegram(f"🏁 TP({reason}) {side.upper()} {symbol} -{ratio*100:.0f}%")
    with _POS_LOCK:
        d = position_data.setdefault(_key(symbol, side), {})
        d.setdefault("tp1_done", False)
        d.setdefault("tp2_done", False)
        # BE 무기 장전은 토글로 제어
        if (abs(ratio - TP1_PCT) < 1e-6 or ratio <= TP1_PCT) and BE_AFTER_TP1:
            d["tp1_done"] = True
        elif (abs(ratio - TP2_PCT) < 1e-6 or (d.get("tp1_done") and ratio <= (TP1_PCT+TP2_PCT+1e-6))) and BE_AFTER_TP2:
            d["tp2_done"] = True
        if (BE_AFTER_TP1 and d.get("tp1_done")) or (BE_AFTER_TP2 and d.get("tp2_done")):
            d["be_armed"] = True
    _update_local_state_from_exchange()
    return {"ok": True}

def close_position(symbol: str, side: str = "long", reason: str = "manual"):
    symbol = convert_symbol(symbol); side = _norm_side(side)

    held = entry = 0.0
    for p in get_open_positions():
        if convert_symbol(p.get("symbol"))==symbol and _norm_side(p.get("side"))==side:
            held  = float(p.get("size") or 0.0)
            entry = float(p.get("entryPrice") or 0.0)
            break

    if held <= 0:
        send_telegram(f"⚠️ CLOSE 스킵: 원격 포지션 없음 {symbol}_{side}")
        return {"ok": False, "reason": "no_position"}

    exit_px = float(get_last_price(symbol) or 0.0)
    realized = 0.0
    if entry > 0 and exit_px > 0:
        if side == "long":
            realized = (exit_px - entry) * held
        else:
            realized = (entry - exit_px) * held

    spec = get_symbol_spec(symbol)
    qty  = round_down_step(held, float(spec.get("sizeStep", 0.001)))
    resp = place_reduce_by_size(symbol, qty, side)
    if str(resp.get("code","")) != "00000":
        send_telegram(f"❌ CLOSE fail {symbol}_{side}: {resp}")
        return {"ok": False, "resp": resp}

    sign = " +" if realized >= 0 else " "
    send_telegram(
        "✅ CLOSE {side} {sym} ({reason})\n"
        "• Exit: {exit_px}\n"
        "• Size: {qty}\n"
        "• Realized~ {sign}{pnl:.2f} USDT".format(
            side=side.upper(), sym=symbol, reason=reason,
            exit_px=exit_px, qty=qty, sign=sign, pnl=realized
        )
    )

    with _POS_LOCK:
        position_data.pop(_key(symbol, side), None)
    return {"ok": True}

# ========= WATCHDOG =========
def _watchdog_loop():
    last_hit_ts: Dict[str, float] = {}
    confirm_cnt: Dict[str, int] = {}
    cooldown_ts: Dict[str, float] = {}

    while True:
        try:
            for p in get_open_positions():
                symbol = convert_symbol(p.get("symbol") or "")
                side   = _norm_side(p.get("side"))
                size   = float(p.get("size") or 0.0)
                entry  = float(p.get("entryPrice") or 0.0)
                if size <= 0 or entry <= 0 or side not in ("long","short"):
                    continue

                mark = float(get_last_price(symbol) or 0.0)
                k = _key(symbol, side)

                hit_pnl = should_pnl_cut(side, mark, entry, LEVERAGE)           # ROE -10% 컷
                hit_price = _price_drawdown_pct(side, mark, entry) >= STOP_PCT  # 가격 ±2% 컷

                be_fire = False
                with _POS_LOCK:
                    d = position_data.setdefault(k, {})
                    d.setdefault("entry", entry)
                    d.setdefault("size", size)
                    if d.get("be_armed"):
                        if (side=="long" and mark <= entry) or (side=="short" and mark >= entry):
                            be_fire = True

                now = time.time()
                if be_fire or hit_pnl or hit_price:
                    if now < cooldown_ts.get(k, 0):
                        continue
                    confirm_cnt[k] = confirm_cnt.get(k, 0) + 1
                    last_hit_ts[k] = now
                    if confirm_cnt[k] >= max(1, STOP_CONFIRM_N):
                        reason = "breakeven" if be_fire else ("failcut" if hit_pnl else "stop")
                        close_position(symbol, side, reason=reason)
                        cooldown_ts[k] = now + STOP_COOLDOWN_SEC
                        confirm_cnt[k] = 0
                else:
                    if now - last_hit_ts.get(k, 0) > STOP_DEBOUNCE_SEC:
                        confirm_cnt[k] = 0

        except Exception as e:
            print("watchdog err:", e)
        time.sleep(STOP_CHECK_SEC)

def start_watchdogs():
    threading.Thread(target=_watchdog_loop, name="stop-watchdog", daemon=True).start()

# ========= RECON =========
def _reconcile_loop():
    while True:
        try:
            if RECON_DEBUG:
                print("recon positions:", get_open_positions())
            _update_local_state_from_exchange()
        except Exception as e:
            print("recon err:", e)
        time.sleep(RECON_INTERVAL_SEC)

def start_reconciler():
    threading.Thread(target=_reconcile_loop, name="reconciler", daemon=True).start()

# ========= RUNTIME OVERRIDES =========
def runtime_overrides(changed: Dict[str, Any]):
    global STOP_PCT, RECON_INTERVAL_SEC, TP1_PCT, TP2_PCT, TP3_PCT, STOP_ROE, BE_AFTER_TP1, BE_AFTER_TP2
    if "STOP_PRICE_MOVE" in changed: STOP_PCT = float(changed["STOP_PRICE_MOVE"])
    if "STOP_ROE" in changed:        STOP_ROE = float(changed["STOP_ROE"])
    if "RECON_INTERVAL_SEC" in changed: RECON_INTERVAL_SEC = float(changed["RECON_INTERVAL_SEC"])
    if "TP1_PCT" in changed: TP1_PCT = float(changed["TP1_PCT"])
    if "TP2_PCT" in changed: TP2_PCT = float(changed["TP2_PCT"])
    if "TP3_PCT" in changed: TP3_PCT = float(changed["TP3_PCT"])
    if "BE_AFTER_TP1" in changed: BE_AFTER_TP1 = bool(int(changed["BE_AFTER_TP1"]))
    if "BE_AFTER_TP2" in changed: BE_AFTER_TP2 = bool(int(changed["BE_AFTER_TP2"]))

def apply_runtime_overrides(changed: Dict[str, Any]):
    return runtime_overrides(changed)

def get_pending_snapshot():
    with _POS_LOCK:
        return {"positions": dict(position_data)}
