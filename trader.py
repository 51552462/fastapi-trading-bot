# trader.py — 체결/감시/부분익절/종료 알림 강화
# - reduce_by_contracts 복구
# - 레버리지 반영 ROE -10% 즉시 손절 (STOP_ROE, 기본 0.10)
# - TP1/TP2 이후 본절(BE) 도달 시 즉시 전체 종료
# - 종료/TP/스킵 등 텔레그램 문구 복구
# - 용량가드(capacity guard) 유지

import os, time, threading
from typing import Dict, Any, Optional

from bitget_api import (
    convert_symbol, get_last_price, get_open_positions,
    place_market_order, place_reduce_by_size, get_symbol_spec, round_down_step,
)

try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str):
        print("[TG]", msg)

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

# ========= STATE =========
# per (symbol, side) 상태 저장 (TP 진행/본절 무기 장전 등)
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
    # (mark-entry)/entry 의 부호를 포지션 방향에 맞춤
    raw = (mark - entry) / entry if entry > 0 else 0.0
    return raw if side == "long" else -raw

def _price_drawdown_pct(side: str, mark: float, entry: float) -> float:
    # 가격 기준 하락폭(%). 롱이면 내려갈수록 +, 숏이면 올라갈수록 +
    chg = _signed_change_pct("short" if side=="long" else "long", mark, entry)
    return abs(chg)

def should_pnl_cut(side: str, mark: float, entry: float, lev: float = None) -> bool:
    """레버리지 반영 ROE 기반 즉시손절. lev 미지정 시 ENV."""
    lev = float(lev or LEVERAGE or 1.0)
    if entry <= 0 or lev <= 0: 
        return False
    roe = _signed_change_pct(side, mark, entry) * lev  # 예: -0.105 == -10.5% ROE
    return roe <= -abs(STOP_ROE)

def _update_local_state_from_exchange():
    """거래소 포지션을 읽어 로컬상태(entry/size)를 대략 동기화."""
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
def _count_positions():
    opens = get_open_positions()
    long_c = sum(1 for p in opens if _norm_side(p.get("side"))=="long" and float(p.get("size") or 0)>0)
    short_c= sum(1 for p in opens if _norm_side(p.get("side"))=="short"and float(p.get("size") or 0)>0)
    return long_c, short_c

def _cap_guard_tick():
    try:
        long_c, short_c = _count_positions()
        with _CAP_LOCK:
            _CAPACITY["last_count"] = long_c + short_c
            _CAPACITY["long_count"] = long_c
            _CAPACITY["short_count"]= short_c
            _CAPACITY["blocked"] = (_CAPACITY["last_count"] >= MAX_OPEN_POSITIONS)
            _CAPACITY["long_blocked"]  = (not LONG_BYPASS_CAP)  and _CAPACITY["blocked"]
            _CAPACITY["short_blocked"] = (not SHORT_BYPASS_CAP) and _CAPACITY["blocked"]
    except Exception as e:
        print("capacity err:", e)

def _cap_guard_loop():
    while True:
        _cap_guard_tick()
        time.sleep(CAP_CHECK_SEC)

def start_capacity_guard():
    threading.Thread(target=_cap_guard_loop, name="capacity-guard", daemon=True).start()

# ========= TRADING OPS =========
def _set_local(symbol, side, size, entry):
    with _POS_LOCK:
        d = position_data.setdefault(_key(symbol, side), {})
        d["size"]  = float(size or 0.0)
        d["entry"] = float(entry or 0.0)

def _rm_local(symbol, side):
    with _POS_LOCK:
        position_data.pop(_key(symbol, side), None)

def _upd_local_qty(symbol, side, new_size):
    with _POS_LOCK:
        d = position_data.setdefault(_key(symbol, side), {})
        d["size"] = float(new_size or 0.0)

def enter_position(symbol: str, side: str = "long", usdt_amount: Optional[float] = None,
                   leverage: Optional[float] = None, timeframe: Optional[str] = None):
    symbol = convert_symbol(symbol)
    side   = _norm_side(side)
    amount = float(usdt_amount or DEFAULT_AMOUNT)

    # capacity guard
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
    """TradingView가 contracts(수량)로 보낼 때 reduce-only 실행"""
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
        send_telegram(f"❌ REDUCE {side.UPPER()} {symbol} {qty} fail: {resp}")
        return {"ok": False, "resp": resp}
    send_telegram(f"✂️ REDUCE {side.upper()} {symbol} {qty}")
    _update_local_state_from_exchange()
    return {"ok": True}

def take_partial_profit(symbol: str, ratio: float, side: str = "long", reason: str = "tp"):
    symbol = convert_symbol(symbol)
    side   = _norm_side(side)
    if ratio <= 0 or ratio > 1: 
        return {"ok": False, "reason": "bad_ratio"}

    # 현재 원격 사이즈 조회
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
    # TP1/TP2 이후 본절 무기 장전
    with _POS_LOCK:
        d = position_data.setdefault(_key(symbol, side), {})
        d.setdefault("tp1_done", False)
        d.setdefault("tp2_done", False)
        if abs(ratio - TP1_PCT) < 1e-6 or ratio <= TP1_PCT:
            d["tp1_done"] = True
        elif abs(ratio - TP2_PCT) < 1e-6 or (d.get("tp1_done") and ratio <= (TP1_PCT+TP2_PCT+1e-6)):
            d["tp2_done"] = True
        if d.get("tp1_done") or d.get("tp2_done"):
            d["be_armed"] = True
    _update_local_state_from_exchange()
    return {"ok": True}

def close_position(symbol: str, side: str = "long", reason: str = "manual"):
    symbol = convert_symbol(symbol); side = _norm_side(side)
    held = 0.0
    for p in get_open_positions():
        if convert_symbol(p.get("symbol"))==symbol and _norm_side(p.get("side"))==side:
            held = float(p.get("size") or 0.0); break
    if held <= 0:
        send_telegram(f"⚠️ CLOSE 스킵: 원격 포지션 없음 {symbol}_{side}")
        return {"ok": False, "reason": "no_position"}

    spec = get_symbol_spec(symbol)
    held = round_down_step(held, float(spec.get("sizeStep", 0.001)))
    resp = place_reduce_by_size(symbol, held, side)
    if str(resp.get("code","")) != "00000":
        send_telegram(f"❌ CLOSE fail {symbol}_{side}: {resp}")
        return {"ok": False, "resp": resp}

    send_telegram(f"✅ CLOSE {side.upper()} {symbol} ({reason})")
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

                # ROE 기반 -10% 컷
                hit_pnl = should_pnl_cut(side, mark, entry, LEVERAGE)
                # 가격 하락폭 기반 컷 (예: 2%)
                hit_price = _price_drawdown_pct(side, mark, entry) >= STOP_PCT

                # TP 이후 본절(BE) 즉시 종료
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

# ========= ADMIN APPLY (선택) =========
def runtime_overrides(changed: Dict[str, Any]):
    """admin이 보낸 런타임 파라미터 반영"""
    global STOP_PCT, RECON_INTERVAL_SEC, TP1_PCT, TP2_PCT, TP3_PCT, STOP_ROE
    if "STOP_PRICE_MOVE" in changed: STOP_PCT = float(changed["STOP_PRICE_MOVE"])
    if "STOP_ROE" in changed:        STOP_ROE = float(changed["STOP_ROE"])
    if "RECON_INTERVAL_SEC" in changed: RECON_INTERVAL_SEC = float(changed["RECON_INTERVAL_SEC"])
    if "TP1_PCT" in changed: TP1_PCT = float(changed["TP1_PCT"])
    if "TP2_PCT" in changed: TP2_PCT = float(changed["TP2_PCT"])
    if "TP3_PCT" in changed: TP3_PCT = float(changed["TP3_PCT"])

# admin_runtime.py는 apply_runtime_overrides(...)를 찾음 → 호환 alias
def apply_runtime_overrides(changed: Dict[str, Any]):
    return runtime_overrides(changed)

# ===== 기타 도우미 ==================================================
def get_pending_snapshot():
    with _POS_LOCK:
        return {"positions": dict(position_data)}
