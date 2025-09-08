# trader.py — 체결/감시/부분익절/종료 알림 강화, -10% 컷(가격/PNL) 동시지원
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

# --- 선택: PnL 정확컷 연산 모듈이 있으면 사용 ---
try:
    from pnl_guard import should_pnl_cut
except Exception:
    def should_pnl_cut(*args, **kwargs): return False

# ============= ENV =============
DEFAULT_AMOUNT = float(os.getenv("DEFAULT_AMOUNT", "80"))
LEVERAGE       = float(os.getenv("LEVERAGE", "5"))

TP1_PCT = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT = float(os.getenv("TP2_PCT", "0.40"))
TP3_PCT = float(os.getenv("TP3_PCT", "0.30"))

# “2% 급반전 컷”: 1~3회 연속확인 허용 형태도 파싱(‘1-3’,‘1~3’ 가능)
def _parse_stop_confirm(env: str, default: int = 1) -> int:
    s = (env or "").strip()
    if not s: return default
    for sep in ("~","-",".."):
        if sep in s:
            try:
                a,b = s.split(sep,1)
                return int(b.strip())
            except Exception:
                break
    try:
        return int(float(s))
    except Exception:
        return default

STOP_PCT          = float(os.getenv("STOP_PRICE_MOVE", "0.02"))   # 가격 -2% (≈ ROE -10%@5x)
STOP_CHECK_SEC    = float(os.getenv("STOP_CHECK_SEC", "2"))
STOP_CONFIRM_N    = _parse_stop_confirm(os.getenv("STOP_CONFIRM_N", "1"))
STOP_DEBOUNCE_SEC = float(os.getenv("STOP_DEBOUNCE_SEC", "2"))
STOP_COOLDOWN_SEC = float(os.getenv("STOP_COOLDOWN_SEC", "3"))

RECON_INTERVAL_SEC = float(os.getenv("RECON_INTERVAL_SEC", "2"))
RECON_DEBUG        = os.getenv("RECON_DEBUG", "0") == "1"

MAX_OPEN_POSITIONS = int(float(os.getenv("MAX_OPEN_POSITIONS", "120")))
CAP_CHECK_SEC      = float(os.getenv("CAP_CHECK_SEC", "5"))
LONG_BYPASS_CAP    = (os.getenv("LONG_BYPASS_CAP", "0") == "1")

# ============= state =============
position_data: Dict[str, Dict[str, Any]] = {}
_POS_LOCK = threading.RLock()
_CAP_LOCK = threading.RLock()

_CAPACITY = {"blocked": False, "last_count": 0, "short_blocked": False, "long_blocked": False,
             "short_count": 0, "long_count": 0, "ts": 0.0}

def _key(symbol, side):
    s = (side or "").lower()
    if s.startswith("l"): s="long"
    if s.startswith("s"): s="short"
    return f"{convert_symbol(symbol)}_{s}"

# ===== capacity guard =====
def _count_positions():
    long_c = short_c = 0
    for p in get_open_positions():
        sd = (p.get("side") or "").lower()
        if sd == "long": long_c += 1
        elif sd == "short": short_c += 1
    return {"long": long_c, "short": short_c}

def _cap_guard_tick():
    with _CAP_LOCK:
        c = _count_positions()
        _CAPACITY.update({
            "last_count": c["long"] + c["short"],
            "long_count": c["long"], "short_count": c["short"],
            "long_blocked": (c["long"] >= MAX_OPEN_POSITIONS) and not LONG_BYPASS_CAP,
            "short_blocked": (c["short"] >= MAX_OPEN_POSITIONS),
            "blocked": (c["long"] + c["short"]) >= MAX_OPEN_POSITIONS,
            "ts": time.time(),
        })

def _cap_guard_loop():
    while True:
        try: _cap_guard_tick()
        except Exception as e: print("cap_guard err:", e)
        time.sleep(CAP_CHECK_SEC)

def start_capacity_guard():
    threading.Thread(target=_cap_guard_loop, name="cap-guard", daemon=True).start()

# ===== 로컬 포지션 캐시 =====
def _set_local(symbol, side, size, entry):
    with _POS_LOCK:
        position_data[_key(symbol, side)] = {"size": size, "entry": entry,
                                             "opened_ts": time.time(),
                                             "tp_hit": False, "breakeven": False}

def _rm_local(symbol, side):
    with _POS_LOCK:
        position_data.pop(_key(symbol, side), None)

def _upd_local_qty(symbol, side, new_size):
    with _POS_LOCK:
        k = _key(symbol, side)
        if k in position_data:
            position_data[k]["size"] = new_size

# ===== 진입/청산 =====
def enter_position(symbol: str, side: str = "long", usdt_amount: Optional[float] = None,
                   leverage: Optional[float] = None, timeframe: Optional[str] = None):
    symbol = convert_symbol(symbol)
    side   = (side or "").lower().strip()
    amount = float(usdt_amount or DEFAULT_AMOUNT)

    # capacity guard
    if (_CAPACITY["blocked"]) or (_CAPACITY["long_blocked"] and side=="long") or (_CAPACITY["short_blocked"] and side=="short"):
        send_telegram(f"⛔ capacity block {side.upper()} {symbol}")
        return {"ok": False, "reason": "cap_block"}

    # 거래소 주문: 금액(USDT) → 수량 변환은 bitget_api 내부에서 1회만 수행
    resp = place_market_order(symbol, amount, side, leverage or LEVERAGE)
    if str(resp.get("code", "")) != "00000":
        send_telegram(f"❌ ENTER FAIL {side.upper()} {symbol} → {resp}")
        return {"ok": False, "reason": "exchange"}

    # 체결 후 로컬 기록
    price = float(get_last_price(symbol) or 0.0)
    qty   = 0.0
    try:
        spec = get_symbol_spec(symbol)
        qty = round_down_step(amount / max(price, 1e-9), spec.get("sizeStep"))
    except Exception:
        pass
    _set_local(symbol, side, qty, price)
    send_telegram(f"✅ ENTER {side.upper()} {symbol} amt≈{amount} qty≈{qty} lev={leverage or LEVERAGE}x")
    return {"ok": True}

def take_partial_profit(symbol: str, ratio: float, side: str = "long", reason: str = "tp"):
    symbol = convert_symbol(symbol); side = (side or "").lower().strip()
    if ratio <= 0 or ratio > 1: return {"ok": False, "reason": "bad_ratio"}

    # 현재 포지션 사이즈
    held = 0.0; entry = 0.0
    for p in get_open_positions():
        if (p.get("symbol")==symbol) and ((p.get("side") or "").lower()==side):
            held = float(p.get("size") or 0.0)
            entry = float(p.get("entryPrice") or 0.0)
            break
    if held <= 0: return {"ok": False, "reason": "no_position"}

    cut = held * ratio
    spec = get_symbol_spec(symbol)
    cut = round_down_step(cut, spec.get("sizeStep"))
    if cut <= 0: return {"ok": False, "reason": "too_small"}

    resp = place_reduce_by_size(symbol, cut, side)
    if str(resp.get("code","")) != "00000":
        send_telegram(f"❌ TP FAIL {side.upper()} {symbol} → {resp}")
        return {"ok": False, "reason": "exchange"}

    mark = float(get_last_price(symbol) or 0.0)
    realized = (mark-entry)*cut if side=="long" else (entry-mark)*cut
    send_telegram(f"✂️ TP {side.UPPER() if hasattr(str,'UPPER') else side.upper()} {symbol} ratio={ratio:.2f} size≈{cut}  realized≈{realized:+.2f} USDT")

    # 잔여 수량 반영 + TP가 발생하면 ‘손절폭을 본절’(breakeven) 로직 활성화
    new_sz = max(0.0, held - cut)
    _upd_local_qty(symbol, side, new_sz)
    with _POS_LOCK:
        k = _key(symbol, side)
        if k in position_data:
            position_data[k]["tp_hit"] = True
            position_data[k]["breakeven"] = True  # 이후 급반전 시 본절 기준으로 컷
    return {"ok": True}

def close_position(symbol: str, side: str = "long", reason: str = "close"):
    symbol = convert_symbol(symbol); side=(side or "").lower().strip()
    held = 0.0; entry = 0.0
    for p in get_open_positions():
        if (p.get("symbol")==symbol) and ((p.get("side") or "").lower()==side):
            held = float(p.get("size") or 0.0)
            entry = float(p.get("entryPrice") or 0.0)
            break
    if held <= 0:
        send_telegram(f"⚠️ CLOSE SKIP {side.upper()} {symbol} size≈0")
        return

    spec = get_symbol_spec(symbol)
    held = round_down_step(held, spec.get("sizeStep"))
    resp = place_reduce_by_size(symbol, held, side)
    if str(resp.get("code","")) != "00000":
        send_telegram(f"❌ CLOSE FAIL {side.upper()} {symbol} → {resp}")
        return

    _rm_local(symbol, side)
    mark = float(get_last_price(symbol) or 0.0)
    realized = (mark-entry)*held if side=="long" else (entry-mark)*held
    try:
        roe = (realized / max(1e-9, (held*entry)/LEVERAGE)) * 100.0
    except Exception:
        roe = 0.0
    send_telegram(
        f"✅ CLOSE {side.upper()} {symbol} ({reason})\n"
        f"• Exit: {mark}\n"
        f"• Size: {held}\n"
        f"• Realized≈ {realized:+.2f} USDT | ROE≈ {roe:.2f}%"
    )

# ===== 감시(-10% 컷 + 2% 급반전) =====
_last_trig: Dict[str, float] = {}
def _price_drawdown_pct(side: str, mark: float, entry: float) -> float:
    if not mark or not entry: return 0.0
    return (entry - mark) / entry if side == "long" else (mark - entry) / entry

def _watchdog_loop():
    while True:
        try:
            for p in get_open_positions():
                symbol = convert_symbol(p.get("symbol"))
                side   = (p.get("side") or "").lower()
                size   = float(p.get("size") or 0.0)
                entry  = float(p.get("entryPrice") or 0.0)
                if size <= 0 or not entry: continue

                mark = float(get_last_price(symbol) or 0.0)
                if mark <= 0: continue

                # ① 레버리지 5배 기준 PNL -10% (≈ 가격 -2%) 즉시 컷
                #    정확한 PnL 모듈이 있으면 우선 적용
                if should_pnl_cut(symbol=symbol, side=side, entry=entry, mark=mark, lev=LEVERAGE):
                    close_position(symbol, side=side, reason="pnl_guard")
                    continue

                dd = _price_drawdown_pct(side, mark, entry)  # 가격기준 하락률
                hit = dd >= STOP_PCT

                # ② TP가 일부 발생한 뒤에는 ‘본절 기준’으로 재설정(급반전 보호)
                k = _key(symbol, side)
                be = False
                with _POS_LOCK:
                    be = position_data.get(k, {}).get("breakeven", False)
                if be:
                    if side == "long" and mark <= entry: hit = True
                    if side == "short" and mark >= entry: hit = True

                # 연속확인 카운트 → STOP_CONFIRM_N회 연속일 때만 컷
                if hit:
                    cnt = _last_trig.get(k, 0.0) + 1
                    _last_trig[k] = cnt
                    if cnt >= STOP_CONFIRM_N:
                        _last_trig[k] = 0
                        close_position(symbol, side=side, reason="stop_guard")
                        time.sleep(STOP_DEBOUNCE_SEC)
                else:
                    _last_trig[k] = 0
        except Exception as e:
            print("watchdog err:", e)
        time.sleep(STOP_CHECK_SEC)

def start_watchdogs():
    threading.Thread(target=_watchdog_loop, name="watchdog", daemon=True).start()

# ===== reconciler (간단형) =====
def _recon_loop():
    # 거래소/로컬 간 드리프트 최소화(간단 버전)
    while True:
        try:
            pass  # 현재 구조에선 별도 재시도 필요 없음(실패 시 텔레그램으로 알림)
        except Exception as e:
            print("recon err:", e)
        time.sleep(RECON_INTERVAL_SEC)

def start_reconciler():
    threading.Thread(target=_recon_loop, name="reconciler", daemon=True).start()

def get_pending_snapshot() -> Dict[str, Any]:
    with _POS_LOCK, _CAP_LOCK:
        return {
            "local_keys": list(position_data.keys()),
            "capacity": {
                "blocked": _CAPACITY["blocked"],
                "last_count": _CAPACITY["last_count"],
                "long_blocked": _CAPACITY["long_blocked"],
                "short_blocked": _CAPACITY["short_blocked"],
                "long_count": _CAPACITY["long_count"],
                "short_count": _CAPACITY["short_count"],
                "max": MAX_OPEN_POSITIONS,
                "ts": _CAPACITY["ts"],
            },
        }
