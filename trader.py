# -*- coding: utf-8 -*-
"""
Trader core (추가/개선 드랍인)
- 기존 로직/기능(엔트리/리콘실러/응급정지/브레이크이븐/적응형 큐레이션/텔레그램 알림 등) 유지
- 추가(요청 반영):
  1) 진입 초반 보호(Grace): 시간+ROI 범위에서 전량 종료 금지, 소량 컷만 허용
  2) MFE 쌓이기 전 컷 완화 + 이중 트레일링(초반 타이트, 추세 확립 후 느슨)
  3) 스테이징 3단계: 1차(동적), 2차(부분), 3차(전량) — 계단형
  4) 숏 연속성 보정: 계단식 하락은 더 태움
  5) Profit Lock: 큰 추세 도달 시 부분 확정 + 초장기 트레일
  6) 변동성/휩쏘(선택): get_klines 있을 때 ATR%/윅 비율로 컷 임계 가감
  7) 봉(Bar) 기반 휩쏘 내성: WHIP_MODE=bar 일 때 최소 보유 봉/연속 봉 컨펌/MFE 씨앗 보장
"""

import os
import time
import threading
from typing import Dict, Optional, Any, List, Tuple

# ---- Bitget API wrapper ----
from bitget_api import (
    convert_symbol,
    get_last_price,
    get_open_positions,
    place_market_order,
    place_reduce_by_size,
    get_symbol_spec,
    round_down_step,
)

# (선택) 캔들 조회가 지원되면 휩쏘/ATR 계산에 사용됨. 없으면 자동 비활성.
try:
    from bitget_api import get_klines  # get_klines(symbol, interval, limit)
    _HAS_KLINES = True
except Exception:
    _HAS_KLINES = False

# ---- Telegram ----
try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str):
        print("[TG]", msg)

# ---- Filters (fallback 안전장치) ----
try:
    from filters.runtime_filters import evaluate_position, evaluate_position_adaptive
except Exception:
    def evaluate_position(**kwargs): return ("hold", "keep")
    def evaluate_position_adaptive(**kwargs): return ("hold", "keep")

try:
    from filters.adaptive_thresholds import compute as compute_adaptive
except Exception:
    def compute_adaptive(open_positions, meta_map, last_price_fn): return {}

# =======================
#        ENV (공통)
# =======================
LEVERAGE = float(os.getenv("LEVERAGE", "5"))
TRACE_LOG = os.getenv("TRACE_LOG", "0") == "1"

TP1_PCT = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT = float(os.getenv("TP2_PCT", "0.40"))
TP3_PCT = float(os.getenv("TP3_PCT", "0.30"))
TP_EPSILON_RATIO = float(os.getenv("TP_EPSILON_RATIO", "0.0005"))

STOP_PCT = float(os.getenv("STOP_PCT", "0.10"))                 # 레버리지 반영 손실률
STOP_PRICE_MOVE = float(os.getenv("STOP_PRICE_MOVE", "0.025"))   # 원시 가격 불리폭
STOP_CHECK_SEC = float(os.getenv("STOP_CHECK_SEC", "1.0"))
STOP_DEBOUNCE_SEC = float(os.getenv("STOP_DEBOUNCE_SEC", "1.2"))

MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "40"))
CAP_CHECK_SEC = float(os.getenv("CAP_CHECK_SEC", "10"))
LONG_BYPASS_CAP = os.getenv("LONG_BYPASS_CAP", "1") == "1"
STRICT_RESERVE_DISABLE = os.getenv("STRICT_RESERVE_DISABLE", "0") == "1"

ENTRY_INFLIGHT_TTL_SEC = float(os.getenv("ENTRY_INFLIGHT_TTL_SEC", "30"))
ENTRY_DUP_TTL_SEC = float(os.getenv("ENTRY_DUP_TTL_SEC", "60"))

BE_ENABLE = os.getenv("BE_ENABLE", "1") == "1"
BE_AFTER_STAGE = int(os.getenv("BE_AFTER_STAGE", "1"))
BE_EPSILON_RATIO = float(os.getenv("BE_EPSILON_RATIO", "0.0005"))

RECON_INTERVAL_SEC = float(os.getenv("RECON_INTERVAL_SEC", "25"))
RECON_DEBUG = os.getenv("RECON_DEBUG", "0") == "1"

# === 스테이징/트레일 공통 ===
PARTIAL_EXIT_ENABLE = os.getenv("PARTIAL_EXIT_ENABLE", "1") == "1"
PARTIAL_EXIT_REASONS = tuple((os.getenv("PARTIAL_EXIT_REASONS", "trailing_stop,policy_roi,axe")
                               .replace(" ", "").split(",")))
PARTIAL_EXIT_DYNAMIC = os.getenv("PARTIAL_EXIT_DYNAMIC", "1") == "1"
PARTIAL_EXIT_FIRST_MIN = float(os.getenv("PARTIAL_EXIT_FIRST_MIN", "0.30"))
PARTIAL_EXIT_FIRST_MAX = float(os.getenv("PARTIAL_EXIT_FIRST_MAX", "0.70"))
PARTIAL_EXIT_INITIAL_PCT = float(os.getenv("PARTIAL_EXIT_INITIAL_PCT",
                                           os.getenv("PARTIAL_EXIT_INITAL_PCT", "0.20")))
PARTIAL_EXIT_SECOND_PCT = float(os.getenv("PARTIAL_EXIT_SECOND_PCT", "0.35"))
PARTIAL_EXIT_MIN_SIZE = float(os.getenv("PARTIAL_EXIT_MIN_SIZE", "10"))
PARTIAL_EXIT_GRACE_MINUTES = float(os.getenv("PARTIAL_EXIT_GRACE_MINUTES", "8"))
PARTIAL_EXIT_RETRIGGER_ADVERSE_BP = float(os.getenv("PARTIAL_EXIT_RETRIGGER_ADVERSE_BP", "25"))
PARTIAL_EXIT_RETRIGGER2_BP = float(os.getenv("PARTIAL_EXIT_RETRIGGER2_BP", "50"))
PARTIAL_EXIT_REARM_SEC = float(os.getenv("PARTIAL_EXIT_REARM_SEC", "150"))

TRAIL_AFTER_STAGE_ENABLE = os.getenv("TRAIL_AFTER_STAGE_ENABLE", "1") == "1"
TRAIL_AFTER_STAGE_MFE_BP = float(os.getenv("TRAIL_AFTER_STAGE_MFE_BP", "15"))

BE_LOCK_AFTER_STAGE = os.getenv("BE_LOCK_AFTER_STAGE", "1") == "1"
BE_LOCK_EPS_BP = float(os.getenv("BE_LOCK_EPS_BP", "5"))

# === 진입 초반 보호/이중 트레일 공통 ===
GRACE_MINUTES = float(os.getenv("GRACE_MINUTES", "12"))
GRACE_ROI_RANGE = float(os.getenv("GRACE_ROI_RANGE", "0.05"))
MFE_EARLY_BP = float(os.getenv("MFE_EARLY_BP", "90"))
TRAIL_TIGHT_BP = float(os.getenv("TRAIL_TIGHT_BP", "12"))
TRAIL_LOOSE_BP = float(os.getenv("TRAIL_LOOSE_BP", "35"))

# === 롱/숏 오버라이드 ===
def _env_side(key: str, side: str, fallback: float) -> float:
    side = (side or "").upper()  # "LONG"/"SHORT"
    v = os.getenv(f"{key}_{side}")
    if v is None or v == "":
        v = os.getenv(key)
    try:
        return float(v) if v not in (None, "",) else float(fallback)
    except:
        return float(fallback)

def _grace_minutes(side: str) -> float:    return _env_side("GRACE_MINUTES", side, GRACE_MINUTES)
def _grace_roi_range(side: str) -> float:  return _env_side("GRACE_ROI_RANGE", side, GRACE_ROI_RANGE)
def _trail_tight_bp(side: str) -> float:   return _env_side("TRAIL_TIGHT_BP", side, TRAIL_TIGHT_BP)
def _trail_loose_bp(side: str) -> float:   return _env_side("TRAIL_LOOSE_BP", side, TRAIL_LOOSE_BP)

# === Profit Lock ===
PROFIT_LOCK_ENABLE = os.getenv("PROFIT_LOCK_ENABLE", "1") == "1"
PROFIT_LOCK_LVL_PCT_LONG = float(os.getenv("PROFIT_LOCK_LVL_PCT_LONG", "0.50"))
PROFIT_LOCK_TP_PCT_LONG  = float(os.getenv("PROFIT_LOCK_TP_PCT_LONG", "0.50"))
PROFIT_LOCK_TRAIL_BP_LONG= float(os.getenv("PROFIT_LOCK_TRAIL_BP_LONG", "40"))
PROFIT_LOCK_LVL_PCT_SHORT= float(os.getenv("PROFIT_LOCK_LVL_PCT_SHORT", "0.30"))
PROFIT_LOCK_TP_PCT_SHORT = float(os.getenv("PROFIT_LOCK_TP_PCT_SHORT", "0.40"))
PROFIT_LOCK_TRAIL_BP_SHORT=float(os.getenv("PROFIT_LOCK_TRAIL_BP_SHORT", "25"))

# === 변동성/휩쏘 감지(선택) ===
WHIP_DETECT_ENABLE = os.getenv("WHIP_DETECT_ENABLE", "1") == "1"
VOL_LOOKBACK = int(os.getenv("VOL_LOOKBACK", "20"))
WHIP_BODY_RATIO = float(os.getenv("WHIP_BODY_RATIO", "2.0"))
WHIP_FREQ_TH = float(os.getenv("WHIP_FREQ_TH", "0.35"))
ATR_RELAX_BP = float(os.getenv("ATR_RELAX_BP", "80"))
ATR_TIGHT_BP = float(os.getenv("ATR_TIGHT_BP", "30"))

# === 봉(Bar) 기반 휩쏘 내성 ===
WHIP_MODE = os.getenv("WHIP_MODE", "").lower()  # "bar" 또는 ""
ENTRY_MIN_HOLD_BARS = int(os.getenv("ENTRY_MIN_HOLD_BARS", "0"))
STOP_CONFIRM_BARS = int(os.getenv("STOP_CONFIRM_BARS", "1"))
CLOSE_MIN_MFE_BP = float(os.getenv("CLOSE_MIN_MFE_BP", "0"))
CATASTROPHE_MULTIPLIER = float(os.getenv("CATASTROPHE_MULTIPLIER", "2.0"))

# ROI/h 임계(동적 1차컷)
def _roi_th_for_tf(tf: str) -> Optional[float]:
    tf = (tf or "1h").lower()
    m = {
        "1h": os.getenv("ROI_PER_HOUR_THRESHOLD_1H"),
        "2h": os.getenv("ROI_PER_HOUR_THRESHOLD_2H"),
        "3h": os.getenv("ROI_PER_HOUR_THRESHOLD_3H"),
        "4h": os.getenv("ROI_PER_HOUR_THRESHOLD_4H"),
        "d" : os.getenv("ROI_PER_HOUR_THRESHOLD_D"),
        "1d": os.getenv("ROI_PER_HOUR_THRESHOLD_D"),
    }
    v = m.get(tf)
    try:
        return float(v) if v not in (None, "",) else None
    except:
        return None

# =======================
#      Local caches
# =======================
position_data: Dict[str, dict] = {}
_POS_LOCK = threading.RLock()

_KEY_LOCKS: Dict[str, threading.RLock] = {}
_KEY_LOCKS_LOCK = threading.Lock()

def _key(symbol: str, side: str) -> str:
    return f"{symbol}_{side}"

def _lock_for(key: str) -> threading.RLock:
    with _KEY_LOCKS_LOCK:
        if key not in _KEY_LOCKS:
            _KEY_LOCKS[key] = threading.RLock()
        return _KEY_LOCKS[key]

# 응급정지 연속 확인 카운터(틱/봉)
_STOP_SEQ: Dict[str, int] = {}
_STOP_BAR_SEQ: Dict[str, int] = {}
# 최근 확정 봉 캐시
_LAST_CLOSED_BAR_TS: Dict[str, int] = {}

# =======================
#        Utils
# =======================
def _pnl_usdt(entry: float, exitp: float, notion: float, side: str) -> float:
    if entry <= 0 or notion <= 0: return 0.0
    return ((exitp - entry) / entry if side == "long" else (entry - exitp) / entry) * notion

def _price_move_pct(entry: float, last: float, side: str) -> float:
    if entry <= 0: return 0.0
    raw = (last - entry) / entry
    return raw if side == "long" else -raw

def _loss_ratio_on_margin(entry: float, last: float, side: str, leverage: float) -> float:
    move = _price_move_pct(entry, last, side)
    loss_on_price = max(0.0, -move)
    return loss_on_price * float(leverage)

def _adverse_from_mfe(side: str, last: float, mfe_price: float) -> float:
    if mfe_price <= 0 or last <= 0: return 0.0
    dd = (mfe_price - last) / mfe_price if side == "long" else (last - mfe_price) / mfe_price
    return max(0.0, dd) * 10000.0  # bp

def _mfe_gain_bp(entry: float, mfe_price: float, side: str) -> float:
    if entry <= 0 or mfe_price <= 0: return 0.0
    gain = (mfe_price - entry) / entry if side == "long" else (entry - mfe_price) / entry
    return max(0.0, gain) * 10000.0

def _roi_per_hour(entry: float, last: float, ts_entry: float) -> float:
    if entry <= 0: return 0.0
    elapsed_h = max(1e-6, (time.time() - float(ts_entry)) / 3600.0)
    roi = (float(last) - float(entry)) / float(entry)
    return roi / elapsed_h

def _dynamic_first_pct(tf: str, entry: float, last: float, ts_entry: float) -> float:
    if not PARTIAL_EXIT_DYNAMIC:
        return PARTIAL_EXIT_FIRST_MIN
    th = _roi_th_for_tf(tf) or 0.06
    roi_h = _roi_per_hour(entry, last, ts_entry)
    strength = max(0.0, min(1.0, roi_h / (th * 1.2)))
    age_h = max(0.0, (time.time() - ts_entry) / 3600.0)
    dur_weight = max(0.0, min(1.0, age_h / 12.0))
    w = (strength * 0.7) - (dur_weight * 0.2)
    w = max(0.0, min(1.0, w))
    pct = PARTIAL_EXIT_FIRST_MIN + (1.0 - w) * (PARTIAL_EXIT_FIRST_MAX - PARTIAL_EXIT_FIRST_MIN)
    return max(0.1, min(0.95, pct))

def _in_grace_zone(entry: float, last: float, side: str, entry_ts: float) -> bool:
    # 시간 기반
    if (time.time() - entry_ts) < _grace_minutes(side) * 60.0:
        return True
    # ROI 범위 기반
    if entry > 0 and last > 0:
        roi = (last - entry) / entry if side == "long" else (entry - last) / entry
        return abs(roi) <= _grace_roi_range(side)
    return False

# (선택) 변동성/휩쏘 감지
def _tf_to_interval(tf: str) -> str:
    tf = (tf or "1h").lower()
    return {"1h": "1H", "2h": "2H", "3h": "3H", "4h": "4H", "d": "1D", "1d": "1D"}.get(tf, "1H")

def _now_ms() -> int:
    return int(time.time() * 1000)

def _bar_ms(tf: str) -> int:
    tf = (tf or "1h").lower()
    if tf == "1h": return 60*60*1000
    if tf == "2h": return 2*60*60*1000
    if tf == "3h": return 3*60*60*1000
    if tf == "4h": return 4*60*60*1000
    if tf in ("d", "1d"): return 24*60*60*1000
    return 60*60*1000

def _get_last_closed_bar_ts(symbol: str, tf: str) -> Optional[int]:
    key = f"{symbol}:{tf}"
    if _HAS_KLINES:
        try:
            ks = get_klines(convert_symbol(symbol), _tf_to_interval(tf), 2)
            if ks and len(ks) >= 2:
                return int(ks[-2][0])  # 확정 봉(open time ms)
        except Exception:
            pass
    # Fallback: 로컬 추정
    bar = _bar_ms(tf)
    return ((_now_ms() // bar) - 1) * bar

def _is_new_closed_bar(symbol: str, tf: str) -> bool:
    last_closed = _get_last_closed_bar_ts(symbol, tf)
    if last_closed is None:
        return False
    key = f"{symbol}:{tf}"
    prev = _LAST_CLOSED_BAR_TS.get(key)
    if prev is None or last_closed > prev:
        _LAST_CLOSED_BAR_TS[key] = last_closed
        return True
    return False

def _elapsed_bars_since(ts_entry: float, tf: str, symbol: str) -> int:
    last_closed = _get_last_closed_bar_ts(symbol, tf)
    if last_closed is None:
        return 0
    bar = _bar_ms(tf)
    first_bar_start = (int(ts_entry*1000) // bar) * bar
    if last_closed <= first_bar_start:
        return 0
    return int((last_closed - first_bar_start) // bar)

# =======================
#   Capacity guard & entry throttle
# =======================
_CAPACITY: Dict[str, Any] = {"blocked": False, "short_blocked": False,
                             "last_count": 0, "short_count": 0, "ts": 0.0}
_CAP_LOCK = threading.Lock()

def _total_open_positions_now() -> int:
    try:
        return sum(1 for _ in get_open_positions())
    except Exception:
        return 0

def capacity_status() -> Dict[str, Any]:
    with _CAP_LOCK:
        return dict(_CAPACITY)

def can_enter_now(side: str) -> bool:
    if STRICT_RESERVE_DISABLE:
        return True
    if side == "long" and LONG_BYPASS_CAP:
        return True
    with _CAP_LOCK:
        return not _CAPACITY["short_blocked"]

def _capacity_loop():
    prev = None
    while True:
        try:
            total_count = _total_open_positions_now()
            short_blocked = total_count >= MAX_OPEN_POSITIONS
            now = time.time()
            with _CAP_LOCK:
                _CAPACITY.update({
                    "blocked": short_blocked,
                    "short_blocked": short_blocked,
                    "last_count": total_count,
                    "short_count": total_count,
                    "ts": now,
                })
            if prev != short_blocked and TRACE_LOG:
                send_telegram(f"ℹ️ capacity short_blocked={short_blocked} count={total_count}/{MAX_OPEN_POSITIONS}")
            prev = short_blocked
        except Exception as e:
            print("capacity error:", e)
        time.sleep(CAP_CHECK_SEC)

def start_capacity_guard():
    threading.Thread(target=_capacity_loop, name="capacity-guard", daemon=True).start()

_RESERVE = {"short": 0}
_RES_LOCK = threading.Lock()

def _strict_try_reserve(side: str) -> bool:
    if side == "long" and LONG_BYPASS_CAP:
        return True
    total = _total_open_positions_now()
    with _RES_LOCK:
        effective = total + _RESERVE["short"]
        if effective >= MAX_OPEN_POSITIONS:
            return False
        _RESERVE["short"] += 1
        return True

def _strict_release(side: str):
    if side == "long" and LONG_BYPASS_CAP:
        return
    with _RES_LOCK:
        if _RESERVE["short"] > 0:
            _RESERVE["short"] -= 1

# =======================
#   Entry gates
# =======================
_ENTRY_BUSY: Dict[str, float] = {}
_RECENT_OK: Dict[str, float] = {}
_ENTRY_G_LOCK = threading.Lock()

def _set_busy(key: str):
    with _ENTRY_G_LOCK:
        _ENTRY_BUSY[key] = time.time()

def _clear_busy(key: str):
    with _ENTRY_G_LOCK:
        _ENTRY_BUSY.pop(key, None)

def _is_busy(key: str) -> bool:
    with _ENTRY_G_LOCK:
        ts = _ENTRY_BUSY.get(key, 0.0)
    return (time.time() - ts) < ENTRY_INFLIGHT_TTL_SEC

def _mark_recent_ok(key: str):
    with _ENTRY_G_LOCK:
        _RECENT_OK[key] = time.time()

def _recent_ok(key: str) -> bool:
    with _ENTRY_G_LOCK:
        ts = _RECENT_OK.get(key, 0.0)
    return (time.time() - ts) < ENTRY_DUP_TTL_SEC

# =======================
#       Entry
# =======================
def _get_remote(symbol: str, side: Optional[str] = None):
    symbol = convert_symbol(symbol)
    for p in get_open_positions():
        if p.get("symbol") == symbol and (side is None or p.get("side") == side):
            return p
    return None

def _get_remote_any_side(symbol: str):
    symbol = convert_symbol(symbol)
    for p in get_open_positions():
        if p.get("symbol") == symbol:
            return p
    return None

def enter_position(symbol: str, usdt_amount: float, side: str = "long", leverage: float = None):
    symbol = convert_symbol(symbol)
    side = (side or "long").lower()
    key = _key(symbol, side)
    lev = float(leverage or LEVERAGE)

    if _is_busy(key) or _recent_ok(key):
        if RECON_DEBUG:
            send_telegram(f"⏸️ skip entry (busy/recent) {key}")
        return

    if not STRICT_RESERVE_DISABLE and not _strict_try_reserve(side):
        st = capacity_status()
        send_telegram(f"🧱 STRICT HOLD {symbol} {side} {st['last_count']}/{MAX_OPEN_POSITIONS}")
        return

    try:
        if not can_enter_now(side):
            st = capacity_status()
            send_telegram(f"⏳ ENTRY HOLD {symbol} {side} {st['last_count']}/{MAX_OPEN_POSITIONS}")
            return

        with _lock_for(key):
            if _recent_ok(key) or _local_has_any(symbol) or _get_remote_any_side(symbol):
                return
            _set_busy(key)

            last = get_last_price(symbol)
            if not last or last <= 0:
                if TRACE_LOG:
                    send_telegram(f"❗ ticker_fail {symbol}")
                return

            resp = place_market_order(
                symbol, usdt_amount,
                side=("buy" if side == "long" else "sell"),
                leverage=lev, reduce_only=False
            )
            code = str(resp.get("code", ""))
            if TRACE_LOG:
                send_telegram(f"📦 order_resp code={code} {symbol} {side}")

            if code == "00000":
                with _POS_LOCK:
                    position_data[key] = {
                        "symbol": symbol, "side": side, "entry_usd": usdt_amount,
                        "ts": time.time(), "entry_ts": time.time(),
                        "tf": "1h",
                        "entry_price": float(last),
                        "mfe_price": float(last), "mfe_ts": time.time(),
                        "stage_exit": 0, "stage_ts": 0.0, "trail_after_stage": 0,
                        "favorable_streak": 0, "last_obs": float(last),
                        "profit_lock": 0,
                    }
                send_telegram(f"✅ OPEN {side.upper()} {symbol} amt≈{usdt_amount} lev={lev} last≈{last}")
                _mark_recent_ok(key)
            else:
                send_telegram(f"❌ OPEN FAIL {side.upper()} {symbol} code={code}")

    except Exception as e:
        send_telegram(f"🔥 ENTRY ERR {symbol} {side} {e}")
    finally:
        _clear_busy(key)
        _strict_release(side)

# =======================
#   Reduce / Close
# =======================
def _local_open_count() -> int:
    with _POS_LOCK:
        return len(position_data)

def _local_has_any(symbol: str) -> bool:
    symbol = convert_symbol(symbol)
    with _POS_LOCK:
        return any(k.startswith(symbol + "_") for k in position_data.keys())

def take_partial_profit(symbol: str, pct: float, side: str = "long"):
    symbol = convert_symbol(symbol)
    side = (side or "long").lower()
    key = _key(symbol, side)
    with _lock_for(key):
        p = _get_remote(symbol, side)
        if not p or float(p.get("size", 0)) <= 0:
            send_telegram(f"⚠️ TP 스킵: 원격 포지션 없음 {key}")
            return

        size_step = float(get_symbol_spec(symbol).get("sizeStep", 0.001))
        cur_size = float(p["size"])
        cut_size = round_down_step(cur_size * float(pct), size_step)
        if cut_size <= 0:
            send_telegram(f"⚠️ TP 스킵: 계산된 사이즈=0 {key}")
            return

        resp = place_reduce_by_size(symbol, cut_size, side)
        if str(resp.get("code", "")) == "00000":
            send_telegram(f"✅ TP {pct*100:.0f}% {side.upper()} {symbol} qty≈{cut_size}")
        else:
            send_telegram(f"❌ TP 실패 {side.upper()} {symbol} code={resp.get('code')}")

def _sweep_full_close(symbol: str, side: str, reason: str, max_retry: int = 5, sleep_s: float = 0.3) -> bool:
    for _ in range(max_retry):
        p = _get_remote(symbol, side)
        size = float(p["size"]) if p and p.get("size") else 0.0
        if size <= 0:
            return True
        place_reduce_by_size(symbol, size, side)
        time.sleep(sleep_s)
    p = _get_remote(symbol, side)
    return (not p) or float(p.get("size", 0)) <= 0

def close_position(symbol: str, side: str = "long", reason: str = "manual"):
    symbol = convert_symbol(symbol)
    side = (side or "long").lower()
    key = _key(symbol, side)
    with _lock_for(key):
        p = _get_remote(symbol, side)
        if not p or float(p.get("size", 0)) <= 0:
            send_telegram(f"⚠️ CLOSE 스킵: 원격 포지션 없음 {key}")
            return

        size = float(p["size"])
        resp = place_reduce_by_size(symbol, size, side)
        exit_price = get_last_price(symbol) or float(p.get("entry_price", 0))
        success = str(resp.get("code", "")) == "00000"
        ok = _sweep_full_close(symbol, side, "reconcile") if success else False

        if success or ok:
            entry = float(p.get("entry_price", 0))
            realized = _pnl_usdt(entry, float(exit_price), entry * size, side)
            with _POS_LOCK:
                position_data.pop(key, None)
            send_telegram(
                f"✅ CLOSE {side.upper()} {symbol} ({reason})\n"
                f"• Exit: {exit_price}\n• Size: {size}\n• Realized≈ {realized:+.2f} USDT"
            )
            _mark_recent_ok(key)

def reduce_by_contracts(symbol: str, contracts: float, side: str = "long"):
    symbol = convert_symbol(symbol)
    side = (side or "long").lower()
    key = _key(symbol, side)
    with _lock_for(key):
        step = float(get_symbol_spec(symbol).get("sizeStep", 0.001))
        qty = round_down_step(float(contracts), step)
        if qty <= 0:
            send_telegram(f"⚠️ reduce 스킵: step 미달 {key}")
            return
        resp = place_reduce_by_size(symbol, qty, side)
        if str(resp.get("code", "")) == "00000":
            send_telegram(f"🔻 Reduce {qty} {side.upper()} {symbol}")

# =======================
#       Watchdogs
# =======================
_STOP_FIRE_TS: Dict[str, float] = {}
def _should_fire_stop(key: str) -> bool:
    ts = _STOP_FIRE_TS.get(key, 0.0)
    now = time.time()
    if now - ts < STOP_DEBOUNCE_SEC:
        return False
    _STOP_FIRE_TS[key] = now
    return True

def _profit_lock_check(symbol: str, side: str, entry: float, mfe_price: float, meta: dict):
    if not PROFIT_LOCK_ENABLE:
        return
    if meta.get("profit_lock", 0) == 1:
        return
    if entry <= 0 or mfe_price <= 0:
        return
    gain = (mfe_price - entry) / entry if side == "long" else (entry - mfe_price) / entry
    lvl = PROFIT_LOCK_LVL_PCT_LONG if side == "long" else PROFIT_LOCK_LVL_PCT_SHORT
    if gain >= lvl:
        tp_pct = PROFIT_LOCK_TP_PCT_LONG if side == "long" else PROFIT_LOCK_TP_PCT_SHORT
        try:
            take_partial_profit(symbol, max(0.1, min(0.9, tp_pct)), side=side)
            send_telegram(f"🔒 PROFIT LOCK {side.upper()} {symbol} MFE≈{gain*100:.1f}% TP≈{tp_pct*100:.0f}%")
        except Exception:
            pass
        with _POS_LOCK:
            meta["profit_lock"] = 1
            meta["pl_trail_bp"] = PROFIT_LOCK_TRAIL_BP_LONG if side == "long" else PROFIT_LOCK_TRAIL_BP_SHORT
            position_data[_key(symbol, side)] = meta

def _staged_exit(symbol: str, side: str, reason: str) -> bool:
    """True→단계 컷만 수행(전량 종료 아님). False→스킵or전량."""
    if not PARTIAL_EXIT_ENABLE:
        return False
    key = _key(symbol, side)
    p = _get_remote(symbol, side)
    if not p or float(p.get("size", 0)) <= 0:
        return False

    entry = float(p.get("entry_price") or 0.0)
    size  = float(p.get("size") or 0.0)
    last  = float(get_last_price(symbol) or 0.0)
    if entry <= 0 or size <= 0 or last <= 0:
        return False

    with _POS_LOCK:
        meta = position_data.get(key, {}) or {}
        stage = int(meta.get("stage_exit") or 0)
        ts_entry = float(meta.get("entry_ts") or time.time())
        tf = (meta.get("tf") or "1h").lower()
        mfe_price = float(meta.get("mfe_price") or last)
        last_stage_ts = float(meta.get("stage_ts") or 0.0)
        fav_streak = int(meta.get("favorable_streak") or 0)

    # 변동성/휩쏘(선택) 감지
    whipsaw, atr_bp = _get_whipsaw_and_atr_bp(symbol, tf)

    # 0) 초반 전량 금지: close 신호가 와도 '초소형 컷'만 허용
    in_grace = _in_grace_zone(entry, last, side, ts_entry)
    if stage == 0 and _is_staged_reason(reason) and in_grace:
        pct = max(0.05, min(0.5, PARTIAL_EXIT_INITIAL_PCT))
        take_partial_profit(symbol, pct, side=side)
        with _POS_LOCK:
            meta["stage_exit"] = 1
            meta["stage_ts"] = time.time()
            meta["trail_after_stage"] = 1 if TRAIL_AFTER_STAGE_ENABLE else 0
            position_data[key] = meta
        send_telegram(f"✂️ STAGED EXIT-0 {side.upper()} {symbol} {int(pct*100)}% (grace, {reason})")
        return True

    # 1) 1차: 동적(기존) — grace 아니면 원래 로직
    if stage == 0 and _is_staged_reason(reason):
        first_pct = _dynamic_first_pct(tf, entry, last, ts_entry)
        if size >= PARTIAL_EXIT_MIN_SIZE:
            take_partial_profit(symbol, first_pct, side=side)
            with _POS_LOCK:
                meta["stage_exit"] = 1
                meta["stage_ts"] = time.time()
                meta["trail_after_stage"] = 1 if TRAIL_AFTER_STAGE_ENABLE else 0
                position_data[key] = meta
            send_telegram(f"✂️ STAGED EXIT-1 {side.upper()} {symbol} {int(first_pct*100)}% [{reason}]")
            return True

    # 2) 2차: 되돌림 ≥ retr_bp → 부분 추가 컷
    if stage == 1:
        if time.time() - last_stage_ts < PARTIAL_EXIT_REARM_SEC:
            return True
        mfe_gain = _mfe_gain_bp(entry, mfe_price, side)
        adverse_bp = _adverse_from_mfe(side, last, mfe_price)
        retr_bp = PARTIAL_EXIT_RETRIGGER_ADVERSE_BP * (1.5 if mfe_gain < MFE_EARLY_BP else 1.0)
        if side == "short" and fav_streak >= 3:
            retr_bp *= 1.25
        if atr_bp >= ATR_RELAX_BP:
            retr_bp *= 1.15
        if atr_bp > 0 and atr_bp <= ATR_TIGHT_BP:
            retr_bp *= 0.9
        if whipsaw:
            retr_bp *= 1.15

        if _is_staged_reason(reason) and adverse_bp >= retr_bp and size >= PARTIAL_EXIT_MIN_SIZE:
            pct = max(0.10, min(0.90, PARTIAL_EXIT_SECOND_PCT))
            take_partial_profit(symbol, pct, side=side)
            with _POS_LOCK:
                meta["stage_exit"] = 2
                meta["stage_ts"] = time.time()
                position_data[key] = meta
            send_telegram(f"✂️ STAGED EXIT-2 {side.upper()} {symbol} {int(pct*100)}% "
                          f"[{reason}, adverse≈{adverse_bp:.0f}bp ≥ {retr_bp:.0f}bp]")
            return True
        return True  # 1차까지 하고 대기

    # 3) 3차: 되돌림 ≥ retr2_bp → 전량 종료 (전량 전 MFE 씨앗 보장)
    if stage == 2:
        mfe_gain = _mfe_gain_bp(entry, mfe_price, side)
        if CLOSE_MIN_MFE_BP > 0 and mfe_gain < CLOSE_MIN_MFE_BP:
            return True  # 씨앗 미형성 → 전량 금지

        adverse_bp = _adverse_from_mfe(side, last, mfe_price)
        retr2_bp = PARTIAL_EXIT_RETRIGGER2_BP
        if side == "short" and fav_streak >= 3:
            retr2_bp *= 1.15
        if atr_bp >= ATR_RELAX_BP:
            retr2_bp *= 1.2
        if atr_bp > 0 and atr_bp <= ATR_TIGHT_BP:
            retr2_bp *= 0.9
        if whipsaw:
            retr2_bp *= 1.2

        if adverse_bp >= retr2_bp and _is_staged_reason(reason):
            send_telegram(f"✂️ STAGED EXIT-3 {side.upper()} {symbol} 100% "
                          f"[{reason}, adverse≈{adverse_bp:.0f}bp ≥ {retr2_bp:.0f}bp]")
            close_position(symbol, side=side, reason=f"staged_{reason}")
            with _POS_LOCK:
                meta["stage_exit"] = 3
                position_data[key] = meta
            return False
        return True

    return False

def _watchdog_loop():
    while True:
        try:
            for p in get_open_positions():
                symbol = p.get("symbol")
                side = (p.get("side") or "").lower()
                entry = float(p.get("entry_price") or 0)
                size = float(p.get("size") or 0)
                if not symbol or side not in ("long", "short") or entry <= 0 or size <= 0:
                    continue

                last = get_last_price(symbol)
                if not last:
                    continue
                last = float(last)

                k = _key(symbol, side)
                with _POS_LOCK:
                    meta = position_data.get(k, {}) or {}
                    mfe = float(meta.get("mfe_price") or 0.0)
                    last_obs = float(meta.get("last_obs") or last)
                    ets = float(meta.get("entry_ts") or time.time())
                    trail_armed = int(meta.get("trail_after_stage") or 0)
                    tf = (meta.get("tf") or "1h").lower()

                # 연속성(숏=연속 하락, 롱=연속 상승)
                try:
                    move_up = last > last_obs
                    fav = (not move_up) if side == "short" else move_up
                    fav_streak = int(meta.get("favorable_streak") or 0)
                    fav_streak = fav_streak + 1 if fav else 0
                    meta["favorable_streak"] = fav_streak
                    meta["last_obs"] = last
                except Exception:
                    pass

                # MFE 갱신
                try:
                    better = (last > mfe) if side == "long" else (mfe == 0 or last < mfe)
                    if better:
                        meta["mfe_price"] = float(last)
                        meta["mfe_ts"] = time.time()
                except Exception:
                    pass

                # Profit Lock 체크
                try:
                    _profit_lock_check(symbol, side, entry, float(meta.get("mfe_price") or last), meta)
                except Exception:
                    pass

                # 잔여분 트레일링(이중 + Profit Lock 커스텀)
                try:
                    if trail_armed and TRAIL_AFTER_STAGE_ENABLE:
                        mfe_price = float(meta.get("mfe_price") or last)
                        age_h = max(0.0, (time.time() - ets) / 3600.0)
                        gain_bp = _mfe_gain_bp(entry, mfe_price, side)

                        bp_th = _trail_tight_bp(side) if (gain_bp < MFE_EARLY_BP or age_h < 3.0) else _trail_loose_bp(side)
                        if meta.get("profit_lock", 0) == 1:
                            bp_th = float(meta.get("pl_trail_bp") or bp_th)

                        adverse_bp = _adverse_from_mfe(side, last, mfe_price)
                        if adverse_bp >= bp_th:
                            send_telegram(f"✂️ AUTO CLOSE {side.upper()} {symbol} "
                                          f"[trailing_after_stage, adverse≈{adverse_bp:.0f}bp, th≈{bp_th:.0f}bp]")
                            close_position(symbol, side=side, reason="trailing_after_stage")
                except Exception as _e:
                    print("trail-after-stage error:", _e)

                # ===== 응급정지(레버리지 반영 + 원시 가격) with Bar-based Guards =====
                loss_ratio = _loss_ratio_on_margin(entry, last, side, leverage=LEVERAGE)
                price_loss = max(0.0, -_price_move_pct(entry, last, side))

                pass_guard_min_hold = False
                pass_guard_confirm = False

                if WHIP_MODE == "bar":
                    # 1) 재난급이 아니면: 최소 보유 봉 전엔 전량 종료 금지
                    bars_elapsed = _elapsed_bars_since(ets, tf, symbol)
                    catastrophe = (loss_ratio >= (STOP_PCT * CATASTROPHE_MULTIPLIER)) or \
                                  (price_loss >= (STOP_PRICE_MOVE * CATASTROPHE_MULTIPLIER))
                    if ENTRY_MIN_HOLD_BARS > 0 and bars_elapsed < ENTRY_MIN_HOLD_BARS and not catastrophe:
                        _STOP_BAR_SEQ[k] = 0
                        pass_guard_min_hold = True

                    # 2) 응급정지 연속 '봉' 확인
                    trigger_now = (loss_ratio >= STOP_PCT) or (price_loss >= STOP_PRICE_MOVE)
                    if trigger_now and not catastrophe and STOP_CONFIRM_BARS > 1:
                        if _is_new_closed_bar(symbol, tf):
                            _STOP_BAR_SEQ[k] = _STOP_BAR_SEQ.get(k, 0) + 1
                        if _STOP_BAR_SEQ.get(k, 0) < STOP_CONFIRM_BARS:
                            pass_guard_confirm = True
                    else:
                        _STOP_BAR_SEQ[k] = 0

                # 실제 컷
                if ((loss_ratio >= STOP_PCT) or (price_loss >= STOP_PRICE_MOVE)) and not (pass_guard_min_hold or pass_guard_confirm):
                    if _should_fire_stop(k):
                        send_telegram(
                            f"⛔ {symbol} {side.upper()} emergencyStop "
                            f"loss≈-{loss_ratio*100:.1f}% / price≈-{price_loss*100:.1f}% "
                            f"(th={STOP_PCT*100:.0f}% or {STOP_PRICE_MOVE*100:.0f}%)"
                        )
                        close_position(symbol, side=side, reason="emergencyStop")

                with _POS_LOCK:
                    position_data[k] = meta

        except Exception as e:
            print("watchdog error:", e)
        time.sleep(STOP_CHECK_SEC)

def _breakeven_watchdog():
    if not BE_ENABLE:
        return
    while True:
        try:
            for p in get_open_positions():
                symbol = p.get("symbol")
                side = (p.get("side") or "").lower()
                entry = float(p.get("entry_price") or 0)
                size = float(p.get("size") or 0)
                if not symbol or side not in ("long", "short") or entry <= 0 or size <= 0:
                    continue

                key = _key(symbol, side)
                with _POS_LOCK:
                    st = position_data.get(key, {}) or {}
                    be_armed = bool(st.get("be_armed"))
                    be_entry = float(st.get("be_entry") or 0.0)
                if not (be_armed and be_entry > 0):
                    continue

                last = get_last_price(symbol)
                if not last:
                    continue
                eps = max(be_entry * BE_EPSILON_RATIO, 0.0)
                trigger = (last <= be_entry - eps) if side == "long" else (last >= be_entry + eps)
                if trigger:
                    send_telegram(f"🧷 Breakeven stop → CLOSE {side.upper()} {symbol} "
                                  f"@≈{last} (entry≈{be_entry})")
                    close_position(symbol, side=side, reason="breakeven")
        except Exception as e:
            print("breakeven watchdog error:", e)
        time.sleep(0.8)

# =======================
#   Adaptive curation
# =======================
def _get_whipsaw_and_atr_bp(symbol: str, tf: str) -> Tuple[bool, float]:
    if not (_HAS_KLINES and WHIP_DETECT_ENABLE):
        return (False, 0.0)
    try:
        ks = get_klines(convert_symbol(symbol), _tf_to_interval(tf), VOL_LOOKBACK)
        if not ks or len(ks) < 5: return (False, 0.0)
        wicks_heavy = 0
        atr_sum = 0.0
        atr_base_sum = 0.0
        for k in ks:
            o, h, l, c = float(k[1]), float(k[2]), float(k[3]), float(k[4])
            body = abs(c - o)
            upper = max(0.0, h - max(c, o))
            lower = max(0.0, min(c, o) - l)
            wick_ratio = (upper + lower) / max(1e-9, body)
            if wick_ratio > WHIP_BODY_RATIO: wicks_heavy += 1
            atr_sum += (h - l)
            atr_base_sum += ((h + l) / 2.0)
        whip_freq = wicks_heavy / len(ks)
        atr_pct = (atr_sum / max(1e-9, atr_base_sum))
        atr_bp = atr_pct * 10000.0
        return (whip_freq >= WHIP_FREQ_TH, atr_bp)
    except Exception:
        return (False, 0.0)

def _curation_loop():
    while True:
        try:
            pos = get_open_positions() or []
            with _POS_LOCK:
                meta_map = dict(position_data)

            def _price(sym: str):
                try:
                    return float(get_last_price(sym))
                except Exception:
                    return None

            thresholds = compute_adaptive(pos, meta_map, _price)

            for p in pos:
                symbol = p.get("symbol")
                side = (p.get("side") or "").lower()
                entry = float(p.get("entry_price") or 0)
                size = float(p.get("size") or 0)
                if not symbol or side not in ("long", "short") or entry <= 0 or size <= 0:
                    continue

                last = _price(symbol)
                if not last:
                    continue

                key = _key(symbol, side)
                with _POS_LOCK:
                    meta = meta_map.get(key, {}) or {}
                tf = (meta.get("tf") or "1h").lower()
                ets = float(meta.get("entry_ts") or time.time())
                age_h = max(0.0, (time.time() - ets) / 3600.0)
                mfe_p = float(meta.get("mfe_price") or entry)
                mfe_t = float(meta.get("mfe_ts") or ets)

                th = thresholds.get(key)
                if th:
                    action, reason = evaluate_position_adaptive(
                        tf=tf, side=side, entry=entry, last=float(last),
                        age_h=age_h, mfe_price=mfe_p, mfe_ts=mfe_t,
                        roi_th=th.get("roi_th", 0.01),
                        plateau_bars=th.get("plateau_bars", 24),
                        mfe_bp=th.get("mfe_bp", 30),
                        trail_scale=th.get("trail_scale", 1.0),
                    )
                else:
                    action, reason = evaluate_position(
                        tf=tf, side=side, entry=entry, last=float(last),
                        age_h=age_h, mfe_price=mfe_p, mfe_ts=mfe_t,
                    )

                # close 명령도 진입 초반엔 전량 금지 → 소량컷/스테이징
                if action == "close":
                    try:
                        # MFE 씨앗 보장
                        if CLOSE_MIN_MFE_BP > 0 and _mfe_gain_bp(entry, float(mfe_p), side) < CLOSE_MIN_MFE_BP:
                            pct = max(0.05, min(0.50, PARTIAL_EXIT_INITIAL_PCT))
                            take_partial_profit(symbol, pct, side=side)
                            send_telegram(f"✂️ CLOSE→SEED CUT {side.upper()} {symbol} {int(pct*100)}% (MFE<{CLOSE_MIN_MFE_BP}bp)")
                            continue
                        # 진입 초반 그레이스 → 소량 컷
                        in_grace = _in_grace_zone(entry, float(last), side, ets)
                        if in_grace and _is_staged_reason(reason):
                            pct = max(0.05, min(0.5, PARTIAL_EXIT_INITIAL_PCT))
                            take_partial_profit(symbol, pct, side=side)
                            send_telegram(f"✂️ GRACE CUT {side.upper()} {symbol} {int(pct*100)}% [{reason}]")
                            continue
                    except Exception as _e:
                        print("grace/seed-cut error:", _e)

                    try:
                        staged_only = _staged_exit(symbol, side, reason)
                        if staged_only:
                            continue
                    except Exception as _e:
                        print("staged exit error:", _e)

                    send_telegram(f"✂️ AUTO CLOSE {side.upper()} {symbol} [{reason}]")
                    close_position(symbol, side=side, reason=reason)

                elif action == "reduce":
                    try:
                        in_grace = _in_grace_zone(entry, float(last), side, ets)
                        pct = max(0.10, min(0.30, PARTIAL_EXIT_INITIAL_PCT)) if in_grace else 0.30
                        send_telegram(f"➖ AUTO REDUCE {side.upper()} {symbol} {int(pct*100)}% [{reason}]")
                    except Exception:
                        pct = 0.30
                    take_partial_profit(symbol, pct, side=side)

        except Exception as e:
            print("curation error:", e)
        time.sleep(20)

# =======================
#     Reconciler
# =======================
_PENDING: Dict[str, Dict[str, dict]] = {"entry": {}, "close": {}, "tp": {}}
_PENDING_LOCK = threading.RLock()

def get_pending_snapshot() -> Dict[str, Dict]:
    with _PENDING_LOCK, _CAP_LOCK, _POS_LOCK:
        return {
            "counts": {k: len(v) for k, v in _PENDING.items()},
            "entry_keys": list(_PENDING["entry"].keys()),
            "close_keys": list(_PENDING["close"].keys()),
            "tp_keys": list(_PENDING["tp"].keys()),
            "interval": RECON_INTERVAL_SEC,
            "debug": RECON_DEBUG,
            "capacity": {
                "blocked": _CAPACITY["blocked"],
                "last_count": _CAPACITY["last_count"],
                "short_blocked": _CAPACITY["short_blocked"],
                "short_count": _CAPACITY["short_count"],
                "max": MAX_OPEN_POSITIONS,
                "interval": CAP_CHECK_SEC,
                "ts": _CAPACITY["ts"],
            },
            "local_keys": list(position_data.keys()),
        }

def _reconciler_loop():
    while True:
        try:
            # (필요 시: 엔트리/TP/클로즈 재시도 로직을 넣을 수 있음 — 기본은 모니터링 전용)
            time.sleep(RECON_INTERVAL_SEC)
        except Exception as e:
            print("reconciler error:", e)

# =======================
#       Starters
# =======================
def start_watchdogs():
    threading.Thread(target=_watchdog_loop, name="emergency-stop-watchdog", daemon=True).start()
    if BE_ENABLE:
        threading.Thread(target=_breakeven_watchdog, name="breakeven-watchdog", daemon=True).start()
    threading.Thread(target=_curation_loop, name="curation-loop", daemon=True).start()

def start_reconciler():
    threading.Thread(target=_reconciler_loop, name="reconciler", daemon=True).start()

# =======================
#     Helpers (local)
# =======================
def _is_staged_reason(reason: str) -> bool:
    r = (reason or "").lower()
    return any(x for x in PARTIAL_EXIT_REASONS if x and x in r)
