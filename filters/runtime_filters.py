# filters/runtime_filters.py
import os, time
from typing import Literal, Tuple

def _f(name, default): 
    try: return float(os.getenv(name, str(default)))
    except: return float(default)

def _i(name, default): 
    try: return int(os.getenv(name, str(default)))
    except: return int(default)

ROI_TH = {
    "1h": _f("ROI_PER_HOUR_THRESHOLD_1H", 0.02),
    "2h": _f("ROI_PER_HOUR_THRESHOLD_2H", 0.015),
    "3h": _f("ROI_PER_HOUR_THRESHOLD_3H", 0.01),
    "4h": _f("ROI_PER_HOUR_THRESHOLD_4H", 0.01),
    "d":  _f("ROI_PER_HOUR_THRESHOLD_D",   0.005),
}

PLATEAU_BARS = {
    "1h": _i("PLATEAU_BARS_1H", 36),
    "2h": _i("PLATEAU_BARS_2H", 24),
    "3h": _i("PLATEAU_BARS_3H", 18),
    "4h": _i("PLATEAU_BARS_4H", 14),
    "d":  _i("PLATEAU_BARS_D",  6),
}

MFE_DBPS = {
    "1h": _i("MFE_DELTA_BP_1H", 30),
    "2h": _i("MFE_DELTA_BP_2H", 28),
    "3h": _i("MFE_DELTA_BP_3H", 25),
    "4h": _i("MFE_DELTA_BP_4H", 25),
    "d":  _i("MFE_DELTA_BP_D",  22),
}

MIN_HOLD = {
    "1h": _f("MIN_HOLD_HOURS_1H", 0.5),
    "2h": _f("MIN_HOLD_HOURS_2H", 0.75),
    "3h": _f("MIN_HOLD_HOURS_3H", 1.0),
    "4h": _f("MIN_HOLD_HOURS_4H", 1.5),
    "d":  _f("MIN_HOLD_HOURS_D",  6.0),
}

# trailing tiers
T1 = (_f("TRAIL_TIER1_PCT", 0.05), _f("TRAIL_TIER1_BACK", 0.02))
T2 = (_f("TRAIL_TIER2_PCT", 0.10), _f("TRAIL_TIER2_BACK", 0.04))
T3 = (_f("TRAIL_TIER3_PCT", 0.20), _f("TRAIL_TIER3_BACK", 0.08))

def _tf_to_minutes(tf: str) -> int:
    m = {"1h":60, "2h":120, "3h":180, "4h":240, "d":1440}
    return m.get((tf or "1h").lower(), 60)

def _pnl_pct(entry: float, last: float, side: str) -> float:
    if entry <= 0: return 0.0
    return (last-entry)/entry if side=="long" else (entry-last)/entry

def _roi_per_hour(entry: float, last: float, side: str, age_h: float) -> float:
    if age_h <= 0: return 0.0
    return _pnl_pct(entry,last,side)/age_h

def _plateau_due(tf: str, mfe_ts: float) -> bool:
    if mfe_ts <= 0: return False
    age_min = (time.time() - mfe_ts)/60.0
    bars = PLATEAU_BARS.get(tf, 24)
    need_min = bars * _tf_to_minutes(tf)
    return age_min >= need_min

def _mfe_pullback(entry: float, last: float, side: str, mfe_price: float, tf: str) -> bool:
    if mfe_price <= 0: return False
    delta = (mfe_price-last)/mfe_price if side=="long" else (last-mfe_price)/mfe_price
    return delta >= (MFE_DBPS.get(tf, 30)/10000.0)

def _trailing_hit(entry: float, last: float, side: str, mfe_price: float) -> bool:
    # tiered trailing from mfe
    if mfe_price <= 0 or entry<=0: return False
    profit = _pnl_pct(entry, mfe_price, side)
    back   = (mfe_price-last)/mfe_price if side=="long" else (last-mfe_price)/mfe_price
    for th, ret in (T3, T2, T1):
        if profit >= th and back >= ret:
            return True
    return False

Action = Literal["hold","reduce","close"]

def evaluate_position(*, tf: str, side: str, entry: float, last: float,
                      age_h: float, mfe_price: float, mfe_ts: float) -> Tuple[Action, str]:
    tf = (tf or "1h").lower()
    side = (side or "long").lower()
    min_hold = MIN_HOLD.get(tf, 0.5)

    # 1) Trailing stop
    if _trailing_hit(entry,last,side,mfe_price):
        return "close", f"trailing_stop_{tf}"

    # 2) ROI/h
    if age_h >= min_hold:
        roi_h = _roi_per_hour(entry,last,side,age_h)
        if roi_h < ROI_TH.get(tf, 0.01):
            return "close", f"roi_h_low_{tf}"

    # 3) Plateau & MFE pullback
    plateau = _plateau_due(tf, mfe_ts)
    pullbk  = _mfe_pullback(entry,last,side,mfe_price,tf)
    if plateau and pullbk:
        return "close", f"plateau_pullback_{tf}"
    if plateau:
        return "reduce", f"plateau_reduce_{tf}"
    if pullbk and age_h >= min_hold:
        return "reduce", f"mfe_pullback_reduce_{tf}"

    return "hold", "keep"

def evaluate_position_adaptive(
    *, tf: str, side: str, entry: float, last: float,
    age_h: float, mfe_price: float, mfe_ts: float,
    roi_th: float, plateau_bars: int, mfe_bp: int, trail_scale: float
) -> Tuple[Action, str]:
    tf = (tf or "1h").lower()
    side = (side or "long").lower()

    if _trailing_hit(entry,last,side,mfe_price):
        return "close", f"trailing_stop_{tf}"

    if age_h >= MIN_HOLD.get(tf, 0.5):
        roi_h = _roi_per_hour(entry,last,side,age_h)
        if roi_h < float(roi_th):
            return "close", f"roi_h_low_{tf}_auto"

    bars_min = plateau_bars * _tf_to_minutes(tf)
    if mfe_ts > 0:
        idle_min = (time.time() - mfe_ts)/60.0
    else:
        idle_min = 0.0

    pullbk = False
    if mfe_price > 0:
        delta = (mfe_price-last)/mfe_price if side=="long" else (last-mfe_price)/mfe_price
        pullbk = (delta >= max(0.0, (float(mfe_bp)/10000.0)))

    if idle_min >= bars_min and pullbk:
        return "close", f"plateau_pullback_{tf}_auto"
    if idle_min >= bars_min:
        return "reduce", f"plateau_reduce_{tf}_auto"
    if pullbk and age_h >= MIN_HOLD.get(tf, 0.5):
        return "reduce", f"mfe_pullback_reduce_{tf}_auto"

    return "hold", "keep"
