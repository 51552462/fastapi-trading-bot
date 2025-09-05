# filters/adaptive_thresholds.py
import os, time, math
from statistics import mean, pstdev
from typing import Dict

def _f(name, default):
    try: return float(os.getenv(name, str(default)))
    except: return float(default)

AUTO_ENABLE   = os.getenv("AUTO_FILTERS_ENABLE", "1") == "1"
TREND_SENS    = _f("AUTO_TREND_SENSITIVITY", 1.0)
CONG_SENS     = _f("AUTO_CONGESTION_SENSITIVITY", 1.0)

def _backup(env_name: str, default: float) -> float:
    try: return float(os.getenv(env_name, str(default)))
    except: return float(default)

def _backup_roi_tf(tf: str) -> float:
    m = {"1h":"ROI_PER_HOUR_THRESHOLD_1H","2h":"ROI_PER_HOUR_THRESHOLD_2H",
         "3h":"ROI_PER_HOUR_THRESHOLD_3H","4h":"ROI_PER_HOUR_THRESHOLD_4H","d":"ROI_PER_HOUR_THRESHOLD_D"}
    return _backup(m.get(tf,"ROI_PER_HOUR_THRESHOLD_1H"), 0.02)

def _backup_plateau_bars(tf: str) -> int:
    m = {"1h":"PLATEAU_BARS_1H","2h":"PLATEAU_BARS_2H","3h":"PLATEAU_BARS_3H","4h":"PLATEAU_BARS_4H","d":"PLATEAU_BARS_D"}
    try: return int(os.getenv(m.get(tf,"PLATEAU_BARS_1H"), "24"))
    except: return 24

def _backup_mfe_bp(tf: str) -> int:
    m = {"1h":"MFE_DELTA_BP_1H","2h":"MFE_DELTA_BP_2H","3h":"MFE_DELTA_BP_3H","4h":"MFE_DELTA_BP_4H","d":"MFE_DELTA_BP_D"}
    try: return int(os.getenv(m.get(tf,"MFE_DELTA_BP_1H"), "30"))
    except: return 30

def _pnl_pct(entry: float, last: float, side: str) -> float:
    if entry <= 0: return 0.0
    return (last-entry)/entry if side=="long" else (entry-last)/entry

def _roi_h(entry: float, last: float, side: str, age_h: float) -> float:
    if age_h <= 0: return 0.0
    return _pnl_pct(entry,last,side)/age_h

def _trend_score(roi_h: float, mfe_price: float, entry: float, side: str, age_h: float) -> float:
    if entry <= 0 or mfe_price <= 0: 
        mfe_gain = 0.0
    else:
        mfe_gain = _pnl_pct(entry, mfe_price, side)
    age_factor = 1.0 / (1.0 + math.log1p(max(0.0, age_h)))
    return max(-1.0, min(1.0, 1.5*roi_h + 0.8*mfe_gain)) * age_factor

def compute(open_positions: list, meta_map: Dict[str, dict], last_price_fn) -> Dict[str, Dict]:
    out: Dict[str, Dict] = {}
    if not AUTO_ENABLE:
        for p in open_positions or []:
            sym = p.get("symbol"); side=(p.get("side") or "").lower()
            if not sym or side not in ("long","short"): continue
            key=f"{sym}_{side}"
            tf = (meta_map.get(key,{}).get("tf") or "1h").lower()
            out[key] = {
                "roi_th": _backup_roi_tf(tf),
                "plateau_bars": _backup_plateau_bars(tf),
                "mfe_bp": _backup_mfe_bp(tf),
                "trail_scale": 1.0,
            }
        return out

    rois=[]; trend_scores=[]; rows=[]
    now=time.time()
    for p in open_positions or []:
        sym=p.get("symbol"); side=(p.get("side") or "").lower()
        entry=float(p.get("entry_price") or 0); size=float(p.get("size") or 0)
        if not sym or side not in ("long","short") or entry<=0 or size<=0: continue
        key=f"{sym}_{side}"
        last=last_price_fn(sym)
        if not last: continue
        meta=meta_map.get(key, {}) or {}
        ets=float(meta.get("entry_ts") or now)
        age_h=max(0.0,(now-ets)/3600.0)
        tf=(meta.get("tf") or "1h").lower()
        mfe_price=float(meta.get("mfe_price") or entry)
        roi=_roi_h(entry,last,side,age_h)
        ts=_trend_score(roi,mfe_price,entry,side,age_h)
        rois.append(roi); trend_scores.append(ts)
        rows.append((key, tf, roi, ts, age_h))

    if not rows:
        return out

    roi_mu = mean(rois); roi_sd = pstdev(rois) if len(rois)>1 else 0.0
    ts_mu  = mean(trend_scores); ts_sd  = pstdev(trend_scores) if len(trend_scores)>1 else 0.0

    congestion = max(1, len(rows))
    cong_scale = 1.0 * CONG_SENS * (1.0 + 0.02*max(0, congestion-20))

    for key, tf, roi, ts, age_h in rows:
        alpha = (1.0/cong_scale) * (0.7 + 0.6*TREND_SENS * max(0.0, (ts - ts_mu)/(ts_sd+1e-9)))
        roi_th = max(0.0, roi_mu - alpha*(roi_sd or 0.0))
        roi_th = max(roi_th, _backup_roi_tf(tf) * 0.6)

        base_bars = _backup_plateau_bars(tf)
        plate_bars = int(base_bars * (1.0 + TREND_SENS*0.5*max(0.0, ts)) / cong_scale)
        plate_bars = max( max(4, int(base_bars*0.4)), plate_bars )

        base_bp = _backup_mfe_bp(tf)
        mfe_bp  = int( base_bp * (1.0 + 0.8*TREND_SENS*max(0.0, ts)) / cong_scale )
        mfe_bp  = max( max(12, int(base_bp*0.5)), mfe_bp )

        trail_scale = max(0.5, min(1.8, 1.0 + TREND_SENS*0.8*ts))

        out[key] = {
            "roi_th": roi_th,
            "plateau_bars": plate_bars,
            "mfe_bp": mfe_bp,
            "trail_scale": trail_scale,
        }
    return out
