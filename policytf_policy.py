# policy/tf_policy.py
import os, time, threading, json
from typing import Dict, Optional
from bitget_api import get_open_positions, get_last_price, convert_symbol
from trader import close_position

try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str): print("[TG]", msg)

def _f(k, d=None):
    v = os.getenv(k, "")
    try: return float(v) if v != "" else d
    except: return d

# === 그룹별 파라미터 (ROI/h %/h, Plateau bars, Δbp) ===
ROI_H_1H = _f("ROI_PER_HOUR_THRESHOLD_1H", None)
ROI_H_2H = _f("ROI_PER_HOUR_THRESHOLD_2H", None)
ROI_H_3H = _f("ROI_PER_HOUR_THRESHOLD_3H", None)
ROI_H_4H = _f("ROI_PER_HOUR_THRESHOLD_4H", None)
ROI_H_D  = _f("ROI_PER_HOUR_THRESHOLD_D",  None)

PB_1H = int(_f("PLATEAU_BARS_1H", 0) or 0)
PB_2H = int(_f("PLATEAU_BARS_2H", 0) or 0)
PB_3H = int(_f("PLATEAU_BARS_3H", 0) or 0)
PB_4H = int(_f("PLATEAU_BARS_4H", 0) or 0)
PB_D  = int(_f("PLATEAU_BARS_D",  0) or 0)

DBP_1H = _f("MFE_DELTA_BP_1H", None)
DBP_2H = _f("MFE_DELTA_BP_2H", None)
DBP_3H = _f("MFE_DELTA_BP_3H", None)
DBP_4H = _f("MFE_DELTA_BP_4H", None)
DBP_D  = _f("MFE_DELTA_BP_D",  None)

MONITOR_SEC        = _f("POLICY_MONITOR_SEC", 15.0) or 15.0
MIN_HOLD_H_1H      = _f("MIN_HOLD_HOURS_1H", 0.5) or 0.5
TRACE              = os.getenv("TRACE_LOG", "0") == "1"

# === (선택) 심볼→TF 강제 매핑 (ENV JSON) ===
_SYMBOL_TF = {}
def _load_symbol_tf_env():
    global _SYMBOL_TF
    raw = os.getenv("SYMBOL_TF_JSON", "")
    try:
        _SYMBOL_TF = json.loads(raw) if raw else {}
    except:
        _SYMBOL_TF = {}
_load_symbol_tf_env()
SYMBOL_TF_RELOAD_SEC = float(os.getenv("SYMBOL_TF_RELOAD_SEC", "300"))
_last_reload = 0.0
def _tf_override_for(symbol: str) -> str:
    global _last_reload
    now = time.time()
    if now - _last_reload > SYMBOL_TF_RELOAD_SEC:
        _load_symbol_tf_env(); _last_reload = now
    return (_SYMBOL_TF.get((symbol or "").upper()) or "").upper()

# === 내부 상태 (tf_hint, entry_ts, mfe) ===
_STATE: Dict[str, Dict] = {}
_STATE_LOCK = threading.RLock()

def ingest_signal(payload: Dict):
    """TradingView alert JSON에 들어온 timeframe 힌트를 기록(없으면 생략)"""
    try:
        sym  = convert_symbol((payload.get("symbol") or "").upper())
        side = (payload.get("side") or "long").lower()
        tf   = (payload.get("timeframe") or "").strip()
        if not sym or side not in ("long","short") or not tf: return
        key = f"{sym}_{side}"
        with _STATE_LOCK:
            st = _STATE.get(key, {}) or {}
            st["tf_hint"] = tf.upper()
            _STATE[key] = st
        if TRACE: send_telegram(f"🧭 TF hint set {key} -> {tf.upper()}")
    except Exception as e:
        if TRACE: send_telegram(f"tf ingest err: {e}")

def _tf_from_hint_or_fallback(key: str, hold_hours: float) -> str:
    symbol = key.split("_", 1)[0]
    ov = _tf_override_for(symbol)
    if ov in ("1H","2H","3H","4H","D"): return ov

    with _STATE_LOCK:
        st = _STATE.get(key, {}) or {}
        tf = (st.get("tf_hint") or "").upper()

    if tf in ("1H","60"): return "1H"
    if tf in ("2H","120"): return "2H"
    if tf in ("3H","180"): return "3H"
    if tf in ("4H","240"): return "4H"
    if tf in ("D","1D","DAY","1440"): return "D"

    # 보유시간 기반 추정 (경계 완화)
    if hold_hours <= 2.0:  return "1H"
    if hold_hours <= 3.0:  return "2H"
    if hold_hours <= 4.5:  return "3H"
    if hold_hours <= 12.0: return "4H"
    return "D"

def _roi_bp(side: str, entry: float, px: float) -> float:
    if entry <= 0 or px <= 0: return 0.0
    return (px - entry) / entry * 10000.0 if side == "long" else (entry - px) / entry * 10000.0

def _update_and_check(symbol: str, side: str, entry: float, last: float, entry_ts_guess: Optional[float]):
    key = f"{symbol}_{side}"; now = time.time()
    with _STATE_LOCK:
        st = _STATE.get(key, {}) or {}
        if "entry_ts" not in st or st.get("entry_ts", 0) <= 0:
            st["entry_ts"] = float(entry_ts_guess or now)
        if "mfe_bp" not in st:
            st["mfe_bp"] = 0.0; st["mfe_ts"] = now
        _STATE[key] = st

    hold_hours = max(1e-6, (now - st["entry_ts"]) / 3600.0)
    tf_group = _tf_from_hint_or_fallback(key, hold_hours)
    roi_bp = _roi_bp(side, entry, last)
    roi_pct = roi_bp / 100.0

    # MFE 갱신
    with _STATE_LOCK:
        if roi_bp > st["mfe_bp"]:
            st["mfe_bp"] = roi_bp
            st["mfe_ts"] = now
            _STATE[key] = st

    # == ROI/h 컷 ==
    def _roi_cut(thr: Optional[float]) -> bool:
        return (thr is not None) and (hold_hours >= MIN_HOLD_H_1H) and ((roi_pct / hold_hours) < thr)

    if tf_group == "1H" and _roi_cut(ROI_H_1H): return "policy_roi_1h"
    if tf_group == "2H" and _roi_cut(ROI_H_2H): return "policy_roi_2h"
    if tf_group == "3H" and _roi_cut(ROI_H_3H): return "policy_roi_3h"
    if tf_group == "4H" and _roi_cut(ROI_H_4H): return "policy_roi_4h"
    if tf_group == "D"  and _roi_cut(ROI_H_D):  return "policy_roi_d"

    # == Plateau(Δbp + 정체 바) ==
    def _plateau(bars: int, delta_bp: Optional[float], tf_h: float) -> bool:
        if not bars or delta_bp is None: return False
        pulled = (st["mfe_bp"] - roi_bp) >= float(delta_bp)
        stagn  = (now - st["mfe_ts"]) >= (bars * tf_h * 3600.0)
        return pulled and stagn

    if tf_group == "1H" and _plateau(PB_1H, DBP_1H, 1.0):  return "policy_plateau_1h"
    if tf_group == "2H" and _plateau(PB_2H, DBP_2H, 2.0):  return "policy_plateau_2h"
    if tf_group == "3H" and _plateau(PB_3H, DBP_3H, 3.0):  return "policy_plateau_3h"
    if tf_group == "4H" and _plateau(PB_4H, DBP_4H, 4.0):  return "policy_plateau_4h"
    if tf_group == "D"  and _plateau(PB_D,  DBP_D,  24.0): return "policy_plateau_d"

    return None

_RUN = {"on": False}
def _loop():
    send_telegram("🟢 Policy manager started")
    while _RUN["on"]:
        try:
            for p in (get_open_positions() or []):
                symbol = convert_symbol(p.get("symbol") or "")
                side   = (p.get("side") or "").lower()
                if not symbol or side not in ("long","short"): continue
                try:
                    entry = float(p.get("entry_price") or 0)
                    size  = float(p.get("size") or 0)
                except: continue
                if entry <= 0 or size <= 0: continue

                last = get_last_price(symbol)
                if not last or last <= 0: continue

                with _STATE_LOCK:
                    st = _STATE.get(f"{symbol}_{side}", {}) or {}
                    entry_ts = st.get("entry_ts", None) or time.time()

                reason = _update_and_check(symbol, side, entry, last, entry_ts)
                if reason:
                    send_telegram(f"🪓 {reason} → CLOSE {side.upper()} {symbol}")
                    close_position(symbol, side=side, reason=reason)
        except Exception as e:
            if TRACE: send_telegram(f"policy loop err: {e}")
        time.sleep(MONITOR_SEC)

def start_policy_manager():
    if _RUN["on"]: return
    _RUN["on"] = True
    threading.Thread(target=_loop, name="policy-manager", daemon=True).start()
