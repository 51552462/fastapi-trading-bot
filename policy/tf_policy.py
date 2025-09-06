# policy/tf_policy.py
# - TradingView 신호의 timeframe 힌트 수집(ingest_signal)
# - 포지션 모니터링: ROI/h 컷, Plateau 컷
# - 1/2/3/4H 시간봉별 파라미터(ENV 우선) + 엔트리 그레이스 존 존중
# - 과도 조기 종료 방지: 연속 확인(정책확인 카운터) + 최소 유지시간
# - 기존 main.py / trader.py 흐름과 100% 호환 (추가만)

import os, time, threading, json
from typing import Dict, Optional
from bitget_api import get_open_positions, get_last_price, convert_symbol
from trader import close_position

try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str): print("[TG]", msg)

# ─────────────────────────────────────────────────────────────
# ENV 유틸
# ─────────────────────────────────────────────────────────────
def _f(k, d=None):
    v = os.getenv(k, "")
    try: return float(v) if v != "" else d
    except: return d

def _i(k, d=None):
    v = os.getenv(k, "")
    try: return int(v) if v != "" else d
    except: return d

def _s(k, d=""):
    v = os.getenv(k, "")
    return v if v != "" else d

TRACE = os.getenv("TRACE_LOG", "0") == "1"

# ─────────────────────────────────────────────────────────────
# 정책 on/off 스위치(안전)
# ─────────────────────────────────────────────────────────────
POLICY_ENABLE = os.getenv("POLICY_ENABLE", "1") == "1"
POLICY_ROI_ENABLE       = os.getenv("POLICY_ROI_ENABLE", "1") == "1"
POLICY_PLATEAU_ENABLE   = os.getenv("POLICY_PLATEAU_ENABLE", "1") == "1"

# TF별 세부 on/off (없으면 상위 스위치 따름)
def _tf_on(base: str, tf: str) -> bool:
    v = os.getenv(f"{base}_{tf}", "")
    if v == "": return True
    return v == "1"

# 모니터 주기
MONITOR_SEC = _f("POLICY_MONITOR_SEC", 15.0) or 15.0

# ─────────────────────────────────────────────────────────────
# 1) ROI/h 컷 파라미터 (시간봉별)
#    - ROI_PER_HOUR_THRESHOLD_{1H,2H,3H,4H,D} (%) : 시간당 최소 수익률 임계
#    - MIN_HOLD_HOURS_{1H,2H,3H,4H,D}             : ROI/h 컷 적용을 시작하는 보유 최소 시간
# ─────────────────────────────────────────────────────────────
ROI_H_1H = _f("ROI_PER_HOUR_THRESHOLD_1H", None)
ROI_H_2H = _f("ROI_PER_HOUR_THRESHOLD_2H", None)
ROI_H_3H = _f("ROI_PER_HOUR_THRESHOLD_3H", None)
ROI_H_4H = _f("ROI_PER_HOUR_THRESHOLD_4H", None)
ROI_H_D  = _f("ROI_PER_HOUR_THRESHOLD_D",  None)

MIN_H_1H = _f("MIN_HOLD_HOURS_1H", 0.5) or 0.5
MIN_H_2H = _f("MIN_HOLD_HOURS_2H", 0.5) or 0.5
MIN_H_3H = _f("MIN_HOLD_HOURS_3H", 0.5) or 0.5
MIN_H_4H = _f("MIN_HOLD_HOURS_4H", 0.5) or 0.5
MIN_H_D  = _f("MIN_HOLD_HOURS_D",  1.0) or 1.0

# ─────────────────────────────────────────────────────────────
# 2) Plateau 컷 파라미터 (시간봉별)
#    - PLATEAU_BARS_{TF}     : 정체 판단 바 수
#    - MFE_DELTA_BP_{TF}     : MFE 대비 되돌림 bp (20=0.20%)
# ─────────────────────────────────────────────────────────────
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

# ─────────────────────────────────────────────────────────────
# 3) 엔트리 보호(그레이스) — trader와 동일 키 사용(있으면 존중)
#    ENTRY_GRACE_SEC_{TF}, FIRST_BAR_IGNORE_SEC_{TF}
#    * 해당 시간 이전엔 정책 컷 자체를 시도하지 않음.
# ─────────────────────────────────────────────────────────────
def _tf_env_f(base: str, tf: str, default: float) -> float:
    key_tf = f"{base}_{tf}"
    v = os.getenv(key_tf, "")
    if v != "":
        try: return float(v)
        except: pass
    v2 = os.getenv(base, "")
    if v2 != "":
        try: return float(v2)
        except: pass
    return default

def _grace_secs(tf: str) -> float:
    return _tf_env_f("ENTRY_GRACE_SEC", tf, 0.0)

def _firstbar_secs(tf: str) -> float:
    return _tf_env_f("FIRST_BAR_IGNORE_SEC", tf, 0.0)

# ─────────────────────────────────────────────────────────────
# (선택) 심볼→TF 강제 매핑 (ENV JSON) — TradingView 수정 없이 운영 가능
#   SYMBOL_TF_JSON='{"BTCUSDT":"1H","ETHUSDT":"4H"}'
# ─────────────────────────────────────────────────────────────
_SYMBOL_TF: Dict[str, str] = {}
def _load_symbol_tf_env():
    global _SYMBOL_TF
    raw = os.getenv("SYMBOL_TF_JSON", "")
    try:
        _SYMBOL_TF = json.loads(raw) if raw else {}
    except:
        _SYMBOL_TF = {}
_load_symbol_tf_env()
SYMBOL_TF_RELOAD_SEC = float(os.getenv("SYMBOL_TF_RELOAD_SEC", "300"))
_last_tf_reload = 0.0

def _tf_override_for(symbol: str) -> str:
    global _last_tf_reload
    now = time.time()
    if now - _last_tf_reload > SYMBOL_TF_RELOAD_SEC:
        _load_symbol_tf_env(); _last_tf_reload = now
    return (_SYMBOL_TF.get((symbol or "").upper()) or "").upper()

# ─────────────────────────────────────────────────────────────
# 내부 상태: tf_hint, entry_ts, mfe, 정책 확인 카운터
# ─────────────────────────────────────────────────────────────
_STATE: Dict[str, Dict] = {}
_STATE_LOCK = threading.RLock()

# 정책 확인 카운터: "조건이 연속 N회" 유지될 때만 컷
_POLICY_HIT_CNT: Dict[str, int] = {}
_POLICY_CNT_LOCK = threading.RLock()
POLICY_CONFIRM_N = _i("POLICY_CONFIRM_N", 2) or 2  # 기본 2회
POLICY_MIN_HOLD_SEC = _f("POLICY_MIN_HOLD_SEC", 20.0) or 20.0  # 조건 유지 최소 누적(초)

# ─────────────────────────────────────────────────────────────
# TF 힌트 수집 (TradingView alert JSON에서 넘어오는 timeframe)
# ─────────────────────────────────────────────────────────────
def ingest_signal(payload: Dict):
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

# TF 정규화
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

    # 보유시간 기반 추정(완만)
    if hold_hours <= 2.0:  return "1H"
    if hold_hours <= 3.0:  return "2H"
    if hold_hours <= 4.5:  return "3H"
    if hold_hours <= 12.0: return "4H"
    return "D"

# ROI → bp(=1/100bp=0.01%)로 변환
def _roi_bp(side: str, entry: float, px: float) -> float:
    if entry <= 0 or px <= 0: return 0.0
    return (px - entry) / entry * 10000.0 if side == "long" else (entry - px) / entry * 10000.0

# 정책 카운터 헬퍼
def _policy_cnt_bump(key: str, hit: bool) -> int:
    with _POLICY_CNT_LOCK:
        if not hit:
            _POLICY_HIT_CNT[key] = 0
            return 0
        _POLICY_HIT_CNT[key] = _POLICY_HIT_CNT.get(key, 0) + 1
        return _POLICY_HIT_CNT[key]

# ─────────────────────────────────────────────────────────────
# 메인 체크
# ─────────────────────────────────────────────────────────────
def _update_and_check(symbol: str, side: str, entry: float, last: float, entry_ts_guess: Optional[float]):
    key = f"{symbol}_{side}"; now = time.time()
    with _STATE_LOCK:
        st = _STATE.get(key, {}) or {}
        if "entry_ts" not in st or st.get("entry_ts", 0) <= 0:
            st["entry_ts"] = float(entry_ts_guess or now)
        if "mfe_bp" not in st:
            st["mfe_bp"] = 0.0
            st["mfe_ts"] = now
        # 정책 누적 hold (초) — 조건 hit 동안 증가, 아니면 감소(반감)
        if "hit_hold_sec" not in st:
            st["hit_hold_sec"] = 0.0
        _STATE[key] = st

    hold_hours = max(1e-6, (now - st["entry_ts"]) / 3600.0)
    tf_group = _tf_from_hint_or_fallback(key, hold_hours)  # '1H','2H','3H','4H','D'
    tf_env = tf_group  # ENV suffix

    roi_bp = _roi_bp(side, entry, last)
    roi_pct = roi_bp / 100.0

    # MFE 갱신
    with _STATE_LOCK:
        if roi_bp > st["mfe_bp"]:
            st["mfe_bp"] = roi_bp
            st["mfe_ts"] = now
            _STATE[key] = st

    # 0) 엔트리 보호: ENTRY_GRACE_SEC/ FIRST_BAR_IGNORE_SEC 존중 (있으면 정책컷 자체 skip)
    grace_sec = _tf_env_f("ENTRY_GRACE_SEC", tf_env, 0.0)
    firstbar_sec = _tf_env_f("FIRST_BAR_IGNORE_SEC", tf_env, 0.0)
    if (now - st["entry_ts"]) < max(grace_sec, firstbar_sec):
        _policy_cnt_bump(key, False)
        with _STATE_LOCK:
            st["hit_hold_sec"] = 0.0
            _STATE[key] = st
        return None

    # 1) ROI/h 컷 (TF on/off & 최소 보유시간 충족 시)
    def _roi_cut_enabled(tf: str) -> bool:
        if not POLICY_ROI_ENABLE: return False
        return _tf_on("POLICY_ROI_ENABLE_TF", tf)

    def _roi_thr_and_min_hold(tf: str):
        if tf == "1H": return ROI_H_1H, MIN_H_1H
        if tf == "2H": return ROI_H_2H, MIN_H_2H
        if tf == "3H": return ROI_H_3H, MIN_H_3H
        if tf == "4H": return ROI_H_4H, MIN_H_4H
        return ROI_H_D, MIN_H_D

    reason = None
    if _roi_cut_enabled(tf_env):
        thr, min_hold = _roi_thr_and_min_hold(tf_env)
        if (thr is not None) and (hold_hours >= float(min_hold or 0.0)):
            # 시간당 수익률(%) = 현재 ROI% / 보유시간(h)
            roi_per_h = (roi_pct / hold_hours)
            roi_hit = (roi_per_h < thr)
            cnt = _policy_cnt_bump(key, roi_hit)
            # 누적 hit 시간 업데이트
            with _STATE_LOCK:
                if roi_hit:
                    st["hit_hold_sec"] = min(st["hit_hold_sec"] + MONITOR_SEC, POLICY_MIN_HOLD_SEC * 3)
                else:
                    st["hit_hold_sec"] = max(0.0, st["hit_hold_sec"] - MONITOR_SEC * 0.5)
                _STATE[key] = st
            if roi_hit and cnt >= POLICY_CONFIRM_N and st["hit_hold_sec"] >= POLICY_MIN_HOLD_SEC:
                reason = f"policy_roi_{tf_env.lower()}"

    # 2) Plateau 컷 (TF on/off)
    def _plateau_enabled(tf: str) -> bool:
        if not POLICY_PLATEAU_ENABLE: return False
        return _tf_on("POLICY_PLATEAU_ENABLE_TF", tf)

    def _plateau(bars: int, delta_bp: Optional[float], tf_hours: float) -> bool:
        if not bars or delta_bp is None: return False
        pulled = (st["mfe_bp"] - roi_bp) >= float(delta_bp)
        stagn  = (now - st["mfe_ts"]) >= (bars * tf_hours * 3600.0)
        return pulled and stagn

    if reason is None and _plateau_enabled(tf_env):
        if tf_env == "1H" and _plateau(PB_1H, DBP_1H, 1.0):  reason = "policy_plateau_1h"
        if tf_env == "2H" and _plateau(PB_2H, DBP_2H, 2.0):  reason = "policy_plateau_2h"
        if tf_env == "3H" and _plateau(PB_3H, DBP_3H, 3.0):  reason = "policy_plateau_3h"
        if tf_env == "4H" and _plateau(PB_4H, DBP_4H, 4.0):  reason = "policy_plateau_4h"
        if tf_env == "D"  and _plateau(PB_D,  DBP_D,  24.0): reason = "policy_plateau_d"

    # 정책 미충족: 카운터/누적시간 완화
    if reason is None:
        _policy_cnt_bump(key, False)
        with _STATE_LOCK:
            st["hit_hold_sec"] = max(0.0, st["hit_hold_sec"] - MONITOR_SEC * 0.3)
            _STATE[key] = st
        return None
    else:
        return reason

# ─────────────────────────────────────────────────────────────
# 루프
# ─────────────────────────────────────────────────────────────
_RUN = {"on": False}

def _loop():
    if not POLICY_ENABLE:
        send_telegram("🟡 Policy manager disabled (POLICY_ENABLE=0)")
        return
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
                except: 
                    continue
                if entry <= 0 or size <= 0: 
                    continue
                last = get_last_price(symbol)
                if not last or last <= 0: 
                    continue

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
