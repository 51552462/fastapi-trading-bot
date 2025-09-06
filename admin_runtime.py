# admin_runtime.py
import os, json
from typing import Dict

RUNTIME_DIR = "./runtime"
OVERRIDES_PATH = f"{RUNTIME_DIR}/overrides.json"
os.makedirs(RUNTIME_DIR, exist_ok=True)

ALLOWED_KEYS = {
    # 컷/그레이스/홀드
    "STOP_PRICE_MOVE","STOP_CHECK_SEC","STOP_COOLDOWN_SEC","STOP_CONFIRM_N","STOP_DEBOUNCE_SEC",
    "STOP_CONFIRM_MIN_HOLD_SEC","ENTRY_GRACE_SEC","FIRST_BAR_IGNORE_SEC",
    # TF별
    "STOP_PRICE_MOVE_1H","STOP_CONFIRM_N_1H","STOP_CONFIRM_MIN_HOLD_SEC_1H","ENTRY_GRACE_SEC_1H","FIRST_BAR_IGNORE_SEC_1H",
    "STOP_PRICE_MOVE_2H","STOP_CONFIRM_N_2H","STOP_CONFIRM_MIN_HOLD_SEC_2H","ENTRY_GRACE_SEC_2H","FIRST_BAR_IGNORE_SEC_2H",
    "STOP_PRICE_MOVE_3H","STOP_CONFIRM_N_3H","STOP_CONFIRM_MIN_HOLD_SEC_3H","ENTRY_GRACE_SEC_3H","FIRST_BAR_IGNORE_SEC_3H",
    "STOP_PRICE_MOVE_4H","STOP_CONFIRM_N_4H","STOP_CONFIRM_MIN_HOLD_SEC_4H","ENTRY_GRACE_SEC_4H","FIRST_BAR_IGNORE_SEC_4H",
    # 기회/총량
    "MAX_OPEN_POSITIONS","RISK_BUDGET_PCT","LONG_BYPASS_CAP","SHORT_BYPASS_CAP",
    "ENTRY_DUP_TTL_SEC","ENTRY_INFLIGHT_TTL_SEC",
    # TP
    "TP1_PCT","TP2_PCT","TP3_PCT","TP_EPSILON_RATIO"
}

def load_overrides() -> Dict[str,str]:
    try:
        if not os.path.isfile(OVERRIDES_PATH): return {}
        with open(OVERRIDES_PATH,"r",encoding="utf-8") as f:
            d = json.load(f) or {}
        return {k:str(v) for k,v in d.items()}
    except Exception:
        return {}

def save_overrides(d: Dict[str,str]) -> None:
    with open(OVERRIDES_PATH,"w",encoding="utf-8") as f:
        json.dump(d,f,ensure_ascii=False,indent=2)

def get_effective_params() -> Dict[str,str]:
    ov = load_overrides(); eff = {}
    keys = set(os.environ.keys()) | set(ov.keys())
    for k in keys:
        eff[k] = ov.get(k, os.environ.get(k,""))
    return eff

def set_params(patch: Dict[str,object]) -> Dict[str,str]:
    ov = load_overrides(); changed = {}
    for k,v in patch.items():
        if k not in ALLOWED_KEYS: continue
        sv = "" if v is None else str(v)
        ov[k] = sv; os.environ[k] = sv; changed[k] = sv
    save_overrides(ov)
    try:
        import trader
        if hasattr(trader,"apply_runtime_overrides"):
            trader.apply_runtime_overrides(changed)
    except Exception:
        pass
    return changed
