# ai/ai_orchestrator.py
import os, time, json
from urllib import request
from urllib.error import URLError
from datetime import datetime

REPORT_DIR = "./reports"
ACTIONS_LOG = os.path.join(REPORT_DIR, "ai_actions.log")

ORCH_ENABLE  = os.getenv("AI_ORCH_ENABLE","1") == "1"
INTERVAL_SEC = int(os.getenv("AI_ORCH_INTERVAL_SEC","300"))
APPLY_MODE   = os.getenv("AI_ORCH_APPLY_MODE","shadow")  # shadow|canary|live
ADMIN_TOKEN  = os.getenv("ADMIN_TOKEN","")
BASE_URL     = os.getenv("PUBLIC_BASE_URL","")

LIMITS = {
  "STOP_CONFIRM_N": (2,5),
  "STOP_PRICE_MOVE": (0.018,0.03),
  "STOP_CONFIRM_MIN_HOLD_SEC": (10,90),
  "ENTRY_GRACE_SEC": (1200,7200),
  "FIRST_BAR_IGNORE_SEC": (1800,10800),
  "MAX_OPEN_POSITIONS": (120,220),
  "RISK_BUDGET_PCT": (20,40)
}
WHITELIST = set(LIMITS.keys()) | {"LONG_BYPASS_CAP","SHORT_BYPASS_CAP","STOP_DEBOUNCE_SEC","STOP_CHECK_SEC"}

def _log(msg):
    os.makedirs(REPORT_DIR, exist_ok=True)
    with open(ACTIONS_LOG,"a",encoding="utf-8") as f:
        f.write(f"[{datetime.utcnow().isoformat()}Z] {msg}\n")
    print(msg)

def _read_json(path, default=None):
    try:
        with open(path,"r",encoding="utf-8") as f: return json.load(f)
    except Exception: return default

def _clip(k, v):
    lo, hi = LIMITS.get(k,(None,None))
    try:
        x = float(v); 
        if lo is not None: x = max(lo,x)
        if hi is not None: x = min(hi,x)
        return x if not isinstance(v,str) else str(x)
    except: return v

def _post_admin(patch: dict):
    if not BASE_URL or not ADMIN_TOKEN: return {"ok":False,"reason":"no_base_url_or_token"}
    url = f"{BASE_URL.rstrip('/')}/admin/params?token={ADMIN_TOKEN}"
    data = json.dumps(patch).encode("utf-8")
    req = request.Request(url, data=data, headers={"Content-Type":"application/json"}, method="POST")
    try:
        with request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except URLError as e:
        return {"ok": False, "reason": str(e)}

def score(kpis: dict) -> float:
    wr = (kpis.get("win_rate") or 0.0)
    roi = (kpis.get("roi_avg") or 0.0)
    im  = (kpis.get("immediate_cut_ratio") or 0.0)
    wh  = (kpis.get("whipsaw_cut_ratio") or 0.0)
    return 100*(0.6*wr + 0.4*max(0.0,roi)) - 20*(im + 0.5*wh)

def propose(k: dict) -> dict:
    patch = {}
    # 작은/긴 추세 보호 강화를 우선
    if (k.get("immediate_cut_ratio") or 0) > 0.15:
        patch.update({"STOP_CONFIRM_N": 4, "STOP_CONFIRM_MIN_HOLD_SEC": 45, "ENTRY_GRACE_SEC": 4800, "FIRST_BAR_IGNORE_SEC": 6600})
    if (k.get("blocked_by_guard") or 0) > 5:
        cur = int(os.getenv("MAX_OPEN_POSITIONS","180"))
        patch["MAX_OPEN_POSITIONS"] = min(220, cur + 20)
    # 클리핑 + 화이트리스트
    patch = {kk:_clip(kk,vv) for kk,vv in patch.items() if kk in WHITELIST}
    return patch

def loop():
    while ORCH_ENABLE:
        k = _read_json(os.path.join(REPORT_DIR,"kpis.json"), {})
        if not k:
            _log("no kpis.json; run /reports/run first"); time.sleep(INTERVAL_SEC); continue
        sc = score(k); _log(f"score={sc:.2f} wr={k.get('win_rate')} roi={k.get('roi_avg')} im={k.get('immediate_cut_ratio')} wh={k.get('whipsaw_cut_ratio')}")
        patch = propose(k)
        if not patch: _log("no patch"); time.sleep(INTERVAL_SEC); continue
        if APPLY_MODE == "shadow":
            _log(f"[shadow] propose: {patch}")
        else:
            resp = _post_admin(patch)
            _log(f"[{APPLY_MODE}] applied: {patch} -> {resp}")
        time.sleep(INTERVAL_SEC)

if __name__ == "__main__":
    loop()
