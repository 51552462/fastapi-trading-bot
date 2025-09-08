# ai_expert.py — STOP을 손절 과다 시 '좁히고', 안정되면 '천천히 원상복귀' (히스테리시스+쿨다운) + 텔레 알림
import os, time, json, threading, glob
from typing import Dict, Any, List

LOG_DIR = os.getenv("TRADE_LOG_DIR", "/var/data/trade_logs")

AI_ENABLE  = os.getenv("AI_ORCH_ENABLE", "1") == "1"
TICK_SEC   = float(os.getenv("AI_ORCH_INTERVAL_SEC", "900") or 900)   # 15분

AI_NOTIFY_TELEGRAM = os.getenv("AI_NOTIFY_TELEGRAM", "1") == "1"
try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str):
        print("[TG]", msg)

# ===== 가드레일(범위와 단일 스텝 폭) =====
MIN_STOP = float(os.getenv("AI_MIN_STOP_PRICE_MOVE", "0.03"))  # 3%
MAX_STOP = float(os.getenv("AI_MAX_STOP_PRICE_MOVE", "0.20"))  # 20%
MAX_DELTA_STOP = float(os.getenv("AI_MAX_DELTA_STOP", "0.02")) # 한 번에 최대 ±2%p

# ===== 되돌림(원상복귀) 기준 =====
# 손절율이 충분히 낮고 안정적일 때 baseline으로 서서히 복귀
STOP_TIGHTEN_RATE  = float(os.getenv("AI_STOP_TIGHTEN_RATE", "0.45"))  # 손절율 ↑ 이 값 이상이면 '좁힘'
STOP_LOOSEN_RATE   = float(os.getenv("AI_STOP_LOOSEN_RATE", "0.20"))   # 손절율 ↓ 이 값 이하가 안정적으로 유지되면 '완화'
STOP_BASELINE      = float(os.getenv("AI_STOP_BASELINE", "0.02"))      # 목표 baseline(예: 0.02 = 2%)
REVERT_STEP        = float(os.getenv("AI_REVERT_STEP", "0.005"))       # 0.5%p씩 완만히 복귀
REVERT_MIN_TRADES  = int(os.getenv("AI_REVERT_MIN_TRADES", "40"))      # 최소 거래 수 조건
REVERT_MIN_HOURS   = int(os.getenv("AI_REVERT_MIN_HOURS", "12"))       # 최소 시간 조건(최근 12시간 안정적)
REVERT_COOLDOWN_SEC= int(os.getenv("AI_REVERT_COOLDOWN_SEC", "7200"))  # 되돌림 후 2시간 대기
STATE_PATH         = os.path.join(LOG_DIR, "ai_expert_state.json")

# ===== TP 관련(이번 버전은 STOP에 집중: TP는 그대로 유지) =====
MIN_TP1, MAX_TP1 = 0.10, 0.40
MIN_TP3, MAX_TP3 = 0.10, 0.60
MAX_DELTA_TP = 0.05

# ===== 손절율 집계 설정 =====
AI_LOOKBACK_TRADES = int(os.getenv("AI_LOOKBACK_TRADES", "120"))   # 최근 N건
AI_BUCKET_SIZE     = int(os.getenv("AI_COUNT_BUCKET_SIZE", "10"))  # 1–10, 11–20 ...
AI_LOOKBACK_HOURS  = int(os.getenv("AI_LOOKBACK_HOURS", "24"))     # 최근 24h

RESULTS_PATH = os.path.join(LOG_DIR, "ai_expert_changelog.jsonl")
KPI_PATH     = os.path.join(LOG_DIR, "ai_expert_kpis.json")

def _apply_runtime(changed: Dict[str, Any]):
    try:
        import trader
        if hasattr(trader, "apply_runtime_overrides"):
            trader.apply_runtime_overrides(changed)
    except Exception as e:
        print("[ai_expert] trader override failed:", e)
    try:
        import policy.tf_policy as tfp
        if hasattr(tfp, "apply_runtime_overrides"):
            tfp.apply_runtime_overrides(changed)
    except Exception as e:
        print("[ai_expert] policy override failed:", e)

def _read_trades() -> List[Dict[str, Any]]:
    files = sorted(glob.glob(os.path.join(LOG_DIR, "*.jsonl")))
    rows: List[Dict[str, Any]] = []
    for f in files[-30:]:
        try:
            with open(f, "r", encoding="utf-8") as fh:
                for line in fh:
                    try:
                        rows.append(json.loads(line))
                    except:
                        pass
        except:
            pass
    return rows[-50000:]

# === 손절 판정: PnL<0 또는 close.reason이 손절성 사유 ===
STOP_REASONS = {"price_guard", "pnl_guard", "stoploss", "failcut", "emaexit"}
def _is_stop_close(t: Dict[str, Any]) -> bool:
    if t.get("event") != "close":
        return False
    r = (t.get("reason") or "").lower()
    if r in STOP_REASONS:
        return True
    try:
        pnl = float(t.get("pnl_pct"))
        if pnl < 0:
            return True
    except:
        pass
    return False

# === KPI (전반) ===
def _calc_kpis(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    wins = losses = 0
    pnl_sum = 0.0
    for t in trades:
        if t.get("event") == "close":
            try: r = float(t.get("pnl_pct"))
            except: r = 0.0
            pnl_sum += r
            if r > 0: wins += 1
            elif r < 0: losses += 1
    wr = wins / max(1, wins + losses)
    avg_r = pnl_sum / max(1, wins + losses)
    return {"win_rate": wr, "avg_r": avg_r, "n_trades": (wins+losses)}

# === 손절율(버킷) ===
def _stop_rate_by_count_buckets(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    closes = [t for t in trades if t.get("event")=="close"][-AI_LOOKBACK_TRADES:]
    buckets = []
    for i in range(0, len(closes), AI_BUCKET_SIZE):
        chunk = closes[i:i+AI_BUCKET_SIZE]
        if not chunk: break
        stop_n = sum(1 for x in chunk if _is_stop_close(x))
        rate = stop_n / max(1, len(chunk))
        buckets.append({"idx": (i//AI_BUCKET_SIZE)+1, "n": len(chunk), "stop_rate": rate})
    top_rate = max((b["stop_rate"] for b in buckets), default=0.0)
    min_rate = min((b["stop_rate"] for b in buckets), default=1.0)
    return {"buckets": buckets, "max_rate": top_rate, "min_rate": min_rate, "total_n": len(closes)}

# === 손절율(24h 4분할) ===
def _stop_rate_by_time_quarters(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    now = time.time()
    horizon = now - AI_LOOKBACK_HOURS * 3600
    closes = [t for t in trades if t.get("event")=="close" and float(t.get("ts", now)) >= horizon]
    qsec = (AI_LOOKBACK_HOURS * 3600) / 4.0
    quarters = []
    for q in range(4):
        start = horizon + q * qsec
        end   = start + qsec
        chunk = [x for x in closes if start <= float(x.get("ts", now)) < end]
        stop_n = sum(1 for x in chunk if _is_stop_close(x))
        rate = stop_n / max(1, len(chunk))
        quarters.append({"q": q+1, "n": len(chunk), "stop_rate": rate})
    top_rate = max((q["stop_rate"] for q in quarters), default=0.0)
    min_rate = min((q["stop_rate"] for q in quarters), default=1.0)
    return {"quarters": quarters, "max_rate": top_rate, "min_rate": min_rate, "total_n": len(closes)}

def _clip(v, lo, hi): return max(lo, min(hi, v))
def _bounded_step(cur, target, max_delta):
    delta = _clip(target - cur, -max_delta, max_delta)
    return cur + delta

def _load_current():
    import trader
    return {
        "STOP_PRICE_MOVE": float(os.getenv("STOP_PRICE_MOVE", getattr(trader, "STOP_PCT", 0.10))),
        "TP1_PCT": float(os.getenv("TP1_PCT", getattr(trader, "TP1_PCT", 0.30))),
        "TP3_PCT": float(os.getenv("TP3_PCT", getattr(trader, "TP3_PCT", 0.30))),
    }

# ── 내부 상태(히스테리시스) 저장/로드 ──
def _load_state() -> Dict[str, Any]:
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {"last_change_ts": 0.0, "stable_ok_seq": 0}

def _save_state(st: Dict[str, Any]):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(st, f)

def _decide_changes(trades: List[Dict[str, Any]], kpi: Dict[str, Any], cur: Dict[str, Any]) -> Dict[str, Any]:
    by_bucket = _stop_rate_by_count_buckets(trades)
    by_time   = _stop_rate_by_time_quarters(trades)

    signal_max = max(by_bucket["max_rate"], by_time["max_rate"])   # “최악”의 손절율
    signal_min = min(by_bucket["min_rate"], by_time["min_rate"])   # “최저” 손절율
    changed: Dict[str, Any] = {}
    stop_cur = float(cur["STOP_PRICE_MOVE"])

    state = _load_state()
    now = time.time()
    can_change = (now - state.get("last_change_ts", 0.0)) >= REVERT_COOLDOWN_SEC

    # 1) 손절 과다 → STOP '좁힘'(값↓)
    if signal_max >= STOP_TIGHTEN_RATE and can_change:
        tighten_step = 0.01 if signal_max < 0.60 else 0.015  # 45~60%: 1%p, 60%+: 1.5%p
        stop_target = _clip(stop_cur - tighten_step, MIN_STOP, MAX_STOP)
        stop_new = _bounded_step(stop_cur, stop_target, MAX_DELTA_STOP)
        if stop_new < stop_cur - 1e-9:
            changed["STOP_PRICE_MOVE"] = round(stop_new, 4)
            state["last_change_ts"] = now
            state["stable_ok_seq"] = 0  # 안정 카운트 리셋

    # 2) 안정 구간 → STOP '완화'(baseline 방향으로 아주 천천히↑)
    else:
        # 안정 조건: (a) 버킷/시간 양쪽 모두 STOP_LOOSEN_RATE 이하, (b) 충분한 표본, (c) 최소 시간 경과
        enough_trades = (by_bucket["total_n"] >= REVERT_MIN_TRADES) and (by_time["total_n"] >= REVERT_MIN_TRADES)
        stable_now = (by_bucket["max_rate"] <= STOP_LOOSEN_RATE) and (by_time["max_rate"] <= STOP_LOOSEN_RATE)
        horizon_ok = (AI_LOOKBACK_HOURS >= REVERT_MIN_HOURS)
        if stable_now and enough_trades and horizon_ok and can_change:
            # 연속 안정 회차 누적(히스테리시스)
            state["stable_ok_seq"] = int(state.get("stable_ok_seq", 0)) + 1
            # 두 번 연속 이상 안정이면 아주 소폭 완화
            if state["stable_ok_seq"] >= 2:
                # baseline을 넘지 않게, step/델타 제한도 지킴
                target = _clip(STOP_BASELINE, MIN_STOP, MAX_STOP)
                if stop_cur < target:  # 이미 target보다 작으면 유지
                    pass
                else:
                    stop_target = _clip(stop_cur + REVERT_STEP, MIN_STOP, target)
                    stop_new = _bounded_step(stop_cur, stop_target, MAX_DELTA_STOP)
                    if stop_new > stop_cur + 1e-9:
                        changed["STOP_PRICE_MOVE"] = round(stop_new, 4)
                        state["last_change_ts"] = now
                        # 과도 완화 방지: 한 번 완화 후 stable 카운트를 1로 감소시켜, 재확인 유도
                        state["stable_ok_seq"] = 1
        else:
            # 안정 조건 미충족 → 카운트 리셋
            state["stable_ok_seq"] = 0

    changed["_diagnostics"] = {
        "signal_max": signal_max,
        "signal_min": signal_min,
        "bucket": by_bucket,
        "quarters": by_time,
        "state": {"stable_ok_seq": state.get("stable_ok_seq",0), "last_change_ts": state.get("last_change_ts",0.0)}
    }
    _save_state(state)
    return changed

def _write_jsonl(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def _save_kpis(kpi: Dict[str, Any], cur: Dict[str, Any], changed: Dict[str, Any]):
    os.makedirs(os.path.dirname(KPI_PATH), exist_ok=True)
    with open(KPI_PATH, "w", encoding="utf-8") as f:
        json.dump({"kpi": kpi, "current": cur, "changed": changed, "ts": time.time()}, f, ensure_ascii=False, indent=2)

def _notify(changed: Dict[str, Any], kpi: Dict[str, Any], cur: Dict[str, Any]):
    if not AI_NOTIFY_TELEGRAM:
        return
    msg = [
        "🤖 AI 튜너 조정",
        f"- WinRate={kpi['win_rate']*100:.1f}% AvgR={kpi['avg_r']:.2f} N={kpi['n_trades']}",
    ]
    diag = changed.get("_diagnostics")
    if "STOP_PRICE_MOVE" in changed:
        prev = cur["STOP_PRICE_MOVE"]; now = changed["STOP_PRICE_MOVE"]
        direction = "↓ 더 촘촘(손절↑)" if now < prev else "↑ 완화(안정)"
        msg.append(f"• STOP(가격컷): {prev*100:.2f}% → {now*100:.2f}%  [{direction}]")
    if diag:
        try:
            msg.append(f"• 신호: worst={diag['signal_max']*100:.1f}% "
                       f"(버킷Top={diag['bucket']['max_rate']*100:.1f}%, 24hTop={diag['quarters']['max_rate']*100:.1f}%), "
                       f"state.stable_seq={diag['state']['stable_ok_seq']}")
        except Exception:
            pass
    # 진단 키 제거 후 저장을 위해 pop 하지 않고 알림만 보냄
    if len(msg) > 2:
        try: send_telegram("\n".join(msg))
        except Exception: pass

def _kpi_all(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    return _calc_kpis(trades)

def _tick():
    trades = _read_trades()
    if not trades:
        return
    kpi = _kpi_all(trades)
    cur = _load_current()
    changed = _decide_changes(trades, kpi, cur)

    if changed:
        # 저장/적용은 진단 키 제외
        apply_obj = {k:v for k,v in changed.items() if not k.startswith("_")}
        if apply_obj:
            _apply_runtime(apply_obj)
        _write_jsonl(RESULTS_PATH, {"ts": time.time(), "kpi": kpi, "changed": changed})
        _notify(apply_obj | {"_diagnostics": changed.get("_diagnostics")}, kpi, cur)
    _save_kpis(kpi, cur, changed)

def _loop():
    last = 0.0
    while True:
        try:
            now = time.time()
            if now - last >= TICK_SEC:
                last = now
                _tick()
        except Exception as e:
            print("[ai_expert] tick error:", e)
        time.sleep(1.0)

def start_ai_expert():
    if not AI_ENABLE:
        return
    threading.Thread(target=_loop, name="ai-expert", daemon=True).start()
