# kpi_pipeline.py — 체결 로그 CSV + KPI 자동 집계
import os, csv, time, math, threading, json
from typing import List, Dict, Any

REPORT_DIR = os.getenv("REPORT_DIR", "./reports")
TRADES_CSV = os.path.join(REPORT_DIR, "trades.csv")
KPIS_JSON  = os.path.join(REPORT_DIR, "kpis.json")
AGG_INTERVAL_SEC = int(float(os.getenv("KPI_AGG_INTERVAL_SEC", "300")))  # 5분

COLS = [
    "ts_open","ts_close","symbol","side","entry","exit","size",
    "pnl_usdt","roe","hold_sec","leverage"
]

def _ensure_dirs():
    os.makedirs(REPORT_DIR, exist_ok=True)
    if not os.path.exists(TRADES_CSV):
        with open(TRADES_CSV, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(COLS)

def log_close_trade(**row):
    """포지션을 완전히 닫을 때 한 줄 기록한다."""
    _ensure_dirs()
    ts_open = float(row.get("ts_open", time.time()))
    ts_close= float(row.get("ts_close", time.time()))
    entry   = float(row.get("entry", 0.0))
    exit_   = float(row.get("exit", 0.0))
    size    = float(row.get("size", 0.0))
    lev     = float(row.get("leverage", 0.0))
    side    = str(row.get("side",""))
    symbol  = str(row.get("symbol","")).upper()

    pnl_usdt = float(row.get("pnl_usdt", (exit_-entry)*(size if side=="long" else -size)))
    hold_sec = max(1.0, ts_close - ts_open)
    signed = ((exit_ - entry) / entry) if entry>0 else 0.0
    signed = signed if side=="long" else -signed
    roe = signed * (lev if lev>0 else 1.0)

    rec = [
        ts_open, ts_close, symbol, side, entry, exit_, size,
        pnl_usdt, roe, hold_sec, lev
    ]
    with open(TRADES_CSV, "a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(rec)

def _read_trades() -> List[Dict[str, Any]]:
    if not os.path.exists(TRADES_CSV):
        return []
    rows = []
    with open(TRADES_CSV, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for x in r:
            try:
                x["ts_open"] = float(x["ts_open"]); x["ts_close"] = float(x["ts_close"])
                x["entry"]   = float(x["entry"]);   x["exit"]     = float(x["exit"])
                x["size"]    = float(x["size"]);    x["pnl_usdt"] = float(x["pnl_usdt"])
                x["roe"]     = float(x["roe"]);     x["hold_sec"] = float(x["hold_sec"])
                x["leverage"]= float(x["leverage"])
                rows.append(x)
            except Exception:
                continue
    return rows

def _max_drawdown(equity_curve: List[float]) -> float:
    peak = -1e18; maxdd = 0.0
    for v in equity_curve:
        peak = max(peak, v)
        dd = (v - peak)
        maxdd = min(maxdd, dd)
    return maxdd  # 음수

def _aggregate_kpis(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    n = len(trades)
    if n == 0:
        return {"win_rate":0.0,"avg_r":0.0,"profit_factor":0.0,"roi_per_hour":0.0,"max_dd":0.0,"n_trades":0}

    wins = [t for t in trades if t["pnl_usdt"]>0]
    losses = [t for t in trades if t["pnl_usdt"]<=0]
    win_rate = len(wins)/n
    sum_gain = sum(t["pnl_usdt"] for t in wins)
    sum_loss = abs(sum(t["pnl_usdt"] for t in losses))
    profit_factor = (sum_gain / sum_loss) if sum_loss>0 else float("inf")

    # R 추정: entry*size를 리스크 기준치로 단순 환산
    r_list = []
    for t in trades:
        denom = t["entry"]*max(t["size"], 1e-9)
        r_list.append(t["pnl_usdt"]/denom if denom>0 else 0.0)
    avg_r = sum(r_list)/n

    # 시간당 ROI: 각 트레이드의 수익률/시간 평균
    roi_per_h_list = []
    for t in trades:
        denom = t["entry"]*max(t["size"], 1e-9)
        ret = (t["pnl_usdt"]/denom) if denom>0 else 0.0
        hours = max(1e-6, t["hold_sec"]/3600.0)
        roi_per_h_list.append(ret/hours)
    roi_per_hour = sum(roi_per_h_list)/n

    # MaxDD: 누적 PnL 기준
    eq = []
    s=0.0
    for t in trades:
        s+=t["pnl_usdt"]; eq.append(s)
    maxdd = _max_drawdown(eq)  # 음수

    # Streak
    streak = 0; streak_win = 0; streak_loss = 0
    for t in trades:
        if t["pnl_usdt"]>0:
            streak = streak+1 if streak>=0 else 1
        else:
            streak = streak-1 if streak<=0 else -1
        streak_win = max(streak_win, streak)
        streak_loss = min(streak_loss, streak)

    avg_hold = sum(t["hold_sec"] for t in trades)/n

    return {
        "win_rate": round(win_rate,4),
        "avg_r": round(avg_r,4),
        "profit_factor": round(profit_factor,4) if profit_factor!=float("inf") else 9999.0,
        "roi_per_hour": round(roi_per_hour,6),
        "max_dd": round(maxdd,2),
        "n_trades": n,
        "streak_win": int(streak_win),
        "streak_loss": int(streak_loss),
        "avg_hold_sec": int(avg_hold),
        "updated_ts": int(time.time())
    }

def aggregate_and_save():
    _ensure_dirs()
    trades = _read_trades()
    kpis = _aggregate_kpis(trades)
    tmp = KPIS_JSON + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(kpis, f, ensure_ascii=False, indent=2)
    os.replace(tmp, KPIS_JSON)
    return kpis

def start_kpi_pipeline():
    _ensure_dirs()
    def _loop():
        while True:
            try:
                aggregate_and_save()
            except Exception as e:
                print("kpi agg err:", e)
            time.sleep(AGG_INTERVAL_SEC)
    threading.Thread(target=_loop, name="kpi-pipeline", daemon=True).start()

def list_trades(limit: int = 200) -> List[Dict[str, Any]]:
    rows = _read_trades()
    return rows[-limit:]
