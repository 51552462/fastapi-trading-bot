#!/usr/bin/env python3
# tools/summarize_logs.py
import os, sys, json, csv, glob, argparse, math
from datetime import datetime
from collections import defaultdict, Counter

def _parse_args():
    p = argparse.ArgumentParser(description="Summarize trade logs (JSONL/CSV) into session-level reports.")
    p.add_argument("--logdir", default=os.environ.get("TRADE_LOG_DIR", "./logs"), help="Directory of logs (default: ./logs)")
    p.add_argument("--out", default="./reports", help="Output directory for summaries")
    p.add_argument("--days", type=int, default=None, help="Only include last N days (by filename date) if provided")
    return p.parse_args()

def _iter_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line: continue
            try:
                yield json.loads(line)
            except:
                continue

def _iter_csv(path):
    with open(path, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            # best-effort convert ts
            if "ts" in row:
                try: row["ts"] = float(row["ts"])
                except: pass
            yield row

def _files_in(dirpath):
    # target files (jsonl preferred; csv also supported)
    pat_jsonl = os.path.join(dirpath, "*.jsonl")
    pat_csv   = os.path.join(dirpath, "*.csv")
    files = sorted(glob.glob(pat_jsonl) + glob.glob(pat_csv))
    return files

def _pick_recent(files, days):
    if not days: return files
    # Expects filename like trades_YYYY-MM-DD.jsonl ; tolerate others
    def _ts_from_name(name):
        base = os.path.basename(name)
        for seg in base.replace(".", "_").split("_"):
            try:
                if len(seg)==10 and seg[4]=="-" and seg[7]=="-":
                    return datetime.strptime(seg, "%Y-%m-%d").timestamp()
            except: pass
        return None
    files_with_ts = [(f, _ts_from_name(f)) for f in files]
    files_with_ts.sort(key=lambda x: (x[1] or 0), reverse=True)
    cutoff = None
    if files_with_ts and files_with_ts[0][1]:
        cutoff = files_with_ts[0][1] - days*86400
    if cutoff is None: 
        # if no date in names, just return all
        return files
    return [f for f,ts in files_with_ts if (ts is None or ts>=cutoff)]

def _norm(s):
    return (s or "").strip()

def _to_float(x, d=0.0):
    try:
        return float(x)
    except:
        return d

def _event_from_row(row):
    # unified event record
    ev = {
        "ts": _to_float(row.get("ts")),
        "iso": row.get("iso") or "",
        "stage": _norm(row.get("stage")),
        "event": _norm(row.get("event") or row.get("data.event")),
        "symbol": _norm(row.get("symbol") or row.get("data.symbol")),
        "side": _norm(row.get("side") or row.get("data.side") or "long").lower(),
        "amount": _to_float(row.get("amount") or row.get("data.amount")),
        "reason": _norm(row.get("reason") or row.get("data.reason")),
        "timeframe": _norm(row.get("timeframe") or row.get("data.timeframe")),
    }
    # look into nested payload if present
    if not ev["event"]:
        # some logger rows are full payload; attempt keys
        for k in ("payload", "data"):
            if isinstance(row.get(k), dict) and row[k].get("event"):
                ev["event"] = _norm(row[k]["event"])
                ev["symbol"] = _norm(row[k].get("symbol", ev["symbol"]))
                ev["side"] = _norm(row[k].get("side", ev["side"])).lower()
                ev["amount"] = _to_float(row[k].get("amount", ev["amount"]))
                ev["reason"] = _norm(row[k].get("reason", ev["reason"]))
                ev["timeframe"] = _norm(row[k].get("timeframe", ev["timeframe"]))
                break
    return ev

def _load_events(files):
    events = []
    for f in files:
        if f.endswith(".jsonl"):
            for row in _iter_jsonl(f):
                events.append(_event_from_row(row))
        elif f.endswith(".csv"):
            for row in _iter_csv(f):
                events.append(_event_from_row(row))
    # keep only meaningful ones
    events = [e for e in events if e.get("symbol")]
    # order by time
    events.sort(key=lambda x: (x.get("ts") or 0))
    return events

def _session_key(symbol, side):
    return f"{symbol.upper()}::{side.lower()}"

def _summarize(events):
    """
    Build sessions:
      - session starts at 'entry'
      - TP/reduce accumulate
      - ends at 'close' (reason recorded)
    If multiple overlapping entries for same symbol/side, sessions stack (index).
    """
    active = defaultdict(list)  # key -> list of open sessions (stack)
    sessions = []               # closed sessions results

    for ev in events:
        sym = ev["symbol"].upper()
        side = ev["side"] or "long"
        key = _session_key(sym, side)
        et = ev["event"]

        if et == "entry":
            sess = {
                "symbol": sym,
                "side": side,
                "start_ts": ev["ts"],
                "start_iso": ev["iso"],
                "timeframe": ev["timeframe"],
                "amount_usdt": ev["amount"] or 0.0,
                "tp_events": 0,
                "tp_amount_sum": 0.0,
                "reduce_events": 0,
                "close_reason": "",
                "end_ts": None,
                "end_iso": "",
                "hold_hours": None
            }
            active[key].append(sess)

        elif et in ("take_profit", "reduce"):
            stack = active.get(key) or []
            if stack:
                sess = stack[-1]  # last opened
                if et == "take_profit":
                    sess["tp_events"] += 1
                    # amount may be in extra payload; not guaranteed
                    sess["tp_amount_sum"] += float(ev.get("amount") or 0.0)
                else:
                    sess["reduce_events"] += 1

        elif et == "close":
            stack = active.get(key) or []
            if stack:
                sess = stack.pop()  # close most recent
                sess["close_reason"] = ev["reason"] or ""
                sess["end_ts"] = ev["ts"]
                sess["end_iso"] = ev["iso"]
                if sess["start_ts"] and sess["end_ts"]:
                    sess["hold_hours"] = round((sess["end_ts"] - sess["start_ts"]) / 3600.0, 4)
                sessions.append(sess)

        # else: ignore ingress/debug/guard etc. (optional extension later)

    return sessions

def _write_csv(path, rows, fieldnames):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})

def _median(a):
    a = [x for x in a if x is not None]
    if not a: return None
    a.sort()
    n = len(a)
    if n % 2: return a[n//2]
    return (a[n//2 - 1] + a[n//2]) / 2.0

def _summaries(sessions):
    kpis = {}
    kpis["total_sessions"] = len(sessions)
    kpis["avg_hold_hours"] = round(sum([s.get("hold_hours") or 0 for s in sessions]) / len(sessions), 4) if sessions else 0
    kpis["median_hold_hours"] = _median([s.get("hold_hours") for s in sessions])
    reason_counter = Counter([s.get("close_reason") or "" for s in sessions])
    kpis["close_reason_dist"] = dict(reason_counter)

    # by timeframe
    tf_map = defaultdict(list)
    for s in sessions:
        tf = (s.get("timeframe") or "").upper()
        tf_map[tf].append(s)

    kpis["by_timeframe"] = {}
    for tf, arr in tf_map.items():
        kpis["by_timeframe"][tf or "UNKNOWN"] = {
            "sessions": len(arr),
            "avg_hold_hours": round(sum([x.get("hold_hours") or 0 for x in arr]) / len(arr), 4) if arr else 0,
            "median_hold_hours": _median([x.get("hold_hours") for x in arr]),
            "close_reason_dist": dict(Counter([x.get("close_reason") or "" for x in arr]))
        }

    # by symbol
    sym_map = defaultdict(list)
    for s in sessions:
        sym_map[s["symbol"]].append(s)

    sym_rows = []
    for sym, arr in sym_map.items():
        sym_rows.append({
            "symbol": sym,
            "sessions": len(arr),
            "avg_hold_hours": round(sum([x.get("hold_hours") or 0 for x in arr]) / len(arr), 4) if arr else 0,
            "median_hold_hours": _median([x.get("hold_hours") for x in arr]),
            "tp_events_avg": round(sum([x.get("tp_events") for x in arr]) / len(arr), 3),
            "plateau_cut_rate": round(sum([1 for x in arr if (x.get('close_reason') or '').startswith('policy_plateau')]) / len(arr), 4),
            "roi_cut_rate": round(sum([1 for x in arr if (x.get('close_reason') or '').startswith('policy_roi')]) / len(arr), 4),
            "manual_close_rate": round(sum([1 for x in arr if (x.get('close_reason') or '') in ('manual','stoploss','emaexit','failcut')]) / len(arr), 4),
        })
    return kpis, sym_rows

def main():
    args = _parse_args()
    logdir = args.logdir
    outdir = args.out

    files = _files_in(logdir)
    files = _pick_recent(files, args.days)
    if not files:
        print(f"[summarize] no log files in {logdir}")
        sys.exit(0)

    print(f"[summarize] reading {len(files)} files from {logdir}")
    events = _load_events(files)
    if not events:
        print("[summarize] no events parsed")
        sys.exit(0)

    sessions = _summarize(events)
    os.makedirs(outdir, exist_ok=True)

    # 1) 세션 레벨 CSV
    sess_fields = ["symbol","side","timeframe","start_iso","end_iso","hold_hours","amount_usdt","tp_events","tp_amount_sum","reduce_events","close_reason"]
    _write_csv(os.path.join(outdir, "sessions.csv"), sessions, sess_fields)

    # 2) KPI JSON + 심볼별 CSV
    kpis, sym_rows = _summaries(sessions)
    with open(os.path.join(outdir, "kpis.json"), "w", encoding="utf-8") as f:
        json.dump(kpis, f, ensure_ascii=False, indent=2)

    sym_fields = ["symbol","sessions","avg_hold_hours","median_hold_hours","tp_events_avg","plateau_cut_rate","roi_cut_rate","manual_close_rate"]
    _write_csv(os.path.join(outdir, "symbol_stats.csv"), sym_rows, sym_fields)

    print(f"[summarize] done.\n  -> {os.path.join(outdir,'sessions.csv')}\n  -> {os.path.join(outdir,'symbol_stats.csv')}\n  -> {os.path.join(outdir,'kpis.json')}")

if __name__ == "__main__":
    main()
