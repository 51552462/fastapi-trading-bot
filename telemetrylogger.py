# telemetry/logger.py
import os, json, time, threading, gzip
from pathlib import Path

LOG_DIR = Path(os.getenv("TRADE_LOG_DIR", "./logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"ingress_{time.strftime('%Y%m%d')}.jsonl"
_LOCK = threading.RLock()

def _rotate_if_needed():
    global LOG_FILE
    today = time.strftime("%Y%m%d")
    if LOG_FILE.name.find(today) == -1:
        LOG_FILE = LOG_DIR / f"ingress_{today}.jsonl"

def log_event(payload: dict, stage: str = "ingress"):
    try:
        _rotate_if_needed()
        rec = {"ts": time.time(), "stage": stage, "data": payload}
        line = json.dumps(rec, ensure_ascii=False)
        with _LOCK:
            with LOG_FILE.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
    except Exception as e:
        print("[telemetry] log_event error:", e)

def tail(n: int = 200):
    try:
        with _LOCK, LOG_FILE.open("r", encoding="utf-8") as f:
            return list(map(str.strip, f.readlines()[-n:]))
    except Exception:
        return []

def compress_day(day: str = None):
    """day='YYYYMMDD' 지정 시 해당 일자 jsonl -> jsonl.gz 생성"""
    if day is None:
        day = time.strftime("%Y%m%d", time.localtime(time.time()-86400))
    src = LOG_DIR / f"ingress_{day}.jsonl"
    dst = LOG_DIR / f"ingress_{day}.jsonl.gz"
    if not src.exists(): return False
    with _LOCK, src.open("rb") as f_in, gzip.open(dst, "wb") as f_out:
        f_out.writelines(f_in)
    return True
