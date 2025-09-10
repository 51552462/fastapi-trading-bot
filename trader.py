# trader.py — 자동매매 엔진 (이어받기/워치독/리컨/용량가드/TP·SL/즉시종료/로그/BE/쿨다운)
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import time
import threading
import inspect
from typing import Dict, Any, Optional, List

from bitget_api import (
    convert_symbol, get_last_price, get_open_positions as _raw_get_positions,
    place_market_order, place_reduce_by_size, get_symbol_spec, round_down_step,
)

# 체결 로그 파이프라인(없으면 no-op)
try:
    from kpi_pipeline import log_close_trade
except Exception:
    def log_close_trade(*args, **kwargs):  # type: ignore
        pass

# 텔레그램(없으면 print 대체)
try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str):  # type: ignore
        print("[TG]", msg)

# =========================
# 환경변수
# =========================
DEFAULT_AMOUNT = float(os.getenv("DEFAULT_AMOUNT", "80"))  # 기본 진입 USDT
LEVERAGE       = float(os.getenv("LEVERAGE", "5"))

# 분할익절 기본 비율 (잔량 기준)
TP1_PCT = float(os.getenv("TP1_PCT", "0.30"))
TP2_PCT = float(os.getenv("TP2_PCT", "0.5714286"))  # 이후 남은 물량 40%에 해당하도록 4/7 기본값
TP3_PCT = float(os.getenv("TP3_PCT", "1.0"))        # 나머지 전부

# 손절: 가격 이동/ROE 기준
STOP_PCT          = float(os.getenv("STOP_PRICE_MOVE", "0.02"))  # ±2%
STOP_ROE          = float(os.getenv("STOP_ROE", "0.10"))         # -10% ROE(레버 반영)

STOP_CHECK_SEC    = float(os.getenv("STOP_CHECK_SEC", "2"))
STOP_CONFIRM_N    = int(float(os.getenv("STOP_CONFIRM_N", "1")))
STOP_DEBOUNCE_SEC = float(os.getenv("STOP_DEBOUNCE_SEC", "2"))
STOP_COOLDOWN_SEC = float(os.getenv("STOP_COOLDOWN_SEC", "3"))

# 리컨실러(원격↔로컬 동기화)
RECON_INTERVAL_SEC = float(os.getenv("RECON_INTERVAL_SEC", "2"))
RECON_DEBUG        = os.getenv("RECON_DEBUG", "0") == "1"

# 포지션 용량 가드
MAX_OPEN_POSITIONS = int(float(os.getenv("MAX_OPEN_POSITIONS", "120")))
CAP_CHECK_SEC      = float(os.getenv("CAP_CHECK_SEC", "5"))
LONG_BYPASS_CAP    = (os.getenv("LONG_BYPASS_CAP", "0") == "1")
SHORT_BYPASS_CAP   = (os.getenv("SHORT_BYPASS_CAP", "0") == "1")

# 추세 보호(정책 종료 차단)
TREND_PROTECT              = os.getenv("TREND_PROTECT", "1") == "1"
PROTECT_AFTER_TP1          = os.getenv("PROTECT_AFTER_TP1", "1") == "1"
PROTECT_AFTER_TP2          = os.getenv("PROTECT_AFTER_TP2", "1") == "1"
POLICY_CLOSE_MIN_HOLD_SEC  = float(os.getenv("POLICY_CLOSE_MIN_HOLD_SEC", "900"))  # 15분
POLICY_CLOSE_ALLOW_NEG_ROE = float(os.getenv("POLICY_CLOSE_ALLOW_NEG_ROE", "0.0"))

# 재오픈 쿨다운
REOPEN_COOLDOWN_SEC = float(os.getenv("REOPEN_COOLDOWN_SEC", "60"))

# =========================
# 내부 상태
# =========================
position_data: Dict[str, Dict[str, Any]] = {}  # key=symbol_side → {size, entry, ts_open, flags...}
_POS_LOCK = threading.RLock()

_CAP_LOCK = threading.RLock()
_CAPACITY = {
    "blocked": False, "last_count": 0,
    "short_blocked": False, "long_blocked": False,
    "short_count": 0, "long_count": 0, "ts": 0.0
}

# 재오픈 쿨다운 기록
LAST_EXIT_TS: Dict[str, float] = {}

# =========================
# 유틸
# =========================
def _safe_get_positions() -> List[Dict[str, Any]]:
    """Bitget 포지션 리스트 안전 조회 (v2/v1 어댑터 래핑)."""
    try:
        # 함수 시그니처 호환성(과거 버전 대비)
        if len(inspect.signature(_raw_get_positions).parameters) >= 1:
            return _raw_get_positions(None)  # type: ignore[arg-type]
        return _raw_get_positions()
    except TypeError:
        try:
            return _raw_get_positions(None)  # type: ignore[arg-type]
        except Exception:
            return []
    except Exception:
        return []

def _key(symbol: str, side: str) -> str:
    s = (side or "").lower()
    if s.startswith("l"): s = "long"
    if s.startswith("s"): s = "short"
    return f"{symbol}_{s}"

def _norm_side(s: str) -> str:
    s = (s or "").lower().strip()
    if s in ("buy", "long", "l"):  return "long"
    if s in ("sell", "short", "s"): return "short"
    return s

def _signed_change_pct(side: str, mark: float, entry: float) -> float:
    """해당 포지션 기준 수익률(+ long, - short 반전)."""
    raw = (mark - entry) / entry if entry > 0 else 0.0
    return raw if side == "long" else -raw

def _price_drawdown_pct(side: str, mark: float, entry: float) -> float:
    """가격 이동폭(롱: 하락폭, 숏: 상승폭)을 양수 비율로 계산."""
    chg = _signed_change_pct("short" if side == "long" else "long", mark, entry)
    return abs(chg)

def should_pnl_cut(side: str, mark: float, entry: float, lev: float | None = None) -> bool:
    """ROE 기반 컷(-STOP_ROE 이하)이면 True."""
    lev = float(lev or LEVERAGE or 1.0)
    if entry <= 0 or lev <= 0: return False
    roe = _signed_change_pct(side, mark, entry) * lev
    return roe <= -abs(STOP_ROE)

def _update_local_state_from_exchange():
    """원격 포지션을 로컬 캐시에 반영."""
    opens = _safe_get_positions()
    with _POS_LOCK:
        seen = set()
        for p in opens:
            sym  = convert_symbol(p.get("symbol") or "")
            side = _norm_side(p.get("side"))
            if not sym or side not in ("long","short"):
                continue
            k = _key(sym, side)
            seen.add(k)
            d = position_data.setdefault(k, {})
            d["size"]  = float(p.get("size") or 0.0)
            d["entry"] = float(p.get("entryPrice") or 0.0)
            d.setdefault("ts_open", d.get("ts_open", time.time()))
            if d["size"] <= 0:
                position_data.pop(k, None)
        for k in list(position_data.keys()):
            if k not in seen and position_data.get(k,{}).get("size",0) <= 0:
                position_data.pop(k, None)

# =========================
# Capacity Guard
# =========================
def _capacity_loop():
    while True:
        try:
            opens = _safe_get_positions()
            long_c  = sum(1 for p in opens if _norm_side(p.get("side"))=="long"  and float(p.get("size") or 0)>0)
            short_c = sum(1 for p in opens if _norm_side(p.get("side"))=="short" and float(p.get("size") or 0)>0)
            with _CAP_LOCK:
                _CAPACITY["last_count"]  = long_c + short_c
                _CAPACITY["long_count"]  = long_c
                _CAPACITY["short_count"] = short_c
                _CAPACITY["blocked"]     = (_CAPACITY["last_count"] >= MAX_OPEN_POSITIONS)
                _CAPACITY["long_blocked"]  = (not LONG_BYPASS_CAP)  and _CAPACITY["blocked"]
                _CAPACITY["short_blocked"] = (not SHORT_BYPASS_CAP) and _CAPACITY["blocked"]
        except Exception as e:
            print("capacity err:", e)
        time.sleep(CAP_CHECK_SEC)

def start_capacity_guard():
    threading.Thread(target=_capacity_loop, name="capacity-guard", daemon=True).start()

# =========================
# Trading Ops
# =========================
def enter_position(symbol: str, side: str = "long", usdt_amount: Optional[float] = None,
                   leverage: Optional[float] = None, timeframe: Optional[str] = None):
    """시장가 신규 진입."""
    symbol = convert_symbol(symbol)
    side   = _norm_side(side)
    amount = float(usdt_amount or DEFAULT_AMOUNT)
    k = _key(symbol, side)

    # 원격 중복 방지
    for p in _safe_get_positions():
        if convert_symbol(p.get("symbol")) == symbol and _norm_side(p.get("side")) == side and float(p.get("size") or 0) > 0:
            send_telegram(f"⚠️ OPEN SKIP: already open {side.upper()} {symbol}")
            return {"ok": False, "reason": "dup_open"}

    # 재오픈 쿨다운
    now = time.time()
    if now - LAST_EXIT_TS.get(k, 0) < REOPEN_COOLDOWN_SEC:
        left = int(REOPEN_COOLDOWN_SEC - (now - LAST_EXIT_TS.get(k, 0)))
        send_telegram(f"⏱️ OPEN SKIP: cooldown {side.upper()} {symbol} {left}s")
        return {"ok": False, "reason": "cooldown"}

    # 용량 가드
    with _CAP_LOCK:
        if _CAPACITY["blocked"]:
            if side=="long" and _CAPACITY["long_blocked"]:
                send_telegram(f"⛔ capacity block LONG {symbol} (count={_CAPACITY['last_count']})")
                return {"ok": False, "reason": "cap_block"}
            if side=="short" and _CAPACITY["short_blocked"]:
                send_telegram(f"⛔ capacity block SHORT {symbol} (count={_CAPACITY['last_count']})")
                return {"ok": False, "reason": "cap_block"}

    resp = place_market_order(symbol, amount, side, leverage or LEVERAGE)
    code = str(resp.get("code",""))
    if code != "00000":
        send_telegram(f"❌ OPEN {side.upper()} {symbol} {amount:.1f}USDT fail: {resp}")
        return {"ok": False, "resp": resp}

    with _POS_LOCK:
        d = position_data.setdefault(k, {})
        d["ts_open"] = time.time()
        d["tp1_done"] = d.get("tp1_done", False)
        d["tp2_done"] = d.get("tp2_done", False)
        d["be_armed"] = d.get("be_armed", False)

    send_telegram(f"✅ OPEN {side.upper()} {symbol} {amount:.2f}USDT @ {leverage or LEVERAGE}x")
    _update_local_state_from_exchange()
    return {"ok": True}

def reduce_by_contracts(symbol: str, contracts: float, side: str):
    """정확 수량(계약수) 줄이기."""
    symbol = convert_symbol(symbol); side = _norm_side(side)
    if contracts <= 0: return {"ok": False, "reason": "bad_contracts"}
    spec = get_symbol_spec(symbol)
    qty  = round_down_step(float(contracts), float(spec.get("sizeStep", 0.001)))
    if qty <= 0: return {"ok": False, "reason": "too_small"}
    resp = place_reduce_by_size(symbol, qty, side)
    if str(resp.get("code","")) != "00000":
        send_telegram(f"❌ REDUCE {side.upper()} {symbol} {qty} fail: {resp}")
        return {"ok": False, "resp": resp}
    send_telegram(f"✂️ REDUCE {side.upper()} {symbol} {qty}")
    _update_local_state_from_exchange()
    return {"ok": True}

def take_partial_profit(symbol: str, ratio: float, side: str = "long", reason: str = "tp"):
    """
    부분 청산:
      - ratio > 0 이면 잔량 * ratio 만큼 감축
      - reason="tp_qty:<수량>" 형태면 정확 수량으로 감축(우선)
    """
    symbol = convert_symbol(symbol); side = _norm_side(side)

    tp_qty = None
    if isinstance(reason, str) and reason.startswith("tp_qty:"):
        try:
            tp_qty = float(reason.split(":", 1)[1])
        except Exception:
            tp_qty = None

    if tp_qty is None and (ratio is None or ratio <= 0 or ratio > 1):
        return {"ok": False, "reason": "bad_ratio_or_qty"}

    held = 0.0
    for p in _safe_get_positions():
        if convert_symbol(p.get("symbol")) == symbol and _norm_side(p.get("side")) == side:
            held = float(p.get("size") or 0.0); break
    if held <= 0:
        send_telegram(f"⚠️ TP SKIP: 원격 포지션 없음 {symbol}_{side}")
        return {"ok": False, "reason": "no_position"}

    cut = float(tp_qty) if tp_qty is not None else (held * float(ratio))
    spec = get_symbol_spec(symbol)
    cut = round_down_step(cut, float(spec.get("sizeStep", 0.001)))
    if cut <= 0: return {"ok": False, "reason": "too_small"}

    resp = place_reduce_by_size(symbol, cut, side)
    if str(resp.get("code","")) != "00000":
        send_telegram(f"❌ TP fail {symbol}_{side} detail={reason}: {resp}")
        return {"ok": False, "resp": resp}

    if tp_qty is not None:
        send_telegram(f"🏁 TP(QTY) {side.upper()} {symbol} -{cut} contracts")
    else:
        send_telegram(f"🏁 TP({reason}) {side.upper()} {symbol} -{ratio*100:.0f}%")

    # 플래그/BE 무장
    with _POS_LOCK:
        d = position_data.setdefault(_key(symbol, side), {})
        d.setdefault("tp1_done", False)
        d.setdefault("tp2_done", False)
        if tp_qty is None:
            if (abs(ratio - TP1_PCT) < 1e-6 or ratio <= TP1_PCT):
                d["tp1_done"] = True
            if (abs(ratio - TP2_PCT) < 1e-6 or (d.get("tp1_done") and ratio >= TP2_PCT-1e-6)):
                d["tp2_done"] = True
            if (d.get("tp1_done") and os.getenv("BE_AFTER_TP1","0")=="1") or \
               (d.get("tp2_done") and os.getenv("BE_AFTER_TP2","1")=="1"):
                d["be_armed"] = True

    _update_local_state_from_exchange()
    return {"ok": True}

def _policy_close_blocked(symbol: str, side: str, reason: str, entry: float) -> bool:
    """추세 보호 정책: 특정 상황에서 policy close 차단."""
    if not TREND_PROTECT:
        return False
    try:
        k = _key(symbol, side)
        with _POS_LOCK:
            d = position_data.get(k, {})
        mark = float(get_last_price(symbol) or 0.0)
        roe  = _signed_change_pct(side, mark, entry) * LEVERAGE
        age  = time.time() - float(d.get("ts_open", time.time()))
        tp_ok = (PROTECT_AFTER_TP1 and d.get("tp1_done")) or (PROTECT_AFTER_TP2 and d.get("tp2_done"))
        be_armed = d.get("be_armed")
        # TP 이후 일정 시간, 플러스 ROE 범위 내에서는 policy close 차단
        if tp_ok and roe > POLICY_CLOSE_ALLOW_NEG_ROE and age < POLICY_CLOSE_MIN_HOLD_SEC:
            return True
        # 본절 무장 후 플러스 구간 유지 땐 차단
        if be_armed:
            if (side=="long" and mark > entry) or (side=="short" and mark < entry):
                return True
        return False
    except Exception:
        return False

def close_position(symbol: str, side: str = "long", reason: str = "manual"):
    """즉시 종료(전량 시장가)."""
    symbol = convert_symbol(symbol); side = _norm_side(side)

    held = entry = 0.0
    for p in _safe_get_positions():
        if convert_symbol(p.get("symbol")) == symbol and _norm_side(p.get("side")) == side:
            held  = float(p.get("size") or 0.0)
            entry = float(p.get("entryPrice") or 0.0)
            break
    if held <= 0:
        send_telegram(f"⚠️ CLOSE 스킵: 원격 포지션 없음 {symbol}_{side}")
        return {"ok": False, "reason": "no_position"}

    if reason.startswith("policy") and _policy_close_blocked(symbol, side, reason, entry):
        send_telegram(f"🛡️ POLICY CLOSE BLOCKED by trend-protect: {side.upper()} {symbol} ({reason})")
        return {"ok": False, "reason": "policy_blocked"}

    exit_px = float(get_last_price(symbol) or 0.0)
    realized = 0.0
    if entry > 0 and exit_px > 0:
        if side == "long": realized = (exit_px - entry) * held
        else:              realized = (entry - exit_px) * held

    spec = get_symbol_spec(symbol)
    qty  = round_down_step(held, float(spec.get("sizeStep", 0.001)))
    resp = place_reduce_by_size(symbol, qty, side)
    if str(resp.get("code","")) != "00000":
        send_telegram(f"❌ CLOSE fail {symbol}_{side}: {resp}")
        return {"ok": False, "resp": resp}

    # 로그/알림
    with _POS_LOCK:
        d = position_data.get(_key(symbol, side), {})
        ts_open = float(d.get("ts_open", time.time()))
    log_close_trade(
        ts_open=ts_open, ts_close=time.time(), symbol=symbol, side=side,
        entry=entry, exit=exit_px, size=qty, pnl_usdt=realized, leverage=LEVERAGE
    )
    sign = " +" if realized >= 0 else " "
    send_telegram(
        "✅ CLOSE {side} {sym} ({reason})\n"
        "• Exit: {exit_px}\n"
        "• Size: {qty}\n"
        "• Realized~{sign}{pnl:.2f} USDT".format(
            side=side.upper(), sym=symbol, reason=reason,
            exit_px=exit_px, qty=qty, sign=sign, pnl=realized
        )
    )
    with _POS_LOCK:
        position_data.pop(_key(symbol, side), None)
    LAST_EXIT_TS[_key(symbol, side)] = time.time()
    return {"ok": True}

# =========================
# Watchdog: 가격 ±2%/ROE -10% 즉시 종료 + BE
# =========================
def _watchdog_loop():
    confirm_cnt: Dict[str, int] = {}
    last_hit_ts: Dict[str, float] = {}
    cooldown_ts: Dict[str, float] = {}

    while True:
        try:
            for p in _safe_get_positions():
                symbol = convert_symbol(p.get("symbol") or "")
                side   = _norm_side(p.get("side"))
                size   = float(p.get("size") or 0.0)
                entry  = float(p.get("entryPrice") or 0.0)
                if size <= 0 or entry <= 0 or side not in ("long","short"):
                    continue

                mark = float(get_last_price(symbol) or 0.0)
                k = _key(symbol, side)

                hit_pnl   = should_pnl_cut(side, mark, entry, LEVERAGE)          # ROE -10%
                hit_price = _price_drawdown_pct(side, mark, entry) >= STOP_PCT   # ±2%

                be_fire = False
                with _POS_LOCK:
                    d = position_data.setdefault(k, {})
                    d.setdefault("entry", entry)
                    d.setdefault("size", size)
                    d.setdefault("ts_open", time.time())
                    if d.get("be_armed"):
                        if (side=="long" and mark <= entry) or (side=="short" and mark >= entry):
                            be_fire = True

                now = time.time()
                if be_fire or hit_pnl or hit_price:
                    if now < cooldown_ts.get(k, 0):
                        continue
                    confirm_cnt[k] = confirm_cnt.get(k, 0) + 1
                    last_hit_ts[k] = now
                    if confirm_cnt[k] >= max(1, STOP_CONFIRM_N):
                        reason = "breakeven" if be_fire else ("failcut" if hit_pnl else "stop")
                        close_position(symbol, side, reason=reason)
                        cooldown_ts[k] = now + STOP_COOLDOWN_SEC
                        confirm_cnt[k] = 0
                else:
                    if now - last_hit_ts.get(k, 0) > STOP_DEBOUNCE_SEC:
                        confirm_cnt[k] = 0
        except Exception as e:
            print("watchdog err:", e)
        time.sleep(STOP_CHECK_SEC)

def start_watchdogs():
    threading.Thread(target=_watchdog_loop, name="stop-watchdog", daemon=True).start()

# =========================
# Reconciler: 원격→로컬 동기화
# =========================
def _reconcile_loop():
    while True:
        try:
            if RECON_DEBUG:
                print("recon positions:", _safe_get_positions())
            _update_local_state_from_exchange()
        except Exception as e:
            print("recon err:", e)
        time.sleep(RECON_INTERVAL_SEC)

def start_reconciler():
    threading.Thread(target=_reconcile_loop, name="reconciler", daemon=True).start()

# =========================
# 런타임 오버라이드/스냅샷
# =========================
def runtime_overrides(changed: Dict[str, Any]):
    global STOP_PCT, RECON_INTERVAL_SEC, TP1_PCT, TP2_PCT, TP3_PCT
    global STOP_ROE, REOPEN_COOLDOWN_SEC
    if "STOP_PRICE_MOVE" in changed:     STOP_PCT = float(changed["STOP_PRICE_MOVE"])
    if "STOP_ROE" in changed:            STOP_ROE = float(changed["STOP_ROE"])
    if "RECON_INTERVAL_SEC" in changed:  RECON_INTERVAL_SEC = float(changed["RECON_INTERVAL_SEC"])
    if "TP1_PCT" in changed:             TP1_PCT = float(changed["TP1_PCT"])
    if "TP2_PCT" in changed:             TP2_PCT = float(changed["TP2_PCT"])
    if "TP3_PCT" in changed:             TP3_PCT = float(changed["TP3_PCT"])
    if "REOPEN_COOLDOWN_SEC" in changed: REOPEN_COOLDOWN_SEC = float(changed["REOPEN_COOLDOWN_SEC"])

def apply_runtime_overrides(changed: Dict[str, Any]):
    return runtime_overrides(changed)

def get_pending_snapshot():
    with _POS_LOCK:
        return {"positions": dict(position_data)}

# =========================
# Startup: 모든 백그라운드 시작 + 이어받기 메시지
# =========================
def start_all_backgrounds():
    """
    부팅 시 한 번 호출.
    - 거래소 포지션을 즉시 동기화해서 로컬 상태 초기화
    - 이어받기 메시지 전송
    - 용량가드, 리컨실러, 워치독 스레드 시작
    """
    try:
        _update_local_state_from_exchange()  # 초기 동기화
    except Exception as e:
        print("init sync err:", e)

    # 이어받기 안내
    try:
        opens = _safe_get_positions()
        n = len([p for p in opens if float(p.get("size") or 0) > 0])
        det = []
        for p in opens:
            try:
                if float(p.get("size") or 0) <= 0:
                    continue
                det.append(f"{convert_symbol(p.get('symbol') or '')}_{_norm_side(p.get('side'))}")
            except Exception:
                pass
        detail = (", ".join(det)) if det else "-"
        send_telegram(f"🔗 Resumed {n} open positions: {detail}")
    except Exception as e:
        print("resume msg err:", e)

    # 백그라운드 스레드 기동
    try:
        start_capacity_guard()
    except Exception as e:
        print("capacity guard start err:", e)
    try:
        start_reconciler()
    except Exception as e:
        print("reconciler start err:", e)
    try:
        start_watchdogs()
    except Exception as e:
        print("watchdog start err:", e)
