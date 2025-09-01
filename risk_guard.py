# risk_guard.py
import os
from typing import Dict
from bitget_api import get_open_positions

try:
    from bitget_api import get_account_equity as _get_equity_api
except Exception:
    _get_equity_api = None

try:
    from bitget_api import get_wallet_balance as _get_wallet_balance
except Exception:
    _get_wallet_balance = None

try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str): print("[TG]", msg)

def _f(k, d=None, cast=float):
    v = os.getenv(k, "")
    if v == "" or v is None: return d
    try: return cast(v)
    except: return d

# === ENV ===
RISK_BUDGET_PCT       = _f("RISK_BUDGET_PCT", 1.2)       # 계좌 대비 총 위험 허용 %
RISK_BUDGET_LONG_PCT  = _f("RISK_BUDGET_LONG_PCT", None) # (선택) 롱 상한
RISK_BUDGET_SHORT_PCT = _f("RISK_BUDGET_SHORT_PCT", None)# (선택) 숏 상한
HEDGE_CREDIT_FACTOR   = _f("HEDGE_CREDIT_FACTOR", 0.50)  # 롱/숏 상쇄 인정(0~1)
DEFAULT_STOP_PCT      = _f("DEFAULT_STOP_PCT", 0.10)     # 청산가 없을 때 위험 가정
ACCOUNT_EQUITY_FALLBACK = _f("ACCOUNT_EQUITY", 10000.0)  # API 실패시 폴백

def _current_equity() -> float:
    # 1) 선호: 전계좌 자본 API
    if _get_equity_api:
        try:
            eq = float(_get_equity_api() or 0.0)
            if eq > 0: return eq
        except Exception:
            pass
    # 2) 월렛 잔고 시도
    if _get_wallet_balance:
        try:
            bal = _get_wallet_balance("USDT")
            if isinstance(bal, (int, float)) and bal > 0: return float(bal)
            if isinstance(bal, dict):
                v = float(bal.get("total", 0) or bal.get("available", 0) or 0)
                if v > 0: return v
        except Exception:
            pass
    # 3) 폴백
    return float(ACCOUNT_EQUITY_FALLBACK or 0.0)

def _estimate_position_risk_usdt(pos: Dict) -> float:
    """
    위험(USDT) = (size × entry_price) × 유효 CUT 거리
    - CUT: 청산가 있으면 그 거리, 없으면 DEFAULT_STOP_PCT 사용
    """
    try:
        entry = float(pos.get("entry_price") or 0)
        size  = float(pos.get("size") or 0)
        liq   = float(pos.get("liq_price") or 0)
    except Exception:
        return 0.0
    if entry <= 0 or size <= 0: return 0.0

    risk_pct = abs((entry - liq) / entry) if (liq and liq > 0) else float(DEFAULT_STOP_PCT or 0.10)
    return max(0.0, size * entry * risk_pct)

def _split_long_short_risk(positions):
    long_r, short_r = 0.0, 0.0
    for p in positions:
        side = (p.get("side") or "").lower()
        r = _estimate_position_risk_usdt(p)
        if side == "short": short_r += r
        else:               long_r  += r
    return long_r, short_r

def _risk_caps(equity: float):
    total_cap = equity * (float(RISK_BUDGET_PCT) / 100.0)
    long_cap  = equity * (float(RISK_BUDGET_LONG_PCT)  / 100.0) if RISK_BUDGET_LONG_PCT  is not None else None
    short_cap = equity * (float(RISK_BUDGET_SHORT_PCT) / 100.0) if RISK_BUDGET_SHORT_PCT is not None else None
    return total_cap, long_cap, short_cap

def can_open(new_pos: Dict) -> bool:
    """
    새 포지션 진입 허용 여부
      new_pos: {"symbol":..., "side": "long"/"short", "entry_price":..., "size":...}
    """
    equity = _current_equity()
    if equity <= 0:
        send_telegram("⛔ RiskGuard: equity 조회 실패 → 보수 차단")
        return False

    positions = get_open_positions() or []
    long_r, short_r = _split_long_short_risk(positions)

    # 헤지 상쇄(롱·숏 겹침)
    overlap = min(long_r, short_r)
    hedge_credit = overlap * float(max(0.0, min(1.0, HEDGE_CREDIT_FACTOR)))
    net_risk = (long_r + short_r) - hedge_credit

    total_cap, long_cap, short_cap = _risk_caps(equity)

    new_r = _estimate_position_risk_usdt(new_pos)
    side = (new_pos.get("side") or "long").lower()

    if side == "long" and long_cap is not None and (long_r + new_r > long_cap):
        send_telegram(f"⛔ RiskGuard(Long) {long_r+new_r:.2f} > cap {long_cap:.2f}")
        return False
    if side == "short" and short_cap is not None and (short_r + new_r > short_cap):
        send_telegram(f"⛔ RiskGuard(Short) {short_r+new_r:.2f} > cap {short_cap:.2f}")
        return False
    if net_risk + new_r > total_cap:
        send_telegram(f"⛔ RiskGuard(Total) {net_risk+new_r:.2f} > cap {total_cap:.2f}")
        return False
    return True
