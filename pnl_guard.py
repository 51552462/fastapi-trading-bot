"""
pnl_guard.py
- 언리얼라이즈드 PnL% 기준 긴급 컷.
- EMERGENCY_UNREAL_PNL_PCT < 0 (예: -10) 로 설정하면 손실 -10% 이하 시 즉시 청산 트리거.
- 가격 급락 컷(STOP_PRICE_MOVE)과 동시에 사용할 수 있으나, 보수적으로는 "둘 중 하나"만 권장.
"""

import os

EMERGENCY_UNREAL_PNL_PCT = float(os.getenv("EMERGENCY_UNREAL_PNL_PCT", "0") or "0")

def should_pnl_cut(side: str, mark_price: float, avg_price: float) -> bool:
    """
    side: 'long'|'short' (대소문자 무관)
    mark_price / avg_price: > 0
    return: 손실이 임계치 이하일 때 True(=청산 권고)
    """
    if EMERGENCY_UNREAL_PNL_PCT >= 0:
        return False
    try:
        if not (mark_price and avg_price) or mark_price <= 0 or avg_price <= 0:
            return False
        s = (side or "").lower()
        if s.startswith("l"):
            pnl_pct = (mark_price / avg_price - 1.0) * 100.0
        else:
            pnl_pct = (avg_price / mark_price - 1.0) * 100.0
        return pnl_pct <= EMERGENCY_UNREAL_PNL_PCT
    except Exception:
        return False
