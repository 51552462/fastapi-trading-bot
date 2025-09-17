# trader_spot.py
# ------------------------------------------------------------
# Spot trader core (entry / partial TPs / stoploss / close)
# - Keeps per-symbol position (qty, cost) in a local JSON file
# - Computes realized PnL on every partial/close
# - Sends Telegram notifications for ALL exits
# - Works with bitget_api_spot (V2 endpoints)
# ------------------------------------------------------------

import os
import json
import time
import threading
from typing import Dict, Any, Optional

from bitget_api_spot import (
    convert_symbol,
    get_last_price_spot,
    get_spot_free_qty,
    place_spot_market_buy,
    place_spot_market_sell_qty,
)

# Telegram spot bot
try:
    from telegram_spot_bot import send_telegram
except Exception:
    def send_telegram(_msg: str):  # fallback
        pass

# =========================
# Config (ratios & state)
# =========================
STATE_FILE = os.getenv("SPOT_POS_STATE_FILE", "spot_pos_state.json")

TP1_RATIO = float(os.getenv("TP1_RATIO", "0.33"))
TP2_RATIO = float(os.getenv("TP2_RATIO", "0.33"))
TP3_RATIO = float(os.getenv("TP3_RATIO", "0.34"))

SL1_RATIO = float(os.getenv("SL1_RATIO", "0.50"))
SL2_RATIO = float(os.getenv("SL2_RATIO", "0.50"))

# =========================
# Position Store
# =========================
_state_lock = threading.Lock()

class PositionStore:
    """
    유지: 심볼별 보유 수량(qty)과 총 원가(cost, USDT).
    avg_entry = cost / qty
    - add_buy: 매수 반영
    - realize_sell: 매도 시 실현손익 반환하고 상태 감소
    """
    def __init__(self, path: str):
        self.path = path
        self.pos: Dict[str, Dict[str, float]] = {}
        self._load()

    def _load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    self.pos = json.load(f)
            except Exception:
                self.pos = {}

    def _save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.pos, f, ensure_ascii=False)
        os.replace(tmp, self.path)

    def get_qty_cost(self, symbol: str) -> (float, float):
        p = self.pos.get(symbol, {"qty": 0.0, "cost": 0.0})
        return float(p.get("qty", 0.0)), float(p.get("cost", 0.0))

    def add_buy(self, symbol: str, qty: float, price: float):
        with _state_lock:
            p = self.pos.get(symbol, {"qty": 0.0, "cost": 0.0})
            p["qty"]  = float(p.get("qty", 0.0))  + float(qty)
            p["cost"] = float(p.get("cost", 0.0)) + float(qty) * float(price)
            self.pos[symbol] = p
            self._save()

    def realize_sell(self, symbol: str, sell_qty: float, sell_price: float) -> float:
        with _state_lock:
            p = self.pos.get(symbol, {"qty": 0.0, "cost": 0.0})
            cur_qty  = float(p.get("qty", 0.0))
            cur_cost = float(p.get("cost", 0.0))
            if cur_qty <= 0:
                # 기록 없으면 0 원가로 가정 (실현값만 계산)
                return float(sell_qty) * float(sell_price)

            sell_qty = min(float(sell_qty), cur_qty)
            avg_cost = cur_cost / cur_qty
            realized = (float(sell_price) - avg_cost) * float(sell_qty)

            # 상태 감소
            remain_qty  = cur_qty - sell_qty
            remain_cost = max(cur_cost - avg_cost * sell_qty, 0.0)
            p["qty"], p["cost"] = remain_qty, remain_cost
            self.pos[symbol] = p
            self._save()
            return float(realized)

pos_store = PositionStore(STATE_FILE)

# =========================
# Notify helpers
# =========================
def _ok(msg: str):
    try: send_telegram(msg)
    except Exception: pass

def _fail(tag: str, symbol: str, reason: str, err: Any):
    try: send_telegram(f"[SPOT] {tag} fail {symbol} ({reason}) -> {err}")
    except Exception: pass

def notify_close(symbol: str, side: str, reason: str, size: float, realized_usdt: float):
    sign = "✅"
    msg = (f"{sign} CLOSE {side.upper()} {symbol}\n"
           f"• Exit: {reason}\n"
           f"• Size: {size}\n"
           f"• Realized≈ {realized_usdt:.2f} USDT")
    _ok(msg)

# =========================
# Core actions
# =========================
def entry_long(symbol: str, usdt_amount: float) -> Dict[str, Any]:
    """
    시장가 매수 진입. usdt_amount 만큼 quoteOrderQty로 우선 시도.
    체결 후 상태 파일에 qty/cost 저장(근사).
    """
    sym = convert_symbol(symbol)
    _ok(f"[SPOT] entry {sym} long amt={usdt_amount}")

    # 체결 가격(근사) 확보
    px = get_last_price_spot(sym) or 0.0
    res = place_spot_market_buy(sym, float(usdt_amount))

    if str(res.get("code")) in ("00000", "0"):
        # 근사 체결 수량 = 금액/가격 (가격 없으면 추후 보강)
        if px > 0:
            est_qty = float(usdt_amount) / float(px)
            pos_store.add_buy(sym, est_qty, px)
        return res

    _fail("BUY", sym, "entry", res)
    return res

def _sell_and_notify(sym: str, qty: float, reason: str) -> Dict[str, Any]:
    """
    시장가 매도 + 실현손익 계산 + 알림 보장
    """
    if qty <= 0:
        return {"code": "LOCAL_NO_QTY", "msg": "qty<=0"}
    res = place_spot_market_sell_qty(sym, float(qty))
    if str(res.get("code")) in ("00000", "0"):
        px = get_last_price_spot(sym) or 0.0
        realized = pos_store.realize_sell(sym, float(qty), px)
        notify_close(sym, "long", reason, float(qty), realized)
    else:
        _fail("CLOSE", sym, reason, res)
    return res

def tp1(symbol: str) -> Dict[str, Any]:
    sym = convert_symbol(symbol)
    qty = get_spot_free_qty(sym, fresh=True) * TP1_RATIO
    return _sell_and_notify(sym, qty, "tp1")

def tp2(symbol: str) -> Dict[str, Any]:
    sym = convert_symbol(symbol)
    qty = get_spot_free_qty(sym, fresh=True) * TP2_RATIO
    return _sell_and_notify(sym, qty, "tp2")

def tp3(symbol: str) -> Dict[str, Any]:
    sym = convert_symbol(symbol)
    qty = get_spot_free_qty(sym, fresh=True) * TP3_RATIO
    return _sell_and_notify(sym, qty, "tp3")

def sl1(symbol: str) -> Dict[str, Any]:
    sym = convert_symbol(symbol)
    qty = get_spot_free_qty(sym, fresh=True) * SL1_RATIO
    return _sell_and_notify(sym, qty, "sl1")

def sl2(symbol: str) -> Dict[str, Any]:
    sym = convert_symbol(symbol)
    qty = get_spot_free_qty(sym, fresh=True) * SL2_RATIO
    return _sell_and_notify(sym, qty, "sl2")

def close_all(symbol: str, reason: str = "market") -> Dict[str, Any]:
    sym = convert_symbol(symbol)
    qty = get_spot_free_qty(sym, fresh=True)
    return _sell_and_notify(sym, qty, reason)

# =========================
# Drawdown killer (-3% 등)
# =========================
def close_if_drawdown(symbol: str, threshold_pct: float = -0.03) -> Optional[Dict[str, Any]]:
    """
    현재 평균진입가 대비 수익률이 threshold 이하이면 즉시 전량 청산.
    threshold_pct는 음수(예: -0.03 == -3%)
    """
    sym = convert_symbol(symbol)
    qty, cost = pos_store.get_qty_cost(sym)
    if qty <= 0 or cost <= 0:
        return None  # 기록 없음
    avg = cost / qty
    px = get_last_price_spot(sym) or 0.0
    if px <= 0:
        return None

    pnl_pct = (px / avg) - 1.0
    if pnl_pct <= float(threshold_pct):
        return close_all(sym, reason=f"dd {threshold_pct*100:.0f}%")
    return None

# =========================
# Convenience (router에서 사용)
# =========================
def handle_signal(signal_type: str, symbol: str, amount: Optional[float] = None) -> Dict[str, Any]:
    """
    main_spot.py에서 호출하기 좋은 단일 엔트리.
    signal_type: 'entry' | 'tp1' | 'tp2' | 'tp3' | 'sl1' | 'sl2' | 'close' | 'dd'
    amount: entry에서 USDT 금액 (필수)
    """
    t = (signal_type or "").lower()
    if t == "entry":
        if amount is None:
            return {"code": "LOCAL_BAD_REQ", "msg": "entry requires amount"}
        return entry_long(symbol, float(amount))
    elif t == "tp1":
        return tp1(symbol)
    elif t == "tp2":
        return tp2(symbol)
    elif t == "tp3":
        return tp3(symbol)
    elif t == "sl1":
        return sl1(symbol)
    elif t == "sl2":
        return sl2(symbol)
    elif t == "close":
        return close_all(symbol, reason="close")
    elif t == "dd":  # 강제 드로우다운 체크 후 종료
        out = close_if_drawdown(symbol, threshold_pct=-0.03)
        return out or {"code": "LOCAL_SKIP", "msg": "no drawdown trigger"}
    return {"code": "LOCAL_UNKNOWN", "msg": f"unknown signal {signal_type}"}
