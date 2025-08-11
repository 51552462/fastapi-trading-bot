# trader.py
from datetime import datetime
from zoneinfo import ZoneInfo
from bitget_api import place_market_order, close_all, get_last_price
from telegram_bot import send_telegram

KST = ZoneInfo("Asia/Seoul")

# 메모리상 포지션 상태
# key: "BTCUSDT_long" → {
#   entry_price: float,
#   usdt_opened: float,      # 최초 진입 노출액
#   usdt_remaining: float,   # 남은 노출액 (부분청산 시 감소)
#   realized_pnl: float      # 지금까지 실현된 PnL 합
# }
position_data = {}

# 일별 통계 (KST 기준)
daily_stats = {
    "date": datetime.now(KST).date(),
    "profit": 0.0,   # 이익 합 (양수만)
    "loss": 0.0      # 손실 합 (양수값으로 저장)
}

def _rollover_if_new_day():
    """KST 기준 날짜 변경 시 일별 통계를 새로 시작 (안전장치)."""
    today = datetime.now(KST).date()
    if daily_stats["date"] != today:
        daily_stats["date"] = today
        daily_stats["profit"] = 0.0
        daily_stats["loss"] = 0.0

def _pnl_usdt(entry_price: float, exit_price: float, notional_usdt: float, side: str) -> float:
    """
    선물/코인 기준 단순화된 USDT 손익 계산.
    - side=='long'  : notional * (exit/entry - 1)
    - side=='short' : notional * (entry/exit - 1)
    """
    if side == "long":
        return notional_usdt * (exit_price / entry_price - 1.0)
    else:
        return notional_usdt * (entry_price / exit_price - 1.0)

def _record_daily(pnl: float):
    """일별 통계에 PnL 반영."""
    _rollover_if_new_day()
    if pnl >= 0:
        daily_stats["profit"] += pnl
    else:
        daily_stats["loss"] += abs(pnl)

def send_daily_summary_and_reset():
    """KST 기준 현재 일자 통계를 텔레그램으로 보내고 리셋."""
    _rollover_if_new_day()
    date_str = daily_stats["date"].isoformat()
    profit = daily_stats["profit"]
    loss   = daily_stats["loss"]
    net    = profit - loss
    send_telegram(
        f"📊 Daily PnL Summary ({date_str}, KST)\n"
        f"✅ Profit: +{profit:.2f} USDT\n"
        f"❌ Loss:   -{loss:.2f} USDT\n"
        f"📈 Net:    {net:+.2f} USDT"
    )
    # 리셋 (다음 날 누적용)
    daily_stats["profit"] = 0.0
    daily_stats["loss"]   = 0.0

def enter_position(symbol: str, usdt_amount: float, side: str="long"):
    key = f"{symbol}_{side}"
    resp = place_market_order(
        symbol, usdt_amount,
        side="buy" if side=="long" else "sell",
        leverage=5
    )
    if resp.get("code") == "00000":
        entry = get_last_price(symbol)
        position_data[key] = {
            "entry_price": entry,
            "usdt_opened": usdt_amount,
            "usdt_remaining": usdt_amount,
            "realized_pnl": 0.0
        }
        send_telegram(
            f"🚀 ENTRY {side.upper()} {symbol}\n"
            f"• Price: {entry:.6f}\n"
            f"• Notional: {usdt_amount:.2f} USDT\n"
            f"• Leverage: 5x"
        )
    else:
        send_telegram(f"❌ Entry 실패 {key}: {resp}")

def take_partial_profit(symbol: str, pct: float, side: str="long"):
    key = f"{symbol}_{side}"
    if key not in position_data:
        send_telegram(f"❌ TakeProfit 실패: {key} 없음")
        return

    data = position_data[key]
    # 남은 노출액 기준 비중 계산
    portion_usdt = round(data["usdt_remaining"] * pct, 6)
    if portion_usdt <= 0:
        send_telegram(f"⚠️ TakeProfit 스킵: 남은 노출액 0 ({key})")
        return

    resp = place_market_order(
        symbol, portion_usdt,
        side="sell" if side=="long" else "buy",
        leverage=5
    )
    exit_price = get_last_price(symbol)

    if resp.get("code") == "00000":
        pnl = _pnl_usdt(data["entry_price"], exit_price, portion_usdt, side)
        data["realized_pnl"] += pnl
        data["usdt_remaining"] -= portion_usdt

        _record_daily(pnl)

        send_telegram(
            f"🤑 TP{int(pct*100)} {side.upper()} {symbol}\n"
            f"• Exit: {exit_price:.6f}\n"
            f"• Portion: {portion_usdt:.2f} USDT ({int(pct*100)}%)\n"
            f"• Realized PnL: {pnl:+.2f} USDT\n"
            f"• Cum Realized: {data['realized_pnl']:+.2f} USDT\n"
            f"• Remaining: {data['usdt_remaining']:.2f} USDT"
        )
    else:
        send_telegram(f"❌ TakeProfit 실패 {key}: {resp}")

def close_position(symbol: str, side: str="long", reason: str=""):
    key = f"{symbol}_{side}"
    # Bitget에 우선 청산 요청
    resp = close_all(symbol)

    # position_data에 정보가 있으면 남은 물량에 대한 PnL 마무리
    if key in position_data:
        data = position_data[key]
        exit_price = get_last_price(symbol)

        if data["usdt_remaining"] > 0:
            pnl = _pnl_usdt(data["entry_price"], exit_price, data["usdt_remaining"], side)
            data["realized_pnl"] += pnl
            _record_daily(pnl)
        else:
            pnl = 0.0

        send_telegram(
            f"🛑 CLOSE ({reason}) {side.upper()} {symbol}\n"
            f"• Exit: {exit_price:.6f}\n"
            f"• Final Realized: {data['realized_pnl']:+.2f} USDT\n"
            f"• Last Portion PnL: {pnl:+.2f} USDT"
        )
        # 상태 제거
        position_data.pop(key, None)
    else:
        # 포지션 정보 없을 때도 알림
        send_telegram(f"🛑 CLOSE ({reason}) {side.upper()} {symbol} → position_data 없음\n응답: {resp}")

    # Bitget 응답 성공/실패도 별도 전송
    if resp.get("code") != "00000":
        send_telegram(f"❌ Bitget Close 실패 {key} → {resp}")

def check_loss_and_exit():
    # -10% 손절 감시
    for key, info in list(position_data.items()):
        symbol, side = key.rsplit("_",1)
        entry = info["entry_price"]
        now   = get_last_price(symbol)

        if side=="long" and now <= entry*0.90:
            send_telegram(f"🚨 -10% SL LONG {symbol}")
            close_position(symbol, side, "stoploss")
        if side=="short" and now >= entry*1.10:
            send_telegram(f"🚨 -10% SL SHORT {symbol}")
            close_position(symbol, side, "stoploss")
