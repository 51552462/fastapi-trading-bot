# trader.py

from bitget_api import place_market_order, get_last_price
from telegram_bot import send_telegram

position_data = {}

def enter_position(symbol: str, usdt_amount: float, side: str = "long"):
    key = f"{symbol}_{side}"
    print(f"📍 진입 시작: {key}, 금액: {usdt_amount}")
    resp = place_market_order(symbol, usdt_amount, side="buy" if side=="long" else "sell", leverage=5)
    print(f"✅ 진입 주문 응답: {resp}")

    if resp.get("code") == "00000":
        entry_price = get_last_price(symbol)
        position_data[key] = {
            "entry_price": entry_price,
            "exit_stage":  0,
            "usdt_amount": usdt_amount
        }
        emoji = "🚀" if side == "long" else "📉"
        msg = (
            f"{emoji} *Entry {side.upper()}* {symbol}\n"
            f"• 금액: {usdt_amount} USDT\n"
            f"• 진입가: {entry_price:.6f}"
        )
        send_telegram(msg)
        return entry_price
    else:
        send_telegram(f"❌ Entry 실패 {symbol}({side}): {resp}")
        return None

def take_partial_profit(symbol: str, pct: float = 0.3, side: str = "long"):
    key = f"{symbol}_{side}"
    if key not in position_data:
        send_telegram(f"❌ TakeProfit 실패: {key} 포지션 없음")
        return

    data       = position_data[key]
    total_usdt = data["usdt_amount"]
    close_usdt = round(total_usdt * pct, 6)

    close_side = "sell" if side == "long" else "buy"
    resp = place_market_order(symbol, close_usdt, side=close_side, leverage=5)
    print(f"🤑 익절 {int(pct*100)}% → USDT {close_usdt}, 응답: {resp}")

    if resp.get("code") == "00000":
        remaining = total_usdt - close_usdt
        data["usdt_amount"] = remaining
        data["exit_stage"] += 1

        msg = (
            f"🤑 *TakeProfit{int(pct*100)} {side.upper()}* {symbol}\n"
            f"• 청산량: {close_usdt} USDT\n"
            f"• 남은 USDT: {remaining:.6f}"
        )
        send_telegram(msg)

        if remaining <= 0.01 or pct >= 1.0 or data["exit_stage"] >= 3:
            send_telegram(f"📕 *Position Closed* {key}")
            position_data.pop(key, None)
    else:
        send_telegram(f"❌ TakeProfit 실패 {key}: {resp}")
    return resp

def stoploss(symbol: str, side: str = "long"):
    key = f"{symbol}_{side}"
    info = position_data.get(key, {})
    entry_price = info.get("entry_price")
    usdt_amount = info.get("usdt_amount")

    if not info:
        send_telegram(f"❌ StopLoss 실패: {key} 포지션 없음")
        return

    close_side = "sell" if side == "long" else "buy"
    close_usdt = round(usdt_amount, 6)
    if close_usdt < 1:
        close_usdt = 1.01  # 최소 수량 보정

    resp = place_market_order(symbol, close_usdt, side=close_side, leverage=5)
    print(f"🛑 손절 응답: {resp}")
    position_data.pop(key, None)

    try:
        exit_price  = get_last_price(symbol)
        if side == "short":
            profit_pct  = (entry_price - exit_price) / entry_price * 100
        else:
            profit_pct  = (exit_price / entry_price - 1) * 100
        profit_usdt = usdt_amount * profit_pct / 100

        report = (
            f"🛑 *StopLoss {side.upper()}* {symbol}\n"
            f"• 진입가: {entry_price:.6f}\n"
            f"• 청산가: {exit_price:.6f}\n"
            f"• P/L: {profit_usdt:.4f} USDT ({profit_pct:.2f}%)"
        )
    except Exception as e:
        report = f"⚠️ 손익 계산 실패: {e}"

    send_telegram(report)
    return resp

def check_loss_and_exit():
    for key, info in list(position_data.items()):
        symbol, side = key.rsplit("_", 1)
        entry_price   = info["entry_price"]
        current_price = get_last_price(symbol)

        if side == "long" and current_price <= entry_price * 0.90:
            msg = (
                f"🚨 *-10% 손절 트리거 (LONG)* {symbol}\n"
                f"• 진입가: {entry_price:.6f}\n"
                f"• 현재가: {current_price:.6f}"
            )
            send_telegram(msg)
            stoploss(symbol, side)

        elif side == "short" and current_price >= entry_price * 1.10:
            msg = (
                f"🚨 *-10% 손절 트리거 (SHORT)* {symbol}\n"
                f"• 진입가: {entry_price:.6f}\n"
                f"• 현재가: {current_price:.6f}"
            )
            send_telegram(msg)
            stoploss(symbol, side)
