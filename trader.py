# trader.py

from bitget_api import place_market_order, close_all, get_last_price
from telegram_bot import send_telegram

# symbol별 진입가·익절단계·USDT금액 저장
position_data = {}  # { symbol: { entry_price, exit_stage, usdt_amount } }

def enter_position(symbol: str, usdt_amount: float):
    print(f"📍 진입 시작: {symbol}, 금액: {usdt_amount}")
    resp = place_market_order(symbol, usdt_amount, side="buy", leverage=5)
    print(f"✅ 진입 주문 응답: {resp}")

    if resp.get("code") == "00000":
        entry_price = get_last_price(symbol)
        position_data[symbol] = {
            "entry_price": entry_price,
            "exit_stage":  0,
            "usdt_amount": usdt_amount
        }
        msg = (
            f"🚀 *Entry* {symbol}\n"
            f"• 금액: {usdt_amount} USDT\n"
            f"• 진입가: {entry_price:.6f}"
        )
        send_telegram(msg)
        return entry_price
    else:
        send_telegram(f"❌ Entry 실패 {symbol}: {resp}")
        return None

def take_partial_profit(symbol: str, pct: float = 0.3):
    if symbol not in position_data:
        send_telegram(f"❌ TakeProfit 실패: {symbol} 포지션 없음")
        return

    data       = position_data[symbol]
    total_usdt = data["usdt_amount"]
    close_usdt = round(total_usdt * pct, 6)

    resp = place_market_order(symbol, close_usdt, side="sell", leverage=5)
    print(f"🤑 익절 {int(pct*100)}% → USDT {close_usdt}, 응답: {resp}")

    if resp.get("code") == "00000":
        # 남은 금액·단계 업데이트
        remaining = total_usdt - close_usdt
        data["usdt_amount"]  = remaining
        data["exit_stage"]  += 1

        msg = (
            f"🤑 *TakeProfit{int(pct*100)}* {symbol}\n"
            f"• 청산량: {close_usdt} USDT\n"
            f"• 남은 USDT: {remaining:.6f}"
        )
        send_telegram(msg)

        # 💡 전체 종료 조건
        if remaining <= 0 or pct >= 1.0:
            send_telegram(f"📕 *Position Closed* {symbol}")
            position_data.pop(symbol, None)

    else:
        send_telegram(f"❌ TakeProfit{int(pct*100)} 실패 {symbol}: {resp}")

    return resp

def stoploss(symbol: str):
    info        = position_data.get(symbol, {})
    entry_price = info.get("entry_price")
    usdt_amount = info.get("usdt_amount")

    resp = close_all(symbol)
    print(f"🛑 손절 응답: {resp}")

    position_data.pop(symbol, None)

    try:
        exit_price  = get_last_price(symbol)
        profit_pct  = (exit_price / entry_price - 1) * 100 if entry_price else 0
        profit_usdt = usdt_amount * profit_pct / 100 if usdt_amount else 0

        report = (
            f"🛑 *StopLoss* {symbol}\n"
            f"• 진입가: {entry_price:.6f}\n"
            f"• 청산가: {exit_price:.6f}\n"
            f"• P/L: {profit_usdt:.4f} USDT ({profit_pct:.2f}%)"
        )
    except Exception as e:
        report = f"⚠️ 손익 계산 실패: {e}"

    send_telegram(report)
    return resp

def check_loss_and_exit():
    for symbol, info in list(position_data.items()):
        entry_price   = info["entry_price"]
        current_price = get_last_price(symbol)

        if current_price <= entry_price * 0.90:
            msg = (
                f"🚨 *-10% 손절 트리거* {symbol}\n"
                f"• 진입가: {entry_price:.6f}\n"
                f"• 현재가: {current_price:.6f}"
            )
            send_telegram(msg)
            stoploss(symbol)
