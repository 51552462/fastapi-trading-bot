from bitget_api import place_market_order, close_all, get_last_price

# { symbol: { entry_price, exit_stage, usdt_amount } }
position_data = {}

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
        print(f"🚀 진입 성공! 진입가: {entry_price}")
        return entry_price
    else:
        print(f"❌ 진입 실패: {resp}")
        return None

def take_partial_profit(symbol: str, pct: float = 0.3):
    if symbol not in position_data:
        print(f"❌ 익절 실패: {symbol} 포지션 없음")
        return

    data = position_data[symbol]
    total_usdt = data["usdt_amount"]
    close_usdt = round(total_usdt * pct, 6)
    print(f"🤑 익절 {int(pct*100)}% → USDT {close_usdt}")

    resp = place_market_order(symbol, close_usdt, side="sell", leverage=5)
    print(f"✅ 익절 주문 응답: {resp}")

    if resp.get("code") == "00000":
        remaining = total_usdt - close_usdt
        if remaining <= 0:
            print(f"📕 포지션 완전 청산: {symbol}")
            position_data.pop(symbol, None)
        else:
            position_data[symbol]["usdt_amount"] = remaining
            position_data[symbol]["exit_stage"] += 1
    else:
        print(f"❌ 익절 실패 응답: {resp}")

    return resp

def stoploss(symbol: str):
    info = position_data.get(symbol, {})
    entry_price = info.get("entry_price")
    usdt_amount = info.get("usdt_amount")

    print(f"🛑 손절/청산 실행: {symbol}")
    resp = close_all(symbol)
    print(f"🛑 손절 응답: {resp}")

    if symbol in position_data:
        position_data.pop(symbol)

    # 종료 직후 손익·수익률 로그
    try:
        exit_price = get_last_price(symbol)
        profit_pct = (exit_price / entry_price - 1) * 100
        profit_usdt = usdt_amount * profit_pct / 100
        print(f"📊 손익 리포트: 진입가 {entry_price:.6f} → 청산가 {exit_price:.6f} | "
              f"손익 {profit_usdt:.4f} USDT ({profit_pct:.2f}%)")
    except Exception as e:
        print(f"⚠️ 손익 계산 실패: {e}")

    return resp

def check_loss_and_exit():
    """
    실시간 현재가 조회 후, 진입가 대비 90% 이하이면 즉시 손절
    """
    for symbol, info in list(position_data.items()):
        entry_price = info["entry_price"]
        current_price = get_last_price(symbol)
        if current_price <= entry_price * 0.90:
            print(f"🚨 실시간 -10% 손절 트리거: {symbol} "
                  f"{entry_price:.6f} → {current_price:.6f}")
            stoploss(symbol)
