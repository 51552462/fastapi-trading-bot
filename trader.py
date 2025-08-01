from bitget_api import place_market_order, close_all, get_last_price

# 메모리 내 포지션 상태 저장
position_data = {}

def enter_position(symbol, usdt_amount):
    """진입 주문 실행 후 진입가 저장"""
    resp = place_market_order(symbol, usdt_amount, side="buy", leverage=5)
    print(f"✅ 진입 주문 응답: {resp}")
    if resp.get("code") == "00000":
        entry_price = get_last_price(symbol)
        position_data[symbol] = {"entry_price": entry_price, "exit_stage": 0}
        return entry_price
    else:
        print(f"❌ 진입 실패: {resp}")
        return None

def take_partial_profit(symbol, pct=0.3):
    """분할 익절 (현재는 전체 청산으로 대체)"""
    if symbol not in position_data:
        print(f"❌ 익절 실패: {symbol} 포지션 없음")
        return
    return close_all(symbol)

def stoploss(symbol):
    """포지션 전체 손절"""
    print(f"🛑 손절: {symbol}")
    close_all(symbol)
    position_data.pop(symbol, None)

def check_loss_and_exit():
    """실시간 -10% 손실 감지 후 손절"""
    for symbol, info in list(position_data.items()):
        entry = info["entry_price"]
        now = get_last_price(symbol)
        drop = (now - entry) / entry
        if drop <= -0.10:
            print(f"🚨 -10% 손실 감지: {symbol} {entry} → {now}")
            stoploss(symbol)

def reset_position(symbol):
    position_data.pop(symbol, None)
