# trader.py
from bitget_api import place_market_order, close_all, get_last_price

position_data = {}  # {"BTCUSDT": {"entry_price": 62800.0, "exit_stage": 0}}

def enter_position(symbol, usdt_amount):
    resp = place_market_order(symbol, usdt_amount, side="buy", leverage=5)
    print(f"✅ 진입 주문 응답: {resp}")
    if resp.get("code") == "00000":
        entry_price = get_last_price(symbol)
        position_data[symbol] = {
            "entry_price": entry_price,
            "exit_stage": 0
        }
        return entry_price
    else:
        print(f"❌ 진입 실패: {resp}")
        return None

def take_partial_profit(symbol, pct=0.3):
    if symbol not in position_data:
        print(f"❌ 익절 실패: {symbol} 포지션 없음")
        return
    # 비트겟 API는 분할 청산 시 사이즈 비율을 직접 지정할 수 없어서 전체 종료로 단순화 처리하거나 추후 확장 필요
    print(f"⚠️ 현재 분할 익절은 전체 종료로 대체됩니다 (테스트 단계)")
    return close_all(symbol)

def stoploss(symbol):
    print(f"🛑 손절: {symbol}")
    close_all(symbol)
    if symbol in position_data:
        del position_data[symbol]

def check_loss_and_exit():
    for symbol, info in list(position_data.items()):
        entry = info["entry_price"]
        now = get_last_price(symbol)
        drop = (now - entry) / entry
        if drop <= -0.10:
            print(f"🚨 -10% 손실 감지: {symbol} {entry} → {now}")
            stoploss(symbol)

def reset_position(symbol):
    if symbol in position_data:
        del position_data[symbol]
