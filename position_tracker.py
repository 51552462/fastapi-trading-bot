# position_tracker.py

from bitget_client import exchange

def start_tracker(symbol: str, side: str, entry_price: float):
    print(f"📈 start_tracker: {side} {symbol} @ {entry_price}")
    # TODO: 모니터링 로직 구현

def close_position(symbol: str):
    market_id = f"{symbol[:-4]}/USDT:USDT"
    try:
        balance = exchange.fetch_position(symbol=market_id)
    except Exception as e:
        print(f"⚠️ close_position: 포지션 조회 실패: {e}")
        return
    contracts = balance.get("contracts") if isinstance(balance, dict) else None
    if not contracts:
        print(f"ℹ️ close_position: {symbol} 포지션 없음, 무시")
        return
    try:
        qty = float(contracts)
    except Exception as e:
        print(f"⚠️ close_position: contracts→float 실패: {e}")
        return
    try:
        order = exchange.create_order(symbol=market_id, type="market", side="sell", amount=qty)
        print(f"🚪 close_position: {symbol} 전량 청산 qty={qty}")
    except Exception as e:
        print(f"⚠️ close_position: 주문 실패: {e}")

def close_partial(symbol: str, ratio: float):
    market_id = f"{symbol[:-4]}/USDT:USDT"
    try:
        balance = exchange.fetch_position(symbol=market_id)
    except Exception as e:
        print(f"⚠️ close_partial: 포지션 조회 실패: {e}")
        return
    contracts = balance.get("contracts") if isinstance(balance, dict) else None
    if not contracts:
        print(f"ℹ️ close_partial: {symbol} 포지션 없음, 무시")
        return
    try:
        total = float(contracts)
        amt   = total * ratio
    except Exception as e:
        print(f"⚠️ close_partial: float 변환 실패: {e}")
        return
    try:
        order = exchange.create_order(symbol=market_id, type="market", side="sell", amount=amt)
        print(f"🔪 close_partial: {symbol} 부분 청산 ratio={ratio}, qty={amt}")
    except Exception as e:
        print(f"⚠️ close_partial: 주문 실패: {e}")
