# position_tracker.py

import time
from bitget_client import exchange

def start_tracker(symbol: str, side: str, entry_price: float):
    """
    포지션 진입 후 PnL 모니터링을 시작합니다.
    필요에 따라 별도 스케줄러나 루프를 통해 실시간 모니터링 로직을 추가하세요.
    """
    print(f"📈 start_tracker: {side} {symbol} @ {entry_price}")
    # TODO: 실제 모니터링 로직 구현 (예: 스케줄러 등록, DB에 기록 등)
    # ex) tracker[symbol] = {"side": side, "entry": entry_price, ...}

def close_position(symbol: str):
    """
    해당 심볼의 전량 포지션을 시장가로 청산합니다.
    포지션이 없거나 조회 실패 시 그냥 무시합니다.
    """
    market_id = f"{symbol[:-4]}/USDT:USDT"
    # 포지션 조회
    try:
        balance = exchange.fetch_position(symbol=market_id)
    except Exception as e:
        print(f"⚠️ close_position: 포지션 조회 실패: {e}")
        return

    # contracts 필드 확인
    contracts = balance.get("contracts") if isinstance(balance, dict) else None
    if not contracts:
        print(f"ℹ️ close_position: {symbol} 포지션 없음, 무시")
        return

    # float 변환
    try:
        qty = float(contracts)
    except Exception as e:
        print(f"⚠️ close_position: contracts→float 변환 실패: {contracts} / {e}")
        return

    side = "sell"  # long 포지션 청산
    # 시장가 전량 청산
    try:
        order = exchange.create_order(
            symbol=market_id,
            type="market",
            side=side,
            amount=qty,
        )
        print(f"🚪 close_position: {symbol} 전량 청산 qty={qty}")
    except Exception as e:
        print(f"⚠️ close_position: 주문 실패: {e}")

def close_partial(symbol: str, ratio: float):
    """
    해당 심볼의 보유 포지션 중 ratio 비율만큼 부분 청산합니다.
    포지션이 없거나 조회 실패 시 그냥 무시합니다.
    """
    market_id = f"{symbol[:-4]}/USDT:USDT"
    # 포지션 조회
    try:
        balance = exchange.fetch_position(symbol=market_id)
    except Exception as e:
        print(f"⚠️ close_partial: 포지션 조회 실패: {e}")
        return

    # contracts 필드 확인
    contracts = balance.get("contracts") if isinstance(balance, dict) else None
    if not contracts:
        print(f"ℹ️ close_partial: {symbol} 포지션 없음, 무시")
        return

    # float 변환 및 비율 적용
    try:
        total = float(contracts)
        amt   = total * ratio
    except Exception as e:
        print(f"⚠️ close_partial: contracts→float 변환 실패: {contracts} / {e}")
        return

    side = "sell"
    # 시장가 부분 청산
    try:
        order = exchange.create_order(
            symbol=market_id,
            type="market",
            side=side,
            amount=amt,
        )
        print(f"🔪 close_partial: {symbol} 부분 청산 ratio={ratio}, qty={amt}")
    except Exception as e:
        print(f"⚠️ close_partial: 주문 실패: {e}")
