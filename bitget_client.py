import os
import ccxt

exchange = ccxt.bitget({
    "apiKey": os.getenv("BITGET_API_KEY"),
    "secret": os.getenv("BITGET_API_SECRET"),
    "password": os.getenv("BITGET_API_PASSWORD"),
    "enableRateLimit": True,
    "options": {
        "defaultType": "swap"
    }
})

exchange.set_sandbox_mode(False)  # 실거래

def place_order(side, symbol, amount_usdt=15, leverage=5):
    market_id = symbol.upper()
    exchange.load_markets()

    if market_id not in exchange.markets:
        raise ValueError(f"{market_id} not found in exchange.markets")

    market = exchange.market(market_id)

    # 레버리지 설정
    exchange.set_leverage(leverage, market_id)

    # 현재가 조회
    ticker = exchange.fetch_ticker(market_id)
    mark_price = ticker["last"]

    # 수량 계산
    quantity = amount_usdt * leverage / mark_price
    min_qty = float(market["limits"]["amount"]["min"])

    if quantity < min_qty:
        print(f"⚠️ place_order: qty={quantity:.6f} < min_qty={min_qty} → 스킵")
        return None

    quantity = exchange.amount_to_precision(market_id, quantity)
    order = exchange.create_order(symbol=market_id, type="market", side=side, amount=quantity)
    print(f"🚀 start_tracker: {side} {market_id} @ {mark_price}")
    return mark_price
