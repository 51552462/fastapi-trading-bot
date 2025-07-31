import os
import ccxt

# ─── Bitget CCXT Exchange 초기화 ───────────────────────
exchange = ccxt.bitget({
    "apiKey":    os.getenv("BITGET_API_KEY"),
    "secret":    os.getenv("BITGET_API_SECRET"),
    "password":  os.getenv("BITGET_API_PASSWORD"),
    "enableRateLimit": True,
    "options": {
        "defaultType": "swap"       # 'swap' 선물, 'spot' 현물
    }
})

# 실거래 모드 설정 (False: 실거래, True: 테스트넷)
exchange.set_sandbox_mode(False)

# ─── 모드 및 엔드포인트 확인 로그 ───────────────────────
print("▶▶▶ Bitget CCXT 초기화 완료")
print("    • API URL     :", exchange.urls.get("api"))
print("    • Sandbox?    :", exchange.options.get("sandbox"))
print("    • MarketType  :", exchange.options.get("defaultType"))
# ────────────────────────────────────────────────────────


def place_order(side, symbol, amount_usdt=15, leverage=5):
    """
    시장가 주문 실행
    side: 'buy' 또는 'sell'
    symbol: CCXT 심볼, 예: "BTC/USDT" 또는 "ETH/USDT"
    amount_usdt: 사용할 USDT 금액
    leverage: 레버리지 배수
    """
    exchange.load_markets()

    # 심볼 통일 (예: "BTCUSDT" → "BTC/USDT")
    unified = symbol.upper()
    if "/" not in unified and unified.endswith("USDT"):
        unified = unified[:-4] + "/USDT"

    # market_id 조회 (Bitget 내부 ID, 보통 "BTCUSDT")
    market = exchange.market(unified)
    market_id = market["id"]

    # 레버리지 설정
    exchange.set_leverage(leverage, market_id)

    # 현재가(마크 가격) 조회
    ticker = exchange.fetch_ticker(market_id)
    mark_price = ticker["last"]

    # 주문 수량 계산
    quantity = amount_usdt * leverage / mark_price
    min_qty = float(market["limits"]["amount"]["min"])
    if quantity < min_qty:
        print(f"⚠️ place_order: qty={quantity:.6f} < min_qty={min_qty} → 스킵")
        return None

    # CCXT 형식에 맞춰 정밀도 조정
    quantity = exchange.amount_to_precision(market_id, quantity)

    # 시장가 주문 실행
    order = exchange.create_order(symbol=market_id, type="market", side=side, amount=quantity)
    print(f"🚀 place_order: {side.upper()} {market_id} @ {mark_price} (qty={quantity})")
    return mark_price
