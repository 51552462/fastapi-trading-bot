# bitget_client.py

import os, time, random
import ccxt
from dotenv import load_dotenv

load_dotenv()
DRY_RUN     = os.getenv("DRY_RUN", "false").lower() == "true"
USE_TESTNET = os.getenv("USE_TESTNET", "false").lower() == "true"

# 1) 기본 CCXT 설정 (테스트넷 URL override 제거)
config = {
    "apiKey":          os.getenv("BITGET_API_KEY"),
    "secret":          os.getenv("BITGET_API_SECRET"),
    "password":        os.getenv("BITGET_API_PASSWORD"),
    "enableRateLimit": True,
    "timeout":         30000,
    "options": {
        "defaultType": "swap",            # USDT-M 선물
        "adjustForTimeDifference": True,
    },
}
# *테스트넷 URL override 생략* — DRY_RUN 모드로만 주로 검증

exchange = ccxt.bitget(config)

def get_market_id(symbol: str) -> str:
    """
    'BTCUSDT' → 'BTC/USDT:USDT'
    """
    base, quote = symbol[:-4], symbol[-4:]
    return f"{base}/{quote}:USDT"

def retry_on_network(fn):
    def wrapper(*args, **kwargs):
        delay = 1.0
        for attempt in range(1, 4):
            try:
                return fn(*args, **kwargs)
            except ccxt.NetworkError as e:
                print(f"⚠️ 네트워크 에러 {attempt}/3: {e} — {delay:.1f}s 후 재시도")
                time.sleep(delay + random.random() * 0.5)
                delay *= 2
        # 3회 모두 실패하면 마지막 호출에서 에러 터뜨림
        return fn(*args, **kwargs)
    return wrapper

@retry_on_network
def place_order(order_type: str, symbol: str, amount_usdt: float = 1) -> float:
    mid = get_market_id(symbol)

    # 2) on-demand 로드: markets가 비어 있거나 심볼 누락 시에만 불러옴
    if not exchange.markets or mid not in exchange.markets:
        try:
            exchange.load_markets()
        except Exception as e:
            print(f"⚠️ 마켓 정보 로드 실패: {e}")
            raise

    if mid not in exchange.markets:
        raise ValueError(f"알 수 없는 마켓 심볼: {mid}")

    # 3) 현재가 조회
    ticker     = exchange.fetch_ticker(mid)
    mark_price = ticker["last"]

    # Dry-Run 모드면 실제 주문 스킵
    if DRY_RUN:
        print(f"[DRY_RUN] {order_type}@{mid}, amount_usdt={amount_usdt}, price={mark_price}")
        return mark_price

    # 4) 실거래 모드: 5배 레버리지 설정 + 시장가 진입
    qty = round(amount_usdt / mark_price, 4)
    exchange.set_leverage(5, mid)
    side = "buy" if order_type == "long" else "sell"
    order = exchange.create_order(symbol=mid, type="market", side=side, amount=qty)
    print(f"✅ [{symbol}] {order_type.upper()} 체결 @ {mark_price} (qty={qty})")
    return mark_price
