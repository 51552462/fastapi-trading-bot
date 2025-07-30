# bitget_client.py

import os, time, random
import ccxt
from dotenv import load_dotenv

load_dotenv()
DRY_RUN     = os.getenv("DRY_RUN", "false").lower() == "true"

exchange = ccxt.bitget({
    "apiKey":          os.getenv("BITGET_API_KEY"),
    "secret":          os.getenv("BITGET_API_SECRET"),
    "password":        os.getenv("BITGET_API_PASSWORD"),
    "enableRateLimit": True,
    "timeout":         30000,
    "options": {
        "defaultType": "swap",
        "adjustForTimeDifference": True,
    },
})

def get_market_id(symbol: str) -> str:
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
        return fn(*args, **kwargs)
    return wrapper

@retry_on_network
def place_order(order_type: str, symbol: str, amount_usdt: float = 1) -> float:
    mid = get_market_id(symbol)

    # on-demand markets 로드
    if not exchange.markets or mid not in exchange.markets:
        exchange.load_markets()
    if mid not in exchange.markets:
        raise ValueError(f"Unknown market: {mid}")

    # 현재가 조회
    ticker     = exchange.fetch_ticker(mid)
    mark_price = ticker["last"]

    # Dry-Run 모드
    if DRY_RUN:
        print(f"[DRY_RUN] {order_type}@{mid}, amount_usdt={amount_usdt}, price={mark_price}")
        return mark_price

    # 최소 수량 검사
    market  = exchange.markets[mid]
    min_qty = market["limits"]["amount"]["min"]
    precision = market["precision"]["amount"]
    raw_qty = amount_usdt / mark_price
    qty     = round(raw_qty, precision)
    if qty < min_qty:
        print(f"⚠️ place_order: qty={qty} < min_qty={min_qty} → 스킵")
        return mark_price

    # 레버리지 5배 설정 + 시장가 주문
    exchange.set_leverage(5, mid)
    side = "buy" if order_type == "long" else "sell"
    order = exchange.create_order(symbol=mid, type="market", side=side, amount=qty)
    print(f"✅ [{symbol}] {order_type.upper()} 체결 @ {mark_price} (qty={qty})")
    return mark_price
