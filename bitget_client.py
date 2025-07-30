import os
import time
import random
import ccxt
from dotenv import load_dotenv

load_dotenv()
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

exchange = ccxt.bitget({
    "apiKey":          os.getenv("BITGET_API_KEY"),
    "secret":          os.getenv("BITGET_API_SECRET"),
    "password":        os.getenv("BITGET_API_PASSWORD"),
    "enableRateLimit": True,
    "timeout":         30000,
    "options": {
        "defaultType":         "swap",       # USDT-마진 선물
        "defaultMarginMode":   "isolated",   # 격리 레버리지
        "defaultPositionMode": "net_mode",   # 단일 포지션(One-Way)
        "adjustForTimeDifference": True,
    },
})
exchange.load_markets()

def get_market_id(symbol: str) -> str:
    """
    "BTCUSDT" → "BTC/USDT:USDT"
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
                print(f"⚠️ 네트워크 에러 {attempt}/3: {e} → {delay:.1f}s 후 재시도")
                time.sleep(delay + random.random() * 0.5)
                delay *= 2
        return fn(*args, **kwargs)
    return wrapper

@retry_on_network
def place_order(order_type: str, symbol: str, amount_usdt: float = 10) -> float:
    """
    order_type: "long" or "short"
    symbol: e.g. "BTCUSDT", "ETHUSDT"
    amount_usdt: 항상 10 USD
    """
    mid = get_market_id(symbol)

    # on-demand 마켓 로드
    if mid not in exchange.markets:
        exchange.load_markets()
    if mid not in exchange.markets:
        raise ValueError(f"Unknown market: {mid}")

    # 현재가 조회
    ticker     = exchange.fetch_ticker(mid)
    mark_price = ticker["last"]

    # Dry-Run 모드
    if DRY_RUN:
        print(f"[DRY_RUN] {order_type}@{mid}, USD={amount_usdt}, price={mark_price}")
        return mark_price

    # 단일(Mod e) 포지션 모드 재설정
    try:
        exchange.set_position_mode("net_mode")
    except Exception as e:
        print(f"⚠️ set_position_mode 실패: {e}")

    # 5배 레버리지
    exchange.set_leverage(5, mid)

    # 수량 계산 & 최소 수량 보정
    market   = exchange.markets[mid]
    min_qty  = market["limits"]["amount"]["min"]
    raw_qty  = amount_usdt / mark_price
    # CCXT 내장 함수로 정밀도 맞추기
    qty_str  = exchange.amount_to_precision(mid, raw_qty)
    qty      = float(qty_str)
    if qty < min_qty:
        print(f"⚠️ place_order: qty={qty} < min_qty={min_qty} → min_qty로 보정")
        qty = min_qty

    # 시장가 주문
    side  = "buy" if order_type == "long" else "sell"
    order = exchange.create_order(symbol=mid, type="market", side=side, amount=qty)
    print(f"✅ [{symbol}] {order_type.upper()} 체결 @ {mark_price} (qty={qty})")
    return mark_price
