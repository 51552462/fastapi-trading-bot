import os, time, random
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
        "defaultType":         "swap",
        "defaultMarginMode":   "isolated",
        "defaultPositionMode": "net_mode",
        "adjustForTimeDifference": True,
    },
})
exchange.load_markets()

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
def place_order(order_type: str, symbol: str, amount_usdt: float = 10) -> float:
    mid = get_market_id(symbol)

    # on-demand 마켓 로드
    if mid not in exchange.markets:
        exchange.load_markets()
    if mid not in exchange.markets:
        raise ValueError(f"Unknown market: {mid}")

    # 현재가
    mark_price = exchange.fetch_ticker(mid)["last"]

    if DRY_RUN:
        print(f"[DRY_RUN] {order_type}@{mid}, USD={amount_usdt}, price={mark_price}")
        return mark_price

    # One-Way 모드 보장
    try:
        exchange.set_position_mode("net_mode")
    except Exception as e:
        print(f"⚠️ set_position_mode 실패: {e}")

    # 레버리지
    exchange.set_leverage(5, mid)

    # 수량 계산 & 최소 수량 처리
    market  = exchange.markets[mid]
    min_qty = market["limits"]["amount"]["min"]
    raw_qty = amount_usdt / mark_price
    try:
        # precision 자동 반영
        qty_str = exchange.amount_to_precision(mid, raw_qty)
        qty     = float(qty_str)
    except Exception as e:
        print(f"⚠️ amount_to_precision 실패: {e} — qty를 min_qty로 보정")
        qty = min_qty

    if qty < min_qty:
        print(f"⚠️ place_order: qty={qty} < min_qty={min_qty} → 보정 to {min_qty}")
        qty = min_qty

    # 시장가 주문
    side  = "buy" if order_type=="long" else "sell"
    order = exchange.create_order(symbol=mid, type="market", side=side, amount=qty)
    print(f"✅ [{symbol}] {order_type.upper()} 체결 @ {mark_price} (qty={qty})")
    return mark_price
