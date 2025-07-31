import os, time, random
import ccxt
from dotenv import load_dotenv

load_dotenv()
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

exchange = ccxt.bitget({
    "apiKey":       os.getenv("BITGET_API_KEY"),
    "secret":       os.getenv("BITGET_API_SECRET"),
    "password":     os.getenv("BITGET_API_PASSWORD"),
    "enableRateLimit": True,
    "timeout":      30000,
    "options": {
        "defaultType":         "swap",
        "defaultMarginMode":   "isolated",
        "defaultPositionMode": "dual_mode",      # ← Dual Mode(Hedge) 활성화
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
    """
    order_type: "long" or "short"
    symbol: e.g. "BTCUSDT", "ETHUSDT"
    amount_usdt: 항상 10 USD
    """
    mid = get_market_id(symbol)

    # on-demand markets 로드
    if mid not in exchange.markets:
        exchange.load_markets()
    if mid not in exchange.markets:
        raise ValueError(f"Unknown market: {mid}")

    # 현재가 조회
    mark_price = exchange.fetch_ticker(mid)["last"]

    # Dry-Run 모드: 페이크 리턴
    if DRY_RUN:
        print(f"[DRY_RUN] {order_type}@{mid}, USD={amount_usdt}, price={mark_price}")
        return mark_price

    # 1) Dual Mode 보장
    try:
        exchange.set_position_mode("both_side", mid)
    except Exception as e:
        print(f"⚠️ set_position_mode 실패: {e}")

    # 2) 레버리지 5배
    exchange.set_leverage(5, mid)

    # 3) 수량 계산 & min_qty 보정
    market   = exchange.markets[mid]
    min_qty  = market["limits"]["amount"]["min"]
    raw_qty  = amount_usdt / mark_price
    qty_str  = exchange.amount_to_precision(mid, raw_qty)  # CCXT 정밀도 처리
    qty      = float(qty_str)
    if qty < min_qty:
        print(f"⚠️ place_order: qty={qty} < min_qty={min_qty} → 보정 to {min_qty}")
        qty = min_qty

    # 4) 주문 파라미터 준비
    side = "buy" if order_type == "long" else "sell"
    # Dual Mode 에서는 positionSide 로 롱/숏 지정
    params = {"positionSide": "long" if order_type=="long" else "short"}

    # 5) 시장가 주문
    order = exchange.create_order(
        symbol=mid,
        type="market",
        side=side,
        amount=qty,
        params=params
    )
    print(f"✅ [{symbol}] {order_type.upper()} 체결 @ {mark_price} (qty={qty})")
    return mark_price
