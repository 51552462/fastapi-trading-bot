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
        "defaultMarginMode":   "isolated",    # ← One-way(단일) 포지션 모드 강제
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
def place_order(order_type: str, symbol: str, amount_usdt: float = 10) -> float:
    """
    order_type: "long" or "short"
    symbol: e.g. "BTCUSDT", "ETHUSDT"
    amount_usdt: 항상 10 USD 고정
    """
    mid = get_market_id(symbol)

    # 1) on-demand로 마켓 정보 로드
    if not exchange.markets or mid not in exchange.markets:
        exchange.load_markets()
    if mid not in exchange.markets:
        raise ValueError(f"Unknown market: {mid}")

    # 2) 현재가 조회
    ticker     = exchange.fetch_ticker(mid)
    mark_price = ticker["last"]

    # 3) Dry-Run 모드면 시뮬레이션만
    if DRY_RUN:
        print(f"[DRY_RUN] {order_type}@{mid}, USD={amount_usdt}, price={mark_price}")
        return mark_price

    # 4) One-way(단일) position 모드 재확인
    try:
        exchange.set_margin_mode("isolated", mid)
    except Exception as e:
        print(f"⚠️ set_margin_mode 실패: {e}")

    # 5) 최소 수량 검사 & 보정
    market   = exchange.markets[mid]
    min_qty  = market["limits"]["amount"]["min"]
    raw_prec = market["precision"].get("amount", 4)
    prec     = int(raw_prec)
    raw_qty  = amount_usdt / mark_price
    qty      = round(raw_qty, prec)
    if qty < min_qty:
        print(f"⚠️ place_order: qty={qty} < min_qty={min_qty} → 보정 to {min_qty}")
        qty = min_qty

    # 6) 레버리지 5배 설정
    exchange.set_leverage(5, mid)

    # 7) 시장가 주문
    side = "buy" if order_type == "long" else "sell"
    order = exchange.create_order(symbol=mid, type="market", side=side, amount=qty)
    print(f"✅ [{symbol}] {order_type.upper()} 체결 @ {mark_price} (qty={qty})")
    return mark_price
