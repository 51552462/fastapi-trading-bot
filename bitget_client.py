import os, time, random
import ccxt
from dotenv import load_dotenv

load_dotenv()

exchange = ccxt.bitget({
    "apiKey":          os.getenv("BITGET_API_KEY"),
    "secret":          os.getenv("BITGET_API_SECRET"),
    "password":        os.getenv("BITGET_API_PASSWORD"),
    "enableRateLimit": True,
    "timeout":         30000,    # 30초
    "options": {
        "defaultType": "swap",   # USDT-M 선물 전용
        "adjustForTimeDifference": True,
    },
})

# 시작하자마자 마켓 로드 (markets를 None ➡ dict로)
exchange.load_markets()

def get_market_id(symbol: str) -> str:
    """
    'BTCUSDT'  → 'BTC/USDT:USDT'
    """
    base  = symbol[:-4]
    quote = symbol[-4:]
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
        # 3회 모두 실패 시 마지막 시도에서 예외 터뜨리기
        return fn(*args, **kwargs)
    return wrapper

@retry_on_network
def place_order(order_type: str, symbol: str, amount_usdt: float = 1) -> float:
    mid = get_market_id(symbol)

    # markets가 None이거나 mid가 없는 경우 다시 로드
    if not exchange.markets or mid not in exchange.markets:
        exchange.load_markets()

    if mid not in exchange.markets:
        raise ValueError(f"알 수 없는 마켓 심볼: {mid}")

    # 1) 현재가 조회
    ticker     = exchange.fetch_ticker(mid)
    mark_price = ticker["last"]
    qty        = round(amount_usdt / mark_price, 4)

    # 2) 5배 레버리지 고정
    exchange.set_leverage(5, mid)

    # 3) 시장가 진입
    side  = "buy" if order_type == "long" else "sell"
    order = exchange.create_order(
        symbol=mid,
        type="market",
        side=side,
        amount=qty,
    )
    print(f"✅ [{symbol}] {order_type.upper()} 체결 @ {mark_price} (qty={qty})")
    return mark_price
