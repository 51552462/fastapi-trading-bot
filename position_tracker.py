from bitget_client import exchange

def close_position(symbol):
    try:
        market_id = symbol.upper()
        exchange.load_markets()
        pos = exchange.fetch_position(symbol=market_id)
        amt = float(pos["contracts"])

        if amt > 0:
            exchange.create_order(symbol=market_id, type="market", side="sell", amount=amt)
            print(f"🔻 close_position: {symbol} 포지션 종료")
        else:
            print(f"ℹ️ close_position: {symbol} 포지션 없음, 무시")
    except Exception as e:
        print("❌ close_position 에러:", e)


def close_partial(symbol, ratio):
    try:
        market_id = symbol.upper()
        exchange.load_markets()
        pos = exchange.fetch_position(symbol=market_id)
        amt = float(pos["contracts"])

        if amt > 0:
            close_amt = round(amt * ratio, 4)
            exchange.create_order(symbol=market_id, type="market", side="sell", amount=close_amt)
            print(f"💠 close_partial: {symbol} {ratio*100:.1f}% 청산 ({close_amt})")
        else:
            print(f"ℹ️ close_partial: {symbol} 포지션 없음, 무시")
    except Exception as e:
        print("❌ close_partial 에러:", e)
