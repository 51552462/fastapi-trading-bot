# trader.py

from bitget_api import place_market_order, close_all, get_last_price
from telegram_bot import send_telegram

# { "BTCUSDT_long": {...}, "BTCUSDT_short": {...} }
position_data = {}

def enter_position(symbol: str, usdt_amount: float, side: str="long"):
    key = f"{symbol}_{side}"
    resp = place_market_order(
        symbol, usdt_amount,
        side="buy" if side=="long" else "sell",
        leverage=5
    )
    if resp.get("code")=="00000":
        entry = get_last_price(symbol)
        position_data[key] = {
            "entry_price": entry,
            "exit_stage": 0,
            "usdt_amount": usdt_amount
        }
        send_telegram(f"🚀 Entry {side.upper()} {symbol} @ {entry:.6f}")
    else:
        send_telegram(f"❌ Entry 실패 {key}: {resp}")

def take_partial_profit(symbol: str, pct: float, side: str="long"):
    key = f"{symbol}_{side}"
    if key not in position_data:
        send_telegram(f"❌ TakeProfit 실패: {key} 없음")
        return
    data = position_data[key]
    qty_usdt = round(data["usdt_amount"] * pct, 6)
    close_side = "sell" if side=="long" else "buy"
    resp = place_market_order(symbol, qty_usdt, side=close_side, leverage=5)
    if resp.get("code")=="00000":
        data["usdt_amount"] -= qty_usdt
        data["exit_stage"] += 1
        send_telegram(f"🤑 TakeProfit{int(pct*100)} {side.upper()} {symbol}")
    else:
        send_telegram(f"❌ TakeProfit 실패 {key}: {resp}")
    # tp3 또는 3번 청산 후 강제 전체 종료
    if pct>=1.0 or data["exit_stage"]>=3:
        close_resp = close_all(symbol)
        send_telegram(f"📕 Position Closed {key} → {close_resp}")
        position_data.pop(key, None)

def close_position(symbol: str, side: str="long", reason:str=""):
    key = f"{symbol}_{side}"
    if key not in position_data:
        send_telegram(f"❌ Close 실패: {key} 없음")
        return
    resp = close_all(symbol)
    send_telegram(f"🛑 Close({reason}) {side.upper()} {symbol} → {resp}")
    position_data.pop(key, None)

def check_loss_and_exit():
    for key,info in list(position_data.items()):
        symbol,side = key.rsplit("_",1)
        entry=info["entry_price"]
        now = get_last_price(symbol)
        # -10% 롱
        if side=="long" and now<=entry*0.90:
            send_telegram(f"🚨 -10% SL LONG {symbol}")
            close_position(symbol, side, "stoploss")
        # -10% 숏
        if side=="short" and now>=entry*1.10:
            send_telegram(f"🚨 -10% SL SHORT {symbol}")
            close_position(symbol, side, "stoploss")
