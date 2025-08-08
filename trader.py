from bitget_api import place_market_order, close_all, get_last_price
from telegram_bot import send_telegram

# { "BTCUSDT_long":{...}, "BTCUSDT_short":{...} }
position_data = {}

def enter_position(symbol: str, usdt_amount: float, side: str="long"):
    key = f"{symbol}_{side}"
    resp = place_market_order(
        symbol, usdt_amount,
        side="buy" if side=="long" else "sell",
        leverage=5
    )
    if resp.get("code") == "00000":
        entry = get_last_price(symbol)
        position_data[key] = {
            "entry_price": entry,
            "exit_stage": 0,
            "usdt_amount": usdt_amount
        }
        send_telegram(f"\ud83d\ude80 Entry {side.upper()} {symbol} @ {entry:.6f}")
    else:
        send_telegram(f"\u274c Entry \ud50c\ub9ac\uc2a4 {key}: {resp}")

def take_partial_profit(symbol: str, pct: float, side: str="long"):
    key = f"{symbol}_{side}"
    if key not in position_data:
        send_telegram(f"\u274c TakeProfit \ud50c\ub9ac\uc2a4: {key} \uc5c6\uc74c")
        return
    data = position_data[key]
    qty_usdt = round(data["usdt_amount"] * pct, 6)
    resp = place_market_order(
        symbol, qty_usdt,
        side="sell" if side=="long" else "buy",
        leverage=5
    )
    if resp.get("code") == "00000":
        data["usdt_amount"] -= qty_usdt
        data["exit_stage"] += 1
        send_telegram(f"\ud83e\udd11 TakeProfit{int(pct*100)} {side.upper()} {symbol}")
    else:
        send_telegram(f"\u274c TakeProfit \ud50c\ub9ac\uc2a4 {key}: {resp}")

    # tp3 \ub610\ub294 \uc0bc \ubc88\uc9f8 \ucc3d\uc0b0 \ud6c4 \uac15\uc81c \uc804\uccb4 \uc885\ub8cc
    if pct >= 1.0 or data["exit_stage"] >= 3:
        close_resp = close_all(symbol)
        if close_resp.get("code") == "00000":
            send_telegram(f"\ud83d\udcd5 Position Closed {key} \u2192 {close_resp}")
            position_data.pop(key, None)
        else:
            send_telegram(f"\u274c Position \uac15\uc81c \uc885\ub8cc \uc2e4\ud328 {key} \u2192 {close_resp}")

def close_position(symbol: str, side: str="long", reason: str=""):
    key = f"{symbol}_{side}"
    if key not in position_data:
        send_telegram(f"\u274c Close \ud50c\ub9ac\uc2a4: {key} \uc5c6\uc74c")
        return
    resp = close_all(symbol)
    if resp.get("code") == "00000":
        send_telegram(f"\ud83d\ude97 Close({reason}) {side.upper()} {symbol} \u2192 {resp}")
        position_data.pop(key, None)
    else:
        send_telegram(f"\u274c Close \uc2e4\ud328 ({reason}) {key}: {resp}")

def check_loss_and_exit():
    for key, info in list(position_data.items()):
        symbol, side = key.rsplit("_",1)
        entry = info["entry_price"]
        now   = get_last_price(symbol)

        if side=="long" and now <= entry*0.90:
            send_telegram(f"\ud83d\udea8 -10% SL LONG {symbol}")
            close_position(symbol, side, "stoploss")
        if side=="short" and now >= entry*1.10:
            send_telegram(f"\ud83d\udea8 -10% SL SHORT {symbol}")
            close_position(symbol, side, "stoploss")
