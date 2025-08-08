from bitget_api import place_market_order, close_all, get_last_price
from telegram_bot import send_telegram

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
    resp = place_market_order(
        symbol, qty_usdt,
        side="sell" if side=="long" else "buy",
        leverage=5
    )
    if resp.get("code") == "00000":
        data["usdt_amount"] -= qty_usdt
        data["exit_stage"] += 1
        send_telegram(f"🤑 TakeProfit{int(pct*100)} {side.upper()} {symbol}")
    else:
        send_telegram(f"❌ TakeProfit 실패 {key}: {resp}")

    if pct >= 1.0 or data["exit_stage"] >= 3:
        close_resp = close_all(symbol)
        if close_resp.get("code") == "00000":
            send_telegram(f"📕 Position Closed {key} → {close_resp}")
            position_data.pop(key, None)
        else:
            send_telegram(f"❌ Position 강제 종료 실패 {key} → {close_resp}")

def close_position(symbol: str, side: str="long", reason: str=""):
    key = f"{symbol}_{side}"
    print(f"🧪 [DEBUG] close_position 호출됨: {key}")
    if key not in position_data:
        print(f"❌ [DEBUG] position_data에 {key} 없음")
        send_telegram(f"❌ Close 실패: {key} 없음")
        return
    resp = close_all(symbol)
    if resp.get("code") == "00000":
        send_telegram(f"🚗 Close({reason}) {side.upper()} {symbol} → {resp}")
        position_data.pop(key, None)
    else:
        send_telegram(f"❌ Close 실패 ({reason}) {key}: {resp}")

def check_loss_and_exit():
    for key, info in list(position_data.items()):
        symbol, side = key.rsplit("_",1)
        entry = info["entry_price"]
        now   = get_last_price(symbol)

        if side=="long" and now <= entry*0.90:
            send_telegram(f"🚨 -10% SL LONG {symbol}")
            close_position(symbol, side, "stoploss")
        if side=="short" and now >= entry*1.10:
            send_telegram(f"🚨 -10% SL SHORT {symbol}")
            close_position(symbol, side, "stoploss")
