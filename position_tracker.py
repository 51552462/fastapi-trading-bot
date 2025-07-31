# position_tracker.py
position_info = {}

def set_entry(symbol, entry_price):
    position_info[symbol] = {
        "entry_price": entry_price,
        "exit_stage": 0
    }

def update_exit_stage(symbol):
    if symbol in position_info:
        position_info[symbol]["exit_stage"] += 1

def reset_position(symbol):
    if symbol in position_info:
        del position_info[symbol]

def get_entry_price(symbol):
    return position_info.get(symbol, {}).get("entry_price")

def get_all_positions():
    return position_info.copy()
