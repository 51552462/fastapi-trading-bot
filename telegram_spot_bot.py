# telegram_spot_bot.py
import os, requests

TG_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")

def send_telegram(msg: str):
    if not TG_TOKEN or not TG_CHAT: 
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TG_CHAT, "text": msg})
    except Exception:
        pass
