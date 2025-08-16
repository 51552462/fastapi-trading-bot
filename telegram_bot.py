import os, requests
from dotenv import load_dotenv

load_dotenv()
TOKEN   = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
BASE    = f"https://api.telegram.org/bot{TOKEN}"

def send_telegram(text: str):
    """Plain text 전송 (parse_mode 미사용)"""
    if not TOKEN or not CHAT_ID:
        print("[TG]", text)
        return
    try:
        requests.post(f"{BASE}/sendMessage", data={"chat_id": CHAT_ID, "text": text}, timeout=10)
    except Exception as e:
        print("❌ Telegram 전송 실패:", e)
