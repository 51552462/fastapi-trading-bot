import os, requests
from dotenv import load_dotenv

load_dotenv()
TOKEN   = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
BASE    = f"https://api.telegram.org/bot{TOKEN}"

def send_telegram(text: str):
    # parse_mode 없이 순수 텍스트 전송 → "can't parse entities" 방지
    payload = {"chat_id": CHAT_ID, "text": text}
    try:
        requests.post(f"{BASE}/sendMessage", data=payload, timeout=10)
    except Exception as e:
        print("❌ Telegram 전송 실패:", e)
