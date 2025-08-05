# telegram_bot.py

import requests

TOKEN   = "7529734185:AAH9ayhwVTdm6qoxmPhmIsQMJthemi2l4I8"
CHAT_ID = "6838834566"
BASE_URL = f"https://api.telegram.org/bot{TOKEN}"

def send_telegram(message: str):
    """
    텔레그램으로 메시지 전송
    parse_mode=Markdown 으로 간단한 서식 지원
    """
    url = f"{BASE_URL}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        res = requests.post(url, json=payload, timeout=5)
        if not res.ok:
            print(f"❌ 텔레그램 전송 실패: {res.status_code}, {res.text}")
    except Exception as e:
        print(f"❌ 텔레그램 예외 발생: {e}")
