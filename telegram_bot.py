import os, requests
from dotenv import load_dotenv

# Load .env if present (no error if absent)
try:
    load_dotenv()
except Exception:
    pass

# Accept multiple env names to avoid mis-config (TOKEN was often named TELEGRAM_BOT_TOKEN)
TOKEN = (
    os.getenv("TELEGRAM_TOKEN")
    or os.getenv("TELEGRAM_BOT_TOKEN")
    or os.getenv("TG_BOT_TOKEN")
    or ""
)
CHAT_ID = (
    os.getenv("TELEGRAM_CHAT_ID")
    or os.getenv("TELEGRAM_TO")
    or os.getenv("TG_CHAT_ID")
    or ""
)

BASE    = f"https://api.telegram.org/bot{TOKEN}"
_ANNOUNCED_OFF = False

def _announce_if_off():
    global _ANNOUNCED_OFF
    if not TOKEN or not CHAT_ID:
        if not _ANNOUNCED_OFF:
            print("[TG] disabled (missing TELEGRAM_TOKEN/TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID).")
            _ANNOUNCED_OFF = True
        return True
    return False

def send_telegram(text: str):
    """Plain text send (parse_mode not used). If disabled, logs to stdout."""
    if _announce_if_off():
        print("[TG]", text)
        return
    try:
        requests.post(f"{BASE}/sendMessage", data={"chat_id": CHAT_ID, "text": text}, timeout=10)
    except Exception as e:
        print("❌ Telegram send failed:", e)
