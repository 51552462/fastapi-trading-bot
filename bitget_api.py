# bitget_api.py
import os, time, hmac, hashlib, base64, requests, json
from dotenv import load_dotenv

# 환경변수 로드
load_dotenv()

# Bitget API 기본 설정
BASE_URL = "https://api.bitget.com"
API_KEY = os.getenv("BITGET_API_KEY")
API_SECRET = os.getenv("BITGET_API_SECRET")
API_PASSPHRASE = os.getenv("BITGET_API_PASSWORD")

# 심볼 변환: TradingView 'BTCUSDT' → Bitget USDT-M 선물 'BTCUSDT_UMCBL'
def convert_symbol(symbol: str) -> str:
    return symbol.upper().replace("/", "").replace("_", "") + "_UMCBL"

# 타임스탬프 생성
def _timestamp():
    return str(int(time.time() * 1000))

# 요청 서명 생성
def _sign(method, path, ts, body=""):
    message = f"{ts}{method.upper()}{path}{body}"
    signature = hmac.new(API_SECRET.encode(), message.encode(), hashlib.sha256).digest()
    return base64.b64encode(signature).decode()

# 요청 헤더 생성
def _headers(method, path, body=""):
    ts = _timestamp()
    return {
        "ACCESS-KEY": API_KEY,
        "ACCESS-SIGN": _sign(method, path, ts, body),
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": API_PASSPHRASE,
        "Content-Type": "application/json"
    }

# 단일 모드 설정: 올바른 V1 엔드포인트 사용
# 공식 문서에 따르면 선물 포지션 모드는 /api/mix/v1/account/setPositionMode 이며
# posMode 값은 "single_side" 또는 "dual_side"
def set_one_way_mode():
    path = "/api/mix/v1/account/setPositionMode"
    url = BASE_URL + path
    body = {
        "posMode": "single_side"
    }
    body_json = json.dumps(body)
    headers = _headers("POST", path, body_json)
    res = requests.post(url, headers=headers, data=body_json)
    print("단일 모드 설정 응답:", res.json())
    return res.json()

# 시장가 주문: 금액(USDT) 기준
# side: "buy" 또는 "sell"
def place_market_order(symbol, usdt_amount, side, leverage=5):
    # 선물용 심볼 변환 및 단일 모드 보장
    sym = convert_symbol(symbol)
    set_one_way_mode()
    path = "/api/mix/v1/order/placeOrder"
    url = BASE_URL + path
    body = {
        "symbol": sym,
        "marginCoin": "USDT",
        "size": str(usdt_amount),  # USDT 금액
        "side": "open_long" if side=="buy" else "open_short",
        "orderType": "market",
        # holdMode 대신 공식 docs 기준 field 사용하지 않아도 되며
        "leverage": str(leverage)
    }
    body_json = json.dumps(body)
    headers = _headers("POST", path, body_json)
    return requests.post(url, headers=headers, data=body_json).json()

# 전량 청산
def close_all(symbol):
    sym = convert_symbol(symbol)
    path = "/api/mix/v1/order/close-position"
    url = BASE_URL + path
    body = {"symbol": sym, "marginCoin": "USDT"}
    body_json = json.dumps(body)
    headers = _headers("POST", path, body_json)
    return requests.post(url, headers=headers, data=body_json).json()

# 실시간 가격 조회
# USDT-M 선물 현재가(endOfCandle보다는 last)
def get_last_price(symbol):
    sym = convert_symbol(symbol)
    url = f"{BASE_URL}/api/mix/v1/market/ticker?symbol={sym}"
    return float(requests.get(url).json()["data"]["last"])


# trader.py
from bitget_api import place_market_order, close_all, get_last_price

# 메모리 내 포지션 상태 저장
position_data = {}

def enter_position(symbol, usdt_amount):
    resp = place_market_order(symbol, usdt_amount, side="buy", leverage=5)
    print(f"✅ 진입 주문 응답: {resp}")
    if resp.get("code") == "00000":
        entry_price = get_last_price(symbol)
        position_data[symbol] = {"entry_price": entry_price, "exit_stage": 0}
        return entry_price
    print(f"❌ 진입 실패: {resp}")
    return None

def take_partial_profit(symbol, pct=0.3):
    if symbol not in position_data:
        print(f"❌ 익절 실패: {symbol} 포지션 없음")
        return
    return close_all(symbol)

def stoploss(symbol):
    print(f"🛑 손절: {symbol}")
    close_all(symbol)
    position_data.pop(symbol, None)

def check_loss_and_exit():
    for symbol, info in list(position_data.items()):
        entry = info["entry_price"]
        now = get_last_price(symbol)
        if (now - entry) / entry <= -0.10:
            print(f"🚨 -10% 손실 감지: {symbol} {entry}→{now}")
            stoploss(symbol)

def reset_position(symbol):
    position_data.pop(symbol, None)

# main.py
import uvicorn, asyncio
from fastapi import FastAPI, Request
from trader import enter_position, take_partial_profit, stoploss, check_loss_and_exit

app = FastAPI()

@app.post("/signal")
async def receive_signal(request: Request):
    data = await request.json()
    print(f"📩 시그널 수신: {data}")
    try:
        t = data.get("type")
        s = data.get("symbol","").upper()
        a = float(data.get("amount",15))
        if t=="entry":
            price = enter_position(s,a)
            if price: return {"status":"ok","entry_price":price}
            return {"status":"error","detail":"order_failed"}
        if t in ["takeprofit1","takeprofit2","takeprofit3"]:
            pct = int(data.get("pct",33))/100
            take_partial_profit(s,pct)
            return {"status":"ok","event":t}
        if t in ["stoploss","liquidation"]:
            stoploss(s)
            return {"status":"ok","event":t}
        return {"status":"error","message":"Unknown signal type"}
    except Exception as e:
        print(f"❌ 예외 발생: {e}")
        return {"status":"error","detail":str(e)}

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(monitor())

async def monitor():
    while True:
        try: check_loss_and_exit()
        except Exception as e: print(f"❌ 감시 오류: {e}")
        await asyncio.sleep(5)

if __name__=="__main__":
    uvicorn.run(app, host="0.0.0.0", port=10000)

# .env
BITGET_API_KEY=your_actual_api_key
BITGET_API_SECRET=your_actual_api_secret
BITGET_API_PASSWORD=your_actual_api_passphrase

# requirements.txt
fastapi
uvicorn
python-dotenv
requests
