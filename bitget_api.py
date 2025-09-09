# bitget_api.py — Bitget ONE-WAY 전용 간단 래퍼 (v2 우선, v1 폴백)
import os, time, hmac, hashlib, json, math
from typing import Optional, Dict, Any, List
from urllib.parse import urlencode
import requests

BASE = os.getenv("BITGET_BASE", "https://api.bitget.com")
AK   = os.getenv("BITGET_API_KEY", "")
AS   = os.getenv("BITGET_API_SECRET", "")
AP   = os.getenv("BITGET_API_PASS", "")
TIMEOUT = 8
USE_V2  = os.getenv("BITGET_USE_V2", "1") == "1"

# 선물 마켓 기본값(USDT-M Perp)
PRODUCT = os.getenv("BITGET_PRODUCT", "umcbl")   # umcbl
MARGIN_COIN = os.getenv("BITGET_MARGIN_COIN", "USDT")
POSITION_MODE = os.getenv("BITGET_POSITION_MODE", "oneway").lower()  # oneway 고정

# -------- 공통 유틸 --------
_sess = requests.Session()
_specs_cache: Dict[str, Dict[str, Any]] = {}
_price_cache: Dict[str, float] = {}
_contracts_cache: float = 0.0

def _ts_ms() -> str:
    return str(int(time.time() * 1000))

def _sign(ts: str, method: str, path: str, body: str = "") -> Dict[str, str]:
    if not (AK and AS and AP):
        return {}
    pre = ts + method.upper() + path + body
    sign = hmac.new(AS.encode(), pre.encode(), hashlib.sha256).digest()
    sign_b64 = sign.hex() if os.getenv("BITGET_SIGN_HEX", "0") == "1" else \
        hmac.new(AS.encode(), pre.encode(), hashlib.sha256).digest()
    # 공식은 base64 이지만 서버 쪽 라이브러리들이 알아서 처리. 여기선 간단화
    import base64
    sign_b64 = base64.b64encode(hmac.new(AS.encode(), pre.encode(), hashlib.sha256).digest()).decode()
    return {
        "ACCESS-KEY": AK,
        "ACCESS-SIGN": sign_b64,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-PASSPHRASE": AP,
        "Content-Type": "application/json"
    }

def _req(method: str, path: str, params: Dict[str, Any] = None, body: Dict[str, Any] = None, auth: bool = False):
    url = BASE + path
    params = params or {}
    body = body or {}
    headers = {}
    data = ""
    if method.upper() == "GET":
        if params:
            url += "?" + urlencode(params)
    else:
        data = json.dumps(body, separators=(",", ":"))
    if auth:
        headers.update(_sign(_ts_ms(), method, path + (("?" + urlencode(params)) if (method.upper()=="GET" and params) else ""), data))
    r = _sess.request(method.upper(), url, headers=headers, data=data if method.upper()!="GET" else None, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

# 심볼 변환: 내부는 'DOGEUSDT', API는 'DOGEUSDT_UMCBL'
def convert_symbol(sym: str) -> str:
    s = (sym or "").replace("_UMCBL", "").replace("-UMCBL", "").replace(":USDT", "USDT").upper()
    if s.endswith("USDT"): 
        return s
    # 트뷰가 'BTCUSDTPERP' 같은 걸 보내더라도 기본 변환
    if s.endswith("USDTPERP"): 
        return s[:-7]
    return s

def _to_api_symbol(core: str) -> str:
    c = convert_symbol(core)
    return f"{c}_UMCBL"

# 틱러
def get_last_price(core: str) -> float:
    sym = _to_api_symbol(core)
    try:
        if USE_V2:
            j = _req("GET", "/api/v2/mix/market/ticker", {"symbol": sym})
            px = float(j.get("data", {}).get("last") or 0)
        else:
            j = _req("GET", "/api/mix/v1/market/ticker", {"symbol": sym})
            px = float(j.get("data", {}).get("last") or 0)
        if px > 0: _price_cache[core] = px
        return px if px > 0 else float(_price_cache.get(core, 0.0))
    except Exception:
        return float(_price_cache.get(core, 0.0))

# 컨트랙트 스펙
def _load_contracts():
    global _contracts_cache
    if time.time() - _contracts_cache < 300:   # 5분 캐시
        return
    path = "/api/v2/mix/market/contracts" if USE_V2 else "/api/mix/v1/market/contracts"
    j = _req("GET", path, {"productType": PRODUCT})
    datas = j.get("data", []) or []
    _specs_cache.clear()
    for d in datas:
        core = convert_symbol(d.get("symbol",""))
        size_step = float(d.get("sizeTick" if USE_V2 else "lotSize", 0.001))
        price_step = float(d.get("priceTick" if USE_V2 else "pricePlace", 0.001))
        min_size = float(d.get("minOrderSize", 0.0) or 0.0)
        _specs_cache[core] = {
            "sizeStep": size_step,
            "priceStep": price_step,
            "minSize": min_size if min_size > 0 else size_step
        }
    _contracts_cache = time.time()

def get_symbol_spec(core: str) -> Dict[str, Any]:
    _load_contracts()
    return _specs_cache.get(convert_symbol(core), {"sizeStep": 0.001, "priceStep": 0.001, "minSize": 0.001})

def round_down_step(x: float, step: float) -> float:
    if step <= 0: return float(x)
    return math.floor(float(x) / step) * step

# 포지션 조회(심볼 없으면 전체) — trader가 심볼 없이 호출해도 안전
def get_open_positions(symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    if symbol:
        syms = [convert_symbol(symbol)]
    else:
        # 전체 포지션
        path = "/api/v2/mix/position/all-position" if USE_V2 else "/api/mix/v1/position/allPosition"
        j = _req("GET", path, {"productType": PRODUCT}, auth=True)
        res = []
        for it in j.get("data", []) or []:
            core = convert_symbol(it.get("symbol",""))
            sz   = float(it.get("total", 0) or it.get("available", 0) or it.get("size", 0))
            entry= float(it.get("avgOpenPrice") or it.get("openPrice") or 0.0)
            # v2는 long/short 구분 대신 holdMode/side. oneway면 side가 'buy'(롱) or 'sell'(숏)
            sd = (it.get("holdSide") or it.get("side") or "").lower()
            side = "long" if sd in ("buy","long") else ("short" if sd in ("sell","short") else "")
            if core and side and sz>0:
                res.append({"symbol": core, "side": side, "size": sz, "entryPrice": entry})
        return res

    # 단일 심볼
    res = []
    try:
        path = "/api/v2/mix/position/single-position" if USE_V2 else "/api/mix/v1/position/singlePosition"
        j = _req("GET", path, {"symbol": _to_api_symbol(syms[0]), "marginCoin": MARGIN_COIN}, auth=True)
        datas = j.get("data", []) or []
        if isinstance(datas, dict): datas = [datas]
        for it in datas:
            core = convert_symbol(it.get("symbol",""))
            sz   = float(it.get("total", 0) or it.get("available", 0) or it.get("size", 0))
            entry= float(it.get("avgOpenPrice") or it.get("openPrice") or 0.0)
            sd   = (it.get("holdSide") or it.get("side") or "").lower()
            side = "long" if sd in ("buy","long") else ("short" if sd in ("sell","short") else "")
            if core and side and sz>0:
                res.append({"symbol": core, "side": side, "size": sz, "entryPrice": entry})
    except Exception:
        pass
    return res

# 마켓 진입 — amount: USDT 명목가
def place_market_order(core: str, amount_usdt: float, side: str, leverage: float) -> Dict[str, Any]:
    price = float(get_last_price(core) or 0.0)
    if price <= 0:
        return {"code":"LOCAL_TICKER_FAIL", "msg":"ticker_none"}
    spec = get_symbol_spec(core)
    size = round_down_step(float(amount_usdt) / price, float(spec.get("sizeStep", 0.001)))
    if size <= 0:
        return {"code":"LOCAL_TICKER_FAIL", "msg":"ticker_none or size<=0"}

    # oneway: 롱=buy, 숏=sell
    side_api = "buy" if side.lower()=="long" else "sell"

    body = {
        "symbol": _to_api_symbol(core),
        "marginCoin": MARGIN_COIN,
        "size": str(size),
        "side": side_api,
        "orderType": "market",
        "force": "gtc",
        "leverage": str(int(leverage) if leverage>=1 else "1"),
        "reduceOnly": False
    }
    path = "/api/v2/mix/order/place-order" if USE_V2 else "/api/mix/v1/order/placeOrder"
    try:
        j = _req("POST", path, body=body, auth=True)
        code = str(j.get("code") or j.get("status") or "00000")
        if code != "00000" and str(code) != "success":
            return {"code": code, "msg": j}
        return {"code":"00000", "data": j.get("data")}
    except Exception as e:
        return {"code":"HTTP_400", "msg": str(e)}

# 사이즈로 줄이기(분할/청산 공용). side는 기존 포지션의 방향(롱/숏).
def place_reduce_by_size(core: str, contracts: float, side: str) -> Dict[str, Any]:
    if contracts <= 0:
        return {"code":"LOCAL_FAIL", "msg":"bad_contracts"}
    # oneway: 롱을 줄일 땐 sell, 숏을 줄일 땐 buy
    side_api = "sell" if side.lower()=="long" else "buy"
    body = {
        "symbol": _to_api_symbol(core),
        "marginCoin": MARGIN_COIN,
        "size": str(contracts),
        "side": side_api,
        "orderType": "market",
        "force": "gtc",
        "reduceOnly": True
    }
    path = "/api/v2/mix/order/place-order" if USE_V2 else "/api/mix/v1/order/placeOrder"
    try:
        j = _req("POST", path, body=body, auth=True)
        code = str(j.get("code") or j.get("status") or "00000")
        if code != "00000" and str(code) != "success":
            return {"code": code, "msg": j}
        return {"code":"00000", "data": j.get("data")}
    except Exception as e:
        return {"code":"HTTP_400", "msg": str(e)}

# 디버그용
def symbol_exists(core: str) -> bool:
    try:
        return get_last_price(core) > 0
    except Exception:
        return False
