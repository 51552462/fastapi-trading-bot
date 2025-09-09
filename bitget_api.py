# -*- coding: utf-8 -*-
"""
bitget_api.py  (FULL, compat + get_open_positions added)

- v2 엔드포인트 우선, 실패 시 v1 폴백
- oneway/hedge, cross/isolated 지원 (ENV)
- side 매핑: long/buy -> buy_single, short/sell -> sell_single
- AMOUNT_MODE: 'notional'(기본) / 'margin'
- 심볼 매핑: 'DOGEUSDT' -> 'DOGEUSDT_UMCBL'
- reduceOnly 청산
- 안전 라운딩(step)
- ✅ 호환용 convert_symbol() 제공
- ✅ get_open_positions() 추가 (trader.py import 오류 해결)
"""
import os, time, json, hmac, hashlib, base64, requests
from typing import Dict, Any, Optional

API_KEY        = os.getenv("BITGET_API_KEY", "")
API_SECRET     = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE = os.getenv("BITGET_API_PASSPHRASE", "")

USE_V2         = os.getenv("BITGET_USE_V2", "1") == "1"
BASE_V2        = "https://api.bitget.com/api/v2"
BASE_V1        = "https://api.bitget.com/api/mix/v1"

MIX_SUFFIX     = os.getenv("BITGET_MIX_SUFFIX", "UMCBL")  # USDT 무기한
POSITION_MODE  = os.getenv("POSITION_MODE", "oneway").lower()   # oneway|hedge
MARGIN_MODE    = os.getenv("MARGIN_MODE", "isolated").lower()   # isolated|cross
AMOUNT_MODE    = os.getenv("AMOUNT_MODE", "notional").lower()   # notional|margin
LEVERAGE_DEFAULT = float(os.getenv("LEVERAGE_DEFAULT", "5"))
HTTP_TIMEOUT   = float(os.getenv("HTTP_TIMEOUT", "10"))

def _ts_ms() -> str:
    return str(int(time.time() * 1000))

def _sign_v2(ts: str, method: str, path_qs: str, body: str) -> str:
    prehash = f"{ts}{method.upper()}{path_qs}{body}"
    mac = hmac.new(API_SECRET.encode(), prehash.encode(), hashlib.sha256).digest()
    return base64.b64encode(mac).decode()

def _headers_v2(ts: str, sign: str) -> Dict[str,str]:
    return {
        "ACCESS-KEY": API_KEY,
        "ACCESS-PASSPHRASE": API_PASSPHRASE,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-SIGN": sign,
        "Content-Type": "application/json",
        "X-CHANNEL-API-CODE": "bitget.openapi"
    }

def _req_v2(method: str, path: str, params: Optional[Dict]=None, body: Optional[Dict]=None) -> Dict:
    from urllib.parse import urlencode
    url = BASE_V2 + path
    qs = "?" + urlencode(params, doseq=True) if params else ""
    if qs: url += qs
    payload = json.dumps(body or {}, separators=(",", ":"), ensure_ascii=False)
    ts = _ts_ms()
    sign = _sign_v2(ts, method, path + (qs or ""), payload if method.upper() != "GET" else "")
    r = requests.request(method.upper(), url, headers=_headers_v2(ts, sign),
                         data=(payload if method.upper() != "GET" else None),
                         timeout=HTTP_TIMEOUT)
    try: return r.json()
    except Exception: return {"code":"HTTP_"+str(r.status_code), "msg": r.text}

def _req_v1(method: str, path: str, params: Optional[Dict]=None, body: Optional[Dict]=None) -> Dict:
    from urllib.parse import urlencode
    url = BASE_V1 + path
    qs = "?" + urlencode(params, doseq=True) if params else ""
    if qs: url += qs
    payload = json.dumps(body or {}, separators=(",", ":"), ensure_ascii=False)
    ts = _ts_ms()
    prehash = f"{ts}{method.upper()}{path + (qs or '')}{payload if method.upper() != 'GET' else ''}"
    sign = base64.b64encode(hmac.new(API_SECRET.encode(), prehash.encode(), hashlib.sha256).digest()).decode()
    headers = {
        "ACCESS-KEY": API_KEY,
        "ACCESS-PASSPHRASE": API_PASSPHRASE,
        "ACCESS-TIMESTAMP": ts,
        "ACCESS-SIGN": sign,
        "Content-Type": "application/json",
    }
    r = requests.request(method.upper(), url, headers=headers,
                         data=(payload if method.upper() != "GET" else None),
                         timeout=HTTP_TIMEOUT)
    try: return r.json()
    except Exception: return {"code":"HTTP_"+str(r.status_code), "msg": r.text}

def _ok(res: Dict) -> bool:
    return str(res.get("code","")) in ("00000","0","success")

def round_down_step(x: float, step: float) -> float:
    if step <= 0: return x
    n = int(x/step)
    return float(n*step)

# ---------- symbol helpers (외부 공개) ----------
def _spot_symbol(symbol: str) -> str:
    return symbol.upper()

def _mix_symbol(symbol: str) -> str:
    s = symbol.upper()
    if s.endswith("_UMCBL") or s.endswith("_USDT") or s.endswith("_UMFUTURE"):
        return s
    return f"{s}_{MIX_SUFFIX}"

def convert_symbol(symbol: str, market: str = "mix") -> str:
    """예전 코드 호환용: market='mix'이면 컨트랙트 심볼 반환."""
    return _mix_symbol(symbol) if market.lower() in ("mix","futures","contract") else _spot_symbol(symbol)

# alias
to_contract = convert_symbol
to_spot     = _spot_symbol

# ---------- spec / ticker ----------
def get_symbol_spec(symbol: str) -> Dict[str, Any]:
    try:
        if USE_V2:
            res = _req_v2("GET", "/mix/market/contracts")
            if _ok(res):
                for it in res.get("data", []):
                    if it.get("symbol") == _mix_symbol(symbol):
                        size_scale = it.get("sizeScale")
                        size_step = 0.001
                        if size_scale is not None:
                            try: size_step = float(f"1e-{int(size_scale)}")
                            except Exception: pass
                        return {"sizeStep": float(size_step), "minSz": float(it.get("minSz", 0.001))}
    except Exception: pass
    try:
        res = _req_v1("GET", "/market/contracts")
        if _ok(res):
            for it in res.get("data", []):
                if it.get("symbol") == _mix_symbol(symbol):
                    step = float(it.get("sizeStep", 0.001))
                    return {"sizeStep": step, "minSz": float(it.get("minSz", 0.001))}
    except Exception: pass
    return {"sizeStep": 0.001, "minSz": 0.001}

def get_last_price(symbol: str) -> Optional[float]:
    try:
        if USE_V2:
            res = _req_v2("GET", "/contract/market/ticker", params={"symbol": _spot_symbol(symbol)})
            if _ok(res):
                data = res.get("data") or {}
                last = data.get("last") or data.get("close")
                if last is not None: return float(last)
    except Exception: pass
    try:
        res = _req_v1("GET", "/market/ticker", params={"symbol": _mix_symbol(symbol)})
        if _ok(res):
            data = res.get("data") or {}
            p = data.get("last") or data.get("close")
            if p is not None: return float(p)
    except Exception: pass
    return None

# ---------- account settings ----------
def set_leverage(symbol: str, leverage: float) -> Dict:
    lev = str(int(leverage))
    if USE_V2:
        body = {
            "symbol": _mix_symbol(symbol),
            "marginCoin": "USDT",
            "leverage": lev,
            "holdSide": "long_short" if POSITION_MODE == "hedge" else "net",
        }
        res = _req_v2("POST", "/mix/account/set-leverage", body=body)
        if _ok(res): return res
    body = {
        "symbol": _mix_symbol(symbol),
        "marginCoin": "USDT",
        "leverage": lev,
        "holdSide": "long_short" if POSITION_MODE == "hedge" else "net",
    }
    return _req_v1("POST", "/account/setLeverage", body=body)

def set_position_mode() -> Dict:
    mode = "long_short" if POSITION_MODE == "hedge" else "net"
    if USE_V2:
        body = {"productType": MIX_SUFFIX.lower(), "positionMode": mode}
        return _req_v2("POST", "/mix/account/set-position-mode", body=body)
    return {"code":"00000","msg":"success","data":{"positionMode":mode}}

def set_margin_mode(symbol: str) -> Dict:
    mm = "cross" if MARGIN_MODE == "cross" else "isolated"
    if USE_V2:
        body = {"symbol": _mix_symbol(symbol), "marginMode": mm}
        return _req_v2("POST", "/mix/account/set-margin-mode", body=body)
    return {"code":"00000","msg":"success","data":{"marginMode":mm}}

# ---------- positions ----------
def get_open_positions(symbol: str) -> Dict[str, Any]:
    """
    현재 열린 포지션 크기/평단 요약 반환.
    return 예:
    {
      "mode": "oneway"|"hedge",
      "long":  {"size": 0.0, "avgPrice": None},
      "short": {"size": 0.0, "avgPrice": None}
    }
    - oneway이면 sign>0 을 long, sign<0 을 short로 매핑
    """
    res_data = {"mode": POSITION_MODE, "long":{"size":0.0,"avgPrice":None}, "short":{"size":0.0,"avgPrice":None}}
    # v2 우선
    try:
        if USE_V2:
            res = _req_v2("GET", "/mix/position/single-position",
                          params={"symbol": _mix_symbol(symbol), "marginCoin": "USDT"})
            if _ok(res):
                data = res.get("data")
                if data:
                    # v2는 hedge시 리스트, oneway(net)일 수량 sign 로 들어올 수 있음
                    items = data if isinstance(data, list) else [data]
                    for it in items:
                        side = (it.get("holdSide") or it.get("positionSide") or "").lower()
                        sz   = float(it.get("total", it.get("positions", it.get("size", 0)) or 0))
                        avg  = it.get("avgOpenPrice") or it.get("avgPrice") or it.get("openAvgPrice")
                        if avg is not None:
                            try: avg = float(avg)
                            except: avg = None
                        if POSITION_MODE == "oneway":
                            # oneway에서는 side 정보가 없거나 'net'
                            # sign 으로 구분할 수 있는 필드 시도
                            qty = float(it.get("total", it.get("size", 0)) or 0)
                            if qty > 0:
                                res_data["long"]["size"] = qty
                                res_data["long"]["avgPrice"] = avg
                            elif qty < 0:
                                res_data["short"]["size"] = abs(qty)
                                res_data["short"]["avgPrice"] = avg
                        else:
                            if "long" in side:
                                res_data["long"]["size"] = float(sz)
                                res_data["long"]["avgPrice"] = avg
                            elif "short" in side:
                                res_data["short"]["size"] = float(sz)
                                res_data["short"]["avgPrice"] = avg
                return res_data
    except Exception:
        pass
    # v1 폴백
    try:
        res = _req_v1("GET", "/position/singlePosition",
                      params={"symbol": _mix_symbol(symbol), "marginCoin": "USDT"})
        if _ok(res):
            data = res.get("data")
            if data:
                items = data if isinstance(data, list) else [data]
                for it in items:
                    side = (it.get("holdSide") or it.get("positionSide") or "").lower()
                    sz   = float(it.get("total", it.get("positions", it.get("size", 0)) or 0))
                    avg  = it.get("avgOpenPrice") or it.get("avgPrice") or it.get("openAvgPrice")
                    if avg is not None:
                        try: avg = float(avg)
                        except: avg = None
                    if POSITION_MODE == "oneway":
                        qty = float(it.get("total", it.get("size", 0)) or 0)
                        if qty > 0:
                            res_data["long"]["size"] = qty;  res_data["long"]["avgPrice"] = avg
                        elif qty < 0:
                            res_data["short"]["size"] = abs(qty); res_data["short"]["avgPrice"] = avg
                    else:
                        if "long" in side:
                            res_data["long"]["size"] = float(sz);   res_data["long"]["avgPrice"] = avg
                        elif "short" in side:
                            res_data["short"]["size"] = float(sz);  res_data["short"]["avgPrice"] = avg
    except Exception:
        pass
    return res_data

# ---------- order/close ----------
def _resolve_side_tag(side: str) -> Optional[str]:
    s = (side or "").lower()
    if s in ("buy","long","open_long"):
        return "buy_single"
    if s in ("sell","short","open_short"):
        return "sell_single"
    return None

def _calc_qty(symbol: str, usdt_amount: float, leverage: float) -> float:
    last = get_last_price(symbol)
    if not last: return 0.0
    spec = get_symbol_spec(symbol)
    step = float(spec.get("sizeStep", 0.001))
    notional = float(usdt_amount) * (float(leverage) if AMOUNT_MODE == "margin" else 1.0)
    qty = notional / float(last)
    qty = round_down_step(qty, step)
    return max(qty, float(spec.get("minSz", 0.0)))

def place_market_order(symbol: str, usdt_amount: float, side: str,
                       leverage: float = None, reduce_only: bool = False) -> Dict:
    try:
        lv = leverage or LEVERAGE_DEFAULT
        set_position_mode()
        set_margin_mode(symbol)
        set_leverage(symbol, lv)

        side_tag = _resolve_side_tag(side)
        if not side_tag:
            return {"code":"LOCAL_BAD_SIDE","msg":f"unknown side {side}"}

        qty = _calc_qty(symbol, usdt_amount, lv)
        if qty <= 0:
            return {"code":"LOCAL_TICKER_FAIL","msg":"ticker_none or size<=0"}

        body = {
            "symbol":     _mix_symbol(symbol),
            "marginCoin": "USDT",
            "size":       str(qty),
            "side":       side_tag,
            "orderType":  "market",
            "leverage":   str(int(lv)),
            "reduceOnly": bool(reduce_only),
            "clientOid":  f"cli-{int(time.time()*1000)}"
        }
        if USE_V2:
            res = _req_v2("POST", "/mix/order/place-order", body=body)
            if _ok(res): return res
        return _req_v1("POST", "/order/placeOrder", body=body)
    except Exception as e:
        return {"code":"LOCAL_EXCEPTION","msg":str(e)}

def close_position(symbol: str, side: str, usdt_amount: float = None) -> Dict:
    try:
        amount = usdt_amount if usdt_amount and usdt_amount > 0 else 1e12  # 사실상 전량
        lv = LEVERAGE_DEFAULT
        qty = _calc_qty(symbol, amount, lv)
        side_tag = _resolve_side_tag(side)
        if not side_tag:
            r1 = place_market_order(symbol, amount, "long",  lv, reduce_only=True)
            r2 = place_market_order(symbol, amount, "short", lv, reduce_only=True)
            return {"code": "00000" if _ok(r1) or _ok(r2) else "LOCAL_CLOSE_FAIL",
                    "msg": "attempted both", "data": {"long": r1, "short": r2}}
        body = {
            "symbol":     _mix_symbol(symbol),
            "marginCoin": "USDT",
            "size":       str(qty),
            "side":       side_tag,
            "orderType":  "market",
            "leverage":   str(int(lv)),
            "reduceOnly": True,
            "clientOid":  f"close-{int(time.time()*1000)}"
        }
        if USE_V2:
            res = _req_v2("POST", "/mix/order/place-order", body=body)
            if _ok(res): return res
        return _req_v1("POST", "/order/placeOrder", body=body)
    except Exception as e:
        return {"code":"LOCAL_EXCEPTION","msg":str(e)}

# High level helpers
def open_long(symbol: str, usdt_amount: float, leverage: float = None) -> Dict:
    return place_market_order(symbol, usdt_amount, "long", leverage or LEVERAGE_DEFAULT, reduce_only=False)

def open_short(symbol: str, usdt_amount: float, leverage: float = None) -> Dict:
    return place_market_order(symbol, usdt_amount, "short", leverage or LEVERAGE_DEFAULT, reduce_only=False)

def close_long(symbol: str) -> Dict:
    return close_position(symbol, "long")

def close_short(symbol: str) -> Dict:
    return close_position(symbol, "short")

__all__ = [
    "convert_symbol","to_contract","to_spot",
    "get_last_price","get_symbol_spec","get_open_positions",
    "set_leverage","set_position_mode","set_margin_mode",
    "place_market_order","open_long","open_short","close_long","close_short"
]
