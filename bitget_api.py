# -*- coding: utf-8 -*-
"""
bitget_api.py  (FULL)

- v2 우선, 실패 시 v1 폴백
- oneway/hedge, cross/isolated 지원(ENV)
- 금액 해석: 기본 'margin' (증거금 기준) ← 100USDT 넣으면 5배에서 명목가 500
- 심볼 매핑: 'DOGEUSDT' -> 'DOGEUSDT_UMCBL'
- 사이드 매핑(원웨이 기준):
    open long  -> buy_single
    close long -> sell_single (reduceOnly)
    open short -> sell_single
    close short-> buy_single (reduceOnly)
- 노출 함수(호환): convert_symbol, get_last_price, get_symbol_spec, get_open_positions,
                  place_market_order, open_long/open_short, close_long/close_short,
                  place_reduce_by_size  ← trader.py 가 임포트
"""
import os, time, json, hmac, hashlib, base64, requests
from typing import Dict, Any, Optional

# -------- ENV --------
API_KEY        = os.getenv("BITGET_API_KEY", "")
API_SECRET     = os.getenv("BITGET_API_SECRET", "")
API_PASSPHRASE = os.getenv("BITGET_API_PASSPHRASE", "")

USE_V2         = os.getenv("BITGET_USE_V2", "1") == "1"
BASE_V2        = "https://api.bitget.com/api/v2"
BASE_V1        = "https://api.bitget.com/api/mix/v1"

MIX_SUFFIX     = os.getenv("BITGET_MIX_SUFFIX", "UMCBL")      # USDT 무기한
POSITION_MODE  = os.getenv("POSITION_MODE", "oneway").lower() # oneway|hedge
MARGIN_MODE    = os.getenv("MARGIN_MODE", "isolated").lower() # isolated|cross
AMOUNT_MODE    = os.getenv("AMOUNT_MODE", "margin").lower()   # margin|notional  ← 기본 margin
LEVERAGE_DEFAULT = float(os.getenv("LEVERAGE_DEFAULT", "5"))
HTTP_TIMEOUT   = float(os.getenv("HTTP_TIMEOUT", "10"))

# -------- 공통 유틸 --------
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
    return float(int(x/step) * step)

# -------- 심볼/스펙 --------
def _spot_symbol(symbol: str) -> str:
    return symbol.upper()

def _mix_symbol(symbol: str) -> str:
    s = symbol.upper()
    return s if s.endswith("_UMCBL") else f"{s}_{MIX_SUFFIX}"

def convert_symbol(symbol: str, market: str = "mix") -> str:
    return _mix_symbol(symbol) if market.lower() in ("mix","futures","contract") else _spot_symbol(symbol)

def get_symbol_spec(symbol: str) -> Dict[str, Any]:
    # v2
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
                            except: pass
                        return {"sizeStep": float(size_step), "minSz": float(it.get("minSz", 0.001))}
    except Exception: pass
    # v1 폴백
    try:
        res = _req_v1("GET", "/market/contracts")
        if _ok(res):
            for it in res.get("data", []):
                if it.get("symbol") == _mix_symbol(symbol):
                    return {"sizeStep": float(it.get("sizeStep", 0.001)),
                            "minSz": float(it.get("minSz", 0.001))}
    except Exception: pass
    return {"sizeStep": 0.001, "minSz": 0.001}

def get_last_price(symbol: str) -> Optional[float]:
    # v2: /contract/market/ticker (심볼 = 'DOGEUSDT')
    try:
        if USE_V2:
            res = _req_v2("GET", "/contract/market/ticker", params={"symbol": _spot_symbol(symbol)})
            if _ok(res):
                data = res.get("data") or {}
                p = data.get("last") or data.get("close")
                if p is not None: return float(p)
    except Exception: pass
    # v1
    try:
        res = _req_v1("GET", "/market/ticker", params={"symbol": _mix_symbol(symbol)})
        if _ok(res):
            data = res.get("data") or {}
            p = data.get("last") or data.get("close")
            if p is not None: return float(p)
    except Exception: pass
    return None

# -------- 계정/설정 --------
def set_position_mode() -> Dict:
    mode = "long_short" if POSITION_MODE == "hedge" else "net"
    if USE_V2:
        return _req_v2("POST", "/mix/account/set-position-mode",
                       body={"productType": MIX_SUFFIX.lower(), "positionMode": mode})
    return {"code":"00000","msg":"success","data":{"positionMode":mode}}

def set_margin_mode(symbol: str) -> Dict:
    mm = "cross" if MARGIN_MODE == "cross" else "isolated"
    if USE_V2:
        return _req_v2("POST", "/mix/account/set-margin-mode",
                       body={"symbol": _mix_symbol(symbol), "marginMode": mm})
    return {"code":"00000","msg":"success","data":{"marginMode":mm}}

def set_leverage(symbol: str, leverage: float) -> Dict:
    lev = str(int(leverage))
    body = {
        "symbol": _mix_symbol(symbol),
        "marginCoin": "USDT",
        "leverage": lev,
        "holdSide": "long_short" if POSITION_MODE == "hedge" else "net",
    }
    if USE_V2:
        res = _req_v2("POST", "/mix/account/set-leverage", body=body)
        if _ok(res): return res
    return _req_v1("POST", "/account/setLeverage", body=body)

# -------- 포지션 조회 --------
def get_open_positions(symbol: str) -> Dict[str, Any]:
    """
    return:
    {
      "mode": "oneway"|"hedge",
      "long":  {"size": float, "avgPrice": float|None},
      "short": {"size": float, "avgPrice": float|None}
    }
    """
    out = {"mode": POSITION_MODE, "long":{"size":0.0,"avgPrice":None}, "short":{"size":0.0,"avgPrice":None}}
    # v2
    try:
        if USE_V2:
            res = _req_v2("GET", "/mix/position/single-position",
                          params={"symbol": _mix_symbol(symbol), "marginCoin": "USDT"})
            if _ok(res):
                data = res.get("data")
                rows = data if isinstance(data, list) else ([data] if data else [])
                for it in rows:
                    # oneway인 경우 qty 부호로 구분될 수 있음
                    qty = float(it.get("total", it.get("size", 0)) or 0)
                    side = (it.get("holdSide") or it.get("positionSide") or "").lower()
                    avg  = it.get("avgOpenPrice") or it.get("avgPrice") or it.get("openAvgPrice")
                    avg  = float(avg) if avg is not None else None
                    if POSITION_MODE == "oneway":
                        if qty > 0:
                            out["long"]["size"] = qty;  out["long"]["avgPrice"] = avg
                        elif qty < 0:
                            out["short"]["size"] = abs(qty); out["short"]["avgPrice"] = avg
                    else:
                        if "long" in side:
                            out["long"]["size"] = abs(qty); out["long"]["avgPrice"] = avg
                        elif "short" in side:
                            out["short"]["size"] = abs(qty); out["short"]["avgPrice"] = avg
                return out
    except Exception:
        pass
    # v1 폴백
    try:
        res = _req_v1("GET", "/position/singlePosition",
                      params={"symbol": _mix_symbol(symbol), "marginCoin": "USDT"})
        if _ok(res):
            data = res.get("data")
            rows = data if isinstance(data, list) else ([data] if data else [])
            for it in rows:
                qty = float(it.get("total", it.get("size", 0)) or 0)
                side = (it.get("holdSide") or it.get("positionSide") or "").lower()
                avg  = it.get("avgOpenPrice") or it.get("avgPrice") or it.get("openAvgPrice")
                avg  = float(avg) if avg is not None else None
                if POSITION_MODE == "oneway":
                    if qty > 0:
                        out["long"]["size"] = qty;  out["long"]["avgPrice"] = avg
                    elif qty < 0:
                        out["short"]["size"] = abs(qty); out["short"]["avgPrice"] = avg
                else:
                    if "long" in side:
                        out["long"]["size"] = abs(qty); out["long"]["avgPrice"] = avg
                    elif "short" in side:
                        out["short"]["size"] = abs(qty); out["short"]["avgPrice"] = avg
    except Exception:
        pass
    return out

# -------- 주문 기본 --------
def _order_side_open(side: str) -> Optional[str]:
    s = (side or "").lower()
    if s in ("buy","long","open_long"):  return "buy_single"
    if s in ("sell","short","open_short"): return "sell_single"
    return None

def _order_side_close_of(side_position: str) -> Optional[str]:
    # 해당 포지션을 '줄이는' 주문 사이드
    s = (side_position or "").lower()
    if s in ("long","buy"):  return "sell_single"  # 롱 줄일 땐 sell
    if s in ("short","sell"): return "buy_single"  # 숏 줄일 땐 buy
    return None

def _calc_qty(symbol: str, amount_usdt: float, leverage: float) -> float:
    """
    amount_usdt 해석:
      - AMOUNT_MODE=margin  : 증거금 기준 → 명목가 = amount * leverage
      - AMOUNT_MODE=notional: 명목가 기준
    """
    last = get_last_price(symbol)
    if not last: return 0.0
    spec = get_symbol_spec(symbol)
    step = float(spec.get("sizeStep", 0.001))
    notional = float(amount_usdt) * (float(leverage) if AMOUNT_MODE == "margin" else 1.0)
    qty = notional / float(last)
    qty = round_down_step(qty, step)
    return max(qty, float(spec.get("minSz", 0.0)))

def _place_market(symbol: str, side_tag: str, qty: float, leverage: float, reduce_only: bool) -> Dict:
    body = {
        "symbol":     _mix_symbol(symbol),
        "marginCoin": "USDT",
        "size":       str(qty),
        "side":       side_tag,
        "orderType":  "market",
        "leverage":   str(int(leverage)),
        "reduceOnly": bool(reduce_only),
        "clientOid":  f"cli-{int(time.time()*1000)}"
    }
    if USE_V2:
        res = _req_v2("POST", "/mix/order/place-order", body=body)
        if _ok(res): return res
    return _req_v1("POST", "/order/placeOrder", body=body)

# -------- 외부 노출: 주문 --------
def place_market_order(symbol: str, usdt_amount: float, side: str,
                       leverage: float = None, reduce_only: bool = False) -> Dict:
    try:
        lv = leverage or LEVERAGE_DEFAULT
        set_position_mode()
        set_margin_mode(symbol)
        set_leverage(symbol, lv)

        qty = _calc_qty(symbol, usdt_amount, lv)
        if qty <= 0:
            return {"code":"LOCAL_TICKER_FAIL","msg":"ticker_none or size<=0"}

        # open/close에 맞는 side_tag 선택
        side_tag = _order_side_open(side) if not reduce_only else _order_side_close_of(side)
        if not side_tag:
            return {"code":"LOCAL_BAD_SIDE","msg":f"unknown side {side}"}

        return _place_market(symbol, side_tag, qty, lv, reduce_only)
    except Exception as e:
        return {"code":"LOCAL_EXCEPTION","msg":str(e)}

def open_long(symbol: str, usdt_amount: float, leverage: float = None) -> Dict:
    return place_market_order(symbol, usdt_amount, "long", leverage or LEVERAGE_DEFAULT, reduce_only=False)

def open_short(symbol: str, usdt_amount: float, leverage: float = None) -> Dict:
    return place_market_order(symbol, usdt_amount, "short", leverage or LEVERAGE_DEFAULT, reduce_only=False)

def close_long(symbol: str, usdt_amount: float = None) -> Dict:
    # usdt_amount 미지정 시 충분히 큰 값으로 '전량' 근사
    amt = usdt_amount if usdt_amount and usdt_amount > 0 else 1e12
    return place_market_order(symbol, amt, "long", LEVERAGE_DEFAULT, reduce_only=True)

def close_short(symbol: str, usdt_amount: float = None) -> Dict:
    amt = usdt_amount if usdt_amount and usdt_amount > 0 else 1e12
    return place_market_order(symbol, amt, "short", LEVERAGE_DEFAULT, reduce_only=True)

def place_reduce_by_size(symbol: str, side_position: str, size: float,
                         leverage: float = None) -> Dict:
    """
    ✅ trader.py 호환용: '해당 포지션을 size 수량만큼 줄이는' 마켓주문
       side_position: 줄일 포지션 방향('long' or 'short')
       size: 계약수량(코인 수량) 기준
    """
    try:
        lv = leverage or LEVERAGE_DEFAULT
        set_position_mode()
        set_margin_mode(symbol)
        set_leverage(symbol, lv)

        side_tag = _order_side_close_of(side_position)
        if not side_tag:
            return {"code":"LOCAL_BAD_SIDE","msg":f"unknown side {side_position}"}

        # 스텝 반올림
        step = float(get_symbol_spec(symbol).get("sizeStep", 0.001))
        qty  = round_down_step(float(size), step)
        if qty <= 0:
            return {"code":"LOCAL_SIZE_BAD","msg":"size<=0"}

        return _place_market(symbol, side_tag, qty, lv, True)
    except Exception as e:
        return {"code":"LOCAL_EXCEPTION","msg":str(e)}

__all__ = [
    # symbol/price/spec
    "convert_symbol","get_last_price","get_symbol_spec","get_open_positions",
    # orders
    "place_market_order","open_long","open_short","close_long","close_short",
    # partial reduce (for trader.py)
    "place_reduce_by_size",
]
