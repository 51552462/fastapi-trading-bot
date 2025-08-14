from typing import Dict, Optional
from bitget_api import (
    convert_symbol, get_last_price, get_open_positions,
    place_market_order, place_reduce_by_size, get_symbol_spec, round_down_step
)
import json, os, time, threading, random

try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str): print("[TG]", msg)

LEVERAGE   = float(os.getenv("LEVERAGE", "5"))
STOP_ROE   = float(os.getenv("STOP_ROE", "-0.10"))
STATS_FILE = os.getenv("TRADE_STATS_FILE", "trade_stats.json")

# 상태 + 락
position_data: Dict[str, dict] = {}
_POS_LOCK = threading.RLock()

# ── KST & 통계 ─────────────────────────────────────────────────────────────
def _now_kst_epoch() -> int: return int(time.time()) + 9*3600
def _today_kst_str() -> str: return time.strftime("%Y-%m-%d", time.gmtime(_now_kst_epoch()))
def _load_stats() -> dict:
    if os.path.exists(STATS_FILE):
        try: return json.load(open(STATS_FILE, "r", encoding="utf-8"))
        except Exception: pass
    return {}
def _save_stats(stats: dict):
    try: json.dump(stats, open(STATS_FILE, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    except Exception as e: print("stats save error:", e)

STATS = _load_stats()
def _ensure_symbol(dayrec: dict, sym: str) -> dict:
    if sym not in dayrec: dayrec[sym] = {"trades":0,"wins":0,"losses":0,"pnl":0.0}
    return dayrec[sym]
def record_entry(sym: str):
    d=_today_kst_str(); STATS.setdefault(d,{}); _ensure_symbol(STATS[d],sym)["trades"]+=1; _save_stats(STATS)
def record_pnl(sym: str, pnl: float):
    d=_today_kst_str(); STATS.setdefault(d,{}); rec=_ensure_symbol(STATS[d],sym); rec["pnl"]=float(rec.get("pnl",0.0))+float(pnl); _save_stats(STATS)
def record_result(sym: str, total: float):
    d=_today_kst_str(); STATS.setdefault(d,{}); rec=_ensure_symbol(STATS[d],sym)
    if total>0: rec["wins"]+=1
    elif total<0: rec["losses"]+=1
    _save_stats(STATS)

# ── 유틸 ──────────────────────────────────────────────────────────────────
def _pnl_usdt(entry, exit, portion_usdt, side):
    return portion_usdt * ((exit - entry) / entry) if side=="long" else portion_usdt * ((entry - exit) / entry)

def _normalize_local_keys():
    with _POS_LOCK:
        for key in list(position_data.keys()):
            try: sym, side = key.rsplit("_", 1)
            except ValueError: continue
            new_key = f"{convert_symbol(sym)}_{side}"
            if new_key != key:
                position_data[new_key] = position_data.pop(key)
                send_telegram(f"🧹 LOCAL KEY 정규화: {key} → {new_key}")

def _get_remote(symbol: str, side: Optional[str]=None):
    for p in get_open_positions():
        if p["symbol"] == symbol and (side is None or p["side"] == side):
            return p
    return None

# ── 주문 실행 ─────────────────────────────────────────────────────────────
def enter_position(symbol: str, usdt_amount: float, side: str="long"):
    symbol = convert_symbol(symbol)
    key = f"{symbol}_{side}"

    # 티커 & 심볼 스펙
    last = get_last_price(symbol)
    if last is None:
        send_telegram(f"❌ ENTRY 실패 {key}: 티커 조회 실패")
        return
    spec = get_symbol_spec(symbol)
    min_qty = float(spec.get("min_qty", 0))
    step    = float(spec.get("step", 0))

    # 3회 재시도(지수 백오프)
    for attempt in range(3):
        qty = round_down_step(usdt_amount / last, step)
        if min_qty and qty < min_qty:
            need = min_qty * last
            send_telegram(
                f"❌ ENTRY 실패 {key}: 최소수량 미달\n"
                f"• price≈{last:.8f}, qty={qty} < min={min_qty}\n"
                f"• 현재 {usdt_amount:.2f} USDT → 최소 필요≈{need:.2f} USDT"
            )
            return

        resp = place_market_order(symbol, usdt_amount,
                                  side="buy" if side=="long" else "sell",
                                  leverage=LEVERAGE, reduce_only=False)
        if resp.get("code") == "00000":
            entry = get_last_price(symbol) or last
            with _POS_LOCK:
                position_data[key]={"entry_price":entry,"usdt_opened":usdt_amount,"usdt_remaining":usdt_amount,"realized_pnl":0.0}
            record_entry(symbol)
            send_telegram(
                f"🚀 ENTRY {side.upper()} {symbol}\n"
                f"• Price: {entry:.8f}\n• Qty≈{qty}\n• Notional: {usdt_amount:.2f} USDT\n• Lev: {LEVERAGE:.0f}x"
            )
            return

        send_telegram(f"⚠️ ENTRY 실패(시도 {attempt+1}/3) {key}: {resp}")
        time.sleep(0.15*(2**attempt) + random.uniform(0,0.15))
        last = get_last_price(symbol) or last

    send_telegram(f"❌ ENTRY 최종 실패 {key}: 재시도 3회 모두 실패")

def take_partial_profit(symbol: str, pct: float, side: str="long"):
    symbol = convert_symbol(symbol)
    key=f"{symbol}_{side}"

    with _POS_LOCK:
        d = position_data.get(key)

    # 로컬 상태가 없으면 → 원격 사이즈 기반 분할(재시작/유실 대비)
    if d is None:
        p = _get_remote(symbol, side)
        if not p:
            send_telegram(f"❌ TP 실패: {key} 포지션 없음(로컬/원격)")
            return
        cut_size = round(p["size"] * pct, 6)
        if cut_size <= 0:
            send_telegram(f"⚠️ TP 스킵: 원격 사이즈 분할 0 ({key})")
            return
        resp = place_reduce_by_size(symbol, cut_size, side, leverage=LEVERAGE)
        exit_price = get_last_price(symbol)
        if resp.get("code")=="00000" and exit_price is not None:
            # 통계는 근사: entry*pct*size * (price diff / entry)
            notional = p["entry_price"] * cut_size
            pnl = _pnl_usdt(p["entry_price"], exit_price, notional, side)
            record_pnl(symbol, pnl)
            send_telegram(f"🤑 TP(remote) {int(pct*100)}% {side.upper()} {symbol}\n• Exit: {exit_price:.8f}\n• Size: {cut_size}\n• Realized: {pnl:+.2f} USDT")
        else:
            send_telegram(f"❌ TP(remote) 실패 {key}: {resp}")
        return

    # 로컬 상태가 있으면 → 기존 방식(USDT 명목 분할)
    portion = round(d["usdt_remaining"] * pct, 6)
    if portion <= 0:
        send_telegram(f"⚠️ TP 스킵: 남은 노출 0 ({key})")
        return
    resp = place_market_order(symbol, portion, side="sell" if side=="long" else "buy", leverage=LEVERAGE, reduce_only=True)
    exit_price = get_last_price(symbol)
    if resp.get("code")=="00000" and exit_price is not None:
        pnl=_pnl_usdt(d["entry_price"], exit_price, portion, side)
        with _POS_LOCK:
            d["realized_pnl"]+=pnl; d["usdt_remaining"]-=portion
            remain = d["usdt_remaining"]
        record_pnl(symbol, pnl)
        send_telegram(f"🤑 TP {int(pct*100)}% {side.upper()} {symbol}\n• Exit: {exit_price:.8f}\n• Portion: {portion:.2f} USDT\n• Realized(+this): {pnl:+.2f}\n• Remain: {remain:.2f} USDT")
        if remain <= 0.01:
            with _POS_LOCK:
                total = d["realized_pnl"]
                del position_data[key]
            record_result(symbol, total)
            send_telegram(f"✅ {key} 완전 종료")
    else:
        send_telegram(f"❌ TP 실패 {key}: {resp}")

def _get_remote_size(symbol: str, side: str) -> Optional[float]:
    p = _get_remote(symbol, side)
    return float(p["size"]) if p else None

def close_position(symbol: str, side: str="long", reason: str="manual"):
    symbol = convert_symbol(symbol)
    key=f"{symbol}_{side}"
    size=_get_remote_size(symbol, side)
    if size is None or size<=0:
        send_telegram(f"⚠️ Close 요청했지만 거래소 포지션 없음: {key} ({reason})")
        with _POS_LOCK:
            if key in position_data: del position_data[key]
        return
    resp=place_reduce_by_size(symbol, size, side, leverage=LEVERAGE)
    exit_price=get_last_price(symbol)
    if resp.get("code")=="00000" and exit_price is not None:
        with _POS_LOCK:
            entry_price = position_data.get(key,{}).get("entry_price", exit_price)
            realized_local = position_data.get(key,{}).get("realized_pnl", 0.0)
        notional = entry_price * size
        pnl=_pnl_usdt(entry_price, exit_price, notional, side)
        total = realized_local + pnl
        record_pnl(symbol, pnl); record_result(symbol, total)
        send_telegram(f"⛔ CLOSE {side.upper()} {symbol} ({reason})\n• Exit: {exit_price:.8f}\n• Realized Total: {total:+.2f} USDT")
        with _POS_LOCK:
            if key in position_data: del position_data[key]
    else:
        send_telegram(f"❌ Close 실패 {key}: {resp}")

# ── ROE 손절(서버) ────────────────────────────────────────────────────────
def check_loss_and_exit():
    _normalize_local_keys()
    with _POS_LOCK:
        items = list(position_data.items())
    for key, info in items:
        symbol, side = key.rsplit("_",1)
        entry=info["entry_price"]; now=get_last_price(symbol)
        if now is None: continue
        roe=(now/entry-1.0)*LEVERAGE if side=="long" else (entry/now-1.0)*LEVERAGE
        if roe <= STOP_ROE:
            send_telegram(f"🚨 ROE {STOP_ROE*100:.0f}% 손절 {side.upper()} {symbol} (entry {entry:.8f} → now {now:.8f})")
            close_position(symbol, side, "roe_stop")

# ── 재시작 자동 복구 ───────────────────────────────────────────────────────
def sync_open_positions():
    _normalize_local_keys()
    remote=get_open_positions(); seen=set()
    with _POS_LOCK:
        for p in remote:
            sym=p["symbol"]; side=p["side"]; size=float(p["size"]); entry=float(p["entry_price"])
            if size<=0 or entry<=0: continue
            key=f"{sym}_{side}"; seen.add(key)
            if key not in position_data:
                notional=round(entry*size,6)
                position_data[key]={"entry_price":entry,"usdt_opened":notional,"usdt_remaining":notional,"realized_pnl":0.0}
                send_telegram(f"🔁 SYNC: {key} 복구 (qty≈{size}, entry≈{entry})")
        for key in list(position_data.keys()):
            if key not in seen:
                send_telegram(f"🔁 SYNC: 거래소에 없는 포지션 → 로컬 제거: {key}")
                del position_data[key]

# ── 일일 리포트 ───────────────────────────────────────────────────────────
def send_daily_summary():
    day=_today_kst_str(); day_stats=STATS.get(day,{})
    if not day_stats: send_telegram(f"📅 {day} 일일 리포트\n오늘 거래 내역이 없습니다."); return
    winners, losers, flats=[],[],[]; total_trades=total_wins=total_losses=0; total_pnl=0.0
    for sym, rec in sorted(day_stats.items()):
        trades=rec.get("trades",0); wins=rec.get("wins",0); losses=rec.get("losses",0); pnl=float(rec.get("pnl",0.0))
        wr=(wins/trades*100) if trades>0 else 0.0
        total_trades+=trades; total_wins+=wins; total_losses+=losses; total_pnl+=pnl
        line=f"{sym} — {trades}회 | PnL {pnl:+.2f} | 승률 {wr:.1f}%"
        (winners if pnl>0 else losers if pnl<0 else flats).append(line)
    overall_wr=(total_wins/total_trades*100) if total_trades>0 else 0.0
    parts=[f"📅 {day} 일일 리포트", f"📊 전체 요약: {total_trades}회 | PnL {total_pnl:+.2f} USDT | 승률 {overall_wr:.1f}% (W:{total_wins}/L:{total_losses})"]
    if winners: parts.append("✅ 수익 종목:"); parts += [f"• {x}" for x in winners]
    if losers:  parts.append("❌ 손실 종목:"); parts += [f"• {x}" for x in losers]
    if flats:   parts.append("➖ 보합/무손익:"); parts += [f"• {x}" for x in flats]
    send_telegram("\n".join(parts))
