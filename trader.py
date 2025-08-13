from typing import Dict, Tuple
from bitget_api import place_market_order, get_last_price, get_open_positions
import json, os, time, math

# 텔레그램 (없으면 콘솔만)
try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str): print("[TG]", msg)

LEVERAGE = 5

# 현재 열린 포지션 메모리 (재시작 시 sync로 복구)
# key: "BTCUSDT_long" / "BTCUSDT_short"
position_data: Dict[str, dict] = {}

# 일일 통계 저장 파일 (프로세스 재시작 후에도 유지 시도)
STATS_FILE = os.getenv("TRADE_STATS_FILE", "trade_stats.json")

# ──────────────────────
# 날짜/저장 유틸 (KST)
# ──────────────────────
def _now_kst_epoch() -> int:
    return int(time.time()) + 9 * 3600  # UTC + 9h

def _today_kst_str() -> str:
    t = _now_kst_epoch()
    return time.strftime("%Y-%m-%d", time.gmtime(t))

def _load_stats() -> dict:
    if os.path.exists(STATS_FILE):
        try:
            with open(STATS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def _save_stats(stats: dict):
    try:
        with open(STATS_FILE, "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print("stats save error:", e)

STATS = _load_stats()

def _ensure_symbol(stats_day: dict, symbol: str) -> dict:
    if symbol not in stats_day:
        stats_day[symbol] = {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0}
    return stats_day[symbol]

def record_entry(symbol: str):
    day = _today_kst_str()
    STATS.setdefault(day, {})
    symrec = _ensure_symbol(STATS[day], symbol)
    symrec["trades"] += 1
    _save_stats(STATS)

def record_pnl(symbol: str, pnl: float):
    day = _today_kst_str()
    STATS.setdefault(day, {})
    symrec = _ensure_symbol(STATS[day], symbol)
    symrec["pnl"] = float(symrec.get("pnl", 0.0)) + float(pnl)
    _save_stats(STATS)

def record_result(symbol: str, realized_total_for_trade: float):
    day = _today_kst_str()
    STATS.setdefault(day, {})
    symrec = _ensure_symbol(STATS[day], symbol)
    if realized_total_for_trade > 0:
        symrec["wins"] += 1
    elif realized_total_for_trade < 0:
        symrec["losses"] += 1
    # 무손익(=0)은 승패 집계에서 제외
    _save_stats(STATS)

# ──────────────────────
# PnL 계산
# ──────────────────────
def _pnl_usdt(entry, exit, portion_usdt, side):
    # notional 기반 근사
    if side == "long":
        return portion_usdt * ((exit - entry) / entry)
    else:
        return portion_usdt * ((entry - exit) / entry)

# ──────────────────────
# 주문/청산
# ──────────────────────
def enter_position(symbol: str, usdt_amount: float, side: str = "long"):
    key = f"{symbol}_{side}"
    resp = place_market_order(symbol, usdt_amount,
                              side="buy" if side == "long" else "sell",
                              leverage=LEVERAGE, reduce_only=False)
    if resp.get("code") == "00000":
        entry = get_last_price(symbol)
        if entry is None:
            send_telegram(f"⚠️ ENTRY 체결 후 가격조회 실패 {key}")
            return
        position_data[key] = {
            "entry_price": entry,
            "usdt_opened": usdt_amount,
            "usdt_remaining": usdt_amount,
            "realized_pnl": 0.0
        }
        # 일일 통계: 거래 횟수 +1
        record_entry(symbol)
        send_telegram(
            f"🚀 ENTRY {side.upper()} {symbol}\n"
            f"• Price: {entry:.6f}\n• Notional: {usdt_amount:.2f} USDT\n• Lev: {LEVERAGE}x"
        )
    else:
        send_telegram(f"❌ Entry 실패 {key}: {resp}")

def take_partial_profit(symbol: str, pct: float, side: str = "long"):
    key = f"{symbol}_{side}"
    if key not in position_data:
        send_telegram(f"❌ TP 실패: {key} 포지션 없음")
        return
    data = position_data[key]
    portion = round(data["usdt_remaining"] * pct, 6)
    if portion <= 0:
        send_telegram(f"⚠️ TP 스킵: 남은 노출 0 ({key})")
        return

    resp = place_market_order(symbol, portion,
                              side="sell" if side == "long" else "buy",
                              leverage=LEVERAGE, reduce_only=True)
    exit_price = get_last_price(symbol)
    if resp.get("code") == "00000" and exit_price is not None:
        pnl = _pnl_usdt(data["entry_price"], exit_price, portion, side)
        data["realized_pnl"] += pnl
        data["usdt_remaining"] -= portion
        # 일일 통계: 실현 PnL 누적
        record_pnl(symbol, pnl)
        send_telegram(
            f"🤑 TP {int(pct*100)}% {side.UPPER()} {symbol}\n"
            f"• Exit: {exit_price:.6f}\n• Portion: {portion:.2f} USDT\n"
            f"• Realized PnL(+this): {pnl:+.2f} USDT\n• Cum Realized: {data['realized_pnl']:+.2f} USDT\n"
            f"• Remaining: {data['usdt_remaining']:.2f} USDT"
        )
        if data["usdt_remaining"] <= 0.01:
            # 최종 종료 간주
            record_result(symbol, data["realized_pnl"])
            del position_data[key]
            send_telegram(f"✅ {key} 완전 종료(잔여 미미)")
    else:
        send_telegram(f"❌ TP 실패 {key}: {resp}")

def close_position(symbol: str, side: str = "long", reason: str = "manual"):
    key = f"{symbol}_{side}"
    if key not in position_data:
        send_telegram(f"⚠️ Close 요청했지만 포지션 없음: {key} ({reason})")
        return
    data = position_data[key]
    portion = data["usdt_remaining"]
    if portion <= 0:
        record_result(symbol, data["realized_pnl"])
        del position_data[key]
        return

    resp = place_market_order(symbol, portion,
                              side="sell" if side == "long" else "buy",
                              leverage=LEVERAGE, reduce_only=True)
    exit_price = get_last_price(symbol)
    if resp.get("code") == "00000" and exit_price is not None:
        pnl = _pnl_usdt(data["entry_price"], exit_price, portion, side)
        total_pnl = data["realized_pnl"] + pnl
        # 일일 통계 반영
        record_pnl(symbol, pnl)
        record_result(symbol, total_pnl)

        send_telegram(
            f"⛔ CLOSE {side.upper()} {symbol} ({reason})\n"
            f"• Exit: {exit_price:.6f}\n• Realized Total: {total_pnl:+.2f} USDT"
        )
        del position_data[key]
    else:
        send_telegram(f"❌ Close 실패 {key}: {resp}")

# ──────────────────────
# ROE -10% 감시
# ──────────────────────
def check_loss_and_exit():
    for key, info in list(position_data.items()):
        symbol, side = key.rsplit("_", 1)
        entry = info["entry_price"]
        now = get_last_price(symbol)
        if now is None:
            continue
        if side == "long":
            roe = (now / entry - 1.0) * LEVERAGE
        else:
            roe = (entry / now - 1.0) * LEVERAGE
        if roe <= -0.10:
            send_telegram(f"🚨 ROE -10% 손절 {side.upper()} {symbol} (entry {entry:.6f} → now {now:.6f})")
            close_position(symbol, side, "roe_stop")

# ──────────────────────
# 재시작 자동 복구 (거래소에서 동기화)
# ──────────────────────
def sync_open_positions():
    """
    거래소의 오픈 포지션을 읽어와 position_data를 복구/동기화.
    - 새로 생긴 포지션은 추가
    - 거래소에 없는 포지션은 로컬에서 제거(외부 종료로 간주)
    """
    remote = get_open_positions()
    seen = set()

    for p in remote:
        sym = p["symbol"]
        side = p["side"]
        size = float(p["size"])
        entry_price = float(p["entry_price"])
        if size <= 0 or entry_price <= 0:
            continue
        key = f"{sym}_{side}"
        seen.add(key)
        if key not in position_data:
            # notional을 entry_price * size로 근사
            notional = round(entry_price * size, 6)
            position_data[key] = {
                "entry_price": entry_price,
                "usdt_opened": notional,
                "usdt_remaining": notional,
                "realized_pnl": 0.0
            }
            send_telegram(f"🔁 SYNC: {key} 복구 (qty≈{size}, entry≈{entry_price})")
        else:
            # 이미 있으면 패스(필요 시 갱신 논리 추가 가능)
            pass

    # 로컬에 있는데 거래소에는 없는 경우 -> 제거
    for key in list(position_data.keys()):
        if key not in seen:
            send_telegram(f"🔁 SYNC: 거래소에 없는 포지션 발견 → 로컬 제거: {key}")
            del position_data[key]

# ──────────────────────
# 일일 텔레그램 리포트
# ──────────────────────
def send_daily_summary():
    day = _today_kst_str()
    day_stats = STATS.get(day, {})
    if not day_stats:
        send_telegram(f"📅 {day} 일일 리포트\n오늘 거래 내역이 없습니다.")
        return

    # 종목별 라인/집계
    winners, losers, flats = [], [], []
    total_trades = total_wins = total_losses = 0
    total_pnl = 0.0

    for sym, rec in sorted(day_stats.items()):
        trades = rec.get("trades", 0)
        wins   = rec.get("wins", 0)
        losses = rec.get("losses", 0)
        pnl    = float(rec.get("pnl", 0.0))
        wr = (wins / trades * 100) if trades > 0 else 0.0

        total_trades += trades
        total_wins   += wins
        total_losses += losses
        total_pnl    += pnl

        line = f"{sym} — {trades}회 | PnL {pnl:+.2f} | 승률 {wr:.1f}%"
        if pnl > 0: winners.append(line)
        elif pnl < 0: losers.append(line)
        else: flats.append(line)

    overall_wr = (total_wins / total_trades * 100) if total_trades > 0 else 0.0

    parts = [f"📅 {day} 일일 리포트"]
    parts.append(f"📊 전체 요약: {total_trades}회 | PnL {total_pnl:+.2f} USDT | 승률 {overall_wr:.1f}% (W:{total_wins}/L:{total_losses})")
    if winners:
        parts.append("✅ 수익 종목:")
        parts += [f"• {x}" for x in winners]
    if losers:
        parts.append("❌ 손실 종목:")
        parts += [f"• {x}" for x in losers]
    if flats:
        parts.append("➖ 보합/무손익:")
        parts += [f"• {x}" for x in flats]

    send_telegram("\n".join(parts))
