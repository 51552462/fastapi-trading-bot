from typing import Dict, Optional
from bitget_api import (
    place_market_order, place_reduce_by_size,
    get_last_price, get_open_positions
)
import json, os, time

# ── Telegram (없으면 콘솔 fallback) ────────────────────────────────────────
try:
    from telegram_bot import send_telegram
except Exception:
    def send_telegram(msg: str): print("[TG]", msg)

# ── 설정 (환경변수로 조절 가능) ────────────────────────────────────────────
LEVERAGE   = float(os.getenv("LEVERAGE", "5"))
STOP_ROE   = float(os.getenv("STOP_ROE", "-0.10"))        # -0.10 = -10%
STATS_FILE = os.getenv("TRADE_STATS_FILE", "trade_stats.json")

# ── 메모리 상태 ────────────────────────────────────────────────────────────
# key = "BTCUSDT_long" / "BTCUSDT_short"
position_data: Dict[str, dict] = {}

# ── KST time & stats ───────────────────────────────────────────────────────
def _now_kst_epoch() -> int: return int(time.time()) + 9*3600
def _today_kst_str() -> str: return time.strftime("%Y-%m-%d", time.gmtime(_now_kst_epoch()))

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
    _ensure_symbol(STATS[day], symbol)["trades"] += 1
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
    _save_stats(STATS)

# ── PnL helper (notional 근사) ─────────────────────────────────────────────
def _pnl_usdt(entry, exit, portion_usdt, side):
    return portion_usdt * ((exit - entry) / entry) if side == "long" else portion_usdt * ((entry - exit) / entry)

# ── 주문 실행 ──────────────────────────────────────────────────────────────
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
        record_entry(symbol)
        send_telegram(
            f"🚀 ENTRY {side.upper()} {symbol}\n"
            f"• Price: {entry:.6f}\n• Notional: {usdt_amount:.2f} USDT\n• Lev: {LEVERAGE:.0f}x"
        )
    else:
        send_telegram(f"❌ Entry 실패 {key}: {resp}")

def take_partial_profit(symbol: str, pct: float, side: str = "long"):
    key = f"{symbol}_{side}"
    if key not in position_data:
        send_telegram(f"❌ TP 실패: {key} 포지션 없음")
        return

    d = position_data[key]
    portion = round(d["usdt_remaining"] * pct, 6)
    if portion <= 0:
        send_telegram(f"⚠️ TP 스킵: 남은 노출 0 ({key})")
        return

    resp = place_market_order(symbol, portion,
                              side="sell" if side == "long" else "buy",
                              leverage=LEVERAGE, reduce_only=True)
    exit_price = get_last_price(symbol)
    if resp.get("code") == "00000" and exit_price is not None:
        pnl = _pnl_usdt(d["entry_price"], exit_price, portion, side)
        d["realized_pnl"] += pnl
        d["usdt_remaining"] -= portion
        record_pnl(symbol, pnl)
        send_telegram(
            f"🤑 TP {int(pct*100)}% {side.upper()} {symbol}\n"
            f"• Exit: {exit_price:.6f}\n• Portion: {portion:.2f} USDT\n"
            f"• Realized(+this): {pnl:+.2f} USDT\n• Cum Realized: {d['realized_pnl']:+.2f} USDT\n"
            f"• Remaining: {d['usdt_remaining']:.2f} USDT"
        )
        if d["usdt_remaining"] <= 0.01:
            record_result(symbol, d["realized_pnl"])
            del position_data[key]
            send_telegram(f"✅ {key} 완전 종료(잔여 미미)")
    else:
        send_telegram(f"❌ TP 실패 {key}: {resp}")

def _get_remote_size(symbol: str, side: str) -> Optional[float]:
    for p in get_open_positions():
        if p["symbol"] == symbol and p["side"] == side:
            return float(p["size"])
    return None

def close_position(symbol: str, side: str = "long", reason: str = "manual"):
    key = f"{symbol}_{side}"
    # 거래소 현재 수량을 가져와 '사이즈 기준'으로 종료 → 잔량 방지
    size = _get_remote_size(symbol, side)
    if size is None or size <= 0:
        send_telegram(f"⚠️ Close 요청했지만 거래소 포지션 없음: {key} ({reason})")
        if key in position_data:
            del position_data[key]
        return

    resp = place_reduce_by_size(symbol, size, side, leverage=LEVERAGE)
    exit_price = get_last_price(symbol)
    if resp.get("code") == "00000" and exit_price is not None:
        entry_price = position_data.get(key, {}).get("entry_price", exit_price)
        notional = entry_price * size
        realized_local = position_data.get(key, {}).get("realized_pnl", 0.0)
        pnl = _pnl_usdt(entry_price, exit_price, notional, side)
        total = realized_local + pnl

        record_pnl(symbol, pnl)
        record_result(symbol, total)
        send_telegram(
            f"⛔ CLOSE {side.upper()} {symbol} ({reason})\n"
            f"• Exit: {exit_price:.6f}\n• Realized Total: {total:+.2f} USDT"
        )
        if key in position_data:
            del position_data[key]
    else:
        send_telegram(f"❌ Close 실패 {key}: {resp}")

# ── ROE watchdog (서버 전담) ───────────────────────────────────────────────
def check_loss_and_exit():
    for key, info in list(position_data.items()):
        symbol, side = key.rsplit("_", 1)
        entry = info["entry_price"]
        now = get_last_price(symbol)
        if now is None:
            continue
        roe = (now / entry - 1.0) * LEVERAGE if side == "long" else (entry / now - 1.0) * LEVERAGE
        if roe <= STOP_ROE:
            send_telegram(
                f"🚨 ROE {STOP_ROE*100:.0f}% 손절 {side.upper()} {symbol} "
                f"(entry {entry:.6f} → now {now:.6f})"
            )
            close_position(symbol, side, "roe_stop")

# ── 재시작 자동 복구 ───────────────────────────────────────────────────────
def sync_open_positions():
    remote = get_open_positions()
    seen = set()

    for p in remote:
        sym = p["symbol"]          # 이미 BTCUSDT 형식
        side = p["side"]
        size = float(p["size"])
        entry_price = float(p["entry_price"])
        if size <= 0 or entry_price <= 0:
            continue

        key = f"{sym}_{side}"
        seen.add(key)
        if key not in position_data:
            notional = round(entry_price * size, 6)
            position_data[key] = {
                "entry_price": entry_price,
                "usdt_opened": notional,
                "usdt_remaining": notional,
                "realized_pnl": 0.0
            }
            send_telegram(f"🔁 SYNC: {key} 복구 (qty≈{size}, entry≈{entry_price})")

    for key in list(position_data.keys()):
        if key not in seen:
            send_telegram(f"🔁 SYNC: 거래소에 없는 포지션 → 로컬 제거: {key}")
            del position_data[key]

# ── 일일 리포트 ───────────────────────────────────────────────────────────
def send_daily_summary():
    day = _today_kst_str()
    day_stats = STATS.get(day, {})
    if not day_stats:
        send_telegram(f"📅 {day} 일일 리포트\n오늘 거래 내역이 없습니다.")
        return

    winners, losers, flats = [], [], []
    total_trades = total_wins = total_losses = 0
    total_pnl = 0.0

    for sym, rec in sorted(day_stats.items()):
        trades = rec.get("trades", 0)
        wins   = rec.get("wins", 0)
        losses = rec.get("losses", 0)
        pnl    = float(rec.get("pnl", 0.0))
        wr     = (wins / trades * 100) if trades > 0 else 0.0

        total_trades += trades
        total_wins   += wins
        total_losses += losses
        total_pnl    += pnl

        line = f"{sym} — {trades}회 | PnL {pnl:+.2f} | 승률 {wr:.1f}%"
        (winners if pnl > 0 else losers if pnl < 0 else flats).append(line)

    overall_wr = (total_wins / total_trades * 100) if total_trades > 0 else 0.0

    parts = [f"📅 {day} 일일 리포트"]
    parts.append(f"📊 전체 요약: {total_trades}회 | PnL {total_pnl:+.2f} USDT | 승률 {overall_wr:.1f}% (W:{total_wins}/L:{total_losses})")
    if winners: parts.append("✅ 수익 종목:"); parts += [f"• {x}" for x in winners]
    if losers:  parts.append("❌ 손실 종목:"); parts += [f"• {x}" for x in losers]
    if flats:   parts.append("➖ 보합/무손익:"); parts += [f"• {x}" for x in flats]

    send_telegram("\n".join(parts))
