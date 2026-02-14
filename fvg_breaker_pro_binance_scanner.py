#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FVG Breaker Pro (Pine v6 logic) -> Python scanner for Binance USDT-M Futures (CCXT).

- Runs automatically in a loop (optional)
- Scans many USDT-M symbols
- Supports multi-timeframe scan
- Configurable bars analyzed, signal age, and most indicator settings

ملاحظة:
هذا تحويل برمجي عملي لمنطق المؤشر إلى ماسح بايثون. التطابق قريب جداً من المنطق،
لكن لا يمكن ضمان 100% بسبب فروقات بيئة التنفيذ بين TradingView و CCXT.
"""

from __future__ import annotations

from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple
import math
import time
import traceback

import ccxt


# ============================== CONFIG ==============================
CONFIG = {
    # Exchange
    "exchange_id": "binanceusdm",
    "quote": "USDT",
    "symbols_limit": 120,           # top-N by quoteVolume
    "sort_by_quote_volume": True,

    # Scan behavior
    "timeframes": ["5m", "15m", "30m", "1h"],
    "bars_back": 700,               # number of candles to analyze
    "signal_age_bars": 8,           # max age (bars) for reported active signal
    "scan_interval_sec": 120,       # loop delay
    "run_forever": True,            # False => run once then exit

    # Concurrency
    "parallel": False,
    "max_workers": 6,

    # Pine-mapped settings
    "swing_length": 5,
    "fvg_threshold_atr_mult": 0.5,
    "min_bars_between": 10,
    "invalidation_mode": "Close",   # "Close" or "Wick"
    "max_active_setups": 10,
    "max_bars_active": 500,

    # Quality filters
    "enable_filters": True,
    "min_risk_reward": 1.5,
    "filter_trend_alignment": True,
    "trend_ema_length": 20,
    "filter_volatility": True,
    "min_volatility_ratio": 0.5,
    "filter_price_action": True,
    "min_candle_size_atr": 0.3,
}


# ============================== MODELS ==============================
@dataclass
class Swing:
    price: float
    index: int
    is_high: bool


@dataclass
class Setup:
    setup_id: int
    is_bullish: bool
    timeframe: str
    symbol: str
    entry_price: float
    stop_price: float
    created_index: int
    bars_active: int = 0
    is_active: bool = True


# ============================== MATH ================================
def sma(values: List[float], length: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    if length <= 0 or len(values) < length:
        return out
    s = sum(values[:length])
    out[length - 1] = s / length
    for i in range(length, len(values)):
        s += values[i] - values[i - length]
        out[i] = s / length
    return out


def ema(values: List[float], length: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(values)
    if length <= 0 or not values:
        return out
    alpha = 2.0 / (length + 1.0)
    seed_len = min(length, len(values))
    seed = sum(values[:seed_len]) / float(seed_len)
    out[seed_len - 1] = seed
    prev = seed
    for i in range(seed_len, len(values)):
        prev = alpha * values[i] + (1 - alpha) * prev
        out[i] = prev
    return out


def atr_rma(high: List[float], low: List[float], close: List[float], length: int = 14) -> List[Optional[float]]:
    n = len(close)
    out: List[Optional[float]] = [None] * n
    if n == 0:
        return out

    tr: List[float] = [0.0] * n
    tr[0] = high[0] - low[0]
    for i in range(1, n):
        tr[i] = max(high[i] - low[i], abs(high[i] - close[i - 1]), abs(low[i] - close[i - 1]))

    if n < length:
        return out

    seed = sum(tr[:length]) / length
    out[length - 1] = seed
    prev = seed
    for i in range(length, n):
        prev = (prev * (length - 1) + tr[i]) / length
        out[i] = prev
    return out


def is_pivot_high(high: List[float], idx: int, left: int, right: int) -> bool:
    if idx - left < 0 or idx + right >= len(high):
        return False
    c = high[idx]
    return all(c > high[j] for j in range(idx - left, idx)) and all(c >= high[j] for j in range(idx + 1, idx + right + 1))


def is_pivot_low(low: List[float], idx: int, left: int, right: int) -> bool:
    if idx - left < 0 or idx + right >= len(low):
        return False
    c = low[idx]
    return all(c < low[j] for j in range(idx - left, idx)) and all(c <= low[j] for j in range(idx + 1, idx + right + 1))


def level_invalidated(level: float, start_idx: int, end_idx: int, check_below: bool,
                     mode: str, high: List[float], low: List[float], close: List[float]) -> bool:
    if end_idx - start_idx <= 1:
        return False
    for i in range(start_idx + 1, end_idx):
        px = low[i] if mode == "Wick" and check_below else high[i] if mode == "Wick" else close[i]
        if check_below and px < level:
            return True
        if not check_below and px > level:
            return True
    return False


# ============================== CORE ================================
def pass_quality_filters(
    is_bull: bool,
    entry: float,
    stop: float,
    candle_size: float,
    fvg_top: float,
    fvg_bot: float,
    atr_val: Optional[float],
    is_uptrend: bool,
    is_downtrend: bool,
    is_high_volatility: bool,
) -> bool:
    if not CONFIG["enable_filters"]:
        return True
    if atr_val is None or atr_val <= 0:
        return False

    risk = abs(entry - stop)
    fvg_size = abs(entry - (fvg_top if is_bull else fvg_bot))
    potential_reward = fvg_size * 2
    rr = (potential_reward / risk) if risk > 0 else 0
    if rr < CONFIG["min_risk_reward"]:
        return False

    if CONFIG["filter_trend_alignment"]:
        if is_bull and is_downtrend:
            return False
        if (not is_bull) and is_uptrend:
            return False

    if CONFIG["filter_volatility"] and not is_high_volatility:
        return False

    if CONFIG["filter_price_action"] and candle_size < (atr_val * CONFIG["min_candle_size_atr"]):
        return False

    return True


def analyze_symbol_tf(symbol: str, tf: str, exchange: ccxt.Exchange) -> List[Setup]:
    candles = exchange.fetch_ohlcv(symbol, timeframe=tf, limit=CONFIG["bars_back"])
    if candles is None or len(candles) < max(60, CONFIG["swing_length"] * 4):
        return []

    t = [int(c[0]) for c in candles]
    o = [float(c[1]) for c in candles]
    h = [float(c[2]) for c in candles]
    l = [float(c[3]) for c in candles]
    c = [float(c[4]) for c in candles]

    atr = atr_rma(h, l, c, 14)
    trend = ema(c, CONFIG["trend_ema_length"])
    atr_avg = sma([x if x is not None else 0.0 for x in atr], 20)

    swings: List[Swing] = []
    setups: List[Setup] = []
    last_setup_bar = -10**9

    sl = CONFIG["swing_length"]
    for i in range(len(candles)):
        # Confirm pivots with right bars delay, like Pine pivothigh/pivotlow behavior.
        center = i - sl
        if center >= sl and center + sl < len(candles):
            if is_pivot_high(h, center, sl, sl):
                swings.append(Swing(price=h[center], index=center, is_high=True))
            if is_pivot_low(l, center, sl, sl):
                swings.append(Swing(price=l[center], index=center, is_high=False))
            if len(swings) > 30:
                swings = swings[-30:]

        # Live state values on current candle
        atr_i = atr[i]
        trend_i = trend[i]
        atr_avg_i = atr_avg[i]

        is_uptrend = trend_i is not None and c[i] > trend_i
        is_downtrend = trend_i is not None and c[i] < trend_i
        is_high_volatility = bool(atr_i is not None and atr_avg_i not in (None, 0) and (atr_i / atr_avg_i) > CONFIG["min_volatility_ratio"])

        # Validate active setups on each bar
        for s in setups:
            if not s.is_active:
                continue
            s.bars_active += 1
            check_hi = h[i] if CONFIG["invalidation_mode"] == "Wick" else c[i]
            check_lo = l[i] if CONFIG["invalidation_mode"] == "Wick" else c[i]
            invalid = False
            if s.is_bullish and check_lo < s.stop_price:
                invalid = True
            if (not s.is_bullish) and check_hi > s.stop_price:
                invalid = True
            if s.bars_active > CONFIG["max_bars_active"]:
                invalid = True
            if invalid:
                s.is_active = False

        if i < 2:
            continue

        can_create_new = (i - last_setup_bar) >= CONFIG["min_bars_between"]
        active_count = sum(1 for x in setups if x.is_active)
        if active_count >= CONFIG["max_active_setups"] or not can_create_new:
            continue

        # FVG detection (same formula)
        is_bull_fvg = h[i - 2] < l[i]
        bull_top = l[i]
        bull_bot = h[i - 2]
        valid_bull_fvg = bool(is_bull_fvg and atr_i is not None and (bull_top - bull_bot) > (atr_i * CONFIG["fvg_threshold_atr_mult"]))

        is_bear_fvg = l[i - 2] > h[i]
        bear_top = l[i - 2]
        bear_bot = h[i]
        valid_bear_fvg = bool(is_bear_fvg and atr_i is not None and (bear_top - bear_bot) > (atr_i * CONFIG["fvg_threshold_atr_mult"]))

        # Bull setup search on latest swings (reverse)
        if valid_bull_fvg and swings:
            for sw in reversed(swings):
                is_first_break = not level_invalidated(sw.price, sw.index, i, False, CONFIG["invalidation_mode"], h, l, c)
                is_body_break = c[i] > sw.price and o[i] < c[i]
                if sw.is_high and is_body_break and is_first_break:
                    breaker_top = max(o[sw.index], c[sw.index])
                    breaker_bot = min(o[sw.index], c[sw.index])
                    overlap = not (breaker_bot > bull_top or breaker_top < bull_bot)
                    if overlap:
                        stop = min(l[i], l[i - 1], l[i - 2])
                        candle_size = abs(c[i] - o[i])
                        if pass_quality_filters(True, bull_bot, stop, candle_size, bull_top, bull_bot,
                                               atr_i, is_uptrend, is_downtrend, is_high_volatility):
                            setups.append(Setup(
                                setup_id=i,
                                is_bullish=True,
                                timeframe=tf,
                                symbol=symbol,
                                entry_price=bull_bot,
                                stop_price=stop,
                                created_index=i,
                            ))
                            last_setup_bar = i
                    break

        # Bear setup search
        if valid_bear_fvg and swings:
            for sw in reversed(swings):
                is_first_break = not level_invalidated(sw.price, sw.index, i, True, CONFIG["invalidation_mode"], h, l, c)
                is_body_break = c[i] < sw.price and o[i] > c[i]
                if (not sw.is_high) and is_body_break and is_first_break:
                    breaker_top = max(o[sw.index], c[sw.index])
                    breaker_bot = min(o[sw.index], c[sw.index])
                    overlap = not (breaker_bot > bear_top or breaker_top < bear_bot)
                    if overlap:
                        stop = max(h[i], h[i - 1], h[i - 2])
                        candle_size = abs(c[i] - o[i])
                        if pass_quality_filters(False, bear_top, stop, candle_size, bear_top, bear_bot,
                                               atr_i, is_uptrend, is_downtrend, is_high_volatility):
                            setups.append(Setup(
                                setup_id=i,
                                is_bullish=False,
                                timeframe=tf,
                                symbol=symbol,
                                entry_price=bear_top,
                                stop_price=stop,
                                created_index=i,
                            ))
                            last_setup_bar = i
                    break

    # Return recent active setups only
    last_idx = len(candles) - 1
    max_age = CONFIG["signal_age_bars"]
    out = [s for s in setups if s.is_active and (last_idx - s.created_index) <= max_age]
    return out


# ============================== EXCHANGE ============================
def build_exchange() -> ccxt.Exchange:
    ex_class = getattr(ccxt, CONFIG["exchange_id"])
    ex = ex_class({"enableRateLimit": True, "options": {"defaultType": "future"}})
    ex.load_markets()
    return ex


def fetch_tickers_safe(exchange: ccxt.Exchange, symbols: List[str]) -> Dict[str, dict]:
    """
    CCXT on Binance can throw when passed mixed market types.
    This helper tries multiple safe strategies and never raises.
    """
    # 1) try with provided symbols directly
    try:
        return exchange.fetch_tickers(symbols)
    except Exception:
        pass

    # 2) try all tickers then filter locally
    try:
        all_tickers = exchange.fetch_tickers()
        if isinstance(all_tickers, dict):
            return {s: all_tickers.get(s, {}) for s in symbols}
    except Exception:
        pass

    # 3) fallback: per-symbol fetch_ticker (slow but robust)
    out: Dict[str, dict] = {}
    for s in symbols:
        try:
            out[s] = exchange.fetch_ticker(s)
        except Exception:
            out[s] = {}
    return out


def get_usdtm_symbols(exchange: ccxt.Exchange) -> List[str]:
    symbols: List[str] = []
    for sym, m in exchange.markets.items():
        if not isinstance(m, dict):
            continue
        if m.get("quote") != CONFIG["quote"]:
            continue
        if not m.get("active", True):
            continue

        # IMPORTANT: keep one market type only to avoid
        # "symbols must be of the same type, either swap or future".
        # For Binance USDT-M scanner we use perpetual linear swaps.
        if not m.get("contract", False):
            continue
        if not m.get("swap", False):
            continue
        if m.get("future", False):
            continue
        if not m.get("linear", False):
            continue

        symbols.append(sym)

    if not CONFIG["sort_by_quote_volume"]:
        return symbols[: CONFIG["symbols_limit"]]

    tickers = fetch_tickers_safe(exchange, symbols)
    scored = []
    for s in symbols:
        tk = tickers.get(s, {}) if isinstance(tickers, dict) else {}
        qv = float(tk.get("quoteVolume") or tk.get("baseVolume") or 0.0)
        scored.append((s, qv))
    scored.sort(key=lambda x: x[1], reverse=True)
    return [s for s, _ in scored[: CONFIG["symbols_limit"]]]


def scan_once(exchange: ccxt.Exchange) -> List[Setup]:
    symbols = get_usdtm_symbols(exchange)
    all_hits: List[Setup] = []

    tasks: List[Tuple[str, str]] = [(s, tf) for s in symbols for tf in CONFIG["timeframes"]]

    if CONFIG["parallel"]:
        with ThreadPoolExecutor(max_workers=CONFIG["max_workers"]) as pool:
            futs = {pool.submit(analyze_symbol_tf, s, tf, exchange): (s, tf) for s, tf in tasks}
            for fut in as_completed(futs):
                s, tf = futs[fut]
                try:
                    hits = fut.result()
                    all_hits.extend(hits)
                except Exception:
                    print(f"[ERR] {s} {tf}:\n{traceback.format_exc(limit=1)}")
    else:
        for s, tf in tasks:
            try:
                all_hits.extend(analyze_symbol_tf(s, tf, exchange))
            except Exception:
                print(f"[ERR] {s} {tf}:\n{traceback.format_exc(limit=1)}")

    return all_hits


def print_hits(hits: List[Setup]) -> None:
    if not hits:
        print("No active signals in configured age window.")
        return

    # sort: bullish first then by timeframe then symbol
    tf_rank = {tf: i for i, tf in enumerate(CONFIG["timeframes"])}
    hits.sort(key=lambda x: (not x.is_bullish, tf_rank.get(x.timeframe, 999), x.symbol))

    print(f"Signals: {len(hits)}")
    for s in hits:
        side = "LONG" if s.is_bullish else "SHORT"
        risk = abs(s.entry_price - s.stop_price)
        print(
            f"[{side}] {s.symbol:<16} tf={s.timeframe:<4} "
            f"entry={s.entry_price:.8f} stop={s.stop_price:.8f} risk={risk:.8f}"
        )


def main() -> None:
    print("Starting FVG Breaker Pro scanner (Binance USDT-M via CCXT)...")
    print(f"Timeframes={CONFIG['timeframes']} | bars_back={CONFIG['bars_back']} | signal_age_bars={CONFIG['signal_age_bars']}")

    exchange = build_exchange()

    while True:
        started = time.strftime("%Y-%m-%d %H:%M:%S")
        print("\n" + "=" * 80)
        print(f"Scan started: {started}")

        hits = scan_once(exchange)
        print_hits(hits)

        if not CONFIG["run_forever"]:
            break

        print(f"Sleeping {CONFIG['scan_interval_sec']} sec...")
        time.sleep(CONFIG["scan_interval_sec"])


if __name__ == "__main__":
    main()
