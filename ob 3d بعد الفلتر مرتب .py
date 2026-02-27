# -*- coding: utf-8 -*-
"""
Order Blocks Volume Delta (Pine-like) + Binance USDT-M Futures Scanner (CCXT)
FINAL BUILD:
- Prints ONLY on:
  1) FIRST TOUCH of an active OB zone (persisted)  -> [TOUCH FIRST]
  2) FIRST RETEST of an active OB zone (persisted) -> ▲ / ▼ triangles
- Live loop every X minutes
- High coins first sorting (24h quoteVolume by default)
- Prints bullish candidates list (setups) at end of each scan cycle

Caches saved next to script:
- touched_obs_cache.json
- retested_obs_cache.json
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict, Any
import time as pytime
import math
import json
import os
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import ccxt


ANSI_RESET = "\033[0m"
ANSI_GREEN = "\033[92m"
ANSI_YELLOW = "\033[93m"
ANSI_RED = "\033[91m"
ANSI_CYAN = "\033[96m"


# ============================================================
# ========================== SETTINGS =========================
# ============================================================

CONFIG = {
    # Exchange / scan
    "exchange_id": "binanceusdm",
    "quote": "USDT",
    "scan_limit_symbols": 0,          # 0 = all
    "rate_limit_sleep": True,

    # High coins first (sorting/filtering)
    "high_coins_first": True,
    "sort_by": "quoteVolume",         # "quoteVolume" | "absPercentage" | "percentage"
    "min_quote_volume_24h": 2.0,      # set >0 to filter
    "min_abs_change_24h_pct": 2.0,    # set >0 to filter
    "top_n_after_sort": 0,        # 0 = all

    # Timeframes / bars
    "timeframe": "1m",
    "vd_timeframe": "",             # "" to use chart TF volume (no LTF)
    "bars_back": 5000,
    "fetch_batch_limit": 1500,

    # LTF coverage tuning
    "LTF_EXTRA_BUFFER_CANDLES": 2000,

    # Indicator params (match Pine defaults)
    "swing_len": 10,
    "STRICT_PIVOT": True,
    "invalidation": "Wick",           # "Wick" or "Close"
    "max_stored_obs": 50,
    "poc_bins": 40,

    # OB Volume Delta filter
    "ob_delta_filter_enabled": True,
    "ob_min_bull_delta_pct": 70,       # show bullish OB only if bull_pct >= this
    "ob_min_bear_delta_pct": 70,       # show bearish OB only if bear_pct >= this

    # ==================== TOUCH ALERTS ====================
    "alert_touch_zone": False,
    "touch_age_bars": 1,            # ✅ 0 = only current candle (prevents printing old touches)
    "first_touch_only": True,         # ✅ persistent first touch only
    "alert_only_last_candle": True,   # ✅ emit alerts only when evaluating last candle

    # ==================== TRIANGLE RETEST ====================
    "print_retest_triangles": True,
    "first_retest_only": True,        # ✅ persistent first retest only

    # ==================== CANDIDATES ====================
    "print_candidates": False,
    "candidates_top_n": 20,
    "candidate_requires_retest": True,    # recommended (stronger)
    "candidate_requires_touch": False,    # optional
    "candidate_min_bull_pct": 51,         # delta filter when available

    # Nearby OB zones (important + possible bounce)
    "alert_nearby_ob": True,
    "nearby_ob_max_distance_pct": 0.35,   # max distance from close to nearest zone edge
    "nearby_ob_max_results": 3,
    "nearby_ob_requires_delta": True,

    # Scanner acceleration (parallel + anti-burst)
    "scan_parallel": True,
    "scan_concurrency": 4,
    "scan_worker_spacing_ms": 120,
    "scan_worker_jitter_ms": 80,

    # Live loop
    "live_loop": False,
    "loop_minutes": 5,
}


# ============================================================
# ================== PERSISTENT CACHE PATHS ===================
# ============================================================

def cache_path(filename: str) -> str:
    try:
        base = os.path.dirname(os.path.abspath(__file__))
        return os.path.join(base, filename)
    except Exception:
        return os.path.join(os.getcwd(), filename)

TOUCH_CACHE_FILE = cache_path("touched_obs_cache.json")
RETEST_CACHE_FILE = cache_path("retested_obs_cache.json")


def _load_cache(path: str) -> set[str]:
    if not os.path.exists(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return set(str(x) for x in data)
    except Exception:
        pass
    return set()


def _save_cache(path: str, cache: set[str]) -> None:
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(sorted(list(cache)), f, ensure_ascii=False, indent=2)
    except Exception:
        pass


FIRST_TOUCH_SEEN: set[str] = _load_cache(TOUCH_CACHE_FILE)
FIRST_RETEST_SEEN: set[str] = _load_cache(RETEST_CACHE_FILE)

# Prevent duplicate prints on same candle time across loops
LAST_ALERT_BAR_TIME: Dict[Tuple[str, str], int] = {}
ALERT_LOCK = threading.Lock()


# ============================================================
# =========================== DATA ============================
# ============================================================

@dataclass
class ObRec:
    left_index: int
    left_time: int
    created_index: int
    created_time: int
    top: float
    bottom: float
    is_bull: bool
    active: bool = True
    retested: bool = False
    retest_index: Optional[int] = None
    retest_time: Optional[int] = None
    invalid_index: Optional[int] = None
    invalid_time: Optional[int] = None
    bull_vol: float = 0.0
    bear_vol: float = 0.0
    total_vol: float = 0.0
    bull_pct: int = 50
    bear_pct: int = 50
    has_delta: bool = False


# ============================================================
# ========================= UTILITIES =========================
# ============================================================

def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def round_half_away_from_zero(x: float) -> int:
    # Pine math.round behavior
    if x >= 0:
        return int(math.floor(x + 0.5))
    else:
        return int(math.ceil(x - 0.5))


def tf_seconds(exchange: ccxt.Exchange, tf: str) -> Optional[int]:
    if not tf:
        return None
    try:
        return int(exchange.parse_timeframe(tf))
    except Exception:
        return None


def touches_zone(z_top: float, z_bot: float, c_high: float, c_low: float) -> bool:
    return (c_high >= z_bot) and (c_low <= z_top)


def should_emit_alert(symbol: str, event_type: str, bar_time_ms: int) -> bool:
    key = (symbol, event_type)
    with ALERT_LOCK:
        last = LAST_ALERT_BAR_TIME.get(key)
        if last is not None and last == bar_time_ms:
            return False
        LAST_ALERT_BAR_TIME[key] = bar_time_ms
        return True


def ob_passes_delta_filter(ob: ObRec) -> bool:
    if not CONFIG.get("ob_delta_filter_enabled", False):
        return True
    if not ob.has_delta:
        return False
    if ob.is_bull:
        return ob.bull_pct >= int(CONFIG.get("ob_min_bull_delta_pct", 70))
    return ob.bear_pct >= int(CONFIG.get("ob_min_bear_delta_pct", 70))


def ob_distance_pct(close_px: float, ob: ObRec) -> float:
    top = max(ob.top, ob.bottom)
    bottom = min(ob.top, ob.bottom)
    if bottom <= close_px <= top:
        return 0.0
    edge = bottom if close_px < bottom else top
    return abs((edge - close_px) / close_px) * 100.0 if close_px != 0 else float("inf")


def format_nearby_ob(symbol: str, close_px: float, ob: ObRec, distance_pct: float) -> str:
    side = "BULL" if ob.is_bull else "BEAR"
    return (
        f"[NEAR OB] {symbol} close={close_px} | {side} OB top={ob.top} bottom={ob.bottom} "
        f"| dist={distance_pct:.3f}% | delta(B/S)={ob.bull_pct}/{ob.bear_pct}"
    )


def round_to_tick(price: float, tick: float) -> float:
    if tick <= 0:
        return price
    return round(price / tick) * tick


def stable_ob_key(symbol: str, ob: ObRec, tick: float) -> str:
    # Stable identity across loops/restarts:
    top = round_to_tick(ob.top, tick)
    bot = round_to_tick(ob.bottom, tick)
    return f"{symbol}|{ob.created_time}|{'B' if ob.is_bull else 'S'}|{top:.10f}|{bot:.10f}"


def format_touch(symbol: str, close_px: float, ob: ObRec, touch_time_ms: int) -> str:
    side = "BULL" if ob.is_bull else "BEAR"
    return (
        f"[TOUCH FIRST] {symbol} close={close_px} | {side} OB "
        f"top={ob.top} bottom={ob.bottom} | created_time={ob.created_time} | touch_time={touch_time_ms}"
    )


def format_triangle(symbol: str, ob: ObRec, retest_time_ms: int) -> str:
    tri = "▲" if ob.is_bull else "▼"
    side = "BULL" if ob.is_bull else "BEAR"
    return (
        f"{tri} {symbol} | {side} OB RETEST | top={ob.top} bottom={ob.bottom} "
        f"| created_time={ob.created_time} | retest_time={retest_time_ms}"
    )


def format_broken_ob(symbol: str, ob: ObRec, invalid_time_ms: int) -> str:
    side = "BULL" if ob.is_bull else "BEAR"
    return (
        f"[BROKEN OB] {symbol} | {side} | top={ob.top} bottom={ob.bottom} "
        f"| created_time={ob.created_time} | invalid_time={invalid_time_ms} | delta(B/S)={ob.bull_pct}/{ob.bear_pct}"
    )


def colorize(text: str, color: str) -> str:
    return f"{color}{text}{ANSI_RESET}"


def print_boxed_section(title: str, rows: List[str], color: str) -> None:
    if not rows:
        return
    width = max(len(title), *(len(r) for r in rows)) + 2
    top = f"┌{'─' * width}┐"
    mid = f"├{'─' * width}┤"
    bot = f"└{'─' * width}┘"
    print(colorize(top, color))
    print(colorize(f"│ {title.ljust(width - 1)}│", color))
    print(colorize(mid, color))
    for r in rows:
        print(colorize(f"│ {r.ljust(width - 1)}│", color))
    print(colorize(bot, color))


# ============================================================
# ===================== MARKET TICK SIZE ======================
# ============================================================

def get_tick_size_from_market(exchange: ccxt.Exchange, symbol: str) -> float:
    try:
        m = exchange.market(symbol)
    except Exception:
        m = None

    if m and "info" in m and m["info"]:
        info = m["info"]
        filters = info.get("filters")
        if isinstance(filters, list):
            for f in filters:
                if not isinstance(f, dict):
                    continue
                if f.get("filterType") == "PRICE_FILTER":
                    ts = f.get("tickSize")
                    tsv = safe_float(ts, 0.0)
                    if tsv > 0:
                        return tsv

    if m and "precision" in m and isinstance(m["precision"], dict):
        p = m["precision"].get("price")
        try:
            return float(10 ** (-int(p)))
        except Exception:
            pass

    return 1e-8


# ============================================================
# ====================== FETCH OHLCV (HTF) ====================
# ============================================================

def fetch_ohlcv_paged_backwards(
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    limit_target: int,
    batch_limit: int,
) -> List[List[float]]:
    try:
        batch = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=None, limit=batch_limit)
        if not batch:
            return []
        all_candles = batch[:]
    except Exception:
        return []

    if len(all_candles) >= limit_target:
        return all_candles[-limit_target:]

    tf_sec = tf_seconds(exchange, timeframe)
    if not tf_sec:
        return all_candles

    while len(all_candles) < limit_target:
        first_ts = all_candles[0][0]
        step_ms = batch_limit * tf_sec * 1000
        since = max(0, first_ts - step_ms)

        try:
            older = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=batch_limit)
        except Exception:
            break

        if not older:
            break

        older = [c for c in older if c[0] < first_ts]
        if not older:
            break

        all_candles = older + all_candles

        if CONFIG["rate_limit_sleep"]:
            exchange.sleep(exchange.rateLimit)

    return all_candles[-limit_target:]


# ============================================================
# ==================== FETCH OHLCV RANGE (LTF) =================
# ============================================================

def fetch_ohlcv_range_forward(
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    since_ms: int,
    until_ms: int,
    batch_limit: int,
) -> List[List[float]]:
    tf_sec = tf_seconds(exchange, timeframe)
    if not tf_sec:
        return []
    tf_ms = tf_sec * 1000

    out: List[List[float]] = []
    cursor = max(0, int(since_ms))

    while cursor < until_ms:
        try:
            chunk = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=cursor, limit=batch_limit)
        except Exception:
            break
        if not chunk:
            break

        for c in chunk:
            if since_ms <= c[0] < until_ms:
                out.append(c)

        last_ts = chunk[-1][0]
        next_cursor = last_ts + tf_ms
        if next_cursor <= cursor:
            next_cursor = cursor + tf_ms
        cursor = next_cursor

        if CONFIG["rate_limit_sleep"]:
            exchange.sleep(exchange.rateLimit)

    if out:
        out.sort(key=lambda x: x[0])
        dedup: List[List[float]] = []
        prev_ts = None
        for c in out:
            if prev_ts is None or c[0] != prev_ts:
                dedup.append(c)
                prev_ts = c[0]
        out = dedup
    return out


# ============================================================
# ============== VOLUME DELTA ENGINE (CHART OR LTF) ============
# ============================================================

def build_ltf_vols_for_htf(
    exchange: ccxt.Exchange,
    symbol: str,
    htf_candles: List[List[float]],
    htf_tf: str,
    ltf_tf: str,
    batch_limit: int,
) -> Tuple[List[float], List[float], List[float]]:
    if not ltf_tf:
        tot, bull, bear = [], [], []
        for ts, o, h, l, c, v in htf_candles:
            vv = float(v)
            tot.append(vv)
            bull.append(vv if c > o else 0.0)
            bear.append(vv if c < o else 0.0)
        return tot, bull, bear

    htf_sec = tf_seconds(exchange, htf_tf)
    ltf_sec = tf_seconds(exchange, ltf_tf)
    if not htf_sec or not ltf_sec or ltf_sec >= htf_sec:
        tot, bull, bear = [], [], []
        for ts, o, h, l, c, v in htf_candles:
            vv = float(v)
            tot.append(vv)
            bull.append(vv if c > o else 0.0)
            bear.append(vv if c < o else 0.0)
        return tot, bull, bear

    htf_start = int(htf_candles[0][0])
    htf_end = int(htf_candles[-1][0] + htf_sec * 1000)

    buffer_candles = int(CONFIG.get("LTF_EXTRA_BUFFER_CANDLES", 2000))
    ltf_ms = ltf_sec * 1000
    buf_ms = buffer_candles * ltf_ms

    since = max(0, htf_start - buf_ms)
    until = htf_end + buf_ms

    ltf_candles = fetch_ohlcv_range_forward(exchange, symbol, ltf_tf, since, until, batch_limit)
    ltf_candles.sort(key=lambda x: x[0])

    n = len(ltf_candles)
    i = 0

    tot_out: List[float] = []
    bull_out: List[float] = []
    bear_out: List[float] = []

    for htf in htf_candles:
        win_start = int(htf[0])
        win_end = win_start + htf_sec * 1000

        while i < n and ltf_candles[i][0] < win_start:
            i += 1

        t_list, b_list, s_list = [], [], []
        j = i
        while j < n and ltf_candles[j][0] < win_end:
            _, o2, _, _, c2, v2 = ltf_candles[j]
            vv = float(v2)
            t_list.append(vv)
            if c2 > o2:
                b_list.append(vv)
            elif c2 < o2:
                s_list.append(vv)
            j += 1

        tot_out.append(math.fsum(t_list))
        bull_out.append(math.fsum(b_list))
        bear_out.append(math.fsum(s_list))

    return tot_out, bull_out, bear_out


# ============================================================
# ======================= POC ENGINE (PINE-LIKE) ==============
# ============================================================

def find_most_touched_price(
    highs: List[float],
    lows: List[float],
    from_idx: int,
    to_idx: int,
    n_bins: int,
    tick_size: float,
) -> Tuple[Optional[float], int]:
    span = to_idx - from_idx
    if span <= 0:
        return None, 0

    min_p = float("inf")
    max_p = float("-inf")
    for idx in range(from_idx, to_idx + 1):
        lo = lows[idx]
        hi = highs[idx]
        if lo < min_p:
            min_p = lo
        if hi > max_p:
            max_p = hi

    if not (min_p < max_p):
        return None, 0

    step = (max_p - min_p) / float(n_bins)
    step = tick_size if step <= 0 else max(step, tick_size)

    diff_cnt = [0.0] * n_bins
    for idx in range(from_idx, to_idx + 1):
        lo2 = lows[idx]
        hi2 = highs[idx]
        s_bin = int(math.floor((lo2 - min_p) / step))
        e_bin = int(math.floor((hi2 - min_p) / step))
        s_bin = max(0, min(n_bins - 1, s_bin))
        e_bin = max(0, min(n_bins - 1, e_bin))
        diff_cnt[s_bin] += 1.0
        if e_bin + 1 < n_bins:
            diff_cnt[e_bin + 1] -= 1.0

    best_bin = 0
    best_cnt = 0.0
    run_cnt = 0.0
    for i in range(n_bins):
        run_cnt += diff_cnt[i]
        if run_cnt > best_cnt:
            best_cnt = run_cnt
            best_bin = i

    poc = min_p + (best_bin + 0.5) * step
    return poc, int(best_cnt)


def volume_at_price(
    highs: List[float],
    lows: List[float],
    bar_tot_vol: List[float],
    bar_bull_vol: List[float],
    bar_bear_vol: List[float],
    from_idx: int,
    to_idx: int,
    poc: Optional[float],
) -> Tuple[int, float, float, float]:
    if poc is None:
        return 0, 0.0, 0.0, 0.0

    touches = 0
    tot_list, bull_list, bear_list = [], [], []
    for idx in range(from_idx, to_idx + 1):
        lo = lows[idx]
        hi = highs[idx]
        if lo <= poc <= hi:
            touches += 1
            tot_list.append(float(bar_tot_vol[idx] or 0.0))
            bull_list.append(float(bar_bull_vol[idx] or 0.0))
            bear_list.append(float(bar_bear_vol[idx] or 0.0))

    return touches, math.fsum(tot_list), math.fsum(bull_list), math.fsum(bear_list)


def calc_most_touched_price_vol(
    highs: List[float],
    lows: List[float],
    bar_tot_vol: List[float],
    bar_bull_vol: List[float],
    bar_bear_vol: List[float],
    from_idx: int,
    to_idx: int,
    n_bins: int,
    tick_size: float,
) -> Tuple[Optional[float], int, float, float, float]:
    poc, _ = find_most_touched_price(highs, lows, from_idx, to_idx, n_bins, tick_size)
    touches, tot, bull, bear = volume_at_price(highs, lows, bar_tot_vol, bar_bull_vol, bar_bear_vol, from_idx, to_idx, poc)
    return poc, touches, tot, bull, bear


# ============================================================
# ===================== OB ENGINE HELPERS ======================
# ============================================================

def ob_overlaps_active(obs: List[ObRec], zone_top: float, zone_bottom: float) -> bool:
    z_top = max(zone_top, zone_bottom)
    z_bot = min(zone_top, zone_bottom)
    for ob in obs:
        if ob.active:
            o_top = max(ob.top, ob.bottom)
            o_bot = min(ob.top, ob.bottom)
            if (z_top >= o_bot) and (z_bot <= o_top):
                return True
    return False


def has_gap_between(highs: List[float], lows: List[float], anchor_idx: int, bos_idx: int, is_bull: bool) -> bool:
    f = min(anchor_idx, bos_idx)
    t = max(anchor_idx, bos_idx)
    if t - f >= 1:
        for abs_idx in range(f + 1, t + 1):
            hi_prev = highs[abs_idx - 1]
            lo_prev = lows[abs_idx - 1]
            hi_now = highs[abs_idx]
            lo_now = lows[abs_idx]
            if is_bull:
                if lo_now > hi_prev:
                    return True
            else:
                if hi_now < lo_prev:
                    return True
    return False


def prune_obs(obs: List[ObRec], max_stored: int) -> None:
    while len(obs) > max_stored:
        removed = False
        for j in range(len(obs) - 1, -1, -1):
            if not obs[j].active:
                obs.pop(j)
                removed = True
                break
        if not removed:
            obs.pop()


def add_ob_from_poc(
    obs: List[ObRec],
    base_idx: int,
    base_time: int,
    top: float,
    bottom: float,
    is_bull: bool,
    tot_seed: float,
    bull_seed: float,
    bear_seed: float,
    created_idx: int,
    created_time: int,
    max_stored: int,
) -> ObRec:
    tot = float(tot_seed)
    b = float(bull_seed)
    s = float(bear_seed)
    has_delta = tot > 0.0

    bull_raw = (b / tot) * 100.0 if has_delta else 50.0
    bull_pct = round_half_away_from_zero(bull_raw) if has_delta else 50
    bull_pct = max(0, min(100, bull_pct))
    bear_pct = 100 - bull_pct if has_delta else 50

    ob = ObRec(
        left_index=base_idx,
        left_time=base_time,
        created_index=created_idx,
        created_time=created_time,
        top=float(top),
        bottom=float(bottom),
        is_bull=bool(is_bull),
        active=True,
        bull_vol=b,
        bear_vol=s,
        total_vol=tot,
        bull_pct=bull_pct,
        bear_pct=bear_pct,
        has_delta=has_delta,
    )
    obs.insert(0, ob)
    prune_obs(obs, max_stored)
    return ob


# ============================================================
# ===================== MAIN RUN (PER SYMBOL) ==================
# ============================================================


def run_symbol(symbol: str, exchange: ccxt.Exchange) -> Tuple[List[Dict[str, Any]], bool, float]:
    """
    Pine-match engine for:
    - ta.pivothigh/ta.pivotlow (left=swing_len, right=swing_len)
    - BOS confirmation and OB creation on the NEXT bar: bosBear = bosBearNow[1]
    - fromIdxBear = slIdx[1], fromIdxBull = shIdx[1]
    - POC + volume-at-POC aggregation identical in spirit to the Pine script
    - Invalidation + Retest logic matches Pine (uses [1] candle for retest, min-gap=4)
    Returns:
      messages_to_print, bullish_candidate_flag, candidate_score
    NOTE: Touch / candidate logic remains configurable, but defaults can be disabled in CONFIG.
    """
    tf = CONFIG["timeframe"]
    vd_tf = CONFIG.get("vd_timeframe", "")
    bars_back = int(CONFIG["bars_back"])
    batch_limit = int(CONFIG["fetch_batch_limit"])

    tick_size = get_tick_size_from_market(exchange, symbol)
    swing_len = int(CONFIG["swing_len"])
    inv_method = str(CONFIG["invalidation"])
    max_stored = int(CONFIG["max_stored_obs"])
    poc_bins = int(CONFIG["poc_bins"])

    htf = fetch_ohlcv_paged_backwards(exchange, symbol, tf, bars_back, batch_limit)
    if len(htf) < (swing_len * 2 + 10):
        return [], False, 0.0

    ts = [int(x[0]) for x in htf]
    o = [float(x[1]) for x in htf]
    h = [float(x[2]) for x in htf]
    l = [float(x[3]) for x in htf]
    c = [float(x[4]) for x in htf]

    # Volume delta engine (chart TF or lower TF like Pine vdTfIn)
    bar_tot_vol, bar_bull_vol, bar_bear_vol = build_ltf_vols_for_htf(
        exchange, symbol, htf, tf, vd_tf, batch_limit
    )

    last_idx = len(htf) - 1

    # These are for optional candidate output (kept for compatibility)
    bullish_candidate = False
    candidate_score = 0.0

    # --- Pivot helpers ---
    # IMPORTANT: ta.pivothigh/low accept ties; they test center == highest/lowest of the window.
    def pivot_high_at(i: int) -> bool:
        seg = h[i - swing_len: i + swing_len + 1]
        center = h[i]
        return center == max(seg)

    def pivot_low_at(i: int) -> bool:
        seg = l[i - swing_len: i + swing_len + 1]
        center = l[i]
        return center == min(seg)

    # --- Formatting for Pine-like alerts ---
    def format_new_ob(ob: ObRec) -> str:
        side = "BULL" if ob.is_bull else "BEAR"
        return (
            f"[NEW OB] {symbol} | {side} | top={ob.top} bottom={ob.bottom} "
            f"| created_time={ob.created_time} | delta(B/S)={ob.bull_pct}/{ob.bear_pct}"
        )

    # --- Retest formatting already exists: format_triangle(...) ---

    out_events: List[Dict[str, Any]] = []

    def add_event(kind: str, message: str, color: str, priority: float) -> None:
        out_events.append({
            "kind": kind,
            "message": message,
            "color": color,
            "priority": float(priority),
        })

    # True SMC state variables (mirrors Pine logic in 3d.txt)
    unconfirmed_h: Optional[float] = None
    unconfirmed_h_idx: Optional[int] = None
    unconfirmed_l: Optional[float] = None
    unconfirmed_l_idx: Optional[int] = None

    confirmed_h: Optional[float] = None
    confirmed_h_idx: Optional[int] = None
    confirmed_l: Optional[float] = None
    confirmed_l_idx: Optional[int] = None

    current_idm_l: Optional[float] = None
    current_idm_h: Optional[float] = None

    bos_bear_now_prev = False
    bos_bull_now_prev = False
    origin_bear_idx_prev: Optional[int] = None
    origin_bull_idx_prev: Optional[int] = None

    last_bull_retest_bar: Optional[int] = None
    last_bear_retest_bar: Optional[int] = None

    obs: List[ObRec] = []

    # Event flags (Pine: reset each bar)
    ev_new_bull_ob = False
    ev_new_bear_ob = False
    ev_bull_retest = False
    ev_bear_retest = False

    for bi in range(len(htf)):
        # Reset per-bar event flags like Pine
        ev_new_bull_ob = False
        ev_new_bear_ob = False
        ev_bull_retest = False
        ev_bear_retest = False

        # Keep previous origins for Pine-style [1] behavior.
        from_idx_bear = origin_bear_idx_prev
        from_idx_bull = origin_bull_idx_prev

        # 1) Register standard swings (ta.pivothigh/low with left/right = swing_len)
        if bi >= swing_len:
            pivot_idx = bi - swing_len
            if pivot_idx - swing_len >= 0 and pivot_idx + swing_len < len(htf):
                if pivot_high_at(pivot_idx):
                    unconfirmed_h = h[pivot_idx]
                    unconfirmed_h_idx = pivot_idx
                    current_idm_l = None
                if pivot_low_at(pivot_idx):
                    unconfirmed_l = l[pivot_idx]
                    unconfirmed_l_idx = pivot_idx
                    current_idm_h = None

        # 2) Register minor swings for IDM (ta.pivothigh/low with left/right = 1)
        if bi >= 2:
            minor_idx = bi - 1
            minor_pl = l[minor_idx] if l[minor_idx] <= l[minor_idx - 1] and l[minor_idx] <= l[minor_idx + 1] else None
            minor_ph = h[minor_idx] if h[minor_idx] >= h[minor_idx - 1] and h[minor_idx] >= h[minor_idx + 1] else None

            if minor_pl is not None and unconfirmed_h_idx is not None and bi > (unconfirmed_h_idx + 1):
                if current_idm_l is None or minor_pl > current_idm_l:
                    current_idm_l = minor_pl

            if minor_ph is not None and unconfirmed_l_idx is not None and bi > (unconfirmed_l_idx + 1):
                if current_idm_h is None or minor_ph < current_idm_h:
                    current_idm_h = minor_ph

        # 3) Confirm structure once IDM is taken.
        if unconfirmed_h is not None and current_idm_l is not None and l[bi] < current_idm_l:
            confirmed_h = unconfirmed_h
            confirmed_h_idx = unconfirmed_h_idx
            unconfirmed_h = None

        if unconfirmed_l is not None and current_idm_h is not None and h[bi] > current_idm_h:
            confirmed_l = unconfirmed_l
            confirmed_l_idx = unconfirmed_l_idx
            unconfirmed_l = None

        # 4) Detect BOS now on confirmed levels.
        bos_bear_now = False
        bos_bull_now = False
        origin_bear_idx_now: Optional[int] = None
        origin_bull_idx_now: Optional[int] = None

        if (
            confirmed_h is not None
            and confirmed_h_idx is not None
            and bi > 0
            and c[bi] > confirmed_h
            and c[bi - 1] <= confirmed_h
        ):
            bos_bull_now = True
            origin_bull_idx_now = confirmed_h_idx
            confirmed_h = None
            confirmed_h_idx = None

        if (
            confirmed_l is not None
            and confirmed_l_idx is not None
            and bi > 0
            and c[bi] < confirmed_l
            and c[bi - 1] >= confirmed_l
        ):
            bos_bear_now = True
            origin_bear_idx_now = confirmed_l_idx
            confirmed_l = None
            confirmed_l_idx = None

        # Pine: bosBear = bosBearNow[1], bosBull = bosBullNow[1]
        bos_bear = bos_bear_now_prev
        bos_bull = bos_bull_now_prev

        # Precompute BOS indices like Pine (bosIdxBear = bar_index - 1)
        bos_idx_bear = bi - 1
        bos_idx_bull = bi - 1

        # Bearish BOS → Bearish OB
        if bos_bear:
            from_idx = from_idx_bear
            to_idx = bos_idx_bear
            if from_idx is not None and 0 <= from_idx <= to_idx < len(htf):
                poc, touches, tot_vol, bull_vol, bear_vol = calc_most_touched_price_vol(
                    h, l, bar_tot_vol, bar_bull_vol, bar_bear_vol, from_idx, to_idx, poc_bins, tick_size
                )
                if poc is not None and touches > 0:
                    span_b = to_idx - from_idx
                    best_idx = None
                    run_max_high = None
                    for step in range(span_b + 1):
                        idx = to_idx - step
                        hi = h[idx]
                        lo_ = l[idx]
                        run_max_high = hi if run_max_high is None else max(run_max_high, hi)
                        touches_poc = (lo_ <= poc <= hi)
                        if touches_poc and hi == run_max_high:
                            best_idx = idx

                    gap_leg = has_gap_between(h, l, best_idx, bos_idx_bear, is_bull=False) if best_idx is not None else False
                    if best_idx is not None and not gap_leg:
                        top = h[best_idx]
                        bottom = l[best_idx]
                        if not ob_overlaps_active(obs, top, bottom):
                            ob = add_ob_from_poc(obs, best_idx, ts[best_idx], top, bottom, False, tot_vol, bull_vol, bear_vol, bos_idx_bear, ts[bos_idx_bear], max_stored)
                            ev_new_bear_ob = True
        # Bullish BOS → Bullish OB
        if bos_bull:
            from_idx = from_idx_bull
            to_idx = bos_idx_bull
            if from_idx is not None and 0 <= from_idx <= to_idx < len(htf):
                poc, touches, tot_vol, bull_vol, bear_vol = calc_most_touched_price_vol(
                    h, l, bar_tot_vol, bar_bull_vol, bar_bear_vol, from_idx, to_idx, poc_bins, tick_size
                )
                if poc is not None and touches > 0:
                    span_h = to_idx - from_idx
                    best_idx2 = None
                    run_min_low = None
                    for step in range(span_h + 1):
                        idx2 = to_idx - step
                        lo2 = l[idx2]
                        hi2 = h[idx2]
                        run_min_low = lo2 if run_min_low is None else min(run_min_low, lo2)
                        touches_poc = (lo2 <= poc <= hi2)
                        if touches_poc and lo2 == run_min_low:
                            best_idx2 = idx2

                    gap_leg2 = has_gap_between(h, l, best_idx2, bos_idx_bull, is_bull=True) if best_idx2 is not None else False
                    if best_idx2 is not None and not gap_leg2:
                        top2 = h[best_idx2]
                        bottom2 = l[best_idx2]
                        if not ob_overlaps_active(obs, top2, bottom2):
                            ob = add_ob_from_poc(obs, best_idx2, ts[best_idx2], top2, bottom2, True, tot_vol, bull_vol, bear_vol, bos_idx_bull, ts[bos_idx_bull], max_stored)
                            ev_new_bull_ob = True
        # Update prev BOS/origin states for next bar (Pine [1] semantics).
        bos_bear_now_prev = bos_bear_now
        bos_bull_now_prev = bos_bull_now
        origin_bear_idx_prev = origin_bear_idx_now
        origin_bull_idx_prev = origin_bull_idx_now

        # Invalidation + retest detection for existing OBs (matches Pine)
        if obs and bi > 0:
            for ob in obs:
                if not ob.active:
                    continue

                # Invalidation
                invalid = False
                if inv_method == "Wick":
                    if ob.is_bull:
                        invalid = l[bi] < ob.bottom
                        inv_idx = bi
                        inv_time = ts[bi]
                    else:
                        invalid = h[bi] > ob.top
                        inv_idx = bi
                        inv_time = ts[bi]
                else:
                    if ob.is_bull:
                        invalid = c[bi - 1] < ob.bottom
                        inv_idx = bi - 1
                        inv_time = ts[bi - 1]
                    else:
                        invalid = c[bi - 1] > ob.top
                        inv_idx = bi - 1
                        inv_time = ts[bi - 1]

                if invalid:
                    ob.active = False
                    ob.invalid_index = inv_idx
                    ob.invalid_time = inv_time
                    if bi == last_idx and ob_passes_delta_filter(ob):
                        ev_key = f"BROKEN_{'B' if ob.is_bull else 'S'}_{ob.created_time}"
                        if should_emit_alert(symbol, ev_key, ts[bi]):
                            delta_strength = max(ob.bull_pct, ob.bear_pct)
                            add_event(
                                "broken",
                                format_broken_ob(symbol, ob, inv_time),
                                ANSI_YELLOW,
                                400.0 + delta_strength,
                            )
                    continue

                # RetestPrev uses candle [1]
                retest_prev = False
                retest_bar = bi - 1
                if ob.is_bull:
                    opens_above_prev = o[retest_bar] > ob.top
                    closes_above_prev = c[retest_bar] > ob.top
                    wick_touches_prev = (l[retest_bar] <= ob.top) and (l[retest_bar] >= ob.bottom)
                    retest_prev = opens_above_prev and closes_above_prev and wick_touches_prev
                else:
                    opens_below_prev = o[retest_bar] < ob.bottom
                    closes_below_prev = c[retest_bar] < ob.bottom
                    wick_touches_prev = (h[retest_bar] >= ob.bottom) and (h[retest_bar] <= ob.top)
                    retest_prev = opens_below_prev and closes_below_prev and wick_touches_prev

                if retest_prev and retest_bar > ob.created_index:
                    ob.retested = True
                    ob.retest_index = retest_bar
                    ob.retest_time = ts[retest_bar]

                    last_side_bar = last_bull_retest_bar if ob.is_bull else last_bear_retest_bar
                    can_log = (last_side_bar is None) or ((retest_bar - last_side_bar) >= 4)

                    if can_log:
                        if ob.is_bull:
                            ev_bull_retest = True
                            last_bull_retest_bar = retest_bar
                        else:
                            ev_bear_retest = True
                            last_bear_retest_bar = retest_bar

        # Emit alerts ONLY on last candle (scanner behavior), and only once per candle time.
        if bi == last_idx:
            bar_time_ms = ts[bi]

            if CONFIG.get("alert_new_ob", True) and ev_new_bull_ob and should_emit_alert(symbol, "NEW_BULL_OB", bar_time_ms):
                # Print ONE per bar like Pine alert
                # pick the newest bull OB for message
                for ob in obs:
                    if ob.is_bull and ob.created_index == bos_idx_bull and ob_passes_delta_filter(ob):
                        delta_strength = max(ob.bull_pct, ob.bear_pct)
                        add_event("new", format_new_ob(ob), ANSI_GREEN, 500.0 + delta_strength)
                        break

            if CONFIG.get("alert_new_ob", True) and ev_new_bear_ob and should_emit_alert(symbol, "NEW_BEAR_OB", bar_time_ms):
                for ob in obs:
                    if (not ob.is_bull) and ob.created_index == bos_idx_bear and ob_passes_delta_filter(ob):
                        delta_strength = max(ob.bull_pct, ob.bear_pct)
                        add_event("new", format_new_ob(ob), ANSI_GREEN, 500.0 + delta_strength)
                        break

            if CONFIG.get("alert_retest", True) and ev_bull_retest and should_emit_alert(symbol, "BULL_RETEST", bar_time_ms):
                # Find most recent bull retest OB
                for ob in obs:
                    if (
                        ob.is_bull
                        and ob.retest_time is not None
                        and ob.retest_time == ts[bi - 1]
                        and ob_passes_delta_filter(ob)
                    ):
                        delta_strength = max(ob.bull_pct, ob.bear_pct)
                        add_event("retest", format_triangle(symbol, ob, ts[bi - 1]), ANSI_CYAN, 700.0 + delta_strength)
                        break

            if CONFIG.get("alert_retest", True) and ev_bear_retest and should_emit_alert(symbol, "BEAR_RETEST", bar_time_ms):
                for ob in obs:
                    if (
                        (not ob.is_bull)
                        and ob.retest_time is not None
                        and ob.retest_time == ts[bi - 1]
                        and ob_passes_delta_filter(ob)
                    ):
                        delta_strength = max(ob.bull_pct, ob.bear_pct)
                        add_event("retest", format_triangle(symbol, ob, ts[bi - 1]), ANSI_CYAN, 700.0 + delta_strength)
                        break

            if CONFIG.get("alert_nearby_ob", True) and c[bi] > 0:
                max_dist_pct = safe_float(CONFIG.get("nearby_ob_max_distance_pct", 0.35), 0.35)
                max_results = max(1, int(CONFIG.get("nearby_ob_max_results", 3)))
                req_delta = bool(CONFIG.get("nearby_ob_requires_delta", True))

                nearby: List[Tuple[float, ObRec]] = []
                for ob in obs:
                    if not ob.active:
                        continue
                    if req_delta and not ob_passes_delta_filter(ob):
                        continue
                    d = ob_distance_pct(c[bi], ob)
                    if d <= max_dist_pct:
                        nearby.append((d, ob))

                nearby.sort(key=lambda x: x[0])
                for d, ob in nearby[:max_results]:
                    event_key = f"NEAR_{'B' if ob.is_bull else 'S'}_{ob.created_time}"
                    if should_emit_alert(symbol, event_key, bar_time_ms):
                        delta_strength = max(ob.bull_pct, ob.bear_pct)
                        add_event(
                            "near",
                            format_nearby_ob(symbol, c[bi], ob, d),
                            ANSI_RED,
                            1000.0 - (d * 100.0) + delta_strength,
                        )

    return out_events, bullish_candidate, candidate_score


def sort_and_filter_symbols_high_first(exchange: ccxt.Exchange, symbols: List[str]) -> List[str]:
    if not CONFIG.get("high_coins_first", True) or not symbols:
        return symbols

    sort_by = str(CONFIG.get("sort_by", "quoteVolume"))
    min_qv = safe_float(CONFIG.get("min_quote_volume_24h", 0.0), 0.0)
    min_abs_pct = safe_float(CONFIG.get("min_abs_change_24h_pct", 0.0), 0.0)

    tickers: Dict[str, Any] = {}
    try:
        tickers = exchange.fetch_tickers(symbols)
    except Exception:
        try:
            tickers = exchange.fetch_tickers()
        except Exception:
            tickers = {}

    scored: List[Tuple[float, str]] = []
    for sym in symbols:
        t = tickers.get(sym) if tickers else None
        if not t:
            scored.append((0.0, sym))
            continue

        quote_vol = safe_float(t.get("quoteVolume"), 0.0)
        pct = safe_float(t.get("percentage"), 0.0)
        abs_pct = abs(pct)

        if min_qv > 0 and quote_vol < min_qv:
            continue
        if min_abs_pct > 0 and abs_pct < min_abs_pct:
            continue

        if sort_by == "quoteVolume":
            score = quote_vol
        elif sort_by == "absPercentage":
            score = abs_pct
        elif sort_by == "percentage":
            score = pct
        else:
            score = quote_vol

        scored.append((score, sym))

    scored.sort(key=lambda x: x[0], reverse=True)
    sorted_syms = [s for _, s in scored]

    top_n = int(CONFIG.get("top_n_after_sort", 0) or 0)
    if top_n > 0:
        sorted_syms = sorted_syms[:top_n]

    return sorted_syms


# ============================================================
# =========================== SCANNER =========================
# ============================================================

def scan_binance_usdtm() -> None:
    exchange = getattr(ccxt, CONFIG["exchange_id"])({
        "enableRateLimit": True,
        "options": {"defaultType": "future"},
    })

    markets = exchange.load_markets()

    symbols: List[str] = []
    for sym, m in markets.items():
        try:
            if not m.get("contract"):
                continue
            if m.get("linear") is False:
                continue
            if m.get("quote") != CONFIG["quote"]:
                continue
            if m.get("active") is False:
                continue
            symbols.append(sym)
        except Exception:
            continue

    if CONFIG.get("scan_limit_symbols", 0) and int(CONFIG["scan_limit_symbols"]) > 0:
        symbols = symbols[: int(CONFIG["scan_limit_symbols"])]

    symbols = sort_and_filter_symbols_high_first(exchange, symbols)

    candidates: List[Tuple[float, str]] = []
    all_events: List[Dict[str, Any]] = []

    def worker(sym: str, slot: int) -> Tuple[str, List[Dict[str, Any]], bool, float]:
        base_sleep = max(0, int(CONFIG.get("scan_worker_spacing_ms", 120)))
        jitter = max(0, int(CONFIG.get("scan_worker_jitter_ms", 80)))
        delay_ms = (slot * base_sleep) + (random.randint(0, jitter) if jitter > 0 else 0)
        if delay_ms > 0:
            pytime.sleep(delay_ms / 1000.0)

        ex = getattr(ccxt, CONFIG["exchange_id"])({
            "enableRateLimit": True,
            "options": {"defaultType": "future"},
        })
        ex.markets = exchange.markets
        ex.markets_by_id = exchange.markets_by_id
        ex.symbols = exchange.symbols
        ex.currencies = exchange.currencies
        ex.currencies_by_id = exchange.currencies_by_id

        try:
            msgs, is_cand, score = run_symbol(sym, ex)
            return sym, msgs, is_cand, score
        except Exception:
            return sym, [], False, 0.0

    parallel = bool(CONFIG.get("scan_parallel", True))
    concurrency = max(1, int(CONFIG.get("scan_concurrency", 4)))

    if parallel and concurrency > 1 and len(symbols) > 1:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            future_map = {
                pool.submit(worker, sym, i % concurrency): sym
                for i, sym in enumerate(symbols)
            }
            for fut in as_completed(future_map):
                sym, msgs, is_cand, score = fut.result()
                all_events.extend(msgs)
                if CONFIG.get("print_candidates", True) and is_cand:
                    candidates.append((score, sym))
    else:
        for sym in symbols:
            sym, msgs, is_cand, score = worker(sym, 0)
            all_events.extend(msgs)
            if CONFIG.get("print_candidates", True) and is_cand:
                candidates.append((score, sym))

    # Organized Arabic output after collecting all symbols
    near_rows = [e["message"] for e in all_events if e.get("kind") == "near"]
    new_rows = [e["message"] for e in all_events if e.get("kind") == "new"]
    broken_rows = [e["message"] for e in all_events if e.get("kind") == "broken"]
    retest_rows = [e["message"] for e in all_events if e.get("kind") == "retest"]

    print_boxed_section("مناطق OB القريبة من السعر", near_rows, ANSI_RED)
    print_boxed_section("مناطق OB الجديدة", new_rows, ANSI_GREEN)
    print_boxed_section("مناطق OB المكسورة", broken_rows, ANSI_YELLOW)
    print_boxed_section("إشارات إعادة الاختبار", retest_rows, ANSI_CYAN)

    if all_events:
        ranked = sorted(all_events, key=lambda x: x.get("priority", 0.0), reverse=True)
        ranked_rows = [f"{i + 1}) {ev['message']}" for i, ev in enumerate(ranked[:30])]
        print_boxed_section("ترتيب أهم مناطق OB بنهاية المسح", ranked_rows, ANSI_CYAN)

    # print candidates at end of cycle
    if CONFIG.get("print_candidates", True) and candidates:
        candidates.sort(key=lambda x: x[0], reverse=True)
        top_n = int(CONFIG.get("candidates_top_n", 20))
        top = candidates[:top_n] if top_n > 0 else candidates

        print("\n=== BULLISH CANDIDATES (SETUPS, NOT GUARANTEED) ===")
        for score, sym in top:
            print(f"⭐ {sym} | score(bullPct)={score}")
        print("===============================================\n")


# ============================================================
# ============================ MAIN ===========================
# ============================================================

if __name__ == "__main__":
    if CONFIG.get("live_loop", False):
        loop_minutes = max(1, int(CONFIG.get("loop_minutes", 5)))
        while True:
            scan_binance_usdtm()
            pytime.sleep(loop_minutes * 60)
    else:
        scan_binance_usdtm()
