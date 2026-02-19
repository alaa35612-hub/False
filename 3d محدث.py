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
import ccxt


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
    "sort_by": "percentage",          # "quoteVolume" | "absPercentage" | "percentage"
    "min_quote_volume_24h": 2.0,      # set >0 to filter
    "min_abs_change_24h_pct": 2.0,    # set >0 to filter
    "min_change_24h_pct": 0.0,        # set >0 to require positive daily rise threshold
    "only_rising_24h": True,          # ignore coins with non-positive 24h change
    "top_n_after_sort": 0,        # 0 = all

    # Timeframes / bars
    "timeframe": "1m",
    "vd_timeframe": "",             # "" to use chart TF volume (no LTF)
    "trigger_timeframe": "1m",      # micro timeframe for first-touch execution
    "trigger_lookback_bars": 5,       # grace window in trigger TF (last X candles)
    "near_ob_max_distance_pct": 0.35, # print untouched OBs if price is close (percent)
    "near_ob_max_rows": 3,            # max nearby untouched OB rows per symbol
    "bars_back": 5000,
    "fetch_batch_limit": 1500,
    "fast_scan_mode": True,
    "fast_scan_bars_back": 1500,
    "sleep_after_symbol": False,

    # LTF coverage tuning
    "LTF_EXTRA_BUFFER_CANDLES": 2000,

    # Indicator params (match Pine defaults)
    "swing_len": 5,
    "STRICT_PIVOT": True,
    "invalidation": "Wick",           # "Wick" or "Close"
    "max_stored_obs": 50,
    "poc_bins": 40,

    # ==================== TOUCH ALERTS ====================
    "alert_touch_zone": True,
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


def is_inside_zone(price: float, z_top: float, z_bot: float) -> bool:
    top = max(z_top, z_bot)
    bot = min(z_top, z_bot)
    return bot <= price <= top


def should_emit_alert(symbol: str, event_type: str, bar_time_ms: int) -> bool:
    key = (symbol, event_type)
    last = LAST_ALERT_BAR_TIME.get(key)
    if last is not None and last == bar_time_ms:
        return False
    LAST_ALERT_BAR_TIME[key] = bar_time_ms
    return True


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


def format_trigger_touch(symbol: str, ob: ObRec, touch_time_ms: int) -> str:
    side = "BULL" if ob.is_bull else "BEAR"
    side_ar = "صاعد" if ob.is_bull else "هابط"
    entry = ob.top if ob.is_bull else ob.bottom
    return (
        f"[TOUCH FIRST][TRIGGER] {symbol} | ob_type={side}/{side_ar} | entry={entry} "
        f"| zone_top={ob.top} zone_bottom={ob.bottom} "
        f"| created_time_htf={ob.created_time} | touch_time_trigger={touch_time_ms}"
    )


def format_near_ob(symbol: str, ob: ObRec, last_price: float, distance_pct: float) -> str:
    side = "BULL" if ob.is_bull else "BEAR"
    side_ar = "صاعد" if ob.is_bull else "هابط"
    return (
        f"[NEAR OB] {symbol} | ob_type={side}/{side_ar} | last_price={last_price} "
        f"| zone_top={ob.top} zone_bottom={ob.bottom} | dist_pct={distance_pct:.4f}% "
        f"| created_time_htf={ob.created_time}"
    )


def format_triangle(symbol: str, ob: ObRec, retest_time_ms: int) -> str:
    tri = "▲" if ob.is_bull else "▼"
    side = "BULL" if ob.is_bull else "BEAR"
    return (
        f"{tri} {symbol} | {side} OB RETEST | top={ob.top} bottom={ob.bottom} "
        f"| created_time={ob.created_time} | retest_time={retest_time_ms}"
    )


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


def monitor_trigger_timeframe_touches(
    exchange: ccxt.Exchange,
    symbol: str,
    active_obs: List[ObRec],
    tick_size: float,
    pre_touched_keys: Optional[set[str]] = None,
) -> List[str]:
    if not active_obs:
        return []

    trigger_tf = str(CONFIG.get("trigger_timeframe", "1m") or "1m")
    lookback = max(1, int(CONFIG.get("trigger_lookback_bars", 5) or 5))
    fetch_limit = max(lookback, 2) + 2

    try:
        trigger_candles = exchange.fetch_ohlcv(symbol, timeframe=trigger_tf, since=None, limit=fetch_limit)
    except Exception:
        return []

    if len(trigger_candles) < 2:
        return []

    monitor_start_idx = max(1, len(trigger_candles) - lookback)
    out: List[str] = []
    near_candidates: List[Tuple[float, str]] = []
    cache_updated = False
    last_price = float(trigger_candles[-1][4])
    pre_touched = pre_touched_keys or set()

    for ob in active_obs:
        key = stable_ob_key(symbol, ob, tick_size)
        if key in FIRST_TOUCH_SEEN or key in pre_touched:
            continue

        z_top = max(ob.top, ob.bottom)
        z_bot = min(ob.top, ob.bottom)
        first_touch_idx: Optional[int] = None

        for idx in range(1, len(trigger_candles)):
            prev_close = float(trigger_candles[idx - 1][4])
            ts_ms, o_, hi, lo, c_, _ = trigger_candles[idx]
            if int(ts_ms) < int(ob.created_time):
                continue

            hi = float(hi)
            lo = float(lo)
            o_ = float(o_)
            c_ = float(c_)

            overlap = touches_zone(z_top, z_bot, hi, lo)
            boundary_touch = (hi >= z_top) or (lo <= z_bot)
            prev_inside = is_inside_zone(prev_close, z_top, z_bot)
            fully_inside_candle = (
                lo >= z_bot and hi <= z_top
                and is_inside_zone(o_, z_top, z_bot)
                and is_inside_zone(c_, z_top, z_bot)
            )

            if overlap and boundary_touch and (not prev_inside) and (not fully_inside_candle):
                first_touch_idx = idx
                break

        if first_touch_idx is not None:
            # If touch happened before current lookback window, mark as already touched and skip alert.
            if first_touch_idx < monitor_start_idx:
                FIRST_TOUCH_SEEN.add(key)
                cache_updated = True
                continue

            touch_time_ms = int(trigger_candles[first_touch_idx][0])
            FIRST_TOUCH_SEEN.add(key)
            cache_updated = True
            out.append(format_trigger_touch(symbol, ob, touch_time_ms))
            continue

        # untouched zone: print near-zone hint when price is close.
        if last_price > 0:
            if is_inside_zone(last_price, z_top, z_bot):
                continue
            if last_price < z_bot:
                distance_pct = ((z_bot - last_price) / last_price) * 100.0
            elif last_price > z_top:
                distance_pct = ((last_price - z_top) / last_price) * 100.0
            else:
                distance_pct = 0.0

            near_max = max(0.0, float(CONFIG.get("near_ob_max_distance_pct", 0.35) or 0.35))
            if 0.0 < distance_pct <= near_max:
                near_candidates.append((distance_pct, format_near_ob(symbol, ob, last_price, distance_pct)))

    if near_candidates:
        near_candidates.sort(key=lambda x: x[0])
        near_rows = max(0, int(CONFIG.get("near_ob_max_rows", 3) or 3))
        for _, msg in near_candidates[:near_rows]:
            out.append(msg)

    if cache_updated:
        _save_cache(TOUCH_CACHE_FILE, FIRST_TOUCH_SEEN)

    return out


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


def run_symbol(symbol: str, exchange: ccxt.Exchange) -> Tuple[List[str], bool, float]:
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
    Structural extraction stays on CONFIG["timeframe"], while first-touch execution can run
    on CONFIG["trigger_timeframe"] over the last CONFIG["trigger_lookback_bars"] candles.
    """
    tf = CONFIG["timeframe"]
    vd_tf = CONFIG.get("vd_timeframe", "")
    bars_back = int(CONFIG["bars_back"])
    if CONFIG.get("fast_scan_mode", False):
        bars_back = min(bars_back, int(CONFIG.get("fast_scan_bars_back", bars_back)))
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
            f"| created_time={ob.created_time}"
        )

    # --- Retest formatting already exists: format_triangle(...) ---

    out_msgs: List[str] = []

    # Pine state variables
    sh_price: Optional[float] = None
    sh_idx: Optional[int] = None
    sl_price: Optional[float] = None
    sl_idx: Optional[int] = None

    bos_bear_now_prev = False
    bos_bull_now_prev = False

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

        # Save previous sh/sl indices (equivalent to shIdx[1]/slIdx[1] when used with bosBear=bosBearNow[1])
        sh_idx_prev = sh_idx
        sl_idx_prev = sl_idx

        # Pivot confirmation happens swing_len bars after the pivot bar, like ta.pivothigh/low
        if bi >= swing_len:
            pivot_idx = bi - swing_len
            if pivot_idx - swing_len >= 0 and pivot_idx + swing_len < len(htf):
                if pivot_high_at(pivot_idx):
                    sh_price = h[pivot_idx]
                    sh_idx = pivot_idx
                if pivot_low_at(pivot_idx):
                    sl_price = l[pivot_idx]
                    sl_idx = pivot_idx

        # BOS now (Pine: uses current bar close and close[1])
        bos_bear_now = (
            sl_price is not None and sl_idx is not None and bi > sl_idx
            and c[bi] < sl_price and (c[bi - 1] >= sl_price if bi > 0 else False)
        )
        bos_bull_now = (
            sh_price is not None and sh_idx is not None and bi > sh_idx
            and c[bi] > sh_price and (c[bi - 1] <= sh_price if bi > 0 else False)
        )

        # Pine: bosBear = bosBearNow[1], bosBull = bosBullNow[1]
        bos_bear = bos_bear_now_prev
        bos_bull = bos_bull_now_prev

        # Precompute BOS indices like Pine (bosIdxBear = bar_index - 1)
        bos_idx_bear = bi - 1
        bos_idx_bull = bi - 1

        # fromIdxBear = slIdx[1], fromIdxBull = shIdx[1]
        from_idx_bear = sl_idx_prev
        from_idx_bull = sh_idx_prev

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
            # Pine resets swing low state after processing
            sl_price, sl_idx = None, None

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
            # Pine resets swing high state after processing
            sh_price, sh_idx = None, None

        # Update prev BOS states for next bar
        bos_bear_now_prev = bos_bear_now
        bos_bull_now_prev = bos_bull_now

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
                    if ob.is_bull and ob.created_index == bos_idx_bull:
                        out_msgs.append(format_new_ob(ob))
                        break

            if CONFIG.get("alert_new_ob", True) and ev_new_bear_ob and should_emit_alert(symbol, "NEW_BEAR_OB", bar_time_ms):
                for ob in obs:
                    if (not ob.is_bull) and ob.created_index == bos_idx_bear:
                        out_msgs.append(format_new_ob(ob))
                        break

            if CONFIG.get("alert_retest", True) and ev_bull_retest and should_emit_alert(symbol, "BULL_RETEST", bar_time_ms):
                # Find most recent bull retest OB
                for ob in obs:
                    if ob.is_bull and ob.retest_time is not None and ob.retest_time == ts[bi - 1]:
                        out_msgs.append(format_triangle(symbol, ob, ts[bi - 1]))
                        break

            if CONFIG.get("alert_retest", True) and ev_bear_retest and should_emit_alert(symbol, "BEAR_RETEST", bar_time_ms):
                for ob in obs:
                    if (not ob.is_bull) and ob.retest_time is not None and ob.retest_time == ts[bi - 1]:
                        out_msgs.append(format_triangle(symbol, ob, ts[bi - 1]))
                        break

    active_obs = [ob for ob in obs if ob.active]

    # Mark OBs already touched in COMPLETED HTF candles to avoid late/false "first touch" alerts.
    historical_touched_keys: set[str] = set()
    touched_cache_updated = False
    completed_last_idx = max(-1, last_idx - 1)
    if completed_last_idx >= 0:
        for ob in active_obs:
            key = stable_ob_key(symbol, ob, tick_size)
            if key in FIRST_TOUCH_SEEN:
                continue

            start_idx = max(0, int(ob.created_index) + 1)
            if start_idx > completed_last_idx:
                continue

            touched_before_now = False
            for idx in range(start_idx, completed_last_idx + 1):
                if touches_zone(ob.top, ob.bottom, h[idx], l[idx]):
                    touched_before_now = True
                    break

            if touched_before_now:
                FIRST_TOUCH_SEEN.add(key)
                historical_touched_keys.add(key)
                touched_cache_updated = True

    if touched_cache_updated:
        _save_cache(TOUCH_CACHE_FILE, FIRST_TOUCH_SEEN)

    if CONFIG.get("alert_touch_zone", True):
        out_msgs.extend(
            monitor_trigger_timeframe_touches(
                exchange,
                symbol,
                active_obs,
                tick_size,
                pre_touched_keys=historical_touched_keys,
            )
        )

    return out_msgs, bullish_candidate, candidate_score


def classify_event(msg: str) -> str:
    if msg.startswith("[TOUCH FIRST][TRIGGER]") or msg.startswith("[TOUCH FIRST]"):
        return "touch"
    if msg.startswith("[NEW OB]"):
        return "new"
    if msg.startswith("▲") or msg.startswith("▼"):
        return "retest"
    if msg.startswith("[NEAR OB]"):
        return "near"
    return "other"


def print_event_stream_header() -> None:
    print("-------------------------------------------------------------")
    print("# | النوع | التفاصيل")
    print("-------------------------------------------------------------")


def print_event_row(kind: str, msg: str, idx: int) -> None:
    colors = {
        "touch": "[94m",   # blue
        "new": "[92m",     # green
        "retest": "[93m",  # yellow
        "near": "\033[96m",    # cyan
        "other": "[0m",
    }
    labels = {
        "touch": "ملامسة",
        "new": "جديد",
        "retest": "إعادة اختبار",
        "near": "قريب من المنطقة",
        "other": "أخرى",
    }
    reset = "[0m"
    label = labels.get(kind, "أخرى")
    color = colors.get(kind, reset)
    print(f"{idx} | {color}{label}{reset} | {msg}")

def sort_and_filter_symbols_high_first(exchange: ccxt.Exchange, symbols: List[str]) -> List[str]:
    if not CONFIG.get("high_coins_first", True) or not symbols:
        return symbols

    sort_by = str(CONFIG.get("sort_by", "quoteVolume"))
    min_qv = safe_float(CONFIG.get("min_quote_volume_24h", 0.0), 0.0)
    min_abs_pct = safe_float(CONFIG.get("min_abs_change_24h_pct", 0.0), 0.0)
    min_pos_pct = safe_float(CONFIG.get("min_change_24h_pct", 0.0), 0.0)
    only_rising = bool(CONFIG.get("only_rising_24h", True))

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
        if only_rising and pct <= 0:
            continue
        if min_pos_pct > 0 and pct < min_pos_pct:
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
    print_event_stream_header()
    event_counter = 0

    for sym in symbols:
        try:
            msgs, is_cand, score = run_symbol(sym, exchange)

            for m in msgs:
                event_counter += 1
                print_event_row(classify_event(m), m, event_counter)

            if CONFIG.get("print_candidates", True) and is_cand:
                candidates.append((score, sym))

        except Exception:
            pass

        if CONFIG.get("sleep_after_symbol", False) and CONFIG["rate_limit_sleep"]:
            exchange.sleep(exchange.rateLimit)

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
