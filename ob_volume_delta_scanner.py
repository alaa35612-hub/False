#!/usr/bin/env python3
"""Order Blocks Volume Delta 3D scanner (Python port).

This script ports the Pine Script logic to Python, scanning Binance USDT-M futures
via CCXT. It recreates swing/BOS detection, POC-anchored OB creation, volume delta
aggregation (optional LTF), invalidation, and retest detection.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

import math
import time

import ccxt


@dataclass
class IndicatorConfig:
    timeframe: str = "15m"
    volume_delta_timeframe: str = "1m"
    max_bars_back: int = 5000
    swing_length: int = 5
    show_nearest: int = 3
    extend_zones: int = 10
    invalidation_method: str = "Wick"  # "Wick" or "Close"
    hide_invalid: bool = True
    enable_volume_delta: bool = True
    use_3d: bool = True
    vd_3d_depth: int = 5
    display_style: str = "Vertical"  # "Vertical" or "Horizontal"
    show_total_volume: bool = True
    show_delta_pct: bool = True
    retest_min_spacing: int = 4
    touch_max_age_bars: int = 50
    retest_max_age_bars: int = 50
    symbols_limit: Optional[int] = None


@dataclass
class ObRec:
    left_index: int
    left_time: int
    created_index: int
    created_time: int
    top: float
    bottom: float
    is_bull: bool
    active: bool
    retested: bool
    retest_index: Optional[int]
    retest_time: Optional[int]
    invalid_index: Optional[int]
    invalid_time: Optional[int]
    bull_vol: float
    bear_vol: float
    total_vol: float
    bull_pct: float
    bear_pct: float
    has_delta: bool


@dataclass
class RetestRec:
    bar_index: int
    is_bull: bool
    ob_left_index: int
    ob_created_index: int


def tf_to_seconds(tf: str) -> Optional[int]:
    if not tf:
        return None
    unit = tf[-1]
    try:
        value = int(tf[:-1])
    except ValueError:
        return None
    if unit == "s":
        return value
    if unit == "m":
        return value * 60
    if unit == "h":
        return value * 60 * 60
    if unit == "d":
        return value * 60 * 60 * 24
    if unit == "w":
        return value * 60 * 60 * 24 * 7
    return None


def fetch_ohlcv_paginated(
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    since: Optional[int],
    limit: int,
) -> List[List[float]]:
    all_rows: List[List[float]] = []
    tf_ms = exchange.parse_timeframe(timeframe) * 1000
    next_since = since
    while len(all_rows) < limit:
        batch_limit = min(1500, limit - len(all_rows))
        rows = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=next_since, limit=batch_limit)
        if not rows:
            break
        all_rows.extend(rows)
        last_ts = rows[-1][0]
        next_since = last_ts + tf_ms
        if len(rows) < batch_limit:
            break
        time.sleep(exchange.rateLimit / 1000)
    return all_rows


def compute_bar_vols(
    ohlcv: List[List[float]],
    ltf_ohlcv: Optional[List[List[float]]],
    use_ltf: bool,
    tf_sec: int,
) -> Tuple[List[float], List[float], List[float]]:
    totals: List[float] = []
    bulls: List[float] = []
    bears: List[float] = []
    if not use_ltf or not ltf_ohlcv:
        for _, o, _, _, c, v in ohlcv:
            totals.append(v)
            bulls.append(v if c > o else 0.0)
            bears.append(v if c < o else 0.0)
        return totals, bulls, bears

    ltf_idx = 0
    ltf_len = len(ltf_ohlcv)
    for i, row in enumerate(ohlcv):
        start_ts = row[0]
        end_ts = start_ts + tf_sec * 1000
        tot = 0.0
        bull = 0.0
        bear = 0.0
        while ltf_idx < ltf_len and ltf_ohlcv[ltf_idx][0] < start_ts:
            ltf_idx += 1
        scan_idx = ltf_idx
        while scan_idx < ltf_len and ltf_ohlcv[scan_idx][0] < end_ts:
            _, o2, _, _, c2, v2 = ltf_ohlcv[scan_idx]
            tot += v2
            if c2 > o2:
                bull += v2
            elif c2 < o2:
                bear += v2
            scan_idx += 1
        totals.append(tot)
        bulls.append(bull)
        bears.append(bear)
    return totals, bulls, bears


def pivot_high(highs: List[float], idx: int, swing: int) -> Optional[float]:
    pivot_idx = idx - swing
    if pivot_idx < swing:
        return None
    left = pivot_idx - swing
    right = pivot_idx + swing
    if right >= len(highs):
        return None
    pivot_val = highs[pivot_idx]
    for i in range(left, right + 1):
        if highs[i] > pivot_val:
            return None
    return pivot_val


def pivot_low(lows: List[float], idx: int, swing: int) -> Optional[float]:
    pivot_idx = idx - swing
    if pivot_idx < swing:
        return None
    left = pivot_idx - swing
    right = pivot_idx + swing
    if right >= len(lows):
        return None
    pivot_val = lows[pivot_idx]
    for i in range(left, right + 1):
        if lows[i] < pivot_val:
            return None
    return pivot_val


def find_most_touched_price(
    highs: List[float],
    lows: List[float],
    from_idx: Optional[int],
    to_idx: Optional[int],
    n_bins: int,
) -> Tuple[Optional[float], int]:
    if from_idx is None or to_idx is None:
        return None, 0
    span = to_idx - from_idx
    if span <= 0:
        return None, 0
    min_p = float("inf")
    max_p = float("-inf")
    for idx in range(from_idx, to_idx + 1):
        lo = lows[idx]
        hi = highs[idx]
        min_p = min(min_p, lo)
        max_p = max(max_p, hi)
    if not (min_p < max_p):
        return None, 0
    step = (max_p - min_p) / n_bins
    if step <= 0:
        step = 1e-8
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
    totals: List[float],
    bulls: List[float],
    bears: List[float],
    from_idx: Optional[int],
    to_idx: Optional[int],
    poc: Optional[float],
) -> Tuple[int, float, float, float]:
    if from_idx is None or to_idx is None or poc is None:
        return 0, 0.0, 0.0, 0.0
    span = to_idx - from_idx
    if span < 0:
        return 0, 0.0, 0.0, 0.0
    touches = 0
    tot_vol = 0.0
    bull_vol = 0.0
    bear_vol = 0.0
    for idx in range(from_idx, to_idx + 1):
        lo = lows[idx]
        hi = highs[idx]
        if lo <= poc <= hi:
            touches += 1
            tot_vol += totals[idx]
            bull_vol += bulls[idx]
            bear_vol += bears[idx]
    return touches, tot_vol, bull_vol, bear_vol


def calc_most_touched_price_vol(
    highs: List[float],
    lows: List[float],
    totals: List[float],
    bulls: List[float],
    bears: List[float],
    from_idx: Optional[int],
    to_idx: Optional[int],
    n_bins: int,
) -> Tuple[Optional[float], int, float, float, float]:
    poc, _ = find_most_touched_price(highs, lows, from_idx, to_idx, n_bins)
    touches, tot_vol, bull_vol, bear_vol = volume_at_price(
        highs,
        lows,
        totals,
        bulls,
        bears,
        from_idx,
        to_idx,
        poc,
    )
    return poc, touches, tot_vol, bull_vol, bear_vol


def ob_overlaps_active(obs: List[ObRec], zone_top: float, zone_bottom: float) -> bool:
    z_top = max(zone_top, zone_bottom)
    z_bot = min(zone_top, zone_bottom)
    for ob in obs:
        if ob.active:
            o_top = max(ob.top, ob.bottom)
            o_bot = min(ob.top, ob.bottom)
            if z_top >= o_bot and z_bot <= o_top:
                return True
    return False


def has_gap_between(
    highs: List[float],
    lows: List[float],
    anchor_idx: int,
    bos_idx: int,
    is_bull: bool,
) -> bool:
    from_idx = min(anchor_idx, bos_idx)
    to_idx = max(anchor_idx, bos_idx)
    if to_idx - from_idx < 1:
        return False
    for abs_idx in range(from_idx + 1, to_idx + 1):
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


def add_ob_from_poc(
    obs: List[ObRec],
    base_idx: int,
    top: float,
    bottom: float,
    is_bull: bool,
    tot_seed: float,
    bull_seed: float,
    bear_seed: float,
    created_idx: int,
    times: List[int],
    current_idx: int,
    max_bars_back: int,
    max_stored: int = 50,
) -> None:
    left_time = times[base_idx]
    created_time = times[created_idx]
    total = tot_seed
    b_vol = bull_seed
    s_vol = bear_seed
    has_delta = total > 0.0
    bull_pct = round((b_vol / total) * 100.0) if has_delta else 50.0
    bear_pct = 100.0 - bull_pct if has_delta else 50.0
    obs.insert(
        0,
        ObRec(
            left_index=base_idx,
            left_time=left_time,
            created_index=created_idx,
            created_time=created_time,
            top=top,
            bottom=bottom,
            is_bull=is_bull,
            active=True,
            retested=False,
            retest_index=None,
            retest_time=None,
            invalid_index=None,
            invalid_time=None,
            bull_vol=b_vol,
            bear_vol=s_vol,
            total_vol=total,
            bull_pct=bull_pct,
            bear_pct=bear_pct,
            has_delta=has_delta,
        ),
    )
    min_left = max(0, current_idx - max_bars_back + 1)
    for i in range(len(obs) - 1, -1, -1):
        if obs[i].left_index < min_left:
            obs.pop(i)
    while len(obs) > max_stored:
        removed = False
        for i in range(len(obs) - 1, -1, -1):
            if not obs[i].active:
                obs.pop(i)
                removed = True
                break
        if not removed:
            obs.pop()


def touches_zone(zone_top: float, zone_bot: float, c_high: float, c_low: float) -> bool:
    return c_high >= zone_bot and c_low <= zone_top


def zone_distance(px: float, zone_top: float, zone_bot: float) -> float:
    if px > zone_top:
        return px - zone_top
    if px < zone_bot:
        return zone_bot - px
    return 0.0


def pick_nearest(
    obs: List[ObRec],
    want_bull: bool,
    k_max: int,
    include_invalid: bool,
    price: float,
) -> List[int]:
    pairs: List[Tuple[float, int]] = []
    for idx, ob in enumerate(obs):
        if ob.is_bull != want_bull:
            continue
        if not include_invalid and not ob.active:
            continue
        dist = zone_distance(price, ob.top, ob.bottom)
        pairs.append((dist, idx))
    pairs.sort(key=lambda item: item[0])
    return [idx for _, idx in pairs[:k_max]]


def scan_symbol(symbol: str, exchange: ccxt.Exchange, cfg: IndicatorConfig) -> dict:
    ohlcv = fetch_ohlcv_paginated(
        exchange,
        symbol,
        cfg.timeframe,
        since=None,
        limit=cfg.max_bars_back,
    )
    if len(ohlcv) < cfg.swing_length * 2 + 10:
        return {"symbol": symbol, "error": "not enough data"}

    tf_sec = tf_to_seconds(cfg.timeframe) or exchange.parse_timeframe(cfg.timeframe)
    ltf_sec = tf_to_seconds(cfg.volume_delta_timeframe) if cfg.volume_delta_timeframe else None
    use_ltf = (
        cfg.volume_delta_timeframe
        and ltf_sec is not None
        and tf_sec is not None
        and ltf_sec < tf_sec
    )

    ltf_ohlcv = None
    if use_ltf:
        since_ts = ohlcv[0][0]
        target_bars = int(len(ohlcv) * (tf_sec / ltf_sec)) + 10
        ltf_ohlcv = fetch_ohlcv_paginated(
            exchange,
            symbol,
            cfg.volume_delta_timeframe,
            since=since_ts,
            limit=target_bars,
        )

    opens = [row[1] for row in ohlcv]
    highs = [row[2] for row in ohlcv]
    lows = [row[3] for row in ohlcv]
    closes = [row[4] for row in ohlcv]
    times = [row[0] for row in ohlcv]

    totals, bulls, bears = compute_bar_vols(ohlcv, ltf_ohlcv, use_ltf, tf_sec)

    obs: List[ObRec] = []
    ob_retests: List[RetestRec] = []
    last_bull_retest_bar: Optional[int] = None
    last_bear_retest_bar: Optional[int] = None

    sh_price: Optional[float] = None
    sh_idx: Optional[int] = None
    sl_price: Optional[float] = None
    sl_idx: Optional[int] = None

    prev_bos_bear_now = False
    prev_bos_bull_now = False
    prev_sh_idx: Optional[int] = None
    prev_sl_idx: Optional[int] = None

    ev_new_bull_ob = False
    ev_new_bear_ob = False
    ev_bull_retest = False
    ev_bear_retest = False

    for i in range(len(ohlcv)):
        ph = pivot_high(highs, i, cfg.swing_length)
        pl = pivot_low(lows, i, cfg.swing_length)
        if ph is not None:
            sh_price = ph
            sh_idx = i - cfg.swing_length
        if pl is not None:
            sl_price = pl
            sl_idx = i - cfg.swing_length

        bos_bear_now = (
            sl_price is not None
            and sl_idx is not None
            and i > sl_idx
            and closes[i] < sl_price
            and i > 0
            and closes[i - 1] >= sl_price
        )
        bos_bull_now = (
            sh_price is not None
            and sh_idx is not None
            and i > sh_idx
            and closes[i] > sh_price
            and i > 0
            and closes[i - 1] <= sh_price
        )

        bos_bear = prev_bos_bear_now
        bos_bull = prev_bos_bull_now

        bos_idx_bear = i - 1
        from_idx_bear = prev_sl_idx
        to_idx_bear = bos_idx_bear
        poc_b, touches_b, tot_vol_b, bull_vol_b, bear_vol_b = calc_most_touched_price_vol(
            highs, lows, totals, bulls, bears, from_idx_bear, to_idx_bear, 40
        )

        bos_idx_bull = i - 1
        from_idx_bull = prev_sh_idx
        to_idx_bull = bos_idx_bull
        poc_h, touches_h, tot_vol_h, bull_vol_h, bear_vol_h = calc_most_touched_price_vol(
            highs, lows, totals, bulls, bears, from_idx_bull, to_idx_bull, 40
        )

        if bos_bear:
            if (
                poc_b is not None
                and touches_b > 0
                and from_idx_bear is not None
                and to_idx_bear is not None
            ):
                span_b = to_idx_bear - from_idx_bear
                best_idx = None
                run_max_high = None
                if span_b >= 0:
                    for step in range(span_b + 1):
                        idx = to_idx_bear - step
                        hi = highs[idx]
                        lo = lows[idx]
                        run_max_high = hi if run_max_high is None else max(run_max_high, hi)
                        touches = lo <= poc_b <= hi
                        if touches and hi == run_max_high:
                            best_idx = idx
                gap_leg = has_gap_between(highs, lows, best_idx, bos_idx_bear, False) if best_idx is not None else False
                if best_idx is not None and not gap_leg:
                    top = highs[best_idx]
                    bottom = lows[best_idx]
                    if not ob_overlaps_active(obs, top, bottom):
                        add_ob_from_poc(
                            obs,
                            best_idx,
                            top,
                            bottom,
                            False,
                            tot_vol_b,
                            bull_vol_b,
                            bear_vol_b,
                            bos_idx_bear,
                            times,
                            i,
                            cfg.max_bars_back,
                        )
                        ev_new_bear_ob = True
            sl_price = None
            sl_idx = None

        if bos_bull:
            if (
                poc_h is not None
                and touches_h > 0
                and from_idx_bull is not None
                and to_idx_bull is not None
            ):
                span_h = to_idx_bull - from_idx_bull
                best_idx2 = None
                run_min_low = None
                if span_h >= 0:
                    for step in range(span_h + 1):
                        idx2 = to_idx_bull - step
                        lo2 = lows[idx2]
                        hi2 = highs[idx2]
                        run_min_low = lo2 if run_min_low is None else min(run_min_low, lo2)
                        touches = lo2 <= poc_h <= hi2
                        if touches and lo2 == run_min_low:
                            best_idx2 = idx2
                gap_leg2 = (
                    has_gap_between(highs, lows, best_idx2, bos_idx_bull, True) if best_idx2 is not None else False
                )
                if best_idx2 is not None and not gap_leg2:
                    top2 = highs[best_idx2]
                    bottom2 = lows[best_idx2]
                    if not ob_overlaps_active(obs, top2, bottom2):
                        add_ob_from_poc(
                            obs,
                            best_idx2,
                            top2,
                            bottom2,
                            True,
                            tot_vol_h,
                            bull_vol_h,
                            bear_vol_h,
                            bos_idx_bull,
                            times,
                            i,
                            cfg.max_bars_back,
                        )
                        ev_new_bull_ob = True
            sh_price = None
            sh_idx = None

        for ob in obs:
            if not ob.active:
                continue
            invalid = False
            inv_idx: Optional[int] = None
            inv_time: Optional[int] = None
            if ob.is_bull:
                if cfg.invalidation_method == "Wick":
                    invalid = lows[i] < ob.bottom
                    inv_idx = i
                    inv_time = times[i]
                else:
                    if i > 0:
                        invalid = closes[i - 1] < ob.bottom
                        inv_idx = i - 1
                        inv_time = times[i - 1]
            else:
                if cfg.invalidation_method == "Wick":
                    invalid = highs[i] > ob.top
                    inv_idx = i
                    inv_time = times[i]
                else:
                    if i > 0:
                        invalid = closes[i - 1] > ob.top
                        inv_idx = i - 1
                        inv_time = times[i - 1]

            if invalid:
                ob.active = False
                ob.invalid_index = inv_idx
                ob.invalid_time = inv_time

            retest_prev = False
            retest_bar: Optional[int] = None
            retest_tm: Optional[int] = None
            if i > 0:
                if ob.is_bull:
                    opens_above_prev = opens[i - 1] > ob.top
                    closes_above_prev = closes[i - 1] > ob.top
                    wick_touches_prev = lows[i - 1] <= ob.top and lows[i - 1] >= ob.bottom
                    retest_prev = opens_above_prev and closes_above_prev and wick_touches_prev
                else:
                    opens_below_prev = opens[i - 1] < ob.bottom
                    closes_below_prev = closes[i - 1] < ob.bottom
                    wick_touches_prev = highs[i - 1] >= ob.bottom and highs[i - 1] <= ob.top
                    retest_prev = opens_below_prev and closes_below_prev and wick_touches_prev

                if retest_prev:
                    retest_bar = i - 1
                    retest_tm = times[i - 1]

            if retest_prev and retest_bar is not None and retest_bar > ob.created_index:
                ob.retested = True
                ob.retest_index = retest_bar
                ob.retest_time = retest_tm

                last_side_bar = last_bull_retest_bar if ob.is_bull else last_bear_retest_bar
                can_log = last_side_bar is None or retest_bar - last_side_bar >= cfg.retest_min_spacing
                if can_log:
                    ob_retests.insert(
                        0, RetestRec(retest_bar, ob.is_bull, ob.left_index, ob.created_index)
                    )
                    if ob.is_bull:
                        ev_bull_retest = True
                        last_bull_retest_bar = retest_bar
                    else:
                        ev_bear_retest = True
                        last_bear_retest_bar = retest_bar

        prev_bos_bear_now = bos_bear_now
        prev_bos_bull_now = bos_bull_now
        prev_sh_idx = sh_idx
        prev_sl_idx = sl_idx

    latest_idx = len(ohlcv) - 1
    last_close = closes[latest_idx]
    bull_active_idx = pick_nearest(obs, True, cfg.show_nearest, False, last_close)
    bear_active_idx = pick_nearest(obs, False, cfg.show_nearest, False, last_close)
    active_obs = [obs[idx] for idx in bull_active_idx + bear_active_idx]
    invalid_obs = [ob for ob in obs if not ob.active] if not cfg.hide_invalid else []
    displayed_obs = active_obs + invalid_obs
    touch_alerts = []
    if cfg.touch_max_age_bars > 0:
        start_idx = max(0, latest_idx - cfg.touch_max_age_bars + 1)
        for ob in displayed_obs:
            for idx in range(start_idx, latest_idx + 1):
                if touches_zone(ob.top, ob.bottom, highs[idx], lows[idx]):
                    touch_alerts.append((ob, idx))
                    break

    retest_alerts = []
    if cfg.retest_max_age_bars > 0:
        start_idx = max(0, latest_idx - cfg.retest_max_age_bars + 1)
        displayed_keys = {(ob.left_index, ob.created_index) for ob in displayed_obs}
        for r in ob_retests:
            if (
                start_idx <= r.bar_index <= latest_idx
                and (r.ob_left_index, r.ob_created_index) in displayed_keys
            ):
                retest_alerts.append(r)

    return {
        "symbol": symbol,
        "new_bull_ob": ev_new_bull_ob,
        "new_bear_ob": ev_new_bear_ob,
        "bull_retest": ev_bull_retest,
        "bear_retest": ev_bear_retest,
        "touches": touch_alerts,
        "retests": retest_alerts,
        "active_ob_count": len(active_obs),
    }


def main() -> None:
    cfg = IndicatorConfig()
    exchange = ccxt.binance({"options": {"defaultType": "future"}})
    exchange.load_markets()

    symbols = [
        s
        for s, m in exchange.markets.items()
        if m.get("quote") == "USDT" and m.get("linear") and m.get("active")
    ]
    if cfg.symbols_limit:
        symbols = symbols[: cfg.symbols_limit]

    results = []
    for symbol in symbols:
        try:
            result = scan_symbol(symbol, exchange, cfg)
            results.append(result)
            if "error" in result:
                print(f"{symbol}: {result['error']}")
                continue
            if result["new_bull_ob"] or result["new_bear_ob"]:
                print(
                    f"{symbol}: NEW OB bull={result['new_bull_ob']} bear={result['new_bear_ob']}"
                )
            if result["bull_retest"] or result["bear_retest"]:
                print(
                    f"{symbol}: RETEST bull={result['bull_retest']} bear={result['bear_retest']}"
                )
            if result["touches"]:
                print(f"{symbol}: TOUCHES {len(result['touches'])} active zones")
        except Exception as exc:
            print(f"{symbol}: error {exc}")

    print("Scan complete.")


if __name__ == "__main__":
    main()
