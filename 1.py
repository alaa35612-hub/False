"""
PRO V6 Full Scanner (logic-oriented translation from Pine Script to Python).

This rewrite mirrors the trading logic (not the drawing elements) of the
original Pine script found in `PRO V6 .txt`. All alert-generating conditions are
reproduced with recursive state where Pine uses historical referencing.

Notes on unavoidable differences:
- request.security / rp_security in Pine fetch higher timeframe candles. In this
  console version everything is driven by a single dataframe per symbol. The HTF
  behaviour is emulated by re-sampling when possible; when resampling is not
  available the current timeframe data is used and this can slightly alter the
  exact bar at which an alert would have fired on TradingView.
- Visual constructs (boxes, labels, tables, fills) are represented only when
  they feed alert logic. Supply/Demand boxes and Cirrus Cloud are omitted because
  they do not gate the alertcondition blocks in the source file.
"""

import math
import random
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple, Callable

import ccxt
import numpy as np
import pandas as pd

# =============================================================================
# CONFIG — mirrors Pine inputs
# =============================================================================
API_KEY = ""
API_SECRET = ""

USE_ALL_SYMBOLS = True
SYMBOLS_WHITELIST: List[str] = []
SCAN_SYMBOL_LIMIT: Optional[int] = None

INPUT_TIMEFRAME = ""  # mirrors `res = input.timeframe('')`
BASE_TIMEFRAME = "15m"
MAX_HISTORY_BARS = 5000
ALERT_LOOKBACK_BARS = 3
ALERT_LOOKBACK_MINUTES = 0

RUN_CONTINUOUS = False
LOOP_SLEEP_SECONDS = 60
SLEEP_BETWEEN_SYMBOLS = 0.25

# === Signal System (Chỉ báo 1) ===
enable_signal_system = True
sensitivity = 5.0
volatility_period = 10

# === Trend Following + Zones (Chỉ báo 2) ===
enable_trend_follow = True
follow_type = "enhanced"  # enhanced | standard
trend_period = 28
trend_multiplier = 5.0
show_zones = True
zone1_level = 61.8
zone2_level = 78.6
zone3_level = 88.6

# === Theil-Sen Estimator (Chỉ báo 3) ===
enable_theilsen = False
ts_len = 100
ts_src_field = "close"
ts_method = "All"  # All | Random
ts_numpairs = 500
ts_showinterval = True
ts_mult = 2.0
ts_showresult = False
ts_extendln = True

# === Trend 2 / SAIYAN OCC ===
res5 = "15"
useRes = True
intRes = 10
basisType = "ALMA"  # TEMA, HullMA, ALMA
basisLen = 50
offsetSigma = 5
offsetALMA = 2.0
delayOffset = 0
tradeType = "BOTH"  # LONG | SHORT | BOTH | NONE
rsi_period = 28
rsi_overbought = 65
rsi_oversold = 35
ema_period = 144
show_basic_signals = True
show_strong_signals = True
show_rsi_confluence = False
h_heikin_ashi = False

# Swing / Supply-Demand inputs that may gate pivots (kept for completeness)
swing_length = 10
history_of_demand_to_keep = 20
box_width = 2.5

# === RSI + KDE Pivots ===
rsiLengthInput = 14
rsiSourceInput = "close"
highPivotLen = 21
lowPivotLen = 21
activationThresholdStr = "Medium"  # High | Medium | Low
KDEKernel = "Gaussian"  # Uniform | Gaussian | Sigmoid
KDEBandwidth = 2.71828
KDEStep = 100
KDELimit = 300
activationThreshold = 0.25
probMode = "Sum"
minPadding = False
labelCooldown = 8
maxDistanceToLastBar = 5000

# === EMA Trend Dashboard (simplified but keeps alert logic) ===
trend_dashboard_length = 200
dashboard_timeframes = ["15", "60", "240", "D", "W"]

# =============================================================================
# Helper utilities mirroring Pine behaviour
# =============================================================================

def nz(val, replacement=0.0):
    if isinstance(val, pd.Series):
        return val.fillna(replacement)
    return replacement if pd.isna(val) else val


def rma(series: pd.Series, length: int) -> pd.Series:
    """Wilder's RMA using the same recursive form Pine uses."""
    alpha = 1.0 / length
    return series.ewm(alpha=alpha, adjust=False, min_periods=length).mean()


def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False, min_periods=length).mean()


def sma(series: pd.Series, length: int) -> pd.Series:
    return series.rolling(length, min_periods=length).mean()


def wma(series: pd.Series, length: int) -> pd.Series:
    weights = np.arange(1, length + 1, dtype=float)
    return series.rolling(length).apply(lambda x: float(np.dot(x, weights)) / weights.sum(), raw=True)


def vwma(close: pd.Series, volume: pd.Series, length: int) -> pd.Series:
    num = (close * volume).rolling(length).sum()
    den = volume.rolling(length).sum()
    return num / den


def smma(series: pd.Series, length: int) -> pd.Series:
    # Pine's smma implementation is the same recursive RMA
    return rma(series, length)


def hull_ma(series: pd.Series, length: int) -> pd.Series:
    if length < 2:
        return series
    half = int(length / 2)
    sqrt_len = int(math.sqrt(length))
    wma_half = wma(series, half)
    wma_full = wma(series, length)
    hull = 2.0 * wma_half - wma_full
    return wma(hull, max(sqrt_len, 1))


def lsma(series: pd.Series, length: int, offset: int = 0) -> pd.Series:
    def _linreg(y: np.ndarray) -> float:
        x = np.arange(len(y))
        x_mean = x.mean()
        y_mean = y.mean()
        beta = float(((x - x_mean) * (y - y_mean)).sum() / ((x - x_mean) ** 2).sum())
        alpha = float(y_mean - beta * x_mean)
        return alpha + beta * (x[-1] + offset)

    return series.rolling(length).apply(_linreg, raw=True)


def alma(series: pd.Series, length: int, offset: float, sigma: float) -> pd.Series:
    if length <= 1:
        return series
    m = offset * (length - 1)
    s = length / sigma

    def _alma_window(x: np.ndarray) -> float:
        idx = np.arange(len(x))
        w = np.exp(-((idx - m) ** 2) / (2.0 * s * s))
        return float(np.sum(x * w) / np.sum(w))

    return series.rolling(length).apply(_alma_window, raw=True)


def true_range(high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    return pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)


def atr(high: pd.Series, low: pd.Series, close: pd.Series, length: int) -> pd.Series:
    return rma(true_range(high, low, close), length)


def crossover(a: pd.Series, b: pd.Series) -> pd.Series:
    return (a > b) & (a.shift(1) <= b.shift(1))


def crossunder(a: pd.Series, b: pd.Series) -> pd.Series:
    return (a < b) & (a.shift(1) >= b.shift(1))


def variant_ma(ma_type: str, series: pd.Series, length: int, off_sig: float, off_alma: float,
               volume: Optional[pd.Series] = None) -> pd.Series:
    ma_type = (ma_type or "").strip()
    if ma_type == "EMA":
        return ema(series, length)
    if ma_type == "DEMA":
        v2 = ema(series, length)
        return 2 * v2 - ema(v2, length)
    if ma_type == "TEMA":
        v2 = ema(series, length)
        return 3 * (v2 - ema(v2, length)) + ema(ema(v2, length), length)
    if ma_type == "WMA":
        return wma(series, length)
    if ma_type == "VWMA":
        return vwma(series, volume if volume is not None else pd.Series(np.ones(len(series))), length)
    if ma_type == "SMMA":
        return smma(series, length)
    if ma_type == "HullMA":
        return hull_ma(series, length)
    if ma_type == "LSMA":
        return lsma(series, length, int(off_sig))
    if ma_type == "ALMA":
        return alma(series, length, off_alma, off_sig if off_sig != 0 else 1)
    if ma_type == "TMA":
        return sma(sma(series, length), length)
    if ma_type == "SSMA":
        # Super smoother filter
        a1 = math.exp(-1.414 * math.pi / length)
        b1 = 2 * a1 * math.cos(1.414 * math.pi / length)
        c2 = b1
        c3 = -a1 * a1
        c1 = 1 - c2 - c3
        out = pd.Series(index=series.index, dtype=float)
        for i, val in enumerate(series):
            if i == 0:
                out.iloc[i] = val
            elif i == 1:
                out.iloc[i] = (val + series.iloc[i - 1]) / 2
            else:
                out.iloc[i] = c1 * (val + series.iloc[i - 1]) / 2 + c2 * out.iloc[i - 1] + c3 * out.iloc[i - 2]
        return out
    return sma(series, length)


# --- Multi-timeframe emulation ---

def timeframe_to_timedelta(tf: str) -> Optional[pd.Timedelta]:
    """Convert a TradingView-style timeframe string to pandas Timedelta."""
    tf = (tf or "").strip()
    if tf in ("", "0"):
        return None
    mapping = {
        "1": "1T",
        "2": "2T",
        "3": "3T",
        "4": "4T",
        "5": "5T",
        "10": "10T",
        "15": "15T",
        "30": "30T",
        "45": "45T",
        "60": "60T",
        "120": "120T",
        "240": "240T",
        "360": "360T",
        "720": "720T",
        "D": "1D",
        "1D": "1D",
        "3D": "3D",
        "W": "1W",
        "1W": "1W",
        "M": "1M",
        "1M": "1M",
    }
    pd_str = mapping.get(tf, None)
    return pd.Timedelta(pd_str) if pd_str else None


def resample_ohlcv(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """Emulate request.security by resampling without lookahead (using closed='left')."""
    delta = timeframe_to_timedelta(timeframe)
    if delta is None:
        return df
    rule = delta
    agg = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }
    resampled = df.resample(rule, closed="left", label="left").agg(agg).dropna(how="any")
    resampled = resampled.reindex(df.resample(rule, closed="left", label="left").mean().index).ffill()
    return resampled


def request_security(df: pd.DataFrame, timeframe: str, func: Callable[[pd.DataFrame], pd.Series]) -> pd.Series:
    """Minimal replacement for Pine's request.security returning series aligned to base df."""
    htf = resample_ohlcv(df, timeframe)
    htf_result = func(htf)
    aligned = htf_result.reindex(htf.index).reindex(df.index, method="ffill")
    return aligned


# =============================================================================
# Core computations
# =============================================================================

def compute_signal_system(df: pd.DataFrame) -> Dict[str, pd.Series]:
    src = df["close"]
    xatr = atr(df["high"], df["low"], df["close"], volatility_period)
    nloss = sensitivity * xatr

    x_signal_line = pd.Series(index=df.index, dtype=float)
    pos = pd.Series(index=df.index, dtype=float)
    for i in range(len(df)):
        src_i = src.iloc[i]
        prev_sig = x_signal_line.iloc[i - 1] if i > 0 else np.nan
        prev_src = src.iloc[i - 1] if i > 0 else np.nan

        iff_1 = src_i - nloss.iloc[i] if src_i > nz(prev_sig, 0) else src_i + nloss.iloc[i]
        iff_2 = math.inf
        if i > 0 and src_i < nz(prev_sig, 0) and prev_src < nz(prev_sig, 0):
            iff_2 = min(nz(prev_sig, 0), src_i + nloss.iloc[i])
        else:
            iff_2 = iff_1

        if i > 0 and src_i > nz(prev_sig, 0) and prev_src > nz(prev_sig, 0):
            x_signal_line.iloc[i] = max(nz(prev_sig, 0), src_i - nloss.iloc[i])
        else:
            x_signal_line.iloc[i] = iff_2

        if i == 0:
            pos.iloc[i] = 0
        else:
            iff_3 = -1 if prev_src > nz(prev_sig, 0) and src_i < nz(prev_sig, 0) else pos.iloc[i - 1]
            pos.iloc[i] = 1 if prev_src < nz(prev_sig, 0) and src_i > nz(prev_sig, 0) else iff_3

    long_signal = enable_signal_system and crossover(src, x_signal_line)
    short_signal = enable_signal_system and crossunder(src, x_signal_line)
    barcolor_signal = src > x_signal_line

    return {
        "x_signal_line": x_signal_line,
        "pos": pos,
        "long_signal": pd.Series(long_signal, index=df.index),
        "short_signal": pd.Series(short_signal, index=df.index),
        "barcolor_signal": pd.Series(barcolor_signal, index=df.index),
    }


def enhanced_ma(series: pd.Series, length: int) -> pd.Series:
    out = pd.Series(index=series.index, dtype=float)
    for i, val in enumerate(series):
        prev = out.iloc[i - 1] if i > 0 else 0.0
        out.iloc[i] = prev + (val - prev) / length
    return out


def compute_trend_follow(df: pd.DataFrame) -> Dict[str, pd.Series]:
    norm_o = df["open"]
    norm_h = df["high"]
    norm_l = df["low"]
    norm_c = df["close"]

    hilo = (norm_h - norm_l).combine(1.5 * sma(norm_h - norm_l, trend_period), min)
    href = np.where(norm_l <= norm_h.shift(1), norm_h - norm_c.shift(1), norm_h - norm_c.shift(1) - 0.5 * (norm_l - norm_h.shift(1)))
    lref = np.where(norm_h >= norm_l.shift(1), norm_c.shift(1) - norm_l,
                    norm_c.shift(1) - norm_l - 0.5 * (norm_l.shift(1) - norm_h))
    true_range_series = pd.Series(np.maximum.reduce([hilo, href, lref]) if follow_type == "enhanced"
                                  else np.maximum.reduce([(norm_h - norm_l), (norm_h - norm_c.shift(1)).abs(), (norm_l - norm_c.shift(1)).abs()]),
                                  index=df.index)

    loss = trend_multiplier * enhanced_ma(true_range_series, trend_period)
    up = norm_c - loss
    dn = norm_c + loss

    trend_up = pd.Series(index=df.index, dtype=float)
    trend_down = pd.Series(index=df.index, dtype=float)
    trend = pd.Series(index=df.index, dtype=float)
    trail = pd.Series(index=df.index, dtype=float)
    ex = pd.Series(index=df.index, dtype=float)

    for i in range(len(df)):
        if i == 0:
            trend_up.iloc[i] = up.iloc[i]
            trend_down.iloc[i] = dn.iloc[i]
            trend.iloc[i] = 1
            trail.iloc[i] = trend_up.iloc[i]
            ex.iloc[i] = norm_h.iloc[i]
            continue

        prev_trend_up = trend_up.iloc[i - 1]
        prev_trend_down = trend_down.iloc[i - 1]
        trend_up.iloc[i] = max(up.iloc[i], prev_trend_up) if norm_c.iloc[i - 1] > prev_trend_up else up.iloc[i]
        trend_down.iloc[i] = min(dn.iloc[i], prev_trend_down) if norm_c.iloc[i - 1] < prev_trend_down else dn.iloc[i]
        trend.iloc[i] = 1 if norm_c.iloc[i] > prev_trend_down else -1 if norm_c.iloc[i] < prev_trend_up else trend.iloc[i - 1]
        trail.iloc[i] = trend_up.iloc[i] if trend.iloc[i] == 1 else trend_down.iloc[i]
        if crossover(pd.Series([trend.iloc[i - 1], trend.iloc[i]], index=[df.index[i - 1], df.index[i]]),
                      pd.Series([0, 0], index=[df.index[i - 1], df.index[i]])).iloc[-1]:
            ex.iloc[i] = norm_h.iloc[i]
        elif crossunder(pd.Series([trend.iloc[i - 1], trend.iloc[i]], index=[df.index[i - 1], df.index[i]]),
                        pd.Series([0, 0], index=[df.index[i - 1], df.index[i]])).iloc[-1]:
            ex.iloc[i] = norm_l.iloc[i]
        else:
            ex.iloc[i] = max(ex.iloc[i - 1], norm_h.iloc[i]) if trend.iloc[i] == 1 else min(ex.iloc[i - 1], norm_l.iloc[i])

    state = pd.Series(np.where(trend == 1, "up", "down"), index=df.index)

    z1 = ex + (trail - ex) * zone1_level / 100.0
    z2 = ex + (trail - ex) * zone2_level / 100.0
    z3 = ex + (trail - ex) * zone3_level / 100.0
    l100 = trail

    z1_long = enable_trend_follow and (state.shift(1) == "up") & crossunder(norm_c, z1.shift(1))
    z2_long = enable_trend_follow and (state.shift(1) == "up") & crossunder(norm_c, z2.shift(1))
    z3_long = enable_trend_follow and (state.shift(1) == "up") & crossunder(norm_c, z3.shift(1))
    z1_short = enable_trend_follow and (state.shift(1) == "down") & crossover(norm_c, z1.shift(1))
    z2_short = enable_trend_follow and (state.shift(1) == "down") & crossover(norm_c, z2.shift(1))
    z3_short = enable_trend_follow and (state.shift(1) == "down") & crossover(norm_c, z3.shift(1))

    return {
        "trend": trend,
        "trail": trail,
        "ex": ex,
        "z1": z1,
        "z2": z2,
        "z3": z3,
        "l100": l100,
        "state": state,
        "z1_long": pd.Series(z1_long, index=df.index),
        "z2_long": pd.Series(z2_long, index=df.index),
        "z3_long": pd.Series(z3_long, index=df.index),
        "z1_short": pd.Series(z1_short, index=df.index),
        "z2_short": pd.Series(z2_short, index=df.index),
        "z3_short": pd.Series(z3_short, index=df.index),
    }


# --- Theil-Sen ---

def all_slopes(n: int, y: pd.Series) -> List[float]:
    slopes: List[float] = []
    if n > 2:
        for i in range(n - 1):
            for j in range(i + 1, n):
                slopes.append((y.iloc[i] - y.iloc[j]) / (j - i))
    return slopes


def rnd_slopes(slopes: List[float], numpoints: int, seeds: Tuple[int, int, int]) -> List[float]:
    s1, s2, s3 = seeds
    result: List[float] = []
    total = len(slopes)
    n = min(total, numpoints)
    for k in range(n):
        s1 = 171 * (s1 % 30269)
        s2 = 172 * (s2 % 30307)
        s3 = 170 * (s3 % 30323)
        rnd = (s1 / 30269.0 + s2 / 30307.0 + s3 / 30323.0) % 1
        idx = int(rnd * total) if total else 0
        if total:
            result.append(slopes[idx])
    return result


def intercepts(src: pd.Series, n: int, slope: float) -> List[float]:
    inter: List[float] = []
    if n > 0:
        for i in range(n):
            inter.append(src.iloc[i] - slope * i)
    return inter


def compute_theil_sen(df: pd.DataFrame) -> Dict[str, pd.Series]:
    src = df[ts_src_field]
    mslope_series = pd.Series(index=df.index, dtype=float)
    minter_series = pd.Series(index=df.index, dtype=float)
    dev_series = pd.Series(index=df.index, dtype=float)
    isbroken_series = pd.Series(index=df.index, dtype=bool)

    for idx in range(len(df)):
        window_len = ts_len if enable_theilsen else 0
        if idx + 1 < window_len or window_len <= 2:
            mslope_series.iloc[idx] = np.nan
            minter_series.iloc[idx] = np.nan
            dev_series.iloc[idx] = np.nan
            isbroken_series.iloc[idx] = False
            continue
        start = idx + 1 - window_len
        segment = src.iloc[start:idx + 1].reset_index(drop=True)
        slopes = all_slopes(window_len, segment)
        if ts_method == "Random":
            seeds = (int(df["close"].iloc[idx - 1]) if idx > 0 else 1,
                     int(df["close"].iloc[idx - 2]) if idx > 1 else 2,
                     int(df["close"].iloc[idx - 3]) if idx > 2 else 3)
            slopes = rnd_slopes(slopes, ts_numpairs, seeds)
        mslope = float(np.median(slopes)) if slopes else 0.0
        inter = intercepts(segment, window_len, mslope)
        minter = float(np.median(inter)) if inter else 0.0
        mslope_series.iloc[idx] = mslope
        minter_series.iloc[idx] = minter

        rmse = 0.0
        for j in range(window_len):
            rmse += (segment.iloc[j] - (minter + mslope * (window_len - j))) ** 2 / window_len
        dev = ts_mult * math.sqrt(rmse)
        dev_series.iloc[idx] = dev

        y2 = minter + mslope * (window_len - 1)
        isbroken_series.iloc[idx] = bool(segment.iloc[-1] > y2 + dev or segment.iloc[-1] < y2 - dev)

    return {
        "mslope": mslope_series,
        "minter": minter_series,
        "dev": dev_series,
        "isbroken": isbroken_series,
    }


# --- Trend 2 ---

def resample_series(series: pd.Series, tf: str) -> pd.Series:
    # simple resampler based on timeframe minutes
    try:
        minutes = int(tf.rstrip("m"))
    except ValueError:
        return series
    rule = f"{minutes}T"
    return series.resample(rule, origin="start").last().reindex(series.index, method="ffill")


def compute_trend2(df: pd.DataFrame) -> Dict[str, pd.Series]:
    close_series = df["close"]
    open_series = df["open"]
    if h_heikin_ashi:
        ha_close = (df["open"] + df["high"] + df["low"] + df["close"]) / 4
        ha_open = ha_close.copy()
        for i in range(len(df)):
            if i == 0:
                ha_open.iloc[i] = (df["open"].iloc[i] + df["close"].iloc[i]) / 2
            else:
                ha_open.iloc[i] = (ha_open.iloc[i - 1] + ha_close.iloc[i - 1]) / 2
        close_series = ha_close
        open_series = ha_open

    close_series_ma = variant_ma(basisType, close_series.shift(delayOffset), basisLen, offsetSigma, offsetALMA)
    open_series_ma = variant_ma(basisType, open_series.shift(delayOffset), basisLen, offsetSigma, offsetALMA)

    if useRes:
        close_series_ma = resample_series(close_series_ma, res5)
        open_series_ma = resample_series(open_series_ma, res5)

    leTrigger = crossover(close_series_ma, open_series_ma)
    seTrigger = crossunder(close_series_ma, open_series_ma)

    rsi_val = ta_rsi(df["close"], rsi_period)
    rsi_ema_val = ema(rsi_val, 10)
    ema_price = ema(df["close"], ema_period)

    rsiOb = (rsi_val > rsi_overbought) & (rsi_val > rsi_ema_val)
    rsiOs = (rsi_val < rsi_oversold) & (rsi_val < rsi_ema_val)
    price_above_ema = df["close"] > ema_price
    price_below_ema = df["close"] < ema_price

    rsi_bullish = rsiOs | ((rsi_val < 50) & price_above_ema)
    rsi_bearish = rsiOb | ((rsi_val > 50) & price_below_ema)

    strong_buy = leTrigger & rsi_bullish
    strong_sell = seTrigger & rsi_bearish

    # Respect tradeType filter
    if tradeType == "LONG":
        seTrigger &= False
        strong_sell &= False
    elif tradeType == "SHORT":
        leTrigger &= False
        strong_buy &= False
    elif tradeType == "NONE":
        leTrigger &= False
        seTrigger &= False
        strong_buy &= False
        strong_sell &= False

    show_le = show_basic_signals & (strong_buy if show_rsi_confluence else leTrigger)
    show_se = show_basic_signals & (strong_sell if show_rsi_confluence else seTrigger)

    return {
        "leTrigger": pd.Series(show_le, index=df.index),
        "seTrigger": pd.Series(show_se, index=df.index),
        "strong_buy": pd.Series(show_strong_signals and strong_buy, index=df.index),
        "strong_sell": pd.Series(show_strong_signals and strong_sell, index=df.index),
    }


# --- RSI + KDE pivots ---

def ta_rsi(series: pd.Series, length: int) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = rma(gain, length)
    avg_loss = rma(loss, length)
    rs = avg_gain / avg_loss
    rs = rs.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return 100 - (100 / (1 + rs))


def gaussian(distance: float, bandwidth: float) -> float:
    return 1.0 / math.sqrt(2.0 * math.pi) * math.pow(math.e, -0.5 * math.pow(distance / bandwidth, 2.0))


def uniform(distance: float, bandwidth: float) -> float:
    return 0.0 if abs(distance) > bandwidth else 0.5


def sigmoid(distance: float, bandwidth: float) -> float:
    return 1.0 / (1.0 + math.exp(-distance / max(bandwidth, 1e-9)))


def kde_probabilities(values: List[float], steps: int, bandwidth: float, kernel: str) -> List[float]:
    if not values:
        return [0.0] * steps
    low = min(values)
    high = max(values)
    if low == high:
        low -= 1
        high += 1
    bins = np.linspace(low, high, steps)
    probs = []
    for b in bins:
        acc = 0.0
        for v in values:
            dist = b - v
            if kernel == "Uniform":
                acc += uniform(dist, bandwidth)
            elif kernel == "Sigmoid":
                acc += sigmoid(dist, bandwidth)
            else:
                acc += gaussian(dist, bandwidth)
        probs.append(acc / len(values))
    return probs


def compute_rsi_kde(df: pd.DataFrame) -> Dict[str, pd.Series]:
    """PRO V6 RSI + KDE pivots (logic mirror).

    The Pine script builds KDE distributions for RSI values captured at price
    swing pivots (ta.pivothigh/low) and evaluates probabilities either via the
    nearest bin or cumulative sum. Alerts fire when the color resets to na on a
    confirmed bar while the previous bar was colored bullish/bearish.
    """

    src = df[rsiSourceInput] if rsiSourceInput in df.columns else df["close"]
    rsi_series = ta_rsi(src, rsiLengthInput)

    # Price-based pivots, mirroring ta.pivothigh/low(highPivotLen, highPivotLen)
    high_pivot = (
        df["high"].shift(highPivotLen)
        .rolling(highPivotLen * 2 + 1)
        .apply(lambda x: 1 if x.iloc[highPivotLen] == x.max() else 0, raw=False)
    )
    low_pivot = (
        df["low"].shift(lowPivotLen)
        .rolling(lowPivotLen * 2 + 1)
        .apply(lambda x: 1 if x.iloc[lowPivotLen] == x.min() else 0, raw=False)
    )

    kde_limit = KDELimit
    activation = activationThreshold if activationThresholdStr == "" else (
        0.4 if activationThresholdStr == "High" else 0.25 if activationThresholdStr == "Medium" else 0.15
    )

    cur_color = pd.Series(np.nan, index=df.index)
    possible_bull = pd.Series(False, index=df.index)
    possible_bear = pd.Series(False, index=df.index)

    highPivotRSIs: List[float] = []
    lowPivotRSIs: List[float] = []
    KDEHighX: Optional[np.ndarray] = None
    KDEHighY: Optional[np.ndarray] = None
    KDEHighYSum: Optional[np.ndarray] = None
    KDELowX: Optional[np.ndarray] = None
    KDELowY: Optional[np.ndarray] = None
    KDELowYSum: Optional[np.ndarray] = None

    def build_kde(arr: List[float]) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        arr_np = np.array(arr, dtype=float)
        arr_range = float(arr_np.max() - arr_np.min()) if arr_np.size > 0 else 0.0
        arr_min = float(arr_np.min()) - (arr_range / 2.0 if minPadding else 0.0)
        step_count = arr_range / KDEStep if KDEStep != 0 else 0.0

        density_range = np.array([arr_min + i * step_count for i in range(KDEStep * 2)], dtype=float)
        x_arr: List[float] = []
        y_arr: List[float] = []

        for val in density_range:
            temp = 0.0
            for item in arr_np:
                distance = val - item
                if KDEKernel == "Gaussian":
                    temp += (1.0 / math.sqrt(2.0 * math.pi)) * math.exp(-0.5 * (distance / (1.0 / KDEBandwidth)) ** 2)
                elif KDEKernel == "Uniform":
                    temp += 0.0 if abs(distance) > (1.0 / KDEBandwidth) else 0.5
                else:  # Sigmoid
                    temp += 2.0 / math.pi * (1.0 / (math.exp(distance / (1.0 / KDEBandwidth)) + math.exp(-(distance / (1.0 / KDEBandwidth)))))
            x_arr.append(val)
            y_arr.append((1.0 / len(arr_np)) * temp if len(arr_np) > 0 else 0.0)

        y_np = np.array(y_arr, dtype=float)
        y_sum = np.cumsum(y_np)
        return np.array(x_arr, dtype=float), y_np, y_sum

    def prefix_sum(arr: np.ndarray, l: int, r: int) -> float:
        if arr.size == 0:
            return 0.0
        return arr[r] - (0.0 if l == 0 else arr[l - 1])

    confirmed = pd.Series(True, index=df.index)
    if len(df) > 0:
        confirmed.iloc[-1] = False  # mimic barstate.isconfirmed for the live bar

    for i in range(len(df)):
        if high_pivot.iloc[i] == 1:
            pivot_idx = max(i - highPivotLen, 0)
            highPivotRSIs.append(rsi_series.iloc[pivot_idx])
            if len(highPivotRSIs) > kde_limit:
                highPivotRSIs = highPivotRSIs[-kde_limit:]
            KDEHighX, KDEHighY, KDEHighYSum = build_kde(highPivotRSIs)

        if low_pivot.iloc[i] == 1:
            pivot_idx = max(i - lowPivotLen, 0)
            lowPivotRSIs.append(rsi_series.iloc[pivot_idx])
            if len(lowPivotRSIs) > kde_limit:
                lowPivotRSIs = lowPivotRSIs[-kde_limit:]
            KDELowX, KDELowY, KDELowYSum = build_kde(lowPivotRSIs)

        highProb = None
        maxHighProb = None
        lowProb = None
        maxLowProb = None

        if (len(df) - i) < maxDistanceToLastBar and KDEHighX is not None and KDEHighY is not None:
            nearest_idx = int(np.searchsorted(KDEHighX, rsi_series.iloc[i]))
            nearest_idx = min(max(nearest_idx, 0), len(KDEHighX) - 1)
            if probMode == "Nearest":
                highProb = float(KDEHighY[nearest_idx])
                maxHighProb = float(np.max(KDEHighY))
            else:
                highProb = float(prefix_sum(KDEHighYSum, 0, nearest_idx))
                maxHighProb = float(np.sum(KDEHighY))

        if (len(df) - i) < maxDistanceToLastBar and KDELowX is not None and KDELowY is not None:
            nearest_idx_low = int(np.searchsorted(KDELowX, rsi_series.iloc[i]))
            nearest_idx_low = min(max(nearest_idx_low, 0), len(KDELowX) - 1)
            if probMode == "Nearest":
                lowProb = float(KDELowY[nearest_idx_low])
                maxLowProb = float(np.max(KDELowY))
            else:
                lowProb = float(prefix_sum(KDELowYSum, nearest_idx_low, len(KDELowYSum) - 1))
                maxLowProb = float(np.sum(KDELowY))

        cur_color.iloc[i] = np.nan
        if KDELowY is not None and KDEHighY is not None:
            if probMode == "Nearest":
                if lowProb is not None and maxLowProb is not None and abs(lowProb - maxLowProb) < activation / 50.0:
                    cur_color.iloc[i] = 1  # bullish color
                if highProb is not None and maxHighProb is not None and abs(highProb - maxHighProb) < activation / 50.0:
                    cur_color.iloc[i] = -1  # bearish color
            else:
                if lowProb is not None and maxLowProb is not None and lowProb > maxLowProb * (1.0 - activation):
                    cur_color.iloc[i] = 1
                elif highProb is not None and maxHighProb is not None and highProb > maxHighProb * (1.0 - activation):
                    cur_color.iloc[i] = -1

        possible_bull.iloc[i] = pd.isna(cur_color.iloc[i]) and cur_color.shift(1).iloc[i] == 1 and confirmed.iloc[i]
        possible_bear.iloc[i] = pd.isna(cur_color.iloc[i]) and cur_color.shift(1).iloc[i] == -1 and confirmed.iloc[i]

    return {
        "curColor": cur_color,
        "possible_bullish_pivot": possible_bull,
        "possible_bearish_pivot": possible_bear,
    }


# --- Trend Dashboard (simplified EMA trend cross) ---

def compute_trend_indicator(df: pd.DataFrame) -> pd.Series:
    ema_fast = ema(df["close"], trend_dashboard_length // 2)
    ema_slow = ema(df["close"], trend_dashboard_length)
    indicator = pd.Series(0, index=df.index, dtype=int)
    indicator = indicator.mask(ema_fast > ema_slow, 1)
    indicator = indicator.mask(ema_fast < ema_slow, -1)
    return indicator


def compute_dashboard_alignment(df: pd.DataFrame) -> Dict[str, pd.Series]:
    """Mirror dashboard/resolution trend alignment across multiple timeframes."""
    trend_status = {}
    for tf in dashboard_timeframes:
        trend_status[tf] = request_security(df, tf, lambda d: compute_trend_indicator(d))

    idx = df.index
    alignment_up = pd.Series(False, index=idx)
    alignment_down = pd.Series(False, index=idx)
    mixed_state = pd.Series(False, index=idx)

    stacked = pd.concat([trend_status[tf] for tf in dashboard_timeframes], axis=1)
    stacked.columns = dashboard_timeframes
    alignment_up = (stacked == 1).all(axis=1)
    alignment_down = (stacked == -1).all(axis=1)
    mixed_state = ~(alignment_up | alignment_down)

    return {
        "trend_status": trend_status,
        "alignment_up": alignment_up,
        "alignment_down": alignment_down,
        "alignment_mixed": mixed_state,
    }


# =============================================================================
# Aggregate signal computation
# =============================================================================

def compute_all_pro_v6_signals(df: pd.DataFrame) -> Dict[str, pd.Series]:
    signals = {}

    sig_sys = compute_signal_system(df)
    signals.update({
        "pro_signal_up": sig_sys["long_signal"],
        "pro_signal_down": sig_sys["short_signal"],
    })

    trend = compute_trend_follow(df)
    signals.update({
        "z1_long": trend["z1_long"],
        "z2_long": trend["z2_long"],
        "z3_long": trend["z3_long"],
        "z1_short": trend["z1_short"],
        "z2_short": trend["z2_short"],
        "z3_short": trend["z3_short"],
    })

    if enable_theilsen:
        ts_channel = compute_theil_sen(df)
        signals["theil_sen_break"] = ts_channel["isbroken"] & ~ts_channel["isbroken"].shift(1).fillna(False)
    else:
        signals["theil_sen_break"] = pd.Series(False, index=df.index)

    trend2 = compute_trend2(df)
    signals.update({
        "long_entry": trend2["leTrigger"],
        "short_entry": trend2["seTrigger"],
        "strong_buy": trend2["strong_buy"],
        "strong_sell": trend2["strong_sell"],
    })

    rsi_kde = compute_rsi_kde(df)
    signals.update({
        "possible_bullish_pivot": rsi_kde["possible_bullish_pivot"],
        "possible_bearish_pivot": rsi_kde["possible_bearish_pivot"],
    })

    signals["trend_indicator"] = compute_trend_indicator(df)

    dashboard = compute_dashboard_alignment(df)
    signals["alignment_up"] = dashboard["alignment_up"] & ~dashboard["alignment_up"].shift(1, fill_value=False)
    signals["alignment_down"] = dashboard["alignment_down"] & ~dashboard["alignment_down"].shift(1, fill_value=False)
    signals["alignment_mixed"] = dashboard["alignment_mixed"] & ~dashboard["alignment_mixed"].shift(1, fill_value=False)

    return signals


# =============================================================================
# CCXT layer
# =============================================================================

def init_exchange():
    exchange = ccxt.binanceusdm({
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "enableRateLimit": True,
    })
    exchange.load_markets()
    return exchange


def resolve_timeframe(exchange, requested: str) -> str:
    tf = (requested or "").strip() or BASE_TIMEFRAME
    if exchange is not None:
        available = getattr(exchange, "timeframes", None) or {}
        if available and tf not in available:
            tf = BASE_TIMEFRAME if BASE_TIMEFRAME in available else next(iter(available))
    return tf


def get_usdtm_symbols(exchange) -> List[str]:
    markets = exchange.markets
    symbols = []
    for s, m in markets.items():
        if m.get("quote") != "USDT":
            continue
        if m.get("settle") not in (None, "USDT"):
            continue
        m_type = m.get("type")
        if m.get("contract") or m.get("future") or m_type in ("future", "swap"):
            symbols.append(s)
    symbols = sorted(set(symbols))
    if not USE_ALL_SYMBOLS:
        symbols = [s for s in symbols if s in SYMBOLS_WHITELIST]
    if SCAN_SYMBOL_LIMIT:
        symbols = symbols[:SCAN_SYMBOL_LIMIT]
    return symbols


def fetch_ohlcv_df(exchange, symbol: str, timeframe: str, limit: int) -> Optional[pd.DataFrame]:
    try:
        raw = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as exc:  # pragma: no cover - runtime guard
        print(f"[ERROR] fetch_ohlcv failed for {symbol} ({timeframe}): {exc}")
        return None
    if not raw:
        return None
    df = pd.DataFrame(raw, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df.sort_values("timestamp", inplace=True)
    df.reset_index(drop=True, inplace=True)
    df.set_index("timestamp", inplace=True)
    return df


def _last_true_index(series: pd.Series, lookback_bars: int):
    subset = series.iloc[-lookback_bars:] if lookback_bars and lookback_bars > 0 else series
    idxs = subset[subset].index
    return idxs[-1] if len(idxs) else None


def print_signals(symbol: str, timeframe: str, df: pd.DataFrame, signals: Dict[str, pd.Series]):
    now_utc = datetime.now(timezone.utc)
    min_time = None
    if ALERT_LOOKBACK_MINUTES and ALERT_LOOKBACK_MINUTES > 0:
        min_time = now_utc - timedelta(minutes=ALERT_LOOKBACK_MINUTES)

    ts_series = df.index
    price_series = df["close"]

    labels = {
        "pro_signal_up": "Pro Signal Up",
        "pro_signal_down": "Pro Signal Down",
        "long_entry": "Long Entry Alert",
        "short_entry": "Short Entry Alert",
        "strong_buy": "Strong Buy Alert",
        "strong_sell": "Strong Sell Alert",
        "possible_bullish_pivot": "Possible Bullish Pivot",
        "possible_bearish_pivot": "Possible Bearish Pivot",
        "z1_long": "Zone 1 Cross Down (Uptrend)",
        "z2_long": "Zone 2 Cross Down (Uptrend)",
        "z3_long": "Zone 3 Cross Down (Uptrend)",
        "z1_short": "Zone 1 Cross Up (Downtrend)",
        "z2_short": "Zone 2 Cross Up (Downtrend)",
        "z3_short": "Zone 3 Cross Up (Downtrend)",
        "theil_sen_break": "Theil-Sen Channel Break",
        "trend_indicator": "Trend Changed",
        "alignment_up": "Dashboard Alignment Up",
        "alignment_down": "Dashboard Alignment Down",
        "alignment_mixed": "Dashboard Alignment Mixed",
    }

    for key, series in signals.items():
        if key == "trend_indicator":
            if len(series) < 2:
                continue
            if series.iloc[-1] != series.iloc[-2]:
                ts_sig = ts_series[-1]
                if min_time and ts_sig < min_time:
                    continue
                price_sig = price_series.iloc[-1]
                trend_str = "Uptrend" if series.iloc[-1] == 1 else "Downtrend" if series.iloc[-1] == -1 else "Neutral"
                print(f"SYMBOL: {symbol} | TIMEFRAME: {timeframe} | SIGNAL: {labels[key]} ({trend_str}) | PRICE: {price_sig:.4f} | TIME: {ts_sig.strftime('%Y-%m-%d %H:%M:%S')}")
            continue

        last_idx = _last_true_index(series, ALERT_LOOKBACK_BARS)
        if last_idx is None:
            continue
        if min_time and last_idx < min_time:
            continue
        price_sig = price_series.loc[last_idx]
        label = labels.get(key, key)
        print(f"SYMBOL: {symbol} | TIMEFRAME: {timeframe} | SIGNAL: {label} | PRICE: {price_sig:.4f} | TIME: {last_idx.strftime('%Y-%m-%d %H:%M:%S')}")


# =============================================================================
# Test harness for CSV comparison
# =============================================================================

def run_test_harness(csv_path: str, timeframe: str = BASE_TIMEFRAME):
    df = pd.read_csv(csv_path)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df.set_index("timestamp", inplace=True)
    signals = compute_all_pro_v6_signals(df)
    for name, series in signals.items():
        true_points = series[series].index
        print(f"{name}: {len(true_points)} signals")


# =============================================================================
# Scanner entry points
# =============================================================================

def process_symbol(exchange, symbol: str, timeframe: str):
    df = fetch_ohlcv_df(exchange, symbol, timeframe, MAX_HISTORY_BARS)
    if df is None or len(df) < 200:
        return
    try:
        signals = compute_all_pro_v6_signals(df)
    except Exception as exc:  # pragma: no cover - runtime guard
        print(f"[ERROR] compute_all_pro_v6_signals failed for {symbol}: {exc}")
        return
    print_signals(symbol, timeframe, df, signals)


def run_scan_once():
    exchange = init_exchange()
    timeframe = resolve_timeframe(exchange, INPUT_TIMEFRAME)
    symbols = get_usdtm_symbols(exchange)
    print(f"[INFO] Scanning {len(symbols)} Binance USDT-M symbols on {timeframe}")
    for symbol in symbols:
        process_symbol(exchange, symbol, timeframe)
        time.sleep(SLEEP_BETWEEN_SYMBOLS)


def main():
    if RUN_CONTINUOUS:
        while True:
            run_scan_once()
            print(f"[INFO] Sleeping {LOOP_SLEEP_SECONDS} seconds before next scan…")
            time.sleep(LOOP_SLEEP_SECONDS)
    else:
        run_scan_once()


if __name__ == "__main__":
    main()
