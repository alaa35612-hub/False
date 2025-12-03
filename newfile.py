"""
PRO V6 Full Scanner (logic-oriented translation from Pine Script to Python)

This file rebuilds the trading logic of the PRO V6 indicator so it can operate as a
console-based scanner on Binance USDT-M Futures using CCXT. Visual-only elements from
Pine (tables, labels, drawings) are not reproduced, but every computation that feeds
the core alerts is represented here.
"""

import time
from datetime import datetime, timedelta, timezone

import ccxt
import numpy as np
import pandas as pd

# =============================================================================
# CONFIG (implements Pine inputs as Python variables as much as is practical)
# =============================================================================

# --- General exchange / scanner config ---------------------------------------

API_KEY = ""         # optional
API_SECRET = ""      # optional

USE_ALL_SYMBOLS = True
SYMBOLS_WHITELIST = ["BTC/USDT", "ETH/USDT"]  # used if USE_ALL_SYMBOLS = False

# Primary resolution used by the scanner; mirrors `res = input.timeframe('')`.
# If left blank, BASE_TIMEFRAME is used.
INPUT_TIMEFRAME = ""

BASE_TIMEFRAME = "15m"        # equivalent to TradingView chart timeframe
MAX_HISTORY_BARS = 5000       # mirrors max_bars_back from the Pine script
ALERT_LOOKBACK_BARS = 3       # only print alerts that occurred within last N bars
ALERT_LOOKBACK_MINUTES = 0    # 0 disables time-based filter

RUN_CONTINUOUS = False
LOOP_SLEEP_SECONDS = 60

SLEEP_BETWEEN_SYMBOLS = 0.25

# --- CHỈ BÁO 1: SIGNAL SYSTEM inputs -----------------------------------------

enable_signal_system = True
sensitivity = 5.0               # "Sensitivity"
volatility_period = 10          # "Volatility Period"

# --- CHỈ BÁO 2: TREND FOLLOWING & ZONES inputs -------------------------------

enable_trend_follow = True
follow_type = "enhanced"        # 'enhanced' or 'standard'
trend_period = 10               # ATR / trueRange period
trend_multiplier = 5.0          # ATR multiplier
show_zones = True
zone1_level = 61.8
zone2_level = 78.6
zone3_level = 88.6

# --- CHỈ BÁO 4: TREND 2 inputs (ALMA basis + RSI) ----------------------------

enable_trend2 = True
trend2_basis_type = "ALMA"      # 'TEMA','HullMA','ALMA', etc.
trend2_basis_length = 50
trend2_offset_sigma = 5
trend2_offset_alma = 2.0
trend2_delay_offset = 0
trend2_rsi_period = 28
trend2_rsi_ob = 65.0
trend2_rsi_os = 35.0
trend2_rsi_ema_length = 10
trend2_ema_period = 144

# --- CHỈ BÁO 3: THEIL-SEN ESTIMATOR inputs -----------------------------------

enable_theilsen = False
ts_len = 100
ts_src_field = "close"          # Pine: ts_src = input(..., defval=close)
ts_method = "All"               # 'All' or 'Random'
ts_numpairs = 500
ts_mult = 2.0                   # prediction interval multiplier

# --- RSI + KDE + Pivots inputs ----------------------------------------------

rsi_kde_length = 14
high_pivot_len = 21
low_pivot_len = 21
kde_bandwidth = 2.71828
kde_steps = 100
kde_limit = 300
kde_activation_threshold = 0.25  # 'Medium' setting in original

# --- Trend dashboard style EMA trend (very simplified) -----------------------

trend_ema_length = 200  # long EMA for basic up/down/neutral trend


# =============================================================================
# HELPER: TA functions
# =============================================================================

def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def sma(series: pd.Series, length: int) -> pd.Series:
    return series.rolling(length, min_periods=length).mean()


def wma(series: pd.Series, length: int) -> pd.Series:
    weights = np.arange(1, length + 1)
    return series.rolling(length).apply(
        lambda x: float(np.dot(x, weights)) / weights.sum(),
        raw=True
    )


def vwma(close: pd.Series, volume: pd.Series, length: int) -> pd.Series:
    num = (close * volume).rolling(length).sum()
    den = volume.rolling(length).sum()
    return num / den


def rsi(series: pd.Series, length: int) -> pd.Series:
    delta = series.diff()
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)
    gain = pd.Series(gain, index=series.index)
    loss = pd.Series(loss, index=series.index)
    avg_gain = gain.ewm(alpha=1.0 / length, min_periods=length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / length, min_periods=length, adjust=False).mean()
    rs = avg_gain / avg_loss
    rs = rs.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return 100.0 - (100.0 / (1.0 + rs))


def true_range(high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr


def atr(high: pd.Series, low: pd.Series, close: pd.Series, length: int) -> pd.Series:
    tr = true_range(high, low, close)
    return tr.ewm(alpha=1.0 / length, min_periods=length, adjust=False).mean()


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


def hull_ma(series: pd.Series, length: int) -> pd.Series:
    if length < 2:
        return series

    half = int(length / 2)
    sqrt_len = int(np.sqrt(length))
    wma_half = wma(series, half)
    wma_full = wma(series, length)
    hull = 2.0 * wma_half - wma_full
    return wma(hull, sqrt_len)


def linreg(series: pd.Series, length: int, offset: int = 0) -> pd.Series:
    """Least-squares regression similar to ta.linreg."""
    def _linreg(y: np.ndarray) -> float:
        x = np.arange(len(y))
        x_mean = x.mean()
        y_mean = y.mean()
        beta = float(((x - x_mean) * (y - y_mean)).sum() / ((x - x_mean) ** 2).sum())
        alpha = float(y_mean - beta * x_mean)
        return alpha + beta * (x[-1] + offset)

    return series.rolling(length).apply(_linreg, raw=True)


def crossover(series1: pd.Series, series2: pd.Series) -> pd.Series:
    prev1 = series1.shift(1)
    prev2 = series2.shift(1)
    return (series1 > series2) & (prev1 <= prev2)


def crossunder(series1: pd.Series, series2: pd.Series) -> pd.Series:
    prev1 = series1.shift(1)
    prev2 = series2.shift(1)
    return (series1 < series2) & (prev1 >= prev2)


# =============================================================================
# VARIANT MA (used by Trend 2 basis)
# =============================================================================

def variant_ma(ma_type: str,
               src: pd.Series,
               length: int,
               off_sig: int,
               off_alma: float,
               volume: pd.Series) -> pd.Series:
    ma_type = ma_type.upper()

    v2 = ema(src, length)  # EMA
    v3 = 2.0 * v2 - ema(v2, length)  # DEMA
    v4 = 3.0 * (v2 - ema(v2, length)) + ema(ema(v2, length), length)  # TEMA
    v5 = wma(src, length)  # WMA
    v6 = vwma(src, volume, length)  # VWMA

    # SMMA
    v7 = pd.Series(index=src.index, dtype="float64")
    sma_1 = sma(src, length)
    v7.iloc[0] = sma_1.iloc[0]
    alpha = 1.0 / length
    for i in range(1, len(src)):
        prev = v7.iloc[i - 1]
        v7.iloc[i] = prev + alpha * (src.iloc[i] - prev)

    v8 = hull_ma(src, length)  # Hull MA
    v9 = linreg(src, length, off_sig)  # LSMA
    v10 = alma(src, length, off_alma, off_sig)  # ALMA
    v11 = sma(sma(src, length), length)  # TMA

    # SuperSmoother (SSMA)
    a1 = np.exp(-1.414 * np.pi / length)
    b1 = 2.0 * a1 * np.cos(1.414 * np.pi / length)
    c2 = b1
    c3 = -a1 * a1
    c1 = 1.0 - c2 - c3
    v12 = pd.Series(index=src.index, dtype="float64")
    if len(src) >= 2:
        v12.iloc[0] = src.iloc[0]
        v12.iloc[1] = src.iloc[1]
        for i in range(2, len(src)):
            v12.iloc[i] = (
                c1 * (src.iloc[i] + src.iloc[i - 1]) / 2.0
                + c2 * v12.iloc[i - 1]
                + c3 * v12.iloc[i - 2]
            )
    else:
        v12[:] = src

    if ma_type == "EMA":
        return v2
    if ma_type == "DEMA":
        return v3
    if ma_type == "TEMA":
        return v4
    if ma_type == "WMA":
        return v5
    if ma_type == "VWMA":
        return v6
    if ma_type == "SMMA":
        return v7
    if ma_type == "HULLMA":
        return v8
    if ma_type == "LSMA":
        return v9
    if ma_type == "ALMA":
        return v10
    if ma_type == "TMA":
        return v11
    if ma_type == "SSMA":
        return v12
    # default SMA
    return sma(src, length)


# =============================================================================
# PIVOTS + KDE (RSI pivot probability logic)
# =============================================================================

def find_pivots(high: pd.Series,
                low: pd.Series,
                left: int,
                right: int):
    n = len(high)
    high_pivots = pd.Series(False, index=high.index)
    low_pivots = pd.Series(False, index=low.index)

    for i in range(left, n - right):
        win_high = high.iloc[i - left : i + right + 1]
        if high.iloc[i] == win_high.max():
            high_pivots.iloc[i] = True
        win_low = low.iloc[i - left : i + right + 1]
        if low.iloc[i] == win_low.min():
            low_pivots.iloc[i] = True

    return high_pivots, low_pivots


def kde_density(x_values: np.ndarray,
                kernel: str,
                bandwidth: float,
                steps: int):
    if len(x_values) == 0:
        return None, None, None

    x_min = float(x_values.min())
    x_max = float(x_values.max())
    padding = (x_max - x_min) * 0.5 if x_max > x_min else 1.0
    x_min -= padding
    x_max += padding

    grid_x = np.linspace(x_min, x_max, steps * 2)

    def gaussian(distance, bw):
        return (1.0 / np.sqrt(2.0 * np.pi)) * np.exp(-0.5 * (distance / bw) ** 2)

    def uniform(distance, bw):
        return np.where(np.abs(distance) > bw, 0.0, 0.5)

    def sigmoid(distance, bw):
        return 2.0 / np.pi * (1.0 / (np.exp(distance / bw) + np.exp(-distance / bw)))

    density_y = np.zeros_like(grid_x)
    for val in x_values:
        dist = grid_x - val
        if kernel == "Gaussian":
            density_y += gaussian(dist, 1.0 / bandwidth)
        elif kernel == "Uniform":
            density_y += uniform(dist, 1.0 / bandwidth)
        elif kernel == "Sigmoid":
            density_y += sigmoid(dist, 1.0 / bandwidth)
        else:
            density_y += gaussian(dist, 1.0 / bandwidth)

    density_y /= float(len(x_values))
    cum_y = np.cumsum(density_y)
    return grid_x, density_y, cum_y


def kde_color_series(rsi_series: pd.Series,
                     rsi_high_pivots: np.ndarray,
                     rsi_low_pivots: np.ndarray,
                     kernel: str = "Gaussian",
                     bandwidth: float = kde_bandwidth,
                     steps: int = kde_steps,
                     activation_threshold: float = kde_activation_threshold):
    """
    Build curColor series:
    1 (bullish), -1 (bearish), 0 (none).
    """
    n = len(rsi_series)
    cur_color = pd.Series(0, index=rsi_series.index, dtype="int32")

    high_x, high_y, high_cum = kde_density(rsi_high_pivots, kernel, bandwidth, steps)
    low_x, low_y, low_cum = kde_density(rsi_low_pivots, kernel, bandwidth, steps)
    if high_x is None or low_x is None:
        return cur_color

    sum_low = float(low_y.sum())
    sum_high = float(high_y.sum())

    for i in range(n):
        val = float(rsi_series.iloc[i])
        idx_h = int(np.argmin(np.abs(high_x - val)))
        high_prob = float(high_cum[idx_h])
        idx_l = int(np.argmin(np.abs(low_x - val)))
        low_prob = float(low_cum[-1] - (low_cum[idx_l - 1] if idx_l > 0 else 0.0))

        if low_prob > sum_low * (1.0 - activation_threshold):
            cur_color.iloc[i] = 1
        elif high_prob > sum_high * (1.0 - activation_threshold):
            cur_color.iloc[i] = -1
        else:
            cur_color.iloc[i] = 0

    return cur_color


# =============================================================================
# THEIL-SEN ESTIMATOR (core calculation + channel break)
# =============================================================================

def all_slopes_ts(n: int, y: np.ndarray) -> np.ndarray:
    """All slopes between all pairs of n points (Theil–Sen)."""
    if n <= 2:
        return np.array([], dtype="float64")
    slopes = []
    for i in range(n - 1):
        for j in range(i + 1, n):
            slopes.append((y[i] - y[j]) / float(j - i))
    return np.array(slopes, dtype="float64")


def rnd_slopes_ts(slopes: np.ndarray, numpoints: int, seed_base: float) -> np.ndarray:
    """Random subset of slopes using pseudo-random generator similar in spirit."""
    slopes = slopes.copy()
    ntmp = len(slopes)
    n = min(ntmp, numpoints)
    if n <= 2 or ntmp == 0:
        return slopes

    s1 = int(seed_base) % 30269
    s2 = (int(seed_base * 1.3) + 1) % 30307
    s3 = (int(seed_base * 1.7) + 2) % 30323

    out = []
    for _ in range(n):
        s1 = 171 * (s1 % 30269)
        s2 = 172 * (s2 % 30307)
        s3 = 170 * (s3 % 30323)
        rnd = (float(s1) / 30269.0 + float(s2) / 30307.0 + float(s3) / 30323.0) % 1.0
        idx = int(rnd * ntmp)
        out.append(slopes[idx])
        slopes = np.delete(slopes, idx)
        ntmp -= 1
        if ntmp == 0:
            break

    return np.array(out, dtype="float64")


def intercepts_ts(src: np.ndarray, n: int, mslope: float) -> np.ndarray:
    if n <= 2:
        return np.array([], dtype="float64")
    intercepts = []
    # replicate: array.push(_I, _src[_n - 1 - i] - _mslope * i)
    for i in range(n):
        intercepts.append(src[n - 1 - i] - mslope * i)
    return np.array(intercepts, dtype="float64")


def theil_sen_channel(src: pd.Series,
                      length: int,
                      method: str,
                      numpairs: int,
                      mult: float):
    """
    Compute Theil-Sen slope, intercept, dev channel and 'isbroken' series.
    """
    n_total = len(src)
    isbroken = pd.Series(False, index=src.index)

    if not enable_theilsen:
        return {
            "mslope": np.nan,
            "minter": np.nan,
            "dev": np.nan,
            "isbroken": isbroken,
        }

    if n_total < length:
        return {
            "mslope": np.nan,
            "minter": np.nan,
            "dev": np.nan,
            "isbroken": isbroken,
        }

    # For performance we only compute channel for the last `length` bars
    win = src.iloc[-length:].values.astype("float64")
    n = len(win)

    S = all_slopes_ts(n, win)
    if method == "Random":
        S = rnd_slopes_ts(S, numpairs, float(win[-1]))
    if len(S) == 0:
        return {
            "mslope": np.nan,
            "minter": np.nan,
            "dev": np.nan,
            "isbroken": isbroken,
        }

    mslope = float(np.median(S))
    I = intercepts_ts(win, n, mslope)
    if len(I) == 0:
        return {
            "mslope": mslope,
            "minter": np.nan,
            "dev": np.nan,
            "isbroken": isbroken,
        }
    minter = float(np.median(I))

    # RMSD over window
    rmse = 0.0
    for j in range(n):
        # Pine: ts_src[j] - (minter + mslope * (ts_current_len - j))
        rmse += (win[j] - (minter + mslope * (n - j))) ** 2 / n
    dev = mult * (rmse ** 0.5)

    # Last bar index
    last_price = float(src.iloc[-1])
    ts_y2 = minter + mslope * (n - 1)

    broken_last = (last_price > ts_y2 + dev) or (last_price < ts_y2 - dev)
    isbroken.iloc[-1] = broken_last
    # NOTE: Pine uses isbroken and not isbroken[1]; هنا نحسب الكسر على آخر شمعة فقط

    return {
        "mslope": mslope,
        "minter": minter,
        "dev": dev,
        "isbroken": isbroken,
    }


# =============================================================================
# CORE INDICATOR LOGIC (all main systems)
# =============================================================================

def compute_signal_system(df: pd.DataFrame):
    """
    PRO V6 'Signal System' re-implementation.

    Returns:
        pro_signal_up, pro_signal_down : Series(bool)
    """
    if not enable_signal_system:
        n = len(df)
        return pd.Series([False] * n, index=df.index), pd.Series([False] * n, index=df.index)

    c = df["close"]
    h = df["high"]
    l = df["low"]

    x_atr = atr(h, l, c, volatility_period)
    n_loss = sensitivity * x_atr

    x_signal_line = pd.Series(index=c.index, dtype="float64")
    x_signal_line.iloc[0] = c.iloc[0]

    for i in range(1, len(df)):
        src = c.iloc[i]
        src_prev = c.iloc[i - 1]
        prev_line = x_signal_line.iloc[i - 1]
        loss = n_loss.iloc[i]

        iff_1 = src - loss if src > prev_line else src + loss
        if src < prev_line and src_prev < prev_line:
            iff_2 = min(prev_line, src + loss)
        else:
            iff_2 = iff_1

        if src > prev_line and src_prev > prev_line:
            x_signal_line.iloc[i] = max(prev_line, src - loss)
        else:
            x_signal_line.iloc[i] = iff_2

    pro_signal_up = crossover(c, x_signal_line)
    pro_signal_down = crossunder(c, x_signal_line)
    return pro_signal_up.fillna(False), pro_signal_down.fillna(False)


def compute_trend_following_zones(df: pd.DataFrame):
    """
    PRO V6 'Trend Following + Zones' (CHỈ BÁO 2).

    Returns:
        trail: Series(float)
        ex: Series(float)           - key points
        state: Series(str)          - 'up'/'down'
        z1, z2, z3: Series(float)   - zone levels
        zone_cross_signals: dict of Series(bool)
    """
    h = df["high"]
    l = df["low"]
    c = df["close"]

    # Normalized price == using current timeframe (we don't change timeframe here)
    norm_h = h
    norm_l = l
    norm_c = c

    # Enhanced MA for trueRange
    def enhanced_ma(src: pd.Series, length: int) -> pd.Series:
        enhanced = pd.Series(index=src.index, dtype="float64")
        for i in range(len(src)):
            if i == 0:
                enhanced.iloc[0] = src.iloc[0]
            else:
                prev = enhanced.iloc[i - 1]
                enhanced.iloc[i] = prev + (src.iloc[i] - prev) / length
        return enhanced

    HiLo = np.minimum(norm_h - norm_l,
                      1.5 * (norm_h - norm_l).rolling(trend_period).mean())
    HRef = np.where(
        norm_l <= norm_h.shift(1),
        norm_h - norm_c.shift(1),
        norm_h - norm_c.shift(1) - 0.5 * (norm_l - norm_h.shift(1)),
    )
    LRef = np.where(
        norm_h >= norm_l.shift(1),
        norm_c.shift(1) - norm_l,
        norm_c.shift(1) - norm_l - 0.5 * (norm_l.shift(1) - norm_h),
    )

    HRef = pd.Series(HRef, index=df.index)
    LRef = pd.Series(LRef, index=df.index)

    if follow_type == "enhanced":
        true_range_tf = np.maximum.reduce([HiLo, HRef, LRef])
        true_range_tf = pd.Series(true_range_tf, index=df.index)
    else:
        true_range_tf = pd.Series(
            np.maximum.reduce(
                [
                    norm_h - norm_l,
                    (norm_h - norm_c.shift(1)).abs(),
                    (norm_l - norm_c.shift(1)).abs(),
                ]
            ),
            index=df.index,
        )

    loss = trend_multiplier * enhanced_ma(true_range_tf, trend_period)

    Up = norm_c - loss
    Dn = norm_c + loss

    TrendUp = Up.copy()
    TrendDown = Dn.copy()
    Trend = pd.Series(1, index=df.index, dtype="int32")

    for i in range(1, len(df)):
        if norm_c.iloc[i - 1] > TrendUp.iloc[i - 1]:
            TrendUp.iloc[i] = max(Up.iloc[i], TrendUp.iloc[i - 1])
        else:
            TrendUp.iloc[i] = Up.iloc[i]

        if norm_c.iloc[i - 1] < TrendDown.iloc[i - 1]:
            TrendDown.iloc[i] = min(Dn.iloc[i], TrendDown.iloc[i - 1])
        else:
            TrendDown.iloc[i] = Dn.iloc[i]

        if norm_c.iloc[i] > TrendDown.iloc[i - 1]:
            Trend.iloc[i] = 1
        elif norm_c.iloc[i] < TrendUp.iloc[i - 1]:
            Trend.iloc[i] = -1
        else:
            Trend.iloc[i] = Trend.iloc[i - 1]

    trail = pd.Series(index=df.index, dtype="float64")
    for i in range(len(df)):
        trail.iloc[i] = TrendUp.iloc[i] if Trend.iloc[i] == 1 else TrendDown.iloc[i]

    ex = pd.Series(0.0, index=df.index, dtype="float64")
    for i in range(1, len(df)):
        if Trend.iloc[i] > 0 and Trend.iloc[i - 1] <= 0:
            ex.iloc[i] = norm_h.iloc[i]
        elif Trend.iloc[i] < 0 and Trend.iloc[i - 1] >= 0:
            ex.iloc[i] = norm_l.iloc[i]
        else:
            if Trend.iloc[i] == 1:
                ex.iloc[i] = max(ex.iloc[i - 1], norm_h.iloc[i])
            elif Trend.iloc[i] == -1:
                ex.iloc[i] = min(ex.iloc[i - 1], norm_l.iloc[i])
            else:
                ex.iloc[i] = ex.iloc[i - 1]

    state = pd.Series(np.where(Trend > 0, "up", "down"), index=df.index)

    z1 = ex + (trail - ex) * zone1_level / 100.0
    z2 = ex + (trail - ex) * zone2_level / 100.0
    z3 = ex + (trail - ex) * zone3_level / 100.0

    norm_c_series = norm_c

    z1_long = enable_trend_follow & (state.shift(1) == "up") & crossunder(norm_c_series, z1.shift(1))
    z2_long = enable_trend_follow & (state.shift(1) == "up") & crossunder(norm_c_series, z2.shift(1))
    z3_long = enable_trend_follow & (state.shift(1) == "up") & crossunder(norm_c_series, z3.shift(1))

    z1_short = enable_trend_follow & (state.shift(1) == "down") & crossover(norm_c_series, z1.shift(1))
    z2_short = enable_trend_follow & (state.shift(1) == "down") & crossover(norm_c_series, z2.shift(1))
    z3_short = enable_trend_follow & (state.shift(1) == "down") & crossover(norm_c_series, z3.shift(1))

    zone_cross_signals = {
        "z1_long": z1_long.fillna(False),
        "z2_long": z2_long.fillna(False),
        "z3_long": z3_long.fillna(False),
        "z1_short": z1_short.fillna(False),
        "z2_short": z2_short.fillna(False),
        "z3_short": z3_short.fillna(False),
    }

    return trail, ex, state, z1, z2, z3, zone_cross_signals


def compute_trend2_signals(df: pd.DataFrame):
    """
    PRO V6 'Trend 2' logic (ALMA/TEMA/Hull basis, RSI filter).

    Returns:
        long_entry, short_entry, strong_buy, strong_sell : Series(bool)
    """
    if not enable_trend2:
        n = len(df)
        false_series = pd.Series([False] * n, index=df.index)
        return false_series, false_series, false_series, false_series

    c = df["close"]
    o = df["open"]
    v = df["volume"]

    close_shifted = c.shift(trend2_delay_offset).fillna(method="bfill")
    open_shifted = o.shift(trend2_delay_offset).fillna(method="bfill")

    close_series = variant_ma(
        trend2_basis_type,
        close_shifted,
        trend2_basis_length,
        trend2_offset_sigma,
        trend2_offset_alma,
        v,
    )
    open_series = variant_ma(
        trend2_basis_type,
        open_shifted,
        trend2_basis_length,
        trend2_offset_sigma,
        trend2_offset_alma,
        v,
    )

    close_series_alt = close_series
    open_series_alt = open_series

    le_trigger = crossover(close_series_alt, open_series_alt)
    se_trigger = crossunder(close_series_alt, open_series_alt)

    rsi_val = rsi(c, trend2_rsi_period)
    rsi_ema = ema(rsi_val, trend2_rsi_ema_length)
    ema_price = ema(c, trend2_ema_period)

    rsi_ob = (rsi_val > trend2_rsi_ob) & (rsi_val > rsi_ema)
    rsi_os = (rsi_val < trend2_rsi_os) & (rsi_val < rsi_ema)

    price_above_ema = c > ema_price
    price_below_ema = c < ema_price

    rsi_bullish = rsi_os | ((rsi_val < 50.0) & price_above_ema)
    rsi_bearish = rsi_ob | ((rsi_val > 50.0) & price_below_ema)

    strong_buy = le_trigger & rsi_bullish
    strong_sell = se_trigger & rsi_bearish

    long_entry = le_trigger
    short_entry = se_trigger

    return (
        long_entry.fillna(False),
        short_entry.fillna(False),
        strong_buy.fillna(False),
        strong_sell.fillna(False),
    )


def compute_rsi_kde_pivots(df: pd.DataFrame):
    """
    PRO V6 RSI + KDE pivot probability (Possible Bullish/Bearish Pivot).
    """
    c = df["close"]
    h = df["high"]
    l = df["low"]

    rsi_kernel = rsi(c, rsi_kde_length)

    high_pivots, low_pivots = find_pivots(h, l, high_pivot_len, high_pivot_len)
    rsi_high_vals = rsi_kernel[high_pivots].dropna().values
    rsi_low_vals = rsi_kernel[low_pivots].dropna().values

    if len(rsi_high_vals) > kde_limit:
        rsi_high_vals = rsi_high_vals[-kde_limit:]
    if len(rsi_low_vals) > kde_limit:
        rsi_low_vals = rsi_low_vals[-kde_limit:]

    cur_color = kde_color_series(
        rsi_kernel,
        rsi_high_vals,
        rsi_low_vals,
        kernel="Gaussian",
        bandwidth=kde_bandwidth,
        steps=kde_steps,
        activation_threshold=kde_activation_threshold,
    )

    prev_color = cur_color.shift(1)
    possible_bullish_pivot = (cur_color == 0) & (prev_color == 1)
    possible_bearish_pivot = (cur_color == 0) & (prev_color == -1)

    return possible_bullish_pivot.fillna(False), possible_bearish_pivot.fillna(False)


def compute_trend_ema_dashboard(df: pd.DataFrame):
    """
    Simplified EMA trend for 'trend changed' style alerts.

    Returns:
        trend_indicator : Series(int) with 1 (uptrend), -1 (downtrend), 0 (neutral)
    """
    c = df["close"]
    ema_val = ema(c, trend_ema_length)
    ema_prev = ema_val.shift(1)

    trend_indicator = pd.Series(0, index=df.index, dtype="int32")
    for i in range(len(df)):
        if ema_val.iloc[i] > ema_prev.iloc[i]:
            trend_indicator.iloc[i] = 1
        elif ema_val.iloc[i] < ema_prev.iloc[i]:
            trend_indicator.iloc[i] = -1
        else:
            trend_indicator.iloc[i] = trend_indicator.iloc[i - 1] if i > 0 else 0
    return trend_indicator


def compute_all_pro_v6_signals(df: pd.DataFrame):
    """
    Aggregates all systems and returns a dict of signal Series.
    """
    pro_up, pro_down = compute_signal_system(df)
    trail, ex, state, z1, z2, z3, zone_cross = compute_trend_following_zones(df)
    long_entry, short_entry, strong_buy, strong_sell = compute_trend2_signals(df)
    pb_pivot, ps_pivot = compute_rsi_kde_pivots(df)

    ts_channel = theil_sen_channel(
        df[ts_src_field],
        ts_len,
        ts_method,
        ts_numpairs,
        ts_mult,
    )

    trend_indicator = compute_trend_ema_dashboard(df)

    signals = {
        # main requested
        "pro_signal_up": pro_up,
        "pro_signal_down": pro_down,
        "long_entry": long_entry,
        "short_entry": short_entry,
        "strong_buy": strong_buy,
        "strong_sell": strong_sell,
        "possible_bullish_pivot": pb_pivot,
        "possible_bearish_pivot": ps_pivot,
        # zones
        "z1_long": zone_cross["z1_long"],
        "z2_long": zone_cross["z2_long"],
        "z3_long": zone_cross["z3_long"],
        "z1_short": zone_cross["z1_short"],
        "z2_short": zone_cross["z2_short"],
        "z3_short": zone_cross["z3_short"],
        # Theil-Sen
        "theil_sen_break": ts_channel["isbroken"],
        # EMA trend dashboard (trend change)
        "trend_indicator": trend_indicator,
    }

    return signals


# =============================================================================
# DATA / SCANNER LAYER (CCXT Binance USDT-M Futures)
# =============================================================================

def init_exchange():
    """
    Initialize ccxt binanceusdm exchange (USDT-M futures).
    """
    exchange = ccxt.binanceusdm({
        "apiKey": API_KEY,
        "secret": API_SECRET,
        "enableRateLimit": True,
    })
    exchange.load_markets()
    return exchange


def get_usdtm_symbols(exchange):
    markets = exchange.markets
    symbols = [s for s, m in markets.items()
               if m.get("quote") == "USDT" and m.get("type") == "future"]
    symbols = sorted(set(symbols))

    if not USE_ALL_SYMBOLS:
        symbols = [s for s in symbols if s in SYMBOLS_WHITELIST]

    return symbols


def fetch_ohlcv_df(exchange, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
    try:
        raw = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    except Exception as exc:
        print(f"[ERROR] fetch_ohlcv failed for {symbol} ({timeframe}): {exc}")
        return None

    if not raw:
        return None

    df = pd.DataFrame(
        raw,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df.sort_values("timestamp", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def _last_true_index(series: pd.Series, lookback_bars: int):
    if lookback_bars is not None and lookback_bars > 0:
        sub = series.iloc[-lookback_bars:]
        idxs = sub[sub].index
    else:
        idxs = series[series].index
    if len(idxs) == 0:
        return None
    return idxs[-1]


def print_signals(symbol: str,
                  timeframe: str,
                  df: pd.DataFrame,
                  signals: dict):
    now_utc = datetime.now(timezone.utc)
    ts_series = df["timestamp"]

    min_time = None
    if ALERT_LOOKBACK_MINUTES and ALERT_LOOKBACK_MINUTES > 0:
        min_time = now_utc - timedelta(minutes=ALERT_LOOKBACK_MINUTES)

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
    }

    for key, series in signals.items():
        if key == "trend_indicator":
            # trend change alert: compare current vs previous
            ti = series
            if len(ti) < 2:
                continue
            if ti.iloc[-1] != ti.iloc[-2]:
                ts_sig = ts_series.iloc[-1]
                if min_time is not None and ts_sig < min_time:
                    continue
                label = labels[key]
                price_sig = price_series.iloc[-1]
                time_str = ts_sig.strftime("%Y-%m-%d %H:%M:%S")
                trend_str = "Uptrend" if ti.iloc[-1] == 1 else "Downtrend" if ti.iloc[-1] == -1 else "Neutral"
                print(
                    f"SYMBOL: {symbol} | TIMEFRAME: {timeframe} | SIGNAL: {label} ({trend_str}) | PRICE: {price_sig:.4f} | TIME: {time_str}"
                )
            continue

        last_idx = _last_true_index(series, ALERT_LOOKBACK_BARS)
        if last_idx is None:
            continue

        ts_sig = ts_series.loc[last_idx]
        if min_time is not None and ts_sig < min_time:
            continue

        price_sig = price_series.loc[last_idx]
        label = labels.get(key, key)
        time_str = ts_sig.strftime("%Y-%m-%d %H:%M:%S")
        print(
            f"SYMBOL: {symbol} | TIMEFRAME: {timeframe} | SIGNAL: {label} | PRICE: {price_sig:.4f} | TIME: {time_str}"
        )


def process_symbol(exchange, symbol: str, timeframe: str):
    df = fetch_ohlcv_df(exchange, symbol, timeframe, MAX_HISTORY_BARS)
    if df is None or len(df) < 200:
        return

    try:
        signals = compute_all_pro_v6_signals(df)
    except Exception as exc:
        print(f"[ERROR] compute_all_pro_v6_signals failed for {symbol}: {exc}")
        return

    print_signals(symbol, timeframe, df, signals)


def run_scan_once():
    exchange = init_exchange()
    symbols = get_usdtm_symbols(exchange)
    timeframe = INPUT_TIMEFRAME or BASE_TIMEFRAME
    print(f"[INFO] Scanning {len(symbols)} Binance USDT-M symbols on {timeframe}")

    for i, symbol in enumerate(symbols, start=1):
        process_symbol(exchange, symbol, timeframe)
        time.sleep(SLEEP_BETWEEN_SYMBOLS)


def main():
    if RUN_CONTINUOUS:
        while True:
            run_scan_once()
            print(f"[INFO] Sleeping {LOOP_SLEEP_SECONDS} seconds before next scan...")
            time.sleep(LOOP_SLEEP_SECONDS)
    else:
        run_scan_once()


if __name__ == "__main__":
    main()
