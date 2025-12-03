Corrections for PRO V6 Python Code to Match TradingView Indicator

To ensure the Python implementation of the PRO V6 indicator matches the original Pine Script with ~99.99% accuracy, the following corrections and enhancements are needed. These address multi-timeframe handling, signal color logic, KDE pivot calculations, inclusion of visual-based signals, and a minor variable naming fix. After applying these changes, the Python code’s outputs (signal names, timing, values, and chart markings) should align almost exactly with the TradingView version across all symbols and timeframes.

1. Synchronize Multi-Timeframe Signals (request.security Behavior)

The original Pine script uses request.security to pull higher-timeframe data (as specified by the Period input res) and synchronize those signals on the lower timeframe chart. To replicate this in Python, implement the following:

Higher Timeframe Data Fetch: If INPUT_TIMEFRAME (the res input) is not empty and differs from the base chart timeframe, fetch an OHLCV dataset for that higher timeframe (using the same MAX_HISTORY_BARS limit). This provides the reference data needed for multi-timeframe signals.

Higher Timeframe Pivot Calculation: Using the fetched higher-TF data, compute pivot highs/lows with the same lengths (high_pivot_len/low_pivot_len). For example, use find_pivots() on the higher-TF highs/lows (e.g. 21 bars left/right by default) to get boolean series of pivot points. This mirrors Pine’s ta.pivothigh/ta.pivotlow on the higher timeframe.

Signal Time Alignment: Map each detected higher-TF pivot to the base timeframe index. In TradingView, a higher-TF pivot signal only appears on the completion of that higher-TF bar. Therefore, find the timestamp of the pivot bar on the higher timeframe and locate the corresponding bar on the base timeframe (likely the bar with the same closing time or the immediately following base bar). Mark a pivot event on the base timeframe at that point. Essentially, propagate the higher-TF pivot value down so that it stays na on base bars until the moment the higher-TF bar closes and confirms the pivot.

Replace/Combine Base Pivots: Use these time-aligned higher-TF pivot booleans in place of (or in addition to) the current base timeframe pivots when generating signals. In practice, if a higher timeframe is specified, the Python code should simulate Pine’s behavior by using the higher-TF pivots for signal logic, not the base pivots. This will ensure signals like trendline breaks or “Possible Pivot” alerts trigger at the exact same bars as in TradingView. For example, a daily pivot high will cause a “Possible Bearish Pivot” on the lower timeframe chart at the daily bar’s close, matching the indicator.


By implementing the above, the multi-timeframe signals will be synchronized correctly. This means that any condition in Pine that uses request.security (such as confirming higher-TF pivots or trends) will be emulated in Python. The goal is that a signal which depends on a higher timeframe (for instance, a trendline or pivot from the higher period) should appear at the identical time and value in the Python output as it does on TradingView.

2. Match curColor Logic and Pivot Signal Transitions

The curColor series in the indicator designates bullish or bearish pivot conditions and is used to trigger “Possible Bullish/Bearish Pivot” signals. It’s critical to reproduce its values and transitions exactly:

Reproduce Pine’s Conditions: In the Pine script, curColor is set based on how the current RSI value relates to the KDE distribution of past pivot RSI values. In “Sum” mode (the default), Pine marks curColor bullish if lowProb exceeds (1 - activationThreshold) of the low-pivot distribution, and bearish if highProb exceeds that threshold of the high-pivot distribution. Ensure the Python code implements these exact comparisons. The current code’s logic using activation_threshold (e.g. 0.25 for “Medium”) is correct – for instance:

if low_prob > sum_low * (1 - kde_activation_threshold):  
    cur_color[i] = 1  # bullish  
elif high_prob > sum_high * (1 - kde_activation_threshold):  
    cur_color[i] = -1  # bearish  
else:  
    cur_color[i] = 0  # none

This mirrors the Pine checks for curColor := bullishColor or bearishColor when those probability conditions are met.

Use Proper Previous-State Logic: Pine only triggers the “Possible Bullish Pivot” alert when a bullish curColor just turned off (i.e. went from bullish on the prior bar to neutral on the current bar), and similarly for bearish. The Python code already captures this by computing:

prev_color = cur_color.shift(1)  
possible_bullish_pivot = (cur_color == 0) & (prev_color == 1)  
possible_bearish_pivot = (cur_color == 0) & (prev_color == -1)

Make sure this logic remains in place. It ensures the “Possible Pivot” signals occur once, at the bar where curColor returns to neutral after being colored (just as Pine uses na(curColor) and curColor[1] == bullishColor to trigger the alert). In practice, this means the Python output will flag a possible pivot exactly at the moment the Pine script draws the pivot arrow on the chart.

NA vs. 0 Representation: In Pine, curColor is a color value or na (no color) when inactive. In Python we use integers (1 for bullish, -1 for bearish, 0 for none). This is fine – just treat 0 as the equivalent of “no color/na”. The transition checks above already account for that. Verify that initially, before any pivots are identified (or if distributions aren’t available), curColor is effectively neutral (no signal). For example, if no pivot distribution exists yet, Pine leaves curColor as na; the Python code should similarly leave it at 0 (which it currently does by default when kde_density returns None).


With these measures, the order and color changes of curColor will mirror the original. Every time curColor flips from green to neutral or red to neutral in Pine (indicating a possible pivot), the Python code will register the same shift from 1 to 0 or -1 to 0, triggering the corresponding “Possible Bullish/Bearish Pivot” output at the identical bar.

3. KDE Pivot Distribution Accuracy (KDELimit and Probability Calc)

The KDE-based pivot probability calculation must follow the indicator’s logic to the letter, as it underpins the curColor signals:

Maintain KDE Value Limits: The Pine script limits the stored pivot RSI samples to KDELimit = 300 points. The Python code should continue to do the same. Currently, it truncates the rsi_high_vals and rsi_low_vals arrays to the latest 300 values before computing the KDE – this is correct. Double-check that this slicing uses the most recent 300 pivot values (which it does with rsi_high_vals[-kde_limit:], etc.). This ensures the KDE distribution is built from a rolling window of recent pivots, just like the Pine code which remove()s the oldest value when the array grows beyond 300.

Exact Probability Computation: The method for deriving highProb and lowProb from the KDE must match Pine’s approach. In Pine’s “Sum” mode, they compute:
– highProb as the cumulative probability from the lowest grid value up to the current RSI (essentially CDF(current_RSI) of the high-pivot PDF). The code achieves this by taking the prefix sum up to the nearest index in the KDE array.
– lowProb as the complementary cumulative probability from the current RSI to the maximum (the upper tail probability). Pine computes this by subtracting the prefix sum below the current point from the total sum.
Verify that kde_color_series does exactly this. In the provided Python code, for each bar’s RSI value val:

idx_h = np.argmin(np.abs(high_x - val))  
high_prob = high_cum[idx_h]  
idx_l = np.argmin(np.abs(low_x - val))  
low_prob = high_cum[-1] - (low_cum[idx_l - 1] if idx_l > 0 else 0.0)

Here, high_cum is the cumulative sum array of the high-pivot density (so high_cum[idx_h] is the CDF up to val), and low_prob is calculated as 1 minus the CDF up to just below val (giving the tail area). This corresponds exactly to Pine’s highProb := prefixSum(KDEHighYSum, 0, nearestIndex) and lowProb := prefixSum(KDELowYSum, nearestIndex, end) in Sum mode. Ensure the code correctly handles edge cases (e.g., if idx_l = 0, it uses the full sum for low_prob). The goal is to have virtually identical highProb/lowProb values as the Pine script for any given bar.

Incremental KDE Updates: In TradingView, the KDE distribution updates only when a new pivot is confirmed (the arrays are updated and the KDE recalculated inside the pivot detection blocks). This means that on bars between pivots, KDEHighY/KDELowY remain the same. The Python implementation, which recomputes KDE from all pivots each time, should inherently yield the same probabilities for those bars (since the input pivot list hasn’t changed). However, for absolute fidelity, you might consider emulating this behavior: only refresh the KDE distribution at bars where a new pivot is added. This can be done by caching the last computed high_x, high_y, low_x, low_y and only recalculating when high_pivots or low_pivots has a new True. While not strictly necessary for output matching, this ensures the probability values and curColor do not inadvertently jitter on bars without new pivots (they shouldn’t, but this extra caution mirrors Pine’s update cadence).


By following the above, the KDE pivot probability logic in Python will be a faithful reproduction of the original. This guarantees that values like lowProb, highProb, and the resulting curColor assignment on each bar match the Pine indicator, and that the 300-point distribution window is respected just as in the original code.

4. Include Trend Ribbon, Cloud, and Zones in Signal Conditions

Several visual elements in the PRO V6 indicator (Trend Ribbon, Cirrus Cloud, Supply/Demand zones, etc.) do not just decorate the chart – they tie into the trend-following logic and alerts. It’s important to ensure any effect they have on alerts is captured in the Python code:

Trend Ribbon / Trend State: The “Trend Ribbon” reflects the current trend (bullish or bearish) on the chart, which in code is represented by the Trend state or state series (“up”/“down”). The Python implementation of the trend-following system already computes state based on the trailing stop logic (ATR-based) and uses it for zone alerts. Make sure that any alerts depending on trend changes are accounted for. For example, the original Pine has a “Dynamic Line Change” alert (which triggers when the trailing stop z1/ex line flips direction or updates). In Python, you can replicate this if needed by checking when ex or trail value changes sign or resets (e.g., when a new high in an uptrend or new low in a downtrend is established). This might correspond to when your state switches or when a new extreme (ex) is set. While the current code does not explicitly output this, consider adding a boolean series for “dynamic change” (e.g., z1 value today ≠ yesterday) to match that alert.

Cirrus Cloud: This is a visual cloud (plotted via filtx1/filtx2 in Pine) that smooths price for trend confirmation. It does not directly produce alerts. However, if the cloud influences any signals (for instance, if certain entries are filtered by the cloud), those conditions should be reflected. From the provided Pine code, the cloud is likely purely visual (turned on/off by CirrusCloud input) and does not appear in the alert conditions. Thus, it may not require any code logic for alerts. Just ensure that if any part of the trend logic was meant to use a smoothed value (the cloud), the Python code does so. In our case, the trend-following code uses an enhanced MA (follow_type="enhanced") for ATR calculation, which is already implemented, so the essence of the cloud’s effect (smoothing price oscillations) is captured.

Supply/Demand Zones: These zones are drawn around recent swing highs (supply) and swing lows (demand). In Pine, they are likely used for visual context and possibly for identifying break-of-structure (BOS) events. Check if any alert conditions relate to these zones being broken or touched. The primary alert conditions we identified in the Pine script are the Zone 1/2/3 Cross alerts, which are implemented in Python (z1_long, z2_long, etc., via the zone_cross_signals). The Python code correctly triggers these when price crosses under a zone in an uptrend or over a zone in a downtrend (using crossunder/crossover with state filters). This aligns with the Pine definitions (e.g., z1_long is true when state[1] == 'up' and crossunder(close, z1[1])). Ensure these zone-cross signals are being output with the exact same names and logic (the provided code does so, naming them “Zone 1/2/3 Cross Down/Up” accordingly in the labels mapping). Aside from those, if the Pine script doesn’t define separate alerts for supply/demand zone creation or BOS, we don’t need to add extra signals. Just verify that the existing zone cross alerts in Python use the same conditions (previous bar’s state and previous zone level) as Pine, which they currently do.

Final Visual Checks: No other purely visual feature (like background colors or table dashboards) directly affects the alert conditions. The Trend Ribbon colors and Cirrus Cloud fill are byproducts of the trend state, which we have covered. The dashboard table and gauge in Pine are visual-only and can be ignored in the Python logic since they don’t generate alerts (the Python scanner focuses on alerts/signals). The key is that all signals which Pine can alert on are represented. Cross-check the list of alertcondition titles in Pine against the Python’s signals dict and printed outputs. You should find: “Pro Signal Up/Down”, “Zone 1/2/3 Cross Down/Up (Uptrend/Downtrend)”, “Long/Short Entry Alert”, “Strong Buy/Sell Alert”, “Possible Bullish/Bearish Pivot”, “Theil-Sen Channel Break”, and “Trend changed”. The Python code covers all of these except possibly the Dynamic Line Change (discussed above) and the multi-timeframe trend alignment alerts (which are part of a Dashboard feature in Pine). Those multi-TF alignment alerts (TF3, TF4, TF5 aligned, etc.) can likely be omitted, as the scanner is focused on single-symbol signals. As a final step, confirm that enabling/disabling features (e.g. enable_trend_follow, enable_trend2) in the Python config correctly turns those signal computations on/off, mirroring Pine’s behavior when those features are toggled.


By accounting for these elements, all chart features that contribute to alerts are included in the Python logic. The trend ribbon and cloud ensure the trend state is correctly determined, and the zone calculations ensure zone-cross alerts fire exactly when they should. Nothing that appears as an alert in Pine should be missing from the Python outputs.

5. Use Correct KDELimit Naming and Usage

Lastly, fix the minor issue of the KDELimit variable name and usage to avoid confusion. In the Pine script, KDELimit is a constant (300) controlling the max number of stored pivot RSI values. In the Python code, this is implemented as kde_limit = 300. To maintain clarity and consistency with the original code:

Rename if Necessary: You may rename kde_limit to KDELimit or add a comment linking it to Pine’s KDELimit. While Python style typically uses lowercase, it’s important the developer recognizes this corresponds exactly to the Pine constant. The main point is to use the correct value (300, unless changed in Pine inputs) and apply it identically.

Ensure Proper Application: Double-check that wherever pivot RSI arrays are handled, this limit is applied as in Pine. The current implementation – trimming the pivot arrays to length 300 before KDE computation – is correct. Maintain this behavior after any modifications. If you adjust the code to update the KDE incrementally, be sure to similarly drop older values when the count exceeds 300. This guarantees the Python’s probability model never includes more historical pivot points than Pine’s does.


By addressing the above, you eliminate any discrepancy in how many data points feed the KDE (which could otherwise skew probabilities). Using the exact KDELimit logic from the original ensures the Python indicator’s sensitivity to older pivots is the same as TradingView’s.


---

Implementing these corrections will align the Python scanner’s outputs with the PRO V6 TradingView indicator to an extremely high degree of fidelity. All signal names, trigger conditions, and timings will correspond one-to-one with the original Pine Script behavior. After these changes, you should test the Python output against TradingView on multiple symbols and timeframes to verify that every alert (Pro Signal up/down, entries, strong buy/sell, zone crosses, pivots, etc.) triggers at the same bars with the same values. With the multi-timeframe handling and fine details corrected, any residual discrepancy should be negligible, meeting the 99.99% accuracy goal.
