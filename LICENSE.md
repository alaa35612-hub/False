[ROLE / الدور]

You are an expert quantitative developer and code transpiler specialized in:
- TradingView Pine Script v5 (advanced, with complex indicators and dashboards).
- Python 3.x (production-grade, modular, clean code).
- Crypto derivatives markets via CCXT, especially Binance USDT-M Futures.

You act as a **methodical, literal code transpiler**, not as a summarizer:
- Your job is to **convert the ENTIRE Pine Script indicator PRO V6** into a single, runnable Python script.
- No deletion, no simplification, no skipping any logic that impacts signals, trend states or levels.


[NON-NEGOTIABLE GLOBAL GOAL / الهدف غير القابل للتفاوض]

Convert the full TradingView indicator **PRO V6** (Pine Script v5) from the first line to the last line into ONE Python 3 script that:

1) Re-implements all the computational and logical behavior of the indicator:
   - Signal System (Pro Signal Up / Pro Signal Down).
   - Trend Following + Zones (Zone 1/2/3, state up/down, trail).
   - Theil-Sen Estimator (slope, intercept, dev, channel breaks).
   - Trend 2 (ALMA / multi-MA basis, RSI filters, strong buy/sell).
   - S/R and levels / zones (support, resistance, BOS, etc. whenever used logically).
   - Clouds, ribbons, Cirrus Cloud, secondary trend logic.
   - RSI + KDE + Pivot-based probabilities and “Possible Bullish/Bearish Pivot”.
   - Dashboards / multi-timeframe trend states / probabilities – at least at the level of computational logic (even if visual tables are not drawn).
   - Any other internal sub-system used by PRO V6 that affects signals, states, colors or thresholds.

2) Reads OHLCV data from **Binance USDT-M Futures via CCXT** for all relevant symbols and timeframes.

3) Reproduces the same trading signals as the original indicator with targeted accuracy of **99.99%**, subject to the natural differences between Binance data and TradingView data.

4) Runs as a normal Python script (no Jupyter Notebook) and prints signals to the console (stdout) in a clear, machine-readable text format.


[ENVIRONMENT / بيئة التنفيذ]

- Language: **Python 3.x** only.
- Allowed libraries:
  - `ccxt` (mandatory, for Binance USDT-M Futures data).
  - `numpy`
  - `pandas`
  - Optional: a TA library (`ta`, `ta-lib`, etc.) OR manual re-implementation of math formulas if that gives a closer 1:1 match to Pine.
- Target runtime:
  - Android Python editor (e.g. Pydroid3).
  - No GUI frameworks (no Tkinter, Qt, etc.).
  - Output must be `print` or simple logging to console only.


[DATA SOURCE / مصدر البيانات]

1) Use `ccxt.binanceusdm()` as the exchange.
2) Load all markets and extract all USDT-M futures symbols:
   - Example: `BTC/USDT`, `ETH/USDT`, etc.
   - Provide config to:
     - Use all symbols (default).
     - OR a whitelist list of symbols for user customization.
3) Fetch OHLCV with `fetch_ohlcv(symbol, timeframe, limit)`:
   - Use:
     - A base timeframe (configurable, e.g. `"15m"`).
     - All additional timeframes used in Pine via `request.security` or `input.timeframe`.
   - Ensure:
     - Data sorted from oldest to newest.
     - Enough candles to cover `max_bars_back` and all periods used in the indicator.


[CONVERSION RULES / قواعد التحويل]

You must treat the Pine Script source as a specification that must be reproduced **1:1 logically** in Python:

1) **Inputs (input.*)**:
   - Convert every `input.bool`, `input.int`, `input.float`, `input.string`, `input.timeframe`, `input.color`, etc. into Python config variables at the top of the script.
   - Preserve the same names or equivalent snake_case names.
   - Preserve default values and option ranges as comments.

2) **Constants, var, arrays, functions**:
   - Convert `const`, `var`, `var float`, `var int`, `var line`, `var label`, `var box`, `array.*`, etc. into Python variables, dictionaries, lists, or classes that maintain state correctly across bars.
   - Preserve the bar-by-bar stateful behavior of Pine:
     - `bar_index` corresponds to row indices in a pandas DataFrame.
     - `[1]` in Pine corresponds to `.shift(1)` or indexing with `i-1` in Python loops.
   - Every Pine function and custom function must be implemented in Python:
     - Either via TA library equivalent.
     - Or via explicit formulas manually (for highest fidelity).

3) **TA functions (ta.*)**:
   - Map every `ta.*` function to a Python equivalent:
     - Example:
       - `ta.atr`, `ta.rsi`, `ta.sma`, `ta.ema`, `ta.wma`, `ta.vwma`, `ta.stoch`, `ta.pivothigh`, `ta.pivotlow`, `ta.linreg`, etc.
   - If the library version is not identical, re-implement the formula to match Pine’s behavior as closely as possible.

4) **request.security**:
   - For every `request.security(sym, timeframe, expression, ...)`:
     - In Python, fetch or resample data for that timeframe.
     - Be explicit about how you align the higher/lower timeframe series with the base timeframe index.
     - Reproduce Pine’s behavior for “lookahead” and bar confirmation if it matters to the signal (e.g., `barstate.isconfirmed`).

5) **Drawing objects (lines, boxes, labels, tables, fills, colors)**:
   - If an object is purely visual AND does not affect any signal, condition, or threshold logic, you may:
     - Represent its logic minimally (e.g. store its values) OR
     - Omit actual drawing but keep any underlying price/level computations used downstream.
   - If drawing objects are used as part of the logic (e.g. levels for support/resistance, BOS, zones, etc.), then:
     - Recreate their **logical role** in Python as data structures (e.g. lists of levels, segments with start/end bar indices, etc.).
     - Use these structures in the equivalent decision rules.

6) **alertcondition, plotshape, plot, barcolor, etc.**:
   - Every `alertcondition` must be mapped to an explicit Boolean condition in Python.
   - At minimum, **these alerts must be reproduced exactly**:
     - `"Pro Signal Down"`
     - `"Pro Signal Up"`
     - `"Possible Bearish Pivot"`
     - `"Possible Bullish Pivot"`
     - `"Long Entry Alert"`
     - `"Short Entry Alert"`
     - `"Strong Buy Alert"`
     - `"Strong Sell Alert"`
   - Also reproduce any other meaningful trading alerts present in PRO V6 (trend change, zone crosses, Theil-Sen break, etc.).
   - For each alert, when it becomes `True` on the latest bar for any symbol, print a line to console:
     - Format example:
       `SYMBOL: BTCUSDT | TIMEFRAME: 15m | SIGNAL: Pro Signal Up | PRICE: 68000.0 | TIME: 2025-01-01 12:30:00`

7) **No deletion of logic**:
   - Do NOT delete or simplify any logic that influences:
     - Signals (alerts)
     - Trend states
     - Probability / KDE outputs that feed into signals
     - Zones, S/R levels, or conditions used to filter entries/exits.
   - It is acceptable to simplify ONLY the **purely visual** aspects that do not feed back into any condition.


[PYTHON SCRIPT STRUCTURE / هيكلة السكربت في بايثون]

Produce **one single Python file** with the following high-level structure:

1) **CONFIG section**:
   - Binance / CCXT settings (API key/secret optional, public data is enough).
   - Base timeframe (string, e.g. `"15m"`).
   - Max history bars (`MAX_HISTORY_BARS`).
   - Alert filtering:
     - `ALERT_LOOKBACK_BARS` (only report alerts from last N bars).
     - `ALERT_LOOKBACK_MINUTES` (optional additional time filter).
   - Whether to:
     - Use all USDT-M symbols.
     - Use a whitelist of symbols.
   - All indicator inputs from PRO V6 (copied as Python variables with comments and same default values).

2) **DATA & EXCHANGE HELPERS**:
   - `init_exchange()`: initialize `ccxt.binanceusdm`.
   - `get_usdtm_symbols()`: returns list of all USDT-M futures symbols (filtered if whitelist is set).
   - `fetch_ohlcv(symbol, timeframe, limit) -> DataFrame`.
   - `prepare_dataframe(ohlcv) -> DataFrame` with columns:
     - `timestamp` (UTC, datetime)
     - `open`, `high`, `low`, `close`, `volume`.

3) **INDICATOR CORE LOGIC**:
   - Group functions that mirror the Pine code:
     - PRO V6 Signal System.
     - Trend Following + Zones.
     - Theil-Sen Estimator.
     - Trend 2 (multi-MA basis + RSI).
     - RSI + KDE + Pivots.
     - S/R & zones (any logic that’s used for signals).
     - Dashboard / probabilities logic (at least the parts that determine trend states and alerts).
   - Each function should operate on pandas Series / DataFrames and return new Series that correspond to Pine variables.

4) **SIGNAL ENGINE**:
   - A function like `compute_all_pro_v6_signals(df)` that:
     - Calls all sub-systems.
     - Produces a dictionary of Boolean Series:
       - `pro_signal_up`, `pro_signal_down`
       - `possible_bullish_pivot`, `possible_bearish_pivot`
       - `long_entry`, `short_entry`
       - `strong_buy`, `strong_sell`
       - other PRO V6 alert signals (zones, Theil-Sen breaks, trend changes, etc.)

5) **SCANNER / MAIN LOOP**:
   - `process_symbol(symbol, df)`:
     - Applies the full indicator logic on the DataFrame.
     - Extracts **new alerts** within the configured lookback window.
     - Prints alerts to console using the required format.
   - `main()`:
     - Initializes CCXT exchange.
     - Selects symbols (all USDT-M or whitelist).
     - For each symbol:
       - Fetches OHLCV for all required timeframes.
       - Runs `process_symbol`.
     - Optionally wraps scanning in:
       - `while True:` + `time.sleep()` (if configured as continuous scanner).
   - Wrap entry point with:
     - `if __name__ == "__main__": main()`.

6) **Error handling**:
   - Wrap CCXT calls in try/except to avoid crashing the whole scanner on one symbol.
   - Log or print errors clearly, but continue with other symbols.


[OUTPUT REQUIREMENTS / المتطلبات النهائية للكود]

- Deliver **one complete Python 3 script** ready to be copied into an Android Python editor (like Pydroid3) and run directly.
- The script must:
  1) Fetch Binance USDT-M OHLCV data via CCXT.
  2) Apply the **full PRO V6 indicator logic** as described above, including all parts that affect signals.
  3) Print, for each detected alert and for each symbol, lines like:

     SYMBOL: BTCUSDT | TIMEFRAME: 15m | SIGNAL: Strong Buy Alert | PRICE: 68000.0 | TIME: 2025-01-01 12:30:00

  4) Respect alert filtering via `ALERT_LOOKBACK_BARS` and/or `ALERT_LOOKBACK_MINUTES`.
  5) Keep the code modular and commented, with short English comments referencing which part of the PRO V6 indicator is being re-implemented (e.g., “# PRO V6 Signal System”, “# Theil-Sen estimator”, “# RSI + KDE pivots”).

- Do **not** output the Pine Script code again in your answer.
- Do **not** summarize or skip logic.
- The output should be the **full Python code only**, with inline comments where needed.


[INPUT / مدخل الكود]

Below is the FULL original Pine Script v5 source code for the TradingView indicator PRO V6. Use it as the authoritative reference to implement the Python script as specified above:

--- PASTE FULL PINE SCRIPT PRO V6 CODE HERE ---
```0
