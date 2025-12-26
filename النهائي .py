#!/usr/bin/env python3
"""High-performance Smart Money Concepts (SMC E5) trading engine for Android.

This script provides a one-click, autorun trading monitor with:
- Vectorized SMC computations (Pullbacks, Break of Structure, Order Blocks).
- Rich TUI dashboard for live signals and system status.
- Async CCXT wrapper with exponential backoff (mobile latency resilient).
- Async Telegram notifier for signal cards.
- Android-aware storage (Markdown reports + SQLite logs).

Designed for ARM mobile processors and Android file systems.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import importlib.util
import logging
import os
import random
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence


PANDAS_AVAILABLE = importlib.util.find_spec("pandas") is not None
NUMPY_AVAILABLE = importlib.util.find_spec("numpy") is not None
RICH_AVAILABLE = importlib.util.find_spec("rich") is not None
CCXT_AVAILABLE = importlib.util.find_spec("ccxt") is not None
AIOHTTP_AVAILABLE = importlib.util.find_spec("aiohttp") is not None

if PANDAS_AVAILABLE:
    import pandas as pd
else:  # pragma: no cover - dependency missing
    pd = None  # type: ignore

if NUMPY_AVAILABLE:
    import numpy as np
else:  # pragma: no cover - dependency missing
    np = None  # type: ignore

if RICH_AVAILABLE:
    from rich import box
    from rich.console import Console
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.progress import (
        BarColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
    )
    from rich.table import Table
else:  # pragma: no cover - dependency missing
    Console = None  # type: ignore
    Layout = None  # type: ignore
    Live = None  # type: ignore
    Panel = None  # type: ignore
    Progress = None  # type: ignore
    Table = None  # type: ignore
    box = None  # type: ignore

if CCXT_AVAILABLE:
    import ccxt  # type: ignore
else:  # pragma: no cover - dependency missing
    ccxt = None  # type: ignore

if AIOHTTP_AVAILABLE:
    import aiohttp
else:  # pragma: no cover - dependency missing
    aiohttp = None  # type: ignore


LOGGER = logging.getLogger("smc_e5")


@dataclass(slots=True)
class SignalCard:
    """Represents a structured trading signal for notifications and logs."""

    symbol: str
    signal_type: str
    direction: str
    risk_reward: float
    zone_low: float
    zone_high: float
    timestamp: dt.datetime


@dataclass(slots=True)
class RuntimeConfig:
    """Configuration for the trading monitor."""

    timeframe: str
    candle_limit: int
    symbols: List[str]
    scan_interval: float
    equity_usd: float
    risk_pct: float
    telegram_token: Optional[str]
    telegram_chat_id: Optional[str]


@dataclass(slots=True)
class StatusSnapshot:
    """Represents current system status for the dashboard."""

    connection_ok: bool
    last_fetch: Optional[dt.datetime]
    equity_usd: float
    symbol_count: int
    last_error: Optional[str]


@dataclass(slots=True)
class IndicatorConfig:
    """Configuration for vectorized SMC computations."""

    swing_lookback: int = 20
    pullback_window: int = 10
    ob_lookback: int = 12


@dataclass(slots=True)
class StoragePaths:
    """Computed storage paths for reports and logs."""

    base_dir: Path
    report_dir: Path
    db_path: Path


class DependencyError(RuntimeError):
    """Raised when required dependencies are missing."""


class CCXTBackoffClient:
    """CCXT wrapper with exponential backoff for mobile connectivity."""

    __slots__ = ("exchange", "max_retries", "base_delay", "max_delay")

    def __init__(self, exchange: Any, *, max_retries: int = 5) -> None:
        self.exchange = exchange
        self.max_retries = max_retries
        self.base_delay = 0.7
        self.max_delay = 8.0

    async def fetch_ohlcv(
        self, symbol: str, timeframe: str, limit: int
    ) -> List[List[float]]:
        """Fetch OHLCV data with exponential backoff and jitter."""

        for attempt in range(self.max_retries + 1):
            try:
                return await asyncio.to_thread(
                    self.exchange.fetch_ohlcv, symbol, timeframe, limit=limit
                )
            except Exception as exc:  # pragma: no cover - network dependent
                if attempt >= self.max_retries:
                    raise
                delay = min(self.base_delay * (2**attempt), self.max_delay)
                jitter = random.uniform(0, 0.35)
                LOGGER.warning("Fetch failed for %s: %s", symbol, exc)
                await asyncio.sleep(delay + jitter)
        return []

    async def fetch_balance(self) -> Optional[Dict[str, Any]]:
        """Fetch account balance with backoff (optional)."""

        for attempt in range(self.max_retries + 1):
            try:
                return await asyncio.to_thread(self.exchange.fetch_balance)
            except Exception as exc:  # pragma: no cover - network dependent
                if attempt >= self.max_retries:
                    return None
                delay = min(self.base_delay * (2**attempt), self.max_delay)
                jitter = random.uniform(0, 0.35)
                LOGGER.warning("Balance fetch failed: %s", exc)
                await asyncio.sleep(delay + jitter)
        return None


class TelegramNotifier:
    """Async Telegram notifier for signal cards."""

    __slots__ = ("token", "chat_id")

    def __init__(self, token: str, chat_id: str) -> None:
        self.token = token
        self.chat_id = chat_id

    async def send_signal(self, signal: SignalCard) -> None:
        """Send formatted signal card to Telegram."""

        if aiohttp is None:  # pragma: no cover - dependency missing
            LOGGER.warning("aiohttp not available; Telegram disabled")
            return

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        text = (
            "📡 *SMC E5 Signal*\n"
            f"• *Symbol:* {signal.symbol}\n"
            f"• *Type:* {signal.signal_type}\n"
            f"• *Direction:* {signal.direction}\n"
            f"• *Risk/Reward:* {signal.risk_reward:.2f}\n"
            f"• *Zone:* {signal.zone_low:.6f} - {signal.zone_high:.6f}\n"
            f"• *Time:* {signal.timestamp.isoformat()}"
        )
        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "Markdown"}

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=10) as response:
                if response.status >= 400:
                    body = await response.text()
                    LOGGER.warning("Telegram error %s: %s", response.status, body)


class AndroidStorage:
    """Android-aware storage helper for reports and logs."""

    __slots__ = ("paths",)

    def __init__(self) -> None:
        self.paths = self._resolve_paths()
        self.paths.report_dir.mkdir(parents=True, exist_ok=True)
        self.paths.db_path.parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _resolve_paths() -> StoragePaths:
        """Resolve storage paths for Android or fallback environments."""

        android_root = Path("/storage/emulated/0")
        if android_root.exists():
            base_dir = android_root / "SMC_E5"
        else:
            base_dir = Path.home() / ".smc_e5"
        report_dir = base_dir / "reports"
        db_path = base_dir / "logs" / "signals.sqlite"
        return StoragePaths(base_dir=base_dir, report_dir=report_dir, db_path=db_path)


class SignalLogger:
    """SQLite logger for signal cards."""

    __slots__ = ("db_path",)

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    signal_type TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    risk_reward REAL NOT NULL,
                    zone_low REAL NOT NULL,
                    zone_high REAL NOT NULL
                )
                """
            )

    def insert(self, signal: SignalCard) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO signals (
                    timestamp, symbol, signal_type, direction,
                    risk_reward, zone_low, zone_high
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal.timestamp.isoformat(),
                    signal.symbol,
                    signal.signal_type,
                    signal.direction,
                    signal.risk_reward,
                    signal.zone_low,
                    signal.zone_high,
                ),
            )


class SMCEngine:
    """Vectorized Smart Money Concepts engine."""

    __slots__ = ("cfg",)

    def __init__(self, cfg: IndicatorConfig) -> None:
        self.cfg = cfg

    def compute(self, candles: Sequence[Sequence[float]]) -> Tuple[pd.DataFrame, List[SignalCard]]:
        """Compute SMC signals from OHLCV candles using vectorized logic."""

        df = self._to_frame(candles)
        if df.empty:
            return df, []

        pullbacks = self._pullbacks(df)
        bos = self._break_of_structure(df)
        order_blocks = self._order_blocks(df, bos)

        df = df.assign(
            pullback=pullbacks,
            bos_direction=bos["direction"],
            ob_low=order_blocks["ob_low"],
            ob_high=order_blocks["ob_high"],
        )

        signals = self._build_signals(df)
        return df, signals

    @staticmethod
    def _to_frame(candles: Sequence[Sequence[float]]) -> pd.DataFrame:
        columns = ["timestamp", "open", "high", "low", "close", "volume"]
        df = pd.DataFrame(candles, columns=columns)
        if df.empty:
            return df
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        return df

    def _pullbacks(self, df: pd.DataFrame) -> pd.Series:
        """Identify pullbacks using vectorized rolling logic."""

        window = self.cfg.pullback_window
        swing_high = df["high"].rolling(window, min_periods=1).max()
        swing_low = df["low"].rolling(window, min_periods=1).min()
        pullback_down = df["close"] < swing_high.shift(1)
        pullback_up = df["close"] > swing_low.shift(1)
        return pd.Series(
            np.where(pullback_down & ~pullback_up, "down", np.where(pullback_up, "up", "")),
            index=df.index,
        )

    def _break_of_structure(self, df: pd.DataFrame) -> pd.DataFrame:
        """Detect BOS events via lookback highs/lows."""

        lookback = self.cfg.swing_lookback
        prev_high = df["high"].rolling(lookback, min_periods=1).max().shift(1)
        prev_low = df["low"].rolling(lookback, min_periods=1).min().shift(1)
        bos_up = df["close"] > prev_high
        bos_down = df["close"] < prev_low
        direction = np.where(bos_up, "bullish", np.where(bos_down, "bearish", ""))
        return pd.DataFrame({"direction": direction, "bos_up": bos_up, "bos_down": bos_down})

    def _order_blocks(self, df: pd.DataFrame, bos: pd.DataFrame) -> pd.DataFrame:
        """Compute order block zones based on last opposite candle."""

        bearish = df["close"] < df["open"]
        bullish = df["close"] > df["open"]

        indices = np.arange(len(df))
        last_bear = pd.Series(np.where(bearish, indices, np.nan)).ffill()
        last_bull = pd.Series(np.where(bullish, indices, np.nan)).ffill()

        ob_idx = np.where(
            bos["bos_up"],
            last_bear,
            np.where(bos["bos_down"], last_bull, np.nan),
        )
        ob_idx = pd.Series(ob_idx).astype("float")
        ob_low = df["low"].reindex(ob_idx.dropna().astype(int)).reindex(df.index).ffill()
        ob_high = df["high"].reindex(ob_idx.dropna().astype(int)).reindex(df.index).ffill()

        return pd.DataFrame({"ob_low": ob_low, "ob_high": ob_high})

    @staticmethod
    def _risk_reward(entry: float, stop: float, target: float) -> float:
        if entry == stop:
            return 0.0
        return abs(target - entry) / abs(entry - stop)

    def _build_signals(self, df: pd.DataFrame) -> List[SignalCard]:
        """Build signal cards from the most recent bar."""

        latest = df.iloc[-1]
        if (
            not latest["bos_direction"]
            or pd.isna(latest["ob_low"])
            or pd.isna(latest["ob_high"])
        ):
            return []

        direction = latest["bos_direction"]
        zone_low = float(latest["ob_low"])
        zone_high = float(latest["ob_high"])
        entry = float(latest["close"])
        stop = zone_low if direction == "bullish" else zone_high
        target = entry + (entry - stop) * 2 if direction == "bullish" else entry - (stop - entry) * 2
        rr = self._risk_reward(entry, stop, target)

        signal = SignalCard(
            symbol="",
            signal_type="BOS + OB",
            direction=direction,
            risk_reward=rr,
            zone_low=zone_low,
            zone_high=zone_high,
            timestamp=latest["timestamp"].to_pydatetime(),
        )
        return [signal]


class RichDashboard:
    """Rich-based terminal UI for live trading signals."""

    __slots__ = ("console",)

    def __init__(self) -> None:
        self.console = Console()

    def build_layout(self, signals: Sequence[SignalCard], status: StatusSnapshot) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body"),
            Layout(name="footer", size=3),
        )

        header = Panel(
            "[bold cyan]Smart Money Algo Pro E5 — Android Edition[/bold cyan]",
            style="bold white on black",
        )
        layout["header"].update(header)

        body = Layout()
        body.split_row(Layout(name="signals"), Layout(name="status", size=40))
        body["signals"].update(self._signals_table(signals))
        body["status"].update(self._status_panel(status))
        layout["body"].update(body)

        footer = Panel(
            "[bold]Live monitoring in progress...[/bold]",
            style="bold green",
        )
        layout["footer"].update(footer)
        return layout

    def _signals_table(self, signals: Sequence[SignalCard]) -> Table:
        table = Table(
            title="Live Signals",
            box=box.DOUBLE,
            header_style="bold magenta",
            show_lines=True,
        )
        table.add_column("Symbol", style="bold cyan")
        table.add_column("Type", style="yellow")
        table.add_column("Direction", style="bold")
        table.add_column("R/R", justify="right")
        table.add_column("Zone", justify="right")
        table.add_column("Time", style="dim")

        if not signals:
            table.add_row("—", "—", "—", "—", "—", "—")
            return table

        for signal in signals:
            direction_style = "green" if signal.direction == "bullish" else "red"
            zone = f"{signal.zone_low:.6f} - {signal.zone_high:.6f}"
            table.add_row(
                signal.symbol,
                signal.signal_type,
                f"[{direction_style}]{signal.direction}[/{direction_style}]",
                f"{signal.risk_reward:.2f}",
                zone,
                signal.timestamp.strftime("%H:%M:%S"),
            )
        return table

    @staticmethod
    def _status_panel(status: StatusSnapshot) -> Panel:
        connection = "✅ Connected" if status.connection_ok else "❌ Disconnected"
        last_fetch = status.last_fetch.isoformat() if status.last_fetch else "—"
        last_error = status.last_error or "—"
        body = (
            f"[bold]Connection:[/bold] {connection}\n"
            f"[bold]Symbols:[/bold] {status.symbol_count}\n"
            f"[bold]Equity:[/bold] ${status.equity_usd:,.2f}\n"
            f"[bold]Last Fetch:[/bold] {last_fetch}\n"
            f"[bold]Last Error:[/bold] {last_error}"
        )
        return Panel(body, title="Status", border_style="blue")

    def progress_bar(self, total: int) -> Progress:
        return Progress(
            SpinnerColumn(style="cyan"),
            TextColumn("Fetching data..."),
            BarColumn(bar_width=None),
            TimeElapsedColumn(),
            transient=True,
        )


class SMCRunner:
    """Orchestrates data fetching, signal computation, and output."""

    __slots__ = (
        "cfg",
        "engine",
        "dashboard",
        "storage",
        "logger",
        "notifier",
        "exchange",
        "status",
    )

    def __init__(
        self,
        cfg: RuntimeConfig,
        engine: SMCEngine,
        dashboard: RichDashboard,
        storage: AndroidStorage,
        logger: SignalLogger,
        notifier: Optional[TelegramNotifier],
        exchange: CCXTBackoffClient,
    ) -> None:
        self.cfg = cfg
        self.engine = engine
        self.dashboard = dashboard
        self.storage = storage
        self.logger = logger
        self.notifier = notifier
        self.exchange = exchange
        self.status = StatusSnapshot(
            connection_ok=True,
            last_fetch=None,
            equity_usd=cfg.equity_usd,
            symbol_count=len(cfg.symbols),
            last_error=None,
        )

    async def run(self) -> None:
        with Live(auto_refresh=False, console=self.dashboard.console) as live:
            while True:
                signals = await self._scan_symbols(live)
                layout = self.dashboard.build_layout(signals, self.status)
                live.update(layout, refresh=True)
                await self._write_report(signals)
                if self.cfg.scan_interval <= 0:
                    break
                await asyncio.sleep(self.cfg.scan_interval)

    async def _scan_symbols(self, live: Live) -> List[SignalCard]:
        signals: List[SignalCard] = []
        progress = self.dashboard.progress_bar(total=len(self.cfg.symbols))
        with progress:
            task_id = progress.add_task("scan", total=len(self.cfg.symbols))
            live.update(progress, refresh=True)
            for symbol in self.cfg.symbols:
                try:
                    candles = await self.exchange.fetch_ohlcv(
                        symbol, self.cfg.timeframe, self.cfg.candle_limit
                    )
                    df, symbol_signals = self.engine.compute(candles)
                    for signal in symbol_signals:
                        signal.symbol = symbol
                        signals.append(signal)
                        self.logger.insert(signal)
                        if self.notifier:
                            await self.notifier.send_signal(signal)
                    self.status.connection_ok = True
                    self.status.last_error = None
                except Exception as exc:  # pragma: no cover - network dependent
                    self.status.connection_ok = False
                    self.status.last_error = str(exc)
                    LOGGER.error("Failed to scan %s: %s", symbol, exc)
                self.status.last_fetch = dt.datetime.now(dt.timezone.utc)
                progress.advance(task_id)
                live.update(progress, refresh=True)

        return signals

    async def _write_report(self, signals: Sequence[SignalCard]) -> None:
        timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S")
        report_path = self.storage.paths.report_dir / f"smc_report_{timestamp}.md"
        lines = [
            "# Smart Money Algo Pro E5 Report",
            f"Generated: {timestamp} UTC",
            "",
            "## Signals",
        ]
        if not signals:
            lines.append("No signals detected.")
        for sig in signals:
            lines.extend(
                [
                    f"- **{sig.symbol}** | {sig.signal_type} | {sig.direction} | "
                    f"R/R: {sig.risk_reward:.2f} | "
                    f"Zone: {sig.zone_low:.6f} - {sig.zone_high:.6f}",
                ]
            )
        report_path.write_text("\n".join(lines), encoding="utf-8")


def _dependency_check() -> None:
    missing = []
    if not PANDAS_AVAILABLE:
        missing.append("pandas")
    if not NUMPY_AVAILABLE:
        missing.append("numpy")
    if not RICH_AVAILABLE:
        missing.append("rich")
    if not CCXT_AVAILABLE:
        missing.append("ccxt")
    if missing:
        raise DependencyError(
            "Missing dependencies: " + ", ".join(missing)
        )


def _parse_args(argv: Optional[Sequence[str]] = None) -> RuntimeConfig:
    parser = argparse.ArgumentParser(description="SMC E5 Android Trading Engine")
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--limit", type=int, default=400)
    parser.add_argument("--symbols", default="BTC/USDT,ETH/USDT")
    parser.add_argument("--scan-interval", type=float, default=15.0)
    parser.add_argument("--equity", type=float, default=1000.0)
    parser.add_argument("--risk", type=float, default=0.01)
    parser.add_argument("--tg-token", default=os.getenv("TG_TOKEN"))
    parser.add_argument("--tg-chat", default=os.getenv("TG_CHAT"))
    args = parser.parse_args(argv)
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    return RuntimeConfig(
        timeframe=args.timeframe,
        candle_limit=args.limit,
        symbols=symbols,
        scan_interval=args.scan_interval,
        equity_usd=args.equity,
        risk_pct=args.risk,
        telegram_token=args.tg_token,
        telegram_chat_id=args.tg_chat,
    )


def _build_exchange() -> CCXTBackoffClient:
    exchange = ccxt.binanceusdm({"enableRateLimit": True})
    exchange.set_sandbox_mode(False)
    return CCXTBackoffClient(exchange)


def _configure_logging(base_dir: Path) -> None:
    log_path = base_dir / "logs" / "smc_runtime.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_path), logging.StreamHandler(sys.stdout)],
    )


def _self_diagnostic() -> None:
    _dependency_check()
    if pd is None or np is None:
        raise DependencyError("Vectorized dependencies not available")


def _create_notifier(cfg: RuntimeConfig) -> Optional[TelegramNotifier]:
    if not cfg.telegram_token or not cfg.telegram_chat_id:
        return None
    return TelegramNotifier(cfg.telegram_token, cfg.telegram_chat_id)


async def _main_async(cfg: RuntimeConfig) -> None:
    storage = AndroidStorage()
    _configure_logging(storage.paths.base_dir)
    dashboard = RichDashboard()
    engine = SMCEngine(IndicatorConfig())
    logger = SignalLogger(storage.paths.db_path)
    notifier = _create_notifier(cfg)
    exchange = _build_exchange()

    runner = SMCRunner(
        cfg=cfg,
        engine=engine,
        dashboard=dashboard,
        storage=storage,
        logger=logger,
        notifier=notifier,
        exchange=exchange,
    )
    await runner.run()


def main(argv: Optional[Sequence[str]] = None) -> int:
    try:
        _self_diagnostic()
        cfg = _parse_args(argv)
        asyncio.run(_main_async(cfg))
        return 0
    except DependencyError as exc:
        print(f"Dependency error: {exc}")
        return 1
    except KeyboardInterrupt:
        print("Interrupted by user")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
