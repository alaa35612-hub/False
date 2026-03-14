#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multi-Timeframe Futures Scanner (1D -> 4H -> 15m -> 5m)
- اتجاه عام من اليومي
- تأكيد من 4 ساعات
- منطقة اهتمام من 15 دقيقة
- نقطة دخول من 5 دقائق
"""

from __future__ import annotations

import json
import os
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


CONFIG_PATH = "config.json"
DEFAULT_CONFIG: Dict[str, Any] = {
    "BASE_URL": "https://fapi.binance.com",
    "REQUEST_TIMEOUT": 12,
    "MAX_RETRIES": 2,
    "CACHE_TTL_SECONDS": 20,
    "FAILED_SYMBOL_COOLDOWN_SEC": 300,
    "MAX_SYMBOLS_PER_CYCLE": 180,
    "MIN_QUOTE_VOLUME_24H": 800_000,
    "MIN_ABS_PRICE_CHANGE_24H": 0.0,
    "PRINT_SCAN_PROGRESS": True,
    "TOP_K_PRINT": 12,
}


def load_config(path: str = CONFIG_PATH) -> Dict[str, Any]:
    cfg = dict(DEFAULT_CONFIG)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
                if isinstance(loaded, dict):
                    cfg.update(loaded)
        except Exception:
            pass
    return cfg


def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v in (None, ""):
            return default
        return float(v)
    except Exception:
        return default


def pct_change(old: float, new: float) -> float:
    if old == 0.0:
        return 0.0
    return ((new - old) / abs(old)) * 100.0


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def mean(values: Iterable[float], default: float = 0.0) -> float:
    vals = [v for v in values if v is not None]
    return statistics.fmean(vals) if vals else default


def zscore_last(values: List[float]) -> float:
    if len(values) < 4:
        return 0.0
    m = mean(values)
    sd = statistics.pstdev(values)
    if sd <= 1e-12:
        return 0.0
    return (values[-1] - m) / sd


class TTLCache:
    def __init__(self) -> None:
        self._data: Dict[str, Tuple[float, Any]] = {}

    def get(self, key: str) -> Optional[Any]:
        item = self._data.get(key)
        if not item:
            return None
        exp, val = item
        if time.time() >= exp:
            self._data.pop(key, None)
            return None
        return val

    def set(self, key: str, value: Any, ttl_sec: int) -> None:
        self._data[key] = (time.time() + ttl_sec, value)


class FailureCache:
    def __init__(self, cooldown_sec: int = 300):
        self.cooldown = cooldown_sec
        self._fails: Dict[str, float] = {}

    def blocked(self, key: str) -> bool:
        return time.time() < self._fails.get(key, 0.0)

    def mark(self, key: str) -> None:
        self._fails[key] = time.time() + self.cooldown


class BinanceFuturesAPI:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.base = cfg["BASE_URL"].rstrip("/")
        self.timeout = int(cfg["REQUEST_TIMEOUT"])
        self.cache = TTLCache()
        self.failures = FailureCache(int(cfg["FAILED_SYMBOL_COOLDOWN_SEC"]))

        retry = Retry(
            total=int(cfg["MAX_RETRIES"]),
            backoff_factor=0.2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session = requests.Session()
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None, ttl: int = 20) -> Any:
        params = params or {}
        q = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        key = f"GET:{path}?{q}"
        cached = self.cache.get(key)
        if cached is not None:
            return cached

        resp = self.session.get(f"{self.base}{path}", params=params, timeout=self.timeout)
        resp.raise_for_status()
        payload = resp.json()
        self.cache.set(key, payload, ttl)
        return payload

    def exchange_info(self) -> Dict[str, Any]:
        return self._get("/fapi/v1/exchangeInfo", ttl=120)

    def ticker_24h(self) -> List[Dict[str, Any]]:
        return self._get("/fapi/v1/ticker/24hr", ttl=10)

    def klines(self, symbol: str, interval: str, limit: int) -> List[List[Any]]:
        return self._get("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit}, ttl=8)

    def basis(self, symbol: str) -> float:
        row = self._get("/fapi/v1/premiumIndex", {"symbol": symbol}, ttl=8)
        return pct_change(safe_float(row.get("indexPrice"), 0.0), safe_float(row.get("markPrice"), 0.0))

    def funding_latest(self, symbol: str) -> float:
        rows = self._get("/fapi/v1/fundingRate", {"symbol": symbol, "limit": 2}, ttl=10)
        return safe_float(rows[-1].get("fundingRate"), 0.0) if rows else 0.0

    def funding_trend(self, symbol: str, limit: int = 6) -> float:
        rows = self._get("/fapi/v1/fundingRate", {"symbol": symbol, "limit": limit}, ttl=20)
        vals = [safe_float(x.get("fundingRate"), 0.0) for x in rows]
        if len(vals) < 2:
            return 0.0
        return vals[-1] - vals[0]

    def top_ratio(self, symbol: str, period: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        p = self._get("/futures/data/topLongShortPositionRatio", {"symbol": symbol, "period": period, "limit": 2}, ttl=10)
        a = self._get("/futures/data/topLongShortAccountRatio", {"symbol": symbol, "period": period, "limit": 2}, ttl=10)
        return (p[-1] if p else {}), (a[-1] if a else {})

    def global_ls_ratio(self, symbol: str, period: str = "5m") -> float:
        rows = self._get("/futures/data/globalLongShortAccountRatio", {"symbol": symbol, "period": period, "limit": 2}, ttl=10)
        return safe_float(rows[-1].get("longShortRatio"), 1.0) if rows else 1.0

    def open_interest_hist(self, symbol: str, period: str = "5m", limit: int = 12) -> List[Dict[str, Any]]:
        key = f"oi_hist:{symbol}:{period}"
        if self.failures.blocked(key):
            return []
        try:
            return self._get("/futures/data/openInterestHist", {"symbol": symbol, "period": period, "limit": limit}, ttl=12)
        except Exception:
            self.failures.mark(key)
            return []


@dataclass
class TimeframeCtx:
    trend: str
    change_pct: float
    ema_bias: str


@dataclass
class FeatureSnapshot:
    symbol: str
    price: float
    daily_ctx: TimeframeCtx
    h4_ctx: TimeframeCtx
    m15_ctx: TimeframeCtx
    m5_ctx: TimeframeCtx
    position_ratio_5m: float
    account_ratio_5m: float
    position_long_pct_5m: float
    account_long_pct_5m: float
    position_ratio_4h: float
    account_ratio_4h: float
    ls_ratio_5m: float
    divergence_pct: float
    oi_change_5m: float
    oi_change_15m: float
    oi_notional_change_15m: float
    funding_current: float
    funding_trend: float
    basis: float
    taker_buy_sell_ratio_5m: float
    trade_count_zscore_5m: float


@dataclass
class Signal:
    symbol: str
    score: float
    classification: str
    signal_quality_tier: str
    direction: str
    signal_stage: str
    price: float
    stop_loss: float
    targets: List[float]
    decisive_feature: str
    reasons: List[str] = field(default_factory=list)
    feature_scores: Dict[str, float] = field(default_factory=dict)


class FeatureBuilder:
    def __init__(self, api: BinanceFuturesAPI):
        self.api = api

    @staticmethod
    def _ctx_from_klines(kl: List[List[Any]]) -> TimeframeCtx:
        if len(kl) < 12:
            return TimeframeCtx("NEUTRAL", 0.0, "NEUTRAL")
        closes = [safe_float(x[4]) for x in kl]
        ch = pct_change(closes[0], closes[-1])
        ema_fast = mean(closes[-4:])
        ema_slow = mean(closes[-10:])
        if ch > 1.0:
            trend = "UP"
        elif ch < -1.0:
            trend = "DOWN"
        else:
            trend = "SIDEWAYS"
        if ema_fast > ema_slow * 1.002:
            bias = "BULLISH"
        elif ema_fast < ema_slow * 0.998:
            bias = "BEARISH"
        else:
            bias = "NEUTRAL"
        return TimeframeCtx(trend=trend, change_pct=round(ch, 3), ema_bias=bias)

    @staticmethod
    def _taker_buy_sell_ratio(kl5: List[List[Any]]) -> float:
        if not kl5:
            return 1.0
        buy_quote = sum(safe_float(x[10]) for x in kl5[-6:])
        total_quote = sum(safe_float(x[7]) for x in kl5[-6:])
        sell_quote = max(1e-9, total_quote - buy_quote)
        return buy_quote / sell_quote

    def snapshot(self, symbol: str) -> Optional[FeatureSnapshot]:
        kl_d = self.api.klines(symbol, "1d", 30)
        kl_4h = self.api.klines(symbol, "4h", 30)
        kl_15m = self.api.klines(symbol, "15m", 30)
        kl_5m = self.api.klines(symbol, "5m", 30)

        if len(kl_d) < 12 or len(kl_4h) < 12 or len(kl_15m) < 12 or len(kl_5m) < 12:
            return None

        last_price = safe_float(kl_5m[-1][4])

        pos5, acc5 = self.api.top_ratio(symbol, "5m")
        pos4, acc4 = self.api.top_ratio(symbol, "4h")

        pos_ratio_5m = safe_float(pos5.get("longShortRatio"), 1.0)
        acc_ratio_5m = safe_float(acc5.get("longShortRatio"), 1.0)
        pos_long_5m = safe_float(pos5.get("longAccount"), 50.0)
        acc_long_5m = safe_float(acc5.get("longAccount"), 50.0)

        oi_hist = self.api.open_interest_hist(symbol, "5m", 12)
        oi_vals = [safe_float(x.get("sumOpenInterest"), 0.0) for x in oi_hist if x]
        oi_nvals = [safe_float(x.get("sumOpenInterestValue"), 0.0) for x in oi_hist if x]

        oi_5 = pct_change(oi_vals[-2], oi_vals[-1]) if len(oi_vals) >= 2 else 0.0
        oi_15 = pct_change(oi_vals[-4], oi_vals[-1]) if len(oi_vals) >= 4 else 0.0
        oi_n15 = pct_change(oi_nvals[-4], oi_nvals[-1]) if len(oi_nvals) >= 4 else 0.0

        funding = self.api.funding_latest(symbol)
        funding_tr = self.api.funding_trend(symbol)
        basis = self.api.basis(symbol)

        return FeatureSnapshot(
            symbol=symbol,
            price=last_price,
            daily_ctx=self._ctx_from_klines(kl_d),
            h4_ctx=self._ctx_from_klines(kl_4h),
            m15_ctx=self._ctx_from_klines(kl_15m),
            m5_ctx=self._ctx_from_klines(kl_5m),
            position_ratio_5m=pos_ratio_5m,
            account_ratio_5m=acc_ratio_5m,
            position_long_pct_5m=pos_long_5m,
            account_long_pct_5m=acc_long_5m,
            position_ratio_4h=safe_float(pos4.get("longShortRatio"), 1.0),
            account_ratio_4h=safe_float(acc4.get("longShortRatio"), 1.0),
            ls_ratio_5m=self.api.global_ls_ratio(symbol, "5m"),
            divergence_pct=pos_long_5m - acc_long_5m,
            oi_change_5m=oi_5,
            oi_change_15m=oi_15,
            oi_notional_change_15m=oi_n15,
            funding_current=funding,
            funding_trend=funding_tr,
            basis=basis,
            taker_buy_sell_ratio_5m=self._taker_buy_sell_ratio(kl_5m),
            trade_count_zscore_5m=zscore_last([safe_float(x[8], 0.0) for x in kl_5m]),
        )


def funding_regime_score(funding: float, trend: float, direction: str) -> float:
    score = 0.0
    if direction == "LONG":
        if -0.25 <= funding <= 0.01:
            score += 0.45
        if funding < 0 and trend > 0:
            score += 0.40
        if funding > 0.03:
            score -= 0.20
    else:
        if -0.01 <= funding <= 0.25:
            score += 0.45
        if funding > 0 and trend < 0:
            score += 0.40
        if funding < -0.03:
            score -= 0.20
    return clamp(score, 0.0, 1.0)


def taker_flow_score(ratio: float, direction: str) -> float:
    used = ratio if direction == "LONG" else (1.0 / max(1e-9, ratio))
    if used >= 4.0:
        return 1.0
    if used >= 2.5:
        return 0.8
    if used >= 1.8:
        return 0.6
    if used >= 1.3:
        return 0.35
    return 0.0


def oi_regime_score(oi5: float, oi15: float) -> float:
    s = 0.0
    if oi5 >= 0.8:
        s += 0.3
    if oi15 >= 1.5:
        s += 0.4
    if oi15 >= 3.0:
        s += 0.2
    return clamp(s, 0.0, 1.0)


def oi_notional_score(v: float) -> float:
    if v >= 6.0:
        return 1.0
    if v >= 3.0:
        return 0.7
    if v >= 1.5:
        return 0.45
    return 0.0


def basis_context_score(basis: float, oi15: float, flow: float, direction: str) -> float:
    abs_b = abs(basis)
    if abs_b <= 0.20:
        return 0.6
    if direction == "LONG" and basis < -0.7 and oi15 < 1.0 and flow < 0.35:
        return 0.0
    if direction == "SHORT" and basis > 0.7 and oi15 < 1.0 and flow < 0.35:
        return 0.0
    if abs_b <= 0.6:
        return 0.4
    return 0.25


def four_h_context_bonus(pos4h: float, acc4h: float, direction: str) -> float:
    b = 0.0
    if direction == "LONG":
        if pos4h > 1.15 and acc4h < 0.95:
            b += 0.25
        if acc4h > 1.4 and pos4h < 1.05:
            b += 0.25
        if pos4h > 1.3 and acc4h > 1.2:
            b += 0.35
    else:
        if pos4h < 0.90 and acc4h > 1.05:
            b += 0.25
        if acc4h < 0.75 and pos4h > 0.95:
            b += 0.25
        if pos4h < 0.85 and acc4h < 0.9:
            b += 0.35
    return b


def tier_from_score(score: float) -> str:
    if score >= 85:
        return "انفجار وشيك جدًا"
    if score >= 72:
        return "انفجار وشيك"
    if score >= 58:
        return "إشارة مؤسسية"
    return "مرشح مبكر"


class MultiTimeframeClassifier:
    """4 fingerprints للاتجاه الصاعد + نفس المنطق معكوسًا للاتجاه الهابط."""

    def _mtf_gate(self, f: FeatureSnapshot, direction: str) -> Tuple[bool, List[str], str]:
        reasons: List[str] = []
        if direction == "LONG":
            if f.daily_ctx.trend != "UP" and f.daily_ctx.ema_bias != "BULLISH":
                return False, ["اليومي لا يدعم الصعود"], "REJECTED"
            reasons.append(f"1D اتجاه={f.daily_ctx.trend} bias={f.daily_ctx.ema_bias}")
            if f.h4_ctx.trend == "DOWN" and f.h4_ctx.ema_bias == "BEARISH":
                return False, reasons + ["4H عكسي قوي ضد الصعود"], "REJECTED"
            reasons.append(f"4H تأكيد={f.h4_ctx.trend}/{f.h4_ctx.ema_bias}")
        else:
            if f.daily_ctx.trend != "DOWN" and f.daily_ctx.ema_bias != "BEARISH":
                return False, ["اليومي لا يدعم الهبوط"], "REJECTED"
            reasons.append(f"1D اتجاه={f.daily_ctx.trend} bias={f.daily_ctx.ema_bias}")
            if f.h4_ctx.trend == "UP" and f.h4_ctx.ema_bias == "BULLISH":
                return False, reasons + ["4H عكسي قوي ضد الهبوط"], "REJECTED"
            reasons.append(f"4H تأكيد={f.h4_ctx.trend}/{f.h4_ctx.ema_bias}")

        stage = "EARLY"
        if abs(f.m5_ctx.change_pct) >= 1.0 or abs(f.m15_ctx.change_pct) >= 1.3:
            stage = "ACTIVE"
        if abs(f.m5_ctx.change_pct) > 2.8 or abs(f.m15_ctx.change_pct) > 4.5:
            stage = "LATE"

        reasons.append(f"15m منطقة اهتمام: trend={f.m15_ctx.trend}, change={f.m15_ctx.change_pct:.2f}%")
        reasons.append(f"5m دخول: trend={f.m5_ctx.trend}, change={f.m5_ctx.change_pct:.2f}%")
        return True, reasons, stage

    def _bullish_patterns(self, f: FeatureSnapshot) -> List[str]:
        matches: List[str] = []
        if (
            f.position_ratio_5m >= 1.20 and f.account_ratio_5m <= 1.05 and f.ls_ratio_5m <= 0.95
            and f.divergence_pct >= 12 and f.oi_change_15m >= 2.0 and f.m15_ctx.change_pct >= 1.0 and abs(f.funding_current) <= 0.08
        ):
            matches.append("POSITION_LED_SQUEEZE_BUILDUP")
        if (
            f.account_ratio_5m >= 1.6 and f.account_long_pct_5m >= 60 and f.position_ratio_5m >= 0.90
            and f.oi_change_15m >= 1.5 and f.m15_ctx.change_pct >= 1.0 and abs(f.basis) <= 0.20
            and (f.funding_current < 0 or abs(f.funding_current) <= 0.03)
        ):
            matches.append("ACCOUNT_LED_ACCUMULATION")
        if (
            f.position_ratio_5m >= 1.8 and f.account_ratio_5m >= 1.2 and f.ls_ratio_5m >= 1.2
            and f.oi_change_15m >= 1.5 and f.funding_current <= 0.02 and f.m15_ctx.change_pct >= 1.0
        ):
            matches.append("CONSENSUS_BULLISH_EXPANSION")
        if (
            abs(f.funding_current) <= 0.02 and f.m5_ctx.change_pct >= 1.2 and f.trade_count_zscore_5m >= 2.0
            and f.taker_buy_sell_ratio_5m >= 0.60
        ):
            matches.append("FLOW_LIQUIDITY_VACUUM_BREAKOUT")
        return matches

    def _bearish_patterns(self, f: FeatureSnapshot) -> List[str]:
        matches: List[str] = []
        if (
            f.position_ratio_5m <= 0.85 and f.account_ratio_5m >= 0.98 and f.ls_ratio_5m >= 1.05
            and f.divergence_pct <= -12 and f.oi_change_15m >= 2.0 and f.m15_ctx.change_pct <= -1.0 and abs(f.funding_current) <= 0.08
        ):
            matches.append("POSITION_LED_DUMP_BUILDUP")
        if (
            f.account_ratio_5m <= 0.75 and f.account_long_pct_5m <= 40 and f.position_ratio_5m <= 1.05
            and f.oi_change_15m >= 1.5 and f.m15_ctx.change_pct <= -1.0 and abs(f.basis) <= 0.20
            and (f.funding_current > 0 or abs(f.funding_current) <= 0.03)
        ):
            matches.append("ACCOUNT_LED_DISTRIBUTION")
        if (
            f.position_ratio_5m <= 0.80 and f.account_ratio_5m <= 0.90 and f.ls_ratio_5m <= 0.90
            and f.oi_change_15m >= 1.5 and f.funding_current >= -0.02 and f.m15_ctx.change_pct <= -1.0
        ):
            matches.append("CONSENSUS_BEARISH_EXPANSION")
        if (
            abs(f.funding_current) <= 0.02 and f.m5_ctx.change_pct <= -1.2 and f.trade_count_zscore_5m >= 2.0
            and (1.0 / max(1e-9, f.taker_buy_sell_ratio_5m)) >= 0.60
        ):
            matches.append("FLOW_LIQUIDITY_VACUUM_BREAKDOWN")
        return matches

    def _score(self, f: FeatureSnapshot, direction: str, matches: List[str], stage: str) -> Tuple[float, Dict[str, float]]:
        flow = taker_flow_score(f.taker_buy_sell_ratio_5m, direction)
        fund = funding_regime_score(f.funding_current, f.funding_trend, direction)
        oi = oi_regime_score(f.oi_change_5m, f.oi_change_15m)
        oinv = oi_notional_score(f.oi_notional_change_15m)
        basis = basis_context_score(f.basis, f.oi_change_15m, flow, direction)
        ctx4 = four_h_context_bonus(f.position_ratio_4h, f.account_ratio_4h, direction)

        feat = {
            "flow": round(flow, 3),
            "funding_regime": round(fund, 3),
            "oi_regime": round(oi, 3),
            "oi_notional": round(oinv, 3),
            "basis_context": round(basis, 3),
            "four_h_bonus": round(ctx4, 3),
            "pattern_density": round(min(1.0, len(matches) / 3), 3),
        }

        base = 0.0
        base += 0.18 * flow
        base += 0.16 * fund
        base += 0.22 * oi
        base += 0.18 * oinv
        base += 0.10 * basis
        base += 0.08 * min(1.0, ctx4)
        base += 0.08 * min(1.0, len(matches) / 3)

        if direction == "LONG":
            if f.divergence_pct >= 18:
                base += 0.06
            elif f.divergence_pct >= 12:
                base += 0.03
        else:
            if f.divergence_pct <= -18:
                base += 0.06
            elif f.divergence_pct <= -12:
                base += 0.03

        if stage == "LATE":
            base -= 0.18

        score = round(clamp(base * 100.0, 0.0, 100.0), 2)
        return score, feat

    def classify_both(self, f: FeatureSnapshot) -> List[Signal]:
        results: List[Signal] = []

        for direction in ("LONG", "SHORT"):
            ok, reasons, stage = self._mtf_gate(f, direction)
            if not ok:
                continue

            matches = self._bullish_patterns(f) if direction == "LONG" else self._bearish_patterns(f)
            if not matches:
                continue

            score, feat = self._score(f, direction, matches, stage)
            if score < 35:
                continue

            reasons.append(f"patterns={','.join(matches)}")
            reasons.append(
                "السبب الحاسم: "
                + (
                    "Position/Account divergence"
                    if abs(f.divergence_pct) >= 12
                    else "flow+OI"
                )
            )

            decisive = max(feat, key=feat.get)
            if direction == "LONG":
                sl = round(f.price * 0.97, 8)
                t = [round(f.price * 1.02, 8), round(f.price * 1.035, 8), round(f.price * 1.055, 8)]
            else:
                sl = round(f.price * 1.03, 8)
                t = [round(f.price * 0.98, 8), round(f.price * 0.965, 8), round(f.price * 0.945, 8)]

            results.append(
                Signal(
                    symbol=f.symbol,
                    score=score,
                    classification="+".join(matches),
                    signal_quality_tier=tier_from_score(score),
                    direction=direction,
                    signal_stage=stage,
                    price=round(f.price, 8),
                    stop_loss=sl,
                    targets=t,
                    decisive_feature=decisive,
                    reasons=reasons,
                    feature_scores=feat,
                )
            )

        return results


class ScannerEngine:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.api = BinanceFuturesAPI(cfg)
        self.builder = FeatureBuilder(self.api)
        self.classifier = MultiTimeframeClassifier()

    def candidate_symbols(self) -> List[str]:
        info = self.api.exchange_info()
        allowed = {
            s.get("symbol")
            for s in info.get("symbols", [])
            if s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING"
        }

        out: List[Tuple[str, float]] = []
        for t in self.api.ticker_24h():
            sym = t.get("symbol", "")
            if sym not in allowed:
                continue
            qv = safe_float(t.get("quoteVolume"), 0.0)
            ch = safe_float(t.get("priceChangePercent"), 0.0)
            if qv >= self.cfg["MIN_QUOTE_VOLUME_24H"] and abs(ch) >= self.cfg["MIN_ABS_PRICE_CHANGE_24H"]:
                out.append((sym, qv))

        out.sort(key=lambda x: x[1], reverse=True)
        return [x[0] for x in out[: int(self.cfg["MAX_SYMBOLS_PER_CYCLE"])]]

    def run_once(self) -> List[Signal]:
        candidates = self.candidate_symbols()
        print(f"[SCAN] candidates={len(candidates)}")
        results: List[Signal] = []

        for i, sym in enumerate(candidates, start=1):
            if self.cfg.get("PRINT_SCAN_PROGRESS", True) and (i == 1 or i % 20 == 0 or i == len(candidates)):
                print(f"[SCAN] processing {i}/{len(candidates)}: {sym}")
            try:
                snap = self.builder.snapshot(sym)
            except Exception as exc:
                print(f"[WARN] snapshot failed for {sym}: {exc}")
                continue
            if not snap:
                continue
            signals = self.classifier.classify_both(snap)
            results.extend(signals)

        results.sort(key=lambda x: x.score, reverse=True)
        print(f"[SCAN] done. signals={len(results)}")
        return results


def signal_to_dict(sig: Signal) -> Dict[str, Any]:
    return {
        "symbol": sig.symbol,
        "score": sig.score,
        "classification": sig.classification,
        "signal_quality_tier": sig.signal_quality_tier,
        "direction": sig.direction,
        "signal_stage": sig.signal_stage,
        "price": sig.price,
        "stop_loss": sig.stop_loss,
        "targets": sig.targets,
        "decisive_feature": sig.decisive_feature,
        "reasons": sig.reasons,
        "feature_scores": sig.feature_scores,
    }


def print_signals(signals: List[Signal], top_k: int = 12) -> None:
    if not signals:
        print("لا توجد إشارات مطابقة حاليًا.")
        return

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"\n=== MULTI-TIMEFRAME SIGNALS @ {now} ===")

    longs = [s for s in signals if s.direction == "LONG"]
    shorts = [s for s in signals if s.direction == "SHORT"]

    print(f"\n--- أفضل العملات للصعود (Top {min(top_k, len(longs))}) ---")
    for s in longs[:top_k]:
        print(json.dumps(signal_to_dict(s), ensure_ascii=False))

    print(f"\n--- أفضل العملات للهبوط (Top {min(top_k, len(shorts))}) ---")
    for s in shorts[:top_k]:
        print(json.dumps(signal_to_dict(s), ensure_ascii=False))


def main() -> None:
    cfg = load_config()
    print("[BOOT] Multi-timeframe scanner started (1D -> 4H -> 15m -> 5m)")

    engine = ScannerEngine(cfg)
    signals = engine.run_once()
    print_signals(signals, top_k=int(cfg.get("TOP_K_PRINT", 12)))


if __name__ == "__main__":
    main()
