#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pre-Pump Detection Engine (Clean-Room Rebuild)
- Built from scratch around pattern-classifier logic for Binance Futures.
- Optimized for Pydroid 3 (minimal deps: requests + stdlib).
- Replaces legacy wide fallbacks/labels with modern signal quality tiers.
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


# ===============================
# Config
# ===============================
CONFIG_PATH = "config.json"
DEFAULT_CONFIG: Dict[str, Any] = {
    "BASE_URL": "https://fapi.binance.com",
    "REQUEST_TIMEOUT": 12,
    "MAX_RETRIES": 2,
    "CACHE_TTL_SECONDS": 20,
    "SHORT_CACHE_TTL_SECONDS": 8,
    "FAILED_SYMBOL_COOLDOWN_SEC": 300,
    "MAX_SYMBOLS_PER_CYCLE": 120,
    "MIN_QUOTE_VOLUME_24H": 1_000_000,
    "MIN_PRICE_CHANGE_24H": 1.0,
    "ENABLE_CONSOLE_OUTPUT": True,
}


def load_config(path: str = CONFIG_PATH) -> Dict[str, Any]:
    cfg = dict(DEFAULT_CONFIG)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                user_cfg = json.load(f)
                if isinstance(user_cfg, dict):
                    cfg.update(user_cfg)
        except Exception:
            pass
    return cfg


# ===============================
# Utilities
# ===============================

def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def pct_change(old: float, new: float) -> float:
    if old == 0:
        return 0.0
    return ((new - old) / abs(old)) * 100.0


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def mean(values: Iterable[float], default: float = 0.0) -> float:
    seq = [x for x in values if x is not None]
    return statistics.fmean(seq) if seq else default


def zscore_last(values: List[float]) -> float:
    if len(values) < 4:
        return 0.0
    m = mean(values)
    std = statistics.pstdev(values) if len(values) > 1 else 0.0
    if std <= 1e-12:
        return 0.0
    return (values[-1] - m) / std


# ===============================
# Cache Layer
# ===============================
class TTLCache:
    def __init__(self) -> None:
        self._data: Dict[str, Tuple[float, Any]] = {}

    def get(self, key: str) -> Optional[Any]:
        item = self._data.get(key)
        if not item:
            return None
        expires_at, value = item
        if time.time() >= expires_at:
            self._data.pop(key, None)
            return None
        return value

    def set(self, key: str, value: Any, ttl: int) -> None:
        self._data[key] = (time.time() + ttl, value)


class FailureCache:
    """Suppress repeated failing symbols/endpoints (fixes openInterest 400 spam)."""

    def __init__(self, cooldown_sec: int = 300):
        self.cooldown_sec = cooldown_sec
        self._map: Dict[str, float] = {}

    def blocked(self, key: str) -> bool:
        until = self._map.get(key, 0.0)
        return time.time() < until

    def mark(self, key: str) -> None:
        self._map[key] = time.time() + self.cooldown_sec


# ===============================
# Binance API Client
# ===============================
class BinanceFuturesAPI:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.base_url = cfg["BASE_URL"].rstrip("/")
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

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None, ttl: Optional[int] = None) -> Any:
        query = "&".join(f"{k}={v}" for k, v in sorted((params or {}).items()))
        key = f"GET:{path}?{query}"

        cached = self.cache.get(key)
        if cached is not None:
            return cached

        url = f"{self.base_url}{path}"
        resp = self.session.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        payload = resp.json()

        if ttl is None:
            ttl = int(self.cfg["CACHE_TTL_SECONDS"])
        self.cache.set(key, payload, ttl)
        return payload

    def exchange_info(self) -> Dict[str, Any]:
        return self._get("/fapi/v1/exchangeInfo", ttl=120)

    def ticker_24h(self) -> List[Dict[str, Any]]:
        return self._get("/fapi/v1/ticker/24hr", ttl=10)

    def funding_rate_latest(self, symbol: str) -> float:
        rows = self._get("/fapi/v1/fundingRate", {"symbol": symbol, "limit": 2}, ttl=10)
        return safe_float(rows[-1].get("fundingRate"), 0.0) if rows else 0.0

    def funding_trend(self, symbol: str, limit: int = 6) -> float:
        rows = self._get("/fapi/v1/fundingRate", {"symbol": symbol, "limit": limit}, ttl=20)
        vals = [safe_float(r.get("fundingRate")) for r in rows]
        if len(vals) < 2:
            return 0.0
        return vals[-1] - vals[0]

    def basis(self, symbol: str) -> float:
        # basis ~= (mark - index)/index*100 via premiumIndex
        row = self._get("/fapi/v1/premiumIndex", {"symbol": symbol}, ttl=8)
        mark = safe_float(row.get("markPrice"), 0.0)
        index = safe_float(row.get("indexPrice"), 0.0)
        return pct_change(index, mark)

    def top_position_account(self, symbol: str, period: str = "5m") -> Tuple[Dict[str, Any], Dict[str, Any]]:
        p = self._get("/futures/data/topLongShortPositionRatio", {"symbol": symbol, "period": period, "limit": 2}, ttl=10)
        a = self._get("/futures/data/topLongShortAccountRatio", {"symbol": symbol, "period": period, "limit": 2}, ttl=10)
        return (p[-1] if p else {}), (a[-1] if a else {})

    def global_ls_ratio(self, symbol: str, period: str = "5m") -> float:
        rows = self._get("/futures/data/globalLongShortAccountRatio", {"symbol": symbol, "period": period, "limit": 2}, ttl=10)
        if not rows:
            return 1.0
        return safe_float(rows[-1].get("longShortRatio"), 1.0)

    def open_interest(self, symbol: str) -> float:
        fail_key = f"oi:{symbol}"
        if self.failures.blocked(fail_key):
            return 0.0
        try:
            row = self._get("/fapi/v1/openInterest", {"symbol": symbol}, ttl=8)
            return safe_float(row.get("openInterest"), 0.0)
        except Exception:
            self.failures.mark(fail_key)
            return 0.0

    def open_interest_hist(self, symbol: str, period: str = "5m", limit: int = 12) -> List[Dict[str, Any]]:
        fail_key = f"oi_hist:{symbol}:{period}"
        if self.failures.blocked(fail_key):
            return []
        try:
            return self._get(
                "/futures/data/openInterestHist",
                {"symbol": symbol, "period": period, "limit": limit},
                ttl=12,
            )
        except Exception:
            self.failures.mark(fail_key)
            return []

    def klines(self, symbol: str, interval: str = "5m", limit: int = 30) -> List[List[Any]]:
        return self._get("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit}, ttl=8)


# ===============================
# Feature Engineering
# ===============================
@dataclass
class FeatureSnapshot:
    symbol: str
    price: float
    price_change_5m: float
    price_change_15m: float
    position_ratio: float
    account_ratio: float
    ls_ratio: float
    position_long_pct: float
    account_long_pct: float
    divergence_pct: float
    oi_change_5m: float
    oi_change_15m: float
    oi_notional_change: float
    funding_current: float
    funding_trend: float
    basis: float
    taker_buy_ratio_recent: float
    trade_count_zscore: float
    four_h_position_ratio: float
    four_h_account_ratio: float


class FeatureBuilder:
    def __init__(self, api: BinanceFuturesAPI):
        self.api = api

    @staticmethod
    def _taker_buy_sell_ratio(klines: List[List[Any]]) -> float:
        if not klines:
            return 1.0
        buy_q = sum(safe_float(k[10]) for k in klines[-6:])
        tot_q = sum(safe_float(k[7]) for k in klines[-6:])
        sell_q = max(1e-9, tot_q - buy_q)
        return buy_q / sell_q

    @staticmethod
    def _trade_count_zscore(klines: List[List[Any]]) -> float:
        counts = [safe_float(k[8]) for k in klines]
        return zscore_last(counts)

    def snapshot(self, symbol: str) -> Optional[FeatureSnapshot]:
        kl5 = self.api.klines(symbol, "5m", 30)
        if len(kl5) < 5:
            return None

        last_close = safe_float(kl5[-1][4])
        price_5m = pct_change(safe_float(kl5[-2][4]), last_close)
        price_15m = pct_change(safe_float(kl5[-4][4]), last_close)

        pos, acc = self.api.top_position_account(symbol, "5m")
        pos4h, acc4h = self.api.top_position_account(symbol, "4h")
        ls_ratio = self.api.global_ls_ratio(symbol, "5m")

        pos_ratio = safe_float(pos.get("longShortRatio"), 1.0)
        acc_ratio = safe_float(acc.get("longShortRatio"), 1.0)
        pos_long_pct = safe_float(pos.get("longAccount"), 50.0)
        acc_long_pct = safe_float(acc.get("longAccount"), 50.0)

        oi_hist = self.api.open_interest_hist(symbol, "5m", 12)
        oi_values = [safe_float(x.get("sumOpenInterest")) for x in oi_hist if x]
        oi_notional = [safe_float(x.get("sumOpenInterestValue")) for x in oi_hist if x]

        oi_5m = pct_change(oi_values[-2], oi_values[-1]) if len(oi_values) >= 2 else 0.0
        oi_15m = pct_change(oi_values[-4], oi_values[-1]) if len(oi_values) >= 4 else 0.0
        oi_notional_chg = pct_change(oi_notional[-4], oi_notional[-1]) if len(oi_notional) >= 4 else 0.0

        funding = self.api.funding_rate_latest(symbol)
        funding_tr = self.api.funding_trend(symbol)
        basis = self.api.basis(symbol)

        return FeatureSnapshot(
            symbol=symbol,
            price=last_close,
            price_change_5m=price_5m,
            price_change_15m=price_15m,
            position_ratio=pos_ratio,
            account_ratio=acc_ratio,
            ls_ratio=ls_ratio,
            position_long_pct=pos_long_pct,
            account_long_pct=acc_long_pct,
            divergence_pct=pos_long_pct - acc_long_pct,
            oi_change_5m=oi_5m,
            oi_change_15m=oi_15m,
            oi_notional_change=oi_notional_chg,
            funding_current=funding,
            funding_trend=funding_tr,
            basis=basis,
            taker_buy_ratio_recent=self._taker_buy_sell_ratio(kl5),
            trade_count_zscore=self._trade_count_zscore(kl5),
            four_h_position_ratio=safe_float(pos4h.get("longShortRatio"), 1.0),
            four_h_account_ratio=safe_float(acc4h.get("longShortRatio"), 1.0),
        )


# ===============================
# Classifier & Scoring
# ===============================

def funding_regime_score(current_funding: float, funding_trend: float) -> float:
    score = 0.0
    if -0.25 <= current_funding <= 0.01:
        score += 0.45
    if current_funding < 0 and funding_trend > 0:
        score += 0.40
    if current_funding > 0.03:
        score -= 0.20
    return clamp(score, 0.0, 1.0)


def taker_flow_score(taker_buy_sell_ratio: float) -> float:
    if taker_buy_sell_ratio >= 4.0:
        return 1.0
    if taker_buy_sell_ratio >= 2.5:
        return 0.8
    if taker_buy_sell_ratio >= 1.8:
        return 0.6
    if taker_buy_sell_ratio >= 1.3:
        return 0.35
    return 0.0


def oi_regime_score(oi_change_5m: float, oi_change_15m: float) -> float:
    score = 0.0
    if oi_change_5m >= 0.8:
        score += 0.3
    if oi_change_15m >= 1.5:
        score += 0.4
    if oi_change_15m >= 3.0:
        score += 0.2
    return clamp(score, 0.0, 1.0)


def oi_notional_score(oi_notional_change: float) -> float:
    if oi_notional_change >= 6.0:
        return 1.0
    if oi_notional_change >= 3.0:
        return 0.7
    if oi_notional_change >= 1.5:
        return 0.45
    return 0.0


def basis_context_score(basis: float, oi_change_15m: float, flow_score: float) -> float:
    abs_basis = abs(basis)
    if abs_basis <= 0.20:
        return 0.6
    if basis < -0.7 and oi_change_15m < 1.0 and flow_score < 0.35:
        return 0.0
    if abs_basis <= 0.6:
        return 0.4
    return 0.25


def four_h_context_bonus(fh_pos: float, fh_acc: float) -> float:
    bonus = 0.0
    if fh_pos > 1.15 and fh_acc < 0.95:
        bonus += 0.25
    if fh_acc > 1.4 and fh_pos < 1.05:
        bonus += 0.25
    if fh_pos > 1.3 and fh_acc > 1.2:
        bonus += 0.35
    return bonus


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


class PrePumpClassifier:
    def classify(self, f: FeatureSnapshot) -> Optional[Signal]:
        flow = taker_flow_score(f.taker_buy_ratio_recent)
        fund = funding_regime_score(f.funding_current, f.funding_trend)
        oi = oi_regime_score(f.oi_change_5m, f.oi_change_15m)
        oinv = oi_notional_score(f.oi_notional_change)
        basis = basis_context_score(f.basis, f.oi_change_15m, flow)
        ctx4h = four_h_context_bonus(f.four_h_position_ratio, f.four_h_account_ratio)

        feature_scores = {
            "flow": round(flow, 3),
            "funding_regime": round(fund, 3),
            "oi_regime": round(oi, 3),
            "oi_notional": round(oinv, 3),
            "basis_context": round(basis, 3),
            "four_h_bonus": round(ctx4h, 3),
        }

        matches: List[str] = []
        reasons: List[str] = []

        # 1) POSITION_LED_SQUEEZE_BUILDUP
        if (
            f.position_ratio >= 1.20
            and f.account_ratio <= 1.05
            and f.ls_ratio <= 0.95
            and f.divergence_pct >= 12
            and f.oi_change_15m >= 2.0
            and f.price_change_15m >= 1.0
            and abs(f.funding_current) <= 0.08
        ):
            matches.append("POSITION_LED_SQUEEZE_BUILDUP")
            reasons.append("Position يقود مع divergence مبكر وOI داعم")

        # 2) ACCOUNT_LED_ACCUMULATION
        if (
            f.account_ratio >= 1.6
            and f.account_long_pct >= 60
            and f.position_ratio >= 0.90
            and f.oi_change_15m >= 1.5
            and f.price_change_15m >= 1.0
            and abs(f.basis) <= 0.20
            and (f.funding_current < 0 or abs(f.funding_current) <= 0.03)
        ):
            matches.append("ACCOUNT_LED_ACCUMULATION")
            reasons.append("Account يقود قبل تمدد المراكز")

        # 3) CONSENSUS_BULLISH_EXPANSION
        if (
            f.position_ratio >= 1.8
            and f.account_ratio >= 1.2
            and f.ls_ratio >= 1.2
            and f.oi_change_15m >= 1.5
            and f.funding_current <= 0.02
            and f.price_change_15m >= 1.0
        ):
            matches.append("CONSENSUS_BULLISH_EXPANSION")
            reasons.append("إجماع صعودي + توسع OI")

        # 4) FLOW_LIQUIDITY_VACUUM_BREAKOUT
        if (
            abs(f.funding_current) <= 0.02
            and f.price_change_5m >= 1.2
            and f.trade_count_zscore >= 2.0
            and f.taker_buy_ratio_recent >= 0.60
        ):
            matches.append("FLOW_LIQUIDITY_VACUUM_BREAKOUT")
            reasons.append("Breakout مدفوع بالتدفق والسيولة")

        if not matches:
            return None

        # late-signal filter
        crowded_funding = f.funding_current > 0.035
        over_extended = f.price_change_15m > 4.5 or f.price_change_5m > 2.8
        weak_build = (oi + oinv) < 0.35 and flow < 0.35
        if crowded_funding and over_extended:
            return None

        base = 0.0
        base += 0.18 * flow
        base += 0.16 * fund
        base += 0.22 * oi
        base += 0.18 * oinv
        base += 0.10 * basis
        base += 0.08 * min(1.0, ctx4h)
        base += 0.08 * min(1.0, len(matches) / 3)

        if f.divergence_pct >= 18:
            base += 0.06
        elif f.divergence_pct >= 12:
            base += 0.03

        if weak_build:
            base -= 0.14

        score = round(clamp(base * 100.0, 0.0, 100.0), 2)

        if crowded_funding or over_extended:
            stage = "LATE"
            score = max(0.0, score - 18.0)
        elif f.price_change_5m < 0.8 and f.oi_change_15m >= 1.5:
            stage = "EARLY"
        else:
            stage = "ACTIVE"

        if score >= 85:
            tier = "انفجار وشيك جدًا"
        elif score >= 72:
            tier = "انفجار وشيك"
        elif score >= 58:
            tier = "إشارة مؤسسية"
        else:
            tier = "مرشح مبكر"

        decisive = max(feature_scores, key=feature_scores.get)
        classification = "+".join(matches)

        # simple risk model for long signals
        sl = round(f.price * 0.97, 8)
        t1 = round(f.price * 1.02, 8)
        t2 = round(f.price * 1.035, 8)
        t3 = round(f.price * 1.055, 8)

        return Signal(
            symbol=f.symbol,
            score=score,
            classification=classification,
            signal_quality_tier=tier,
            direction="LONG",
            signal_stage=stage,
            price=round(f.price, 8),
            stop_loss=sl,
            targets=[t1, t2, t3],
            decisive_feature=decisive,
            reasons=reasons,
            feature_scores=feature_scores,
        )


# ===============================
# Scanner Engine
# ===============================
class ScannerEngine:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.api = BinanceFuturesAPI(cfg)
        self.builder = FeatureBuilder(self.api)
        self.classifier = PrePumpClassifier()

    def candidate_symbols(self) -> List[str]:
        ex = self.api.exchange_info()
        valid = {
            s.get("symbol")
            for s in ex.get("symbols", [])
            if s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
            and s.get("status") == "TRADING"
        }

        tickers = self.api.ticker_24h()
        filtered: List[Tuple[str, float]] = []
        for t in tickers:
            sym = t.get("symbol", "")
            if sym not in valid:
                continue
            qv = safe_float(t.get("quoteVolume"))
            chg = safe_float(t.get("priceChangePercent"))
            if qv >= self.cfg["MIN_QUOTE_VOLUME_24H"] and chg >= self.cfg["MIN_PRICE_CHANGE_24H"]:
                filtered.append((sym, qv))

        filtered.sort(key=lambda x: x[1], reverse=True)
        max_n = int(self.cfg["MAX_SYMBOLS_PER_CYCLE"])
        return [x[0] for x in filtered[:max_n]]

    def run_once(self) -> List[Signal]:
        out: List[Signal] = []
        for sym in self.candidate_symbols():
            snap = self.builder.snapshot(sym)
            if not snap:
                continue
            sig = self.classifier.classify(snap)
            if sig:
                out.append(sig)

        out.sort(key=lambda s: s.score, reverse=True)
        return out


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


def print_signals(signals: List[Signal]) -> None:
    if not signals:
        print("لا توجد إشارات مطابقة حاليًا.")
        return
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"\n=== PRE-PUMP SIGNALS @ {now} ===")
    for s in signals:
        print(json.dumps(signal_to_dict(s), ensure_ascii=False))


def main() -> None:
    cfg = load_config()
    engine = ScannerEngine(cfg)
    signals = engine.run_once()
    if cfg.get("ENABLE_CONSOLE_OUTPUT", True):
        print_signals(signals)


if __name__ == "__main__":
    main()
