#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
الماسح المتوازي المتقدم - الإصدار الاحترافي v5.3.1 مع معايرة تكيفية للأنماط
================================================================================
نسخة v5 احترافية موجهة لـ Pydroid 3 لالتقاط سيناريوهات التدفق، ضغط الشورت، وتمدد الفائدة المفتوحة بدقة أعلى.
تم إضافة: فصل Position Ratio عن Account Ratio، تحليل زمني لانقلاب النسب،
ونمط PRE_EXPLOSION_SETUP، وتجميع الأنماط المتعددة لنفس الرمز بدل التوقف عند أول نمط.
كما تم تخفيف شرط APR لنمط PRE_CLOSE_SQUEEZE وخفض حد الإرسال المبكر،
وتحديث SMART_MONEY_DIVERGENCE ليعتمد على ضغط الشراء وتمدّد OI قصير المدى.
إضافة إلى سجل تشخيص JSONL، وتحمل أفضل لأخطاء SQLite، ووضع خفيف للأداء على الهاتف.
"""

import os
import json
import time
import threading
import sqlite3
import atexit
import random
import re
from datetime import datetime, timedelta
from collections import defaultdict, deque
from typing import Dict, List, Optional, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import numpy as np
import pandas as pd
from scipy import stats
from binance.client import Client
from binance.exceptions import BinanceAPIException
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =============================================================================
# أدوات ملفات ومسارات
# =============================================================================
def ensure_parent_dir(path: str) -> None:
    """إنشاء المجلد الأب للمسار إذا لزم الأمر. آمنة مع المسارات النسبية في Pydroid 3."""
    try:
        parent = os.path.dirname(os.path.abspath(path))
        if parent and not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)
    except Exception:
        pass


def safe_float(value, default=0.0):
    try:
        if value is None or value == '':
            return default
        return float(value)
    except Exception:
        return default


# محاولة استيراد hmmlearn (اختياري)
try:
    from hmmlearn import hmm
    HMM_AVAILABLE = True
except ImportError:
    HMM_AVAILABLE = False
    print("⚠️ hmmlearn غير مثبت. سيتم تعطيل تحليل HMM.")

# =============================================================================
# الإعدادات من ملف config.json
# =============================================================================
CONFIG_PATH = "config.json"
DEFAULT_CONFIG = {
    # Binance API
    "BINANCE_API_KEY": "YOUR_API_KEY",
    "BINANCE_API_SECRET": "YOUR_API_SECRET",
    "USE_TESTNET": False,

    # Telegram
    "TELEGRAM_TOKEN": "YOUR_BOT_TOKEN",
    "TELEGRAM_CHAT_ID": "YOUR_CHAT_ID",

    # الفلترة الأولية
    "MIN_VOLUME_24H": 500_000,
    "MIN_PRICE_CHANGE_24H": 2.0,
    "MIN_OI_USDT": 100_000,
    "VOLUME_TO_AVG_RATIO_MIN": 1.2,

    # عتبات عامة (z-score)
    "OI_CHANGE_ZSCORE": 1.5,
    "TOP_TRADER_ZSCORE": 1.5,
    "FUNDING_RATE_ZSCORE": 1.5,

    # عتبات دنيا مطلقة جديدة
    "MIN_ABS_OI_CHANGE": 2.0,           # أقل تغير مطلق لـ OI (%)
    "MIN_ABS_FUNDING_CHANGE": 0.5,      # أقل تغير مطلق للتمويل (%)
    "MIN_VOLUME_USD": 1_000_000,        # أقل حجم تداول يومي بالدولار

    # نمط بصمة الحيتان
    "WHALE_TOP_LONG_ZSCORE": 2.0,
    "WHALE_OI_CHANGE_ZSCORE": 2.0,
    "WHALE_FUNDING_MAX": 0.0,

    # نمط ما قبل الارتفاع
    "PRE_PUMP_FUNDING_MAX": 0.01,
    "PRE_PUMP_OI_CHANGE_MIN": 10.0,
    "PRE_PUMP_TOP_LONG_MIN": 60.0,
    "PRE_PUMP_HOURS": [2, 3, 4],

    # كشف الارتفاع المفاجئ
    "SKYROCKET_THRESHOLD": 8.0,
    "SKYROCKET_TIMEFRAMES": ["5m", "15m", "1h"],

    # عتبات الأنماط الخمسة
    "EXTREME_FUNDING_NEGATIVE": -0.1,
    "MODERATE_FUNDING_POSITIVE_MIN": 0.01,
    "MODERATE_FUNDING_POSITIVE_MAX": 0.1,
    "FUNDING_COUNTDOWN_HOURS": 1.0,
    "MIN_APR_PERCENT": 50.0,
    "LIQUIDITY_SWEEP_WICK_RATIO": 0.3,

    # عتبات الإضافات
    "BASIS_STRONG_NEGATIVE": -0.5,
    "BASIS_STRONG_POSITIVE": 1.0,
    "VOLUME_SPIKE_THRESHOLD": 2.0,
    "OI_ACCELERATION_THRESHOLD": 0.5,

    # إعدادات الإطار الزمني
    "PRIMARY_TIMEFRAME": "5m",
    "OI_HISTORY_TIMEFRAME": "15m",
    "PRICE_CHANGE_TIMEFRAME": "5m",
    "NUMBER_OF_CANDLES": 12,

    # إعدادات تنبيهات التحديث
    "ENABLE_SCORE_UPDATE_ALERTS": True,
    "MIN_SCORE_INCREASE_FOR_ALERT": 10,
    "UPDATE_ALERT_COOLDOWN_MINUTES": 30,

    # عتبات تصنيف القوة
    "STRONG_WHALE_RATIO": 75.0,
    "EXTREME_FUNDING_SHORT": -0.3,
    "OI_SURGE_THRESHOLD": 20.0,
    "VOLUME_EXPLOSION": 3.0,
    "BASIS_EXTREME_NEGATIVE": -1.0,
    "BASIS_EXTREME_POSITIVE": 2.0,

    # إعدادات الفريم اليومي
    "DAILY_TIMEFRAME": "1d",
    "DAILY_LOOKBACK_DAYS": 30,
    "DAILY_MIN_AVG_TOP_RATIO": 70.0,
    "DAILY_OI_TREND_DAYS": 7,
    "DAILY_FUNDING_FLIP_DAYS": 7,
    "DAILY_BASIS_THRESHOLD": 1.0,
    "DAILY_TAKER_VOLUME_RATIO": 1.2,

    # إعدادات تحسين تصنيف القوة
    "POWER_CRITICAL_THRESHOLD": 90,
    "POWER_IMPORTANT_THRESHOLD": 75,
    "POWER_ADD_DAILY_BOOST": 30,

    # إعدادات الأداء (تم تعديلها للسرعة القصوى)
    "MAX_WORKERS": 20,                    # زيادة عدد العمال
    "CHUNK_SIZE": 50,
    "SLEEP_BETWEEN_CHUNKS": 5,
    "SLEEP_BETWEEN_SYMBOLS": 0.0,         # إزالة التأخير بين الرموز
    "REQUEST_TIMEOUT": 20,
    "MAX_RETRIES": 3,
    "DEBUG": True,
    "CACHE_TTL_SECONDS": 30,               # للبيانات اللحظية
    "HISTORIC_CACHE_TTL": 300,             # للبيانات التاريخية (5 دقائق)
    "VOLUME_AVG_CACHE_TTL": 86400,         # لمتوسط الحجم (24 ساعة)
    "SCAN_INTERVAL_MINUTES": 1,

    # إعدادات HMM
    "ENABLE_HMM": True,
    "HMM_N_STATES": 4,
    "HMM_LOOKBACK_DAYS": 60,
    "HMM_UPDATE_INTERVAL": 3600,

    # إعدادات التعلم الذاتي
    "ENABLE_LEARNING": True,
    "LEARNING_INTERVAL_HOURS": 6,
    "MIN_SIGNALS_FOR_LEARNING": 5,
    "PERFORMANCE_THRESHOLD": 0.6,
    "WEIGHT_ADJUSTMENT_RATE": 0.1,

    # أوزان الأنماط
    "PATTERN_WEIGHTS": {
        "BEAR_SQUEEZE": 0.20,
        "WHALE_MOMENTUM": 0.15,
        "PRE_CLOSE_SQUEEZE": 0.25,
        "RETAIL_TRAP": 0.10,
        "PRE_PUMP": 0.30,
        "WHALE_SETUP": 0.25,
        "LONG_TERM_ACCUMULATION": 0.35,
        "STRONG_UPTREND": 0.40,
        "SKYROCKET": 0.50,
        "SMART_MONEY_DIVERGENCE": 0.35,
        "PRE_EXPLOSION_SETUP": 0.38
    },

    # إعدادات إدارة المخاطر
    "RISK_PER_TRADE": 2.0,
    "ATR_PERIOD": 14,
    "ATR_MULTIPLIER_STOP": 2.0,
    "ATR_MULTIPLIER_TAKE_PROFIT": 3.0,

    # إعدادات قاعدة البيانات
    "DB_PATH": "signals.db",
    "SAVE_SIGNALS": True,

    # إعدادات التشخيص والإخراج الملائمة لـ Pydroid 3
    "ENABLE_DIAGNOSTICS": True,
    "DIAGNOSTICS_PATH": "diagnostics.jsonl",
    "EVAL_HORIZONS_MINUTES": [5, 15, 30],
    "BACKTEST_LOOKBACK_HOURS": 72,
    "MAX_DIAGNOSTIC_RECORDS": 5000,
    "WRITE_REJECTION_LOGS": True,
    "PYDROID_MODE": True,
    "MAX_SYMBOLS_PER_CYCLE": 120,
    "MAX_AVG_VOLUME_CHECKS": 20,

    # إعدادات المعايرة التكيفية للأنماط
    "ENABLE_AUTO_CALIBRATION": True,
    "CALIBRATION_LOOKBACK_HOURS": 96,
    "CALIBRATION_PATH": "pattern_calibration.json",
    "MIN_SIGNALS_FOR_CALIBRATION": 6,
    "PATTERN_SCORE_BIAS_LIMIT": 12,

    # عتبات إضافية
    "MIN_APR_PRE_CLOSE": 60,             # أقل APR لقبول نمط PRE_CLOSE_SQUEEZE بعد التخفيف
    "SIGNAL_MIN_SCORE": 65,              # أقل درجة لقبول الإشارة المبكرة

    # باتش v5.4: تجميع تقوده الحسابات + خطر الإرهاق + دعم نافذة 03:00 UTC
    "ACCOUNT_LED_MIN_ACCOUNT_RATIO": 2.0,
    "ACCOUNT_LED_MIN_POSITION_RATIO": 1.0,
    "ACCOUNT_LED_MIN_PRICE_15M": 0.2,
    "ACCOUNT_LED_MIN_BUY_RATIO": 0.54,
    "ACCOUNT_LED_MAX_FUNDING_ABS": 0.03,
    "ACCOUNT_LED_MAX_BASIS_ABS": 0.12,
    "EXHAUSTION_MIN_PRICE_15M": 6.0,
    "EXHAUSTION_MIN_SELL_RATIO": 0.52,
    "EXHAUSTION_MAX_OI_5M": 0.6,
    "ENABLE_TIME_WINDOW_BOOST": True,
    "TIME_WINDOW_BOOST_HOURS": [2, 3],
    "TIME_WINDOW_BOOST_SCORE": 5,

    # باتش v5.5: معايرة pre-pump المؤسسية
    "SMART_DIVERGENCE_MIN_PRICE_15M": 1.0,
    "SMART_DIVERGENCE_MIN_OI_15M": 2.0,
    "SMART_DIVERGENCE_MIN_POSITION_RATIO": 1.0,
    "SMART_DIVERGENCE_MAX_ACCOUNT_RATIO": 1.05,
    "SMART_DIVERGENCE_MAX_LS_RATIO": 1.05,
    "SMART_DIVERGENCE_MIN_BUY_RATIO": 0.56,
    "CONSENSUS_BULL_MIN_POSITION_RATIO": 1.10,
    "CONSENSUS_BULL_MIN_ACCOUNT_RATIO": 1.40,
    "CONSENSUS_BULL_MIN_LS_RATIO": 1.25,
    "CONSENSUS_BULL_MIN_OI_15M": 2.0,
    "CONSENSUS_BULL_MIN_PRICE_15M": 1.0,
    "FLOW_BREAKOUT_MIN_PRICE_5M": 1.2,
    "FLOW_BREAKOUT_MIN_BUY_RATIO": 0.58,
    "FLOW_BREAKOUT_MIN_TRADE_ZSCORE": 1.8,
    "FLOW_BREAKOUT_MAX_FUNDING_ABS": 0.02,
    "OI_EXPANSION_MIN_PRICE_15M": 1.5,
    "OI_EXPANSION_MIN_OI_15M": 3.0,
    "OI_EXPANSION_MIN_BUY_RATIO": 0.55,
    "OI_EXPANSION_MIN_POSITION_RATIO": 0.95,
    "OI_EXPANSION_MIN_NOTIONAL_CHANGE": 0.5,
    "SMART_MONEY_DIVERGENCE_STRONG": 10.0,
    "TOP_ACCOUNT_LONG_STRONG": 68.0,
    "TOP_ACCOUNT_RATIO_STRONG": 1.8,
    "TAKER_RATIO_GOOD": 1.8,
    "TAKER_RATIO_STRONG": 2.5,
    "TAKER_RATIO_EXCEPTIONAL": 4.0,
    "POSITION_LED_SQUEEZE_MIN_POSITION_RATIO": 1.15,
    "POSITION_LED_SQUEEZE_MAX_ACCOUNT_RATIO": 0.95,
    "POSITION_LED_SQUEEZE_MAX_LS_RATIO": 0.95,
    "POSITION_LED_SQUEEZE_MIN_DIVERGENCE": 12.0,
    "POSITION_LED_SQUEEZE_MIN_PRICE_15M": 0.6,
    "POSITION_LED_SQUEEZE_MIN_OI_15M": 1.8,
    "POSITION_LED_SQUEEZE_MIN_BUY_RATIO": 0.50,
    "POSITION_LED_SQUEEZE_MIN_POSITION_DELTA": 0.05,
    "POSITION_LED_SQUEEZE_MIN_POSITION_SLOPE": 0.0,
    "POSITION_LED_SQUEEZE_NEG_FUNDING_IMPROVEMENT_BONUS": 0.15
}

def load_config():
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            user_config = json.load(f)
            config = DEFAULT_CONFIG.copy()
            config.update(user_config)
            return config
    else:
        with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
            json.dump(DEFAULT_CONFIG, f, indent=4, ensure_ascii=False)
        print(f"✅ تم إنشاء ملف {CONFIG_PATH} افتراضياً. يرجى تعديله حسب الحاجة.")
        return DEFAULT_CONFIG.copy()

CONFIG = load_config()

# استخراج الإعدادات
BINANCE_API_KEY = CONFIG["BINANCE_API_KEY"]
BINANCE_API_SECRET = CONFIG["BINANCE_API_SECRET"]
USE_TESTNET = CONFIG["USE_TESTNET"]
TELEGRAM_TOKEN = CONFIG["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = CONFIG["TELEGRAM_CHAT_ID"]
MIN_VOLUME_24H = CONFIG["MIN_VOLUME_24H"]
MIN_PRICE_CHANGE_24H = CONFIG["MIN_PRICE_CHANGE_24H"]
MIN_OI_USDT = CONFIG["MIN_OI_USDT"]
VOLUME_TO_AVG_RATIO_MIN = CONFIG["VOLUME_TO_AVG_RATIO_MIN"]
OI_CHANGE_ZSCORE = CONFIG["OI_CHANGE_ZSCORE"]
TOP_TRADER_ZSCORE = CONFIG["TOP_TRADER_ZSCORE"]
FUNDING_RATE_ZSCORE = CONFIG["FUNDING_RATE_ZSCORE"]
MIN_ABS_OI_CHANGE = CONFIG["MIN_ABS_OI_CHANGE"]
MIN_ABS_FUNDING_CHANGE = CONFIG["MIN_ABS_FUNDING_CHANGE"]
MIN_VOLUME_USD = CONFIG["MIN_VOLUME_USD"]
WHALE_TOP_LONG_ZSCORE = CONFIG["WHALE_TOP_LONG_ZSCORE"]
WHALE_OI_CHANGE_ZSCORE = CONFIG["WHALE_OI_CHANGE_ZSCORE"]
WHALE_FUNDING_MAX = CONFIG["WHALE_FUNDING_MAX"]
PRE_PUMP_FUNDING_MAX = CONFIG["PRE_PUMP_FUNDING_MAX"]
PRE_PUMP_OI_CHANGE_MIN = CONFIG["PRE_PUMP_OI_CHANGE_MIN"]
PRE_PUMP_TOP_LONG_MIN = CONFIG["PRE_PUMP_TOP_LONG_MIN"]
PRE_PUMP_HOURS = CONFIG["PRE_PUMP_HOURS"]
SKYROCKET_THRESHOLD = CONFIG["SKYROCKET_THRESHOLD"]
SKYROCKET_TIMEFRAMES = CONFIG["SKYROCKET_TIMEFRAMES"]
EXTREME_FUNDING_NEGATIVE = CONFIG["EXTREME_FUNDING_NEGATIVE"]
MODERATE_FUNDING_POSITIVE_MIN = CONFIG["MODERATE_FUNDING_POSITIVE_MIN"]
MODERATE_FUNDING_POSITIVE_MAX = CONFIG["MODERATE_FUNDING_POSITIVE_MAX"]
FUNDING_COUNTDOWN_HOURS = CONFIG["FUNDING_COUNTDOWN_HOURS"]
MIN_APR_PERCENT = CONFIG["MIN_APR_PERCENT"]
LIQUIDITY_SWEEP_WICK_RATIO = CONFIG["LIQUIDITY_SWEEP_WICK_RATIO"]
BASIS_STRONG_NEGATIVE = CONFIG["BASIS_STRONG_NEGATIVE"]
BASIS_STRONG_POSITIVE = CONFIG["BASIS_STRONG_POSITIVE"]
VOLUME_SPIKE_THRESHOLD = CONFIG["VOLUME_SPIKE_THRESHOLD"]
OI_ACCELERATION_THRESHOLD = CONFIG["OI_ACCELERATION_THRESHOLD"]
PRIMARY_TIMEFRAME = CONFIG["PRIMARY_TIMEFRAME"]
OI_HISTORY_TIMEFRAME = CONFIG["OI_HISTORY_TIMEFRAME"]
PRICE_CHANGE_TIMEFRAME = CONFIG["PRICE_CHANGE_TIMEFRAME"]
NUMBER_OF_CANDLES = CONFIG["NUMBER_OF_CANDLES"]
ENABLE_SCORE_UPDATE_ALERTS = CONFIG["ENABLE_SCORE_UPDATE_ALERTS"]
MIN_SCORE_INCREASE_FOR_ALERT = CONFIG["MIN_SCORE_INCREASE_FOR_ALERT"]
UPDATE_ALERT_COOLDOWN_MINUTES = CONFIG["UPDATE_ALERT_COOLDOWN_MINUTES"]
STRONG_WHALE_RATIO = CONFIG["STRONG_WHALE_RATIO"]
EXTREME_FUNDING_SHORT = CONFIG["EXTREME_FUNDING_SHORT"]
OI_SURGE_THRESHOLD = CONFIG["OI_SURGE_THRESHOLD"]
VOLUME_EXPLOSION = CONFIG["VOLUME_EXPLOSION"]
BASIS_EXTREME_NEGATIVE = CONFIG["BASIS_EXTREME_NEGATIVE"]
BASIS_EXTREME_POSITIVE = CONFIG["BASIS_EXTREME_POSITIVE"]
DAILY_TIMEFRAME = CONFIG["DAILY_TIMEFRAME"]
DAILY_LOOKBACK_DAYS = CONFIG["DAILY_LOOKBACK_DAYS"]
DAILY_MIN_AVG_TOP_RATIO = CONFIG["DAILY_MIN_AVG_TOP_RATIO"]
DAILY_OI_TREND_DAYS = CONFIG["DAILY_OI_TREND_DAYS"]
DAILY_FUNDING_FLIP_DAYS = CONFIG["DAILY_FUNDING_FLIP_DAYS"]
DAILY_BASIS_THRESHOLD = CONFIG["DAILY_BASIS_THRESHOLD"]
DAILY_TAKER_VOLUME_RATIO = CONFIG["DAILY_TAKER_VOLUME_RATIO"]
POWER_CRITICAL_THRESHOLD = CONFIG["POWER_CRITICAL_THRESHOLD"]
POWER_IMPORTANT_THRESHOLD = CONFIG["POWER_IMPORTANT_THRESHOLD"]
POWER_ADD_DAILY_BOOST = CONFIG["POWER_ADD_DAILY_BOOST"]
MAX_WORKERS = CONFIG["MAX_WORKERS"]
CHUNK_SIZE = CONFIG["CHUNK_SIZE"]
SLEEP_BETWEEN_CHUNKS = CONFIG["SLEEP_BETWEEN_CHUNKS"]
SLEEP_BETWEEN_SYMBOLS = CONFIG["SLEEP_BETWEEN_SYMBOLS"]
REQUEST_TIMEOUT = CONFIG["REQUEST_TIMEOUT"]
MAX_RETRIES = CONFIG["MAX_RETRIES"]
DEBUG = CONFIG["DEBUG"]
CACHE_TTL_SECONDS = CONFIG["CACHE_TTL_SECONDS"]
HISTORIC_CACHE_TTL = CONFIG.get("HISTORIC_CACHE_TTL", 300)
VOLUME_AVG_CACHE_TTL = CONFIG.get("VOLUME_AVG_CACHE_TTL", 86400)
SCAN_INTERVAL_MINUTES = CONFIG["SCAN_INTERVAL_MINUTES"]
ENABLE_HMM = CONFIG["ENABLE_HMM"] and HMM_AVAILABLE
HMM_N_STATES = CONFIG["HMM_N_STATES"]
HMM_LOOKBACK_DAYS = CONFIG["HMM_LOOKBACK_DAYS"]
HMM_UPDATE_INTERVAL = CONFIG["HMM_UPDATE_INTERVAL"]
ENABLE_LEARNING = CONFIG["ENABLE_LEARNING"]
LEARNING_INTERVAL_HOURS = CONFIG["LEARNING_INTERVAL_HOURS"]
MIN_SIGNALS_FOR_LEARNING = CONFIG["MIN_SIGNALS_FOR_LEARNING"]
PERFORMANCE_THRESHOLD = CONFIG["PERFORMANCE_THRESHOLD"]
WEIGHT_ADJUSTMENT_RATE = CONFIG["WEIGHT_ADJUSTMENT_RATE"]
PATTERN_WEIGHTS = CONFIG["PATTERN_WEIGHTS"]
RISK_PER_TRADE = CONFIG["RISK_PER_TRADE"]
ATR_PERIOD = CONFIG["ATR_PERIOD"]
ATR_MULTIPLIER_STOP = CONFIG["ATR_MULTIPLIER_STOP"]
ATR_MULTIPLIER_TAKE_PROFIT = CONFIG["ATR_MULTIPLIER_TAKE_PROFIT"]
DB_PATH = CONFIG["DB_PATH"]
SAVE_SIGNALS = CONFIG["SAVE_SIGNALS"]
ENABLE_DIAGNOSTICS = CONFIG.get("ENABLE_DIAGNOSTICS", True)
DIAGNOSTICS_PATH = CONFIG.get("DIAGNOSTICS_PATH", "diagnostics.jsonl")
EVAL_HORIZONS_MINUTES = CONFIG.get("EVAL_HORIZONS_MINUTES", [5, 15, 30])
BACKTEST_LOOKBACK_HOURS = CONFIG.get("BACKTEST_LOOKBACK_HOURS", 72)
MAX_DIAGNOSTIC_RECORDS = CONFIG.get("MAX_DIAGNOSTIC_RECORDS", 5000)
WRITE_REJECTION_LOGS = CONFIG.get("WRITE_REJECTION_LOGS", True)
PYDROID_MODE = CONFIG.get("PYDROID_MODE", True)
MAX_SYMBOLS_PER_CYCLE = CONFIG.get("MAX_SYMBOLS_PER_CYCLE", 120)
MAX_AVG_VOLUME_CHECKS = CONFIG.get("MAX_AVG_VOLUME_CHECKS", 20)
TRADING_SYMBOLS_SET = set()
MIN_APR_PRE_CLOSE = CONFIG.get("MIN_APR_PRE_CLOSE", 60)
SIGNAL_MIN_SCORE = CONFIG.get("SIGNAL_MIN_SCORE", 65)
ACCOUNT_LED_MIN_ACCOUNT_RATIO = CONFIG.get("ACCOUNT_LED_MIN_ACCOUNT_RATIO", 2.0)
ACCOUNT_LED_MIN_POSITION_RATIO = CONFIG.get("ACCOUNT_LED_MIN_POSITION_RATIO", 1.0)
ACCOUNT_LED_MIN_PRICE_15M = CONFIG.get("ACCOUNT_LED_MIN_PRICE_15M", 0.2)
ACCOUNT_LED_MIN_BUY_RATIO = CONFIG.get("ACCOUNT_LED_MIN_BUY_RATIO", 0.54)
ACCOUNT_LED_MAX_FUNDING_ABS = CONFIG.get("ACCOUNT_LED_MAX_FUNDING_ABS", 0.03)
ACCOUNT_LED_MAX_BASIS_ABS = CONFIG.get("ACCOUNT_LED_MAX_BASIS_ABS", 0.12)
EXHAUSTION_MIN_PRICE_15M = CONFIG.get("EXHAUSTION_MIN_PRICE_15M", 6.0)
EXHAUSTION_MIN_SELL_RATIO = CONFIG.get("EXHAUSTION_MIN_SELL_RATIO", 0.52)
EXHAUSTION_MAX_OI_5M = CONFIG.get("EXHAUSTION_MAX_OI_5M", 0.6)
ENABLE_TIME_WINDOW_BOOST = CONFIG.get("ENABLE_TIME_WINDOW_BOOST", True)
TIME_WINDOW_BOOST_HOURS = CONFIG.get("TIME_WINDOW_BOOST_HOURS", [2, 3])
TIME_WINDOW_BOOST_SCORE = CONFIG.get("TIME_WINDOW_BOOST_SCORE", 5)
CALIBRATION_PATH = CONFIG.get("CALIBRATION_PATH", "pattern_calibration.json")
CALIBRATION_LOOKBACK_HOURS = CONFIG.get("CALIBRATION_LOOKBACK_HOURS", 96)
ENABLE_AUTO_CALIBRATION = CONFIG.get("ENABLE_AUTO_CALIBRATION", True)
MIN_SIGNALS_FOR_CALIBRATION = CONFIG.get("MIN_SIGNALS_FOR_CALIBRATION", 6)
PATTERN_SCORE_BIAS_LIMIT = CONFIG.get("PATTERN_SCORE_BIAS_LIMIT", 12)
SMART_DIVERGENCE_MIN_PRICE_15M = CONFIG.get("SMART_DIVERGENCE_MIN_PRICE_15M", 1.0)
SMART_DIVERGENCE_MIN_OI_15M = CONFIG.get("SMART_DIVERGENCE_MIN_OI_15M", 2.0)
SMART_DIVERGENCE_MIN_POSITION_RATIO = CONFIG.get("SMART_DIVERGENCE_MIN_POSITION_RATIO", 1.0)
SMART_DIVERGENCE_MAX_ACCOUNT_RATIO = CONFIG.get("SMART_DIVERGENCE_MAX_ACCOUNT_RATIO", 1.05)
SMART_DIVERGENCE_MAX_LS_RATIO = CONFIG.get("SMART_DIVERGENCE_MAX_LS_RATIO", 1.05)
SMART_DIVERGENCE_MIN_BUY_RATIO = CONFIG.get("SMART_DIVERGENCE_MIN_BUY_RATIO", 0.56)
CONSENSUS_BULL_MIN_POSITION_RATIO = CONFIG.get("CONSENSUS_BULL_MIN_POSITION_RATIO", 1.10)
CONSENSUS_BULL_MIN_ACCOUNT_RATIO = CONFIG.get("CONSENSUS_BULL_MIN_ACCOUNT_RATIO", 1.40)
CONSENSUS_BULL_MIN_LS_RATIO = CONFIG.get("CONSENSUS_BULL_MIN_LS_RATIO", 1.25)
CONSENSUS_BULL_MIN_OI_15M = CONFIG.get("CONSENSUS_BULL_MIN_OI_15M", 2.0)
CONSENSUS_BULL_MIN_PRICE_15M = CONFIG.get("CONSENSUS_BULL_MIN_PRICE_15M", 1.0)
FLOW_BREAKOUT_MIN_PRICE_5M = CONFIG.get("FLOW_BREAKOUT_MIN_PRICE_5M", 1.2)
FLOW_BREAKOUT_MIN_BUY_RATIO = CONFIG.get("FLOW_BREAKOUT_MIN_BUY_RATIO", 0.58)
FLOW_BREAKOUT_MIN_TRADE_ZSCORE = CONFIG.get("FLOW_BREAKOUT_MIN_TRADE_ZSCORE", 1.8)
FLOW_BREAKOUT_MAX_FUNDING_ABS = CONFIG.get("FLOW_BREAKOUT_MAX_FUNDING_ABS", 0.02)
OI_EXPANSION_MIN_PRICE_15M = CONFIG.get("OI_EXPANSION_MIN_PRICE_15M", 1.5)
OI_EXPANSION_MIN_OI_15M = CONFIG.get("OI_EXPANSION_MIN_OI_15M", 3.0)
OI_EXPANSION_MIN_BUY_RATIO = CONFIG.get("OI_EXPANSION_MIN_BUY_RATIO", 0.55)
OI_EXPANSION_MIN_POSITION_RATIO = CONFIG.get("OI_EXPANSION_MIN_POSITION_RATIO", 0.95)
OI_EXPANSION_MIN_NOTIONAL_CHANGE = CONFIG.get("OI_EXPANSION_MIN_NOTIONAL_CHANGE", 0.5)
SMART_MONEY_DIVERGENCE_STRONG = CONFIG.get("SMART_MONEY_DIVERGENCE_STRONG", 10.0)
TOP_ACCOUNT_LONG_STRONG = CONFIG.get("TOP_ACCOUNT_LONG_STRONG", 68.0)
TOP_ACCOUNT_RATIO_STRONG = CONFIG.get("TOP_ACCOUNT_RATIO_STRONG", 1.8)
TAKER_RATIO_GOOD = CONFIG.get("TAKER_RATIO_GOOD", 1.8)
TAKER_RATIO_STRONG = CONFIG.get("TAKER_RATIO_STRONG", 2.5)
TAKER_RATIO_EXCEPTIONAL = CONFIG.get("TAKER_RATIO_EXCEPTIONAL", 4.0)
POSITION_LED_SQUEEZE_MIN_POSITION_RATIO = CONFIG.get("POSITION_LED_SQUEEZE_MIN_POSITION_RATIO", 1.15)
POSITION_LED_SQUEEZE_MAX_ACCOUNT_RATIO = CONFIG.get("POSITION_LED_SQUEEZE_MAX_ACCOUNT_RATIO", 0.95)
POSITION_LED_SQUEEZE_MAX_LS_RATIO = CONFIG.get("POSITION_LED_SQUEEZE_MAX_LS_RATIO", 0.95)
POSITION_LED_SQUEEZE_MIN_DIVERGENCE = CONFIG.get("POSITION_LED_SQUEEZE_MIN_DIVERGENCE", 12.0)
POSITION_LED_SQUEEZE_MIN_PRICE_15M = CONFIG.get("POSITION_LED_SQUEEZE_MIN_PRICE_15M", 0.6)
POSITION_LED_SQUEEZE_MIN_OI_15M = CONFIG.get("POSITION_LED_SQUEEZE_MIN_OI_15M", 1.8)
POSITION_LED_SQUEEZE_MIN_BUY_RATIO = CONFIG.get("POSITION_LED_SQUEEZE_MIN_BUY_RATIO", 0.50)
POSITION_LED_SQUEEZE_MIN_POSITION_DELTA = CONFIG.get("POSITION_LED_SQUEEZE_MIN_POSITION_DELTA", 0.05)
POSITION_LED_SQUEEZE_MIN_POSITION_SLOPE = CONFIG.get("POSITION_LED_SQUEEZE_MIN_POSITION_SLOPE", 0.0)
POSITION_LED_SQUEEZE_NEG_FUNDING_IMPROVEMENT_BONUS = CONFIG.get("POSITION_LED_SQUEEZE_NEG_FUNDING_IMPROVEMENT_BONUS", 0.15)


# =============================================================================
# سجل التشخيص والقبول/الرفض
# =============================================================================
class DiagnosticsLogger:
    def __init__(self, path=DIAGNOSTICS_PATH, enabled=True, max_records=MAX_DIAGNOSTIC_RECORDS):
        self.path = path
        self.enabled = enabled
        self.max_records = max_records
        self.lock = threading.Lock()
        if self.enabled:
            ensure_parent_dir(self.path)

    def _append(self, payload):
        if not self.enabled:
            return
        try:
            payload = dict(payload)
            payload.setdefault('ts', datetime.now().isoformat())
            line = json.dumps(payload, ensure_ascii=False)
            with self.lock:
                with open(self.path, 'a', encoding='utf-8') as f:
                    f.write(line + '\n')
                self._trim_if_needed()
        except Exception:
            pass

    def _trim_if_needed(self):
        try:
            if self.max_records <= 0 or not os.path.exists(self.path):
                return
            with open(self.path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            if len(lines) > self.max_records:
                with open(self.path, 'w', encoding='utf-8') as f:
                    f.writelines(lines[-self.max_records:])
        except Exception:
            pass

    def log_reject(self, symbol, pattern, reason, data=None):
        if not WRITE_REJECTION_LOGS:
            return None
        self._append({'type': 'reject', 'symbol': symbol, 'pattern': pattern, 'reason': reason, 'data': data or {}})
        return None

    def log_accept(self, symbol, pattern, score=None, price=None, reasons=None, data=None):
        self._append({'type': 'accept', 'symbol': symbol, 'pattern': pattern, 'score': score, 'price': price, 'reasons': reasons or [], 'data': data or {}})

    def log_error(self, symbol, stage, error):
        self._append({'type': 'error', 'symbol': symbol, 'stage': stage, 'error': str(error)})

diagnostics = DiagnosticsLogger()

def log_rejection(symbol, pattern, reason, **data):
    return diagnostics.log_reject(symbol, pattern, reason, data)

def log_acceptance(symbol, pattern, score=None, price=None, reasons=None, **data):
    diagnostics.log_accept(symbol, pattern, score=score, price=price, reasons=reasons, data=data)

# =============================================================================
# نظام التخزين المؤقت (Cache) مع دعم TTL متغير
# =============================================================================
class TTLCache:
    def __init__(self):
        self.cache = {}
        self.lock = threading.Lock()

    def get(self, key):
        with self.lock:
            if key in self.cache:
                value, expiry = self.cache[key]
                if expiry > time.time():
                    return value
                else:
                    del self.cache[key]
        return None

    def set(self, key, value, ttl=CACHE_TTL_SECONDS):
        with self.lock:
            expiry = time.time() + ttl
            self.cache[key] = (value, expiry)

    def clear(self):
        with self.lock:
            self.cache.clear()

cache = TTLCache()

# =============================================================================
# مدير الوزن (Weight Manager) لتتبع استهلاك نقاط API
# =============================================================================
class WeightManager:
    def __init__(self, max_weight_per_minute=6000):
        self.max_weight = max_weight_per_minute
        self.used_weight = 0
        self.reset_time = time.time() + 60
        self.lock = threading.Lock()

    def can_request(self, weight=1):
        """التحقق مما إذا كان يمكن إرسال طلب بهذا الوزن."""
        with self.lock:
            now = time.time()
            if now > self.reset_time:
                self.used_weight = 0
                self.reset_time = now + 60
            if self.used_weight + weight <= self.max_weight:
                self.used_weight += weight
                return True
            else:
                return False

    def wait_if_needed(self, weight=1):
        """الانتظار حتى يتوفر الوزن الكافي."""
        while not self.can_request(weight):
            # الانتظار حتى إعادة التعيين التالية
            sleep_time = self.reset_time - time.time()
            if sleep_time > 0:
                time.sleep(min(sleep_time, 1))
        return True

weight_manager = WeightManager()

# =============================================================================
# إدارة الجلسات (Session Pool) مع إعادة المحاولة
# =============================================================================
class SessionManager:
    def __init__(self, max_workers):
        self.max_workers = max_workers
        self.sessions = []
        self.lock = threading.Lock()
        self._init_sessions()

    def _init_sessions(self):
        for i in range(self.max_workers):
            self.sessions.append(self._create_session())

    def _create_session(self):
        session = requests.Session()
        retry = Retry(
            total=MAX_RETRIES,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        return session

    def get_session(self, index=None):
        if index is not None:
            return self.sessions[index % self.max_workers]
        return random.choice(self.sessions)

    def close_all(self):
        for s in self.sessions:
            s.close()

session_manager = SessionManager(MAX_WORKERS)
atexit.register(session_manager.close_all)

# =============================================================================
# إدارة Binance Client
# =============================================================================
def get_binance_client():
    if USE_TESTNET:
        return Client(BINANCE_API_KEY, BINANCE_API_SECRET, testnet=True)
    else:
        return Client(BINANCE_API_KEY, BINANCE_API_SECRET)

client = get_binance_client()

failed_open_interest_symbols = {}
failed_open_interest_lock = threading.Lock()
OPEN_INTEREST_FAILURE_TTL_SECONDS = 900

def _is_failed_open_interest_symbol(symbol):
    now = time.time()
    with failed_open_interest_lock:
        exp = failed_open_interest_symbols.get(symbol)
        if exp and exp > now:
            return True
        if exp and exp <= now:
            failed_open_interest_symbols.pop(symbol, None)
    return False

def _mark_failed_open_interest_symbol(symbol, ttl=OPEN_INTEREST_FAILURE_TTL_SECONDS):
    with failed_open_interest_lock:
        failed_open_interest_symbols[symbol] = time.time() + max(60, int(ttl))

def _is_valid_usdt_perp_symbol(symbol):
    if not symbol:
        return False
    symbol = str(symbol).upper()
    if TRADING_SYMBOLS_SET:
        return symbol in TRADING_SYMBOLS_SET
    syms = get_all_usdt_perpetuals()
    return symbol in set(syms)

# =============================================================================
# دوال جلب البيانات من Binance مع تحسينات الأداء والتخزين المؤقت
# =============================================================================
BASE_URL = "https://fapi.binance.com"

def fetch_json(url, params=None, use_cache=True, weight=1, cache_ttl=None):
    if cache_ttl is None:
        cache_ttl = CACHE_TTL_SECONDS
    cache_key = url + str(sorted(params.items())) if params else url
    if use_cache:
        cached = cache.get(cache_key)
        if cached is not None:
            return cached

    # الانتظار حتى يتوفر الوزن
    weight_manager.wait_if_needed(weight)

    session = session_manager.get_session()
    max_retries = MAX_RETRIES
    retry_delay = 1  # ثانية أولية
    for attempt in range(max_retries + 1):
        try:
            resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                if use_cache:
                    cache.set(cache_key, data, ttl=cache_ttl)
                return data
            elif resp.status_code in (429, 418):
                # تجاوز حد الطلبات - الانتظار حسب Retry-After أو تأخير تصاعدي
                retry_after = int(resp.headers.get('Retry-After', retry_delay))
                print(f"⚠️ تجاوز حد الطلبات، انتظار {retry_after} ثانية...")
                time.sleep(retry_after)
                retry_delay *= 2  # exponential backoff
                continue
            else:
                if '/fapi/v1/openInterest' in url and resp.status_code == 400:
                    return None
                if DEBUG:
                    print(f"⚠️ HTTP {resp.status_code} لـ {url}")
                return None
        except requests.exceptions.RequestException as e:
            if DEBUG:
                print(f"🔴 خطأ في {url} (محاولة {attempt+1}): {e}")
            if attempt < max_retries:
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                return None
    return None

def get_all_usdt_perpetuals():
    data = fetch_json(f"{BASE_URL}/fapi/v1/exchangeInfo", use_cache=True, weight=10, cache_ttl=3600)  # ساعة كاملة
    if not data:
        return []
    return [s['symbol'] for s in data['symbols']
            if s['contractType'] == 'PERPETUAL' and s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING']

def get_ticker_24hr():
    # وزن 40 لجلب الكل
    return fetch_json(f"{BASE_URL}/fapi/v1/ticker/24hr", use_cache=True, weight=40, cache_ttl=10)  # 10 ثوانٍ فقط

def get_ticker_24hr_one_symbol(symbol):
    # نستخدم cache لمدة ثانيتين فقط لأن السعر يتغير بسرعة
    data = fetch_json(f"{BASE_URL}/fapi/v1/ticker/24hr", {'symbol': symbol}, use_cache=True, weight=1, cache_ttl=2)
    return data

def get_open_interest(symbol):
    symbol = str(symbol or '').strip().upper()
    if not symbol:
        return None
    if _is_failed_open_interest_symbol(symbol):
        return None
    if not _is_valid_usdt_perp_symbol(symbol):
        _mark_failed_open_interest_symbol(symbol, ttl=3600)
        mark_symbol_invalid(symbol)
        return None

    data = fetch_json(f"{BASE_URL}/fapi/v1/openInterest", {'symbol': symbol}, use_cache=False, weight=1)
    if data is None:
        _mark_failed_open_interest_symbol(symbol)
        mark_symbol_invalid(symbol)
        return None
    try:
        return float(data['openInterest']) if data else None
    except Exception:
        _mark_failed_open_interest_symbol(symbol)
        return None

def get_funding_rate(symbol):
    data = fetch_json(f"{BASE_URL}/fapi/v1/premiumIndex", {'symbol': symbol}, use_cache=True, weight=1, cache_ttl=5)  # 5 ثوانٍ
    if data:
        return float(data['lastFundingRate']) * 100, int(data['nextFundingTime'])
    return None, None

def get_top_long_short_ratio(symbol):
    """متوافق للخلفية القديمة: يعيد Position Ratio snapshot فقط."""
    snapshot = get_top_position_snapshot(symbol, period='5m')
    if snapshot:
        return snapshot['ratio'], snapshot['long_pct'], snapshot['short_pct']
    return None, None, None

def get_top_position_snapshot(symbol, period='5m'):
    """جلب لقطة Position Ratio الخاصة بكبار المتداولين."""
    data = fetch_json(
        f"{BASE_URL}/futures/data/topLongShortPositionRatio",
        {'symbol': symbol, 'period': period, 'limit': 1},
        use_cache=True, weight=1, cache_ttl=10
    )
    if data and len(data) > 0:
        row = data[0]
        return {
            'ratio': float(row['longShortRatio']),
            'long_pct': float(row['longAccount']),
            'short_pct': float(row['shortAccount']),
        }
    return None

def get_top_account_snapshot(symbol, period='5m'):
    """جلب لقطة Account Ratio الخاصة بكبار المتداولين."""
    data = fetch_json(
        f"{BASE_URL}/futures/data/topLongShortAccountRatio",
        {'symbol': symbol, 'period': period, 'limit': 1},
        use_cache=True, weight=1, cache_ttl=10
    )
    if data and len(data) > 0:
        row = data[0]
        return {
            'ratio': float(row['longShortRatio']),
            'long_pct': float(row['longAccount']),
            'short_pct': float(row['shortAccount']),
        }
    return None

def get_position_ratio_history(symbol, period='5m', limit=12):
    """جلب تاريخ نسبة مراكز كبار المتداولين (Position Ratio) مع ترتيب زمني (الأقدم أولاً)."""
    cache_key = f"pos_ratio_hist_{symbol}_{period}_{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    data = fetch_json(f"{BASE_URL}/futures/data/topLongShortPositionRatio",
                      {'symbol': symbol, 'period': period, 'limit': limit}, use_cache=False, weight=1)
    if data:
        ratios = [float(d['longShortRatio']) for d in data][::-1]
        cache.set(cache_key, ratios, ttl=HISTORIC_CACHE_TTL)
        return ratios
    return []

def get_account_ratio_history(symbol, period='5m', limit=12):
    """جلب تاريخ نسبة حسابات كبار المتداولين (Account Ratio) مع ترتيب زمني (الأقدم أولاً)."""
    cache_key = f"acc_ratio_hist_{symbol}_{period}_{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    data = fetch_json(f"{BASE_URL}/futures/data/topLongShortAccountRatio",
                      {'symbol': symbol, 'period': period, 'limit': limit}, use_cache=False, weight=1)
    if data:
        ratios = [float(d['longShortRatio']) for d in data][::-1]
        cache.set(cache_key, ratios, ttl=HISTORIC_CACHE_TTL)
        return ratios
    return []

def get_klines(symbol, interval=None, limit=None):
    if interval is None:
        interval = PRIMARY_TIMEFRAME
    if limit is None:
        limit = NUMBER_OF_CANDLES
    # وزن klines = 1 لكل 100 شمعة (تقريباً)
    weight = max(1, limit // 100)
    cache_key = f"klines_{symbol}_{interval}_{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    data = fetch_json(f"{BASE_URL}/fapi/v1/klines",
                      {'symbol': symbol, 'interval': interval, 'limit': limit}, use_cache=False, weight=weight)
    if data:
        result = {
            'open': [float(k[1]) for k in data],
            'high': [float(k[2]) for k in data],
            'low': [float(k[3]) for k in data],
            'close': [float(k[4]) for k in data],
            'volume': [float(k[5]) for k in data],
            'quote_volume': [float(k[7]) for k in data],
            'trades': [float(k[8]) for k in data],
            'taker_buy_volume': [float(k[9]) for k in data],
            'taker_buy_quote_volume': [float(k[10]) for k in data]
        }
        cache.set(cache_key, result, ttl=30)  # 30 ثانية للشموع
        return result
    return None

def get_oi_history(symbol, period=None, limit=30):
    """جلب تاريخ الفائدة المفتوحة مع ترتيب زمني (الأقدم أولاً)."""
    if period is None:
        period = OI_HISTORY_TIMEFRAME
    cache_key = f"oi_hist_{symbol}_{period}_{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    data = fetch_json(f"{BASE_URL}/futures/data/openInterestHist",
                      {'symbol': symbol, 'period': period, 'limit': limit}, use_cache=False, weight=1)
    if data:
        # البيانات تأتي من الأحدث إلى الأقدم، نعكسها
        oi = [float(d['sumOpenInterest']) for d in data][::-1]
        cache.set(cache_key, oi, ttl=HISTORIC_CACHE_TTL)
        return oi
    return []

def get_funding_rate_history(symbol, limit=30):
    """جلب تاريخ معدلات التمويل مع ترتيب زمني (الأقدم أولاً)."""
    cache_key = f"funding_hist_{symbol}_{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    data = fetch_json(f"{BASE_URL}/fapi/v1/fundingRate",
                      {'symbol': symbol, 'limit': limit}, use_cache=False, weight=1)
    if data:
        # البيانات تأتي من الأحدث إلى الأقدم، نعكسها
        rates = [float(d['fundingRate']) * 100 for d in data][::-1]
        cache.set(cache_key, rates, ttl=HISTORIC_CACHE_TTL)
        return rates
    return []

def get_basis(symbol):
    data = fetch_json(f"{BASE_URL}/fapi/v1/premiumIndex", {'symbol': symbol}, use_cache=True, weight=1, cache_ttl=2)
    if data:
        mark = float(data['markPrice'])
        index = float(data['indexPrice'])
        return (mark - index) / index * 100
    return 0.0

def get_mark_price(symbol):
    data = fetch_json(f"{BASE_URL}/fapi/v1/premiumIndex", {'symbol': symbol}, use_cache=True, weight=1, cache_ttl=2)
    if data:
        return float(data['markPrice'])
    return None

def pct_change(old, new):
    return ((new - old) / old) * 100 if old and old != 0 else 0


def get_oi_notional_history(symbol, period='5m', limit=12):
    """جلب تاريخ القيمة الاسمية للفائدة المفتوحة إن كانت متاحة."""
    cache_key = f"oi_notional_hist_{symbol}_{period}_{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    data = fetch_json(f"{BASE_URL}/futures/data/openInterestHist", {'symbol': symbol, 'period': period, 'limit': limit}, use_cache=False, weight=1)
    if data:
        values = []
        for d in data[::-1]:
            v = d.get('sumOpenInterestValue')
            values.append(float(v) if v not in (None, '') else 0.0)
        cache.set(cache_key, values, ttl=HISTORIC_CACHE_TTL)
        return values
    return []

def rolling_mean(seq):
    return float(np.mean(seq)) if seq else 0.0

def compute_taker_buy_ratio(kl):
    ratios = []
    if not kl or 'volume' not in kl or 'taker_buy_volume' not in kl:
        return ratios
    for vol, tb in zip(kl['volume'], kl['taker_buy_volume']):
        ratios.append((tb / vol) if vol and vol > 0 else 0.0)
    return ratios

def compute_taker_buy_quote_ratio(kl):
    ratios = []
    if not kl or 'quote_volume' not in kl or 'taker_buy_quote_volume' not in kl:
        return ratios
    for qv, tbq in zip(kl['quote_volume'], kl['taker_buy_quote_volume']):
        ratios.append((tbq / qv) if qv and qv > 0 else 0.0)
    return ratios

def trade_count_zscore(kl, lookback=12):
    if not kl or 'trades' not in kl or len(kl['trades']) < max(6, lookback):
        return 0.0
    hist = kl['trades'][-lookback:-1]
    if len(hist) < 3:
        return 0.0
    return zscore(hist, kl['trades'][-1])

def get_global_long_short_snapshot(symbol, period='5m'):
    data = fetch_json(f"{BASE_URL}/futures/data/globalLongShortAccountRatio",
                      {'symbol': symbol, 'period': period, 'limit': 1}, use_cache=True, weight=1, cache_ttl=10)
    if data and len(data) > 0:
        row = data[0]
        return {
            'ratio': float(row.get('longShortRatio', 0.0) or 0.0),
            'long_pct': float(row.get('longAccount', 0.0) or 0.0),
            'short_pct': float(row.get('shortAccount', 0.0) or 0.0),
        }
    return None

def smart_divergence_score(position_ratio, account_ratio):
    return float(position_ratio or 0.0) - float(account_ratio or 0.0)

def compute_smart_money_divergence(account_long_pct, position_long_pct):
    """قياس الفجوة المبكرة عندما تكون المراكز أميل للشراء من الحسابات.
    القيم الموجبة تعني Position-led squeeze setup.
    """
    return float(position_long_pct or 0.0) - float(account_long_pct or 0.0)

def smart_money_divergence_feature_score(div):
    if div >= 18:
        return 1.0
    if div >= 12:
        return 0.8
    if div >= 8:
        return 0.6
    if div >= 5:
        return 0.35
    return 0.0

def top_account_score(ratio, long_pct):
    ratio = float(ratio or 0.0)
    long_pct = float(long_pct or 0.0)
    if ratio >= 3.0 or long_pct >= 75:
        return 1.0
    if ratio >= 2.2 or long_pct >= 70:
        return 0.8
    if ratio >= 1.6 or long_pct >= 64:
        return 0.55
    if ratio >= 1.25 or long_pct >= 58:
        return 0.30
    return 0.0

def funding_regime_score(current_funding, funding_trend):
    score = 0.0
    if current_funding is None:
        return 0.0
    if -0.25 <= current_funding <= 0.01:
        score += 0.45
    if current_funding < 0 and funding_trend > 0:
        score += 0.40
    if 0.01 < current_funding <= 0.03:
        score += 0.10
    if current_funding > 0.03:
        score -= 0.20
    return max(0.0, min(score, 1.0))

def taker_flow_score(taker_buy_sell_ratio):
    if taker_buy_sell_ratio >= 4.0:
        return 1.0
    if taker_buy_sell_ratio >= 2.5:
        return 0.8
    if taker_buy_sell_ratio >= 1.8:
        return 0.6
    if taker_buy_sell_ratio >= 1.3:
        return 0.35
    return 0.0

def safe_buy_sell_ratio_from_buy_ratio(buy_ratio):
    buy_ratio = float(buy_ratio or 0.0)
    sell_ratio = max(1e-9, 1.0 - buy_ratio)
    return buy_ratio / sell_ratio

def get_funding_trend(symbol, limit=6):
    hist = get_funding_rate_history(symbol, limit=limit)
    if len(hist) < 2:
        return 0.0
    return hist[-1] - hist[-2]

def basis_context_score(basis, oi_change_15m=0.0, taker_score=0.0):
    if basis is None:
        return 0.5
    b = float(basis)
    if -0.20 <= b <= 0.20:
        return 0.85
    if -0.35 <= b < -0.20:
        return 0.70
    if 0.20 < b <= 0.45:
        return 0.65
    if b < -0.50 and oi_change_15m < 1.0 and taker_score < 0.35:
        return 0.10
    return 0.40

def oi_regime_score(oi_change_5m, oi_change_15m):
    score = 0.0
    if oi_change_15m >= 3.0:
        score += 0.70
    elif oi_change_15m >= 1.5:
        score += 0.45
    if oi_change_5m >= 1.2:
        score += 0.25
    elif oi_change_5m >= 0.6:
        score += 0.15
    return max(0.0, min(score, 1.0))

def oi_notional_score(oi_notional_change):
    x = float(oi_notional_change or 0.0)
    if x >= 2.5:
        return 1.0
    if x >= 1.6:
        return 0.8
    if x >= 0.8:
        return 0.6
    if x >= 0.4:
        return 0.35
    return 0.0

FEATURE_WEIGHTS = {
    "oi_regime": 0.20,
    "compression": 0.16,
    "taker_aggression": 0.18,
    "cvd_flow": 0.10,
    "liquidity_vacuum": 0.12,
    "smart_money_accounts": 0.12,
    "smart_money_divergence": 0.07,
    "funding_regime": 0.03,
    "basis_context": 0.02
}

def weighted_feature_score(feature_scores):
    total = 0.0
    for k, w in FEATURE_WEIGHTS.items():
        total += float(feature_scores.get(k, 0.0)) * w
    return max(0.0, min(total, 1.0))

def compute_four_h_context_bonus(symbol):
    bonus = 0.0
    pos4 = get_top_position_snapshot(symbol, period='4h')
    acc4 = get_top_account_snapshot(symbol, period='4h')
    if not pos4 or not acc4:
        return 0.0
    four_h_position_ratio = float(pos4.get('ratio', 0.0) or 0.0)
    four_h_account_ratio = float(acc4.get('ratio', 0.0) or 0.0)
    if four_h_position_ratio > 1.15 and four_h_account_ratio < 0.95:
        bonus += 0.25
    if four_h_account_ratio > 1.4 and four_h_position_ratio < 1.05:
        bonus += 0.25
    if four_h_position_ratio > 1.3 and four_h_account_ratio > 1.2:
        bonus += 0.35
    return bonus

def classify_bullish_signal(features):
    position_ratio = float(features.get('position_ratio', 0.0) or 0.0)
    account_ratio = float(features.get('account_ratio', 0.0) or 0.0)
    ls_ratio = float(features.get('ls_ratio', account_ratio) or 0.0)
    top_position_long_pct = float(features.get('top_position_long_pct', 0.0) or 0.0)
    top_account_long_pct = float(features.get('top_account_long_pct', 0.0) or 0.0)
    position_led_divergence = top_position_long_pct - top_account_long_pct
    account_led_divergence = top_account_long_pct - top_position_long_pct
    oi_change_15m = float(features.get('oi_change_15m', 0.0) or 0.0)
    price_change_15m = float(features.get('price_change_15m', 0.0) or 0.0)
    price_change_5m = float(features.get('price_change_5m', 0.0) or 0.0)
    taker_buy_ratio_recent = float(features.get('taker_buy_ratio_recent', 0.0) or 0.0)
    trade_count_z = float(features.get('trade_count_zscore', 0.0) or 0.0)
    funding_abs = abs(float(features.get('funding_rate', 0.0) or 0.0))
    oi_notional_strong = bool(features.get('oi_notional_strong', False))

    if (position_ratio >= 1.40 and account_ratio <= 0.90 and ls_ratio <= 0.90 and position_led_divergence >= 18 and oi_notional_strong and taker_buy_ratio_recent >= 0.55):
        return 'POSITION_LED_SQUEEZE_BUILDUP', 'position_led_divergence'
    if (position_ratio >= 1.20 and account_ratio <= 1.05 and ls_ratio <= 0.95 and position_led_divergence >= 12 and oi_change_15m >= 2.0 and price_change_15m >= 1.0 and funding_abs <= 0.08):
        return 'POSITION_LED_SQUEEZE_BUILDUP', 'position_led_divergence'
    if (account_ratio >= 1.8 and top_account_long_pct >= 62 and position_ratio <= 1.0 and features.get('oi_rising_strongly', False) and features.get('price_breakout_confirmed', False)):
        return 'ACCOUNT_LED_ACCUMULATION', 'account_led_divergence'
    if (account_ratio >= 1.6 and top_account_long_pct >= 60 and position_ratio >= 0.90 and oi_change_15m >= 1.5 and price_change_15m >= 1.0 and abs(float(features.get('basis', 0.0) or 0.0)) <= 0.20 and (float(features.get('funding_rate', 0.0) or 0.0) < 0 or funding_abs <= 0.03)):
        return 'ACCOUNT_LED_ACCUMULATION', 'top_account_ratio'
    if (position_ratio >= 1.8 and account_ratio >= 1.2 and ls_ratio >= 1.2 and oi_change_15m >= 1.5 and float(features.get('funding_rate', 0.0) or 0.0) <= 0.02 and price_change_15m >= 1.0):
        return 'CONSENSUS_BULLISH_EXPANSION', 'oi_regime'
    if funding_abs <= 0.02 and price_change_5m >= 1.2 and trade_count_z >= 2.0 and taker_buy_ratio_recent >= 0.60:
        return 'FLOW_LIQUIDITY_VACUUM_BREAKOUT', 'taker_buy_sell_ratio'
    return 'NO_CLEAR_BULLISH_FAMILY', 'insufficient_signature'

def get_utc_hour_now():
    return datetime.utcnow().hour

def apply_time_window_boost(signal_data, utc_hour=None):
    if not signal_data or not ENABLE_TIME_WINDOW_BOOST:
        return signal_data
    try:
        hour = get_utc_hour_now() if utc_hour is None else int(utc_hour)
        if hour in set(TIME_WINDOW_BOOST_HOURS):
            signal_data['score'] = min(100, float(signal_data.get('score', 0)) + TIME_WINDOW_BOOST_SCORE)
            reasons = signal_data.setdefault('reasons', [])
            boost_reason = f"نافذة زمنية داعمة UTC {hour:02d}:00"
            if boost_reason not in reasons:
                reasons.append(boost_reason)
    except Exception:
        pass
    return signal_data

def classify_signal_stage(price_5m=0.0, price_15m=0.0, oi_5m=0.0, oi_15m=0.0, funding_rate=None, buy_ratio=0.0, score=0.0):
    funding_abs = abs(funding_rate) if funding_rate is not None else 0.0
    if price_5m >= 3.5 or price_15m >= 8.0 or score >= 90 or funding_abs >= 0.08:
        return 'LATE'
    if price_5m >= 2.0 or price_15m >= 4.0 or oi_15m >= 6.0 or score >= 80:
        return 'CONFIRMED'
    if buy_ratio >= 0.58 and (oi_5m >= 1.5 or oi_15m >= 2.5) and price_15m >= 0.8:
        return 'TRIGGERED'
    if buy_ratio >= 0.55 and (oi_5m >= 1.0 or oi_15m >= 2.0):
        return 'ARMED'
    return 'SETUP'

def pre_pump_early_filter(price_5m=0.0, price_15m=0.0, funding_rate=None, oi_15m=0.0):
    """فلتر مبكر موحّد: يمنع الإشارات المتأخرة/المزدحمة قبل الاعتماد النهائي."""
    funding_abs = abs(float(funding_rate or 0.0)) if funding_rate is not None else 0.0

    # امتداد سعري متأخر
    if price_15m >= 6.5 or price_5m >= 3.2:
        return False, 'late_price_extension'

    # تمويل مزدحم مع حركة ممتدة = غالبًا متأخر
    if funding_rate is not None and float(funding_rate) > 0.03 and price_15m >= 2.5:
        return False, 'crowded_positive_funding_after_move'

    # عند OI مرتفع جدًا + سعر ممتد غالبًا late-confirmation
    if oi_15m >= 8.0 and price_15m >= 4.0:
        return False, 'late_oi_expansion_confirmation'

    # تمويل مبالغ به بصرف النظر
    if funding_abs >= 0.08:
        return False, 'extreme_funding_regime'

    return True, None

def attach_signal_stage(signal_data, price_5m=0.0, price_15m=0.0, oi_5m=0.0, oi_15m=0.0, funding_rate=None, buy_ratio=0.0):
    if not signal_data:
        return signal_data
    signal_data['signal_stage'] = classify_signal_stage(price_5m=price_5m, price_15m=price_15m, oi_5m=oi_5m, oi_15m=oi_15m, funding_rate=funding_rate, buy_ratio=buy_ratio, score=signal_data.get('score', 0))
    return signal_data


def get_short_term_context(symbol, interval='5m', limit=12):
    """سياق قصير المدى لتأكيد اتجاه الإشارة ومنع الإشارات المتأخرة أو المعاكسة."""
    try:
        kl = get_klines(symbol, interval, limit)
        if not kl or len(kl['close']) < max(8, limit):
            return None
        close = kl['close']
        high = kl['high']
        low = kl['low']
        vol = kl['volume']
        tb = kl['taker_buy_volume']

        ratios = []
        for i in range(len(vol)):
            ratios.append((tb[i] / vol[i]) if vol[i] > 0 else 0.5)

        recent_buy = mean(ratios[-3:])
        prev_buy = mean(ratios[-6:-3]) if len(ratios) >= 6 else recent_buy
        recent_sell = 1 - recent_buy
        prev_sell = 1 - prev_buy

        price_5m = pct_change(close[-2], close[-1])
        price_15m = pct_change(close[-4], close[-1]) if len(close) >= 4 else price_5m
        impulse_up = close[-1] > close[-2] and close[-1] >= max(close[-4:-1])
        impulse_down = close[-1] < close[-2] and close[-1] <= min(close[-4:-1])
        ema_fast = ema(close[-6:], alpha=0.45)
        above_ema = close[-1] >= ema_fast
        below_ema = close[-1] <= ema_fast
        body = abs(close[-1] - kl['open'][-1])
        rng = max(1e-9, high[-1] - low[-1])
        body_ratio = body / rng
        return {
            'price_5m': price_5m,
            'price_15m': price_15m,
            'recent_buy_ratio': recent_buy,
            'prev_buy_ratio': prev_buy,
            'recent_sell_ratio': recent_sell,
            'prev_sell_ratio': prev_sell,
            'buy_accel': recent_buy - prev_buy,
            'sell_accel': recent_sell - prev_sell,
            'impulse_up': impulse_up,
            'impulse_down': impulse_down,
            'above_ema': above_ema,
            'below_ema': below_ema,
            'body_ratio': body_ratio,
            'last_close': close[-1],
            'last_high': high[-1],
            'last_low': low[-1],
        }
    except Exception:
        return None


def directional_consistency_ok(signal_data, context):
    if not signal_data or not context:
        return False, 'missing_short_term_context'

    direction = signal_data.get('direction', 'UP')
    pattern = signal_data.get('pattern', '')
    p5 = context['price_5m']
    p15 = context['price_15m']
    buy = context['recent_buy_ratio']
    sell = context['recent_sell_ratio']
    body_ratio = context['body_ratio']

    if direction == 'UP':
        if not (context['impulse_up'] or (context['above_ema'] and p5 > -0.15 and p15 > 0.15)):
            return False, 'up_direction_not_confirmed_by_recent_candles'
        if buy < 0.52 and context['buy_accel'] < 0.0:
            return False, 'up_signal_without_taker_buy_support'
        if pattern in {'WHALE_MOMENTUM', 'WHALE_MOMENTUM_UP', 'FLOW_LIQUIDITY_VACUUM_BREAKOUT', 'SKYROCKET', 'STRONG_UPTREND', 'CONSENSUS_BULLISH_EXPANSION', 'ACCOUNT_LED_ACCUMULATION'} and p15 > 6.5:
            return False, 'up_signal_arrived_too_late'
        if pattern in {'PRE_SQUEEZE_UP', 'PRE_EXPLOSION_SETUP', 'SMART_DIVERGENCE_SQUEEZE', 'POSITION_LED_SQUEEZE_BUILDUP'} and p15 > 5.0:
            return False, 'setup_arrived_after_expansion'
        if body_ratio < 0.18 and p5 < 0:
            return False, 'up_signal_lost_impulse'
        return True, None

    if direction == 'DOWN':
        if not (context['impulse_down'] or (context['below_ema'] and p5 < 0.15 and p15 < -0.15)):
            return False, 'down_direction_not_confirmed_by_recent_candles'
        if sell < 0.52 and context['sell_accel'] < 0.0:
            return False, 'down_signal_without_taker_sell_support'
        if pattern in {'RETAIL_TRAP', 'RETAIL_TRAP_DOWN', 'BEAR_SQUEEZE', 'WHALE_MOMENTUM_DOWN', 'EXHAUSTION_RISK'} and p15 < -7.0:
            return False, 'down_signal_arrived_too_late'
        if body_ratio < 0.18 and p5 > 0:
            return False, 'down_signal_lost_impulse'
        return True, None

    return True, None


def normalize_signal_pattern(signal_data):
    if not signal_data:
        return signal_data
    pattern = signal_data.get('pattern')
    direction = signal_data.get('direction')
    if pattern == 'WHALE_MOMENTUM':
        signal_data['pattern'] = 'WHALE_MOMENTUM_UP' if direction == 'UP' else 'WHALE_MOMENTUM_DOWN'
    elif pattern == 'RETAIL_TRAP':
        signal_data['pattern'] = 'RETAIL_TRAP_DOWN' if direction == 'DOWN' else 'RETAIL_TRAP_UP'
    return signal_data


def validate_and_finalize_signal(signal_data):
    if not signal_data:
        return None
    signal_data.setdefault('reasons', [])
    signal_data.setdefault('feature_scores', {})
    if 'classification' not in signal_data or not signal_data.get('classification'):
        signal_data['classification'] = 'NO_CLEAR_BULLISH_FAMILY' if signal_data.get('direction', 'UP') == 'UP' else 'BEARISH_GENERIC'
    if 'decisive_feature' not in signal_data or not signal_data.get('decisive_feature'):
        signal_data['decisive_feature'] = 'score'
    context = get_short_term_context(signal_data['symbol'], '5m', 12)
    if not context:
        diagnostics.log_reject(signal_data.get('symbol', 'UNKNOWN'), signal_data.get('pattern', 'UNKNOWN'), 'missing_validation_context', {})
        return None

    ok, reason = directional_consistency_ok(signal_data, context)
    if not ok:
        diagnostics.log_reject(signal_data.get('symbol', 'UNKNOWN'), signal_data.get('pattern', 'UNKNOWN'), reason, context)
        return None

    oi_5m = 0.0
    oi_15m = 0.0
    oi5 = get_oi_history(signal_data['symbol'], '5m', 6)
    oi15 = get_oi_history(signal_data['symbol'], '15m', 4)
    if len(oi5) >= 3:
        oi_5m = pct_change(oi5[-3], oi5[-1])
    if len(oi15) >= 2:
        oi_15m = pct_change(oi15[-2], oi15[-1])

    signal_data = normalize_signal_pattern(signal_data)
    signal_data['validation_context'] = {
        'price_5m': context['price_5m'],
        'price_15m': context['price_15m'],
        'buy_ratio': context['recent_buy_ratio'],
        'sell_ratio': context['recent_sell_ratio'],
        'buy_accel': context['buy_accel'],
        'sell_accel': context['sell_accel'],
        'body_ratio': context['body_ratio'],
        'trigger_high': context['last_high'],
        'trigger_low': context['last_low'],
    }
    signal_data['trigger_high'] = context['last_high']
    signal_data['trigger_low'] = context['last_low']
    signal_data.setdefault('reasons', [])
    direction_reason = f"تأكيد اتجاه {signal_data.get('direction')} من الشموع الأخيرة"
    if direction_reason not in signal_data['reasons']:
        signal_data['reasons'].append(direction_reason)
    signal_data = attach_signal_stage(
        signal_data,
        price_5m=context['price_5m'],
        price_15m=context['price_15m'],
        oi_5m=oi_5m,
        oi_15m=oi_15m,
        funding_rate=signal_data.get('funding'),
        buy_ratio=context['recent_buy_ratio']
    )
    if signal_data.get('signal_stage') == 'LATE' and signal_data.get('pattern') in {'WHALE_MOMENTUM_UP', 'FLOW_LIQUIDITY_VACUUM_BREAKOUT', 'SKYROCKET', 'STRONG_UPTREND', 'CONSENSUS_BULLISH_EXPANSION'}:
        diagnostics.log_reject(signal_data.get('symbol', 'UNKNOWN'), signal_data.get('pattern', 'UNKNOWN'), 'late_stage_after_validation', signal_data.get('validation_context', {}))
        return None
    diagnostics.log_accept(signal_data.get('symbol', 'UNKNOWN'), signal_data.get('pattern', 'UNKNOWN'), signal_data.get('score'), signal_data.get('price'), signal_data.get('reasons', []), signal_data.get('validation_context', {}))
    return signal_data

def calculate_apr(funding_rate_percent):
    return funding_rate_percent * 2190

# =============================================================================
# دوال إحصائية متقدمة
# =============================================================================
def mean(lst):
    return sum(lst) / len(lst) if lst else 0

def std(lst):
    if len(lst) < 2:
        return 0
    m = mean(lst)
    variance = sum((x - m) ** 2 for x in lst) / (len(lst) - 1)
    return variance ** 0.5

def zscore(series, value):
    if len(series) < 3:
        return 0
    m = mean(series)
    s = std(series)
    if s == 0:
        return 0
    return (value - m) / s

def percentile(series, value):
    if not series:
        return 50
    count_less = sum(1 for x in series if x < value)
    return (count_less / len(series)) * 100

def ema(series, alpha=0.3):
    if not series:
        return 0
    result = series[0]
    for val in series[1:]:
        result = alpha * val + (1 - alpha) * result
    return result

def rsi(prices, period=14):
    if len(prices) < period + 1:
        return 50
    gains = []
    losses = []
    for i in range(1, len(prices)):
        change = prices[i] - prices[i-1]
        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(-change)
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100
    rs = avg_gain / avg_loss
    return 100 - 100 / (1 + rs)

def macd(prices):
    if len(prices) < 26:
        return 0
    ema12 = ema(prices[-12:], alpha=2/(12+1))
    ema26 = ema(prices[-26:], alpha=2/(26+1))
    return ema12 - ema26

def whale_retail_gap(tr, gr):
    if tr and gr and gr > 0:
        gap = tr / gr
        if gap > 1.2:
            return "bullish_gap", gap
        elif gap < 0.8:
            return "bearish_gap", gap
    return "neutral", 1.0

def detect_liquidity_sweep(highs, lows, closes, volumes, current_price):
    if len(highs) < 10:
        return False
    avg_volume = mean(volumes[-11:-1]) if len(volumes) >= 11 else mean(volumes)
    for i in range(1, 6):
        if i > len(highs):
            break
        high = highs[-i]
        low = lows[-i]
        close = closes[-i]
        volume = volumes[-i]
        candle_range = high - low
        if candle_range == 0:
            continue
        is_bullish = close > (high + low) / 2
        if is_bullish:
            wick_ratio = (high - close) / candle_range
        else:
            wick_ratio = (close - low) / candle_range
        volume_ratio = volume / avg_volume if avg_volume > 0 else 1
        if wick_ratio > LIQUIDITY_SWEEP_WICK_RATIO and volume_ratio > 1.5:
            if is_bullish and abs(current_price - high) / current_price < 0.01:
                return True
            if not is_bullish and abs(current_price - low) / current_price < 0.01:
                return True
    return False

# =============================================================================
# دوال جديدة للتحسينات (z-score, تاريخ يومي، إدارة مخاطر)
# =============================================================================

def get_historical_stats(symbol, feature, limit=30):
    """
    جلب إحصائيات تاريخية لمؤشر معين (مثل نسبة كبار، OI، تمويل)
    لحساب المتوسط والانحراف المعياري.
    """
    if feature == 'top_ratio':
        data = get_daily_top_ratio(symbol, limit)
        return data if data else []
    elif feature == 'oi':
        data = get_oi_history(symbol, period='4h', limit=limit*6)  # تقريب يومي
        if data:
            # نأخذ كل 6 قيم كقيمة يومية
            daily = [data[i] for i in range(0, len(data), 6)]
            return daily[-limit:]
        return []
    elif feature == 'funding':
        data = get_funding_rate_history(symbol, limit=limit*3)
        if data:
            # نأخذ كل 3 قيم (8 ساعات) كقيمة يومية
            daily = [sum(data[i:i+3])/len(data[i:i+3]) for i in range(0, len(data), 3)]
            return daily[-limit:]
        return []
    return []

def get_daily_top_ratio(symbol, limit=30):
    """نسبة كبار على فريم يومي (من 4h)"""
    cache_key = f"daily_top_{symbol}_{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    data = fetch_json(f"{BASE_URL}/futures/data/topLongShortPositionRatio",
                      {'symbol': symbol, 'period': '4h', 'limit': limit*6}, use_cache=False, weight=1)
    if not data:
        return []
    # البيانات من الأحدث إلى الأقدم، نعكس
    data = data[::-1]
    daily = []
    for i in range(0, len(data), 6):
        if i < len(data):
            daily.append(float(data[i]['longAccount']))
    result = daily[-limit:]
    cache.set(cache_key, result, ttl=HISTORIC_CACHE_TTL)
    return result

def get_historical_avg_volume(symbol, days=7):
    cache_key = f"avg_vol_{symbol}_{days}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    klines = get_klines(symbol, '1d', limit=days)
    if not klines or len(klines['volume']) < days:
        return None
    volumes = klines['volume']
    avg = mean(volumes)
    cache.set(cache_key, avg, ttl=VOLUME_AVG_CACHE_TTL)
    return avg

def analyze_basis(symbol, current_reasons=None):
    basis = get_basis(symbol)
    if basis is None:
        return 0, []
    score = 0
    reasons = []
    reason_text = ""
    if basis < BASIS_STRONG_NEGATIVE:
        score += 15
        reason_text = f"📉 أساس سالب قوي {basis:+.2f}%"
    elif basis > BASIS_STRONG_POSITIVE:
        score += 5
        reason_text = f"⚠️ أساس موجب قوي {basis:+.2f}% (قد يكون تشبعاً)"
    if reason_text and (not current_reasons or reason_text not in current_reasons):
        reasons.append(reason_text)
    return score, reasons

def analyze_oi_acceleration(symbol, current_reasons=None):
    oi_hist = get_oi_history(symbol, period=OI_HISTORY_TIMEFRAME, limit=6)
    if len(oi_hist) < 6:
        return 0, []
    mom1 = oi_hist[-1] - oi_hist[-3]
    mom2 = oi_hist[-3] - oi_hist[-6]
    acceleration = mom1 - mom2
    if acceleration <= 0:
        return 0, []
    reason_text = ""
    if acceleration > OI_ACCELERATION_THRESHOLD * 1_000_000:
        score = 20
        reason_text = f"⚡ تسارع OI (قيمة {acceleration:.0f})"
    elif acceleration > OI_ACCELERATION_THRESHOLD * 500_000:
        score = 10
        reason_text = f"📈 تسارع OI معتدل"
    else:
        return 0, []
    reasons = []
    if reason_text and (not current_reasons or reason_text not in current_reasons):
        reasons.append(reason_text)
    return score, reasons

def analyze_volume_spike(symbol, current_reasons=None):
    ticker = get_ticker_24hr_one_symbol(symbol)
    if not ticker:
        return 0, []
    current_volume = float(ticker['volume'])
    avg_volume = get_historical_avg_volume(symbol, days=7)
    if not avg_volume or avg_volume == 0:
        return 0, []
    spike_ratio = current_volume / avg_volume
    reason_text = ""
    if spike_ratio >= VOLUME_SPIKE_THRESHOLD:
        score = 20
        reason_text = f"📊 حجم التداول {spike_ratio:.1f}x المتوسط"
    elif spike_ratio >= VOLUME_SPIKE_THRESHOLD * 0.75:
        score = 10
        reason_text = f"📈 حجم أعلى من المتوسط"
    else:
        return 0, []
    reasons = []
    if reason_text and (not current_reasons or reason_text not in current_reasons):
        reasons.append(reason_text)
    return score, reasons

def calculate_atr(symbol, period=14):
    """
    حساب Average True Range (ATR) على الفريم الرئيسي.
    """
    klines = get_klines(symbol, interval=PRIMARY_TIMEFRAME, limit=period+1)
    if not klines or len(klines['close']) < period+1:
        return None
    tr_list = []
    for i in range(1, len(klines['close'])):
        high = klines['high'][i]
        low = klines['low'][i]
        prev_close = klines['close'][i-1]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        tr_list.append(tr)
    atr = sum(tr_list[-period:]) / period
    return atr

def linear_trend_slope(y):
    """
    حساب ميل الانحدار الخطي لسلسلة y.
    """
    if len(y) < 2:
        return 0
    x = list(range(len(y)))
    n = len(y)
    sum_x = sum(x)
    sum_y = sum(y)
    sum_xy = sum(xi * yi for xi, yi in zip(x, y))
    sum_x2 = sum(xi**2 for xi in x)
    denominator = n * sum_x2 - sum_x**2
    if denominator == 0:
        return 0
    slope = (n * sum_xy - sum_x * sum_y) / denominator
    return slope


def analyze_ratio_shift(symbol, period='5m', limit=12):
    """تحليل الانعكاس الزمني بين Position Ratio وAccount Ratio."""
    pos_hist = get_position_ratio_history(symbol, period=period, limit=limit)
    acc_hist = get_account_ratio_history(symbol, period=period, limit=limit)

    if len(pos_hist) < 6 or len(acc_hist) < 6:
        return None

    pos_now = pos_hist[-1]
    pos_prev = pos_hist[-4]
    acc_now = acc_hist[-1]
    acc_prev = acc_hist[-4]

    pos_delta = pos_now - pos_prev
    acc_delta = acc_now - acc_prev

    pos_slope = linear_trend_slope(pos_hist[-6:])
    acc_slope = linear_trend_slope(acc_hist[-6:])

    return {
        'pos_now': pos_now,
        'pos_prev': pos_prev,
        'acc_now': acc_now,
        'acc_prev': acc_prev,
        'pos_delta': pos_delta,
        'acc_delta': acc_delta,
        'pos_slope': pos_slope,
        'acc_slope': acc_slope,
        'divergence_now': pos_now - acc_now
    }

def check_with_zscore(symbol, feature, value, z_threshold):
    """
    التحقق مما إذا كانت قيمة معينة تتجاوز z-score معين باستخدام البيانات التاريخية.
    مع شرط إضافي: يجب أن تتجاوز القيمة الحد الأدنى المطلق.
    """
    # التحقق من الحد الأدنى المطلق أولاً
    if feature == 'oi' and abs(value) < MIN_ABS_OI_CHANGE:
        return False
    if feature == 'funding' and abs(value) < MIN_ABS_FUNDING_CHANGE:
        return False

    hist = get_historical_stats(symbol, feature, limit=30)
    if len(hist) < 10:
        return False  # بيانات غير كافية
    z = zscore(hist, value)
    return abs(z) >= z_threshold

# =============================================================================
# دوال جديدة لتطبيع معدلات التمويل وتحليل التدفق وعناقيد التصفية
# =============================================================================

# تخزين مؤقت لفترات التمويل لكل رمز
funding_intervals_cache = {}

def get_funding_interval(symbol):
    """إرجاع فترة التمويل بالدقائق للرمز."""
    if symbol in funding_intervals_cache:
        return funding_intervals_cache[symbol]
    # جلب معلومات الرمز من exchangeInfo (يمكن تخزينها مؤقتًا)
    data = fetch_json(f"{BASE_URL}/fapi/v1/exchangeInfo", use_cache=True, weight=10, cache_ttl=3600)
    if data:
        for s in data['symbols']:
            if s['symbol'] == symbol:
                # فترة التمويل موجودة في s['fundingInterval'] (بالساعات) أو s['fundingIntervalHours']
                interval_hours = s.get('fundingInterval', 8)  # الافتراضي 8 ساعات
                interval_minutes = interval_hours * 60
                funding_intervals_cache[symbol] = interval_minutes
                return interval_minutes
    return 8 * 60  # افتراضي 8 ساعات

def normalize_funding_rate(funding_rate_percent, interval_minutes):
    """تحويل معدل التمويل إلى APR سنوي."""
    # funding_rate_percent هو النسبة المئوية للفترة
    # APR = funding_rate_percent * (365 * 24 * 60 / interval_minutes)
    periods_per_year = (365 * 24 * 60) / interval_minutes
    return funding_rate_percent * periods_per_year

def analyze_intrabar_orderflow(symbol):
    """
    تحليل تدفق الأوامر خلال الشمعة الحالية باستخدام ticker 24hr و taker volume.
    """
    ticker = get_ticker_24hr_one_symbol(symbol)
    if not ticker:
        return 0, [], {}
    # حجم المشتري والبائع خلال آخر 24 ساعة (يمكن أن يكون مؤشرًا)
    taker_buy_vol = float(ticker.get('takerBuyBaseVolume', 0))
    taker_sell_vol = float(ticker.get('takerSellBaseVolume', 0))
    total_taker_vol = taker_buy_vol + taker_sell_vol
    if total_taker_vol == 0:
        return 0, [], {}
    buy_ratio = taker_buy_vol / total_taker_vol
    sell_ratio = taker_sell_vol / total_taker_vol

    # حجم التداول الحالي مقابل المتوسط
    current_volume = float(ticker['volume'])
    avg_volume = get_historical_avg_volume(symbol, days=7)
    volume_spike_ratio = current_volume / avg_volume if avg_volume and avg_volume > 0 else 1.0

    # مؤشر الامتصاص: عندما يرتفع الحجم مع ثبات السعر
    price_change_1h = 0
    closes_1h = get_klines(symbol, '1h', 2)
    if closes_1h and len(closes_1h['close']) >= 2:
        price_change_1h = pct_change(closes_1h['close'][-2], closes_1h['close'][-1])

    # عدم توازن: إذا كان buy_ratio أعلى بكثير من 0.5 مع حجم مرتفع وسعر ثابت أو مرتفع قليلاً
    score = 0
    reasons = []
    if buy_ratio > 0.65 and volume_spike_ratio > 1.5:
        score += 30
        reasons.append(f"🟢 عدم توازن شراء ({buy_ratio:.1%}) بحجم {volume_spike_ratio:.1f}x")
    elif sell_ratio > 0.65 and volume_spike_ratio > 1.5:
        score += 30
        reasons.append(f"🔴 عدم توازن بيع ({sell_ratio:.1%}) بحجم {volume_spike_ratio:.1f}x")

    # امتصاص: حجم مرتفع مع تغير سعر ضئيل (أقل من 1%)
    if volume_spike_ratio > 2.0 and abs(price_change_1h) < 1.0:
        score += 20
        reasons.append(f"💊 امتصاص (حجم {volume_spike_ratio:.1f}x، سعر ثابت)")

    return score, reasons, {'buy_ratio': buy_ratio, 'sell_ratio': sell_ratio, 'volume_ratio': volume_spike_ratio}

def estimate_liquidation_levels(symbol, price, lookback_hours=24):
    """
    تقدير مستويات التصفية بناءً على الرافعة المالية.
    تعيد قائمة بمستويات السعر التي قد تسبب تصفيات كبيرة.
    """
    klines = get_klines(symbol, '1h', limit=lookback_hours)
    if not klines or len(klines['high']) < 2:
        return []
    highs = klines['high']
    lows = klines['low']
    closes = klines['close']

    # أعلى وأدنى سعر خلال الفترة
    recent_high = max(highs)
    recent_low = min(lows)

    # الرافعات الشائعة: 10x, 20x, 50x (نسبة التصفية = 1/الرافعة)
    leverages = [10, 20, 50]
    liquidation_levels = []

    # للمراكز الطويلة: سعر التصفية = سعر الدخول / (1 + 1/رافعة) (تقريباً)
    # للمراكز القصيرة: سعر التصفية = سعر الدخول / (1 - 1/رافعة)
    # نفترض أن معظم المراكز دخلت عند القمة (للطويلة) أو القاع (للقصيرة)
    for lev in leverages:
        liq_short_above = recent_high * (1 + 1/lev)  # لمن هم في مراكز قصيرة عند القمة
        liq_long_below = recent_low / (1 + 1/lev)    # لمن هم في مراكز طويلة عند القاع
        liquidation_levels.append(('SHORT', liq_short_above, lev))
        liquidation_levels.append(('LONG', liq_long_below, lev))

    # ترتيب حسب القرب من السعر الحالي
    liquidation_levels.sort(key=lambda x: abs(x[1] - price))

    # إرجاع أقرب 5 مستويات
    return liquidation_levels[:5]

def check_liquidation_clusters(symbol, price):
    """
    تقييم ما إذا كان السعر الحالي قريباً من عنقود تصفية.
    """
    levels = estimate_liquidation_levels(symbol, price)
    if not levels:
        return 0, []

    score = 0
    reasons = []
    for direction, level, lev in levels:
        distance_pct = abs(level - price) / price * 100
        if distance_pct < 2.0:  # في نطاق 2%
            score += max(0, 30 - distance_pct * 5)  # كلما اقترب زادت الدرجة
            reasons.append(f"💥 عنقود تصفية {direction} على بعد {distance_pct:.2f}% (رافعة {lev}x)")

    return min(score, 50), reasons[:3]

# =============================================================================
# دوال الفريم اليومي
# =============================================================================

def get_daily_klines(symbol, limit=30):
    return get_klines(symbol, interval='1d', limit=limit)

def get_daily_oi_history(symbol, limit=30):
    oi_hist = get_oi_history(symbol, period='4h', limit=limit*6)
    if not oi_hist:
        return []
    daily_oi = [oi_hist[i] for i in range(0, len(oi_hist), 6)]
    return daily_oi[-limit:]

def get_daily_funding_history(symbol, limit=30):
    funding_hist = get_funding_rate_history(symbol, limit=limit*3)
    if not funding_hist:
        return []
    daily_funding = []
    for i in range(0, len(funding_hist), 3):
        chunk = funding_hist[i:i+3]
        if chunk:
            daily_funding.append(sum(chunk)/len(chunk))
    return daily_funding[-limit:]

def get_daily_taker_volume(symbol):
    ticker = get_ticker_24hr_one_symbol(symbol)
    if not ticker:
        return None, None
    buy_vol = float(ticker.get('takerBuyBaseVolume', 0))
    sell_vol = float(ticker.get('takerSellBaseVolume', 0))
    return buy_vol, sell_vol

def detect_long_term_accumulation(symbol):
    try:
        daily_ratios = get_daily_top_ratio(symbol, limit=DAILY_OI_TREND_DAYS)
        if len(daily_ratios) < DAILY_OI_TREND_DAYS:
            return None
        avg_ratio = mean(daily_ratios)
        if avg_ratio < DAILY_MIN_AVG_TOP_RATIO:
            return None

        daily_oi = get_daily_oi_history(symbol, limit=DAILY_OI_TREND_DAYS)
        if len(daily_oi) < DAILY_OI_TREND_DAYS:
            return None
        oi_slope = linear_trend_slope(daily_oi)
        if oi_slope <= 0:
            return None

        funding_rate, _ = get_funding_rate(symbol)
        if funding_rate is None:
            return log_rejection(symbol, 'LONG_TERM_ACCUMULATION', 'funding_unavailable')
        funding_hist = get_funding_rate_history(symbol, limit=DAILY_FUNDING_FLIP_DAYS*3)
        funding_flip = False
        if len(funding_hist) >= 3:
            pos_before = any(f > 0 for f in funding_hist[-DAILY_FUNDING_FLIP_DAYS*3:-3])
            neg_now = funding_rate < 0
            if pos_before and neg_now:
                funding_flip = True

        buy_vol, sell_vol = get_daily_taker_volume(symbol)
        if buy_vol is None or sell_vol is None:
            return None
        if buy_vol <= sell_vol * DAILY_TAKER_VOLUME_RATIO:
            return None

        basis = get_basis(symbol)
        if abs(basis) > DAILY_BASIS_THRESHOLD:
            return None

        score = 70
        reasons = [f"تراكم طويل (نسبة كبار {avg_ratio:.1f}%)"]

        if funding_flip:
            score += 15
            reasons.append("تحول التمويل إلى سلبي")
        if funding_rate < 0:
            score += 10
            reasons.append(f"تمويل سلبي {funding_rate:+.3f}%")
        if oi_slope > 0:
            score += 10
            reasons.append("اتجاه OI إيجابي")

        price = get_mark_price(symbol)
        result = {
            'symbol': symbol,
            'score': min(score, 100),
            'pattern': 'LONG_TERM_ACCUMULATION',
            'direction': 'UP',
            'price': price,
            'reasons': reasons[:5],
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        if DEBUG:
            print(f"⚠️ خطأ في detect_long_term_accumulation لـ {symbol}: {e}")
        return None

# =============================================================================
# تقييم القوة مع دمج HMM وإدارة المخاطر
# =============================================================================
def assess_signal_quality(symbol, base_score, pattern_data):
    """تقييم حديث لجودة الإشارة مبني على البصمات المؤسسية المبكرة."""
    quality_score = float(base_score or 0.0)
    quality_reasons = []

    try:
        pattern_data = pattern_data or {}
        classification = str(pattern_data.get('classification') or pattern_data.get('pattern') or '')
        direction = str(pattern_data.get('direction') or 'UP')
        stage = str(pattern_data.get('signal_stage') or 'SETUP')
        decisive_feature = str(pattern_data.get('decisive_feature') or '')
        feature_scores = pattern_data.get('feature_scores') or {}
        reasons = pattern_data.get('reasons') or []

        class_bonus = {
            'POSITION_LED_SQUEEZE_BUILDUP': 8,
            'ACCOUNT_LED_ACCUMULATION': 8,
            'CONSENSUS_BULLISH_EXPANSION': 10,
            'FLOW_LIQUIDITY_VACUUM_BREAKOUT': 9,
        }.get(classification, 0)
        quality_score += class_bonus

        oi_score = float(feature_scores.get('oi_regime', 0.0) or 0.0)
        oi_nv_score = float(feature_scores.get('oi_notional_score', feature_scores.get('oi_notional', 0.0)) or 0.0)
        flow_score = float(feature_scores.get('taker_aggression', 0.0) or 0.0)
        div_score = float(feature_scores.get('smart_money_divergence', 0.0) or 0.0)
        acc_score = float(feature_scores.get('smart_money_accounts', 0.0) or 0.0)
        funding_score = float(feature_scores.get('funding_regime', 0.0) or 0.0)
        basis_score = float(feature_scores.get('basis_context', 0.0) or 0.0)

        quality_score += oi_score * 12 + oi_nv_score * 10 + flow_score * 12
        quality_score += div_score * 8 + acc_score * 7
        quality_score += funding_score * 4 + basis_score * 3

        if decisive_feature in {'position_led_divergence', 'account_led_divergence', 'oi_notional_change', 'taker_buy_sell_ratio'}:
            quality_score += 4

        if stage in {'SETUP', 'ARMED'}:
            quality_score += 4
        elif stage == 'TRIGGERED':
            quality_score += 2
        elif stage == 'LATE':
            quality_score -= 18

        text_blob = ' | '.join([str(x) for x in reasons]).lower()
        if 'late' in text_blob or 'crowded' in text_blob or 'متأخر' in text_blob or 'مزدحم' in text_blob:
            quality_score -= 10

        if pattern_data.get('multi_pattern_hint'):
            quality_score += 8

        # 4H supportive context bonus reflected indirectly in score via feature-based signal score; add small explicit bonus if available in reasons
        if '4h' in text_blob or 'فريم 4' in text_blob:
            quality_score += 3

        quality_score = max(0.0, min(100.0, quality_score))

        if quality_score >= 88:
            quality_level = 'انفجار وشيك جدًا'
        elif quality_score >= 75:
            quality_level = 'انفجار وشيك'
        elif quality_score >= 58:
            quality_level = 'إشارة مؤسسية'
        else:
            quality_level = 'مرشح مبكر'

        if direction == 'DOWN' and quality_level in {'انفجار وشيك', 'انفجار وشيك جدًا'}:
            quality_level = 'إشارة مؤسسية'

        quality_reasons.extend([
            f'classification={classification or "UNKNOWN"}',
            f'stage={stage}',
            f'oi={oi_score:.2f}/oinv={oi_nv_score:.2f}',
            f'flow={flow_score:.2f}',
        ])

    except Exception as e:
        if DEBUG:
            print(f"⚠️ خطأ في تقييم الجودة لـ {symbol}: {e}")
        quality_level = 'مرشح مبكر'

    return int(round(quality_score)), quality_level, quality_reasons




def calculate_risk_management(symbol, price, direction):
    """
    حساب وقف الخسارة وأهداف الربح بناءً على ATR.
    تعيد أرقامًا صالحة (0 إذا فشلت) لتجنب أخطاء None.
    """
    atr = calculate_atr(symbol, ATR_PERIOD)
    if atr is None or price is None:
        return 0.0, 0.0, 0.0
    if direction == 'UP':
        stop_loss = price - atr * ATR_MULTIPLIER_STOP
        take_profit1 = price + atr * ATR_MULTIPLIER_TAKE_PROFIT
        take_profit2 = price + atr * ATR_MULTIPLIER_TAKE_PROFIT * 2
    else:
        stop_loss = price + atr * ATR_MULTIPLIER_STOP
        take_profit1 = price - atr * ATR_MULTIPLIER_TAKE_PROFIT
        take_profit2 = price - atr * ATR_MULTIPLIER_TAKE_PROFIT * 2
    # التأكد من عدم وجود None
    return stop_loss or 0.0, take_profit1 or 0.0, take_profit2 or 0.0

# =============================================================================
# محلل HMM
# =============================================================================
class HMMMarketRegimeDetector:
    def __init__(self):
        self.model = None
        self.scaler_mean = None
        self.scaler_std = None
        self.current_regime = -1
        self.regime_names = {
            0: "🟢 هادئ (Calm)",
            1: "📈 صعودي (Bullish Trending)",
            2: "📉 هبوطي (Bearish Trending)",
            3: "⚡ متقلب (High Volatility)"
        }
        self.last_fit_time = 0

    def _prepare_features(self):
        klines = get_klines('BTCUSDT', '1h', HMM_LOOKBACK_DAYS * 24)
        if not klines or len(klines['close']) < 100:
            return None
        prices = klines['close']
        returns = [pct_change(prices[i], prices[i+1]) for i in range(len(prices)-1)]
        volatilities = []
        for i in range(24, len(returns)):
            vol = np.std(returns[i-24:i])
            volatilities.append(vol)
        min_len = min(len(returns[24:]), len(volatilities))
        if min_len < 50:
            return None
        X = np.column_stack((returns[24:24+min_len], volatilities[:min_len]))
        return X

    def fit_model(self):
        if not ENABLE_HMM or not HMM_AVAILABLE:
            return False
        X = self._prepare_features()
        if X is None or len(X) < 100:
            return False
        self.scaler_mean = np.mean(X, axis=0)
        self.scaler_std = np.std(X, axis=0)
        X_scaled = (X - self.scaler_mean) / (self.scaler_std + 1e-9)

        self.model = hmm.GaussianHMM(
            n_components=HMM_N_STATES,
            covariance_type="full",
            n_iter=1000,
            random_state=42
        )
        self.model.fit(X_scaled)
        states = self.model.predict(X_scaled)
        self.current_regime = states[-1]
        return True

    def get_current_regime(self):
        now = time.time()
        if now - self.last_fit_time >= HMM_UPDATE_INTERVAL:
            self.fit_model()
            self.last_fit_time = now
        return {
            'regime_id': self.current_regime,
            'regime_name': self.regime_names.get(self.current_regime, "غير معروف"),
            'confidence': 0.8,
        }

    def get_regime_multiplier(self):
        if not ENABLE_HMM or self.model is None:
            return 1.0
        regime = self.get_current_regime()
        if regime['regime_id'] == 0:
            return 1.0
        elif regime['regime_id'] == 1:
            return 1.2
        elif regime['regime_id'] == 2:
            return 0.6
        elif regime['regime_id'] == 3:
            return 0.8
        else:
            return 1.0

hmm_detector = HMMMarketRegimeDetector()

# =============================================================================
# نظام التعلم الذاتي (مع تحسين تقييم الاتجاه الهابط)
# =============================================================================
class LearningSystem:
    def __init__(self, db):
        self.db = db
        self.last_learning_time = datetime.now()
        self.pattern_weights = PATTERN_WEIGHTS.copy()
        self.pattern_performance = defaultdict(lambda: {'total': 0, 'success': 0, 'score_sum': 0})

    def _evaluate_change_success(self, direction, change, horizon_minutes):
        thresholds = {5: 1.0, 15: 2.0, 30: 3.0}
        threshold = thresholds.get(horizon_minutes, 2.0)
        if direction == 'UP':
            return 1 if change >= threshold else 0
        return 1 if change <= -threshold else 0

    def evaluate_signals(self):
        total_evaluated = 0
        for horizon in EVAL_HORIZONS_MINUTES:
            pending = self.db.get_pending_eval_signals(horizon, hours_ago=BACKTEST_LOOKBACK_HOURS)
            if not pending:
                continue
            for sig in pending:
                sig_id, symbol, pattern, price, direction, ts = sig
                current_price = get_mark_price(symbol)
                if current_price is None or price in (None, 0):
                    continue
                change = (current_price - price) / price * 100
                success = self._evaluate_change_success(direction, change, int(horizon))
                self.db.update_signal_evaluation(sig_id, int(horizon), current_price, success)
                if int(horizon) == 15:
                    # نبقي التعلم الأساسي معتمدًا على أفق 15 دقيقة لأنه أكثر توازنًا على الهاتف
                    with sqlite3.connect(DB_PATH) as conn:
                        conn.execute("UPDATE signals SET evaluated = 1, outcome = ?, price_after = ? WHERE id = ?",
                                     (success, current_price, sig_id))
                    self.pattern_performance[pattern]['total'] += 1
                    self.pattern_performance[pattern]['success'] += success
                    self.pattern_performance[pattern]['score_sum'] += abs(change)
                total_evaluated += 1
        return total_evaluated

    def invalidate_signals(self):
        invalidated_count = 0
        active = self.db.get_active_signals_for_invalidation(lookback_hours=BACKTEST_LOOKBACK_HOURS)
        for sig in active:
            sig_id, symbol, pattern, price, direction, ts, trigger_high, trigger_low, signal_stage = sig
            current_price = get_mark_price(symbol)
            if current_price is None:
                continue
            context = get_short_term_context(symbol, '5m', 12)
            if not context:
                continue

            reason = None
            if direction == 'UP':
                if trigger_low and current_price < trigger_low * 0.998:
                    reason = 'broke_trigger_low_after_alert'
                elif context['price_5m'] < -0.8 and context['recent_sell_ratio'] > 0.56:
                    reason = 'bearish_reversal_after_alert'
                elif context['below_ema'] and context['impulse_down'] and context['sell_accel'] > 0.02:
                    reason = 'down_impulse_invalidated_up_signal'
            else:
                if trigger_high and current_price > trigger_high * 1.002:
                    reason = 'broke_trigger_high_after_alert'
                elif context['price_5m'] > 0.8 and context['recent_buy_ratio'] > 0.56:
                    reason = 'bullish_reversal_after_alert'
                elif context['above_ema'] and context['impulse_up'] and context['buy_accel'] > 0.02:
                    reason = 'up_impulse_invalidated_down_signal'

            if reason:
                self.db.invalidate_signal(sig_id, current_price, reason)
                diagnostics.log_reject(symbol, pattern, reason, {
                    'direction': direction,
                    'current_price': current_price,
                    'trigger_high': trigger_high,
                    'trigger_low': trigger_low,
                    'signal_stage': signal_stage
                })
                invalidated_count += 1
        return invalidated_count

    def adjust_weights(self):
        if not ENABLE_LEARNING:
            return

        self.evaluate_signals()

        total_weight = 0
        new_weights = {}
        for pattern, stats in self.pattern_performance.items():
            if stats['total'] < MIN_SIGNALS_FOR_LEARNING:
                continue
            accuracy = stats['success'] / stats['total']
            if accuracy > PERFORMANCE_THRESHOLD:
                old_weight = self.pattern_weights.get(pattern, 0.2)
                new_weight = old_weight * (1 + WEIGHT_ADJUSTMENT_RATE * (accuracy - PERFORMANCE_THRESHOLD))
                new_weight = min(new_weight, 1.0)
            else:
                old_weight = self.pattern_weights.get(pattern, 0.2)
                new_weight = old_weight * (1 - WEIGHT_ADJUSTMENT_RATE * (PERFORMANCE_THRESHOLD - accuracy))
                new_weight = max(new_weight, 0.05)
            new_weights[pattern] = new_weight
            total_weight += new_weight

        if total_weight > 0:
            for pattern in new_weights:
                new_weights[pattern] /= total_weight
            self.pattern_weights.update(new_weights)
            self.save_weights()

    def save_weights(self):
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                config = json.load(f)
            config['PATTERN_WEIGHTS'] = self.pattern_weights
            with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f"⚠️ خطأ في حفظ الأوزان: {e}")

# =============================================================================
# الفلترة الأولية (سريعة) مع تحسين السيولة والارتفاعات المفاجئة
# =============================================================================
def quick_filter():
    print("🔍 جلب بيانات السوق الأولية...")
    tickers = get_ticker_24hr()
    if not tickers:
        print("❌ فشل جلب tickers")
        return []

    valid_symbol_pattern = re.compile(r'^[A-Z0-9_]+$')

    # تسريع: فلترة أولية سريعة جدًا ثم تقييم الحجم التاريخي لجزء محدود فقط
    pre_candidates = []
    for t in tickers:
        symbol = str(t.get('symbol', '')).strip().upper()
        if not symbol or not valid_symbol_pattern.match(symbol):
            continue
        if TRADING_SYMBOLS_SET and symbol not in TRADING_SYMBOLS_SET:
            continue
        if is_symbol_invalid(symbol):
            continue

        volume = safe_float(t.get('quoteVolume', 0.0), 0.0)
        price_change_24h = safe_float(t.get('priceChangePercent', 0.0), 0.0)

        if volume < MIN_VOLUME_USD:
            continue

        pre_candidates.append((symbol, volume, price_change_24h))

    # ترتيب حسب السيولة ثم قصر العدد لتسريع الدورة
    pre_candidates.sort(key=lambda x: x[1], reverse=True)
    fast_pool = pre_candidates[:max(MAX_SYMBOLS_PER_CYCLE * 3, MAX_SYMBOLS_PER_CYCLE)]

    candidates = []
    avg_vol_checks = 0
    for symbol, volume, price_change_24h in fast_pool:
        if price_change_24h >= SKYROCKET_THRESHOLD:
            candidates.append(symbol)
            continue

        vol_ok = True
        if avg_vol_checks < MAX_AVG_VOLUME_CHECKS:
            avg_vol = get_historical_avg_volume(symbol, days=7)
            avg_vol_checks += 1
            vol_ok = (avg_vol is not None and avg_vol > 0 and volume / avg_vol >= VOLUME_TO_AVG_RATIO_MIN) if avg_vol else True

        if volume >= MIN_VOLUME_24H and price_change_24h >= MIN_PRICE_CHANGE_24H and vol_ok:
            candidates.append(symbol)

        if len(candidates) >= MAX_SYMBOLS_PER_CYCLE:
            break

    print(f"✅ {len(candidates)} عملة اجتازت الفلترة الأولية")
    return candidates

# =============================================================================
# الأنماط الخمسة (محدثة باستخدام z-score) + الأنماط الجديدة للارتفاع
# =============================================================================

def bear_squeeze_pattern(symbol):
    try:
        oi_hist = get_oi_history(symbol, '4h', 2)
        if len(oi_hist) < 2:
            return None
        oi_change_4h = pct_change(oi_hist[-2], oi_hist[-1])

        if abs(oi_change_4h) < MIN_ABS_OI_CHANGE:
            return None

        funding_hist = get_funding_rate_history(symbol, 2)
        if len(funding_hist) < 2:
            return None
        funding_4h = funding_hist[-1]

        closes_4h = get_klines(symbol, '4h', 2)
        if not closes_4h or len(closes_4h['close']) < 2:
            return None
        price_change_4h = pct_change(closes_4h['close'][-2], closes_4h['close'][-1])

        _, tr_long, _ = get_top_long_short_ratio(symbol)
        tr = tr_long / (100 - tr_long) if tr_long and (100 - tr_long) != 0 else None

        oi_hist_long = get_oi_history(symbol, '4h', 6)
        oi_accel = 0
        if len(oi_hist_long) >= 6:
            mom1 = oi_hist_long[-1] - oi_hist_long[-3]
            mom2 = oi_hist_long[-3] - oi_hist_long[-6]
            oi_accel = mom1 - mom2

        score = 0
        reasons = []
        reasons_set = set()  # لتتبع الأسباب المضافة

        if funding_4h < EXTREME_FUNDING_NEGATIVE:
            score += 40
            reason = f"🔥 تمويل متطرف ({funding_4h:+.3f}%)"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)
        elif funding_4h < -0.05 or check_with_zscore(symbol, 'funding', funding_4h, FUNDING_RATE_ZSCORE):
            score += 25
            reason = f"⚠️ تمويل سلبي قوي"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)

        if oi_change_4h > 15 and oi_accel > 0:
            score += 35
            reason = f"OI قفز +{oi_change_4h:.1f}% (متسارع)"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)
        elif oi_change_4h > 10 or check_with_zscore(symbol, 'oi', oi_change_4h, OI_CHANGE_ZSCORE):
            score += 25
            reason = f"OI +{oi_change_4h:+.1f}%"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)

        if tr and tr < 1.1:
            score += 30
            reason = f"🐋 كبار يبيعون بصمت (نسبة {tr:.2f})"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)

        if price_change_4h > 2:
            score += 20
            reason = f"سعر بدأ الصعود +{price_change_4h:.1f}%"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)
        elif price_change_4h > 0.5:
            score += 10

        basis_score, basis_reasons = analyze_basis(symbol, reasons_set)
        score += basis_score
        for r in basis_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        oi_accel_score, oi_accel_reasons = analyze_oi_acceleration(symbol, reasons_set)
        score += oi_accel_score
        for r in oi_accel_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        volume_spike_score, volume_spike_reasons = analyze_volume_spike(symbol, reasons_set)
        score += volume_spike_score
        for r in volume_spike_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        if score >= 60:
            return {
                'symbol': symbol,
                'score': min(score, 100),
                'pattern': 'BEAR_SQUEEZE',
                'direction': 'DOWN',
                'price': closes_4h['close'][-1],
                'oi_change': oi_change_4h,
                'funding': funding_4h,
                'reasons': reasons[:5],
                'timestamp': datetime.now().isoformat()
            }
    except Exception as e:
        if DEBUG:
            print(f"⚠️ خطأ في bear_squeeze لـ {symbol}: {e}")
    return None

def whale_momentum_pattern(symbol):
    try:
        oi_hist = get_oi_history(symbol, '4h', 2)
        if len(oi_hist) < 2:
            return None
        oi_change_4h = pct_change(oi_hist[-2], oi_hist[-1])

        if abs(oi_change_4h) < MIN_ABS_OI_CHANGE:
            return None

        funding_hist = get_funding_rate_history(symbol, 2)
        if len(funding_hist) < 2:
            return None
        funding_4h = funding_hist[-1]

        closes_4h = get_klines(symbol, '4h', 2)
        if not closes_4h or len(closes_4h['close']) < 2:
            return None
        price_change_4h = pct_change(closes_4h['close'][-2], closes_4h['close'][-1])

        tr_ratio, tr_long, _ = get_top_long_short_ratio(symbol)
        tr = tr_ratio

        score = 0
        reasons = []
        reasons_set = set()

        if MODERATE_FUNDING_POSITIVE_MIN <= funding_4h <= MODERATE_FUNDING_POSITIVE_MAX:
            score += 25
            reason = f"💰 تمويل إيجابي معتدل ({funding_4h:+.3f}%)"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)

        if tr and tr > 2.2:
            score += 40
            reason = f"🐋 كبار يشترون بكثافة (نسبة {tr:.2f})"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)
        elif tr and tr > 1.9:
            score += 30
            reason = f"كبار يشترون بقوة"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)
        elif tr and tr > 1.6 or check_with_zscore(symbol, 'top_ratio', tr_long, TOP_TRADER_ZSCORE):
            score += 20
            reason = f"كبار يميلون للشراء"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)

        if tr:
            gap_type, gap_value = whale_retail_gap(tr, 1.0)
            if gap_type == "bullish_gap":
                score += 25
                reason = f"📊 فجوة صعودية (كبار {gap_value:.2f}x الصغار)"
                if reason not in reasons_set:
                    reasons.append(reason)
                    reasons_set.add(reason)

        # سبب OI
        if oi_change_4h > 15:
            score += 25
            reason = f"OI قوي +{oi_change_4h:.1f}%"
        elif oi_change_4h > 8 or check_with_zscore(symbol, 'oi', oi_change_4h, OI_CHANGE_ZSCORE):
            score += 15
            reason = f"OI +{oi_change_4h:+.1f}%"
        else:
            reason = None
        if reason and reason not in reasons_set:
            reasons.append(reason)
            reasons_set.add(reason)

        # سبب السعر
        if price_change_4h > 5:
            score += 20
            reason = f"سعر +{price_change_4h:.1f}%"
        elif price_change_4h > 2:
            score += 10
            reason = f"سعر +{price_change_4h:.1f}%"
        else:
            reason = None
        if reason and reason not in reasons_set:
            reasons.append(reason)
            reasons_set.add(reason)

        basis_score, basis_reasons = analyze_basis(symbol, reasons_set)
        score += basis_score
        for r in basis_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        oi_accel_score, oi_accel_reasons = analyze_oi_acceleration(symbol, reasons_set)
        score += oi_accel_score
        for r in oi_accel_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        volume_spike_score, volume_spike_reasons = analyze_volume_spike(symbol, reasons_set)
        score += volume_spike_score
        for r in volume_spike_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        if score >= 60:
            return {
                'symbol': symbol,
                'score': min(score, 100),
                'pattern': 'WHALE_MOMENTUM',
                'direction': 'UP',
                'price': closes_4h['close'][-1],
                'oi_change': oi_change_4h,
                'funding': funding_4h,
                'reasons': reasons[:5],
                'timestamp': datetime.now().isoformat()
            }
    except Exception as e:
        if DEBUG:
            print(f"⚠️ خطأ في whale_momentum لـ {symbol}: {e}")
    return None

def pre_close_squeeze_pattern(symbol):
    try:
        funding_rate, next_time = get_funding_rate(symbol)
        if funding_rate is None:
            return log_rejection(symbol, 'PRE_CLOSE_SQUEEZE', 'funding_unavailable')

        now = int(time.time() * 1000)
        time_left = (next_time - now) / (1000 * 3600) if next_time else 999

        funding_hist = get_funding_rate_history(symbol, 4)
        if len(funding_hist) < 4:
            return None

        oi_hist = get_oi_history(symbol, '1h', 2)
        if len(oi_hist) < 2:
            return None
        oi_change_1h = pct_change(oi_hist[-2], oi_hist[-1])

        if abs(oi_change_1h) < MIN_ABS_OI_CHANGE:
            return None

        klines = get_klines(symbol, interval=PRIMARY_TIMEFRAME, limit=20)
        liquidity_sweep = False
        if klines:
            liquidity_sweep = detect_liquidity_sweep(
                klines['high'], klines['low'],
                klines['close'], klines['volume'],
                klines['close'][-1]
            )

        price = get_mark_price(symbol)

        # حساب APR
        apr = calculate_apr(funding_rate)
        if apr < MIN_APR_PRE_CLOSE and funding_rate < 0.015:
            return None

        score = 0
        reasons = []
        reasons_set = set()

        if 0 < time_left <= FUNDING_COUNTDOWN_HOURS:
            score += 30
            reason = f"⏰ أقل من ساعة ({time_left:.2f}h)"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)
            if time_left < 0.25:
                score += 10
                reason = f"⚡ لحظة حرجة!"
                if reason not in reasons_set:
                    reasons.append(reason)
                    reasons_set.add(reason)

        if apr > 500:
            score += 40
            reason = f"💰 APR هائل {apr:.0f}%"
        elif apr > MIN_APR_PRE_CLOSE:
            score += 30
            reason = f"💰 APR {apr:.0f}%"
        else:
            reason = None
        if reason and reason not in reasons_set:
            reasons.append(reason)
            reasons_set.add(reason)

        if len(funding_hist) >= 3:
            funding_trend = funding_hist[-1] - funding_hist[-3]
            if funding_trend > 0.02:
                score += 25
                reason = f"📈 تمويل يتحسن بـ {funding_trend:+.3f}%"
                if reason not in reasons_set:
                    reasons.append(reason)
                    reasons_set.add(reason)

        if oi_change_1h > 10:
            score += 20
            reason = f"OI قفز +{oi_change_1h:.1f}%"
        elif oi_change_1h > 5 or check_with_zscore(symbol, 'oi', oi_change_1h, OI_CHANGE_ZSCORE):
            score += 10
            reason = f"OI +{oi_change_1h:+.1f}%"
        else:
            reason = None
        if reason and reason not in reasons_set:
            reasons.append(reason)
            reasons_set.add(reason)

        if liquidity_sweep:
            score += 15
            reason = f"🌊 مسح سيولة"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)

        basis_score, basis_reasons = analyze_basis(symbol, reasons_set)
        score += basis_score
        for r in basis_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        oi_accel_score, oi_accel_reasons = analyze_oi_acceleration(symbol, reasons_set)
        score += oi_accel_score
        for r in oi_accel_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        volume_spike_score, volume_spike_reasons = analyze_volume_spike(symbol, reasons_set)
        score += volume_spike_score
        for r in volume_spike_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        if score >= 60:
            return {
                'symbol': symbol,
                'score': min(score, 100),
                'pattern': 'PRE_CLOSE_SQUEEZE',
                'direction': 'UP',
                'price': price,
                'oi_change': oi_change_1h,
                'funding': funding_rate,
                'apr': apr,
                'reasons': reasons[:5],
                'timestamp': datetime.now().isoformat()
            }
    except Exception as e:
        if DEBUG:
            print(f"⚠️ خطأ في pre_close_squeeze لـ {symbol}: {e}")
    return None

def retail_trap_pattern(symbol):
    try:
        funding_hist = get_funding_rate_history(symbol, 2)
        if len(funding_hist) < 2:
            return None
        funding_4h = funding_hist[-1]

        closes_4h = get_klines(symbol, '4h', 2)
        if not closes_4h or len(closes_4h['close']) < 2:
            return None
        price_change_4h = pct_change(closes_4h['close'][-2], closes_4h['close'][-1])

        closes_1d = get_klines(symbol, '1d', 2)
        price_change_24h = 0
        if closes_1d and len(closes_1d['close']) >= 2:
            price_change_24h = pct_change(closes_1d['close'][-2], closes_1d['close'][-1])

        tr_ratio, tr_long, _ = get_top_long_short_ratio(symbol)
        tr = tr_ratio

        score = 0
        reasons = []
        reasons_set = set()

        if price_change_24h < -20:
            score += 50
            reason = f"💥 انهيار {price_change_24h:.1f}% في 24 ساعة"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)
        elif price_change_4h < -5:
            score += 30
            reason = f"سعر ينهار {price_change_4h:.1f}%"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)
        elif price_change_4h < -2:
            score += 15
            reason = f"سعر ينهار {price_change_4h:.1f}%"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)

        # سبب نسبة كبار
        if tr and tr > 2.5:
            score += 50
            reason = f"تجمهر خطير (نسبة {tr:.2f})"
        elif tr and tr > 2.0:
            score += 40
            reason = f"تجمهر شديد"
        elif tr and tr > 1.5:
            score += 30
            reason = f"نسبة شراء مرتفعة"
        elif tr and tr > 1.3 or check_with_zscore(symbol, 'top_ratio', tr_long, TOP_TRADER_ZSCORE):
            score += 15
            reason = f"شراء زائد"
        else:
            reason = None
        if reason and reason not in reasons_set:
            reasons.append(reason)
            reasons_set.add(reason)

        if tr:
            gap_type, gap_value = whale_retail_gap(tr, 1.0)
            if gap_type == "bearish_gap":
                score += 35
                reason = f"🐋 كبار يبيعون بينما الصغار يشترون (فجوة {gap_value:.2f})"
                if reason not in reasons_set:
                    reasons.append(reason)
                    reasons_set.add(reason)

        if funding_4h > 0.01 and price_change_4h < -2:
            score += 25
            reason = f"💰 تمويل إيجابي مع هبوط حاد"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)

        basis_score, basis_reasons = analyze_basis(symbol, reasons_set)
        if basis_score > 0 and basis_reasons:
            for r in basis_reasons:
                if r not in reasons_set:
                    reasons.append(r)
                    reasons_set.add(r)
                    score += basis_score  # يتم إضافة الـ score مرة واحدة فقط

        volume_spike_score, volume_spike_reasons = analyze_volume_spike(symbol, reasons_set)
        score += volume_spike_score
        for r in volume_spike_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        if score >= 60:
            return {
                'symbol': symbol,
                'score': min(score, 100),
                'pattern': 'RETAIL_TRAP',
                'direction': 'DOWN',
                'price': closes_4h['close'][-1],
                'funding': funding_4h,
                'reasons': reasons[:5],
                'timestamp': datetime.now().isoformat()
            }
    except Exception as e:
        if DEBUG:
            print(f"⚠️ خطأ في retail_trap لـ {symbol}: {e}")
    return None

def detect_pre_pump(symbol):
    try:
        funding_hist = get_funding_rate_history(symbol, limit=4)
        if len(funding_hist) < 4:
            return None
        low_funding = all(r < PRE_PUMP_FUNDING_MAX for r in funding_hist)
        if not low_funding:
            return None

        oi_current = get_open_interest(symbol)
        oi_hist = get_oi_history(symbol, period=OI_HISTORY_TIMEFRAME, limit=2)
        if not oi_current or len(oi_hist) < 2:
            return None
        oi_change = pct_change(oi_hist[-2], oi_current)

        if abs(oi_change) < MIN_ABS_OI_CHANGE:
            return None

        _, tr_long, _ = get_top_long_short_ratio(symbol)
        if not tr_long or tr_long < PRE_PUMP_TOP_LONG_MIN:
            return None

        current_hour = datetime.utcnow().hour
        if current_hour not in PRE_PUMP_HOURS:
            return None

        price = get_mark_price(symbol)

        score = 85
        reasons = [
            f"تمويل منخفض (<{PRE_PUMP_FUNDING_MAX}%) لفترة",
            f"OI +{oi_change:.1f}%",
            f"كبار {tr_long:.1f}%",
            f"وقت انخفاض السيولة ({current_hour}:00)"
        ]
        reasons_set = set(reasons)

        basis_score, basis_reasons = analyze_basis(symbol, reasons_set)
        score += basis_score
        for r in basis_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        oi_accel_score, oi_accel_reasons = analyze_oi_acceleration(symbol, reasons_set)
        score += oi_accel_score
        for r in oi_accel_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        volume_spike_score, volume_spike_reasons = analyze_volume_spike(symbol, reasons_set)
        score += volume_spike_score
        for r in volume_spike_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        return {
            'symbol': symbol,
            'score': min(score, 100),
            'pattern': 'PRE_PUMP',
            'direction': 'UP',
            'price': price,
            'oi_change_15': oi_change,
            'funding': funding_hist[-1],
            'top_long': tr_long,
            'reasons': reasons[:5],
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        if DEBUG:
            print(f"⚠️ خطأ في detect_pre_pump لـ {symbol}: {e}")
    return None

def detect_whale_setup(symbol):
    try:
        oi_current = get_open_interest(symbol)
        oi_hist = get_oi_history(symbol, period=OI_HISTORY_TIMEFRAME, limit=4)
        if not oi_current or len(oi_hist) < 4:
            return None

        oi_change_15 = pct_change(oi_hist[-2], oi_current) if len(oi_hist) >= 2 else 0
        oi_change_30 = pct_change(oi_hist[-3], oi_current) if len(oi_hist) >= 3 else 0

        # رفض إذا كان التغير أقل من الحد الأدنى المطلق
        if abs(oi_change_15) < MIN_ABS_OI_CHANGE:
            return None

        closes = get_klines(symbol, interval=PRIMARY_TIMEFRAME, limit=NUMBER_OF_CANDLES)
        if not closes or len(closes['close']) < NUMBER_OF_CANDLES:
            return None
        price = closes['close'][-1]
        price_1h_ago = closes['close'][0]
        price_change_1h = pct_change(price_1h_ago, price)

        _, tr_long, _ = get_top_long_short_ratio(symbol)
        funding_rate, _ = get_funding_rate(symbol)
        funding_hist = get_funding_rate_history(symbol, 2)
        funding_prev = funding_hist[-2] if len(funding_hist) >= 2 else None

        score = 0
        reasons = []
        reasons_set = set()

        if check_with_zscore(symbol, 'top_ratio', tr_long, WHALE_TOP_LONG_ZSCORE):
            score += 40
            reason = f"🐋 كبار {tr_long:.1f}%"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)
        elif tr_long and tr_long > 60:
            score += 20
            reason = f"كبار {tr_long:.1f}%"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)

        if check_with_zscore(symbol, 'oi', oi_change_15, WHALE_OI_CHANGE_ZSCORE):
            score += 35
            reason = f"📈 OI +{oi_change_15:+.1f}%"
        elif oi_change_15 > 7:
            score += 15
            reason = f"OI +{oi_change_15:+.1f}%"
        else:
            reason = None
        if reason and reason not in reasons_set:
            reasons.append(reason)
            reasons_set.add(reason)

        if funding_rate is not None and funding_rate < WHALE_FUNDING_MAX:
            score += 30
            reason = f"💰 تمويل {funding_rate:+.3f}%"
        elif funding_rate is not None and funding_rate < 0:
            score += 10
            reason = f"💰 تمويل {funding_rate:+.3f}%"
        else:
            reason = None
        if reason and reason not in reasons_set:
            reasons.append(reason)
            reasons_set.add(reason)

        if funding_prev and funding_rate and funding_prev > 0 and funding_rate < 0:
            score += 25
            reason = "🔄 تمويل تحول إلى سلبي"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)

        if oi_change_15 > oi_change_30 * 1.5 and oi_change_30 > 0:
            score += 20
            reason = "⚡ تسارع OI"
            if reason not in reasons_set:
                reasons.append(reason)
                reasons_set.add(reason)

        if price_change_1h < 5:
            score += 10
            # لا نضيف سبباً هنا

        basis_score, basis_reasons = analyze_basis(symbol, reasons_set)
        score += basis_score
        for r in basis_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        oi_accel_score, oi_accel_reasons = analyze_oi_acceleration(symbol, reasons_set)
        score += oi_accel_score
        for r in oi_accel_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        volume_spike_score, volume_spike_reasons = analyze_volume_spike(symbol, reasons_set)
        score += volume_spike_score
        for r in volume_spike_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        # إضافة تحليل التدفق وعناقيد التصفية
        flow_score, flow_reasons, _ = analyze_intrabar_orderflow(symbol)
        score += flow_score
        for r in flow_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        liq_score, liq_reasons = check_liquidation_clusters(symbol, price)
        score += liq_score
        for r in liq_reasons:
            if r not in reasons_set:
                reasons.append(r)
                reasons_set.add(r)

        if score >= 60:
            return {
                'symbol': symbol,
                'score': min(score, 100),
                'pattern': 'WHALE_SETUP',
                'direction': 'UP',
                'price': price,
                'change_1h': price_change_1h,
                'oi_change_15': oi_change_15,
                'funding': funding_rate,
                'top_long': tr_long,
                'reasons': reasons[:5],
                'timestamp': datetime.now().isoformat()
            }
    except Exception as e:
        if DEBUG:
            print(f"⚠️ خطأ في detect_whale_setup لـ {symbol}: {e}")
    return None

# =============================================================================
# أنماط جديدة للكشف عن الارتفاعات
# =============================================================================

def detect_strong_uptrend(symbol):
    """
    نمط يكشف عن ارتفاع قوي خلال آخر ساعة أو 4 ساعات.
    """
    try:
        # فحص آخر ساعة
        closes_1h = get_klines(symbol, '1h', 2)
        if closes_1h and len(closes_1h['close']) >= 2:
            change_1h = pct_change(closes_1h['close'][-2], closes_1h['close'][-1])
            if change_1h >= SKYROCKET_THRESHOLD:
                price = closes_1h['close'][-1]
                score = 80
                reasons = [f"ارتفاع قوي {change_1h:+.1f}% خلال آخر ساعة"]
                return {
                    'symbol': symbol,
                    'score': min(score, 100),
                    'pattern': 'STRONG_UPTREND',
                    'direction': 'UP',
                    'price': price,
                    'reasons': reasons,
                    'timestamp': datetime.now().isoformat()
                }

        # فحص آخر 4 ساعات
        closes_4h = get_klines(symbol, '4h', 2)
        if closes_4h and len(closes_4h['close']) >= 2:
            change_4h = pct_change(closes_4h['close'][-2], closes_4h['close'][-1])
            if change_4h >= SKYROCKET_THRESHOLD * 1.5:  # عتبة أعلى قليلاً لأربع ساعات
                price = closes_4h['close'][-1]
                score = 85
                reasons = [f"ارتفاع قوي {change_4h:+.1f}% خلال آخر 4 ساعات"]
                return {
                    'symbol': symbol,
                    'score': min(score, 100),
                    'pattern': 'STRONG_UPTREND',
                    'direction': 'UP',
                    'price': price,
                    'reasons': reasons,
                    'timestamp': datetime.now().isoformat()
                }
    except Exception as e:
        if DEBUG:
            print(f"⚠️ خطأ في detect_strong_uptrend لـ {symbol}: {e}")
    return None

def detect_skyrocket_enhanced(symbol):
    """
    كشف الارتفاع المفاجئ عبر فريمات متعددة.
    """
    try:
        for tf in SKYROCKET_TIMEFRAMES:
            closes = get_klines(symbol, interval=tf, limit=2)
            if closes and len(closes['close']) >= 2:
                change = pct_change(closes['close'][-2], closes['close'][-1])
                if change >= SKYROCKET_THRESHOLD:
                    price = closes['close'][-1]
                    reasons = [f'ارتفاع مفاجئ +{change:.1f}% خلال {tf}']
                    return {
                        'symbol': symbol,
                        'score': 100,
                        'pattern': 'SKYROCKET',
                        'direction': 'UP',
                        'price': price,
                        'reasons': reasons,
                        'timestamp': datetime.now().isoformat()
                    }
    except Exception as e:
        if DEBUG:
            print(f"⚠️ خطأ في detect_skyrocket_enhanced لـ {symbol}: {e}")
    return None

# =============================================================================
# نمط جديد: تباين الأموال الذكية (Smart Money Divergence) - الإصدار المحسن v2
# =============================================================================
def detect_smart_money_divergence(symbol):
    """
    يلتقط مرحلة التحضير قبل الانفجار:
    - OI يرتفع على 5m/15m
    - السعر يبدأ بالصعود لكن ليس انفجارًا مكتملًا بعد
    - top account ratio لا يؤكد الحركة أو يسوء
    - ضغط شراء قصير المدى قوي
    """
    try:
        ratio_shift = analyze_ratio_shift(symbol, period='5m', limit=12)
        if not ratio_shift:
            return log_rejection(symbol, 'SMART_MONEY_DIVERGENCE', 'ratio_shift_unavailable')

        kl = get_klines(symbol, interval='5m', limit=12)
        if not kl or len(kl['close']) < 12:
            return log_rejection(symbol, 'SMART_MONEY_DIVERGENCE', 'insufficient_klines')

        buy_ratios = []
        for i in range(len(kl['volume'])):
            total_vol = kl['volume'][i]
            taker_buy = kl['taker_buy_volume'][i]
            buy_ratios.append((taker_buy / total_vol) if total_vol > 0 else 0)

        if len(buy_ratios) < 6:
            return log_rejection(symbol, 'SMART_MONEY_DIVERGENCE', 'insufficient_buy_ratio_points')

        recent_buy_ratio = mean(buy_ratios[-3:])
        prev_buy_ratio = mean(buy_ratios[-6:-3])
        buy_pressure_delta = recent_buy_ratio - prev_buy_ratio

        oi_5m = get_oi_history(symbol, period='5m', limit=6)
        oi_15m = get_oi_history(symbol, period='15m', limit=4)
        if len(oi_5m) < 6 or len(oi_15m) < 4:
            return log_rejection(symbol, 'SMART_MONEY_DIVERGENCE', 'insufficient_oi_history')

        oi_change_5m = pct_change(oi_5m[-3], oi_5m[-1])
        oi_change_15m = pct_change(oi_15m[-2], oi_15m[-1])

        if oi_change_5m < 1.5 and oi_change_15m < 2.5:
            return log_rejection(symbol, 'SMART_MONEY_DIVERGENCE', 'oi_not_expanding', oi_change_5m=oi_change_5m, oi_change_15m=oi_change_15m)

        price_change_15m = pct_change(kl['close'][-4], kl['close'][-1])
        price_change_5m = pct_change(kl['close'][-2], kl['close'][-1])

        if price_change_15m < 0.8:
            return log_rejection(symbol, 'SMART_MONEY_DIVERGENCE', 'price_move_too_small', price_change_15m=price_change_15m)
        if price_change_15m > 7:
            return log_rejection(symbol, 'SMART_MONEY_DIVERGENCE', 'price_move_too_late', price_change_15m=price_change_15m)

        funding_rate, _ = get_funding_rate(symbol)
        if funding_rate is None:
            return log_rejection(symbol, 'SMART_MONEY_DIVERGENCE', 'funding_unavailable')

        crowded_long = funding_rate > 0.04
        acc_not_confirming = ratio_shift['acc_delta'] <= 0
        pos_holding = ratio_shift['pos_delta'] >= -0.03
        divergence_expanding = ratio_shift['divergence_now'] >= 0.08

        strong_buy_pressure = recent_buy_ratio >= 0.58 and buy_pressure_delta > 0.03
        oi_expanding = oi_change_5m >= 1.5 or oi_change_15m >= 3.0

        if not ((acc_not_confirming and pos_holding) or divergence_expanding):
            return log_rejection(symbol, 'SMART_MONEY_DIVERGENCE', 'no_positioning_divergence', acc_delta=ratio_shift['acc_delta'], pos_delta=ratio_shift['pos_delta'], divergence_now=ratio_shift['divergence_now'])
        if not (strong_buy_pressure and oi_expanding):
            return log_rejection(symbol, 'SMART_MONEY_DIVERGENCE', 'buy_pressure_or_oi_weak', recent_buy_ratio=recent_buy_ratio, buy_pressure_delta=buy_pressure_delta, oi_change_5m=oi_change_5m, oi_change_15m=oi_change_15m)

        score = 55
        reasons = []

        if acc_not_confirming:
            score += 12
            reasons.append(f"حسابات الكبار لا تؤكد الحركة ({ratio_shift['acc_delta']:+.3f})")

        if divergence_expanding:
            score += 10
            reasons.append(f"اتساع divergence بين position/account ({ratio_shift['divergence_now']:.3f})")

        if oi_change_5m >= 2:
            score += 10
            reasons.append(f"OI 5m +{oi_change_5m:.1f}%")

        if oi_change_15m >= 3:
            score += 8
            reasons.append(f"OI 15m +{oi_change_15m:.1f}%")

        if strong_buy_pressure:
            score += 12
            reasons.append(f"ضغط شراء قصير قوي ({recent_buy_ratio:.1%})")

        if 0.8 <= price_change_15m <= 4.5:
            score += 8
            reasons.append(f"بداية اندفاع سعري +{price_change_15m:.1f}%/15m")

        if 0 < price_change_5m <= 2.5:
            score += 4
            reasons.append(f"زخم 5m +{price_change_5m:.1f}%")

        if not crowded_long:
            score += 5
            reasons.append(f"التمويل غير مزدحم {funding_rate:+.3f}%")

        if score < 65:
            return log_rejection(symbol, 'PRE_EXPLOSION_SETUP', 'score_below_threshold', score=score)

        price = kl['close'][-1]
        return {
            'symbol': symbol,
            'score': min(score, 100),
            'pattern': 'SMART_MONEY_DIVERGENCE',
            'direction': 'UP',
            'price': price,
            'oi_change': oi_change_15m,
            'funding': funding_rate,
            'reasons': reasons[:6],
            'timestamp': datetime.now().isoformat()
        }
        log_acceptance(symbol, 'SMART_MONEY_DIVERGENCE', score=result['score'], price=result['price'], reasons=result['reasons'])
        return result

    except Exception as e:
        diagnostics.log_error(symbol, 'SMART_MONEY_DIVERGENCE', str(e))
        if DEBUG:
            print(f"⚠️ خطأ في detect_smart_money_divergence لـ {symbol}: {e}")
        return None


def detect_pre_explosion_setup(symbol):
    """التقاط الإعداد المبكر قبل الانفجار السعري."""
    try:
        kl = get_klines(symbol, '5m', 12)
        if not kl or len(kl['close']) < 12:
            return log_rejection(symbol, 'PRE_EXPLOSION_SETUP', 'insufficient_klines')

        oi_hist = get_oi_history(symbol, period='5m', limit=6)
        if len(oi_hist) < 6:
            return log_rejection(symbol, 'PRE_EXPLOSION_SETUP', 'insufficient_oi_history')

        price_15m = pct_change(kl['close'][-4], kl['close'][-1])
        price_30m = pct_change(kl['close'][-7], kl['close'][-1])
        oi_15m = pct_change(oi_hist[-3], oi_hist[-1])

        buy_ratios = []
        for i in range(len(kl['volume'])):
            vol = kl['volume'][i]
            tb = kl['taker_buy_volume'][i]
            buy_ratios.append(tb / vol if vol > 0 else 0)

        recent_buy = mean(buy_ratios[-3:])
        prev_buy = mean(buy_ratios[-6:-3])

        if price_15m < 0.8 or price_15m > 5.5:
            return log_rejection(symbol, 'PRE_EXPLOSION_SETUP', 'price_window_rejected', price_15m=price_15m)
        if oi_15m < 2.0:
            return log_rejection(symbol, 'PRE_EXPLOSION_SETUP', 'oi_too_low', oi_15m=oi_15m)
        if recent_buy < 0.56 or recent_buy <= prev_buy:
            return log_rejection(symbol, 'PRE_EXPLOSION_SETUP', 'buy_pressure_too_weak', recent_buy=recent_buy, prev_buy=prev_buy)

        funding_rate, _ = get_funding_rate(symbol)
        if funding_rate is None:
            return log_rejection(symbol, 'PRE_EXPLOSION_SETUP', 'funding_unavailable')

        score = 58
        reasons = [
            f"بداية تحرك سعري +{price_15m:.1f}%/15m",
            f"OI +{oi_15m:.1f}%/15m",
            f"ضغط شراء 5m {recent_buy:.1%}",
        ]

        if funding_rate <= 0.03:
            score += 8
            reasons.append(f"التمويل غير مزدحم {funding_rate:+.3f}%")

        ratio_shift = analyze_ratio_shift(symbol, '5m', 12)
        if ratio_shift and ratio_shift['acc_delta'] <= 0:
            score += 10
            reasons.append("حسابات الكبار لا تؤكد الصعود")

        if price_30m <= 6:
            score += 6
            reasons.append(f"الحركة ما زالت مبكرة +{price_30m:.1f}%/30m")

        if score < 65:
            return log_rejection(symbol, 'SMART_MONEY_DIVERGENCE', 'score_below_threshold', score=score)

        result = {
            'symbol': symbol,
            'score': min(score, 100),
            'pattern': 'PRE_EXPLOSION_SETUP',
            'direction': 'UP',
            'price': kl['close'][-1],
            'reasons': reasons[:6],
            'timestamp': datetime.now().isoformat()
        }
        attach_signal_stage(result, price_15m=price_15m, oi_15m=oi_15m, funding_rate=funding_rate, buy_ratio=recent_buy)
        log_acceptance(symbol, 'PRE_EXPLOSION_SETUP', score=result['score'], price=result['price'], reasons=result['reasons'], stage=result.get('signal_stage'))
        return result

    except Exception as e:
        diagnostics.log_error(symbol, 'PRE_EXPLOSION_SETUP', str(e))
        if DEBUG:
            print(f"⚠️ خطأ في detect_pre_explosion_setup لـ {symbol}: {e}")
        return None



def detect_position_led_squeeze_buildup(symbol):
    """التقاط حالات Position-led squeeze build-up قبل اندفاع الجمهور."""
    try:
        kl = get_klines(symbol, interval='5m', limit=12)
        if not kl or len(kl['close']) < 12:
            return log_rejection(symbol, 'POSITION_LED_SQUEEZE_BUILDUP', 'insufficient_klines')
        buy_ratios = compute_taker_buy_ratio(kl)
        if len(buy_ratios) < 6:
            return log_rejection(symbol, 'POSITION_LED_SQUEEZE_BUILDUP', 'insufficient_buy_ratio_points')
        oi_5m_hist = get_oi_history(symbol, period='5m', limit=6)
        oi_15m_hist = get_oi_history(symbol, period='15m', limit=4)
        oi_notional_hist = get_oi_notional_history(symbol, period='5m', limit=6)
        if len(oi_5m_hist) < 6 or len(oi_15m_hist) < 4:
            return log_rejection(symbol, 'POSITION_LED_SQUEEZE_BUILDUP', 'insufficient_oi_history')
        position_snapshot = get_top_position_snapshot(symbol, period='5m')
        account_snapshot = get_top_account_snapshot(symbol, period='5m')
        crowd_snapshot = get_global_long_short_snapshot(symbol, period='5m')
        if not position_snapshot or not account_snapshot:
            return log_rejection(symbol, 'POSITION_LED_SQUEEZE_BUILDUP', 'snapshot_unavailable')

        recent_buy = rolling_mean(buy_ratios[-3:])
        prev_buy = rolling_mean(buy_ratios[-6:-3])
        buy_accel = recent_buy - prev_buy
        taker_ratio = safe_buy_sell_ratio_from_buy_ratio(recent_buy)
        taker_score = taker_flow_score(taker_ratio)
        oi_change_5m = pct_change(oi_5m_hist[-3], oi_5m_hist[-1])
        oi_change_15m = pct_change(oi_15m_hist[-2], oi_15m_hist[-1])
        oi_notional_change = pct_change(oi_notional_hist[-3], oi_notional_hist[-1]) if len(oi_notional_hist) >= 3 else 0.0
        price_change_5m = pct_change(kl['close'][-2], kl['close'][-1])
        price_change_15m = pct_change(kl['close'][-4], kl['close'][-1])
        ls_ratio = crowd_snapshot['ratio'] if crowd_snapshot else account_snapshot['ratio']
        top_position_long_pct = float(position_snapshot.get('long_pct', 0.0) or 0.0)
        top_account_long_pct = float(account_snapshot.get('long_pct', 0.0) or 0.0)
        position_led_divergence = top_position_long_pct - top_account_long_pct
        account_led_divergence = top_account_long_pct - top_position_long_pct
        funding_rate, _ = get_funding_rate(symbol)
        if funding_rate is None:
            return log_rejection(symbol, 'POSITION_LED_SQUEEZE_BUILDUP', 'funding_unavailable')
        funding_trend = get_funding_trend(symbol, limit=6)
        funding_score_val = funding_regime_score(funding_rate, funding_trend)
        basis = get_basis(symbol)

        if position_snapshot['ratio'] < 1.20:
            return log_rejection(symbol, 'POSITION_LED_SQUEEZE_BUILDUP', 'position_ratio_too_low', position_ratio=position_snapshot['ratio'])
        if ls_ratio > 0.98:
            return log_rejection(symbol, 'POSITION_LED_SQUEEZE_BUILDUP', 'ls_ratio_not_weak', ls_ratio=ls_ratio)
        if position_led_divergence < 12:
            return log_rejection(symbol, 'POSITION_LED_SQUEEZE_BUILDUP', 'position_led_divergence_too_small', position_led_divergence=position_led_divergence)
        if oi_change_15m < 2.0 or price_change_15m < 1.0:
            return log_rejection(symbol, 'POSITION_LED_SQUEEZE_BUILDUP', 'oi_or_price_not_ready', oi_change_15m=oi_change_15m, price_change_15m=price_change_15m)
        if abs(funding_rate) > 0.08:
            return log_rejection(symbol, 'POSITION_LED_SQUEEZE_BUILDUP', 'funding_too_crowded', funding_rate=funding_rate)

        oi_score = oi_regime_score(oi_change_5m, oi_change_15m)
        oi_nv_score = oi_notional_score(oi_notional_change)
        basis_score = basis_context_score(basis, oi_change_15m, taker_score)
        compression_score = min(1.0, max(0.0, (1.0 - ls_ratio) * 2.2))
        divergence_score = smart_money_divergence_feature_score(position_led_divergence)
        account_score = top_account_score(account_snapshot['ratio'], top_account_long_pct)
        feature_scores = {
            'oi_regime': round(oi_score, 3),
            'compression': round(compression_score, 3),
            'taker_aggression': round(taker_score, 3),
            'cvd_flow': round(max(0.0, min(1.0, (recent_buy - 0.5) * 2.5)), 3),
            'liquidity_vacuum': round(max(0.0, min(1.0, (1.0 - ls_ratio) * 1.8)), 3),
            'smart_money_accounts': round(account_score, 3),
            'smart_money_divergence': round(divergence_score, 3),
            'funding_regime': round(funding_score_val, 3),
            'basis_context': round(basis_score, 3),
            'oi_notional_score': round(oi_nv_score, 3)
        }
        score = int(50 + weighted_feature_score(feature_scores) * 40 + oi_nv_score * 8 + compute_four_h_context_bonus(symbol) * 10)
        reasons = [
            f"Position Ratio {position_snapshot['ratio']:.2f}",
            f"Account Ratio {account_snapshot['ratio']:.2f} (انخفاض الحسابات جزء من البصمة)",
            f"position_led_divergence {position_led_divergence:+.2f}pt",
            f"account_led_divergence {account_led_divergence:+.2f}pt",
            f"L/S ratio ضعيف {ls_ratio:.2f}",
            f"OI 5m {oi_change_5m:+.1f}% | OI 15m {oi_change_15m:+.1f}%",
            f"OI Notional {oi_notional_change:+.1f}%",
            f"Taker buy/sell {taker_ratio:.2f}x | buy accel {buy_accel:+.2%}",
        ]
        if basis is not None:
            reasons.append(f"Basis سياقي {basis:+.3f}%")
        classification, decisive_feature = classify_bullish_signal({
            'position_ratio': position_snapshot['ratio'], 'account_ratio': account_snapshot['ratio'], 'ls_ratio': ls_ratio,
            'top_position_long_pct': top_position_long_pct, 'top_account_long_pct': top_account_long_pct,
            'oi_change_15m': oi_change_15m, 'price_change_15m': price_change_15m, 'price_change_5m': price_change_5m,
            'funding_rate': funding_rate, 'basis': basis, 'taker_buy_ratio_recent': recent_buy,
            'trade_count_zscore': trade_count_zscore(kl, lookback=min(10, len(kl['trades']))),
            'oi_notional_strong': oi_nv_score >= 0.8
        })
        early_ok, early_reason = pre_pump_early_filter(price_5m=price_change_5m, price_15m=price_change_15m, funding_rate=funding_rate, oi_15m=oi_change_15m if 'oi_change_15m' in locals() else 0.0)
        if not early_ok:
            return log_rejection(symbol, 'POSITION_LED_SQUEEZE_BUILDUP', early_reason, price_change_5m=price_change_5m, price_change_15m=price_change_15m, funding_rate=funding_rate)

        result = {'symbol': symbol, 'score': min(score, 100), 'pattern': 'POSITION_LED_SQUEEZE_BUILDUP', 'classification': classification, 'decisive_feature': decisive_feature, 'direction': 'UP', 'price': kl['close'][-1], 'reasons': reasons[:8], 'funding': funding_rate, 'timestamp': datetime.now().isoformat(), 'feature_scores': feature_scores}
        attach_signal_stage(result, price_change_5m, price_change_15m, oi_change_5m, oi_change_15m, funding_rate, recent_buy)
        apply_time_window_boost(result)
        log_acceptance(symbol, 'POSITION_LED_SQUEEZE_BUILDUP', score=result['score'], price=result['price'], reasons=result['reasons'], stage=result.get('signal_stage'), classification=result.get('classification'), decisive_feature=result.get('decisive_feature'), feature_scores=result['feature_scores'], position_led_divergence=position_led_divergence, account_led_divergence=account_led_divergence, taker_buy_sell_ratio=taker_ratio, oi_notional_change=oi_notional_change)
        return result
    except Exception as e:
        diagnostics.log_error(symbol, 'POSITION_LED_SQUEEZE_BUILDUP', str(e))
        return None

def detect_pre_squeeze_up(symbol):
    """التقاط نموذج Smart Divergence / Short Squeeze المبكر."""
    try:
        ratio_shift = analyze_ratio_shift(symbol, period='5m', limit=12)
        if not ratio_shift:
            return log_rejection(symbol, 'SMART_DIVERGENCE_SQUEEZE', 'ratio_shift_unavailable')
        kl = get_klines(symbol, interval='5m', limit=12)
        if not kl or len(kl['close']) < 12:
            return log_rejection(symbol, 'SMART_DIVERGENCE_SQUEEZE', 'insufficient_klines')

        buy_ratios = compute_taker_buy_ratio(kl)
        if len(buy_ratios) < 6:
            return log_rejection(symbol, 'SMART_DIVERGENCE_SQUEEZE', 'insufficient_buy_ratio_points')

        recent_buy = rolling_mean(buy_ratios[-3:])
        prev_buy = rolling_mean(buy_ratios[-6:-3])
        buy_accel = recent_buy - prev_buy
        taker_ratio = safe_buy_sell_ratio_from_buy_ratio(recent_buy)
        taker_score = taker_flow_score(taker_ratio)

        oi_5m_hist = get_oi_history(symbol, period='5m', limit=6)
        oi_15m_hist = get_oi_history(symbol, period='15m', limit=4)
        if len(oi_5m_hist) < 6 or len(oi_15m_hist) < 4:
            return log_rejection(symbol, 'SMART_DIVERGENCE_SQUEEZE', 'insufficient_oi_history')

        oi_change_5m = pct_change(oi_5m_hist[-3], oi_5m_hist[-1])
        oi_change_15m = pct_change(oi_15m_hist[-2], oi_15m_hist[-1])
        price_change_5m = pct_change(kl['close'][-2], kl['close'][-1])
        price_change_15m = pct_change(kl['close'][-4], kl['close'][-1])

        position_snapshot = get_top_position_snapshot(symbol, period='5m')
        account_snapshot = get_top_account_snapshot(symbol, period='5m')
        crowd_snapshot = get_global_long_short_snapshot(symbol, period='5m')
        if not position_snapshot or not account_snapshot:
            return log_rejection(symbol, 'SMART_DIVERGENCE_SQUEEZE', 'snapshot_unavailable')

        ls_ratio = crowd_snapshot['ratio'] if crowd_snapshot else account_snapshot['ratio']
        ratio_divergence = smart_divergence_score(position_snapshot['ratio'], account_snapshot['ratio'])
        long_divergence = compute_smart_money_divergence(account_snapshot.get('long_pct', 0.0), position_snapshot.get('long_pct', 0.0))
        divergence_score = smart_money_divergence_feature_score(long_divergence)
        account_score_val = top_account_score(account_snapshot['ratio'], account_snapshot.get('long_pct', 0.0))
        funding_rate, _ = get_funding_rate(symbol)
        funding_trend = get_funding_trend(symbol, limit=6)
        funding_score_val = funding_regime_score(funding_rate, funding_trend)
        if funding_rate is None:
            return log_rejection(symbol, 'SMART_DIVERGENCE_SQUEEZE', 'funding_unavailable')

        if price_change_15m < min(SMART_DIVERGENCE_MIN_PRICE_15M, POSITION_LED_SQUEEZE_MIN_PRICE_15M):
            return log_rejection(symbol, 'SMART_DIVERGENCE_SQUEEZE', 'price_move_too_small', price_change_15m=price_change_15m)
        if oi_change_15m < min(SMART_DIVERGENCE_MIN_OI_15M, POSITION_LED_SQUEEZE_MIN_OI_15M):
            return log_rejection(symbol, 'SMART_DIVERGENCE_SQUEEZE', 'oi_not_expanding_enough', oi_change_15m=oi_change_15m)
        if position_snapshot['ratio'] < min(SMART_DIVERGENCE_MIN_POSITION_RATIO, POSITION_LED_SQUEEZE_MIN_POSITION_RATIO):
            return log_rejection(symbol, 'SMART_DIVERGENCE_SQUEEZE', 'position_ratio_too_low', position_ratio=position_snapshot['ratio'])
        if account_snapshot['ratio'] > SMART_DIVERGENCE_MAX_ACCOUNT_RATIO and ls_ratio > SMART_DIVERGENCE_MAX_LS_RATIO and long_divergence < 8.0:
            return log_rejection(symbol, 'SMART_DIVERGENCE_SQUEEZE', 'account_and_ls_too_bullish_for_divergence', account_ratio=account_snapshot['ratio'], ls_ratio=ls_ratio, smart_money_divergence=long_divergence)
        dynamic_buy_floor = SMART_DIVERGENCE_MIN_BUY_RATIO
        if funding_rate is not None and funding_rate < -0.05 and long_divergence >= POSITION_LED_SQUEEZE_MIN_DIVERGENCE and oi_change_15m >= POSITION_LED_SQUEEZE_MIN_OI_15M:
            dynamic_buy_floor = min(dynamic_buy_floor, POSITION_LED_SQUEEZE_MIN_BUY_RATIO)
        if recent_buy < dynamic_buy_floor:
            return log_rejection(symbol, 'SMART_DIVERGENCE_SQUEEZE', 'buy_pressure_too_weak', recent_buy=recent_buy, taker_buy_sell_ratio=taker_ratio, dynamic_buy_floor=dynamic_buy_floor)
        if divergence_score <= 0.0 and ratio_divergence <= 0.20 and long_divergence < POSITION_LED_SQUEEZE_MIN_DIVERGENCE:
            return log_rejection(symbol, 'SMART_DIVERGENCE_SQUEEZE', 'divergence_too_small', divergence=ratio_divergence, smart_money_divergence=long_divergence)

        score = 52
        score += int(account_score_val * 12)
        score += int(divergence_score * 14)
        score += int(taker_score * 14)
        score += int(min(1.0, max(0.0, oi_change_15m / 6.0)) * 10)
        score += int(funding_score_val * 8)
        reasons = [
            f"Account Ratio {account_snapshot['ratio']:.2f}",
            f"Account Long {account_snapshot.get('long_pct', 0.0):.2f}%",
            f"Position Ratio {position_snapshot['ratio']:.2f}",
            f"Position Long {position_snapshot.get('long_pct', 0.0):.2f}%",
            f"Position-led divergence {long_divergence:+.2f}pt",
            f"Taker buy/sell {taker_ratio:.2f}x",
            f"OI 15m +{oi_change_15m:.1f}%",
            f"Funding regime {funding_rate:+.3f}% / trend {funding_trend:+.3f}",
        ]
        if buy_accel >= 0.03:
            score += 6
            reasons.append(f"تسارع شراء +{buy_accel:.1%}")
        if ratio_shift['acc_delta'] <= 0:
            score += 4
            reasons.append(f"Account delta {ratio_shift['acc_delta']:+.2f}")
        if crowd_snapshot and crowd_snapshot['ratio'] < 1.0:
            score += 4
            reasons.append(f"Crowd ratio {crowd_snapshot['ratio']:.2f}")
        if position_snapshot['ratio'] >= POSITION_LED_SQUEEZE_MIN_POSITION_RATIO and account_snapshot['ratio'] <= POSITION_LED_SQUEEZE_MAX_ACCOUNT_RATIO:
            score += 6
            reasons.append("Position bullish / accounts short = squeeze fuel")
        if ls_ratio <= POSITION_LED_SQUEEZE_MAX_LS_RATIO:
            score += 4
            reasons.append(f"L/S ratio bearish {ls_ratio:.2f}")

        classification, decisive_feature = classify_bullish_signal({
            'funding_rate': funding_rate,
            'account_score': account_score_val,
            'divergence_score': divergence_score,
            'taker_score': taker_score,
            'position_ratio': position_snapshot['ratio'],
            'account_ratio': account_snapshot['ratio'],
            'ls_ratio': ls_ratio,
            'oi_change_15m': oi_change_15m,
        })

        result = {
            'symbol': symbol,
            'score': min(score, 100),
            'pattern': 'SMART_DIVERGENCE_SQUEEZE',
            'classification': classification,
            'decisive_feature': decisive_feature,
            'direction': 'UP',
            'price': kl['close'][-1],
            'reasons': reasons[:8],
            'funding': funding_rate,
            'timestamp': datetime.now().isoformat(),
            'feature_scores': {
                'top_account_ratio': round(account_score_val, 3),
                'smart_money_divergence': round(divergence_score, 3),
                'taker_aggression': round(taker_score, 3),
                'funding_regime': round(funding_score_val, 3),
            }
        }
        attach_signal_stage(result, price_change_5m, price_change_15m, oi_change_5m, oi_change_15m, funding_rate, recent_buy)
        apply_time_window_boost(result)
        log_acceptance(symbol, 'SMART_DIVERGENCE_SQUEEZE', score=result['score'], price=result['price'], reasons=result['reasons'], stage=result.get('signal_stage'), classification=classification, decisive_feature=decisive_feature, feature_scores=result['feature_scores'], smart_money_divergence=long_divergence, taker_buy_sell_ratio=taker_ratio)
        return result
    except Exception as e:
        diagnostics.log_error(symbol, 'SMART_DIVERGENCE_SQUEEZE', str(e))
        return None

def detect_consensus_bullish_expansion(symbol):
    """اتفاق صاعد بين الحسابات والمراكز والتدفق."""
    try:
        kl = get_klines(symbol, interval='5m', limit=12)
        if not kl or len(kl['close']) < 12:
            return log_rejection(symbol, 'CONSENSUS_BULLISH_EXPANSION', 'insufficient_klines')
        oi_5m_hist = get_oi_history(symbol, period='5m', limit=6)
        oi_15m_hist = get_oi_history(symbol, period='15m', limit=4)
        if len(oi_15m_hist) < 4:
            return log_rejection(symbol, 'CONSENSUS_BULLISH_EXPANSION', 'insufficient_oi_history')
        position_snapshot = get_top_position_snapshot(symbol, period='5m')
        account_snapshot = get_top_account_snapshot(symbol, period='5m')
        crowd_snapshot = get_global_long_short_snapshot(symbol, period='5m')
        if not position_snapshot or not account_snapshot:
            return log_rejection(symbol, 'CONSENSUS_BULLISH_EXPANSION', 'snapshot_unavailable')
        buy_ratios = compute_taker_buy_ratio(kl)
        recent_buy = rolling_mean(buy_ratios[-3:]) if len(buy_ratios) >= 3 else 0.0
        taker_ratio = safe_buy_sell_ratio_from_buy_ratio(recent_buy)
        taker_score = taker_flow_score(taker_ratio)
        oi_change_5m = pct_change(oi_5m_hist[-3], oi_5m_hist[-1]) if len(oi_5m_hist) >= 3 else 0.0
        oi_change_15m = pct_change(oi_15m_hist[-2], oi_15m_hist[-1])
        price_change_5m = pct_change(kl['close'][-2], kl['close'][-1])
        price_change_15m = pct_change(kl['close'][-4], kl['close'][-1])
        ls_ratio = crowd_snapshot['ratio'] if crowd_snapshot else account_snapshot['ratio']
        funding_rate, _ = get_funding_rate(symbol)
        if funding_rate is None:
            return log_rejection(symbol, 'CONSENSUS_BULLISH_EXPANSION', 'funding_unavailable')
        if position_snapshot['ratio'] < 1.8 or account_snapshot['ratio'] < 1.2 or ls_ratio < 1.2:
            return log_rejection(symbol, 'CONSENSUS_BULLISH_EXPANSION', 'ratios_below_consensus', position_ratio=position_snapshot['ratio'], account_ratio=account_snapshot['ratio'], ls_ratio=ls_ratio)
        if oi_change_15m < 1.5 or price_change_15m < 1.0:
            return log_rejection(symbol, 'CONSENSUS_BULLISH_EXPANSION', 'oi_or_price_too_small', oi_change_15m=oi_change_15m, price_change_15m=price_change_15m)
        if funding_rate > 0.02:
            return log_rejection(symbol, 'CONSENSUS_BULLISH_EXPANSION', 'funding_crowded', funding_rate=funding_rate)
        funding_score_val = funding_regime_score(funding_rate, get_funding_trend(symbol, limit=6))
        basis = get_basis(symbol)
        account_score = top_account_score(account_snapshot['ratio'], account_snapshot.get('long_pct', 0.0))
        feature_scores = {
            'oi_regime': round(oi_regime_score(oi_change_5m, oi_change_15m), 3),
            'compression': round(max(0.0, min(1.0, (ls_ratio - 1.0) / 0.7)), 3),
            'taker_aggression': round(taker_score, 3),
            'cvd_flow': round(max(0.0, min(1.0, (recent_buy - 0.5) * 2.0)), 3),
            'liquidity_vacuum': round(max(0.0, min(1.0, trade_count_zscore(kl, lookback=min(10, len(kl['trades']))) / 3.0)), 3),
            'smart_money_accounts': round(account_score, 3),
            'smart_money_divergence': round(max(0.0, 1.0 - abs(position_snapshot.get('long_pct',0.0)-account_snapshot.get('long_pct',0.0))/30.0), 3),
            'funding_regime': round(funding_score_val, 3),
            'basis_context': round(basis_context_score(basis, oi_change_15m, taker_score), 3),
        }
        score = int(52 + weighted_feature_score(feature_scores) * 42 + compute_four_h_context_bonus(symbol) * 10)
        reasons = [f"Position Ratio {position_snapshot['ratio']:.2f}", f"Account Ratio {account_snapshot['ratio']:.2f}", f"L/S {ls_ratio:.2f}", f"OI 15m +{oi_change_15m:.1f}%", f"سعر 15m +{price_change_15m:.1f}%", f"Taker buy/sell {taker_ratio:.2f}x"]
        classification, decisive_feature = classify_bullish_signal({'position_ratio': position_snapshot['ratio'], 'account_ratio': account_snapshot['ratio'], 'ls_ratio': ls_ratio, 'top_position_long_pct': position_snapshot.get('long_pct', 0.0), 'top_account_long_pct': account_snapshot.get('long_pct', 0.0), 'oi_change_15m': oi_change_15m, 'price_change_15m': price_change_15m, 'price_change_5m': price_change_5m, 'funding_rate': funding_rate, 'taker_buy_ratio_recent': recent_buy, 'trade_count_zscore': trade_count_zscore(kl, lookback=min(10, len(kl['trades']))), 'basis': basis})
        early_ok, early_reason = pre_pump_early_filter(price_5m=price_change_5m, price_15m=price_change_15m, funding_rate=funding_rate, oi_15m=oi_change_15m if 'oi_change_15m' in locals() else 0.0)
        if not early_ok:
            return log_rejection(symbol, 'CONSENSUS_BULLISH_EXPANSION', early_reason, price_change_5m=price_change_5m, price_change_15m=price_change_15m, funding_rate=funding_rate)

        result = {'symbol': symbol, 'score': min(score, 100), 'pattern': 'CONSENSUS_BULLISH_EXPANSION', 'classification': classification, 'decisive_feature': decisive_feature, 'direction': 'UP', 'price': kl['close'][-1], 'reasons': reasons[:8], 'funding': funding_rate, 'timestamp': datetime.now().isoformat(), 'feature_scores': feature_scores}
        attach_signal_stage(result, price_change_5m, price_change_15m, oi_change_5m, oi_change_15m, funding_rate, recent_buy)
        apply_time_window_boost(result)
        log_acceptance(symbol, 'CONSENSUS_BULLISH_EXPANSION', score=result['score'], price=result['price'], reasons=result['reasons'], stage=result.get('signal_stage'), classification=classification, decisive_feature=decisive_feature, feature_scores=result['feature_scores'])
        return result
    except Exception as e:
        diagnostics.log_error(symbol, 'CONSENSUS_BULLISH_EXPANSION', str(e))
        return None

def detect_flow_breakout(symbol):
    """اختراق تدفقي مع سيولة رقيقة وتمويل غير مزدحم."""
    try:
        kl = get_klines(symbol, interval='5m', limit=12)
        if not kl or len(kl['close']) < 12:
            return log_rejection(symbol, 'FLOW_LIQUIDITY_VACUUM_BREAKOUT', 'insufficient_klines')
        buy_ratios = compute_taker_buy_ratio(kl)
        if len(buy_ratios) < 6:
            return log_rejection(symbol, 'FLOW_LIQUIDITY_VACUUM_BREAKOUT', 'insufficient_buy_ratio_points')
        price_change_5m = pct_change(kl['close'][-2], kl['close'][-1])
        price_change_15m = pct_change(kl['close'][-4], kl['close'][-1])
        recent_high = max(kl['high'][-7:-1])
        local_high_break = kl['close'][-1] > recent_high
        recent_buy = rolling_mean(buy_ratios[-3:])
        taker_ratio = safe_buy_sell_ratio_from_buy_ratio(recent_buy)
        taker_score = taker_flow_score(taker_ratio)
        trade_z = trade_count_zscore(kl, lookback=min(10, len(kl['trades'])))
        funding_rate, _ = get_funding_rate(symbol)
        if funding_rate is None:
            return log_rejection(symbol, 'FLOW_LIQUIDITY_VACUUM_BREAKOUT', 'funding_unavailable')
        if abs(funding_rate) > 0.02:
            return log_rejection(symbol, 'FLOW_LIQUIDITY_VACUUM_BREAKOUT', 'funding_not_neutral', funding_rate=funding_rate)
        if price_change_5m < 1.2 or not local_high_break:
            return log_rejection(symbol, 'FLOW_LIQUIDITY_VACUUM_BREAKOUT', 'breakout_not_confirmed', price_change_5m=price_change_5m, local_high_break=local_high_break)
        if trade_z < 2.0 or recent_buy < 0.60:
            return log_rejection(symbol, 'FLOW_LIQUIDITY_VACUUM_BREAKOUT', 'flow_not_strong_enough', trade_count_zscore=trade_z, taker_buy_ratio_recent=recent_buy)
        funding_score_val = funding_regime_score(funding_rate, get_funding_trend(symbol, limit=6))
        feature_scores = {
            'oi_regime': 0.0,
            'compression': 0.0,
            'taker_aggression': round(taker_score, 3),
            'cvd_flow': round(max(0.0, min(1.0, (recent_buy - 0.5) * 2.8)), 3),
            'liquidity_vacuum': round(max(0.0, min(1.0, trade_z / 3.0)), 3),
            'smart_money_accounts': 0.0,
            'smart_money_divergence': 0.0,
            'funding_regime': round(funding_score_val, 3),
            'basis_context': round(0.5, 3),
        }
        score = int(54 + weighted_feature_score(feature_scores) * 40 + compute_four_h_context_bonus(symbol) * 8)
        reasons = [f"اختراق سعري واضح فوق {recent_high:.6f}", f"price_change_5m +{price_change_5m:.1f}%", f"trade_count_zscore {trade_z:.2f}", f"taker_buy_ratio_recent {recent_buy:.1%}", f"Taker buy/sell {taker_ratio:.2f}x", f"Funding non-crowded {funding_rate:+.3f}%"]
        classification, decisive_feature = classify_bullish_signal({'price_change_5m': price_change_5m, 'trade_count_zscore': trade_z, 'taker_buy_ratio_recent': recent_buy, 'funding_rate': funding_rate})
        early_ok, early_reason = pre_pump_early_filter(price_5m=price_change_5m, price_15m=price_change_15m, funding_rate=funding_rate, oi_15m=oi_change_15m if 'oi_change_15m' in locals() else 0.0)
        if not early_ok:
            return log_rejection(symbol, 'FLOW_LIQUIDITY_VACUUM_BREAKOUT', early_reason, price_change_5m=price_change_5m, price_change_15m=price_change_15m, funding_rate=funding_rate)

        result = {'symbol': symbol, 'score': min(score, 100), 'pattern': 'FLOW_LIQUIDITY_VACUUM_BREAKOUT', 'classification': classification, 'decisive_feature': decisive_feature, 'direction': 'UP', 'price': kl['close'][-1], 'reasons': reasons[:8], 'funding': funding_rate, 'timestamp': datetime.now().isoformat(), 'feature_scores': feature_scores}
        attach_signal_stage(result, price_change_5m, price_change_15m, 0.0, 0.0, funding_rate, recent_buy)
        apply_time_window_boost(result)
        log_acceptance(symbol, 'FLOW_LIQUIDITY_VACUUM_BREAKOUT', score=result['score'], price=result['price'], reasons=result['reasons'], stage=result.get('signal_stage'), classification=classification, decisive_feature=decisive_feature, feature_scores=result['feature_scores'], taker_buy_sell_ratio=taker_ratio)
        return result
    except Exception as e:
        diagnostics.log_error(symbol, 'FLOW_LIQUIDITY_VACUUM_BREAKOUT', str(e))
        return None

def detect_oi_expansion_pump(symbol):
    """استمرار صاعد مدفوع بتمدد OI والقيمة الاسمية له مع تخفيف وزن position ratio."""
    try:
        kl = get_klines(symbol, interval='5m', limit=12)
        if not kl or len(kl['close']) < 12:
            return log_rejection(symbol, 'OI_EXPANSION_CONTINUATION', 'insufficient_klines')
        oi_hist_5m = get_oi_history(symbol, period='5m', limit=6)
        oi_hist_15m = get_oi_history(symbol, period='15m', limit=4)
        oi_notional_hist = get_oi_notional_history(symbol, period='5m', limit=6)
        if len(oi_hist_5m) < 6 or len(oi_hist_15m) < 4:
            return log_rejection(symbol, 'OI_EXPANSION_CONTINUATION', 'insufficient_oi_history')
        buy_ratios = compute_taker_buy_ratio(kl)
        if len(buy_ratios) < 6:
            return log_rejection(symbol, 'OI_EXPANSION_CONTINUATION', 'insufficient_buy_ratio_points')

        recent_buy = rolling_mean(buy_ratios[-3:])
        taker_ratio = safe_buy_sell_ratio_from_buy_ratio(recent_buy)
        taker_score = taker_flow_score(taker_ratio)
        price_change_5m = pct_change(kl['close'][-2], kl['close'][-1])
        price_change_15m = pct_change(kl['close'][-4], kl['close'][-1])
        oi_change_5m = pct_change(oi_hist_5m[-3], oi_hist_5m[-1])
        oi_change_15m = pct_change(oi_hist_15m[-2], oi_hist_15m[-1])
        oi_notional_change = pct_change(oi_notional_hist[-2], oi_notional_hist[-1]) if len(oi_notional_hist) >= 2 and oi_notional_hist[-2] > 0 else 0.0
        position_snapshot = get_top_position_snapshot(symbol, period='5m')
        account_snapshot = get_top_account_snapshot(symbol, period='5m')
        if not position_snapshot:
            return log_rejection(symbol, 'OI_EXPANSION_CONTINUATION', 'position_snapshot_unavailable')
        funding_rate, _ = get_funding_rate(symbol)
        funding_trend = get_funding_trend(symbol, limit=6)
        funding_score_val = funding_regime_score(funding_rate, funding_trend)
        if funding_rate is None:
            return log_rejection(symbol, 'OI_EXPANSION_CONTINUATION', 'funding_unavailable')

        if price_change_15m < OI_EXPANSION_MIN_PRICE_15M:
            return log_rejection(symbol, 'OI_EXPANSION_CONTINUATION', 'price_move_too_small', price_change_15m=price_change_15m)
        if oi_change_15m < OI_EXPANSION_MIN_OI_15M:
            return log_rejection(symbol, 'OI_EXPANSION_CONTINUATION', 'oi_change_too_small', oi_change_15m=oi_change_15m)
        if recent_buy < OI_EXPANSION_MIN_BUY_RATIO:
            return log_rejection(symbol, 'OI_EXPANSION_CONTINUATION', 'buy_ratio_too_low', recent_buy=recent_buy, taker_buy_sell_ratio=taker_ratio)
        if position_snapshot['ratio'] < OI_EXPANSION_MIN_POSITION_RATIO:
            return log_rejection(symbol, 'OI_EXPANSION_CONTINUATION', 'position_ratio_too_low', position_ratio=position_snapshot['ratio'])
        if oi_notional_change < OI_EXPANSION_MIN_NOTIONAL_CHANGE:
            return log_rejection(symbol, 'OI_EXPANSION_CONTINUATION', 'oi_notional_change_too_small', oi_notional_change=oi_notional_change)

        account_score_val = top_account_score(account_snapshot['ratio'], account_snapshot.get('long_pct', 0.0)) if account_snapshot else 0.0
        score = 52
        score += int(min(1.0, max(0.0, oi_change_15m / 7.0)) * 14)
        score += int(min(1.0, max(0.0, oi_notional_change / 5.0)) * 12)
        score += int(taker_score * 12)
        score += int(account_score_val * 8)
        score += int(funding_score_val * 4)
        # تخفيف وزن position ratio: عامل تأكيدي فقط
        if position_snapshot['ratio'] >= 1.2:
            score += 2

        reasons = [
            f"OI 15m +{oi_change_15m:.1f}%",
            f"OI notional +{oi_notional_change:.1f}%",
            f"Position Ratio {position_snapshot['ratio']:.2f}",
            f"ضغط شراء {recent_buy:.1%}",
            f"Taker buy/sell {taker_ratio:.2f}x",
            f"سعر 15m +{price_change_15m:.1f}%",
        ]
        if account_snapshot:
            reasons.append(f"Account Ratio {account_snapshot['ratio']:.2f}")
        classification, decisive_feature = 'INSTITUTIONAL_ACCUMULATION', 'oi_notional_change'
        result = {'symbol': symbol, 'score': min(score, 100), 'pattern': 'OI_EXPANSION_CONTINUATION', 'classification': classification, 'decisive_feature': decisive_feature, 'direction': 'UP', 'price': kl['close'][-1], 'reasons': reasons[:8], 'funding': funding_rate, 'timestamp': datetime.now().isoformat(), 'feature_scores': {'oi_regime': round(min(1.0, max(0.0, oi_change_15m / 7.0)), 3), 'oi_notional': round(min(1.0, max(0.0, oi_notional_change / 5.0)), 3), 'taker_aggression': round(taker_score, 3), 'top_account_ratio': round(account_score_val, 3), 'funding_regime': round(funding_score_val, 3)}}
        attach_signal_stage(result, price_change_5m, price_change_15m, oi_change_5m, oi_change_15m, funding_rate, recent_buy)
        apply_time_window_boost(result)
        log_acceptance(symbol, 'OI_EXPANSION_CONTINUATION', score=result['score'], price=result['price'], reasons=result['reasons'], stage=result.get('signal_stage'), classification=classification, decisive_feature=decisive_feature, feature_scores=result['feature_scores'], taker_buy_sell_ratio=taker_ratio)
        return result
    except Exception as e:
        diagnostics.log_error(symbol, 'OI_EXPANSION_CONTINUATION', str(e))
        return None

def detect_account_led_accumulation(symbol):
    """تجميع تقوده الحسابات مع دعم السعر وOI دون رفض بسبب ضعف position ratio."""
    try:
        kl = get_klines(symbol, interval='5m', limit=12)
        if not kl or len(kl['close']) < 12:
            return log_rejection(symbol, 'ACCOUNT_LED_ACCUMULATION', 'insufficient_klines')
        account_snapshot = get_top_account_snapshot(symbol, period='5m')
        position_snapshot = get_top_position_snapshot(symbol, period='5m')
        if not account_snapshot or not position_snapshot:
            return log_rejection(symbol, 'ACCOUNT_LED_ACCUMULATION', 'snapshot_unavailable')
        oi_5m_hist = get_oi_history(symbol, period='5m', limit=6)
        oi_15m_hist = get_oi_history(symbol, period='15m', limit=4)
        if len(oi_15m_hist) < 4:
            return log_rejection(symbol, 'ACCOUNT_LED_ACCUMULATION', 'insufficient_oi_history')
        buy_ratios = compute_taker_buy_ratio(kl)
        if len(buy_ratios) < 6:
            return log_rejection(symbol, 'ACCOUNT_LED_ACCUMULATION', 'insufficient_buy_ratio_points')
        recent_buy = rolling_mean(buy_ratios[-3:])
        taker_ratio = safe_buy_sell_ratio_from_buy_ratio(recent_buy)
        taker_score = taker_flow_score(taker_ratio)
        oi_change_5m = pct_change(oi_5m_hist[-3], oi_5m_hist[-1]) if len(oi_5m_hist) >= 3 else 0.0
        oi_change_15m = pct_change(oi_15m_hist[-2], oi_15m_hist[-1])
        price_change_5m = pct_change(kl['close'][-2], kl['close'][-1])
        price_change_15m = pct_change(kl['close'][-4], kl['close'][-1])
        funding_rate, _ = get_funding_rate(symbol)
        if funding_rate is None:
            return log_rejection(symbol, 'ACCOUNT_LED_ACCUMULATION', 'funding_unavailable')
        basis = get_basis(symbol)
        basis_abs = abs(basis) if basis is not None else 0.0

        account_ratio = float(account_snapshot['ratio'])
        position_ratio = float(position_snapshot['ratio'])
        top_account_long_pct = float(account_snapshot.get('long_pct', 0.0) or 0.0)
        top_position_long_pct = float(position_snapshot.get('long_pct', 0.0) or 0.0)
        account_led_divergence = top_account_long_pct - top_position_long_pct
        position_led_divergence = top_position_long_pct - top_account_long_pct

        if account_ratio < 1.6 or top_account_long_pct < 60.0:
            return log_rejection(symbol, 'ACCOUNT_LED_ACCUMULATION', 'account_footprint_too_weak', account_ratio=account_ratio, account_long_pct=top_account_long_pct)
        if position_ratio < 0.90:
            return log_rejection(symbol, 'ACCOUNT_LED_ACCUMULATION', 'position_ratio_too_low_for_early_accumulation', position_ratio=position_ratio)
        if oi_change_15m < 1.5 or price_change_15m < 1.0:
            return log_rejection(symbol, 'ACCOUNT_LED_ACCUMULATION', 'oi_or_price_not_ready', oi_change_15m=oi_change_15m, price_change_15m=price_change_15m)
        if basis_abs > 0.20 and (oi_change_15m < 2.0 or taker_score < 0.35):
            return log_rejection(symbol, 'ACCOUNT_LED_ACCUMULATION', 'basis_conflict_with_weak_flow', basis=basis, oi_change_15m=oi_change_15m, taker_score=taker_score)
        if not (funding_rate < 0 or abs(funding_rate) <= 0.03):
            return log_rejection(symbol, 'ACCOUNT_LED_ACCUMULATION', 'funding_regime_not_supportive', funding_rate=funding_rate)

        funding_score_val = funding_regime_score(funding_rate, get_funding_trend(symbol, limit=6))
        account_score_val = top_account_score(account_ratio, top_account_long_pct)
        feature_scores = {
            'oi_regime': round(oi_regime_score(oi_change_5m, oi_change_15m), 3),
            'compression': round(max(0.0, min(1.0, (1.1 - position_ratio) / 0.4)), 3),
            'taker_aggression': round(taker_score, 3),
            'cvd_flow': round(max(0.0, min(1.0, (recent_buy - 0.5) * 2.4)), 3),
            'liquidity_vacuum': 0.0,
            'smart_money_accounts': round(account_score_val, 3),
            'smart_money_divergence': round(max(0.0, min(1.0, account_led_divergence / 18.0)), 3),
            'funding_regime': round(funding_score_val, 3),
            'basis_context': round(basis_context_score(basis, oi_change_15m, taker_score), 3),
        }
        score = int(50 + weighted_feature_score(feature_scores) * 42 + compute_four_h_context_bonus(symbol) * 10)
        reasons = [
            f"Account Ratio {account_ratio:.2f}",
            f"Account Long {top_account_long_pct:.2f}%",
            f"Position Ratio {position_ratio:.2f} (الضعف المبكر مقبول)",
            f"account_led_divergence {account_led_divergence:+.2f}pt",
            f"position_led_divergence {position_led_divergence:+.2f}pt",
            f"OI 15m +{oi_change_15m:.1f}%",
            f"سعر 15m +{price_change_15m:.1f}%",
            f"Taker buy/sell {taker_ratio:.2f}x",
        ]
        classification, decisive_feature = classify_bullish_signal({'position_ratio': position_ratio, 'account_ratio': account_ratio, 'top_position_long_pct': top_position_long_pct, 'top_account_long_pct': top_account_long_pct, 'oi_change_15m': oi_change_15m, 'price_change_15m': price_change_15m, 'price_change_5m': price_change_5m, 'funding_rate': funding_rate, 'basis': basis, 'taker_buy_ratio_recent': recent_buy, 'oi_rising_strongly': oi_change_15m >= 2.3, 'price_breakout_confirmed': price_change_5m >= 0.7})
        early_ok, early_reason = pre_pump_early_filter(price_5m=price_change_5m, price_15m=price_change_15m, funding_rate=funding_rate, oi_15m=oi_change_15m if 'oi_change_15m' in locals() else 0.0)
        if not early_ok:
            return log_rejection(symbol, 'ACCOUNT_LED_ACCUMULATION', early_reason, price_change_5m=price_change_5m, price_change_15m=price_change_15m, funding_rate=funding_rate)

        result = {'symbol': symbol, 'score': min(score, 100), 'pattern': 'ACCOUNT_LED_ACCUMULATION', 'classification': classification, 'decisive_feature': decisive_feature, 'direction': 'UP', 'price': kl['close'][-1], 'reasons': reasons[:8], 'funding': funding_rate, 'timestamp': datetime.now().isoformat(), 'feature_scores': feature_scores}
        attach_signal_stage(result, price_change_5m, price_change_15m, oi_change_5m, oi_change_15m, funding_rate, recent_buy)
        apply_time_window_boost(result)
        log_acceptance(symbol, 'ACCOUNT_LED_ACCUMULATION', score=result['score'], price=result['price'], reasons=result['reasons'], stage=result.get('signal_stage'), classification=result.get('classification'), decisive_feature=result.get('decisive_feature'), feature_scores=result.get('feature_scores'), taker_buy_sell_ratio=taker_ratio, account_led_divergence=account_led_divergence, position_led_divergence=position_led_divergence)
        return result
    except Exception as e:
        diagnostics.log_error(symbol, 'ACCOUNT_LED_ACCUMULATION', str(e))
        return None

def detect_exhaustion_risk(symbol):
    """خطر إرهاق بعد صعود قوي وتدهور الزخم قصير المدى."""
    try:
        ctx = get_short_term_context(symbol, '5m', 12)
        if not ctx:
            return log_rejection(symbol, 'EXHAUSTION_RISK', 'missing_short_term_context')
        ratio_shift = analyze_ratio_shift(symbol, period='5m', limit=12)
        if not ratio_shift:
            return log_rejection(symbol, 'EXHAUSTION_RISK', 'ratio_shift_unavailable')
        oi_hist_5m = get_oi_history(symbol, period='5m', limit=6)
        oi_change_5m = pct_change(oi_hist_5m[-3], oi_hist_5m[-1]) if len(oi_hist_5m) >= 3 else 0.0
        if ctx['price_15m'] < EXHAUSTION_MIN_PRICE_15M:
            return log_rejection(symbol, 'EXHAUSTION_RISK', 'price_not_extended_enough', price_change_15m=ctx['price_15m'])
        if ctx['recent_sell_ratio'] < EXHAUSTION_MIN_SELL_RATIO:
            return log_rejection(symbol, 'EXHAUSTION_RISK', 'sell_pressure_not_high_enough', recent_sell_ratio=ctx['recent_sell_ratio'])
        if ratio_shift['acc_delta'] >= 0 and ratio_shift['pos_delta'] >= 0:
            return log_rejection(symbol, 'EXHAUSTION_RISK', 'ratios_not_deteriorating', acc_delta=ratio_shift['acc_delta'], pos_delta=ratio_shift['pos_delta'])
        if oi_change_5m > EXHAUSTION_MAX_OI_5M:
            return log_rejection(symbol, 'EXHAUSTION_RISK', 'oi_still_expanding', oi_change_5m=oi_change_5m)
        score = 61
        reasons = [
            f"امتداد 15m +{ctx['price_15m']:.1f}%",
            f"ضغط بيع {ctx['recent_sell_ratio']:.1%}",
            f"Account delta {ratio_shift['acc_delta']:+.2f}",
            f"Position delta {ratio_shift['pos_delta']:+.2f}",
            f"OI 5m {oi_change_5m:+.1f}%",
        ]
        if ctx['body_ratio'] < 0.35:
            score += 4
            reasons.append(f"ضعف جسم الشمعة {ctx['body_ratio']:.2f}")
        result = {'symbol': symbol, 'score': min(score, 100), 'pattern': 'EXHAUSTION_RISK', 'direction': 'DOWN', 'price': ctx['last_close'], 'reasons': reasons[:7], 'timestamp': datetime.now().isoformat()}
        log_acceptance(symbol, 'EXHAUSTION_RISK', score=result['score'], price=result['price'], reasons=result['reasons'], stage='LATE')
        result['signal_stage'] = 'LATE'
        return result
    except Exception as e:
        diagnostics.log_error(symbol, 'EXHAUSTION_RISK', str(e))
        return None

# =============================================================================
# قاموس legacy-only للأنماط القديمة (تشخيص داخلي فقط)
# =============================================================================
PATTERN_ARABIC = {
    'LONG_TERM_ACCUMULATION': {
        'name': 'تراكم طويل الأجل',
        'default_direction': 'شراء',
        'emoji': '📅'
    },
    'PRE_PUMP': {
        'name': 'ما قبل الارتفاع',
        'default_direction': 'شراء',
        'emoji': '🕒'
    },
    'WHALE_SETUP': {
        'name': 'بصمة الحيتان',
        'default_direction': 'شراء',
        'emoji': '🐋'
    },
    'BEAR_SQUEEZE': {
        'name': 'عصر الدببة',
        'default_direction': 'بيع',
        'emoji': '🐻'
    },
    'WHALE_MOMENTUM_UP': {
        'name': 'زخم الحيتان الصاعد',
        'default_direction': 'شراء',
        'emoji': '🐋'
    },
    'WHALE_MOMENTUM_DOWN': {
        'name': 'زخم الحيتان الهابط',
        'default_direction': 'بيع',
        'emoji': '🐋'
    },
    'PRE_CLOSE_SQUEEZE': {
        'name': 'ما قبل الإغلاق',
        'default_direction': 'شراء',
        'emoji': '⏰'
    },
    'RETAIL_TRAP_UP': {
        'name': 'فخ التجزئة الصاعد',
        'default_direction': 'شراء',
        'emoji': '🔺'
    },
    'RETAIL_TRAP_DOWN': {
        'name': 'فخ التجزئة الهابط',
        'default_direction': 'بيع',
        'emoji': '🔻'
    },
    'SKYROCKET': {
        'name': 'ارتفاع مفاجئ',
        'default_direction': 'شراء',
        'emoji': '💥'
    },
    'STRONG_UPTREND': {
        'name': 'اتجاه صاعد قوي',
        'default_direction': 'شراء',
        'emoji': '📈'
    },
    'SMART_MONEY_DIVERGENCE': {
        'name': 'تباين الأموال الذكية',
        'default_direction': 'شراء',
        'emoji': '🧠'
    },
    'PRE_EXPLOSION_SETUP': {
        'name': 'إعداد ما قبل الانفجار',
        'default_direction': 'شراء',
        'emoji': '🚀'
    },
    'PRE_SQUEEZE_UP': {
        'name': 'ضغط صاعد مبكر',
        'default_direction': 'شراء',
        'emoji': '🧲'
    },
    'SMART_DIVERGENCE_SQUEEZE': {
        'name': 'ضغط تباين ذكي',
        'default_direction': 'شراء',
        'emoji': '🧠'
    },
    'POSITION_LED_SQUEEZE_BUILDUP': {
        'name': 'تهيئة ضغط بقيادة المراكز',
        'default_direction': 'شراء',
        'emoji': '🎯'
    },
    'CONSENSUS_BULLISH_EXPANSION': {
        'name': 'توسع صاعد متوافق',
        'default_direction': 'شراء',
        'emoji': '🟢'
    },
    'FLOW_BREAKOUT': {
        'name': 'اختراق تدفقي',
        'default_direction': 'شراء',
        'emoji': '⚡'
    },
    'FLOW_BREAKOUT_NEUTRAL_FUNDING': {
        'name': 'اختراق تدفقي بتمويل محايد',
        'default_direction': 'شراء',
        'emoji': '⚡'
    },
    'FLOW_LIQUIDITY_VACUUM_BREAKOUT': {
        'name': 'اختراق فراغ سيولة تدفقي',
        'default_direction': 'شراء',
        'emoji': '⚡'
    },
    'OI_EXPANSION_PUMP': {
        'name': 'ضخ بتمدد الفائدة المفتوحة',
        'default_direction': 'شراء',
        'emoji': '🟡'
    },
    'OI_EXPANSION_CONTINUATION': {
        'name': 'استمرار بتمدد الفائدة المفتوحة',
        'default_direction': 'شراء',
        'emoji': '🟡'
    },
    'SHORT_SQUEEZE_BUILDUP': {
        'name': 'بناء ضغط قصير',
        'default_direction': 'شراء',
        'emoji': '🧠'
    },
    'INSTITUTIONAL_ACCUMULATION': {
        'name': 'تراكم مؤسسي',
        'default_direction': 'شراء',
        'emoji': '🏦'
    },
    'LIQUIDITY_VACUUM_BREAKOUT': {
        'name': 'اختراق فراغ سيولة',
        'default_direction': 'شراء',
        'emoji': '⚡'
    },
    'NO_CLEAR_BULLISH_FAMILY': {
        'name': 'بصمة صاعدة غير مكتملة',
        'default_direction': 'شراء',
        'emoji': '⚪'
    }
}

CLASSIFICATION_DISPLAY = {
    'POSITION_LED_SQUEEZE_BUILDUP': {'name': 'بناء سكيز تقوده المراكز', 'emoji': '🎯', 'direction': 'UP'},
    'ACCOUNT_LED_ACCUMULATION': {'name': 'تجميع تقوده الحسابات', 'emoji': '🏦', 'direction': 'UP'},
    'CONSENSUS_BULLISH_EXPANSION': {'name': 'توسع صاعد مؤسسي متوافق', 'emoji': '🟢', 'direction': 'UP'},
    'FLOW_LIQUIDITY_VACUUM_BREAKOUT': {'name': 'اختراق فراغ سيولة تدفقي', 'emoji': '⚡', 'direction': 'UP'},
    'INSTITUTIONAL_ACCUMULATION': {'name': 'تراكم مؤسسي', 'emoji': '🏦', 'direction': 'UP'},
    'SHORT_SQUEEZE_BUILDUP': {'name': 'بناء ضغط قصير', 'emoji': '🧠', 'direction': 'UP'},
    'LIQUIDITY_VACUUM_BREAKOUT': {'name': 'اختراق فراغ سيولة', 'emoji': '⚡', 'direction': 'UP'},
    'NO_CLEAR_BULLISH_FAMILY': {'name': 'بصمة صاعدة غير مكتملة', 'emoji': '⚪', 'direction': 'UP'},
    'EXHAUSTION_RISK': {'name': 'خطر إرهاق صاعد', 'emoji': '🧯', 'direction': 'DOWN'},
    'BEARISH_GENERIC': {'name': 'إشارة هابطة عامة', 'emoji': '🔻', 'direction': 'DOWN'},
    'LATE_CROWD_LONG': {'name': 'ازدحام شراء متأخر', 'emoji': '🚫', 'direction': 'DOWN'},
}

def get_primary_display(signal_data):
    classification = signal_data.get('classification')
    pattern = signal_data.get('pattern')
    key = classification or pattern or 'UNKNOWN'
    info = CLASSIFICATION_DISPLAY.get(key)
    if info:
        return key, info.get('name', key), info.get('emoji', '🔹')
    if key in PATTERN_ARABIC:
        return key, key, '🔹'
    return key, key, '🔹'

def direction_to_ar(direction):
    if direction == 'UP':
        return '🟢 شراء'
    if direction == 'DOWN':
        return '🔴 بيع'
    return '⚪ محايد'

def format_feature_scores_brief(feature_scores, top_n=4):
    if not feature_scores or not isinstance(feature_scores, dict):
        return 'غير متاح'
    pairs = []
    for k, v in feature_scores.items():
        try:
            pairs.append((k, float(v)))
        except Exception:
            continue
    pairs.sort(key=lambda x: x[1], reverse=True)
    brief = [f"{k}:{v:.2f}" for k, v in pairs[:top_n]]
    return ' | '.join(brief) if brief else 'غير متاح'

# =============================================================================
# قاعدة بيانات الإشارات
# =============================================================================
class SignalDatabase:
    REQUIRED_SIGNAL_COLUMNS = {
        "timestamp": "TEXT",
        "symbol": "TEXT",
        "pattern": "TEXT",
        "score": "REAL",
        "price": "REAL",
        "direction": "TEXT DEFAULT 'UP'",
        "reasons": "TEXT",
        "signal_quality_tier": "TEXT",
        "stop_loss": "REAL",
        "take_profit1": "REAL",
        "take_profit2": "REAL",
        "signal_stage": "TEXT DEFAULT 'SETUP'",
        "evaluated": "INTEGER DEFAULT 0",
        "outcome": "INTEGER DEFAULT 0",
        "price_after": "REAL",
        "price_after_5m": "REAL",
        "price_after_15m": "REAL",
        "price_after_30m": "REAL",
        "outcome_5m": "INTEGER DEFAULT -1",
        "outcome_15m": "INTEGER DEFAULT -1",
        "outcome_30m": "INTEGER DEFAULT -1",
        "evaluated_5m": "INTEGER DEFAULT 0",
        "evaluated_15m": "INTEGER DEFAULT 0",
        "evaluated_30m": "INTEGER DEFAULT 0",
        "trigger_high": "REAL",
        "trigger_low": "REAL",
        "invalidated": "INTEGER DEFAULT 0",
        "invalidation_reason": "TEXT",
        "invalidation_price": "REAL",
        "invalidated_at": "TEXT"
    }

    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        ensure_parent_dir(self.db_path)
        self._init_db()

    def _configure_connection(self, conn):
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
        except sqlite3.OperationalError:
            pass

    def _get_existing_columns(self, conn):
        rows = conn.execute("PRAGMA table_info(signals)").fetchall()
        return {row[1] for row in rows}

    def _ensure_signal_schema(self, conn):
        self._configure_connection(conn)
        existing_columns = self._get_existing_columns(conn)
        for col_name, col_def in self.REQUIRED_SIGNAL_COLUMNS.items():
            if col_name not in existing_columns:
                try:
                    conn.execute(f"ALTER TABLE signals ADD COLUMN {col_name} {col_def}")
                except sqlite3.OperationalError as e:
                    # SQLite لا يدعم IF NOT EXISTS في ALTER TABLE ADD COLUMN،
                    # لذا نتجاهل الخطأ فقط إذا كان العمود موجودًا بالفعل.
                    if "duplicate column name" not in str(e).lower():
                        raise
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_symbol_time ON signals(symbol, timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_evaluated ON signals(evaluated)")

    def _init_db(self):
        with sqlite3.connect(self.db_path, timeout=10) as conn:
            self._configure_connection(conn)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    symbol TEXT,
                    pattern TEXT,
                    score REAL,
                    price REAL,
                    direction TEXT,
                    reasons TEXT,
                    signal_quality_tier TEXT,
                    stop_loss REAL,
                    take_profit1 REAL,
                    take_profit2 REAL,
                    signal_stage TEXT DEFAULT 'SETUP',
                    evaluated INTEGER DEFAULT 0,
                    outcome INTEGER DEFAULT 0,
                    price_after REAL,
                    trigger_high REAL,
                    trigger_low REAL,
                    invalidated INTEGER DEFAULT 0,
                    invalidation_reason TEXT,
                    invalidation_price REAL,
                    invalidated_at TEXT
                )
            """)
            self._ensure_signal_schema(conn)

    def save_signal(self, signal_data):
        if not SAVE_SIGNALS:
            return
        with sqlite3.connect(self.db_path, timeout=10) as conn:
            self._ensure_signal_schema(conn)
            conn.execute("""
                INSERT INTO signals (timestamp, symbol, pattern, score, price, direction, reasons, signal_quality_tier, stop_loss, take_profit1, take_profit2, signal_stage, trigger_high, trigger_low)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal_data.get('timestamp', datetime.now().isoformat()),
                signal_data.get('symbol', ''),
                signal_data.get('pattern', 'UNKNOWN'),
                float(signal_data.get('score', 0) or 0),
                float(signal_data.get('price', 0) or 0),
                signal_data.get('direction', 'UP'),
                json.dumps(signal_data.get('reasons', []), ensure_ascii=False),
                signal_data.get('signal_quality_tier', 'مرشح مبكر'),
                float(signal_data.get('stop_loss', 0) or 0),
                float(signal_data.get('take_profit1', 0) or 0),
                float(signal_data.get('take_profit2', 0) or 0),
                signal_data.get('signal_stage', 'SETUP'),
                float(signal_data.get('trigger_high', 0) or 0),
                float(signal_data.get('trigger_low', 0) or 0)
            ))

    def update_signal_evaluation(self, sig_id, horizon_minutes, current_price, success):
        horizon_minutes = int(horizon_minutes)
        if horizon_minutes not in (5, 15, 30):
            return
        price_col = f"price_after_{horizon_minutes}m"
        outcome_col = f"outcome_{horizon_minutes}m"
        eval_col = f"evaluated_{horizon_minutes}m"
        with sqlite3.connect(self.db_path, timeout=10) as conn:
            self._configure_connection(conn)
            conn.execute(
                f"UPDATE signals SET {price_col} = ?, {outcome_col} = ?, {eval_col} = 1, price_after = COALESCE(price_after, ?) WHERE id = ?",
                (current_price, success, current_price, sig_id)
            )

    def get_pending_eval_signals(self, horizon_minutes, hours_ago=72):
        horizon_minutes = int(horizon_minutes)
        eval_col = f"evaluated_{horizon_minutes}m"
        cutoff = (datetime.now() - timedelta(hours=hours_ago)).isoformat()
        max_age = (datetime.now() - timedelta(minutes=horizon_minutes)).isoformat()
        with sqlite3.connect(self.db_path, timeout=10) as conn:
            self._configure_connection(conn)
            cur = conn.execute(
                f"SELECT id, symbol, pattern, price, direction, timestamp FROM signals WHERE {eval_col} = 0 AND timestamp >= ? AND timestamp <= ?",
                (cutoff, max_age)
            )
            return cur.fetchall()

    def get_active_signals_for_invalidation(self, lookback_hours=6):
        cutoff = (datetime.now() - timedelta(hours=lookback_hours)).isoformat()
        with sqlite3.connect(self.db_path, timeout=10) as conn:
            self._configure_connection(conn)
            cur = conn.execute(
                "SELECT id, symbol, pattern, price, direction, timestamp, COALESCE(trigger_high,0), COALESCE(trigger_low,0), signal_stage FROM signals WHERE invalidated = 0 AND timestamp >= ? AND signal_stage IN ('SETUP','ARMED','TRIGGERED','CONFIRMED')",
                (cutoff,)
            )
            return cur.fetchall()

    def invalidate_signal(self, sig_id, current_price, reason):
        with sqlite3.connect(self.db_path, timeout=10) as conn:
            self._configure_connection(conn)
            conn.execute(
                "UPDATE signals SET invalidated = 1, signal_stage = 'INVALIDATED', invalidation_price = ?, invalidation_reason = ?, invalidated_at = ? WHERE id = ?",
                (current_price, reason, datetime.now().isoformat(), sig_id)
            )

    def get_backtest_summary(self, lookback_hours=72):
        cutoff = (datetime.now() - timedelta(hours=lookback_hours)).isoformat()
        with sqlite3.connect(self.db_path, timeout=10) as conn:
            self._configure_connection(conn)
            cur = conn.execute("""
                SELECT pattern,
                       COUNT(*) AS total,
                       SUM(CASE WHEN outcome_5m = 1 THEN 1 ELSE 0 END) AS win_5m,
                       SUM(CASE WHEN outcome_15m = 1 THEN 1 ELSE 0 END) AS win_15m,
                       SUM(CASE WHEN outcome_30m = 1 THEN 1 ELSE 0 END) AS win_30m
                FROM signals
                WHERE timestamp >= ? AND invalidated = 0
                GROUP BY pattern
                ORDER BY total DESC
            """, (cutoff,))
            return cur.fetchall()

    def get_pattern_quality_metrics(self, lookback_hours=96):
        cutoff = (datetime.now() - timedelta(hours=lookback_hours)).isoformat()
        with sqlite3.connect(self.db_path, timeout=10) as conn:
            self._configure_connection(conn)
            cur = conn.execute("""
                SELECT pattern,
                       COUNT(*) AS total,
                       SUM(CASE WHEN outcome_5m = 1 THEN 1 ELSE 0 END) AS win_5m,
                       SUM(CASE WHEN outcome_15m = 1 THEN 1 ELSE 0 END) AS win_15m,
                       SUM(CASE WHEN outcome_30m = 1 THEN 1 ELSE 0 END) AS win_30m,
                       SUM(CASE WHEN invalidated = 1 THEN 1 ELSE 0 END) AS invalidated_count
                FROM signals
                WHERE timestamp >= ?
                GROUP BY pattern
                ORDER BY total DESC
            """, (cutoff,))
            return cur.fetchall()

    def get_un_evaluated_signals(self, hours_ago=24):
        cutoff = (datetime.now() - timedelta(hours=hours_ago)).isoformat()
        with sqlite3.connect(self.db_path, timeout=10) as conn:
            self._configure_connection(conn)
            cur = conn.execute("SELECT id, symbol, pattern, price, direction, timestamp FROM signals WHERE evaluated = 0 AND timestamp < ?", (cutoff,))
            return cur.fetchall()

db = SignalDatabase()

# =============================================================================
# نظام المعايرة التكيفية للأنماط
# =============================================================================
VALID_CALIBRATION_PATTERNS = {
    "POSITION_LED_SQUEEZE_BUILDUP",
    "ACCOUNT_LED_ACCUMULATION",
    "CONSENSUS_BULLISH_EXPANSION",
    "FLOW_LIQUIDITY_VACUUM_BREAKOUT",
    "EXHAUSTION_RISK",
    "BEAR_SQUEEZE",
    "PRE_CLOSE_SQUEEZE",
    "RETAIL_TRAP_DOWN",
    "RETAIL_TRAP_UP",
}


def _normalize_calibration_pattern(pattern):
    pattern = str(pattern or '').strip().upper()
    if not pattern or pattern == 'NULL' or pattern not in VALID_CALIBRATION_PATTERNS:
        return ''
    return pattern

class PatternCalibrator:
    def __init__(self, db_obj, path=CALIBRATION_PATH):
        self.db = db_obj
        self.path = path
        self.rules = {}
        self.last_refresh = None
        self.load()

    def load(self):
        try:
            if os.path.exists(self.path):
                with open(self.path, 'r', encoding='utf-8') as f:
                    raw_rules = json.load(f)
                if isinstance(raw_rules, dict):
                    cleaned = {}
                    for k, v in raw_rules.items():
                        nk = _normalize_calibration_pattern(k)
                        if nk and isinstance(v, dict):
                            cleaned[nk] = v
                    self.rules = cleaned
                else:
                    self.rules = {}
            else:
                self.rules = {}
        except Exception:
            self.rules = {}

    def save(self):
        try:
            ensure_parent_dir(self.path)
            with open(self.path, 'w', encoding='utf-8') as f:
                json.dump(self.rules, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def refresh_from_db(self, lookback_hours=CALIBRATION_LOOKBACK_HOURS):
        if not ENABLE_AUTO_CALIBRATION:
            return {}
        rows = self.db.get_pattern_quality_metrics(lookback_hours=lookback_hours)
        filtered_rows = []
        total_signals = 0
        for row in rows:
            pattern, total, win5, win15, win30, invalidated = row
            pattern = _normalize_calibration_pattern(pattern)
            total = int(total or 0)
            if not pattern or total <= 0:
                continue
            filtered_rows.append((pattern, total, win5, win15, win30, invalidated))
            total_signals += total

        if total_signals < MIN_SIGNALS_FOR_CALIBRATION:
            self.rules = {k: v for k, v in self.rules.items() if _normalize_calibration_pattern(k)}
            self.last_refresh = datetime.now()
            return {}

        updated = {}
        for row in filtered_rows:
            pattern, total, win5, win15, win30, invalidated = row
            if total < MIN_SIGNALS_FOR_CALIBRATION:
                continue
            win5 = int(win5 or 0)
            win15 = int(win15 or 0)
            win30 = int(win30 or 0)
            invalidated = int(invalidated or 0)
            wr5 = win5 / total
            wr15 = win15 / total
            wr30 = win30 / total
            invr = invalidated / total
            quality = (0.20 * wr5) + (0.45 * wr15) + (0.35 * wr30) - (0.25 * invr)
            quality = max(0.0, min(1.0, quality))
            score_bias = int(round((quality - 0.55) * 20))
            score_bias = max(-PATTERN_SCORE_BIAS_LIMIT, min(PATTERN_SCORE_BIAS_LIMIT, score_bias))
            min_accept_score = 65
            if quality < 0.40:
                min_accept_score = 82
            elif quality < 0.50:
                min_accept_score = 76
            elif quality < 0.60:
                min_accept_score = 71
            elif quality > 0.72:
                min_accept_score = 64
            updated[pattern] = {
                'total': total,
                'win_rate_5m': round(wr5, 4),
                'win_rate_15m': round(wr15, 4),
                'win_rate_30m': round(wr30, 4),
                'invalidation_rate': round(invr, 4),
                'quality': round(quality, 4),
                'score_bias': score_bias,
                'min_accept_score': min_accept_score,
                'updated_at': datetime.now().isoformat()
            }
        self.rules = {k: v for k, v in self.rules.items() if _normalize_calibration_pattern(k)}
        if updated:
            self.rules.update(updated)
            self.save()
        self.last_refresh = datetime.now()
        return updated

    def apply(self, signal_data):
        if not signal_data or not ENABLE_AUTO_CALIBRATION:
            return signal_data
        rule = self.rules.get(_normalize_calibration_pattern(signal_data.get('pattern')))
        if not rule:
            return signal_data
        signal_data['calibration_quality'] = rule.get('quality')
        signal_data['calibration_min_accept_score'] = rule.get('min_accept_score')
        bias = int(rule.get('score_bias', 0))
        if bias:
            signal_data['score'] = max(0, min(100, int(signal_data.get('score', 0) + bias)))
            signal_data.setdefault('reasons', []).append(f"معايرة النمط {bias:+d} (جودة {rule.get('quality', 0):.2f})")
        if signal_data.get('score', 0) < int(rule.get('min_accept_score', 65)):
            diagnostics.log_event('CALIBRATION_REJECT', signal_data.get('symbol', ''), signal_data.get('pattern', ''), {
                'score': signal_data.get('score'),
                'min_accept_score': rule.get('min_accept_score'),
                'quality': rule.get('quality')
            })
            return None
        return signal_data

    def print_summary(self):
        clean_rules = {
            _normalize_calibration_pattern(k): v
            for k, v in self.rules.items()
            if _normalize_calibration_pattern(k) and isinstance(v, dict)
        }
        if not clean_rules:
            print("🎛️ لا توجد معايرة محفوظة بعد.")
            return
        print("🎛️ ملخص المعايرة:")
        items = sorted(clean_rules.items(), key=lambda kv: str(kv[0] or ''))
        for pattern, rule in items:
            if not isinstance(rule, dict) or 'quality' not in rule:
                continue
            quality = float(rule.get('quality', 0.0) or 0.0)
            bias = int(rule.get('score_bias', 0) or 0)
            min_accept = int(rule.get('min_accept_score', 65) or 65)
            print(f"   • {pattern}: جودة={quality:.2f} | bias={bias:+d} | min={min_accept}")

pattern_calibrator = PatternCalibrator(db)

# =============================================================================
# نظام تتبع تحديثات الإشارات
# =============================================================================
class SignalUpdateTracker:
    def __init__(self):
        self.last_signals = {}
        self.lock = threading.Lock()

    def should_alert(self, symbol, new_score, new_pattern):
        with self.lock:
            now = time.time()
            if symbol not in self.last_signals:
                self.last_signals[symbol] = {
                    'score': new_score,
                    'pattern': new_pattern,
                    'timestamp': now
                }
                return True

            last = self.last_signals[symbol]
            if last['pattern'] != new_pattern:
                self.last_signals[symbol] = {
                    'score': new_score,
                    'pattern': new_pattern,
                    'timestamp': now
                }
                return True

            score_increase = new_score - last['score']
            time_since_last = now - last['timestamp']
            cooldown_seconds = UPDATE_ALERT_COOLDOWN_MINUTES * 60

            if score_increase >= MIN_SCORE_INCREASE_FOR_ALERT and time_since_last >= cooldown_seconds:
                self.last_signals[symbol] = {
                    'score': new_score,
                    'pattern': new_pattern,
                    'timestamp': now
                }
                return True
            else:
                return False

signal_tracker = SignalUpdateTracker()

# =============================================================================
# ذاكرة مؤقتة للرموز غير الصالحة (تجنب إعادة المحاولة في نفس الدورة)
# =============================================================================
invalid_symbols = set()
invalid_symbols_lock = threading.Lock()

def mark_symbol_invalid(symbol):
    with invalid_symbols_lock:
        invalid_symbols.add(symbol)

def is_symbol_invalid(symbol):
    with invalid_symbols_lock:
        return symbol in invalid_symbols

def clear_invalid_symbols():
    with invalid_symbols_lock:
        invalid_symbols.clear()


CORE_BULLISH_PATTERNS = {
    'POSITION_LED_SQUEEZE_BUILDUP',
    'ACCOUNT_LED_ACCUMULATION',
    'CONSENSUS_BULLISH_EXPANSION',
    'FLOW_LIQUIDITY_VACUUM_BREAKOUT',
}

BEARISH_PATTERNS = {
    'BEAR_SQUEEZE',
    'RETAIL_TRAP_DOWN',
    'WHALE_MOMENTUM_DOWN',
    'EXHAUSTION_RISK',
}

def _enforce_signal_consistency(signal_data):
    if not signal_data:
        return signal_data
    pattern = str(signal_data.get('pattern', '') or '')
    direction = signal_data.get('direction', 'UP')
    classification = signal_data.get('classification')

    if pattern in CORE_BULLISH_PATTERNS:
        signal_data['direction'] = 'UP'
        if classification not in CORE_BULLISH_PATTERNS:
            signal_data['classification'] = pattern

    if pattern in BEARISH_PATTERNS and direction != 'DOWN':
        signal_data['direction'] = 'DOWN'

    return signal_data

def _dedupe_symbol_signals(results):
    best = {}
    for r in results:
        key = (r.get('pattern'), r.get('classification'), r.get('direction'))
        if key not in best or float(r.get('score', 0)) > float(best[key].get('score', 0)):
            best[key] = r
    return list(best.values())

def evaluate_symbol_patterns(sym):
    """تقييم الأنماط: العائلات الصاعدة الأربع هي المرجع الحاكم + مسارات المخاطر/الهبوط فقط."""
    results = []

    core_bullish_functions = [
        detect_position_led_squeeze_buildup,
        detect_account_led_accumulation,
        detect_consensus_bullish_expansion,
        detect_flow_breakout,
    ]

    bearish_functions = [
        detect_exhaustion_risk,
        bear_squeeze_pattern,
        pre_close_squeeze_pattern,
        retail_trap_pattern,
    ]

    def _run_detector(fn):
        try:
            res = fn(sym)
            if not res:
                return None
            _enforce_signal_consistency(res)
            quality_score, quality_level, quality_reasons = assess_signal_quality(res['symbol'], res['score'], res)
            res['signal_quality_tier'] = quality_level
            for reason in quality_reasons:
                if reason not in res['reasons']:
                    res['reasons'].append(reason)
            stop_loss, tp1, tp2 = calculate_risk_management(res['symbol'], res['price'], res['direction'])
            res['stop_loss'] = stop_loss
            res['take_profit1'] = tp1
            res['take_profit2'] = tp2
            return validate_and_finalize_signal(res)
        except Exception as e:
            diagnostics.log_error(sym, fn.__name__, str(e))
            if DEBUG:
                print(f"⚠️ خطأ في {fn.__name__} لـ {sym}: {e}")
            return None

    for fn in core_bullish_functions:
        res = _run_detector(fn)
        if res:
            results.append(res)

    for fn in bearish_functions:
        res = _run_detector(fn)
        if res:
            results.append(res)

    return _dedupe_symbol_signals(results)

# =============================================================================
# دالة إرسال إشارة فردية
# =============================================================================
def send_individual_signal(signal_data):
    """إرسال تنبيه فوري عند اكتشاف إشارة جديدة أو محدثة مع جعل classification هو العنوان الرئيسي."""
    if signal_data['score'] < SIGNAL_MIN_SCORE:
        diagnostics.log_reject(signal_data.get('symbol', 'UNKNOWN'), signal_data.get('pattern', 'UNKNOWN'), 'signal_below_send_threshold', {'score': signal_data.get('score')})
        return

    primary_key, primary_name, primary_emoji = get_primary_display(signal_data)
    direction = signal_data.get('direction', 'UP')
    direction_ar = direction_to_ar(direction)
    signal_stage = signal_data.get('signal_stage', 'SETUP')
    decisive_feature = signal_data.get('decisive_feature', 'score')
    reasons = "، ".join(signal_data.get('reasons', []))
    feature_scores_txt = format_feature_scores_brief(signal_data.get('feature_scores', {}))
    stop_loss = signal_data.get('stop_loss') or 0
    tp1 = signal_data.get('take_profit1') or 0
    tp2 = signal_data.get('take_profit2') or 0
    multi_pattern_hint = signal_data.get('multi_pattern_hint', '')

    message = (
        f"🔔 *إشارة جديدة: {signal_data['symbol']}* {multi_pattern_hint}\n"
        f"{primary_emoji} التصنيف: *{primary_name}* (`{primary_key}`) [{signal_data.get('signal_quality_tier', 'مرشح مبكر')}]\n"
        f"   الدرجة: {signal_data['score']}\n"
        f"   الاتجاه: {direction_ar}\n"
        f"   المرحلة: {signal_stage}\n"
        f"   السعر: {signal_data['price']:.6f}\n"
        f"   وقف الخسارة: {stop_loss:.6f} | الهدف1: {tp1:.6f} | الهدف2: {tp2:.6f}\n"
        f"   العامل الحاسم: {decisive_feature}\n"
        f"   أعلى الميزات: {feature_scores_txt}\n"
        f"   أسباب: {reasons}\n"
    )
    send_telegram(message)

# =============================================================================
# المسح العميق (معدل لإرسال التنبيهات الفورية)
# =============================================================================
def deep_scan(candidates, learning_system):
    symbol_signals = defaultdict(list)
    candidates = list(candidates[:MAX_SYMBOLS_PER_CYCLE])
    print(f"🔍 تحليل عميق لـ {len(candidates)} عملة...")

    clear_invalid_symbols()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_eval = {executor.submit(evaluate_symbol_patterns, sym): sym for sym in candidates if not is_symbol_invalid(sym)}
        for future in as_completed(future_eval):
            sym = future_eval[future]
            try:
                symbol_results = future.result(timeout=35)
                if symbol_results:
                    for res in symbol_results:
                        primary_key, primary_name, primary_emoji = get_primary_display(res)
                        print(f"   {primary_emoji} {sym}: {res['score']} | {primary_key}")
                        symbol_signals[sym].append(res)
            except Exception as e:
                print(f"   ❌ {sym}: {str(e)[:50]}")
                if "400" in str(e) or "404" in str(e):
                    mark_symbol_invalid(sym)

    final_results = []
    for sym, sigs in symbol_signals.items():
        if len(sigs) > 1:
            for sig in sigs:
                sig['multi_pattern_hint'] = "🔥 (فرصة دخول عالية)"
                final_results.append(sig)
        else:
            final_results.extend(sigs)

    return final_results

# =============================================================================
# التنبيهات عبر تليجرام (تقرير نهائي)
# =============================================================================
def send_telegram(message, echo_to_console=True):
    if TELEGRAM_TOKEN == "YOUR_BOT_TOKEN":
        if echo_to_console:
            print("\n" + message)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.post(url, json={'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'Markdown'}, timeout=10)
    except Exception as e:
        print(f"تليجرام خطأ: {e}")

def generate_report(results):
    if not results:
        print("⚠️ لا توجد إشارات.")
        return

    results.sort(key=lambda x: x['score'], reverse=True)
    lines = ["🚀 *تقرير الماسح المتكامل (Classification-first)*\n"]

    for r in results[:10]:
        primary_key, primary_name, primary_emoji = get_primary_display(r)
        direction_ar = direction_to_ar(r.get('direction', 'UP'))
        reasons = "، ".join(r.get('reasons', []))
        stop_loss = r.get('stop_loss') or 0
        tp1 = r.get('take_profit1') or 0
        tp2 = r.get('take_profit2') or 0
        multi_hint = r.get('multi_pattern_hint', '')
        signal_stage = r.get('signal_stage', 'SETUP')
        decisive_feature = r.get('decisive_feature', 'score')
        feature_scores_txt = format_feature_scores_brief(r.get('feature_scores', {}))

        lines.append(
            f"{primary_emoji} *{r['symbol']}*: {r['score']} | *{primary_name}* (`{primary_key}`) {multi_hint} [{r.get('signal_quality_tier', 'مرشح مبكر')}]\n"
            f"   الاتجاه: {direction_ar}\n"
            f"   المرحلة: {signal_stage}\n"
            f"   السعر: {r['price']:.6f}\n"
            f"   وقف الخسارة: {stop_loss:.6f} | الهدف1: {tp1:.6f} | الهدف2: {tp2:.6f}\n"
            f"   العامل الحاسم: {decisive_feature}\n"
            f"   أعلى الميزات: {feature_scores_txt}\n"
            f"   أسباب: {reasons}\n"

        )

        try:
            db.save_signal(r)
        except Exception as e:
            diagnostics.log_error(r.get('symbol', 'UNKNOWN'), 'save_signal', str(e))
            if DEBUG:
                print(f"⚠️ فشل حفظ الإشارة لـ {r.get('symbol', 'UNKNOWN')}: {e}")

    full = "\n".join(lines)
    print(full)
    send_telegram(full, echo_to_console=False)

def generate_backtest_report(lookback_hours=BACKTEST_LOOKBACK_HOURS):
    rows = db.get_backtest_summary(lookback_hours=lookback_hours)
    if not rows:
        print("📈 لا توجد بيانات كافية للباك تست بعد.")
        return
    print("\n📈 ملخص الباك تست المحلي:")
    for pattern, total, win5, win15, win30 in rows[:10]:
        total = total or 0
        r5 = (win5 / total * 100) if total else 0
        r15 = (win15 / total * 100) if total else 0
        r30 = (win30 / total * 100) if total else 0
        print(f"   {pattern}: total={total} | 5m={r5:.1f}% | 15m={r15:.1f}% | 30m={r30:.1f}%")

# =============================================================================
# الحلقة الرئيسية
# =============================================================================
def main():
    print("="*70)
    print("الماسح المتوازي المتقدم - الإصدار v5.3.1 الاحترافي لـ Pydroid 3".center(70))
    print("="*70)
    if ENABLE_DIAGNOSTICS:
        print(f"🧪 سجل التشخيص: {DIAGNOSTICS_PATH}")

    all_symbols = get_all_usdt_perpetuals()
    global TRADING_SYMBOLS_SET
    TRADING_SYMBOLS_SET = set(all_symbols)
    print(f"✅ إجمالي العقود: {len(all_symbols)}")

    learning_system = LearningSystem(db)

    while True:
        start_time = time.time()
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] بدء الدورة...")

        if ENABLE_HMM:
            print("🔄 تدريب نموذج HMM (تحديث حقيقي لكل دورة)...")
            hmm_detector.fit_model()
            hmm_detector.last_fit_time = time.time()
            regime = hmm_detector.get_current_regime()
            print(f"📊 حالة السوق الحالية: {regime['regime_name']}")

        if ENABLE_AUTO_CALIBRATION:
            updated = pattern_calibrator.refresh_from_db(CALIBRATION_LOOKBACK_HOURS)
            if updated:
                print(f"🎛️ تم تحديث معايرة {len(updated)} نمط من نتائجك الأخيرة")
            else:
                print("🎛️ لا توجد تحديثات معايرة جديدة في هذه الدورة")
            pattern_calibrator.print_summary()

        candidates = quick_filter()
        if not candidates:
            print("⚠️ لا توجد مرشحات بعد الفلترة.")
        else:
            results = deep_scan(candidates, learning_system)
            generate_report(results)

        invalidated_count = learning_system.invalidate_signals()
        if invalidated_count:
            print(f"🚫 تم إبطال {invalidated_count} إشارة بعد كسر الشروط المرجعية")

        if ENABLE_LEARNING:
            evaluated_count = learning_system.evaluate_signals()
            if evaluated_count:
                print(f"🧠 تم تقييم {evaluated_count} نتيجة عبر آفاق 5m/15m/30m")
            hours_since_last = (datetime.now() - learning_system.last_learning_time).total_seconds() / 3600
            if hours_since_last >= LEARNING_INTERVAL_HOURS:
                print("🔄 جاري تحديث الأوزان وعرض ملخص الباك تست...")
                learning_system.adjust_weights()
                generate_backtest_report()
                learning_system.last_learning_time = datetime.now()

        elapsed = time.time() - start_time
        print(f"⏱️ استغرق المسح: {elapsed:.2f} ثانية")
        print(f"⏳ انتظار {SCAN_INTERVAL_MINUTES} دقيقة قبل الدورة التالية...")
        time.sleep(SCAN_INTERVAL_MINUTES * 60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️ تم إيقاف البرنامج.")
        session_manager.close_all()