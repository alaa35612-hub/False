#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
الماسح المتوازي المتقدم - الإصدار النهائي مع التنبيهات الفورية وجميع التحسينات
================================================================================
نسخة محسنة بمعايير أكثر صرامة وجودة إشارات عالية.
تم إضافة: توحيد الأسباب، رفع عتبة الإشارات إلى 70، شرط APR لنمط PRE_CLOSE_SQUEEZE،
وتجميع الأنماط المتعددة لنفس الرمز كفرصة دخول عالية.
وتصحيح أخطاء HTTP 400 والمتغيرات المحلية.
وتحديث نمط SMART_MONEY_DIVERGENCE ليكون أكثر مرونة مع التمويل.
وتسريع الأداء عبر زيادة العمال وإزالة التأخيرات وتحسين التخزين المؤقت.
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
        "SMART_MONEY_DIVERGENCE": 0.35
    },

    # إعدادات إدارة المخاطر
    "RISK_PER_TRADE": 2.0,
    "ATR_PERIOD": 14,
    "ATR_MULTIPLIER_STOP": 2.0,
    "ATR_MULTIPLIER_TAKE_PROFIT": 3.0,

    # إعدادات قاعدة البيانات
    "DB_PATH": "signals.db",
    "SAVE_SIGNALS": True,

    # عتبات إضافية
    "MIN_APR_PRE_CLOSE": 200,            # أقل APR لقبول نمط PRE_CLOSE_SQUEEZE
    "SIGNAL_MIN_SCORE": 70               # أقل درجة لقبول الإشارة (تجاهل الأقل)
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
MIN_APR_PRE_CLOSE = CONFIG.get("MIN_APR_PRE_CLOSE", 200)
SIGNAL_MIN_SCORE = CONFIG.get("SIGNAL_MIN_SCORE", 70)

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
    data = fetch_json(f"{BASE_URL}/fapi/v1/openInterest", {'symbol': symbol}, use_cache=False, weight=1)
    if data is None:
        # إذا فشل الطلب، نسجل الرمز كغير صالح لمنع المحاولات المتكررة
        mark_symbol_invalid(symbol)
        return None
    return float(data['openInterest']) if data else None

def get_funding_rate(symbol):
    data = fetch_json(f"{BASE_URL}/fapi/v1/premiumIndex", {'symbol': symbol}, use_cache=True, weight=1, cache_ttl=5)  # 5 ثوانٍ
    if data:
        return float(data['lastFundingRate']) * 100, int(data['nextFundingTime'])
    return None, None

def get_top_long_short_ratio(symbol):
    data = fetch_json(f"{BASE_URL}/futures/data/topLongShortPositionRatio",
                      {'symbol': symbol, 'period': '5m', 'limit': 1}, use_cache=True, weight=1, cache_ttl=10)  # 10 ثوانٍ
    if data and len(data) > 0:
        return float(data[0]['longShortRatio']), float(data[0]['longAccount']), float(data[0]['shortAccount'])
    return None, None, None

def get_position_ratio_history(symbol, period='5m', limit=12):
    """جلب تاريخ نسبة مراكز كبار المتداولين (Position Ratio) مع ترتيب زمني (الأقدم أولاً)."""
    cache_key = f"pos_ratio_hist_{symbol}_{period}_{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    data = fetch_json(f"{BASE_URL}/futures/data/topLongShortPositionRatio",
                      {'symbol': symbol, 'period': period, 'limit': limit}, use_cache=False, weight=1)
    if data:
        # البيانات تأتي من الأحدث إلى الأقدم، لذلك نعكسها لتصبح من الأقدم إلى الأحدث
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
        # البيانات تأتي من الأحدث إلى الأقدم، لذلك نعكسها
        ratios = [float(d['longAccount']) for d in data][::-1]
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
            'taker_buy_volume': [float(k[9]) for k in data]
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
            return None
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
        return {
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
def assess_signal_power(symbol, base_score, pattern_data):
    power_score = 0
    power_reasons = []
    power_level = "عادية"
    reasons_set = set()  # لتتبع الأسباب المضافة

    try:
        _, tr_long, _ = get_top_long_short_ratio(symbol)
        if tr_long and tr_long > STRONG_WHALE_RATIO:
            power_score += 20
            reason = f"🐋 هيمنة كبار {tr_long:.1f}%"
            if reason not in reasons_set:
                power_reasons.append(reason)
                reasons_set.add(reason)

        funding_rate, _ = get_funding_rate(symbol)
        if funding_rate and funding_rate < EXTREME_FUNDING_SHORT:
            power_score += 15
            reason = f"🔥 تمويل سلبي شديد {funding_rate:+.3f}%"
            if reason not in reasons_set:
                power_reasons.append(reason)
                reasons_set.add(reason)

        oi_hist = get_oi_history(symbol, period='15m', limit=2)
        if len(oi_hist) >= 2:
            oi_change = pct_change(oi_hist[-2], oi_hist[-1])
            if oi_change > OI_SURGE_THRESHOLD:
                power_score += 15
                reason = f"📈 OI قفز {oi_change:+.1f}%"
                if reason not in reasons_set:
                    power_reasons.append(reason)
                    reasons_set.add(reason)

        ticker = get_ticker_24hr_one_symbol(symbol)
        if ticker:
            current_volume = float(ticker['volume'])
            avg_volume = get_historical_avg_volume(symbol, days=7)
            if avg_volume and avg_volume > 0:
                vol_ratio = current_volume / avg_volume
                if vol_ratio > VOLUME_EXPLOSION:
                    power_score += 15
                    reason = f"💥 حجم {vol_ratio:.1f}x المتوسط"
                    if reason not in reasons_set:
                        power_reasons.append(reason)
                        reasons_set.add(reason)

        basis = get_basis(symbol)
        if basis < BASIS_EXTREME_NEGATIVE:
            power_score += 10
            reason = f"📉 أساس سالب متطرف {basis:+.2f}%"
            if reason not in reasons_set:
                power_reasons.append(reason)
                reasons_set.add(reason)
        elif basis > BASIS_EXTREME_POSITIVE:
            power_score += 10
            reason = f"📈 أساس موجب متطرف {basis:+.2f}%"
            if reason not in reasons_set:
                power_reasons.append(reason)
                reasons_set.add(reason)

        # دمج مؤشرات الفريم اليومي
        daily_ratios = get_daily_top_ratio(symbol, limit=DAILY_OI_TREND_DAYS)
        if len(daily_ratios) >= DAILY_OI_TREND_DAYS:
            avg_daily_ratio = mean(daily_ratios)
            if avg_daily_ratio > DAILY_MIN_AVG_TOP_RATIO:
                power_score += POWER_ADD_DAILY_BOOST
                reason = f"📅 تراكم يومي {avg_daily_ratio:.1f}%"
                if reason not in reasons_set:
                    power_reasons.append(reason)
                    reasons_set.add(reason)

        # تطبيق مضاعف حالة السوق
        regime_multiplier = hmm_detector.get_regime_multiplier() if ENABLE_HMM else 1.0
        total_score = (base_score + power_score) * regime_multiplier

        if total_score >= POWER_CRITICAL_THRESHOLD:
            power_level = "حرجة 🔥"
        elif total_score >= POWER_IMPORTANT_THRESHOLD:
            power_level = "مهمة ⚡"
        else:
            power_level = "عادية"

    except Exception as e:
        if DEBUG:
            print(f"⚠️ خطأ في تقييم القوة لـ {symbol}: {e}")

    return power_score, power_level, power_reasons

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

    def evaluate_signals(self):
        cutoff_time = datetime.now() - timedelta(hours=LEARNING_INTERVAL_HOURS)
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.execute("""
                SELECT id, symbol, pattern, price, direction, timestamp 
                FROM signals 
                WHERE evaluated = 0 AND timestamp < ?
            """, (cutoff_time.isoformat(),))
            signals = cur.fetchall()

        if len(signals) < MIN_SIGNALS_FOR_LEARNING:
            return

        for sig in signals:
            sig_id, symbol, pattern, price, direction, ts = sig
            current_price = get_mark_price(symbol)
            if current_price is None:
                continue
            change = (current_price - price) / price * 100
            if direction == 'UP':
                success = 1 if change > 5 else 0
            else:  # DOWN
                success = 1 if change < -5 else 0

            with sqlite3.connect(DB_PATH) as conn:
                conn.execute("UPDATE signals SET evaluated = 1, outcome = ?, price_after = ? WHERE id = ?",
                             (success, current_price, sig_id))

            self.pattern_performance[pattern]['total'] += 1
            self.pattern_performance[pattern]['success'] += success
            self.pattern_performance[pattern]['score_sum'] += abs(change)

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

    candidates = []
    # تعبير منتظم للتحقق من الرموز اللاتينية (أحرف وأرقام و _ فقط)
    valid_symbol_pattern = re.compile(r'^[A-Z0-9_]+$')

    for t in tickers:
        symbol = t['symbol']

        # استبعاد الرموز التي تحتوي على أحرف غير لاتينية
        if not valid_symbol_pattern.match(symbol):
            if DEBUG:
                print(f"⚠️ استبعاد رمز غير لاتيني: {symbol}")
            continue

        volume = float(t['quoteVolume'])
        price_change_24h = float(t['priceChangePercent'])
        avg_vol = get_historical_avg_volume(symbol, days=7)
        vol_ok = (avg_vol is not None and volume / avg_vol >= VOLUME_TO_AVG_RATIO_MIN) if avg_vol else True

        # التحقق من الحد الأدنى لحجم التداول بالدولار
        if volume < MIN_VOLUME_USD:
            if DEBUG:
                print(f"⚠️ استبعاد {symbol} بسبب حجم منخفض: {volume:.0f} < {MIN_VOLUME_USD}")
            continue

        # إذا كان الارتفاع كبيرًا جدًا (أكثر من 8% في 24 ساعة)، نتجاوز شرط الحجم
        if price_change_24h >= SKYROCKET_THRESHOLD:
            candidates.append(symbol)
            continue

        if volume >= MIN_VOLUME_24H and price_change_24h >= MIN_PRICE_CHANGE_24H and vol_ok:
            candidates.append(symbol)

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
                'direction': 'UP',
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
            return None

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
        if apr < MIN_APR_PRE_CLOSE:
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
    الكشف عن تباين الأموال الذكية باستخدام الفرق بين نسب المراكز والحسابات.
    نسخة محسنة مع مرونة أكبر في شرط التمويل وإضافة ضغط الشراء.
    """
    try:
        # 1. جلب نسبة المراكز والحسابات
        pos_ratio, acc_long, _ = get_top_long_short_ratio(symbol)  # acc_long هي % الحسابات الطويلة
        if pos_ratio is None or acc_long is None:
            return None

        # تحويل نسبة المراكز إلى نسبة مئوية طويلة
        if pos_ratio <= 0:
            return None
        long_pos_pct = (pos_ratio / (1 + pos_ratio)) * 100

        # 2. حساب التباين
        divergence = long_pos_pct - acc_long
        if divergence < 10:  # عتبة قابلة للتعديل
            return None

        # 3. فحص OI
        oi_current = get_open_interest(symbol)
        oi_hist = get_oi_history(symbol, period='15m', limit=2)
        if not oi_current or len(oi_hist) < 2 or oi_hist[-2] == 0:
            return None
        oi_change = pct_change(oi_hist[-2], oi_current)

        if abs(oi_change) < MIN_ABS_OI_CHANGE:
            return None

        # 4. تحليل ضغط الشراء (taker volume)
        buy_volume_ratio = None
        ticker = get_ticker_24hr_one_symbol(symbol)
        if ticker:
            buy_vol = float(ticker.get('takerBuyBaseVolume', 0))
            sell_vol = float(ticker.get('takerSellBaseVolume', 0))
            if buy_vol + sell_vol > 0:
                buy_volume_ratio = buy_vol / (buy_vol + sell_vol)

        # 5. شروط أكثر مرونة:
        #    - التمويل مقبول إذا كان <= 0.05% (بدلاً من 0.01%) أو إذا كان التباين كبيراً جداً (>20)
        #    - أو أن حجم الشراء يغلب بشكل كبير (> 60%) كدليل على ضغط شراء قوي
        funding_rate, _ = get_funding_rate(symbol)
        funding_ok = (funding_rate is not None and funding_rate <= 0.05)
        strong_divergence = divergence >= 20
        strong_buy_pressure = buy_volume_ratio and buy_volume_ratio > 0.6

        if not (funding_ok or strong_divergence or strong_buy_pressure):
            return None

        # 6. فحص تغير السعر (نتجنب القمم)
        price = get_mark_price(symbol)
        closes_1h = get_klines(symbol, '1h', 2)
        if not closes_1h or len(closes_1h['close']) < 2:
            return None
        price_change_1h = pct_change(closes_1h['close'][-2], closes_1h['close'][-1])
        if price_change_1h > 8:  # تجاوز الارتفاع المبكر
            return None

        # 7. حساب الدرجة
        score = min(60 + divergence + oi_change, 100)
        reasons = [
            f"🧠 تباين المراكز عن الحسابات: {divergence:.1f} نقطة",
            f"OI زاد {oi_change:+.1f}%",
            f"تمويل {funding_rate:+.3f}%"
        ]
        if buy_volume_ratio:
            reasons.append(f"📊 ضغط شراء {buy_volume_ratio:.1%}")

        return {
            'symbol': symbol,
            'score': score,
            'pattern': 'SMART_MONEY_DIVERGENCE',
            'direction': 'UP',
            'price': price,
            'reasons': reasons,
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        if DEBUG:
            print(f"⚠️ خطأ في smart_money_divergence_v2 لـ {symbol}: {e}")
        return None

# =============================================================================
# قاموس الترجمة العربية (محدث)
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
        'default_direction': 'شراء',
        'emoji': '🐻'
    },
    'WHALE_MOMENTUM': {
        'name': 'زخم الحيتان',
        'default_direction': 'شراء',
        'emoji': '🐋'
    },
    'PRE_CLOSE_SQUEEZE': {
        'name': 'ما قبل الإغلاق',
        'default_direction': 'شراء',
        'emoji': '⏰'
    },
    'RETAIL_TRAP': {
        'name': 'فخ التجزئة',
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
    }
}

# =============================================================================
# قاعدة بيانات الإشارات
# =============================================================================
class SignalDatabase:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
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
                    power_level TEXT,
                    stop_loss REAL,
                    take_profit1 REAL,
                    take_profit2 REAL,
                    evaluated INTEGER DEFAULT 0,
                    outcome INTEGER DEFAULT 0,
                    price_after REAL
                )
            """)
            columns_to_add = [
                ("pattern", "TEXT"),
                ("score", "REAL"),
                ("power_level", "TEXT"),
                ("stop_loss", "REAL"),
                ("take_profit1", "REAL"),
                ("take_profit2", "REAL"),
                ("evaluated", "INTEGER DEFAULT 0"),
                ("outcome", "INTEGER DEFAULT 0"),
                ("price_after", "REAL")
            ]
            for col_name, col_def in columns_to_add:
                try:
                    conn.execute(f"ALTER TABLE signals ADD COLUMN {col_name} {col_def}")
                except sqlite3.OperationalError:
                    pass

    def save_signal(self, signal_data):
        if not SAVE_SIGNALS:
            return
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO signals (timestamp, symbol, pattern, score, price, direction, reasons, power_level, stop_loss, take_profit1, take_profit2)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal_data['timestamp'],
                signal_data['symbol'],
                signal_data['pattern'],
                signal_data['score'],
                signal_data.get('price', 0),
                signal_data.get('direction', 'UP'),
                json.dumps(signal_data.get('reasons', [])),
                signal_data.get('power_level', 'عادية'),
                signal_data.get('stop_loss', 0),
                signal_data.get('take_profit1', 0),
                signal_data.get('take_profit2', 0)
            ))

    def get_un_evaluated_signals(self, hours_ago=24):
        cutoff = (datetime.now() - timedelta(hours=hours_ago)).isoformat()
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute("SELECT id, symbol, pattern, price, direction, timestamp FROM signals WHERE evaluated = 0 AND timestamp < ?", (cutoff,))
            return cur.fetchall()

db = SignalDatabase()

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

# =============================================================================
# دالة إرسال إشارة فردية
# =============================================================================
def send_individual_signal(signal_data):
    """إرسال تنبيه فوري عند اكتشاف إشارة جديدة أو محدثة."""
    if signal_data['score'] < SIGNAL_MIN_SCORE:
        return  # تجاهل الإشارات الضعيفة

    pattern_key = signal_data['pattern']
    pattern_info = PATTERN_ARABIC.get(pattern_key, {
        'name': pattern_key,
        'default_direction': 'غير معروف',
        'emoji': '🔹'
    })
    emoji = pattern_info['emoji']
    pattern_name_ar = pattern_info['name']
    direction = signal_data.get('direction', 'UP')
    if direction == 'UP':
        direction_ar = '🟢 شراء'
    elif direction == 'DOWN':
        direction_ar = '🔴 بيع'
    else:
        direction_ar = pattern_info['default_direction']

    reasons = "، ".join(signal_data.get('reasons', []))
    stop_loss = signal_data.get('stop_loss') or 0
    tp1 = signal_data.get('take_profit1') or 0
    tp2 = signal_data.get('take_profit2') or 0

    # إضافة إشارة إذا كان هناك أنماط متعددة (سيتم إضافتها في deep_scan)
    multi_pattern_hint = signal_data.get('multi_pattern_hint', '')

    message = (
        f"🔔 *إشارة جديدة: {signal_data['symbol']}* {multi_pattern_hint}\n"
        f"{emoji} النمط: {pattern_name_ar} [{signal_data.get('power_level', 'عادية')}]\n"
        f"   الدرجة: {signal_data['score']}\n"
        f"   الاتجاه: {direction_ar}\n"
        f"   السعر: {signal_data['price']:.6f}\n"
        f"   وقف الخسارة: {stop_loss:.6f} | الهدف1: {tp1:.6f} | الهدف2: {tp2:.6f}\n"
        f"   أسباب: {reasons}"
    )
    send_telegram(message)

# =============================================================================
# المسح العميق (معدل لإرسال التنبيهات الفورية)
# =============================================================================
def deep_scan(candidates, learning_system):
    all_results = []  # جميع الإشارات
    symbol_signals = defaultdict(list)  # لتجميع الإشارات لكل رمز
    print(f"🔍 تحليل عميق لـ {len(candidates)} عملة...")

    clear_invalid_symbols()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_long = {executor.submit(detect_long_term_accumulation, sym): sym for sym in candidates if not is_symbol_invalid(sym)}
        long_results = []
        remaining = []
        for future in as_completed(future_long):
            sym = future_long[future]
            try:
                res = future.result(timeout=30)
                if res:
                    print(f"   📅 {sym}: {res['score']} LONG_TERM_ACCUMULATION")
                    # تقييم القوة وإدارة المخاطر
                    power_score, power_level, power_reasons = assess_signal_power(res['symbol'], res['score'], {})
                    res['power_level'] = power_level
                    res['reasons'].extend(power_reasons)
                    stop_loss, tp1, tp2 = calculate_risk_management(res['symbol'], res['price'], res['direction'])
                    res['stop_loss'] = stop_loss
                    res['take_profit1'] = tp1
                    res['take_profit2'] = tp2
                    long_results.append(res)
                    symbol_signals[sym].append(res)
                else:
                    remaining.append(sym)
                # لا تأخير بين الرموز
            except Exception as e:
                print(f"   ❌ {sym}: {str(e)[:50]}")
                if "400" in str(e) or "404" in str(e):
                    mark_symbol_invalid(sym)
                remaining.append(sym)

        if remaining:
            future_pre = {executor.submit(detect_pre_pump, sym): sym for sym in remaining if not is_symbol_invalid(sym)}
            pre_results = []
            still_remaining = []
            for future in as_completed(future_pre):
                sym = future_pre[future]
                try:
                    res = future.result(timeout=30)
                    if res:
                        print(f"   🕒 {sym}: {res['score']} PRE_PUMP")
                        power_score, power_level, power_reasons = assess_signal_power(res['symbol'], res['score'], {})
                        res['power_level'] = power_level
                        res['reasons'].extend(power_reasons)
                        stop_loss, tp1, tp2 = calculate_risk_management(res['symbol'], res['price'], res['direction'])
                        res['stop_loss'] = stop_loss
                        res['take_profit1'] = tp1
                        res['take_profit2'] = tp2
                        pre_results.append(res)
                        symbol_signals[sym].append(res)
                    else:
                        still_remaining.append(sym)
                except Exception as e:
                    print(f"   ❌ {sym}: {str(e)[:50]}")
                    if "400" in str(e) or "404" in str(e):
                        mark_symbol_invalid(sym)
                    still_remaining.append(sym)

            if still_remaining:
                future_whale = {executor.submit(detect_whale_setup, sym): sym for sym in still_remaining if not is_symbol_invalid(sym)}
                whale_results = []
                still_remaining2 = []
                for future in as_completed(future_whale):
                    sym = future_whale[future]
                    try:
                        res = future.result(timeout=30)
                        if res:
                            print(f"   🐋 {sym}: {res['score']} WHALE_SETUP")
                            power_score, power_level, power_reasons = assess_signal_power(res['symbol'], res['score'], {})
                            res['power_level'] = power_level
                            res['reasons'].extend(power_reasons)
                            stop_loss, tp1, tp2 = calculate_risk_management(res['symbol'], res['price'], res['direction'])
                            res['stop_loss'] = stop_loss
                            res['take_profit1'] = tp1
                            res['take_profit2'] = tp2
                            whale_results.append(res)
                            symbol_signals[sym].append(res)
                        else:
                            still_remaining2.append(sym)
                    except Exception as e:
                        print(f"   ❌ {sym}: {str(e)[:50]}")
                        if "400" in str(e) or "404" in str(e):
                            mark_symbol_invalid(sym)
                        still_remaining2.append(sym)

                if still_remaining2:
                    individual_results = []
                    for sym in still_remaining2:
                        if is_symbol_invalid(sym):
                            continue
                        bear = bear_squeeze_pattern(sym)
                        if bear:
                            print(f"   🐻 {sym}: {bear['score']} BEAR_SQUEEZE")
                            power_score, power_level, power_reasons = assess_signal_power(bear['symbol'], bear['score'], {})
                            bear['power_level'] = power_level
                            bear['reasons'].extend(power_reasons)
                            stop_loss, tp1, tp2 = calculate_risk_management(bear['symbol'], bear['price'], bear['direction'])
                            bear['stop_loss'] = stop_loss
                            bear['take_profit1'] = tp1
                            bear['take_profit2'] = tp2
                            individual_results.append(bear)
                            symbol_signals[sym].append(bear)
                            continue
                        whale = whale_momentum_pattern(sym)
                        if whale:
                            print(f"   🐋 {sym}: {whale['score']} WHALE_MOMENTUM")
                            power_score, power_level, power_reasons = assess_signal_power(whale['symbol'], whale['score'], {})
                            whale['power_level'] = power_level
                            whale['reasons'].extend(power_reasons)
                            stop_loss, tp1, tp2 = calculate_risk_management(whale['symbol'], whale['price'], whale['direction'])
                            whale['stop_loss'] = stop_loss
                            whale['take_profit1'] = tp1
                            whale['take_profit2'] = tp2
                            individual_results.append(whale)
                            symbol_signals[sym].append(whale)
                            continue
                        pre = pre_close_squeeze_pattern(sym)
                        if pre:
                            print(f"   ⏰ {sym}: {pre['score']} PRE_CLOSE_SQUEEZE")
                            power_score, power_level, power_reasons = assess_signal_power(pre['symbol'], pre['score'], {})
                            pre['power_level'] = power_level
                            pre['reasons'].extend(power_reasons)
                            stop_loss, tp1, tp2 = calculate_risk_management(pre['symbol'], pre['price'], pre['direction'])
                            pre['stop_loss'] = stop_loss
                            pre['take_profit1'] = tp1
                            pre['take_profit2'] = tp2
                            individual_results.append(pre)
                            symbol_signals[sym].append(pre)
                            continue
                        retail = retail_trap_pattern(sym)
                        if retail:
                            print(f"   🔻 {sym}: {retail['score']} RETAIL_TRAP")
                            power_score, power_level, power_reasons = assess_signal_power(retail['symbol'], retail['score'], {})
                            retail['power_level'] = power_level
                            retail['reasons'].extend(power_reasons)
                            stop_loss, tp1, tp2 = calculate_risk_management(retail['symbol'], retail['price'], retail['direction'])
                            retail['stop_loss'] = stop_loss
                            retail['take_profit1'] = tp1
                            retail['take_profit2'] = tp2
                            individual_results.append(retail)
                            symbol_signals[sym].append(retail)
                            continue
                        # أنماط الارتفاعات
                        strong_up = detect_strong_uptrend(sym)
                        if strong_up:
                            print(f"   📈 {sym}: {strong_up['score']} STRONG_UPTREND")
                            power_score, power_level, power_reasons = assess_signal_power(strong_up['symbol'], strong_up['score'], {})
                            strong_up['power_level'] = power_level
                            strong_up['reasons'].extend(power_reasons)
                            stop_loss, tp1, tp2 = calculate_risk_management(strong_up['symbol'], strong_up['price'], strong_up['direction'])
                            strong_up['stop_loss'] = stop_loss
                            strong_up['take_profit1'] = tp1
                            strong_up['take_profit2'] = tp2
                            individual_results.append(strong_up)
                            symbol_signals[sym].append(strong_up)
                            continue
                        skyrocket = detect_skyrocket_enhanced(sym)
                        if skyrocket:
                            print(f"   💥 {sym}: {skyrocket['score']} SKYROCKET")
                            power_score, power_level, power_reasons = assess_signal_power(skyrocket['symbol'], skyrocket['score'], {})
                            skyrocket['power_level'] = power_level
                            skyrocket['reasons'].extend(power_reasons)
                            stop_loss, tp1, tp2 = calculate_risk_management(skyrocket['symbol'], skyrocket['price'], skyrocket['direction'])
                            skyrocket['stop_loss'] = stop_loss
                            skyrocket['take_profit1'] = tp1
                            skyrocket['take_profit2'] = tp2
                            individual_results.append(skyrocket)
                            symbol_signals[sym].append(skyrocket)
                            continue
                        # نمط تباين الأموال الذكية
                        smart = detect_smart_money_divergence(sym)
                        if smart:
                            print(f"   🧠 {sym}: {smart['score']} SMART_MONEY_DIVERGENCE")
                            power_score, power_level, power_reasons = assess_signal_power(smart['symbol'], smart['score'], {})
                            smart['power_level'] = power_level
                            smart['reasons'].extend(power_reasons)
                            stop_loss, tp1, tp2 = calculate_risk_management(smart['symbol'], smart['price'], smart['direction'])
                            smart['stop_loss'] = stop_loss
                            smart['take_profit1'] = tp1
                            smart['take_profit2'] = tp2
                            individual_results.append(smart)
                            symbol_signals[sym].append(smart)
                            continue
                    # جمع كل النتائج
                    all_results = (long_results + pre_results + whale_results + individual_results)
                else:
                    all_results = (long_results + pre_results + whale_results)
            else:
                all_results = (long_results + pre_results)
        else:
            all_results = long_results

    # بعد جمع كل الإشارات، نضيف تعليق "فرصة دخول عالية" إذا تعددت الأنماط
    final_results = []
    for sym, sigs in symbol_signals.items():
        if len(sigs) > 1:
            # هناك أكثر من نمط لنفس الرمز
            for sig in sigs:
                sig['multi_pattern_hint'] = "🔥 (فرصة دخول عالية)"
                final_results.append(sig)
        else:
            final_results.extend(sigs)

    return final_results

# =============================================================================
# التنبيهات عبر تليجرام (تقرير نهائي)
# =============================================================================
def send_telegram(message):
    if TELEGRAM_TOKEN == "YOUR_BOT_TOKEN":
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
    lines = ["🚀 *تقرير الماسح المتكامل*\n"]
    for r in results[:10]:
        pattern_key = r['pattern']
        pattern_info = PATTERN_ARABIC.get(pattern_key, {
            'name': pattern_key,
            'default_direction': 'غير معروف',
            'emoji': '🔹'
        })
        emoji = pattern_info['emoji']
        pattern_name_ar = pattern_info['name']
        direction = r.get('direction', 'UP')
        if direction == 'UP':
            direction_ar = '🟢 شراء'
        elif direction == 'DOWN':
            direction_ar = '🔴 بيع'
        else:
            direction_ar = pattern_info['default_direction']

        reasons = "، ".join(r.get('reasons', []))
        stop_loss = r.get('stop_loss') or 0
        tp1 = r.get('take_profit1') or 0
        tp2 = r.get('take_profit2') or 0
        multi_hint = r.get('multi_pattern_hint', '')

        lines.append(
            f"{emoji} *{r['symbol']}*: {r['score']} ({pattern_name_ar}) {multi_hint} [{r.get('power_level', 'عادية')}]\n"
            f"   الاتجاه: {direction_ar}\n"
            f"   السعر: {r['price']:.6f}\n"
            f"   وقف الخسارة: {stop_loss:.6f} | الهدف1: {tp1:.6f} | الهدف2: {tp2:.6f}\n"
            f"   أسباب: {reasons}\n"
        )
        db.save_signal(r)

    full = "\n".join(lines)
    print(full)
    send_telegram(full)

# =============================================================================
# الحلقة الرئيسية
# =============================================================================
def main():
    print("="*70)
    print("الماسح المتوازي المتقدم - الإصدار النهائي مع التنبيهات الفورية".center(70))
    print("="*70)

    all_symbols = get_all_usdt_perpetuals()
    print(f"✅ إجمالي العقود: {len(all_symbols)}")

    if ENABLE_HMM:
        print("🔄 تدريب نموذج HMM...")
        hmm_detector.fit_model()
        regime = hmm_detector.get_current_regime()
        print(f"📊 حالة السوق الحالية: {regime['regime_name']}")

    learning_system = LearningSystem(db)

    while True:
        start_time = time.time()
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] بدء الدورة...")

        candidates = quick_filter()
        if not candidates:
            print("⚠️ لا توجد مرشحات بعد الفلترة.")
        else:
            results = deep_scan(candidates, learning_system)
            generate_report(results)

        if ENABLE_LEARNING:
            hours_since_last = (datetime.now() - learning_system.last_learning_time).total_seconds() / 3600
            if hours_since_last >= LEARNING_INTERVAL_HOURS:
                print("🔄 جاري تقييم الإشارات وتحديث الأوزان...")
                learning_system.adjust_weights()
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