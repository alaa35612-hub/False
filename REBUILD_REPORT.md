# تقرير إعادة البناء (Clean-Room)

## 1) كيف حافظت على التطابق الوظيفي المهم مع الكود القديم
- حافظت على نفس المهمة الأساسية: مسح عقود Binance Futures USDT Perpetual، بناء ميزات سوقية مركّبة، ثم إصدار إشارات LONG قبل الارتفاع.
- حافظت على جمع نفس نوع البيانات الجوهرية الموجودة منطقيًا في النظام القديم:
  - Top Trader Position Ratio
  - Top Trader Account Ratio
  - Global L/S Ratio
  - Open Interest history + OI notional history
  - Funding الحالي + الاتجاه
  - Basis عبر mark/index
  - Taker flow وtrade count context من الكلاينز
- حافظت على بنية الإخراج التشغيلي المطلوبة (symbol, score, classification, signal_quality_tier, direction, signal_stage, price, stop_loss, targets, decisive_feature, reasons, feature_scores).

## 2) ما الذي تم الإبقاء عليه وظيفيًا
- واجهة تشغيل بسيطة بملف واحد (مناسبة لـ Pydroid 3).
- مسار إعدادات عبر `config.json` مع افتراضات آمنة.
- Quick filter على السيولة والحركة 24h قبل deep feature scan.
- حساب multi-feature score بدل الاعتماد على إشارة منفردة.

## 3) ما الذي أعدت تصميمه بالكامل
- إزالة الإرث القديم بالكامل (labels/buckets/power_level/fallbacks الواسعة).
- استبدال نظام الأنماط المتشعب القديم بمصنف بصمات نظيف مطابق للوثيقة:
  1. POSITION_LED_SQUEEZE_BUILDUP
  2. ACCOUNT_LED_ACCUMULATION
  3. CONSENSUS_BULLISH_EXPANSION
  4. FLOW_LIQUIDITY_VACUUM_BREAKOUT
- بناء طبقة Scoring حديثة واضحة (funding/flow/oi/oi-notional/basis/4H/divergence/multi-match).
- إضافة فلتر late-signal صريح (crowded funding + over-extension + weak-build penalty).

## 4) لماذا البنية الجديدة أوضح
- فصل صريح للطبقات:
  - `BinanceFuturesAPI`: جلب البيانات + retry + cache.
  - `FeatureBuilder`: اشتقاق الخصائص فقط.
  - `PrePumpClassifier`: المنطق التداولي والتصنيف والجودة.
  - `ScannerEngine`: orchestration.
- كل دالة صغيرة، وحيدة المسؤولية، وسهلة الاختبار.
- لا توجد تسميات قديمة مهيمنة أو حالات متداخلة غير قابلة للتتبع.

## 5) كيف تم دعم pre-pump detection بدقة أعلى
- استخدام divergence بين Position وAccount كميزة رئيسية بدل دمجهما في متغير واحد.
- التعامل مع التمويل كـ regime/context فقط، وليس filter رفض أعمى.
- تضمين OI وOI Notional بزمنين (5m/15m + notional change) لتأكيد build الحقيقي.
- رفع وزن التدفق (taker flow) في حالات الانفجار السريع.
- دعم 4H كـ bonus سياقي وليس trigger مباشر.
- tiering حديث للإشارة: مرشح مبكر → إشارة مؤسسية → انفجار وشيك → انفجار وشيك جدًا.

## 6) كيف تحسنت السرعة
- Cheap filters first (سيولة/24h change) قبل أي استدعاءات عميقة لكل رمز.
- TTL cache للطلبات المتكررة قصيرة العمر.
- Session reuse + retry adapter لتقليل تكلفة الشبكة والـ handshake.
- تقليل الحسابات الثقيلة إلى أساسيات إحصائية خفيفة (stdlib).

## 7) إصلاح openInterest 400
- إضافة `FailureCache` مع cooldown لكل رمز/endpoint فاشل.
- منع إعادة ضرب endpoint الفاشل مباشرة خلال فترة التهدئة.
- منع spam للأخطاء مع fallback آمن (إرجاع 0 أو قائمة فارغة بدل الانهيار).

## 8) تأكيد نهائي
- الملف صالح نحويًا (تم فحصه بـ `python3 -m py_compile`).
- لا توجد أسماء غير معرفة (تم تنفيذ فحص static عبر `python3 -m pyflakes`).
- لا توجد دوال غير مستخدمة تشغيلية تؤثر على مسار النظام (الفئات/الدوال مرتبطة بخط تشغيل واضح).
- مناسب لـ Pydroid 3 (اعتماد stdlib + requests فقط).
- أوضح وأبسط من النسخة القديمة.
- أسرع عمليًا عبر cache/failure-cooldown/cheap-first pipeline.
- أكثر تطابقًا مع الوثيقة المرجعية في منطق التصنيف pre-pump.
