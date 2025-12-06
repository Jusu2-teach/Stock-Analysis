"""
业务趋势分析子系统 (Business Trend Analysis)
=============================================

专业的财务指标趋势分析框架，支持：
- 7种分析探针（LogTrend, Robust, Inflection, Cyclical, Deterioration, Rolling, Volatility）
- 29条评分规则（否决、扣分、交叉验证、周期调整、加分）
- 指标类型适配器（自动调整分析参数）
- 行业差异化配置

核心组件：
- TrendAnalyzer: 趋势分析器主类
- MetricAdapter: 指标类型适配器
- TrendRuleEngine: 规则引擎
- MetricProfile: 指标特性配置

作者: AStock Analysis System
日期: 2025-12-06
"""

# 核心分析器
from .core import (
    TrendAnalyzer,
    MetricProbe,
    MetricProbeContext,
    get_default_metric_probes,
    TrendRuleEngine,
    trend_rule_engine,
    ConfigResolver,
    TrendEvaluator,
    TrendResultCollector,
    DEFAULT_TREND_RULES,
)

# 数据模型
from .models import (
    TrendAnalyzerConfig,
    TrendSeriesConfig,
    TrendSnapshot,
    TrendVector,
    TrendEvaluationResult,
    TrendContext,
    RuleResult,
)

# 配置
from .config import (
    trend_field_schema,
    get_default_config,
    TrendAnalysisConfig,
    get_filter_config,
    get_decline_thresholds,
    get_cyclical_thresholds,
)

# 指标档案与适配器
from .metric_profiles import (
    MetricCategory,
    MetricProfile,
    METRIC_PROFILES,
    get_metric_profile,
    detect_metric_category,
)

from .metric_adapter import (
    MetricAdapter,
    AdapterFactory,
    AdaptedConfig,
    create_metric_config_for_pipeline,
)

# 策略
from .strategies import (
    TrendStrategy,
    get_default_strategies,
)


__all__ = [
    # 核心分析器
    "TrendAnalyzer",
    "MetricProbe",
    "MetricProbeContext",
    "get_default_metric_probes",
    "TrendRuleEngine",
    "trend_rule_engine",
    "ConfigResolver",
    "TrendEvaluator",
    "TrendResultCollector",
    "DEFAULT_TREND_RULES",

    # 数据模型
    "TrendAnalyzerConfig",
    "TrendSeriesConfig",
    "TrendSnapshot",
    "TrendVector",
    "TrendEvaluationResult",
    "TrendContext",
    "RuleResult",

    # 配置
    "trend_field_schema",
    "get_default_config",
    "TrendAnalysisConfig",
    "get_filter_config",
    "get_decline_thresholds",
    "get_cyclical_thresholds",

    # 指标档案与适配器
    "MetricCategory",
    "MetricProfile",
    "METRIC_PROFILES",
    "get_metric_profile",
    "detect_metric_category",
    "MetricAdapter",
    "AdapterFactory",
    "AdaptedConfig",
    "create_metric_config_for_pipeline",

    # 策略
    "TrendStrategy",
    "get_default_strategies",
]