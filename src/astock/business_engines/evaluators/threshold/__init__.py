"""
Threshold Evaluator Module (v2.0 纯净版)
========================================

阈值评估器模块 - 企业级 DDD + Protocol-based 架构。

✅ 重构完成 (2026-01-10):
- DDD 领域驱动设计
- Protocol-based 接口
- 工厂模式 + 自动发现
- 23条规则全部类化
- 统一错误处理
- 纯净架构 (无向后兼容层)

快速开始:
    >>> from evaluators.threshold import (
    ...     TrendContext, RuleEngine, get_default_factory
    ... )
    >>> engine = RuleEngine()
    >>> result = engine.evaluate(context)

作者: AStock Analysis System
日期: 2026-01-10
版本: 2.0.0
"""

# 领域模型
from .domain_models import (
    TrendContext,
    TrendMetrics,
    VolatilityMetrics,
    DeteriorationMetrics,
    InflectionMetrics,
    CyclicalMetrics,
    DataQualityMetrics,
    ReferenceMetric,
    TrendDirection,
    VolatilityRegime,
    CyclePhase,
    DeteriorationSeverity,
)

# Protocol 接口
from .protocols import (
    RuleProtocol,
    StrategyProtocol,
    RuleEngineProtocol,
    RuleFactoryProtocol,
)

# 结果数据类
from .results import (
    RuleResultImpl,
    StrategyResultImpl,
    EvaluationResultImpl,
    create_veto_result,
    create_penalty_result,
    create_bonus_result,
    create_info_result,
)

# 向后兼容别名
RuleResult = RuleResultImpl
StrategyResult = StrategyResultImpl
EvaluationResult = EvaluationResultImpl

# 工厂
from .factories import (
    RuleFactory,
    get_default_factory,
    reset_default_factory,
)

# 配置
from .rule_config import (
    RuleConfig,
    ScoringConfig,
    VetoThresholds,
    PenaltyThresholds,
    BonusThresholds,
    ValidationThresholds,
    RuleCategory,
    DEFAULT_CONFIG,
)

# 引擎
from .engine import (
    RuleEngine,
    TrendEvaluator,
    RuleExecutionSummary,
)

# 策略
from .strategies import (
    HighGrowthStrategy,
    TurnaroundStrategy,
    StableDividendStrategy,
    CyclicalBottomStrategy,
    MoatDefenseStrategy,
    create_all_strategies,
    get_default_strategies,
)

# 错误处理
from .warnings import (
    WarningLevel,
    WarningCode,
    EvaluatorWarning,
    WarningCollector,
    create_warning,
    log_warning,
)

# 业务配置
from .industry_config import (
    INDUSTRY_CATEGORY_MAP,
    CYCLICAL_INDUSTRIES,
    get_industry_category,
    is_cyclical_industry,
    get_category_thresholds,
    get_roic_thresholds,
    get_roiic_thresholds,
)

from .metric_thresholds import (
    MetricCategory,
    MetricThresholdConfig,
    METRIC_THRESHOLDS,
    get_metric_thresholds,
    get_metric_filter_config,
)


__all__ = [
    # 领域模型
    "TrendContext",
    "TrendMetrics",
    "VolatilityMetrics",
    "DeteriorationMetrics",
    "InflectionMetrics",
    "CyclicalMetrics",
    "DataQualityMetrics",
    "ReferenceMetric",
    "TrendDirection",
    "VolatilityRegime",
    "CyclePhase",
    "DeteriorationSeverity",

    # Protocol 接口
    "RuleProtocol",
    "StrategyProtocol",
    "RuleEngineProtocol",
    "RuleFactoryProtocol",

    # 结果数据类
    "RuleResultImpl",
    "StrategyResultImpl",
    "EvaluationResultImpl",
    "create_veto_result",
    "create_penalty_result",
    "create_bonus_result",
    "create_info_result",

    # 工厂
    "RuleFactory",
    "get_default_factory",
    "reset_default_factory",

    # 配置
    "RuleConfig",
    "DEFAULT_CONFIG",
    "ScoringConfig",
    "VetoThresholds",
    "PenaltyThresholds",
    "BonusThresholds",
    "ValidationThresholds",
    "RuleCategory",

    # 引擎
    "RuleEngine",
    "TrendEvaluator",
    "RuleExecutionSummary",

    # 策略
    "HighGrowthStrategy",
    "TurnaroundStrategy",
    "StableDividendStrategy",
    "CyclicalBottomStrategy",
    "MoatDefenseStrategy",
    "create_all_strategies",
    "get_default_strategies",

    # 错误处理
    "WarningLevel",
    "WarningCode",
    "EvaluatorWarning",
    "WarningCollector",
    "create_warning",
    "log_warning",

    # 业务配置
    "INDUSTRY_CATEGORY_MAP",
    "CYCLICAL_INDUSTRIES",
    "get_industry_category",
    "is_cyclical_industry",
    "get_category_thresholds",
    "get_roic_thresholds",
    "get_roiic_thresholds",
    "MetricCategory",
    "MetricThresholdConfig",
    "METRIC_THRESHOLDS",
    "get_metric_thresholds",
    "get_metric_filter_config",
]
