"""
Threshold Evaluator Module (v2.0 重构版)
========================================

阈值评估器模块，提供基于规则的趋势质量评估。

重构变更 (2025-12-19):
- 统一参数配置到 rule_config.py
- 规则整合从 36 条降至 23 条
- 单一引擎架构 (移除双引擎)
- 规则按类型分组到 rules/ 目录

模块结构:
- rule_config.py: 统一参数配置中心
- rules/: 规则实现
  - veto.py: 6 条一票否决规则
  - penalty.py: 8 条扣分规则
  - bonus.py: 5 条加分规则
  - validation.py: 4 条交叉验证规则
- engine.py: 统一规则引擎
- models.py: 数据模型
- strategies.py: 5 个投资策略
- context.py: 评估上下文
- industry_config.py: 行业配置
- metric_thresholds.py: 指标阈值配置

架构位置:
    ProbeOutputs (from analyzers/trend/probes)
        ↓
    RuleEngine (本模块)
        ↓
    EvaluationResult
        ↓
    Threshold Report

设计原则:
- 探针层 (analyzers/trend/probes): 纯数学计算，返回 ProbeOutputs
- 评估层 (本模块): 业务阈值判断，返回 Pass/Fail/Score
- 两层分离，探针可复用，阈值可配置
"""

# 引擎 (统一入口)
from .engine import (
    RuleEngine,
    TrendEvaluator,
    EvaluationResult,
    RuleOutcome,
    # 向后兼容别名
    ThresholdEvaluator,
    ThresholdEvaluatorConfig,
    TrendRuleEngine,
    trend_rule_engine,
)

# 数据模型
from .models import (
    StrategyMatchResult,
    # 向后兼容别名
    ThresholdEvaluationResult,
    RuleResult,
    RuleCategory,
    ThresholdRule,
)

# 统一配置
from .rule_config import (
    RuleConfig,
    ScoringConfig,
    VetoThresholds,
    PenaltyThresholds,
    BonusThresholds,
    ValidationThresholds,
    DEFAULT_CONFIG,
)

# 规则模块 (按类型分组)
from .rules import (
    ALL_RULES,
    ALL_VETO_RULES,
    ALL_PENALTY_RULES,
    ALL_BONUS_RULES,
    ALL_VALIDATION_RULES,
    # 单独导出便于测试
    Rule,
    RuleResult as NewRuleResult,
)

# 策略模块
from .strategies import (
    TrendStrategy,
    BaseStrategy,
    StrategyResult,
    HighGrowthStrategy,
    TurnaroundStrategy,
    StableDividendStrategy,
    CyclicalBottomStrategy,
    MoatDefenseStrategy,
    get_default_strategies,
    get_strategy_by_name,
)

# 上下文
from .context import (
    EvaluationContext,
    EvaluationContextBuilder,
)

# 配置模块
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
    # ===== 核心 API (推荐使用) =====
    # 引擎
    "RuleEngine",
    "TrendEvaluator",
    "EvaluationResult",
    "RuleOutcome",
    # 配置
    "RuleConfig",
    "DEFAULT_CONFIG",
    # 规则
    "ALL_RULES",
    "Rule",
    # 策略
    "get_default_strategies",
    "StrategyResult",

    # ===== 向后兼容 API =====
    "ThresholdEvaluator",
    "ThresholdEvaluatorConfig",
    "TrendRuleEngine",
    "trend_rule_engine",
    "ThresholdEvaluationResult",
    "RuleResult",
    "RuleCategory",
    "ThresholdRule",
    "StrategyMatchResult",

    # ===== 配置模块 =====
    "ScoringConfig",
    "VetoThresholds",
    "PenaltyThresholds",
    "BonusThresholds",
    "ValidationThresholds",

    # ===== 规则分组 =====
    "ALL_VETO_RULES",
    "ALL_PENALTY_RULES",
    "ALL_BONUS_RULES",
    "ALL_VALIDATION_RULES",

    # ===== 策略 =====
    "TrendStrategy",
    "BaseStrategy",
    "HighGrowthStrategy",
    "TurnaroundStrategy",
    "StableDividendStrategy",
    "CyclicalBottomStrategy",
    "MoatDefenseStrategy",
    "get_strategy_by_name",

    # ===== 上下文 =====
    "EvaluationContext",
    "EvaluationContextBuilder",

    # ===== 行业配置 =====
    "INDUSTRY_CATEGORY_MAP",
    "CYCLICAL_INDUSTRIES",
    "get_industry_category",
    "is_cyclical_industry",
    "get_category_thresholds",
    "get_roic_thresholds",
    "get_roiic_thresholds",

    # ===== 指标阈值 =====
    "MetricCategory",
    "MetricThresholdConfig",
    "METRIC_THRESHOLDS",
    "get_metric_thresholds",
    "get_metric_filter_config",
]
