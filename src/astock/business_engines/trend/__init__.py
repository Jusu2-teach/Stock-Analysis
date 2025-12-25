"""
业务趋势分析子系统 (Business Trend Analysis)
=============================================

专业的财务指标趋势分析框架，支持：
- 7种分析探针（LogTrend, Robust, Inflection, Cyclical, Deterioration, Rolling, Volatility）
- 指标类型适配器（自动调整分析参数）
- 行业差异化配置

核心组件：
- TrendAnalyzer: 趋势分析器主类
- MetricAdapter: 指标类型适配器
- MetricProfile: 指标特性配置

**架构说明**:
- Probe 层是纯数学趋势分析，不包含任何行业阈值或业务规则
- 评估规则和策略已迁移到 evaluators/threshold/ 模块
- TrendRuleEngine、TrendEvaluator、DEFAULT_TREND_RULES 现在位于 evaluators.threshold.engine

作者: AStock Analysis System
日期: 2025-12-06
"""

# 核心分析器（纯探针层）
from .core import (
    TrendAnalyzer,
    MetricProbe,
    get_default_metric_probes,
    ConfigResolver,
    TrendResultCollector,
)

# 数据模型
from .models import (
    TrendAnalyzerConfig,
    TrendSeriesConfig,
    TrendSnapshot,
    TrendVector,
    TrendEvaluationResult,
    MetricProbeContext,
)

# 配置 (纯算法参数)
from .config import (
    get_default_config,
    TrendAnalysisConfig,
    DEFAULT_CV_THRESHOLDS,
)

# 注意：业务配置（行业阈值、周期性判断）已移至 evaluators/threshold 模块
# trend/ 层是纯数学层，不应直接导入或重导出业务配置
# 需要业务配置的代码应从 evaluators.threshold 导入


__all__ = [
    # 核心分析器（纯探针层）
    "TrendAnalyzer",
    "MetricProbe",
    "get_default_metric_probes",
    "ConfigResolver",
    "TrendResultCollector",

    # 数据模型
    "TrendAnalyzerConfig",
    "TrendSeriesConfig",
    "TrendSnapshot",
    "TrendVector",
    "TrendEvaluationResult",
    "MetricProbeContext",

    # 配置 (纯算法参数)
    "get_default_config",
    "TrendAnalysisConfig",
    "DEFAULT_CV_THRESHOLDS",
]