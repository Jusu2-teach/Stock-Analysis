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

**数据流向**:
- Probe 层是纯数学趋势分析，输出各种 Result 对象
- 结果通过 ProbeOutputs (core/probe_engine/builders.py) 统一接口传递
- EvaluationContext 和 T.R.U.T.H. 都从 ProbeOutputs 获取数据

**架构原则**:
- 单一数据源: ProbeOutputs 是探针结果的唯一出口
- 阈值在探针: 所有判断阈值在各 Probe 内部定义
- 零重复计算: 不在 Pipeline 层重新计算探针已经计算的结论

作者: AStock Analysis System
日期: 2025-12-06
更新: 2025-12-25 (移除冗余 Pipeline 层)
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