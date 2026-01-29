"""
趋势分析探针模块
================

提供多种专业的时间序列分析探针:

基础探针:
    - LogTrendProbe: 对数趋势计算（CAGR、OLS斜率）
    - VolatilityProbe: 波动率分析（CV、ARCH效应）
    - RobustTrendProbe: 稳健趋势估计（Theil-Sen、Mann-Kendall）
    - RollingProbe: 滚动窗口趋势

高级探针:
    - InflectionProbe: 拐点检测
    - DeteriorationProbe: 恶化检测
    - CyclicalProbe: 周期性检测（HP滤波、ACF、Hurst指数）

多时间窗口分析 (增强版 v2.0):
    - MultiHorizonProbe: 多时间窗口分析器（整合指标分类、策略配置、周期分析）
    - StructuralBreakDetector: 结构断点检测
    - MetricCategory: 指标类别枚举
    - WindowStrategy: 窗口策略配置
    - classify_metric: 指标自动分类函数

财务上下文探针 (v3.1 新增):
    - FinancialContextProbe: 提取资产结构和风险检测字段 (用于 β/δ_fraud 因子)
    - FinancialContextResult: 财务上下文结果模型

所有探针遵循统一协议 (ProbeProtocol):
    - compute(values, **kwargs) -> Result
    - default() -> Result
"""

from .common import DataQualityChecker, OutlierDetectorFactory, OutlierDetector
from .log_trend_probe import LogTrendProbe
from .volatility_probe import VolatilityProbe
from .inflection_probe import InflectionProbe
from .deterioration_probe import DeteriorationProbe
from .cyclical_probe import CyclicalProbe, check_cyclical_preconditions
from .rolling_probe import RollingProbe
from .robust_probe import RobustTrendProbe
from .multi_horizon_probe import (
    MultiHorizonProbe,
    StructuralBreakDetector,
    analyze_multi_horizon,
    detect_structural_break,
    MultiHorizonResult,
    StructuralBreakResult,
    HorizonAnalysis,
    BreakType,
    # v2.0 新增导出
    MetricCategory,
    WindowStrategy,
    METRIC_STRATEGIES,
    classify_metric,
)
from .financial_context_probe import (
    FinancialContextProbe,
    FinancialContextResult,
    create_financial_context_probe,
    compute_financial_context,
)

__all__ = [
    # 基础工具
    "DataQualityChecker",
    "OutlierDetectorFactory",
    "OutlierDetector",

    # 探针
    "LogTrendProbe",
    "VolatilityProbe",
    "InflectionProbe",
    "DeteriorationProbe",
    "CyclicalProbe",
    "RollingProbe",
    "RobustTrendProbe",
    "MultiHorizonProbe",
    "FinancialContextProbe",  # v3.1 新增

    # 周期性辅助
    "check_cyclical_preconditions",

    # 多时间窗口分析
    "StructuralBreakDetector",
    "analyze_multi_horizon",
    "detect_structural_break",
    "MultiHorizonResult",
    "StructuralBreakResult",
    "HorizonAnalysis",
    "BreakType",

    # v2.0 指标分类与策略
    "MetricCategory",
    "WindowStrategy",
    "METRIC_STRATEGIES",
    "classify_metric",

    # v3.1 财务上下文
    "FinancialContextResult",
    "create_financial_context_probe",
    "compute_financial_context",
]
