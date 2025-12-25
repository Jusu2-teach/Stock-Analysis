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

多时间窗口分析:
    - MultiHorizonProbe: 多时间窗口分析器
    - StructuralBreakDetector: 结构断点检测
    - ProfessionalDataWindowStrategy: 专业数据窗口策略

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
)
from .data_window_strategy import (
    ProfessionalDataWindowStrategy,
    analyze_with_professional_strategy,
    classify_metric,
    MetricCategory,
    WindowStrategy,
    ProfessionalAnalysisResult,
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

    # 专业数据窗口策略
    "ProfessionalDataWindowStrategy",
    "analyze_with_professional_strategy",
    "classify_metric",
    "MetricCategory",
    "WindowStrategy",
    "ProfessionalAnalysisResult",
]
