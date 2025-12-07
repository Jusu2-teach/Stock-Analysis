"""
趋势分析探针模块
================

提供多种专业的时间序列分析探针:

基础探针:
    - LogTrendCalculator: 对数趋势计算（CAGR、OLS斜率）
    - VolatilityCalculator: 波动率分析（CV、ARCH效应）
    - RobustTrendProbe: 稳健趋势估计（Theil-Sen、Mann-Kendall）
    - RollingTrendCalculator: 滚动窗口趋势

高级探针:
    - InflectionDetector: 拐点检测
    - DeteriorationDetector: 恶化检测
    - CyclicalPatternDetector: 周期性检测（HP滤波、ACF、Hurst指数）

多时间窗口分析 (v2.0):
    - MultiHorizonAnalyzer: 多时间窗口分析器
    - StructuralBreakDetector: 结构断点检测
    - ProfessionalDataWindowStrategy: 专业数据窗口策略

使用建议:
    对于10年数据，使用 ProfessionalDataWindowStrategy 可以智能地:
    1. 用近5年数据计算趋势指标（反映当前状态）
    2. 用全10年数据检测周期性和结构断点
    3. 自动识别公司质变点，只使用有效数据
"""

from .common import DataQualityChecker, OutlierDetectorFactory, OutlierDetector
from .log_trend_probe import LogTrendCalculator
from .volatility_probe import VolatilityCalculator
from .inflection_probe import InflectionDetector
from .deterioration_probe import DeteriorationDetector
from .cyclical_probe import CyclicalPatternDetector, check_cyclical_preconditions
from .rolling_probe import RollingTrendCalculator
from .robust_probe import RobustTrendProbe
from .multi_horizon_probe import (
    MultiHorizonAnalyzer,
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

    # 基础探针
    "LogTrendCalculator",
    "VolatilityCalculator",
    "InflectionDetector",
    "DeteriorationDetector",
    "CyclicalPatternDetector",
    "RollingTrendCalculator",
    "RobustTrendProbe",

    # 周期性辅助
    "check_cyclical_preconditions",

    # 多时间窗口分析
    "MultiHorizonAnalyzer",
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
