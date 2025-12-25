"""
探针规格配置 (Probe Specifications)
===================================

所有探针的配置集中在此文件，实现：
1. 统一的适配方式
2. 零特殊处理逻辑
3. 新增探针只需添加配置

设计原则：
- 所有探针遵循统一协议：compute(values, **kwargs)
- 探针内部处理可选参数，不需要外部适配
"""

from __future__ import annotations

from typing import Any, Dict, List

import numpy as np

from .unified import ProbeSpec, UnifiedProbeEngine

# 导入探针类 (新统一名称)
from ...analyzers.trend.probes.log_trend_probe import LogTrendProbe
from ...analyzers.trend.probes.volatility_probe import VolatilityProbe
from ...analyzers.trend.probes.cyclical_probe import CyclicalProbe
from ...analyzers.trend.probes.deterioration_probe import DeteriorationProbe
from ...analyzers.trend.probes.rolling_probe import RollingProbe
from ...analyzers.trend.probes.robust_probe import RobustTrendProbe
from ...analyzers.trend.probes.inflection_probe import InflectionProbe
from ...analyzers.trend.probes.multi_horizon_probe import (
    MultiHorizonProbe,
    StructuralBreakDetector,
)

# 导入结果模型
from ...analyzers.trend.models import (
    LogTrendResult,
    VolatilityResult,
    CyclicalPatternResult,
    RecentDeteriorationResult,
    RollingTrendResult,
    RobustTrendResult,
    InflectionResult,
    DataQualitySummary,
    TrendWarning,
)


# ============================================================================
# 默认结果工厂函数
# ============================================================================

def _empty_quality() -> DataQualitySummary:
    return DataQualitySummary(
        original="unknown", cleaned="unknown", effective="unknown",
        has_loss_years=False, loss_year_count=0,
        has_near_zero_years=False, near_zero_count=0,
        has_loss_years_cleaned=False, loss_year_count_cleaned=0,
        has_near_zero_years_cleaned=False, near_zero_count_cleaned=0,
    )


def _empty_log_trend() -> LogTrendResult:
    return LogTrendResult(
        log_slope=0.0, slope=0.0, intercept=0.0, r_squared=0.0,
        p_value=1.0, std_err=0.0, cagr_approx=0.0, crosses_zero=False,
        used_cleaned_data=False, quality=_empty_quality(),
        outliers=None, metadata={}, warnings=["Insufficient data"],
    )


def _empty_volatility() -> VolatilityResult:
    return VolatilityResult(
        std_dev=0.0, cv=0.0, range_ratio=0.0, volatility_type="unknown",
        mean_near_zero=False, warnings=["Insufficient data"],
        detrended_cv=0.0, has_arch_effect=False, arch_correlation=0.0,
        volatility_regime="stable", volatility_change_ratio=1.0,
    )


def _empty_cyclical() -> CyclicalPatternResult:
    return CyclicalPatternResult(
        is_cyclical=False, cyclical_confidence=0.0, current_phase="unknown",
        cycle_position=0.0, fft_dominant_period=None, peak_to_trough_ratio=1.0,
        warnings=["Insufficient data"],
    )


def _empty_deterioration() -> RecentDeteriorationResult:
    return RecentDeteriorationResult(
        has_deterioration=False, severity="none", recent_decline_pct=0.0,
        total_decline_pct=0.0, peak_to_latest_ratio=1.0,
        warnings=["Insufficient data"], deterioration_probability=0.0,
        deterioration_pattern="none", consecutive_decline_years=0,
    )


def _empty_rolling() -> RollingTrendResult:
    return RollingTrendResult(
        recent_3y_slope=0.0, recent_5y_slope=0.0, trend_acceleration=0.0,
        is_accelerating=False, is_decelerating=False,
        warnings=["Insufficient data"],
    )


def _empty_robust() -> RobustTrendResult:
    return RobustTrendResult(
        robust_slope=float("nan"), robust_intercept=float("nan"),
        robust_slope_ci_low=float("nan"), robust_slope_ci_high=float("nan"),
        mann_kendall_tau=0.0, mann_kendall_p_value=1.0,
        is_valid=False, warnings=[TrendWarning(
            code="INSUFFICIENT_DATA",
            level="warning",
            message="Insufficient data",
        )],
    )


def _empty_inflection() -> InflectionResult:
    return InflectionResult(
        has_inflection=False, inflection_type="none", inflection_year=None,
        slope_change=0.0, before_slope=0.0, after_slope=0.0, confidence=0.0,
        warnings=["Insufficient data"],
    )


def _empty_multi_horizon() -> Dict[str, Any]:
    return {"structural_break": None, "multi_horizon": None}


# ============================================================================
# 复合探针包装器（如 MultiHorizon）
# ============================================================================

class MultiHorizonCalculator:
    """
    多视野计算器（包装器）

    组合 MultiHorizonProbe 和 StructuralBreakDetector。
    对外提供统一的 compute 接口。
    """

    def __init__(self):
        self._analyzer = MultiHorizonProbe()
        self._break_detector = StructuralBreakDetector()

    def compute(self, values: List[float]) -> Dict[str, Any]:
        """统一计算接口"""
        arr = np.array(values)
        return {
            "structural_break": self._break_detector.detect(arr),
            "multi_horizon": self._analyzer.compute(arr),
        }


# ============================================================================
# 探针规格定义（配置驱动）
# ============================================================================

# 分类常量
class ProbeCategory:
    TREND = "trend"
    VOLATILITY = "volatility"
    CYCLICAL = "cyclical"
    DETERIORATION = "deterioration"
    ROLLING = "rolling"
    ROBUST = "robust"
    INFLECTION = "inflection"
    MULTI_HORIZON = "multi_horizon"


# 所有探针的规格配置
PROBE_SPECS: List[ProbeSpec] = [
    ProbeSpec(
        name="log_trend",
        description="Log trend analysis (OLS + WLS + Bootstrap CI + CAGR)",
        category=ProbeCategory.TREND,
        min_points=3,
        calculator_class=LogTrendProbe,
        compute_method="compute",
        default_factory=_empty_log_trend,
    ),
    ProbeSpec(
        name="volatility",
        description="Volatility analysis (CV + ARCH + detrended CV)",
        category=ProbeCategory.VOLATILITY,
        min_points=3,
        calculator_class=VolatilityProbe,
        compute_method="compute",
        default_factory=_empty_volatility,
    ),
    ProbeSpec(
        name="cyclical",
        description="Cyclical pattern detection (HP filter + FFT + DFA)",
        category=ProbeCategory.CYCLICAL,
        min_points=5,
        calculator_class=CyclicalProbe,
        compute_method="compute",
        default_factory=_empty_cyclical,
    ),
    ProbeSpec(
        name="deterioration",
        description="Deterioration detection (Bayesian probability)",
        category=ProbeCategory.DETERIORATION,
        min_points=3,
        calculator_class=DeteriorationProbe,
        compute_method="compute",
        default_factory=_empty_deterioration,
    ),
    ProbeSpec(
        name="rolling",
        description="Rolling window trend analysis (3y/5y)",
        category=ProbeCategory.ROLLING,
        min_points=4,
        calculator_class=RollingProbe,
        compute_method="compute",
        default_factory=_empty_rolling,
    ),
    ProbeSpec(
        name="robust",
        description="Robust trend estimation (Theil-Sen + Mann-Kendall)",
        category=ProbeCategory.ROBUST,
        min_points=5,
        calculator_class=RobustTrendProbe,
        compute_method="compute",
        default_factory=_empty_robust,
        # RobustTrendProbe 已重构为统一协议，不再需要 kwargs_factory
    ),
    ProbeSpec(
        name="inflection",
        description="Inflection point detection (CUSUM + segmented regression)",
        category=ProbeCategory.INFLECTION,
        min_points=5,
        calculator_class=InflectionProbe,
        compute_method="compute",
        default_factory=_empty_inflection,
    ),
    ProbeSpec(
        name="multi_horizon",
        description="Multi-horizon analysis (structural break detection)",
        category=ProbeCategory.MULTI_HORIZON,
        min_points=6,
        calculator_class=MultiHorizonCalculator,
        compute_method="compute",
        default_factory=_empty_multi_horizon,
    ),
]


# ============================================================================
# 预配置引擎工厂
# ============================================================================

def create_default_engine() -> UnifiedProbeEngine:
    """
    创建预配置的默认引擎

    包含所有 8 个标准探针。
    """
    engine = UnifiedProbeEngine()
    engine.register_many(PROBE_SPECS)
    return engine


def create_core_engine() -> UnifiedProbeEngine:
    """
    创建核心探针引擎

    只包含：log_trend, volatility, deterioration
    用于快速分析场景。
    """
    core_names = {"log_trend", "volatility", "deterioration"}
    core_specs = [s for s in PROBE_SPECS if s.name in core_names]

    engine = UnifiedProbeEngine()
    engine.register_many(core_specs)
    return engine


def get_probe_spec(name: str) -> ProbeSpec:
    """按名称获取探针规格"""
    for spec in PROBE_SPECS:
        if spec.name == name:
            return spec
    raise ValueError(f"Unknown probe: {name}")


def list_all_probes() -> List[str]:
    """列出所有可用的探针"""
    return [s.name for s in PROBE_SPECS]


def list_probes_by_category(category: str) -> List[str]:
    """按分类列出探针"""
    return [s.name for s in PROBE_SPECS if s.category == category]
