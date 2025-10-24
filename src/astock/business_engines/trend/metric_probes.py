"""
趋势探针集合
==========

封装 Log 斜率、波动率、周期性、恶化检测等核心算法，并以统一的数据类
返回结果，便于 TrendAnalyzer 组合使用。模块定义 `MetricProbe` 协议、默认
探针列表，以及在计算失败时的降级策略。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional, Protocol

import pandas as pd

from .trend_analysis import (
    calculate_log_trend_slope,
    calculate_rolling_trend,
    calculate_volatility_metrics,
    detect_cyclical_pattern,
    detect_inflection_point,
    detect_recent_deterioration,
)
from .trend_defaults import (
    empty_cyclical_result,
    empty_deterioration_result,
    empty_inflection_result,
    empty_log_trend_result,
    empty_rolling_result,
    empty_volatility_result,
)
from .trend_models import (
    CyclicalPatternResult,
    InflectionResult,
    LogTrendResult,
    RecentDeteriorationResult,
    RollingTrendResult,
    VolatilityResult,
)


class FatalMetricProbeError(RuntimeError):
    """Raised when a fatal metric probe fails and the group must be skipped."""

    def __init__(self, probe_name: str, original: Exception) -> None:
        super().__init__(f"Metric probe '{probe_name}' failed: {original}")
        self.probe_name = probe_name
        self.original = original


@dataclass(frozen=True)
class MetricProbeContext:
    group_key: str
    metric_name: str
    industry: Optional[str]
    group_df: pd.DataFrame


class MetricProbe(Protocol):
    """Interface for metric probes."""

    name: str
    fatal: bool

    def compute(self, values: List[float], context: MetricProbeContext) -> Any:
        """Compute a metric result for the provided series."""

    def default(self, context: MetricProbeContext) -> Any:
        """Return a safe default result when computation fails."""


class BaseMetricProbe:
    fatal: bool = False

    def compute(self, values: List[float], context: MetricProbeContext) -> Any:  # pragma: no cover
        raise NotImplementedError

    def default(self, context: MetricProbeContext) -> Any:  # pragma: no cover
        raise NotImplementedError


class LogTrendProbe(BaseMetricProbe):
    name = "log_trend"
    fatal = True

    def compute(self, values: List[float], context: MetricProbeContext) -> LogTrendResult:
        return calculate_log_trend_slope(values)

    def default(self, context: MetricProbeContext) -> LogTrendResult:
        return empty_log_trend_result()


class VolatilityProbe(BaseMetricProbe):
    name = "volatility"

    def compute(self, values: List[float], context: MetricProbeContext) -> VolatilityResult:
        return calculate_volatility_metrics(values)

    def default(self, context: MetricProbeContext) -> VolatilityResult:
        return empty_volatility_result()


class InflectionProbe(BaseMetricProbe):
    name = "inflection"

    def compute(self, values: List[float], context: MetricProbeContext) -> InflectionResult:
        return detect_inflection_point(values)

    def default(self, context: MetricProbeContext) -> InflectionResult:
        return empty_inflection_result()


class DeteriorationProbe(BaseMetricProbe):
    name = "deterioration"

    def compute(self, values: List[float], context: MetricProbeContext) -> RecentDeteriorationResult:
        return detect_recent_deterioration(values, context.industry or "default")

    def default(self, context: MetricProbeContext) -> RecentDeteriorationResult:
        return empty_deterioration_result()


class CyclicalProbe(BaseMetricProbe):
    name = "cyclical"

    def compute(self, values: List[float], context: MetricProbeContext) -> CyclicalPatternResult:
        return detect_cyclical_pattern(values, context.industry or "default")

    def default(self, context: MetricProbeContext) -> CyclicalPatternResult:
        return empty_cyclical_result()


class RollingTrendProbe(BaseMetricProbe):
    name = "rolling"

    def compute(self, values: List[float], context: MetricProbeContext) -> RollingTrendResult:
        return calculate_rolling_trend(values)

    def default(self, context: MetricProbeContext) -> RollingTrendResult:
        return empty_rolling_result()


def get_default_metric_probes() -> List[MetricProbe]:
    """Return the default suite of metric probes."""

    return [
        LogTrendProbe(),
        VolatilityProbe(),
        InflectionProbe(),
        DeteriorationProbe(),
        CyclicalProbe(),
        RollingTrendProbe(),
    ]
