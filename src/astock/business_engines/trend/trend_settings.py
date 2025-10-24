"""
趋势分析配置
============

定义 TrendAnalyzer 的全局配置、时间序列处理策略以及输出字段选择，便于
在不同项目中通过数据类进行类型安全的调参。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Optional, Sequence, Tuple, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .metric_probes import MetricProbe
    from .trend_schema import TrendField


def _default_fields() -> Tuple["TrendField", ...]:
    from .trend_schema import trend_field_schema

    return tuple(trend_field_schema())


@dataclass(frozen=True)
class TrendSeriesConfig:
    """Control how raw metric series are prepared ahead of probe execution."""

    window_size: Optional[int] = 5
    order_column: Optional[str] = "end_date"
    weights: Optional[Sequence[float]] = None
    fill_strategy: Literal["median", "ffill", "bfill", "zero", "constant"] = "median"
    fill_value: Optional[float] = None
    min_valid_ratio: float = 0.6
    allow_partial_window: bool = False
    drop_non_finite: bool = True


@dataclass(frozen=True)
class TrendAnalyzerConfig:
    """Bundle analyzer-wide tuning knobs so metric definitions stay declarative."""

    series: TrendSeriesConfig = field(default_factory=TrendSeriesConfig)
    probes: Optional[Sequence["MetricProbe"]] = None
    output_fields: Tuple["TrendField", ...] = field(default_factory=_default_fields)
    reference_metrics: Sequence[str] = field(default_factory=tuple)
