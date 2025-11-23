"""
趋势分析数据模型
================

集中定义趋势分析过程中使用的 dataclass 结果、配置结构以及字段 Schema。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field, asdict
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple, Iterable, Protocol
import pandas as pd

# ============================================================================
# 基础接口
# ============================================================================

class SerializableResult:
    """Mixin providing a common ``to_dict`` helper for dataclass results."""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class TrendWarning:
    code: str
    level: str = "info"
    message: str = ""
    context: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# 分析结果模型
# ============================================================================

@dataclass
class DataQualitySummary(SerializableResult):
    original: str
    cleaned: str
    effective: str
    has_loss_years: bool
    loss_year_count: int
    has_near_zero_years: bool
    near_zero_count: int
    has_loss_years_cleaned: bool
    loss_year_count_cleaned: int
    has_near_zero_years_cleaned: bool
    near_zero_count_cleaned: int


@dataclass
class OutlierDetectionResult(SerializableResult):
    method: str
    threshold: Optional[float]
    has_outliers: bool
    indices: List[int]
    values: List[float]
    cleaned_values: List[float]
    cleaning_ratio: float
    cleaning_applied: bool
    data_contamination: str
    risk_level: str
    warnings: List[TrendWarning] = field(default_factory=list)


@dataclass
class RobustTrendResult(SerializableResult):
    robust_slope: float
    robust_intercept: float
    robust_slope_ci_low: float
    robust_slope_ci_high: float
    mann_kendall_tau: float
    mann_kendall_p_value: float
    is_valid: bool
    warnings: List[TrendWarning] = field(default_factory=list)


@dataclass
class LogTrendResult(SerializableResult):
    log_slope: float
    slope: float
    intercept: float
    r_squared: float
    p_value: float
    std_err: float
    cagr_approx: float
    crosses_zero: bool
    used_cleaned_data: bool
    quality: DataQualitySummary
    outliers: Optional[OutlierDetectionResult]
    metadata: Dict[str, Any] = field(default_factory=dict)
    warnings: List[TrendWarning] = field(default_factory=list)


@dataclass
class VolatilityResult(SerializableResult):
    std_dev: float
    cv: float
    range_ratio: float
    volatility_type: str
    mean_near_zero: bool
    warnings: List[TrendWarning] = field(default_factory=list)


@dataclass
class InflectionResult(SerializableResult):
    has_inflection: bool
    inflection_type: str
    early_slope: float
    middle_slope: float
    recent_slope: float
    slope_change: float
    confidence: float
    early_r_squared: float
    recent_r_squared: float
    warnings: List[TrendWarning] = field(default_factory=list)


@dataclass
class RecentDeteriorationResult(SerializableResult):
    has_deterioration: bool
    severity: str
    year4_to_5_change: float
    year3_to_4_change: float
    year4_to_5_pct: float
    year3_to_4_pct: float
    total_decline_pct: float
    is_high_level_stable: bool
    decline_threshold_pct: float
    decline_threshold_abs: float
    industry: str
    warnings: List[TrendWarning] = field(default_factory=list)


@dataclass
class CyclicalPatternResult(SerializableResult):
    is_cyclical: bool
    peak_to_trough_ratio: float
    has_middle_peak: bool
    has_wave_pattern: bool
    trend_r_squared: float
    cv: float
    current_phase: str
    industry_cyclical: bool
    cyclical_confidence: float
    peak_to_trough_threshold: float
    trend_r_squared_max: float
    cv_threshold: float
    industry: str
    confidence_factors: List[str] = field(default_factory=list)
    warnings: List[TrendWarning] = field(default_factory=list)


@dataclass
class RollingTrendResult(SerializableResult):
    recent_3y_slope: float
    recent_3y_r_squared: float
    full_5y_slope: float
    full_5y_r_squared: float
    trend_acceleration: float
    is_accelerating: bool
    is_decelerating: bool
    warnings: List[TrendWarning] = field(default_factory=list)


@dataclass
class LinearTrendResult(SerializableResult):
    slope: float
    intercept: float
    r_squared: float
    p_value: float
    std_err: float
    warnings: List[TrendWarning] = field(default_factory=list)


# ============================================================================
# 规则与上下文模型
# ============================================================================

RuleKind = Literal["veto", "penalty", "bonus"]


@dataclass
class TrendThresholds:
    min_latest_value: Optional[float]
    severe_decline: float
    mild_decline: float
    latest_threshold: Optional[float]
    trend_significance: float


@dataclass
class TrendContext:
    group_key: str
    metric_name: str
    log_slope: float
    r_squared: float
    cv: float
    latest_value: float
    weighted_avg: float
    cagr_approx: float
    total_decline_pct: float
    deterioration_result: Dict[str, Any]
    latest_vs_weighted_ratio: float
    is_cyclical: bool
    current_phase: str
    peak_to_trough_ratio: float
    has_deterioration: bool
    deterioration_severity: str
    has_inflection: bool
    inflection_type: str
    slope_change: float
    is_accelerating: bool
    is_decelerating: bool
    trend_acceleration: float
    recent_3y_slope: float
    has_loss_years: bool
    loss_year_count: int
    has_near_zero_years: bool
    near_zero_count: int
    robust_slope: float = 0.0
    mann_kendall_tau: float = 0.0
    mann_kendall_p_value: float = 1.0
    reference_metrics: Dict[str, Dict[str, float]] = field(default_factory=dict)

    def deterioration_value(self, key: str, default: float = 0.0) -> float:
        value = self.deterioration_result.get(key, default)
        try:
            return float(value)
        except (TypeError, ValueError):
            return float(default)

    @classmethod
    def from_vector(
        cls,
        group_key: str,
        metric_name: str,
        vector: "TrendVector",
    ) -> "TrendContext":
        return cls(
            group_key=group_key,
            metric_name=metric_name,
            log_slope=vector.log_slope,
            r_squared=vector.r_squared,
            cv=vector.cv,
            latest_value=vector.latest_value,
            weighted_avg=vector.weighted_avg,
            cagr_approx=vector.cagr_approx,
            total_decline_pct=vector.total_decline_pct,
            deterioration_result=vector.deterioration_result,
            latest_vs_weighted_ratio=vector.latest_vs_weighted_ratio,
            is_cyclical=vector.is_cyclical,
            current_phase=vector.current_phase,
            peak_to_trough_ratio=vector.peak_to_trough_ratio,
            has_deterioration=vector.has_deterioration,
            deterioration_severity=vector.deterioration_severity,
            has_inflection=vector.has_inflection,
            inflection_type=vector.inflection_type,
            slope_change=vector.slope_change,
            is_accelerating=vector.is_accelerating,
            is_decelerating=vector.is_decelerating,
            trend_acceleration=vector.trend_acceleration,
            recent_3y_slope=vector.recent_3y_slope,
            has_loss_years=vector.has_loss_years,
            loss_year_count=vector.loss_year_count,
            has_near_zero_years=vector.has_near_zero_years,
            near_zero_count=vector.near_zero_count,
            robust_slope=vector.robust_slope,
            mann_kendall_tau=vector.mann_kendall_tau,
            mann_kendall_p_value=vector.mann_kendall_p_value,
            reference_metrics=vector.reference_metrics,
        )


@dataclass
class TrendRuleParameters:
    penalty_factor: float
    max_penalty: float
    severe_single_year_decline_pct: float
    severe_single_year_penalty: float
    relative_decline_ratio_60: float
    relative_decline_penalty_60: float
    relative_decline_ratio_70: float
    relative_decline_penalty_70: float
    sustained_decline_threshold: float
    sustained_decline_penalty: float
    roiic_veto_weighted_threshold: float
    roiic_veto_latest_threshold: float
    roiic_negative_penalty_buffer: float
    roiic_negative_penalty_scale: float
    roiic_negative_penalty_cap: float
    roiic_divergence_slope_gap: float
    roiic_positive_bonus_threshold: float

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "TrendRuleParameters":
        return cls(
            penalty_factor=float(config.get("penalty_factor", 20)),
            max_penalty=float(config.get("max_penalty", 20)),
            severe_single_year_decline_pct=float(config.get("severe_single_year_decline_pct", -30.0)),
            severe_single_year_penalty=float(config.get("severe_single_year_penalty", 15)),
            relative_decline_ratio_60=float(config.get("relative_decline_ratio_60", 0.60)),
            relative_decline_penalty_60=float(config.get("relative_decline_penalty_60", 15)),
            relative_decline_ratio_70=float(config.get("relative_decline_ratio_70", 0.70)),
            relative_decline_penalty_70=float(config.get("relative_decline_penalty_70", 10)),
            sustained_decline_threshold=float(config.get("sustained_decline_threshold", -0.15)),
            sustained_decline_penalty=float(config.get("sustained_decline_penalty", 10)),
            roiic_veto_weighted_threshold=float(config.get("roiic_veto_weighted_threshold", -20.0)),
            roiic_veto_latest_threshold=float(config.get("roiic_veto_latest_threshold", -10.0)),
            roiic_negative_penalty_buffer=float(config.get("roiic_negative_penalty_buffer", 0.0)),
            roiic_negative_penalty_scale=float(config.get("roiic_negative_penalty_scale", 8.0)),
            roiic_negative_penalty_cap=float(config.get("roiic_negative_penalty_cap", 12.0)),
            roiic_divergence_slope_gap=float(config.get("roiic_divergence_slope_gap", 0.12)),
            roiic_positive_bonus_threshold=float(config.get("roiic_positive_bonus_threshold", 8.0)),
        )


@dataclass
class RuleResult:
    rule_name: str
    kind: RuleKind
    message: str
    value: float = 0.0
    log_level: int = logging.DEBUG
    log_prefix: str = "过滤淘汰"


@dataclass
class TrendRuleOutcome:
    passes: bool
    elimination_reason: str
    penalty: float
    penalty_details: List[str]
    bonus_details: List[str]
    auxiliary_notes: List[str] = field(default_factory=list)


TrendRuleEvaluator = Callable[[TrendContext, TrendRuleParameters, TrendThresholds], Optional[RuleResult]]


@dataclass
class TrendRuleConfig:
    thresholds: TrendThresholds
    parameters: TrendRuleParameters

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "TrendRuleConfig":
        severe_decline = float(
            config.get(
                "log_severe_decline_slope",
                config.get("severe_decline_slope", -0.30),
            )
        )
        mild_decline = float(
            config.get(
                "log_mild_decline_slope",
                config.get("mild_decline_slope", -0.15),
            )
        )
        latest_threshold = config.get("latest_threshold", config.get("min_latest_value"))
        trend_significance = float(config.get("trend_significance", 0.4))

        thresholds = TrendThresholds(
            min_latest_value=config.get("min_latest_value"),
            severe_decline=severe_decline,
            mild_decline=mild_decline,
            latest_threshold=latest_threshold,
            trend_significance=trend_significance,
        )

        parameters = TrendRuleParameters.from_config(config)
        return cls(thresholds=thresholds, parameters=parameters)

    def with_thresholds(self, thresholds: TrendThresholds) -> "TrendRuleConfig":
        return TrendRuleConfig(thresholds=thresholds, parameters=self.parameters)


@dataclass
class TrendRule:
    name: str
    evaluator: TrendRuleEvaluator

    def evaluate(
        self,
        context: TrendContext,
        params: TrendRuleParameters,
        thresholds: TrendThresholds
    ) -> Optional[RuleResult]:
        return self.evaluator(context, params, thresholds)


@dataclass
class TrendEvaluationResult:
    passes: bool
    elimination_reason: str
    penalty: float
    penalty_details: List[str]
    bonus_details: List[str]
    trend_score: float
    auxiliary_notes: List[str] = field(default_factory=list)
    strategies: List[str] = field(default_factory=list)
    strategy_reasons: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class TrendVector:
    log_slope: float
    r_squared: float
    cv: float
    latest_value: float
    weighted_avg: float
    cagr_approx: float
    total_decline_pct: float
    deterioration_result: Dict[str, Any]
    latest_vs_weighted_ratio: float
    is_cyclical: bool
    current_phase: str
    peak_to_trough_ratio: float
    has_deterioration: bool
    deterioration_severity: str
    has_inflection: bool
    inflection_type: str
    slope_change: float
    is_accelerating: bool
    is_decelerating: bool
    trend_acceleration: float
    recent_3y_slope: float
    has_loss_years: bool
    loss_year_count: int
    has_near_zero_years: bool
    near_zero_count: int
    robust: RobustTrendResult
    reference_metrics: Dict[str, Dict[str, float]] = field(default_factory=dict)

    @property
    def robust_slope(self) -> float:
        return self.robust.robust_slope

    @property
    def mann_kendall_tau(self) -> float:
        return self.robust.mann_kendall_tau

    @property
    def mann_kendall_p_value(self) -> float:
        return self.robust.mann_kendall_p_value


@dataclass(frozen=True)
class TrendSnapshot:
    group_key: str
    metric_name: str
    vector: TrendVector
    evaluation: TrendEvaluationResult
    trend: LogTrendResult
    volatility: VolatilityResult
    inflection: InflectionResult
    deterioration: RecentDeteriorationResult
    cyclical: CyclicalPatternResult
    rolling: RollingTrendResult
    robust: RobustTrendResult
    quality: DataQualitySummary
    latest_value: float
    weighted_avg: float
    latest_vs_weighted_ratio: float
    extra_fields: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# 字段 Schema
# ============================================================================

@dataclass(frozen=True)
class TrendField:
    """Declarative definition for a trend output column."""

    key: str
    attr_path: str
    description: str
    unit: str = ""
    category: str = "core"

    def resolve(self, snapshot: TrendSnapshot) -> Any:
        value: Any = snapshot
        for part in self.attr_path.split("."):
            value = getattr(value, part)
        return value


# ============================================================================
# 探针上下文与配置
# ============================================================================

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
    output_fields: Tuple["TrendField", ...] = field(default_factory=tuple)
    reference_metrics: Sequence[str] = field(default_factory=tuple)

