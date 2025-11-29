"""
趋势分析核心模块
================

整合趋势分析的核心逻辑，包括：
1. 默认值生成器
2. 指标探针 (Metric Probes)
3. 规则引擎 (Rule Engine)
4. 趋势分析器 (Trend Analyzer)
5. 配置解析器 (Config Resolver)
6. 趋势评估器 (Trend Evaluator)
7. 结果收集器 (Trend Result Collector)

此模块是趋势分析业务逻辑的单一入口。
"""

from __future__ import annotations

import logging
from typing import Any, ClassVar, Dict, Iterable, List, Optional, Sequence, Tuple, TYPE_CHECKING, Protocol, Callable

import numpy as np
import pandas as pd

from .models import (
    CyclicalPatternResult,
    DataQualitySummary,
    InflectionResult,
    LogTrendResult,
    RecentDeteriorationResult,
    RollingTrendResult,
    RobustTrendResult,
    TrendSnapshot,
    TrendVector,
    VolatilityResult,
    TrendWarning,
    TrendContext,
    TrendRuleParameters,
    TrendThresholds,
    RuleResult,
    TrendRuleOutcome,
    TrendRule,
    TrendEvaluationResult,
    TrendField,
    MetricProbeContext,
    TrendAnalyzerConfig,
    TrendSeriesConfig,
    TrendRuleConfig,
)
from .config import (
    get_default_config,
    trend_field_schema,
    get_default_fields,
    get_industry_category,
)
from .probes.common import (
    calculate_weighted_average,
    FatalMetricProbeError,
)
from .probes.log_trend_probe import LogTrendCalculator
from .probes.volatility_probe import VolatilityCalculator
from .probes.inflection_probe import InflectionDetector
from .probes.deterioration_probe import DeteriorationDetector
from .probes.cyclical_probe import CyclicalPatternDetector
from .probes.rolling_probe import RollingTrendCalculator
from .probes.robust_probe import RobustTrendProbe

from .rules import (
    rule_roiic_capital_destruction,
    rule_min_latest_value,
    rule_low_significance_decline,
    rule_high_volatility_instability,
    rule_severe_decline,
    rule_severe_deterioration_veto,
    rule_structural_decline_veto,
    rule_roiic_negative_penalty,
    rule_compound_recent_deterioration,
    rule_mild_decline_penalty,
    rule_deterioration_penalty,
    rule_sustained_decline,
    rule_single_year_decline,
    rule_relative_decline,
    rule_roiic_roic_divergence,
    rule_inflection_penalty_or_bonus,
    rule_cyclical_adjustment,
    rule_acceleration_adjustment,
    rule_roiic_positive_bonus,
    rule_growth_momentum_bonus,
    rule_earnings_quality_divergence,
    rule_sustainable_growth_check,
)
from .strategies import get_default_strategies, TrendStrategy

if TYPE_CHECKING:
    from .models import TrendEvaluationResult

logger = logging.getLogger(__name__)


# ============================================================================
# 1. 默认值生成器
# ============================================================================

def empty_quality() -> DataQualitySummary:
    """Return a neutral data quality summary."""
    return DataQualitySummary(
        original="unknown",
        cleaned="unknown",
        effective="unknown",
        has_loss_years=False,
        loss_year_count=0,
        has_near_zero_years=False,
        near_zero_count=0,
        has_loss_years_cleaned=False,
        loss_year_count_cleaned=0,
        has_near_zero_years_cleaned=False,
        near_zero_count_cleaned=0,
    )


def empty_log_trend_result() -> LogTrendResult:
    """Return a fallback log trend computation."""
    return LogTrendResult(
        log_slope=0.0,
        slope=0.0,
        intercept=0.0,
        r_squared=0.0,
        p_value=1.0,
        std_err=0.0,
        cagr_approx=0.0,
        crosses_zero=False,
        used_cleaned_data=False,
        quality=empty_quality(),
        outliers=None,
        metadata={},
        warnings=[],
    )


def empty_robust_result() -> RobustTrendResult:
    return RobustTrendResult(
        robust_slope=float("nan"),
        robust_intercept=float("nan"),
        robust_slope_ci_low=float("nan"),
        robust_slope_ci_high=float("nan"),
        mann_kendall_tau=0.0,
        mann_kendall_p_value=1.0,
        is_valid=False,
        warnings=[],
    )


def empty_volatility_result() -> VolatilityResult:
    return VolatilityResult(
        std_dev=0.0,
        cv=0.0,
        range_ratio=0.0,
        volatility_type="unknown",
        mean_near_zero=False,
        warnings=[],
    )


def empty_inflection_result() -> InflectionResult:
    return InflectionResult(
        has_inflection=False,
        inflection_type="none",
        early_slope=0.0,
        middle_slope=0.0,
        recent_slope=0.0,
        slope_change=0.0,
        confidence=0.0,
        early_r_squared=0.0,
        recent_r_squared=0.0,
        warnings=[],
    )


def empty_deterioration_result() -> RecentDeteriorationResult:
    return RecentDeteriorationResult(
        has_deterioration=False,
        severity="none",
        year4_to_5_change=0.0,
        year3_to_4_change=0.0,
        year4_to_5_pct=0.0,
        year3_to_4_pct=0.0,
        total_decline_pct=0.0,
        is_high_level_stable=False,
        decline_threshold_pct=-5.0,
        decline_threshold_abs=-2.0,
        industry="default",
        warnings=[],
    )


def empty_cyclical_result() -> CyclicalPatternResult:
    return CyclicalPatternResult(
        is_cyclical=False,
        peak_to_trough_ratio=1.0,
        has_middle_peak=False,
        has_wave_pattern=False,
        trend_r_squared=0.0,
        cv=0.0,
        current_phase="unknown",
        industry_cyclical=False,
        cyclical_confidence=0.0,
        peak_to_trough_threshold=3.0,
        trend_r_squared_max=0.7,
        cv_threshold=0.25,
        industry="default",
        confidence_factors=[],
        warnings=[],
    )


def empty_rolling_result() -> RollingTrendResult:
    return RollingTrendResult(
        recent_3y_slope=0.0,
        recent_3y_r_squared=0.0,
        full_5y_slope=0.0,
        full_5y_r_squared=0.0,
        trend_acceleration=0.0,
        is_accelerating=False,
        is_decelerating=False,
        warnings=[],
    )


# ============================================================================
# 2. 指标探针 (Metric Probes)
# ============================================================================

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

    def compute(self, values: List[float], context: MetricProbeContext) -> Any:
        raise NotImplementedError

    def default(self, context: MetricProbeContext) -> Any:
        raise NotImplementedError


class LogTrendProbe(BaseMetricProbe):
    name = "log_trend"
    fatal = True

    def compute(self, values: List[float], context: MetricProbeContext) -> LogTrendResult:
        calculator = LogTrendCalculator()
        return calculator.calculate(values)

    def default(self, context: MetricProbeContext) -> LogTrendResult:
        return empty_log_trend_result()


class VolatilityProbe(BaseMetricProbe):
    name = "volatility"

    def compute(self, values: List[float], context: MetricProbeContext) -> VolatilityResult:
        calculator = VolatilityCalculator()
        return calculator.calculate(values)

    def default(self, context: MetricProbeContext) -> VolatilityResult:
        return empty_volatility_result()


class InflectionProbe(BaseMetricProbe):
    name = "inflection"

    def compute(self, values: List[float], context: MetricProbeContext) -> InflectionResult:
        detector = InflectionDetector()
        return detector.detect(values)

    def default(self, context: MetricProbeContext) -> InflectionResult:
        return empty_inflection_result()


class DeteriorationProbe(BaseMetricProbe):
    name = "deterioration"

    def compute(self, values: List[float], context: MetricProbeContext) -> RecentDeteriorationResult:
        detector = DeteriorationDetector()
        return detector.detect(values, context.industry or "default")

    def default(self, context: MetricProbeContext) -> RecentDeteriorationResult:
        return empty_deterioration_result()


class CyclicalProbe(BaseMetricProbe):
    name = "cyclical"

    def compute(self, values: List[float], context: MetricProbeContext) -> CyclicalPatternResult:
        detector = CyclicalPatternDetector()
        return detector.detect(values, context.industry or "default")

    def default(self, context: MetricProbeContext) -> CyclicalPatternResult:
        return empty_cyclical_result()


class RollingTrendProbe(BaseMetricProbe):
    name = "rolling"

    def compute(self, values: List[float], context: MetricProbeContext) -> RollingTrendResult:
        calculator = RollingTrendCalculator()
        return calculator.calculate(values)

    def default(self, context: MetricProbeContext) -> RollingTrendResult:
        return empty_rolling_result()


class RobustProbe(BaseMetricProbe):
    name = "robust"

    def compute(self, values: List[float], context: MetricProbeContext) -> RobustTrendResult:
        probe = RobustTrendProbe()
        return probe.compute(values, context)

    def default(self, context: MetricProbeContext) -> RobustTrendResult:
        probe = RobustTrendProbe()
        return probe.default(context)


def get_default_metric_probes() -> List[MetricProbe]:
    """Return the default suite of metric probes."""
    return [
        LogTrendProbe(),
        VolatilityProbe(),
        InflectionProbe(),
        DeteriorationProbe(),
        CyclicalProbe(),
        RollingTrendProbe(),
        RobustProbe(),
    ]


# ============================================================================
# 3. 规则引擎 (Rule Engine)
# ============================================================================

class TrendRuleEngine:
    def __init__(self, rules: List[TrendRule]) -> None:
        self._rules = rules

    def run(
        self,
        context: TrendContext,
        params: TrendRuleParameters,
        thresholds: TrendThresholds,
        logger: Optional[logging.Logger] = None
    ) -> TrendRuleOutcome:
        penalty = 0.0
        penalty_details: List[str] = []
        bonus_details: List[str] = []
        auxiliary_notes: List[str] = []
        is_auxiliary_metric = context.metric_name.lower() == "roiic"

        for rule in self._rules:
            result = rule.evaluate(context, params, thresholds)
            if result is None:
                continue

            if result.kind == "veto":
                if is_auxiliary_metric:
                    if logger:
                        logger.log(
                            result.log_level,
                            f"⚠️ 【ROIIC辅助】{context.group_key}: {result.message}",
                        )
                    auxiliary_notes.append(result.message)
                    continue

                if logger:
                    prefix = result.log_prefix
                    if prefix:
                        logger.log(result.log_level, f"❌ {prefix}: {context.group_key} - {result.message}")
                    else:
                        logger.log(result.log_level, f"❌ {context.group_key}: {result.message}")
                return TrendRuleOutcome(False, result.message, penalty, penalty_details, bonus_details, auxiliary_notes)

            if result.kind == "penalty":
                if is_auxiliary_metric:
                    if logger:
                        logger.log(
                            result.log_level,
                            f"⚠️ 【ROIIC辅助】{context.group_key}: {result.message}",
                        )
                    auxiliary_notes.append(result.message)
                    continue

                penalty += result.value
                penalty_details.append(result.message)
                continue

            if result.kind == "bonus":
                if is_auxiliary_metric:
                    if logger:
                        logger.log(
                            result.log_level,
                            f"✅ 【ROIIC辅助】{context.group_key}: {result.message}",
                        )
                    auxiliary_notes.append(result.message)
                    continue

                if result.value > 0:
                    penalty = max(0.0, penalty - result.value)
                bonus_details.append(result.message)

        return TrendRuleOutcome(True, "", penalty, penalty_details, bonus_details, auxiliary_notes)


DEFAULT_TREND_RULES: List[TrendRule] = [
    TrendRule("roiic_capital_destruction_veto", rule_roiic_capital_destruction),
    TrendRule("min_latest_value", rule_min_latest_value),
    TrendRule("low_significance_decline", rule_low_significance_decline),
    TrendRule("high_volatility_instability", rule_high_volatility_instability),
    TrendRule("severe_decline", rule_severe_decline),
    TrendRule("severe_deterioration_veto", rule_severe_deterioration_veto),
    TrendRule("structural_decline_veto", rule_structural_decline_veto),
    TrendRule("roiic_negative_penalty", rule_roiic_negative_penalty),
    TrendRule("compound_recent_deterioration", rule_compound_recent_deterioration),
    TrendRule("mild_decline_penalty", rule_mild_decline_penalty),
    TrendRule("deterioration_penalty", rule_deterioration_penalty),
    TrendRule("sustained_decline", rule_sustained_decline),
    TrendRule("single_year_decline", rule_single_year_decline),
    TrendRule("relative_decline", rule_relative_decline),
    TrendRule("roiic_roic_divergence_penalty", rule_roiic_roic_divergence),
    TrendRule("inflection_penalty_or_bonus", rule_inflection_penalty_or_bonus),
    TrendRule("cyclical_adjustment", rule_cyclical_adjustment),
    TrendRule("acceleration_adjustment", rule_acceleration_adjustment),
    TrendRule("roiic_positive_bonus", rule_roiic_positive_bonus),
    TrendRule("growth_momentum_bonus", rule_growth_momentum_bonus),
    TrendRule("earnings_quality_divergence", rule_earnings_quality_divergence),
    TrendRule("sustainable_growth_check", rule_sustainable_growth_check),
]

trend_rule_engine = TrendRuleEngine(DEFAULT_TREND_RULES)


# ============================================================================
# 4. 趋势分析器 (Trend Analyzer)
# ============================================================================

class TrendAnalyzer:
    """Encapsulate per-group trend calculations to keep the orchestrator lean."""

    _PROBE_RESULT_MAP: ClassVar[Dict[str, Tuple[str, type]]] = {
        "log_trend": ("trend_result", LogTrendResult),
        "volatility": ("volatility_result", VolatilityResult),
        "inflection": ("inflection_result", InflectionResult),
        "deterioration": ("deterioration_result", RecentDeteriorationResult),
        "cyclical": ("cyclical_result", CyclicalPatternResult),
        "rolling": ("rolling_result", RollingTrendResult),
        "robust": ("robust_result", RobustTrendResult),
    }

    def __init__(
        self,
        group_key: str,
        group_df: pd.DataFrame,
        metric_name: str,
        group_column: str,
        prefix: str,
        suffix: str,
        keep_cols: List[str],
        reference_metrics: Optional[List[str]] = None,
        logger: Optional[logging.Logger] = None,
        metric_probes: Optional[List[MetricProbe]] = None,
        config: Optional[TrendAnalyzerConfig] = None,
        field_schema: Optional[Iterable["TrendField"]] = None,
    ) -> None:
        self.group_key = group_key
        self.metric_name = metric_name
        self.group_column = group_column
        self.prefix = prefix
        self.suffix = suffix
        self.keep_cols = list(dict.fromkeys(keep_cols))
        self.logger = logger or logging.getLogger(__name__)

        self.config = config or TrendAnalyzerConfig()
        self.series_config: TrendSeriesConfig = self.config.series

        if field_schema is not None:
            self.field_schema = tuple(field_schema)
        else:
            configured_fields = tuple(self.config.output_fields) if self.config.output_fields else tuple()
            self.field_schema = configured_fields or tuple(trend_field_schema())

        if reference_metrics is not None:
            reference_candidates: Sequence[str] = reference_metrics
        else:
            reference_candidates = self.config.reference_metrics
        self.reference_metrics = list(dict.fromkeys(reference_candidates or []))

        if metric_probes is not None:
            probes_source: Sequence[MetricProbe] = metric_probes
        elif self.config.probes is not None:
            probes_source = self.config.probes
        else:
            probes_source = get_default_metric_probes()
        self.metric_probes: List[MetricProbe] = list(probes_source)

        self.group_df = self._ordered_group_df(group_df)

        self.valid: bool = True
        self.error_reason: str = ""

        self.values_list: List[float] = []
        self.weighted_avg: float = 0.0
        self.trend_result: LogTrendResult = empty_log_trend_result()
        self.volatility_result: VolatilityResult = empty_volatility_result()
        self.inflection_result: InflectionResult = empty_inflection_result()
        self.deterioration_result: RecentDeteriorationResult = empty_deterioration_result()
        self.cyclical_result: CyclicalPatternResult = empty_cyclical_result()
        self.rolling_result: RollingTrendResult = empty_rolling_result()
        self.robust_result: RobustTrendResult = empty_robust_result()

        self.latest_value: float = 0.0
        self.latest_vs_weighted_ratio: float = 1.0
        self.reference_stats: Dict[str, Dict[str, Any]] = {}

        self.extra_fields = {
            col: self.group_df[col].iloc[-1]
            for col in self.keep_cols
            if col in self.group_df.columns
        }
        self.industry = self.extra_fields.get("industry")

        self._prepare()

    # ------------------------------------------------------------------
    def _ordered_group_df(self, df: pd.DataFrame) -> pd.DataFrame:
        series_cfg = self.series_config
        if series_cfg.order_column and series_cfg.order_column in df.columns:
            return df.sort_values(series_cfg.order_column, kind="mergesort").reset_index(drop=True)
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    def _prepare(self) -> None:
        try:
            self.values_list = self._prepare_metric_series(self.metric_name)
            self.weighted_avg = self._compute_weighted_average()
            self._run_metric_probes()
        except FatalMetricProbeError as fatal_exc:
            self.valid = False
            self.error_reason = str(fatal_exc.original)
            self.logger.warning(
                "%s 指标%s致命失败: %s",
                self.group_key,
                self.metric_name,
                fatal_exc.original,
            )
            return
        except Exception as exc:  # fatal failure, skip this group entirely
            self.valid = False
            self.error_reason = str(exc)
            self.logger.warning("%s 指标%s预处理失败: %s", self.group_key, self.metric_name, exc)
            return

        self.reference_stats = self._compute_reference_metrics()

        self.latest_value = self.values_list[-1]
        self.latest_vs_weighted_ratio = (
            self.latest_value / self.weighted_avg if self.weighted_avg > 0 else 1.0
        )

    # ------------------------------------------------------------------
    def _prepare_metric_series(self, column: str) -> List[float]:
        if column not in self.group_df.columns:
            raise ValueError(f"缺少指标列: {column}")

        series_cfg = self.series_config
        values_array = self.group_df[column].to_numpy(dtype=float, copy=True)

        target_window = series_cfg.window_size
        if target_window is None and series_cfg.weights is not None:
            target_window = len(series_cfg.weights)

        if target_window is not None:
            if values_array.size < target_window and not series_cfg.allow_partial_window:
                raise ValueError(f"需要至少{target_window}期数据, 实际{values_array.size}期")
            if values_array.size > target_window:
                values_array = values_array[-target_window:]

        total_count = values_array.size
        finite_mask = np.isfinite(values_array) if series_cfg.drop_non_finite else ~np.isnan(values_array)
        valid_count = int(finite_mask.sum())

        if valid_count == 0:
            raise ValueError("全部为缺失值")

        if not series_cfg.allow_partial_window:
            min_required = max(1, int(np.ceil(total_count * series_cfg.min_valid_ratio)))
            if valid_count < min_required:
                raise ValueError("有效数据不足")

        if valid_count < total_count:
            values_array = self._fill_missing_values(values_array, finite_mask)

        if not np.all(np.isfinite(values_array)):
            raise ValueError("仍存在非法数值")

        return values_array.astype(float).tolist()

    # ------------------------------------------------------------------
    def _fill_missing_values(self, values_array: np.ndarray, finite_mask: np.ndarray) -> np.ndarray:
        strategy = self.series_config.fill_strategy

        if strategy == "median":
            median_val = float(np.median(values_array[finite_mask]))
            if not np.isfinite(median_val):
                raise ValueError("缺失值过多，无法计算中位数")
            return np.where(finite_mask, values_array, median_val)

        cleaned = values_array.astype(float, copy=True)
        cleaned[~finite_mask] = np.nan

        if strategy == "ffill":
            filled = pd.Series(cleaned, dtype=float).ffill().bfill()
        elif strategy == "bfill":
            filled = pd.Series(cleaned, dtype=float).bfill().ffill()
        elif strategy == "zero":
            filled = np.where(finite_mask, cleaned, 0.0)
            return filled.astype(float)
        elif strategy == "constant":
            if self.series_config.fill_value is None:
                raise ValueError("fill_value未配置")
            filled = np.where(finite_mask, cleaned, float(self.series_config.fill_value))
            return filled.astype(float)
        else:
            raise ValueError(f"不支持的填充策略: {strategy}")

        if isinstance(filled, pd.Series):
            if filled.isna().any():
                raise ValueError("缺失值过多，无法填充")
            return filled.to_numpy(dtype=float)

        if np.isnan(filled).any():
            raise ValueError("缺失值过多，无法填充")
        return filled.astype(float)

    # ------------------------------------------------------------------
    def _run_metric_probes(self) -> None:
        context = MetricProbeContext(
            group_key=self.group_key,
            metric_name=self.metric_name,
            industry=self.industry,
            group_df=self.group_df,
        )

        for probe in self.metric_probes:
            try:
                result = probe.compute(self.values_list, context)
            except Exception as exc:
                if getattr(probe, "fatal", False):
                    raise FatalMetricProbeError(probe.name, exc) from exc

                self.logger.warning(
                    "%s %s指标计算失败: %s, 使用默认值",
                    self.group_key,
                    probe.name,
                    exc,
                )
                result = probe.default(context)

            self._assign_probe_result(probe.name, result)

    # ------------------------------------------------------------------
    def _assign_probe_result(self, probe_name: str, result: Any) -> None:
        target = self._PROBE_RESULT_MAP.get(probe_name)
        if target:
            attr_name, expected_type = target
            if isinstance(result, expected_type):
                setattr(self, attr_name, result)
                return
            self.logger.debug(
                "%s 指标结果类型异常 '%s': 期待 %s, 实得 %s",
                self.group_key,
                probe_name,
                expected_type.__name__,
                type(result).__name__,
            )
            return

        self.logger.debug("%s 未识别的指标结果 '%s', 忽略: %r", self.group_key, probe_name, result)

    # ------------------------------------------------------------------
    def _compute_reference_metrics(self) -> Dict[str, Dict[str, Any]]:
        if not self.reference_metrics:
            return {}

        reference_stats: Dict[str, Dict[str, Any]] = {}
        for ref_metric in self.reference_metrics:
            if ref_metric not in self.group_df.columns:
                self.logger.debug("%s 参考指标缺失: %s", self.group_key, ref_metric)
                continue

            try:
                values = self._prepare_metric_series(ref_metric)
                if len(values) < 2:
                    continue

                weighted_avg = float(
                    calculate_weighted_average(values, weights=self.series_config.weights)
                )
                calculator = LogTrendCalculator()
                trend = calculator.calculate(values)

                rolling_calc = RollingTrendCalculator()
                rolling = rolling_calc.calculate(values)

                reference_stats[ref_metric.lower()] = {
                    "latest": values[-1],
                    "weighted_avg": weighted_avg,
                    "log_slope": trend.log_slope,
                    "r_squared": trend.r_squared,
                    "recent_3y_slope": rolling.recent_3y_slope,
                    "trend_acceleration": rolling.trend_acceleration,
                }
            except Exception as exc:
                self.logger.debug("%s 参考指标%s计算失败: %s", self.group_key, ref_metric, exc)

        return reference_stats

    # ------------------------------------------------------------------
    def _compute_weighted_average(self) -> float:
        try:
            return float(
                calculate_weighted_average(
                    self.values_list,
                    weights=self.series_config.weights,
                    adaptive=True,  # Enable adaptive weighting by default
                )
            )
        except Exception as exc:
            raise ValueError(f"加权平均计算失败: {exc}") from exc

    # ------------------------------------------------------------------
    def build_trend_vector(self) -> TrendVector:
        trend = self.trend_result
        volatility = self.volatility_result
        inflection = self.inflection_result
        deterioration = self.deterioration_result
        cyclical = self.cyclical_result
        rolling = self.rolling_result
        robust = self.robust_result
        quality = trend.quality

        return TrendVector(
            log_slope=trend.log_slope,
            r_squared=trend.r_squared,
            cv=volatility.cv,
            latest_value=self.latest_value,
            weighted_avg=self.weighted_avg,
            cagr_approx=trend.cagr_approx,
            total_decline_pct=deterioration.total_decline_pct,
            deterioration_result=deterioration.to_dict(),
            latest_vs_weighted_ratio=self.latest_vs_weighted_ratio,
            is_cyclical=cyclical.is_cyclical,
            current_phase=cyclical.current_phase,
            peak_to_trough_ratio=cyclical.peak_to_trough_ratio,
            has_deterioration=deterioration.has_deterioration,
            deterioration_severity=deterioration.severity,
            has_inflection=inflection.has_inflection,
            inflection_type=inflection.inflection_type,
            slope_change=inflection.slope_change,
            is_accelerating=rolling.is_accelerating,
            is_decelerating=rolling.is_decelerating,
            trend_acceleration=rolling.trend_acceleration,
            recent_3y_slope=rolling.recent_3y_slope,
            has_loss_years=quality.has_loss_years,
            loss_year_count=quality.loss_year_count,
            has_near_zero_years=quality.has_near_zero_years,
            near_zero_count=quality.near_zero_count,
            robust=robust,
            reference_metrics=self.reference_stats,
        )

    def build_snapshot(
        self,
        evaluation: "TrendEvaluationResult",
        vector: TrendVector,
    ) -> TrendSnapshot:
        return TrendSnapshot(
            group_key=self.group_key,
            metric_name=self.metric_name,
            vector=vector,
            evaluation=evaluation,
            trend=self.trend_result,
            volatility=self.volatility_result,
            inflection=self.inflection_result,
            deterioration=self.deterioration_result,
            cyclical=self.cyclical_result,
            rolling=self.rolling_result,
            robust=self.robust_result,
            quality=self.trend_result.quality,
            latest_value=self.latest_value,
            weighted_avg=self.weighted_avg,
            latest_vs_weighted_ratio=self.latest_vs_weighted_ratio,
            extra_fields=dict(self.extra_fields),
        )

    # ------------------------------------------------------------------
    def build_result_row(
        self,
        snapshot: TrendSnapshot,
        include_penalty: bool,
    ) -> Dict[str, Any]:
        row: Dict[str, Any] = {self.group_column: snapshot.group_key}

        for col, value in snapshot.extra_fields.items():
            row[col] = value

        metric_prefix = f"{self.prefix}{snapshot.metric_name}"
        suffix = self.suffix

        for field in self.field_schema:
            try:
                value = field.resolve(snapshot)
            except AttributeError as exc:
                self.logger.debug(
                    "%s 字段%s解析失败: %s",
                    self.group_key,
                    field.key,
                    exc,
                )
                value = None

            column_name = f"{metric_prefix}_{field.key}{suffix}"
            row[column_name] = value

        if include_penalty:
            penalties = snapshot.evaluation.penalty_details
            row[f"{metric_prefix}_penalty{suffix}"] = snapshot.evaluation.penalty
            row[f"{metric_prefix}_penalty_details{suffix}"] = "; ".join(penalties) if penalties else ""

        # Add Strategy Columns
        strategies = snapshot.evaluation.strategies
        row[f"{metric_prefix}_strategies{suffix}"] = ",".join(strategies) if strategies else ""
        row[f"{metric_prefix}_strategy_reasons{suffix}"] = "; ".join(snapshot.evaluation.strategy_reasons) if snapshot.evaluation.strategy_reasons else ""

        # Add specific boolean flags for common strategies
        for strategy_name in ["high_growth", "turnaround"]:
             col_name = f"{metric_prefix}_is_{strategy_name}{suffix}"
             row[col_name] = 1 if strategy_name in strategies else 0

        notes = snapshot.evaluation.auxiliary_notes
        if notes:
            row[f"{metric_prefix}_notes{suffix}"] = "; ".join(notes)

        return row


# ============================================================================
# 5. 配置解析器 (Config Resolver)
# ============================================================================

class ConfigResolver:
    """Resolves configuration based on industry and other factors."""

    def __init__(self, industry_configs: Optional[Dict[str, Dict[str, Any]]] = None):
        self.industry_configs = industry_configs or {}
        self._usage_stats: Dict[str, int] = {}

    def resolve(
        self,
        group_key: str,
        base_config: Dict[str, Any],
        group_df: pd.DataFrame,
        logger: Optional[logging.Logger] = None
    ) -> Tuple[Dict[str, Any], str]:
        """
        Resolve the final configuration for a specific group.
        Returns (resolved_config, industry).
        """
        industry = "default"
        if 'industry' in group_df.columns:
            industry_val = group_df['industry'].iloc[0]
            if isinstance(industry_val, str):
                industry = industry_val

        # Get industry category (e.g., "cyclical", "growth", "stable")
        # This might be used to look up configs if direct industry match fails
        # For now, we just use the industry name directly as per previous logic

        current_config = base_config.copy()

        # Apply industry-specific overrides
        if industry in self.industry_configs:
            current_config.update(self.industry_configs[industry])
            self._usage_stats[industry] = self._usage_stats.get(industry, 0) + 1
        else:
            # Try to find by category if not found by exact name
            category = get_industry_category(industry)
            if category in self.industry_configs:
                current_config.update(self.industry_configs[category])
                self._usage_stats[category] = self._usage_stats.get(category, 0) + 1
            else:
                self._usage_stats["default"] = self._usage_stats.get("default", 0) + 1

        return current_config, industry

    def usage_stats(self) -> Dict[str, int]:
        return self._usage_stats


# ============================================================================
# 6. 趋势评估器 (Trend Evaluator)
# ============================================================================

class TrendEvaluator:
    """Evaluates trend vectors against rules."""

    def __init__(self, logger: Optional[logging.Logger] = None, strategies: Optional[List[TrendStrategy]] = None):
        self.logger = logger or logging.getLogger(__name__)
        self.strategies = strategies or get_default_strategies()

    def evaluate(
        self,
        group_key: str,
        metric_name: str,
        config: Dict[str, Any],
        trend_vector: TrendVector
    ) -> TrendEvaluationResult:
        """
        Evaluate the trend vector and return an evaluation result.
        """
        # Convert dict config to TrendRuleConfig objects
        rule_config = TrendRuleConfig.from_dict(config)

        # Create context
        context = TrendContext.from_vector(group_key, metric_name, trend_vector)

        # Run rule engine
        outcome = trend_rule_engine.run(
            context,
            rule_config.parameters,
            rule_config.thresholds,
            self.logger
        )

        # Run strategies
        matched_strategies = []
        strategy_reasons = []
        strategy_bonus = 0.0

        for strategy in self.strategies:
            result = strategy.evaluate(context)
            if result.matched:
                matched_strategies.append(result.name)
                strategy_reasons.append(result.reason)
                strategy_bonus += result.score_boost
                if self.logger:
                    self.logger.info(f"🎯 {group_key} 命中策略 [{strategy.name}]: {result.reason}")

        # Calculate final score
        base_score = 100.0
        # Apply penalty first, then add strategy bonus
        final_score = max(0.0, base_score - outcome.penalty)
        final_score += strategy_bonus

        return TrendEvaluationResult(
            passes=outcome.passes,
            elimination_reason=outcome.elimination_reason,
            penalty=outcome.penalty,
            penalty_details=outcome.penalty_details,
            bonus_details=outcome.bonus_details,
            trend_score=final_score,
            auxiliary_notes=outcome.auxiliary_notes,
            strategies=matched_strategies,
            strategy_reasons=strategy_reasons
        )


# ============================================================================
# 7. 结果收集器 (Trend Result Collector)
# ============================================================================

class TrendResultCollector:
    """Collects analysis results and converts them to a DataFrame."""

    def __init__(self) -> None:
        self.results: List[Dict[str, Any]] = []

    def add(self, row: Dict[str, Any]) -> None:
        self.results.append(row)

    def to_dataframe(self) -> pd.DataFrame:
        if not self.results:
            return pd.DataFrame()
        return pd.DataFrame(self.results)
