"""
趋势分析器核心组件
==================

协调各类指标探针、行业配置与数据预处理，生成 `TrendSnapshot` 与
`TrendVector`。该模块负责驱动 TrendAnalyzer 的生命周期，是业务趋势
引擎的调度中枢。
"""

from __future__ import annotations

import logging
from typing import Any, ClassVar, Dict, Iterable, List, Optional, Sequence, Tuple, TYPE_CHECKING

import numpy as np
import pandas as pd

from .trend_analysis import (
    calculate_log_trend_slope,
    calculate_rolling_trend,
    calculate_weighted_average,
)
from .metric_probes import (
    FatalMetricProbeError,
    MetricProbe,
    MetricProbeContext,
    get_default_metric_probes,
)
from .trend_models import (
    CyclicalPatternResult,
    DataQualitySummary,
    InflectionResult,
    LogTrendResult,
    RecentDeteriorationResult,
    RollingTrendResult,
    TrendSnapshot,
    TrendVector,
    VolatilityResult,
)
from .trend_defaults import (
    empty_cyclical_result,
    empty_deterioration_result,
    empty_inflection_result,
    empty_log_trend_result,
    empty_rolling_result,
    empty_volatility_result,
)
from .trend_schema import trend_field_schema
from .trend_settings import TrendAnalyzerConfig, TrendSeriesConfig

if TYPE_CHECKING:  # pragma: no cover - type checking only
    from ..engines.duckdb_trend import TrendEvaluationResult
    from .trend_schema import TrendField


class TrendAnalyzer:
    """Encapsulate per-group trend calculations to keep the orchestrator lean."""

    _PROBE_RESULT_MAP: ClassVar[Dict[str, Tuple[str, type]]] = {
        "log_trend": ("trend_result", LogTrendResult),
        "volatility": ("volatility_result", VolatilityResult),
        "inflection": ("inflection_result", InflectionResult),
        "deterioration": ("deterioration_result", RecentDeteriorationResult),
        "cyclical": ("cyclical_result", CyclicalPatternResult),
        "rolling": ("rolling_result", RollingTrendResult),
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
                trend = calculate_log_trend_slope(values)
                rolling = calculate_rolling_trend(values)

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

        notes = snapshot.evaluation.auxiliary_notes
        row[f"{metric_prefix}_notes{suffix}"] = "; ".join(notes) if notes else ""

        return row
