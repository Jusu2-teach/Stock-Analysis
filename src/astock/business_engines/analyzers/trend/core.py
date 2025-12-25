"""
趋势分析核心模块
================

整合趋势分析的核心逻辑，包括：
1. 默认值生成器
2. 指标探针 (Metric Probes)
3. 趋势分析器 (Trend Analyzer)
4. 配置解析器 (Config Resolver)
5. 结果收集器 (Trend Result Collector)

此模块是趋势分析业务逻辑的单一入口。

**架构说明**:
- Probe 层是纯数学趋势分析，不包含任何行业阈值或业务规则
- 评估规则和策略已迁移到 evaluators/threshold/ 模块
- TrendRuleEngine、TrendEvaluator 现在位于 evaluators.threshold.engine
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
    TrendField,
    MetricProbeContext,
    TrendAnalyzerConfig,
    TrendSeriesConfig,
    TrendEvaluationResult,
)
from .config import (
    get_default_config,
    DEFAULT_CV_THRESHOLDS,
)

# 移除 evaluators 依赖 - 趋势分析层是纯数学层
# 业务逻辑（行业阈值、周期性判断）应在 evaluators/truth 层处理
# 此处只使用统计学标准阈值
from .probes.common import (
    calculate_weighted_average,
    FatalMetricProbeError,
)
# 导入探针类
from .probes.log_trend_probe import LogTrendProbe
from .probes.volatility_probe import VolatilityProbe as VolatilityProbeImpl

def trend_field_schema() -> List[TrendField]:
    """返回默认的趋势分析字段 schema

    定义输出 DataFrame 中需要的字段及其来源路径。
    """
    return [
        # 核心趋势指标
        TrendField("slope", "trend_result.slope", "趋势斜率", "", "core"),
        TrendField("r_squared", "trend_result.r_squared", "R²拟合优度", "", "core"),
        TrendField("p_value", "trend_result.p_value", "统计显著性", "", "core"),
        TrendField("cagr", "trend_result.cagr", "复合增长率", "%", "core"),
        TrendField("trend_direction", "trend_result.trend_direction", "趋势方向", "", "core"),

        # 波动性指标
        TrendField("cv", "volatility_result.cv", "变异系数", "", "volatility"),
        TrendField("volatility", "volatility_result.volatility", "波动率", "", "volatility"),
        TrendField("zscore", "volatility_result.zscore", "Z分数", "", "volatility"),

        # 加权平均
        TrendField("weighted_avg", "weighted_avg", "加权平均值", "", "core"),

        # 最新值
        TrendField("latest_value", "latest_value", "最新值", "", "core"),
    ]


from .probes.inflection_probe import InflectionProbe as InflectionProbeImpl
from .probes.deterioration_probe import DeteriorationProbe as DeteriorationProbeImpl
from .probes.cyclical_probe import CyclicalProbe as CyclicalProbeImpl
from .probes.rolling_probe import RollingProbe
from .probes.robust_probe import RobustTrendProbe
from .probes.multi_horizon_probe import (
    MultiHorizonProbe,
    StructuralBreakDetector,
    MultiHorizonResult,
    StructuralBreakResult,
    BreakType,
)

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
        # 专业增强字段 v2.0
        detrended_cv=0.0,
        has_arch_effect=False,
        arch_correlation=0.0,
        volatility_regime="stable",
        volatility_change_ratio=1.0,
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
        # 专业增强字段 v2.0
        consecutive_decline_years=0,
        deterioration_acceleration=0.0,
        deterioration_pattern="none",
        deterioration_probability=0.0,
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
        cycle_position="unknown",
        fft_dominant_period=0.0,
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
        acceleration_confidence=0.0,
        is_accelerating=False,
        is_decelerating=False,
        early_3y_slope=0.0,
        early_3y_r_squared=0.0,
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


class LogTrendMetricProbe(BaseMetricProbe):
    name = "log_trend"
    fatal = True

    def compute(self, values: List[float], context: MetricProbeContext) -> LogTrendResult:
        probe = LogTrendProbe()
        return probe.compute(values)

    def default(self, context: MetricProbeContext) -> LogTrendResult:
        return empty_log_trend_result()


class VolatilityMetricProbe(BaseMetricProbe):
    name = "volatility"

    def compute(self, values: List[float], context: MetricProbeContext) -> VolatilityResult:
        probe = VolatilityProbeImpl()
        # 纯数学层：使用默认统计阈值，不引入业务逻辑
        # 业务逻辑（如行业特定阈值）在 evaluators 层处理
        return probe.compute(values, cv_thresholds=DEFAULT_CV_THRESHOLDS)

    def default(self, context: MetricProbeContext) -> VolatilityResult:
        return empty_volatility_result()


class InflectionMetricProbe(BaseMetricProbe):
    name = "inflection"

    def compute(self, values: List[float], context: MetricProbeContext) -> InflectionResult:
        probe = InflectionProbeImpl()
        return probe.compute(values)

    def default(self, context: MetricProbeContext) -> InflectionResult:
        return empty_inflection_result()


class DeteriorationMetricProbe(BaseMetricProbe):
    name = "deterioration"

    def compute(self, values: List[float], context: MetricProbeContext) -> RecentDeteriorationResult:
        probe = DeteriorationProbeImpl()
        # 纯数学层：使用默认统计阈值，不引入业务逻辑
        # 业务逻辑（如行业特定阈值、先验概率）在 evaluators 层处理
        return probe.compute(
            values,
            decline_threshold_pct=-15.0,  # 默认15%下跌阈值
            decline_threshold_abs=-2.0,
            high_level_threshold=20.0,
            prior_probability=0.3,  # 默认先验
            industry=context.industry,
        )

    def default(self, context: MetricProbeContext) -> RecentDeteriorationResult:
        return empty_deterioration_result()


class CyclicalMetricProbe(BaseMetricProbe):
    name = "cyclical"

    def compute(self, values: List[float], context: MetricProbeContext) -> CyclicalPatternResult:
        probe = CyclicalProbeImpl()
        # 纯数学层：使用默认统计阈值，不引入业务逻辑
        # 业务逻辑（如行业特定阈值、先验概率）在 evaluators 层处理
        return probe.compute(
            values,
            prior_probability=0.3,  # 默认先验
            cv_threshold=0.3,  # 默认CV阈值
            peak_valley_threshold=2.0,  # 默认峰谷比
            industry=context.industry,
        )

    def default(self, context: MetricProbeContext) -> CyclicalPatternResult:
        return empty_cyclical_result()


class RollingMetricProbe(BaseMetricProbe):
    name = "rolling"

    def compute(self, values: List[float], context: MetricProbeContext) -> RollingTrendResult:
        probe = RollingProbe()
        return probe.compute(values)

    def default(self, context: MetricProbeContext) -> RollingTrendResult:
        return empty_rolling_result()


class RobustMetricProbe(BaseMetricProbe):
    name = "robust"

    def compute(self, values: List[float], context: MetricProbeContext) -> RobustTrendResult:
        probe = RobustTrendProbe()
        return probe.compute(values, context=context)

    def default(self, context: MetricProbeContext) -> RobustTrendResult:
        probe = RobustTrendProbe()
        return probe.default()


def get_default_metric_probes() -> List[MetricProbe]:
    """Return the default suite of metric probes."""
    return [
        LogTrendMetricProbe(),
        VolatilityMetricProbe(),
        InflectionMetricProbe(),
        DeteriorationMetricProbe(),
        CyclicalMetricProbe(),
        RollingMetricProbe(),
        RobustMetricProbe(),
    ]


# ============================================================================
# 3. 趋势分析器 (Trend Analyzer)
# ============================================================================

class TrendAnalyzer:
    """Encapsulate per-group trend calculations to keep the orchestrator lean.

    双窗口设计:
    - full_values_list: 全量数据，用于断点检测和周期分析
    - values_list: 近N年数据(window_size)，用于趋势计算
    """

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

        # 双窗口数据
        self.full_values_list: List[float] = []  # 全量数据(断点/周期分析)
        self.values_list: List[float] = []       # 近N年数据(趋势计算)

        self.weighted_avg: float = 0.0
        self.trend_result: LogTrendResult = empty_log_trend_result()
        self.volatility_result: VolatilityResult = empty_volatility_result()
        self.inflection_result: InflectionResult = empty_inflection_result()
        self.deterioration_result: RecentDeteriorationResult = empty_deterioration_result()
        self.cyclical_result: CyclicalPatternResult = empty_cyclical_result()
        self.rolling_result: RollingTrendResult = empty_rolling_result()
        self.robust_result: RobustTrendResult = empty_robust_result()

        # 多时间窗口分析结果
        self.multi_horizon_result: Optional[MultiHorizonResult] = None
        self.structural_break: Optional[StructuralBreakResult] = None

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
        """准备数据：双窗口设计

        1. full_values_list: 全量数据，用于断点检测和周期分析
        2. values_list: 趋势计算数据
           - window_size=None: 使用全量数据(与断点检测相同)
           - window_size=N: 只用最近N年数据
        """
        try:
            # 步骤1: 准备全量数据(用于断点/周期分析)
            self.full_values_list = self._prepare_full_metric_series(self.metric_name)

            # 步骤2: 准备趋势计算数据
            # window_size=None 表示使用全量数据，否则截取最近N年
            if self.series_config.window_size is None:
                # 不截断，直接使用全量数据
                self.values_list = self.full_values_list.copy()
            else:
                self.values_list = self._prepare_trend_series(self.metric_name)

            # 步骤3: 多时间窗口分析(断点检测+周期分析)
            # 条件: 启用配置 且 数据量足够(至少6年才有意义做断点检测)
            if self.series_config.enable_multi_horizon and len(self.full_values_list) >= 6:
                self._run_multi_horizon_analysis()

            # 步骤4: 计算加权平均和运行探针
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
    def _prepare_full_metric_series(self, column: str) -> List[float]:
        """准备全量数据序列(用于断点检测和周期分析)"""
        if column not in self.group_df.columns:
            raise ValueError(f"缺少指标列: {column}")

        series_cfg = self.series_config
        values_array = self.group_df[column].to_numpy(dtype=float, copy=True)

        # 全量数据：不做窗口截断
        total_count = values_array.size
        finite_mask = np.isfinite(values_array) if series_cfg.drop_non_finite else ~np.isnan(values_array)
        valid_count = int(finite_mask.sum())

        if valid_count == 0:
            raise ValueError("全部为缺失值")

        if valid_count < total_count:
            values_array = self._fill_missing_values(values_array, finite_mask)

        if not np.all(np.isfinite(values_array)):
            raise ValueError("仍存在非法数值")

        return values_array.astype(float).tolist()

    # ------------------------------------------------------------------
    def _prepare_trend_series(self, column: str) -> List[float]:
        """准备趋势计算数据序列(截取最近N年，由window_size控制)

        注意: 此方法仅在 window_size 不为 None 时被调用
        """
        if column not in self.group_df.columns:
            raise ValueError(f"缺少指标列: {column}")

        series_cfg = self.series_config
        values_array = self.group_df[column].to_numpy(dtype=float, copy=True)

        target_window = series_cfg.window_size
        if target_window is None and series_cfg.weights is not None:
            target_window = len(series_cfg.weights)

        # 窗口截断：只取最近N年
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
    def _run_multi_horizon_analysis(self) -> None:
        """运行多时间窗口分析(断点检测+周期分析)"""
        try:
            # 使用全量数据进行断点检测
            break_detector = StructuralBreakDetector(
                min_segment=3,
                effect_size_threshold=self.series_config.break_detection_threshold
            )
            self.structural_break = break_detector.detect(self.full_values_list)

            # 如果检测到断点，记录日志
            if self.structural_break.has_break:
                self.logger.debug(
                    "%s 检测到结构断点: 类型=%s, 位置=%d, 效应量=%.2f",
                    self.group_key,
                    self.structural_break.break_type.value,
                    self.structural_break.break_point or -1,
                    self.structural_break.effect_size
                )

            # 完整的多时间窗口分析
            # recent_years: 如果 window_size 为 None，使用全量数据长度
            recent_years = self.series_config.window_size or len(self.full_values_list)
            horizon_analyzer = MultiHorizonProbe(
                recent_years=recent_years,
                break_threshold=self.series_config.break_detection_threshold
            )
            self.multi_horizon_result = horizon_analyzer.compute(
                self.full_values_list,
                metric_name=self.metric_name
            )

        except Exception as exc:
            self.logger.debug(
                "%s 多时间窗口分析失败: %s",
                self.group_key, exc
            )
            self.structural_break = None
            self.multi_horizon_result = None

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
                result = probe.compute(self.values_list, context=context)
            except TypeError:
                # 尝试不传 context（兼容旧版探针签名）
                try:
                    result = probe.compute(self.values_list)
                except Exception as exc2:
                    self.logger.warning(
                        "%s %s指标计算失败: %s, 使用默认值",
                        self.group_key,
                        probe.name,
                        exc2,
                    )
                    result = self._safe_default(probe, context)
            except Exception as exc:
                if getattr(probe, "fatal", False):
                    raise FatalMetricProbeError(probe.name, exc) from exc

                self.logger.warning(
                    "%s %s指标计算失败: %s, 使用默认值",
                    self.group_key,
                    probe.name,
                    exc,
                )
                result = self._safe_default(probe, context)

            self._assign_probe_result(probe.name, result)

    # ------------------------------------------------------------------
    def _safe_default(self, probe: Any, context: MetricProbeContext) -> Any:
        """安全调用探针的 default 方法，兼容不同签名"""
        try:
            return probe.default(context=context)
        except TypeError:
            try:
                return probe.default()
            except Exception:
                return None

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
                values = self._prepare_trend_series(ref_metric)
                if len(values) < 2:
                    continue

                weighted_avg = float(
                    calculate_weighted_average(values, weights=self.series_config.weights)
                )
                trend_probe = LogTrendProbe()
                trend = trend_probe.compute(values)

                rolling_probe = RollingProbe()
                rolling = rolling_probe.compute(values)

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

        # 收集所有探针的 warnings
        all_warnings = []
        all_warnings.extend(trend.warnings or [])
        all_warnings.extend(volatility.warnings or [])
        all_warnings.extend(inflection.warnings or [])
        all_warnings.extend(deterioration.warnings or [])
        all_warnings.extend(cyclical.warnings or [])
        all_warnings.extend(rolling.warnings or [])
        all_warnings.extend(robust.warnings or [])

        # 从 trend.metadata 提取 WLS 和 Bootstrap 信息
        trend_metadata = trend.metadata or {}
        bootstrap_ci = trend_metadata.get("bootstrap_ci", {})

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
            cycle_position=cyclical.cycle_position,
            fft_dominant_period=cyclical.fft_dominant_period,
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
            warnings=all_warnings,
            # 专业增强字段 v2.0
            deterioration_probability=deterioration.deterioration_probability,
            deterioration_pattern=deterioration.deterioration_pattern,
            wls_slope=trend_metadata.get("wls_slope"),
            bootstrap_ci_low=bootstrap_ci.get("low"),
            bootstrap_ci_high=bootstrap_ci.get("high"),
            has_arch_effect=volatility.has_arch_effect,
            volatility_regime=volatility.volatility_regime,
            volatility_change_ratio=volatility.volatility_change_ratio,
            detrended_cv=volatility.detrended_cv,
            # 改进规则 v2.1 - 原始数据支持
            raw_values=tuple(self.values_list),
            max_value=max(self.values_list) if self.values_list else None,
        )

    def build_snapshot(
        self,
        evaluation: "TrendEvaluationResult",
        vector: TrendVector,
    ) -> TrendSnapshot:
        # 多时间窗口分析结果
        has_break = False
        break_idx = None
        break_effect = 0.0
        data_regime = "stable"

        if self.structural_break is not None:
            has_break = self.structural_break.has_break
            break_idx = self.structural_break.break_point
            break_effect = self.structural_break.effect_size

        if self.multi_horizon_result is not None:
            data_regime = self.multi_horizon_result.data_regime

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
            # 多时间窗口分析结果
            full_data_years=len(self.full_values_list),
            trend_window_years=len(self.values_list),
            has_structural_break=has_break,
            break_year_index=break_idx,
            break_effect_size=break_effect,
            data_regime=data_regime,
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

        # 多时间窗口分析结果
        row[f"{metric_prefix}_full_years{suffix}"] = snapshot.full_data_years
        row[f"{metric_prefix}_trend_years{suffix}"] = snapshot.trend_window_years
        row[f"{metric_prefix}_has_break{suffix}"] = 1 if snapshot.has_structural_break else 0
        row[f"{metric_prefix}_break_idx{suffix}"] = snapshot.break_year_index
        row[f"{metric_prefix}_break_effect{suffix}"] = snapshot.break_effect_size
        row[f"{metric_prefix}_regime{suffix}"] = snapshot.data_regime

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
        根据 group_key 查找并合并配置。

        纯数学层只做配置查找，不做任何业务逻辑判断。

        Returns:
            (resolved_config, config_key): 合并后的配置和使用的配置键名
        """
        config_key = "default"
        if 'industry' in group_df.columns:
            val = group_df['industry'].iloc[0]
            if isinstance(val, str):
                config_key = val

        current_config = base_config.copy()

        # 简单配置查找与合并
        if config_key in self.industry_configs:
            current_config.update(self.industry_configs[config_key])
            self._usage_stats[config_key] = self._usage_stats.get(config_key, 0) + 1
        else:
            self._usage_stats["default"] = self._usage_stats.get("default", 0) + 1

        return current_config, config_key

    def usage_stats(self) -> Dict[str, int]:
        return self._usage_stats


# ============================================================================
# 5. 结果收集器 (Trend Result Collector)
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
