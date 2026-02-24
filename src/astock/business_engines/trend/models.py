"""
趋势分析数据模型
================

集中定义趋势分析过程中使用的 dataclass 结果、配置结构以及字段 Schema。
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple, Protocol, TYPE_CHECKING
import pandas as pd

if TYPE_CHECKING:
    from .pipeline.stages.pattern_synthesis import TrendResult

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
    # 专业增强字段 v2.0
    detrended_cv: float = 0.0  # 去趋势后的CV
    has_arch_effect: bool = False  # 是否有ARCH效应（波动聚集）
    arch_correlation: float = 0.0  # ARCH相关系数
    volatility_regime: str = "stable"  # 波动率体制: stable, increasing_vol, decreasing_vol
    volatility_change_ratio: float = 1.0  # 波动率变化比率 (后期/前期)


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
    # 专业增强字段 v2.0
    consecutive_decline_years: int = 0  # 连续下跌年数
    deterioration_acceleration: float = 0.0  # 恶化加速度
    deterioration_pattern: str = "none"  # 恶化模式分类
    deterioration_probability: float = 0.0  # 贝叶斯恶化概率 (0-1)


@dataclass
class CyclicalPatternResult(SerializableResult):
    """
    周期性分析结果

    包含 HP滤波、Hurst指数、ACF 等专业分析的完整输出。
    v2.0: 添加 hp_cycle_amplitude, hurst_exponent, acf_lag1 等关键字段
    """
    is_cyclical: bool
    peak_to_trough_ratio: float
    has_middle_peak: bool
    has_wave_pattern: bool
    trend_r_squared: float
    cv: float
    current_phase: str
    cycle_position: str  # 周期位置: bottom, mid_up, top, mid_down, unknown
    fft_dominant_period: float  # FFT检测的主导周期(年)，0表示无周期
    industry_cyclical: bool
    cyclical_confidence: float
    peak_to_trough_threshold: float
    trend_r_squared_max: float
    cv_threshold: float
    industry: str

    # === v2.0 新增: HP滤波分析 ===
    hp_cycle_amplitude: float = 0.0  # HP滤波周期振幅 (标准化)
    hp_cycle_volatility: float = 0.0  # HP滤波周期波动率

    # === v2.0 新增: Hurst指数分析 ===
    hurst_exponent: float = 0.5  # Hurst指数 (0.5=随机游走, <0.5=均值回归, >0.5=趋势持续)
    hurst_interpretation: str = "random_walk"  # 解释: mean_reverting | random_walk | trending
    hurst_confidence: float = 0.0  # Hurst估计置信度

    # === v2.0 新增: ACF分析 ===
    acf_lag1: float = 0.0  # 一阶自相关系数
    acf_has_cyclical_pattern: bool = False  # ACF是否显示周期模式
    ljung_box_pvalue: float = 1.0  # Ljung-Box检验p值

    # === 原有字段 (列表类型移至最后) ===
    confidence_factors: List[str] = field(default_factory=list)
    warnings: List[TrendWarning] = field(default_factory=list)


@dataclass
class RollingTrendResult(SerializableResult):
    recent_3y_slope: float
    recent_3y_r_squared: float
    full_5y_slope: float
    full_5y_r_squared: float
    trend_acceleration: float  # 原始加速度 (recent - early)，未被R²压缩
    acceleration_confidence: float  # 加速度的置信度 (基于R²)
    is_accelerating: bool
    is_decelerating: bool
    early_3y_slope: float  # 前3年斜率 (年1-3)
    early_3y_r_squared: float  # 前3年R²
    warnings: List[TrendWarning] = field(default_factory=list)


# ============================================================================
# 规则上下文模型 (被 evaluators 层使用)
# ============================================================================
# 注意: 具体的规则实现已迁移到 evaluators/threshold/ 模块
# 此处只保留被共享的数据模型

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
    cycle_position: str  # 周期位置: bottom, mid_up, top, mid_down, unknown
    fft_dominant_period: float  # FFT检测的主导周期(年)
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
    warnings: List[TrendWarning] = field(default_factory=list)
    # 专业增强字段 v2.0
    deterioration_probability: float = 0.0  # 贝叶斯恶化概率
    deterioration_pattern: str = "none"  # 恶化模式
    wls_slope: Optional[float] = None  # WLS斜率
    bootstrap_ci_low: Optional[float] = None  # Bootstrap置信区间下界
    bootstrap_ci_high: Optional[float] = None  # Bootstrap置信区间上界
    has_arch_effect: bool = False  # ARCH效应
    volatility_regime: str = "stable"  # 波动率体制
    volatility_change_ratio: float = 1.0  # 波动率变化比率
    detrended_cv: float = 0.0  # 去趋势CV
    # 改进规则 v2.1 - 原始数据支持
    raw_values: Optional[List[float]] = None  # 原始时间序列数据
    max_value: Optional[float] = None  # 历史最大值

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
            cycle_position=vector.cycle_position,
            fft_dominant_period=vector.fft_dominant_period,
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
            warnings=vector.warnings,
            # 专业增强字段 v2.0
            deterioration_probability=vector.deterioration_probability,
            deterioration_pattern=vector.deterioration_pattern,
            wls_slope=vector.wls_slope,
            bootstrap_ci_low=vector.bootstrap_ci_low,
            bootstrap_ci_high=vector.bootstrap_ci_high,
            has_arch_effect=vector.has_arch_effect,
            volatility_regime=vector.volatility_regime,
            volatility_change_ratio=vector.volatility_change_ratio,
            detrended_cv=vector.detrended_cv,
            # 改进规则 v2.1 - 原始数据支持
            raw_values=list(vector.raw_values) if vector.raw_values else None,
            max_value=vector.max_value,
        )

    @classmethod
    def from_trend_result(
        cls,
        group_key: str,
        metric_name: str,
        result: "TrendResult",  # 从 pipeline.stages.pattern_synthesis
    ) -> "TrendContext":
        """
        从 Pipeline 的 TrendResult 构建 TrendContext

        这是统一数据传递路径的核心方法:
        - evaluators 通过此方法获取数据
        - truth 系统直接访问 TrendResult._probe_results
        - 两者共享同一数据源 (TrendResult)

        Args:
            group_key: 股票代码
            metric_name: 指标名称
            result: Pipeline 输出的 TrendResult

        Returns:
            TrendContext: evaluators 使用的上下文
        """
        # 从探针结果获取完整数据
        det = result.deterioration_result
        cyc = result.cyclical_result
        inf = result.inflection_result
        roll = result.rolling_result
        rob = result.robust_result
        vol = result.volatility_result
        log_trend = result.log_trend_result

        return cls(
            group_key=group_key,
            metric_name=metric_name,
            log_slope=result.slope,
            r_squared=result.wls_r_squared,
            cv=result.cv,
            latest_value=0.0,  # 需要从原始数据获取
            weighted_avg=result.weighted_mean,
            cagr_approx=result.slope,  # log slope ≈ CAGR
            total_decline_pct=det.total_decline_pct,
            deterioration_result=result.deterioration_dict,
            latest_vs_weighted_ratio=0.0,  # 需要从原始数据计算
            is_cyclical=result.is_cyclical,
            current_phase=result.current_phase,
            cycle_position=result.cycle_position,
            fft_dominant_period=result.fft_dominant_period,
            peak_to_trough_ratio=cyc.peak_to_trough_ratio,
            has_deterioration=det.deterioration_probability > 0.5,
            deterioration_severity=det.severity,
            has_inflection=result.inflection_detected,
            inflection_type=getattr(inf, 'inflection_type', 'none'),
            slope_change=getattr(inf, 'slope_change', 0.0),
            is_accelerating=roll.is_accelerating,
            is_decelerating=roll.is_decelerating,
            trend_acceleration=result.acceleration,
            recent_3y_slope=result.rolling_3y_slope,
            has_loss_years=result.has_loss_years,
            loss_year_count=result.loss_year_count,
            has_near_zero_years=result.has_near_zero,
            near_zero_count=result.near_zero_count,
            robust_slope=result.theil_sen_slope,
            mann_kendall_tau=result.mann_kendall_tau,
            mann_kendall_p_value=result.mann_kendall_pvalue,
            reference_metrics={},
            warnings=[],
            # 专业增强字段 v2.0
            deterioration_probability=result.deterioration_probability,
            deterioration_pattern=result.deterioration_pattern,
            wls_slope=result.wls_slope,
            bootstrap_ci_low=result.bootstrap_ci_lower,
            bootstrap_ci_high=result.bootstrap_ci_upper,
            has_arch_effect=result.arch_effect_detected,
            volatility_regime=vol.volatility_regime if hasattr(vol, 'volatility_regime') else 'stable',
            volatility_change_ratio=vol.volatility_change_ratio if hasattr(vol, 'volatility_change_ratio') else 1.0,
            detrended_cv=result.detrended_cv,
            # 改进规则 v2.1 - 原始数据支持
            raw_values=list(result.values) if result.values else None,
            max_value=max(result.values) if result.values else None,
        )


# ============================================================================
# 评估结果模型 (被 evaluators 层使用)
# ============================================================================
# 注意: TrendRuleParameters, TrendRuleConfig, TrendRule 等规则相关类
#       已迁移到 evaluators/threshold/ 模块，此处只保留共享的结果模型

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
    cycle_position: str  # 周期位置: bottom, mid_up, top, mid_down, unknown
    fft_dominant_period: float  # FFT检测的主导周期(年)
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
    warnings: List[TrendWarning] = field(default_factory=list)
    # 专业增强字段 v2.0 - 从各探针结果提取
    deterioration_probability: float = 0.0  # 贝叶斯恶化概率
    deterioration_pattern: str = "none"  # 恶化模式
    wls_slope: Optional[float] = None  # WLS斜率
    bootstrap_ci_low: Optional[float] = None  # Bootstrap CI下界
    bootstrap_ci_high: Optional[float] = None  # Bootstrap CI上界
    has_arch_effect: bool = False  # ARCH效应
    volatility_regime: str = "stable"  # 波动率体制
    volatility_change_ratio: float = 1.0  # 波动率变化比率
    detrended_cv: float = 0.0  # 去趋势CV
    # 改进规则 v2.1 - 原始数据支持
    raw_values: Tuple[float, ...] = field(default_factory=tuple)  # 原始时间序列数据
    max_value: Optional[float] = None  # 历史最大值

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

    # 多时间窗口分析结果(可选)
    full_data_years: int = 0           # 全量数据年数
    trend_window_years: int = 5        # 趋势计算窗口年数
    has_structural_break: bool = False # 是否存在结构断点
    break_year_index: Optional[int] = None     # 断点位置
    break_effect_size: float = 0.0     # 断点效应量
    data_regime: str = "stable"        # 数据体制: stable/broken/transitional


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
    """Control how raw metric series are prepared ahead of probe execution.

    双窗口设计:
    - window_size: 用于趋势计算(斜率/CAGR/加权平均)的近期窗口
      - None: 使用全量数据计算趋势（与断点检测使用相同数据范围）
      - 整数N: 只使用最近N年数据计算趋势
    - 全量数据: 用于断点检测和周期分析(自动使用输入的全部数据)
    """

    window_size: Optional[int] = None  # 趋势计算窗口(近N年)，None表示使用全部数据
    order_column: Optional[str] = "end_date"
    weights: Optional[Sequence[float]] = None
    fill_strategy: Literal["median", "ffill", "bfill", "zero", "constant"] = "median"
    fill_value: Optional[float] = None
    min_valid_ratio: float = 0.6
    allow_partial_window: bool = False
    drop_non_finite: bool = True

    # 多时间窗口分析配置
    enable_multi_horizon: bool = True  # 是否启用多时间窗口分析
    break_detection_threshold: float = 0.20  # 断点效应量阈值(均值变化比例)
    # 阈值说明:
    # - 0.15: 宽松，可能检测到较多断点，适合探索性分析
    # - 0.20: 推荐，平衡灵敏度和准确性
    # - 0.30: 严格，只检测显著断点，适合保守分析


@dataclass(frozen=True)
class TrendAnalyzerConfig:
    """Bundle analyzer-wide tuning knobs so metric definitions stay declarative.

    关键配置:
    - series.window_size: 趋势计算使用近N年数据
    - series.enable_multi_horizon: 是否启用断点/周期分析(使用全量数据)
    """

    series: TrendSeriesConfig = field(default_factory=TrendSeriesConfig)
    probes: Optional[Sequence["MetricProbe"]] = None
    output_fields: Tuple["TrendField", ...] = field(default_factory=tuple)
    reference_metrics: Sequence[str] = field(default_factory=tuple)

