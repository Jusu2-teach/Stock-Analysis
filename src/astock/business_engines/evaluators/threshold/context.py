"""
Evaluation Context
==================

评估上下文：从 ProbeOutputs 构建规则评估所需的上下文。

核心职责：
1. 从 ProbeOutputs 提取关键字段
2. 提供统一的上下文接口
3. 支持向后兼容（与现有 TrendContext 兼容）

设计原则：
- 扁平化：将探针结果展平为直接可访问的字段
- 类型安全：所有字段都有明确类型
- 可扩展：支持添加额外上下文字段
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ...core.probe_engine.builders import ProbeOutputs


@dataclass
class EvaluationContext:
    """
    评估上下文

    将 ProbeOutputs 中的探针结果展平为规则可直接访问的字段。

    字段来源说明：
    - log_trend 探针: log_slope, r_squared, cagr_approx, p_value
    - volatility 探针: cv, detrended_cv, has_arch_effect, volatility_regime
    - cyclical 探针: is_cyclical, cycle_position, current_phase
    - deterioration 探针: has_deterioration, deterioration_probability, total_decline_pct
    - rolling 探针: recent_3y_slope, trend_acceleration, is_accelerating
    - robust 探针: robust_slope, mann_kendall_tau
    - inflection 探针: has_inflection, inflection_type, slope_change

    Attributes:
        group_key: 分组键（如公司代码）
        metric_name: 指标名称

        # 基本数据
        latest_value: 最新值
        weighted_avg: 加权平均值
        latest_vs_weighted_ratio: 最新值/加权平均

        # 对数趋势
        log_slope: 对数斜率
        r_squared: R²
        cagr_approx: 近似 CAGR
        p_value: p 值

        # 波动性
        cv: 变异系数
        detrended_cv: 去趋势 CV
        has_arch_effect: 是否有 ARCH 效应
        volatility_regime: 波动率体制
        volatility_change_ratio: 波动率变化比

        # 周期性
        is_cyclical: 是否周期性
        cycle_position: 周期位置 (0-1)
        current_phase: 当前阶段
        peak_to_trough_ratio: 峰谷比

        # 恶化检测
        has_deterioration: 是否恶化
        deterioration_severity: 恶化严重程度
        deterioration_probability: 恶化概率
        deterioration_pattern: 恶化模式
        total_decline_pct: 总跌幅
        consecutive_decline_years: 连续下跌年数

        # 滚动窗口
        recent_3y_slope: 近3年斜率
        recent_5y_slope: 近5年斜率
        trend_acceleration: 趋势加速度
        is_accelerating: 是否加速
        is_decelerating: 是否减速

        # 稳健趋势
        robust_slope: 稳健斜率
        mann_kendall_tau: Mann-Kendall tau

        # 拐点检测
        has_inflection: 是否有拐点
        inflection_type: 拐点类型
        slope_change: 斜率变化

        # 数据质量
        has_loss_years: 是否有亏损年份
        loss_year_count: 亏损年份数
        effective_years: 有效年数

        # 参考指标
        reference_metrics: 参考指标数据

        # 额外上下文
        extra: 额外字段
    """
    # 标识
    group_key: str = ""
    metric_name: str = ""

    # 基本数据
    latest_value: float = 0.0
    weighted_avg: float = 0.0
    latest_vs_weighted_ratio: float = 1.0
    raw_values: Optional[tuple] = None
    max_value: Optional[float] = None

    # 对数趋势
    log_slope: float = 0.0
    r_squared: float = 0.0
    cagr_approx: float = 0.0
    p_value: float = 1.0
    wls_slope: Optional[float] = None
    bootstrap_ci_low: Optional[float] = None
    bootstrap_ci_high: Optional[float] = None

    # 波动性
    cv: float = 0.0
    detrended_cv: float = 0.0
    has_arch_effect: bool = False
    volatility_regime: str = "stable"
    volatility_change_ratio: float = 1.0

    # 周期性
    is_cyclical: bool = False
    cycle_position: float = 0.0
    current_phase: str = "unknown"
    peak_to_trough_ratio: float = 1.0
    fft_dominant_period: Optional[float] = None
    cyclical_confidence: float = 0.0

    # 恶化检测
    has_deterioration: bool = False
    deterioration_severity: str = "none"
    deterioration_probability: float = 0.0
    deterioration_pattern: str = "none"
    total_decline_pct: float = 0.0
    recent_decline_pct: float = 0.0
    consecutive_decline_years: int = 0
    peak_to_latest_ratio: float = 1.0

    # 滚动窗口
    recent_3y_slope: float = 0.0
    recent_5y_slope: float = 0.0
    trend_acceleration: float = 0.0
    is_accelerating: bool = False
    is_decelerating: bool = False

    # 稳健趋势
    robust_slope: float = 0.0
    mann_kendall_tau: float = 0.0
    mann_kendall_p_value: float = 1.0

    # 拐点检测
    has_inflection: bool = False
    inflection_type: str = "none"
    slope_change: float = 0.0
    inflection_confidence: float = 0.0

    # 数据质量
    has_loss_years: bool = False
    loss_year_count: int = 0
    has_near_zero_years: bool = False
    near_zero_count: int = 0
    effective_years: int = 0

    # 参考指标
    reference_metrics: Dict[str, Dict[str, float]] = field(default_factory=dict)

    # 额外上下文
    extra: Dict[str, Any] = field(default_factory=dict)

    def get(self, key: str, default: Any = None) -> Any:
        """获取字段值（支持嵌套访问）"""
        if hasattr(self, key):
            return getattr(self, key)
        return self.extra.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """设置字段值"""
        if hasattr(self, key):
            setattr(self, key, value)
        else:
            self.extra[key] = value

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        result = {
            "group_key": self.group_key,
            "metric_name": self.metric_name,
            "latest_value": self.latest_value,
            "weighted_avg": self.weighted_avg,
            "log_slope": self.log_slope,
            "r_squared": self.r_squared,
            "cagr_approx": self.cagr_approx,
            "cv": self.cv,
            "detrended_cv": self.detrended_cv,
            "is_cyclical": self.is_cyclical,
            "cycle_position": self.cycle_position,
            "has_deterioration": self.has_deterioration,
            "deterioration_probability": self.deterioration_probability,
            "total_decline_pct": self.total_decline_pct,
            "recent_3y_slope": self.recent_3y_slope,
            "trend_acceleration": self.trend_acceleration,
            "has_inflection": self.has_inflection,
            "inflection_type": self.inflection_type,
        }
        result.update(self.extra)
        return result


class EvaluationContextBuilder:
    """
    评估上下文构建器

    从 ProbeOutputs 构建 EvaluationContext。

    Example:
        context = (
            EvaluationContextBuilder()
            .with_probe_outputs(probe_outputs)
            .with_basic_data(latest_value=15.0, weighted_avg=12.0)
            .with_reference_metrics({"roic": {"latest": 15.0}})
            .build()
        )
    """

    def __init__(self):
        self._context = EvaluationContext()

    def with_group_key(self, group_key: str) -> "EvaluationContextBuilder":
        """设置分组键"""
        self._context.group_key = group_key
        return self

    def with_metric_name(self, metric_name: str) -> "EvaluationContextBuilder":
        """设置指标名称"""
        self._context.metric_name = metric_name
        return self

    def with_basic_data(
        self,
        latest_value: float,
        weighted_avg: float,
        raw_values: Optional[tuple] = None,
    ) -> "EvaluationContextBuilder":
        """设置基本数据"""
        self._context.latest_value = latest_value
        self._context.weighted_avg = weighted_avg
        self._context.raw_values = raw_values

        if weighted_avg != 0:
            self._context.latest_vs_weighted_ratio = latest_value / weighted_avg

        if raw_values:
            self._context.max_value = max(raw_values)
            self._context.effective_years = len(raw_values)

        return self

    def with_probe_outputs(self, outputs: "ProbeOutputs") -> "EvaluationContextBuilder":
        """从 ProbeOutputs 提取所有字段"""
        self._context.metric_name = outputs.indicator_name
        self._context.effective_years = outputs.effective_years

        # 从 raw_values 提取基本数据
        if outputs.raw_values is not None:
            self._context.raw_values = tuple(outputs.raw_values)
            if len(outputs.raw_values) > 0:
                self._context.latest_value = outputs.raw_values[-1]
                self._context.max_value = max(outputs.raw_values)

        # 从 log_trend 提取
        if outputs.log_trend:
            lt = outputs.log_trend
            self._context.log_slope = getattr(lt, 'log_slope', 0.0)
            self._context.r_squared = getattr(lt, 'r_squared', 0.0)
            self._context.cagr_approx = getattr(lt, 'cagr_approx', 0.0)
            self._context.p_value = getattr(lt, 'p_value', 1.0)

            # WLS 和 Bootstrap（从 metadata）
            metadata = getattr(lt, 'metadata', {}) or {}
            self._context.wls_slope = metadata.get('wls_slope')
            bootstrap_ci = metadata.get('bootstrap_ci', {})
            self._context.bootstrap_ci_low = bootstrap_ci.get('low')
            self._context.bootstrap_ci_high = bootstrap_ci.get('high')

            # 数据质量
            quality = getattr(lt, 'quality', None)
            if quality:
                self._context.has_loss_years = getattr(quality, 'has_loss_years', False)
                self._context.loss_year_count = getattr(quality, 'loss_year_count', 0)
                self._context.has_near_zero_years = getattr(quality, 'has_near_zero_years', False)
                self._context.near_zero_count = getattr(quality, 'near_zero_count', 0)

        # 从 volatility 提取
        if outputs.volatility:
            v = outputs.volatility
            self._context.cv = getattr(v, 'cv', 0.0)
            self._context.detrended_cv = getattr(v, 'detrended_cv', 0.0)
            self._context.has_arch_effect = getattr(v, 'has_arch_effect', False)
            self._context.volatility_regime = getattr(v, 'volatility_regime', 'stable')
            self._context.volatility_change_ratio = getattr(v, 'volatility_change_ratio', 1.0)

        # 从 cyclical 提取
        if outputs.cyclical:
            c = outputs.cyclical
            self._context.is_cyclical = getattr(c, 'is_cyclical', False)
            self._context.cycle_position = getattr(c, 'cycle_position', 0.0)
            self._context.current_phase = getattr(c, 'current_phase', 'unknown')
            self._context.peak_to_trough_ratio = getattr(c, 'peak_to_trough_ratio', 1.0)
            self._context.fft_dominant_period = getattr(c, 'fft_dominant_period', None)
            self._context.cyclical_confidence = getattr(c, 'cyclical_confidence', 0.0)

        # 从 deterioration 提取
        if outputs.deterioration:
            d = outputs.deterioration
            self._context.has_deterioration = getattr(d, 'has_deterioration', False)
            self._context.deterioration_severity = getattr(d, 'severity', 'none')
            self._context.deterioration_probability = getattr(d, 'deterioration_probability', 0.0)
            self._context.deterioration_pattern = getattr(d, 'deterioration_pattern', 'none')
            self._context.total_decline_pct = getattr(d, 'total_decline_pct', 0.0)
            self._context.recent_decline_pct = getattr(d, 'recent_decline_pct', 0.0)
            self._context.consecutive_decline_years = getattr(d, 'consecutive_decline_years', 0)
            self._context.peak_to_latest_ratio = getattr(d, 'peak_to_latest_ratio', 1.0)

        # 从 rolling 提取
        if outputs.rolling:
            r = outputs.rolling
            self._context.recent_3y_slope = getattr(r, 'recent_3y_slope', 0.0)
            self._context.recent_5y_slope = getattr(r, 'recent_5y_slope', 0.0)
            self._context.trend_acceleration = getattr(r, 'trend_acceleration', 0.0)
            self._context.is_accelerating = getattr(r, 'is_accelerating', False)
            self._context.is_decelerating = getattr(r, 'is_decelerating', False)

        # 从 robust 提取
        if outputs.robust:
            rb = outputs.robust
            self._context.robust_slope = getattr(rb, 'robust_slope', 0.0)
            self._context.mann_kendall_tau = getattr(rb, 'mann_kendall_tau', 0.0)
            self._context.mann_kendall_p_value = getattr(rb, 'mann_kendall_p_value', 1.0)

        # 从 inflection 提取
        if outputs.inflection:
            i = outputs.inflection
            self._context.has_inflection = getattr(i, 'has_inflection', False)
            self._context.inflection_type = getattr(i, 'inflection_type', 'none')
            self._context.slope_change = getattr(i, 'slope_change', 0.0)
            self._context.inflection_confidence = getattr(i, 'confidence', 0.0)

        return self

    def with_reference_metrics(
        self,
        reference_metrics: Dict[str, Dict[str, float]],
    ) -> "EvaluationContextBuilder":
        """设置参考指标"""
        self._context.reference_metrics = reference_metrics
        return self

    def with_extra(self, **kwargs) -> "EvaluationContextBuilder":
        """设置额外字段"""
        self._context.extra.update(kwargs)
        return self

    def build(self) -> EvaluationContext:
        """构建上下文"""
        return self._context

    @classmethod
    def from_probe_outputs(
        cls,
        outputs: "ProbeOutputs",
        group_key: str = "",
        reference_metrics: Optional[Dict[str, Dict[str, float]]] = None,
    ) -> EvaluationContext:
        """
        便捷方法：直接从 ProbeOutputs 创建上下文

        Args:
            outputs: 探针输出
            group_key: 分组键
            reference_metrics: 参考指标

        Returns:
            EvaluationContext
        """
        builder = cls()
        builder.with_group_key(group_key)
        builder.with_probe_outputs(outputs)

        if reference_metrics:
            builder.with_reference_metrics(reference_metrics)

        return builder.build()
