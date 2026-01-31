"""
═══════════════════════════════════════════════════════════════════════════════
AStock Evaluators v2.0 - 时间衰减模块
═══════════════════════════════════════════════════════════════════════════════

实现指数时间衰减机制，使近期数据权重更高。
基于半衰期模型：weight = exp(-λt)，其中 λ = ln(2) / half_life

⚠️ 重要说明 (v2.1 更新):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

此模块在 v2.0 设计时考虑了对原始时间序列数据的时间衰减处理。
但经过架构审查，发现 PDDA 输出的数据格式为：

    每家公司每个指标 → 1 行聚合结果（~40 个特征列）

而非多行时间序列。时间衰减已在 trend 层（log_trend_probe.py）通过 WLS
（加权最小二乘）实现，半衰期约 5 年。

因此，本模块在 evaluators v2 主流程中 **不再使用**。

保留此模块的原因：
1. 供其他需要时间加权的场景使用（如状态机历史状态衰减）
2. 作为通用时间衰减工具库
3. 向后兼容

如果你需要对时间序列数据进行时间衰减，请使用此模块。
如果你处理的是 PDDA 聚合后的单行特征数据，直接使用即可。

作者: AStock Team
版本: 2.1.0 (不再被主引擎使用)
═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import List, Optional, Sequence

import numpy as np
from numpy.typing import NDArray


@dataclass(frozen=True)
class TemporalConfig:
    """时间衰减配置"""

    half_life_years: float = 2.0  # 半衰期（年）
    min_weight: float = 0.05     # 最小权重阈值
    normalize: bool = True        # 是否归一化权重
    reference_point: str = "latest"  # 时间参考点: "latest" | "earliest" | "midpoint"

    def __post_init__(self) -> None:
        if self.half_life_years <= 0:
            raise ValueError("半衰期必须为正数")
        if not 0 < self.min_weight < 1:
            raise ValueError("最小权重必须在 (0, 1) 之间")

    @property
    def decay_constant(self) -> float:
        """衰减常数 λ = ln(2) / half_life"""
        return math.log(2) / self.half_life_years


@dataclass
class TemporalWeights:
    """时间权重结果"""

    raw_weights: NDArray[np.float64]
    normalized_weights: NDArray[np.float64]
    effective_sample_size: float
    time_spans_years: NDArray[np.float64]
    config: TemporalConfig

    @property
    def is_valid(self) -> bool:
        """验证权重有效性"""
        return (
            np.all(self.normalized_weights >= 0) and
            np.isclose(np.sum(self.normalized_weights), 1.0)
        )

    def __repr__(self) -> str:
        return (
            f"TemporalWeights(n={len(self.raw_weights)}, "
            f"ESS={self.effective_sample_size:.2f}, "
            f"half_life={self.config.half_life_years}y)"
        )


class TemporalDecay:
    """
    时间衰减计算器

    实现指数衰减模型，支持：
    - 可配置的半衰期
    - 最小权重阈值（避免远期数据被完全忽略）
    - 有效样本量（ESS）计算
    - 多种时间参考点

    Example:
        >>> decay = TemporalDecay(half_life_years=2.0)
        >>> weights = decay.compute_weights(n_periods=10, period_years=1.0)
        >>> print(weights.normalized_weights)
        array([0.05, 0.07, 0.10, 0.14, 0.19, ...])
    """

    def __init__(self, config: Optional[TemporalConfig] = None):
        self.config = config or TemporalConfig()

    def compute_weights(
        self,
        n_periods: int,
        period_years: float = 1.0,
        custom_time_spans: Optional[Sequence[float]] = None
    ) -> TemporalWeights:
        """
        计算时间衰减权重

        Args:
            n_periods: 时间点数量
            period_years: 每个周期的年数（默认为1年，即年度数据）
            custom_time_spans: 自定义时间间隔（覆盖等间隔假设）

        Returns:
            TemporalWeights 包含原始权重、归一化权重和有效样本量
        """
        if n_periods < 1:
            raise ValueError("n_periods 必须至少为 1")

        # 计算时间跨度
        if custom_time_spans is not None:
            time_spans = np.array(custom_time_spans, dtype=np.float64)
            if len(time_spans) != n_periods:
                raise ValueError("custom_time_spans 长度必须等于 n_periods")
        else:
            # 等间隔时间点，从最早到最近
            time_spans = np.arange(n_periods, dtype=np.float64) * period_years

        # 转换为距离参考点的时间
        time_from_ref = self._compute_time_from_reference(time_spans)

        # 计算原始权重: w = exp(-λt)
        λ = self.config.decay_constant
        raw_weights = np.exp(-λ * time_from_ref)

        # 应用最小权重阈值
        raw_weights = np.maximum(raw_weights, self.config.min_weight)

        # 归一化
        if self.config.normalize:
            normalized_weights = raw_weights / np.sum(raw_weights)
        else:
            normalized_weights = raw_weights.copy()

        # 计算有效样本量 (Kish's formula)
        ess = self._compute_effective_sample_size(normalized_weights)

        return TemporalWeights(
            raw_weights=raw_weights,
            normalized_weights=normalized_weights,
            effective_sample_size=ess,
            time_spans_years=time_spans,
            config=self.config
        )

    def _compute_time_from_reference(
        self, time_spans: NDArray[np.float64]
    ) -> NDArray[np.float64]:
        """计算距离参考点的时间距离"""
        if self.config.reference_point == "latest":
            # 最近时间点权重最高
            return time_spans.max() - time_spans
        elif self.config.reference_point == "earliest":
            # 最早时间点权重最高
            return time_spans - time_spans.min()
        elif self.config.reference_point == "midpoint":
            # 中间时间点权重最高
            midpoint = (time_spans.max() + time_spans.min()) / 2
            return np.abs(time_spans - midpoint)
        else:
            raise ValueError(f"Unknown reference_point: {self.config.reference_point}")

    def _compute_effective_sample_size(
        self, weights: NDArray[np.float64]
    ) -> float:
        """
        计算有效样本量 (Effective Sample Size)

        Kish's formula: ESS = (Σw)² / Σ(w²)
        对于归一化权重简化为: ESS = 1 / Σ(w²)
        """
        sum_sq = np.sum(weights ** 2)
        if sum_sq > 0:
            return 1.0 / sum_sq
        return 0.0

    def weighted_mean(
        self,
        values: Sequence[float],
        weights: Optional[TemporalWeights] = None,
        period_years: float = 1.0
    ) -> float:
        """
        计算时间加权平均

        Args:
            values: 时间序列值
            weights: 预计算的权重，如果为 None 则自动计算
            period_years: 周期年数

        Returns:
            加权平均值
        """
        values_arr = np.array(values, dtype=np.float64)

        if weights is None:
            weights = self.compute_weights(len(values_arr), period_years)

        if len(values_arr) != len(weights.normalized_weights):
            raise ValueError("值数组长度必须与权重长度匹配")

        return float(np.dot(values_arr, weights.normalized_weights))

    def weighted_std(
        self,
        values: Sequence[float],
        weights: Optional[TemporalWeights] = None,
        period_years: float = 1.0
    ) -> float:
        """
        计算时间加权标准差

        使用可靠性权重公式避免偏差
        """
        values_arr = np.array(values, dtype=np.float64)

        if weights is None:
            weights = self.compute_weights(len(values_arr), period_years)

        w = weights.normalized_weights
        mean = np.dot(values_arr, w)

        # 加权方差（使用可靠性权重）
        # V = Σw(x-μ)² / (1 - Σw²)
        numerator = np.dot(w, (values_arr - mean) ** 2)
        denominator = 1.0 - np.sum(w ** 2)

        if denominator > 1e-10:
            variance = numerator / denominator
        else:
            variance = numerator  # 退化情况

        return float(np.sqrt(max(0, variance)))

    def weighted_regression_slope(
        self,
        values: Sequence[float],
        weights: Optional[TemporalWeights] = None,
        period_years: float = 1.0
    ) -> tuple[float, float]:
        """
        计算时间加权线性回归斜率

        Returns:
            (slope, r_squared)
        """
        values_arr = np.array(values, dtype=np.float64)
        n = len(values_arr)

        if weights is None:
            weights = self.compute_weights(n, period_years)

        w = weights.normalized_weights
        t = np.arange(n, dtype=np.float64)

        # 加权均值
        t_mean = np.dot(w, t)
        y_mean = np.dot(w, values_arr)

        # 加权协方差和方差
        cov_ty = np.dot(w, (t - t_mean) * (values_arr - y_mean))
        var_t = np.dot(w, (t - t_mean) ** 2)
        var_y = np.dot(w, (values_arr - y_mean) ** 2)

        if var_t < 1e-10:
            return 0.0, 0.0

        slope = cov_ty / var_t

        # R² 计算
        if var_y > 1e-10:
            r_squared = (cov_ty ** 2) / (var_t * var_y)
        else:
            r_squared = 0.0

        return float(slope), float(r_squared)


# ═══════════════════════════════════════════════════════════════════════════════
# 工厂函数
# ═══════════════════════════════════════════════════════════════════════════════

def create_decay(
    half_life_years: float = 2.0,
    min_weight: float = 0.05,
    normalize: bool = True
) -> TemporalDecay:
    """便捷工厂函数"""
    config = TemporalConfig(
        half_life_years=half_life_years,
        min_weight=min_weight,
        normalize=normalize
    )
    return TemporalDecay(config)


# 预置实例
DEFAULT_DECAY = TemporalDecay()
AGGRESSIVE_DECAY = TemporalDecay(TemporalConfig(half_life_years=1.0))
CONSERVATIVE_DECAY = TemporalDecay(TemporalConfig(half_life_years=3.0))
