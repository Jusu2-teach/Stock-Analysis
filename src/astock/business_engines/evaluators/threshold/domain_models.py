"""
Evaluators Domain Models - 领域驱动设计
=========================================

重构后的领域模型：遵循 DDD 原则，拆分为多个不可变值对象

设计原则：
1. 单一职责原则 (SRP) - 每个类只负责一类数据
2. 不可变性 (Immutability) - 使用 frozen dataclass 保证线程安全
3. 值对象 (Value Object) - 无身份标识，仅基于值比较
4. 组合优于继承 - TrendContext 通过组合而非继承获得功能

版本: 2.0.0
作者: AStock Analysis System
日期: 2026-01-10
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional, Sequence, Tuple
from enum import Enum


# ============================================================================
# 枚举定义 - 语义化业务状态
# ============================================================================

class TrendDirection(str, Enum):
    """趋势方向"""
    UP = "up"
    DOWN = "down"
    FLAT = "flat"
    UNKNOWN = "unknown"

    @property
    def emoji(self) -> str:
        return {"up": "📈", "down": "📉", "flat": "➡️", "unknown": "❓"}[self.value]


class VolatilityRegime(str, Enum):
    """波动率体制"""
    STABLE = "stable"               # 稳定
    INCREASING = "increasing_vol"   # 波动率上升
    DECREASING = "decreasing_vol"   # 波动率下降
    EXTREME = "extreme"             # 极端波动

    @property
    def display_name(self) -> str:
        names = {
            "stable": "稳定",
            "increasing_vol": "波动率上升",
            "decreasing_vol": "波动率下降",
            "extreme": "极端波动"
        }
        return names[self.value]


class CyclePhase(str, Enum):
    """周期相位"""
    TROUGH = "trough"           # 谷底
    RECOVERY = "recovery"       # 回升
    EXPANSION = "expansion"     # 扩张
    PEAK = "peak"               # 顶峰
    DOWNTURN = "downturn"       # 下行
    CONTRACTION = "contraction" # 收缩
    UNKNOWN = "unknown"

    @property
    def position(self) -> str:
        """周期位置（简化版）"""
        mapping = {
            "trough": "bottom",
            "recovery": "mid_up",
            "expansion": "mid_up",
            "peak": "top",
            "downturn": "mid_down",
            "contraction": "mid_down",
            "unknown": "unknown"
        }
        return mapping[self.value]


class DeteriorationSeverity(str, Enum):
    """恶化严重程度"""
    NONE = "none"
    MILD = "mild"
    MODERATE = "moderate"
    SEVERE = "severe"
    CATASTROPHIC = "catastrophic"

    @property
    def score(self) -> float:
        """转换为数值评分 (0-1)"""
        scores = {
            "none": 0.0,
            "mild": 0.2,
            "moderate": 0.5,
            "severe": 0.8,
            "catastrophic": 1.0
        }
        return scores[self.value]


# ============================================================================
# 值对象 (Value Objects) - 不可变数据类
# ============================================================================

@dataclass(frozen=True)
class TrendMetrics:
    """趋势相关指标 (Trend-Related Metrics)

    职责: 封装所有与趋势线性回归相关的统计量
    """
    log_slope: float                    # 对数回归斜率 (主趋势)
    linear_slope: float                 # 线性回归斜率
    r_squared: float                    # R² 拟合优度
    cagr_approx: float                  # 复合年均增长率近似值
    robust_slope: float                 # 稳健回归斜率 (Theil-Sen)
    recent_3y_slope: float              # 近3年斜率
    wls_slope: Optional[float] = None   # 加权最小二乘斜率

    # 趋势显著性
    mann_kendall_tau: float = 0.0       # Mann-Kendall 检验 tau 统计量
    mann_kendall_p_value: float = 1.0   # Mann-Kendall 检验 p 值

    # 趋势加速度
    trend_acceleration: float = 0.0     # 趋势加速度 (recent - early)
    is_accelerating: bool = False       # 是否加速
    is_decelerating: bool = False       # 是否减速

    # 置信区间
    bootstrap_ci_low: Optional[float] = None   # Bootstrap 置信区间下界
    bootstrap_ci_high: Optional[float] = None  # Bootstrap 置信区间上界

    @property
    def direction(self) -> TrendDirection:
        """趋势方向"""
        if abs(self.log_slope) < 0.01:
            return TrendDirection.FLAT
        return TrendDirection.UP if self.log_slope > 0 else TrendDirection.DOWN

    @property
    def is_significant(self) -> bool:
        """趋势是否显著 (p < 0.05)"""
        return self.mann_kendall_p_value < 0.05

    @property
    def confidence_score(self) -> float:
        """综合置信度评分 (0-1)"""
        # R² 占 60%，MK 检验 p 值占 40%
        r2_score = self.r_squared
        mk_score = 1.0 - self.mann_kendall_p_value
        return 0.6 * r2_score + 0.4 * mk_score


@dataclass(frozen=True)
class VolatilityMetrics:
    """波动性指标 (Volatility Metrics)

    职责: 封装所有与波动性相关的统计量
    """
    cv: float                           # 变异系数 (标准差/均值)
    std_dev: float                      # 标准差
    detrended_cv: float = 0.0           # 去趋势后的变异系数

    # 波动率体制
    volatility_regime: VolatilityRegime = VolatilityRegime.STABLE
    volatility_change_ratio: float = 1.0  # 波动率变化比率 (后期/前期)

    # ARCH 效应检测
    has_arch_effect: bool = False       # 是否存在 ARCH 效应 (波动聚集)

    @property
    def volatility_type(self) -> str:
        """波动性类型分类"""
        if self.cv < 0.15:
            return "low"
        elif self.cv < 0.30:
            return "moderate"
        elif self.cv < 0.50:
            return "high"
        else:
            return "extreme"

    @property
    def is_stable(self) -> bool:
        """是否稳定 (低波动)"""
        return self.cv < 0.15 and self.volatility_regime == VolatilityRegime.STABLE


@dataclass(frozen=True)
class DeteriorationMetrics:
    """恶化检测指标 (Deterioration Detection Metrics)

    职责: 封装所有与业绩恶化相关的检测结果
    """
    has_deterioration: bool             # 是否检测到恶化
    severity: DeteriorationSeverity     # 恶化严重程度
    total_decline_pct: float            # 总跌幅百分比
    consecutive_decline_years: int = 0  # 连续下跌年数

    # 恶化概率 (贝叶斯)
    deterioration_probability: float = 0.0  # 恶化概率 (0-1)
    deterioration_pattern: str = "none"     # 恶化模式分类
    deterioration_acceleration: float = 0.0 # 恶化加速度

    @property
    def is_severe(self) -> bool:
        """是否严重恶化"""
        return self.severity in (DeteriorationSeverity.SEVERE, DeteriorationSeverity.CATASTROPHIC)

    @property
    def risk_level(self) -> str:
        """风险等级"""
        if self.severity == DeteriorationSeverity.CATASTROPHIC:
            return "critical"
        elif self.severity == DeteriorationSeverity.SEVERE:
            return "high"
        elif self.severity == DeteriorationSeverity.MODERATE:
            return "medium"
        elif self.severity == DeteriorationSeverity.MILD:
            return "low"
        return "none"


@dataclass(frozen=True)
class InflectionMetrics:
    """拐点检测指标 (Inflection Point Metrics)

    职责: 封装趋势拐点检测结果
    """
    has_inflection: bool                # 是否存在拐点
    inflection_type: str                # 拐点类型
    slope_change: float                 # 斜率变化量
    confidence: float = 0.0             # 拐点置信度

    @property
    def is_v_shaped_recovery(self) -> bool:
        """是否V型反转"""
        return self.inflection_type == "deterioration_to_recovery"

    @property
    def is_peak_to_decline(self) -> bool:
        """是否顶部转向下跌"""
        return self.inflection_type == "growth_to_deterioration"


@dataclass(frozen=True)
class CyclicalMetrics:
    """周期性指标 (Cyclical Pattern Metrics)

    职责: 封装周期性分析结果
    """
    is_cyclical: bool                   # 是否周期性
    current_phase: CyclePhase           # 当前周期相位
    peak_to_trough_ratio: float         # 峰谷比
    fft_dominant_period: float = 0.0    # FFT主导周期 (年)

    # HP滤波分析
    hp_cycle_amplitude: float = 0.0     # HP周期振幅
    hurst_exponent: float = 0.5         # Hurst指数

    # ACF分析
    acf_lag1: float = 0.0               # 一阶自相关系数
    cyclical_confidence: float = 0.0    # 周期性置信度

    @property
    def cycle_position(self) -> str:
        """周期位置 (简化版)"""
        return self.current_phase.position

    @property
    def is_bottom(self) -> bool:
        """是否处于底部"""
        return self.current_phase in (CyclePhase.TROUGH, CyclePhase.RECOVERY)

    @property
    def is_top(self) -> bool:
        """是否处于顶部"""
        return self.current_phase in (CyclePhase.PEAK, CyclePhase.DOWNTURN)


@dataclass(frozen=True)
class DataQualityMetrics:
    """数据质量指标 (Data Quality Metrics)

    职责: 封装数据质量检测结果
    """
    has_loss_years: bool                # 是否有亏损年份
    loss_year_count: int                # 亏损年份数量
    has_near_zero_years: bool           # 是否有接近0的年份
    near_zero_count: int                # 接近0的年份数量

    # 原始数据
    raw_values: Tuple[float, ...] = field(default_factory=tuple)
    max_value: Optional[float] = None   # 历史最大值
    latest_value: float = 0.0           # 最新值
    weighted_avg: float = 0.0           # 加权平均值

    @property
    def latest_vs_weighted_ratio(self) -> float:
        """最新值相对加权平均值的比率"""
        if self.weighted_avg == 0:
            return 1.0
        return self.latest_value / self.weighted_avg

    @property
    def has_quality_issues(self) -> bool:
        """是否存在数据质量问题"""
        return self.has_loss_years or self.has_near_zero_years


@dataclass(frozen=True)
class ReferenceMetric:
    """参考指标 (Reference Metric)

    职责: 封装交叉验证的参考指标数据
    """
    metric_name: str                    # 指标名称 (如 "roe", "roic")
    slope: float                        # 斜率
    cagr: float                         # CAGR
    r_squared: float                    # R²
    latest_value: float = 0.0           # 最新值
    cv: float = 0.0                     # 变异系数

    @property
    def direction(self) -> TrendDirection:
        """趋势方向"""
        if abs(self.slope) < 0.01:
            return TrendDirection.FLAT
        return TrendDirection.UP if self.slope > 0 else TrendDirection.DOWN


# ============================================================================
# 聚合根 (Aggregate Root) - TrendContext
# ============================================================================

@dataclass(frozen=True)
class TrendContext:
    """趋势分析上下文 (Trend Analysis Context)

    聚合根 - 通过组合多个值对象提供完整的趋势分析数据

    设计原则:
    1. 不可变性 (frozen=True) - 线程安全
    2. 组合优于继承 - 通过组合值对象获得功能
    3. 单一职责 - 仅作为数据容器，不包含业务逻辑
    4. 富领域模型 - 提供便捷的属性方法访问组合数据

    版本: 2.0.0
    """
    # 基本标识
    ts_code: str                        # 股票代码
    metric_name: str                    # 指标名称

    # 组合值对象 (核心领域数据)
    trend: TrendMetrics                 # 趋势指标
    volatility: VolatilityMetrics       # 波动性指标
    deterioration: DeteriorationMetrics # 恶化检测
    inflection: InflectionMetrics       # 拐点检测
    cyclical: CyclicalMetrics           # 周期性分析
    quality: DataQualityMetrics         # 数据质量

    # 参考指标 (交叉验证)
    reference_metrics: Mapping[str, ReferenceMetric] = field(default_factory=dict)

    # 配置参数 (用于规则判断)
    min_latest_value: Optional[float] = None  # 最低值要求

    # ========================================================================
    # 辅助方法
    # ========================================================================

    def get_reference(self, metric: str) -> Optional[ReferenceMetric]:
        """获取参考指标"""
        return self.reference_metrics.get(metric.lower())

    def has_reference(self, metric: str) -> bool:
        """是否存在参考指标"""
        return metric.lower() in self.reference_metrics


# ============================================================================
# 导出
# ============================================================================

__all__ = [
    # 枚举
    'TrendDirection',
    'VolatilityRegime',
    'CyclePhase',
    'DeteriorationSeverity',
    # 值对象
    'TrendMetrics',
    'VolatilityMetrics',
    'DeteriorationMetrics',
    'InflectionMetrics',
    'CyclicalMetrics',
    'DataQualityMetrics',
    'ReferenceMetric',
    # 聚合根
    'TrendContext',
]
