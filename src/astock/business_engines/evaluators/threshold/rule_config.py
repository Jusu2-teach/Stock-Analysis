"""
规则配置 (Rule Configuration)
==============================

唯一的规则参数配置源 - Single Source of Truth

所有规则的阈值、系数都在此定义，避免分散和不一致。

作者: AStock Analysis System
日期: 2025-12-19
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional
from enum import Enum


class RuleCategory(Enum):
    """规则分类"""
    VETO = "veto"           # 一票否决
    PENALTY = "penalty"     # 扣分
    BONUS = "bonus"         # 加分
    VALIDATION = "validation"  # 交叉验证


@dataclass(frozen=True)
class ScoringConfig:
    """评分配置"""
    base_score: float = 100.0           # 基础分
    pass_threshold: float = 60.0        # 及格线
    penalty_factor: float = 15.0        # 默认扣分系数
    max_penalty: float = 20.0           # 单规则最大扣分
    max_bonus: float = 15.0             # 单规则最大加分


@dataclass(frozen=True)
class VetoThresholds:
    """
    否决规则阈值

    触发任一条件即一票否决
    """
    # === 趋势衰退否决 ===
    severe_decline_slope: float = -0.10          # 严重衰退斜率
    severe_decline_r2_min: float = 0.40          # 趋势显著性要求

    # === 峰值跌幅否决 ===
    peak_decline_pct: float = 70.0               # 从峰值跌幅%否决
    peak_decline_cyclical_pct: float = 80.0      # 周期股放宽

    # === 累计跌幅否决 ===
    cumulative_decline_pct: float = 60.0         # 累计跌幅%否决
    cumulative_decline_cyclical_pct: float = 70.0

    # === 连续下跌否决 ===
    consecutive_decline_years: int = 4           # 连续下跌年数
    consecutive_decline_cyclical: int = 5        # 周期股放宽

    # === 累计崩塌否决 (高位跌到低位) ===
    collapse_max_threshold: float = 30.0         # 曾经最高值阈值
    collapse_latest_threshold: float = 5.0       # 当前值阈值
    collapse_decline_pct: float = 80.0           # 跌幅阈值

    # === 恶化否决 ===
    deterioration_decline_pct: float = 40.0      # 恶化跌幅否决
    deterioration_ratio: float = 0.70            # 最新/加权比否决

    # === ROIIC 专用否决 ===
    roiic_weighted_threshold: float = -20.0      # 加权平均阈值
    roiic_latest_threshold: float = -10.0        # 最新值阈值


@dataclass(frozen=True)
class PenaltyThresholds:
    """
    扣分规则阈值

    触发条件累计扣分
    """
    # === 轻度衰退 ===
    mild_decline_slope: float = -0.03            # 轻度衰退斜率
    mild_decline_max_penalty: float = 15.0       # 最大扣分

    # === 波动性 ===
    high_volatility_cv: float = 0.40             # 高波动CV阈值
    volatility_penalty_base: float = 8.0         # 波动扣分基数

    # === 相对跌幅 ===
    relative_decline_60: float = 0.60            # 跌幅60%阈值
    relative_decline_60_penalty: float = 15.0
    relative_decline_70: float = 0.70            # 跌幅70%阈值
    relative_decline_70_penalty: float = 10.0

    # === 单年暴跌 ===
    single_year_decline_pct: float = -30.0       # 单年跌幅%
    single_year_penalty: float = 15.0

    # === 恶化程度 ===
    deterioration_severe_penalty: float = 15.0
    deterioration_moderate_penalty: float = 10.0
    deterioration_mild_penalty: float = 5.0

    # === 连续下跌 (未达否决) ===
    consecutive_3y_penalty: float = 12.0
    consecutive_2y_penalty: float = 6.0

    # === ROIIC 扣分 ===
    roiic_negative_buffer: float = 0.0           # 负值缓冲
    roiic_negative_scale: float = 8.0            # 负值扣分系数
    roiic_negative_cap: float = 12.0             # 负值扣分上限
    roiic_divergence_gap: float = 0.12           # ROIC/ROIIC分化阈值


@dataclass(frozen=True)
class BonusThresholds:
    """
    加分规则阈值

    触发条件累计加分
    """
    # === 成长动能 ===
    growth_momentum_min_slope: float = 0.10      # 最低增长斜率
    growth_momentum_max_bonus: float = 8.0

    # === 拐点恢复 ===
    inflection_recovery_max_bonus: float = 10.0

    # === 均值回归豁免 ===
    mean_reversion_ratio_min: float = 0.80       # 最新/加权比最低要求
    mean_reversion_mild_bonus: float = 3.0
    mean_reversion_moderate_bonus: float = 5.0

    # === 周期位置 ===
    cyclical_bottom_bonus: float = 8.0           # 底部加分
    cyclical_recovery_bonus: float = 4.0         # 回升期加分
    cyclical_top_penalty: float = 5.0            # 顶部扣分
    cyclical_downturn_penalty: float = 3.0       # 下行期扣分

    # === ROIIC 改善 ===
    roiic_positive_threshold: float = 8.0        # 正向加分阈值
    roiic_positive_max_bonus: float = 8.0


@dataclass(frozen=True)
class ValidationThresholds:
    """
    交叉验证阈值

    用于检验数据质量和一致性
    """
    # === 盈利质量 (利润 vs 现金流) ===
    profit_ocf_divergence: float = 0.20          # 背离阈值
    profit_positive_threshold: float = 0.10      # 利润增速正阈值
    ocf_negative_threshold: float = -0.05        # 现金流负阈值
    earnings_quality_penalty: float = 15.0

    # === 杜邦分解 ===
    dupont_roe_threshold: float = 0.05           # ROE增速阈值
    dupont_margin_threshold: float = -0.03       # 利润率下跌阈值
    dupont_penalty_max: float = 8.0

    # === 自由现金流 ===
    fcf_chronic_negative_penalty: float = 12.0
    fcf_deteriorating_slope: float = -0.15
    fcf_deteriorating_penalty_max: float = 10.0

    # === 可持续增长 ===
    sustainable_revenue_threshold: float = 0.20  # 营收增速
    sustainable_roe_threshold: float = 0.08      # ROE最低要求
    sustainable_growth_penalty: float = 12.0


@dataclass
class RuleConfig:
    """
    规则配置 - 统一配置入口

    Example:
        config = RuleConfig()

        # 获取否决阈值
        if slope < config.veto.severe_decline_slope:
            return veto()

        # 获取扣分阈值
        penalty = abs(slope) * config.scoring.penalty_factor
    """
    scoring: ScoringConfig = field(default_factory=ScoringConfig)
    veto: VetoThresholds = field(default_factory=VetoThresholds)
    penalty: PenaltyThresholds = field(default_factory=PenaltyThresholds)
    bonus: BonusThresholds = field(default_factory=BonusThresholds)
    validation: ValidationThresholds = field(default_factory=ValidationThresholds)

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "RuleConfig":
        """从字典创建配置"""
        scoring_dict = config.get("scoring", {})
        veto_dict = config.get("veto", {})
        penalty_dict = config.get("penalty", {})
        bonus_dict = config.get("bonus", {})
        validation_dict = config.get("validation", {})

        return cls(
            scoring=ScoringConfig(**{k: v for k, v in scoring_dict.items() if hasattr(ScoringConfig, k)}),
            veto=VetoThresholds(**{k: v for k, v in veto_dict.items() if hasattr(VetoThresholds, k)}),
            penalty=PenaltyThresholds(**{k: v for k, v in penalty_dict.items() if hasattr(PenaltyThresholds, k)}),
            bonus=BonusThresholds(**{k: v for k, v in bonus_dict.items() if hasattr(BonusThresholds, k)}),
            validation=ValidationThresholds(**{k: v for k, v in validation_dict.items() if hasattr(ValidationThresholds, k)}),
        )

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        from dataclasses import asdict
        return {
            "scoring": asdict(self.scoring),
            "veto": asdict(self.veto),
            "penalty": asdict(self.penalty),
            "bonus": asdict(self.bonus),
            "validation": asdict(self.validation),
        }

    def with_cyclical_adjustments(self, is_cyclical: bool) -> "RuleConfig":
        """
        为周期股调整阈值

        周期股的否决条件更宽松
        """
        if not is_cyclical:
            return self

        # 创建调整后的否决阈值
        adjusted_veto = VetoThresholds(
            severe_decline_slope=self.veto.severe_decline_slope,
            severe_decline_r2_min=self.veto.severe_decline_r2_min,
            peak_decline_pct=self.veto.peak_decline_cyclical_pct,
            peak_decline_cyclical_pct=self.veto.peak_decline_cyclical_pct,
            cumulative_decline_pct=self.veto.cumulative_decline_cyclical_pct,
            cumulative_decline_cyclical_pct=self.veto.cumulative_decline_cyclical_pct,
            consecutive_decline_years=self.veto.consecutive_decline_cyclical,
            consecutive_decline_cyclical=self.veto.consecutive_decline_cyclical,
            collapse_max_threshold=self.veto.collapse_max_threshold,
            collapse_latest_threshold=self.veto.collapse_latest_threshold,
            collapse_decline_pct=self.veto.collapse_decline_pct,
            deterioration_decline_pct=self.veto.deterioration_decline_pct + 10,  # 放宽10%
            deterioration_ratio=self.veto.deterioration_ratio - 0.10,  # 放宽10%
            roiic_weighted_threshold=self.veto.roiic_weighted_threshold,
            roiic_latest_threshold=self.veto.roiic_latest_threshold,
        )

        return RuleConfig(
            scoring=self.scoring,
            veto=adjusted_veto,
            penalty=self.penalty,
            bonus=self.bonus,
            validation=self.validation,
        )


# ============================================================================
# 默认配置实例
# ============================================================================

DEFAULT_CONFIG = RuleConfig()


# ============================================================================
# 指标特定配置
# ============================================================================

def get_metric_config(metric_name: str, base_config: Optional[RuleConfig] = None) -> RuleConfig:
    """
    获取指标特定配置

    不同指标可能有不同的阈值要求

    Args:
        metric_name: 指标名称
        base_config: 基础配置

    Returns:
        针对该指标调整后的配置
    """
    config = base_config or DEFAULT_CONFIG
    metric = metric_name.lower()

    # ROIIC 使用更严格的配置（但作为辅助指标处理）
    if "roiic" in metric:
        return config

    # 利润率指标
    if "margin" in metric or "rate" in metric:
        return config

    # 默认配置
    return config


# ============================================================================
# 导出
# ============================================================================

__all__ = [
    'RuleCategory',
    'ScoringConfig',
    'VetoThresholds',
    'PenaltyThresholds',
    'BonusThresholds',
    'ValidationThresholds',
    'RuleConfig',
    'DEFAULT_CONFIG',
    'get_metric_config',
]
