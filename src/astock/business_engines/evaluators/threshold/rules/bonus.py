"""
加分规则 v2.0 (Bonus Rules - Refactored)
========================================

使用 Protocol-based 架构重构的加分规则。

设计原则:
- 实现 RuleProtocol 接口
- 使用新的 domain_models.TrendContext
- 不可变规则配置 (frozen dataclass)
- 清晰的业务语义

规则清单 (5个):
1. GrowthMomentumBonusRule - 成长动能加分
2. InflectionRecoveryBonusRule - 拐点恢复加分
3. MeanReversionBonusRule - 均值回归豁免加分
4. CyclicalPositionBonusRule - 周期位置调整
5. ROIICPositiveBonusRule - ROIIC改善加分

作者: AStock Analysis System (Refactored)
日期: 2026-01-10
版本: 2.0.0
"""

from typing import Optional
import logging

from ..domain_models import TrendContext, CyclePhase
from ..protocols import RuleProtocol
from ..results import RuleResultImpl, create_bonus_result, create_penalty_result
from ..rule_config import RuleConfig, RuleCategory

logger = logging.getLogger(__name__)


# ============================================================================
# 辅助函数
# ============================================================================

def is_roiic_metric(context: TrendContext) -> bool:
    """判断是否为 ROIIC 指标"""
    return context.metric_name.lower() in {"roiic", "roii", "roic_incr"}


def get_reference_metric(context: TrendContext, metric_name: str) -> Optional[dict]:
    """获取参考指标统计数据"""
    for ref_name, ref in context.reference_metrics.items():
        if ref_name.lower() == metric_name.lower():
            return {
                "latest": ref.latest_value,
                "slope": ref.slope,
                "cagr": ref.cagr,
                "r_squared": ref.r_squared,
            }
    return None


# ============================================================================
# 1. 成长动能加分规则
# ============================================================================

class GrowthMomentumBonusRule:
    """
    成长动能加分规则

    整合原来的:
    - rule_growth_momentum_bonus
    - rule_acceleration_adjustment (正向部分)

    触发条件:
    - log_slope > 0 且 recent_3y_slope > 0
    - 趋势加速度为正

    加重条件:
    - 加速增长额外加分

    Examples:
        >>> rule = GrowthMomentumBonusRule()
        >>> result = rule.execute(context, config)
    """

    name: str = "growth_momentum_bonus"
    category: RuleCategory = RuleCategory.BONUS
    priority: int = 200
    enabled: bool = True
    description: str = "成长动能加分"

    def execute(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> Optional[RuleResultImpl]:
        """执行规则"""
        bonus_cfg = config.bonus

        # 基本条件: 斜率为正
        if context.trend.log_slope <= 0 or context.trend.recent_3y_slope <= 0:
            return None

        # 加速度加分
        if context.trend.trend_acceleration <= 0 and not context.trend.is_accelerating:
            return None

        # 计算成长分
        growth_score = context.trend.log_slope + max(context.trend.recent_3y_slope, 0)
        if context.trend.trend_acceleration > 0:
            growth_score += context.trend.trend_acceleration * 0.5

        if growth_score <= 0:
            return None

        bonus = min(growth_score * 20, bonus_cfg.growth_momentum_max_bonus)

        if bonus <= 0:
            return None

        # 加速增长额外加分
        accel_note = ""
        if context.trend.is_accelerating and context.trend.recent_3y_slope > 0:
            accel_bonus = min(abs(context.trend.trend_acceleration) / 2, 3.0)
            bonus += accel_bonus
            accel_note = f", 加速+{accel_bonus:.1f}"

        bonus = min(bonus, config.scoring.max_bonus)

        return create_bonus_result(
            "growth_momentum_bonus",
            f"成长动能+{bonus:.1f}分"
            f"(斜率{context.trend.log_slope:.3f}, "
            f"近3年{context.trend.recent_3y_slope:.3f}{accel_note})",
            bonus,
            metadata={
                "log_slope": context.trend.log_slope,
                "recent_3y_slope": context.trend.recent_3y_slope,
                "acceleration": context.trend.trend_acceleration,
            }
        )


# ============================================================================
# 2. 拐点恢复加分规则
# ============================================================================

class InflectionRecoveryBonusRule:
    """
    拐点恢复加分规则

    整合原来的:
    - rule_inflection_penalty_or_bonus (正向部分)
    - rule_wls_ols_divergence (正向部分)

    触发条件:
    - 拐点类型为恢复
    - WLS斜率优于OLS (近期改善)

    Examples:
        >>> rule = InflectionRecoveryBonusRule()
        >>> result = rule.execute(context, config)
    """

    name: str = "inflection_recovery_bonus"
    category: RuleCategory = RuleCategory.BONUS
    priority: int = 210
    enabled: bool = True
    description: str = "拐点恢复加分"

    def execute(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> Optional[RuleResultImpl]:
        """执行规则"""
        bonus_cfg = config.bonus

        # === 拐点恢复加分 ===
        if context.inflection.has_inflection:
            # 检查斜率变化 (从负转正)
            if (context.inflection.pre_inflection_slope is not None and
                context.inflection.post_inflection_slope is not None):

                pre_slope = context.inflection.pre_inflection_slope
                post_slope = context.inflection.post_inflection_slope

                # 恶化转好转: 前期下降，后期上升
                if pre_slope < 0 and post_slope > 0:
                    slope_change = abs(context.inflection.slope_change or 0)
                    recovery_bonus = min(
                        slope_change * 2,
                        bonus_cfg.inflection_recovery_max_bonus
                    )
                    return create_bonus_result(
                        "inflection_recovery_bonus",
                        f"恶化转好+{recovery_bonus:.1f}分"
                        f"(斜率变化{slope_change:.2f})",
                        recovery_bonus,
                        metadata={
                            "pre_slope": pre_slope,
                            "post_slope": post_slope,
                            "slope_change": slope_change,
                        }
                    )

        # === WLS 近期改善加分 ===
        # WLS (Weighted Least Squares) 给近期数据更高权重
        wls_slope = getattr(context.trend, 'wls_slope', None)
        ols_slope = context.trend.log_slope

        if wls_slope is not None:
            diff = wls_slope - ols_slope

            # WLS更优 = 近期改善
            if diff > 0.05 and wls_slope > ols_slope:
                bonus = min(diff * 20, 6.0)
                return create_bonus_result(
                    "wls_improvement_bonus",
                    f"近期改善+{bonus:.1f}分"
                    f"(WLS={wls_slope:.3f} > OLS={ols_slope:.3f})",
                    bonus,
                    metadata={
                        "wls_slope": wls_slope,
                        "ols_slope": ols_slope,
                        "diff": diff,
                    }
                )

            # 困境反转信号: OLS衰退但WLS稳定
            if ols_slope < -0.05 and wls_slope > -0.02:
                return create_bonus_result(
                    "wls_turnaround_bonus",
                    "困境反转信号+4分(整体下滑但近期企稳)",
                    4.0,
                    metadata={
                        "wls_slope": wls_slope,
                        "ols_slope": ols_slope,
                    }
                )

        return None


# ============================================================================
# 3. 均值回归豁免加分规则
# ============================================================================

class MeanReversionBonusRule:
    """
    均值回归豁免加分规则

    高基数正常回落的豁免

    触发条件:
    - 有恶化但程度为 mild 或 moderate
    - 最新值仍高于加权均值80%
    - 最新值仍达到绝对门槛

    Examples:
        >>> rule = MeanReversionBonusRule()
        >>> result = rule.execute(context, config)
    """

    name: str = "mean_reversion_bonus"
    category: RuleCategory = RuleCategory.BONUS
    priority: int = 220
    enabled: bool = True
    description: str = "均值回归豁免"

    def execute(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> Optional[RuleResultImpl]:
        """执行规则"""
        from ..domain_models import DeteriorationSeverity

        bonus_cfg = config.bonus

        # 只对有恶化的情况触发
        if not context.deterioration.has_deterioration:
            return None

        if context.deterioration.severity not in {
            DeteriorationSeverity.MODERATE,
            DeteriorationSeverity.MILD
        }:
            return None

        # 最新值仍高于加权均值80%
        if context.quality.latest_vs_weighted_ratio < bonus_cfg.mean_reversion_ratio_min:
            return None

        # 最新值仍达到绝对门槛
        min_val = context.min_latest_value
        if min_val is not None and context.quality.latest_value < min_val:
            return None

        # 高位正常回调，给予加分
        if context.deterioration.severity == DeteriorationSeverity.MILD:
            bonus = bonus_cfg.mean_reversion_mild_bonus
        else:
            bonus = bonus_cfg.mean_reversion_moderate_bonus

        return create_bonus_result(
            "mean_reversion_bonus",
            f"均值回归豁免+{bonus:.0f}分"
            f"(最新={context.quality.latest_value:.1f}"
            f"仍为加权{context.quality.latest_vs_weighted_ratio:.0%})",
            bonus,
            metadata={
                "latest_value": context.quality.latest_value,
                "weighted_ratio": context.quality.latest_vs_weighted_ratio,
                "severity": context.deterioration.severity.value,
            }
        )


# ============================================================================
# 4. 周期位置调整规则
# ============================================================================

class CyclicalPositionBonusRule:
    """
    周期位置调整规则

    整合原来的:
    - rule_cyclical_adjustment
    - rule_cycle_position_adjustment

    根据周期位置给予加分或扣分

    周期阶段:
    - 谷底: 加分 (逆向机会)
    - 回升: 加分 (景气回升)
    - 顶部: 扣分 (警惕回落)
    - 下行: 扣分 (景气下行)

    Examples:
        >>> rule = CyclicalPositionBonusRule()
        >>> result = rule.execute(context, config)
    """

    name: str = "cyclical_position_bonus"
    category: RuleCategory = RuleCategory.BONUS
    priority: int = 230
    enabled: bool = True
    description: str = "周期位置调整"

    def execute(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> Optional[RuleResultImpl]:
        """执行规则"""
        if not context.cyclical.is_cyclical:
            return None

        bonus_cfg = config.bonus
        current_phase = context.cyclical.cycle_phase

        # === 底部加分 ===
        if current_phase == CyclePhase.TROUGH:
            bonus = bonus_cfg.cyclical_bottom_bonus
            return create_bonus_result(
                "cyclical_bottom_bonus",
                f"周期底部+{bonus:.0f}分(逆向机会)",
                bonus,
                metadata={"phase": current_phase.value}
            )

        # === 回升期加分 ===
        if current_phase == CyclePhase.RECOVERY:
            bonus = bonus_cfg.cyclical_recovery_bonus
            return create_bonus_result(
                "cyclical_recovery_bonus",
                f"周期回升+{bonus:.0f}分(景气回升)",
                bonus,
                metadata={"phase": current_phase.value}
            )

        # === 顶部预警 (返回扣分) ===
        if current_phase == CyclePhase.PEAK:
            return create_penalty_result(
                "cyclical_top_penalty",
                f"周期顶部-{bonus_cfg.cyclical_top_penalty:.0f}分(警惕回落)",
                bonus_cfg.cyclical_top_penalty,
                metadata={"phase": current_phase.value}
            )

        # === 下行期扣分 ===
        if current_phase == CyclePhase.DOWNTURN:
            return create_penalty_result(
                "cyclical_downturn_penalty",
                f"周期回落-{bonus_cfg.cyclical_downturn_penalty:.0f}分(景气下行)",
                bonus_cfg.cyclical_downturn_penalty,
                metadata={"phase": current_phase.value}
            )

        return None


# ============================================================================
# 5. ROIIC改善加分规则
# ============================================================================

class ROIICPositiveBonusRule:
    """
    ROIIC改善加分规则

    ROIIC为正且趋势向好

    触发条件:
    - 加权平均达到阈值
    - 趋势向好 (斜率为正)
    - ROIC也不恶化 (交叉验证)

    Examples:
        >>> rule = ROIICPositiveBonusRule()
        >>> result = rule.execute(context, config)
    """

    name: str = "roiic_positive_bonus"
    category: RuleCategory = RuleCategory.BONUS
    priority: int = 240
    enabled: bool = True
    description: str = "ROIIC改善加分"

    def execute(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> Optional[RuleResultImpl]:
        """执行规则"""
        if not is_roiic_metric(context):
            return None

        bonus_cfg = config.bonus

        # 加权平均必须达到阈值
        if context.quality.weighted_avg < bonus_cfg.roiic_positive_threshold:
            return None

        # 趋势必须向好
        if (context.trend.log_slope <= 0 or
            context.trend.recent_3y_slope <= 0):
            return None

        # ROIC交叉验证: ROIC也不能恶化
        roic_stats = get_reference_metric(context, "roic")
        if roic_stats and roic_stats.get("log_slope") is not None:
            if roic_stats["log_slope"] < 0:
                return None

        # 计算加分
        growth_score = (
            context.trend.log_slope +
            max(context.trend.recent_3y_slope, 0.0) * 0.5
        )
        if growth_score <= 0:
            return None

        bonus = min(growth_score * 20, bonus_cfg.roiic_positive_max_bonus)

        if bonus <= 0:
            return None

        return create_bonus_result(
            "roiic_positive_bonus",
            f"ROIIC改善+{bonus:.1f}分(斜率{context.trend.log_slope:.3f})",
            bonus,
            metadata={
                "log_slope": context.trend.log_slope,
                "weighted_avg": context.quality.weighted_avg,
            }
        )


# ============================================================================
# 规则工厂
# ============================================================================

def create_all_bonus_rules() -> list[RuleProtocol]:
    """
    创建所有加分规则实例

    Returns:
        规则实例列表，按优先级排序
    """
    rules = [
        GrowthMomentumBonusRule(),
        InflectionRecoveryBonusRule(),
        MeanReversionBonusRule(),
        CyclicalPositionBonusRule(),
        ROIICPositiveBonusRule(),
    ]

    # 按优先级排序
    return sorted(rules, key=lambda r: r.priority)


__all__ = [
    # 规则类
    'GrowthMomentumBonusRule',
    'InflectionRecoveryBonusRule',
    'MeanReversionBonusRule',
    'CyclicalPositionBonusRule',
    'ROIICPositiveBonusRule',
    # 工厂函数
    'create_all_bonus_rules',
    # 辅助函数
    'is_roiic_metric',
    'get_reference_metric',
]
