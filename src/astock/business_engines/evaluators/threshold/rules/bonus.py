"""
加分规则 (Bonus Rules)
=======================

加分规则：正向激励，提升评分。

规则清单 (5个):
1. rule_growth_momentum_bonus - 成长动能加分
2. rule_inflection_recovery_bonus - 拐点恢复加分
3. rule_mean_reversion_bonus - 均值回归豁免加分
4. rule_cyclical_position_bonus - 周期位置调整
5. rule_roiic_positive_bonus - ROIIC改善加分
"""

from typing import Optional, List

from .base import (
    RuleResult, Rule, TrendContext, RuleConfig, RuleCategory,
    is_roiic_metric, get_reference_metric, logger
)


# ============================================================================
# 1. 成长动能加分
# ============================================================================

def rule_growth_momentum_bonus(context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
    """
    成长动能加分

    整合原来的:
    - rule_growth_momentum_bonus
    - rule_acceleration_adjustment (正向部分)

    触发条件:
    - log_slope > 0 且 recent_3y_slope > 0
    - 趋势加速度为正
    """
    bonus_cfg = config.bonus

    # 基本条件: 斜率为正
    if context.log_slope <= 0 or context.recent_3y_slope <= 0:
        return None

    # 加速度加分
    if context.trend_acceleration <= 0 and not context.is_accelerating:
        return None

    # 计算成长分
    growth_score = context.log_slope + max(context.recent_3y_slope, 0)
    if context.trend_acceleration > 0:
        growth_score += context.trend_acceleration * 0.5

    if growth_score <= 0:
        return None

    bonus = min(growth_score * 20, bonus_cfg.growth_momentum_max_bonus)

    if bonus <= 0:
        return None

    # 加速增长额外加分
    accel_note = ""
    if context.is_accelerating and context.recent_3y_slope > 0:
        accel_bonus = min(abs(context.trend_acceleration) / 2, 3.0)
        bonus += accel_bonus
        accel_note = f", 加速+{accel_bonus:.1f}"

    bonus = min(bonus, config.scoring.max_bonus)

    return RuleResult.bonus(
        "growth_momentum_bonus",
        f"成长动能+{bonus:.1f}分(斜率{context.log_slope:.3f}, 近3年{context.recent_3y_slope:.3f}{accel_note})",
        bonus
    )


# ============================================================================
# 2. 拐点恢复加分
# ============================================================================

def rule_inflection_recovery_bonus(context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
    """
    拐点恢复加分

    整合原来的:
    - rule_inflection_penalty_or_bonus (正向部分)
    - rule_wls_ols_divergence (正向部分)

    触发条件:
    - 拐点类型为恢复
    - WLS斜率优于OLS (近期改善)
    """
    bonus_cfg = config.bonus

    # === 拐点恢复加分 ===
    if context.has_inflection and context.inflection_type == "deterioration_to_recovery":
        slope_change = abs(context.slope_change)
        recovery_bonus = min(slope_change * 2, bonus_cfg.inflection_recovery_max_bonus)
        return RuleResult.bonus(
            "inflection_recovery_bonus",
            f"恶化转好+{recovery_bonus:.1f}分(斜率变化{context.slope_change:.2f})",
            recovery_bonus
        )

    # === WLS 近期改善加分 ===
    wls_slope = context.wls_slope
    ols_slope = context.log_slope

    if wls_slope is not None:
        diff = wls_slope - ols_slope

        # WLS更正 = 近期改善
        if diff > 0.05 and wls_slope > ols_slope:
            bonus = min(diff * 20, 6.0)
            return RuleResult.bonus(
                "wls_improvement_bonus",
                f"近期改善+{bonus:.1f}分(WLS={wls_slope:.3f} > OLS={ols_slope:.3f})",
                bonus
            )

        # 困境反转信号: OLS衰退但WLS稳定
        if ols_slope < -0.05 and wls_slope > -0.02:
            return RuleResult.bonus(
                "wls_turnaround_bonus",
                f"困境反转信号+4分(整体下滑但近期企稳)",
                4.0
            )

    return None


# ============================================================================
# 3. 均值回归豁免加分
# ============================================================================

def rule_mean_reversion_bonus(context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
    """
    均值回归豁免加分

    高基数正常回落的豁免

    触发条件:
    - 有恶化但程度为 mild 或 moderate
    - 最新值仍高于加权均值80%
    - 最新值仍达到绝对门槛
    """
    bonus_cfg = config.bonus

    # 只对有恶化的情况触发
    if not context.has_deterioration:
        return None

    if context.deterioration_severity not in ("moderate", "mild"):
        return None

    # 最新值仍高于加权均值80%
    if context.latest_vs_weighted_ratio < bonus_cfg.mean_reversion_ratio_min:
        return None

    # 最新值仍达到绝对门槛
    min_val = getattr(context, 'min_latest_value', None)
    if min_val is not None and context.latest_value < min_val:
        return None

    # 高位正常回调，给予加分
    if context.deterioration_severity == "mild":
        bonus = bonus_cfg.mean_reversion_mild_bonus
    else:
        bonus = bonus_cfg.mean_reversion_moderate_bonus

    return RuleResult.bonus(
        "mean_reversion_bonus",
        f"均值回归豁免+{bonus:.0f}分(最新={context.latest_value:.1f}仍为加权{context.latest_vs_weighted_ratio:.0%})",
        bonus
    )


# ============================================================================
# 4. 周期位置调整
# ============================================================================

def rule_cyclical_position_bonus(context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
    """
    周期位置调整

    整合原来的:
    - rule_cyclical_adjustment
    - rule_cycle_position_adjustment

    根据周期位置给予加分或扣分
    注意：扣分返回负bonus，由引擎处理
    """
    if not context.is_cyclical:
        return None

    bonus_cfg = config.bonus
    cycle_position = context.cycle_position
    current_phase = context.current_phase

    # === 底部加分 ===
    if cycle_position == "bottom" or current_phase == "trough":
        bonus = bonus_cfg.cyclical_bottom_bonus
        return RuleResult.bonus(
            "cyclical_bottom_bonus",
            f"周期底部+{bonus:.0f}分(逆向机会)",
            bonus
        )

    # === 回升期加分 ===
    if cycle_position == "mid_up" or current_phase == "rising":
        bonus = bonus_cfg.cyclical_recovery_bonus
        return RuleResult.bonus(
            "cyclical_recovery_bonus",
            f"周期回升+{bonus:.0f}分(景气回升)",
            bonus
        )

    # === 顶部预警 (返回负值作为扣分) ===
    if cycle_position == "top" or current_phase == "peak":
        # 这里返回 penalty 类型而非 bonus
        return RuleResult.penalty(
            "cyclical_top_penalty",
            f"周期顶部-{bonus_cfg.cyclical_top_penalty:.0f}分(警惕回落)",
            bonus_cfg.cyclical_top_penalty
        )

    # === 下行期扣分 ===
    if cycle_position == "mid_down" or current_phase == "falling":
        return RuleResult.penalty(
            "cyclical_downturn_penalty",
            f"周期回落-{bonus_cfg.cyclical_downturn_penalty:.0f}分(景气下行)",
            bonus_cfg.cyclical_downturn_penalty
        )

    return None


# ============================================================================
# 5. ROIIC 改善加分
# ============================================================================

def rule_roiic_positive_bonus(context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
    """
    ROIIC 改善加分

    ROIIC 为正且趋势向好
    """
    if not is_roiic_metric(context):
        return None

    bonus_cfg = config.bonus

    # 加权平均必须达到阈值
    if context.weighted_avg < bonus_cfg.roiic_positive_threshold:
        return None

    # 趋势必须向好
    if context.log_slope <= 0 or context.recent_3y_slope <= 0:
        return None

    # ROIC 交叉验证: ROIC 也不能恶化
    roic_stats = get_reference_metric(context, "roic")
    if roic_stats and roic_stats.get("log_slope") is not None:
        if roic_stats["log_slope"] < 0:
            return None

    # 计算加分
    growth_score = context.log_slope + max(context.recent_3y_slope, 0.0) * 0.5
    if growth_score <= 0:
        return None

    bonus = min(growth_score * 20, bonus_cfg.roiic_positive_max_bonus)

    if bonus <= 0:
        return None

    return RuleResult.bonus(
        "roiic_positive_bonus",
        f"ROIIC改善+{bonus:.1f}分(斜率{context.log_slope:.3f})",
        bonus
    )


# ============================================================================
# 加分规则列表
# ============================================================================

BONUS_RULES: List[Rule] = [
    Rule("growth_momentum_bonus", RuleCategory.BONUS, rule_growth_momentum_bonus,
         "成长动能加分", priority=200),
    Rule("inflection_recovery_bonus", RuleCategory.BONUS, rule_inflection_recovery_bonus,
         "拐点恢复加分", priority=210),
    Rule("mean_reversion_bonus", RuleCategory.BONUS, rule_mean_reversion_bonus,
         "均值回归豁免", priority=220),
    Rule("cyclical_position_bonus", RuleCategory.BONUS, rule_cyclical_position_bonus,
         "周期位置调整", priority=230),
    Rule("roiic_positive_bonus", RuleCategory.BONUS, rule_roiic_positive_bonus,
         "ROIIC改善加分", priority=240),
]


__all__ = [
    'rule_growth_momentum_bonus',
    'rule_inflection_recovery_bonus',
    'rule_mean_reversion_bonus',
    'rule_cyclical_position_bonus',
    'rule_roiic_positive_bonus',
    'BONUS_RULES',
]
