"""
扣分规则 (Penalty Rules)
=========================

扣分规则：累计扣分影响最终得分。

规则清单 (8个):
1. rule_mild_decline_penalty - 轻度衰退扣分
2. rule_deterioration_penalty - 恶化程度扣分
3. rule_volatility_penalty - 高波动扣分
4. rule_relative_decline_penalty - 相对跌幅扣分
5. rule_single_year_decline_penalty - 单年暴跌扣分
6. rule_consecutive_decline_penalty - 连续下跌扣分
7. rule_roiic_negative_penalty - ROIIC为负扣分
8. rule_roiic_divergence_penalty - ROIIC/ROIC背离扣分
"""

from typing import Optional, List

from .base import (
    RuleResult, Rule, TrendContext, RuleConfig, RuleCategory,
    is_roiic_metric, get_reference_metric, logger
)


# ============================================================================
# 1. 轻度衰退扣分
# ============================================================================

def rule_mild_decline_penalty(context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
    """
    轻度衰退扣分

    整合原来的:
    - rule_mild_decline_penalty
    - rule_sustained_decline

    触发条件:
    - log_slope < mild_decline_slope 且趋势显著
    """
    penalty_cfg = config.penalty
    veto_cfg = config.veto

    # 轻度衰退: 斜率为负但未达严重程度
    if context.log_slope >= penalty_cfg.mild_decline_slope:
        return None

    # 趋势不显著时跳过
    if context.r_squared < veto_cfg.severe_decline_r2_min:
        return None

    # 计算扣分
    penalty_factor = config.scoring.penalty_factor
    base_penalty = abs(context.log_slope) * penalty_factor

    # 持续衰退加重: 最新值低于加权平均
    if context.latest_value < context.weighted_avg:
        base_penalty *= 1.3
        message = f"持续衰退-{base_penalty:.1f}分(斜率{context.log_slope:.3f}, 最新<加权)"
    else:
        message = f"轻度衰退-{base_penalty:.1f}分(斜率{context.log_slope:.3f})"

    penalty = min(base_penalty, penalty_cfg.mild_decline_max_penalty)

    return RuleResult.penalty("mild_decline_penalty", message, penalty)


# ============================================================================
# 2. 恶化程度扣分
# ============================================================================

def rule_deterioration_penalty(context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
    """
    恶化程度扣分

    整合原来的:
    - rule_deterioration_penalty
    - rule_bayesian_deterioration_alert (贝叶斯增强)
    - rule_chronic_decline_pattern (恶化模式)

    根据恶化严重程度和模式计算扣分
    """
    if not context.has_deterioration or context.deterioration_severity == "none":
        return None

    penalty_cfg = config.penalty

    # 基础扣分
    base_penalties = {
        "severe": penalty_cfg.deterioration_severe_penalty,
        "moderate": penalty_cfg.deterioration_moderate_penalty,
        "mild": penalty_cfg.deterioration_mild_penalty,
    }

    base_penalty = base_penalties.get(context.deterioration_severity, 0)
    if base_penalty <= 0:
        return None

    # === 贝叶斯增强 ===
    bayesian_multiplier = 1.0
    if context.deterioration_probability > 0.85:
        bayesian_multiplier = 1.3  # 高置信度恶化加重
    elif context.deterioration_probability > 0.70:
        bayesian_multiplier = 1.15
    elif context.deterioration_probability < 0.30 and context.has_deterioration:
        bayesian_multiplier = 0.7  # 低概率可能误判，减轻

    # === 恶化模式增强 ===
    pattern_multiplier = 1.0
    pattern = context.deterioration_pattern

    if pattern == "accelerating_decline":
        pattern_multiplier = 1.4  # 加速下滑最危险
    elif pattern == "chronic_decline":
        pattern_multiplier = 1.2  # 慢性衰退
    elif pattern == "cliff_drop":
        pattern_multiplier = 1.1  # 断崖式
    elif pattern == "high_level_pullback":
        pattern_multiplier = 0.8  # 高位回调减轻

    # 计算最终扣分
    final_penalty = base_penalty * bayesian_multiplier * pattern_multiplier
    final_penalty = min(final_penalty, config.scoring.max_penalty)

    severity_labels = {
        "severe": "严重恶化",
        "moderate": "中度恶化",
        "mild": "轻度恶化",
    }
    severity_label = severity_labels.get(context.deterioration_severity, context.deterioration_severity)

    message = f"{severity_label}-{final_penalty:.1f}分"
    if pattern and pattern != "none":
        message += f"({pattern})"

    return RuleResult.penalty("deterioration_penalty", message, final_penalty)


# ============================================================================
# 3. 高波动扣分
# ============================================================================

def rule_volatility_penalty(context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
    """
    高波动扣分

    整合原来的:
    - rule_volatility_regime_adjustment
    - 高波动性不稳定检测

    触发条件:
    - CV > 阈值
    - 波动率体制为上升
    - 存在ARCH效应
    """
    penalty_cfg = config.penalty

    cv = context.cv
    vol_regime = context.volatility_regime
    vol_change = context.volatility_change_ratio
    has_arch = context.has_arch_effect

    # 基础高波动扣分
    if cv < penalty_cfg.high_volatility_cv:
        # 波动不高，但检查ARCH效应
        if has_arch:
            return RuleResult.penalty(
                "arch_effect_penalty",
                f"波动聚集提示-2分(ARCH效应)",
                2.0
            )
        return None

    # 计算扣分
    base_penalty = penalty_cfg.volatility_penalty_base

    # 波动率飙升
    if vol_regime == "increasing_vol" and vol_change > 2.0:
        penalty = min(vol_change * 2, 8.0)
        return RuleResult.penalty(
            "volatility_surge_penalty",
            f"波动率飙升-{penalty:.1f}分(近期波动是早期{vol_change:.1f}倍)",
            penalty
        )

    # ARCH效应 + 波动上升
    if has_arch and vol_regime == "increasing_vol":
        return RuleResult.penalty(
            "arch_volatility_penalty",
            f"波动聚集风险-6分(ARCH+波动上升)",
            6.0
        )

    # 普通高波动
    excess_cv = (cv - penalty_cfg.high_volatility_cv) / penalty_cfg.high_volatility_cv
    penalty = min(base_penalty * (1 + excess_cv), config.scoring.max_penalty / 2)

    return RuleResult.penalty(
        "high_volatility_penalty",
        f"高波动-{penalty:.1f}分(CV={cv:.2f})",
        penalty
    )


# ============================================================================
# 4. 相对跌幅扣分
# ============================================================================

def rule_relative_decline_penalty(context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
    """
    相对跌幅扣分

    最新值相对于加权平均的跌幅
    """
    penalty_cfg = config.penalty

    ratio = context.latest_vs_weighted_ratio

    # 跌幅60%以上
    if ratio < penalty_cfg.relative_decline_60:
        return RuleResult.penalty(
            "relative_decline_60_penalty",
            f"相对加权暴跌-{penalty_cfg.relative_decline_60_penalty:.0f}分(最新仅为加权{ratio:.1%})",
            penalty_cfg.relative_decline_60_penalty
        )

    # 跌幅70%以上
    if ratio < penalty_cfg.relative_decline_70:
        return RuleResult.penalty(
            "relative_decline_70_penalty",
            f"相对加权下滑-{penalty_cfg.relative_decline_70_penalty:.0f}分(最新为加权{ratio:.1%})",
            penalty_cfg.relative_decline_70_penalty
        )

    return None


# ============================================================================
# 5. 单年暴跌扣分
# ============================================================================

def rule_single_year_decline_penalty(context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
    """
    单年暴跌扣分

    单年跌幅超过阈值
    """
    penalty_cfg = config.penalty

    # 获取年度变化
    year4_to_5 = context.deterioration_value("year4_to_5_pct") if hasattr(context, 'deterioration_value') else 0
    year3_to_4 = context.deterioration_value("year3_to_4_pct") if hasattr(context, 'deterioration_value') else 0

    # 如果没有年度变化数据，从raw_values计算
    if year4_to_5 == 0 and year3_to_4 == 0:
        values = context.raw_values
        if values and len(values) >= 2:
            for i in range(len(values) - 1, 0, -1):
                if values[i - 1] != 0:
                    pct_change = ((values[i] - values[i - 1]) / abs(values[i - 1])) * 100
                    if pct_change < year4_to_5:
                        year4_to_5 = pct_change

    worst_year = min(year4_to_5, year3_to_4) if year3_to_4 != 0 else year4_to_5

    if worst_year < penalty_cfg.single_year_decline_pct:
        return RuleResult.penalty(
            "single_year_decline_penalty",
            f"单年暴跌-{penalty_cfg.single_year_penalty:.0f}分(年跌{worst_year:.1f}%)",
            penalty_cfg.single_year_penalty
        )

    return None


# ============================================================================
# 6. 连续下跌扣分 (智能版)
# ============================================================================

def rule_consecutive_decline_penalty(context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
    """
    连续下跌扣分 (智能版)

    整合原来的:
    - rule_smart_consecutive_decline

    智能连续下跌计数：微小反弹(<2%)不打断连续下跌
    """
    penalty_cfg = config.penalty

    values = context.raw_values
    if values is None or len(values) < 3:
        return None

    MICRO_BOUNCE_THRESHOLD = 2.0  # 涨幅小于2%视为无实质性反弹

    smart_consecutive = 0

    for i in range(len(values) - 1, 0, -1):
        current = values[i]
        previous = values[i - 1]

        if previous == 0:
            continue

        pct_change = ((current - previous) / abs(previous)) * 100

        if pct_change < -2.0:  # 实质性下跌
            smart_consecutive += 1
        elif pct_change < MICRO_BOUNCE_THRESHOLD:  # 微小反弹
            pass  # 不打断计数
        else:  # 实质性反弹
            break

    # 计算累计跌幅
    if values[0] != 0:
        total_decline_pct = ((values[-1] - values[0]) / abs(values[0])) * 100
    else:
        total_decline_pct = 0

    # 连续3年以上实质性下跌
    if smart_consecutive >= 3 and total_decline_pct < -30:
        penalty = min(smart_consecutive * 4, penalty_cfg.consecutive_3y_penalty + 4)
        return RuleResult.penalty(
            "consecutive_decline_severe_penalty",
            f"连续下跌-{penalty:.0f}分({smart_consecutive}年下跌，累计{total_decline_pct:.1f}%)",
            penalty
        )

    # 连续2年下跌
    if smart_consecutive >= 2 and total_decline_pct < -20:
        penalty = penalty_cfg.consecutive_2y_penalty
        return RuleResult.penalty(
            "consecutive_decline_warning_penalty",
            f"连续下跌警示-{penalty:.0f}分({smart_consecutive}年下跌)",
            penalty
        )

    return None


# ============================================================================
# 7. ROIIC 为负扣分
# ============================================================================

def rule_roiic_negative_penalty(context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
    """
    ROIIC 为负扣分

    ROIIC 为负表示新增投资效率低下
    """
    if not is_roiic_metric(context):
        return None

    penalty_cfg = config.penalty

    weighted_component = max(-context.weighted_avg - penalty_cfg.roiic_negative_buffer, 0.0)
    slope_component = max(-context.log_slope, 0.0)

    if weighted_component <= 0 and slope_component <= 0:
        return None

    # 计算扣分
    weighted_penalty = weighted_component / max(penalty_cfg.roiic_negative_scale, 1.0)
    slope_penalty = slope_component * config.scoring.penalty_factor * 0.3
    penalty = weighted_penalty + slope_penalty

    # ROIC 交叉验证加重
    roic_stats = get_reference_metric(context, "roic")
    if roic_stats:
        roic_latest = roic_stats.get("latest")
        roic_slope = roic_stats.get("log_slope")

        if roic_slope is not None and roic_slope <= penalty_cfg.mild_decline_slope:
            penalty *= 1.3
        if roic_latest is not None and roic_latest < 8.0:
            penalty *= 1.2

    penalty = min(penalty, penalty_cfg.roiic_negative_cap)

    if penalty < 1e-3:
        return None

    return RuleResult.penalty(
        "roiic_negative_penalty",
        f"ROIIC为负-{penalty:.1f}分(加权{context.weighted_avg:.1f}%)",
        penalty
    )


# ============================================================================
# 8. ROIIC/ROIC 背离扣分
# ============================================================================

def rule_roiic_divergence_penalty(context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
    """
    ROIIC/ROIC 背离扣分

    ROIC 良好但 ROIIC 下跌，表示新投资效率在恶化
    """
    if not is_roiic_metric(context):
        return None

    penalty_cfg = config.penalty

    roic_stats = get_reference_metric(context, "roic")
    if not roic_stats:
        return None

    roic_slope = roic_stats.get("log_slope")
    roic_latest = roic_stats.get("latest")

    if roic_slope is None or roic_latest is None:
        return None

    # ROIC 良好
    if roic_latest < 8.0:
        return None

    # ROIIC 在下跌
    if context.log_slope >= 0:
        return None

    # 计算背离
    slope_gap = roic_slope - context.log_slope
    if slope_gap < penalty_cfg.roiic_divergence_gap:
        return None

    penalty = min(slope_gap * config.scoring.penalty_factor * 0.4, config.scoring.max_penalty / 2)
    penalty = max(penalty, 2.0)

    return RuleResult.penalty(
        "roiic_divergence_penalty",
        f"ROIIC与ROIC背离-{penalty:.1f}分(ROIC斜率{roic_slope:.3f} > ROIIC {context.log_slope:.3f})",
        penalty
    )


# ============================================================================
# 扣分规则列表
# ============================================================================

PENALTY_RULES: List[Rule] = [
    Rule("mild_decline_penalty", RuleCategory.PENALTY, rule_mild_decline_penalty,
         "轻度衰退扣分", priority=100),
    Rule("deterioration_penalty", RuleCategory.PENALTY, rule_deterioration_penalty,
         "恶化程度扣分", priority=110),
    Rule("volatility_penalty", RuleCategory.PENALTY, rule_volatility_penalty,
         "高波动扣分", priority=120),
    Rule("relative_decline_penalty", RuleCategory.PENALTY, rule_relative_decline_penalty,
         "相对跌幅扣分", priority=130),
    Rule("single_year_decline_penalty", RuleCategory.PENALTY, rule_single_year_decline_penalty,
         "单年暴跌扣分", priority=140),
    Rule("consecutive_decline_penalty", RuleCategory.PENALTY, rule_consecutive_decline_penalty,
         "连续下跌扣分", priority=150),
    Rule("roiic_negative_penalty", RuleCategory.PENALTY, rule_roiic_negative_penalty,
         "ROIIC为负扣分", priority=160),
    Rule("roiic_divergence_penalty", RuleCategory.PENALTY, rule_roiic_divergence_penalty,
         "ROIIC/ROIC背离扣分", priority=170),
]


__all__ = [
    'rule_mild_decline_penalty',
    'rule_deterioration_penalty',
    'rule_volatility_penalty',
    'rule_relative_decline_penalty',
    'rule_single_year_decline_penalty',
    'rule_consecutive_decline_penalty',
    'rule_roiic_negative_penalty',
    'rule_roiic_divergence_penalty',
    'PENALTY_RULES',
]
