"""
趋势分析规则库 (Trend Analysis Rules)
=====================================

定义具体的评分和过滤规则函数。
这些规则被 TrendRuleEngine 调用。
"""

import logging
from typing import Optional, Dict, List
from .models import (
    TrendContext,
    TrendRuleParameters,
    TrendThresholds,
    RuleResult,
)

logger = logging.getLogger(__name__)

def _is_roiic_metric(context: TrendContext) -> bool:
    return context.metric_name.lower() == "roiic"

def _get_reference_metric(context: TrendContext, metric: str) -> Optional[Dict[str, float]]:
    metrics = context.reference_metrics or {}
    return metrics.get(metric.lower())

# ============================================================================
# 核心否决规则 (Veto Rules)
# ============================================================================

def rule_roiic_capital_destruction(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    if not _is_roiic_metric(context): return None
    if context.weighted_avg > params.roiic_veto_weighted_threshold: return None
    if context.latest_value > params.roiic_veto_latest_threshold: return None
    if context.log_slope > thresholds.severe_decline: return None
    if context.r_squared < max(thresholds.trend_significance, 0.4): return None

    roic_stats = _get_reference_metric(context, "roic")
    roic_threshold = thresholds.min_latest_value if thresholds.min_latest_value is not None else 8.0
    roic_flag = False
    if roic_stats:
        roic_latest = roic_stats.get("latest")
        roic_log_slope = roic_stats.get("log_slope")
        roic_recent = roic_stats.get("recent_3y_slope")
        if roic_latest is not None and roic_latest < roic_threshold: roic_flag = True
        if roic_log_slope is not None and roic_log_slope <= thresholds.mild_decline: roic_flag = True
        if roic_recent is not None and roic_recent < 0: roic_flag = True
    else:
        roic_flag = True

    deterioration_flag = (context.deterioration_severity in {"severe", "moderate"} or context.total_decline_pct >= 40)
    if roic_flag and deterioration_flag:
        message = f"ROIIC持续为负-加权{context.weighted_avg:.1f}%, 最新{context.latest_value:.1f}%"
        return RuleResult("roiic_capital_destruction_veto", "veto", message, log_level=logging.INFO, log_prefix="【ROIIC一票否决】")
    return None

def rule_min_latest_value(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    min_latest_value = thresholds.min_latest_value
    if min_latest_value is None: return None
    latest_value = context.latest_value

    # === 优化B: 困境反转/高成长豁免 ===
    # 如果最新值虽然略低，但趋势极其强劲（V型反转 或 加速增长），则给予豁免
    # 豁免条件：最新值达到门槛的 60% 且 (是V型反转 或 正在加速)
    if latest_value >= min_latest_value * 0.6:
        if context.inflection_type in ("deterioration_to_recovery", "acceleration"):
             logger.info(f"🚀 触发困境反转豁免: {context.group_key} 最新={latest_value:.2f} < {min_latest_value}, 但形态为 {context.inflection_type}")
             return None
        if context.is_accelerating and context.trend_acceleration > 0.1:
             logger.info(f"🚀 触发加速成长豁免: {context.group_key} 最新={latest_value:.2f}, 加速度={context.trend_acceleration:.2f}")
             return None

    if latest_value >= min_latest_value: return None

    # 严重亏损否决
    if latest_value < 0 and context.has_loss_years and context.loss_year_count >= 3:
        # 周期股豁免：如果是周期股且处于回升期，允许亏损
        if not (context.is_cyclical and context.current_phase == "rising"):
            message = f"连续亏损一票否决-最新{context.metric_name}={latest_value:.2f}, 5年亏损{context.loss_year_count}年"
            return RuleResult("min_latest_value_extreme_loss", "veto", message, log_level=logging.INFO, log_prefix="【一票否决】")

    # 断崖式下跌否决
    # === 优化C: 周期股放宽跌幅阈值 ===
    decline_limit = 75 if context.is_cyclical else 60
    if context.total_decline_pct >= decline_limit:
        message = f"断崖式恶化一票否决-总跌幅{context.total_decline_pct:.1f}%≥{decline_limit}%"
        return RuleResult("min_latest_value_extreme_decline", "veto", message, log_level=logging.INFO, log_prefix="【一票否决】")

    # 普通未达标扣分
    shortfall = min_latest_value - latest_value
    baseline = max(abs(min_latest_value), 1e-6)
    shortfall_ratio = max(0.0, shortfall / baseline)
    if shortfall_ratio <= 0: return None

    base_penalty = shortfall_ratio * params.penalty_factor
    severity_multiplier = 1.0
    if shortfall_ratio >= 0.40: severity_multiplier = 2.5
    elif shortfall_ratio >= 0.25: severity_multiplier = 1.8
    elif shortfall_ratio >= 0.15: severity_multiplier = 1.3

    if context.latest_vs_weighted_ratio < 0.65: severity_multiplier += 0.4
    elif context.latest_vs_weighted_ratio < 0.75: severity_multiplier += 0.2

    modifier = 1.0
    modifier_notes: List[str] = []
    if context.log_slope >= 0.12 and context.recent_3y_slope >= 0.12:
        modifier *= 0.4
        modifier_notes.append("强劲成长豁免")
    elif context.log_slope > 0 and context.recent_3y_slope > 0:
        modifier *= 0.6
        modifier_notes.append("趋势回升减免")
    if context.trend_acceleration > 0 and context.is_accelerating:
        modifier *= 0.75
        modifier_notes.append("加速度减免")

    penalty_value = min(base_penalty * modifier * severity_multiplier, params.max_penalty)
    penalty_value = max(0.0, penalty_value)
    if penalty_value == 0: return None

    note_suffix = f"（{'、'.join(modifier_notes)}）" if modifier_notes else ""
    message = f"盈利率低于门槛-{penalty_value:.1f}分(最新{context.metric_name}={latest_value:.2f} < {min_latest_value}){note_suffix}"
    return RuleResult("min_latest_value_penalty", "penalty", message, penalty_value)

def rule_low_significance_decline(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    if context.r_squared < 0.4 and context.cv < 0.15:
        severe_guardrail = thresholds.severe_decline * 1.5
        if thresholds.latest_threshold is not None and context.log_slope < severe_guardrail and context.latest_value < thresholds.latest_threshold:
            message = f"严重衰退-稳定型(log斜率={context.log_slope:.3f}, CV={context.cv:.2f}, 最新={context.latest_value:.1f})"
            return RuleResult("low_significance_decline", "veto", message)
    return None

def rule_high_volatility_instability(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    if context.r_squared < 0.4 and context.cv > 0.30:
        min_latest_value = thresholds.min_latest_value
        if min_latest_value is not None and context.latest_value < min_latest_value * 1.3:
            message = f"高波动不稳定(CV={context.cv:.2f}, R²={context.r_squared:.2f}, 最新={context.latest_value:.1f})"
            return RuleResult("high_volatility_instability", "veto", message)
    return None

def rule_severe_decline(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    """
    严重衰退否决规则
    包含稳健性豁免逻辑：如果OLS显示衰退但Theil-Sen显示稳定，则豁免。
    """
    # === 优化A: 周期谷底豁免 ===
    # 如果是周期性行业，且处于谷底或复苏初期，即使数据衰退严重，也不应直接否决（可能是买点）
    if context.is_cyclical and context.current_phase in ("trough", "recovery"):
        logger.info(f"🛡️ 触发周期谷底豁免(严重衰退): {context.group_key} 处于 {context.current_phase} 阶段")
        return None

    if context.log_slope < thresholds.severe_decline and context.r_squared > thresholds.trend_significance and thresholds.latest_threshold is not None and context.latest_value < thresholds.latest_threshold:

        # === 鲁棒性豁免逻辑 ===
        # 如果存在稳健斜率，且稳健斜率明显优于OLS斜率（未触及严重衰退线），且两者差异较大
        # 说明可能是单年异常值拉低了OLS斜率
        if context.robust_slope is not None and not isinstance(context.robust_slope, str) and not np.isnan(context.robust_slope):
             if context.robust_slope > thresholds.severe_decline and abs(context.robust_slope - context.log_slope) > 0.1:
                 logger.info(f"🛡️ 触发稳健性豁免: {context.group_key} OLS={context.log_slope:.3f}, Robust={context.robust_slope:.3f}")
                 return None

        message = f"严重衰退(log斜率={context.log_slope:.3f}, CAGR≈{context.cagr_approx*100:.1f}%, R²={context.r_squared:.2f}, 最新={context.latest_value:.1f})"
        return RuleResult("severe_decline", "veto", message)
    return None

def rule_severe_deterioration_veto(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    # === 优化A: 周期谷底豁免 ===
    if context.is_cyclical and context.current_phase in ("trough", "recovery"):
        logger.info(f"🛡️ 触发周期谷底豁免(严重恶化): {context.group_key} 处于 {context.current_phase} 阶段")
        return None

    if context.deterioration_severity != "severe": return None
    if context.total_decline_pct > 40:
        message = f"严重恶化一票否决-跌幅{context.total_decline_pct:.1f}%>40%"
        return RuleResult("severe_deterioration_decline", "veto", message, log_level=logging.INFO, log_prefix="【一票否决】")
    if context.latest_vs_weighted_ratio < 0.7:
        message = f"严重恶化一票否决-最新值仅为加权平均{context.latest_vs_weighted_ratio:.1%}<70%"
        return RuleResult("severe_deterioration_ratio", "veto", message, log_level=logging.INFO, log_prefix="【一票否决】")
    return None

def rule_structural_decline_veto(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    severe_trend = context.log_slope <= thresholds.severe_decline
    persistent_decline = context.log_slope <= thresholds.mild_decline and context.recent_3y_slope <= -0.05
    if not (severe_trend or persistent_decline): return None
    if context.r_squared < max(thresholds.trend_significance, 0.5): return None
    if context.latest_vs_weighted_ratio > 0.85: return None
    if context.total_decline_pct < 25: return None
    if context.trend_acceleration > -0.05 and context.recent_3y_slope > -0.02: return None
    message = f"结构性衰退一票否决-对数斜率{context.log_slope:.3f}, 最新仅为加权{context.latest_vs_weighted_ratio:.1%}, 近3年斜率{context.recent_3y_slope:.3f}, 总跌幅{context.total_decline_pct:.1f}%"
    return RuleResult("structural_decline_veto", "veto", message, log_level=logging.INFO, log_prefix="【一票否决】")

# ============================================================================
# 扣分规则 (Penalty Rules)
# ============================================================================

def rule_roiic_negative_penalty(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    if not _is_roiic_metric(context): return None
    weighted_component = max(-context.weighted_avg - params.roiic_negative_penalty_buffer, 0.0)
    slope_component = max(-context.log_slope, 0.0)
    if weighted_component <= 0 and slope_component <= 0: return None

    weighted_penalty = weighted_component / max(params.roiic_negative_penalty_scale, 1.0)
    slope_penalty = slope_component * params.penalty_factor * 0.3
    penalty_value = weighted_penalty + slope_penalty

    roic_stats = _get_reference_metric(context, "roic")
    roic_threshold = thresholds.min_latest_value if thresholds.min_latest_value is not None else 8.0
    if roic_stats:
        roic_latest = roic_stats.get("latest")
        roic_log_slope = roic_stats.get("log_slope")
        if roic_log_slope is not None and roic_log_slope <= thresholds.mild_decline: penalty_value *= 1.3
        if roic_latest is not None and roic_latest < roic_threshold: penalty_value *= 1.2

    penalty_cap = min(params.roiic_negative_penalty_cap, params.max_penalty)
    penalty_value = max(0.0, min(penalty_value, penalty_cap))
    if penalty_value < 1e-3: return None
    message = f"ROIIC为负拖累-{penalty_value:.1f}分(加权{context.weighted_avg:.1f}%, 斜率{context.log_slope:.3f})"
    return RuleResult("roiic_negative_penalty", "penalty", message, penalty_value)

def rule_roiic_roic_divergence(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    if not _is_roiic_metric(context): return None
    roic_stats = _get_reference_metric(context, "roic")
    if not roic_stats: return None
    roic_log_slope = roic_stats.get("log_slope")
    roic_latest = roic_stats.get("latest")
    if roic_log_slope is None or roic_latest is None: return None
    roic_threshold = thresholds.min_latest_value if thresholds.min_latest_value is not None else 8.0
    if roic_latest < roic_threshold: return None
    if context.log_slope >= 0: return None
    slope_gap = roic_log_slope - context.log_slope
    if slope_gap < params.roiic_divergence_slope_gap: return None
    penalty = min(slope_gap * params.penalty_factor * 0.4, params.max_penalty / 2)
    penalty = max(penalty, 2.0)
    message = f"ROIIC与ROIC背离-{penalty:.1f}分(ROIC斜率{roic_log_slope:.3f} > ROIIC {context.log_slope:.3f})"
    return RuleResult("roiic_roic_divergence_penalty", "penalty", message, penalty)

def rule_mild_decline_penalty(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    if context.log_slope < thresholds.mild_decline and context.r_squared > thresholds.trend_significance:
        penalty_factor = params.penalty_factor
        max_penalty = params.max_penalty
        trend_penalty = min(abs(context.log_slope) * penalty_factor, max_penalty)
        message = f"轻度衰退-{trend_penalty:.1f}分(log斜率{context.log_slope:.3f})"
        return RuleResult("mild_decline_penalty", "penalty", message, trend_penalty)
    return None

def rule_deterioration_penalty(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    if not context.has_deterioration or context.deterioration_severity == "none": return None
    penalties = {"severe": 15, "moderate": 10, "mild": 5}
    value = penalties.get(context.deterioration_severity)
    if value is None or value <= 0: return None
    severity_label = {"severe": "严重恶化", "moderate": "中度恶化", "mild": "轻度恶化"}.get(context.deterioration_severity, context.deterioration_severity)
    message = f"{severity_label}-{value}分"
    return RuleResult("deterioration_penalty", "penalty", message, float(value))

def rule_sustained_decline(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    if context.log_slope < params.sustained_decline_threshold and context.latest_value < context.weighted_avg:
        sustained_penalty = params.sustained_decline_penalty
        message = f"持续衰退重罚-{sustained_penalty}分(最新<加权)"
        return RuleResult("sustained_decline", "penalty", message, float(sustained_penalty))
    return None

def rule_single_year_decline(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    year4_to_5_pct = context.deterioration_value("year4_to_5_pct")
    year3_to_4_pct = context.deterioration_value("year3_to_4_pct")
    severe_single_year_threshold = params.severe_single_year_decline_pct
    if year4_to_5_pct < severe_single_year_threshold or year3_to_4_pct < severe_single_year_threshold:
        single_year_penalty = params.severe_single_year_penalty
        worst_year = min(year4_to_5_pct, year3_to_4_pct)
        message = f"单年巨幅下滑-{single_year_penalty}分(年跌{worst_year:.1f}%)"
        return RuleResult("single_year_decline", "penalty", message, float(single_year_penalty))
    return None

def rule_relative_decline(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    ratio = context.latest_vs_weighted_ratio
    ratio60 = params.relative_decline_ratio_60
    penalty60 = params.relative_decline_penalty_60
    ratio70 = params.relative_decline_ratio_70
    penalty70 = params.relative_decline_penalty_70
    if ratio < ratio60:
        message = f"相对加权暴跌-{penalty60}分(最新仅为加权{ratio:.1%})"
        return RuleResult("relative_decline_60", "penalty", message, float(penalty60))
    if ratio < ratio70:
        message = f"相对加权下滑-{penalty70}分(最新为加权{ratio:.1%})"
        return RuleResult("relative_decline_70", "penalty", message, float(penalty70))
    return None

def rule_compound_recent_deterioration(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    if not context.has_deterioration or context.deterioration_severity == "none": return None
    negative_signals = 0
    if context.inflection_type == "growth_to_decline": negative_signals += 1
    if context.is_decelerating and context.recent_3y_slope < 0: negative_signals += 1
    if context.log_slope < thresholds.mild_decline: negative_signals += 1
    if context.latest_vs_weighted_ratio < 0.75: negative_signals += 1
    if negative_signals < 2: return None
    if context.deterioration_severity == "severe" and context.total_decline_pct >= 35:
        message = "复合恶化一票否决-趋势反转、加速下滑与大幅回撤同时触发"
        return RuleResult("compound_deterioration_veto", "veto", message, log_level=logging.INFO, log_prefix="【一票否决】")
    penalty_value = min(10 + negative_signals * 2, params.max_penalty)
    message = f"复合恶化-{penalty_value:.1f}分(触发{negative_signals}项恶化信号)"
    return RuleResult("compound_deterioration_penalty", "penalty", message, float(penalty_value))

def rule_inflection_penalty_or_bonus(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    if not context.has_inflection or context.inflection_type == "none": return None
    slope_change = context.slope_change
    if context.inflection_type == "growth_to_decline":
        decline_penalty = min(abs(slope_change) * 2, 10)
        message = f"增长转衰退-{decline_penalty:.1f}分(斜率变化{slope_change:.2f})"
        return RuleResult("inflection_decline", "penalty", message, decline_penalty)
    if context.inflection_type == "deterioration_to_recovery":
        recovery_bonus = min(abs(slope_change) * 2, 10)
        message = f"恶化转好+{recovery_bonus:.1f}分(斜率变化{slope_change:.2f})"
        return RuleResult("inflection_recovery", "bonus", message, recovery_bonus)
    return None

def rule_cyclical_adjustment(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    if not context.is_cyclical: return None
    if context.current_phase == "trough":
        bonus = min(context.peak_to_trough_ratio / 2, 5)
        message = f"周期谷底+{bonus:.1f}分(峰谷比{context.peak_to_trough_ratio:.2f})"
        return RuleResult("cyclical_trough", "bonus", message, bonus)
    if context.current_phase == "peak":
        penalty_value = min(context.peak_to_trough_ratio / 3, 5)
        message = f"周期峰顶-{penalty_value:.1f}分(峰谷比{context.peak_to_trough_ratio:.2f})"
        return RuleResult("cyclical_peak", "penalty", message, penalty_value)
    return None

def rule_acceleration_adjustment(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    if context.is_accelerating and context.recent_3y_slope > 0:
        bonus = min(abs(context.trend_acceleration) / 2, 5)
        message = f"加速上升+{bonus:.1f}分(加速度{context.trend_acceleration:.2f})"
        return RuleResult("trend_accelerating", "bonus", message, bonus)
    if context.is_decelerating and context.recent_3y_slope < 0:
        penalty_value = min(abs(context.trend_acceleration) / 2, 5)
        message = f"加速下滑-{penalty_value:.1f}分(加速度{context.trend_acceleration:.2f})"
        return RuleResult("trend_decelerating", "penalty", message, penalty_value)
    return None

def rule_roiic_positive_bonus(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    if not _is_roiic_metric(context): return None
    if context.weighted_avg < params.roiic_positive_bonus_threshold: return None
    if context.log_slope <= 0 or context.recent_3y_slope <= 0: return None
    roic_stats = _get_reference_metric(context, "roic")
    if roic_stats and roic_stats.get("log_slope") is not None and roic_stats["log_slope"] < 0: return None
    growth_score = context.log_slope + max(context.recent_3y_slope, 0.0) * 0.5
    if growth_score <= 0: return None
    bonus = min(growth_score * 20, 8.0)
    if bonus <= 0: return None
    message = f"ROIIC改善动能+{bonus:.1f}分(斜率{context.log_slope:.3f})"
    return RuleResult("roiic_positive_bonus", "bonus", message, bonus)

def rule_growth_momentum_bonus(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    if context.log_slope <= 0 or context.recent_3y_slope <= 0: return None
    if context.trend_acceleration <= 0 and not context.is_accelerating: return None
    growth_score = context.log_slope + max(context.recent_3y_slope, 0)
    if context.trend_acceleration > 0: growth_score += context.trend_acceleration * 0.5
    if growth_score <= 0: return None
    bonus_value = min(growth_score * 20, 8.0)
    if bonus_value <= 0: return None
    message = f"成长动能+{bonus_value:.1f}分(对数斜率{context.log_slope:.3f}, 近3年斜率{context.recent_3y_slope:.3f})"
    return RuleResult("growth_momentum_bonus", "bonus", message, bonus_value)

import numpy as np
