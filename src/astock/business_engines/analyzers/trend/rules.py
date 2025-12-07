"""
趋势分析规则库 (Trend Analysis Rules)
=====================================

定义具体的评分和过滤规则函数。
这些规则被 TrendRuleEngine 调用。
"""

import logging
import numpy as np
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
# 交叉验证规则 (Cross-Validation Rules)
# ============================================================================

def rule_earnings_quality_divergence(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    """
    【含金量检验】净利润与经营现金流背离校验
    逻辑：如果净利润高速增长，但经营现金流（OCF）持续恶化或显著低于利润，提示"纸面富贵"风险。
    """
    # 只在分析利润类指标时触发
    if "profit" not in context.metric_name.lower(): return None

    # 获取参考指标 OCF (需要在 pipeline 中配置 reference)
    ocf_stats = _get_reference_metric(context, "ocfps")
    if not ocf_stats: return None

    profit_slope = context.log_slope
    ocf_slope = ocf_stats.get("log_slope", 0.0)

    # 1. 剪刀差风险：利润向上，现金流向下 (使用可配置阈值)
    profit_threshold = params.cross_val_profit_positive_threshold  # 默认 0.10
    ocf_threshold = params.cross_val_ocf_negative_threshold  # 默认 -0.05
    if profit_slope > profit_threshold and ocf_slope < ocf_threshold:
        message = f"盈利质量预警-利润高增({profit_slope:.1%})但现金流恶化({ocf_slope:.1%})"
        return RuleResult("earnings_quality_divergence", "penalty", message, 15.0)

    # 2. 长期造假嫌疑：利润增速远超现金流增速 (使用可配置阈值)
    gap_threshold = params.cross_val_profit_ocf_gap  # 默认 0.20
    if profit_slope - ocf_slope > gap_threshold:
        message = f"现金流跟不上利润-增速差{profit_slope - ocf_slope:.1%}"
        return RuleResult("profit_cash_gap", "penalty", message, 10.0)

    return None

def rule_sustainable_growth_check(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    """
    【内生增长检验】营收增速 vs ROE
    逻辑：长期来看，营收增速不应大幅超过 ROE。如果 营收增长 30% 但 ROE 只有 5%，说明增长靠吸血（融资）。
    """
    if "revenue" not in context.metric_name.lower(): return None

    roe_stats = _get_reference_metric(context, "roe")
    if not roe_stats: return None

    revenue_growth = context.cagr_approx
    roe_latest = roe_stats.get("latest", 0.0) / 100.0 # 假设 ROE 是百分比 15.0

    # 如果是高增长 (>20%) 但 ROE 很低 (<8%)
    if revenue_growth > 0.20 and roe_latest < 0.08:
        message = f"低效扩张风险-营收增速{revenue_growth:.1%}远超ROE{roe_latest:.1%}"
        return RuleResult("unsustainable_growth", "penalty", message, 12.0)

    return None

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

# ============================================================================
# 新增专业规则：杜邦分解一致性校验
# ============================================================================

def rule_dupont_consistency(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    """
    【杜邦分析一致性】ROE驱动因素校验

    ROE = 净利率 × 资产周转率 × 权益乘数

    逻辑：
    1. 如果ROE上升但净利率下降，说明依赖杠杆/周转提速，不可持续
    2. 如果ROE上升但毛利率下降，说明可能在打价格战换市场份额
    """
    if "roe" not in context.metric_name.lower():
        return None

    # 获取净利率参考数据
    nm_stats = _get_reference_metric(context, "net_margin")
    gm_stats = _get_reference_metric(context, "gross_margin")

    roe_slope = context.log_slope

    # 情景1: ROE向上(>5%) 但 净利率向下(<-3%) = 杠杆驱动
    if nm_stats:
        nm_slope = nm_stats.get("log_slope", 0.0)
        if roe_slope > 0.05 and nm_slope < -0.03:
            penalty = min(abs(nm_slope - roe_slope) * 10, 8.0)
            message = f"杜邦分解预警-ROE增({roe_slope:.1%})靠杠杆/周转,净利率跌({nm_slope:.1%})"
            return RuleResult("dupont_leverage_risk", "penalty", message, penalty)

    # 情景2: ROE向上 但 毛利率显著下降 = 价格战风险
    if gm_stats:
        gm_slope = gm_stats.get("log_slope", 0.0)
        if roe_slope > 0.05 and gm_slope < -0.05:
            penalty = min(abs(gm_slope) * 8, 6.0)
            message = f"毛利率侵蚀预警-ROE增长可能靠降价换量,毛利跌({gm_slope:.1%})"
            return RuleResult("dupont_margin_erosion", "penalty", message, penalty)

    return None

def rule_mean_reversion_adjustment(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    """
    【均值回归调整】高基数正常回落豁免

    逻辑：
    如果最新值虽然下跌，但仍高于5年加权均值的80%，且绝对值仍达标，
    则减轻处罚（高位正常回调 vs 真正恶化）。

    适用场景：茅台从30%ROE跌到25%，虽然下跌但仍是顶级水平。
    """
    # 只对"看起来在恶化"的情况触发
    if not context.has_deterioration:
        return None
    if context.deterioration_severity not in ("moderate", "mild"):
        return None

    # 关键条件：最新值仍高于加权均值的80%
    if context.latest_vs_weighted_ratio < 0.8:
        return None

    # 关键条件：最新值仍达到绝对门槛（如有）
    min_val = thresholds.min_latest_value
    if min_val is not None and context.latest_value < min_val:
        return None

    # 通过所有条件 = 高位正常回调，给予加分（抵消部分deterioration扣分）
    bonus = 3.0 if context.deterioration_severity == "mild" else 5.0
    message = f"均值回归豁免+{bonus:.0f}分(最新={context.latest_value:.1f}仍为加权均值{context.latest_vs_weighted_ratio:.0%})"
    return RuleResult("mean_reversion_adjustment", "bonus", message, bonus)


# ============================================================================
# 新增专业规则：周期位置规则
# ============================================================================

def rule_cycle_position_adjustment(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    """
    【周期位置调整】根据周期位置调整评分

    周期位置来自 CyclicalPatternDetector 的 cycle_position 字段：
    - "bottom": 周期底部 → 加分（逆向买入机会）
    - "mid_up": 底部回升 → 小加分
    - "top": 周期顶部 → 扣分（警惕均值回归）
    - "mid_down": 顶部回落 → 小扣分

    触发条件：必须是已识别的周期性股票
    """
    if not context.is_cyclical:
        return None

    # 直接从 context 获取 cycle_position（更专业的方式）
    cycle_position = context.cycle_position

    if not cycle_position or cycle_position == "unknown":
        return None

    if cycle_position == "bottom":
        # 周期底部：即使基本面差，也可能是买入时机
        bonus = 8.0
        message = f"周期底部加分+{bonus:.0f}分(逆向机会,放宽否决条件)"
        return RuleResult("cycle_bottom_bonus", "bonus", message, bonus)

    elif cycle_position == "mid_up":
        # 底部回升期：基本面改善确认
        bonus = 4.0
        message = f"周期回升期+{bonus:.0f}分(景气回升趋势确立)"
        return RuleResult("cycle_recovery_bonus", "bonus", message, bonus)

    elif cycle_position == "top":
        # 周期顶部：警惕估值过高和均值回归
        penalty = 5.0
        message = f"周期顶部预警-{penalty:.0f}分(警惕景气回落)"
        return RuleResult("cycle_top_penalty", "penalty", message, penalty)

    elif cycle_position == "mid_down":
        # 顶部回落期：下行趋势确认
        penalty = 3.0
        message = f"周期回落期-{penalty:.0f}分(景气下行趋势)"
        return RuleResult("cycle_downturn_penalty", "penalty", message, penalty)

    return None


def rule_cycle_veto_override(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    """
    【周期底部否决豁免】周期底部放宽一票否决

    逻辑：对于已确认的周期股，如果当前处于周期底部且正在回升，
    则即使基本面数据触发了否决条件，也应该给予豁免机会。

    这是一个"否决豁免"规则，通过返回 bonus 来抵消之前的 veto。
    实际实现需要在规则引擎中特殊处理。

    注意：此规则应该在所有否决规则之后运行。
    """
    if not context.is_cyclical:
        return None

    # 必须正在回升
    if context.current_phase != "rising":
        return None

    # 直接从 context 获取 cycle_position（更专业的方式）
    cycle_position = context.cycle_position

    if cycle_position not in ("bottom", "mid_up"):
        return None

    # 周期底部回升：标记为"否决豁免候选"
    # 注意：这不会直接取消否决，而是在引擎层面判断
    bonus = 10.0  # 足够大的加分，可能抵消某些扣分
    message = f"周期底部豁免候选+{bonus:.0f}分(周期股底部回升,建议人工复核)"
    return RuleResult("cycle_veto_override_candidate", "bonus", message, bonus)


# ============================================================================
# 新增专业规则：自由现金流质量验证
# ============================================================================

def rule_fcf_quality_check(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    """
    【自由现金流质量】验证盈利的现金含量

    自由现金流 ≈ 经营现金流 - 资本开支

    逻辑：
    1. 利润增长但OCF停滞 = 应收账款堆积风险
    2. OCF连续多年为负 = 商业模式存疑
    3. OCF/净利润 < 70% = 盈利质量较低

    这是对 rule_earnings_quality_check 的补充，更严格的现金流检验。
    """
    # 获取现金流数据
    ocf_stats = _get_reference_metric(context, "ocfps")
    if not ocf_stats:
        return None

    ocf_latest = ocf_stats.get("latest", 0.0)
    ocf_slope = ocf_stats.get("log_slope", 0.0)
    ocf_weighted = ocf_stats.get("weighted_avg", 0.0)

    # 情景1: 长期现金流为负（累计加权为负）
    if ocf_weighted < 0:
        penalty = 12.0
        message = f"现金流长期为负-{penalty:.0f}分(OCF加权={ocf_weighted:.2f})"
        return RuleResult("fcf_chronic_negative", "penalty", message, penalty)

    # 情景2: 现金流恶化趋势（斜率 < -15%）
    if ocf_slope < -0.15:
        penalty = min(abs(ocf_slope) * 30, 10.0)
        message = f"现金流恶化趋势-{penalty:.1f}分(OCF斜率={ocf_slope:.1%})"
        return RuleResult("fcf_deteriorating", "penalty", message, penalty)

    # 情景3: 最新现金流转负
    if ocf_latest < 0 and ocf_weighted > 0:
        penalty = 8.0
        message = f"现金流转负预警-{penalty:.0f}分(最新OCF={ocf_latest:.2f})"
        return RuleResult("fcf_turned_negative", "penalty", message, penalty)

    # 情景4: 现金流与利润背离（如果分析的是利润相关指标）
    if "profit" in context.metric_name.lower() or "eps" in context.metric_name.lower():
        profit_slope = context.log_slope
        if profit_slope > 0.10 and ocf_slope < 0:
            gap = profit_slope - ocf_slope
            penalty = min(gap * 20, 15.0)
            message = f"利润现金流背离-{penalty:.1f}分(利润↑{profit_slope:.1%} vs OCF↓{ocf_slope:.1%})"
            return RuleResult("fcf_profit_divergence", "penalty", message, penalty)

    return None


def rule_capex_intensity_check(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    """
    【资本开支强度】检测重资产扩张风险

    逻辑：
    如果ROIC在下降但公司仍在大幅扩张（OCF用于资本开支），
    可能是"增长陷阱"——资本回报率下降但仍在烧钱扩张。

    触发条件：分析 ROIC 且 ROIC 下降 且 有参考 OCF 数据
    """
    if "roic" not in context.metric_name.lower():
        return None

    if context.log_slope >= 0:
        return None  # ROIC 没有下降

    ocf_stats = _get_reference_metric(context, "ocfps")
    if not ocf_stats:
        return None

    ocf_latest = ocf_stats.get("latest", 0.0)
    ocf_slope = ocf_stats.get("log_slope", 0.0)

    # 如果 ROIC 下降 且 OCF 也在下降 = 可能是扩张过度
    if context.log_slope < -0.10 and ocf_slope < -0.10:
        penalty = 6.0
        message = f"扩张效率下降-{penalty:.0f}分(ROIC↓{context.log_slope:.1%}且OCF↓{ocf_slope:.1%})"
        return RuleResult("capex_efficiency_decline", "penalty", message, penalty)

    return None


# ============================================================================
# 新增专业规则：爆发增长验证
# ============================================================================

def rule_explosive_growth_validation(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    """
    【爆发增长验证】高增长的可持续性检验

    逻辑：
    如果某指标爆发增长(>30%)，需要交叉验证：
    1. 营收增长要有利润跟随
    2. 利润增长要有现金流支撑
    3. ROE增长不能只靠杠杆

    防止虚假繁荣。
    """
    # 只对高增长情况触发
    if context.log_slope < 0.25:
        return None

    metric_lower = context.metric_name.lower()

    # 营收爆发：检查利润是否跟上
    if "revenue" in metric_lower:
        profit_stats = _get_reference_metric(context, "eps")
        if profit_stats:
            profit_slope = profit_stats.get("log_slope", 0.0)
            if profit_slope < context.log_slope * 0.5:  # 利润增速不到营收增速的一半
                penalty = 5.0
                message = f"增收不增利-{penalty:.0f}分(营收↑{context.log_slope:.1%}但利润仅↑{profit_slope:.1%})"
                return RuleResult("revenue_profit_gap", "penalty", message, penalty)

    # 利润爆发：检查现金流
    if "profit" in metric_lower or "eps" in metric_lower:
        ocf_stats = _get_reference_metric(context, "ocfps")
        if ocf_stats:
            ocf_slope = ocf_stats.get("log_slope", 0.0)
            if ocf_slope < context.log_slope * 0.4:
                penalty = 6.0
                message = f"利润含金量不足-{penalty:.0f}分(利润↑{context.log_slope:.1%}但OCF仅↑{ocf_slope:.1%})"
                return RuleResult("profit_cash_quality", "penalty", message, penalty)

    return None


# ============================================================================
# 新增专业规则：基于贝叶斯概率和高级统计指标
# ============================================================================

def rule_bayesian_deterioration_alert(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    """
    【贝叶斯恶化预警】基于贝叶斯后验概率的恶化判断

    比传统规则更精确：综合考虑多个恶化信号的联合概率。

    触发条件：
    - deterioration_probability > 0.85: 高置信度恶化 → 严重扣分
    - deterioration_probability > 0.70: 中置信度恶化 → 中等扣分
    - deterioration_probability < 0.30: 低恶化概率 → 可能是误判，给予豁免加分
    """
    prob = context.deterioration_probability

    if prob is None or prob == 0.0:
        return None  # 无贝叶斯概率数据

    # 高置信度恶化（>85%）
    if prob > 0.85:
        # 如果传统检测也认为恶化，则加重处罚
        if context.has_deterioration and context.deterioration_severity == "severe":
            penalty = 12.0
            message = f"贝叶斯高置信度恶化-{penalty:.0f}分(恶化概率{prob:.1%}，严重恶化确认)"
            return RuleResult("bayesian_severe_deterioration", "penalty", message, penalty)
        else:
            penalty = 8.0
            message = f"贝叶斯恶化预警-{penalty:.0f}分(恶化概率{prob:.1%})"
            return RuleResult("bayesian_deterioration_warning", "penalty", message, penalty)

    # 中置信度恶化（70-85%）
    elif prob > 0.70:
        penalty = 5.0
        message = f"贝叶斯中度恶化预警-{penalty:.0f}分(恶化概率{prob:.1%})"
        return RuleResult("bayesian_moderate_deterioration", "penalty", message, penalty)

    # 低恶化概率（<30%）但传统检测报告恶化 → 可能是误判
    elif prob < 0.30 and context.has_deterioration:
        bonus = 3.0
        message = f"贝叶斯豁免+{bonus:.0f}分(恶化概率仅{prob:.1%}，传统检测可能误判)"
        return RuleResult("bayesian_false_positive_exempt", "bonus", message, bonus)

    return None


def rule_volatility_regime_adjustment(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    """
    【波动率体制调整】基于波动率变化趋势的风险调整

    - 波动率上升: 不确定性增加，降低置信度
    - 波动率下降: 趋势更可靠
    - ARCH效应: 波动聚集，可能有更大波动即将到来
    """
    vol_regime = context.volatility_regime
    vol_change = context.volatility_change_ratio
    has_arch = context.has_arch_effect

    if vol_regime is None:
        return None

    # 波动率显著上升 (>2倍)
    if vol_regime == "increasing_vol" and vol_change > 2.0:
        penalty = min(vol_change * 2, 8.0)
        message = f"波动率飙升预警-{penalty:.1f}分(近期波动是早期的{vol_change:.1f}倍)"
        return RuleResult("volatility_surge_penalty", "penalty", message, penalty)

    # ARCH效应 + 波动率上升 = 双重风险
    if has_arch and vol_regime == "increasing_vol":
        penalty = 6.0
        message = f"波动聚集风险-{penalty:.0f}分(ARCH效应+波动上升)"
        return RuleResult("arch_volatility_risk", "penalty", message, penalty)

    # 单独的ARCH效应（温和警告）
    if has_arch:
        penalty = 2.0
        message = f"波动聚集提示-{penalty:.0f}分(大波动后可能跟着大波动)"
        return RuleResult("arch_effect_warning", "penalty", message, penalty)

    # 波动率显著下降 + 趋势向好 = 可靠性加分
    if vol_regime == "decreasing_vol" and vol_change < 0.5 and context.log_slope > 0:
        bonus = 3.0
        message = f"波动收敛加分+{bonus:.0f}分(波动率下降,趋势更可靠)"
        return RuleResult("volatility_convergence_bonus", "bonus", message, bonus)

    return None


def rule_bootstrap_confidence_adjustment(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    """
    【Bootstrap置信度调整】基于斜率置信区间的判断修正

    Bootstrap CI 比 p-value 更可靠：
    - 如果CI跨越零，趋势方向不确定
    - 如果CI很窄，趋势判断更可靠
    - 如果CI很宽，需要谨慎解读
    """
    ci_low = context.bootstrap_ci_low
    ci_high = context.bootstrap_ci_high

    if ci_low is None or ci_high is None:
        return None

    ci_width = ci_high - ci_low
    slope = context.log_slope

    # 1. 置信区间跨越零：趋势方向不确定
    if ci_low < 0 < ci_high:
        # 如果传统检测认为是显著趋势，但CI跨零，则需要打折
        if context.r_squared > 0.5:
            penalty = 4.0
            message = f"趋势不确定预警-{penalty:.0f}分(Bootstrap CI跨零: [{ci_low:.3f}, {ci_high:.3f}])"
            return RuleResult("bootstrap_uncertain_trend", "penalty", message, penalty)

    # 2. CI非常窄（<0.05）且与趋势方向一致 → 高置信度
    if ci_width < 0.05:
        if slope > 0 and ci_low > 0.02:
            bonus = 3.0
            message = f"高置信度增长+{bonus:.0f}分(Bootstrap CI窄: [{ci_low:.3f}, {ci_high:.3f}])"
            return RuleResult("bootstrap_confident_growth", "bonus", message, bonus)
        elif slope < 0 and ci_high < -0.02:
            # 高置信度衰退，这是坏事，加重扣分
            penalty = 3.0
            message = f"高置信度衰退-{penalty:.0f}分(Bootstrap CI确认下行)"
            return RuleResult("bootstrap_confident_decline", "penalty", message, penalty)

    # 3. CI非常宽（>0.3）→ 数据质量差或波动太大
    if ci_width > 0.3:
        penalty = 2.0
        message = f"趋势不可靠-{penalty:.0f}分(Bootstrap CI过宽: {ci_width:.3f})"
        return RuleResult("bootstrap_unreliable_trend", "penalty", message, penalty)

    return None


def rule_wls_ols_divergence(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    """
    【WLS-OLS背离检测】加权与普通最小二乘的背离分析

    当WLS斜率与OLS斜率显著不同时，说明近期趋势与整体趋势有差异。
    这是一个重要的趋势转折信号。
    """
    wls_slope = context.wls_slope
    ols_slope = context.log_slope

    if wls_slope is None:
        return None

    diff = wls_slope - ols_slope

    # WLS更负（近期恶化加速）
    if diff < -0.05 and wls_slope < 0:
        penalty = min(abs(diff) * 30, 8.0)
        message = f"近期恶化加速-{penalty:.1f}分(WLS={wls_slope:.3f} < OLS={ols_slope:.3f})"
        return RuleResult("wls_recent_deterioration", "penalty", message, penalty)

    # WLS更正（近期改善）
    if diff > 0.05 and wls_slope > ols_slope:
        bonus = min(diff * 20, 6.0)
        message = f"近期改善信号+{bonus:.1f}分(WLS={wls_slope:.3f} > OLS={ols_slope:.3f})"
        return RuleResult("wls_recent_improvement", "bonus", message, bonus)

    # OLS显示衰退但WLS显示稳定/改善 → 困境反转信号
    if ols_slope < -0.05 and wls_slope > -0.02:
        bonus = 4.0
        message = f"困境反转信号+{bonus:.0f}分(整体下滑但近期企稳)"
        return RuleResult("wls_turnaround_signal", "bonus", message, bonus)

    return None


def rule_chronic_decline_pattern(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    """
    【慢性衰退模式识别】基于恶化模式的精细化处理

    不同的恶化模式有不同的投资含义：
    - accelerating_decline: 最危险，加速下滑
    - chronic_decline: 长期阴跌，商业模式可能有问题
    - cliff_drop: 单次暴跌，可能是事件驱动
    - grinding_decline: 缓慢侵蚀，需要警惕
    - high_level_pullback: 高位回调，可能是正常波动
    """
    pattern = context.deterioration_pattern

    if pattern is None or pattern == "none":
        return None

    if pattern == "accelerating_decline":
        # 加速下滑是最危险的模式
        penalty = 10.0
        message = f"加速下滑模式-{penalty:.0f}分(恶化速度越来越快)"
        return RuleResult("accelerating_decline_pattern", "penalty", message, penalty)

    elif pattern == "chronic_decline":
        # 慢性衰退表明结构性问题
        penalty = 8.0
        message = f"慢性衰退模式-{penalty:.0f}分(连续多年下跌，可能有结构性问题)"
        return RuleResult("chronic_decline_pattern", "penalty", message, penalty)

    elif pattern == "cliff_drop":
        # 断崖式下跌可能是事件驱动，需要具体分析
        penalty = 6.0
        message = f"断崖式下跌-{penalty:.0f}分(单年暴跌，关注是否为一次性事件)"
        return RuleResult("cliff_drop_pattern", "penalty", message, penalty)

    elif pattern == "grinding_decline":
        # 阴跌，温水煮青蛙
        penalty = 5.0
        message = f"阴跌模式-{penalty:.0f}分(缓慢侵蚀，需警惕)"
        return RuleResult("grinding_decline_pattern", "penalty", message, penalty)

    elif pattern == "high_level_pullback":
        # 高位回调通常是正常的
        # 如果当前值仍然很高，可以豁免
        if context.latest_value > (thresholds.min_latest_value or 10) * 1.5:
            bonus = 2.0
            message = f"高位正常回调+{bonus:.0f}分(绝对值仍处高位)"
            return RuleResult("high_level_pullback_exempt", "bonus", message, bonus)

    return None


# ============================================================================
# 改进规则 v2.1: 峰值跌幅、智能连续下跌、绝对水平保护
# ============================================================================

def rule_peak_decline_severe(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    """
    【峰值跌幅规则】检测从历史峰值的累计大幅下跌

    解决问题：义翘神州(155% -> 1.78%)这类案例，虽然最后一年微涨，
    但累计跌幅巨大，应该被严重扣分甚至否决。

    触发条件：
    - 从峰值跌幅 > 70%: 一票否决
    - 从峰值跌幅 > 50%: 严重扣分 (-15分)
    - 从峰值跌幅 > 30%: 中等扣分 (-8分)
    """
    peak_value = context.max_value
    latest_value = context.latest_value

    if peak_value is None or latest_value is None:
        return None

    if peak_value <= 0:
        return None

    # 计算从峰值的跌幅百分比
    peak_decline_pct = ((latest_value - peak_value) / peak_value) * 100

    # 如果是上涨或轻微下跌，不处理
    if peak_decline_pct > -30:
        return None

    # 从峰值跌幅超过70% - 一票否决
    if peak_decline_pct < -70:
        message = f"峰值暴跌否决(从{peak_value:.1f}跌至{latest_value:.1f}，跌幅{peak_decline_pct:.1f}%)"
        return RuleResult("peak_decline_veto", "veto", message, 0.0)

    # 从峰值跌幅超过50% - 严重扣分
    if peak_decline_pct < -50:
        penalty = 15.0
        message = f"峰值大幅下跌-{penalty:.0f}分(从{peak_value:.1f}跌至{latest_value:.1f}，跌幅{peak_decline_pct:.1f}%)"
        return RuleResult("peak_decline_severe", "penalty", message, penalty)

    # 从峰值跌幅超过30% - 中等扣分
    penalty = 8.0
    message = f"峰值明显下跌-{penalty:.0f}分(从{peak_value:.1f}跌至{latest_value:.1f}，跌幅{peak_decline_pct:.1f}%)"
    return RuleResult("peak_decline_moderate", "penalty", message, penalty)


def rule_smart_consecutive_decline(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    """
    【智能连续下跌检测】改进的连续下跌计数

    解决问题：创耀科技最后一年+0.07微涨，打断了连续下跌计数，
    导致规则失效。

    改进：微小反弹(涨幅<2%)不应重置连续下跌计数。
    使用"实质性下跌"的概念而非简单的同比下跌。
    """
    values = context.raw_values

    if values is None or len(values) < 3:
        return None

    # 智能连续下跌计数：微小反弹(<2%)不打断连续下跌
    MICRO_BOUNCE_THRESHOLD = 2.0  # 涨幅小于2%视为无实质性反弹

    smart_consecutive = 0
    cumulative_decline = 0.0

    for i in range(len(values) - 1, 0, -1):
        current = values[i]
        previous = values[i - 1]

        if previous == 0:
            continue

        pct_change = ((current - previous) / abs(previous)) * 100

        # 实质性下跌：跌幅超过2%
        if pct_change < -2.0:
            smart_consecutive += 1
            cumulative_decline += pct_change
        # 微小反弹：涨幅小于2%，不打断连续计数，但也不累加
        elif pct_change < MICRO_BOUNCE_THRESHOLD:
            # 继续计数，但不增加连续年数（保持当前计数）
            pass
        else:
            # 实质性反弹，停止计数
            break

    # 计算累计跌幅（从起点到终点）
    if values[0] != 0:
        total_decline_pct = ((values[-1] - values[0]) / abs(values[0])) * 100
    else:
        total_decline_pct = 0

    # 智能连续下跌 >= 3年 且 累计跌幅显著
    if smart_consecutive >= 3 and total_decline_pct < -30:
        penalty = min(smart_consecutive * 4, 16.0)
        message = f"智能连续下跌-{penalty:.0f}分({smart_consecutive}年实质性下跌，累计跌幅{total_decline_pct:.1f}%)"
        return RuleResult("smart_consecutive_severe", "penalty", message, penalty)

    # 智能连续下跌 >= 2年
    if smart_consecutive >= 2 and total_decline_pct < -20:
        penalty = smart_consecutive * 3.0
        message = f"连续下跌警示-{penalty:.0f}分({smart_consecutive}年下跌，累计{total_decline_pct:.1f}%)"
        return RuleResult("smart_consecutive_warning", "penalty", message, penalty)

    return None


def rule_absolute_level_protection(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    """
    【绝对水平保护】对高ROIC水平给予保护性加分

    解决问题：华特达因ROIC仍有22%，远超平均水平，
    但因近期下跌被降级到C。应该考虑绝对水平。

    逻辑：
    - ROIC > 25%: 优质资产保护 +5分
    - ROIC > 20%: 良好资产保护 +3分
    - ROIC > 15%: 合格资产保护 +2分

    但如果正在加速恶化，保护减半。
    """
    latest = context.latest_value

    if latest is None:
        return None

    # 获取恶化状态（如果正在加速恶化，保护减半）
    is_accelerating = False
    if context.deterioration_pattern in ("accelerating_decline", "cliff_drop"):
        is_accelerating = True

    protection_factor = 0.5 if is_accelerating else 1.0

    # 优质资产 (ROIC > 25%)
    if latest > 25:
        bonus = 5.0 * protection_factor
        if is_accelerating:
            message = f"优质资产保护+{bonus:.1f}分(ROIC={latest:.1f}%仍优秀，但恶化中减半)"
        else:
            message = f"优质资产保护+{bonus:.0f}分(ROIC={latest:.1f}%属优质资产)"
        return RuleResult("excellent_asset_protection", "bonus", message, bonus)

    # 良好资产 (ROIC > 20%)
    if latest > 20:
        bonus = 3.0 * protection_factor
        if is_accelerating:
            message = f"良好资产保护+{bonus:.1f}分(ROIC={latest:.1f}%良好，但恶化中减半)"
        else:
            message = f"良好资产保护+{bonus:.0f}分(ROIC={latest:.1f}%属良好资产)"
        return RuleResult("good_asset_protection", "bonus", message, bonus)

    # 合格资产 (ROIC > 15%)
    if latest > 15:
        bonus = 2.0 * protection_factor
        if is_accelerating:
            message = f"合格资产保护+{bonus:.1f}分(ROIC={latest:.1f}%合格，但恶化中减半)"
        else:
            message = f"合格资产保护+{bonus:.0f}分(ROIC={latest:.1f}%属合格资产)"
        return RuleResult("fair_asset_protection", "bonus", message, bonus)

    return None


def rule_cumulative_decline_veto(context: TrendContext, params: TrendRuleParameters, thresholds: TrendThresholds) -> Optional[RuleResult]:
    """
    【累计跌幅否决】当ROIC从高位跌到低位时的否决规则

    专门处理：ROIC从高位(如155%)跌到低位(如1.78%)的情况
    即使不是连续下跌，这种累计恶化也应该被否决。
    """
    values = context.raw_values
    latest = context.latest_value

    if values is None or len(values) < 3 or latest is None:
        return None

    # 找到历史最高值
    max_val = max(values)

    if max_val <= 0:
        return None

    # 计算从最高值的跌幅
    decline_from_max = ((latest - max_val) / max_val) * 100

    # 条件：从高位大幅跌落到低位
    # 1. 历史最高 > 30% (曾经是优质资产)
    # 2. 当前 < 5% (已经变成劣质资产)
    # 3. 跌幅 > 80%
    if max_val > 30 and latest < 5 and decline_from_max < -80:
        message = f"累计崩塌否决(曾达{max_val:.1f}%，现仅{latest:.1f}%，跌幅{decline_from_max:.1f}%)"
        return RuleResult("cumulative_collapse_veto", "veto", message, 0.0)

    # 较温和的情况：曾经优质，现在平庸
    if max_val > 25 and latest < 10 and decline_from_max < -60:
        penalty = 12.0
        message = f"品质退化严重-{penalty:.0f}分(曾达{max_val:.1f}%，现仅{latest:.1f}%)"
        return RuleResult("quality_degradation_severe", "penalty", message, penalty)

    return None
