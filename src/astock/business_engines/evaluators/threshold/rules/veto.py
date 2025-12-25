"""
否决规则 (Veto Rules)
=====================

一票否决规则：触发任一条件即失败。

规则清单 (6个):
1. rule_min_latest_value_veto - 最低值否决
2. rule_severe_trend_decline_veto - 严重趋势衰退 (整合原 severe_decline + structural_decline)
3. rule_severe_deterioration_veto - 严重恶化 (整合原 severe_deterioration + compound)
4. rule_peak_decline_veto - 峰值暴跌否决
5. rule_cumulative_collapse_veto - 累计崩塌否决
6. rule_roiic_capital_destruction_veto - ROIIC资本毁灭
"""

from typing import Optional, List
import numpy as np

from .base import (
    RuleResult, Rule, TrendContext, RuleConfig, RuleCategory,
    is_roiic_metric, get_reference_metric,
    is_cyclical_exemption, is_turnaround_exemption, logger
)


# ============================================================================
# 1. 最低值否决
# ============================================================================

def rule_min_latest_value_veto(context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
    """
    最低值否决规则

    触发条件:
    - 最新值低于配置的最低要求
    - 连续亏损 (3年以上)
    - 断崖式下跌 (跌幅超过阈值)

    豁免条件:
    - 困境反转/高成长: 最新值达到门槛60%且趋势强劲
    - 周期股底部回升期
    """
    # 如果没有设置最低值要求，跳过
    min_latest = getattr(context, 'min_latest_value', None)
    if min_latest is None:
        return None

    latest = context.latest_value

    # === 豁免检查 ===

    # 困境反转豁免: 最新值达到门槛60%且形态为反转
    if latest >= min_latest * 0.6:
        if is_turnaround_exemption(context):
            logger.info(f"🚀 困境反转豁免: {context.group_key} 最新={latest:.2f}")
            return None

    # 周期底部豁免
    if is_cyclical_exemption(context):
        logger.info(f"🛡️ 周期底部豁免: {context.group_key}")
        return None

    # 达标则跳过
    if latest >= min_latest:
        return None

    # === 否决条件 ===

    # 连续亏损否决: 最新值为负且连续亏损3年以上
    if latest < 0 and context.has_loss_years and context.loss_year_count >= 3:
        return RuleResult.veto(
            "min_latest_value_loss_veto",
            f"连续亏损-最新{context.metric_name}={latest:.2f}, 亏损{context.loss_year_count}年"
        )

    # 断崖式下跌否决: 累计跌幅超过阈值
    decline_limit = config.veto.cumulative_decline_cyclical_pct if context.is_cyclical else config.veto.cumulative_decline_pct
    if context.total_decline_pct >= decline_limit:
        return RuleResult.veto(
            "min_latest_value_decline_veto",
            f"断崖式恶化-总跌幅{context.total_decline_pct:.1f}%≥{decline_limit}%"
        )

    # 未达否决条件，返回 None (交给扣分规则处理)
    return None


# ============================================================================
# 2. 严重趋势衰退否决 (整合 severe_decline + structural_decline)
# ============================================================================

def rule_severe_trend_decline_veto(context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
    """
    严重趋势衰退否决

    整合原来的:
    - rule_severe_decline
    - rule_structural_decline_veto
    - rule_low_significance_decline
    - rule_high_volatility_instability

    触发条件 (满足任一):
    A. 严重衰退: log_slope < severe_decline 且 R² > 阈值 且 最新值低于门槛
    B. 结构性衰退: 斜率恶化 + 持续下跌 + 最新值/加权<85% + 总跌幅>25%

    豁免条件:
    - 周期股谷底/回升期
    - 稳健斜率 (Theil-Sen) 未显示衰退
    """
    veto = config.veto

    # === 周期底部豁免 ===
    if is_cyclical_exemption(context):
        logger.info(f"🛡️ 周期底部豁免(趋势衰退): {context.group_key}")
        return None

    # === 条件A: 严重衰退 ===
    severe_decline = (
        context.log_slope < veto.severe_decline_slope and
        context.r_squared > veto.severe_decline_r2_min
    )

    if severe_decline:
        # 稳健性豁免: 如果 Theil-Sen 斜率明显优于 OLS
        if context.robust_slope is not None and not np.isnan(context.robust_slope):
            if context.robust_slope > veto.severe_decline_slope:
                slope_diff = abs(context.robust_slope - context.log_slope)
                if slope_diff > 0.1:
                    logger.info(f"🛡️ 稳健性豁免: OLS={context.log_slope:.3f}, Robust={context.robust_slope:.3f}")
                    return None

        return RuleResult.veto(
            "severe_trend_decline_veto",
            f"严重衰退-对数斜率={context.log_slope:.3f}, CAGR≈{context.cagr_approx*100:.1f}%, R²={context.r_squared:.2f}"
        )

    # === 条件B: 结构性衰退 ===
    mild_decline = veto.severe_decline_slope * 0.3  # 使用较宽松的阈值
    structural_decline = (
        context.log_slope <= mild_decline and
        context.recent_3y_slope <= -0.05 and
        context.latest_vs_weighted_ratio < 0.85 and
        context.total_decline_pct >= 25 and
        context.r_squared >= veto.severe_decline_r2_min
    )

    if structural_decline:
        # 如果趋势加速度为正且近期斜率改善，不否决
        if context.trend_acceleration > -0.05 and context.recent_3y_slope > -0.02:
            return None

        return RuleResult.veto(
            "structural_decline_veto",
            f"结构性衰退-斜率{context.log_slope:.3f}, 近3年{context.recent_3y_slope:.3f}, 最新/加权{context.latest_vs_weighted_ratio:.1%}"
        )

    return None


# ============================================================================
# 3. 严重恶化否决 (整合 severe_deterioration + compound)
# ============================================================================

def rule_severe_deterioration_veto(context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
    """
    严重恶化否决

    整合原来的:
    - rule_severe_deterioration_veto
    - rule_compound_recent_deterioration

    触发条件:
    A. 严重恶化: deterioration_severity == "severe" 且 (跌幅>40% 或 最新/加权<70%)
    B. 复合恶化: 多个恶化信号同时触发 (趋势反转+加速下滑+大幅回撤)

    豁免条件:
    - 周期股谷底/回升期
    """
    veto = config.veto

    # === 周期底部豁免 ===
    if is_cyclical_exemption(context):
        logger.info(f"🛡️ 周期底部豁免(恶化): {context.group_key}")
        return None

    # === 条件A: 严重恶化 ===
    if context.deterioration_severity == "severe":
        if context.total_decline_pct > veto.deterioration_decline_pct:
            return RuleResult.veto(
                "severe_deterioration_veto",
                f"严重恶化-跌幅{context.total_decline_pct:.1f}%>{veto.deterioration_decline_pct}%"
            )

        if context.latest_vs_weighted_ratio < veto.deterioration_ratio:
            return RuleResult.veto(
                "severe_deterioration_veto",
                f"严重恶化-最新仅为加权{context.latest_vs_weighted_ratio:.1%}<{veto.deterioration_ratio:.0%}"
            )

    # === 条件B: 复合恶化 ===
    if context.has_deterioration and context.deterioration_severity != "none":
        negative_signals = 0

        if context.inflection_type == "growth_to_decline":
            negative_signals += 1
        if context.is_decelerating and context.recent_3y_slope < 0:
            negative_signals += 1
        if context.log_slope < config.penalty.mild_decline_slope:
            negative_signals += 1
        if context.latest_vs_weighted_ratio < 0.75:
            negative_signals += 1

        # 复合恶化否决: 3个以上恶化信号 + 严重恶化 + 大幅跌幅
        if negative_signals >= 3 and context.deterioration_severity == "severe" and context.total_decline_pct >= 35:
            return RuleResult.veto(
                "compound_deterioration_veto",
                f"复合恶化-{negative_signals}项信号同时触发"
            )

    return None


# ============================================================================
# 4. 峰值暴跌否决 (整合 peak_decline_severe)
# ============================================================================

def rule_peak_decline_veto(context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
    """
    峰值暴跌否决

    从历史峰值的大幅下跌

    触发条件:
    - 从峰值跌幅超过阈值 (默认70%，周期股80%)

    解决问题:
    - 义翘神州 155% -> 1.78% 这类情况
    """
    veto = config.veto

    peak_value = context.max_value
    latest_value = context.latest_value

    if peak_value is None or latest_value is None or peak_value <= 0:
        return None

    # 计算从峰值的跌幅
    decline_pct = ((peak_value - latest_value) / peak_value) * 100

    # 根据是否周期股选择阈值
    threshold = veto.peak_decline_cyclical_pct if context.is_cyclical else veto.peak_decline_pct

    if decline_pct >= threshold:
        return RuleResult.veto(
            "peak_decline_veto",
            f"峰值暴跌-从{peak_value:.1f}跌至{latest_value:.1f}，跌幅{decline_pct:.1f}%≥{threshold}%"
        )

    return None


# ============================================================================
# 5. 累计崩塌否决 (整合 cumulative_decline_veto)
# ============================================================================

def rule_cumulative_collapse_veto(context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
    """
    累计崩塌否决

    曾经是优质资产 (高ROIC) 但现在已变成劣质资产的情况

    触发条件:
    - 历史最高 > 30% (曾经优质)
    - 当前 < 5% (已经劣质)
    - 跌幅 > 80%
    """
    veto = config.veto

    values = context.raw_values
    latest = context.latest_value

    if values is None or len(values) < 3 or latest is None:
        return None

    max_val = max(values)
    if max_val <= 0:
        return None

    # 计算跌幅
    decline_pct = ((max_val - latest) / max_val) * 100

    # 崩塌条件: 曾经优质 + 现在劣质 + 大幅下跌
    if (max_val > veto.collapse_max_threshold and
        latest < veto.collapse_latest_threshold and
        decline_pct > veto.collapse_decline_pct):
        return RuleResult.veto(
            "cumulative_collapse_veto",
            f"累计崩塌-曾达{max_val:.1f}%，现仅{latest:.1f}%，跌幅{decline_pct:.1f}%"
        )

    return None


# ============================================================================
# 6. ROIIC 资本毁灭否决
# ============================================================================

def rule_roiic_capital_destruction_veto(context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
    """
    ROIIC 资本毁灭否决

    ROIIC 持续为负表示新增投资在毁灭价值

    触发条件 (同时满足):
    - 是 ROIIC 指标
    - 加权平均 < -20%
    - 最新值 < -10%
    - 趋势显著 (R² > 0.4)
    - ROIC 也在恶化
    - 恶化程度为 severe 或 moderate
    """
    if not is_roiic_metric(context):
        return None

    veto = config.veto

    # 基本条件检查
    if context.weighted_avg > veto.roiic_weighted_threshold:
        return None
    if context.latest_value > veto.roiic_latest_threshold:
        return None
    if context.log_slope > veto.severe_decline_slope:
        return None
    if context.r_squared < max(veto.severe_decline_r2_min, 0.4):
        return None

    # ROIC 交叉验证
    roic_stats = get_reference_metric(context, "roic")
    roic_flag = False

    if roic_stats:
        roic_latest = roic_stats.get("latest")
        roic_slope = roic_stats.get("log_slope")
        roic_recent = roic_stats.get("recent_3y_slope")

        # ROIC 也在恶化
        if roic_latest is not None and roic_latest < 8.0:
            roic_flag = True
        if roic_slope is not None and roic_slope <= config.penalty.mild_decline_slope:
            roic_flag = True
        if roic_recent is not None and roic_recent < 0:
            roic_flag = True
    else:
        roic_flag = True  # 没有 ROIC 数据也算风险

    # 恶化程度检查
    deterioration_flag = (
        context.deterioration_severity in {"severe", "moderate"} or
        context.total_decline_pct >= 40
    )

    if roic_flag and deterioration_flag:
        return RuleResult.veto(
            "roiic_capital_destruction_veto",
            f"ROIIC资本毁灭-加权{context.weighted_avg:.1f}%, 最新{context.latest_value:.1f}%"
        )

    return None


# ============================================================================
# 否决规则列表
# ============================================================================

VETO_RULES: List[Rule] = [
    Rule("min_latest_value_veto", RuleCategory.VETO, rule_min_latest_value_veto,
         "最低值否决", priority=10),
    Rule("cumulative_collapse_veto", RuleCategory.VETO, rule_cumulative_collapse_veto,
         "累计崩塌否决", priority=15),
    Rule("peak_decline_veto", RuleCategory.VETO, rule_peak_decline_veto,
         "峰值暴跌否决", priority=20),
    Rule("severe_trend_decline_veto", RuleCategory.VETO, rule_severe_trend_decline_veto,
         "严重趋势衰退", priority=30),
    Rule("severe_deterioration_veto", RuleCategory.VETO, rule_severe_deterioration_veto,
         "严重恶化否决", priority=40),
    Rule("roiic_capital_destruction_veto", RuleCategory.VETO, rule_roiic_capital_destruction_veto,
         "ROIIC资本毁灭", priority=50),
]


__all__ = [
    'rule_min_latest_value_veto',
    'rule_severe_trend_decline_veto',
    'rule_severe_deterioration_veto',
    'rule_peak_decline_veto',
    'rule_cumulative_collapse_veto',
    'rule_roiic_capital_destruction_veto',
    'VETO_RULES',
]
