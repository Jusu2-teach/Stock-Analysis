"""
交叉验证规则 (Validation Rules)
================================

交叉验证规则：通过多指标交叉验证检测数据质量和一致性问题。

规则清单 (4个):
1. rule_earnings_quality_check - 盈利质量检验 (利润 vs 现金流)
2. rule_dupont_consistency_check - 杜邦分解一致性
3. rule_fcf_quality_check - 自由现金流质量
4. rule_sustainable_growth_check - 可持续增长检验
"""

from typing import Optional, List

from .base import (
    RuleResult, Rule, TrendContext, RuleConfig, RuleCategory,
    get_reference_metric, logger
)


# ============================================================================
# 1. 盈利质量检验
# ============================================================================

def rule_earnings_quality_check(context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
    """
    盈利质量检验

    整合原来的:
    - rule_earnings_quality_divergence
    - rule_explosive_growth_validation (利润部分)

    检测利润与现金流的背离

    触发条件:
    - 利润类指标
    - 利润高增但现金流恶化
    - 利润增速远超现金流
    """
    validation = config.validation

    # 只在分析利润类指标时触发
    metric_lower = context.metric_name.lower()
    if "profit" not in metric_lower and "eps" not in metric_lower:
        return None

    # 获取现金流数据
    ocf_stats = get_reference_metric(context, "ocfps")
    if not ocf_stats:
        return None

    profit_slope = context.log_slope
    ocf_slope = ocf_stats.get("log_slope", 0.0)

    # === 剪刀差风险: 利润向上，现金流向下 ===
    if (profit_slope > validation.profit_positive_threshold and
        ocf_slope < validation.ocf_negative_threshold):
        return RuleResult.penalty(
            "earnings_quality_divergence",
            f"盈利质量预警-{validation.earnings_quality_penalty:.0f}分(利润↑{profit_slope:.1%}但现金流↓{ocf_slope:.1%})",
            validation.earnings_quality_penalty
        )

    # === 利润含金量不足: 利润增速远超现金流 ===
    gap = profit_slope - ocf_slope
    if gap > validation.profit_ocf_divergence:
        penalty = min(gap * 30, validation.earnings_quality_penalty)
        return RuleResult.penalty(
            "profit_cash_gap",
            f"现金流跟不上利润-{penalty:.1f}分(差距{gap:.1%})",
            penalty
        )

    # === 爆发增长验证: 高增长但现金流不佳 ===
    if profit_slope > 0.25 and ocf_slope < profit_slope * 0.4:
        return RuleResult.penalty(
            "profit_cash_quality",
            f"利润含金量不足-6分(利润↑{profit_slope:.1%}但OCF仅↑{ocf_slope:.1%})",
            6.0
        )

    return None


# ============================================================================
# 2. 杜邦分解一致性
# ============================================================================

def rule_dupont_consistency_check(context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
    """
    杜邦分解一致性检验

    ROE = 净利率 × 资产周转率 × 权益乘数

    检测 ROE 增长的真实来源

    触发条件:
    - ROE 类指标
    - ROE 上升但净利率下降 (杠杆驱动)
    - ROE 上升但毛利率下降 (价格战风险)
    """
    validation = config.validation

    if "roe" not in context.metric_name.lower():
        return None

    roe_slope = context.log_slope

    # ROE 没有上升，不检查
    if roe_slope < validation.dupont_roe_threshold:
        return None

    # === 检查净利率 ===
    nm_stats = get_reference_metric(context, "net_margin")
    if nm_stats:
        nm_slope = nm_stats.get("log_slope", 0.0)
        if nm_slope < validation.dupont_margin_threshold:
            penalty = min(abs(nm_slope - roe_slope) * 10, validation.dupont_penalty_max)
            return RuleResult.penalty(
                "dupont_leverage_risk",
                f"杜邦分解预警-{penalty:.1f}分(ROE↑{roe_slope:.1%}靠杠杆,净利率↓{nm_slope:.1%})",
                penalty
            )

    # === 检查毛利率 ===
    gm_stats = get_reference_metric(context, "gross_margin")
    if gm_stats:
        gm_slope = gm_stats.get("log_slope", 0.0)
        if gm_slope < -0.05:  # 毛利率下跌5%以上
            penalty = min(abs(gm_slope) * 8, 6.0)
            return RuleResult.penalty(
                "dupont_margin_erosion",
                f"毛利率侵蚀预警-{penalty:.1f}分(ROE增长可能靠降价换量,毛利↓{gm_slope:.1%})",
                penalty
            )

    return None


# ============================================================================
# 3. 自由现金流质量
# ============================================================================

def rule_fcf_quality_check(context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
    """
    自由现金流质量检验

    整合原来的:
    - rule_fcf_quality_check
    - rule_capex_intensity_check

    检测现金流健康状况

    触发条件:
    - 长期现金流为负
    - 现金流恶化趋势
    - 现金流转负
    - ROIC下降但仍在扩张
    """
    validation = config.validation

    ocf_stats = get_reference_metric(context, "ocfps")
    if not ocf_stats:
        return None

    ocf_latest = ocf_stats.get("latest", 0.0)
    ocf_slope = ocf_stats.get("log_slope", 0.0)
    ocf_weighted = ocf_stats.get("weighted_avg", 0.0)

    # === 长期现金流为负 ===
    if ocf_weighted < 0:
        return RuleResult.penalty(
            "fcf_chronic_negative",
            f"现金流长期为负-{validation.fcf_chronic_negative_penalty:.0f}分(OCF加权={ocf_weighted:.2f})",
            validation.fcf_chronic_negative_penalty
        )

    # === 现金流恶化趋势 ===
    if ocf_slope < validation.fcf_deteriorating_slope:
        penalty = min(abs(ocf_slope) * 30, validation.fcf_deteriorating_penalty_max)
        return RuleResult.penalty(
            "fcf_deteriorating",
            f"现金流恶化趋势-{penalty:.1f}分(OCF斜率={ocf_slope:.1%})",
            penalty
        )

    # === 现金流转负 ===
    if ocf_latest < 0 and ocf_weighted > 0:
        return RuleResult.penalty(
            "fcf_turned_negative",
            f"现金流转负预警-8分(最新OCF={ocf_latest:.2f})",
            8.0
        )

    # === ROIC 下降但仍在扩张 ===
    if "roic" in context.metric_name.lower():
        if context.log_slope < -0.10 and ocf_slope < -0.10:
            return RuleResult.penalty(
                "capex_efficiency_decline",
                f"扩张效率下降-6分(ROIC↓{context.log_slope:.1%}且OCF↓{ocf_slope:.1%})",
                6.0
            )

    return None


# ============================================================================
# 4. 可持续增长检验
# ============================================================================

def rule_sustainable_growth_check(context: TrendContext, config: RuleConfig) -> Optional[RuleResult]:
    """
    可持续增长检验

    整合原来的:
    - rule_sustainable_growth_check
    - rule_explosive_growth_validation (营收部分)

    检测增长的可持续性

    触发条件:
    - 营收高增但 ROE 很低 (低效扩张)
    - 营收爆发但利润未跟上 (增收不增利)
    """
    validation = config.validation

    metric_lower = context.metric_name.lower()

    # === 营收类指标检验 ===
    if "revenue" not in metric_lower:
        return None

    revenue_growth = context.cagr_approx

    # === 低效扩张: 营收高增但ROE很低 ===
    roe_stats = get_reference_metric(context, "roe")
    if roe_stats:
        roe_latest = roe_stats.get("latest", 0.0) / 100.0  # 假设ROE是百分比

        if revenue_growth > validation.sustainable_revenue_threshold and roe_latest < validation.sustainable_roe_threshold:
            return RuleResult.penalty(
                "unsustainable_growth",
                f"低效扩张风险-{validation.sustainable_growth_penalty:.0f}分(营收↑{revenue_growth:.1%}远超ROE{roe_latest:.1%})",
                validation.sustainable_growth_penalty
            )

    # === 增收不增利: 营收爆发但利润未跟上 ===
    if context.log_slope > 0.25:  # 营收高增长
        profit_stats = get_reference_metric(context, "eps")
        if profit_stats:
            profit_slope = profit_stats.get("log_slope", 0.0)
            if profit_slope < context.log_slope * 0.5:
                return RuleResult.penalty(
                    "revenue_profit_gap",
                    f"增收不增利-5分(营收↑{context.log_slope:.1%}但利润仅↑{profit_slope:.1%})",
                    5.0
                )

    return None


# ============================================================================
# 交叉验证规则列表
# ============================================================================

VALIDATION_RULES: List[Rule] = [
    Rule("earnings_quality_check", RuleCategory.VALIDATION, rule_earnings_quality_check,
         "盈利质量检验", priority=300),
    Rule("dupont_consistency_check", RuleCategory.VALIDATION, rule_dupont_consistency_check,
         "杜邦分解一致性", priority=310),
    Rule("fcf_quality_check", RuleCategory.VALIDATION, rule_fcf_quality_check,
         "自由现金流质量", priority=320),
    Rule("sustainable_growth_check", RuleCategory.VALIDATION, rule_sustainable_growth_check,
         "可持续增长检验", priority=330),
]


__all__ = [
    'rule_earnings_quality_check',
    'rule_dupont_consistency_check',
    'rule_fcf_quality_check',
    'rule_sustainable_growth_check',
    'VALIDATION_RULES',
]
