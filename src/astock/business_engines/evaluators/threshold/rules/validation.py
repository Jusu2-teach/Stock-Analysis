"""
交叉验证规则 v2.0 (Validation Rules - Refactored)
================================================

使用 Protocol-based 架构重构的交叉验证规则。

设计原则:
- 实现 RuleProtocol 接口
- 使用新的 domain_models.TrendContext
- 不可变规则配置 (frozen dataclass)
- 多指标交叉验证

规则清单 (4个):
1. EarningsQualityCheckRule - 盈利质量检验 (利润 vs 现金流)
2. DupontConsistencyCheckRule - 杜邦分解一致性
3. FCFQualityCheckRule - 自由现金流质量
4. SustainableGrowthCheckRule - 可持续增长检验

作者: AStock Analysis System (Refactored)
日期: 2026-01-10
版本: 2.0.0
"""

from typing import Optional
import logging

from ..domain_models import TrendContext
from ..protocols import RuleProtocol
from ..results import RuleResultImpl, create_penalty_result
from ..rule_config import RuleConfig, RuleCategory

logger = logging.getLogger(__name__)


# ============================================================================
# 辅助函数
# ============================================================================

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
# 1. 盈利质量检验规则
# ============================================================================

class EarningsQualityCheckRule:
    """
    盈利质量检验规则

    整合原来的:
    - rule_earnings_quality_divergence
    - rule_explosive_growth_validation (利润部分)

    检测利润与现金流的背离

    触发条件:
    - 利润类指标
    - 利润高增但现金流恶化 (剪刀差)
    - 利润增速远超现金流 (含金量不足)
    - 爆发增长但现金流跟不上

    Examples:
        >>> rule = EarningsQualityCheckRule()
        >>> result = rule.execute(context, config)
    """

    name: str = "earnings_quality_check"
    category: RuleCategory = RuleCategory.VALIDATION
    priority: int = 300
    enabled: bool = True
    description: str = "盈利质量检验"

    def execute(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> Optional[RuleResultImpl]:
        """执行规则"""
        validation = config.validation

        # 只在分析利润类指标时触发
        metric_lower = context.metric_name.lower()
        if "profit" not in metric_lower and "eps" not in metric_lower:
            return None

        # 获取现金流数据
        ocf_stats = get_reference_metric(context, "ocfps")
        if not ocf_stats:
            return None

        profit_slope = context.trend.log_slope
        ocf_slope = ocf_stats.get("log_slope", 0.0)

        # === 剪刀差风险: 利润向上，现金流向下 ===
        if (profit_slope > validation.profit_positive_threshold and
            ocf_slope < validation.ocf_negative_threshold):
            return create_penalty_result(
                "earnings_quality_divergence",
                f"盈利质量预警-{validation.earnings_quality_penalty:.0f}分"
                f"(利润↑{profit_slope:.1%}但现金流↓{ocf_slope:.1%})",
                validation.earnings_quality_penalty,
                metadata={
                    "profit_slope": profit_slope,
                    "ocf_slope": ocf_slope,
                }
            )

        # === 利润含金量不足: 利润增速远超现金流 ===
        gap = profit_slope - ocf_slope
        if gap > validation.profit_ocf_divergence:
            penalty = min(gap * 30, validation.earnings_quality_penalty)
            return create_penalty_result(
                "profit_cash_gap",
                f"现金流跟不上利润-{penalty:.1f}分(差距{gap:.1%})",
                penalty,
                metadata={
                    "gap": gap,
                    "profit_slope": profit_slope,
                    "ocf_slope": ocf_slope,
                }
            )

        # === 爆发增长验证: 高增长但现金流不佳 ===
        if profit_slope > 0.25 and ocf_slope < profit_slope * 0.4:
            return create_penalty_result(
                "profit_cash_quality",
                f"利润含金量不足-6分"
                f"(利润↑{profit_slope:.1%}但OCF仅↑{ocf_slope:.1%})",
                6.0,
                metadata={
                    "profit_slope": profit_slope,
                    "ocf_slope": ocf_slope,
                }
            )

        return None


# ============================================================================
# 2. 杜邦分解一致性检验规则
# ============================================================================

class DupontConsistencyCheckRule:
    """
    杜邦分解一致性检验规则

    ROE = 净利率 × 资产周转率 × 权益乘数

    检测ROE增长的真实来源

    触发条件:
    - ROE类指标
    - ROE上升但净利率下降 (杠杆驱动)
    - ROE上升但毛利率下降 (价格战风险)

    Examples:
        >>> rule = DupontConsistencyCheckRule()
        >>> result = rule.execute(context, config)
    """

    name: str = "dupont_consistency_check"
    category: RuleCategory = RuleCategory.VALIDATION
    priority: int = 310
    enabled: bool = True
    description: str = "杜邦分解一致性"

    def execute(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> Optional[RuleResultImpl]:
        """执行规则"""
        validation = config.validation

        if "roe" not in context.metric_name.lower():
            return None

        roe_slope = context.trend.log_slope

        # ROE没有上升，不检查
        if roe_slope < validation.dupont_roe_threshold:
            return None

        # === 检查净利率 ===
        nm_stats = get_reference_metric(context, "net_margin")
        if nm_stats:
            nm_slope = nm_stats.get("log_slope", 0.0)
            if nm_slope < validation.dupont_margin_threshold:
                penalty = min(
                    abs(nm_slope - roe_slope) * 10,
                    validation.dupont_penalty_max
                )
                return create_penalty_result(
                    "dupont_leverage_risk",
                    f"杜邦分解预警-{penalty:.1f}分"
                    f"(ROE↑{roe_slope:.1%}靠杠杆,净利率↓{nm_slope:.1%})",
                    penalty,
                    metadata={
                        "roe_slope": roe_slope,
                        "nm_slope": nm_slope,
                    }
                )

        # === 检查毛利率 ===
        gm_stats = get_reference_metric(context, "gross_margin")
        if gm_stats:
            gm_slope = gm_stats.get("log_slope", 0.0)
            if gm_slope < -0.05:  # 毛利率下跌5%以上
                penalty = min(abs(gm_slope) * 8, 6.0)
                return create_penalty_result(
                    "dupont_margin_erosion",
                    f"毛利率侵蚀预警-{penalty:.1f}分"
                    f"(ROE增长可能靠降价换量,毛利↓{gm_slope:.1%})",
                    penalty,
                    metadata={
                        "roe_slope": roe_slope,
                        "gm_slope": gm_slope,
                    }
                )

        return None


# ============================================================================
# 3. 自由现金流质量检验规则
# ============================================================================

class FCFQualityCheckRule:
    """
    自由现金流质量检验规则

    整合原来的:
    - rule_fcf_quality_check
    - rule_capex_intensity_check

    检测现金流健康状况

    触发条件:
    - 长期现金流为负
    - 现金流恶化趋势
    - 现金流转负
    - ROIC下降但仍在扩张

    Examples:
        >>> rule = FCFQualityCheckRule()
        >>> result = rule.execute(context, config)
    """

    name: str = "fcf_quality_check"
    category: RuleCategory = RuleCategory.VALIDATION
    priority: int = 320
    enabled: bool = True
    description: str = "自由现金流质量"

    def execute(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> Optional[RuleResultImpl]:
        """执行规则"""
        validation = config.validation

        ocf_stats = get_reference_metric(context, "ocfps")
        if not ocf_stats:
            return None

        ocf_latest = ocf_stats.get("latest", 0.0)
        ocf_slope = ocf_stats.get("log_slope", 0.0)
        ocf_weighted = ocf_stats.get("weighted_avg", 0.0)

        # === 长期现金流为负 ===
        if ocf_weighted is not None and ocf_weighted < 0:
            return create_penalty_result(
                "fcf_chronic_negative",
                f"现金流长期为负-{validation.fcf_chronic_negative_penalty:.0f}分"
                f"(OCF加权={ocf_weighted:.2f})",
                validation.fcf_chronic_negative_penalty,
                metadata={"ocf_weighted": ocf_weighted}
            )

        # === 现金流恶化趋势 ===
        if ocf_slope < validation.fcf_deteriorating_slope:
            penalty = min(
                abs(ocf_slope) * 30,
                validation.fcf_deteriorating_penalty_max
            )
            return create_penalty_result(
                "fcf_deteriorating",
                f"现金流恶化趋势-{penalty:.1f}分(OCF斜率={ocf_slope:.1%})",
                penalty,
                metadata={"ocf_slope": ocf_slope}
            )

        # === 现金流转负 ===
        if (ocf_latest < 0 and
            ocf_weighted is not None and ocf_weighted > 0):
            return create_penalty_result(
                "fcf_turned_negative",
                f"现金流转负预警-8分(最新OCF={ocf_latest:.2f})",
                8.0,
                metadata={
                    "ocf_latest": ocf_latest,
                    "ocf_weighted": ocf_weighted,
                }
            )

        # === ROIC下降但仍在扩张 ===
        if "roic" in context.metric_name.lower():
            if context.trend.log_slope < -0.10 and ocf_slope < -0.10:
                return create_penalty_result(
                    "capex_efficiency_decline",
                    f"扩张效率下降-6分"
                    f"(ROIC↓{context.trend.log_slope:.1%}且OCF↓{ocf_slope:.1%})",
                    6.0,
                    metadata={
                        "roic_slope": context.trend.log_slope,
                        "ocf_slope": ocf_slope,
                    }
                )

        return None


# ============================================================================
# 4. 可持续增长检验规则
# ============================================================================

class SustainableGrowthCheckRule:
    """
    可持续增长检验规则

    整合原来的:
    - rule_sustainable_growth_check
    - rule_explosive_growth_validation (营收部分)

    检测增长的可持续性

    触发条件:
    - 营收高增但ROE很低 (低效扩张)
    - 营收爆发但利润未跟上 (增收不增利)

    Examples:
        >>> rule = SustainableGrowthCheckRule()
        >>> result = rule.execute(context, config)
    """

    name: str = "sustainable_growth_check"
    category: RuleCategory = RuleCategory.VALIDATION
    priority: int = 330
    enabled: bool = True
    description: str = "可持续增长检验"

    def execute(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> Optional[RuleResultImpl]:
        """执行规则"""
        validation = config.validation

        metric_lower = context.metric_name.lower()

        # === 营收类指标检验 ===
        if "revenue" not in metric_lower:
            return None

        revenue_growth = context.trend.cagr_approx

        # === 低效扩张: 营收高增但ROE很低 ===
        roe_stats = get_reference_metric(context, "roe")
        if roe_stats:
            roe_latest = roe_stats.get("latest", 0.0)
            if roe_latest is not None:
                # 假设ROE可能是百分比或小数
                roe_value = roe_latest / 100.0 if roe_latest > 1 else roe_latest

                if (revenue_growth > validation.sustainable_revenue_threshold and
                    roe_value < validation.sustainable_roe_threshold):
                    return create_penalty_result(
                        "unsustainable_growth",
                        f"低效扩张风险-{validation.sustainable_growth_penalty:.0f}分"
                        f"(营收↑{revenue_growth:.1%}远超ROE{roe_value:.1%})",
                        validation.sustainable_growth_penalty,
                        metadata={
                            "revenue_growth": revenue_growth,
                            "roe": roe_value,
                        }
                    )

        # === 增收不增利: 营收爆发但利润未跟上 ===
        if context.trend.log_slope > 0.25:  # 营收高增长
            profit_stats = get_reference_metric(context, "eps")
            if profit_stats:
                profit_slope = profit_stats.get("log_slope", 0.0)
                if profit_slope < context.trend.log_slope * 0.5:
                    return create_penalty_result(
                        "revenue_profit_gap",
                        f"增收不增利-5分"
                        f"(营收↑{context.trend.log_slope:.1%}"
                        f"但利润仅↑{profit_slope:.1%})",
                        5.0,
                        metadata={
                            "revenue_slope": context.trend.log_slope,
                            "profit_slope": profit_slope,
                        }
                    )

        return None


# ============================================================================
# 规则工厂
# ============================================================================

def create_all_validation_rules() -> list[RuleProtocol]:
    """
    创建所有验证规则实例

    Returns:
        规则实例列表，按优先级排序
    """
    rules = [
        EarningsQualityCheckRule(),
        DupontConsistencyCheckRule(),
        FCFQualityCheckRule(),
        SustainableGrowthCheckRule(),
    ]

    # 按优先级排序
    return sorted(rules, key=lambda r: r.priority)


__all__ = [
    # 规则类
    'EarningsQualityCheckRule',
    'DupontConsistencyCheckRule',
    'FCFQualityCheckRule',
    'SustainableGrowthCheckRule',
    # 工厂函数
    'create_all_validation_rules',
    # 辅助函数
    'get_reference_metric',
]
