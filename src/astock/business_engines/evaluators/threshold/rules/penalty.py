"""
扣分规则 v2.0 (Penalty Rules - Refactored)
==========================================

使用 Protocol-based 架构重构的扣分规则。

设计原则:
- 实现 RuleProtocol 接口
- 使用新的 domain_models.TrendContext
- 不可变规则配置 (frozen dataclass)
- 清晰的业务语义

规则清单 (8个):
1. MildDeclinePenaltyRule - 轻度衰退扣分
2. DeteriorationPenaltyRule - 恶化程度扣分
3. VolatilityPenaltyRule - 高波动扣分
4. RelativeDeclinePenaltyRule - 相对跌幅扣分
5. SingleYearDeclinePenaltyRule - 单年暴跌扣分
6. ConsecutiveDeclinePenaltyRule - 连续下跌扣分
7. ROIICNegativePenaltyRule - ROIIC为负扣分
8. ROIICDivergencePenaltyRule - ROIIC/ROIC背离扣分

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
# 辅助函数 (从 veto.py 共享)
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
# 1. 轻度衰退扣分规则
# ============================================================================

class MildDeclinePenaltyRule:
    """
    轻度衰退扣分规则

    整合原来的:
    - rule_mild_decline_penalty
    - rule_sustained_decline

    触发条件:
    - log_slope < mild_decline_slope 且趋势显著

    加重条件:
    - 最新值低于加权平均 (持续衰退)

    Examples:
        >>> rule = MildDeclinePenaltyRule()
        >>> result = rule.execute(context, config)
    """

    name: str = "mild_decline_penalty"
    category: RuleCategory = RuleCategory.PENALTY
    priority: int = 100
    enabled: bool = True
    description: str = "轻度衰退扣分"

    def execute(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> Optional[RuleResultImpl]:
        """执行规则"""
        penalty_cfg = config.penalty
        veto_cfg = config.veto

        # 轻度衰退: 斜率为负但未达严重程度
        if context.trend.log_slope >= penalty_cfg.mild_decline_slope:
            return None

        # 趋势不显著时跳过
        if context.trend.r_squared < veto_cfg.severe_decline_r2_min:
            return None

        # 计算扣分
        penalty_factor = config.scoring.penalty_factor
        base_penalty = abs(context.trend.log_slope) * penalty_factor

        # 持续衰退加重: 最新值低于加权平均
        if context.quality.latest_value < context.quality.weighted_avg:
            base_penalty *= 1.3
            message = (
                f"持续衰退-{base_penalty:.1f}分"
                f"(斜率{context.trend.log_slope:.3f}, 最新<加权)"
            )
        else:
            message = f"轻度衰退-{base_penalty:.1f}分(斜率{context.trend.log_slope:.3f})"

        penalty = min(base_penalty, penalty_cfg.mild_decline_max_penalty)

        return create_penalty_result(
            "mild_decline_penalty",
            message,
            penalty,
            metadata={
                "log_slope": context.trend.log_slope,
                "latest_value": context.quality.latest_value,
                "weighted_avg": context.quality.weighted_avg,
            }
        )


# ============================================================================
# 2. 恶化程度扣分规则
# ============================================================================

class DeteriorationPenaltyRule:
    """
    恶化程度扣分规则

    整合原来的:
    - rule_deterioration_penalty
    - rule_bayesian_deterioration_alert (贝叶斯增强)
    - rule_chronic_decline_pattern (恶化模式)

    根据恶化严重程度和模式计算扣分

    加重因子:
    - 贝叶斯置信度 (0.7-1.3x)
    - 恶化模式 (0.8-1.4x)

    Examples:
        >>> rule = DeteriorationPenaltyRule()
        >>> result = rule.execute(context, config)
    """

    name: str = "deterioration_penalty"
    category: RuleCategory = RuleCategory.PENALTY
    priority: int = 110
    enabled: bool = True
    description: str = "恶化程度扣分"

    def execute(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> Optional[RuleResultImpl]:
        """执行规则"""
        from ..domain_models import DeteriorationSeverity

        if (not context.deterioration.has_deterioration or
            context.deterioration.severity == DeteriorationSeverity.NONE):
            return None

        penalty_cfg = config.penalty

        # 基础扣分
        base_penalties = {
            DeteriorationSeverity.SEVERE: penalty_cfg.deterioration_severe_penalty,
            DeteriorationSeverity.MODERATE: penalty_cfg.deterioration_moderate_penalty,
            DeteriorationSeverity.MILD: penalty_cfg.deterioration_mild_penalty,
        }

        base_penalty = base_penalties.get(context.deterioration.severity, 0)
        if base_penalty <= 0:
            return None

        # === 贝叶斯增强 ===
        bayesian_multiplier = 1.0
        deterioration_prob = getattr(context.deterioration, 'probability', 0.5)

        if deterioration_prob > 0.85:
            bayesian_multiplier = 1.3  # 高置信度恶化加重
        elif deterioration_prob > 0.70:
            bayesian_multiplier = 1.15
        elif deterioration_prob < 0.30 and context.deterioration.has_deterioration:
            bayesian_multiplier = 0.7  # 低概率可能误判，减轻

        # === 恶化模式增强 ===
        pattern_multiplier = 1.0
        pattern = getattr(context.deterioration, 'pattern', None)

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
            DeteriorationSeverity.SEVERE: "严重恶化",
            DeteriorationSeverity.MODERATE: "中度恶化",
            DeteriorationSeverity.MILD: "轻度恶化",
        }
        severity_label = severity_labels.get(
            context.deterioration.severity,
            str(context.deterioration.severity.value)
        )

        message = f"{severity_label}-{final_penalty:.1f}分"
        if pattern and pattern != "none":
            message += f"({pattern})"

        return create_penalty_result(
            "deterioration_penalty",
            message,
            final_penalty,
            metadata={
                "severity": context.deterioration.severity.value,
                "pattern": pattern,
                "bayesian_multiplier": bayesian_multiplier,
                "pattern_multiplier": pattern_multiplier,
            }
        )


# ============================================================================
# 3. 高波动扣分规则
# ============================================================================

class VolatilityPenaltyRule:
    """
    高波动扣分规则

    整合原来的:
    - rule_volatility_regime_adjustment
    - 高波动性不稳定检测

    触发条件:
    - CV > 阈值
    - 波动率体制为上升
    - 存在ARCH效应

    Examples:
        >>> rule = VolatilityPenaltyRule()
        >>> result = rule.execute(context, config)
    """

    name: str = "volatility_penalty"
    category: RuleCategory = RuleCategory.PENALTY
    priority: int = 120
    enabled: bool = True
    description: str = "高波动扣分"

    def execute(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> Optional[RuleResultImpl]:
        """执行规则"""
        from ..domain_models import VolatilityRegime

        penalty_cfg = config.penalty

        cv = context.volatility.cv
        vol_regime = context.volatility.volatility_regime
        vol_trend = context.volatility.volatility_change_ratio
        has_arch = getattr(context.volatility, 'has_arch_effect', False)

        # 基础高波动扣分
        if cv < penalty_cfg.high_volatility_cv:
            # 波动不高，但检查ARCH效应
            if has_arch:
                return create_penalty_result(
                    "arch_effect_penalty",
                    "波动聚集提示-2分(ARCH效应)",
                    2.0,
                    metadata={"has_arch": True}
                )
            return None

        # 计算扣分
        base_penalty = penalty_cfg.volatility_penalty_base

        # 波动率飙升 (vol_trend > 2.0 表示近期波动是早期的2倍以上)
        if vol_regime == VolatilityRegime.INCREASING and vol_trend > 2.0:
            penalty = min(vol_trend * 2, 8.0)
            return create_penalty_result(
                "volatility_surge_penalty",
                f"波动率飙升-{penalty:.1f}分(近期波动是早期{vol_trend:.1f}倍)",
                penalty,
                metadata={
                    "vol_regime": vol_regime.value,
                    "vol_trend": vol_trend,
                }
            )

        # ARCH效应 + 波动上升
        if has_arch and vol_regime == VolatilityRegime.INCREASING:
            return create_penalty_result(
                "arch_volatility_penalty",
                "波动聚集风险-6分(ARCH+波动上升)",
                6.0,
                metadata={
                    "has_arch": True,
                    "vol_regime": vol_regime.value,
                }
            )

        # 普通高波动
        excess_cv = (cv - penalty_cfg.high_volatility_cv) / penalty_cfg.high_volatility_cv
        penalty = min(base_penalty * (1 + excess_cv), config.scoring.max_penalty / 2)

        return create_penalty_result(
            "high_volatility_penalty",
            f"高波动-{penalty:.1f}分(CV={cv:.2f})",
            penalty,
            metadata={"cv": cv}
        )


# ============================================================================
# 4. 相对跌幅扣分规则
# ============================================================================

class RelativeDeclinePenaltyRule:
    """
    相对跌幅扣分规则

    最新值相对于加权平均的跌幅

    阈值:
    - <60%: 重度扣分
    - <70%: 中度扣分

    Examples:
        >>> rule = RelativeDeclinePenaltyRule()
        >>> result = rule.execute(context, config)
    """

    name: str = "relative_decline_penalty"
    category: RuleCategory = RuleCategory.PENALTY
    priority: int = 130
    enabled: bool = True
    description: str = "相对跌幅扣分"

    def execute(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> Optional[RuleResultImpl]:
        """执行规则"""
        penalty_cfg = config.penalty

        ratio = context.quality.latest_vs_weighted_ratio

        # 跌幅60%以上
        if ratio < penalty_cfg.relative_decline_60:
            return create_penalty_result(
                "relative_decline_60_penalty",
                f"相对加权暴跌-{penalty_cfg.relative_decline_60_penalty:.0f}分"
                f"(最新仅为加权{ratio:.1%})",
                penalty_cfg.relative_decline_60_penalty,
                metadata={"ratio": ratio}
            )

        # 跌幅70%以上
        if ratio < penalty_cfg.relative_decline_70:
            return create_penalty_result(
                "relative_decline_70_penalty",
                f"相对加权下滑-{penalty_cfg.relative_decline_70_penalty:.0f}分"
                f"(最新为加权{ratio:.1%})",
                penalty_cfg.relative_decline_70_penalty,
                metadata={"ratio": ratio}
            )

        return None


# ============================================================================
# 5. 单年暴跌扣分规则
# ============================================================================

class SingleYearDeclinePenaltyRule:
    """
    单年暴跌扣分规则

    单年跌幅超过阈值

    计算方式:
    - 从 raw_values 计算年度最大跌幅
    - 如果超过阈值则扣分

    Examples:
        >>> rule = SingleYearDeclinePenaltyRule()
        >>> result = rule.execute(context, config)
    """

    name: str = "single_year_decline_penalty"
    category: RuleCategory = RuleCategory.PENALTY
    priority: int = 140
    enabled: bool = True
    description: str = "单年暴跌扣分"

    def execute(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> Optional[RuleResultImpl]:
        """执行规则"""
        penalty_cfg = config.penalty

        values = context.quality.raw_values
        if values is None or len(values) < 2:
            return None

        # 计算年度变化
        worst_year = 0.0
        for i in range(len(values) - 1, 0, -1):
            if values[i - 1] != 0:
                pct_change = ((values[i] - values[i - 1]) / abs(values[i - 1])) * 100
                if pct_change < worst_year:
                    worst_year = pct_change

        if worst_year < penalty_cfg.single_year_decline_pct:
            return create_penalty_result(
                "single_year_decline_penalty",
                f"单年暴跌-{penalty_cfg.single_year_penalty:.0f}分(年跌{worst_year:.1f}%)",
                penalty_cfg.single_year_penalty,
                metadata={"worst_year_pct": worst_year}
            )

        return None


# ============================================================================
# 6. 连续下跌扣分规则 (智能版)
# ============================================================================

class ConsecutiveDeclinePenaltyRule:
    """
    连续下跌扣分规则 (智能版)

    整合原来的:
    - rule_smart_consecutive_decline

    智能连续下跌计数：微小反弹(<2%)不打断连续下跌

    阈值:
    - 连续3年: 严重扣分
    - 连续2年: 警示扣分

    Examples:
        >>> rule = ConsecutiveDeclinePenaltyRule()
        >>> result = rule.execute(context, config)
    """

    name: str = "consecutive_decline_penalty"
    category: RuleCategory = RuleCategory.PENALTY
    priority: int = 150
    enabled: bool = True
    description: str = "连续下跌扣分"

    MICRO_BOUNCE_THRESHOLD = 2.0  # 涨幅小于2%视为无实质性反弹

    def execute(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> Optional[RuleResultImpl]:
        """执行规则"""
        penalty_cfg = config.penalty

        values = context.quality.raw_values
        if values is None or len(values) < 3:
            return None

        # 智能连续下跌计数
        smart_consecutive = 0

        for i in range(len(values) - 1, 0, -1):
            current = values[i]
            previous = values[i - 1]

            if previous == 0:
                continue

            pct_change = ((current - previous) / abs(previous)) * 100

            if pct_change < -2.0:  # 实质性下跌
                smart_consecutive += 1
            elif pct_change < self.MICRO_BOUNCE_THRESHOLD:  # 微小反弹
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
            penalty = min(
                smart_consecutive * 4,
                penalty_cfg.consecutive_3y_penalty + 4
            )
            return create_penalty_result(
                "consecutive_decline_severe_penalty",
                f"连续下跌-{penalty:.0f}分"
                f"({smart_consecutive}年下跌，累计{total_decline_pct:.1f}%)",
                penalty,
                metadata={
                    "consecutive_years": smart_consecutive,
                    "total_decline_pct": total_decline_pct,
                }
            )

        # 连续2年下跌
        if smart_consecutive >= 2 and total_decline_pct < -20:
            penalty = penalty_cfg.consecutive_2y_penalty
            return create_penalty_result(
                "consecutive_decline_warning_penalty",
                f"连续下跌警示-{penalty:.0f}分({smart_consecutive}年下跌)",
                penalty,
                metadata={
                    "consecutive_years": smart_consecutive,
                    "total_decline_pct": total_decline_pct,
                }
            )

        return None


# ============================================================================
# 7. ROIIC为负扣分规则
# ============================================================================

class ROIICNegativePenaltyRule:
    """
    ROIIC为负扣分规则

    ROIIC为负表示新增投资效率低下

    计算方式:
    - 加权平均 < 阈值
    - 斜率为负
    - ROIC交叉验证加重

    Examples:
        >>> rule = ROIICNegativePenaltyRule()
        >>> result = rule.execute(context, config)
    """

    name: str = "roiic_negative_penalty"
    category: RuleCategory = RuleCategory.PENALTY
    priority: int = 160
    enabled: bool = True
    description: str = "ROIIC为负扣分"

    def execute(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> Optional[RuleResultImpl]:
        """执行规则"""
        if not is_roiic_metric(context):
            return None

        penalty_cfg = config.penalty

        weighted_component = max(
            -context.quality.weighted_avg - penalty_cfg.roiic_negative_buffer,
            0.0
        )
        slope_component = max(-context.trend.log_slope, 0.0)

        if weighted_component <= 0 and slope_component <= 0:
            return None

        # 计算扣分
        weighted_penalty = weighted_component / max(penalty_cfg.roiic_negative_scale, 1.0)
        slope_penalty = slope_component * config.scoring.penalty_factor * 0.3
        penalty = weighted_penalty + slope_penalty

        # ROIC交叉验证加重
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

        return create_penalty_result(
            "roiic_negative_penalty",
            f"ROIIC为负-{penalty:.1f}分(加权{context.quality.weighted_avg:.1f}%)",
            penalty,
            metadata={"weighted_avg": context.quality.weighted_avg}
        )


# ============================================================================
# 8. ROIIC/ROIC背离扣分规则
# ============================================================================

class ROIICDivergencePenaltyRule:
    """
    ROIIC/ROIC背离扣分规则

    ROIC良好但ROIIC下跌，表示新投资效率在恶化

    触发条件:
    - ROIC良好 (> 8%)
    - ROIIC在下跌
    - 斜率差距 > 阈值

    Examples:
        >>> rule = ROIICDivergencePenaltyRule()
        >>> result = rule.execute(context, config)
    """

    name: str = "roiic_divergence_penalty"
    category: RuleCategory = RuleCategory.PENALTY
    priority: int = 170
    enabled: bool = True
    description: str = "ROIIC/ROIC背离扣分"

    def execute(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> Optional[RuleResultImpl]:
        """执行规则"""
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

        # ROIC良好
        if roic_latest < 8.0:
            return None

        # ROIIC在下跌
        if context.trend.log_slope >= 0:
            return None

        # 计算背离
        slope_gap = roic_slope - context.trend.log_slope
        if slope_gap < penalty_cfg.roiic_divergence_gap:
            return None

        penalty = min(
            slope_gap * config.scoring.penalty_factor * 0.4,
            config.scoring.max_penalty / 2
        )
        penalty = max(penalty, 2.0)

        return create_penalty_result(
            "roiic_divergence_penalty",
            f"ROIIC与ROIC背离-{penalty:.1f}分"
            f"(ROIC斜率{roic_slope:.3f} > ROIIC {context.trend.log_slope:.3f})",
            penalty,
            metadata={
                "roic_slope": roic_slope,
                "roiic_slope": context.trend.log_slope,
                "slope_gap": slope_gap,
            }
        )


# ============================================================================
# 规则工厂
# ============================================================================

def create_all_penalty_rules() -> list[RuleProtocol]:
    """
    创建所有扣分规则实例

    Returns:
        规则实例列表，按优先级排序
    """
    rules = [
        MildDeclinePenaltyRule(),
        DeteriorationPenaltyRule(),
        VolatilityPenaltyRule(),
        RelativeDeclinePenaltyRule(),
        SingleYearDeclinePenaltyRule(),
        ConsecutiveDeclinePenaltyRule(),
        ROIICNegativePenaltyRule(),
        ROIICDivergencePenaltyRule(),
    ]

    # 按优先级排序
    return sorted(rules, key=lambda r: r.priority)


__all__ = [
    # 规则类
    'MildDeclinePenaltyRule',
    'DeteriorationPenaltyRule',
    'VolatilityPenaltyRule',
    'RelativeDeclinePenaltyRule',
    'SingleYearDeclinePenaltyRule',
    'ConsecutiveDeclinePenaltyRule',
    'ROIICNegativePenaltyRule',
    'ROIICDivergencePenaltyRule',
    # 工厂函数
    'create_all_penalty_rules',
    # 辅助函数
    'is_roiic_metric',
    'get_reference_metric',
]
