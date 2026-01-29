"""
否决规则 v2.0 (Veto Rules - Refactored)
=========================================

使用 Protocol-based 架构重构的否决规则。

设计原则:
- 实现 RuleProtocol 接口
- 使用新的 domain_models.TrendContext
- 不可变规则配置 (frozen dataclass)
- 清晰的业务语义

规则清单 (6个):
1. MinLatestValueVetoRule - 最低值否决
2. SevereTrendDeclineVetoRule - 严重趋势衰退
3. SevereDeteriorationVetoRule - 严重恶化
4. PeakDeclineVetoRule - 峰值暴跌否决
5. CumulativeCollapseVetoRule - 累计崩塌否决
6. ROIICCapitalDestructionVetoRule - ROIIC资本毁灭

作者: AStock Analysis System (Refactored)
日期: 2026-01-10
版本: 2.0.0
"""

from typing import Optional
import logging

from ..domain_models import (
    TrendContext, DeteriorationSeverity, TrendDirection
)
from ..protocols import RuleProtocol
from ..results import RuleResultImpl, create_veto_result
from ..rule_config import RuleConfig, RuleCategory

logger = logging.getLogger(__name__)


# ============================================================================
# 辅助函数 (Helper Functions)
# ============================================================================

def is_roiic_metric(context: TrendContext) -> bool:
    """判断是否为 ROIIC 指标"""
    return context.metric_name.lower() in {"roiic", "roii", "roic_incr"}


def is_cyclical_exemption(context: TrendContext) -> bool:
    """
    周期股底部豁免检查

    豁免条件:
    - 是周期股
    - 处于谷底或回升期
    - 趋势开始好转
    """
    if not context.cyclical.is_cyclical:
        return False

    from ..domain_models import CyclePhase

    # 谷底/回升期豁免
    if context.cyclical.cycle_phase in {CyclePhase.TROUGH, CyclePhase.RECOVERY}:
        # 必须有好转迹象
        if context.trend.recent_3y_slope > 0:
            return True

    return False


def is_turnaround_exemption(context: TrendContext) -> bool:
    """
    困境反转豁免检查

    豁免条件:
    - 有反转拐点
    - 近期趋势改善
    - 趋势加速度为正
    """
    from ..domain_models import TrendDirection

    # 必须有反转
    if not context.inflection.has_inflection:
        return False

    # 近期趋势改善
    if context.trend.recent_3y_slope <= 0:
        return False

    # 加速度为正
    if context.trend.trend_acceleration <= 0:
        return False

    return True


def get_reference_metric(context: TrendContext, metric_name: str) -> Optional[dict]:
    """
    获取参考指标统计数据

    Args:
        context: 趋势上下文
        metric_name: 指标名称 (如 'roic')

    Returns:
        参考指标的统计字典，如果不存在返回 None
    """
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
# 1. 最低值否决规则
# ============================================================================

class MinLatestValueVetoRule:
    """
    最低值否决规则

    触发条件 (任一):
    - 连续亏损 (3年以上)
    - 断崖式下跌 (跌幅超过阈值)

    豁免条件:
    - 困境反转: 最新值达到门槛60%且趋势强劲
    - 周期股底部回升期

    Examples:
        >>> rule = MinLatestValueVetoRule()
        >>> result = rule.execute(context, config)
        >>> if result and result.is_veto:
        ...     print(f"否决: {result.message}")
    """

    name: str = "min_latest_value_veto"
    category: RuleCategory = RuleCategory.VETO
    priority: int = 10
    enabled: bool = True
    description: str = "最低值否决规则"

    def execute(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> Optional[RuleResultImpl]:
        """
        执行规则

        Args:
            context: 趋势上下文 (新版 domain_models)
            config: 规则配置

        Returns:
            规则结果 (None 表示通过)
        """
        # 如果没有设置最低值要求，跳过
        min_latest = context.min_latest_value
        if min_latest is None:
            return None

        latest = context.quality.latest_value

        # === 豁免检查 ===

        # 困境反转豁免: 最新值达到门槛60%且形态为反转
        if latest >= min_latest * 0.6:
            if is_turnaround_exemption(context):
                logger.info(
                    f"🚀 困境反转豁免: {context.ts_code}_{context.metric_name} "
                    f"最新={latest:.2f}"
                )
                return None

        # 周期底部豁免
        if is_cyclical_exemption(context):
            logger.info(
                f"🛡️ 周期底部豁免: {context.ts_code}_{context.metric_name}"
            )
            return None

        # 达标则跳过
        if latest >= min_latest:
            return None

        # === 否决条件 ===

        # 连续亏损否决: 最新值为负且连续亏损3年以上
        if latest < 0 and context.deterioration.has_loss_years:
            if context.deterioration.loss_year_count >= 3:
                return create_veto_result(
                    "min_latest_value_loss_veto",
                    f"连续亏损-最新{context.metric_name}={latest:.2f}, "
                    f"亏损{context.deterioration.loss_year_count}年",
                    metadata={
                        "latest_value": latest,
                        "loss_years": context.deterioration.loss_year_count,
                    }
                )

        # 断崖式下跌否决: 累计跌幅超过阈值
        veto = config.veto
        decline_limit = (
            veto.cumulative_decline_cyclical_pct
            if context.cyclical.is_cyclical
            else veto.cumulative_decline_pct
        )

        if context.deterioration.total_decline_pct >= decline_limit:
            return create_veto_result(
                "min_latest_value_decline_veto",
                f"断崖式恶化-总跌幅{context.deterioration.total_decline_pct:.1f}%"
                f"≥{decline_limit}%",
                metadata={
                    "decline_pct": context.deterioration.total_decline_pct,
                    "threshold": decline_limit,
                }
            )

        # 未达否决条件，返回 None (交给扣分规则处理)
        return None


# ============================================================================
# 2. 严重趋势衰退否决规则
# ============================================================================

class SevereTrendDeclineVetoRule:
    """
    严重趋势衰退否决规则

    整合原来的:
    - rule_severe_decline
    - rule_structural_decline_veto

    触发条件 (满足任一):
    A. 严重衰退: log_slope < severe_decline 且 R² > 阈值
    B. 结构性衰退: 斜率恶化 + 持续下跌 + 最新值/加权<85% + 总跌幅>25%

    豁免条件:
    - 周期股谷底/回升期
    - 稳健斜率 (Theil-Sen) 未显示衰退

    Examples:
        >>> rule = SevereTrendDeclineVetoRule()
        >>> result = rule.execute(context, config)
    """

    name: str = "severe_trend_decline_veto"
    category: RuleCategory = RuleCategory.VETO
    priority: int = 30
    enabled: bool = True
    description: str = "严重趋势衰退否决"

    def execute(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> Optional[RuleResultImpl]:
        """执行规则"""
        veto = config.veto

        # === 周期底部豁免 ===
        if is_cyclical_exemption(context):
            logger.info(
                f"🛡️ 周期底部豁免(趋势衰退): "
                f"{context.ts_code}_{context.metric_name}"
            )
            return None

        # === 条件A: 严重衰退 ===
        severe_decline = (
            context.trend.log_slope < veto.severe_decline_slope and
            context.trend.r_squared > veto.severe_decline_r2_min
        )

        if severe_decline:
            # 稳健性豁免: 如果 Theil-Sen 斜率明显优于 OLS
            robust_slope = context.trend.robust_slope
            if robust_slope is not None:
                if robust_slope > veto.severe_decline_slope:
                    slope_diff = abs(robust_slope - context.trend.log_slope)
                    if slope_diff > 0.1:
                        logger.info(
                            f"🛡️ 稳健性豁免: "
                            f"OLS={context.trend.log_slope:.3f}, "
                            f"Robust={robust_slope:.3f}"
                        )
                        return None

            return create_veto_result(
                "severe_trend_decline_veto",
                f"严重衰退-对数斜率={context.trend.log_slope:.3f}, "
                f"CAGR≈{context.trend.cagr_approx*100:.1f}%, "
                f"R²={context.trend.r_squared:.2f}",
                metadata={
                    "log_slope": context.trend.log_slope,
                    "cagr": context.trend.cagr_approx,
                    "r_squared": context.trend.r_squared,
                }
            )

        # === 条件B: 结构性衰退 ===
        mild_decline = veto.severe_decline_slope * 0.3
        structural_decline = (
            context.trend.log_slope <= mild_decline and
            context.trend.recent_3y_slope <= -0.05 and
            context.quality.latest_vs_weighted_ratio < 0.85 and
            context.deterioration.total_decline_pct >= 25 and
            context.trend.r_squared >= veto.severe_decline_r2_min
        )

        if structural_decline:
            # 如果趋势加速度为正且近期斜率改善，不否决
            if (context.trend.trend_acceleration > -0.05 and
                context.trend.recent_3y_slope > -0.02):
                return None

            return create_veto_result(
                "structural_decline_veto",
                f"结构性衰退-斜率{context.trend.log_slope:.3f}, "
                f"近3年{context.trend.recent_3y_slope:.3f}, "
                f"最新/加权{context.quality.latest_vs_weighted_ratio:.1%}",
                metadata={
                    "log_slope": context.trend.log_slope,
                    "recent_slope": context.trend.recent_3y_slope,
                    "ratio": context.quality.latest_vs_weighted_ratio,
                }
            )

        return None


# ============================================================================
# 3. 严重恶化否决规则
# ============================================================================

class SevereDeteriorationVetoRule:
    """
    严重恶化否决规则

    整合原来的:
    - rule_severe_deterioration_veto
    - rule_compound_recent_deterioration

    触发条件:
    A. 严重恶化: deterioration_severity == "severe" 且 (跌幅>40% 或 最新/加权<70%)
    B. 复合恶化: 多个恶化信号同时触发 (趋势反转+加速下滑+大幅回撤)

    豁免条件:
    - 周期股谷底/回升期

    Examples:
        >>> rule = SevereDeteriorationVetoRule()
        >>> result = rule.execute(context, config)
    """

    name: str = "severe_deterioration_veto"
    category: RuleCategory = RuleCategory.VETO
    priority: int = 40
    enabled: bool = True
    description: str = "严重恶化否决"

    def execute(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> Optional[RuleResultImpl]:
        """执行规则"""
        veto = config.veto

        # === 周期底部豁免 ===
        if is_cyclical_exemption(context):
            logger.info(
                f"🛡️ 周期底部豁免(恶化): {context.ts_code}_{context.metric_name}"
            )
            return None

        # === 条件A: 严重恶化 ===
        if context.deterioration.severity == DeteriorationSeverity.SEVERE:
            if context.deterioration.total_decline_pct > veto.deterioration_decline_pct:
                return create_veto_result(
                    "severe_deterioration_veto",
                    f"严重恶化-跌幅{context.deterioration.total_decline_pct:.1f}%"
                    f">{veto.deterioration_decline_pct}%",
                    metadata={
                        "decline_pct": context.deterioration.total_decline_pct,
                        "threshold": veto.deterioration_decline_pct,
                    }
                )

            if context.quality.latest_vs_weighted_ratio < veto.deterioration_ratio:
                return create_veto_result(
                    "severe_deterioration_veto",
                    f"严重恶化-最新仅为加权"
                    f"{context.quality.latest_vs_weighted_ratio:.1%}"
                    f"<{veto.deterioration_ratio:.0%}",
                    metadata={
                        "ratio": context.quality.latest_vs_weighted_ratio,
                        "threshold": veto.deterioration_ratio,
                    }
                )

        # === 条件B: 复合恶化 ===
        if (context.deterioration.has_deterioration and
            context.deterioration.severity != DeteriorationSeverity.NONE):

            negative_signals = 0

            # 信号1: 趋势反转 (增长转衰退)
            if context.inflection.has_inflection:
                # 需要检查是否为负向拐点
                # TODO: 完善拐点类型判断
                negative_signals += 1

            # 信号2: 加速下滑
            if context.trend.is_decelerating and context.trend.recent_3y_slope < 0:
                negative_signals += 1

            # 信号3: 斜率恶化
            if context.trend.log_slope < config.penalty.mild_decline_slope:
                negative_signals += 1

            # 信号4: 最新值显著低于加权
            if context.quality.latest_vs_weighted_ratio < 0.75:
                negative_signals += 1

            # 复合恶化否决: 3个以上恶化信号 + 严重恶化 + 大幅跌幅
            if (negative_signals >= 3 and
                context.deterioration.severity == DeteriorationSeverity.SEVERE and
                context.deterioration.total_decline_pct >= 35):
                return create_veto_result(
                    "compound_deterioration_veto",
                    f"复合恶化-{negative_signals}项信号同时触发",
                    metadata={
                        "signal_count": negative_signals,
                        "decline_pct": context.deterioration.total_decline_pct,
                    }
                )

        return None


# ============================================================================
# 4. 峰值暴跌否决规则
# ============================================================================

class PeakDeclineVetoRule:
    """
    峰值暴跌否决规则

    从历史峰值的大幅下跌

    触发条件:
    - 从峰值跌幅超过阈值 (默认70%，周期股80%)

    解决问题:
    - 义翘神州 155% -> 1.78% 这类情况

    Examples:
        >>> rule = PeakDeclineVetoRule()
        >>> result = rule.execute(context, config)
    """

    name: str = "peak_decline_veto"
    category: RuleCategory = RuleCategory.VETO
    priority: int = 20
    enabled: bool = True
    description: str = "峰值暴跌否决"

    def execute(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> Optional[RuleResultImpl]:
        """执行规则"""
        veto = config.veto

        peak_value = context.quality.max_value
        latest_value = context.quality.latest_value

        if peak_value is None or latest_value is None or peak_value <= 0:
            return None

        # 计算从峰值的跌幅
        decline_pct = ((peak_value - latest_value) / peak_value) * 100

        # 根据是否周期股选择阈值
        threshold = (
            veto.peak_decline_cyclical_pct
            if context.cyclical.is_cyclical
            else veto.peak_decline_pct
        )

        if decline_pct >= threshold:
            return create_veto_result(
                "peak_decline_veto",
                f"峰值暴跌-从{peak_value:.1f}跌至{latest_value:.1f}，"
                f"跌幅{decline_pct:.1f}%≥{threshold}%",
                metadata={
                    "peak_value": peak_value,
                    "latest_value": latest_value,
                    "decline_pct": decline_pct,
                    "threshold": threshold,
                }
            )

        return None


# ============================================================================
# 5. 累计崩塌否决规则
# ============================================================================

class CumulativeCollapseVetoRule:
    """
    累计崩塌否决规则

    曾经是优质资产 (高ROIC) 但现在已变成劣质资产的情况

    触发条件:
    - 历史最高 > 30% (曾经优质)
    - 当前 < 5% (已经劣质)
    - 跌幅 > 80%

    Examples:
        >>> rule = CumulativeCollapseVetoRule()
        >>> result = rule.execute(context, config)
    """

    name: str = "cumulative_collapse_veto"
    category: RuleCategory = RuleCategory.VETO
    priority: int = 15
    enabled: bool = True
    description: str = "累计崩塌否决"

    def execute(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> Optional[RuleResultImpl]:
        """执行规则"""
        veto = config.veto

        raw_values = context.quality.raw_values
        latest = context.quality.latest_value

        if raw_values is None or len(raw_values) < 3 or latest is None:
            return None

        max_val = max(raw_values)
        if max_val <= 0:
            return None

        # 计算跌幅
        decline_pct = ((max_val - latest) / max_val) * 100

        # 崩塌条件: 曾经优质 + 现在劣质 + 大幅下跌
        if (max_val > veto.collapse_max_threshold and
            latest < veto.collapse_latest_threshold and
            decline_pct > veto.collapse_decline_pct):
            return create_veto_result(
                "cumulative_collapse_veto",
                f"累计崩塌-曾达{max_val:.1f}%，现仅{latest:.1f}%，"
                f"跌幅{decline_pct:.1f}%",
                metadata={
                    "max_value": max_val,
                    "latest_value": latest,
                    "decline_pct": decline_pct,
                }
            )

        return None


# ============================================================================
# 6. ROIIC 资本毁灭否决规则
# ============================================================================

class ROIICCapitalDestructionVetoRule:
    """
    ROIIC 资本毁灭否决规则

    ROIIC 持续为负表示新增投资在毁灭价值

    触发条件 (同时满足):
    - 是 ROIIC 指标
    - 加权平均 < -20%
    - 最新值 < -10%
    - 趋势显著 (R² > 0.4)
    - ROIC 也在恶化
    - 恶化程度为 severe 或 moderate

    Examples:
        >>> rule = ROIICCapitalDestructionVetoRule()
        >>> result = rule.execute(context, config)
    """

    name: str = "roiic_capital_destruction_veto"
    category: RuleCategory = RuleCategory.VETO
    priority: int = 50
    enabled: bool = True
    description: str = "ROIIC资本毁灭否决"

    def execute(
        self,
        context: TrendContext,
        config: RuleConfig
    ) -> Optional[RuleResultImpl]:
        """执行规则"""
        if not is_roiic_metric(context):
            return None

        veto = config.veto

        # 基本条件检查
        if context.quality.weighted_avg > veto.roiic_weighted_threshold:
            return None
        if context.quality.latest_value > veto.roiic_latest_threshold:
            return None
        if context.trend.log_slope > veto.severe_decline_slope:
            return None
        if context.trend.r_squared < max(veto.severe_decline_r2_min, 0.4):
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
            context.deterioration.severity in {
                DeteriorationSeverity.SEVERE,
                DeteriorationSeverity.MODERATE
            } or
            context.deterioration.total_decline_pct >= 40
        )

        if roic_flag and deterioration_flag:
            return create_veto_result(
                "roiic_capital_destruction_veto",
                f"ROIIC资本毁灭-加权{context.quality.weighted_avg:.1f}%, "
                f"最新{context.quality.latest_value:.1f}%",
                metadata={
                    "weighted_avg": context.quality.weighted_avg,
                    "latest_value": context.quality.latest_value,
                }
            )

        return None


# ============================================================================
# 规则工厂
# ============================================================================

def create_all_veto_rules() -> list[RuleProtocol]:
    """
    创建所有否决规则实例

    Returns:
        规则实例列表，按优先级排序
    """
    rules = [
        MinLatestValueVetoRule(),
        CumulativeCollapseVetoRule(),
        PeakDeclineVetoRule(),
        SevereTrendDeclineVetoRule(),
        SevereDeteriorationVetoRule(),
        ROIICCapitalDestructionVetoRule(),
    ]

    # 按优先级排序
    return sorted(rules, key=lambda r: r.priority)


__all__ = [
    # 规则类
    'MinLatestValueVetoRule',
    'SevereTrendDeclineVetoRule',
    'SevereDeteriorationVetoRule',
    'PeakDeclineVetoRule',
    'CumulativeCollapseVetoRule',
    'ROIICCapitalDestructionVetoRule',
    # 工厂函数
    'create_all_veto_rules',
    # 辅助函数
    'is_roiic_metric',
    'is_cyclical_exemption',
    'is_turnaround_exemption',
    'get_reference_metric',
]
