"""
投资策略 v2.0 (Strategies - Refactored)
======================================

使用 Protocol-based 架构的投资策略系统。

设计原则:
- 实现 StrategyProtocol 接口
- 使用新的 domain_models
- 工厂模式创建
- 模式识别而非评分

策略清单 (5个):
1. HighGrowthStrategy - 高增长/优质护城河
2. TurnaroundStrategy - 困境反转
3. StableDividendStrategy - 稳定分红型
4. CyclicalBottomStrategy - 周期底部抄底
5. MoatDefenseStrategy - 护城河防守

作者: AStock Analysis System
日期: 2026-01-10
版本: 2.0.0
"""

from typing import List, Optional
import logging

from .domain_models import (
    TrendContext,
    TrendDirection,
    VolatilityRegime,
    CyclePhase,
    DeteriorationSeverity,
)
from .protocols import StrategyProtocol
from .results import StrategyResultImpl

logger = logging.getLogger(__name__)


# ============================================================================
# 辅助函数
# ============================================================================

def is_efficiency_metric(metric_name: str) -> bool:
    """判断是否为效率/比率类指标"""
    keywords = ["roic", "roe", "margin", "rate", "ratio", "yield", "percent"]
    return any(k in metric_name.lower() for k in keywords)


def is_scale_metric(metric_name: str) -> bool:
    """判断是否为规模类指标"""
    keywords = ["revenue", "profit", "eps", "sales", "income", "ebit"]
    return any(k in metric_name.lower() for k in keywords)


# ============================================================================
# 1. 高增长策略
# ============================================================================

class HighGrowthStrategy:
    """
    高增长/优质护城河策略

    特征:
    - 效率指标: 高位稳定 (ROIC/ROE > 15%, 低波动)
    - 规模指标: 高速成长 (CAGR > 20%, 加速增长)
    - 无严重恶化

    Examples:
        >>> strategy = HighGrowthStrategy()
        >>> result = strategy.evaluate(context)
    """

    name: str = "high_growth"
    description: str = "高增长/优质护城河"
    priority: int = 100
    enabled: bool = True

    def evaluate(self, context: TrendContext) -> Optional[StrategyResultImpl]:
        """评估策略匹配"""

        # 排除严重恶化
        if context.deterioration.severity in {
            DeteriorationSeverity.SEVERE,
            DeteriorationSeverity.CATASTROPHIC
        }:
            return None

        metric_type = "efficiency" if is_efficiency_metric(context.metric_name) else "scale"

        # === 效率指标: 护城河模式 ===
        if metric_type == "efficiency":
            min_value = 15.0
            if "net_margin" in context.metric_name.lower():
                min_value = 10.0
            elif "gross_margin" in context.metric_name.lower():
                min_value = 40.0

            # 高位稳定
            if (context.quality.latest_value >= min_value and
                context.volatility.is_stable and
                context.trend.trend_direction != TrendDirection.DOWNWARD):

                confidence = min(
                    (context.quality.latest_value / min_value) * 0.4 +
                    (1 - context.volatility.cv) * 0.3 +
                    (1 if context.trend.log_slope > 0 else 0.5) * 0.3,
                    1.0
                )

                return StrategyResultImpl(
                    name=self.name,
                    matched=True,
                    reason=f"护城河: {context.metric_name}高位稳定({context.quality.latest_value:.1f})",
                    confidence=confidence,
                    recommendations=["长期持有", "核心资产"],
                    metadata={
                        "latest_value": context.quality.latest_value,
                        "cv": context.volatility.cv,
                    }
                )

        # === 规模指标: 高增长模式 ===
        elif metric_type == "scale":
            cagr = context.trend.cagr_approx

            # 高速成长
            if (cagr > 0.20 and
                context.trend.is_accelerating and
                not context.deterioration.has_deterioration):

                confidence = min(cagr / 0.30, 1.0)  # CAGR 30%+ 满分

                return StrategyResultImpl(
                    name=self.name,
                    matched=True,
                    reason=f"高增长: {context.metric_name} CAGR {cagr:.1%}",
                    confidence=confidence,
                    recommendations=["成长股投资", "关注持续性"],
                    metadata={
                        "cagr": cagr,
                        "accelerating": context.trend.is_accelerating,
                    }
                )

        return None


# ============================================================================
# 2. 困境反转策略
# ============================================================================

class TurnaroundStrategy:
    """
    困境反转策略

    特征:
    - 有明显拐点 (从负转正)
    - 近期改善明显 (WLS > OLS)
    - 当前位置不高 (价值洼地)

    Examples:
        >>> strategy = TurnaroundStrategy()
        >>> result = strategy.evaluate(context)
    """

    name: str = "turnaround"
    description: str = "困境反转"
    priority: int = 110
    enabled: bool = True

    def evaluate(self, context: TrendContext) -> Optional[StrategyResultImpl]:
        """评估策略匹配"""

        # 拐点恢复信号 - 使用 inflection_type 判断
        if context.inflection.has_inflection:
            # V型反转: 从恶化转向恢复
            if context.inflection.is_v_shaped_recovery:
                slope_change = abs(context.inflection.slope_change or 0)
                confidence = min(slope_change / 0.30, 1.0)

                return StrategyResultImpl(
                    name=self.name,
                    matched=True,
                    reason=f"V型反转: 斜率变化{slope_change:.2%}",
                    confidence=confidence,
                    recommendations=["逆向投资", "关注反转持续性"],
                    metadata={
                        "inflection_type": context.inflection.inflection_type,
                        "slope_change": slope_change,
                    }
                )

        # 近期改善信号 (WLS > OLS)
        wls_slope = context.trend.wls_slope
        ols_slope = context.trend.log_slope

        if wls_slope is not None:
            diff = wls_slope - ols_slope

            # 困境反转: OLS衰退但WLS稳定
            if ols_slope < -0.05 and wls_slope > -0.02 and diff > 0.10:
                confidence = min(diff / 0.20, 1.0)

                return StrategyResultImpl(
                    name=self.name,
                    matched=True,
                    reason=f"困境企稳: 整体下滑({ols_slope:.1%})但近期稳定({wls_slope:.1%})",
                    confidence=confidence,
                    recommendations=["等待确认", "分批建仓"],
                    metadata={
                        "wls_slope": wls_slope,
                        "ols_slope": ols_slope,
                        "improvement": diff,
                    }
                )

        return None


# ============================================================================
# 3. 稳定分红策略
# ============================================================================

class StableDividendStrategy:
    """
    稳定分红型策略

    特征:
    - 低波动 (CV < 15%)
    - 趋势平稳或缓慢上升
    - 适用于分红/现金流指标

    Examples:
        >>> strategy = StableDividendStrategy()
        >>> result = strategy.evaluate(context)
    """

    name: str = "stable_dividend"
    description: str = "稳定分红型"
    priority: int = 120
    enabled: bool = True

    def evaluate(self, context: TrendContext) -> Optional[StrategyResultImpl]:
        """评估策略匹配"""

        # 只对分红/现金流相关指标触发
        metric_lower = context.metric_name.lower()
        if not any(k in metric_lower for k in ["dividend", "ocf", "fcf", "cash"]):
            return None

        # 低波动 + 平稳趋势
        if (context.volatility.cv < 0.15 and
            context.volatility.is_stable and
            context.trend.log_slope >= -0.05):

            # 信心度基于稳定性
            confidence = 1.0 - context.volatility.cv / 0.15

            return StrategyResultImpl(
                name=self.name,
                matched=True,
                reason=f"稳定分红: {context.metric_name}低波动({context.volatility.cv:.1%})",
                confidence=confidence,
                recommendations=["防守型配置", "稳定现金流"],
                metadata={
                    "cv": context.volatility.cv,
                    "log_slope": context.trend.log_slope,
                }
            )

        return None


# ============================================================================
# 4. 周期底部策略
# ============================================================================

class CyclicalBottomStrategy:
    """
    周期底部抄底策略

    特征:
    - 周期性股票
    - 处于谷底或回升期
    - 历史周期验证

    Examples:
        >>> strategy = CyclicalBottomStrategy()
        >>> result = strategy.evaluate(context)
    """

    name: str = "cyclical_bottom"
    description: str = "周期底部抄底"
    priority: int = 130
    enabled: bool = True

    def evaluate(self, context: TrendContext) -> Optional[StrategyResultImpl]:
        """评估策略匹配"""

        if not context.cyclical.is_cyclical:
            return None

        current_phase = context.cyclical.current_phase

        # 谷底机会
        if current_phase == CyclePhase.TROUGH:
            return StrategyResultImpl(
                name=self.name,
                matched=True,
                reason=f"周期底部: {context.metric_name}处于谷底",
                confidence=0.7,
                recommendations=["逆向布局", "等待周期回升"],
                metadata={"phase": current_phase.value}
            )

        # 回升期确认
        if current_phase == CyclePhase.RECOVERY:
            return StrategyResultImpl(
                name=self.name,
                matched=True,
                reason=f"周期回升: {context.metric_name}景气向上",
                confidence=0.8,
                recommendations=["趋势跟随", "关注周期顶部"],
                metadata={"phase": current_phase.value}
            )

        return None


# ============================================================================
# 5. 护城河防守策略
# ============================================================================

class MoatDefenseStrategy:
    """
    护城河防守策略

    特征:
    - 效率指标高位
    - 长期稳定 (5年+)
    - 轻微回调豁免 (均值回归)

    Examples:
        >>> strategy = MoatDefenseStrategy()
        >>> result = strategy.evaluate(context)
    """

    name: str = "moat_defense"
    description: str = "护城河防守"
    priority: int = 140
    enabled: bool = True

    def evaluate(self, context: TrendContext) -> Optional[StrategyResultImpl]:
        """评估策略匹配"""

        if not is_efficiency_metric(context.metric_name):
            return None

        # 高位稳定 + 轻微回调
        if (context.quality.weighted_avg > 15.0 and
            context.quality.latest_vs_weighted_ratio > 0.80 and
            context.deterioration.severity in {
                DeteriorationSeverity.NONE,
                DeteriorationSeverity.MILD
            }):

            confidence = min(
                context.quality.weighted_avg / 20.0 * 0.5 +
                context.quality.latest_vs_weighted_ratio * 0.5,
                1.0
            )

            return StrategyResultImpl(
                name=self.name,
                matched=True,
                reason=f"护城河坚固: {context.metric_name}长期高位({context.quality.weighted_avg:.1f})",
                confidence=confidence,
                recommendations=["持有核心仓位", "逢低加仓"],
                metadata={
                    "weighted_avg": context.quality.weighted_avg,
                    "latest_ratio": context.quality.latest_vs_weighted_ratio,
                }
            )

        return None


# ============================================================================
# 策略工厂
# ============================================================================

def create_all_strategies() -> List[StrategyProtocol]:
    """
    创建所有策略实例

    Returns:
        策略实例列表，按优先级排序
    """
    strategies = [
        HighGrowthStrategy(),
        TurnaroundStrategy(),
        StableDividendStrategy(),
        CyclicalBottomStrategy(),
        MoatDefenseStrategy(),
    ]

    # 按优先级排序
    return sorted(strategies, key=lambda s: s.priority)


def get_default_strategies() -> List[StrategyProtocol]:
    """获取默认策略列表 (向后兼容)"""
    return create_all_strategies()


__all__ = [
    # 策略类
    'HighGrowthStrategy',
    'TurnaroundStrategy',
    'StableDividendStrategy',
    'CyclicalBottomStrategy',
    'MoatDefenseStrategy',
    # 工厂函数
    'create_all_strategies',
    'get_default_strategies',
    # 辅助函数
    'is_efficiency_metric',
    'is_scale_metric',
]
