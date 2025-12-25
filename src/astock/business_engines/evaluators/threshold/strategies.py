"""
投资策略 (Strategies)
=====================

投资策略定义：模式匹配和投资建议。

重构说明:
- 清理了与规则重叠的逻辑
- 策略专注于模式识别，规则专注于硬性筛选
- 策略不再重复规则的加分逻辑

策略清单 (5个):
1. HighGrowthStrategy - 高增长/优质护城河
2. TurnaroundStrategy - 困境反转
3. StableDividendStrategy - 稳定分红型
4. CyclicalBottomStrategy - 周期底部抄底
5. MoatDefenseStrategy - 护城河防守

作者: AStock Analysis System
日期: 2025-12-19
"""

from dataclasses import dataclass, field
from typing import List, Protocol, Optional, Dict, Any
import math

# 从 trend 导入数据模型
from ...trend.models import TrendContext


@dataclass
class StrategyResult:
    """策略评估结果"""
    name: str
    matched: bool
    reason: str = ""
    score_boost: float = 0.0
    confidence: float = 0.0
    recommendations: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class TrendStrategy(Protocol):
    """策略协议定义"""
    name: str
    description: str

    def evaluate(self, context: TrendContext) -> StrategyResult:
        """评估当前上下文是否符合策略定义"""
        ...


class BaseStrategy:
    """策略基类"""

    def _is_efficiency_metric(self, metric_name: str) -> bool:
        """判断是否为效率/比率类指标"""
        keywords = ["roic", "roe", "margin", "rate", "ratio", "yield", "percent"]
        return any(k in metric_name.lower() for k in keywords)

    def _get_robust_growth_rate(self, context: TrendContext) -> float:
        """获取稳健增长率"""
        if context.cv < 0.15:
            return context.log_slope
        return context.robust_slope if context.robust_slope != 0 else context.log_slope


# ============================================================================
# 1. 高增长策略
# ============================================================================

class HighGrowthStrategy(BaseStrategy):
    """
    高增长/优质护城河策略

    特征:
    - 效率指标: 高位稳定 (ROIC/ROE > 15%, 趋势稳定)
    - 规模指标: 高速成长 (CAGR > 20%, 加速增长)
    """
    name = "high_growth"
    description = "高增长/优质护城河"

    def evaluate(self, context: TrendContext) -> StrategyResult:
        if math.isnan(context.latest_value) or math.isnan(context.log_slope):
            return StrategyResult(self.name, False)

        metric_type = "efficiency" if self._is_efficiency_metric(context.metric_name) else "scale"
        growth_rate = self._get_robust_growth_rate(context)

        # === 效率指标: 护城河模式 ===
        if metric_type == "efficiency":
            min_value = 15.0
            if "net_margin" in context.metric_name.lower():
                min_value = 10.0
            elif "gross_margin" in context.metric_name.lower():
                min_value = 40.0

            if context.latest_value < min_value:
                return StrategyResult(self.name, False)

            if growth_rate < -0.02:
                return StrategyResult(self.name, False)

            # 稳健性检查
            min_r2 = 0.4
            if context.mann_kendall_tau > 0.4:
                min_r2 = 0.2

            if context.r_squared < min_r2 and context.cv > 0.2:
                return StrategyResult(self.name, False)

            return StrategyResult(
                self.name, True,
                f"优质护城河({context.latest_value:.1f}>{min_value}, 趋势稳定)",
                score_boost=10.0,
                confidence=0.8,
                recommendations=["长期持有", "关注护城河持续性"]
            )

        # === 规模指标: 成长模式 ===
        else:
            min_growth = 0.20

            if growth_rate < min_growth:
                return StrategyResult(self.name, False)

            # 防止假增长
            if context.cv > 0.3 and context.mann_kendall_tau <= 0:
                return StrategyResult(self.name, False)

            # 高波动但非显著趋势
            if growth_rate > 0.3 and context.mann_kendall_p_value > 0.1:
                return StrategyResult(self.name, False, "增长不显著")

            return StrategyResult(
                self.name, True,
                f"高速成长(CAGR={growth_rate:.1%})",
                score_boost=10.0,
                confidence=0.75,
                recommendations=["关注增长持续性", "警惕估值过高"]
            )


# ============================================================================
# 2. 困境反转策略
# ============================================================================

class TurnaroundStrategy(BaseStrategy):
    """
    困境反转策略

    特征:
    - 曾经亏损或大幅下跌
    - 当前已恢复到安全区域
    - 近期趋势强劲 (近3年斜率 > 15%)
    """
    name = "turnaround"
    description = "困境反转/由亏转盈"

    def evaluate(self, context: TrendContext) -> StrategyResult:
        if math.isnan(context.latest_value):
            return StrategyResult(self.name, False)

        # 设定恢复门槛
        recovery_threshold = 5.0
        if "net_margin" in context.metric_name.lower():
            recovery_threshold = 2.0
        elif "gross_margin" in context.metric_name.lower():
            recovery_threshold = 15.0

        # 必须已恢复
        if context.latest_value < recovery_threshold:
            return StrategyResult(self.name, False)

        # 动能必须强劲
        if context.recent_3y_slope < 0.15:
            return StrategyResult(self.name, False)

        # 防骗线: 高波动时需要趋势确认
        if context.cv > 0.5 and context.mann_kendall_tau < -0.2:
            return StrategyResult(self.name, False)

        # === 反转场景识别 ===
        is_turnaround = False
        reason = ""

        # 场景A: 扭亏为盈
        if context.has_loss_years and context.latest_value > recovery_threshold:
            is_turnaround = True
            reason = f"扭亏为盈(曾亏损{context.loss_year_count}年)"

        # 场景B: V型反转
        elif context.inflection_type == "deterioration_to_recovery":
            is_turnaround = True
            reason = f"V型反转(斜率改善{context.slope_change:.2f})"

        # 场景C: 深度底部反转
        elif context.total_decline_pct > 30 and context.recent_3y_slope > 0.3:
            is_turnaround = True
            reason = f"底部反转(曾跌{context.total_decline_pct:.0f}%)"

        if is_turnaround:
            return StrategyResult(
                self.name, True, reason,
                score_boost=8.0,
                confidence=0.7,
                recommendations=["关注反转持续性", "分批建仓"]
            )

        return StrategyResult(self.name, False)


# ============================================================================
# 3. 稳定分红策略
# ============================================================================

class StableDividendStrategy(BaseStrategy):
    """
    稳定分红型策略

    特征:
    - 高且稳定的盈利能力
    - 极低波动性 (CV < 0.20)
    - 无明显衰退趋势
    """
    name = "stable_dividend"
    description = "稳定分红/现金奶牛"

    def evaluate(self, context: TrendContext) -> StrategyResult:
        if math.isnan(context.latest_value):
            return StrategyResult(self.name, False)

        # 仅适用于效率指标
        if not self._is_efficiency_metric(context.metric_name):
            return StrategyResult(self.name, False)

        # 绝对值要求
        min_value = 12.0
        if context.latest_value < min_value:
            return StrategyResult(self.name, False)

        # 稳定性要求
        if context.cv > 0.20:
            return StrategyResult(self.name, False)

        # 趋势要求
        if context.log_slope < -0.05:
            return StrategyResult(self.name, False)

        # 最新值不能大幅低于加权均值
        if context.latest_vs_weighted_ratio < 0.85:
            return StrategyResult(self.name, False)

        # 计算置信度
        confidence = min(
            (1.0 - context.cv / 0.2) * 0.4 +
            (context.latest_value / min_value - 1.0) * 0.3 +
            (context.latest_vs_weighted_ratio - 0.85) * 0.3,
            1.0
        )

        return StrategyResult(
            self.name, True,
            f"稳定分红型(值={context.latest_value:.1f}, CV={context.cv:.1%})",
            score_boost=6.0,
            confidence=confidence,
            recommendations=["适合长期持有", "关注分红率"]
        )


# ============================================================================
# 4. 周期底部策略
# ============================================================================

class CyclicalBottomStrategy(BaseStrategy):
    """
    周期底部抄底策略

    特征:
    - 确认的周期性行业
    - 当前处于周期底部区域
    - 有复苏迹象
    """
    name = "cyclical_bottom"
    description = "周期底部抄底"

    def evaluate(self, context: TrendContext) -> StrategyResult:
        # 必须是周期股
        if not context.is_cyclical:
            return StrategyResult(self.name, False)

        cycle_position = context.cycle_position

        if cycle_position not in ("bottom", "mid_up"):
            return StrategyResult(self.name, False)

        # 必须有复苏迹象
        if context.current_phase != "rising":
            return StrategyResult(self.name, False)

        # 近期趋势必须转正
        if context.recent_3y_slope < 0:
            return StrategyResult(self.name, False)

        # 构建置信度
        confidence = 0.5
        reasons = [f"周期底部({cycle_position})"]

        if context.inflection_type == "deterioration_to_recovery":
            reasons.append("V型反转")
            confidence += 0.2

        if context.fft_dominant_period and context.fft_dominant_period > 0:
            reasons.append(f"周期{context.fft_dominant_period:.0f}年")
            confidence += 0.15

        if context.recent_3y_slope > 0.1:
            confidence += 0.15

        return StrategyResult(
            self.name, True,
            ", ".join(reasons),
            score_boost=8.0,
            confidence=min(confidence, 1.0),
            recommendations=["逆向投资机会", "需结合行业研究", "控制仓位"]
        )


# ============================================================================
# 5. 护城河防守策略
# ============================================================================

class MoatDefenseStrategy(BaseStrategy):
    """
    护城河防守策略

    特征:
    - 利润率长期高位稳定
    - 即使行业波动也保持稳定
    - 侧重防御性
    """
    name = "moat_defense"
    description = "护城河防守/稳定盈利"

    def evaluate(self, context: TrendContext) -> StrategyResult:
        if math.isnan(context.latest_value):
            return StrategyResult(self.name, False)

        metric = context.metric_name.lower()

        # 针对利润率指标
        if "margin" not in metric and "roe" not in metric and "roic" not in metric:
            return StrategyResult(self.name, False)

        # 护城河门槛
        moat_threshold = 40.0 if "gross" in metric else 15.0
        if context.latest_value < moat_threshold:
            return StrategyResult(self.name, False)

        # 稳定性
        if context.cv > 0.15:
            return StrategyResult(self.name, False)

        # 趋势
        if context.log_slope < -0.03:
            return StrategyResult(self.name, False)

        # R² 清晰
        if context.r_squared < 0.5:
            return StrategyResult(self.name, False)

        # 护城河强度
        moat_strength = (context.latest_value - moat_threshold) / moat_threshold
        confidence = min(
            moat_strength * 0.5 +
            (1.0 - context.cv / 0.15) * 0.3 +
            context.r_squared * 0.2,
            1.0
        )

        return StrategyResult(
            self.name, True,
            f"强护城河({context.latest_value:.1f}>{moat_threshold})",
            score_boost=8.0,
            confidence=confidence,
            recommendations=["核心持仓", "关注竞争格局变化"]
        )


# ============================================================================
# 策略工厂
# ============================================================================

def get_default_strategies() -> List[TrendStrategy]:
    """获取默认策略列表"""
    return [
        HighGrowthStrategy(),
        TurnaroundStrategy(),
        StableDividendStrategy(),
        CyclicalBottomStrategy(),
        MoatDefenseStrategy(),
    ]


def get_strategy_by_name(name: str) -> Optional[TrendStrategy]:
    """根据名称获取策略"""
    strategies = {s.name: s for s in get_default_strategies()}
    return strategies.get(name)


__all__ = [
    'StrategyResult',
    'TrendStrategy',
    'BaseStrategy',
    'HighGrowthStrategy',
    'TurnaroundStrategy',
    'StableDividendStrategy',
    'CyclicalBottomStrategy',
    'MoatDefenseStrategy',
    'get_default_strategies',
    'get_strategy_by_name',
]
