"""
趋势分析策略模块
================

定义基于趋势分析结果的高级选股策略，如"高增长优质"、"困境反转"等。
这些策略独立于基础评分规则，用于生成特定的选股标签。
"""

from dataclasses import dataclass, field
from typing import List, Protocol, Optional, Dict, Any
import math
from .models import TrendContext

@dataclass
class StrategyResult:
    name: str
    matched: bool
    reason: str = ""
    score_boost: float = 0.0  # 策略匹配后的额外加分（可选）
    metadata: Dict[str, Any] = field(default_factory=dict) # 存储计算中间值，便于调试

class TrendStrategy(Protocol):
    name: str
    description: str

    def evaluate(self, context: TrendContext) -> StrategyResult:
        """评估当前上下文是否符合策略定义"""
        ...

class BaseStrategy:
    """策略基类，提供指标类型识别和自适应阈值"""

    def __init__(self, custom_thresholds: Optional[Dict[str, float]] = None):
        self.custom_thresholds = custom_thresholds or {}

    def _is_efficiency_metric(self, metric_name: str) -> bool:
        """判断是否为效率/比率类指标 (ROIC, ROE, Margin)"""
        keywords = ["roic", "roe", "margin", "rate", "ratio", "yield", "percent"]
        return any(k in metric_name.lower() for k in keywords)

    def _get_adaptive_threshold(self, metric_name: str, key: str, default: float) -> float:
        if key in self.custom_thresholds:
            return self.custom_thresholds[key]

        metric = metric_name.lower()

        # === 绝对值门槛 (min_value) ===
        if key == "min_value":
            # 效率指标
            if "net_margin" in metric: return 10.0     # 净利率 > 10% 算优质
            if "gross_margin" in metric: return 40.0   # 毛利率 > 40% 算优质
            if "roe" in metric: return 15.0            # ROE > 15%
            if "roic" in metric: return 15.0           # ROIC > 15%
            # 规模指标 (假设单位统一，这里较难给通用值，通常依赖增长率)
            return default

        # === 增长率门槛 (min_growth) ===
        if key == "min_growth":
            # 营收/利润要求更高增速
            if "revenue" in metric or "profit" in metric: return 0.20  # 20% 增长
            # 效率指标不要求高增速，只要不跌即可
            if self._is_efficiency_metric(metric): return 0.0

        return default

class HighGrowthStrategy(BaseStrategy):
    """
    高增长/优质护城河策略 (High Growth & Quality Moat)

    根据指标类型自动切换逻辑：

    [模式 A: 效率指标 (ROIC, ROE, Margin)] -> 寻找"护城河"
    - 核心：高位企稳 (High & Stable)
    - 标准：最新值极高 (>20%) + 趋势稳健 (R²高) + 未恶化 (斜率>=0)

    [模式 B: 规模指标 (Revenue, Profit)] -> 寻找"瞪羚"
    - 核心：高速奔跑 (Fast Growth)
    - 标准：CAGR 极高 (>20%) + 加速状态
    """
    name = "high_growth"
    description = "高增长/优质护城河"

    def evaluate(self, context: TrendContext) -> StrategyResult:
        if math.isnan(context.latest_value) or math.isnan(context.log_slope):
            return StrategyResult(self.name, False)

        metric_type = "efficiency" if self._is_efficiency_metric(context.metric_name) else "scale"

        # === 模式 A: 效率指标 (寻找护城河) ===
        if metric_type == "efficiency":
            # 门槛要求更高：ROIC至少要15%，甚至20%
            min_value = self._get_adaptive_threshold(context.metric_name, "min_value", 15.0)

            # 1. 绝对值必须足够高 (这是核心)
            if context.latest_value < min_value:
                return StrategyResult(self.name, False)

            # 2. 趋势不能恶化 (斜率 >= -0.02，允许微幅波动，但不能明显下降)
            if context.log_slope < -0.02:
                return StrategyResult(self.name, False)

            # 3. 必须稳健 (忽上忽下的不算护城河)
            if context.r_squared < 0.4 and context.cv > 0.2:
                return StrategyResult(self.name, False)

            return StrategyResult(
                self.name, True,
                f"优质护城河(高位企稳: {context.latest_value:.1f} > {min_value}, 波动率CV={context.cv:.2f})",
                score_boost=15.0
            )

        # === 模式 B: 规模指标 (寻找高成长) ===
        else:
            min_growth = self._get_adaptive_threshold(context.metric_name, "min_growth", 0.20)

            # 1. 增速必须快
            if context.log_slope < min_growth:
                return StrategyResult(self.name, False)

            # 2. 必须是真实增长 (R²不能太低)
            if context.r_squared < 0.5:
                return StrategyResult(self.name, False)

            # 3. 最好在加速
            if not context.is_accelerating and context.recent_3y_slope < context.log_slope:
                # 如果没加速，但增速极高 (>30%)，也放行
                if context.log_slope < 0.30:
                    return StrategyResult(self.name, False)

            return StrategyResult(
                self.name, True,
                f"高速成长(CAGR={context.log_slope:.1%}, 加速={context.is_accelerating})",
                score_boost=15.0
            )

class TurnaroundStrategy(BaseStrategy):
    """
    困境反转策略 (Turnaround)

    专业级判断：
    1. 拒绝"死猫跳"：要求近3年趋势强劲 (recent_slope > 0.2)
    2. 拒绝"假反转"：要求最新值必须回到安全线以上 (recovery_threshold)
    3. 区分"V型反转"与"底部磨底"
    """
    name = "turnaround"
    description = "困境反转/由亏转盈"

    def evaluate(self, context: TrendContext) -> StrategyResult:
        if math.isnan(context.latest_value):
            return StrategyResult(self.name, False)

        # 设定安全线：反转必须是有效的
        # 对于净利率，回到 2% 以上才算活过来；对于 ROIC，回到 5% 以上
        recovery_threshold = 5.0
        if "net_margin" in context.metric_name.lower(): recovery_threshold = 2.0
        if "gross_margin" in context.metric_name.lower(): recovery_threshold = 15.0

        # 1. 必须已经"活过来"了 (最新值 > 安全线)
        if context.latest_value < recovery_threshold:
            return StrategyResult(self.name, False)

        # 2. 必须确认复苏趋势 (最新值 > 5年均值 OR 均值为负)
        # 如果均值是正的，最新值必须超过均值，证明走出了低谷
        if context.weighted_avg > 0 and context.latest_value < context.weighted_avg * 0.9:
            return StrategyResult(self.name, False)

        # 3. 动能必须强劲 (近3年斜率 > 0.15)
        if context.recent_3y_slope < 0.15:
            return StrategyResult(self.name, False)

        reason = ""
        is_turnaround = False

        # 场景 A: 扭亏为盈 (最强信号)
        if context.has_loss_years and context.latest_value > recovery_threshold:
            is_turnaround = True
            reason = f"扭亏为盈(曾亏损{context.loss_year_count}年 -> 最新{context.latest_value:.1f})"

        # 场景 B: 深度V型反转
        elif context.inflection_type == "deterioration_to_recovery":
            is_turnaround = True
            reason = f"V型反转(形态确认, 斜率改善{context.slope_change:.2f})"

        # 场景 C: 底部困境反转 (虽然没亏，但之前跌得很惨，现在猛拉)
        # 条件：总跌幅曾很大，但近期斜率极高
        elif context.total_decline_pct > 30 and context.recent_3y_slope > 0.3:
            is_turnaround = True
            reason = f"底部强力反转(曾跌{context.total_decline_pct:.0f}%, 近期斜率{context.recent_3y_slope:.2f})"

        if is_turnaround:
            return StrategyResult(self.name, True, reason, score_boost=10.0)

        return StrategyResult(self.name, False)

def get_default_strategies() -> List[TrendStrategy]:
    return [
        HighGrowthStrategy(),
        TurnaroundStrategy(),
    ]