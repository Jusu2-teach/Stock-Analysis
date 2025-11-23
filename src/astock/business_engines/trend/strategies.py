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

    def _get_robust_growth_rate(self, context: TrendContext) -> float:
        """
        获取稳健的增长率指标 (针对 A 股高波动特性优化)

        逻辑：
        1. 如果波动率低 (CV < 0.15)，直接用普通斜率 (OLS)，反应更灵敏。
        2. 如果波动率高 (CV >= 0.15)，优先使用稳健斜率 (Theil-Sen)，
           因为它能忽略异常值 (Outliers) 的干扰，避免被单年暴涨误导。
        """
        if context.cv < 0.15:
            return context.log_slope
        else:
            # 如果 robust_slope 为 0 (计算失败或无趋势)，回退到 log_slope
            return context.robust_slope if context.robust_slope != 0 else context.log_slope

class HighGrowthStrategy(BaseStrategy):
    """
    高增长/优质护城河策略 (High Growth & Quality Moat)

    针对 A 股高波动特性的优化：
    1. 引入 'Robust Slope' (Theil-Sen 估算)：
       A 股常有单年暴雷或暴涨，普通线性回归会被拉偏。
       本策略在高波动时自动切换到稳健斜率，只看"中位数趋势"。
    2. 动态 R² 门槛：
       A 股很难有 R² > 0.8 的完美曲线。
       如果 Mann-Kendall 趋势显著 (p_value < 0.1)，允许 R² 稍低。
    """
    name = "high_growth"
    description = "高增长/优质护城河"

    def evaluate(self, context: TrendContext) -> StrategyResult:
        if math.isnan(context.latest_value) or math.isnan(context.log_slope):
            return StrategyResult(self.name, False)

        metric_type = "efficiency" if self._is_efficiency_metric(context.metric_name) else "scale"

        # 使用稳健增长率
        growth_rate = self._get_robust_growth_rate(context)

        # === 模式 A: 效率指标 (寻找护城河) ===
        if metric_type == "efficiency":
            min_value = self._get_adaptive_threshold(context.metric_name, "min_value", 15.0)

            # 1. 绝对值必须足够高
            if context.latest_value < min_value:
                return StrategyResult(self.name, False)

            # 2. 趋势不能恶化 (使用稳健斜率判断)
            if growth_rate < -0.02:
                return StrategyResult(self.name, False)

            # 3. 稳健性检查 (A股特供版)
            # 如果 Mann-Kendall 检验显示趋势显著向上 (tau > 0.4)，则放宽 R² 要求
            min_r2 = 0.4
            if context.mann_kendall_tau > 0.4:
                min_r2 = 0.2  # 趋势很强但波动大，允许 R² 低一点

            if context.r_squared < min_r2 and context.cv > 0.2:
                return StrategyResult(self.name, False)

            return StrategyResult(
                self.name, True,
                f"优质护城河(高位企稳: {context.latest_value:.1f} > {min_value}, 稳健斜率={growth_rate:.2f})",
                score_boost=15.0
            )

        # === 模式 B: 规模指标 (寻找高成长) ===
        else:
            min_growth = self._get_adaptive_threshold(context.metric_name, "min_growth", 0.20)

            # 1. 增速必须快 (使用稳健斜率)
            if growth_rate < min_growth:
                return StrategyResult(self.name, False)

            # 2. 真实性验证
            # 如果是高波动 (CV > 0.3)，必须要求 Mann-Kendall 确认趋势存在
            if context.cv > 0.3 and context.mann_kendall_tau <= 0:
                return StrategyResult(self.name, False) # 波动大且无显著趋势，可能是假增长

            # 3. 最好在加速
            if not context.is_accelerating and context.recent_3y_slope < growth_rate:
                if growth_rate < 0.30:
                    return StrategyResult(self.name, False)

            return StrategyResult(
                self.name, True,
                f"高速成长(稳健CAGR={growth_rate:.1%}, MK趋势={context.mann_kendall_tau:.2f})",
                score_boost=15.0
            )

class TurnaroundStrategy(BaseStrategy):
    """
    困境反转策略 (Turnaround)

    A 股实战优化：
    1. 过滤"消息面炒作"：要求必须有业绩兑现 (latest_value > recovery_threshold)。
    2. 过滤"单年脉冲"：要求 Robust Slope (稳健斜率) 必须转正，
       或者近期斜率 (recent_3y_slope) 极强。
    """
    name = "turnaround"
    description = "困境反转/由亏转盈"

    def evaluate(self, context: TrendContext) -> StrategyResult:
        if math.isnan(context.latest_value):
            return StrategyResult(self.name, False)

        # 设定安全线
        recovery_threshold = 5.0
        if "net_margin" in context.metric_name.lower(): recovery_threshold = 2.0
        if "gross_margin" in context.metric_name.lower(): recovery_threshold = 15.0

        # 1. 必须已经"活过来"了
        if context.latest_value < recovery_threshold:
            return StrategyResult(self.name, False)

        # 2. 必须确认复苏趋势
        if context.weighted_avg > 0 and context.latest_value < context.weighted_avg * 0.9:
            return StrategyResult(self.name, False)

        # 3. 动能必须强劲
        if context.recent_3y_slope < 0.15:
            return StrategyResult(self.name, False)

        # 4. A股特供：防骗线逻辑
        # 如果波动率极大 (CV > 0.5)，要求 Mann-Kendall 必须不能是显著负相关
        if context.cv > 0.5 and context.mann_kendall_tau < -0.2:
            return StrategyResult(self.name, False)

        reason = ""
        is_turnaround = False

        # 场景 A: 扭亏为盈
        if context.has_loss_years and context.latest_value > recovery_threshold:
            is_turnaround = True
            reason = f"扭亏为盈(曾亏损{context.loss_year_count}年 -> 最新{context.latest_value:.1f})"

        # 场景 B: 深度V型反转
        elif context.inflection_type == "deterioration_to_recovery":
            is_turnaround = True
            reason = f"V型反转(形态确认, 斜率改善{context.slope_change:.2f})"

        # 场景 C: 底部困境反转
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