"""
趋势分析策略模块 (Trend Analysis Strategies)
=============================================

定义基于趋势分析结果的高级选股策略：
- HighGrowthStrategy: 高增长/优质护城河
- TurnaroundStrategy: 困境反转/由亏转盈
- StableDividendStrategy: 稳定分红型（新增）
- CyclicalBottomStrategy: 周期底部抄底（新增）

与 metric_adapter 整合，自动获取指标特性。

作者: AStock Analysis System
日期: 2025-12-06
"""

from dataclasses import dataclass, field
from typing import List, Protocol, Optional, Dict, Any
import math
from .models import TrendContext

# 导入指标适配器（可选，用于获取指标特性）
try:
    from .metric_adapter import AdapterFactory
    HAS_METRIC_ADAPTER = True
except ImportError:
    HAS_METRIC_ADAPTER = False


@dataclass
class StrategyResult:
    """策略评估结果"""
    name: str
    matched: bool
    reason: str = ""
    score_boost: float = 0.0  # 策略匹配后的额外加分
    confidence: float = 0.0   # 置信度 (0-1)
    metadata: Dict[str, Any] = field(default_factory=dict)


class TrendStrategy(Protocol):
    """策略协议定义"""
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

        # === 新增：专业级质量过滤 ===

        # 1. 拒绝"有毒增长" (Toxic Growth)
        # 如果是营收/利润指标，必须检查 ROIC/ROE 是否及格
        if metric_type == "scale":
            roe_stats = context.reference_metrics.get("roe")
            if roe_stats and roe_stats.get("latest", 0) < 5.0:
                return StrategyResult(self.name, False, "增长无效(ROE<5%)")

        # 2. 拒绝"高波动伪增长"
        # A股很多公司是周期性的，某一年暴涨 100% 会拉高斜率
        # 强制要求 Mann-Kendall 必须显著 (p_value < 0.1) 才能算"真成长"
        if context.log_slope > 0.3 and context.mann_kendall_p_value > 0.1:
             return StrategyResult(self.name, False, "增长不显著(可能是单年脉冲)")

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
            return StrategyResult(self.name, True, reason, score_boost=10.0, confidence=0.7)

        return StrategyResult(self.name, False)


class StableDividendStrategy(BaseStrategy):
    """
    稳定分红型策略 (Stable Dividend / Cash Cow)

    特征：
    - 高且稳定的盈利能力 (ROE/ROIC)
    - 极低的波动性 (CV < 0.15)
    - 无明显衰退趋势
    - 现金流健康

    适用于：消费、公用事业、部分金融股
    """
    name = "stable_dividend"
    description = "稳定分红/现金奶牛"

    def evaluate(self, context: TrendContext) -> StrategyResult:
        if math.isnan(context.latest_value):
            return StrategyResult(self.name, False)

        metric_type = "efficiency" if self._is_efficiency_metric(context.metric_name) else "scale"

        # 此策略主要针对效率指标
        if metric_type != "efficiency":
            return StrategyResult(self.name, False, "仅适用于效率指标")

        # 1. 绝对值必须高
        min_value = self._get_adaptive_threshold(context.metric_name, "min_value", 12.0)
        if context.latest_value < min_value:
            return StrategyResult(self.name, False, f"绝对值不足({context.latest_value:.1f}<{min_value})")

        # 2. 波动性必须低
        if context.cv > 0.20:
            return StrategyResult(self.name, False, f"波动过大(CV={context.cv:.2%})")

        # 3. 趋势不能明显恶化
        if context.log_slope < -0.05:
            return StrategyResult(self.name, False, f"趋势下行({context.log_slope:.1%})")

        # 4. 最新值不能大幅低于加权均值
        if context.latest_vs_weighted_ratio < 0.85:
            return StrategyResult(self.name, False, "近期表现不佳")

        # 5. 检查现金流（如果有参考指标）
        ocf_stats = context.reference_metrics.get("ocfps")
        if ocf_stats:
            ocf_slope = ocf_stats.get("log_slope", 0)
            if ocf_slope < -0.15:
                return StrategyResult(self.name, False, "现金流恶化")

        # 计算置信度
        confidence = min(
            (1.0 - context.cv / 0.2) * 0.4 +  # 波动越低置信度越高
            (context.latest_value / min_value - 1.0) * 0.3 +  # 绝对值越高置信度越高
            (context.latest_vs_weighted_ratio - 0.85) * 0.3,
            1.0
        )

        return StrategyResult(
            self.name, True,
            f"稳定分红型(值={context.latest_value:.1f},CV={context.cv:.1%},稳定)",
            score_boost=8.0,
            confidence=confidence
        )


class CyclicalBottomStrategy(BaseStrategy):
    """
    周期底部抄底策略 (Cyclical Bottom Fishing)

    特征：
    - 确认的周期性行业
    - 当前处于周期底部区域
    - 有复苏迹象（current_phase = rising）
    - FFT检测到周期模式（可选加分）

    风险提示：此策略需要额外的行业研究确认
    """
    name = "cyclical_bottom"
    description = "周期底部抄底"

    def evaluate(self, context: TrendContext) -> StrategyResult:
        # 1. 必须是周期股
        if not context.is_cyclical:
            return StrategyResult(self.name, False, "非周期行业")

        # 2. 直接从 context 获取周期位置（更专业的方式）
        cycle_position = context.cycle_position
        fft_period = context.fft_dominant_period

        if cycle_position not in ("bottom", "mid_up"):
            return StrategyResult(self.name, False, f"非底部区域({cycle_position})")

        # 3. 必须有复苏迹象
        if context.current_phase != "rising":
            return StrategyResult(self.name, False, "尚未开始回升")

        # 4. 近期趋势必须转正
        if context.recent_3y_slope < 0:
            return StrategyResult(self.name, False, "近期仍在下跌")

        # 构建原因和置信度
        reasons = [f"周期底部({cycle_position})"]
        confidence = 0.5

        if context.inflection_type == "deterioration_to_recovery":
            reasons.append("V型反转确认")
            confidence += 0.2

        if fft_period and fft_period > 0:
            reasons.append(f"FFT检测{fft_period:.1f}年周期")
            confidence += 0.15

        if context.recent_3y_slope > 0.1:
            reasons.append(f"近期强势({context.recent_3y_slope:.1%})")
            confidence += 0.15

        return StrategyResult(
            self.name, True,
            ", ".join(reasons),
            score_boost=12.0,  # 周期底部如果判断正确收益可观
            confidence=min(confidence, 1.0)
        )


class MoatDefenseStrategy(BaseStrategy):
    """
    护城河防守策略 (Moat Defense)

    特征：
    - 毛利率/净利率长期高位稳定
    - 即使行业波动，利润率也保持稳定
    - 适合追求长期稳定回报的投资者

    与 HighGrowthStrategy 的区别：
    - HighGrowth 侧重增长
    - MoatDefense 侧重稳定性和防御性
    """
    name = "moat_defense"
    description = "护城河防守/稳定盈利"

    def evaluate(self, context: TrendContext) -> StrategyResult:
        if math.isnan(context.latest_value):
            return StrategyResult(self.name, False)

        metric = context.metric_name.lower()

        # 此策略针对利润率指标
        if "margin" not in metric and "roe" not in metric and "roic" not in metric:
            return StrategyResult(self.name, False, "仅适用于利润率类指标")

        # 1. 绝对值必须处于行业领先水平
        moat_threshold = 40.0 if "gross" in metric else 15.0
        if context.latest_value < moat_threshold:
            return StrategyResult(self.name, False, f"未达护城河门槛({context.latest_value:.1f}<{moat_threshold})")

        # 2. 稳定性要求
        if context.cv > 0.15:
            return StrategyResult(self.name, False, f"波动过大(CV={context.cv:.1%})")

        # 3. 趋势要求：不允许明显下滑
        if context.log_slope < -0.03:
            return StrategyResult(self.name, False, "利润率侵蚀中")

        # 4. R² 要求：趋势必须清晰
        if context.r_squared < 0.5:
            return StrategyResult(self.name, False, "趋势不清晰")

        # 5. 交叉验证：如果有参考指标，检查一致性
        if "gross" in metric:
            nm_stats = context.reference_metrics.get("netprofit_margin")
            if nm_stats and nm_stats.get("log_slope", 0) < -0.10:
                return StrategyResult(self.name, False, "净利率下滑，毛利优势未转化")

        # 计算护城河强度
        moat_strength = (context.latest_value - moat_threshold) / moat_threshold
        confidence = min(
            moat_strength * 0.5 +
            (1.0 - context.cv / 0.15) * 0.3 +
            context.r_squared * 0.2,
            1.0
        )

        return StrategyResult(
            self.name, True,
            f"强护城河({context.latest_value:.1f}>{moat_threshold},CV={context.cv:.1%})",
            score_boost=10.0,
            confidence=confidence
        )


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