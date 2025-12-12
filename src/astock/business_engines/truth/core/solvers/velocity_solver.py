"""
Velocity Solver - 速度求解器 v3.0 终极版
=========================================

功能：计算预期增长率的物理边界

物理隐喻：
- 增长如同物体运动，不能超越"光速"
- 每个公司有自己的速度极限
- v3.0: 引入空气动力学阻力系数

上帝方程 II v3.0 (空气动力学版):
$$
T_{growth} = \\frac{GDP_g + k_1(γ×E×V_{gate})}{1 + C_D} × I(V > V_{min}) - Penalty_{decay}
$$

其中阻力系数:
$$
C_D = w_1×β + w_2×(1-MarketCapRank) + w_3×δ_{fraud}
$$

v3.0 核心进化（融合 Gemini 建议 + 我们的硬触发）:
1. 空气阻力系数 C_D: 重资产、小盘股、欺诈公司减速
2. V因子一票否决: V < V_min 直接零增长
3. 衰退硬惩罚: 独立于阻力的额外惩罚
4. 周期位置感知: 底部给加成，顶部给折扣

与 v2.0 的关键差异:
| v2.0 | v3.0 | 效果 |
|------|------|------|
| 线性相加 | 阻力作分母 | 非线性衰减 |
| 无规模因子 | C_D含规模 | 小盘股难高增长 |
| V_eff加乘 | V门控否决 | V<0.4零增长 |

作者: AStock Analysis System
日期: 2025-01 (v3.0)
"""

import math
from typing import Optional, Tuple
from dataclasses import dataclass

from ...models import CompanyGenome, GrowthBoundResult
from ...config import TruthConfig, get_default_truth_config


# ============================================================================
# v3.0 核心函数
# ============================================================================

def transform_verification_v3(v: float, v_min: float = 0.4) -> Tuple[float, bool]:
    """
    V因子非线性变换 v3.0 —— S型变换 + 门控

    Returns:
        (v_transformed, gate_passed): 变换后的V值和是否通过门控
    """
    v_eff = 1 / (1 + math.exp(-8 * (v - 0.5)))
    gate_passed = v >= v_min
    v_gated = v_eff if gate_passed else 0.0
    return v_gated, gate_passed


def compute_drag_coefficient(
    beta: float,
    delta_fraud: float,
    market_cap_rank: float = 0.5,  # 0=最小, 1=最大
    w1: float = 0.3,
    w2: float = 0.2,
    w3: float = 0.4
) -> float:
    """
    计算空气阻力系数 v3.0 (Gemini 核心建议)

    核心洞见：成长像空气中飞行，阻力是分母

    公式: C_D = w₁×β + w₂×(1-MarketCapRank) + w₃×δ_fraud

    阻力来源:
    - β (重资产): 重飞机飞不快，轻资产才能高速增长
    - MarketCapRank (规模): 小盘股阻力大（基数小增长率高但不可持续）
    - δ_fraud (欺诈): 造假公司的"增长"需要怀疑

    效果示例:
    | β   | 规模 | δ_fraud | C_D  | 增长衰减 |
    |-----|------|---------|------|----------|
    | 0.2 | 大   | 0.0     | 0.06 | 1.06x    |
    | 0.8 | 小   | 0.0     | 0.44 | 1.44x    |
    | 0.5 | 中   | 0.5     | 0.45 | 1.45x    |
    | 0.8 | 小   | 0.5     | 0.64 | 1.64x    |

    v3.1 优化（来自Gemini建议）:
    - 欺诈乘数效应：fraud > 0.3 时，阻力呈二次方增长
    - fraud > 0.5: 阻力翻倍
    - fraud > 0.6: 阻力 x4
    - 让造假公司"无法起飞"
    """
    # 基础阻力（结构性因素）
    C_D_base = w1 * beta + w2 * (1 - market_cap_rank)

    # 欺诈乘数：fraud > 0.3 开始二次方惩罚
    # fraud = 0.3 → mult = 1.0
    # fraud = 0.5 → mult = 2.0
    # fraud = 0.6 → mult = 3.25
    # fraud = 0.7 → mult = 5.0
    fraud_excess = max(0, delta_fraud - 0.3)
    fraud_multiplier = 1 + (fraud_excess * 5) ** 2

    # 最终阻力 = 基础阻力 × 欺诈乘数
    return C_D_base * fraud_multiplier


def infer_cycle_position(alpha: float, delta_decay: float) -> float:
    """
    从基因推断周期位置 v2.0+ (保留我们的优势)

    周期位置 S ∈ [0, 1]:
    - S = 0: 周期底部（即将反弹）
    - S = 0.5: 周期中部
    - S = 1: 周期顶部（即将下跌）

    推断逻辑:
    - 高α + 高δ_decay → 周期顶部/下行区
    - 高α + 低δ_decay → 周期底部/上行区
    - 低α → 中性位置
    """
    if alpha < 0.3:
        return 0.5

    base_position = delta_decay
    confidence = min(1.0, alpha * 1.5)
    cycle_position = 0.5 + (base_position - 0.5) * confidence

    return max(0.0, min(1.0, cycle_position))


def compute_cycle_growth_modifier(cycle_position: float, alpha: float) -> float:
    """
    计算周期位置对增长的修正 v3.1 (非线性惩罚版)

    核心洞见：
    - 周期底部：线性加成（机会均匀分布）
    - 周期顶部：指数惩罚（高处不胜寒，风险指数级上升）

    v3.1 优化（来自Gemini建议）:
    - 顶部(Pos > 0.7): 使用指数惩罚 exp(3×(pos-0.7))-1
    - 底部(Pos < 0.3): 保持线性加成
    - 封底 -15%，防止极端情况

    效果对比:
    | Pos  | v3.0 线性 | v3.1 指数 |
    |------|-----------|----------|
    | 0.75 | -0.20α    | -0.16α   |
    | 0.85 | -0.28α    | -0.55α   |
    | 0.95 | -0.36α    | -1.12α   | (封底-0.15)
    """
    if alpha < 0.3:
        return 0.0  # 非周期股无修正

    if cycle_position > 0.7:
        # 顶部风险指数放大（高处不胜寒）
        penalty = -alpha * (math.exp(3 * (cycle_position - 0.7)) - 1)
        return max(penalty, -0.15)  # 封底 -15%

    elif cycle_position < 0.3:
        # 底部奖励保持线性
        return alpha * (0.3 - cycle_position) * 2  # 最大 +0.6α

    else:
        # 中部区：中性
        return 0.0


@dataclass
class VelocitySolverResult:
    """
    速度求解器结果（GrowthBoundResult 的包装）

    Attributes:
        growth_bound: 完整的增长边界计算结果
        interpretation: 结果解读
    """
    growth_bound: GrowthBoundResult
    interpretation: str = ""

    @property
    def max_sustainable_growth(self) -> float:
        """最大可持续增长率"""
        return self.growth_bound.max_sustainable_growth

    @property
    def min_expected_growth(self) -> float:
        """最小期望增长率"""
        return self.growth_bound.min_expected_growth


def velocity_solver(
    genome: CompanyGenome,
    config: TruthConfig = None,
    market_cap_rank: float = 0.5,  # v3.0新增：市值排名
) -> GrowthBoundResult:
    """
    速度求解器 v3.0 终极版 - 计算增长边界（空气动力学版）

    核心进化（融合 Gemini 建议 + 我们的硬触发）:
    1. 空气阻力系数 C_D 作为分母
    2. V因子一票否决
    3. 周期位置增长修正
    4. 衰退硬惩罚

    上帝方程 II v3.0:
    $$
    T_{growth} = \\frac{GDP_g + k_1(γ×E×V_{gate})}{1 + C_D} + CycleModifier - DecayPenalty
    $$

    与 v2.0 的关键差异:
    | v2.0 | v3.0 | 效果 |
    |------|------|------|
    | 线性相加 | 阻力作分母 | 非线性衰减 |
    | 无规模因子 | C_D含规模 | 小盘股难高增长 |
    | V_eff加乘 | V门控否决 | V<0.4零增长 |

    Args:
        genome: 公司六维基因组
        config: T.R.U.T.H. 配置
        market_cap_rank: 市值排名 [0,1]，0=最小，1=最大

    Returns:
        GrowthBoundResult: 增长边界计算结果
    """
    if config is None:
        config = get_default_truth_config()

    macro = config.macro
    solver = config.solver

    # ============================================
    # 1. GDP_g: 经济增长基准
    # ============================================
    gdp_growth = macro.gdp_nominal_growth

    # ============================================
    # 2. E: 市场情绪因子
    # ============================================
    E = macro.market_sentiment_factor

    # ============================================
    # 3. V因子门控 v3.0
    # ============================================
    v_gated, gate_passed = transform_verification_v3(genome.verification)

    # 门控失败 → 零增长预期
    if not gate_passed:
        return GrowthBoundResult(
            gdp_growth=gdp_growth,
            true_growth_boost=0.0,
            cycle_reversal=0.0,
            decay_adjustment=0.0,
            cycle_position=0.5,
            v_effective=0.0,
            max_sustainable_growth=0.0,  # 一票否决：零增长
            min_expected_growth=-0.05,  # 负增长预期
            gate_passed=False,
            drag_coefficient=1.0,
        )

    # ============================================
    # 4. 空气阻力系数 C_D v3.0 (Gemini核心建议)
    # ============================================
    w1 = getattr(solver, 'drag_w1_beta', 0.3)
    w2 = getattr(solver, 'drag_w2_size', 0.2)
    w3 = getattr(solver, 'drag_w3_fraud', 0.4)

    drag_coefficient = compute_drag_coefficient(
        beta=genome.beta,
        delta_fraud=genome.delta_fraud,
        market_cap_rank=market_cap_rank,
        w1=w1, w2=w2, w3=w3
    )

    # ============================================
    # 5. 真成长加速（门控后）
    # ============================================
    true_growth_boost = solver.k1_velocity * genome.gamma * E * v_gated

    # ============================================
    # 6. 周期位置增长修正 v3.0
    # ============================================
    cycle_position = infer_cycle_position(genome.alpha, genome.delta_decay)
    cycle_modifier = compute_cycle_growth_modifier(cycle_position, genome.alpha)

    # 周期修正乘上基础增长
    cycle_growth_adj = cycle_modifier * gdp_growth

    # ============================================
    # 7. 衰退硬惩罚 v3.0
    # ============================================
    k3_decay = getattr(solver, 'k3_velocity_decay', 0.08)
    decay_penalty = k3_decay * genome.delta_decay

    # ============================================
    # 上帝方程 II v3.0 (空气动力学版)
    # ============================================
    # 基础增长动力
    growth_thrust = gdp_growth + true_growth_boost

    # 阻力衰减（核心创新）
    growth_after_drag = growth_thrust / (1.0 + drag_coefficient)

    # 周期修正 + 衰退惩罚
    max_sustainable_growth = growth_after_drag + cycle_growth_adj - decay_penalty

    # 增长边界保护
    max_sustainable_growth = max(max_sustainable_growth, solver.growth_floor)
    max_sustainable_growth = min(max_sustainable_growth, solver.growth_ceiling)

    # 最小期望增长
    min_base = gdp_growth - 0.02
    if genome.delta_decay > 0.5:
        min_base = gdp_growth - 0.05
    min_expected_growth = max(min_base, solver.growth_floor)

    return GrowthBoundResult(
        gdp_growth=gdp_growth,
        true_growth_boost=true_growth_boost,
        cycle_reversal=cycle_growth_adj,  # v3.0: 改名为周期调整
        decay_adjustment=-decay_penalty,
        cycle_position=cycle_position,
        v_effective=v_gated,
        max_sustainable_growth=max_sustainable_growth,
        min_expected_growth=min_expected_growth,
        gate_passed=gate_passed,  # v3.0新增
        drag_coefficient=drag_coefficient,  # v3.0新增
    )


def create_velocity_result(
    genome: CompanyGenome,
    config: TruthConfig = None,
    market_cap_rank: float = 0.5,
) -> VelocitySolverResult:
    """
    创建 VelocitySolverResult 对象（便捷工厂函数）v3.0
    """
    growth_bound = velocity_solver(genome, config, market_cap_rank)

    # 生成解读
    interpretations = []

    # v3.0 门控检测
    gate_passed = getattr(growth_bound, 'gate_passed', True)
    if not gate_passed:
        interpretation = "🚫 V门控失败 → 零增长预期"
        return VelocitySolverResult(growth_bound=growth_bound, interpretation=interpretation)

    # v3.0 阻力系数
    drag = getattr(growth_bound, 'drag_coefficient', 0)
    if drag > 0.3:
        interpretations.append(f"⚠️ 阻力系数={drag:.2f}")

    if growth_bound.true_growth_boost > 0.02:
        interpretations.append(f"真成长加速 +{growth_bound.true_growth_boost:.1%}")

    if abs(growth_bound.cycle_reversal) > 0.01:
        sign = "+" if growth_bound.cycle_reversal > 0 else ""
        interpretations.append(f"周期调整 {sign}{growth_bound.cycle_reversal:.1%}")

    decay_adj = getattr(growth_bound, 'decay_adjustment', 0)
    if decay_adj < -0.02:
        interpretations.append(f"衰退惩罚 {decay_adj:.1%}")

    # v3.0 周期位置解读
    cycle_pos = getattr(growth_bound, 'cycle_position', 0.5)
    if cycle_pos < 0.3:
        interpretations.append("📈周期底部")
    elif cycle_pos > 0.7:
        interpretations.append("📉周期顶部")

    interpretation = (
        f"可持续增长 {growth_bound.min_expected_growth:.1%} ~ "
        f"{growth_bound.max_sustainable_growth:.1%}"
    )
    if interpretations:
        interpretation += " (" + ", ".join(interpretations) + ")"

    return VelocitySolverResult(
        growth_bound=growth_bound,
        interpretation=interpretation,
    )
