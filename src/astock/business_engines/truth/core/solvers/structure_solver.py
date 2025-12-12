"""
Structure Solver - 结构求解器 v3.0 终极版
==========================================

功能：预测财务斜率的合理值（周期位置感知动态通道）

物理隐喻：
- 结构如同建筑骨架，决定稳定性
- 不同结构有不同的承重能力
- v3.0: 周期位置决定斜率通道，拐点加速杀

上帝方程 III v3.0 (周期动态通道版):
$$
T_{slope} = \\begin{cases}
BaseSlope × (1 + Boost) & \\text{if CyclePos < 0.3 (底部)} \\\\
BaseSlope × (1 - Decay) & \\text{if CyclePos > 0.7 (顶部)} \\\\
BaseSlope & \\text{otherwise (中部)}
\\end{cases}
$$

其中:
$$
BaseSlope = -0.02 + k_1(1-β) + k_3(γ×V_{gate}) - k_2×δ_{decay}×(1 + ω×β)
$$

v3.0 核心进化（融合 Gemini 建议 + 我们的硬触发）:
1. 周期位置感知动态通道: 底部放大正斜率，顶部压缩斜率
2. 拐点加速杀: δ_decay × (1 + ω×β) 重资产衰退加速
3. V因子门控: V < 0.4 直接预期负斜率
4. 欺诈因子斜率惩罚: 造假公司不配有正斜率

与 v2.0 的关键差异:
| v2.0 | v3.0 | 效果 |
|------|------|------|
| 线性周期调整 | 动态通道乘数 | 非线性放大/压缩 |
| δ_decay独立 | δ_decay×(1+ωβ) | 重资产衰退更痛 |
| V_eff加乘 | V门控否决 | V<0.4负斜率 |

作者: AStock Analysis System
日期: 2025-01 (v3.0)
"""

import math
from typing import Optional, Tuple
from dataclasses import dataclass

from ...models import CompanyGenome, SlopeResult
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


def infer_cycle_position(alpha: float, delta_decay: float) -> float:
    """从基因推断周期位置（与velocity_solver共享逻辑）"""
    if alpha < 0.3:
        return 0.5
    base_position = delta_decay
    confidence = min(1.0, alpha * 1.5)
    cycle_position = 0.5 + (base_position - 0.5) * confidence
    return max(0.0, min(1.0, cycle_position))


def compute_decay_asset_interaction(delta_decay: float, beta: float, omega: float = 0.5) -> float:
    """
    计算衰退-资产交互项 v3.0 (Gemini 核心建议)

    核心洞见：重资产公司衰退更致命
    - 高固定成本，难以转型
    - 折旧压力大
    - 产能过剩问题

    公式: interaction = δ_decay × (1 + ω × β)

    效果:
    | δ_decay | β   | 旧版 | 新版 | 加速比 |
    |---------|-----|------|------|--------|
    | 0.5     | 0.2 | 0.5  | 0.55 | 1.1x   |
    | 0.5     | 0.8 | 0.5  | 0.70 | 1.4x   |
    | 0.8     | 0.8 | 0.8  | 1.12 | 1.4x   |
    """
    return delta_decay * (1.0 + omega * beta)


def compute_cycle_channel_multiplier(cycle_position: float, alpha: float) -> Tuple[float, str]:
    """
    计算周期位置动态通道乘数 v3.1 (非线性惩罚版)

    核心洞见：周期不同阶段，斜率预期截然不同

    v3.1 优化（来自Gemini建议）:
    - 底部区: 线性放大（机会均匀分布）
    - 顶部区: 指数压缩（高处不胜寒，风险指数级上升）

    效果对比:
    | Pos  | v3.0 线性乘数 | v3.1 指数乘数 |
    |------|---------------|---------------|
    | 0.75 | ×0.87         | ×0.88         |
    | 0.85 | ×0.60         | ×0.52         |
    | 0.95 | ×0.33         | ×0.14         | (更狠！)

    Returns:
        (multiplier, channel_name): 通道乘数和通道名称
    """
    if alpha < 0.3:
        # 非周期股，不应用通道调整
        return 1.0, "non_cyclic"

    if cycle_position < 0.3:
        # 周期底部：反弹预期，线性放大正斜率
        boost = 0.5 * (0.3 - cycle_position) / 0.3  # 最大+50%
        return 1.0 + boost * alpha, "bottom_boost"

    elif cycle_position > 0.7:
        # 周期顶部：指数压缩斜率（高处不胜寒）
        # 使用指数衰减：mult = exp(-k×(pos-0.7)×α)
        # k=4 时，pos=0.95, α=0.8 → mult ≈ 0.14
        exp_decay = math.exp(-4 * (cycle_position - 0.7) * alpha)
        multiplier = max(0.1, exp_decay)  # 封底 0.1，防止完全归零
        return multiplier, "top_decay_exp"

    else:
        # 中部区：中性
        return 1.0, "neutral"


def compute_fraud_slope_penalty(delta_fraud: float, threshold: float = 0.3) -> float:
    """
    计算欺诈因子斜率惩罚 v3.0

    核心洞见：造假公司的"增长斜率"不可信

    - δ_fraud > 0.3: 斜率预期被打折
    - δ_fraud > 0.5: 斜率预期大幅打折甚至转负

    Returns:
        penalty_factor: 斜率惩罚因子 [0, 1]，越小越严重
    """
    if delta_fraud <= threshold:
        return 1.0

    # 超过阈值后线性惩罚
    excess = delta_fraud - threshold
    penalty = 1.0 - (excess / (1.0 - threshold))  # 线性降到0
    return max(0.0, penalty)


@dataclass
class StructureSolverResult:
    """
    结构求解器结果（SlopeResult 的包装）

    Attributes:
        slope: 完整的斜率计算结果
        interpretation: 结果解读
    """
    slope: SlopeResult
    interpretation: str = ""

    @property
    def expected_slope(self) -> float:
        """预期斜率"""
        return self.slope.expected_slope

    @property
    def slope_quality(self) -> str:
        """斜率质量评估"""
        if self.slope.expected_slope > 0.01:
            return "改善"
        elif self.slope.expected_slope > -0.01:
            return "稳定"
        elif self.slope.expected_slope > -0.03:
            return "轻微恶化"
        else:
            return "显著恶化"


def structure_solver(
    genome: CompanyGenome,
    config: TruthConfig = None,
) -> SlopeResult:
    """
    结构求解器 v3.0 终极版 - 预测财务斜率（周期动态通道版）

    核心进化（融合 Gemini 建议 + 我们的硬触发）:
    1. 周期位置感知动态通道
    2. 拐点加速杀: δ_decay × (1 + ω×β)
    3. V因子门控
    4. 欺诈因子斜率惩罚

    上帝方程 III v3.0:
    $$
    BaseSlope = -0.02 + k_1(1-β) + k_3(γ×V_{gate}) - k_2×δ_{decay}×(1+ω×β)
    T_{slope} = BaseSlope × ChannelMultiplier × FraudPenalty
    $$

    与 v2.0 的关键差异:
    | v2.0 | v3.0 | 效果 |
    |------|------|------|
    | 线性周期调整 | 动态通道乘数 | 非线性放大/压缩 |
    | δ_decay独立 | δ_decay×(1+ωβ) | 重资产衰退更痛 |
    | V_eff加乘 | V门控否决 | V<0.4负斜率 |

    Args:
        genome: 公司六维基因组
        config: T.R.U.T.H. 配置

    Returns:
        SlopeResult: 斜率预测结果
    """
    if config is None:
        config = get_default_truth_config()

    solver = config.solver

    # ============================================
    # 1. 自然衰退基线（熵增定律）
    # ============================================
    natural_decay = solver.natural_decay_rate  # 默认 -0.02

    # ============================================
    # 2. k₁(1-β): 轻资产优势
    # ============================================
    asset_advantage = solver.k1_structure * (1 - genome.beta)

    # ============================================
    # 3. V因子门控 v3.0
    # ============================================
    v_gated, gate_passed = transform_verification_v3(genome.verification)

    # 门控失败 → 预期负斜率
    if not gate_passed:
        return SlopeResult(
            natural_decay=natural_decay,
            asset_advantage=0.0,
            growth_support=0.0,
            decay_acceleration=0.0,
            cycle_adjustment=0.0,
            cycle_position=0.5,
            v_effective=0.0,
            expected_slope=-0.05,  # 一票否决：负斜率
            gate_passed=False,
            channel_multiplier=1.0,
            channel_name="gate_failed",
            fraud_penalty=0.0,
        )

    # ============================================
    # 4. k₃(γ×V_gate): 真成长支撑 v3.0
    # ============================================
    k3_structure = getattr(solver, 'k3_structure', 0.03)
    growth_support = k3_structure * genome.gamma * v_gated

    # ============================================
    # 5. 衰退-资产交互加速杀 v3.0 (Gemini核心建议)
    # δ_decay × (1 + ω × β)
    # ============================================
    omega = getattr(solver, 'omega_decay_asset', 0.5)
    decay_interaction = compute_decay_asset_interaction(genome.delta_decay, genome.beta, omega)
    decay_acceleration = solver.k2_structure * decay_interaction

    # ============================================
    # 6. 周期位置动态通道 v3.0 (Gemini核心建议)
    # ============================================
    cycle_position = infer_cycle_position(genome.alpha, genome.delta_decay)
    channel_multiplier, channel_name = compute_cycle_channel_multiplier(cycle_position, genome.alpha)

    # ============================================
    # 7. 欺诈因子斜率惩罚 v3.0
    # ============================================
    fraud_penalty = compute_fraud_slope_penalty(genome.delta_fraud)

    # ============================================
    # 上帝方程 III v3.0 (动态通道版)
    # ============================================
    # 基础斜率
    base_slope = (
        natural_decay
        + asset_advantage
        + growth_support
        - decay_acceleration
    )

    # 应用动态通道乘数 + 欺诈惩罚
    expected_slope = base_slope * channel_multiplier * fraud_penalty

    # 斜率边界保护
    expected_slope = max(expected_slope, solver.slope_floor)
    expected_slope = min(expected_slope, solver.slope_ceiling)

    return SlopeResult(
        natural_decay=natural_decay,
        asset_advantage=asset_advantage,
        growth_support=growth_support,
        decay_acceleration=decay_acceleration,
        cycle_adjustment=0.0,  # v3.0: 改用channel_multiplier
        cycle_position=cycle_position,
        v_effective=v_gated,
        expected_slope=expected_slope,
        gate_passed=gate_passed,           # v3.0新增
        channel_multiplier=channel_multiplier,  # v3.0新增
        channel_name=channel_name,         # v3.0新增
        fraud_penalty=fraud_penalty,       # v3.0新增
    )


def create_structure_result(
    genome: CompanyGenome,
    config: TruthConfig = None,
) -> StructureSolverResult:
    """
    创建 StructureSolverResult 对象（便捷工厂函数）v3.0
    """
    slope = structure_solver(genome, config)

    # 生成解读
    interpretations = []

    # v3.0 门控检测
    gate_passed = getattr(slope, 'gate_passed', True)
    if not gate_passed:
        interpretation = "🚫 V门控失败 → 负斜率预期"
        return StructureSolverResult(slope=slope, interpretation=interpretation)

    # v3.0 动态通道
    channel_name = getattr(slope, 'channel_name', 'neutral')
    channel_mult = getattr(slope, 'channel_multiplier', 1.0)
    if channel_name == "bottom_boost":
        interpretations.append(f"📈周期底部通道(×{channel_mult:.2f})")
    elif channel_name == "top_decay":
        interpretations.append(f"📉周期顶部通道(×{channel_mult:.2f})")

    if slope.asset_advantage > 0.01:
        interpretations.append(f"轻资产优势 +{slope.asset_advantage:.1%}")

    if slope.decay_acceleration > 0.01:
        interpretations.append(f"衰退加速 -{slope.decay_acceleration:.1%}")

    growth_support = getattr(slope, 'growth_support', 0)
    if growth_support > 0.01:
        interpretations.append(f"成长支撑 +{growth_support:.1%}")

    # v3.0 欺诈惩罚
    fraud_pen = getattr(slope, 'fraud_penalty', 1.0)
    if fraud_pen < 0.8:
        interpretations.append(f"⚠️欺诈折扣(×{fraud_pen:.2f})")

    # 斜率质量判断
    if slope.expected_slope > 0.01:
        quality = "改善趋势 📈"
    elif slope.expected_slope > -0.01:
        quality = "相对稳定 ➡️"
    elif slope.expected_slope > -0.03:
        quality = "轻微恶化 📉"
    else:
        quality = "显著恶化 ⚠️"

    interpretation = f"预期斜率 {slope.expected_slope:.1%} ({quality})"
    if interpretations:
        interpretation += " [" + ", ".join(interpretations) + "]"

    return StructureSolverResult(
        slope=slope,
        interpretation=interpretation,
    )
