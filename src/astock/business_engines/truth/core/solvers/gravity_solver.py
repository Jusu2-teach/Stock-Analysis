"""
Gravity Solver - 重力求解器 v3.0 终极版
========================================

功能：计算 ROIC/ROE 的动态阈值（分母）

物理隐喻：
- 资本必须克服"地心引力"才能创造价值
- 基准引力 = 无风险利率（资金的时间价值）
- 不同公司需要克服不同的引力
- v3.0: 引入非线性物理交互，风险是乘数而非加数

上帝方程 I v3.0 (非线性风险乘数版):
$$
T_{roic} = R_f × (1 + λ_{risk}·δ_{fraud}) + k_1·β·(1 + κ·α) - k_3(γ·E·V_{gate}) + k_4·δ_{decay}·(1 + φ·β)
$$

v3.0 核心进化（融合 Gemini 建议 + 我们的硬触发）:
1. 基准利率质量膨胀: R_f × (1 + λ×δ_fraud) —— 不纯的基因配更贵的钱
2. 双重杠杆交互项: β × (1 + κ×α) —— 重资产+强周期=毁灭机器
3. V因子门控: V_gate = V_eff × I(V > 0.4) —— S型非线性 + 一票否决
4. 衰退-资产交互: δ_decay × (1 + φ×β) —— 重资产衰退更致命
5. 硬触发机制: 熔断/拐点检测 —— 保留我们的优势

作者: AStock Analysis System
日期: 2025-01 (v3.0)
"""

import math
from typing import Optional, Tuple
from dataclasses import dataclass

from ...models import CompanyGenome, ThresholdResult
from ...config import TruthConfig, get_default_truth_config


# ============================================================================
# v3.0 核心函数
# ============================================================================

def transform_verification_v3(v: float) -> Tuple[float, bool]:
    """
    V因子非线性变换 v3.0 —— S型变换 + 门控

    融合方案:
    - S型非线性（我们的优势）
    - 一票否决门控（Gemini的建议）

    Returns:
        (v_transformed, gate_passed): 变换后的V值和是否通过门控
    """
    # S型变换
    v_eff = 1 / (1 + math.exp(-8 * (v - 0.5)))

    # 门控: V < 0.4 直接否决
    gate_passed = v >= 0.4

    # 门控后的有效值
    v_gated = v_eff if gate_passed else 0.0

    return v_gated, gate_passed


def compute_risk_multiplier(delta_fraud: float, lambda_risk: float = 0.8) -> float:
    """
    计算风险乘数 v3.0

    核心洞见（来自Gemini）：
    - 风险是乘数效应，不是加数
    - δ_fraud = 0.4（未熔断但有瑕疵）时，资金成本应该膨胀

    公式: multiplier = 1 + λ × δ_fraud

    效果:
    | δ_fraud | multiplier | R_f=2.5%变为 |
    |---------|------------|--------------|
    | 0.0     | 1.0        | 2.5%         |
    | 0.3     | 1.24       | 3.1%         |
    | 0.5     | 1.40       | 3.5%         |
    | 0.7     | 1.56       | 3.9%         |
    """
    return 1.0 + lambda_risk * delta_fraud


def compute_leverage_interaction(beta: float, alpha: float, kappa: float = 0.5) -> float:
    """
    计算双重杠杆交互项 v3.0

    核心洞见（来自Gemini）：
    - 重资产(β) + 强周期(α) = 毁灭机器
    - 不是 β - α 互相抵消，而是 β × (1 + κ×α) 非线性放大

    公式: interaction = β × (1 + κ × α)

    效果:
    | β   | α   | 旧版(β-α) | 新版(β×(1+0.5α)) |
    |-----|-----|-----------|------------------|
    | 0.8 | 0.2 | 0.6       | 0.88 (+47%)      |
    | 0.8 | 0.8 | 0.0       | 1.12 (+∞!)       |
    | 0.3 | 0.8 | -0.5      | 0.42             |

    关键: 高β高α时，惩罚不是消失，而是爆炸！
    """
    return beta * (1.0 + kappa * alpha)


def compute_decay_asset_interaction(delta_decay: float, beta: float, phi: float = 0.4) -> float:
    """
    计算衰退-资产交互项 v3.0

    核心洞见：重资产公司衰退更致命（固定成本高，难以转型）

    公式: interaction = δ_decay × (1 + φ × β)
    """
    return delta_decay * (1.0 + phi * beta)


@dataclass
class GravitySolverResult:
    """
    重力求解器结果（ThresholdResult 的包装）

    Attributes:
        threshold: 完整的阈值计算结果
        interpretation: 结果解读
    """
    threshold: ThresholdResult
    interpretation: str = ""

    @property
    def final_threshold(self) -> float:
        """最终阈值"""
        return self.threshold.final_threshold

    @property
    def theory_threshold(self) -> float:
        """理论阈值（校准前）"""
        return self.threshold.theory_threshold


def gravity_solver(
    genome: CompanyGenome,
    config: TruthConfig = None,
) -> ThresholdResult:
    """
    重力求解器 v3.0 终极版 - 计算动态阈值（分母）

    核心进化（融合 Gemini 建议 + 我们的硬触发）:
    1. 基准利率质量膨胀: R_f × (1 + λ×δ_fraud)
    2. 双重杠杆交互项: β × (1 + κ×α)
    3. V因子门控: V_gate = V_eff × I(V > 0.4)
    4. 衰退-资产交互: δ_decay × (1 + φ×β)
    5. 硬触发机制: 熔断/拐点检测

    上帝方程 I v3.0:
    $$
    T_{roic} = R_f × (1 + λ·δ_{fraud}) + k_1·β·(1 + κ·α) - k_3(γ·E·V_{gate}) + k_4·δ_{decay}·(1 + φ·β)
    $$

    与 v2.0 的关键差异:
    | v2.0 | v3.0 | 效果 |
    |------|------|------|
    | R_f + k₅δ_fraud | R_f × (1+λδ_fraud) | 风险是乘数 |
    | k₁β - k₂α | k₁β(1+κα) | 双重杠杆爆炸 |
    | V_eff | V_gate (V>0.4) | 一票否决 |
    | δ_decay | δ_decay(1+φβ) | 重资产衰退更痛 |

    Args:
        genome: 公司六维基因组
        config: T.R.U.T.H. 配置

    Returns:
        ThresholdResult: 阈值计算结果
    """
    if config is None:
        config = get_default_truth_config()

    macro = config.macro
    solver = config.solver

    # ============================================
    # v3.0 硬触发检测（保留我们的优势）
    # ============================================
    circuit_break = False
    circuit_break_reason = ""

    # 检查欺诈熔断
    if hasattr(genome, 'delta_fraud_breakdown'):
        breakdown = genome.delta_fraud_breakdown
        if breakdown.get('hard_fuse_triggered', False) or breakdown.get('madoff_fuse_triggered', False):
            circuit_break = True
            circuit_break_reason = breakdown.get('fuse_reason', '欺诈熔断')

    # 检查拐点逃顶触发
    if hasattr(genome, 'delta_decay_breakdown'):
        breakdown = genome.delta_decay_breakdown
        if breakdown.get('hard_trigger', False):
            circuit_break_reason += " + 拐点预警"

    # ============================================
    # 1. 基准利率质量膨胀 v3.0 (Gemini核心建议)
    # R_f × (1 + λ × δ_fraud)
    # ============================================
    lambda_risk = getattr(solver, 'lambda_risk', 0.8)
    risk_multiplier = compute_risk_multiplier(genome.delta_fraud, lambda_risk)
    base_rate = macro.risk_free_rate * risk_multiplier

    # ============================================
    # 2. 双重杠杆交互项 v3.0 (Gemini核心建议)
    # β × (1 + κ × α)
    # ============================================
    kappa = getattr(solver, 'kappa_leverage', 0.5)
    leverage_interaction = compute_leverage_interaction(genome.beta, genome.alpha, kappa)
    beta_premium = solver.k1_beta * leverage_interaction

    # ============================================
    # 3. V因子门控 v3.0 (融合: S型 + 一票否决)
    # ============================================
    E = macro.market_sentiment_factor
    v_gated, gate_passed = transform_verification_v3(genome.verification)

    # 门控后的成长奖励
    growth_discount = solver.k3_gamma * genome.gamma * E * v_gated if gate_passed else 0.0

    # ============================================
    # 4. 衰退-资产交互项 v3.0 (Gemini建议增强)
    # δ_decay × (1 + φ × β)
    # ============================================
    phi = getattr(solver, 'phi_decay', 0.4)
    decay_interaction = compute_decay_asset_interaction(genome.delta_decay, genome.beta, phi)
    decay_penalty = solver.k4_decay * decay_interaction

    # ============================================
    # 上帝方程 I v3.0 (非线性物理交互版)
    # ============================================
    # 注意: v3.0 移除了 -k₂α 项，因为 α 已经融入 β×(1+κα) 交互项
    theory_threshold = (
        base_rate           # R_f × (1 + λ×δ_fraud) - 风险膨胀的资金成本
        + beta_premium      # k₁ × β × (1 + κ×α) - 双重杠杆交互
        - growth_discount   # k₃ × γ × E × V_gate - 门控后的真成长奖励
        + decay_penalty     # k₄ × δ_decay × (1 + φ×β) - 衰退-资产交互
    )

    # 阈值保护
    theory_threshold = max(theory_threshold, solver.threshold_floor)
    theory_threshold = min(theory_threshold, solver.threshold_ceiling)

    # V门控失败 → 阈值上浮 (一票否决的实现)
    if not gate_passed:
        theory_threshold = min(theory_threshold * 1.2, solver.threshold_ceiling)

    # 熔断处理
    if circuit_break:
        theory_threshold = solver.threshold_ceiling

    return ThresholdResult(
        base_rate=base_rate,
        beta_premium=beta_premium,
        alpha_discount=0.0,  # v3.0: α已融入交互项
        growth_discount=growth_discount,
        decay_penalty=decay_penalty,
        fraud_premium=0.0,  # v3.0: 已融入base_rate
        cluster_residual=0.0,
        size_residual=0.0,
        theory_threshold=theory_threshold,
        final_threshold=theory_threshold,
        circuit_break=circuit_break,
        circuit_break_reason=circuit_break_reason,
        v_effective=v_gated,
        fraud_penalty_factor=risk_multiplier,  # v3.0: 风险乘数
        gate_passed=gate_passed,  # v3.0新增: 门控状态
        leverage_interaction=leverage_interaction,  # v3.0新增: 交互项
    )


def create_gravity_result(
    genome: CompanyGenome,
    config: TruthConfig = None,
) -> GravitySolverResult:
    """
    创建 GravitySolverResult 对象（便捷工厂函数）v3.0
    """
    threshold = gravity_solver(genome, config)

    # 生成解读
    interpretations = []

    # 熔断检测
    if hasattr(threshold, 'circuit_break') and threshold.circuit_break:
        reason = getattr(threshold, 'circuit_break_reason', '熔断')
        interpretation = f"⛔ 熔断！{reason} → 阈值={threshold.final_threshold:.1%}"
        return GravitySolverResult(threshold=threshold, interpretation=interpretation)

    # v3.0 门控检测
    gate_passed = getattr(threshold, 'gate_passed', True)
    if not gate_passed:
        interpretations.append("🚫 V门控失败(V<0.4)")

    # v3.0 双重杠杆交互
    leverage = getattr(threshold, 'leverage_interaction', 0)
    if leverage > 0.5:
        interpretations.append(f"⚠️ 双杠杆交互={leverage:.2f}")

    if threshold.beta_premium > 0.03:
        interpretations.append(f"重资产惩罚 +{threshold.beta_premium:.1%}")

    if threshold.growth_discount > 0.02:
        interpretations.append(f"真成长奖励 -{threshold.growth_discount:.1%}")

    if threshold.decay_penalty > 0.02:
        interpretations.append(f"衰退惩罚 +{threshold.decay_penalty:.1%}")

    # v3.0 风险乘数解读
    risk_mult = getattr(threshold, 'fraud_penalty_factor', 1.0)
    if risk_mult > 1.2:
        interpretations.append(f"⚠️ 风险膨胀×{risk_mult:.2f}")

    interpretation = f"ROIC阈值 {threshold.final_threshold:.1%}"
    if interpretations:
        interpretation += " (" + ", ".join(interpretations) + ")"

    return GravitySolverResult(
        threshold=threshold,
        interpretation=interpretation,
    )
