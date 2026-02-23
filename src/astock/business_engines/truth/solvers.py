"""T.R.U.T.H. 物理求解器 - 专业级阈值边界推导

三大求解器输出动态阈值，而非简单评分：
    - Gravity (重力场): 基于因子向量计算 ROIC 阈值 → 判断"低估值买入安全边际"
    - Velocity (速度场): 基于因子向量计算增长边界 → 判断"增长天花板"
    - Structure (结构场): 基于因子向量计算护城河宽度 → 判断"竞争优势持久性"

物理类比:
    - Gravity ~ 引力井深度: α↑β↑ 需要更深的安全边际 (更高ROIC)
    - Velocity ~ 逃逸速度: γ↑δ↓ 增长天花板越高
    - Structure ~ 轨道稳定性: V↑ 护城河越宽

架构说明:
    - 使用鸭子类型 (duck typing)，不依赖 ABC 继承
    - 实现 SolverProtocol (typing.Protocol) 即可作为求解器
    - 每个求解器提供 explain() 方法生成人类可读解释

版本: 3.3.0
日期: 2026-01-06
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional, Tuple

from .models import (
    DynamicThreshold,
    FactorId,
    FactorResult,
    SolverId,
    SolverResult,
    TruthWarning,
    WarningLevel,
)
from .config import (
    TruthConfig,
    GravitySolverConfig,
    VelocitySolverConfig,
    StructureSolverConfig,
)


# ============================================================================
# 辅助函数
# ============================================================================

def get_factor_score(factors: Mapping[FactorId, FactorResult],
                     factor_id: FactorId,
                     default: float = 0.5) -> float:
    """安全获取因子分数"""
    result = factors.get(factor_id)
    if result is None:
        return default
    return result.score if result.score is not None else default


def clamp(value: float, vmin: float, vmax: float) -> float:
    """裁剪到范围"""
    return max(vmin, min(vmax, value))


# ============================================================================
# 求解器基类 (鸭子类型 - 实现 SolverProtocol 即可)
# ============================================================================

# 不再使用 ABC 继承，改用 Protocol 鸭子类型
# 任何实现了 solver_id, solve(), explain() 的类都是有效求解器

# NOTE: 以下是实现 SolverProtocol 的约定:
#   - solver_id: SolverId  # 求解器标识
#   - def solve(ts_code, factors, config) -> Tuple[SolverResult, List[TruthWarning]]
#   - def explain(result: SolverResult) -> str  # 人类可读解释


# ============================================================================
# Gravity 求解器: ROIC 阈值计算
# ============================================================================

@dataclass
class GravitySolver:
    """Gravity 求解器: 基于因子向量推导 ROIC 安全阈值

    物理类比: 引力井深度
        - 高周期性(α) + 重资产(β) = 需要更深的安全边际
        - 这些公司的 ROIC 要求更高才能证明其价值

    阈值公式:
        roic_threshold = base_roic × (1 + α_coeff × α + β_coeff × β)
                         × fraud_penalty × decay_penalty

    其中:
        - base_roic: 基准 ROIC (通常 10-12%)
        - α_coeff: 周期性惩罚系数
        - β_coeff: 资本密度惩罚系数
        - fraud_penalty: 欺诈熵惩罚 (δ_fraud)
        - decay_penalty: 衰退熵惩罚 (δ_decay)
    """

    solver_id: SolverId = SolverId.GRAVITY

    def solve(self,
              ts_code: str,
              factors: Mapping[FactorId, FactorResult],
              config: TruthConfig) -> Tuple[SolverResult, List[TruthWarning]]:

        warnings: List[TruthWarning] = []
        components: Dict[str, float] = {}
        conf = config.gravity_solver

        # 获取因子分数
        alpha = get_factor_score(factors, FactorId.ALPHA, 0.5)  # 周期性
        beta = get_factor_score(factors, FactorId.BETA, 0.5)    # 资本密度
        delta_fraud = get_factor_score(factors, FactorId.DELTA_FRAUD, 0.0)
        delta_decay = get_factor_score(factors, FactorId.DELTA_DECAY, 0.0)
        verification = get_factor_score(factors, FactorId.VERIFICATION, 0.5)

        components["alpha"] = alpha
        components["beta"] = beta
        components["delta_fraud"] = delta_fraud
        components["delta_decay"] = delta_decay
        components["verification"] = verification

        # ============================================================
        # v4.1 WACC 估算 (替代固定 base_roic)
        # WACC = E/(D+E) × Cost_of_Equity + D/(D+E) × Cost_of_Debt × (1-Tax)
        # Cost_of_Equity = Rf + β_market × ERP
        # D/(D+E) 由 λ 因子分数近似推导
        # ============================================================
        if conf.use_wacc_estimate:
            # 从 λ 因子获取杠杆信息 (λ 分数高 = 高杠杆风险)
            lambda_score = get_factor_score(factors, FactorId.LAMBDA, 0.3)
            components["lambda_leverage"] = lambda_score

            # λ 分数映射到 D/V 比率:
            # λ=0 (无杠杆) → D/V≈0.0, λ=0.5 (中等) → D/V≈0.35, λ=1.0 (极高) → D/V≈0.70
            debt_to_value = lambda_score * 0.70
            equity_to_value = 1.0 - debt_to_value

            # Cost of Equity = Rf + β_market × ERP
            # 使用 α (周期性) 近似 β_market: 高周期 α→高β
            beta_market = 0.6 + alpha * 1.2  # 范围 [0.6, 1.8]
            cost_of_equity = config.macro.risk_free_rate + beta_market * conf.equity_risk_premium

            # Cost of Debt (税后)
            cost_of_debt_after_tax = conf.default_debt_cost * (1.0 - conf.default_tax_rate)

            # WACC
            wacc = equity_to_value * cost_of_equity + debt_to_value * cost_of_debt_after_tax
            wacc = max(4.0, min(20.0, wacc))  # 合理范围

            base_threshold = wacc
            components["wacc_estimate"] = wacc
            components["beta_market"] = beta_market
            components["debt_to_value"] = debt_to_value
            components["cost_of_equity"] = cost_of_equity
        else:
            base_threshold = conf.base_roic_threshold

        # ============================================================
        # v3.4 加法模型 (替代原连乘模型):
        # T_roic = T_base + k1*(1-β) + k2*α - k3*δ_decay - k4*V
        #
        # 原连乘模型问题: 因子值域0~1时 (1+k*x) 的变化幅度极小,
        # 导致所有公司的阈值几乎相同 (0.82~0.83)
        # 加法模型: 每个因子直接贡献 ±百分点, 区分度大幅提升
        # ============================================================

        # k1: 轻资产加成 (轻资产公司需要更高 ROIC 证明价值)
        light_asset_adj = conf.k_light_asset * (1.0 - beta)
        components["light_asset_adj"] = light_asset_adj

        # k2: 周期性惩罚 (高周期性需要更高 ROIC 补偿风险)
        cycle_adj = conf.k_cycle_tolerance * alpha
        components["cycle_adj"] = cycle_adj

        # k3: 衰退惩罚 (衰退中的公司阈值降低, 给复苏机会)
        decay_penalty = conf.k_decay_penalty * delta_decay
        components["decay_penalty"] = decay_penalty

        # k4: 真成长折扣 (验证因子高 = 成长可信, 阈值可降)
        verification_bonus = conf.k_verification_bonus * verification
        components["verification_bonus"] = verification_bonus

        # 欺诈熔断检查
        if conf.fraud_meltdown_enabled and delta_fraud > 0.7:
            warnings.append(TruthWarning(
                code="GRAVITY_FRAUD_MELTDOWN",
                level=WarningLevel.FATAL,
                title="欺诈熔断触发",
                message=f"欺诈熵 δ_fraud={delta_fraud:.2f} > 0.7, ROIC 阈值计算无效",
                source="gravity_solver",
                values={"delta_fraud": delta_fraud},
            ))
            return SolverResult(
                solver_id=self.solver_id,
                ts_code=ts_code,
                score=0.0,
                confidence=0.0,
                thresholds={},
                components=components,
                details={"status": "fraud_meltdown"},
            ), warnings

        # 欺诈熵线性惩罚 (非熔断情况下)
        fraud_adj = 2.0 * delta_fraud  # 最大 +2pp
        components["fraud_adj"] = fraud_adj

        # v3.4 / v4.1 加法模型最终计算
        roic_threshold = (base_threshold
                         + light_asset_adj    # 轻资产 → 要求更高
                         + cycle_adj          # 高周期 → 要求更高
                         + fraud_adj          # 高欺诈 → 要求更高
                         - decay_penalty      # 衰退 → 降低要求 (给复苏机会)
                         - verification_bonus # 真成长 → 降低要求
                         )

        # 限制在合理范围 [4%, 22%]
        roic_threshold = clamp(roic_threshold, 4.0, 22.0)
        components["roic_threshold"] = roic_threshold

        # 置信区间 (±20%, 比原来15%更宽, 反映输入不确定性)
        margin = roic_threshold * 0.20
        lower_bound = max(4.0, roic_threshold - margin)
        upper_bound = min(22.0, roic_threshold + margin)

        # ============================================================
        # 生成动态阈值和评分
        # ============================================================

        threshold = DynamicThreshold(
            name="roic_safe_threshold",
            value=roic_threshold,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            confidence=0.85,
            unit="percent",
            description=f"安全边际: ROIC 应 > {roic_threshold:.1f}% 才具备投资价值",
        )

        # v3.4 sigmoid 归一化 — 扩大区分范围
        # 加法模型输出范围 ~4%~22%, 中心 10%, 缩放 4.0
        # 4% → 0.82, 10% → 0.50, 16% → 0.18, 22% → 0.05
        # 关键: 低要求(=好公司) 得分高, 高要求(=差公司) 得分低
        centered = (roic_threshold - 10.0) / 4.0
        normalized_score = 1.0 / (1.0 + math.exp(centered))
        normalized_score = clamp(normalized_score, 0.0, 1.0)

        # 置信度基于数据完整性
        data_count = sum(1 for f in factors.values() if f is not None and f.score is not None)
        confidence = min(1.0, data_count / 6.0 * 1.2)

        # 警告
        if roic_threshold > 20.0:
            warnings.append(TruthWarning(
                code="GRAVITY_HIGH_THRESHOLD",
                level=WarningLevel.WARNING,
                title="高 ROIC 要求",
                message=f"需要 ROIC > {roic_threshold:.1f}% 才有安全边际",
                source="gravity_solver",
                values={"threshold": roic_threshold},
            ))

        return SolverResult(
            solver_id=self.solver_id,
            ts_code=ts_code,
            score=normalized_score,
            confidence=confidence,
            thresholds={"roic": threshold},
            components=components,
            details={
                "interpretation": f"要求 ROIC ≥ {roic_threshold:.1f}% (置信区间 {lower_bound:.1f}%-{upper_bound:.1f}%)",
            },
        ), warnings

    def explain(self, result: SolverResult) -> str:
        """生成人类可读的解释文本"""
        components = result.components or {}
        details = result.details or {}
        thresholds = result.thresholds or {}

        roic_threshold = components.get("roic_threshold", 12.0)
        alpha = components.get("alpha", 0.5)
        beta = components.get("beta", 0.5)

        # 资产类型判断
        if beta > 0.65:
            asset_desc = "重资产"
        elif beta < 0.35:
            asset_desc = "轻资产"
        else:
            asset_desc = "中等资产"

        parts = [f"Gravity: ROIC阈值={roic_threshold:.1f}%"]
        parts.append(f"{asset_desc}(β={beta:.2f})")

        if alpha > 0.6:
            parts.append(f"高周期(α={alpha:.2f})")

        wacc = components.get("wacc_estimate")
        if wacc is not None:
            parts.append(f"WACC≈{wacc:.1f}%")

        if "interpretation" in details:
            parts.append(details["interpretation"])

        return "，".join(parts)


# ============================================================================
# Velocity 求解器: 增长边界计算
# ============================================================================

@dataclass
class VelocitySolver:
    """Velocity 求解器: 基于因子向量推导增长天花板

    物理类比: 逃逸速度
        - 高成长动能(γ) + 低衰退(δ_decay) = 增长天花板更高
        - V 因子高 = 成长更真实，边界更可信

    阈值公式:
        growth_ceiling = base_growth × γ_boost × (1 - decay_drag) × v_quality

    输出:
        - growth_ceiling: 可持续增长率上限
        - growth_floor: 增长率下限 (即使乐观情况下)
    """

    solver_id: SolverId = SolverId.VELOCITY

    def solve(self,
              ts_code: str,
              factors: Mapping[FactorId, FactorResult],
              config: TruthConfig) -> Tuple[SolverResult, List[TruthWarning]]:

        warnings: List[TruthWarning] = []
        components: Dict[str, float] = {}
        conf = config.velocity_solver

        # 获取因子分数
        gamma = get_factor_score(factors, FactorId.GAMMA, 0.5)  # 成长动能
        delta_fraud = get_factor_score(factors, FactorId.DELTA_FRAUD, 0.0)
        delta_decay = get_factor_score(factors, FactorId.DELTA_DECAY, 0.0)
        verification = get_factor_score(factors, FactorId.VERIFICATION, 0.5)
        alpha = get_factor_score(factors, FactorId.ALPHA, 0.5)  # 周期性

        components["gamma"] = gamma
        components["delta_decay"] = delta_decay
        components["verification"] = verification
        components["alpha"] = alpha

        # ============================================================
        # v5.2: 使用独立信号替代 γ×V 乘积 (消除三重计数)
        # 原公式: T_growth = GDP + k1×(γ×V) + k2×α×0.5
        # 问题: γ 和 V 已作为独立因子(34%权重)贡献于 factor composite,
        #       solver 层再用 γ×V 乘积 = 三重编码, 约50%总分受γ和V驱动
        # 修复: 只用 γ (成长动能), V 的信号由因子层独立贡献
        #       增加 δ_decay 的独立贡献 (衰退拖累增长天花板)
        # ============================================================

        # k1: 成长动能加成 (γ 越大，成长越强，增长天花板越高)
        true_growth_factor = conf.k_true_growth * gamma
        components["true_growth_factor"] = true_growth_factor

        # k2: 周期容忍 (α 越大，周期性越强，增长预期降低)
        cycle_tolerance = conf.k_cycle_tolerance * alpha * 0.5
        components["cycle_tolerance"] = cycle_tolerance

        # k3: 衰退拖累 (δ_decay 越大，增长天花板越低)
        decay_drag = 0.5 * delta_decay  # 范围 [0, 0.5]
        components["decay_drag"] = decay_drag

        # 基准增长 = GDP + CPI (名义增速) — 从宏观配置读取
        base_growth = config.macro.gdp_growth_rate + config.macro.cpi_rate
        components["base_growth"] = base_growth

        # 增长天花板
        growth_ceiling = base_growth + true_growth_factor * 100 - cycle_tolerance * 100 - decay_drag * 100
        growth_ceiling = clamp(growth_ceiling, 0.0, 50.0)  # 限制 0-50%
        components["growth_ceiling"] = growth_ceiling

        # 增长地板 (基于不确定性)
        uncertainty = 0.3 + 0.2 * alpha + 0.1 * delta_decay
        growth_floor = max(0.0, growth_ceiling * (1.0 - uncertainty))
        components["growth_floor"] = growth_floor
        components["uncertainty"] = uncertainty

        # ============================================================
        # Step 6: 生成动态阈值
        # ============================================================

        ceiling_threshold = DynamicThreshold(
            name="growth_ceiling",
            value=growth_ceiling,
            lower_bound=growth_floor,
            upper_bound=min(growth_ceiling * 1.3, 50.0),
            confidence=0.75,
            unit="percent_annual",
            description=f"可持续增长上限: {growth_ceiling:.1f}%/年",
        )

        floor_threshold = DynamicThreshold(
            name="growth_floor",
            value=growth_floor,
            lower_bound=0.0,
            upper_bound=growth_floor * 1.2,
            confidence=0.80,
            unit="percent_annual",
            description=f"保守增长下限: {growth_floor:.1f}%/年",
        )

        # 评分: 使用 sigmoid 归一化, 天花板越高越好
        # v3.4: 中心12%→10%, scale 5→4 扩大区分度
        # 5% → 0.12, 8% → 0.38, 10% → 0.50, 14% → 0.73, 20% → 0.92
        centered = (growth_ceiling - 10.0) / 4.0
        normalized_score = 1.0 / (1.0 + math.exp(-centered))
        normalized_score = clamp(normalized_score, 0.0, 1.0)

        # 置信度
        confidence = 0.85 - 0.2 * uncertainty

        # 分类标签
        label = "stagnant"
        for lbl, (low, high) in conf.labels.items():
            if low <= normalized_score <= high:
                label = lbl
                break

        # 警告
        if growth_ceiling < 5.0:
            warnings.append(TruthWarning(
                code="VELOCITY_LOW_CEILING",
                level=WarningLevel.WARNING,
                title="增长天花板低",
                message=f"可持续增长上限仅 {growth_ceiling:.1f}%/年",
                source="velocity_solver",
                values={"ceiling": growth_ceiling},
            ))
        elif growth_ceiling > 30.0:
            warnings.append(TruthWarning(
                code="VELOCITY_HIGH_CEILING",
                level=WarningLevel.INFO,
                title="高增长潜力",
                message=f"增长天花板达 {growth_ceiling:.1f}%/年",
                source="velocity_solver",
                values={"ceiling": growth_ceiling},
            ))

        return SolverResult(
            solver_id=self.solver_id,
            ts_code=ts_code,
            score=normalized_score,
            confidence=confidence,
            thresholds={
                "growth_ceiling": ceiling_threshold,
                "growth_floor": floor_threshold,
            },
            components=components,
            details={
                "interpretation": f"增长区间 {growth_floor:.1f}% ~ {growth_ceiling:.1f}%/年",
                "label": label,
                "uncertainty_level": "high" if uncertainty > 0.5 else "moderate" if uncertainty > 0.3 else "low",
            },
        ), warnings

    def explain(self, result: SolverResult) -> str:
        """生成人类可读的解释文本"""
        components = result.components or {}
        details = result.details or {}

        growth_ceiling = components.get("growth_ceiling", 10.0)
        growth_floor = components.get("growth_floor", 0.0)
        gamma = components.get("gamma", 0.5)
        verification = components.get("verification", 0.5)
        uncertainty = details.get("uncertainty_level", "moderate")

        parts = [f"Velocity: 增长区间 {growth_floor:.1f}%~{growth_ceiling:.1f}%/年"]

        # 成长质量描述
        if gamma > 0.7 and verification > 0.6:
            parts.append("真成长型")
        elif gamma > 0.5:
            parts.append("中等成长")
        else:
            parts.append("低成长")

        parts.append(f"不确定性{uncertainty}")

        return "，".join(parts)


# ============================================================================
# Structure 求解器: 护城河宽度计算
# ============================================================================

@dataclass
class StructureSolver:
    """Structure 求解器: 基于因子向量推导护城河宽度

    物理类比: 护城河对抗"熵增"

    核心: 考察斜率而非绝对值
        - 毛利率斜率 > 0: 护城河加深
        - 毛利率斜率 < 0: 护城河被侵蚀

    护城河维度:
        1. 盈利稳定性: 低 α (周期性)
        2. 资本效率: 低 β (轻资产) - 轻资产更需要护城河
        3. 成长质量: 高 V (验证因子)
        4. 可持续性: 低 δ_decay (衰退)

    输出:
        - moat_width: 护城河宽度 (0-100)
        - moat_label: 分类标签
    """

    solver_id: SolverId = SolverId.STRUCTURE

    def solve(self,
              ts_code: str,
              factors: Mapping[FactorId, FactorResult],
              config: TruthConfig) -> Tuple[SolverResult, List[TruthWarning]]:

        warnings: List[TruthWarning] = []
        components: Dict[str, float] = {}
        conf = config.structure_solver

        # 获取因子分数
        alpha = get_factor_score(factors, FactorId.ALPHA, 0.5)
        beta = get_factor_score(factors, FactorId.BETA, 0.5)
        gamma = get_factor_score(factors, FactorId.GAMMA, 0.5)
        delta_fraud = get_factor_score(factors, FactorId.DELTA_FRAUD, 0.0)
        delta_decay = get_factor_score(factors, FactorId.DELTA_DECAY, 0.0)
        verification = get_factor_score(factors, FactorId.VERIFICATION, 0.5)

        components["alpha"] = alpha
        components["beta"] = beta
        components["gamma"] = gamma
        components["delta_fraud"] = delta_fraud
        components["delta_decay"] = delta_decay
        components["verification"] = verification

        # ============================================================
        # 维度1: 盈利稳定性 (低周期性 = 稳定)
        # ============================================================

        stability_score = 1.0 - alpha  # 0=高周期, 1=稳定
        components["stability_score"] = stability_score

        # ============================================================
        # 维度2: 资本壁垒 (重资产 = 天然护城河)
        # ============================================================

        # 高 β = 重资产 = 天然资本壁垒 (新进入者需要大量资本)
        # 低 β = 轻资产 = 无资本壁垒，需要品牌/网络效应等护城河
        capital_barrier = beta  # 0=无壁垒(轻资产), 1=高壁垒(重资产)
        components["capital_barrier"] = capital_barrier

        # 轻资产审查: 轻资产公司需更高标准证明护城河
        light_asset_factor = 1.0 - beta
        scrutiny_adjustment = conf.k_light_asset_scrutiny * light_asset_factor
        components["light_asset_factor"] = light_asset_factor
        components["scrutiny_adjustment"] = scrutiny_adjustment

        # ============================================================
        # 维度3: 成长动能 (高成长 = 护城河有经济动力支撑)
        # 注意: 之前使用 verification (V因子), 但 V 已在 factor 层级
        #       有 20% 权重, 在此重复使用会导致双重计算。
        #       改用 gamma (成长因子) 更合理: 正在扩张的公司护城河有底气
        # ============================================================

        quality_score = gamma
        components["quality_score"] = quality_score

        # ============================================================
        # 维度4: 可持续性 (低衰退 = 护城河不在侵蚀)
        # ============================================================

        # 使用 k_decay_penalty 系数
        decay_penalty = conf.k_decay_penalty * delta_decay
        durability_score = max(0.0, 1.0 - decay_penalty)
        components["durability_score"] = durability_score
        components["decay_penalty"] = decay_penalty

        # ============================================================
        # 加权计算护城河宽度
        # ============================================================

        # 固定权重: 稳定性30%, 资本壁垒20%, 质量25%, 可持续25%
        moat_width = (
            0.30 * stability_score +
            0.20 * capital_barrier +
            0.25 * quality_score +
            0.25 * durability_score
        )

        # 轻资产审查调整 (轻资产公司阈值更高)
        moat_width -= scrutiny_adjustment
        moat_width = clamp(moat_width, 0.0, 1.0)

        # 欺诈熵惩罚 (非熔断式，线性惩罚)
        if delta_fraud > 0.5:
            fraud_penalty = 1.0 - (delta_fraud - 0.5) * 2.0  # [0.5, 1.0] -> [1.0, 0.0]
            moat_width *= max(0.0, fraud_penalty)
            warnings.append(TruthWarning(
                code="STRUCTURE_FRAUD_WARNING",
                level=WarningLevel.WARNING,
                title="护城河存疑",
                message=f"欺诈熵 δ_fraud={delta_fraud:.2f} 较高，护城河可能虚假",
                source="structure_solver",
                values={"delta_fraud": delta_fraud},
            ))

        components["moat_width_raw"] = moat_width

        # ============================================================
        # 转换为 0-100 分
        # ============================================================

        moat_width_100 = moat_width * 100.0
        moat_width_100 = clamp(moat_width_100, 0.0, 100.0)

        # ============================================================
        # 分类标签
        # ============================================================

        label = "deteriorating"
        for lbl, (low, high) in conf.labels.items():
            if low <= moat_width <= high:
                label = lbl
                break

        # ============================================================
        # 生成动态阈值
        # ============================================================

        width_threshold = DynamicThreshold(
            name="moat_width",
            value=moat_width_100,
            lower_bound=max(0, moat_width_100 - 15),
            upper_bound=min(100, moat_width_100 + 10),
            confidence=0.80,
            unit="score_0_100",
            description=self._get_moat_description(moat_width_100),
        )

        # 评分 = 护城河宽度
        normalized_score = moat_width
        confidence = 0.85

        # 护城河分类
        moat_type = self._classify_moat(moat_width_100)

        # 警告
        if moat_width_100 < 30:
            warnings.append(TruthWarning(
                code="STRUCTURE_NARROW_MOAT",
                level=WarningLevel.WARNING,
                title="窄护城河",
                message=f"护城河宽度仅 {moat_width_100:.0f} 分，竞争优势不明显",
                source="structure_solver",
                values={"moat_width": moat_width_100},
            ))
        elif moat_width_100 >= 70:
            warnings.append(TruthWarning(
                code="STRUCTURE_WIDE_MOAT",
                level=WarningLevel.INFO,
                title="宽护城河",
                message=f"护城河宽度 {moat_width_100:.0f} 分，竞争优势显著",
                source="structure_solver",
                values={"moat_width": moat_width_100},
            ))

        return SolverResult(
            solver_id=self.solver_id,
            ts_code=ts_code,
            score=normalized_score,
            confidence=confidence,
            thresholds={
                "moat_width": width_threshold,
            },
            components=components,
            details={
                "moat_type": moat_type,
                "label": label,
                "interpretation": width_threshold.description,
            },
        ), warnings

    def _get_moat_description(self, width: float) -> str:
        """获取护城河描述"""
        if width >= 80:
            return "超宽护城河：顶级竞争优势，难以复制"
        elif width >= 60:
            return "宽护城河：明显竞争优势，可持续多年"
        elif width >= 40:
            return "窄护城河：一定竞争优势，但可能被侵蚀"
        elif width >= 20:
            return "微弱护城河：竞争优势不明显"
        else:
            return "无护城河：缺乏持久竞争优势"

    def _classify_moat(self, width: float) -> str:
        """护城河分类"""
        if width >= 70:
            return "wide"
        elif width >= 50:
            return "moderate"
        elif width >= 30:
            return "narrow"
        else:
            return "none"

    def explain(self, result: SolverResult) -> str:
        """生成人类可读的解释文本"""
        components = result.components or {}
        details = result.details or {}

        moat_width = components.get("moat_width_raw", 0.5) * 100
        moat_type = details.get("moat_type", "unknown")

        moat_label = {
            "wide": "宽护城河 🏰",
            "moderate": "中等护城河",
            "narrow": "窄护城河",
            "none": "无护城河 ⚠️",
        }.get(moat_type, "未知")

        parts = [f"Structure: {moat_label} (宽度{moat_width:.0f}分)"]

        stability = components.get("stability_score", 0.5)
        quality = components.get("quality_score", 0.5)

        if stability > 0.7:
            parts.append("盈利稳定")
        if quality > 0.7:
            parts.append("成长质量高")

        if "interpretation" in details:
            parts.append(details["interpretation"])

        return "，".join(parts)


# ============================================================================
# 类型别名 (兼容性)
# ============================================================================

# TruthSolver 现在是一个类型别名，保持向后兼容
from typing import Union
TruthSolver = Union[GravitySolver, VelocitySolver, StructureSolver]


# ============================================================================
# 工厂函数
# ============================================================================

def get_all_solvers() -> List[TruthSolver]:
    """获取所有求解器实例"""
    return [
        GravitySolver(),
        VelocitySolver(),
        StructureSolver(),
    ]


def get_solver_by_id(solver_id: SolverId) -> TruthSolver:
    """根据ID获取求解器"""
    mapping = {
        SolverId.GRAVITY: GravitySolver(),
        SolverId.VELOCITY: VelocitySolver(),
        SolverId.STRUCTURE: StructureSolver(),
    }
    return mapping.get(solver_id, GravitySolver())


# ============================================================================
# 导出
# ============================================================================

__all__ = [
    # 类型别名 (兼容性)
    "TruthSolver",
    # 求解器实现
    "GravitySolver",
    "VelocitySolver",
    "StructureSolver",
    # 工厂函数
    "get_all_solvers",
    "get_solver_by_id",
    # 辅助函数
    "get_factor_score",
    "clamp",
]
