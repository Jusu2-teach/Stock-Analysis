"""T.R.U.T.H. 配置系统 (v4.1)

四层配置体系：
    - Layer 0: 时间衰减参数
    - Layer 1: 七维因子配置 (新增 λ 杠杆因子)
    - Layer 2: 三大求解器参数
    - Layer 3: 校准与评分

设计原则：只保留实际被使用的配置项

版本: 4.1.0
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Tuple

from .models import FactorId


# ============================================================================
# Layer 0: 时间衰减配置
# ============================================================================

@dataclass(frozen=True)
class TimeDecayConfig:
    """时间衰减配置 - 近期数据权重更高"""
    half_life_years: float = 1.5
    ewma_alpha: float = 0.5


# ============================================================================
# Layer 1: 因子配置
# ============================================================================

@dataclass(frozen=True)
class AlphaFactorConfig:
    """α 因子 (周期性) - 业绩对宏观经济的敏感弹性

    v5.0 去共线性: 移除 cv (与 detrended_cv r>0.9, VIF>5)
    重新分配: detrended_cv(0.40) + R²反向(0.30) + 周期标志(0.20) + Hurst(0.10)
    """
    component_weights: Mapping[str, float] = field(default_factory=lambda: {
        "detrended_cv": 0.40,
        "r_squared_inverse": 0.30,
        "is_cyclical": 0.20,
        "hurst_exponent": 0.10,
    })


@dataclass(frozen=True)
class BetaFactorConfig:
    """β 因子 (资本密度) - 赚取利润所需的资本投入

    数据源: financial_context 探针的资产结构比率
    v5.0 去共线性: 移除 nca_ratio (hard_asset是其子集, r>0.8)
    重新分配: hard_asset(0.50) + intang反向(0.25) + working_capital反向(0.25)
    """
    component_weights: Mapping[str, float] = field(default_factory=lambda: {
        "hard_asset_ratio": 0.50,      # (固定资产+在建工程) / 总资产
        "intang_ratio": 0.25,          # 无形资产 / 总资产 (反向)
        "working_capital_ratio": 0.25, # 营运资本 / 总资产 (反向)
    })


@dataclass(frozen=True)
class GammaFactorConfig:
    """γ 因子 (成长动能) - 业务扩张加速度

    v5.0:
    - 致命BUG修复: cagr/50.0 → cagr/0.50 (CAGR是小数→原归一化完全失效)
    - 去共线性: 移除 robust_slope (与 log_slope r>0.95)
    - CAGR权重提升至 0.45 (成长因子的核心度量)
    """
    component_weights: Mapping[str, float] = field(default_factory=lambda: {
        "cagr": 0.45,
        "log_slope": 0.30,
        "recent_3y_slope": 0.20,
        "r_squared_penalty": 0.05,
    })
    high_growth_threshold: float = 0.15  # CAGR > 15%
    moderate_growth_threshold: float = 0.05


@dataclass(frozen=True)
class DeltaFraudFactorConfig:
    """δ_fraud 因子 (欺诈熵) - 财务造假检测"""
    component_weights: Mapping[str, float] = field(default_factory=lambda: {
        "ocf_profit_divergence": 0.30,
        "receivables_growth": 0.20,
        "margin_smoothness": 0.20,
        "revenue_r_squared": 0.15,
        "cross_validation": 0.15,
    })
    meltdown_threshold: float = 0.58  # δ_fraud > 0.58 触发熔断
    too_smooth_cv_threshold: float = 0.03  # A股毛利率CV中位数~0.20, <3%为异常平滑
    too_perfect_r2_threshold: float = 0.95  # A股R²中位数~0.6, >95%为异常完美


@dataclass(frozen=True)
class DeltaDecayFactorConfig:
    """δ_decay 因子 (衰退熵) - 商业模式恶化检测"""
    component_weights: Mapping[str, float] = field(default_factory=lambda: {
        "has_deterioration": 0.25,
        "consecutive_decline": 0.25,
        "total_decline_pct": 0.20,
        "deterioration_acceleration": 0.15,
        "negative_slope": 0.15,
    })
    severe_decline_threshold: float = 0.30
    consecutive_years_threshold: int = 3


@dataclass(frozen=True)
class LambdaFactorConfig:
    """λ 因子 (杠杆强度) - 偿债安全边际与资本结构健康度

    v4.1 新增: 填补 Altman Z-Score 和 AQR QMJ Safety 维度的空白
    数据源: financial_context 探针的负债结构比率
    """
    # v5.0 去共线性: 移除 equity_multiplier (EM=1/(1-D/A), 完全共线)
    component_weights: Mapping[str, float] = field(default_factory=lambda: {
        "debt_to_assets": 0.40,         # 资产负债率 (核心)
        "debt_trend": 0.25,             # 负债率变动趋势
        "cash_coverage": 0.35,          # 现金覆盖度 (吸收原EM权重)
    })
    safe_debt_ratio: float = 0.50       # 安全负债率上限
    danger_debt_ratio: float = 0.75     # 危险负债率


@dataclass(frozen=True)
class VerificationFactorConfig:
    """V 因子 (真相验证) - 成长含金量验证

    v5.1 升级: 移除 ocf_profit_ratio (与 δ_fraud 重叠),
    替换为 rev_profit_consistency (利润/营收增速一致性, 独立信号)
    """
    component_weights: Mapping[str, float] = field(default_factory=lambda: {
        "ocf_revenue_ratio": 0.55,         # v5.1: ↑ 0.40→0.55 (核心 OCF 增速验证)
        "rev_profit_consistency": 0.25,     # v5.1: 新增 (替代 ocf_profit_ratio)
        "sloan_accruals": 0.20,            # v5.1: ↑ 0.15→0.20 (Sloan 应计质量)
    })
    true_growth_threshold: float = 0.8
    fake_growth_threshold: float = 0.3


# ============================================================================
# 宏观经济参数
# ============================================================================

@dataclass(frozen=True)
class MacroConfig:
    """宏观经济参数 - 可从外部注入，避免硬编码"""
    gdp_growth_rate: float = 4.5    # GDP 增速 (%)
    cpi_rate: float = 1.5           # CPI 通胀率 (%)
    risk_free_rate: float = 2.5     # 无风险利率 (%)


# ============================================================================
# Layer 2: 求解器配置
# ============================================================================

@dataclass(frozen=True)
class GravitySolverConfig:
    """重力求解器 - ROIC 动态阈值计算

    v3.4 变更:
    - 从连乘模型改为加法模型，每个因子直接贡献±百分点
    - 扩大系数范围，使输出从 [7.6-15.9%] 扩展到 [4-22%]
    - 这样不同质量公司的 ROIC 阈值差异显著
    """
    base_roic_threshold: float = 10.0   # 基准ROIC (原8.0)
    base_roe_threshold: float = 12.0
    # v4.1: WACC 估算参数 (替代固定 base_roic)
    use_wacc_estimate: bool = True      # 启用 WACC 估算
    equity_risk_premium: float = 6.0    # A股股权风险溢价 (%)
    default_debt_cost: float = 4.5      # 默认债务成本 (%)
    default_tax_rate: float = 0.25      # 企业所得税率
    # 加法模型系数: 每个因子贡献的百分点范围
    k_light_asset: float = 4.0          # 轻资产加成: 最高+4pp (原0.50乘法)
    k_cycle_tolerance: float = 3.0      # 周期惩罚: 最高+3pp (原0.30乘法)
    k_decay_penalty: float = 5.0        # 衰退惩罚: 最高-5pp (原0.40乘法)
    k_verification_bonus: float = 3.0   # 真成长加成: 最高-3pp (原0.25乘法)
    fraud_meltdown_enabled: bool = True


@dataclass(frozen=True)
class VelocitySolverConfig:
    """速度求解器 - 增长边界计算

    v3.4 变更:
    - k_true_growth: 0.15→0.25 扩大成长因子影响力
    - sigmoid中心从12%→10%，使中等增长公司也有区分度
    """
    k_true_growth: float = 0.25       # 原0.15, 真成长影响力
    k_cycle_tolerance: float = 0.12   # 原0.10
    # 增长分类标签: (normalized_score 范围)
    labels: Mapping[str, Tuple[float, float]] = field(default_factory=lambda: {
        "hypergrowth": (0.60, 1.00),   # 高速增长
        "fast_growth": (0.40, 0.60),   # 快速增长
        "moderate": (0.20, 0.40),      # 中速增长
        "slow": (0.10, 0.20),          # 低速增长
        "stagnant": (0.00, 0.10),      # 停滞
    })


@dataclass(frozen=True)
class StructureSolverConfig:
    """结构求解器 - 护城河宽度计算"""
    moat_deepening_threshold: float = 0.01
    moat_erosion_threshold: float = -0.02
    k_light_asset_scrutiny: float = 0.03
    k_decay_penalty: float = 0.15
    # 护城河分类标签: (moat_width 范围, 0-1)
    labels: Mapping[str, Tuple[float, float]] = field(default_factory=lambda: {
        "wide_moat": (0.70, 1.00),       # 宽护城河
        "narrow_moat": (0.50, 0.70),     # 窄护城河
        "uncertain": (0.30, 0.50),       # 不确定
        "deteriorating": (0.00, 0.30),   # 恶化中
    })


# ============================================================================
# Layer 3: 校准与评分配置
# ============================================================================

@dataclass(frozen=True)
class CalibrationConfig:
    """校准配置 - 规模/行业/置信度调整

    v3.4 变更:
    - full_confidence_years: 10→7 (A股上市<10年公司占比高)
    - max_confidence_5y: 0.55→0.85 (5年数据已足够判断趋势)
    - 新增 confidence_curve: 平滑置信度曲线而非硬性 cap
    """
    size_adjustments: Mapping[str, float] = field(default_factory=lambda: {
        "mega": -1.5,   # >1000亿
        "large": 0.0,   # 300-1000亿
        "mid": 1.0,     # 100-300亿
        "small": 2.0,   # 30-100亿
        "micro": 3.0,   # <30亿
    })
    min_data_years: int = 3
    full_confidence_years: int = 7      # 7年即给满置信度 (原10年太严)
    max_confidence_5y: float = 0.85     # 5年数据置信度上限 (原0.55太低)
    min_confidence_3y: float = 0.60     # 3年数据最低置信度
    industry_adjustment_enabled: bool = True
    industry_adjustment_weight: float = 0.05


@dataclass(frozen=True)
class ScoringConfig:
    """综合评分配置

    v3.4 变更:
    - factor_vs_solver_weight: 0.5→0.6 (因子比求解器更有区分度)
    - signal/grade thresholds 大幅下调: A股市场整体质量偏低,
      原阈值导致0家A+/A, 现在top 5%能拿到A
    """
    factor_weights: Mapping[str, float] = field(default_factory=lambda: {
        "ALPHA": 0.12,             # v4.6 ↑ 0.08→0.12: 周期性识别更重要
        "BETA": 0.08,              # v4.6 ↑ 0.07→0.08: 轻微提升
        "GAMMA": 0.18,             # v4.6 ↓ 0.22→0.18: 减少成长偏好
        "LAMBDA": 0.12,            # v4.1 不变
        "DELTA_FRAUD": 0.16,       # 不变
        "DELTA_DECAY": 0.18,       # v4.6 ↑ 0.12→0.18: 衰退惩罚严重不足
        "VERIFICATION": 0.16,      # v4.6 ↓ 0.23→0.16: V修复后给真实值,降低权重
    })
    solver_weights: Mapping[str, float] = field(default_factory=lambda: {
        "GRAVITY": 0.50,     # v5.1: ↑ 0.35→0.50 (ROIC阈值推导, 核心价值锚)
        "VELOCITY": 0.40,    # v5.1: ↑ 0.35→0.40 (增长边界, 独立信号)
        "STRUCTURE": 0.10,   # v5.1: ↓ 0.30→0.10 (ρ≈0.80与因子高度共线)
    })
    factor_vs_solver_weight: float = 0.60  # 因子权重提高 (原0.5)
    signal_thresholds: Mapping[str, float] = field(default_factory=lambda: {
        "strong_buy": 0.68,   # 原0.75, top ~3%
        "buy": 0.58,          # 原0.62, top ~10%
        "hold": 0.45,         # 原0.48, top ~35%
        "caution": 0.32,      # 原0.35
    })
    grade_thresholds: Mapping[str, float] = field(default_factory=lambda: {
        "A": 0.68,    # 原0.75
        "B": 0.58,    # 原0.62
        "C": 0.45,    # 原0.48
        "D": 0.32,    # 原0.35
    })


# ============================================================================
# 主配置类
# ============================================================================

@dataclass(frozen=True)
class TruthConfig:
    """T.R.U.T.H. 系统主配置"""
    algo_version: str = "5.4.0"
    config_version: str = "default"

    # Layer 0
    time_decay: TimeDecayConfig = field(default_factory=TimeDecayConfig)

    # Layer 1 (七维因子)
    alpha_config: AlphaFactorConfig = field(default_factory=AlphaFactorConfig)
    beta_config: BetaFactorConfig = field(default_factory=BetaFactorConfig)
    gamma_config: GammaFactorConfig = field(default_factory=GammaFactorConfig)
    lambda_config: LambdaFactorConfig = field(default_factory=LambdaFactorConfig)
    delta_fraud_config: DeltaFraudFactorConfig = field(default_factory=DeltaFraudFactorConfig)
    delta_decay_config: DeltaDecayFactorConfig = field(default_factory=DeltaDecayFactorConfig)
    verification_config: VerificationFactorConfig = field(default_factory=VerificationFactorConfig)

    # 宏观参数
    macro: MacroConfig = field(default_factory=MacroConfig)

    # Layer 2
    gravity_solver: GravitySolverConfig = field(default_factory=GravitySolverConfig)
    velocity_solver: VelocitySolverConfig = field(default_factory=VelocitySolverConfig)
    structure_solver: StructureSolverConfig = field(default_factory=StructureSolverConfig)

    # Layer 3
    calibration: CalibrationConfig = field(default_factory=CalibrationConfig)
    scoring: ScoringConfig = field(default_factory=ScoringConfig)

    # 全局开关
    fraud_meltdown_enabled: bool = True
    debug_mode: bool = False


# ============================================================================
# 配置工厂函数
# ============================================================================

def get_default_config() -> TruthConfig:
    """获取默认配置"""
    return TruthConfig()


def get_conservative_config() -> TruthConfig:
    """保守配置 (更严格的阈值)"""
    return TruthConfig(
        config_version="conservative",
        gravity_solver=GravitySolverConfig(
            base_roic_threshold=10.0,
            base_roe_threshold=12.0,
        ),
        delta_fraud_config=DeltaFraudFactorConfig(
            meltdown_threshold=0.50,
        ),
    )


def get_growth_focused_config() -> TruthConfig:
    """成长导向配置"""
    return TruthConfig(
        config_version="growth_focused",
        scoring=ScoringConfig(
            solver_weights={
                "GRAVITY": 0.30,
                "VELOCITY": 0.45,
                "STRUCTURE": 0.25,
            }
        ),
    )


# ============================================================================
# 导出
# ============================================================================

__all__ = [
    # Layer 0
    "TimeDecayConfig",
    # Layer 1
    "AlphaFactorConfig",
    "BetaFactorConfig",
    "GammaFactorConfig",
    "LambdaFactorConfig",
    "DeltaFraudFactorConfig",
    "DeltaDecayFactorConfig",
    "VerificationFactorConfig",
    # 宏观参数
    "MacroConfig",
    # Layer 2
    "GravitySolverConfig",
    "VelocitySolverConfig",
    "StructureSolverConfig",
    # Layer 3
    "CalibrationConfig",
    "ScoringConfig",
    # 主配置
    "TruthConfig",
    "get_default_config",
    "get_conservative_config",
    "get_growth_focused_config",
]
