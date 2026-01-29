"""T.R.U.T.H. 配置系统 (v3.2)

四层配置体系：
    - Layer 0: 时间衰减参数
    - Layer 1: 六维因子配置
    - Layer 2: 三大求解器参数
    - Layer 3: 校准与评分

设计原则：只保留实际被使用的配置项

版本: 3.2.0
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Tuple

from .domain import FactorId


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
    """α 因子 (周期性) - 业绩对宏观经济的敏感弹性"""
    component_weights: Mapping[str, float] = field(default_factory=lambda: {
        "detrended_cv": 0.35,
        "r_squared_inverse": 0.25,
        "cv": 0.20,
        "is_cyclical": 0.15,
        "hurst_exponent": 0.05,
    })


@dataclass(frozen=True)
class BetaFactorConfig:
    """β 因子 (资本密度) - 赚取利润所需的资本投入"""
    component_weights: Mapping[str, float] = field(default_factory=lambda: {
        "roic_level": 0.40,
        "roic_volatility": 0.25,
        "margin_level": 0.20,
        "ocf_to_profit_ratio": 0.15,
    })


@dataclass(frozen=True)
class GammaFactorConfig:
    """γ 因子 (成长动能) - 业务扩张加速度"""
    component_weights: Mapping[str, float] = field(default_factory=lambda: {
        "cagr": 0.35,
        "log_slope": 0.25,
        "recent_3y_slope": 0.20,
        "robust_slope": 0.15,
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
    too_smooth_cv_threshold: float = 0.01
    too_perfect_r2_threshold: float = 0.99


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
class VerificationFactorConfig:
    """V 因子 (真相验证) - 成长含金量验证"""
    component_weights: Mapping[str, float] = field(default_factory=lambda: {
        "ocf_revenue_ratio": 0.50,
        "ocf_profit_ratio": 0.30,
        "consistency": 0.20,
    })
    true_growth_threshold: float = 0.8
    fake_growth_threshold: float = 0.3


# ============================================================================
# Layer 2: 求解器配置
# ============================================================================

@dataclass(frozen=True)
class GravitySolverConfig:
    """重力求解器 - ROIC 动态阈值计算"""
    base_roic_threshold: float = 8.0
    base_roe_threshold: float = 10.0
    k_light_asset: float = 0.08
    k_cycle_tolerance: float = 0.04
    k_decay_penalty: float = 0.06
    k_verification_bonus: float = 0.03
    fraud_meltdown_enabled: bool = True


@dataclass(frozen=True)
class VelocitySolverConfig:
    """速度求解器 - 增长边界计算"""
    gdp_growth_rate: float = 5.0
    cpi_rate: float = 2.0
    k_true_growth: float = 0.15
    k_cycle_tolerance: float = 0.10
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
    """校准配置 - 规模/行业/置信度调整"""
    size_adjustments: Mapping[str, float] = field(default_factory=lambda: {
        "mega": -1.5,   # >1000亿
        "large": 0.0,   # 300-1000亿
        "mid": 1.0,     # 100-300亿
        "small": 2.0,   # 30-100亿
        "micro": 3.0,   # <30亿
    })
    min_data_years: int = 3
    full_confidence_years: int = 10
    max_confidence_5y: float = 0.55
    industry_adjustment_enabled: bool = True
    industry_adjustment_weight: float = 0.05


@dataclass(frozen=True)
class ScoringConfig:
    """综合评分配置"""
    factor_weights: Mapping[str, float] = field(default_factory=lambda: {
        "ALPHA": 0.10,
        "BETA": 0.10,
        "GAMMA": 0.25,
        "DELTA_FRAUD": 0.20,
        "DELTA_DECAY": 0.15,
        "VERIFICATION": 0.20,
    })
    solver_weights: Mapping[str, float] = field(default_factory=lambda: {
        "GRAVITY": 0.40,
        "VELOCITY": 0.35,
        "STRUCTURE": 0.25,
    })
    factor_vs_solver_weight: float = 0.4
    signal_thresholds: Mapping[str, float] = field(default_factory=lambda: {
        "strong_buy": 0.85,   # 提高: 0.80 -> 0.85
        "buy": 0.72,          # 提高: 0.65 -> 0.72
        "hold": 0.55,         # 提高: 0.50 -> 0.55
        "caution": 0.40,      # 提高: 0.35 -> 0.40
    })
    grade_thresholds: Mapping[str, float] = field(default_factory=lambda: {
        "A": 0.85,            # 提高: 0.80 -> 0.85
        "B": 0.72,            # 提高: 0.65 -> 0.72
        "C": 0.55,            # 提高: 0.50 -> 0.55
        "D": 0.40,            # 提高: 0.35 -> 0.40
    })


# ============================================================================
# 主配置类
# ============================================================================

@dataclass(frozen=True)
class TruthConfig:
    """T.R.U.T.H. 系统主配置"""
    algo_version: str = "3.0.0"
    config_version: str = "default"

    # Layer 0
    time_decay: TimeDecayConfig = field(default_factory=TimeDecayConfig)

    # Layer 1
    alpha_config: AlphaFactorConfig = field(default_factory=AlphaFactorConfig)
    beta_config: BetaFactorConfig = field(default_factory=BetaFactorConfig)
    gamma_config: GammaFactorConfig = field(default_factory=GammaFactorConfig)
    delta_fraud_config: DeltaFraudFactorConfig = field(default_factory=DeltaFraudFactorConfig)
    delta_decay_config: DeltaDecayFactorConfig = field(default_factory=DeltaDecayFactorConfig)
    verification_config: VerificationFactorConfig = field(default_factory=VerificationFactorConfig)

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
    "DeltaFraudFactorConfig",
    "DeltaDecayFactorConfig",
    "VerificationFactorConfig",
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
