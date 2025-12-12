"""
T.R.U.T.H. System - Configuration Module
========================================

所有可调参数的集中管理：
- 宏观经济参数（支持动态获取）
- 基因计算参数
- 物理求解器系数
- 校准参数

设计原则：
1. 所有魔法数字都有明确的经济学/统计学依据
2. 宏观参数支持动态注入
3. 参数分层：核心参数 vs 微调参数
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# 宏观经济参数获取接口
# ============================================================================

def get_10y_bond_yield() -> float:
    """
    获取中国10年期国债收益率

    生产环境可对接 Wind/东方财富/Tushare API
    当前返回保守默认值
    """
    # TODO: 接入实时数据源
    # 2024年底约 2.0-2.5%，我们用2.5%作为保守估计
    return 0.025


def get_gdp_growth_rate() -> float:
    """
    获取GDP增长率预期

    生产环境可对接宏观数据API
    当前返回官方目标值
    """
    # 2024-2025年官方目标约 5%
    return 0.05


def get_market_risk_premium() -> float:
    """
    获取市场风险溢价（ERP）

    A股长期ERP约 5-7%
    """
    return 0.06


# ============================================================================
# 核心配置数据类
# ============================================================================

@dataclass
class MacroParams:
    """
    宏观经济参数

    用于上帝方程中的基准利率和市场情绪因子E
    """

    # 无风险利率（10年期国债收益率）
    risk_free_rate: float = 0.025

    # GDP增长率预期
    gdp_growth_rate: float = 0.05

    # 市场风险溢价
    equity_risk_premium: float = 0.06

    # 名义GDP增长率（用于速度求解器）
    gdp_nominal_growth: float = 0.05

    # 市场情绪因子 E（用于上帝方程 I 中的 γ×E×V 项）
    # E=1.0: 中性市场
    # E>1.0: 牛市情绪，允许更高估值（对成长股更宽容）
    # E<1.0: 熊市情绪，更保守（要求更高的当期回报）
    market_sentiment_factor: float = 1.0

    # 参数获取时间戳
    timestamp: str = ""

    @classmethod
    def create_dynamic(cls) -> "MacroParams":
        """动态获取宏观参数"""
        try:
            return cls(
                risk_free_rate=get_10y_bond_yield(),
                gdp_growth_rate=get_gdp_growth_rate(),
                equity_risk_premium=get_market_risk_premium(),
                timestamp=datetime.now().isoformat(),
            )
        except Exception as e:
            logger.warning(f"动态获取宏观参数失败，使用默认值: {e}")
            return cls(timestamp=datetime.now().isoformat())


@dataclass
class GeneComputeParams:
    """基因计算参数"""

    # ========== α 周期性基因 ==========
    # CV阈值饱和点（CV超过此值视为强周期）
    cv_saturation: float = 4.0

    # 峰谷比饱和点
    peak_trough_saturation: float = 9.0

    # 周期因子权重
    cycle_factor_weights: Dict[str, float] = field(default_factory=lambda: {
        'detrended_cv': 0.30,       # 去趋势CV（最重要）
        'peak_to_trough': 0.25,     # 峰谷比
        'low_r_squared': 0.20,      # 低拟合度
        'wave_pattern': 0.15,       # 波动模式
        'reversal_count': 0.10,     # 反转次数
    })

    # ========== β 资本密度基因 ==========
    # 因子权重
    beta_factor_weights: Dict[str, float] = field(default_factory=lambda: {
        'roic_volatility': 0.40,    # ROIC波动（主要代理）
        'margin_stability': 0.30,   # 毛利率稳定性
        'capex_intensity': 0.30,    # 资本支出强度（如有数据）
    })

    # ========== γ 成长动能基因 ==========
    # 营收增速权重
    revenue_growth_weight: float = 0.5

    # 利润增速权重
    profit_growth_weight: float = 0.5

    # 复合增长率年化参数
    cagr_years: int = 3

    # γ归一化：增长率映射到[0,1]
    # 0% -> 0.3, 15% -> 0.6, 30%+ -> 0.9
    gamma_growth_anchors: Dict[str, float] = field(default_factory=lambda: {
        'zero': 0.3,      # 0%增长对应的γ
        'moderate': 0.6,  # 15%增长对应的γ
        'high': 0.9,      # 30%+增长对应的γ
    })

    # ========== δ_fraud 欺诈熵基因 ==========
    # 熔断阈值（超过此值直接排除）
    fraud_fuse_threshold: float = 0.58

    # 商誉硬杀阈值（商誉/净资产）
    goodwill_kill_threshold: float = 0.40

    # 四维熵权重
    fraud_entropy_weights: Dict[str, float] = field(default_factory=lambda: {
        'accrual_anomaly': 0.30,      # 应计异常
        'fcf_divergence': 0.30,       # 现金流背离
        'goodwill_risk': 0.25,        # 商誉风险
        'related_party': 0.15,        # 关联交易
    })

    # 应计异常阈值（应计利润/总资产）
    accrual_anomaly_threshold: float = 0.10

    # 现金流背离阈值（|净利润-经营现金流|/营收）
    fcf_divergence_threshold: float = 0.15

    # ========== δ_decay 衰退熵基因 ==========
    # 近期恶化窗口（年）
    decay_recent_years: int = 2

    # 严重衰退斜率阈值
    severe_decay_slope: float = -0.05

    # 衰退熵权重
    decay_entropy_weights: Dict[str, float] = field(default_factory=lambda: {
        'recent_decline': 0.40,    # 近期下滑
        'acceleration': 0.30,      # 下滑加速
        'margin_erosion': 0.30,    # 毛利率侵蚀
    })

    # ========== V 验证因子 ==========
    # 预收奖励上限
    advance_receipt_bonus_cap: float = 0.25

    # 预收/营收系数
    advance_receipt_coefficient: float = 0.6

    # 现金转化系数
    cash_conversion_coefficient: float = 0.4


@dataclass
class SolverParams:
    """
    物理求解器参数 v3.0 终极版

    ========== 上帝方程 I v3.0 (Gravity Solver - 非线性风险乘数) ==========
    T_roic = R_f × (1 + λ·δ_fraud) + k₁·β·(1 + κ·α) - k₃(γ·E·V_gate) + k₄·δ_decay·(1 + φ·β)

    ========== 上帝方程 II v3.0 (Velocity Solver - 空气动力学) ==========
    T_growth = [GDP_g + k₁(γ×E×V_gate)] / (1 + C_D) + CycleModifier - DecayPenalty
    其中 C_D = w₁β + w₂(1-MarketCapRank) + w₃δ_fraud

    ========== 上帝方程 III v3.0 (Structure Solver - 动态通道) ==========
    T_slope = [BaseSlope] × ChannelMultiplier × FraudPenalty
    其中 BaseSlope = -0.02 + k₁(1-β) + k₃(γ×V_gate) - k₂×δ_decay×(1+ω×β)

    v3.0 核心进化（融合 Gemini 建议）:
    1. 基准利率质量膨胀: R_f × (1 + λ×δ_fraud)
    2. 双重杠杆交互项: β × (1 + κ×α)
    3. 空气阻力系数: C_D 作为分母
    4. 周期位置动态通道: 乘数效应
    5. V因子一票否决: V < 0.4 门控
    """

    # ========== 重力求解器 v3.0（分母） ==========
    # T = R_f × (1 + λ×δ_fraud) + k₁×β×(1 + κ×α) - k₃(γEV_gate) + k₄×δ_decay×(1+φβ)

    # k₁: 重资产惩罚基础系数（β）
    k1_beta: float = 0.08

    # λ (lambda_risk): 风险乘数系数（δ_fraud膨胀基准利率）v3.0新增
    # δ_fraud=0.5 时，资金成本膨胀 1.4x
    lambda_risk: float = 0.8

    # κ (kappa_leverage): 双重杠杆交互系数 v3.0新增
    # β × (1 + κ×α): 重资产+强周期=毁灭机器
    kappa_leverage: float = 0.5

    # φ (phi_decay): 衰退-资产交互系数 v3.0新增
    # δ_decay × (1 + φ×β): 重资产衰退更痛
    phi_decay: float = 0.4

    # k₂: 周期豁免系数（α）—— v3.0: α已融入交互项，此参数降低重要性
    k2_alpha: float = 0.03

    # k₃: 真成长奖励系数（γ×E×V_gate）
    k3_gamma: float = 0.08

    # k₄: 衰退惩罚基础系数（δ_decay）
    k4_decay: float = 0.08

    # k₅: 欺诈风险溢价系数 —— v3.0: 已融入λ乘数，保留向后兼容
    k5_fraud: float = 0.06

    # k₄: 验证奖励系数（废弃，已整合到 k3_gamma 中）
    k4_verification: float = 0.03  # 保留向后兼容

    # 阈值下限保护
    threshold_floor: float = 0.02  # 最低2%

    # 阈值上限保护
    threshold_ceiling: float = 0.25  # 最高25%

    # ========== 速度求解器 v3.0（增速评估 - 空气动力学） ==========
    # T_growth = [GDP_g + k₁(γ×E×V_gate)] / (1 + C_D) + CycleModifier - DecayPenalty

    # k₁: 真成长加速系数（γ×E×V_gate）
    k1_velocity: float = 0.15

    # k₂: 周期反转期望系数 —— v3.0: 改用CycleModifier
    k2_velocity: float = 0.10

    # k₃: 衰退调整系数
    k3_velocity_decay: float = 0.08

    # ===== 空气阻力系数 C_D 参数 v3.0新增 =====
    # C_D = w₁×β + w₂×(1-MarketCapRank) + w₃×δ_fraud

    # w₁: β 对阻力的贡献（重资产阻力大）
    drag_w1_beta: float = 0.3

    # w₂: 规模对阻力的贡献（小盘股阻力大）
    drag_w2_size: float = 0.2

    # w₃: 欺诈对阻力的贡献（造假公司阻力大）
    drag_w3_fraud: float = 0.4

    # 增长边界保护
    growth_floor: float = -0.05   # 最低-5%
    growth_ceiling: float = 0.30  # 最高30%

    # 增速加分系数（旧参数，保留向后兼容）
    growth_bonus_coefficient: float = 0.5

    # 增速衰减惩罚（旧参数，保留向后兼容）
    growth_decay_penalty: float = 0.8

    # ========== 结构求解器 v3.0（毛利率斜率 - 动态通道） ==========
    # T_slope = [BaseSlope] × ChannelMultiplier × FraudPenalty
    # BaseSlope = -0.02 + k₁(1-β) + k₃(γ×V_gate) - k₂×δ_decay×(1+ω×β)

    # 自然衰退基线（熵增定律）
    natural_decay_rate: float = -0.02

    # k₁: 轻资产优势系数（1-β）
    k1_structure: float = 0.05

    # k₂: 衰退加速系数（δ_decay）
    k2_structure: float = 0.08

    # k₃: 真成长支撑系数
    k3_structure: float = 0.03

    # k₄: 周期位置调整系数 —— v3.0: 改用ChannelMultiplier
    k4_structure: float = 0.02

    # ω (omega_decay_asset): 衰退-资产交互系数 v3.0新增
    # δ_decay × (1 + ω×β): 重资产公司衰退加速
    omega_decay_asset: float = 0.5

    # 斜率边界保护
    slope_floor: float = -0.10   # 最低-10%/年
    slope_ceiling: float = 0.05  # 最高+5%/年

    # 毛利率斜率恶化阈值（旧参数，保留向后兼容）
    margin_slope_warning: float = -0.02  # 年化-2%



@dataclass
class RepresentativeParams:
    """代表性指标计算参数"""

    # ========== EWMA参数 ==========
    # 默认权重（5年）
    default_weights: List[float] = field(
        default_factory=lambda: [0.10, 0.15, 0.20, 0.25, 0.30]
    )

    # 权重衰减因子（用于生成其他年份权重）
    weight_decay: float = 0.8

    # ========== 非对称动量修正 ==========
    # 上涨奖励系数（设为0，不奖励上涨）
    upward_momentum_coef: float = 0.0

    # 下跌惩罚系数
    downward_momentum_coef: float = 0.8

    # 惩罚放大倍数
    downward_penalty_multiplier: float = 2.0

    # ========== 地板保护 ==========
    # 地板比例（相对最新一年）
    floor_ratio: float = 0.4

    # 绝对地板（硬性下限）
    absolute_floor: float = -0.05


@dataclass
class CalibrationParams:
    """校准参数"""

    # ========== 聚类校准 ==========
    # 默认聚类数
    n_clusters: int = 20

    # 随机种子（保证可复现）
    random_state: int = 42

    # KMeans迭代次数
    kmeans_n_init: int = 10

    # Top N% 用于计算聚类残差目标
    top_percentile: float = 0.20

    # 残差学习率（Δ_cluster = 残差 × λ）
    residual_lambda: float = 0.5

    # ========== 规模校准 ==========
    # 规模分层阈值（市值百分位）
    size_buckets: Dict[str, float] = field(default_factory=lambda: {
        'micro': 0.10,   # 0-10%
        'small': 0.30,   # 10-30%
        'mid': 0.60,     # 30-60%
        'large': 0.85,   # 60-85%
        'mega': 1.00,    # 85-100%
    })

    # 规模残差上限
    size_residual_cap: float = 0.03

    # ========== 置信度 ==========
    # 5年数据置信度上限
    five_year_confidence_ceiling: float = 0.55

    # 10年数据置信度上限
    ten_year_confidence_ceiling: float = 0.85

    # 数据质量惩罚
    data_quality_penalty: float = 0.1


@dataclass
class TruthConfig:
    """
    T.R.U.T.H. 系统总配置

    包含所有子配置模块
    """
    # 子配置
    macro: MacroParams = field(default_factory=MacroParams)
    genes: GeneComputeParams = field(default_factory=GeneComputeParams)
    solver: SolverParams = field(default_factory=SolverParams)
    representative: RepresentativeParams = field(default_factory=RepresentativeParams)
    calibration: CalibrationParams = field(default_factory=CalibrationParams)

    # 版本
    version: str = "1.0.0"

    # 调试模式
    debug: bool = False

    def __post_init__(self):
        """验证配置"""
        self._validate_weights()
        self._validate_thresholds()

    def _validate_weights(self):
        """验证权重和为1"""
        weight_configs = [
            ('cycle_factor_weights', self.genes.cycle_factor_weights),
            ('beta_factor_weights', self.genes.beta_factor_weights),
            ('fraud_entropy_weights', self.genes.fraud_entropy_weights),
            ('decay_entropy_weights', self.genes.decay_entropy_weights),
        ]

        for name, weights in weight_configs:
            total = sum(weights.values())
            if abs(total - 1.0) > 0.001:
                raise ValueError(f"{name} 权重和应为1.0，当前为{total:.3f}")

    def _validate_thresholds(self):
        """验证阈值合理性"""
        if self.solver.threshold_floor >= self.solver.threshold_ceiling:
            raise ValueError("threshold_floor 应小于 threshold_ceiling")

        if not (0 < self.genes.fraud_fuse_threshold < 1):
            raise ValueError("fraud_fuse_threshold 应在 (0, 1) 区间")

    @classmethod
    def create_with_dynamic_macro(cls) -> "TruthConfig":
        """创建带动态宏观参数的配置"""
        return cls(macro=MacroParams.create_dynamic())

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典（用于序列化）"""
        from dataclasses import asdict
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TruthConfig":
        """从字典创建（用于反序列化）"""
        return cls(
            macro=MacroParams(**data.get('macro', {})),
            genes=GeneComputeParams(**data.get('genes', {})),
            solver=SolverParams(**data.get('solver', {})),
            representative=RepresentativeParams(**data.get('representative', {})),
            calibration=CalibrationParams(**data.get('calibration', {})),
            version=data.get('version', '1.0.0'),
            debug=data.get('debug', False),
        )


# ============================================================================
# 全局默认配置
# ============================================================================

_DEFAULT_CONFIG: Optional[TruthConfig] = None


def get_default_truth_config() -> TruthConfig:
    """获取默认配置（懒加载单例）"""
    global _DEFAULT_CONFIG
    if _DEFAULT_CONFIG is None:
        _DEFAULT_CONFIG = TruthConfig()
    return _DEFAULT_CONFIG


def set_default_truth_config(config: TruthConfig) -> None:
    """设置默认配置"""
    global _DEFAULT_CONFIG
    _DEFAULT_CONFIG = config


# ============================================================================
# 配置预设
# ============================================================================

def create_conservative_config() -> TruthConfig:
    """保守配置 - 更严格的阈值"""
    config = TruthConfig()

    # 提高熔断阈值敏感度
    config.genes.fraud_fuse_threshold = 0.50

    # 降低成长折扣
    config.solver.k3_gamma = 0.03

    # 提高阈值下限
    config.solver.threshold_floor = 0.04

    return config


def create_aggressive_config() -> TruthConfig:
    """激进配置 - 更宽松的阈值"""
    config = TruthConfig()

    # 放宽熔断阈值
    config.genes.fraud_fuse_threshold = 0.65

    # 增加成长折扣
    config.solver.k3_gamma = 0.07

    # 降低阈值下限
    config.solver.threshold_floor = 0.01

    return config


def create_value_focused_config() -> TruthConfig:
    """价值投资配置 - 侧重现金流验证"""
    config = TruthConfig()

    # 增加验证因子权重
    config.solver.k4_verification = 0.05

    # 提高现金转化系数
    config.genes.cash_conversion_coefficient = 0.6

    return config


def create_growth_focused_config() -> TruthConfig:
    """成长投资配置 - 侧重动量"""
    config = TruthConfig()

    # 增加成长折扣
    config.solver.k3_gamma = 0.08

    # 降低周期性溢价
    config.solver.k1_alpha = 0.05

    return config
