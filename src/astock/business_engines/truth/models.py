"""
T.R.U.T.H. System - Data Models
===============================

定义所有核心数据结构：
- CompanyGenome: 六维基因组
- TruthResult: 最终计算结果
- ClusterProfile: 聚类画像
- CalibrationResult: 校准结果

设计原则：
1. 不可变数据类 (frozen=True)
2. 完整的类型注解
3. 向量化友好的结构
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum
import numpy as np


class SignalType(Enum):
    """筛选信号类型"""
    STRONG_BUY = "strong_buy"      # 强烈买入
    BUY = "buy"                     # 买入
    HOLD = "hold"                   # 持有/观望
    AVOID = "avoid"                 # 回避
    STRONG_AVOID = "strong_avoid"  # 强烈回避（欺诈风险/严重衰退）


class GradeLevel(Enum):
    """评级等级"""
    S = "S"  # 顶级（印钞机）
    A = "A"  # 优秀
    B = "B"  # 良好
    C = "C"  # 一般
    D = "D"  # 较差
    F = "F"  # 不合格/危险


@dataclass(frozen=True)
class CompanyGenome:
    """
    六维基因组

    每个基因归一化到 [0, 1] 区间：
    - α (alpha): 周期性基因 - 0=稳定, 1=强周期
    - β (beta): 资本密度基因 - 0=轻资产, 1=重资产
    - γ (gamma): 成长动能基因 - 0=衰退, 1=高成长
    - δ_fraud: 欺诈熵基因 - 0=透明, 1=高风险
    - δ_decay: 衰退熵基因 - 0=稳定, 1=严重衰退
    - V (verification): 真相验证因子 - 0=低可信, 1=高可信

    验证因子V用于分母调整，不参与聚类
    """
    ts_code: str
    company_name: str

    # 核心六维基因
    alpha: float          # 周期性 [0,1]
    beta: float           # 资本密度 [0,1]
    gamma: float          # 成长动能 [0,1]
    delta_fraud: float    # 欺诈熵 [0,1]
    delta_decay: float    # 衰退熵 [0,1]
    verification: float   # 验证因子 [0,1]

    # 原始指标（用于计算）
    roic_series: Tuple[float, ...] = field(default_factory=tuple)
    roe_series: Tuple[float, ...] = field(default_factory=tuple)
    revenue_growth_series: Tuple[float, ...] = field(default_factory=tuple)
    gross_margin_series: Tuple[float, ...] = field(default_factory=tuple)

    # 元数据
    industry: str = ""
    data_years: int = 5
    data_quality_score: float = 1.0

    def to_vector(self, include_verification: bool = False) -> np.ndarray:
        """
        转换为向量用于聚类

        Args:
            include_verification: 是否包含V因子（默认不包含）

        Returns:
            5维或6维向量
        """
        if include_verification:
            return np.array([
                self.alpha, self.beta, self.gamma,
                self.delta_fraud, self.delta_decay, self.verification
            ])
        else:
            # 聚类只用5维（V因子不参与）
            return np.array([
                self.alpha, self.beta, self.gamma,
                self.delta_fraud, self.delta_decay
            ])

    @property
    def is_fraud_risk(self) -> bool:
        """是否触发欺诈熔断"""
        return self.delta_fraud >= 0.58

    @property
    def is_severe_decay(self) -> bool:
        """是否严重衰退"""
        return self.delta_decay >= 0.7

    @property
    def gene_labels(self) -> Dict[str, str]:
        """基因解读标签"""
        labels = {}

        # α - 周期性
        if self.alpha < 0.3:
            labels['alpha'] = "🛡️ 防御型"
        elif self.alpha < 0.6:
            labels['alpha'] = "📊 温和周期"
        else:
            labels['alpha'] = "🎢 强周期"

        # β - 资本密度
        if self.beta < 0.3:
            labels['beta'] = "🪶 轻资产"
        elif self.beta < 0.6:
            labels['beta'] = "⚖️ 中等资产"
        else:
            labels['beta'] = "🏭 重资产"

        # γ - 成长动能
        if self.gamma < 0.3:
            labels['gamma'] = "📉 衰退/停滞"
        elif self.gamma < 0.6:
            labels['gamma'] = "➡️ 稳定"
        else:
            labels['gamma'] = "🚀 高成长"

        # δ_fraud - 欺诈熵
        if self.delta_fraud < 0.3:
            labels['delta_fraud'] = "✅ 透明"
        elif self.delta_fraud < 0.58:
            labels['delta_fraud'] = "⚠️ 需关注"
        else:
            labels['delta_fraud'] = "🚨 高风险"

        # δ_decay - 衰退熵
        if self.delta_decay < 0.3:
            labels['delta_decay'] = "💪 稳健"
        elif self.delta_decay < 0.6:
            labels['delta_decay'] = "⚠️ 有压力"
        else:
            labels['delta_decay'] = "🔻 严重衰退"

        # V - 验证因子
        if self.verification < 0.3:
            labels['verification'] = "❓ 低可信"
        elif self.verification < 0.6:
            labels['verification'] = "📋 一般"
        else:
            labels['verification'] = "✅ 高可信"

        return labels


@dataclass(frozen=True)
class RepresentativeMetrics:
    """
    代表性指标计算结果

    核心公式：
    Rep_ROIC = EWMA(ROIC) + Δ_momentum + Δ_deterioration
    """
    # 基础统计
    ewma_value: float         # 指数加权移动平均
    simple_mean: float        # 简单平均
    latest_value: float       # 最新一年值

    # 趋势修正
    trend_slope: float        # OLS斜率
    momentum_delta: float     # 非对称动量修正（只惩罚下跌）
    deterioration_delta: float  # 恶化惩罚

    # 最终代表性值
    representative_value: float  # = ewma + momentum + deterioration

    # 保护性下限
    floor_value: float        # max(latest * 0.4, -0.05)
    final_value: float        # max(representative, floor)


@dataclass(frozen=True)
class ThresholdResult:
    """
    动态阈值计算结果 v3.0

    上帝方程 I v3.0 (非线性风险乘数版):
    T_roic = R_f × (1 + λ·δ_fraud) + k₁·β·(1 + κ·α) - k₃(γ·E·V_gate) + k₄·δ_decay·(1 + φ·β)

    v3.0 核心进化:
    1. 基准利率质量膨胀: R_f × (1 + λ×δ_fraud)
    2. 双重杠杆交互项: β × (1 + κ×α)
    3. V因子门控: V_gate = V_eff × I(V > 0.4)
    4. 衰退-资产交互: δ_decay × (1 + φ×β)
    """
    # 各项分解（必需字段）
    base_rate: float          # R_f × (1+λδ_fraud) 风险膨胀后的资金成本
    beta_premium: float       # k₁×β×(1+κα) 双重杠杆交互（正数）
    alpha_discount: float     # -k₂α 周期豁免（v3.0: 已融入交互项）
    growth_discount: float    # -k₃(γ×E×V_gate) 真成长奖励（正数）
    decay_penalty: float      # +k₄×δ_decay×(1+φβ) 衰退-资产交互（正数）

    # 校准修正
    cluster_residual: float   # Δ_cluster 聚类残差
    size_residual: float      # Δ_size 规模残差

    # 最终阈值
    theory_threshold: float   # 理论阈值（校准前）
    final_threshold: float    # 最终阈值（校准后）

    # v3.0 新增字段（带默认值）
    fraud_premium: float = 0.0        # v2.0遗留，v3.0已融入base_rate
    circuit_break: bool = False       # 熔断标志
    circuit_break_reason: str = ""    # 熔断原因
    v_effective: float = 0.0          # V因子变换后的值
    fraud_penalty_factor: float = 1.0 # v3.0: 风险乘数
    gate_passed: bool = True          # v3.0新增: V门控状态
    leverage_interaction: float = 0.0 # v3.0新增: β×(1+κα)交互项

    # 可选字段（向后兼容）
    alpha_premium: float = 0.0      # 已废弃
    verification_bonus: float = 0.0  # 已废弃
    min_threshold: float = 0.02     # 阈值下限保护


@dataclass(frozen=True)
class GrowthBoundResult:
    """
    增长边界计算结果 v3.0

    上帝方程 II v3.0 (空气动力学版):
    T_growth = [GDP_g + k₁(γ×E×V_gate)] / (1 + C_D) + CycleModifier - DecayPenalty

    其中 C_D = w₁×β + w₂×(1-MarketCapRank) + w₃×δ_fraud

    v3.0 核心进化:
    1. 空气阻力系数 C_D 作为分母
    2. V因子一票否决
    3. 周期位置增长修正
    """
    gdp_growth: float              # GDP_g 经济增长基准
    true_growth_boost: float       # +k₁(γ×E×V_gate) 真成长加速
    cycle_reversal: float          # v3.0: CycleModifier 周期修正
    max_sustainable_growth: float  # 最大可持续增长率
    min_expected_growth: float     # 最小期望增长率

    # v3.0 新增字段（带默认值）
    decay_adjustment: float = 0.0  # 衰退调整
    cycle_position: float = 0.5    # 周期位置 [0,1]
    v_effective: float = 0.0       # V因子变换后的值
    gate_passed: bool = True       # v3.0新增: V门控状态
    drag_coefficient: float = 0.0  # v3.0新增: 空气阻力系数 C_D


@dataclass(frozen=True)
class SlopeResult:
    """
    斜率预测结果 v3.0

    上帝方程 III v3.0 (动态通道版):
    T_slope = [BaseSlope] × ChannelMultiplier × FraudPenalty

    其中 BaseSlope = -0.02 + k₁(1-β) + k₃(γ×V_gate) - k₂×δ_decay×(1+ω×β)

    v3.0 核心进化:
    1. 周期位置感知动态通道
    2. 拐点加速杀: δ_decay × (1 + ω×β)
    3. V因子门控
    4. 欺诈因子斜率惩罚
    """
    natural_decay: float           # -0.02 自然衰退基线
    asset_advantage: float         # +k₁(1-β) 轻资产优势
    decay_acceleration: float      # -k₂×δ_decay×(1+ωβ) 衰退-资产交互
    expected_slope: float          # 预期斜率

    # v3.0 新增字段（带默认值）
    growth_support: float = 0.0    # +k₃(γ×V_gate) 真成长支撑
    cycle_adjustment: float = 0.0  # v2.0遗留，v3.0改用channel_multiplier
    cycle_position: float = 0.5    # 周期位置 [0,1]
    v_effective: float = 0.0       # V因子变换后的值
    gate_passed: bool = True       # v3.0新增: V门控状态
    channel_multiplier: float = 1.0  # v3.0新增: 动态通道乘数
    channel_name: str = "neutral"   # v3.0新增: 通道名称
    fraud_penalty: float = 1.0     # v3.0新增: 欺诈斜率惩罚因子


@dataclass(frozen=True)
class TruthResult:
    """
    T.R.U.T.H. 系统最终输出

    包含完整的分析结果和决策依据
    """
    ts_code: str
    company_name: str

    # 六维基因
    genome: CompanyGenome

    # 代表性指标
    rep_roic: RepresentativeMetrics
    rep_roe: Optional[RepresentativeMetrics] = None

    # 动态阈值
    threshold: ThresholdResult = None

    # 最终筛选结果
    passes_screen: bool = False
    signal: SignalType = SignalType.HOLD
    grade: GradeLevel = GradeLevel.C

    # 超额收益
    excess_return: float = 0.0  # Rep_ROIC - T_final

    # 置信度
    confidence: float = 0.5    # [0, 1]

    # 聚类信息
    cluster_id: int = -1
    cluster_archetype: str = ""

    # 诊断信息
    warnings: List[str] = field(default_factory=list)
    breakdown: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        # 计算派生字段（因为frozen=True，需要用object.__setattr__）
        if self.threshold and self.rep_roic:
            excess = self.rep_roic.final_value - self.threshold.final_threshold
            object.__setattr__(self, 'excess_return', excess)


@dataclass
class ClusterProfile:
    """
    聚类画像

    描述一个聚类的特征和包含的公司
    """
    cluster_id: int
    archetype: str  # 原型名称，如"印钞机型"、"重周期型"

    # 聚类中心（5维）
    centroid: Dict[str, float]  # {alpha: 0.2, beta: 0.3, ...}

    # 统计信息
    count: int
    member_codes: List[str] = field(default_factory=list)

    # 校准参数
    residual_target: float = 0.0  # 残差修正目标
    top20_median_roic: float = 0.0  # Top20%公司的ROIC中位数

    # 基因统计
    gene_stats: Dict[str, Dict[str, float]] = field(default_factory=dict)
    # 格式: {gene_name: {mean, std, min, max}}


@dataclass
class CalibrationResult:
    """
    校准结果

    包含所有校准参数和诊断信息
    """
    # 聚类校准
    cluster_residuals: Dict[int, float]  # cluster_id -> Δ_cluster

    # 规模校准
    size_residuals: Dict[str, float]  # size_bucket -> Δ_size
    # size_bucket: "micro", "small", "mid", "large", "mega"

    # 校准元数据
    calibration_date: str
    sample_size: int
    convergence_metric: float  # 收敛指标（残差平方和变化率）

    # 诊断信息
    warnings: List[str] = field(default_factory=list)


@dataclass
class FraudCheckResult:
    """
    欺诈检测结果

    基于四维熵的综合评估
    """
    # 四维子熵
    accrual_entropy: float     # 应计异常熵
    fcf_divergence: float      # 现金流背离熵
    goodwill_risk: float       # 商誉风险熵
    related_party_entropy: float  # 关联交易熵

    # 综合熵
    combined_entropy: float    # δ_fraud

    # 熔断判断
    is_fused: bool            # 是否触发熔断
    fuse_reason: str = ""     # 熔断原因

    # 商誉硬杀
    goodwill_to_equity: float = 0.0
    is_goodwill_kill: bool = False


@dataclass
class BatchResult:
    """
    批量计算结果

    用于处理整个公司池
    """
    results: List[TruthResult]
    cluster_profiles: List[ClusterProfile]
    calibration: CalibrationResult

    # 聚合统计
    total_count: int
    passed_count: int
    pass_rate: float

    # 信号分布
    signal_distribution: Dict[SignalType, int] = field(default_factory=dict)
    grade_distribution: Dict[GradeLevel, int] = field(default_factory=dict)

    # 计算元数据
    computation_time_seconds: float = 0.0
    config_version: str = ""

    def get_passed(self) -> List[TruthResult]:
        """获取通过筛选的公司"""
        return [r for r in self.results if r.passes_screen]

    def get_by_grade(self, grade: GradeLevel) -> List[TruthResult]:
        """按评级筛选"""
        return [r for r in self.results if r.grade == grade]

    def get_by_signal(self, signal: SignalType) -> List[TruthResult]:
        """按信号筛选"""
        return [r for r in self.results if r.signal == signal]

    def to_dataframe(self):
        """转换为DataFrame"""
        import pandas as pd

        records = []
        for r in self.results:
            records.append({
                'ts_code': r.ts_code,
                'company_name': r.company_name,
                'alpha': r.genome.alpha,
                'beta': r.genome.beta,
                'gamma': r.genome.gamma,
                'delta_fraud': r.genome.delta_fraud,
                'delta_decay': r.genome.delta_decay,
                'verification': r.genome.verification,
                'rep_roic': r.rep_roic.final_value if r.rep_roic else None,
                'threshold': r.threshold.final_threshold if r.threshold else None,
                'excess_return': r.excess_return,
                'passes_screen': r.passes_screen,
                'signal': r.signal.value,
                'grade': r.grade.value,
                'confidence': r.confidence,
                'cluster_id': r.cluster_id,
                'cluster_archetype': r.cluster_archetype,
            })

        return pd.DataFrame(records)
