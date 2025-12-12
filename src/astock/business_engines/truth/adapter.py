"""
T.R.U.T.H. System - Probe Adapter
=================================

探针适配器：将现有探针输出映射到基因计算输入。

核心职责：
1. 从 LogTrendResult, VolatilityResult, CyclicalPatternResult,
   RecentDeteriorationResult 等探针输出中提取字段
2. 映射到六维基因计算所需的标准化输入
3. 处理缺失字段、异常值的降级策略

设计原则：
- 复用而非重复：利用已有探针的专业计算（WLS、HP滤波、贝叶斯等）
- 显式映射：每个基因的数据来源清晰可追溯
- 优雅降级：部分探针缺失时仍能运行，但给出警告

参考：TRUTH_SYSTEM_DESIGN.md 第八章"探针映射表"

作者: AStock Analysis System
日期: 2025-01
"""

from dataclasses import dataclass, field
from typing import Dict, Optional, Any, List
import numpy as np
import logging

# 导入现有探针的输出模型
from ..analyzers.trend.models import (
    LogTrendResult,
    VolatilityResult,
    CyclicalPatternResult,
    RecentDeteriorationResult,
    RollingTrendResult,
    RobustTrendResult,
    InflectionResult,
)

logger = logging.getLogger(__name__)


# ============================================================================
# 探针输出集合
# ============================================================================

@dataclass
class ProbeOutputs:
    """
    单个指标的所有探针输出集合

    例如 ROIC 指标会有：
    - log_trend: LogTrendResult
    - volatility: VolatilityResult
    - cyclical: CyclicalPatternResult
    - deterioration: RecentDeteriorationResult
    - rolling: RollingTrendResult
    - robust: RobustTrendResult (可选)
    """
    indicator_name: str
    log_trend: Optional[LogTrendResult] = None
    volatility: Optional[VolatilityResult] = None
    cyclical: Optional[CyclicalPatternResult] = None
    deterioration: Optional[RecentDeteriorationResult] = None
    rolling: Optional[RollingTrendResult] = None
    robust: Optional[RobustTrendResult] = None
    inflection: Optional[InflectionResult] = None

    # 原始数据（降级用）
    raw_values: Optional[np.ndarray] = None

    def has_core_probes(self) -> bool:
        """检查核心探针是否齐全"""
        return all([
            self.log_trend is not None,
            self.volatility is not None,
            self.cyclical is not None,
            self.deterioration is not None,
        ])

    def missing_probes(self) -> List[str]:
        """列出缺失的探针"""
        missing = []
        if self.log_trend is None:
            missing.append("log_trend")
        if self.volatility is None:
            missing.append("volatility")
        if self.cyclical is None:
            missing.append("cyclical")
        if self.deterioration is None:
            missing.append("deterioration")
        if self.rolling is None:
            missing.append("rolling")
        return missing


@dataclass
class MultiIndicatorProbeOutputs:
    """
    多指标的探针输出集合（公司级别）

    一个公司需要多个指标的探针输出：
    - roic: ROIC的探针输出
    - gross_margin: 毛利率的探针输出
    - revenue: 营收的探针输出
    - ocf: 经营现金流的探针输出
    - net_profit: 净利润的探针输出
    """
    company_code: str
    company_name: str = ""

    # 核心指标探针输出
    roic: Optional[ProbeOutputs] = None
    gross_margin: Optional[ProbeOutputs] = None
    revenue: Optional[ProbeOutputs] = None
    ocf: Optional[ProbeOutputs] = None  # Operating Cash Flow
    net_profit: Optional[ProbeOutputs] = None

    # 辅助财务数据（用于 δ_fraud 和 V 因子）
    total_assets: float = 0.0
    equity: float = 0.0
    goodwill: float = 0.0
    receivables: float = 0.0
    related_party_transactions: float = 0.0
    advance_receipts: float = 0.0  # 预收款项
    inventory: float = 0.0  # 存货

    def list_missing(self) -> Dict[str, List[str]]:
        """列出各指标缺失的探针"""
        result = {}
        for name in ['roic', 'gross_margin', 'revenue', 'ocf', 'net_profit']:
            outputs = getattr(self, name)
            if outputs is None:
                result[name] = ['all']
            else:
                missing = outputs.missing_probes()
                if missing:
                    result[name] = missing
        return result


# ============================================================================
# 基因输入标准格式 (v2.0 - Signal Fusion Core)
# ============================================================================

@dataclass
class AlphaGeneInput:
    """
    周期性基因 α 的输入

    v2.0 进化：增加 Hurst 指数用于区分"真周期"与"趋势性高波动"
    """
    # 来自 volatility_probe
    detrended_cv: float = 0.0
    cv: float = 0.0
    has_arch_effect: bool = False

    # 来自 cyclical_probe
    cyclical_confidence: float = 0.0
    peak_to_trough_ratio: float = 1.0
    is_cyclical: bool = False

    # v2.0 新增：Hurst指数（区分真周期与趋势）
    # H ≈ 0.5 随机游走, H < 0.5 均值回归(真周期), H > 0.5 趋势性
    hurst_exponent: float = 0.5

    # 来自 log_trend_probe
    r_squared: float = 0.0

    # 数据质量
    is_degraded: bool = False
    degradation_reason: str = ""


@dataclass
class BetaGeneInput:
    """
    资本密度基因 β 的输入

    v2.0 进化：增加 DOL(经营杠杆系数) 相关输入，检测"隐性重资产"
    """
    # 来自 ROIC 的 volatility_probe
    roic_detrended_cv: float = 0.0

    # 来自 OCF 的 volatility_probe
    ocf_cv: float = 0.0

    # 来自 log_trend_probe
    roic_log_slope: float = 0.0
    revenue_log_slope: float = 0.0

    # v2.0 新增：利润/营收波动率（DOL检测）
    profit_cv: float = 0.0  # 利润波动率
    revenue_cv: float = 0.0  # 营收波动率
    profit_log_slope: float = 0.0  # 利润对数斜率

    # 数据质量
    is_degraded: bool = False
    degradation_reason: str = ""


@dataclass
class GammaGeneInput:
    """
    成长动能基因 γ 的输入

    v2.0 进化：增加稳健斜率、R²惩罚、断点检测
    """
    # 来自 log_trend_probe
    revenue_cagr: float = 0.0
    profit_cagr: float = 0.0
    roic_cagr: float = 0.0

    # v2.0 新增：R² 用于惩罚不稳定增长
    r_squared: float = 0.0

    # 来自 rolling_probe
    trend_acceleration: float = 0.0
    recent_3y_slope: float = 0.0

    # v2.0 新增：稳健斜率（Theil-Sen，抗噪）
    robust_slope: float = 0.0
    has_robust: bool = False  # 是否有稳健估计

    # v2.0 新增：断点检测（MultiHorizon探针）
    has_structural_break: bool = False
    break_confidence: float = 0.0
    post_break_slope: float = 0.0  # 断点后斜率

    # 数据质量
    is_degraded: bool = False
    degradation_reason: str = ""


@dataclass
class DeltaFraudInput:
    """
    欺诈熵基因 δ_fraud 的输入

    v2.0 进化：增加"麦道夫特征"检测（Too Smooth = 造假信号）
    """
    # 来自财务数据直接计算
    net_profit: float = 0.0
    operating_cashflow: float = 0.0
    revenue: float = 0.0
    total_assets: float = 0.0
    goodwill: float = 0.0
    equity: float = 0.0
    receivables: float = 0.0
    related_party_transactions: float = 0.0

    # v2.0 新增：麦道夫特征检测
    margin_cv: float = 0.1  # 毛利率CV，<0.01 = 太光滑
    revenue_r_squared: float = 0.5  # 营收R²，>0.99 = 太完美
    ocf_has_arch_effect: bool = False  # 现金流ARCH效应 = 突击回款

    # 数据质量
    is_degraded: bool = False
    degradation_reason: str = ""


@dataclass
class DeltaDecayInput:
    """
    衰退熵基因 δ_decay 的输入

    v2.0 进化：增加拐点预警系统（逃顶能力）
    """
    # 来自 deterioration_probe
    deterioration_probability: float = 0.0
    consecutive_decline_years: int = 0
    deterioration_pattern: str = "none"
    has_deterioration: bool = False

    # 来自 volatility_probe
    volatility_regime: str = "stable"
    volatility_change_ratio: float = 1.0

    # 来自 inflection_probe - v2.0 强化
    has_inflection: bool = False
    inflection_type: str = "none"
    inflection_confidence: float = 0.0  # v2.0 新增：拐点置信度

    # v2.0 新增：近期斜率（用于衰退检测）
    recent_3y_slope: float = 0.0

    # 数据质量
    is_degraded: bool = False
    degradation_reason: str = ""


@dataclass
class VFactorInput:
    """验证因子 V 的输入"""
    # 来自 OCF 的 log_trend_probe
    ocf_cagr: float = 0.0

    # 来自 revenue 的 log_trend_probe
    revenue_cagr: float = 0.0

    # 来自财务数据
    advance_receipts: float = 0.0  # 预收款项
    latest_revenue: float = 0.0

    # 来自 OCF 的 volatility_probe
    ocf_volatility_type: str = "stable"

    # 数据质量
    is_degraded: bool = False
    degradation_reason: str = ""


@dataclass
class GenomeInput:
    """完整的基因计算输入"""
    alpha: AlphaGeneInput = field(default_factory=AlphaGeneInput)
    beta: BetaGeneInput = field(default_factory=BetaGeneInput)
    gamma: GammaGeneInput = field(default_factory=GammaGeneInput)
    delta_fraud: DeltaFraudInput = field(default_factory=DeltaFraudInput)
    delta_decay: DeltaDecayInput = field(default_factory=DeltaDecayInput)
    v_factor: VFactorInput = field(default_factory=VFactorInput)

    # 元信息
    company_code: str = ""
    company_name: str = ""
    data_years: int = 0

    def get_degradation_summary(self) -> Dict[str, str]:
        """获取降级摘要"""
        summary = {}
        for name in ['alpha', 'beta', 'gamma', 'delta_fraud', 'delta_decay', 'v_factor']:
            gene_input = getattr(self, name)
            if gene_input.is_degraded:
                summary[name] = gene_input.degradation_reason
        return summary


# ============================================================================
# 探针适配器
# ============================================================================

class ProbeAdapter:
    """
    探针输出 → 基因输入 适配器

    核心功能：
    1. 从探针输出中提取字段
    2. 处理缺失数据的降级策略
    3. 生成标准化的基因输入

    映射关系（来自 TRUTH_SYSTEM_DESIGN.md 第八章）：

    | 探针 | 输出字段 | 映射到基因 |
    |-----|---------|-----------|
    | log_trend_probe | log_slope, r_squared, cagr | γ, α |
    | volatility_probe | detrended_cv, cv, arch_effect | α, δ_decay |
    | cyclical_probe | cyclical_confidence, peak_to_trough | α |
    | deterioration_probe | deterioration_probability, pattern | δ_decay |
    | rolling_probe | trend_acceleration, recent_3y_slope | γ |
    | robust_probe | robust_slope, mk_tau | 验证用 |
    """

    def __init__(self, allow_degradation: bool = True):
        """
        Args:
            allow_degradation: 是否允许降级运行（部分探针缺失时）
        """
        self.allow_degradation = allow_degradation

    def adapt(self, probe_outputs: MultiIndicatorProbeOutputs) -> GenomeInput:
        """
        将多指标探针输出转换为基因计算输入

        Args:
            probe_outputs: 公司的所有指标探针输出

        Returns:
            GenomeInput: 标准化的基因输入
        """
        result = GenomeInput(
            company_code=probe_outputs.company_code,
            company_name=probe_outputs.company_name,
        )

        # 提取各基因输入
        result.alpha = self._extract_alpha_input(probe_outputs)
        result.beta = self._extract_beta_input(probe_outputs)
        result.gamma = self._extract_gamma_input(probe_outputs)
        result.delta_fraud = self._extract_delta_fraud_input(probe_outputs)
        result.delta_decay = self._extract_delta_decay_input(probe_outputs)
        result.v_factor = self._extract_v_factor_input(probe_outputs)

        # 推断数据年数
        if probe_outputs.roic and probe_outputs.roic.raw_values is not None:
            result.data_years = len(probe_outputs.roic.raw_values)

        return result

    def _extract_alpha_input(self, outputs: MultiIndicatorProbeOutputs) -> AlphaGeneInput:
        """
        提取周期性基因 α 的输入

        来源：
        - volatility_probe → detrended_cv, cv, has_arch_effect
        - cyclical_probe → cyclical_confidence, peak_to_trough_ratio
        - log_trend_probe → r_squared
        """
        alpha = AlphaGeneInput()
        roic = outputs.roic

        if roic is None:
            alpha.is_degraded = True
            alpha.degradation_reason = "ROIC探针输出缺失"
            return alpha

        # 从 volatility_probe 提取
        if roic.volatility:
            alpha.detrended_cv = roic.volatility.detrended_cv
            alpha.cv = roic.volatility.cv
            alpha.has_arch_effect = roic.volatility.has_arch_effect
        else:
            alpha.is_degraded = True
            alpha.degradation_reason = "缺失volatility_probe"

        # 从 cyclical_probe 提取
        if roic.cyclical:
            alpha.cyclical_confidence = roic.cyclical.cyclical_confidence
            alpha.peak_to_trough_ratio = roic.cyclical.peak_to_trough_ratio
            alpha.is_cyclical = roic.cyclical.is_cyclical
        else:
            if not alpha.is_degraded:
                alpha.is_degraded = True
                alpha.degradation_reason = "缺失cyclical_probe"
            else:
                alpha.degradation_reason += "; 缺失cyclical_probe"

        # 从 log_trend_probe 提取
        if roic.log_trend:
            alpha.r_squared = roic.log_trend.r_squared
        else:
            if not alpha.is_degraded:
                alpha.is_degraded = True
                alpha.degradation_reason = "缺失log_trend_probe"
            else:
                alpha.degradation_reason += "; 缺失log_trend_probe"

        return alpha

    def _extract_beta_input(self, outputs: MultiIndicatorProbeOutputs) -> BetaGeneInput:
        """
        提取资本密度基因 β 的输入

        来源（设计文档确认的混合法）：
        - roic.volatility_probe → detrended_cv
        - ocf.volatility_probe → cv
        - roic.log_trend_probe → log_slope
        - revenue.log_trend_probe → log_slope
        """
        beta = BetaGeneInput()

        # 从 ROIC 探针提取
        if outputs.roic:
            if outputs.roic.volatility:
                beta.roic_detrended_cv = outputs.roic.volatility.detrended_cv
            if outputs.roic.log_trend:
                beta.roic_log_slope = outputs.roic.log_trend.log_slope
        else:
            beta.is_degraded = True
            beta.degradation_reason = "缺失ROIC探针"

        # 从 OCF 探针提取
        if outputs.ocf and outputs.ocf.volatility:
            beta.ocf_cv = outputs.ocf.volatility.cv
        else:
            # OCF 是辅助，允许降级
            beta.ocf_cv = 0.3  # 默认中等波动
            logger.warning("OCF探针缺失，使用默认cv=0.3")

        # 从 Revenue 探针提取
        if outputs.revenue and outputs.revenue.log_trend:
            beta.revenue_log_slope = outputs.revenue.log_trend.log_slope
        else:
            if not beta.is_degraded:
                beta.is_degraded = True
                beta.degradation_reason = "缺失Revenue探针"
            else:
                beta.degradation_reason += "; 缺失Revenue探针"

        return beta

    def _extract_gamma_input(self, outputs: MultiIndicatorProbeOutputs) -> GammaGeneInput:
        """
        提取成长动能基因 γ 的输入

        来源：
        - revenue.log_trend_probe → cagr_approx
        - net_profit.log_trend_probe → cagr_approx
        - roic.rolling_probe → trend_acceleration, recent_3y_slope
        """
        gamma = GammaGeneInput()

        # 从 Revenue 探针提取 CAGR
        if outputs.revenue and outputs.revenue.log_trend:
            gamma.revenue_cagr = outputs.revenue.log_trend.cagr_approx
        else:
            gamma.is_degraded = True
            gamma.degradation_reason = "缺失Revenue的log_trend_probe"

        # 从 Net Profit 探针提取 CAGR
        if outputs.net_profit and outputs.net_profit.log_trend:
            gamma.profit_cagr = outputs.net_profit.log_trend.cagr_approx
        else:
            # 可以用 ROIC CAGR 降级
            if outputs.roic and outputs.roic.log_trend:
                gamma.profit_cagr = outputs.roic.log_trend.cagr_approx
                logger.warning("使用ROIC CAGR替代利润CAGR")
            elif not gamma.is_degraded:
                gamma.is_degraded = True
                gamma.degradation_reason = "缺失利润CAGR"

        # 从 ROIC 探针提取 CAGR
        if outputs.roic and outputs.roic.log_trend:
            gamma.roic_cagr = outputs.roic.log_trend.cagr_approx

        # 从 rolling_probe 提取加速度
        if outputs.roic and outputs.roic.rolling:
            gamma.trend_acceleration = outputs.roic.rolling.trend_acceleration
            gamma.recent_3y_slope = outputs.roic.rolling.recent_3y_slope
        else:
            # rolling_probe 缺失时，使用 log_slope 近似
            if outputs.roic and outputs.roic.log_trend:
                gamma.recent_3y_slope = outputs.roic.log_trend.log_slope
                gamma.trend_acceleration = 0.0
            else:
                if not gamma.is_degraded:
                    gamma.is_degraded = True
                    gamma.degradation_reason = "缺失rolling_probe和log_trend_probe"

        return gamma

    def _extract_delta_fraud_input(self, outputs: MultiIndicatorProbeOutputs) -> DeltaFraudInput:
        """
        提取欺诈熵基因 δ_fraud 的输入

        来源：
        - 主要来自财务数据直接计算
        - 未来可以新增 fraud_detection_probe
        """
        delta = DeltaFraudInput()

        # 从辅助财务数据提取
        delta.total_assets = outputs.total_assets
        delta.equity = outputs.equity
        delta.goodwill = outputs.goodwill
        delta.receivables = outputs.receivables
        delta.related_party_transactions = outputs.related_party_transactions

        # 从 OCF 和 net_profit 探针提取最新值
        if outputs.ocf and outputs.ocf.raw_values is not None and len(outputs.ocf.raw_values) > 0:
            delta.operating_cashflow = float(outputs.ocf.raw_values[-1])
        else:
            delta.is_degraded = True
            delta.degradation_reason = "缺失OCF数据"

        if outputs.net_profit and outputs.net_profit.raw_values is not None and len(outputs.net_profit.raw_values) > 0:
            delta.net_profit = float(outputs.net_profit.raw_values[-1])
        else:
            if not delta.is_degraded:
                delta.is_degraded = True
                delta.degradation_reason = "缺失净利润数据"

        if outputs.revenue and outputs.revenue.raw_values is not None and len(outputs.revenue.raw_values) > 0:
            delta.revenue = float(outputs.revenue.raw_values[-1])
        else:
            if not delta.is_degraded:
                delta.is_degraded = True
                delta.degradation_reason = "缺失营收数据"

        return delta

    def _extract_delta_decay_input(self, outputs: MultiIndicatorProbeOutputs) -> DeltaDecayInput:
        """
        提取衰退熵基因 δ_decay 的输入

        来源：
        - roic.deterioration_probe → deterioration_probability, consecutive_decline_years
        - roic.volatility_probe → volatility_regime, volatility_change_ratio
        - roic.inflection_probe → has_inflection, inflection_type (可选)
        """
        delta = DeltaDecayInput()
        roic = outputs.roic

        if roic is None:
            delta.is_degraded = True
            delta.degradation_reason = "ROIC探针输出缺失"
            return delta

        # 从 deterioration_probe 提取
        if roic.deterioration:
            delta.deterioration_probability = roic.deterioration.deterioration_probability
            delta.consecutive_decline_years = roic.deterioration.consecutive_decline_years
            delta.deterioration_pattern = roic.deterioration.deterioration_pattern
            delta.has_deterioration = roic.deterioration.has_deterioration
        else:
            delta.is_degraded = True
            delta.degradation_reason = "缺失deterioration_probe"

        # 从 volatility_probe 提取（波动率体制变化）
        if roic.volatility:
            delta.volatility_regime = roic.volatility.volatility_regime
            delta.volatility_change_ratio = roic.volatility.volatility_change_ratio

        # 从 inflection_probe 提取（可选）
        if roic.inflection:
            delta.has_inflection = roic.inflection.has_inflection
            delta.inflection_type = roic.inflection.inflection_type

        return delta

    def _extract_v_factor_input(self, outputs: MultiIndicatorProbeOutputs) -> VFactorInput:
        """
        提取验证因子 V 的输入

        来源：
        - ocf.log_trend_probe → cagr_approx
        - revenue.log_trend_probe → cagr_approx
        - ocf.volatility_probe → volatility_type
        - 财务数据 → advance_receipts, latest_revenue
        """
        v = VFactorInput()

        # 从 OCF 探针提取
        if outputs.ocf:
            if outputs.ocf.log_trend:
                v.ocf_cagr = outputs.ocf.log_trend.cagr_approx
            if outputs.ocf.volatility:
                v.ocf_volatility_type = outputs.ocf.volatility.volatility_type
        else:
            v.is_degraded = True
            v.degradation_reason = "缺失OCF探针"

        # 从 Revenue 探针提取
        if outputs.revenue and outputs.revenue.log_trend:
            v.revenue_cagr = outputs.revenue.log_trend.cagr_approx
        else:
            if not v.is_degraded:
                v.is_degraded = True
                v.degradation_reason = "缺失Revenue探针"

        # 从财务数据提取
        v.advance_receipts = outputs.advance_receipts
        if outputs.revenue and outputs.revenue.raw_values is not None and len(outputs.revenue.raw_values) > 0:
            v.latest_revenue = float(outputs.revenue.raw_values[-1])

        return v


# ============================================================================
# 便捷函数
# ============================================================================

def create_probe_outputs_from_dict(
    company_code: str,
    indicator_results: Dict[str, Dict[str, Any]],
    financial_data: Dict[str, float] = None,
) -> MultiIndicatorProbeOutputs:
    """
    从字典格式创建探针输出

    用于与现有流水线整合。

    Args:
        company_code: 公司代码
        indicator_results: 各指标的探针结果字典
            格式: {
                'roic': {
                    'log_trend': LogTrendResult,
                    'volatility': VolatilityResult,
                    'cyclical': CyclicalPatternResult,
                    'deterioration': RecentDeteriorationResult,
                    'rolling': RollingTrendResult,
                    'raw_values': np.ndarray,
                },
                'gross_margin': {...},
                ...
            }
        financial_data: 财务数据
            格式: {
                'total_assets': float,
                'equity': float,
                'goodwill': float,
                ...
            }

    Returns:
        MultiIndicatorProbeOutputs
    """
    outputs = MultiIndicatorProbeOutputs(company_code=company_code)

    for indicator_name in ['roic', 'gross_margin', 'revenue', 'ocf', 'net_profit']:
        if indicator_name in indicator_results:
            data = indicator_results[indicator_name]
            probe_output = ProbeOutputs(
                indicator_name=indicator_name,
                log_trend=data.get('log_trend'),
                volatility=data.get('volatility'),
                cyclical=data.get('cyclical'),
                deterioration=data.get('deterioration'),
                rolling=data.get('rolling'),
                robust=data.get('robust'),
                inflection=data.get('inflection'),
                raw_values=data.get('raw_values'),
            )
            setattr(outputs, indicator_name, probe_output)

    # 财务数据
    if financial_data:
        outputs.total_assets = financial_data.get('total_assets', 0.0)
        outputs.equity = financial_data.get('equity', 0.0)
        outputs.goodwill = financial_data.get('goodwill', 0.0)
        outputs.receivables = financial_data.get('receivables', 0.0)
        outputs.related_party_transactions = financial_data.get('related_party_transactions', 0.0)
        outputs.advance_receipts = financial_data.get('advance_receipts', 0.0)
        outputs.inventory = financial_data.get('inventory', 0.0)

    return outputs
