"""
Verification Gene - 真相验证基因 (V) v2.0
=========================================

功能：验证公司成长的"真实性"，用于上帝方程中的"真成长奖励"乘数。

v2.0 进化核心：体制惩罚 + 预收奖励加强
- 原始逻辑保持：现金流验证
- 新增体制惩罚：极端体制(波动)下V打折
- 预收奖励提升：更强的"强势地位"奖励

熔炼方程 v2.0：
V = V_raw × V_quality × (1 + Bonus_prepay)

其中：
- V_raw = min(1, OCF_cagr / Revenue_cagr)  # 基础现金流匹配
- V_quality = f(OCF稳定性) ∈ [0.6, 1.3]   # 体制质量乘数
- Bonus_prepay = min(预收/营收 × 系数, 0.25)  # 预收奖励

物理含义：
- V 高 → 成长有现金流支撑（真成长）
- V 低 → 成长是虚假的（假成长：利润高但现金流差）

上帝方程作用：
- T = R_f + k₁β - k₂α - k₃(γ×E×V) + k₄δ
- V 作为乘数，只有 V 高时，γ 的成长奖励才能真正降低阈值

作者: AStock Analysis System
日期: 2025-01
"""

from typing import Dict, Tuple
from dataclasses import dataclass
import numpy as np
import logging

from ...adapter import VFactorInput
from ...config import TruthConfig, get_default_truth_config

logger = logging.getLogger(__name__)


def clip_01(value: float) -> float:
    """裁剪到 [0, 1] 区间"""
    return max(0.0, min(1.0, value))


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """安全除法"""
    if denominator == 0 or np.isnan(denominator):
        return default
    return numerator / denominator


@dataclass
class VerificationGene:
    """
    真相验证基因计算结果

    Attributes:
        value: 基因值 [0, 1]，0=假成长，1=真成长
        breakdown: 各因子得分分解
        is_degraded: 是否降级计算
        degradation_reason: 降级原因
    """
    value: float
    breakdown: Dict[str, float]
    is_degraded: bool = False
    degradation_reason: str = ""

    @property
    def interpretation(self) -> str:
        """基因解读"""
        if self.value >= 0.7:
            return "真成长：现金流强劲支撑，成长可信度高"
        elif self.value >= 0.4:
            return "部分验证：现金流与利润基本匹配，但有一定偏离"
        else:
            return "⚠️ 假成长：利润与现金流严重背离，成长可能是虚假的"


def compute_verification_from_probes(
    v_input: VFactorInput,
    config: TruthConfig = None,
) -> Tuple[float, Dict[str, float]]:
    """
    从探针输出计算验证因子 V v2.0

    数据来源映射：
    - ocf_cagr ← ocf.log_trend_probe.cagr_approx
    - revenue_cagr ← revenue.log_trend_probe.cagr_approx
    - ocf_volatility_type ← ocf.volatility_probe.volatility_type
    - advance_receipts, latest_revenue ← 财务数据

    熔炼方程 v2.0：
    V = V_raw × V_quality × (1 + Bonus_prepay)

    - V_raw = min(1.0, ocf_cagr / revenue_cagr) if both > 0
    - V_quality = 体制质量乘数 ∈ [0.6, 1.3]
    - Bonus_prepay = min(advance_ratio × 系数, 0.25)

    Args:
        v_input: 探针适配器提供的输入
        config: T.R.U.T.H. 配置

    Returns:
        (verification_value, breakdown_dict)
    """
    if config is None:
        config = get_default_truth_config()

    params = config.genes
    breakdown = {}

    if v_input.is_degraded:
        return 0.5, {'degraded': True, 'reason': v_input.degradation_reason}

    # ============================================
    # 1. V_raw：基础现金流匹配度
    # ============================================
    ocf_cagr = v_input.ocf_cagr
    revenue_cagr = v_input.revenue_cagr

    if revenue_cagr > 0.01 and ocf_cagr >= 0:
        # 现金流CAGR / 营收CAGR，>1 说明现金流跟得上收入
        v_raw = min(1.0, ocf_cagr / revenue_cagr)
    elif revenue_cagr > 0.01 and ocf_cagr < 0:
        # 收入增长但现金流下降，警示信号
        v_raw = 0.0
    elif revenue_cagr <= 0.01:
        # 收入不增长，看现金流绝对水平
        v_raw = 0.5 if ocf_cagr >= 0 else 0.2
    else:
        v_raw = 0.5

    breakdown['ocf_cagr'] = ocf_cagr
    breakdown['revenue_cagr'] = revenue_cagr
    breakdown['v_raw'] = v_raw
    breakdown['source_cagr'] = 'log_trend_probe.cagr_approx'

    # ============================================
    # 2. V_quality：体制质量乘数（核心v2.0改进）
    # ============================================
    # 现金流稳定性决定可信度
    # 极端波动 → 降权（体制惩罚）
    vol_type = v_input.ocf_volatility_type
    quality_multipliers = {
        'ultra_stable': 1.3,      # 极稳现金流：奖励
        'stable': 1.2,            # 稳定：奖励
        'moderate': 1.0,          # 中性
        'volatile': 0.8,          # 波动：体制惩罚
        'highly_volatile': 0.6,   # 剧烈波动：重度惩罚
    }
    v_quality = quality_multipliers.get(vol_type.lower(), 1.0)
    breakdown['ocf_volatility_type'] = vol_type
    breakdown['v_quality'] = v_quality
    breakdown['source_vol'] = 'volatility_probe.volatility_type'

    # v2.0 体制惩罚解释
    if v_quality < 1.0:
        breakdown['regime_penalty'] = f"⚠️ 体制惩罚: 现金流{vol_type}, 乘数={v_quality}"
    elif v_quality > 1.0:
        breakdown['regime_bonus'] = f"✓ 体制奖励: 现金流{vol_type}, 乘数={v_quality}"

    # ============================================
    # 3. Bonus_prepay：预收款奖励
    # ============================================
    # 高预收款 = 强势地位（茅台、格力等）
    advance = v_input.advance_receipts
    revenue = v_input.latest_revenue
    advance_ratio = safe_divide(advance, revenue, 0)

    # v2.0: 提高预收奖励系数
    prepay_coef = params.advance_receipt_coefficient
    prepay_cap = params.advance_receipt_bonus_cap
    bonus_prepay = min(advance_ratio * prepay_coef, prepay_cap)

    breakdown['advance_receipts'] = advance
    breakdown['latest_revenue'] = revenue
    breakdown['advance_ratio'] = advance_ratio
    breakdown['bonus_prepay'] = bonus_prepay

    if advance_ratio > 0.1:
        breakdown['prepay_interpretation'] = f"✓ 强势地位：预收/营收={advance_ratio:.1%}"

    # ============================================
    # 4. 综合V因子 v2.0
    # ============================================
    # V = V_raw × V_quality × (1 + Bonus_prepay)
    # 注意：(1 + Bonus_prepay) 而不是 + Bonus_prepay
    verification = v_raw * v_quality * (1 + bonus_prepay)

    breakdown['v_raw'] = v_raw
    breakdown['v_quality'] = v_quality
    breakdown['bonus_factor'] = 1 + bonus_prepay
    breakdown['final_v'] = verification
    breakdown['version'] = 'v2.0_regime_aware'

    return clip_01(verification), breakdown


def create_verification_gene(
    v_input: VFactorInput,
    config: TruthConfig = None,
) -> VerificationGene:
    """
    创建 VerificationGene 对象（便捷工厂函数）
    """
    value, breakdown = compute_verification_from_probes(v_input, config)
    return VerificationGene(
        value=value,
        breakdown=breakdown,
        is_degraded=v_input.is_degraded,
        degradation_reason=v_input.degradation_reason if v_input.is_degraded else "",
    )
