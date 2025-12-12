"""
Beta Gene - 资本密度基因 (β) v2.0
=================================

功能：区分轻资产与重资产公司，用于上帝方程中的"重资产惩罚"。

v2.0 进化核心：DOL经营杠杆检测"隐性重资产"
- 原始痛点：财报没有直接的固定资产/总资产比率
- 解决方案：DOL = ∂ln(Profit)/∂ln(Revenue) → 隐性重资产检测

物理含义：
- β 高 → 重资产公司（钢铁、航空、电力）或隐性重资产（看似轻资产但利润放大）
- β 低 → 轻资产公司（软件、白酒、品牌消费）

熔炼方程 v2.0：
β = 0.6 × β_static + 0.4 × Norm(DOL × CV_amplification)

其中：
- β_static = leverage_ratio（原始的roic_cv/ocf_cv方法）
- DOL = profit_log_slope / revenue_log_slope（经营杠杆系数）
- CV_amplification = profit_cv / revenue_cv（利润波动放大率）
- implied_capital_intensity = DOL × CV_amplification（隐含资本密度）

关键洞察：
- DOL > 1.5：利润增速是营收增速的1.5倍 → 高经营杠杆
- CV_amplification > 1：利润波动 > 营收波动 → 重资产特征
- 两者乘积 > 2：隐性重资产警报！

作者: AStock Analysis System
日期: 2025-01
"""

from typing import Dict, Tuple
from dataclasses import dataclass
import logging

from ...adapter import BetaGeneInput
from ...config import TruthConfig, get_default_truth_config

logger = logging.getLogger(__name__)


def clip_01(value: float) -> float:
    """裁剪到 [0, 1] 区间"""
    return max(0.0, min(1.0, value))


@dataclass
class BetaGene:
    """
    资本密度基因计算结果

    Attributes:
        value: 基因值 [0, 1]，0=轻资产，1=重资产
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
            return "重资产：固定成本高，利润对营收变化敏感，周期下行风险大"
        elif self.value >= 0.4:
            return "中等资产密度：有一定经营杠杆，但不极端"
        else:
            return "轻资产：可变成本为主，抗周期能力强，护城河可能来自品牌/技术"


def compute_beta_from_probes(
    beta_input: BetaGeneInput,
    config: TruthConfig = None,
) -> Tuple[float, Dict[str, float]]:
    """
    从探针输出计算资本密度基因 β v2.0

    核心进化：DOL经营杠杆检测"隐性重资产"
    ======================================

    数据来源映射：
    - roic_detrended_cv ← roic.volatility_probe.detrended_cv
    - ocf_cv ← ocf.volatility_probe.cv
    - roic_log_slope ← roic.log_trend_probe.log_slope
    - revenue_log_slope ← revenue.log_trend_probe.log_slope

    v2.0 新增：
    - profit_cv ← profit.volatility_probe.cv
    - revenue_cv ← revenue.volatility_probe.cv
    - profit_log_slope ← profit.log_trend_probe.log_slope

    熔炼方程 v2.0：
    β = 0.6 × β_static + 0.4 × Norm(implied_capital_intensity)

    其中：
    - β_static = min(roic_cv / ocf_cv, 3.0) / 3.0
    - DOL = profit_log_slope / revenue_log_slope
    - CV_amplification = profit_cv / revenue_cv
    - implied_capital_intensity = DOL × CV_amplification

    归一化：implied_capital_intensity=3 → score=1.0

    Args:
        beta_input: 探针适配器提供的输入
        config: T.R.U.T.H. 配置

    Returns:
        (beta_value, breakdown_dict)
    """
    if config is None:
        config = get_default_truth_config()

    params = config.genes
    breakdown = {}

    if beta_input.is_degraded:
        return 0.5, {'degraded': True, 'reason': beta_input.degradation_reason}

    # ============================================
    # 1. β_static：原始杠杆比率（权重0.60）
    # ============================================
    # 重资产公司：ROIC波动 > OCF波动（固定成本放大利润波动）
    roic_cv = beta_input.roic_detrended_cv
    ocf_cv = max(beta_input.ocf_cv, 0.01)  # 避免除零
    leverage_ratio = min(roic_cv / ocf_cv, 3.0) / 3.0

    beta_static = leverage_ratio

    breakdown['roic_cv'] = roic_cv
    breakdown['ocf_cv'] = ocf_cv
    breakdown['leverage_ratio'] = leverage_ratio
    breakdown['beta_static'] = beta_static
    breakdown['source_static'] = 'roic_cv/ocf_cv ratio'

    # ============================================
    # 2. v2.0 DOL经营杠杆系数
    # ============================================
    # DOL = ∂ln(Profit)/∂ln(Revenue) = profit_slope / revenue_slope
    profit_slope = beta_input.profit_log_slope if beta_input.profit_log_slope is not None else beta_input.roic_log_slope
    rev_slope = beta_input.revenue_log_slope

    if abs(rev_slope) > 0.01:
        dol = profit_slope / rev_slope
    else:
        dol = 1.0  # 中性值

    # DOL > 1: 利润增速 > 营收增速 → 高经营杠杆
    # DOL = 1.5: 营收增10%，利润增15%
    breakdown['dol'] = dol
    breakdown['profit_log_slope'] = profit_slope
    breakdown['revenue_log_slope'] = rev_slope
    breakdown['dol_interpretation'] = f"营收增10%→利润增{dol*10:.1f}%"

    # ============================================
    # 3. v2.0 波动放大率
    # ============================================
    # CV_amplification = profit_cv / revenue_cv
    profit_cv = beta_input.profit_cv if beta_input.profit_cv is not None else roic_cv
    revenue_cv = beta_input.revenue_cv if beta_input.revenue_cv is not None else max(ocf_cv, 0.01)

    cv_amplification = profit_cv / max(revenue_cv, 0.01)

    breakdown['profit_cv'] = profit_cv
    breakdown['revenue_cv'] = revenue_cv
    breakdown['cv_amplification'] = cv_amplification

    # ============================================
    # 4. v2.0 隐含资本密度
    # ============================================
    # implied_capital_intensity = DOL × CV_amplification
    implied_capital_intensity = max(0, dol * cv_amplification)

    # 归一化：intensity=3 → score=1.0
    implied_score = clip_01(implied_capital_intensity / 3.0)

    breakdown['implied_capital_intensity'] = implied_capital_intensity
    breakdown['implied_score'] = implied_score

    # 隐性重资产警报判断
    is_hidden_heavy_asset = implied_capital_intensity > 2.0
    breakdown['is_hidden_heavy_asset'] = is_hidden_heavy_asset
    if is_hidden_heavy_asset:
        breakdown['hidden_asset_warning'] = f"⚠️ 隐性重资产！DOL={dol:.2f} × CV放大={cv_amplification:.2f} = {implied_capital_intensity:.2f}"

    # ============================================
    # 5. 加权融合 v2.0
    # ============================================
    # β = 0.6 × β_static + 0.4 × implied_score
    beta = 0.6 * beta_static + 0.4 * implied_score

    breakdown['weight_static'] = 0.6
    breakdown['weight_implied'] = 0.4
    breakdown['final_beta'] = beta
    breakdown['version'] = 'v2.0_dol_aware'

    return clip_01(beta), breakdown


def create_beta_gene(
    beta_input: BetaGeneInput,
    config: TruthConfig = None,
) -> BetaGene:
    """
    创建 BetaGene 对象（便捷工厂函数）
    """
    value, breakdown = compute_beta_from_probes(beta_input, config)
    return BetaGene(
        value=value,
        breakdown=breakdown,
        is_degraded=beta_input.is_degraded,
        degradation_reason=beta_input.degradation_reason if beta_input.is_degraded else "",
    )
