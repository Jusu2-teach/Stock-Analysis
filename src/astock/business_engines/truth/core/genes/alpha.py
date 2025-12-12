"""
Alpha Gene - 周期性基因 (α) v2.0
================================

功能：识别周期性公司，用于上帝方程中的"周期豁免"。

v2.0 进化核心：区分"真周期"与"伪波动"
- 高成长股（如英伟达）波动也大，但不是周期股
- 使用 Hurst 指数 + 置信度门控

物理含义：
- α 高 → 强周期股（钢铁、航运、化工）
- α 低 → 弱周期股（消费、医药、公用事业）

熔炼方程 v2.0：
α = Norm(CV_detrend) × P_cyc × (1 - |H - 0.5|) + 0.2 × Norm(R_pt)

解析：
- 只有当 P_cyc(周期置信度) 高时，波动率才转化为 α
- Hurst 指数接近 0.5（随机游走）或 <0.5（均值回归）时增加权重
- 若 H → 1（强趋势），说明是成长股，降低 α 权重

作者: AStock Analysis System
日期: 2025-01
"""

from typing import Dict, Tuple
from dataclasses import dataclass
import logging

from ...adapter import AlphaGeneInput
from ...config import TruthConfig, get_default_truth_config

logger = logging.getLogger(__name__)


def clip_01(value: float) -> float:
    """裁剪到 [0, 1] 区间"""
    return max(0.0, min(1.0, value))


@dataclass
class AlphaGene:
    """
    周期性基因计算结果

    Attributes:
        value: 基因值 [0, 1]，0=非周期，1=强周期
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
            return "强周期性：适合在周期底部买入，顶部卖出"
        elif self.value >= 0.4:
            return "中等周期性：受宏观经济影响，但有一定抗周期能力"
        else:
            return "弱周期性：业绩稳定，适合长期持有"


def compute_alpha_from_probes(
    alpha_input: AlphaGeneInput,
    config: TruthConfig = None,
) -> Tuple[float, Dict[str, float]]:
    """
    从探针输出计算周期性基因 α (v2.0 Signal Fusion Core)

    v2.0 熔炼方程：
    α = Norm(CV_detrend) × P_cyc × hurst_factor + 0.2 × Norm(R_pt) + 0.1 × arch_score

    其中 hurst_factor = 1 - |hurst - 0.5| × 2
    - Hurst = 0.5 (随机游走) → factor = 1.0 (纯周期)
    - Hurst = 0.3 (均值回归) → factor = 0.6 (周期性)
    - Hurst = 0.8 (趋势性)   → factor = 0.4 (成长股，降权)

    核心进化：置信度门控 + Hurst判别，区分真周期与伪波动

    Args:
        alpha_input: 探针适配器提供的输入
        config: T.R.U.T.H. 配置

    Returns:
        (alpha_value, breakdown_dict)
    """
    if config is None:
        config = get_default_truth_config()

    params = config.genes
    breakdown = {}

    if alpha_input.is_degraded:
        return 0.5, {'degraded': True, 'reason': alpha_input.degradation_reason}

    # === 1. 去趋势CV（基础波动率）===
    detrended_cv = alpha_input.detrended_cv
    cv_normalized = clip_01(detrended_cv / params.cv_saturation)
    breakdown['detrended_cv'] = detrended_cv
    breakdown['cv_normalized'] = cv_normalized

    # === 2. 周期置信度门控 (P_cyc) ===
    # 核心进化：只有周期置信度高时，波动才转化为α
    cyclical_confidence = alpha_input.cyclical_confidence
    breakdown['cyclical_confidence'] = cyclical_confidence

    # === 3. Hurst指数判别 (v2.0 新增) ===
    # H ≈ 0.5 随机游走 → 可能是周期
    # H < 0.5 均值回归 → 强周期特征
    # H > 0.5 趋势性   → 成长股，不是周期股
    hurst = alpha_input.hurst_exponent
    # hurst_factor: H=0.5时为1.0, 偏离0.5时降低
    # 但对于 H<0.5 (均值回归) 保持较高权重
    if hurst <= 0.5:
        # 均值回归或随机：保持高权重
        hurst_factor = 1.0 - abs(hurst - 0.5) * 1.0  # 最低0.5
    else:
        # 趋势性：大幅降权
        hurst_factor = 1.0 - (hurst - 0.5) * 1.6  # H=0.8时factor=0.52
    hurst_factor = max(0.2, min(1.0, hurst_factor))
    breakdown['hurst_exponent'] = hurst
    breakdown['hurst_factor'] = hurst_factor

    # === 4. 峰谷比得分 ===
    pt_ratio = alpha_input.peak_to_trough_ratio
    pt_score = clip_01((pt_ratio - 1) / (params.peak_trough_saturation - 1))
    breakdown['peak_trough_ratio'] = pt_ratio
    breakdown['pt_score'] = pt_score

    # === 5. ARCH效应（波动聚集）===
    arch_score = 1.0 if alpha_input.has_arch_effect else 0.0
    breakdown['has_arch_effect'] = alpha_input.has_arch_effect
    breakdown['arch_score'] = arch_score

    # === v2.0 熔炼方程 ===
    # 核心公式：波动率 × 周期置信度 × Hurst因子
    main_component = cv_normalized * cyclical_confidence * hurst_factor
    pt_component = 0.2 * pt_score
    arch_component = 0.1 * arch_score

    alpha = main_component + pt_component + arch_component

    breakdown['main_component'] = main_component
    breakdown['pt_component'] = pt_component
    breakdown['arch_component'] = arch_component
    breakdown['final_alpha'] = alpha
    breakdown['version'] = '2.0_signal_fusion'

    return clip_01(alpha), breakdown


def create_alpha_gene(
    alpha_input: AlphaGeneInput,
    config: TruthConfig = None,
) -> AlphaGene:
    """
    创建 AlphaGene 对象（便捷工厂函数）
    """
    value, breakdown = compute_alpha_from_probes(alpha_input, config)
    return AlphaGene(
        value=value,
        breakdown=breakdown,
        is_degraded=alpha_input.is_degraded,
        degradation_reason=alpha_input.degradation_reason if alpha_input.is_degraded else "",
    )
