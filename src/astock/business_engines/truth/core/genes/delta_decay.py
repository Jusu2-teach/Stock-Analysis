"""
Delta Decay Gene - 衰退熵基因 (δ_decay) v2.0
============================================

功能：检测公司基本面恶化趋势，用于上帝方程中的"衰退惩罚"。

v2.0 进化核心：从"线性外推"进化为"拐点预警"
- 原始痛点：跌了3年才发现衰退
- 解决方案：Inflection探针实现逃顶能力

熔炼方程 v2.0：
δ_decay = 0.35×P_det + 0.20×years + 0.20×Norm(-slope_recent) + 0.15×Trigger + 0.10×pattern

Trigger = I(inflection_type == "NegativeReversal") × confidence

解析：
- 一旦 Inflection 探针检测到 "Negative Reversal" (顶部反转) 且置信度高
- δ_decay 会瞬间飙升，让系统具备逃顶能力

硬触发规则：
- 拐点类型为 NegativeReversal 且置信度>0.8 → δ_decay强制>=0.7

作者: AStock Analysis System
日期: 2025-01
"""

from typing import Dict, Tuple
from dataclasses import dataclass
import logging

from ...adapter import DeltaDecayInput
from ...config import TruthConfig, get_default_truth_config

logger = logging.getLogger(__name__)


def clip_01(value: float) -> float:
    """裁剪到 [0, 1] 区间"""
    return max(0.0, min(1.0, value))


@dataclass
class DeltaDecayGene:
    """
    衰退熵基因计算结果

    Attributes:
        value: 基因值 [0, 1]，0=健康，1=严重衰退
        breakdown: 各因子得分分解
        is_degraded: 是否降级计算
        degradation_reason: 降级原因
    """
    value: float
    breakdown: Dict[str, float]
    is_degraded: bool = False
    degradation_reason: str = ""

    @property
    def is_severe_decay(self) -> bool:
        """是否严重衰退（阈值0.7）"""
        return self.value >= 0.7

    @property
    def interpretation(self) -> str:
        """基因解读"""
        if self.value >= 0.7:
            return "⚠️ 严重衰退：基本面持续恶化，建议回避"
        elif self.value >= 0.4:
            return "中度恶化：存在下行风险，需密切关注拐点"
        else:
            return "基本面健康：无明显恶化迹象"


def compute_delta_decay_from_probes(
    delta_input: DeltaDecayInput,
    config: TruthConfig = None,
) -> Tuple[float, Dict[str, float]]:
    """
    从探针输出计算衰退熵基因 δ_decay v2.0

    核心进化：从"线性外推"到"拐点预警"
    =====================================

    v1.0 痛点：跌了3年才发现衰退
    v2.0 解法：Inflection探针实现逃顶能力

    数据来源映射：
    - deterioration_probability ← deterioration_probe.deterioration_probability
    - consecutive_decline_years ← deterioration_probe.consecutive_decline_years
    - deterioration_pattern ← deterioration_probe.deterioration_pattern
    - volatility_regime ← volatility_probe.volatility_regime
    - volatility_change_ratio ← volatility_probe.volatility_change_ratio

    v2.0 新增：
    - inflection_type ← inflection_probe.inflection_type (NegativeReversal/PositiveReversal/None)
    - inflection_confidence ← inflection_probe.confidence
    - recent_3y_slope ← trend_probe.recent_3y_slope (近3年斜率，负值=下行)

    熔炼方程 v2.0：
    δ_decay = 0.35×P_det + 0.20×years + 0.20×Norm(-slope_recent) + 0.15×Trigger + 0.10×pattern

    Trigger = I(inflection_type == "NegativeReversal") × confidence

    硬触发规则：
    - 拐点类型为 NegativeReversal 且置信度>0.8 → δ_decay强制>=0.7

    Args:
        delta_input: 探针适配器提供的输入
        config: T.R.U.T.H. 配置

    Returns:
        (delta_decay_value, breakdown_dict)
    """
    if config is None:
        config = get_default_truth_config()

    params = config.genes
    breakdown = {}

    if delta_input.is_degraded:
        return 0.3, {'degraded': True, 'reason': delta_input.degradation_reason}

    # ============================================
    # 1. 贝叶斯恶化概率得分（权重0.35）
    # ============================================
    prob = delta_input.deterioration_probability
    prob_score = clip_01(prob)
    breakdown['deterioration_probability'] = prob
    breakdown['prob_score'] = prob_score
    breakdown['source_prob'] = 'deterioration_probe.deterioration_probability'

    # ============================================
    # 2. 连续下跌年数得分（权重0.20）
    # ============================================
    years = delta_input.consecutive_decline_years
    # 0年→0, 1年→0.25, 2年→0.5, 3年→0.75, 4+年→1.0
    years_score = clip_01(years / 4.0)
    breakdown['consecutive_decline_years'] = years
    breakdown['years_score'] = years_score
    breakdown['source_years'] = 'deterioration_probe.consecutive_decline_years'

    # ============================================
    # 3. v2.0 近期斜率得分（权重0.20）- 新增逃顶能力
    # ============================================
    # recent_3y_slope: 负值越大=下行越猛
    # 归一化：slope=-0.2 (年跌20%) → score=1.0
    recent_slope = delta_input.recent_3y_slope if delta_input.recent_3y_slope is not None else 0.0

    if recent_slope >= 0:
        slope_score = 0.0  # 正斜率=上行，不惩罚
    else:
        # 负斜率归一化：-0.2→1.0, -0.1→0.5, 0→0
        slope_score = clip_01(abs(recent_slope) / 0.2)

    breakdown['recent_3y_slope'] = recent_slope
    breakdown['slope_score'] = slope_score
    breakdown['slope_interpretation'] = f"年跌{abs(recent_slope)*100:.1f}%" if recent_slope < 0 else "上行趋势"

    # ============================================
    # 4. v2.0 拐点触发器（权重0.15）- 核心进化
    # ============================================
    # Trigger = I(inflection_type == "NegativeReversal") × confidence
    inflection_type = delta_input.inflection_type if delta_input.inflection_type else "none"
    inflection_conf = delta_input.inflection_confidence if delta_input.inflection_confidence is not None else 0.0

    is_negative_reversal = inflection_type.lower() in ["negativereversal", "negative_reversal", "top_reversal"]
    trigger_score = inflection_conf if is_negative_reversal else 0.0

    breakdown['inflection_type'] = inflection_type
    breakdown['inflection_confidence'] = inflection_conf
    breakdown['trigger_score'] = trigger_score
    breakdown['is_top_reversal'] = is_negative_reversal

    # ============================================
    # 5. 恶化模式得分（权重0.10）
    # ============================================
    pattern = delta_input.deterioration_pattern
    pattern_scores = {
        'none': 0.0,
        'mild': 0.25,
        'mild_decline': 0.25,
        'moderate': 0.5,
        'severe': 0.75,
        'sustained': 0.8,
        'cliff': 1.0,
        'cliff_fall': 1.0,
        'accelerating': 0.9,
    }
    pattern_score = pattern_scores.get(pattern.lower(), 0.3)
    breakdown['deterioration_pattern'] = pattern
    breakdown['pattern_score'] = pattern_score

    # ============================================
    # 加权计算 v2.0
    # ============================================
    # 新权重分配：概率35% + 年数20% + 近期斜率20% + 拐点触发15% + 模式10%
    weights = params.decay_entropy_weights
    delta_decay = (
        weights.get('probability', 0.35) * prob_score +
        weights.get('consecutive_years', 0.20) * years_score +
        weights.get('recent_slope', 0.20) * slope_score +      # v2.0新增
        weights.get('inflection_trigger', 0.15) * trigger_score +  # v2.0新增
        weights.get('pattern', 0.10) * pattern_score
    )

    # ============================================
    # v2.0 硬触发规则：拐点逃顶
    # ============================================
    hard_trigger = False
    if is_negative_reversal and inflection_conf > 0.8:
        # NegativeReversal + 高置信度 → 强制δ_decay >= 0.7
        hard_trigger = True
        breakdown['hard_trigger_reason'] = f"顶部反转(置信度{inflection_conf:.2f})"
        delta_decay = max(delta_decay, 0.7)

    breakdown['hard_trigger'] = hard_trigger
    breakdown['final_delta_decay'] = delta_decay
    breakdown['version'] = 'v2.0_inflection_aware'

    return clip_01(delta_decay), breakdown


def create_delta_decay_gene(
    delta_input: DeltaDecayInput,
    config: TruthConfig = None,
) -> DeltaDecayGene:
    """
    创建 DeltaDecayGene 对象（便捷工厂函数）
    """
    value, breakdown = compute_delta_decay_from_probes(delta_input, config)
    return DeltaDecayGene(
        value=value,
        breakdown=breakdown,
        is_degraded=delta_input.is_degraded,
        degradation_reason=delta_input.degradation_reason if delta_input.is_degraded else "",
    )
