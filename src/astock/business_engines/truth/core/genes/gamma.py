"""
Gamma Gene - 成长动能基因 (γ) v2.0
==================================

功能：评估公司的成长性，用于上帝方程中的"真成长奖励"。

v2.0 进化核心：从"复合增速"进化为"稳健动能"
- 原始痛点：容易被单年非经常性损益骗过
- 解决方案：稳健斜率 + R²惩罚 + 断点重置

熔炼方程 v2.0：
Step 1 (断点重置): 若 has_break 为真，强制仅使用断点后数据计算 Slope
Step 2 (动能合成): γ = Norm(slope_robust) × (1 + 0.5 × max(0, a)) × √R²

解析：
- 用 Theil-Sen 稳健斜率剔除噪音
- 奖励加速上涨 (a > 0)
- R² 惩罚：如果 R² 低（忽上忽下），说明增长不可持续，γ 大幅打折

作者: AStock Analysis System
日期: 2025-01
"""

from typing import Dict, Tuple
from dataclasses import dataclass
import math
import logging

from ...adapter import GammaGeneInput
from ...config import TruthConfig, get_default_truth_config

logger = logging.getLogger(__name__)


def clip_01(value: float) -> float:
    """裁剪到 [0, 1] 区间"""
    return max(0.0, min(1.0, value))


@dataclass
class GammaGene:
    """
    成长动能基因计算结果

    Attributes:
        value: 基因值 [0, 1]，0=衰退，1=高成长
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
            return "高成长：营收/利润快速增长，但需验证现金流支撑"
        elif self.value >= 0.4:
            return "中等成长：稳健增长，平衡成长与估值"
        else:
            return "低成长/衰退：增长乏力，需高度警惕基本面恶化"


def _normalize_slope_to_gamma(slope: float, anchors: dict) -> float:
    """
    将斜率/CAGR映射到[0,1]区间的gamma值

    锚点映射：
    - ≤0% → 0.0~0.3
    - 15% → 0.6
    - 30%+ → 0.9~1.0
    """
    if slope <= 0:
        # 负增长：映射到[0, 0.3]
        return max(0, anchors['zero'] * (1 + slope))
    elif slope <= 0.15:
        # 0-15%：映射到[0.3, 0.6]
        return anchors['zero'] + (anchors['moderate'] - anchors['zero']) * (slope / 0.15)
    elif slope <= 0.30:
        # 15-30%：映射到[0.6, 0.9]
        return anchors['moderate'] + (anchors['high'] - anchors['moderate']) * ((slope - 0.15) / 0.15)
    else:
        # >30%：映射到[0.9, 1.0]
        return anchors['high'] + (1 - anchors['high']) * min((slope - 0.30) / 0.20, 1)


def compute_gamma_from_probes(
    gamma_input: GammaGeneInput,
    config: TruthConfig = None,
) -> Tuple[float, Dict[str, float]]:
    """
    从探针输出计算成长动能基因 γ (v2.0 Signal Fusion Core)

    v2.0 熔炼方程：
    Step 1: 断点重置 - 若 has_structural_break 且置信度高，用断点后斜率
    Step 2: 选择斜率 - 优先用稳健斜率(Theil-Sen)，否则用普通CAGR
    Step 3: 动能合成 - γ = Norm(slope) × (1 + 0.5 × max(0, accel)) × √R²

    核心进化：
    1. 稳健斜率 - 抗噪，不被异常值骗
    2. 断点处理 - 业务转型后只看新数据
    3. R²惩罚 - 不稳定增长大幅打折

    Args:
        gamma_input: 探针适配器提供的输入
        config: T.R.U.T.H. 配置

    Returns:
        (gamma_value, breakdown_dict)
    """
    if config is None:
        config = get_default_truth_config()

    params = config.genes
    breakdown = {}

    if gamma_input.is_degraded:
        return 0.3, {'degraded': True, 'reason': gamma_input.degradation_reason}

    # === Step 1: 断点检测与重置 ===
    use_post_break = False
    if gamma_input.has_structural_break and gamma_input.break_confidence > 0.7:
        use_post_break = True
        logger.info(f"检测到结构断点(置信度{gamma_input.break_confidence:.2f})，使用断点后数据")

    breakdown['has_structural_break'] = gamma_input.has_structural_break
    breakdown['break_confidence'] = gamma_input.break_confidence
    breakdown['use_post_break'] = use_post_break

    # === Step 2: 选择最佳斜率估计 ===
    if use_post_break and gamma_input.post_break_slope != 0:
        # 优先使用断点后斜率
        slope = gamma_input.post_break_slope
        slope_source = 'post_break_slope'
    elif gamma_input.has_robust and gamma_input.robust_slope != 0:
        # 其次使用稳健斜率（Theil-Sen，抗噪）
        slope = gamma_input.robust_slope
        slope_source = 'robust_slope (Theil-Sen)'
    else:
        # 降级使用加权CAGR
        slope = (
            params.revenue_growth_weight * gamma_input.revenue_cagr +
            params.profit_growth_weight * gamma_input.profit_cagr
        )
        slope_source = 'weighted_cagr'

    breakdown['slope'] = slope
    breakdown['slope_source'] = slope_source
    breakdown['revenue_cagr'] = gamma_input.revenue_cagr
    breakdown['profit_cagr'] = gamma_input.profit_cagr
    breakdown['robust_slope'] = gamma_input.robust_slope

    # === Step 3: 基础gamma（斜率→[0,1]映射）===
    anchors = params.gamma_growth_anchors
    gamma_base = _normalize_slope_to_gamma(slope, anchors)
    breakdown['gamma_base'] = gamma_base

    # === Step 4: 加速度奖励 ===
    # 正加速（趋势加速）给予奖励，负加速（减速）给予惩罚
    acceleration = gamma_input.trend_acceleration
    accel_bonus = 0.5 * max(0, acceleration)  # 只奖励正加速
    accel_penalty = 0.3 * min(0, acceleration)  # 负加速惩罚
    accel_factor = 1.0 + accel_bonus + accel_penalty

    breakdown['trend_acceleration'] = acceleration
    breakdown['accel_factor'] = accel_factor

    # === Step 5: R² 惩罚（核心进化）===
    # 低R²说明增长不稳定，大幅打折
    r_squared = gamma_input.r_squared if gamma_input.r_squared > 0 else 0.5  # 默认中性
    r2_penalty = math.sqrt(r_squared)  # √R² 惩罚

    breakdown['r_squared'] = r_squared
    breakdown['r2_penalty'] = r2_penalty

    # === v2.0 熔炼方程 ===
    gamma = gamma_base * accel_factor * r2_penalty

    breakdown['final_gamma'] = gamma
    breakdown['version'] = '2.0_signal_fusion'

    return clip_01(gamma), breakdown


def create_gamma_gene(
    gamma_input: GammaGeneInput,
    config: TruthConfig = None,
) -> GammaGene:
    """
    创建 GammaGene 对象（便捷工厂函数）
    """
    value, breakdown = compute_gamma_from_probes(gamma_input, config)
    return GammaGene(
        value=value,
        breakdown=breakdown,
        is_degraded=gamma_input.is_degraded,
        degradation_reason=gamma_input.degradation_reason if gamma_input.is_degraded else "",
    )
