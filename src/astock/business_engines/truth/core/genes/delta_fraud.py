"""
Delta Fraud Gene - 欺诈熵基因 (δ_fraud) v2.0
============================================

功能：识别财务造假风险，用于"熔断机制"。

v2.0 进化核心：从"财务比率"进化为"统计异常检测"
- 原始痛点：只看存贷双高，高手造假会把比率做得很好看
- 解决方案：麦道夫特征检测（Too Smooth = 造假信号）

熔炼方程 v2.0：
δ_fraud = max(传统财务熵, 异常分数)

异常分数 = I(CV_margin < 0.01) + I(R²_revenue > 0.99) + I(ARCH_ocf)

解析：
- 麦道夫特征：毛利率CV极低 + 营收R²极高 = 人为操纵
- 脉冲特征：现金流ARCH效应 = 突击回款

硬杀规则：
- 商誉/净资产 > 40% → 直接熔断
- 异常分数 >= 0.67 → 直接熔断

作者: AStock Analysis System
日期: 2025-01
"""

from typing import Dict, Tuple
from dataclasses import dataclass
import numpy as np
import logging

from ...adapter import DeltaFraudInput
from ...config import TruthConfig, get_default_truth_config
from ...models import FraudCheckResult

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
class DeltaFraudGene:
    """
    欺诈熵基因计算结果

    Attributes:
        value: 基因值 [0, 1]，0=低风险，1=高风险
        fraud_result: 详细的欺诈检测结果
        is_fused: 是否触发熔断
        fuse_reason: 熔断原因
    """
    value: float
    fraud_result: FraudCheckResult
    is_fused: bool = False
    fuse_reason: str = ""

    @property
    def interpretation(self) -> str:
        """基因解读"""
        if self.is_fused:
            return f"⚠️ 熔断触发: {self.fuse_reason}"
        elif self.value >= 0.5:
            return "高欺诈风险：利润与现金流背离严重，需深入调查"
        elif self.value >= 0.3:
            return "中等风险：存在一些异常信号，建议关注"
        else:
            return "低欺诈风险：财务指标较为健康"


def compute_delta_fraud_from_probes(
    delta_input: DeltaFraudInput,
    config: TruthConfig = None,
) -> Tuple[float, FraudCheckResult]:
    """
    从财务数据计算欺诈熵基因 δ_fraud (v2.0 Signal Fusion Core)

    v2.0 熔炼方程：
    δ_fraud = max(传统财务熵, 麦道夫异常分数)

    传统四维熵：
    1. 应计异常熵 - (净利润 - 经营现金流) / 总资产
    2. 现金流背离熵 - |净利润 - 经营现金流| / 营收
    3. 商誉风险熵 - 商誉 / 净资产
    4. 关联交易熵 - 关联交易 / 营收

    v2.0 麦道夫特征（统计异常检测）：
    - Too Smooth Margin: 毛利率CV < 0.01 → 几乎一条直线
    - Too Perfect Revenue: 营收R² > 0.99 → 完美45度上扬
    - Cash Manipulation: 现金流ARCH效应 → 突击回款

    Args:
        delta_input: 财务数据输入
        config: T.R.U.T.H. 配置

    Returns:
        (delta_fraud_value, fraud_check_result)
    """
    if config is None:
        config = get_default_truth_config()

    params = config.genes
    weights = params.fraud_entropy_weights

    if delta_input.is_degraded:
        return 0.3, FraudCheckResult(
            accrual_entropy=0.0,
            fcf_divergence=0.0,
            goodwill_risk=0.0,
            related_party_entropy=0.0,
            combined_entropy=0.3,
            is_fused=False,
            fuse_reason="降级模式: " + delta_input.degradation_reason,
            goodwill_to_equity=0.0,
            is_goodwill_kill=False,
        )

    net_profit = delta_input.net_profit
    operating_cashflow = delta_input.operating_cashflow
    revenue = delta_input.revenue
    total_assets = delta_input.total_assets
    goodwill = delta_input.goodwill
    equity = delta_input.equity
    related_party = delta_input.related_party_transactions

    # ============================================================
    # 传统四维财务熵
    # ============================================================

    # 1. 应计异常熵
    accruals = net_profit - operating_cashflow
    accrual_ratio = safe_divide(accruals, total_assets, 0)
    accrual_entropy = clip_01(abs(accrual_ratio) / params.accrual_anomaly_threshold)

    # 2. 现金流背离熵
    fcf_divergence_ratio = safe_divide(abs(net_profit - operating_cashflow), revenue, 0)
    fcf_entropy = clip_01(fcf_divergence_ratio / params.fcf_divergence_threshold)

    # 3. 商誉风险熵
    goodwill_ratio = safe_divide(goodwill, equity, 0) if equity > 0 else 0
    goodwill_entropy = clip_01(goodwill_ratio / params.goodwill_kill_threshold)
    is_goodwill_kill = goodwill_ratio > params.goodwill_kill_threshold

    # 4. 关联交易熵
    related_ratio = safe_divide(related_party, revenue, 0)
    related_entropy = clip_01(related_ratio / 0.3)

    # 传统综合熵
    traditional_entropy = (
        weights['accrual_anomaly'] * accrual_entropy +
        weights['fcf_divergence'] * fcf_entropy +
        weights['goodwill_risk'] * goodwill_entropy +
        weights['related_party'] * related_entropy
    )

    # ============================================================
    # v2.0 麦道夫特征检测（统计异常）
    # ============================================================

    # 1. 毛利率太光滑 (Too Smooth Margin)
    # CV < 0.01 说明毛利率几乎不波动，高度可疑
    margin_cv = delta_input.margin_cv
    too_smooth_margin = 1.0 if margin_cv < 0.01 else (0.5 if margin_cv < 0.03 else 0.0)

    # 2. 营收太完美 (Too Perfect Revenue)
    # R² > 0.99 说明营收是完美的45度线，几乎不可能
    revenue_r2 = delta_input.revenue_r_squared
    too_perfect_revenue = 1.0 if revenue_r2 > 0.99 else (0.5 if revenue_r2 > 0.95 else 0.0)

    # 3. 现金流操纵 (Cash Manipulation)
    # ARCH效应 = 波动聚集 = 突击回款
    cash_manipulation = 1.0 if delta_input.ocf_has_arch_effect else 0.0

    # 麦道夫异常分数
    madoff_score = (too_smooth_margin + too_perfect_revenue + cash_manipulation) / 3

    # 双重太完美 = 极高危
    if too_smooth_margin >= 0.5 and too_perfect_revenue >= 0.5:
        madoff_score = max(madoff_score, 0.8)  # 提升到高危
        logger.warning("检测到麦道夫特征：毛利率+营收双重太完美")

    # ============================================================
    # 综合熔炼
    # ============================================================

    # 取最高危值（传统 or 麦道夫）
    combined = max(traditional_entropy, madoff_score)

    # 熔断判断
    is_madoff_fuse = madoff_score >= 0.67
    is_traditional_fuse = traditional_entropy >= params.fraud_fuse_threshold
    is_fused = is_goodwill_kill or is_madoff_fuse or is_traditional_fuse

    fuse_reason = ""
    if is_goodwill_kill:
        fuse_reason = f"商誉/净资产={goodwill_ratio:.1%} > {params.goodwill_kill_threshold:.0%}"
    elif is_madoff_fuse:
        fuse_reason = f"麦道夫特征={madoff_score:.2f}>=0.67 (毛利率CV={margin_cv:.3f}, 营收R²={revenue_r2:.3f})"
    elif is_traditional_fuse:
        fuse_reason = f"传统欺诈熵={traditional_entropy:.2f} >= {params.fraud_fuse_threshold}"

    result = FraudCheckResult(
        accrual_entropy=accrual_entropy,
        fcf_divergence=fcf_entropy,
        goodwill_risk=goodwill_entropy,
        related_party_entropy=related_entropy,
        combined_entropy=combined,
        is_fused=is_fused,
        fuse_reason=fuse_reason,
        goodwill_to_equity=goodwill_ratio,
        is_goodwill_kill=is_goodwill_kill,
        # v2.0 新增字段（通过扩展dict传递）
        # madoff_score=madoff_score,
        # too_smooth_margin=too_smooth_margin,
        # too_perfect_revenue=too_perfect_revenue,
        # cash_manipulation=cash_manipulation,
    )

    return clip_01(combined), result


def create_delta_fraud_gene(
    delta_input: DeltaFraudInput,
    config: TruthConfig = None,
) -> DeltaFraudGene:
    """
    创建 DeltaFraudGene 对象（便捷工厂函数）
    """
    value, fraud_result = compute_delta_fraud_from_probes(delta_input, config)
    return DeltaFraudGene(
        value=value,
        fraud_result=fraud_result,
        is_fused=fraud_result.is_fused,
        fuse_reason=fraud_result.fuse_reason,
    )
