"""
T.R.U.T.H. System - Core Engine
===============================

核心计算引擎，实现：
1. 六维基因测序
2. 三大物理求解器
3. 代表性指标计算

设计原则：
1. 纯函数优先（便于测试和并行）
2. 向量化计算（numpy优先）
3. 完整的日志和诊断信息
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any, Sequence
import numpy as np
from scipy import stats
import logging

from .models import (
    CompanyGenome,
    RepresentativeMetrics,
    ThresholdResult,
    TruthResult,
    SignalType,
    GradeLevel,
    FraudCheckResult,
)
from .config import TruthConfig, get_default_truth_config

logger = logging.getLogger(__name__)


# ============================================================================
# 辅助函数
# ============================================================================

def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """安全除法"""
    if denominator == 0 or np.isnan(denominator):
        return default
    return numerator / denominator


def clip_01(value: float) -> float:
    """裁剪到 [0, 1] 区间"""
    return max(0.0, min(1.0, value))


def compute_cv(values: np.ndarray) -> float:
    """计算变异系数"""
    mean_val = np.mean(values)
    if abs(mean_val) < 1e-6:
        return 0.0
    return np.std(values) / abs(mean_val)


def compute_detrended_cv(values: np.ndarray) -> float:
    """
    计算去趋势变异系数

    先用OLS去除线性趋势，再计算残差的CV
    这样区分"向上/向下的稳定增长"和"真正的周期波动"
    """
    n = len(values)
    if n < 3:
        return compute_cv(values)

    x = np.arange(n)
    slope, intercept, _, _, _ = stats.linregress(x, values)

    # 去趋势残差
    trend = slope * x + intercept
    residuals = values - trend

    # 残差的标准差 / 均值的绝对值
    mean_val = np.mean(values)
    if abs(mean_val) < 1e-6:
        return 0.0

    return np.std(residuals) / abs(mean_val)


def compute_peak_to_trough_ratio(values: np.ndarray) -> float:
    """计算峰谷比"""
    max_val = np.max(values)
    min_val = np.min(values)

    if min_val <= 0:
        # 有负值或零，使用范围/均值
        range_val = max_val - min_val
        mean_val = np.mean(values)
        if abs(mean_val) < 1e-6:
            return 1.0
        return range_val / abs(mean_val)

    return max_val / min_val


def count_reversals(values: np.ndarray) -> int:
    """计算反转次数（方向变化）"""
    if len(values) < 3:
        return 0

    diffs = np.diff(values)
    signs = np.sign(diffs)

    # 去除零（连续相等的情况）
    signs = signs[signs != 0]

    if len(signs) < 2:
        return 0

    # 符号变化次数
    return int(np.sum(np.abs(np.diff(signs)) > 0))


def ewma(values: np.ndarray, weights: np.ndarray = None) -> float:
    """
    指数加权移动平均

    权重默认：[0.10, 0.15, 0.20, 0.25, 0.30]（最新权重最大）
    """
    n = len(values)
    if n == 0:
        return 0.0

    if weights is None:
        # 默认权重（支持不同长度）
        if n == 5:
            weights = np.array([0.10, 0.15, 0.20, 0.25, 0.30])
        elif n == 10:
            weights = np.array([0.03, 0.04, 0.05, 0.07, 0.09, 0.11, 0.13, 0.15, 0.16, 0.17])
        else:
            # 指数衰减权重
            decay = 0.8
            raw_weights = np.array([decay ** (n - 1 - i) for i in range(n)])
            weights = raw_weights / raw_weights.sum()

    # 确保长度匹配
    if len(weights) != n:
        decay = 0.8
        raw_weights = np.array([decay ** (n - 1 - i) for i in range(n)])
        weights = raw_weights / raw_weights.sum()

    return float(np.dot(values, weights))


# ============================================================================
# 基因计算函数
# ============================================================================

def compute_alpha(
    roic_series: np.ndarray,
    config: TruthConfig,
    industry: str = "",
) -> Tuple[float, Dict[str, float]]:
    """
    计算周期性基因 α

    核心逻辑：
    1. 去趋势CV（最重要）- 区分稳定成长和真正周期
    2. 峰谷比
    3. R²低（高不确定性）
    4. 反转次数

    Returns:
        (alpha_value, breakdown_dict)
    """
    params = config.genes
    breakdown = {}

    if len(roic_series) < 3:
        return 0.5, {'reason': 'insufficient_data'}

    # 1. 去趋势CV（权重0.30）
    detrended_cv = compute_detrended_cv(roic_series)
    cv_score = clip_01(detrended_cv / params.cv_saturation)
    breakdown['detrended_cv'] = detrended_cv
    breakdown['cv_score'] = cv_score

    # 2. 峰谷比（权重0.25）
    pt_ratio = compute_peak_to_trough_ratio(roic_series)
    pt_score = clip_01((pt_ratio - 1) / (params.peak_trough_saturation - 1))
    breakdown['peak_trough_ratio'] = pt_ratio
    breakdown['pt_score'] = pt_score

    # 3. R²（权重0.20）- 低R²表示高不确定性
    x = np.arange(len(roic_series))
    _, _, r_value, _, _ = stats.linregress(x, roic_series)
    r_squared = r_value ** 2
    low_r2_score = clip_01(1 - r_squared)  # 低R² -> 高分
    breakdown['r_squared'] = r_squared
    breakdown['low_r2_score'] = low_r2_score

    # 4. 反转次数（权重0.15）
    reversals = count_reversals(roic_series)
    max_possible_reversals = len(roic_series) - 2
    reversal_score = clip_01(reversals / max(max_possible_reversals, 1))
    breakdown['reversals'] = reversals
    breakdown['reversal_score'] = reversal_score

    # 5. 波动模式（权重0.10）- 是否有明显的周期波动
    # 简化：用标准CV补充
    simple_cv = compute_cv(roic_series)
    wave_score = clip_01(simple_cv / params.cv_saturation)
    breakdown['simple_cv'] = simple_cv
    breakdown['wave_score'] = wave_score

    # 加权计算
    weights = params.cycle_factor_weights
    alpha = (
        weights['detrended_cv'] * cv_score +
        weights['peak_to_trough'] * pt_score +
        weights['low_r_squared'] * low_r2_score +
        weights.get('wave_pattern', 0.15) * wave_score +
        weights.get('reversal_count', 0.10) * reversal_score
    )

    return clip_01(alpha), breakdown


def compute_beta(
    roic_series: np.ndarray,
    gross_margin_series: np.ndarray,
    config: TruthConfig,
) -> Tuple[float, Dict[str, float]]:
    """
    计算资本密度基因 β

    使用波动性作为资本密度的代理变量：
    - 重资产企业的ROIC波动通常更大（高固定成本杠杆）
    - 轻资产企业的毛利率更稳定

    注意：这是一个代理方法，理想情况下应使用：
    - 固定资产/总资产
    - 资本支出/营收
    - 折旧/营收
    """
    params = config.genes
    breakdown = {}

    if len(roic_series) < 3:
        return 0.5, {'reason': 'insufficient_data'}

    # 1. ROIC波动性（权重0.40）
    roic_volatility = np.std(roic_series)
    # 归一化：假设重资产ROIC波动约0.15，轻资产约0.03
    vol_score = clip_01(roic_volatility / 0.15)
    breakdown['roic_volatility'] = roic_volatility
    breakdown['vol_score'] = vol_score

    # 2. 毛利率稳定性（权重0.30）
    if len(gross_margin_series) >= 3:
        margin_cv = compute_cv(gross_margin_series)
        # 高CV -> 高β（重资产）
        margin_score = clip_01(margin_cv / 0.3)  # CV>0.3视为高波动
    else:
        margin_score = 0.5
    breakdown['margin_cv'] = margin_cv if len(gross_margin_series) >= 3 else None
    breakdown['margin_score'] = margin_score

    # 3. 资本支出强度代理（权重0.30）
    # 使用ROIC水平的逆向代理：低ROIC常见于重资产
    # 注意：这不是完美的代理，但在缺乏资本支出数据时可用
    mean_roic = np.mean(roic_series)
    # 低ROIC -> 高β
    # ROIC 5% -> β≈0.7, ROIC 20% -> β≈0.3
    capex_proxy_score = clip_01(1 - (mean_roic - 0.05) / 0.20)
    breakdown['mean_roic'] = mean_roic
    breakdown['capex_proxy_score'] = capex_proxy_score

    # 加权计算
    weights = params.beta_factor_weights
    beta = (
        weights['roic_volatility'] * vol_score +
        weights['margin_stability'] * margin_score +
        weights.get('capex_intensity', 0.30) * capex_proxy_score
    )

    return clip_01(beta), breakdown


def compute_gamma(
    revenue_growth_series: np.ndarray,
    profit_growth_series: np.ndarray,
    config: TruthConfig,
) -> Tuple[float, Dict[str, float]]:
    """
    计算成长动能基因 γ

    基于营收和利润的增长趋势
    γ ∈ [0, 1]: 0=衰退, 0.3=停滞, 0.6=稳定增长, 0.9=高成长
    """
    params = config.genes
    breakdown = {}

    # 计算复合增长率
    def calc_cagr(growth_rates: np.ndarray) -> float:
        """从年增长率序列计算CAGR"""
        if len(growth_rates) == 0:
            return 0.0
        # growth_rates 是百分比形式，如 [0.1, 0.15, 0.2]
        # 计算累积增长
        cumulative = np.prod(1 + growth_rates)
        n = len(growth_rates)
        if cumulative <= 0:
            return -1.0  # 负增长
        return cumulative ** (1 / n) - 1

    # 营收CAGR
    if len(revenue_growth_series) >= 2:
        revenue_cagr = calc_cagr(revenue_growth_series[-params.cagr_years:])
    else:
        revenue_cagr = 0.0
    breakdown['revenue_cagr'] = revenue_cagr

    # 利润CAGR
    if len(profit_growth_series) >= 2:
        profit_cagr = calc_cagr(profit_growth_series[-params.cagr_years:])
    else:
        profit_cagr = 0.0
    breakdown['profit_cagr'] = profit_cagr

    # 加权CAGR
    weighted_cagr = (
        params.revenue_growth_weight * revenue_cagr +
        params.profit_growth_weight * profit_cagr
    )
    breakdown['weighted_cagr'] = weighted_cagr

    # 映射到 [0, 1]
    # 使用锚点进行分段线性映射
    anchors = params.gamma_growth_anchors
    if weighted_cagr <= 0:
        # 负增长或零增长
        gamma = anchors['zero'] * (1 + weighted_cagr)  # 线性衰减
        gamma = max(0, gamma)
    elif weighted_cagr <= 0.15:
        # 0% - 15%: 线性映射到 [0.3, 0.6]
        gamma = anchors['zero'] + (anchors['moderate'] - anchors['zero']) * (weighted_cagr / 0.15)
    elif weighted_cagr <= 0.30:
        # 15% - 30%: 线性映射到 [0.6, 0.9]
        gamma = anchors['moderate'] + (anchors['high'] - anchors['moderate']) * ((weighted_cagr - 0.15) / 0.15)
    else:
        # >30%: 饱和在高区间
        gamma = anchors['high'] + (1 - anchors['high']) * min((weighted_cagr - 0.30) / 0.20, 1)

    breakdown['gamma'] = gamma
    return clip_01(gamma), breakdown


def compute_delta_fraud(
    net_profit: float,
    operating_cashflow: float,
    revenue: float,
    total_assets: float,
    goodwill: float,
    equity: float,
    receivables: float,
    related_party_transactions: float,
    config: TruthConfig,
) -> Tuple[float, FraudCheckResult]:
    """
    计算欺诈熵基因 δ_fraud

    四维熵：
    1. 应计异常熵 - (净利润 - 经营现金流) / 总资产
    2. 现金流背离熵 - |净利润 - 经营现金流| / 营收
    3. 商誉风险熵 - 商誉 / 净资产
    4. 关联交易熵 - 关联交易 / 营收
    """
    params = config.genes
    weights = params.fraud_entropy_weights

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

    # 商誉硬杀检查
    is_goodwill_kill = goodwill_ratio > params.goodwill_kill_threshold

    # 4. 关联交易熵
    related_ratio = safe_divide(related_party_transactions, revenue, 0)
    related_entropy = clip_01(related_ratio / 0.3)  # 30%视为高风险

    # 综合熵
    combined = (
        weights['accrual_anomaly'] * accrual_entropy +
        weights['fcf_divergence'] * fcf_entropy +
        weights['goodwill_risk'] * goodwill_entropy +
        weights['related_party'] * related_entropy
    )

    # 熔断判断
    is_fused = combined >= params.fraud_fuse_threshold or is_goodwill_kill
    fuse_reason = ""
    if is_goodwill_kill:
        fuse_reason = f"商誉/净资产={goodwill_ratio:.1%} > {params.goodwill_kill_threshold:.0%}"
    elif combined >= params.fraud_fuse_threshold:
        fuse_reason = f"欺诈熵={combined:.2f} >= {params.fraud_fuse_threshold}"

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
    )

    return clip_01(combined), result


def compute_delta_decay(
    roic_series: np.ndarray,
    gross_margin_series: np.ndarray,
    config: TruthConfig,
) -> Tuple[float, Dict[str, float]]:
    """
    计算衰退熵基因 δ_decay

    三维熵：
    1. 近期下滑 - 最近2年的下滑幅度
    2. 下滑加速 - 二阶导数（下滑是否加速）
    3. 毛利率侵蚀 - 毛利率的下滑
    """
    params = config.genes
    breakdown = {}

    if len(roic_series) < 3:
        return 0.3, {'reason': 'insufficient_data'}

    n = len(roic_series)
    recent_years = params.decay_recent_years

    # 1. 近期下滑（权重0.40）
    if n >= recent_years + 1:
        recent_change = roic_series[-1] - roic_series[-(recent_years + 1)]
        # 负变化 -> 高衰退熵
        # -10%变化 -> δ_decay ≈ 1
        decline_score = clip_01(-recent_change / 0.10) if recent_change < 0 else 0
    else:
        decline_score = 0
    breakdown['recent_change'] = recent_change if n >= recent_years + 1 else None
    breakdown['decline_score'] = decline_score

    # 2. 下滑加速（权重0.30）
    # 计算一阶差分和二阶差分
    first_diff = np.diff(roic_series)
    if len(first_diff) >= 2:
        second_diff = np.diff(first_diff)
        # 负的二阶导数表示下滑加速
        recent_acceleration = second_diff[-1] if len(second_diff) > 0 else 0
        # 加速下滑 -> 高分
        accel_score = clip_01(-recent_acceleration / 0.05) if recent_acceleration < 0 else 0
    else:
        accel_score = 0
    breakdown['acceleration'] = recent_acceleration if len(first_diff) >= 2 else None
    breakdown['accel_score'] = accel_score

    # 3. 毛利率侵蚀（权重0.30）
    if len(gross_margin_series) >= 3:
        x = np.arange(len(gross_margin_series))
        slope, _, _, _, _ = stats.linregress(x, gross_margin_series)
        # 负斜率 -> 高分
        margin_erosion_score = clip_01(-slope / 0.03) if slope < 0 else 0
    else:
        margin_erosion_score = 0
        slope = None
    breakdown['margin_slope'] = slope
    breakdown['margin_erosion_score'] = margin_erosion_score

    # 加权计算
    weights = params.decay_entropy_weights
    delta_decay = (
        weights['recent_decline'] * decline_score +
        weights['acceleration'] * accel_score +
        weights['margin_erosion'] * margin_erosion_score
    )

    return clip_01(delta_decay), breakdown


def compute_verification(
    operating_cashflow: float,
    net_profit: float,
    advance_receipts: float,
    revenue: float,
    config: TruthConfig,
) -> Tuple[float, Dict[str, float]]:
    """
    计算真相验证因子 V

    V = f(现金转化率, 预收款比例)

    高V表示盈利质量高（现金流支撑强）
    """
    params = config.genes
    breakdown = {}

    # 1. 现金转化率
    # 经营现金流 / 净利润 > 1 表示现金收入超过会计利润
    if net_profit > 0:
        cash_conversion = safe_divide(operating_cashflow, net_profit, 0)
        # 归一化：0.5-1.5 映射到 [0, 1]
        conversion_score = clip_01((cash_conversion - 0.5) / 1.0)
    else:
        # 亏损企业，如果现金流为正，给予基础分
        conversion_score = 0.3 if operating_cashflow > 0 else 0
    breakdown['cash_conversion'] = cash_conversion if net_profit > 0 else None
    breakdown['conversion_score'] = conversion_score

    # 2. 预收款奖励
    # V_bonus = min(预收/营收 * 0.6, 0.25)
    advance_ratio = safe_divide(advance_receipts, revenue, 0)
    v_bonus = min(
        advance_ratio * params.advance_receipt_coefficient,
        params.advance_receipt_bonus_cap
    )
    breakdown['advance_ratio'] = advance_ratio
    breakdown['v_bonus'] = v_bonus

    # 综合V因子
    # 基础分 + 现金转化贡献 + 预收奖励
    base_v = 0.3  # 基础分
    verification = base_v + conversion_score * params.cash_conversion_coefficient + v_bonus

    return clip_01(verification), breakdown


# ============================================================================
# 代表性指标计算
# ============================================================================

def compute_representative_roic(
    roic_series: np.ndarray,
    config: TruthConfig,
) -> RepresentativeMetrics:
    """
    计算代表性ROIC

    核心公式：
    Rep_ROIC = EWMA(ROIC) + Δ_momentum + Δ_deterioration

    非对称动量修正：
    - 上涨不奖励 (Δ = 0)
    - 下跌惩罚 (Δ = slope × 2 × 0.8)
    """
    params = config.representative

    if len(roic_series) == 0:
        return RepresentativeMetrics(
            ewma_value=0, simple_mean=0, latest_value=0,
            trend_slope=0, momentum_delta=0, deterioration_delta=0,
            representative_value=0, floor_value=params.absolute_floor,
            final_value=params.absolute_floor
        )

    # 基础统计
    ewma_value = ewma(roic_series)
    simple_mean = float(np.mean(roic_series))
    latest_value = float(roic_series[-1])

    # OLS趋势斜率
    if len(roic_series) >= 3:
        x = np.arange(len(roic_series))
        slope, _, _, _, _ = stats.linregress(x, roic_series)
    else:
        slope = 0.0

    # 非对称动量修正
    if slope >= 0:
        # 上涨：不奖励
        momentum_delta = 0.0
    else:
        # 下跌：惩罚
        # Δ = slope × multiplier × coefficient
        momentum_delta = slope * params.downward_penalty_multiplier * params.downward_momentum_coef

    # 恶化惩罚（近期vs早期）
    if len(roic_series) >= 4:
        recent_avg = np.mean(roic_series[-2:])
        early_avg = np.mean(roic_series[:-2])
        if recent_avg < early_avg:
            deterioration_delta = (recent_avg - early_avg) * 0.5  # 额外惩罚
        else:
            deterioration_delta = 0.0
    else:
        deterioration_delta = 0.0

    # 代表性值
    representative_value = ewma_value + momentum_delta + deterioration_delta

    # 地板保护
    # floor = max(最新年 × 0.4, -0.05)
    floor_value = max(
        latest_value * params.floor_ratio,
        params.absolute_floor
    )

    # 最终值
    final_value = max(representative_value, floor_value)

    return RepresentativeMetrics(
        ewma_value=ewma_value,
        simple_mean=simple_mean,
        latest_value=latest_value,
        trend_slope=slope,
        momentum_delta=momentum_delta,
        deterioration_delta=deterioration_delta,
        representative_value=representative_value,
        floor_value=floor_value,
        final_value=final_value,
    )


# ============================================================================
# 物理求解器
# ============================================================================

def gravity_solver(
    genome: CompanyGenome,
    config: TruthConfig,
) -> ThresholdResult:
    """
    重力求解器 - 计算动态阈值（分母）

    上帝方程 I（来自 TRUTH_SYSTEM_DESIGN.md）：
    T_roic = R_f + k₁β - k₂α - k₃(γ×E×V) + k₄δ_decay

    其中：
    - R_f: 无风险利率（资金成本底线）
    - k₁β: 重资产惩罚（β越高，要求回报越高）
    - k₂α: 周期豁免（α越高，阈值越低，因为周期底部允许低回报）
    - k₃(γ×E×V): 真成长奖励（只有真成长才能降低阈值）
    - k₄δ_decay: 衰退惩罚（恶化中的公司需要更高门槛）
    """
    macro = config.macro
    solver = config.solver

    # 基础利率 R_f
    base_rate = macro.risk_free_rate

    # k₁β: 重资产惩罚（正号，增加阈值）
    # 重资产公司必须有更高回报才值得投资
    beta_premium = solver.k1_beta * genome.beta

    # k₂α: 周期豁免（负号，降低阈值）
    # 周期股在底部允许低回报
    alpha_discount = solver.k2_alpha * genome.alpha

    # k₃(γ×E×V): 真成长奖励（负号，降低阈值）
    # 只有 γ(成长) × E(市场情绪) × V(真钱验证) 同时高时，才允许低当期回报
    E = macro.market_sentiment_factor  # 市场情绪因子，默认1.0
    growth_discount = solver.k3_gamma * genome.gamma * E * genome.verification

    # k₄δ_decay: 衰退惩罚（正号，增加阈值）
    # 恶化中的公司需要更高门槛
    decay_penalty = solver.k4_decay * genome.delta_decay

    # 上帝方程 I: T = R_f + k₁β - k₂α - k₃(γEV) + k₄δ_decay
    theory_threshold = (
        base_rate
        + beta_premium
        - alpha_discount
        - growth_discount
        + decay_penalty
    )

    # 阈值保护（防止极端值）
    theory_threshold = max(theory_threshold, solver.threshold_floor)
    theory_threshold = min(theory_threshold, solver.threshold_ceiling)

    return ThresholdResult(
        base_rate=base_rate,
        beta_premium=beta_premium,
        alpha_discount=alpha_discount,
        growth_discount=growth_discount,
        decay_penalty=decay_penalty,
        cluster_residual=0.0,  # 后续由校准器填充
        size_residual=0.0,     # 后续由校准器填充
        theory_threshold=theory_threshold,
        final_threshold=theory_threshold,  # 校准前等于理论值
    )


def velocity_solver(
    genome: CompanyGenome,
    revenue_growth: float,
    config: TruthConfig,
) -> Tuple[float, bool, str]:
    """
    速度求解器 - 评估增长动能的动态阈值

    上帝方程 II（来自 TRUTH_SYSTEM_DESIGN.md）：
    T_growth = GDP_g + k₁(γ×E×V) - k₂(α×(1-S))

    其中：
    - GDP_g: GDP增速基准（及格线）
    - k₁(γ×E×V): 成长溢价（真成长 + 牛市 = 要求更高增速）
    - k₂(α×(1-S)): 周期豁免（强周期 + 萧条 = 允许负增长）

    Returns:
        (growth_threshold, passes, message)
    """
    macro = config.macro
    solver = config.solver

    # 基准：GDP增速
    gdp_growth = macro.gdp_growth_rate

    # k₁(γ×E×V): 成长溢价
    # 真成长公司在牛市时应该展现更高增速
    E = macro.market_sentiment_factor
    growth_premium = 0.15 * genome.gamma * E * genome.verification

    # k₂(α×(1-S)): 周期豁免
    # 宏观景气度 S：用 market_sentiment_factor 映射（E高=景气好）
    S = min(1.0, max(0.0, (E - 0.5) * 2))  # 将E映射到[0,1]
    cycle_exemption = 0.10 * genome.alpha * (1 - S)

    # 上帝方程 II
    growth_threshold = gdp_growth + growth_premium - cycle_exemption

    # 判断是否通过
    passes = revenue_growth >= growth_threshold

    # 生成消息
    if passes:
        if revenue_growth > growth_threshold + 0.10:
            message = f"营收增速 {revenue_growth:.1%} 显著超越阈值 {growth_threshold:.1%}"
        else:
            message = f"营收增速 {revenue_growth:.1%} 达标（阈值 {growth_threshold:.1%}）"
    else:
        gap = growth_threshold - revenue_growth
        message = f"营收增速 {revenue_growth:.1%} 低于阈值 {growth_threshold:.1%}（差距 {gap:.1%}）"

    return growth_threshold, passes, message


def structure_solver(
    gross_margin_slope: float,
    genome: CompanyGenome,
    config: TruthConfig,
) -> Tuple[bool, str]:
    """
    结构求解器 - 检测毛利率结构是否恶化

    上帝方程 III（来自 TRUTH_SYSTEM_DESIGN.md）：
    T_slope = -0.02 + k₁(1-β) - k₂×δ_decay

    其中：
    - -0.02: 自然波动容忍（每年2%下滑不算恶化）
    - k₁(1-β): 轻资产严查（软件/白酒等轻资产公司毛利率下跌是大问题）
    - k₂×δ_decay: 衰退惩罚（已经在恶化的公司门槛更严）

    Returns:
        (has_warning, warning_message)
    """
    solver = config.solver

    # 上帝方程 III 参数
    natural_tolerance = -0.02  # 每年2%自然波动容忍
    k1_light_asset = 0.03      # 轻资产严查系数
    k2_decay = 0.02            # 衰退惩罚系数

    # 计算动态阈值
    # 轻资产(β低) → (1-β)高 → 阈值更接近0 → 更严格
    # 重资产(β高) → (1-β)低 → 阈值更负 → 更宽容
    slope_threshold = (
        natural_tolerance
        + k1_light_asset * (1 - genome.beta)
        - k2_decay * genome.delta_decay
    )

    # 判断是否有预警
    if gross_margin_slope < slope_threshold:
        severity = "严重" if gross_margin_slope < slope_threshold - 0.02 else ""
        return True, f"{severity}毛利率年化下滑 {gross_margin_slope:.1%}（阈值 {slope_threshold:.1%}），存在结构性风险"

    return False, ""


# ============================================================================
# 信号生成
# ============================================================================

def determine_signal(
    passes_screen: bool,
    excess_return: float,
    genome: CompanyGenome,
    config: TruthConfig,
) -> SignalType:
    """
    根据筛选结果和基因特征确定最终信号
    """
    # 熔断检查
    if genome.is_fraud_risk:
        return SignalType.STRONG_AVOID

    if genome.is_severe_decay:
        return SignalType.AVOID

    if not passes_screen:
        if excess_return < -0.05:
            return SignalType.AVOID
        return SignalType.HOLD

    # 通过筛选，根据超额收益确定信号强度
    if excess_return > 0.10:
        return SignalType.STRONG_BUY
    elif excess_return > 0.03:
        return SignalType.BUY
    else:
        return SignalType.HOLD


def determine_grade(
    excess_return: float,
    genome: CompanyGenome,
    confidence: float,
) -> GradeLevel:
    """
    根据超额收益和基因特征确定评级
    """
    # 熔断直接F
    if genome.is_fraud_risk or genome.is_severe_decay:
        return GradeLevel.F

    # 低置信度降级
    confidence_penalty = 1 if confidence >= 0.5 else 0

    if excess_return > 0.12:
        return GradeLevel.S if confidence_penalty else GradeLevel.A
    elif excess_return > 0.08:
        return GradeLevel.A if confidence_penalty else GradeLevel.B
    elif excess_return > 0.04:
        return GradeLevel.B if confidence_penalty else GradeLevel.C
    elif excess_return > 0:
        return GradeLevel.C
    elif excess_return > -0.05:
        return GradeLevel.D
    else:
        return GradeLevel.F


# ============================================================================
# 主引擎类
# ============================================================================

class TruthEngine:
    """
    T.R.U.T.H. 系统核心引擎

    职责：
    1. 基因测序
    2. 代表性指标计算
    3. 动态阈值计算
    4. 筛选判断
    """

    def __init__(self, config: TruthConfig = None):
        self.config = config or get_default_truth_config()
        logger.info(f"TruthEngine initialized with config v{self.config.version}")

    def sequence_genome(
        self,
        ts_code: str,
        company_name: str,
        roic_series: Sequence[float],
        roe_series: Sequence[float] = None,
        revenue_growth_series: Sequence[float] = None,
        gross_margin_series: Sequence[float] = None,
        profit_growth_series: Sequence[float] = None,
        # 用于欺诈熵的单期数据（最新年度）
        net_profit: float = 0,
        operating_cashflow: float = 0,
        revenue: float = 0,
        total_assets: float = 0,
        goodwill: float = 0,
        equity: float = 0,
        receivables: float = 0,
        related_party_transactions: float = 0,
        advance_receipts: float = 0,
        industry: str = "",
        data_years: int = 5,
    ) -> CompanyGenome:
        """
        对公司进行六维基因测序

        Args:
            ts_code: 股票代码
            company_name: 公司名称
            roic_series: ROIC时间序列（从旧到新）
            roe_series: ROE时间序列
            revenue_growth_series: 营收增长率序列
            gross_margin_series: 毛利率序列
            profit_growth_series: 利润增长率序列
            net_profit: 最新年度净利润
            operating_cashflow: 最新年度经营现金流
            revenue: 最新年度营收
            total_assets: 总资产
            goodwill: 商誉
            equity: 净资产
            receivables: 应收账款
            related_party_transactions: 关联交易额
            advance_receipts: 预收账款
            industry: 行业
            data_years: 数据年数

        Returns:
            CompanyGenome 六维基因组
        """
        # 转换为numpy数组
        roic_arr = np.array(roic_series, dtype=float)
        roe_arr = np.array(roe_series or [], dtype=float)
        revenue_growth_arr = np.array(revenue_growth_series or [], dtype=float)
        gross_margin_arr = np.array(gross_margin_series or [], dtype=float)
        profit_growth_arr = np.array(profit_growth_series or [], dtype=float)

        # 计算六维基因
        alpha, alpha_breakdown = compute_alpha(roic_arr, self.config, industry)
        beta, beta_breakdown = compute_beta(roic_arr, gross_margin_arr, self.config)
        gamma, gamma_breakdown = compute_gamma(revenue_growth_arr, profit_growth_arr, self.config)

        delta_fraud, fraud_result = compute_delta_fraud(
            net_profit, operating_cashflow, revenue, total_assets,
            goodwill, equity, receivables, related_party_transactions,
            self.config
        )

        delta_decay, decay_breakdown = compute_delta_decay(roic_arr, gross_margin_arr, self.config)

        verification, v_breakdown = compute_verification(
            operating_cashflow, net_profit, advance_receipts, revenue, self.config
        )

        # 数据质量评分
        data_quality = self._assess_data_quality(roic_arr, gross_margin_arr, data_years)

        return CompanyGenome(
            ts_code=ts_code,
            company_name=company_name,
            alpha=alpha,
            beta=beta,
            gamma=gamma,
            delta_fraud=delta_fraud,
            delta_decay=delta_decay,
            verification=verification,
            roic_series=tuple(roic_arr),
            roe_series=tuple(roe_arr),
            revenue_growth_series=tuple(revenue_growth_arr),
            gross_margin_series=tuple(gross_margin_arr),
            industry=industry,
            data_years=data_years,
            data_quality_score=data_quality,
        )

    def _assess_data_quality(
        self,
        roic_series: np.ndarray,
        gross_margin_series: np.ndarray,
        data_years: int,
    ) -> float:
        """评估数据质量"""
        quality = 1.0

        # 数据年数惩罚
        if data_years < 5:
            quality *= 0.8
        elif data_years < 10:
            quality *= 0.9

        # 缺失值惩罚
        nan_ratio = np.isnan(roic_series).mean() if len(roic_series) > 0 else 0
        quality *= (1 - nan_ratio)

        # 极端值惩罚
        if len(roic_series) > 0:
            extreme_ratio = np.mean(np.abs(roic_series) > 1.0)  # ROIC > 100% 视为异常
            quality *= (1 - extreme_ratio * 0.5)

        return clip_01(quality)

    def compute_truth(
        self,
        genome: CompanyGenome,
    ) -> TruthResult:
        """
        计算单个公司的完整T.R.U.T.H.结果

        Args:
            genome: 公司基因组

        Returns:
            TruthResult 完整结果
        """
        warnings = []

        # 1. 计算代表性ROIC
        roic_arr = np.array(genome.roic_series)
        rep_roic = compute_representative_roic(roic_arr, self.config)

        # 2. 计算动态阈值（重力求解器）
        threshold = gravity_solver(genome, self.config)

        # 3. 结构检查（毛利率 - 结构求解器）
        if len(genome.gross_margin_series) >= 3:
            gm_arr = np.array(genome.gross_margin_series)
            x = np.arange(len(gm_arr))
            slope, _, _, _, _ = stats.linregress(x, gm_arr)
            has_warning, warning_msg = structure_solver(slope, genome, self.config)
            if has_warning:
                warnings.append(warning_msg)

        # 4. 熔断检查
        if genome.is_fraud_risk:
            warnings.append(f"⚠️ 欺诈熵熔断: δ_fraud={genome.delta_fraud:.2f}")

        if genome.is_severe_decay:
            warnings.append(f"⚠️ 严重衰退: δ_decay={genome.delta_decay:.2f}")

        # 5. 筛选判断
        passes_screen = (
            rep_roic.final_value >= threshold.final_threshold
            and not genome.is_fraud_risk
        )

        # 6. 超额收益
        excess_return = rep_roic.final_value - threshold.final_threshold

        # 7. 置信度
        calib_params = self.config.calibration
        if genome.data_years <= 5:
            confidence = min(genome.data_quality_score, calib_params.five_year_confidence_ceiling)
        else:
            confidence = min(genome.data_quality_score, calib_params.ten_year_confidence_ceiling)

        # 8. 信号和评级
        signal = determine_signal(passes_screen, excess_return, genome, self.config)
        grade = determine_grade(excess_return, genome, confidence)

        return TruthResult(
            ts_code=genome.ts_code,
            company_name=genome.company_name,
            genome=genome,
            rep_roic=rep_roic,
            threshold=threshold,
            passes_screen=passes_screen,
            signal=signal,
            grade=grade,
            excess_return=excess_return,
            confidence=confidence,
            warnings=warnings,
            breakdown={
                'rep_roic_ewma': rep_roic.ewma_value,
                'rep_roic_momentum': rep_roic.momentum_delta,
                'rep_roic_floor': rep_roic.floor_value,
                'threshold_theory': threshold.theory_threshold,
                'threshold_components': {
                    'base_rate': threshold.base_rate,
                    'alpha_premium': threshold.alpha_premium,
                    'beta_premium': threshold.beta_premium,
                    'growth_discount': threshold.growth_discount,
                    'verification_bonus': threshold.verification_bonus,
                },
            }
        )

    def evaluate_single(
        self,
        ts_code: str,
        company_name: str,
        roic_series: Sequence[float],
        **kwargs,
    ) -> TruthResult:
        """
        单公司完整评估（测序 + 计算）

        便捷方法，组合 sequence_genome 和 compute_truth
        """
        genome = self.sequence_genome(ts_code, company_name, roic_series, **kwargs)
        return self.compute_truth(genome)

    def batch_evaluate(
        self,
        companies_data: List[Dict[str, Any]],
    ) -> List[TruthResult]:
        """
        批量评估多家公司

        Args:
            companies_data: 公司数据列表，每个字典包含 evaluate_single 所需的参数

        Returns:
            List[TruthResult] 结果列表
        """
        results = []
        for i, data in enumerate(companies_data):
            try:
                result = self.evaluate_single(**data)
                results.append(result)
            except Exception as e:
                logger.error(f"Error evaluating company {data.get('ts_code', i)}: {e}")
                continue

        logger.info(f"Batch evaluation completed: {len(results)}/{len(companies_data)} successful")
        return results

    def evaluate_from_genome(
        self,
        genome: CompanyGenome,
        gross_margin_series: np.ndarray = None,
        industry: str = "",
    ) -> TruthResult:
        """
        从已计算的基因组评估公司

        这是与探针整合流水线配合使用的方法：
        1. 先使用 ProbeAdapter + compute_genome_from_probes 计算基因组
        2. 然后使用本方法计算阈值和最终判定

        Args:
            genome: 由 compute_genome_from_probes 计算得到的基因组
            gross_margin_series: 毛利率序列（可选，用于结构检查）
            industry: 行业名称

        Returns:
            TruthResult: 完整的分析结果
        """
        warnings = []

        # 从基因组获取 ROIC 数据
        if genome.roic_series and len(genome.roic_series) > 0:
            roic_arr = np.array(genome.roic_series)
        else:
            roic_arr = None

        # 1. 计算代表性ROIC
        if roic_arr is not None and len(roic_arr) > 0:
            rep_roic = compute_representative_roic(roic_arr, self.config)
        else:
            # 降级模式：使用基因信息估算
            estimated_roic = 0.10  # 默认10%
            if genome.gamma > 0.6:
                estimated_roic += 0.05
            if genome.delta_decay > 0.5:
                estimated_roic -= 0.03
            rep_roic = RepresentativeMetrics(
                ewma_value=estimated_roic,
                simple_mean=estimated_roic,
                latest_value=estimated_roic,
                trend_slope=0,
                momentum_delta=0,
                deterioration_delta=0,
                representative_value=estimated_roic,
                floor_value=-0.05,
                final_value=estimated_roic,
            )
            warnings.append("⚠️ 降级模式：缺少ROIC序列，使用估算值")

        # 2. 计算动态阈值（重力求解器）
        threshold = gravity_solver(genome, self.config)

        # 3. 结构检查（毛利率 - 结构求解器）
        if gross_margin_series is not None and len(gross_margin_series) >= 3:
            x = np.arange(len(gross_margin_series))
            slope, _, _, _, _ = stats.linregress(x, gross_margin_series)
            has_warning, warning_msg = structure_solver(slope, genome, self.config)
            if has_warning:
                warnings.append(warning_msg)

        # 4. 熔断检查 - 使用 is_fraud_risk 属性
        if genome.is_fraud_risk:
            warnings.append(f"⚠️ 欺诈熵熔断: δ_fraud={genome.delta_fraud:.2f}")

        # 检查严重衰退 - 使用 is_severe_decay 属性
        if genome.is_severe_decay:
            warnings.append(f"⚠️ 严重衰退: δ_decay={genome.delta_decay:.2f}")

        # 5. 筛选判断
        passes_screen = (
            rep_roic.final_value >= threshold.final_threshold
            and not genome.is_fraud_risk
        )

        # 6. 超额收益
        excess_return = rep_roic.final_value - threshold.final_threshold

        # 7. 置信度
        calib_params = self.config.calibration
        data_years = genome.data_years if genome.data_years else 5
        data_quality_score = genome.data_quality_score if hasattr(genome, 'data_quality_score') else 0.8
        if data_years <= 5:
            confidence = min(data_quality_score, calib_params.five_year_confidence_ceiling)
        else:
            confidence = min(data_quality_score, calib_params.ten_year_confidence_ceiling)

        # 8. 信号和评级
        signal = determine_signal(passes_screen, excess_return, genome, self.config)
        grade = determine_grade(excess_return, genome, confidence)

        return TruthResult(
            ts_code=genome.ts_code,
            company_name=genome.company_name,
            genome=genome,
            rep_roic=rep_roic,
            threshold=threshold,
            passes_screen=passes_screen,
            signal=signal,
            grade=grade,
            excess_return=excess_return,
            confidence=confidence,
            warnings=warnings,
            breakdown={
                'rep_roic_ewma': rep_roic.ewma_value,
                'rep_roic_momentum': rep_roic.momentum_delta,
                'rep_roic_floor': rep_roic.floor_value,
                'threshold_theory': threshold.theory_threshold,
                'threshold_components': {
                    'base_rate': threshold.base_rate,
                    'alpha_premium': threshold.alpha_premium,
                    'beta_premium': threshold.beta_premium,
                    'growth_discount': threshold.growth_discount,
                    'verification_bonus': threshold.verification_bonus,
                },
                'from_probe_integration': True,
            }
        )
