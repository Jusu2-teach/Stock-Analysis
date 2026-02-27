"""
恶化检测器 (Deterioration Detector)
===================================

检测指标近期恶化趋势，用于识别基本面拐点风险。

专业性增强 v3.0：
1. 连续恶化年数统计
2. 恶化加速度检测（越跌越快）
3. 恶化模式分类（5种模式）
4. 贝叶斯恶化概率：综合多因素量化恶化置信度
5. 高位回调豁免

⚠️ 设计原则 (v3.0):
==================
此探针是 **纯数学工具**，不包含任何业务逻辑：
- ✅ 计算恶化概率、连续下跌年数、加速度
- ✅ 所有阈值由调用方传入
- ❌ 不调用 get_decline_thresholds()
- ❌ 不知道什么是"周期性行业"

调用方通过参数控制：
- decline_threshold_pct: 下跌百分比阈值 (默认 -5.0)
- decline_threshold_abs: 下跌绝对值阈值 (默认 -2.0)
- high_level_threshold: 高位阈值 (默认 20.0)
- prior_probability: 贝叶斯先验 (默认 0.3)

作者: AStock Analysis System
日期: 2025-01-07
版本: 3.0 (Pure Math Edition)
"""

import logging
from typing import List, Tuple
import numpy as np

from ..models import RecentDeteriorationResult, TrendWarning
from ..config import get_default_config
from .common import DataQualityChecker

logger = logging.getLogger(__name__)


# ============================================================================
# 默认阈值 (纯统计学标准，可被调用方覆盖)
# ============================================================================

DEFAULT_DETERIORATION_THRESHOLDS = {
    'decline_threshold_pct': -5.0,    # 下跌百分比阈值
    'decline_threshold_abs': -2.0,    # 下跌绝对值阈值
    'high_level_threshold': 20.0,     # 高位阈值
    'prior_probability': 0.3,         # 贝叶斯先验
}


# ============================================================================
# 贝叶斯恶化概率计算 (纯数学)
# ============================================================================

def calculate_deterioration_probability(
    consecutive_years: int,
    acceleration: float,
    recent_change_pct: float,
    total_change_pct: float,
    is_below_threshold: bool,
    prior: float = 0.3,
) -> Tuple[float, List[Tuple[str, float]]]:
    """
    贝叶斯恶化概率计算

    综合多个恶化信号，使用贝叶斯更新计算后验概率。

    Args:
        consecutive_years: 连续下跌年数
        acceleration: 恶化加速度
        recent_change_pct: 近期变化百分比
        total_change_pct: 累计变化百分比
        is_below_threshold: 是否低于高位阈值
        prior: 先验概率 (由调用方根据业务知识设置)

    Returns:
        (posterior_probability, evidence_list)
    """
    prior_prob = max(0.05, min(0.8, prior))
    evidence_factors: List[Tuple[str, float]] = []

    # 1. 连续下跌年数的似然比
    if consecutive_years == 0:
        lr_consecutive = 0.5
    elif consecutive_years == 1:
        lr_consecutive = 1.2
    elif consecutive_years == 2:
        lr_consecutive = 2.5
    elif consecutive_years == 3:
        lr_consecutive = 5.0
    else:
        lr_consecutive = 10.0
    evidence_factors.append(("连续下跌年数", lr_consecutive))

    # 2. 恶化加速度的似然比
    if acceleration < -0.2:
        lr_acceleration = 0.5
    elif acceleration < 0.1:
        lr_acceleration = 1.0
    elif acceleration < 0.5:
        lr_acceleration = 2.0
    else:
        lr_acceleration = min(3.0 + acceleration * 2, 8.0)
    evidence_factors.append(("恶化加速度", lr_acceleration))

    # 3. 近期跌幅的似然比
    if recent_change_pct > 0:
        lr_recent = 0.4
    elif recent_change_pct > -5:
        lr_recent = 1.0
    elif recent_change_pct > -15:
        lr_recent = 2.0
    elif recent_change_pct > -30:
        lr_recent = 4.0
    else:
        lr_recent = 8.0
    evidence_factors.append(("近期变化", lr_recent))

    # 4. 总跌幅的似然比
    if total_change_pct > 10:
        lr_total = 0.3
    elif total_change_pct > 0:
        lr_total = 0.6
    elif total_change_pct > -15:
        lr_total = 1.5
    elif total_change_pct > -30:
        lr_total = 3.0
    else:
        lr_total = 6.0
    evidence_factors.append(("累计变化", lr_total))

    # 5. 是否低于阈值的似然比
    if is_below_threshold:
        lr_threshold = 3.0
    else:
        lr_threshold = 0.8
    evidence_factors.append(("低于阈值", lr_threshold))

    # 贝叶斯更新
    log_combined_lr = (
        np.log(lr_consecutive) +
        np.log(lr_acceleration) +
        np.log(lr_recent) +
        np.log(lr_total) +
        np.log(lr_threshold)
    )
    log_combined_lr = np.clip(log_combined_lr, -20, 20)
    combined_lr = np.exp(log_combined_lr)

    prior_odds = prior_prob / (1 - prior_prob)
    posterior_odds = prior_odds * combined_lr

    if posterior_odds > 1e10:
        posterior_probability = 1.0
    elif posterior_odds < 1e-10:
        posterior_probability = 0.0
    else:
        posterior_probability = posterior_odds / (1 + posterior_odds)
    posterior_probability = max(0.0, min(1.0, posterior_probability))

    return float(posterior_probability), evidence_factors


class DeteriorationProbe:
    """
    增强版恶化检测探针 (纯数学版)

    Unified interface following ProbeProtocol:
    - compute(values, **kwargs) -> RecentDeteriorationResult
    - default() -> RecentDeteriorationResult

    v3.0 变更：
    - 移除所有 get_decline_thresholds 调用
    - 所有阈值由调用方传入
    """

    def compute(
        self,
        values: List[float],
        decline_threshold_pct: float = -5.0,
        decline_threshold_abs: float = -2.0,
        high_level_threshold: float = 20.0,
        prior_probability: float = 0.3,
        industry: str = None,
    ) -> RecentDeteriorationResult:
        """
        检测恶化趋势

        Args:
            values: 数值序列 (至少3个数据点)
            decline_threshold_pct: 下跌百分比阈值，低于此值视为有意义下跌
            decline_threshold_abs: 下跌绝对值阈值
            high_level_threshold: 高位阈值，用于判断高位回调豁免
            prior_probability: 贝叶斯先验概率
            industry: 行业名称（仅用于报告，不影响计算）

        Returns:
            RecentDeteriorationResult 检测结果
        """
        config = get_default_config()
        checker = DataQualityChecker(config)
        values_array = checker.ensure_window(values)
        len(values_array)

        # 取最近3年数据
        year_n_2, year_n_1, year_n = values_array[-3], values_array[-2], values_array[-1]
        year3, year4, year5 = year_n_2, year_n_1, year_n

        def pct_change(current: float, previous: float) -> float:
            denominator = max(abs(previous), config.mean_near_zero_eps)
            return ((current - previous) / denominator) * 100.0

        # 计算年度变化
        change_3_to_4 = year4 - year3
        change_3_to_4_pct = pct_change(year4, year3)
        change_4_to_5 = year5 - year4
        change_4_to_5_pct = pct_change(year5, year4)

        # ========== 计算连续下跌年数 ==========
        consecutive_decline_years = self._count_consecutive_declines(
            values_array, decline_threshold_pct, config.mean_near_zero_eps
        )

        # ========== 计算恶化加速度 ==========
        deterioration_acceleration = self._calculate_acceleration(
            change_3_to_4_pct, change_4_to_5_pct
        )

        # 判断是否有有意义的下跌
        is_meaningful_decline_3_to_4 = (change_3_to_4_pct < decline_threshold_pct) or (
            change_3_to_4 < decline_threshold_abs
        )

        is_meaningful_decline_4_to_5 = (change_4_to_5_pct < decline_threshold_pct) or (
            change_4_to_5 < decline_threshold_abs
        )

        has_deterioration = False
        severity = "none"
        deterioration_pattern = "none"

        # ========== 严重程度判断 ==========
        if is_meaningful_decline_4_to_5 and is_meaningful_decline_3_to_4:
            has_deterioration = True
            if deterioration_acceleration > 0.5:
                severity = "severe"
                deterioration_pattern = "accelerating_decline"
            elif consecutive_decline_years >= 3:
                severity = "severe"
                deterioration_pattern = "chronic_decline"
            else:
                severity = "moderate"
                deterioration_pattern = "sustained_decline"

        elif is_meaningful_decline_4_to_5:
            if year5 < high_level_threshold:
                has_deterioration = True
                if change_4_to_5_pct < -30:
                    severity = "severe"
                    deterioration_pattern = "cliff_drop"
                else:
                    severity = "moderate"
                    deterioration_pattern = "single_year_drop"
            else:
                has_deterioration = True
                severity = "mild"
                deterioration_pattern = "high_level_pullback"

        elif consecutive_decline_years >= 3:
            has_deterioration = True
            severity = "moderate"
            deterioration_pattern = "grinding_decline"

        total_decline_pct = pct_change(year5, year3)
        is_high_level_stable = (year5 > high_level_threshold) and (abs(total_decline_pct) < 10.0)

        # 高位回调豁免
        if year5 > high_level_threshold * 1.5 and severity in ("moderate", "mild"):
            deterioration_pattern = "high_level_pullback"

        # 构建警告信息
        warnings: List[TrendWarning] = []
        if has_deterioration:
            level = "warn" if severity == "severe" else "info"
            msg = f"检测到近期恶化: {severity} ({deterioration_pattern})"
            if consecutive_decline_years >= 2:
                msg += f", 连续下跌{consecutive_decline_years}年"
            if deterioration_acceleration > 0.3:
                msg += f", 恶化加速中"

            warnings.append(
                TrendWarning(
                    code="DETERIORATION_DETECTED",
                    level=level,
                    message=msg,
                    context={
                        "severity": severity,
                        "pattern": deterioration_pattern,
                        "change_4_to_5_pct": float(change_4_to_5_pct),
                        "consecutive_decline_years": consecutive_decline_years,
                        "deterioration_acceleration": float(deterioration_acceleration),
                    },
                )
            )

        # 加速恶化警告
        if deterioration_acceleration > 0.5 and has_deterioration:
            warnings.append(
                TrendWarning(
                    code="ACCELERATING_DETERIORATION",
                    level="warn",
                    message=f"恶化加速预警: 下跌速度比上期加快{deterioration_acceleration:.1%}",
                    context={
                        "acceleration": float(deterioration_acceleration),
                        "year3_to_4_pct": float(change_3_to_4_pct),
                        "year4_to_5_pct": float(change_4_to_5_pct),
                    },
                )
            )

        # ========== 贝叶斯恶化概率 ==========
        is_below_threshold = year5 < high_level_threshold
        deterioration_probability, evidence_factors = calculate_deterioration_probability(
            consecutive_years=consecutive_decline_years,
            acceleration=deterioration_acceleration,
            recent_change_pct=change_4_to_5_pct,
            total_change_pct=total_decline_pct,
            is_below_threshold=is_below_threshold,
            prior=prior_probability,
        )

        # 高概率恶化警告
        if deterioration_probability > 0.7:
            top_evidence = sorted(evidence_factors, key=lambda x: x[1], reverse=True)[:3]
            evidence_desc = ", ".join([f"{e[0]}(LR={e[1]:.1f})" for e in top_evidence])

            warnings.append(
                TrendWarning(
                    code="HIGH_DETERIORATION_PROBABILITY",
                    level="warn" if deterioration_probability > 0.85 else "info",
                    message=f"贝叶斯恶化概率={deterioration_probability:.1%}，主要证据: {evidence_desc}",
                    context={
                        "probability": float(deterioration_probability),
                        "evidence_factors": {e[0]: e[1] for e in evidence_factors},
                    },
                )
            )

        return RecentDeteriorationResult(
            has_deterioration=bool(has_deterioration),
            severity=severity,
            year4_to_5_change=float(change_4_to_5),
            year3_to_4_change=float(change_3_to_4),
            year4_to_5_pct=float(change_4_to_5_pct),
            year3_to_4_pct=float(change_3_to_4_pct),
            total_decline_pct=float(total_decline_pct),
            is_high_level_stable=bool(is_high_level_stable),
            decline_threshold_pct=float(decline_threshold_pct),
            decline_threshold_abs=float(decline_threshold_abs),
            industry=industry or "unknown",
            warnings=warnings,
            consecutive_decline_years=int(consecutive_decline_years),
            deterioration_acceleration=float(deterioration_acceleration),
            deterioration_pattern=deterioration_pattern,
            deterioration_probability=float(deterioration_probability),
        )

    def _count_consecutive_declines(
        self,
        values: np.ndarray,
        threshold_pct: float,
        eps: float
    ) -> int:
        """计算连续下跌年数（从最近一年往回数）"""
        consecutive = 0

        for i in range(len(values) - 1, 0, -1):
            current = values[i]
            previous = values[i - 1]

            denominator = max(abs(previous), eps)
            pct_change = ((current - previous) / denominator) * 100.0

            if pct_change < threshold_pct:
                consecutive += 1
            else:
                break

        return consecutive

    def _calculate_acceleration(
        self,
        change_3_to_4_pct: float,
        change_4_to_5_pct: float
    ) -> float:
        """
        计算恶化加速度

        正值 = 恶化加速（越跌越快）
        负值 = 恶化减速（跌势放缓）
        """
        # 场景A: 两期都下跌 - 持续恶化
        if change_3_to_4_pct < 0 and change_4_to_5_pct < 0:
            base = max(abs(change_3_to_4_pct), 1.0)
            acceleration = (abs(change_4_to_5_pct) - abs(change_3_to_4_pct)) / base
            return float(acceleration)

        # 场景B: 由涨转跌 - 急转直下
        if change_3_to_4_pct >= 0 and change_4_to_5_pct < 0:
            base = max(abs(change_3_to_4_pct), 1.0)
            reversal_magnitude = abs(change_4_to_5_pct) / base
            bonus = min(change_3_to_4_pct / 10.0, 0.5) if change_3_to_4_pct > 0 else 0
            return float(reversal_magnitude + bonus)

        # 场景C: 由跌转涨 - 触底反弹
        if change_3_to_4_pct < 0 and change_4_to_5_pct >= 0:
            base = max(abs(change_3_to_4_pct), 1.0)
            recovery = -abs(change_4_to_5_pct + abs(change_3_to_4_pct)) / base
            return float(min(recovery, -0.5))

        # 场景D: 两期都上涨 - 无恶化
        return 0.0

    def default(self) -> RecentDeteriorationResult:
        """Return default result for insufficient data (ProbeProtocol compliance)."""
        return RecentDeteriorationResult(
            has_deterioration=False,
            severity="none",
            year4_to_5_change=0.0,
            year3_to_4_change=0.0,
            year4_to_5_pct=0.0,
            year3_to_4_pct=0.0,
            total_decline_pct=0.0,
            is_high_level_stable=False,
            decline_threshold_pct=-15.0,
            decline_threshold_abs=-2.0,
            industry="unknown",
            warnings=[TrendWarning(
                code="INSUFFICIENT_DATA",
                level="warning",
                message="Insufficient data",
            )],
            consecutive_decline_years=0,
            deterioration_acceleration=0.0,
            deterioration_pattern="none",
            deterioration_probability=0.0,
        )