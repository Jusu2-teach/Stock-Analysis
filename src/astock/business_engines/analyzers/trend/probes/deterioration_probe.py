"""
恶化检测器 (Deterioration Detector)
===================================

检测指标近期恶化趋势，用于识别基本面拐点风险。

专业性增强 v2.0：
1. 连续恶化年数统计
2. 恶化加速度检测（越跌越快）
3. 恶化模式分类（5种模式）
4. 贝叶斯恶化概率：综合多因素量化恶化置信度
5. 高位回调豁免

作者: AStock Analysis System
日期: 2025-01-07
"""

import logging
from typing import List, Tuple
import numpy as np

from ..models import RecentDeteriorationResult, TrendWarning
from ..config import get_default_config, get_decline_thresholds
from .common import DataQualityChecker

logger = logging.getLogger(__name__)


# ============================================================================
# 贝叶斯恶化概率计算
# ============================================================================

def calculate_deterioration_probability(
    consecutive_years: int,
    acceleration: float,
    recent_change_pct: float,
    total_change_pct: float,
    is_below_threshold: bool,
) -> Tuple[float, List[Tuple[str, float]]]:
    """
    贝叶斯恶化概率计算

    综合多个恶化信号，使用贝叶斯更新计算后验概率。
    这比简单的规则分类更能量化恶化的"置信度"。

    先验概率 P(恶化) = 0.3（基线假设30%的公司有恶化迹象）

    似然比计算：
    - 连续下跌年数: P(连续n年|恶化) / P(连续n年|正常)
    - 加速度: P(加速|恶化) / P(加速|正常)
    - 跌幅: P(跌幅x%|恶化) / P(跌幅x%|正常)

    Returns:
        (posterior_probability, evidence_list)
    """
    prior = 0.3  # 先验概率
    evidence_factors: List[Tuple[str, float]] = []

    # 1. 连续下跌年数的似然比
    # 1年: LR=1.2, 2年: LR=2.5, 3年: LR=5.0, 4+年: LR=10.0
    if consecutive_years == 0:
        lr_consecutive = 0.5  # 没有连续下跌，是恶化的概率降低
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
    # 负加速度（放缓）: LR=0.6
    # 零加速度: LR=1.0
    # 正加速度（加速恶化）: LR 与加速度成正比
    if acceleration < -0.2:
        lr_acceleration = 0.5  # 恶化放缓
    elif acceleration < 0.1:
        lr_acceleration = 1.0  # 匀速
    elif acceleration < 0.5:
        lr_acceleration = 2.0  # 轻微加速
    else:
        lr_acceleration = min(3.0 + acceleration * 2, 8.0)  # 显著加速
    evidence_factors.append(("恶化加速度", lr_acceleration))

    # 3. 近期跌幅的似然比
    if recent_change_pct > 0:
        lr_recent = 0.4  # 近期上涨，不支持恶化假设
    elif recent_change_pct > -5:
        lr_recent = 1.0  # 轻微下跌
    elif recent_change_pct > -15:
        lr_recent = 2.0  # 中等下跌
    elif recent_change_pct > -30:
        lr_recent = 4.0  # 显著下跌
    else:
        lr_recent = 8.0  # 暴跌
    evidence_factors.append(("近期变化", lr_recent))

    # 4. 总跌幅的似然比
    if total_change_pct > 10:
        lr_total = 0.3  # 实际是上涨
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
        lr_threshold = 3.0  # 低于阈值是强恶化信号
    else:
        lr_threshold = 0.8
    evidence_factors.append(("低于阈值", lr_threshold))

    # 贝叶斯更新：P(恶化|证据) = P(证据|恶化) * P(恶化) / P(证据)
    # 使用似然比的乘积 - 使用对数空间计算防止溢出
    log_combined_lr = (
        np.log(lr_consecutive) +
        np.log(lr_acceleration) +
        np.log(lr_recent) +
        np.log(lr_total) +
        np.log(lr_threshold)
    )
    # 限制在合理范围内防止溢出
    log_combined_lr = np.clip(log_combined_lr, -20, 20)
    combined_lr = np.exp(log_combined_lr)

    # 后验odds = prior_odds * combined_lr
    prior_odds = prior / (1 - prior)
    posterior_odds = prior_odds * combined_lr

    # 转换回概率 - 防止数值不稳定
    if posterior_odds > 1e10:
        posterior_prob = 1.0
    elif posterior_odds < 1e-10:
        posterior_prob = 0.0
    else:
        posterior_prob = posterior_odds / (1 + posterior_odds)
    posterior_prob = max(0.0, min(1.0, posterior_prob))

    return float(posterior_prob), evidence_factors


class DeteriorationDetector:
    """
    增强版恶化检测器

    新增功能：
    - consecutive_decline_years: 连续下跌年数
    - deterioration_acceleration: 恶化加速度（正值=加速恶化）
    - deterioration_pattern: 恶化模式分类
    """

    def detect(self, values: List[float], industry: str = None) -> RecentDeteriorationResult:
        config = get_default_config()
        checker = DataQualityChecker(config)
        values_array = checker.ensure_window(values)
        n = len(values_array)

        # 取最近3年数据 (使用负索引，支持任意年份数据)
        # year_n_2 = 倒数第3年, year_n_1 = 倒数第2年, year_n = 最新年
        year_n_2, year_n_1, year_n = values_array[-3], values_array[-2], values_array[-1]

        # 保持向后兼容的变量名
        year3, year4, year5 = year_n_2, year_n_1, year_n

        def pct_change(current: float, previous: float) -> float:
            denominator = max(abs(previous), config.mean_near_zero_eps)
            return ((current - previous) / denominator) * 100.0

        # 计算年度变化
        change_3_to_4 = year4 - year3
        change_3_to_4_pct = pct_change(year4, year3)
        change_4_to_5 = year5 - year4
        change_4_to_5_pct = pct_change(year5, year4)

        # 获取行业阈值
        if industry:
            try:
                thresholds = get_decline_thresholds(industry)
                DECLINE_THRESHOLD_PCT = thresholds["decline_threshold_pct"]
                DECLINE_THRESHOLD_ABS = thresholds["decline_threshold_abs"]
                high_level_threshold = thresholds["high_level_threshold"]
            except Exception as e:
                logger.warning(f"Failed to get industry thresholds ({industry}): {e}, using defaults")
                DECLINE_THRESHOLD_PCT = -5.0
                DECLINE_THRESHOLD_ABS = -2.0
                high_level_threshold = 20.0
        else:
            DECLINE_THRESHOLD_PCT = -5.0
            DECLINE_THRESHOLD_ABS = -2.0
            high_level_threshold = 20.0

        # ========== 新增：计算连续下跌年数 ==========
        consecutive_decline_years = self._count_consecutive_declines(
            values_array, DECLINE_THRESHOLD_PCT, config.mean_near_zero_eps
        )

        # ========== 新增：计算恶化加速度 ==========
        deterioration_acceleration = self._calculate_acceleration(
            change_3_to_4_pct, change_4_to_5_pct
        )

        # 判断是否有有意义的下跌
        is_meaningful_decline_3_to_4 = (change_3_to_4_pct < DECLINE_THRESHOLD_PCT) or (
            change_3_to_4 < DECLINE_THRESHOLD_ABS
        )

        is_meaningful_decline_4_to_5 = (change_4_to_5_pct < DECLINE_THRESHOLD_PCT) or (
            change_4_to_5 < DECLINE_THRESHOLD_ABS
        )

        has_deterioration = False
        severity = "none"
        deterioration_pattern = "none"

        # ========== 增强的严重程度判断 ==========
        if is_meaningful_decline_4_to_5 and is_meaningful_decline_3_to_4:
            has_deterioration = True
            # 连续2年下跌 + 加速恶化 = 非常严重
            if deterioration_acceleration > 0.5:
                severity = "severe"
                deterioration_pattern = "accelerating_decline"  # 加速下滑
            elif consecutive_decline_years >= 3:
                severity = "severe"
                deterioration_pattern = "chronic_decline"  # 慢性衰退
            else:
                severity = "severe"
                deterioration_pattern = "sustained_decline"  # 持续下跌

        elif is_meaningful_decline_4_to_5:
            # 只有最近1年下跌
            if year5 < high_level_threshold:
                has_deterioration = True
                # 检查是否是断崖式下跌（单年跌幅超过30%）
                if change_4_to_5_pct < -30:
                    severity = "severe"
                    deterioration_pattern = "cliff_drop"  # 断崖式下跌
                else:
                    severity = "moderate"
                    deterioration_pattern = "single_year_drop"
            else:
                # 高位小幅回调
                has_deterioration = True
                severity = "mild"
                deterioration_pattern = "high_level_pullback"  # 高位回调

        elif consecutive_decline_years >= 3:
            # 虽然近2年单独看不算严重，但连续3+年阴跌
            has_deterioration = True
            severity = "moderate"
            deterioration_pattern = "grinding_decline"  # 阴跌

        total_decline_pct = pct_change(year5, year3)
        is_high_level_stable = (year5 > high_level_threshold) and (abs(total_decline_pct) < 10.0)

        # ========== 新增：高位回调豁免 ==========
        # 如果绝对值仍高于门槛的1.5倍，即使在下跌也可能只是正常回调
        if year5 > high_level_threshold * 1.5 and severity in ("moderate", "mild"):
            deterioration_pattern = "high_level_pullback"
            # 可以考虑降低severity，但保留信息

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

        # 特殊警告：加速恶化
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

        # ========== 新增：贝叶斯恶化概率 ==========
        is_below_threshold = year5 < high_level_threshold
        deterioration_probability, evidence_factors = calculate_deterioration_probability(
            consecutive_years=consecutive_decline_years,
            acceleration=deterioration_acceleration,
            recent_change_pct=change_4_to_5_pct,
            total_change_pct=total_decline_pct,
            is_below_threshold=is_below_threshold,
        )

        # 贝叶斯概率警告（高置信度恶化）
        if deterioration_probability > 0.7:
            # 构建证据说明
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
            decline_threshold_pct=float(DECLINE_THRESHOLD_PCT),
            decline_threshold_abs=float(DECLINE_THRESHOLD_ABS),
            industry=industry or "default",
            warnings=warnings,
            # 新增专业字段
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
        """
        计算连续下跌年数（从最近一年往回数）

        Args:
            values: 5年数据
            threshold_pct: 下跌阈值（百分比）
            eps: 防止除零的小数

        Returns:
            连续下跌年数
        """
        consecutive = 0

        # 从最近一年（index 4）往回数
        for i in range(len(values) - 1, 0, -1):
            current = values[i]
            previous = values[i - 1]

            denominator = max(abs(previous), eps)
            pct_change = ((current - previous) / denominator) * 100.0

            if pct_change < threshold_pct:
                consecutive += 1
            else:
                break  # 一旦有一年不下跌，停止计数

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

        Returns:
            加速度（无量纲，0表示匀速，1表示加速1倍）
        """
        # 如果上期是下跌，这期跌得更狠 = 加速
        # 如果上期是下跌，这期跌幅收窄 = 减速

        # 只在两期都是下跌时计算加速度
        if change_3_to_4_pct >= 0 or change_4_to_5_pct >= 0:
            return 0.0

        # 下跌幅度比较（都是负数，绝对值越大跌得越狠）
        # 例：上期-10%，本期-20% → 加速 = (-20 - -10) / |-10| = -1.0 (加速1倍)
        # 例：上期-20%，本期-10% → 加速 = (-10 - -20) / |-20| = 0.5 (减速50%)

        base = max(abs(change_3_to_4_pct), 1.0)  # 防止除零
        acceleration = (change_4_to_5_pct - change_3_to_4_pct) / base

        # 取反让正数表示恶化加速
        return float(-acceleration)
