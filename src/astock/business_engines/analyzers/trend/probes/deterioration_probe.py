"""
恶化检测器 (Deterioration Detector)
===================================

检测指标近期恶化趋势，用于识别基本面拐点风险。

增强功能：
1. 连续恶化年数统计
2. 恶化加速度检测（越跌越快）
3. 恶化模式分类（持续恶化 vs 单次下跌）
4. 高位回调豁免

作者: AStock Analysis System
日期: 2025-12-06
"""

import logging
from typing import List, Tuple
import numpy as np

from ..models import RecentDeteriorationResult, TrendWarning
from ..config import get_default_config, get_decline_thresholds
from .common import DataQualityChecker

logger = logging.getLogger(__name__)


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
            # 新增字段（需要在models.py中添加，暂时放在warnings的context中）
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
