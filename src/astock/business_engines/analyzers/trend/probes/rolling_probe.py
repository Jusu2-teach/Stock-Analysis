"""
滚动趋势探针 (Rolling Trend Probe)
=================================

计算多窗口趋势指标，用于检测趋势加速/减速。

专业改进点：
1. 使用对数变换后的斜率（与主趋势探针一致）
2. 加速度计算考虑置信度权重
3. 引入早期斜率用于更精确的拐点判断
"""

import logging
import numpy as np
from scipy import stats
from typing import List, Tuple

from ..models import RollingTrendResult, TrendWarning
from ..config import get_default_config
from .common import DataQualityChecker

logger = logging.getLogger(__name__)

class RollingTrendCalculator:
    """Rolling trend calculator with enhanced acceleration detection."""

    def _compute_log_slope(self, values: np.ndarray) -> Tuple[float, float]:
        """计算对数变换后的斜率和R²（与LogTrendProbe一致）"""
        if len(values) < 2:
            return 0.0, 0.0
        try:
            x = np.arange(len(values))
            y = np.arcsinh(values)  # 使用arcsinh处理负值
            slope, _, r_value, _, _ = stats.linregress(x, y)
            return float(slope), float(r_value ** 2)
        except (ValueError, RuntimeWarning):
            return 0.0, 0.0

    def calculate(self, values: List[float]) -> RollingTrendResult:
        config = get_default_config()
        checker = DataQualityChecker(config)
        values_array = checker.ensure_window(values)

        # 1. 全窗口趋势 (5年)
        full_5y_slope, full_5y_r_squared = self._compute_log_slope(values_array)

        # 2. 近期趋势 (后3年)
        recent_3y_slope = 0.0
        recent_3y_r_squared = 0.0
        if len(values_array) >= 3:
            recent_3y_slope, recent_3y_r_squared = self._compute_log_slope(values_array[-3:])
        else:
            recent_3y_slope = full_5y_slope
            recent_3y_r_squared = full_5y_r_squared

        # 3. 早期趋势 (前3年) - 用于拐点检测
        early_3y_slope = 0.0
        early_3y_r_squared = 0.0
        if len(values_array) >= 3:
            early_3y_slope, early_3y_r_squared = self._compute_log_slope(values_array[:3])

        # 4. 加速度计算 (专业改进版)
        # 传统方法: acceleration = recent_slope - full_slope
        # 改进方法: 使用早期vs近期的对比，并加权R²置信度
        raw_acceleration = recent_3y_slope - early_3y_slope

        # 置信度加权：只有当两段趋势都可信时，加速度才可信
        confidence = min(recent_3y_r_squared, early_3y_r_squared)
        trend_acceleration = raw_acceleration * confidence  # 低置信度时压缩加速度

        # 5. 判断阈值 (考虑数据量级)
        # 动态阈值：基于全样本斜率的20%作为显著变化
        threshold = max(abs(full_5y_slope) * 0.2, 0.05)  # 最低0.05防止除零

        is_accelerating = trend_acceleration > threshold and recent_3y_r_squared > 0.3
        is_decelerating = trend_acceleration < -threshold and recent_3y_r_squared > 0.3

        warnings: List[TrendWarning] = []
        if is_accelerating:
            warnings.append(
                TrendWarning(
                    code="TREND_ACCELERATING",
                    level="info",
                    message="Trend accelerating",
                    context={"acceleration": float(trend_acceleration)},
                )
            )
        elif is_decelerating:
            warnings.append(
                TrendWarning(
                    code="TREND_DECELERATING",
                    level="info",
                    message="Trend decelerating",
                    context={"acceleration": float(trend_acceleration)},
                )
            )

        return RollingTrendResult(
            recent_3y_slope=recent_3y_slope,
            recent_3y_r_squared=recent_3y_r_squared,
            full_5y_slope=full_5y_slope,
            full_5y_r_squared=full_5y_r_squared,
            trend_acceleration=trend_acceleration,
            is_accelerating=is_accelerating,
            is_decelerating=is_decelerating,
            warnings=warnings,
        )
