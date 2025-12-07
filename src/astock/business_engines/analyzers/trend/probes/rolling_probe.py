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
from typing import List, Tuple

from ..models import RollingTrendResult, TrendWarning
from ..config import get_default_config
from .common import DataQualityChecker
from .fast_stats import fast_linregress_no_pvalue

logger = logging.getLogger(__name__)

class RollingTrendCalculator:
    """Rolling trend calculator with enhanced acceleration detection."""

    def _compute_log_slope(self, values: np.ndarray) -> Tuple[float, float]:
        """计算对数变换后的斜率和R²（与LogTrendProbe一致）"""
        if len(values) < 2:
            return 0.0, 0.0
        try:
            x = np.arange(len(values), dtype=np.float64)
            y = np.arcsinh(values)  # 使用arcsinh处理负值
            slope, _, _, r_squared = fast_linregress_no_pvalue(x, y)
            return float(slope), float(r_squared)
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

        # 4. 加速度计算 (修复版)
        # 改进：R²作为置信度标签，而非乘数
        # 原始加速度 = 近期斜率 - 早期斜率
        raw_acceleration = recent_3y_slope - early_3y_slope

        # 置信度：两段趋势的最小 R²
        # 不再用于乘以加速度，而是单独记录用于判断
        acceleration_confidence = min(recent_3y_r_squared, early_3y_r_squared)

        # 保留原始加速度值，不再压缩
        trend_acceleration = raw_acceleration

        # 5. 判断阈值 (考虑数据量级)
        # 动态阈值：基于全样本斜率的20%作为显著变化
        threshold = max(abs(full_5y_slope) * 0.2, 0.05)  # 最低0.05防止除零

        # 只有当置信度足够 (>0.3) 时才确认加速/减速
        is_accelerating = trend_acceleration > threshold and acceleration_confidence > 0.3
        is_decelerating = trend_acceleration < -threshold and acceleration_confidence > 0.3

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
            acceleration_confidence=acceleration_confidence,
            is_accelerating=is_accelerating,
            is_decelerating=is_decelerating,
            early_3y_slope=early_3y_slope,
            early_3y_r_squared=early_3y_r_squared,
            warnings=warnings,
        )
