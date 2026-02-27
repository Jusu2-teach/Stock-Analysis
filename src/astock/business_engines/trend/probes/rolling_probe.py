"""
滚动趋势探针 (Rolling Trend Probe)
=================================

计算多窗口趋势指标，用于检测趋势加速/减速。

专业改进点 v3.0：
1. 使用对数变换后的斜率（与主趋势探针一致）
2. 加速度计算考虑置信度权重
3. 引入早期斜率用于更精确的拐点判断
4. 阈值由调用方传入

⚠️ 设计原则 (v3.0):
==================
此探针是 **纯数学工具**，不包含任何业务逻辑：
- ✅ 计算滚动斜率、加速度
- ✅ 所有阈值由调用方传入
- ❌ 不包含 INDICATOR_THRESHOLDS 字典
- ❌ 不知道 'roe' 或 'gross_margin'

调用方通过参数控制：
- acceleration_threshold: 加速度阈值 (默认 0.05)

作者: AStock Analysis System
日期: 2025-01-07
版本: 3.0 (Pure Math Edition)
"""

import logging
import numpy as np
from typing import List

from ..models import RollingTrendResult, TrendWarning
from ..config import get_default_config
from .common import DataQualityChecker
from .fast_stats import fast_linregress_no_pvalue

logger = logging.getLogger(__name__)


# ============================================================================
# 默认阈值 (纯统计学标准)
# ============================================================================

DEFAULT_ACCELERATION_THRESHOLD = 0.05


class RollingProbe:
    """
    滚动趋势探针 (纯数学版)

    Unified interface following ProbeProtocol:
    - compute(values, **kwargs) -> RollingTrendResult
    - default() -> RollingTrendResult

    v3.0 变更：
    - 移除 INDICATOR_THRESHOLDS 字典
    - 阈值由调用方传入
    """

    def _compute_log_slope(self, values: np.ndarray) -> tuple[float, float]:
        """计算对数变换后的斜率和R²"""
        if len(values) < 2:
            return 0.0, 0.0
        try:
            x = np.arange(len(values), dtype=np.float64)
            y = np.arcsinh(values)  # 使用arcsinh处理负值
            slope, _, _, r_squared = fast_linregress_no_pvalue(x, y)
            return float(slope), float(r_squared)
        except (ValueError, RuntimeWarning):
            return 0.0, 0.0

    def compute(
        self,
        values: List[float],
        acceleration_threshold: float = DEFAULT_ACCELERATION_THRESHOLD,
    ) -> RollingTrendResult:
        """
        计算滚动趋势指标

        Args:
            values: 数值序列
            acceleration_threshold: 加速度阈值，用于判断是否加速/减速。
                                   由调用方根据指标类型设置：
                                   - 盈利类指标(ROE, ROIC): 建议 0.08
                                   - 稳定类指标(毛利率): 建议 0.04
                                   - 增长类指标: 建议 0.12
                                   - 默认: 0.05

        Returns:
            RollingTrendResult 滚动趋势分析结果
        """
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

        # 4. 加速度计算
        raw_acceleration = recent_3y_slope - early_3y_slope
        acceleration_confidence = min(recent_3y_r_squared, early_3y_r_squared)
        trend_acceleration = raw_acceleration

        # 5. 使用传入的阈值判断加速/减速
        # 动态调整：基于全样本斜率的20%，但不低于传入阈值
        dynamic_threshold = abs(full_5y_slope) * 0.2
        effective_threshold = max(dynamic_threshold, acceleration_threshold)

        # 只有当置信度足够 (>0.3) 时才确认加速/减速
        is_accelerating = trend_acceleration > effective_threshold and acceleration_confidence > 0.3
        is_decelerating = trend_acceleration < -effective_threshold and acceleration_confidence > 0.3

        warnings: List[TrendWarning] = []
        if is_accelerating:
            warnings.append(
                TrendWarning(
                    code="TREND_ACCELERATING",
                    level="info",
                    message="Trend accelerating",
                    context={
                        "acceleration": float(trend_acceleration),
                        "threshold": float(effective_threshold),
                        "confidence": float(acceleration_confidence),
                    },
                )
            )
        elif is_decelerating:
            warnings.append(
                TrendWarning(
                    code="TREND_DECELERATING",
                    level="info",
                    message="Trend decelerating",
                    context={
                        "acceleration": float(trend_acceleration),
                        "threshold": float(effective_threshold),
                        "confidence": float(acceleration_confidence),
                    },
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

    def default(self) -> RollingTrendResult:
        """Return default result for insufficient data (ProbeProtocol compliance)."""
        return RollingTrendResult(
            recent_3y_slope=0.0,
            recent_3y_r_squared=0.0,
            full_5y_slope=0.0,
            full_5y_r_squared=0.0,
            trend_acceleration=0.0,
            acceleration_confidence=0.0,
            is_accelerating=False,
            is_decelerating=False,
            early_3y_slope=0.0,
            early_3y_r_squared=0.0,
            warnings=[TrendWarning(
                code="INSUFFICIENT_DATA",
                level="warning",
                message="Insufficient data",
            )],
        )