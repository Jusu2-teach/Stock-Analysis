"""
拐点检测器 (Inflection Point Detector)
======================================

使用分段线性回归(Piecewise Linear Regression)检测趋势结构性变化。

增强功能：
1. F检验显著性校验
2. 最小段长度约束（防止过拟合）
3. 多拐点检测支持
4. 拐点置信度评估

作者: AStock Analysis System
日期: 2025-12-06
"""

import logging
import numpy as np
from scipy import stats
from typing import List, Tuple, Optional, Dict

from ..models import InflectionResult, TrendWarning
from ..config import get_default_config
from .common import DataQualityChecker

logger = logging.getLogger(__name__)


class InflectionDetector:
    """
    增强版拐点检测器

    使用分段线性回归识别结构性断点，增加了：
    - F检验验证拐点统计显著性
    - 最小段长度约束（默认2年）
    - 改进的拐点类型分类
    """

    def __init__(
        self,
        min_segment_length: int = 2,
        significance_threshold: float = 0.35,
        slope_diff_threshold: float = 0.3,
        f_test_alpha: float = 0.10,
    ):
        """
        Args:
            min_segment_length: 最小段长度（默认2年，确保每段有足够数据）
            significance_threshold: SSE降低阈值（默认35%）
            slope_diff_threshold: 斜率变化阈值（默认0.3）
            f_test_alpha: F检验显著性水平（默认0.10，宽松一些因为数据点少）
        """
        self.min_segment_length = min_segment_length
        self.significance_threshold = significance_threshold
        self.slope_diff_threshold = slope_diff_threshold
        self.f_test_alpha = f_test_alpha

    def detect(self, values: List[float]) -> InflectionResult:
        config = get_default_config()
        checker = DataQualityChecker(config)
        values_array = checker.ensure_window(values)
        n = len(values_array)

        # 数据点不足时使用简化方法
        if n < 2 * self.min_segment_length + 1:  # 至少需要5个点
            return self._simple_detect(values_array)

        # 1. 全局回归（H0: 没有拐点，单一直线）
        x = np.arange(n)
        slope_g, intercept_g, r_val_g, _, std_err_g = stats.linregress(x, values_array)
        y_pred_g = slope_g * x + intercept_g
        sse_global = np.sum((values_array - y_pred_g) ** 2)
        df_global = n - 2  # 自由度 = n - 参数数量

        if sse_global < 1e-9:  # 完美直线
            return self._empty_result()

        # 2. 寻找最优分割点（分段回归）
        best_k = -1
        min_sse_split = float('inf')
        best_params: Dict = {}

        # 搜索范围：确保每段至少有 min_segment_length 个点
        for k in range(self.min_segment_length - 1, n - self.min_segment_length):
            # 左段: 0 到 k+1 (包含 k+1 个点)
            x_l = x[:k+1]
            y_l = values_array[:k+1]
            if len(x_l) < self.min_segment_length:
                continue

            slope_l, intercept_l, r_l, _, std_l = stats.linregress(x_l, y_l)
            sse_l = np.sum((y_l - (slope_l * x_l + intercept_l)) ** 2)

            # 右段: k 到 n
            x_r = x[k:]
            y_r = values_array[k:]
            if len(x_r) < self.min_segment_length:
                continue

            slope_r, intercept_r, r_r, _, std_r = stats.linregress(x_r, y_r)
            sse_r = np.sum((y_r - (slope_r * x_r + intercept_r)) ** 2)

            sse_total = sse_l + sse_r

            if sse_total < min_sse_split:
                min_sse_split = sse_total
                best_k = k
                best_params = {
                    'slope_l': slope_l, 'r_l': r_l, 'std_l': std_l, 'n_l': len(x_l),
                    'slope_r': slope_r, 'r_r': r_r, 'std_r': std_r, 'n_r': len(x_r),
                    'sse_l': sse_l, 'sse_r': sse_r,
                }

        if best_k < 0:
            return self._empty_result()

        # 3. 显著性检验
        # 3.1 SSE降低比例
        sse_reduction = 1.0 - (min_sse_split / sse_global)

        # 3.2 F检验：比较分段模型 vs 全局模型
        df_split = n - 4  # 分段模型有4个参数（2个斜率 + 2个截距）
        f_stat, f_pvalue = self._f_test(sse_global, min_sse_split, df_global, df_split)

        # 3.3 斜率变化检验
        slope_l = best_params['slope_l']
        slope_r = best_params['slope_r']
        slope_change = slope_r - slope_l

        # 4. 综合判断是否有显著拐点
        has_inflection = (
            sse_reduction > self.significance_threshold and
            abs(slope_change) > self.slope_diff_threshold and
            f_pvalue < self.f_test_alpha  # F检验显著
        )

        # 5. 拐点类型分类
        inflection_type = "none"
        if has_inflection:
            inflection_type = self._classify_inflection(slope_l, slope_r)

        # 6. 计算综合置信度
        confidence = self._calculate_confidence(
            sse_reduction, f_pvalue, abs(slope_change),
            best_params['r_l']**2, best_params['r_r']**2
        )

        # 7. 构建警告信息
        warnings = []
        if has_inflection:
            level = "warn" if "decline" in inflection_type or "worsening" in inflection_type else "info"
            warnings.append(
                TrendWarning(
                    code="INFLECTION_DETECTED_ADVANCED",
                    level=level,
                    message=f"第{best_k+1}年检测到结构性拐点: {inflection_type}",
                    context={
                        "split_index": int(best_k),
                        "sse_reduction": float(sse_reduction),
                        "slope_change": float(slope_change),
                        "f_statistic": float(f_stat),
                        "f_pvalue": float(f_pvalue),
                        "confidence": float(confidence),
                    },
                )
            )

        return InflectionResult(
            has_inflection=has_inflection,
            inflection_type=inflection_type,
            early_slope=float(slope_l),
            middle_slope=0.0,
            recent_slope=float(slope_r),
            slope_change=float(slope_change),
            confidence=float(confidence),
            early_r_squared=float(best_params['r_l']**2),
            recent_r_squared=float(best_params['r_r']**2),
            warnings=warnings,
        )

    def _f_test(
        self,
        sse_restricted: float,
        sse_unrestricted: float,
        df_restricted: int,
        df_unrestricted: int
    ) -> Tuple[float, float]:
        """
        F检验：比较受限模型（全局）与非受限模型（分段）

        H0: 全局模型足够好（没有显著拐点）
        H1: 分段模型显著更好（有显著拐点）
        """
        if df_unrestricted <= 0 or sse_unrestricted <= 0:
            return 0.0, 1.0

        df_diff = df_restricted - df_unrestricted  # 额外参数数量（2）
        if df_diff <= 0:
            return 0.0, 1.0

        sse_diff = sse_restricted - sse_unrestricted

        # F = (SSE_diff / df_diff) / (SSE_unrestricted / df_unrestricted)
        f_stat = (sse_diff / df_diff) / (sse_unrestricted / df_unrestricted)

        # P值
        if f_stat > 0:
            f_pvalue = 1 - stats.f.cdf(f_stat, df_diff, df_unrestricted)
        else:
            f_pvalue = 1.0

        return float(f_stat), float(f_pvalue)

    def _classify_inflection(self, slope_l: float, slope_r: float) -> str:
        """分类拐点类型"""
        if slope_l > 0 and slope_r < 0:
            return "growth_to_decline"  # 倒V型：增长转衰退
        elif slope_l < 0 and slope_r > 0:
            return "deterioration_to_recovery"  # V型：衰退转复苏
        elif slope_l > 0 and slope_r > slope_l:
            return "acceleration"  # 加速增长
        elif slope_l < 0 and slope_r < slope_l:
            return "accelerated_decline"  # 加速下滑
        elif slope_r > slope_l:
            return "improving"  # 改善
        else:
            return "worsening"  # 恶化

    def _calculate_confidence(
        self,
        sse_reduction: float,
        f_pvalue: float,
        slope_diff: float,
        r2_early: float,
        r2_recent: float
    ) -> float:
        """
        综合置信度计算

        考虑因素：
        - SSE降低比例（越高越好）
        - F检验p值（越低越好）
        - 斜率变化幅度（越大越明显）
        - 两段的R²（越高说明分段拟合越好）
        """
        # 各因素得分 (0-1)
        sse_score = min(sse_reduction / 0.6, 1.0)
        pvalue_score = 1.0 - min(f_pvalue / 0.2, 1.0)
        slope_score = min(slope_diff / 1.0, 1.0)
        fit_score = (r2_early + r2_recent) / 2

        # 加权平均
        confidence = (
            sse_score * 0.3 +
            pvalue_score * 0.3 +
            slope_score * 0.2 +
            fit_score * 0.2
        )

        return float(min(confidence, 1.0))

    def _simple_detect(self, values_array: np.ndarray) -> InflectionResult:
        """Fallback to simple window-based detection for short series."""
        # ...existing code...
        window_size = 3
        years = np.arange(window_size)
        slopes: List[float] = []
        r_squares: List[float] = []

        for start in range(values_array.size - window_size + 1):
            segment = values_array[start : start + window_size]
            try:
                slope, _, r_value, _, _ = stats.linregress(years, segment)
            except ValueError:
                slope = 0.0
                r_value = 0.0
            slopes.append(float(slope))
            r_squares.append(float(r_value**2))

        slopes_array = np.asarray(slopes)
        r_squares_array = np.asarray(r_squares)

        early_slope = slopes_array[0]
        middle_slope = slopes_array[1] if slopes_array.size > 1 else slopes_array[0]
        recent_slope = slopes_array[-1]
        slope_change = recent_slope - early_slope

        slope_diffs = np.diff(slopes_array) if slopes_array.size > 1 else np.array([0.0])
        diff_sign_change = bool(
            slope_diffs.size >= 2
            and (
                (slope_diffs[0] > 0 and slope_diffs[1] < 0)
                or (slope_diffs[0] < 0 and slope_diffs[1] > 0)
            )
        )

        SLOPE_THRESHOLD = 2.0
        POS_TREND_THRESHOLD = 1.0
        NEG_TREND_THRESHOLD = -1.0

        has_inflection = False
        inflection_type = "none"

        if (
            early_slope <= NEG_TREND_THRESHOLD
            and recent_slope >= POS_TREND_THRESHOLD
            and slope_change >= SLOPE_THRESHOLD
        ):
            has_inflection = True
            inflection_type = "deterioration_to_recovery"
        elif (
            early_slope >= POS_TREND_THRESHOLD
            and recent_slope <= NEG_TREND_THRESHOLD
            and slope_change <= -SLOPE_THRESHOLD
        ):
            has_inflection = True
            inflection_type = "growth_to_decline"
        elif diff_sign_change and abs(slope_change) >= SLOPE_THRESHOLD:
            has_inflection = True
            inflection_type = (
                "deterioration_to_recovery" if slope_change > 0 else "growth_to_decline"
            )

        confidence = float(r_squares_array.mean()) if r_squares_array.size else 0.0

        warnings: List[TrendWarning] = []
        if has_inflection:
            level = "warn" if inflection_type == "growth_to_decline" else "info"
            warnings.append(
                TrendWarning(
                    code="INFLECTION_DETECTED",
                    level=level,
                    message=f"Inflection detected: {inflection_type}",
                    context={"slope_change": float(slope_change)},
                )
            )

        return InflectionResult(
            has_inflection=bool(has_inflection),
            inflection_type=inflection_type,
            early_slope=float(early_slope),
            middle_slope=float(middle_slope),
            recent_slope=float(recent_slope),
            slope_change=float(slope_change),
            confidence=confidence,
            early_r_squared=float(r_squares_array[0] if r_squares_array.size else 0.0),
            recent_r_squared=float(r_squares_array[-1] if r_squares_array.size else 0.0),
            warnings=warnings,
        )

    def _empty_result(self) -> InflectionResult:
        return InflectionResult(
            has_inflection=False,
            inflection_type="none",
            early_slope=0.0,
            middle_slope=0.0,
            recent_slope=0.0,
            slope_change=0.0,
            confidence=0.0,
            early_r_squared=0.0,
            recent_r_squared=0.0,
            warnings=[],
        )
