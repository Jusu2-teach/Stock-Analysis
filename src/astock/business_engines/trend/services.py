"""
趋势分析服务模块（重构合并版）
===============================

合并原 services/ 目录下的所有服务类：
- LogTrendCalculator: Log 趋势计算
- CyclicalPatternDetector: 周期性检测
- DataQualityChecker: 数据质量检查
- OutlierDetector: 异常值检测

原文件：
- services/log_trend_calculator.py
- services/cyclical_detector.py
- services/data_quality.py
- services/outlier_detector.py
"""

import logging
import numpy as np
from scipy import stats
from scipy.stats import theilslopes
from typing import List, Optional, Tuple, Dict, Any, Sequence
from abc import ABC, abstractmethod

from .trend_models import (
    LogTrendResult,
    DataQualitySummary,
    OutlierDetectionResult,
    TrendWarning,
    CyclicalPatternResult,
)
from .config import TrendAnalysisConfig, get_default_config

logger = logging.getLogger(__name__)


# ============================================================================
# 数据质量检查器
# ============================================================================

class DataQualityChecker:
    """数据质量检查服务"""

    def __init__(self, config: TrendAnalysisConfig = None):
        self.config = config or get_default_config()

    def ensure_window(self, values: Sequence[float]) -> np.ndarray:
        """确保数据窗口符合最小要求"""
        arr = np.array(values, dtype=float)
        if len(arr) < self.config.min_periods:
            raise ValueError(
                f"数据窗口过小: {len(arr)} < {self.config.min_periods}"
            )
        return arr

    def classify_quality(self, values: np.ndarray) -> 'DataQualityClassification':
        """分类数据质量"""
        has_loss = bool(np.any(values < 0))
        loss_count = int(np.sum(values < 0))

        near_zero_mask = np.abs(values) < self.config.near_zero_threshold
        has_near_zero = bool(np.any(near_zero_mask))
        near_zero_count = int(np.sum(near_zero_mask))

        # 质量分类
        if has_loss and loss_count >= 2:
            quality = "poor"
        elif has_loss:
            quality = "has_loss"
        elif near_zero_count >= 2:
            quality = "has_near_zero"
        else:
            quality = "good"

        return DataQualityClassification(
            quality=quality,
            has_loss_years=has_loss,
            loss_year_count=loss_count,
            has_near_zero_years=has_near_zero,
            near_zero_count=near_zero_count,
        )


class DataQualityClassification:
    """数据质量分类结果"""

    def __init__(
        self,
        quality: str,
        has_loss_years: bool,
        loss_year_count: int,
        has_near_zero_years: bool,
        near_zero_count: int,
    ):
        self.quality = quality
        self.has_loss_years = has_loss_years
        self.loss_year_count = loss_year_count
        self.has_near_zero_years = has_near_zero_years
        self.near_zero_count = near_zero_count


# ============================================================================
# 异常值检测器
# ============================================================================

class OutlierDetector(ABC):
    """异常值检测器基类"""

    def __init__(self, config: TrendAnalysisConfig):
        self.config = config

    @abstractmethod
    def detect(self, values: List[float]) -> OutlierDetectionResult:
        """检测异常值"""
        pass


class IQROutlierDetector(OutlierDetector):
    """基于 IQR 的异常值检测器"""

    def detect(self, values: List[float]) -> OutlierDetectionResult:
        arr = np.array(values, dtype=float)
        q1, q3 = np.percentile(arr, [25, 75])
        iqr = q3 - q1
        threshold = self.config.iqr_multiplier

        lower_bound = q1 - threshold * iqr
        upper_bound = q3 + threshold * iqr

        outlier_mask = (arr < lower_bound) | (arr > upper_bound)
        outlier_indices = np.where(outlier_mask)[0].tolist()

        has_outliers = len(outlier_indices) > 0

        # 清洗：用中位数替换
        cleaned = arr.copy()
        if has_outliers:
            median = np.median(arr[~outlier_mask])
            cleaned[outlier_mask] = median

        warnings = []
        if has_outliers:
            warnings.append(
                TrendWarning(
                    code="OUTLIERS_DETECTED_IQR",
                    level="info",
                    message=f"检测到 {len(outlier_indices)} 个异常值（IQR方法）",
                    context={
                        "indices": outlier_indices,
                        "threshold": threshold,
                        "bounds": [lower_bound, upper_bound],
                    },
                )
            )

        outlier_values = arr[outlier_mask].tolist() if has_outliers else []
        cleaning_ratio = len(outlier_indices) / len(arr) if len(arr) > 0 else 0.0

        # 数据污染程度
        if cleaning_ratio > 0.3:
            data_contamination = "high"
            risk_level = "high"
        elif cleaning_ratio > 0.1:
            data_contamination = "medium"
            risk_level = "medium"
        else:
            data_contamination = "low"
            risk_level = "low"

        return OutlierDetectionResult(
            method="iqr",
            threshold=threshold,
            has_outliers=has_outliers,
            indices=outlier_indices,
            values=outlier_values,
            cleaned_values=cleaned.tolist(),
            cleaning_ratio=cleaning_ratio,
            cleaning_applied=has_outliers,
            data_contamination=data_contamination,
            risk_level=risk_level,
            warnings=warnings,
        )


class ZScoreOutlierDetector(OutlierDetector):
    """基于 Z-Score 的异常值检测器"""

    def detect(self, values: List[float]) -> OutlierDetectionResult:
        arr = np.array(values, dtype=float)
        mean = np.mean(arr)
        std = np.std(arr, ddof=1)

        if std == 0:
            return OutlierDetectionResult(
                method="zscore",
                threshold=self.config.zscore_threshold,
                has_outliers=False,
                indices=[],
                values=[],
                cleaned_values=values,
                cleaning_ratio=0.0,
                cleaning_applied=False,
                data_contamination="none",
                risk_level="low",
                warnings=[],
            )

        z_scores = np.abs((arr - mean) / std)
        threshold = self.config.zscore_threshold

        outlier_mask = z_scores > threshold
        outlier_indices = np.where(outlier_mask)[0].tolist()
        has_outliers = len(outlier_indices) > 0

        cleaned = arr.copy()
        if has_outliers:
            median = np.median(arr[~outlier_mask])
            cleaned[outlier_mask] = median

        warnings = []
        if has_outliers:
            warnings.append(
                TrendWarning(
                    code="OUTLIERS_DETECTED_ZSCORE",
                    level="info",
                    message=f"检测到 {len(outlier_indices)} 个异常值（Z-Score方法）",
                    context={
                        "indices": outlier_indices,
                        "threshold": threshold,
                    },
                )
            )

        outlier_values = arr[outlier_mask].tolist() if has_outliers else []
        cleaning_ratio = len(outlier_indices) / len(arr) if len(arr) > 0 else 0.0

        if cleaning_ratio > 0.3:
            data_contamination = "high"
            risk_level = "high"
        elif cleaning_ratio > 0.1:
            data_contamination = "medium"
            risk_level = "medium"
        else:
            data_contamination = "low"
            risk_level = "low"

        return OutlierDetectionResult(
            method="zscore",
            threshold=threshold,
            has_outliers=has_outliers,
            indices=outlier_indices,
            values=outlier_values,
            cleaned_values=cleaned.tolist(),
            cleaning_ratio=cleaning_ratio,
            cleaning_applied=has_outliers,
            data_contamination=data_contamination,
            risk_level=risk_level,
            warnings=warnings,
        )


class MADOutlierDetector(OutlierDetector):
    """基于 MAD（中位数绝对偏差）的异常值检测器"""

    def detect(self, values: List[float]) -> OutlierDetectionResult:
        arr = np.array(values, dtype=float)
        median = np.median(arr)

        # 计算 MAD
        mad = np.median(np.abs(arr - median))

        if mad == 0:
            return OutlierDetectionResult(
                method="mad",
                threshold=self.config.zscore_threshold,
                has_outliers=False,
                indices=[],
                values=[],
                cleaned_values=values,
                cleaning_ratio=0.0,
                cleaning_applied=False,
                data_contamination="none",
                risk_level="low",
                warnings=[],
            )        # 修正的 MAD (使用常数 1.4826 使其与正态分布的标准差一致)
        mad_scaled = 1.4826 * mad
        threshold = self.config.zscore_threshold  # 使用相同的阈值标准

        # 计算修正后的 z-score
        modified_z_scores = np.abs((arr - median) / mad_scaled)

        outlier_mask = modified_z_scores > threshold
        outlier_indices = np.where(outlier_mask)[0].tolist()
        has_outliers = len(outlier_indices) > 0

        cleaned = arr.copy()
        if has_outliers:
            # 用中位数替换异常值
            cleaned[outlier_mask] = median

        warnings = []
        if has_outliers:
            warnings.append(
                TrendWarning(
                    code="OUTLIERS_DETECTED_MAD",
                    level="info",
                    message=f"检测到 {len(outlier_indices)} 个异常值（MAD方法）",
                    context={
                        "indices": outlier_indices,
                        "threshold": threshold,
                    },
                )
            )

        outlier_values = arr[outlier_mask].tolist() if has_outliers else []
        cleaning_ratio = len(outlier_indices) / len(arr) if len(arr) > 0 else 0.0

        if cleaning_ratio > 0.3:
            data_contamination = "high"
            risk_level = "high"
        elif cleaning_ratio > 0.1:
            data_contamination = "medium"
            risk_level = "medium"
        else:
            data_contamination = "low"
            risk_level = "low"

        return OutlierDetectionResult(
            method="mad",
            threshold=threshold,
            has_outliers=has_outliers,
            indices=outlier_indices,
            values=outlier_values,
            cleaned_values=cleaned.tolist(),
            cleaning_ratio=cleaning_ratio,
            cleaning_applied=has_outliers,
            data_contamination=data_contamination,
            risk_level=risk_level,
            warnings=warnings,
        )


class OutlierDetectorFactory:
    """异常值检测器工厂"""

    @staticmethod
    def create(method: str, config: TrendAnalysisConfig) -> OutlierDetector:
        """创建检测器"""
        if method == "iqr":
            return IQROutlierDetector(config)
        elif method == "zscore":
            return ZScoreOutlierDetector(config)
        elif method == "mad":
            return MADOutlierDetector(config)
        else:
            raise ValueError(f"未知的异常值检测方法: {method}")


# ============================================================================
# Log 趋势计算器
# ============================================================================

class LogTrendCalculator:
    """Log 趋势计算器（重构版）"""

    def __init__(self, config: TrendAnalysisConfig = None):
        self.config = config or get_default_config()
        self.quality_checker = DataQualityChecker(self.config)

    def calculate(
        self,
        values: List[float],
        check_outliers: bool = True,
        outlier_method: str = None,
    ) -> LogTrendResult:
        """计算 Log 趋势"""
        outlier_method = outlier_method or self.config.default_outlier_method

        # 1. 准备数据
        values_array = self.quality_checker.ensure_window(values)
        values_original = values_array.copy()

        # 2. 异常值检测
        outlier_result, values_cleaned, used_cleaned = self._handle_outliers(
            values_array, check_outliers, outlier_method
        )

        # 3. 数据质量评估
        quality_summary = self._assess_data_quality(
            values_original, values_cleaned
        )

        # 4. 趋势计算
        trend_metrics = self._compute_trend_metrics(values_cleaned)

        # 5. 鲁棒性检查
        robust_metrics = self._compute_robust_metrics(
            values_cleaned, trend_metrics
        )

        # 6. CAGR 计算
        cagr_approx = self._compute_cagr(
            values_original, quality_summary, trend_metrics
        )

        # 7. 构建结果
        return self._build_result(
            trend_metrics=trend_metrics,
            robust_metrics=robust_metrics,
            cagr_approx=cagr_approx,
            quality_summary=quality_summary,
            outlier_result=outlier_result,
            used_cleaned=used_cleaned,
            outlier_method=outlier_method,
            check_outliers=check_outliers,
        )

    def _handle_outliers(
        self,
        values: np.ndarray,
        check_outliers: bool,
        method: str,
    ) -> Tuple[Optional[OutlierDetectionResult], np.ndarray, bool]:
        """处理异常值"""
        if not check_outliers:
            return None, values, False

        try:
            detector = OutlierDetectorFactory.create(method, self.config)
            outlier_result = detector.detect(values.tolist())

            if outlier_result.has_outliers and outlier_result.cleaning_applied:
                cleaned = np.array(outlier_result.cleaned_values, dtype=float)
                return outlier_result, cleaned, True

            return outlier_result, values, False

        except Exception as exc:
            logger.warning(f"异常值检测失败: {exc}")
            return None, values, False

    def _assess_data_quality(
        self,
        original: np.ndarray,
        cleaned: np.ndarray,
    ) -> DataQualitySummary:
        """评估数据质量"""
        quality_original = self.quality_checker.classify_quality(original)
        quality_cleaned = self.quality_checker.classify_quality(cleaned)

        quality_rank = {
            "good": 0,
            "has_near_zero": 1,
            "has_loss": 2,
            "poor": 3
        }

        if quality_rank[quality_original.quality] > quality_rank[quality_cleaned.quality]:
            effective_quality = quality_original.quality
        else:
            effective_quality = quality_cleaned.quality

        return DataQualitySummary(
            original=quality_original.quality,
            cleaned=quality_cleaned.quality,
            effective=effective_quality,
            has_loss_years=quality_original.has_loss_years,
            loss_year_count=quality_original.loss_year_count,
            has_near_zero_years=quality_original.has_near_zero_years,
            near_zero_count=quality_original.near_zero_count,
            has_loss_years_cleaned=quality_cleaned.has_loss_years,
            loss_year_count_cleaned=quality_cleaned.loss_year_count,
            has_near_zero_years_cleaned=quality_cleaned.has_near_zero_years,
            near_zero_count_cleaned=quality_cleaned.near_zero_count,
        )

    def _compute_trend_metrics(self, values: np.ndarray) -> Dict[str, Any]:
        """计算趋势指标"""
        years = np.arange(values.size)
        transformed = np.arcsinh(values)
        crosses_zero = bool(np.any(values < 0) and np.any(values > 0))

        log_slope, log_intercept, r_value, p_value, std_err = stats.linregress(
            years, transformed
        )

        linear_slope, linear_intercept, _, _, _ = stats.linregress(
            years, values
        )

        return {
            'log_slope': float(log_slope),
            'log_intercept': float(log_intercept),
            'linear_slope': float(linear_slope),
            'linear_intercept': float(linear_intercept),
            'r_value': float(r_value),
            'r_squared': float(r_value ** 2),
            'p_value': float(p_value),
            'std_err': float(std_err),
            'crosses_zero': crosses_zero,
            'transformed': transformed,
            'years': years,
        }

    def _compute_robust_metrics(
        self,
        values: np.ndarray,
        trend_metrics: Dict[str, Any],
    ) -> Dict[str, Any]:
        """计算鲁棒性指标"""
        if len(values) < 2:
            return {
                'robust_status': 'insufficient_data',
                'robust_slope': float('nan'),
                'robust_intercept': float('nan'),
                'robust_ci_low': float('nan'),
                'robust_ci_high': float('nan'),
                'robust_ci_width': float('nan'),
                'robust_gap': float('nan'),
                'robust_error': None,
                'robust_warnings': [],
            }

        years = trend_metrics['years']
        transformed = trend_metrics['transformed']
        log_slope = trend_metrics['log_slope']
        r_squared = trend_metrics['r_squared']

        try:
            robust_slope, robust_intercept, robust_ci_low, robust_ci_high = theilslopes(
                transformed, years, alpha=self.config.robust_alpha
            )

            robust_ci_width = float(robust_ci_high - robust_ci_low)
            robust_gap = abs(log_slope - robust_slope)

            robust_warnings = []
            if robust_gap > self.config.robust_gap_threshold and r_squared < 0.7:
                robust_warnings.append(
                    TrendWarning(
                        code="ROBUST_SLOPE_DISCREPANCY",
                        level="info",
                        message="鲁棒斜率与OLS斜率差异较大",
                        context={
                            "ols_slope": float(log_slope),
                            "robust_slope": float(robust_slope),
                            "gap": float(robust_gap),
                        },
                    )
                )

            return {
                'robust_status': 'ok',
                'robust_slope': float(robust_slope),
                'robust_intercept': float(robust_intercept),
                'robust_ci_low': float(robust_ci_low),
                'robust_ci_high': float(robust_ci_high),
                'robust_ci_width': robust_ci_width,
                'robust_gap': robust_gap,
                'robust_error': None,
                'robust_warnings': robust_warnings,
            }

        except Exception as exc:
            return {
                'robust_status': 'error',
                'robust_slope': float('nan'),
                'robust_intercept': float('nan'),
                'robust_ci_low': float('nan'),
                'robust_ci_high': float('nan'),
                'robust_ci_width': float('nan'),
                'robust_gap': float('nan'),
                'robust_error': str(exc),
                'robust_warnings': [
                    TrendWarning(
                        code="ROBUST_SLOPE_FALLBACK",
                        level="info",
                        message="Theil-Sen计算失败",
                        context={"error": str(exc)},
                    )
                ],
            }

    def _compute_cagr(
        self,
        values: np.ndarray,
        quality: DataQualitySummary,
        trend_metrics: Dict[str, Any],
    ) -> float:
        """计算 CAGR"""
        if quality.has_loss_years or trend_metrics['crosses_zero'] or np.any(values <= 0):
            return float('nan')

        period_years = len(values) - 1
        if period_years > 0 and values[0] > 0:
            cagr = (values[-1] / values[0]) ** (1.0 / period_years) - 1.0
            return float(cagr)

        return float('nan')

    def _build_result(
        self,
        trend_metrics: Dict[str, Any],
        robust_metrics: Dict[str, Any],
        cagr_approx: float,
        quality_summary: DataQualitySummary,
        outlier_result: Optional[OutlierDetectionResult],
        used_cleaned: bool,
        outlier_method: str,
        check_outliers: bool,
    ) -> LogTrendResult:
        """构建结果"""
        warnings = []

        if outlier_result:
            warnings.extend(outlier_result.warnings)

        if quality_summary.effective == "poor":
            warnings.append(
                TrendWarning(
                    code="DATA_QUALITY_POOR",
                    level="warn",
                    message="数据质量差",
                    context={
                        "original": quality_summary.original,
                        "cleaned": quality_summary.cleaned,
                    },
                )
            )

        if robust_metrics['robust_warnings']:
            warnings.extend(robust_metrics['robust_warnings'])

        metadata = {
            "log_transform": "asinh",
            "periods_used": len(trend_metrics['years']),
            "outlier_method": outlier_result.method if outlier_result else (
                outlier_method if check_outliers else None
            ),
            "robust_method": "theil_sen",
            "robust_alpha": self.config.robust_alpha,
            "robust_status": robust_metrics['robust_status'],
        }

        return LogTrendResult(
            log_slope=trend_metrics['log_slope'],
            slope=trend_metrics['linear_slope'],
            intercept=trend_metrics['log_intercept'],
            r_squared=trend_metrics['r_squared'],
            p_value=trend_metrics['p_value'],
            std_err=trend_metrics['std_err'],
            cagr_approx=cagr_approx,
            crosses_zero=trend_metrics['crosses_zero'],
            used_cleaned_data=used_cleaned,
            quality=quality_summary,
            outliers=outlier_result,
            robust_slope=robust_metrics['robust_slope'],
            robust_intercept=robust_metrics['robust_intercept'],
            robust_slope_ci_low=robust_metrics['robust_ci_low'],
            robust_slope_ci_high=robust_metrics['robust_ci_high'],
            metadata=metadata,
            warnings=warnings,
        )


# ============================================================================
# 周期性检测器
# ============================================================================

class CyclicalPatternDetector:
    """周期性模式检测器（重构版）"""

    def __init__(self, config: TrendAnalysisConfig = None):
        self.config = config or get_default_config()

    def detect(
        self,
        values: List[float],
        industry: str = None,
    ) -> CyclicalPatternResult:
        """检测周期性模式"""
        arr = np.array(values, dtype=float)

        if len(arr) < self.config.min_periods:
            return CyclicalPatternResult(
                is_cyclical=False,
                peak_to_trough_ratio=0.0,
                has_middle_peak=False,
                has_wave_pattern=False,
                trend_r_squared=0.0,
                cv=0.0,
                current_phase="unknown",
                industry_cyclical=False,
                cyclical_confidence=0.0,
                peak_to_trough_threshold=3.0,
                trend_r_squared_max=0.3,
                cv_threshold=0.3,
                industry=industry or "unknown",
                confidence_factors=[],
                warnings=[
                    TrendWarning(
                        code="INSUFFICIENT_DATA",
                        level="info",
                        message=f"数据点不足: {len(arr)}",
                        context={},
                    )
                ],
            )

        # 获取行业阈值
        from .config import get_cyclical_thresholds
        thresholds = get_cyclical_thresholds(industry)

        # 计算变异系数
        mean = np.mean(arr)
        std = np.std(arr, ddof=1)
        cv = abs(std / mean) if mean != 0 else float('inf')

        # 计算峰谷比
        if len(arr) >= 3:
            peaks_indices = []
            valleys_indices = []

            for i in range(1, len(arr) - 1):
                if arr[i] > arr[i-1] and arr[i] > arr[i+1]:
                    peaks_indices.append(i)
                elif arr[i] < arr[i-1] and arr[i] < arr[i+1]:
                    valleys_indices.append(i)

            if peaks_indices and valleys_indices:
                peaks = arr[peaks_indices]
                valleys = arr[valleys_indices]
                peak_valley_ratio = np.mean(peaks) / np.mean(valleys) if np.mean(valleys) > 0 else float('inf')
            else:
                peak_valley_ratio = 1.0
        else:
            peak_valley_ratio = 1.0

        # 判断
        is_cyclical = (
            cv > thresholds['cv_threshold'] and
            peak_valley_ratio > thresholds['peak_valley_ratio']
        )

        confidence = min(
            (cv / thresholds['cv_threshold']) * 0.5 +
            (peak_valley_ratio / thresholds['peak_valley_ratio']) * 0.5,
            1.0
        )

        # 判断当前阶段
        current_phase = "unknown"
        if len(arr) >= 2:
            if arr[-1] > arr[-2]:
                current_phase = "rising"
            else:
                current_phase = "falling"

        warnings = []
        if is_cyclical:
            warnings.append(
                TrendWarning(
                    code="CYCLICAL_PATTERN_DETECTED",
                    level="info",
                    message=f"检测到周期性模式（置信度: {confidence:.2%}）",
                    context={
                        "cv": float(cv),
                        "peak_valley_ratio": float(peak_valley_ratio),
                    },
                )
            )

        return CyclicalPatternResult(
            is_cyclical=is_cyclical,
            peak_to_trough_ratio=float(peak_valley_ratio),
            has_middle_peak=len(peaks_indices) > 0,
            has_wave_pattern=len(peaks_indices) > 1 and len(valleys_indices) > 1,
            trend_r_squared=0.0,  # 简化版本不计算
            cv=float(cv),
            current_phase=current_phase,
            industry_cyclical=is_cyclical,
            cyclical_confidence=float(confidence),
            peak_to_trough_threshold=float(thresholds['peak_valley_ratio']),
            trend_r_squared_max=0.3,
            cv_threshold=float(thresholds['cv_threshold']),
            industry=industry or "unknown",
            confidence_factors=[
                f"CV={cv:.3f} (threshold={thresholds['cv_threshold']:.3f})",
                f"Peak/Valley={peak_valley_ratio:.3f} (threshold={thresholds['peak_valley_ratio']:.3f})",
            ],
            warnings=warnings,
        )


# ============================================================================
# 向后兼容的函数接口
# ============================================================================

def calculate_log_trend_slope(
    values: List[float],
    check_outliers: bool = True,
    outlier_method: str = None,
    config: TrendAnalysisConfig = None,
) -> LogTrendResult:
    """计算 Log 趋势（向后兼容接口）"""
    calculator = LogTrendCalculator(config)
    return calculator.calculate(values, check_outliers, outlier_method)


def detect_cyclical_pattern(
    values: List[float],
    industry: str = None,
    config: TrendAnalysisConfig = None,
) -> CyclicalPatternResult:
    """检测周期性模式（向后兼容接口）"""
    detector = CyclicalPatternDetector(config)
    return detector.detect(values, industry)


# 导出
__all__ = [
    # 服务类
    'DataQualityChecker',
    'OutlierDetectorFactory',
    'OutlierDetector',
    'LogTrendCalculator',
    'CyclicalPatternDetector',
    # 向后兼容函数
    'calculate_log_trend_slope',
    'detect_cyclical_pattern',
]
