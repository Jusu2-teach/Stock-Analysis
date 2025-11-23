
import logging
import numpy as np
from scipy import stats
from typing import List, Dict, Any, Optional, Tuple

from ..models import LogTrendResult, TrendWarning, DataQualitySummary, OutlierDetectionResult
from ..config import TrendAnalysisConfig, get_default_config
from .common import DataQualityChecker, OutlierDetectorFactory

logger = logging.getLogger(__name__)

class LogTrendCalculator:
    """Log trend calculator."""

    def __init__(self, config: TrendAnalysisConfig = None):
        self.config = config or get_default_config()
        self.quality_checker = DataQualityChecker(self.config)

    def calculate(
        self,
        values: List[float],
        check_outliers: bool = True,
        outlier_method: str = None,
    ) -> LogTrendResult:
        outlier_method = outlier_method or self.config.default_outlier_method

        values_array = self.quality_checker.ensure_window(values)
        values_original = values_array.copy()

        outlier_result, values_cleaned, used_cleaned = self._handle_outliers(
            values_array, check_outliers, outlier_method
        )

        quality_summary = self._assess_data_quality(
            values_original, values_cleaned
        )

        trend_metrics = self._compute_trend_metrics(values_cleaned)

        cagr_approx = self._compute_cagr(
            values_original, quality_summary, trend_metrics
        )

        return self._build_result(
            trend_metrics=trend_metrics,
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
            logger.warning(f"Outlier detection failed: {exc}")
            return None, values, False

    def _assess_data_quality(
        self,
        original: np.ndarray,
        cleaned: np.ndarray,
    ) -> DataQualitySummary:
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

    def _compute_cagr(
        self,
        values: np.ndarray,
        quality: DataQualitySummary,
        trend_metrics: Dict[str, Any],
    ) -> float:
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
        cagr_approx: float,
        quality_summary: DataQualitySummary,
        outlier_result: Optional[OutlierDetectionResult],
        used_cleaned: bool,
        outlier_method: str,
        check_outliers: bool,
    ) -> LogTrendResult:
        warnings = []

        if outlier_result:
            warnings.extend(outlier_result.warnings)

        if quality_summary.effective == "poor":
            warnings.append(
                TrendWarning(
                    code="DATA_QUALITY_POOR",
                    level="warn",
                    message="Data quality is poor",
                    context={
                        "original": quality_summary.original,
                        "cleaned": quality_summary.cleaned,
                    },
                )
            )

        metadata = {
            "log_transform": "asinh",
            "periods_used": len(trend_metrics['years']),
            "outlier_method": outlier_result.method if outlier_result else (
                outlier_method if check_outliers else None
            ),
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
            metadata=metadata,
            warnings=warnings,
        )
