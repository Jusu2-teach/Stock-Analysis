import logging
import numpy as np
from typing import List, Optional, Sequence, Tuple, Any
from abc import ABC, abstractmethod

from ..models import (
    DataQualitySummary,
    OutlierDetectionResult,
    TrendWarning,
)
from ..config import TrendAnalysisConfig, get_default_config

logger = logging.getLogger(__name__)

class DataQualityClassification:
    """Data quality classification result."""
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

class DataQualityChecker:
    """Data quality check service."""

    def __init__(self, config: TrendAnalysisConfig = None):
        self.config = config or get_default_config()

    def ensure_window(self, values: Sequence[float]) -> np.ndarray:
        """Ensure data window meets minimum requirements."""
        arr = np.array(values, dtype=float)
        if len(arr) < self.config.min_periods:
            raise ValueError(
                f"Data window too small: {len(arr)} < {self.config.min_periods}"
            )
        return arr

    def classify_quality(self, values: np.ndarray) -> DataQualityClassification:
        """Classify data quality."""
        has_loss = bool(np.any(values < 0))
        loss_count = int(np.sum(values < 0))

        near_zero_mask = np.abs(values) < self.config.near_zero_threshold
        has_near_zero = bool(np.any(near_zero_mask))
        near_zero_count = int(np.sum(near_zero_mask))

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

class OutlierDetector(ABC):
    """Base class for outlier detectors."""

    def __init__(self, config: TrendAnalysisConfig):
        self.config = config

    @abstractmethod
    def detect(self, values: List[float]) -> OutlierDetectionResult:
        pass

class IQROutlierDetector(OutlierDetector):
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
                    message=f"Detected {len(outlier_indices)} outliers (IQR)",
                    context={
                        "indices": outlier_indices,
                        "threshold": threshold,
                        "bounds": [lower_bound, upper_bound],
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
                    message=f"Detected {len(outlier_indices)} outliers (Z-Score)",
                    context={"indices": outlier_indices, "threshold": threshold},
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
    def detect(self, values: List[float]) -> OutlierDetectionResult:
        arr = np.array(values, dtype=float)
        median = np.median(arr)
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
            )

        mad_scaled = 1.4826 * mad
        threshold = self.config.zscore_threshold
        modified_z_scores = np.abs((arr - median) / mad_scaled)

        outlier_mask = modified_z_scores > threshold
        outlier_indices = np.where(outlier_mask)[0].tolist()
        has_outliers = len(outlier_indices) > 0

        cleaned = arr.copy()
        if has_outliers:
            cleaned[outlier_mask] = median

        warnings = []
        if has_outliers:
            warnings.append(
                TrendWarning(
                    code="OUTLIERS_DETECTED_MAD",
                    level="info",
                    message=f"Detected {len(outlier_indices)} outliers (MAD)",
                    context={"indices": outlier_indices, "threshold": threshold},
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
    @staticmethod
    def create(method: str, config: TrendAnalysisConfig) -> OutlierDetector:
        if method == "iqr":
            return IQROutlierDetector(config)
        elif method == "zscore":
            return ZScoreOutlierDetector(config)
        elif method == "mad":
            return MADOutlierDetector(config)
        else:
            raise ValueError(f"Unknown outlier detection method: {method}")

def calculate_weighted_average(
    values: List[float],
    weights: Optional[Sequence[float]] = None,
    adaptive: bool = False
) -> float:
    """
    Calculate weighted average.

    Args:
        values: List of values.
        weights: Optional fixed weights.
        adaptive: If True, adjusts weights based on volatility (CV).
                  High volatility -> Flatter weights (trust history more).
                  Low volatility -> Steeper weights (trust recent more).
    """
    config = get_default_config()
    if weights is None:
        weight_array = config.default_weights
    else:
        weight_array = np.asarray(weights, dtype=float)
        if weight_array.ndim != 1 or weight_array.size == 0:
            raise ValueError("Weights must be 1D and non-empty")

    checker = DataQualityChecker(config)
    values_array = checker.ensure_window(values)

    # Handle length mismatch first
    if len(weight_array) != len(values_array):
        if len(weight_array) > len(values_array):
            weight_array = weight_array[-len(values_array):]
        else:
            weight_array = np.arange(1, len(values_array) + 1, dtype=float)

    # Adaptive Logic
    if adaptive:
        mean = np.mean(values_array)
        std = np.std(values_array, ddof=1)
        cv = abs(std / mean) if mean != 0 else 0.0

        # If volatility is high (>25%), flatten the weights to reduce noise impact
        if cv > 0.25:
            n = len(values_array)
            equal_weights = np.ones(n) / n
            # Blend: 60% original (progressive), 40% equal (flat)
            # This reduces the dominance of the most recent year if it's an outlier
            weight_array = 0.6 * weight_array + 0.4 * equal_weights

    total_weight = float(np.sum(weight_array))
    if not np.isfinite(total_weight) or abs(total_weight) < 1e-12:
        raise ValueError("Sum of weights must be finite and non-zero")

    normalized_weights = weight_array / total_weight
    weighted_avg = np.sum(values_array * normalized_weights)

    return float(weighted_avg)

class FatalMetricProbeError(Exception):
    """Fatal error during metric probe execution."""
    def __init__(self, probe_name: str, original: Exception):
        super().__init__(f"Probe {probe_name} failed: {original}")
        self.probe_name = probe_name
        self.original = original
