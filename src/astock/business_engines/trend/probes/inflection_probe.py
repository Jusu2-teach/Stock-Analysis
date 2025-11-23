
import logging
import numpy as np
from scipy import stats
from typing import List

from ..models import InflectionResult, TrendWarning
from ..config import get_default_config
from .common import DataQualityChecker

logger = logging.getLogger(__name__)

class InflectionDetector:
    """
    Advanced Inflection Point Detector.

    Uses Piecewise Linear Regression (Segmented Regression) to identify
    structural breaks in the trend.
    """

    def detect(self, values: List[float]) -> InflectionResult:
        config = get_default_config()
        checker = DataQualityChecker(config)
        values_array = checker.ensure_window(values)
        n = len(values_array)

        # Fallback for short series
        if n < 5:
            return self._simple_detect(values_array)

        # 1. Global Regression
        x = np.arange(n)
        slope_g, intercept_g, r_val_g, _, _ = stats.linregress(x, values_array)
        y_pred_g = slope_g * x + intercept_g
        sse_global = np.sum((values_array - y_pred_g) ** 2)

        if sse_global < 1e-9: # Perfect line
            return self._empty_result()

        # 2. Find Best Split Point (Piecewise Regression)
        best_k = -1
        min_sse_split = float('inf')
        best_params = {}

        # Allow split between index 1 and n-2 (at least 2 points per segment)
        for k in range(1, n - 2):
            # Left segment: 0 to k+1 (inclusive of k+1 points)
            x_l = x[:k+2]
            y_l = values_array[:k+2]
            slope_l, intercept_l, r_l, _, _ = stats.linregress(x_l, y_l)
            sse_l = np.sum((y_l - (slope_l * x_l + intercept_l)) ** 2)

            # Right segment: k to n (inclusive)
            # Note: We share the pivot point k to ensure continuity approximation
            # But strictly speaking, independent regression is easier and robust enough here
            x_r = x[k:]
            y_r = values_array[k:]
            slope_r, intercept_r, r_r, _, _ = stats.linregress(x_r, y_r)
            sse_r = np.sum((y_r - (slope_r * x_r + intercept_r)) ** 2)

            sse_total = sse_l + sse_r

            if sse_total < min_sse_split:
                min_sse_split = sse_total
                best_k = k
                best_params = {
                    'slope_l': slope_l, 'r_l': r_l,
                    'slope_r': slope_r, 'r_r': r_r
                }

        # 3. Evaluate Significance
        # Reduction in SSE
        sse_reduction = 1.0 - (min_sse_split / sse_global)

        # Thresholds
        SIGNIFICANCE_THRESHOLD = 0.4  # 40% reduction in error required
        SLOPE_DIFF_THRESHOLD = 0.5    # Minimum change in slope

        has_inflection = False
        inflection_type = "none"
        slope_change = 0.0

        if sse_reduction > SIGNIFICANCE_THRESHOLD:
            slope_l = best_params['slope_l']
            slope_r = best_params['slope_r']
            slope_change = slope_r - slope_l

            if abs(slope_change) > SLOPE_DIFF_THRESHOLD:
                has_inflection = True
                if slope_l > 0 and slope_r < 0:
                    inflection_type = "growth_to_decline" # 倒V
                elif slope_l < 0 and slope_r > 0:
                    inflection_type = "deterioration_to_recovery" # V型
                elif slope_l > 0 and slope_r > slope_l:
                    inflection_type = "acceleration"
                elif slope_l < 0 and slope_r < slope_l:
                    inflection_type = "accelerated_decline"
                elif slope_r > slope_l:
                    inflection_type = "improving"
                else:
                    inflection_type = "worsening"

        # 4. Construct Result
        warnings = []
        if has_inflection:
            level = "warn" if "decline" in inflection_type or "worsening" in inflection_type else "info"
            warnings.append(
                TrendWarning(
                    code="INFLECTION_DETECTED_ADVANCED",
                    level=level,
                    message=f"Structural break detected at year {best_k+1}: {inflection_type}",
                    context={
                        "split_index": int(best_k),
                        "sse_reduction": float(sse_reduction),
                        "slope_change": float(slope_change)
                    },
                )
            )

        return InflectionResult(
            has_inflection=has_inflection,
            inflection_type=inflection_type,
            early_slope=float(best_params.get('slope_l', slope_g)),
            middle_slope=0.0, # Not used in this model
            recent_slope=float(best_params.get('slope_r', slope_g)),
            slope_change=float(slope_change),
            confidence=float(sse_reduction),
            early_r_squared=float(best_params.get('r_l', r_val_g)**2),
            recent_r_squared=float(best_params.get('r_r', r_val_g)**2),
            warnings=warnings,
        )

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
