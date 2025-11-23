
import logging
import numpy as np
from typing import List

from ..models import CyclicalPatternResult, TrendWarning
from ..config import get_default_config, get_cyclical_thresholds
from .common import DataQualityChecker

logger = logging.getLogger(__name__)

class CyclicalPatternDetector:
    """Cyclical pattern detector."""

    def __init__(self, config=None):
        self.config = config or get_default_config()

    def detect(self, values: List[float], industry: str = None) -> CyclicalPatternResult:
        arr = np.array(values, dtype=float)

        if len(arr) < self.config.min_periods:
            return self._insufficient_data_result(len(arr), industry)

        # 1. Leverage Industry Priors
        is_known_cyclical = self.config.is_cyclical_industry(industry)

        # 2. Adjust Thresholds based on Prior
        thresholds = get_cyclical_thresholds(industry)
        if is_known_cyclical:
            # Relax thresholds for known cyclical industries to avoid false negatives
            # due to short 5-year window
            thresholds['cv_threshold'] *= 0.8
            thresholds['peak_valley_ratio'] *= 0.8

        mean = np.mean(arr)
        std = np.std(arr, ddof=1)
        cv = abs(std / mean) if mean != 0 else float('inf')

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

        # 3. Enhanced Detection Logic
        is_cyclical = (
            cv > thresholds['cv_threshold'] and
            peak_valley_ratio > thresholds['peak_valley_ratio']
        )

        # If known cyclical, be more aggressive in detection
        if is_known_cyclical and not is_cyclical:
             # If CV is high enough, assume cyclical even if peak/valley ratio is not perfect
             # (e.g. just one big drop)
             if cv > thresholds['cv_threshold'] * 1.2:
                 is_cyclical = True

        confidence = min(
            (cv / thresholds['cv_threshold']) * 0.5 +
            (peak_valley_ratio / thresholds['peak_valley_ratio']) * 0.5,
            1.0
        )

        if is_known_cyclical:
            confidence = min(confidence * 1.2, 1.0)
            # Force true if confidence is decent
            if confidence > 0.6:
                is_cyclical = True

        current_phase = "unknown"
        if len(arr) >= 2:
            if arr[-1] > arr[-2]:
                current_phase = "rising"
            else:
                current_phase = "falling"

        warnings = []
        if is_cyclical:
            msg = f"Cyclical pattern detected (confidence: {confidence:.2%})"
            if is_known_cyclical:
                msg += " [Industry Prior]"

            warnings.append(
                TrendWarning(
                    code="CYCLICAL_PATTERN_DETECTED",
                    level="info",
                    message=msg,
                    context={
                        "cv": float(cv),
                        "peak_valley_ratio": float(peak_valley_ratio),
                        "is_known_cyclical": is_known_cyclical
                    },
                )
            )

        return CyclicalPatternResult(
            is_cyclical=is_cyclical,
            peak_to_trough_ratio=float(peak_valley_ratio),
            has_middle_peak=len(peaks_indices) > 0,
            has_wave_pattern=len(peaks_indices) > 1 and len(valleys_indices) > 1,
            trend_r_squared=0.0,
            cv=float(cv),
            current_phase=current_phase,
            industry_cyclical=is_known_cyclical,
            cyclical_confidence=float(confidence),
            peak_to_trough_threshold=float(thresholds['peak_valley_ratio']),
            trend_r_squared_max=0.3,
            cv_threshold=float(thresholds['cv_threshold']),
            industry=industry or "unknown",
            confidence_factors=[
                f"CV={cv:.3f} (threshold={thresholds['cv_threshold']:.3f})",
                f"Peak/Valley={peak_valley_ratio:.3f} (threshold={thresholds['peak_valley_ratio']:.3f})",
                f"Industry Prior={is_known_cyclical}"
            ],
            warnings=warnings,
        )

    def _insufficient_data_result(self, count: int, industry: str) -> CyclicalPatternResult:
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
                    message=f"Insufficient data points: {count}",
                    context={},
                )
            ],
        )
