
import logging
from typing import List

from ..models import RecentDeteriorationResult, TrendWarning
from ..config import get_default_config, get_decline_thresholds
from .common import DataQualityChecker

logger = logging.getLogger(__name__)

class DeteriorationDetector:
    """Recent deterioration detector."""

    def detect(self, values: List[float], industry: str = None) -> RecentDeteriorationResult:
        config = get_default_config()
        checker = DataQualityChecker(config)
        values_array = checker.ensure_window(values)

        year3, year4, year5 = values_array[2], values_array[3], values_array[4]

        def pct_change(current: float, previous: float) -> float:
            denominator = max(abs(previous), config.mean_near_zero_eps)
            return ((current - previous) / denominator) * 100.0

        change_3_to_4 = year4 - year3
        change_3_to_4_pct = pct_change(year4, year3)
        change_4_to_5 = year5 - year4
        change_4_to_5_pct = pct_change(year5, year4)

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

        is_meaningful_decline_3_to_4 = (change_3_to_4_pct < DECLINE_THRESHOLD_PCT) or (
            change_3_to_4 < DECLINE_THRESHOLD_ABS
        )

        is_meaningful_decline_4_to_5 = (change_4_to_5_pct < DECLINE_THRESHOLD_PCT) or (
            change_4_to_5 < DECLINE_THRESHOLD_ABS
        )

        has_deterioration = False
        severity = "none"

        if is_meaningful_decline_4_to_5 and is_meaningful_decline_3_to_4:
            has_deterioration = True
            severity = "severe"
        elif is_meaningful_decline_4_to_5:
            if year5 < high_level_threshold:
                has_deterioration = True
                severity = "moderate"
            else:
                has_deterioration = True
                severity = "mild"

        total_decline_pct = pct_change(year5, year3)
        is_high_level_stable = (year5 > high_level_threshold) and (abs(total_decline_pct) < 10.0)

        warnings: List[TrendWarning] = []
        if has_deterioration:
            warnings.append(
                TrendWarning(
                    code="DETERIORATION_DETECTED",
                    level="warn" if severity == "severe" else "info",
                    message=f"Recent deterioration detected: {severity}",
                    context={
                        "severity": severity,
                        "change_4_to_5_pct": float(change_4_to_5_pct),
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
        )
