
import logging
import numpy as np
from scipy import stats
from typing import List

from ..models import RollingTrendResult, TrendWarning
from ..config import get_default_config
from .common import DataQualityChecker

logger = logging.getLogger(__name__)

class RollingTrendCalculator:
    """Rolling trend calculator."""

    def calculate(self, values: List[float]) -> RollingTrendResult:
        config = get_default_config()
        checker = DataQualityChecker(config)
        values_array = checker.ensure_window(values)

        full_5y_slope = 0.0
        full_5y_r_squared = 0.0
        try:
            years_5 = np.arange(len(values_array))
            slope, _, r_value, _, _ = stats.linregress(years_5, values_array)
            full_5y_slope = float(slope)
            full_5y_r_squared = float(r_value**2)
        except ValueError:
            pass

        recent_3y_slope = 0.0
        recent_3y_r_squared = 0.0
        if len(values_array) >= 3:
            try:
                recent_values = values_array[-3:]
                years_3 = np.arange(3)
                slope, _, r_value, _, _ = stats.linregress(years_3, recent_values)
                recent_3y_slope = float(slope)
                recent_3y_r_squared = float(r_value**2)
            except ValueError:
                pass
        else:
            recent_3y_slope = full_5y_slope
            recent_3y_r_squared = full_5y_r_squared

        trend_acceleration = recent_3y_slope - full_5y_slope
        is_accelerating = trend_acceleration > 0.1
        is_decelerating = trend_acceleration < -0.1

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
