
import logging
import numpy as np
from typing import List

from ..models import VolatilityResult, TrendWarning
from ..config import get_default_config
from .common import DataQualityChecker

logger = logging.getLogger(__name__)

class VolatilityCalculator:
    """Volatility calculator."""

    def calculate(self, values: List[float]) -> VolatilityResult:
        config = get_default_config()
        checker = DataQualityChecker(config)
        values_array = checker.ensure_window(values)

        std_dev = np.std(values_array, ddof=1)
        mean_val = np.mean(values_array)
        mean_abs = abs(mean_val)
        mean_near_zero = mean_abs < config.mean_near_zero_eps

        if mean_near_zero:
            cv = float("inf")
        else:
            cv = std_dev / mean_abs

        range_val = np.max(values_array) - np.min(values_array)
        if mean_near_zero:
            range_ratio = float("inf")
        else:
            range_ratio = range_val / mean_abs

        if mean_near_zero:
            volatility_type = "extreme_volatility"
        elif cv < 0.12:
            volatility_type = "ultra_stable"
        elif cv < 0.20:
            volatility_type = "stable"
        elif cv < 0.35:
            volatility_type = "moderate"
        elif cv < 0.55:
            volatility_type = "volatile"
        else:
            volatility_type = "high_volatility"

        warnings: List[TrendWarning] = []
        if volatility_type in ("high_volatility", "extreme_volatility"):
            warnings.append(
                TrendWarning(
                    code="HIGH_VOLATILITY",
                    level="warn" if volatility_type == "extreme_volatility" else "info",
                    message="High volatility detected",
                    context={"cv": float(cv), "volatility_type": volatility_type},
                )
            )

        return VolatilityResult(
            std_dev=float(std_dev),
            cv=float(cv),
            range_ratio=float(range_ratio),
            volatility_type=volatility_type,
            mean_near_zero=bool(mean_near_zero),
            warnings=warnings,
        )
