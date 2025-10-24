"""
趋势默认值与回退
================

提供趋势探针、规则在异常或缺失数据时使用的占位结果，确保整个流水线
即使遇到部分计算失败也能继续运行并输出可控的数据结构。
"""

from __future__ import annotations

from typing import Any, Dict

from .trend_models import (
    CyclicalPatternResult,
    DataQualitySummary,
    InflectionResult,
    LogTrendResult,
    RecentDeteriorationResult,
    RollingTrendResult,
    VolatilityResult,
)


def empty_quality() -> DataQualitySummary:
    """Return a neutral data quality summary."""

    return DataQualitySummary(
        original="unknown",
        cleaned="unknown",
        effective="unknown",
        has_loss_years=False,
        loss_year_count=0,
        has_near_zero_years=False,
        near_zero_count=0,
        has_loss_years_cleaned=False,
        loss_year_count_cleaned=0,
        has_near_zero_years_cleaned=False,
        near_zero_count_cleaned=0,
    )


def empty_log_trend_result() -> LogTrendResult:
    """Return a fallback log trend computation."""

    return LogTrendResult(
        log_slope=0.0,
        slope=0.0,
        intercept=0.0,
        r_squared=0.0,
        p_value=1.0,
        std_err=0.0,
        cagr_approx=0.0,
        crosses_zero=False,
        used_cleaned_data=False,
        quality=empty_quality(),
        outliers=None,
        robust_slope=float("nan"),
        robust_intercept=float("nan"),
        robust_slope_ci_low=float("nan"),
        robust_slope_ci_high=float("nan"),
        metadata={},
        warnings=[],
    )


def empty_volatility_result() -> VolatilityResult:
    return VolatilityResult(
        std_dev=0.0,
        cv=0.0,
        range_ratio=0.0,
        volatility_type="unknown",
        mean_near_zero=False,
        warnings=[],
    )


def empty_inflection_result() -> InflectionResult:
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


def empty_deterioration_result() -> RecentDeteriorationResult:
    return RecentDeteriorationResult(
        has_deterioration=False,
        severity="none",
        year4_to_5_change=0.0,
        year3_to_4_change=0.0,
        year4_to_5_pct=0.0,
        year3_to_4_pct=0.0,
        total_decline_pct=0.0,
        is_high_level_stable=False,
        decline_threshold_pct=-5.0,
        decline_threshold_abs=-2.0,
        industry="default",
        warnings=[],
    )


def empty_cyclical_result() -> CyclicalPatternResult:
    return CyclicalPatternResult(
        is_cyclical=False,
        peak_to_trough_ratio=1.0,
        has_middle_peak=False,
        has_wave_pattern=False,
        trend_r_squared=0.0,
        cv=0.0,
        current_phase="unknown",
        industry_cyclical=False,
        cyclical_confidence=0.0,
        peak_to_trough_threshold=3.0,
        trend_r_squared_max=0.7,
        cv_threshold=0.25,
        industry="default",
        confidence_factors=[],
        warnings=[],
    )


def empty_rolling_result() -> RollingTrendResult:
    return RollingTrendResult(
        recent_3y_slope=0.0,
        recent_3y_r_squared=0.0,
        full_5y_slope=0.0,
        full_5y_r_squared=0.0,
        trend_acceleration=0.0,
        is_accelerating=False,
        is_decelerating=False,
        warnings=[],
    )


