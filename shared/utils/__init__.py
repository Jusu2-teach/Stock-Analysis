"""
Shared Utilities Module
=======================

通用工具函数，供整个项目使用。

模块:
- math: 数学工具函数（clip_01, safe_divide, ewma 等）
- stats: 统计工具函数（compute_cv, compute_detrended_cv 等）
"""

from .math import (
    clip_01,
    safe_divide,
    ewma,
    ewma_series,
    harmonic_mean,
    sigmoid,
    softmax,
)

from .stats import (
    compute_cv,
    compute_detrended_cv,
    compute_peak_to_trough_ratio,
    count_reversals,
    compute_hurst_exponent,
)

__all__ = [
    # math
    "clip_01",
    "safe_divide",
    "ewma",
    "ewma_series",
    "harmonic_mean",
    "sigmoid",
    "softmax",
    # stats
    "compute_cv",
    "compute_detrended_cv",
    "compute_peak_to_trough_ratio",
    "count_reversals",
    "compute_hurst_exponent",
]