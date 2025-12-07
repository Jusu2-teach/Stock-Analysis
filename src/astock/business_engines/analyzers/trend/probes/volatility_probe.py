"""
波动率分析器 (Volatility Analyzer)
==================================

专业的波动率分析，用于评估财务指标的稳定性。

专业性增强 v2.0：
1. 变异系数 (CV) 分析
2. ARCH 效应检测（波动聚集性）
3. 趋势调整波动率（去趋势后的波动）
4. 异常波动检测

作者: AStock Analysis System
日期: 2025-01-07
"""

import logging
import numpy as np
from scipy import stats
from typing import List, Tuple, Optional

from ..models import VolatilityResult, TrendWarning
from ..config import get_default_config
from .common import DataQualityChecker
from .fast_stats import fast_linregress_no_pvalue

logger = logging.getLogger(__name__)


# ============================================================================
# 专业波动率分析工具
# ============================================================================

def detect_arch_effect(values: np.ndarray) -> Tuple[bool, float, float]:
    """
    简化版 ARCH 效应检测

    ARCH (AutoRegressive Conditional Heteroscedasticity) 效应
    表示波动率具有时间聚集性：大波动后往往跟着大波动。

    对于年度数据(n=5)，使用简化版检测：
    检查相邻年份变化的绝对值是否相关。

    Returns:
        (has_arch_effect, arch_correlation, arch_significance)
    """
    n = len(values)
    if n < 4:
        return False, 0.0, 1.0

    # 计算一阶差分（年度变化）
    changes = np.diff(values)
    abs_changes = np.abs(changes)

    if len(abs_changes) < 3:
        return False, 0.0, 1.0

    # 检测相邻绝对变化的自相关
    # ARCH 效应: 大变化后跟着大变化
    lag1_corr, _ = stats.pearsonr(abs_changes[:-1], abs_changes[1:])

    # 由于样本量小，使用宽松阈值
    # 正相关 > 0.4 视为有聚集效应
    has_arch = lag1_corr > 0.4 if not np.isnan(lag1_corr) else False

    # 计算显著性（基于 Fisher 变换的近似）
    if np.isnan(lag1_corr):
        return False, 0.0, 1.0

    # 样本量太小，p值仅供参考
    n_pairs = len(abs_changes) - 1
    if n_pairs >= 3:
        t_stat = lag1_corr * np.sqrt((n_pairs - 2) / (1 - lag1_corr ** 2 + 1e-10))
        p_value = 2 * (1 - stats.t.cdf(abs(t_stat), n_pairs - 2))
    else:
        p_value = 1.0

    return bool(has_arch), float(lag1_corr), float(p_value)


def detrended_volatility(values: np.ndarray) -> Tuple[float, float]:
    """
    趋势调整波动率

    去除线性趋势后的波动率，更准确地反映"噪音"水平。
    适用于有明显趋势的序列。

    Returns:
        (detrended_std, detrended_cv)
    """
    n = len(values)
    if n < 3:
        return float(np.std(values, ddof=1)), float('inf')

    # 线性回归去趋势（使用快速版本）
    x = np.arange(n, dtype=np.float64)
    slope, intercept, _, _ = fast_linregress_no_pvalue(x, values)
    trend = slope * x + intercept
    residuals = values - trend

    # 残差的标准差
    detrended_std = np.std(residuals, ddof=1)

    # 残差的 CV（相对于原序列均值）
    mean_val = np.mean(values)
    if abs(mean_val) < 1e-10:
        detrended_cv = float('inf')
    else:
        detrended_cv = detrended_std / abs(mean_val)

    return float(detrended_std), float(detrended_cv)


def detect_volatility_regime(values: np.ndarray) -> Tuple[str, float]:
    """
    波动率体制检测

    检测前半段 vs 后半段的波动率变化，识别波动率体制转换。

    Returns:
        (regime_type, volatility_change_ratio)
        - regime_type: "stable", "increasing_vol", "decreasing_vol"
        - volatility_change_ratio: 后半段波动 / 前半段波动
    """
    n = len(values)
    if n < 4:
        return "stable", 1.0

    mid = n // 2

    # 前半段和后半段的波动率
    early_changes = np.abs(np.diff(values[:mid + 1]))
    recent_changes = np.abs(np.diff(values[mid:]))

    if len(early_changes) == 0 or len(recent_changes) == 0:
        return "stable", 1.0

    early_vol = np.mean(early_changes)
    recent_vol = np.mean(recent_changes)

    if early_vol < 1e-10:
        return "increasing_vol" if recent_vol > 1e-10 else "stable", float('inf')

    ratio = recent_vol / early_vol

    if ratio > 1.5:
        regime = "increasing_vol"  # 波动率上升
    elif ratio < 0.67:
        regime = "decreasing_vol"  # 波动率下降
    else:
        regime = "stable"

    return regime, float(ratio)


class VolatilityCalculator:
    """
    增强版波动率计算器

    专业特性：
    - ARCH 效应检测
    - 趋势调整波动率
    - 波动率体制识别
    """

    def calculate(self, values: List[float]) -> VolatilityResult:
        config = get_default_config()
        checker = DataQualityChecker(config)
        values_array = checker.ensure_window(values)

        # ========== 1. 基础波动率指标 ==========
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

        # ========== 2. 趋势调整波动率 ==========
        detrended_std, detrended_cv = detrended_volatility(values_array)

        # ========== 3. ARCH 效应检测 ==========
        has_arch, arch_corr, arch_pvalue = detect_arch_effect(values_array)

        # ========== 4. 波动率体制检测 ==========
        vol_regime, vol_change_ratio = detect_volatility_regime(values_array)

        # ========== 5. 波动率类型分类 ==========
        # 使用趋势调整后的 CV 更准确
        effective_cv = min(cv, detrended_cv) if detrended_cv != float('inf') else cv

        if mean_near_zero:
            volatility_type = "extreme_volatility"
        elif effective_cv < 0.12:
            volatility_type = "ultra_stable"
        elif effective_cv < 0.20:
            volatility_type = "stable"
        elif effective_cv < 0.35:
            volatility_type = "moderate"
        elif effective_cv < 0.55:
            volatility_type = "volatile"
        else:
            volatility_type = "high_volatility"

        # ========== 6. 构建警告 ==========
        warnings: List[TrendWarning] = []

        if volatility_type in ("high_volatility", "extreme_volatility"):
            warnings.append(
                TrendWarning(
                    code="HIGH_VOLATILITY",
                    level="warn" if volatility_type == "extreme_volatility" else "info",
                    message=f"高波动率检测: CV={cv:.2f}, 去趋势CV={detrended_cv:.2f}",
                    context={
                        "cv": float(cv),
                        "detrended_cv": float(detrended_cv),
                        "volatility_type": volatility_type,
                    },
                )
            )

        # ARCH 效应警告
        if has_arch:
            warnings.append(
                TrendWarning(
                    code="ARCH_EFFECT_DETECTED",
                    level="info",
                    message=f"波动聚集效应: 大变化后往往跟着大变化 (相关性={arch_corr:.2f})",
                    context={
                        "arch_correlation": float(arch_corr),
                        "arch_pvalue": float(arch_pvalue),
                    },
                )
            )

        # 波动率体制变化警告
        if vol_regime == "increasing_vol" and vol_change_ratio > 2.0:
            warnings.append(
                TrendWarning(
                    code="VOLATILITY_INCREASING",
                    level="warn",
                    message=f"波动率显著上升: 近期波动是早期的{vol_change_ratio:.1f}倍",
                    context={
                        "regime": vol_regime,
                        "change_ratio": float(vol_change_ratio),
                    },
                )
            )

        return VolatilityResult(
            std_dev=float(std_dev),
            cv=float(cv),
            range_ratio=float(range_ratio),
            volatility_type=volatility_type,
            mean_near_zero=bool(mean_near_zero),
            warnings=warnings,
            # 新增专业指标
            detrended_cv=float(detrended_cv),
            has_arch_effect=bool(has_arch),
            arch_correlation=float(arch_corr),
            volatility_regime=vol_regime,
            volatility_change_ratio=float(vol_change_ratio),
        )
