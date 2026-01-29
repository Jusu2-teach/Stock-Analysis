"""
Statistics Utilities - 统计工具函数
====================================

提供项目全局使用的统计工具函数。

设计原则：
1. 纯函数（无副作用）
2. 边界处理（空数组、NaN）
3. 高性能（scipy + numpy）

使用示例：
    from shared.utils import compute_cv, compute_detrended_cv

    cv = compute_cv(np.array([1, 2, 3, 4, 5]))
    detrended_cv = compute_detrended_cv(np.array([1, 2, 3, 4, 5]))
"""

from typing import Tuple, Optional
import numpy as np
from scipy import stats as scipy_stats


def compute_cv(values: np.ndarray) -> float:
    """
    计算变异系数（Coefficient of Variation）。

    CV = 标准差 / 均值绝对值

    Args:
        values: 数值数组

    Returns:
        变异系数，均值接近零时返回 0

    Examples:
        >>> compute_cv(np.array([10, 12, 8, 11, 9]))
        0.14...
    """
    if len(values) == 0:
        return 0.0

    mean_val = np.mean(values)
    if abs(mean_val) < 1e-6:
        return 0.0

    return float(np.std(values) / abs(mean_val))


def compute_detrended_cv(values: np.ndarray) -> float:
    """
    计算去趋势变异系数。

    先用 OLS 回归去除线性趋势，再计算残差的变异系数。
    这样可以区分"稳定增长"和"真正的周期波动"。

    Args:
        values: 数值数组（时间序列）

    Returns:
        去趋势后的变异系数

    Examples:
        >>> # 稳定增长序列，去趋势后 CV 很低
        >>> compute_detrended_cv(np.array([1, 2, 3, 4, 5]))
        0.0
        >>> # 周期波动序列，去趋势后 CV 较高
        >>> compute_detrended_cv(np.array([1, 3, 2, 4, 3]))
        0.3...
    """
    n = len(values)
    if n < 3:
        return compute_cv(values)

    x = np.arange(n)
    slope, intercept, _, _, _ = scipy_stats.linregress(x, values)

    # 去趋势残差
    trend = slope * x + intercept
    residuals = values - trend

    # 残差的标准差 / 均值的绝对值
    mean_val = np.mean(values)
    if abs(mean_val) < 1e-6:
        return 0.0

    return float(np.std(residuals) / abs(mean_val))


def compute_peak_to_trough_ratio(values: np.ndarray) -> float:
    """
    计算峰谷比（Peak-to-Trough Ratio）。

    用于衡量数据的振幅，高峰谷比表示波动剧烈。

    Args:
        values: 数值数组

    Returns:
        峰谷比，最小为 1.0

    Examples:
        >>> compute_peak_to_trough_ratio(np.array([10, 20, 15, 25, 12]))
        2.5  # 25 / 10
    """
    if len(values) == 0:
        return 1.0

    max_val = np.max(values)
    min_val = np.min(values)

    if min_val <= 0:
        # 有负值或零，使用范围/均值
        range_val = max_val - min_val
        mean_val = np.mean(values)
        if abs(mean_val) < 1e-6:
            return 1.0
        return float(1.0 + range_val / abs(mean_val))

    return float(max_val / min_val)


def count_reversals(values: np.ndarray) -> int:
    """
    计算反转次数（方向变化次数）。

    反转指连续两次变化的符号相反。
    高反转次数暗示周期性或震荡特征。

    Args:
        values: 数值数组

    Returns:
        反转次数

    Examples:
        >>> count_reversals(np.array([1, 3, 2, 4, 3]))
        3  # 上、下、上、下
    """
    if len(values) < 3:
        return 0

    diffs = np.diff(values)
    signs = np.sign(diffs)

    # 去除零（连续相等的情况）
    signs = signs[signs != 0]

    if len(signs) < 2:
        return 0

    # 符号变化次数
    return int(np.sum(np.abs(np.diff(signs)) > 0))


def compute_trend_slope(
    values: np.ndarray,
    annualized: bool = True,
) -> Tuple[float, float, float]:
    """
    计算线性趋势斜率。

    Args:
        values: 数值数组
        annualized: 是否返回年化斜率（假设每个数据点间隔 1 年）

    Returns:
        (slope, r_squared, p_value): 斜率、R²、p值

    Examples:
        >>> slope, r2, p = compute_trend_slope(np.array([10, 12, 14, 16, 18]))
        >>> slope
        2.0
        >>> r2
        1.0
    """
    n = len(values)
    if n < 2:
        return 0.0, 0.0, 1.0

    x = np.arange(n)
    slope, intercept, r_value, p_value, std_err = scipy_stats.linregress(x, values)

    r_squared = r_value ** 2

    return float(slope), float(r_squared), float(p_value)


def compute_cagr(values: np.ndarray) -> float:
    """
    计算复合年增长率（CAGR）。

    Args:
        values: 绝对值数组（如营收序列）

    Returns:
        CAGR（小数形式，如 0.15 表示 15%）

    Examples:
        >>> compute_cagr(np.array([100, 110, 121, 133.1]))
        0.10  # 10% CAGR
    """
    n = len(values)
    if n < 2:
        return 0.0

    start_val = values[0]
    end_val = values[-1]

    if start_val <= 0 or end_val <= 0:
        return 0.0

    years = n - 1
    return float((end_val / start_val) ** (1 / years) - 1)


def compute_cagr_from_growth_rates(growth_rates: np.ndarray) -> float:
    """
    从增长率序列计算 CAGR。

    Args:
        growth_rates: 增长率数组（小数形式，如 [0.1, 0.15, 0.2]）

    Returns:
        CAGR

    Examples:
        >>> compute_cagr_from_growth_rates(np.array([0.1, 0.1, 0.1]))
        0.10
    """
    if len(growth_rates) == 0:
        return 0.0

    # 累积增长
    cumulative = np.prod(1 + growth_rates)
    n = len(growth_rates)

    if cumulative <= 0:
        return -1.0

    return float(cumulative ** (1 / n) - 1)


def compute_volatility(
    values: np.ndarray,
    annualized: bool = True,
    periods_per_year: int = 1,
) -> float:
    """
    计算波动率。

    Args:
        values: 数值数组
        annualized: 是否年化
        periods_per_year: 每年的周期数（年度数据=1，季度=4，月度=12）

    Returns:
        波动率

    Examples:
        >>> compute_volatility(np.array([0.1, -0.05, 0.08, -0.02, 0.12]))
        0.07...
    """
    if len(values) < 2:
        return 0.0

    std = np.std(values, ddof=1)

    if annualized:
        return float(std * np.sqrt(periods_per_year))

    return float(std)


def compute_hurst_exponent(values: np.ndarray, max_lag: Optional[int] = None) -> float:
    """
    计算 Hurst 指数，用于判断时间序列特性。

    - H < 0.5: 均值回归（反趋势）
    - H = 0.5: 随机游走
    - H > 0.5: 趋势持续

    Args:
        values: 时间序列数据
        max_lag: 最大滞后期（默认为数据长度的一半）

    Returns:
        Hurst 指数 [0, 1]

    Examples:
        >>> # 随机游走序列，H ≈ 0.5
        >>> compute_hurst_exponent(np.random.randn(100).cumsum())
        0.5...
    """
    n = len(values)
    if n < 20:
        return 0.5  # 数据不足，返回随机游走假设

    if max_lag is None:
        max_lag = min(n // 2, 100)

    lags = range(2, max_lag)
    rs_values = []

    for lag in lags:
        # 计算 R/S 统计量
        chunks = n // lag
        if chunks < 1:
            continue

        rs_list = []
        for i in range(chunks):
            chunk = values[i * lag:(i + 1) * lag]
            mean_chunk = np.mean(chunk)
            deviations = chunk - mean_chunk
            cumulative = np.cumsum(deviations)

            r = np.max(cumulative) - np.min(cumulative)
            s = np.std(chunk)

            if s > 0:
                rs_list.append(r / s)

        if rs_list:
            rs_values.append((lag, np.mean(rs_list)))

    if len(rs_values) < 3:
        return 0.5

    # 对 log(R/S) vs log(lag) 做线性回归
    lags_log = np.log([x[0] for x in rs_values])
    rs_log = np.log([x[1] for x in rs_values])

    slope, _, _, _, _ = scipy_stats.linregress(lags_log, rs_log)

    # H 就是斜率
    return float(np.clip(slope, 0.0, 1.0))
