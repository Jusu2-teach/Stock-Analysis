"""
Math Utilities - 数学工具函数
=============================

提供项目全局使用的数学工具函数，消除重复实现。

设计原则：
1. 纯函数（无副作用）
2. 类型安全（完整类型注解）
3. 边界处理（NaN、Inf、除零）
4. 高性能（numpy 向量化）

使用示例：
    from shared.utils import clip_01, safe_divide, ewma

    value = clip_01(1.5)  # -> 1.0
    ratio = safe_divide(10, 0, default=0.0)  # -> 0.0
    weighted_avg = ewma(np.array([1, 2, 3, 4, 5]))  # -> 加权平均
"""

from typing import Optional, Sequence
import numpy as np


def clip_01(value: float) -> float:
    """
    将值裁剪到 [0, 1] 区间。

    Args:
        value: 输入值

    Returns:
        裁剪后的值，保证在 [0, 1] 范围内

    Examples:
        >>> clip_01(1.5)
        1.0
        >>> clip_01(-0.3)
        0.0
        >>> clip_01(0.7)
        0.7
    """
    if np.isnan(value):
        return 0.0
    return max(0.0, min(1.0, value))


def clip_range(value: float, low: float, high: float) -> float:
    """
    将值裁剪到指定区间 [low, high]。

    Args:
        value: 输入值
        low: 下界
        high: 上界

    Returns:
        裁剪后的值

    Examples:
        >>> clip_range(150, 0, 100)
        100.0
        >>> clip_range(-10, 0, 100)
        0.0
    """
    if np.isnan(value):
        return low
    return max(low, min(high, value))


def safe_divide(
    numerator: float,
    denominator: float,
    default: float = 0.0,
) -> float:
    """
    安全除法，处理除零和 NaN 情况。

    Args:
        numerator: 分子
        denominator: 分母
        default: 当分母为零或 NaN 时返回的默认值

    Returns:
        除法结果，或默认值

    Examples:
        >>> safe_divide(10, 2)
        5.0
        >>> safe_divide(10, 0)
        0.0
        >>> safe_divide(10, float('nan'), default=-1)
        -1.0
    """
    if denominator == 0 or np.isnan(denominator):
        return default
    result = numerator / denominator
    if np.isnan(result) or np.isinf(result):
        return default
    return result


def ewma(
    values: np.ndarray,
    weights: Optional[np.ndarray] = None,
    half_life: Optional[float] = None,
) -> float:
    """
    指数加权移动平均（Exponentially Weighted Moving Average）。

    支持三种模式：
    1. 自定义权重
    2. 指定半衰期（half_life）
    3. 默认权重（根据数据长度自动生成）

    Args:
        values: 数值序列（时间顺序，最早在前）
        weights: 自定义权重数组（可选）
        half_life: 半衰期（年），用于生成指数衰减权重

    Returns:
        加权平均值

    Examples:
        >>> ewma(np.array([1, 2, 3, 4, 5]))  # 默认权重
        3.7
        >>> ewma(np.array([1, 2, 3]), half_life=1.0)  # 半衰期1年
        2.57
    """
    n = len(values)
    if n == 0:
        return 0.0

    if weights is not None:
        # 使用自定义权重
        if len(weights) != n:
            # 权重长度不匹配，重新生成
            weights = None
        else:
            # 归一化
            weights = weights / weights.sum()

    if weights is None:
        if half_life is not None:
            # 基于半衰期生成权重
            # 半衰期 h 年，衰减因子 λ = ln(2) / h
            decay = np.log(2) / half_life
            raw_weights = np.array([np.exp(decay * i) for i in range(n)])
        else:
            # 默认权重（支持不同长度）
            if n == 5:
                raw_weights = np.array([0.10, 0.15, 0.20, 0.25, 0.30])
            elif n == 10:
                raw_weights = np.array([0.03, 0.04, 0.05, 0.07, 0.09,
                                        0.11, 0.13, 0.15, 0.16, 0.17])
            else:
                # 指数衰减，decay=0.8
                decay = 0.8
                raw_weights = np.array([decay ** (n - 1 - i) for i in range(n)])

        weights = raw_weights / raw_weights.sum()

    return float(np.dot(values, weights))


def ewma_series(
    values: np.ndarray,
    span: int = 3,
) -> np.ndarray:
    """
    计算指数加权移动平均序列（返回与输入等长的数组）。

    这是 pandas ewm().mean() 的 numpy 实现。

    Args:
        values: 数值序列
        span: 跨度参数，用于计算衰减因子 alpha = 2 / (span + 1)

    Returns:
        EWMA 序列

    Examples:
        >>> ewma_series(np.array([1, 2, 3, 4, 5]), span=3)
        array([1.  , 1.5 , 2.25, 3.125, 4.0625])
    """
    n = len(values)
    if n == 0:
        return np.array([])

    alpha = 2.0 / (span + 1)
    result = np.zeros(n)
    result[0] = values[0]

    for i in range(1, n):
        result[i] = alpha * values[i] + (1 - alpha) * result[i - 1]

    return result


def harmonic_mean(values: Sequence[float], weights: Optional[Sequence[float]] = None) -> float:
    """
    调和平均数（Harmonic Mean）。

    调和平均数对极端低值敏感，适合用于比率类指标的聚合。

    Args:
        values: 数值序列（必须全部 > 0）
        weights: 可选权重

    Returns:
        调和平均值

    Examples:
        >>> harmonic_mean([1, 2, 4])
        1.714...
        >>> harmonic_mean([10, 20, 30], weights=[1, 2, 1])
        17.14...
    """
    values = np.array(values)

    # 过滤非正值
    valid_mask = values > 0
    if not np.any(valid_mask):
        return 0.0

    valid_values = values[valid_mask]

    if weights is not None:
        weights = np.array(weights)[valid_mask]
        weights = weights / weights.sum()
        return float(1.0 / np.dot(weights, 1.0 / valid_values))
    else:
        return float(len(valid_values) / np.sum(1.0 / valid_values))


def geometric_mean(values: Sequence[float]) -> float:
    """
    几何平均数（Geometric Mean）。

    适合用于增长率的平均。

    Args:
        values: 数值序列（必须全部 > 0）

    Returns:
        几何平均值

    Examples:
        >>> geometric_mean([1, 2, 4])
        2.0
    """
    values = np.array(values)
    valid_mask = values > 0
    if not np.any(valid_mask):
        return 0.0

    valid_values = values[valid_mask]
    return float(np.exp(np.mean(np.log(valid_values))))


def softmax(values: np.ndarray, temperature: float = 1.0) -> np.ndarray:
    """
    Softmax 函数，用于将值转换为概率分布。

    Args:
        values: 输入数组
        temperature: 温度参数，越高分布越平滑

    Returns:
        概率分布数组，和为 1

    Examples:
        >>> softmax(np.array([1, 2, 3]))
        array([0.09, 0.24, 0.67])
    """
    scaled = values / temperature
    exp_values = np.exp(scaled - np.max(scaled))  # 数值稳定性
    return exp_values / exp_values.sum()


def sigmoid(x: float, center: float = 0.5, steepness: float = 8.0) -> float:
    """
    S 型函数（Sigmoid），用于平滑的 0-1 映射。

    Args:
        x: 输入值
        center: 中心点（输出 0.5 的位置）
        steepness: 陡峭度，越大过渡越陡

    Returns:
        [0, 1] 范围内的输出

    Examples:
        >>> sigmoid(0.5, center=0.5, steepness=8)
        0.5
        >>> sigmoid(0.8, center=0.5, steepness=8)
        0.92
    """
    import math
    try:
        return 1.0 / (1.0 + math.exp(-steepness * (x - center)))
    except OverflowError:
        return 0.0 if x < center else 1.0
