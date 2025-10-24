"""
趋势分析模块
===========

提供财务指标的趋势分析功能:
1. Log斜率计算 (基于CAGR的专业标准)
2. 线性回归斜率计算 (保留作为对照)
3. 加权平均计算
4. 趋势评估与淘汰规则
"""

import logging
import numpy as np
from scipy import stats
from scipy.stats import theilslopes
from typing import List, Optional, Sequence, Tuple

from .trend_models import (
    CyclicalPatternResult,
    DataQualitySummary,
    InflectionResult,
    LogTrendResult,
    OutlierDetectionResult,
    RecentDeteriorationResult,
    RollingTrendResult,
    TrendWarning,
    VolatilityResult,
)
from .config import (
    get_cyclical_thresholds,
    get_decline_thresholds,
)

logger = logging.getLogger(__name__)

# ========== 配置参数 ==========

# 加权方案: 线性递减(最早→最新)
WEIGHTS = np.array([0.1, 0.15, 0.2, 0.25, 0.3])

# Log斜率阈值 (v2.0 新增)
LOG_SEVERE_DECLINE_SLOPE = -0.30    # 严重衰退: 每年-30% (5年从10%→2.4%)
LOG_MILD_DECLINE_SLOPE = -0.15      # 轻度衰退: 每年-15% (5年从10%→4.4%)

# Log计算安全值
LOG_SAFE_MIN_VALUE = 0.01           # 最小值截断(防止log(0)或log(负数))
MEAN_NEAR_ZERO_EPS = 1e-6          # 均值接近0时的稳健判定阈值
# 鲁棒斜率计算置信区间水平 (Theil–Sen)
ROBUST_ALPHA = 0.95

# 异常值检测阈值 (v2.3 调整)
Z_SCORE_THRESHOLD = 3.0              # Z-score阈值：超过3.0倍标准差视为异常
IQR_MULTIPLIER = 1.5                 # IQR倍数：用于箱线图异常检测
MAD_Z_THRESHOLD = 3.5                # Modified Z-score阈值（默认3.5）
MAD_NORMALIZER = 0.6745              # 将MAD缩放为类标准差的系数

# 默认异常检测方法
DEFAULT_OUTLIER_METHOD = 'mad'

# 置信度因子权重 (v2.2.1 调整)
FACTOR_WEIGHTS = {
    'industry': 0.25,
    'peak_to_trough': 0.20,
    'low_r_squared': 0.20,
    'wave_pattern': 0.15,
    'high_cv': 0.15,
    'middle_peak': 0.05,
}

# 周期性置信度饱和倍率 (v2.2.2 使用对数缩放)
PEAK_TO_TROUGH_SATURATION_MULTIPLIER = 9.0  # 峰谷比达到阈值的9倍即记满分
CV_SATURATION_MULTIPLIER = 4.0             # CV达到阈值的4倍即记满分


def _ensure_window(values: List[float], window: int = 5) -> np.ndarray:
    """Return the latest ``window`` values as a NumPy array.

    Accepts any sequence length >= ``window`` and trims older observations.
    """

    arr = np.asarray(values, dtype=float)
    if arr.size < window:
        raise ValueError(f"需要至少{window}期数据, 实际仅{arr.size}期")
    if arr.size > window:
        arr = arr[-window:]
    return arr


def _classify_quality(values_array: np.ndarray) -> Tuple[str, bool, int, bool, int]:
    """Assess data quality based on亏损/贴近0的期数."""

    loss_mask = values_array < 0
    near_zero_mask = (values_array >= 0) & (values_array < 1.0)

    loss_count = int(np.sum(loss_mask))
    near_zero_count = int(np.sum(near_zero_mask))

    if loss_count >= 2 or near_zero_count >= 2:
        quality = "poor"
    elif loss_count == 1:
        quality = "has_loss"
    elif near_zero_count == 1:
        quality = "has_near_zero"
    else:
        quality = "good"

    return (
        quality,
        bool(loss_count > 0),
        loss_count,
        bool(near_zero_count > 0),
        near_zero_count,
    )


def detect_outliers(
    values: List[float], method: str = DEFAULT_OUTLIER_METHOD
) -> OutlierDetectionResult:
    """
    检测异常值 (v2.1新增 - 数据质量检查)

    核心目的: 识别会计调整、一次性收益等导致的异常数据点

    方法:
    1. Z-Score: |value - mean| > 阈值 * std
    2. IQR: value < Q1 - multiplier*IQR 或 value > Q3 + multiplier*IQR
    3. MAD: Modified Z-Score > 阈值 (对重尾分布更鲁棒)

    Args:
        values: 5年数据列表 [年份1, 年份2, 年份3, 年份4, 年份5]
        method: 检测方法 ('mad', 'z_score' 或 'iqr')

    Returns:
        `OutlierDetectionResult` 数据类，包含异常检测指标、清洗结果与风险提示。
    """
    values_array = _ensure_window(values)
    outlier_indices = []
    outlier_values = []
    method_normalized = (method or DEFAULT_OUTLIER_METHOD).lower()
    threshold_value: Optional[float] = None

    if method_normalized == "z_score":
        mean_val = np.mean(values_array)
        std_val = np.std(values_array, ddof=1)
        if std_val > 0:
            threshold_value = Z_SCORE_THRESHOLD
            z_scores = np.abs((values_array - mean_val) / std_val)
            outlier_mask = z_scores > threshold_value
            outlier_indices = np.where(outlier_mask)[0].tolist()
            outlier_values = values_array[outlier_mask].tolist()
        else:
            pass
    elif method_normalized == "iqr":
        q1 = np.percentile(values_array, 25)
        q3 = np.percentile(values_array, 75)
        iqr = q3 - q1
        threshold_value = IQR_MULTIPLIER
        lower_bound = q1 - threshold_value * iqr
        upper_bound = q3 + threshold_value * iqr
        outlier_mask = (values_array < lower_bound) | (values_array > upper_bound)
        outlier_indices = np.where(outlier_mask)[0].tolist()
        outlier_values = values_array[outlier_mask].tolist()
    elif method_normalized == "mad":
        median_val = np.median(values_array)
        abs_deviation = np.abs(values_array - median_val)
        mad = np.median(abs_deviation)
        threshold_value = MAD_Z_THRESHOLD
        if mad > 0:
            modified_z = (MAD_NORMALIZER * abs_deviation) / mad
            outlier_mask = modified_z > threshold_value
            outlier_indices = np.where(outlier_mask)[0].tolist()
            outlier_values = values_array[outlier_mask].tolist()
        else:
            outlier_mask = np.zeros_like(values_array, dtype=bool)
    else:
        raise ValueError(f"不支持的检测方法: {method}")

    cleaned_values = values_array.copy()
    if len(outlier_indices) > 0:
        median_val = np.median(values_array)
        for idx in outlier_indices:
            cleaned_values[idx] = median_val

    has_outliers = len(outlier_indices) > 0
    cleaning_ratio = len(outlier_indices) / float(values_array.size)
    cleaning_applied = has_outliers

    # v2.1.2新增: 多异常值风险标记
    outlier_count = len(outlier_indices)
    if outlier_count >= 2:
        data_contamination = "high"  # 高度污染：2个或以上异常值
        risk_level = "high"
    elif outlier_count == 1:
        data_contamination = "low"  # 轻度污染：1个异常值
        risk_level = "medium"
    else:
        data_contamination = "none"  # 无污染
        risk_level = "low"

    warnings: List[TrendWarning] = []
    if data_contamination == "high":
        warnings.append(
            TrendWarning(
                code="OUTLIER_HEAVY_CONTAMINATION",
                level="warn",
                message="存在多个异常值, 结果依赖中位数替换",
                context={"count": outlier_count, "method": method_normalized},
            )
        )
    elif has_outliers:
        warnings.append(
            TrendWarning(
                code="OUTLIER_DETECTED",
                level="info",
                message="检测到异常值并使用中位数替换",
                context={"count": outlier_count, "method": method_normalized},
            )
        )

    return OutlierDetectionResult(
        method=method_normalized,
        threshold=threshold_value,
        has_outliers=bool(has_outliers),
        indices=outlier_indices,
        values=outlier_values,
        cleaned_values=cleaned_values.tolist(),
        cleaning_ratio=cleaning_ratio,
        cleaning_applied=cleaning_applied,
        data_contamination=data_contamination,
        risk_level=risk_level,
        warnings=warnings,
    )


def calculate_weighted_average(
    values: List[float], weights: Optional[Sequence[float]] = None
) -> float:
    """
    计算5年加权平均值

    Args:
        values: 5年数据列表 [最早, ..., 最新]

    Returns:
        加权平均值
    """
    if weights is None:
        weight_array = WEIGHTS
        window = weight_array.size
    else:
        weight_array = np.asarray(weights, dtype=float)
        if weight_array.ndim != 1 or weight_array.size == 0:
            raise ValueError("权重列表必须是一维且非空")
        window = int(weight_array.size)

    values_array = _ensure_window(values, window=window)

    total_weight = float(np.sum(weight_array))
    if not np.isfinite(total_weight) or abs(total_weight) < 1e-12:
        raise ValueError("权重和必须为有限且非零值")

    normalized_weights = weight_array / total_weight
    weighted_avg = np.sum(values_array * normalized_weights)

    return float(weighted_avg)


def calculate_log_trend_slope(
    values: List[float],
    check_outliers: bool = True,
    outlier_method: str = DEFAULT_OUTLIER_METHOD,
) -> LogTrendResult:
    """
    计算5年数据的Log斜率趋势 (基于CAGR的专业金融标准)

    v2.1改进: 正确处理负值和微小正值
    - 负值(亏损): 标记为loss_year，不参与log回归
    - 微小正值(0-1%): 标记为near_zero，特殊处理
    - 正常正值(>1%): 正常log回归
    - 2025.10调整: 使用asinh双曲变换保留穿零方向且在0点连续

    核心优势:
    1. 识别基数效应: 10%→20%(翻倍) vs 50%→60%(仅20%增长)
    2. 识别致命衰退: 10%→0%(归零) vs 30%→20%(高基数回落)
    3. 符合金融标准: Log斜率 ≈ 连续复合年化增长率(CAGR)
    4. 处理极端值: 更稳健,不被异常值主导
    5. 正确区分亏损和微利

    Args:
        values: 5年数据列表 [最早, ..., 最新]
        check_outliers: 是否启用异常值检测并使用清洗后数据 (默认启用)
    outlier_method: 异常值检测方法 ('mad', 'z_score' 或 'iqr')

    Returns:
        `LogTrendResult` 数据类，统一封装趋势回归结果与元数据。
    """
    window_values = _ensure_window(values)
    years = np.arange(window_values.size)
    values_original = window_values.astype(float)
    values_to_use = values_original.copy()
    outlier_result: Optional[OutlierDetectionResult] = None
    used_cleaned = False

    if check_outliers:
        try:
            outlier_result = detect_outliers(
                window_values.tolist(), method=outlier_method
            )
            if outlier_result.has_outliers:
                values_to_use = np.array(outlier_result.cleaned_values, dtype=float)
                used_cleaned = bool(outlier_result.cleaning_applied)
        except Exception as exc:  # 防御性处理，异常时退回原数据
            logger.warning("异常值检测失败，使用原始数据: %s", exc)
            outlier_result = None
            values_to_use = values_original.copy()

    values_array = values_to_use

    # v2.1: 数据质量检查（原始数据 + 清洗后数据）
    (
        data_quality_original,
        has_loss_years,
        loss_year_count,
        has_near_zero_years,
        near_zero_count,
    ) = _classify_quality(values_original)

    (
        data_quality_cleaned,
        has_loss_years_cleaned,
        loss_year_count_cleaned,
        has_near_zero_years_cleaned,
        near_zero_count_cleaned,
    ) = _classify_quality(values_array)

    # 选择作为有效数据质量的更差者，避免清洗掩盖风险
    quality_rank = {"good": 0, "has_near_zero": 1, "has_loss": 2, "poor": 3}
    data_quality_effective = data_quality_cleaned
    if quality_rank[data_quality_original] > quality_rank[data_quality_effective]:
        data_quality_effective = data_quality_original

    # v2025.10: 使用asinh双曲变换平滑穿零区域
    crosses_zero = bool(np.any(values_array < 0) and np.any(values_array > 0))

    transformed_values = np.arcsinh(values_array)

    log_slope, log_intercept, r_value, p_value, std_err = stats.linregress(
        years, transformed_values
    )

    # 原始线性回归(保留作为对照)
    linear_slope, linear_intercept, _, _, _ = stats.linregress(years, values_array)

    # 鲁棒Theil–Sen斜率（防范极端值干扰）
    robust_slope = float("nan")
    robust_intercept = float("nan")
    robust_ci_low = float("nan")
    robust_ci_high = float("nan")
    robust_ci_width = float("nan")
    robust_gap = float("nan")
    robust_status = "not_attempted"
    robust_error: Optional[str] = None
    robust_warnings: List[TrendWarning] = []

    if len(values_array) >= 2:
        robust_status = "attempted"
        try:
            robust_slope, robust_intercept, robust_ci_low, robust_ci_high = theilslopes(
                transformed_values,
                years,
                alpha=ROBUST_ALPHA,
            )
            robust_ci_width = float(robust_ci_high - robust_ci_low)
            robust_status = "ok"
        except Exception as exc:  # theilslopes在数据重复或异常情况下可能失败
            robust_status = "error"
            robust_error = str(exc)
            robust_warnings.append(
                TrendWarning(
                    code="ROBUST_SLOPE_FALLBACK",
                    level="info",
                    message="Theil–Sen 鲁棒斜率计算失败，已保留OLS结果",
                    context={"error": str(exc)},
                )
            )
    else:
        robust_status = "insufficient_data"

    if robust_status == "ok" and np.isfinite(log_slope) and np.isfinite(robust_slope):
        robust_gap = abs(log_slope - robust_slope)
        if robust_gap > 0.1 and (r_value**2) < 0.7:
            robust_warnings.append(
                TrendWarning(
                    code="ROBUST_SLOPE_DISCREPANCY",
                    level="info",
                    message="鲁棒Theil–Sen斜率与OLS斜率差异较大，趋势可能受极端值影响",
                    context={
                        "ols_slope": float(log_slope),
                        "robust_slope": float(robust_slope),
                        "gap": float(robust_gap),
                    },
                )
            )

    # CAGR近似: exp(log_slope) - 1
    if has_loss_years or crosses_zero or np.any(values_original <= 0):
        cagr_approx = float("nan")
    else:
        period_years = len(values_array) - 1
        if period_years > 0 and values_array[0] > 0:
            cagr_approx = (values_array[-1] / values_array[0]) ** (
                1.0 / period_years
            ) - 1.0
        else:
            cagr_approx = float("nan")

    quality_summary = DataQualitySummary(
        original=data_quality_original,
        cleaned=data_quality_cleaned,
        effective=data_quality_effective,
        has_loss_years=bool(has_loss_years),
        loss_year_count=int(loss_year_count),
        has_near_zero_years=bool(has_near_zero_years),
        near_zero_count=int(near_zero_count),
        has_loss_years_cleaned=bool(has_loss_years_cleaned),
        loss_year_count_cleaned=int(loss_year_count_cleaned),
        has_near_zero_years_cleaned=bool(has_near_zero_years_cleaned),
        near_zero_count_cleaned=int(near_zero_count_cleaned),
    )

    metadata = {
        "log_transform": "asinh",
        "periods_used": int(len(values_array)),
        "outlier_method": outlier_result.method
        if outlier_result
        else (outlier_method if check_outliers else None),
        "outlier_threshold": outlier_result.threshold if outlier_result else None,
        "robust_method": "theil_sen",
        "robust_alpha": ROBUST_ALPHA,
        "robust_status": robust_status,
        "robust_ci_width": float(robust_ci_width)
        if np.isfinite(robust_ci_width)
        else None,
        "robust_slope_gap": float(robust_gap) if np.isfinite(robust_gap) else None,
    }
    if robust_error:
        metadata["robust_error"] = robust_error

    warnings: List[TrendWarning] = []
    if outlier_result:
        warnings.extend(outlier_result.warnings)

    if data_quality_effective == "poor":
        warnings.append(
            TrendWarning(
                code="DATA_QUALITY_POOR",
                level="warn",
                message="数据质量被评估为差，趋势结果需谨慎解读",
                context={
                    "original": data_quality_original,
                    "cleaned": data_quality_cleaned,
                },
            )
        )
    elif data_quality_effective in ("has_loss", "has_near_zero"):
        warnings.append(
            TrendWarning(
                code="DATA_QUALITY_WEAK",
                level="info",
                message="存在亏损或低基数年份，趋势敏感度降低",
                context={"effective": data_quality_effective},
            )
        )

    if robust_warnings:
        warnings.extend(robust_warnings)

    return LogTrendResult(
        log_slope=float(log_slope),
        slope=float(linear_slope),
        intercept=float(log_intercept),
        r_squared=float(r_value**2),
        p_value=float(p_value),
        std_err=float(std_err),
        cagr_approx=float(cagr_approx),
        crosses_zero=bool(crosses_zero),
        used_cleaned_data=bool(used_cleaned),
        quality=quality_summary,
        outliers=outlier_result,
        robust_slope=float(robust_slope),
        robust_intercept=float(robust_intercept),
        robust_slope_ci_low=float(robust_ci_low),
        robust_slope_ci_high=float(robust_ci_high),
        metadata=metadata,
        warnings=warnings,
    )


def calculate_volatility_metrics(values: List[float]) -> VolatilityResult:
    """
    计算波动率指标 (v2.0新增 - P0优先级)

    核心目的: 区分"低R²稳定"和"低R²波动"

    问题案例:
    1. 稳定企业: [22, 23, 22, 23, 22] → R²=0.10 (波动小,拟合无意义)
    2. 波动企业: [10, 20, 5, 25, 8]  → R²=0.20 (波动大,拟合度低)

    两者R²都低,但风险完全不同!

    Args:
        values: 5年数据列表 [最早, ..., 最新]

    Returns:
        `VolatilityResult` 数据类，聚合波动率指标和风控提示。
    """
    values_array = _ensure_window(values)

    # 标准差(绝对波动)
    std_dev = np.std(values_array, ddof=1)  # 样本标准差

    # 均值
    mean_val = np.mean(values_array)
    mean_abs = abs(mean_val)
    mean_near_zero = mean_abs < MEAN_NEAR_ZERO_EPS

    # 变异系数(CV) = 标准差/均值 (相对波动)
    if mean_near_zero:
        cv = float("inf")
    else:
        cv = std_dev / mean_abs

    # 极差比 = (最大值 - 最小值) / 均值
    range_val = np.max(values_array) - np.min(values_array)
    if mean_near_zero:
        range_ratio = float("inf")
    else:
        range_ratio = range_val / mean_abs

    # v2.1改进: 波动类型分类（基于经验分位数）
    # 参考A股市场实际数据分布：
    # - 25%分位: cv ≈ 0.12
    # - 50%分位: cv ≈ 0.20
    # - 75%分位: cv ≈ 0.35
    # - 90%分位: cv ≈ 0.55
    if mean_near_zero:
        volatility_type = "extreme_volatility"
    elif cv < 0.12:
        volatility_type = "ultra_stable"  # 极稳定（前25%）
    elif cv < 0.20:
        volatility_type = "stable"  # 稳定（25%-50%）
    elif cv < 0.35:
        volatility_type = "moderate"  # 中等波动（50%-75%）
    elif cv < 0.55:
        volatility_type = "volatile"  # 波动（75%-90%）
    else:
        volatility_type = "high_volatility"  # 高波动（90%+）

    warnings: List[TrendWarning] = []
    if volatility_type in ("high_volatility", "extreme_volatility"):
        warnings.append(
            TrendWarning(
                code="HIGH_VOLATILITY",
                level="warn" if volatility_type == "extreme_volatility" else "info",
                message="波动率较高，趋势可信度可能下降",
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


def detect_inflection_point(values: List[float]) -> InflectionResult:
    """
    检测趋势拐点 (v2.0新增 - P1优先级)

    核心目的: 识别趋势反转信号
    - 恶化→好转: 前期下滑,近期回升 (潜在机会)
    - 好转→恶化: 前期上升,近期下滑 (风险警示)

    方法: 分段线性回归对比
    - 前3年趋势 vs 后2年趋势

    Args:
        values: 5年数据列表 [年份1, 年份2, 年份3, 年份4, 年份5]

    Returns:
        `InflectionResult` 数据类，包含分段斜率、拐点判断与置信度。
    """
    values_array = _ensure_window(values)

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
                message=f"检测到趋势拐点: {inflection_type}",
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


def detect_recent_deterioration(
    values: List[float], industry: str = None
) -> RecentDeteriorationResult:
    """
    检测近期恶化 (v2.2改进 - 行业差异化)

    规则:
    1. 最近2年都下滑 (第4→5年, 第3→4年)
    2. 每年跌幅超过行业阈值
    3. 累计跌幅达到一定比例

    Args:
        values: 5年数据列表 [年份1, 年份2, 年份3, 年份4, 年份5]
        industry: 行业名称（可选，v2.2新增）

    Returns:
        `RecentDeteriorationResult` 数据类，暴露恶化程度、行业阈值与提示信息。
    """
    values_array = _ensure_window(values)
    year3, year4, year5 = values_array[2], values_array[3], values_array[4]

    def pct_change(current: float, previous: float) -> float:
        denominator = max(abs(previous), MEAN_NEAR_ZERO_EPS)
        return ((current - previous) / denominator) * 100.0

    # 第3→4年变化
    change_3_to_4 = year4 - year3
    change_3_to_4_pct = pct_change(year4, year3)

    # 第4→5年变化
    change_4_to_5 = year5 - year4
    change_4_to_5_pct = pct_change(year5, year4)

    # v2.2: 行业差异化下降幅度阈值
    if industry:
        try:
            thresholds = get_decline_thresholds(industry)
            DECLINE_THRESHOLD_PCT = thresholds["decline_threshold_pct"]
            DECLINE_THRESHOLD_ABS = thresholds["decline_threshold_abs"]
            high_level_threshold = thresholds["high_level_threshold"]
        except Exception as e:
            logger.warning(f"获取行业阈值失败（{industry}）: {e}，使用默认值")
            DECLINE_THRESHOLD_PCT = -5.0
            DECLINE_THRESHOLD_ABS = -2.0
            high_level_threshold = 20.0
    else:
        # 默认下降幅度阈值
        DECLINE_THRESHOLD_PCT = -5.0  # 跌幅>5%才算有意义的下滑
        DECLINE_THRESHOLD_ABS = -2.0  # 或绝对值>2个百分点
        high_level_threshold = 20.0

    # 判断是否连续有意义的下滑
    is_meaningful_decline_3_to_4 = (change_3_to_4_pct < DECLINE_THRESHOLD_PCT) or (
        change_3_to_4 < DECLINE_THRESHOLD_ABS
    )

    is_meaningful_decline_4_to_5 = (change_4_to_5_pct < DECLINE_THRESHOLD_PCT) or (
        change_4_to_5 < DECLINE_THRESHOLD_ABS
    )

    consecutive_decline = is_meaningful_decline_3_to_4 and is_meaningful_decline_4_to_5

    # 累计跌幅百分比 (相对于第3年)
    total_decline_pct = pct_change(year5, year3)

    # v2.2: 检测是否高水平稳定（微小波动）- 使用行业阈值
    is_high_level = abs(year5) > high_level_threshold  # 最新值仍然很高（行业差异化）
    is_small_fluctuation = abs(total_decline_pct) < 5.0  # 总波动<5%
    is_high_level_stable = is_high_level and is_small_fluctuation

    # 恶化判断
    has_deterioration = False
    severity = "none"

    if consecutive_decline and not is_high_level_stable:
        has_deterioration = True
        # 严重程度分级
        if total_decline_pct < -30:  # 跌幅>30%
            severity = "severe"
        elif total_decline_pct < -15:  # 跌幅>15%
            severity = "moderate"
        else:  # 跌幅>5%
            severity = "mild"

    warnings: List[TrendWarning] = []
    if has_deterioration:
        warnings.append(
            TrendWarning(
                code="DETERIORATION_DETECTED",
                level="warn" if severity == "severe" else "info",
                message=f"近两期出现{severity}级恶化",
                context={"total_decline_pct": float(total_decline_pct)},
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
        industry=industry if industry else "default",
        warnings=warnings,
    )


def detect_cyclical_pattern(
    values: List[float], industry: str = None
) -> CyclicalPatternResult:
    """
    检测周期性特征 (v2.2改进 - 行业差异化)

    核心目的: 识别周期性行业企业，放宽周期底部的筛选标准

    周期性判断标准（行业差异化）:
    1. 行业属于周期性: 小金属、化工、建材等
    2. 峰谷差异大 + 无明显趋势: max/min > threshold（行业差异）
    3. 有"峰-谷-峰"波动模式
    4. CV > 行业阈值

    Args:
        values: 5年数据列表 [年份1, 年份2, 年份3, 年份4, 年份5]
        industry: 行业名称（可选，v2.2强化）

    Returns:
        `CyclicalPatternResult` 数据类，整合峰谷、波动模式与行业配置后的结论。
    """
    values_array = _ensure_window(values)

    # 周期性行业列表
    CYCLICAL_INDUSTRIES = [
        "小金属",
        "黄金",
        "钢铁",
        "煤炭",
        "石油",
        "化工",
        "化纤",
        "建材",
        "水泥",
        "玻璃",
        "有色",
        "铝",
        "铜",
        "稀土",
        "锂",
        "造纸",
        "航运",
        "港口",
        "航空",
        "房地产",
    ]

    # 1. 行业判断
    industry_cyclical = False
    if industry:
        industry_cyclical = any(cyc_ind in industry for cyc_ind in CYCLICAL_INDUSTRIES)

    # v2.2: 获取行业差异化阈值
    if industry:
        try:
            thresholds = get_cyclical_thresholds(industry)
            peak_to_trough_threshold = thresholds["peak_to_trough_ratio"]
            trend_r_squared_max = thresholds["trend_r_squared_max"]
            cv_threshold = thresholds["cv_min"]
        except Exception as e:
            logger.warning(f"获取行业周期性阈值失败（{industry}）: {e}，使用默认值")
            peak_to_trough_threshold = 3.0
            trend_r_squared_max = 0.7
            cv_threshold = 0.25
    else:
        # 默认阈值
        peak_to_trough_threshold = 3.0
        trend_r_squared_max = 0.7
        cv_threshold = 0.25

    # 2. 峰谷比判断（使用绝对值确保负数同样适用）
    abs_values = np.abs(values_array)
    raw_max = np.max(values_array)
    raw_min = np.min(values_array)

    positive_abs = abs_values[abs_values > MEAN_NEAR_ZERO_EPS]
    min_abs = np.min(positive_abs) if positive_abs.size else MEAN_NEAR_ZERO_EPS
    max_abs = np.max(abs_values) if abs_values.size else 0.0
    peak_to_trough_ratio = max_abs / min_abs if min_abs > 0 else float("inf")

    # 3. 趋势性检查（v2.1新增）- 避免误判高成长
    years = np.arange(values_array.size)
    try:
        _, _, r_value, _, _ = stats.linregress(years, values_array)
        trend_r_squared = r_value**2
    except:
        trend_r_squared = 0.0

    # 4. CV（变异系数）检查（v2.1新增）
    mean_val = np.mean(values_array)
    mean_abs = abs(mean_val)
    std_val = np.std(values_array, ddof=1)
    cv = std_val / mean_abs if mean_abs > MEAN_NEAR_ZERO_EPS else float("inf")

    # 5. 波动模式检查（v2.1新增）- 检测"峰-谷-峰"或"谷-峰-谷"
    # 计算一阶差分
    diffs = np.diff(values_array)
    # 检测方向变化
    sign_changes = np.diff(np.sign(diffs))
    direction_changes = np.sum(sign_changes != 0)

    # 有2次以上方向变化 → 有波动模式
    has_wave_pattern = direction_changes >= 2

    # 6. 中间波峰判断
    middle_values = values_array[1:4]  # 年份2,3,4
    edge_values = [values_array[0], values_array[4]]  # 年份1,5
    middle_max = np.max(middle_values)
    edge_max = np.max(edge_values)
    has_middle_peak = middle_max > edge_max * 1.2  # 中间峰值比边缘高20%以上

    # 7. 当前阶段判断
    latest_value = values_array[-1]
    second_latest = values_array[-2]
    latest_abs = abs(latest_value)
    min_abs_value = np.min(abs_values) if abs_values.size else 0.0

    # 判断是否在波峰/波谷（使用幅度而非符号）
    is_at_peak = max_abs > 0 and latest_abs >= max_abs * 0.9
    trough_baseline = max(min_abs_value, MEAN_NEAR_ZERO_EPS)
    is_at_trough = latest_abs <= trough_baseline * 1.2

    if is_at_peak:
        current_phase = "peak"
    elif is_at_trough:
        current_phase = "trough"
    elif latest_value > second_latest:
        current_phase = "rising"
    elif latest_value < second_latest:
        current_phase = "falling"
    else:
        current_phase = "unknown"

    # 8. 综合判断是否周期性（v2.2.1: 规范化置信度模型）
    def clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
        return max(lower, min(upper, value))

    # 将各因子标准化到0-1范围，避免权重相互压制
    industry_score = 1.0 if industry_cyclical else 0.0

    ratio_score_norm = 0.0
    if peak_to_trough_threshold > 0 and peak_to_trough_ratio > peak_to_trough_threshold:
        ratio_excess = peak_to_trough_ratio / peak_to_trough_threshold
        saturation_base = max(PEAK_TO_TROUGH_SATURATION_MULTIPLIER, 1.0001)
        ratio_score_norm = clamp(np.log(ratio_excess) / np.log(saturation_base))

    trend_score_norm = 0.0
    if trend_r_squared_max > 0:
        trend_score_norm = clamp(1.0 - clamp(trend_r_squared / trend_r_squared_max))

    wave_pattern_score = 1.0 if has_wave_pattern else 0.0

    high_cv_score = 0.0
    if cv_threshold > 0 and cv > cv_threshold:
        cv_excess = cv / cv_threshold
        cv_saturation_base = max(CV_SATURATION_MULTIPLIER, 1.0001)
        high_cv_score = clamp(np.log(cv_excess) / np.log(cv_saturation_base))

    middle_peak_score = 1.0 if has_middle_peak else 0.0

    confidence_score = (
        industry_score * FACTOR_WEIGHTS["industry"]
        + ratio_score_norm * FACTOR_WEIGHTS["peak_to_trough"]
        + trend_score_norm * FACTOR_WEIGHTS["low_r_squared"]
        + wave_pattern_score * FACTOR_WEIGHTS["wave_pattern"]
        + high_cv_score * FACTOR_WEIGHTS["high_cv"]
        + middle_peak_score * FACTOR_WEIGHTS["middle_peak"]
    )

    cyclical_confidence = clamp(confidence_score)
    is_cyclical = cyclical_confidence >= 0.5

    confidence_factors = [
        f"行业周期性={industry_score:.2f}×{FACTOR_WEIGHTS['industry']:.2f}",
        f"峰谷比归一化={ratio_score_norm:.2f}×{FACTOR_WEIGHTS['peak_to_trough']:.2f}",
        f"低R²归一化={trend_score_norm:.2f}×{FACTOR_WEIGHTS['low_r_squared']:.2f}",
        f"波动模式={wave_pattern_score:.2f}×{FACTOR_WEIGHTS['wave_pattern']:.2f}",
        f"高CV归一化={high_cv_score:.2f}×{FACTOR_WEIGHTS['high_cv']:.2f}",
        f"中间波峰={middle_peak_score:.2f}×{FACTOR_WEIGHTS['middle_peak']:.2f}",
    ]

    # 排除高成长企业（v2.1关键改进）
    # 如果R²>0.85且斜率>0，说明是稳定高成长，非周期
    if trend_r_squared > 0.85:
        try:
            slope, _, _, _, _ = stats.linregress(years, values_array)
            if slope > 0:  # 稳定上升趋势
                is_cyclical = False
                cyclical_confidence = 0.0
                confidence_factors.append("高成长排除(-1.0)")
        except:
            pass

    warnings: List[TrendWarning] = []
    if is_cyclical:
        warnings.append(
            TrendWarning(
                code="CYCICAL_PATTERN",
                level="info",
                message="识别到周期性特征，需结合行业阶段判断",
                context={
                    "phase": current_phase,
                    "confidence": float(cyclical_confidence),
                },
            )
        )

    return CyclicalPatternResult(
        is_cyclical=bool(is_cyclical),
        peak_to_trough_ratio=float(peak_to_trough_ratio),
        has_middle_peak=bool(has_middle_peak),
        has_wave_pattern=bool(has_wave_pattern),
        trend_r_squared=float(trend_r_squared),
        cv=float(cv),
        current_phase=current_phase,
        industry_cyclical=bool(industry_cyclical),
        cyclical_confidence=float(cyclical_confidence),
        peak_to_trough_threshold=float(peak_to_trough_threshold),
        trend_r_squared_max=float(trend_r_squared_max),
        cv_threshold=float(cv_threshold),
        industry=industry if industry else "default",
        confidence_factors=confidence_factors,
        warnings=warnings,
    )


def calculate_rolling_trend(values: List[float]) -> RollingTrendResult:
    """
    计算3年滚动窗口趋势 (v2.0新增 - P2功能)

    核心目的: 提供短期趋势判断，补充5年整体趋势

    计算方式:
    - 最近3年 (年份3-5): 短期趋势
    - 对比5年整体趋势，判断是否加速/减速

    Args:
        values: 5年数据列表 [年份1, 年份2, 年份3, 年份4, 年份5]

    Returns:
        `RollingTrendResult` 数据类，给出短期趋势与加速度结论。
    """
    values_array = _ensure_window(values)

    # 最近3年 (年份3,4,5 → 索引2,3,4)
    recent_3y_values = values_array[-3:]
    recent_3y_years = np.arange(recent_3y_values.size)

    # 完整窗口
    full_5y_values = values_array
    full_5y_years = np.arange(full_5y_values.size)

    # 计算最近3年趋势
    try:
        recent_slope, _, recent_r, _, _ = stats.linregress(
            recent_3y_years, recent_3y_values
        )
        recent_r_squared = recent_r**2
    except:
        recent_slope = 0.0
        recent_r_squared = 0.0

    # 计算完整5年趋势
    try:
        full_slope, _, full_r, _, _ = stats.linregress(full_5y_years, full_5y_values)
        full_r_squared = full_r**2
    except:
        full_slope = 0.0
        full_r_squared = 0.0

    # 趋势加速度 = 3年斜率 - 5年斜率
    trend_acceleration = recent_slope - full_slope

    # 加速/减速判断 (阈值: 2个点/年)
    # v2.1: 降低阈值从2.0到1.0,更灵敏地捕捉加速/减速
    ACCELERATION_THRESHOLD = 1.0  # 原为2.0,现改为1.0

    is_accelerating = False
    is_decelerating = False

    if trend_acceleration > ACCELERATION_THRESHOLD:
        is_accelerating = True
    elif trend_acceleration < -ACCELERATION_THRESHOLD:
        is_decelerating = True

    warnings: List[TrendWarning] = []
    if is_accelerating:
        warnings.append(
            TrendWarning(
                code="TREND_ACCELERATING",
                level="info",
                message="短期趋势在加速向上",
                context={"trend_acceleration": float(trend_acceleration)},
            )
        )
    elif is_decelerating:
        warnings.append(
            TrendWarning(
                code="TREND_DECELERATING",
                level="info",
                message="短期趋势在加速下滑",
                context={"trend_acceleration": float(trend_acceleration)},
            )
        )

    return RollingTrendResult(
        recent_3y_slope=float(recent_slope),
        recent_3y_r_squared=float(recent_r_squared),
        full_5y_slope=float(full_slope),
        full_5y_r_squared=float(full_r_squared),
        trend_acceleration=float(trend_acceleration),
        is_accelerating=bool(is_accelerating),
        is_decelerating=bool(is_decelerating),
        warnings=warnings,
    )
