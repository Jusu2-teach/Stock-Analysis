"""
趋势分析模块
===========

提供财务指标的趋势分析功能:
1. Log斜率计算 (基于CAGR的专业标准)
2. 线性回归斜率计算 (保留作为对照)
3. 加权平均计算
4. 趋势评估与淘汰规则

注意：本模块正在重构中。
- 新代码：使用 services/ 下的服务类
- 旧代码：保留用于向后兼容，将逐步删除
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
    TrendAnalysisConfig,
    get_default_config,
)

# 导入新的服务（重构后的实现）
from .services import (
    DataQualityChecker,
    OutlierDetectorFactory,
    calculate_log_trend_slope as _new_calculate_log_trend_slope,
    detect_cyclical_pattern as _new_detect_cyclical_pattern,
)

logger = logging.getLogger(__name__)

# ========== 配置管理 ==========
# 使用统一的配置类管理所有参数
_config = get_default_config()


# ========== 辅助函数 ==========

def _ensure_window(values: List[float], window: int = 5) -> np.ndarray:
    """确保数据窗口大小，内部使用"""
    checker = DataQualityChecker(_config)
    return checker.ensure_window(values, window)


def detect_outliers(
    values: List[float], method: str = "mad"
) -> OutlierDetectionResult:
    """检测异常值

    Args:
        values: 5年数据列表
        method: 检测方法 ('mad', 'z_score' 或 'iqr')

    Returns:
        OutlierDetectionResult 数据类
    """
    detector = OutlierDetectorFactory.create(method, _config)
    return detector.detect(values)


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
        weight_array = _config.default_weights
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
    outlier_method: str = "mad",
) -> LogTrendResult:
    """计算Log斜率趋势 (基于CAGR的金融标准)

    Args:
        values: 5年数据列表
        check_outliers: 是否启用异常值检测
        outlier_method: 异常值检测方法

    Returns:
        LogTrendResult 数据类
    """
    return _new_calculate_log_trend_slope(
        values=values,
        check_outliers=check_outliers,
        outlier_method=outlier_method,
        config=_config,
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
    mean_near_zero = mean_abs < _config.mean_near_zero_eps

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
        denominator = max(abs(previous), _config.mean_near_zero_eps)
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
    """检测周期性特征

    Args:
        values: 5年数据列表
        industry: 行业名称（可选）

    Returns:
        CyclicalPatternResult 数据类
    """
    return _new_detect_cyclical_pattern(
        values=values,
        industry=industry,
        config=_config,
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
