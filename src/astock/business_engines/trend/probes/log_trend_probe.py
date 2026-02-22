"""
对数趋势计算器 (Log Trend Calculator)
=====================================

核心功能：
1. 自适应变换：根据数据特性选择 log / arcsinh 变换
2. 加权最小二乘法 (WLS)：处理异方差问题，近期数据权重更大
3. Bootstrap 置信区间：小样本下替代 t 分布假设
4. 多方法融合：OLS + WLS + 稳健估计

专业性增强 v2.0：
- 支持时间衰减权重（指数/线性）
- 自动检测异方差性（Breusch-Pagan 简化版）
- 提供斜率的置信区间估计

作者: AStock Analysis System
日期: 2025-01-07
"""

import logging
import numpy as np
from scipy import stats
from typing import List, Dict, Any, Optional, Tuple

from shared.performance import probe_timing
from ..models import LogTrendResult, TrendWarning, DataQualitySummary, OutlierDetectionResult
from ..config import TrendAnalysisConfig, get_default_config
from .common import DataQualityChecker, OutlierDetectorFactory
from .fast_stats import fast_linregress, fast_linregress_no_pvalue

logger = logging.getLogger(__name__)


# ============================================================================
# 专业统计工具
# ============================================================================

def weighted_least_squares(
    x: np.ndarray,
    y: np.ndarray,
    weights: np.ndarray
) -> Tuple[float, float, float, float]:
    """
    加权最小二乘法 (WLS) 回归

    用于处理异方差问题：财务数据通常近期波动性不同于早期。

    Args:
        x: 自变量（年份索引）
        y: 因变量（变换后的指标值）
        weights: 权重向量（近期权重更大）

    Returns:
        (slope, intercept, r_squared_weighted, std_err_weighted)
    """
    n = len(x)
    if n < 2:
        return 0.0, 0.0, 0.0, float('inf')

    # 归一化权重
    w = weights / weights.sum()

    # 加权均值
    x_mean = np.sum(w * x)
    y_mean = np.sum(w * y)

    # 加权协方差和方差
    cov_xy = np.sum(w * (x - x_mean) * (y - y_mean))
    var_x = np.sum(w * (x - x_mean) ** 2)

    if var_x < 1e-10:
        return 0.0, float(y_mean), 0.0, float('inf')

    # WLS 斜率和截距
    slope = cov_xy / var_x
    intercept = y_mean - slope * x_mean

    # 加权残差
    y_pred = slope * x + intercept
    residuals = y - y_pred
    ss_res = np.sum(w * residuals ** 2)
    ss_tot = np.sum(w * (y - y_mean) ** 2)

    # 加权 R²
    r_squared = 1.0 - (ss_res / ss_tot) if ss_tot > 1e-10 else 0.0
    r_squared = max(0.0, min(1.0, r_squared))

    # 加权标准误（简化版）
    mse = ss_res / max(n - 2, 1)
    std_err = np.sqrt(mse / var_x) if var_x > 1e-10 else float('inf')

    return float(slope), float(intercept), float(r_squared), float(std_err)


def exponential_decay_weights(n: int, decay_factor: float = 0.15) -> np.ndarray:
    """
    指数衰减权重

    近期数据权重更大，反映"时间价值"：最近的基本面变化更重要。

    Args:
        n: 数据点数量
        decay_factor: 衰减因子（默认0.15，对应约5年半衰期）

    Returns:
        权重数组，最新一年权重最大
    """
    # 年份索引: 0, 1, 2, 3, 4 (0是最早，4是最新)
    t = np.arange(n)
    # exp(decay * t) 让最新年份（t大）权重更大
    weights = np.exp(decay_factor * t)
    return weights


def bootstrap_slope_ci(
    x: np.ndarray,
    y: np.ndarray,
    n_bootstrap: int = 100,
    ci_level: float = 0.95,
    seed: int = None
) -> Tuple[float, float, float]:
    """
    Bootstrap 重采样计算斜率置信区间（向量化实现）

    对于小样本（n=5），t分布假设不可靠。Bootstrap 提供非参数替代。
    使用 NumPy 向量化，比 Python 循环快 10-50 倍。

    Args:
        x: 自变量
        y: 因变量
        n_bootstrap: 重采样次数
        ci_level: 置信水平
        seed: 随机种子（None=随机，用于生产环境；固定值用于测试可重复性）

    Returns:
        (slope_median, ci_lower, ci_upper)
    """
    n = len(x)
    if n < 3:
        return 0.0, float('-inf'), float('inf')

    rng = np.random.default_rng(seed)

    # ========== 向量化 Bootstrap ==========
    # 一次性生成所有 bootstrap 索引: (n_bootstrap, n)
    indices = rng.integers(0, n, size=(n_bootstrap, n))

    # 使用高级索引批量获取 bootstrap 样本
    x_boot = x[indices]  # shape: (n_bootstrap, n)
    y_boot = y[indices]  # shape: (n_bootstrap, n)

    # 向量化计算斜率: slope = Cov(x,y) / Var(x)
    # 对每个 bootstrap 样本计算均值
    x_mean = x_boot.mean(axis=1, keepdims=True)  # (n_bootstrap, 1)
    y_mean = y_boot.mean(axis=1, keepdims=True)  # (n_bootstrap, 1)

    # 计算协方差和方差
    x_centered = x_boot - x_mean  # (n_bootstrap, n)
    y_centered = y_boot - y_mean  # (n_bootstrap, n)

    covariance = (x_centered * y_centered).sum(axis=1)  # (n_bootstrap,)
    variance = (x_centered ** 2).sum(axis=1)  # (n_bootstrap,)

    # 过滤方差过小的样本（避免除零）
    valid_mask = variance > 1e-10

    if valid_mask.sum() < 50:  # 有效样本不足
        return 0.0, float('-inf'), float('inf')

    # 计算斜率（仅对有效样本）
    slopes = covariance[valid_mask] / variance[valid_mask]

    # 过滤非有限值
    slopes = slopes[np.isfinite(slopes)]

    if len(slopes) < 50:
        return 0.0, float('-inf'), float('inf')

    alpha = 1 - ci_level
    ci_lower = np.percentile(slopes, alpha / 2 * 100)
    ci_upper = np.percentile(slopes, (1 - alpha / 2) * 100)
    slope_median = np.median(slopes)

    return float(slope_median), float(ci_lower), float(ci_upper)


def detect_heteroscedasticity(residuals: np.ndarray, x: np.ndarray) -> Tuple[bool, float]:
    """
    简化版异方差检测（基于残差绝对值与x的相关性）

    如果残差的绝对值与时间显著相关，说明存在异方差。

    Returns:
        (has_heteroscedasticity, correlation)
    """
    if len(residuals) < 4:
        return False, 0.0

    abs_residuals = np.abs(residuals)
    correlation, p_value = stats.spearmanr(x, abs_residuals)

    # 相关系数 > 0.5 且 p < 0.3（宽松阈值，因为样本小）
    has_hetero = abs(correlation) > 0.5 and p_value < 0.3

    return bool(has_hetero), float(correlation)

class LogTrendProbe:
    """Log trend probe with adaptive transformation.

    Unified interface following ProbeProtocol:
    - compute(values, **kwargs) -> LogTrendResult
    - default() -> LogTrendResult
    """

    def __init__(self, config: TrendAnalysisConfig = None):
        self.config = config or get_default_config()
        self.quality_checker = DataQualityChecker(self.config)

    def compute(
        self,
        values: List[float],
        check_outliers: bool = True,
        outlier_method: str = None,
        allow_negative: bool = True,  # 新增：是否允许负值，决定变换方法
    ) -> LogTrendResult:
        outlier_method = outlier_method or self.config.default_outlier_method

        values_array = self.quality_checker.ensure_window(values)
        values_original = values_array.copy()

        outlier_result, values_cleaned, used_cleaned = self._handle_outliers(
            values_array, check_outliers, outlier_method
        )

        quality_summary = self._assess_data_quality(
            values_original, values_cleaned
        )

        trend_metrics = self._compute_trend_metrics(values_cleaned, allow_negative)

        cagr_approx = self._compute_cagr(
            values_original, quality_summary, trend_metrics
        )

        return self._build_result(
            trend_metrics=trend_metrics,
            cagr_approx=cagr_approx,
            quality_summary=quality_summary,
            outlier_result=outlier_result,
            used_cleaned=used_cleaned,
            outlier_method=outlier_method,
            check_outliers=check_outliers,
        )

    def _handle_outliers(
        self,
        values: np.ndarray,
        check_outliers: bool,
        method: str,
    ) -> Tuple[Optional[OutlierDetectionResult], np.ndarray, bool]:
        if not check_outliers:
            return None, values, False

        try:
            detector = OutlierDetectorFactory.create(method, self.config)
            outlier_result = detector.detect(values.tolist())

            if outlier_result.has_outliers and outlier_result.cleaning_applied:
                cleaned = np.array(outlier_result.cleaned_values, dtype=float)
                return outlier_result, cleaned, True

            return outlier_result, values, False

        except Exception as exc:
            logger.warning(f"Outlier detection failed: {exc}")
            return None, values, False

    def _assess_data_quality(
        self,
        original: np.ndarray,
        cleaned: np.ndarray,
    ) -> DataQualitySummary:
        quality_original = self.quality_checker.classify_quality(original)
        quality_cleaned = self.quality_checker.classify_quality(cleaned)

        quality_rank = {
            "good": 0,
            "has_near_zero": 1,
            "has_loss": 2,
            "poor": 3
        }

        if quality_rank[quality_original.quality] > quality_rank[quality_cleaned.quality]:
            effective_quality = quality_original.quality
        else:
            effective_quality = quality_cleaned.quality

        return DataQualitySummary(
            original=quality_original.quality,
            cleaned=quality_cleaned.quality,
            effective=effective_quality,
            has_loss_years=quality_original.has_loss_years,
            loss_year_count=quality_original.loss_year_count,
            has_near_zero_years=quality_original.has_near_zero_years,
            near_zero_count=quality_original.near_zero_count,
            has_loss_years_cleaned=quality_cleaned.has_loss_years,
            loss_year_count_cleaned=quality_cleaned.loss_year_count,
            has_near_zero_years_cleaned=quality_cleaned.has_near_zero_years,
            near_zero_count_cleaned=quality_cleaned.near_zero_count,
        )

    def _compute_trend_metrics(self, values: np.ndarray, allow_negative: bool = True) -> Dict[str, Any]:
        """计算趋势指标，使用多方法融合。

        增强功能：
        1. 根据数据特性选择 arcsinh / log 变换
        2. 同时计算 OLS 和 WLS 斜率
        3. Bootstrap 置信区间（小样本）
        4. 自动检测异方差性

        Args:
            values: 原始数值序列
            allow_negative: 是否允许负值
                - True: 使用 arcsinh 变换（处理负值和零）
                - False: 使用 log 变换（仅适用于正值，更准确的CAGR解释）
        """
        years = np.arange(values.size)
        crosses_zero = bool(np.any(values < 0) and np.any(values > 0))

        # 根据 allow_negative 和实际数据选择变换方法
        if allow_negative or crosses_zero or np.any(values <= 0):
            # 使用 arcsinh: 适用于可能为负或零的数据
            # arcsinh(x) ≈ ln(2x) for large x, 但可以处理负值
            transformed = np.arcsinh(values)
            transform_method = "arcsinh"
        else:
            # 使用 log: 适用于恒正数据，斜率直接解释为CAGR
            # log_slope ≈ CAGR (连续复合增长率)
            transformed = np.log(values)
            transform_method = "log"

        # ========== 1. 标准 OLS 回归（使用快速版本） ==========
        log_slope, log_intercept, r_value, p_value, std_err = fast_linregress(
            years, transformed
        )

        linear_slope, linear_intercept, _, _ = fast_linregress_no_pvalue(
            years, values
        )

        # ========== 2. 加权最小二乘 (WLS) ==========
        # 使用指数衰减权重，近期数据权重更大
        weights = exponential_decay_weights(len(years), decay_factor=0.15)
        wls_slope, wls_intercept, wls_r_squared, wls_std_err = weighted_least_squares(
            years, transformed, weights
        )

        # ========== 3. 异方差检测 ==========
        ols_residuals = transformed - (log_slope * years + log_intercept)
        has_heteroscedasticity, hetero_corr = detect_heteroscedasticity(ols_residuals, years)

        # ========== 4. Bootstrap 置信区间 ==========
        # 对于小样本，Bootstrap 比 t 分布更可靠
        # 注: 向量化实现，500次重采样性能优秀且统计更稳定
        boot_median, boot_ci_low, boot_ci_high = bootstrap_slope_ci(
            years, transformed, n_bootstrap=500, ci_level=0.95
        )

        # ========== 5. 融合斜率估计 ==========
        # 如果检测到异方差，更信任 WLS；否则使用 OLS 和 WLS 的加权平均
        if has_heteroscedasticity:
            # 异方差显著时，WLS 权重 70%
            fused_slope = 0.3 * log_slope + 0.7 * wls_slope
            slope_method = "wls_dominant"
        else:
            # 无显著异方差时，OLS 权重 60%（样本量小时OLS更稳定）
            fused_slope = 0.6 * log_slope + 0.4 * wls_slope
            slope_method = "ols_dominant"

        return {
            # 核心指标
            'log_slope': float(log_slope),
            'log_intercept': float(log_intercept),
            'linear_slope': float(linear_slope),
            'linear_intercept': float(linear_intercept),
            'r_value': float(r_value),
            'r_squared': float(r_value ** 2),
            'p_value': float(p_value),
            'std_err': float(std_err),
            'crosses_zero': crosses_zero,
            'transformed': transformed,
            'years': years,
            'transform_method': transform_method,

            # WLS 增强指标
            'wls_slope': float(wls_slope),
            'wls_intercept': float(wls_intercept),
            'wls_r_squared': float(wls_r_squared),
            'wls_std_err': float(wls_std_err),

            # 异方差诊断
            'has_heteroscedasticity': has_heteroscedasticity,
            'heteroscedasticity_correlation': float(hetero_corr),

            # Bootstrap 置信区间
            'bootstrap_slope_median': float(boot_median),
            'bootstrap_ci_low': float(boot_ci_low),
            'bootstrap_ci_high': float(boot_ci_high),

            # 融合估计
            'fused_slope': float(fused_slope),
            'slope_method': slope_method,
        }

    def _compute_cagr(
        self,
        values: np.ndarray,
        quality: DataQualitySummary,
        trend_metrics: Dict[str, Any],
    ) -> float:
        if quality.has_loss_years or trend_metrics['crosses_zero'] or np.any(values <= 0):
            return float('nan')

        period_years = len(values) - 1
        if period_years > 0 and values[0] > 0:
            cagr = (values[-1] / values[0]) ** (1.0 / period_years) - 1.0
            return float(cagr)

        return float('nan')

    def _build_result(
        self,
        trend_metrics: Dict[str, Any],
        cagr_approx: float,
        quality_summary: DataQualitySummary,
        outlier_result: Optional[OutlierDetectionResult],
        used_cleaned: bool,
        outlier_method: str,
        check_outliers: bool,
    ) -> LogTrendResult:
        warnings = []

        if outlier_result:
            warnings.extend(outlier_result.warnings)

        if quality_summary.effective == "poor":
            warnings.append(
                TrendWarning(
                    code="DATA_QUALITY_POOR",
                    level="warn",
                    message="Data quality is poor",
                    context={
                        "original": quality_summary.original,
                        "cleaned": quality_summary.cleaned,
                    },
                )
            )

        # arcsinh 变换时 CAGR 解释警告
        if trend_metrics.get('transform_method') == 'arcsinh':
            warnings.append(
                TrendWarning(
                    code="ARCSINH_CAGR_INTERPRETATION",
                    level="info",
                    message="使用arcsinh变换，log_slope不能直接解释为CAGR。请参考cagr_approx字段获取实际CAGR（仅适用于恒正数据）",
                    context={
                        "transform_method": "arcsinh",
                        "log_slope": trend_metrics.get('log_slope'),
                        "note": "arcsinh(x) ≈ ln(2x) for large x, but differs for small/negative values",
                    },
                )
            )

        # 异方差警告
        if trend_metrics.get('has_heteroscedasticity', False):
            warnings.append(
                TrendWarning(
                    code="HETEROSCEDASTICITY_DETECTED",
                    level="info",
                    message="Heteroscedasticity detected, WLS preferred",
                    context={
                        "correlation": trend_metrics.get('heteroscedasticity_correlation', 0),
                        "slope_method": trend_metrics.get('slope_method', 'unknown'),
                    },
                )
            )

        # OLS 与 WLS 斜率显著差异警告
        ols_slope = trend_metrics.get('log_slope', 0)
        wls_slope = trend_metrics.get('wls_slope', 0)
        if abs(ols_slope - wls_slope) > 0.05:
            warnings.append(
                TrendWarning(
                    code="OLS_WLS_DIVERGENCE",
                    level="info",
                    message=f"OLS({ols_slope:.3f}) and WLS({wls_slope:.3f}) slopes differ significantly",
                    context={
                        "ols_slope": ols_slope,
                        "wls_slope": wls_slope,
                        "difference": abs(ols_slope - wls_slope),
                    },
                )
            )

        metadata = {
            "log_transform": trend_metrics.get('transform_method', 'arcsinh'),
            "periods_used": len(trend_metrics['years']),
            "outlier_method": outlier_result.method if outlier_result else (
                outlier_method if check_outliers else None
            ),
            # 新增专业诊断信息
            "wls_slope": trend_metrics.get('wls_slope'),
            "wls_r_squared": trend_metrics.get('wls_r_squared'),
            "fused_slope": trend_metrics.get('fused_slope'),
            "slope_method": trend_metrics.get('slope_method'),
            "has_heteroscedasticity": trend_metrics.get('has_heteroscedasticity', False),
            "bootstrap_ci": {
                "median": trend_metrics.get('bootstrap_slope_median'),
                "low": trend_metrics.get('bootstrap_ci_low'),
                "high": trend_metrics.get('bootstrap_ci_high'),
            }, 
        }

        return LogTrendResult(
            log_slope=trend_metrics['fused_slope'],  # v4.3: 使用 OLS+WLS 融合斜率
            slope=trend_metrics['linear_slope'],
            intercept=trend_metrics['log_intercept'],
            r_squared=trend_metrics['r_squared'],
            p_value=trend_metrics['p_value'],
            std_err=trend_metrics['std_err'],
            cagr_approx=cagr_approx,
            crosses_zero=trend_metrics['crosses_zero'],
            used_cleaned_data=used_cleaned,
            quality=quality_summary,
            outliers=outlier_result,
            metadata=metadata,
            warnings=warnings,
        )
    def default(self) -> LogTrendResult:
        """Return default result for insufficient data (ProbeProtocol compliance)."""
        return LogTrendResult(
            log_slope=0.0, slope=0.0, intercept=0.0, r_squared=0.0,
            p_value=1.0, std_err=0.0, cagr_approx=0.0, crosses_zero=False,
            used_cleaned_data=False,
            quality=DataQualitySummary(
                original="unknown", cleaned="unknown", effective="unknown",
                has_loss_years=False, loss_year_count=0,
                has_near_zero_years=False, near_zero_count=0,
                has_loss_years_cleaned=False, loss_year_count_cleaned=0,
                has_near_zero_years_cleaned=False, near_zero_count_cleaned=0,
            ),
            outliers=None,
            metadata={},
            warnings=["Insufficient data"],
        )