"""
Log 趋势计算服务（重构版）
=========================

将原来400行的巨型函数拆分为多个职责清晰的小函数。
"""

import logging
import numpy as np
from scipy import stats
from scipy.stats import theilslopes
from typing import List, Optional, Tuple, Dict, Any

from ..trend_models import (
    LogTrendResult,
    DataQualitySummary,
    OutlierDetectionResult,
    TrendWarning,
)
from ..config import TrendAnalysisConfig, get_default_config
from .data_quality import DataQualityChecker
from .outlier_detector import OutlierDetectorFactory

logger = logging.getLogger(__name__)


class LogTrendCalculator:
    """Log 趋势计算器

    将原来400行的calculate_log_trend_slope函数重构为
    职责清晰的类，每个方法只做一件事。
    """

    def __init__(self, config: TrendAnalysisConfig = None):
        """初始化计算器

        Args:
            config: 趋势分析配置
        """
        self.config = config or get_default_config()
        self.quality_checker = DataQualityChecker(self.config)

    def calculate(
        self,
        values: List[float],
        check_outliers: bool = True,
        outlier_method: str = None,
    ) -> LogTrendResult:
        """计算 Log 趋势（主入口）

        Args:
            values: 数据列表
            check_outliers: 是否检测异常值
            outlier_method: 异常值检测方法

        Returns:
            Log 趋势结果
        """
        outlier_method = outlier_method or self.config.default_outlier_method

        # 1. 准备数据
        values_array = self.quality_checker.ensure_window(values)
        values_original = values_array.copy()

        # 2. 异常值检测与清洗
        outlier_result, values_cleaned, used_cleaned = self._handle_outliers(
            values_array, check_outliers, outlier_method
        )

        # 3. 数据质量评估
        quality_summary = self._assess_data_quality(
            values_original, values_cleaned
        )

        # 4. 趋势计算
        trend_metrics = self._compute_trend_metrics(values_cleaned)

        # 5. 鲁棒性检查
        robust_metrics = self._compute_robust_metrics(
            values_cleaned, trend_metrics
        )

        # 6. CAGR 计算
        cagr_approx = self._compute_cagr(
            values_original, quality_summary, trend_metrics
        )

        # 7. 构建结果
        return self._build_result(
            trend_metrics=trend_metrics,
            robust_metrics=robust_metrics,
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
        """处理异常值

        Returns:
            (异常值检测结果, 清洗后的值, 是否使用了清洗)
        """
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
            logger.warning(f"异常值检测失败，使用原始数据: {exc}")
            return None, values, False

    def _assess_data_quality(
        self,
        original: np.ndarray,
        cleaned: np.ndarray,
    ) -> DataQualitySummary:
        """评估数据质量

        Returns:
            数据质量摘要
        """
        quality_original = self.quality_checker.classify_quality(original)
        quality_cleaned = self.quality_checker.classify_quality(cleaned)

        # 选择更差的质量评级（避免清洗掩盖风险）
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

    def _compute_trend_metrics(
        self,
        values: np.ndarray,
    ) -> Dict[str, Any]:
        """计算趋势指标

        Returns:
            趋势指标字典
        """
        years = np.arange(values.size)

        # 使用 asinh 双曲变换（可处理负值和0）
        transformed = np.arcsinh(values)
        crosses_zero = bool(np.any(values < 0) and np.any(values > 0))

        # Log 回归
        log_slope, log_intercept, r_value, p_value, std_err = stats.linregress(
            years, transformed
        )

        # 线性回归（保留作为对照）
        linear_slope, linear_intercept, _, _, _ = stats.linregress(
            years, values
        )

        return {
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
        }

    def _compute_robust_metrics(
        self,
        values: np.ndarray,
        trend_metrics: Dict[str, Any],
    ) -> Dict[str, Any]:
        """计算鲁棒性指标（Theil-Sen）

        Returns:
            鲁棒性指标字典
        """
        if len(values) < 2:
            return {
                'robust_status': 'insufficient_data',
                'robust_slope': float('nan'),
                'robust_intercept': float('nan'),
                'robust_ci_low': float('nan'),
                'robust_ci_high': float('nan'),
                'robust_ci_width': float('nan'),
                'robust_gap': float('nan'),
                'robust_error': None,
                'robust_warnings': [],
            }

        years = trend_metrics['years']
        transformed = trend_metrics['transformed']
        log_slope = trend_metrics['log_slope']
        r_squared = trend_metrics['r_squared']

        try:
            robust_slope, robust_intercept, robust_ci_low, robust_ci_high = theilslopes(
                transformed,
                years,
                alpha=self.config.robust_alpha,
            )

            robust_ci_width = float(robust_ci_high - robust_ci_low)
            robust_gap = abs(log_slope - robust_slope)

            # 检查差异
            robust_warnings = []
            if robust_gap > self.config.robust_gap_threshold and r_squared < 0.7:
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

            return {
                'robust_status': 'ok',
                'robust_slope': float(robust_slope),
                'robust_intercept': float(robust_intercept),
                'robust_ci_low': float(robust_ci_low),
                'robust_ci_high': float(robust_ci_high),
                'robust_ci_width': robust_ci_width,
                'robust_gap': robust_gap,
                'robust_error': None,
                'robust_warnings': robust_warnings,
            }

        except Exception as exc:
            return {
                'robust_status': 'error',
                'robust_slope': float('nan'),
                'robust_intercept': float('nan'),
                'robust_ci_low': float('nan'),
                'robust_ci_high': float('nan'),
                'robust_ci_width': float('nan'),
                'robust_gap': float('nan'),
                'robust_error': str(exc),
                'robust_warnings': [
                    TrendWarning(
                        code="ROBUST_SLOPE_FALLBACK",
                        level="info",
                        message="Theil–Sen 鲁棒斜率计算失败，已保留OLS结果",
                        context={"error": str(exc)},
                    )
                ],
            }

    def _compute_cagr(
        self,
        values: np.ndarray,
        quality: DataQualitySummary,
        trend_metrics: Dict[str, Any],
    ) -> float:
        """计算 CAGR 近似值

        Returns:
            CAGR 或 NaN
        """
        # 有亏损、穿零或负值时不计算 CAGR
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
        robust_metrics: Dict[str, Any],
        cagr_approx: float,
        quality_summary: DataQualitySummary,
        outlier_result: Optional[OutlierDetectionResult],
        used_cleaned: bool,
        outlier_method: str,
        check_outliers: bool,
    ) -> LogTrendResult:
        """构建结果对象

        Returns:
            LogTrendResult 对象
        """
        # 收集所有警告
        warnings = []

        # 异常值警告
        if outlier_result:
            warnings.extend(outlier_result.warnings)

        # 数据质量警告
        if quality_summary.effective == "poor":
            warnings.append(
                TrendWarning(
                    code="DATA_QUALITY_POOR",
                    level="warn",
                    message="数据质量被评估为差，趋势结果需谨慎解读",
                    context={
                        "original": quality_summary.original,
                        "cleaned": quality_summary.cleaned,
                    },
                )
            )
        elif quality_summary.effective in ("has_loss", "has_near_zero"):
            warnings.append(
                TrendWarning(
                    code="DATA_QUALITY_WEAK",
                    level="info",
                    message="存在亏损或低基数年份，趋势敏感度降低",
                    context={"effective": quality_summary.effective},
                )
            )

        # 鲁棒性警告
        if robust_metrics['robust_warnings']:
            warnings.extend(robust_metrics['robust_warnings'])

        # 构建元数据
        metadata = {
            "log_transform": "asinh",
            "periods_used": len(trend_metrics['years']),
            "outlier_method": outlier_result.method if outlier_result else (
                outlier_method if check_outliers else None
            ),
            "outlier_threshold": outlier_result.threshold if outlier_result else None,
            "robust_method": "theil_sen",
            "robust_alpha": self.config.robust_alpha,
            "robust_status": robust_metrics['robust_status'],
            "robust_ci_width": robust_metrics['robust_ci_width'] if np.isfinite(
                robust_metrics['robust_ci_width']
            ) else None,
            "robust_slope_gap": robust_metrics['robust_gap'] if np.isfinite(
                robust_metrics['robust_gap']
            ) else None,
        }

        if robust_metrics['robust_error']:
            metadata["robust_error"] = robust_metrics['robust_error']

        return LogTrendResult(
            log_slope=trend_metrics['log_slope'],
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
            robust_slope=robust_metrics['robust_slope'],
            robust_intercept=robust_metrics['robust_intercept'],
            robust_slope_ci_low=robust_metrics['robust_ci_low'],
            robust_slope_ci_high=robust_metrics['robust_ci_high'],
            metadata=metadata,
            warnings=warnings,
        )
