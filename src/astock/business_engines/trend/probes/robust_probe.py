"""
稳健趋势探针 (Robust Trend Probe)
=================================

使用非参数统计方法（Theil-Sen 估算器和 Mann-Kendall 检验）来评估趋势的稳健性。
这对于处理包含异常值或非正态分布的短序列（如5年财务数据）特别有效。
"""

import logging
import numpy as np
from scipy.stats import theilslopes, kendalltau
from typing import List, Any

from ..models import (
    RobustTrendResult,
    TrendWarning,
    MetricProbeContext,
)
from ..config import get_default_config

logger = logging.getLogger(__name__)

class RobustTrendProbe:
    """
    稳健趋势分析探针

    计算指标:
    1. Theil-Sen Slope: 稳健斜率（对异常值不敏感）
    2. Mann-Kendall Tau: 趋势相关性系数
    3. Mann-Kendall P-value: 趋势显著性
    """

    name = "robust"
    fatal = False

    def __init__(self):
        self.config = get_default_config()

    def compute(self, values: List[float], context: MetricProbeContext) -> RobustTrendResult:
        if len(values) < 3:
            return self.default(context)

        try:
            # 准备数据
            y = np.array(values, dtype=float)
            x = np.arange(len(y))

            # 对数变换 (与 LogTrendCalculator 保持一致，分析增长率)
            # 注意：如果数据包含负数或零，arcsinh 是一个好的选择
            y_transformed = np.arcsinh(y)

            # 1. Theil-Sen 估算
            # alpha=0.95 用于计算置信区间
            slope, intercept, lo_slope, hi_slope = theilslopes(y_transformed, x, alpha=0.95)

            # 2. Mann-Kendall 检验
            # 使用变换后的数据或原始数据皆可，因为它是基于秩的（单调变换不影响）
            tau, p_value = kendalltau(x, y_transformed)

            # 3. 差异检测 (与 OLS 比较的逻辑在规则引擎中处理，这里只负责计算)

            return RobustTrendResult(
                robust_slope=float(slope),
                robust_intercept=float(intercept),
                robust_slope_ci_low=float(lo_slope),
                robust_slope_ci_high=float(hi_slope),
                mann_kendall_tau=float(tau),
                mann_kendall_p_value=float(p_value),
                is_valid=True,
                warnings=[]
            )

        except Exception as e:
            logger.warning(f"RobustTrendProbe computation failed for {context.group_key}: {e}")
            return self.default(context)

    def default(self, context: MetricProbeContext) -> RobustTrendResult:
        return RobustTrendResult(
            robust_slope=float('nan'),
            robust_intercept=float('nan'),
            robust_slope_ci_low=float('nan'),
            robust_slope_ci_high=float('nan'),
            mann_kendall_tau=0.0,
            mann_kendall_p_value=1.0,
            is_valid=False,
            warnings=[
                TrendWarning(
                    code="ROBUST_CALC_FAILED",
                    level="info",
                    message="稳健趋势计算失败或数据不足",
                    context={}
                )
            ]
        )
