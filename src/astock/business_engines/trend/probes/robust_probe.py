"""
稳健趋势探针 (Robust Trend Probe)
=================================

使用非参数统计方法评估趋势的稳健性，专为短序列（5-10年财务数据）设计。

核心方法：
1. Theil-Sen 估算器：基于中位数斜率，breakdown point = 29.3%
   - 对最多约30%的异常值具有抵抗能力
   - 比 OLS 更稳健，但保持较好的统计效率（渐近相对效率 AREs ≈ 0.98）

2. Mann-Kendall 趋势检验：非参数单调趋势检验
   - 不假设数据分布（无正态性要求）
   - 基于秩次，对异常值不敏感
   - 适用于检测是否存在单调增/减趋势

理论依据：
- Sen, P. K. (1968). "Estimates of the Regression Coefficient Based on Kendall's Tau"
- Mann, H. B. (1945). "Nonparametric Tests Against Trend"
- Kendall, M. G. (1975). "Rank Correlation Methods"

注意：本探针在原始尺度上计算斜率，保持业务可解释性。
趋势显著性检验使用原始数据（基于秩，单调变换不影响结果）。

作者: AStock Analysis System
日期: 2025-12-07
"""

import logging
import math
import numpy as np
from scipy.stats import theilslopes, norm
from scipy.special import comb
from typing import List, Tuple

from ..models import (
    RobustTrendResult,
    TrendWarning,
    MetricProbeContext,
)
from ..config import get_default_config

logger = logging.getLogger(__name__)


# ============================================================================
# Mann-Kendall 趋势检验 (专业实现)
# ============================================================================

def _mk_exact_p_value(s: int, n: int) -> float:
    """Mann-Kendall S 统计量的精确双侧 p 值 (n ≤ 10, 无 ties)

    v8.1: 使用 Kendall (1975) 递推计数算法替代正态近似。
    对于 n≤10，正态近似可能产生显著偏差（例如 n=5 时误差可达 15%）。
    精确测试通过枚举所有排列的 S 分布来完成。

    算法: 动态规划计算 S 在 H0 (无趋势) 下的精确分布
    - count(k, s) = 前 k 个元素排列中 S=s 的排列数
    - 当第 k 个元素插入排名 r 时, 贡献 c = 2r - k - 1
    时间复杂度: O(n² × max_S), n=10 时 max_S=45, 完全可行

    References:
        - Kendall, M. G. (1975). Rank Correlation Methods, 4th ed. Griffin.
        - Gilbert, R. O. (1987). Statistical Methods for Environmental Pollution Monitoring.
    """
    # 动态规划: 逐步构建 S 的精确分布
    prev: dict[int, int] = {0: 1}  # count(1, 0) = 1

    for k in range(2, n + 1):
        curr: dict[int, int] = {}
        for s_prev, cnt in prev.items():
            for r in range(1, k + 1):
                # 第 k 个元素排名为 r 时对 S 的贡献
                contribution = 2 * r - k - 1
                s_new = s_prev + contribution
                curr[s_new] = curr.get(s_new, 0) + cnt
        prev = curr

    # 总排列数 = n! (校验一致性)
    total = sum(prev.values())

    # 双侧 p 值: P(|S| >= |s_observed|)
    abs_s = abs(s)
    count_extreme = sum(cnt for s_val, cnt in prev.items() if abs(s_val) >= abs_s)

    return count_extreme / total


def mann_kendall_test(y: np.ndarray) -> Tuple[float, float, str]:
    """
    Mann-Kendall 趋势检验

    这是正确的 Mann-Kendall 实现，用于检测时间序列中的单调趋势。
    与 scipy.stats.kendalltau 的区别：
    - kendalltau: 检验两个变量 X, Y 的相关性
    - Mann-Kendall: 检验单个序列 Y 是否随时间有单调趋势

    对于等间隔时间序列 x = [0,1,2,...,n-1]，两者数值上相等，
    但 Mann-Kendall 的语义更准确，且提供了标准化的统计量 Z。

    算法：
    1. 计算 S = Σ sign(y_j - y_i) for all i < j
    2. 计算 Var(S)，考虑 ties 修正
    3. 计算标准化 Z 统计量，使用正态近似（n > 10 时有效）

    Args:
        y: 时间序列数据（按时间顺序）

    Returns:
        (tau, p_value, trend_direction)
        - tau: Kendall's τ 相关系数，范围 [-1, 1]
        - p_value: 双侧检验 p 值
        - trend_direction: "increasing" / "decreasing" / "no_trend"

    References:
        - Mann, H. B. (1945). Econometrica, 13, 245-259.
        - Kendall, M. G. (1975). Rank Correlation Methods.
        - Gilbert, R. O. (1987). Statistical Methods for Environmental Pollution Monitoring.
    """
    n = len(y)

    if n < 3:
        return 0.0, 1.0, "no_trend"

    # 步骤1: 计算 S 统计量
    # S = Σ sign(y_j - y_i) for all i < j
    s = 0
    for i in range(n - 1):
        for j in range(i + 1, n):
            diff = y[j] - y[i]
            if diff > 0:
                s += 1
            elif diff < 0:
                s -= 1
            # diff == 0 时不计入 (tie)

    # 步骤2: 计算 Kendall's τ
    # τ = S / (n * (n-1) / 2)
    # 这是 S 的标准化版本
    n_pairs = n * (n - 1) / 2
    tau = s / n_pairs if n_pairs > 0 else 0.0

    # 步骤3: 计算方差 Var(S)，需要考虑 ties 修正
    # 基础方差（无 ties）: Var(S) = n(n-1)(2n+5) / 18
    var_s = n * (n - 1) * (2 * n + 5) / 18

    # Ties 修正: 减去 Σ t_i(t_i-1)(2t_i+5) / 18
    # 其中 t_i 是第 i 组相同值的个数
    unique, counts = np.unique(y, return_counts=True)
    tie_groups = counts[counts > 1]  # 只考虑有重复的组

    if len(tie_groups) > 0:
        tie_correction = np.sum(
            tie_groups * (tie_groups - 1) * (2 * tie_groups + 5)
        ) / 18
        var_s -= tie_correction

    # 确保方差非负
    var_s = max(var_s, 1e-10)

    # 步骤4: 计算标准化 Z 统计量（带连续性修正）
    # Z = (S - sign(S)) / sqrt(Var(S))
    if s > 0:
        z = (s - 1) / math.sqrt(var_s)
    elif s < 0:
        z = (s + 1) / math.sqrt(var_s)
    else:
        z = 0.0

    # 步骤5: 计算双侧 p 值
    # v8.1: n ≤ 10 且无 ties 时使用精确分布 (Kendall 1975)
    # 正态近似在 n < 10 时误差可达 15% (Gilbert 1987, Table A18)
    # 有 ties 时精确分布改变, 退回正态近似 + ties 修正 (仍然合理)
    if n <= 10 and len(tie_groups) == 0:
        p_value = _mk_exact_p_value(s, n)
    else:
        # n > 10 或有 ties: 正态近似足够准确
        p_value = 2 * norm.sf(abs(z))  # 双侧检验

    # 确定趋势方向（基于显著性水平 0.05）
    if p_value < 0.05:
        if tau > 0:
            trend_direction = "increasing"
        else:
            trend_direction = "decreasing"
    else:
        trend_direction = "no_trend"

    return float(tau), float(p_value), trend_direction


def compute_sen_slope_efficiency(n: int) -> float:
    """
    计算 Theil-Sen 估算器相对于 OLS 的渐近相对效率 (ARE)

    对于正态分布数据：ARE ≈ 0.98（接近 OLS 效率）
    对于重尾分布：ARE > 1（比 OLS 更有效）

    Args:
        n: 样本量

    Returns:
        ARE 估计值
    """
    # 对于正态分布，Theil-Sen 的 ARE ≈ (3/π) ≈ 0.955
    # 实际效率随样本量略有变化
    base_are = 3 / math.pi  # ≈ 0.9549

    # 小样本修正
    if n < 10:
        return base_are * (1 - 1 / (2 * n))
    return base_are


class RobustTrendProbe:
    """
    稳健趋势分析探针

    核心指标：
    1. Theil-Sen Slope: 稳健斜率（基于所有点对斜率的中位数）
       - 对最多 29.3% 的异常值具有抵抗能力
       - 在原始尺度上计算，保持业务可解释性

    2. Mann-Kendall Tau (τ): 趋势单调性系数
       - 范围 [-1, 1]，正值表示上升趋势，负值表示下降趋势
       - 基于秩次，不受异常值和非正态分布影响

    3. Mann-Kendall P-value: 趋势显著性
       - 检验原假设 H0: 无趋势 vs H1: 存在单调趋势
       - p < 0.05 表示趋势在统计上显著

    4. 置信区间 (95%): Theil-Sen 斜率的非参数置信区间
       - 基于斜率分布的分位数
       - 不依赖正态性假设

    与 OLS 对比的意义：
    - 若 OLS slope ≈ Theil-Sen slope: 数据较干净，无明显异常值
    - 若两者差异大: 存在异常值或杠杆点，应以 Theil-Sen 为准
    """

    name = "robust"
    fatal = False

    def __init__(self):
        self.config = get_default_config()

    def compute(self, values: List[float], **kwargs) -> RobustTrendResult:
        """
        计算稳健趋势指标

        Args:
            values: 时间序列数据（按时间顺序，从早到晚）
            **kwargs: 可选参数
                - context (MetricProbeContext): 探针上下文（可选）

        Returns:
            RobustTrendResult 包含稳健斜率和 Mann-Kendall 检验结果
        """
        # 从 kwargs 获取可选的 context
        context = kwargs.get('context')
        group_key = context.group_key if context else "unknown"

        n = len(values)

        if n < 3:
            return self._default_result(
                group_key,
                "INSUFFICIENT_DATA",
                f"数据点不足（需要 ≥3，实际 {n}）"
            )

        try:
            y = np.array(values, dtype=float)
            x = np.arange(n, dtype=float)

            # 检查数据有效性
            if np.any(~np.isfinite(y)):
                valid_mask = np.isfinite(y)
                if valid_mask.sum() < 3:
                    return self._default_result(
                        group_key,
                        "INVALID_DATA",
                        "有效数据点不足"
                    )
                y = y[valid_mask]
                x = np.arange(len(y), dtype=float)

            # ================================================================
            # 1. Theil-Sen 稳健斜率估计
            # ================================================================
            # 在原始尺度上计算，保持业务可解释性
            # 例如：ROIC 从 10% 到 15%，斜率 = 1pp/年
            #
            # method='separate': 使用 Conover (1999) 的方法计算置信区间
            # 这比 'joint' 方法在小样本下更稳健
            slope, intercept, lo_slope, hi_slope = theilslopes(
                y, x,
                alpha=0.95,      # 95% 置信区间
                method='separate'  # Conover 方法，小样本更稳定
            )

            # ================================================================
            # 2. Mann-Kendall 趋势检验
            # ================================================================
            # 使用原始数据进行检验
            # 注意：Mann-Kendall 基于秩，对单调变换不变
            # 但在原始尺度上检验语义更清晰
            tau, p_value, trend_direction = mann_kendall_test(y)

            # ================================================================
            # 3. 生成警告（如果需要）
            # ================================================================
            warnings = []

            # 检查置信区间是否跨越零
            if lo_slope < 0 < hi_slope:
                warnings.append(TrendWarning(
                    code="SLOPE_CI_CROSSES_ZERO",
                    level="info",
                    message=f"斜率置信区间跨越零 [{lo_slope:.4f}, {hi_slope:.4f}]，趋势方向不确定",
                    context={"ci_low": lo_slope, "ci_high": hi_slope}
                ))

            # 检查 τ 与斜率符号是否一致（应该一致）
            if (tau > 0 and slope < 0) or (tau < 0 and slope > 0):
                warnings.append(TrendWarning(
                    code="TAU_SLOPE_INCONSISTENT",
                    level="warning",
                    message=f"τ ({tau:.3f}) 与斜率 ({slope:.4f}) 符号不一致，可能存在数据问题",
                    context={"tau": tau, "slope": slope}
                ))

            # 样本量警告
            if n < 5:
                warnings.append(TrendWarning(
                    code="SMALL_SAMPLE",
                    level="info",
                    message=f"样本量较小 (n={n})，置信区间可能较宽",
                    context={"n": n}
                ))

            return RobustTrendResult(
                robust_slope=float(slope),
                robust_intercept=float(intercept),
                robust_slope_ci_low=float(lo_slope),
                robust_slope_ci_high=float(hi_slope),
                mann_kendall_tau=float(tau),
                mann_kendall_p_value=float(p_value),
                is_valid=True,
                warnings=warnings
            )

        except Exception as e:
            logger.warning(
                f"RobustTrendProbe computation failed for {group_key}: {e}",
                exc_info=True
            )
            return self._default_result(
                group_key,
                "COMPUTATION_ERROR",
                f"计算异常: {str(e)}"
            )

    def _default_result(
        self,
        group_key: str,
        error_code: str,
        message: str
    ) -> RobustTrendResult:
        """生成默认（无效）结果"""
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
                    code=error_code,
                    level="warning",
                    message=message,
                    context={"group_key": group_key}
                )
            ]
        )

    def default(self) -> RobustTrendResult:
        """返回默认结果（符合统一协议）"""
        return self._default_result(
            "unknown",
            "ROBUST_CALC_FAILED",
            "稳健趋势计算失败或数据不足"
        )
