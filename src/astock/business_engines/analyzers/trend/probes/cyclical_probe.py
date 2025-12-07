"""
周期性模式检测器 (Cyclical Pattern Detector)
============================================

企业级周期性检测，基于金融计量经济学最佳实践。

核心方法论
----------
1. **Hodrick-Prescott滤波 (HP Filter)**: 分离趋势和周期成分
   - 标准宏观经济学方法 (Hodrick & Prescott, 1997)
   - 年度数据推荐λ=6.25 (Ravn & Uhlig, 2002)

2. **自相关分析 (ACF)**: 检测序列相关性
   - 周期性序列会在滞后k处有显著正自相关（k=周期长度）
   - Ljung-Box检验验证显著性

3. **峰谷检测 + 周期规则性**:
   - 峰谷间隔的标准差/均值 < 0.3 表示规则周期

4. **去趋势波动分析 (DFA)**: 区分真周期vs随机游走
   - Hurst指数 H ≈ 0.5 随机游走
   - H < 0.5 均值回复（周期性）
   - H > 0.5 趋势持续

5. **行业先验贝叶斯更新**:
   - 先验来自GICS行业周期性分类
   - 用数据特征更新后验概率

关键局限性与前置条件
--------------------
**数据要求**:
- 最少5年数据（能检测部分周期特征）
- 理想10年以上（能检测完整周期）
- 数据频率：年度财务数据

**理论局限**:
- 奈奎斯特定理：检测周期T，至少需要2T长度的数据
- 5年数据最多只能可靠检测2-2.5年周期
- 3-7年商业周期在5年数据上只能做**概率估计**，不能确定性判断

**适用场景**:
- ✅ 行业周期性分类（结合先验）
- ✅ 周期位置估计（顶/底/中部）
- ✅ 波动性vs趋势区分
- ❌ 精确周期长度测定（需要更长数据）

学术参考
--------
- Hodrick, R.J. & Prescott, E.C. (1997). "Postwar US Business Cycles:
  An Empirical Investigation." Journal of Money, Credit and Banking.
- Ravn, M.O. & Uhlig, H. (2002). "On Adjusting the Hodrick-Prescott
  Filter for the Frequency of Observations." Review of Economics and Statistics.
- Hamilton, J.D. (2018). "Why You Should Never Use the Hodrick-Prescott Filter."
  Review of Economics and Statistics. (重要批评，需要注意)
- Harding, D. & Pagan, A. (2002). "Dissecting the Cycle: A Methodological
  Investigation." Journal of Monetary Economics.

作者: AStock Analysis System
日期: 2025-12-07
版本: 2.0 (Professional Edition)
"""

import logging
import warnings
import numpy as np
from scipy import stats
from scipy import signal
from typing import List, Tuple, Optional, Dict, Any
from dataclasses import dataclass, field

from ..models import CyclicalPatternResult, TrendWarning
from ..config import get_default_config, get_cyclical_thresholds
from .common import DataQualityChecker

logger = logging.getLogger(__name__)


# =============================================================================
# 前置条件检查器
# =============================================================================

@dataclass
class CyclicalPreconditions:
    """周期性检测的前置条件评估结果"""
    is_valid: bool                      # 数据是否满足检测条件
    data_years: int                     # 数据年数
    min_detectable_period: float        # 最小可检测周期（年）
    max_reliable_period: float          # 最大可靠检测周期（年）
    confidence_ceiling: float           # 置信度上限（数据局限决定）
    warnings: List[str] = field(default_factory=list)

    @property
    def reliability_grade(self) -> str:
        """数据可靠性等级"""
        if self.data_years >= 15:
            return "A"  # 优秀：可检测完整商业周期
        elif self.data_years >= 10:
            return "B"  # 良好：可检测大部分周期
        elif self.data_years >= 7:
            return "C"  # 中等：有限周期检测能力
        elif self.data_years >= 5:
            return "D"  # 较差：主要依赖先验
        else:
            return "F"  # 不合格：无法检测


def check_cyclical_preconditions(n_years: int) -> CyclicalPreconditions:
    """
    检查周期性检测的前置条件

    基于奈奎斯特采样定理和时间序列分析最佳实践。

    Args:
        n_years: 数据年数

    Returns:
        CyclicalPreconditions 前置条件评估结果
    """
    warnings_list = []

    # 奈奎斯特定理：检测周期T，至少需要2T的数据
    # 实践中，为了可靠检测，最好有3-4个完整周期
    min_detectable = n_years / 2.0  # 理论最小
    max_reliable = n_years / 3.0    # 可靠检测（至少3个周期点）

    # 置信度上限由数据长度决定
    if n_years >= 15:
        confidence_ceiling = 0.95
    elif n_years >= 10:
        confidence_ceiling = 0.85
    elif n_years >= 7:
        confidence_ceiling = 0.70
    elif n_years >= 5:
        confidence_ceiling = 0.55
    else:
        confidence_ceiling = 0.30

    is_valid = n_years >= 5

    if n_years < 5:
        warnings_list.append(
            f"数据不足：{n_years}年数据无法进行可靠的周期性分析"
        )
    elif n_years < 7:
        warnings_list.append(
            f"数据有限：{n_years}年数据只能检测{max_reliable:.1f}年以下的短周期，"
            f"商业周期(3-7年)检测可靠性低"
        )
    elif n_years < 10:
        warnings_list.append(
            f"数据中等：{n_years}年数据可以初步检测商业周期，"
            f"但长周期(>5年)检测可靠性有限"
        )

    return CyclicalPreconditions(
        is_valid=is_valid,
        data_years=n_years,
        min_detectable_period=min_detectable,
        max_reliable_period=max_reliable,
        confidence_ceiling=confidence_ceiling,
        warnings=warnings_list,
    )


@dataclass
class FFTResult:
    """FFT分析结果"""
    dominant_period: Optional[float]  # 主导周期（年）
    period_strength: float            # 周期强度 (0-1)
    is_periodic: bool                 # 是否有显著周期性
    secondary_periods: List[float]    # 次要周期列表
    spectrum_peaks: List[Tuple[float, float]]  # (周期, 功率) 列表


@dataclass
class HPFilterResult:
    """Hodrick-Prescott滤波结果"""
    trend: np.ndarray           # 趋势成分
    cycle: np.ndarray           # 周期成分
    cycle_amplitude: float      # 周期振幅（占趋势比例）
    cycle_volatility: float     # 周期波动率


@dataclass
class AutocorrelationResult:
    """自相关分析结果"""
    acf_values: np.ndarray      # ACF值
    significant_lags: List[int] # 显著滞后期
    ljung_box_pvalue: float     # Ljung-Box检验p值
    has_cyclical_acf: bool      # ACF是否显示周期性模式


@dataclass
class HurstResult:
    """Hurst指数分析结果"""
    hurst_exponent: float       # Hurst指数
    interpretation: str         # 解释: "mean_reverting" | "random_walk" | "trending"
    confidence: float           # 估计置信度


# =============================================================================
# Hodrick-Prescott 滤波器
# =============================================================================

class HPFilter:
    """
    Hodrick-Prescott滤波器

    分离时间序列的趋势和周期成分。

    数学原理:
    最小化: Σ(y_t - τ_t)² + λ·Σ[(τ_{t+1} - τ_t) - (τ_t - τ_{t-1})]²

    其中:
    - y_t: 原始序列
    - τ_t: 趋势成分
    - λ: 平滑参数（年度数据推荐6.25）

    参考:
    - Ravn & Uhlig (2002) 建议年度数据λ=6.25
    - Hamilton (2018) 批评HP滤波器会引入伪周期，需谨慎解读
    """

    def __init__(self, lamb: float = 6.25):
        """
        Args:
            lamb: 平滑参数λ
                  - 年度数据: 6.25 (Ravn & Uhlig推荐)
                  - 季度数据: 1600 (传统值)
                  - 月度数据: 129600
        """
        self.lamb = lamb

    def filter(self, y: np.ndarray) -> HPFilterResult:
        """
        执行HP滤波

        Args:
            y: 时间序列数据

        Returns:
            HPFilterResult 包含趋势和周期成分
        """
        n = len(y)

        if n < 4:
            return HPFilterResult(
                trend=y.copy(),
                cycle=np.zeros_like(y),
                cycle_amplitude=0.0,
                cycle_volatility=0.0,
            )

        # 构建二阶差分矩阵 D
        # D[i, i] = 1, D[i, i+1] = -2, D[i, i+2] = 1
        D = np.zeros((n-2, n))
        for i in range(n-2):
            D[i, i] = 1
            D[i, i+1] = -2
            D[i, i+2] = 1

        # 求解: (I + λ·D'D)·τ = y
        # τ = (I + λ·D'D)^{-1} · y
        I = np.eye(n)
        A = I + self.lamb * (D.T @ D)

        try:
            trend = np.linalg.solve(A, y)
        except np.linalg.LinAlgError:
            # 矩阵奇异，返回原序列
            trend = y.copy()

        cycle = y - trend

        # 计算周期振幅（相对于趋势的波动）
        trend_mean = np.mean(np.abs(trend))
        cycle_amplitude = np.std(cycle) / trend_mean if trend_mean > 0 else 0.0
        cycle_volatility = np.std(cycle)

        return HPFilterResult(
            trend=trend,
            cycle=cycle,
            cycle_amplitude=float(cycle_amplitude),
            cycle_volatility=float(cycle_volatility),
        )


# =============================================================================
# 自相关分析器
# =============================================================================

class AutocorrelationAnalyzer:
    """
    自相关函数(ACF)分析器

    周期性序列的特征:
    - ACF在滞后k处有显著峰值（k=周期长度）
    - ACF缓慢衰减，呈现振荡模式

    非周期序列的特征:
    - ACF快速衰减至0
    - 无显著的周期性峰值
    """

    def __init__(self, max_lag: int = None, significance_level: float = 0.05):
        """
        Args:
            max_lag: 最大滞后期（默认为n//2）
            significance_level: 显著性水平
        """
        self.max_lag = max_lag
        self.significance_level = significance_level

    def analyze(self, y: np.ndarray) -> AutocorrelationResult:
        """
        执行自相关分析

        Args:
            y: 时间序列数据

        Returns:
            AutocorrelationResult 分析结果
        """
        n = len(y)
        max_lag = self.max_lag or min(n // 2, 5)

        if n < 5:
            return AutocorrelationResult(
                acf_values=np.array([1.0]),
                significant_lags=[],
                ljung_box_pvalue=1.0,
                has_cyclical_acf=False,
            )

        # 计算ACF
        y_centered = y - np.mean(y)
        acf_values = np.zeros(max_lag + 1)
        acf_values[0] = 1.0  # ACF(0) = 1

        var_y = np.var(y_centered)
        if var_y == 0:
            return AutocorrelationResult(
                acf_values=acf_values,
                significant_lags=[],
                ljung_box_pvalue=1.0,
                has_cyclical_acf=False,
            )

        for k in range(1, max_lag + 1):
            if k >= n:
                break
            acf_values[k] = np.sum(y_centered[k:] * y_centered[:-k]) / ((n - k) * var_y)

        # 显著性阈值 (Bartlett公式的近似)
        # 95%置信区间约为 ±1.96/√n
        threshold = stats.norm.ppf(1 - self.significance_level / 2) / np.sqrt(n)

        # 找出显著的滞后期（排除lag=0）
        significant_lags = [
            k for k in range(1, len(acf_values))
            if abs(acf_values[k]) > threshold
        ]

        # Ljung-Box检验
        # Q = n(n+2) * Σ(ρ_k² / (n-k))
        Q = 0
        for k in range(1, min(max_lag + 1, n)):
            Q += (acf_values[k] ** 2) / (n - k)
        Q *= n * (n + 2)

        # Q统计量在H0下服从χ²(max_lag)分布
        ljung_box_pvalue = 1 - stats.chi2.cdf(Q, df=max_lag)

        # 判断是否有周期性ACF模式
        # 周期性: ACF在某些滞后期有显著正相关
        has_cyclical_acf = (
            len(significant_lags) > 0 and
            ljung_box_pvalue < self.significance_level and
            any(acf_values[k] > threshold for k in significant_lags)  # 至少有正相关
        )

        return AutocorrelationResult(
            acf_values=acf_values,
            significant_lags=significant_lags,
            ljung_box_pvalue=float(ljung_box_pvalue),
            has_cyclical_acf=has_cyclical_acf,
        )


# =============================================================================
# Hurst指数估计器 (R/S分析)
# =============================================================================

class HurstExponentEstimator:
    """
    Hurst指数估计器 (Rescaled Range Analysis)

    Hurst指数H的解释:
    - H ≈ 0.5: 随机游走（无记忆）
    - H < 0.5: 均值回复（反持续，周期性倾向）
    - H > 0.5: 趋势持续（动量效应）

    对于周期性序列:
    - 通常 H < 0.5，因为周期性意味着均值回复

    注意:
    - 短序列上估计不稳定
    - 至少需要20+数据点才比较可靠
    """

    def estimate(self, y: np.ndarray) -> HurstResult:
        """
        使用R/S方法估计Hurst指数

        Args:
            y: 时间序列数据

        Returns:
            HurstResult 估计结果
        """
        n = len(y)

        # 数据太短，无法可靠估计
        if n < 8:
            return HurstResult(
                hurst_exponent=0.5,
                interpretation="unknown",
                confidence=0.0,
            )

        # 使用不同的分段长度计算R/S
        # 典型选择: 8, 16, 32, ... 直到 n/2
        min_chunk = 4
        max_chunk = n // 2

        chunk_sizes = []
        rs_values = []

        size = min_chunk
        while size <= max_chunk:
            rs = self._compute_rs(y, size)
            if rs is not None and rs > 0:
                chunk_sizes.append(size)
                rs_values.append(rs)
            size = int(size * 1.5) + 1

        if len(chunk_sizes) < 2:
            return HurstResult(
                hurst_exponent=0.5,
                interpretation="unknown",
                confidence=0.0,
            )

        # 对log(R/S) vs log(n)做线性回归
        # 斜率即为Hurst指数
        log_sizes = np.log(chunk_sizes)
        log_rs = np.log(rs_values)

        slope, intercept, r_value, p_value, std_err = stats.linregress(log_sizes, log_rs)

        hurst = float(slope)

        # 限制在合理范围内
        hurst = max(0.0, min(1.0, hurst))

        # 解释
        if abs(hurst - 0.5) < 0.1:
            interpretation = "random_walk"
        elif hurst < 0.5:
            interpretation = "mean_reverting"  # 周期性倾向
        else:
            interpretation = "trending"

        # 置信度基于R²和数据量
        confidence = float(r_value ** 2) * min(n / 20, 1.0)

        return HurstResult(
            hurst_exponent=hurst,
            interpretation=interpretation,
            confidence=float(confidence),
        )

    def _compute_rs(self, y: np.ndarray, chunk_size: int) -> Optional[float]:
        """计算给定分段大小的平均R/S值"""
        n = len(y)
        n_chunks = n // chunk_size

        if n_chunks == 0:
            return None

        rs_list = []
        for i in range(n_chunks):
            chunk = y[i * chunk_size : (i + 1) * chunk_size]

            # 计算累积离差
            mean_chunk = np.mean(chunk)
            cumdev = np.cumsum(chunk - mean_chunk)

            # Range
            R = np.max(cumdev) - np.min(cumdev)

            # Standard deviation
            S = np.std(chunk, ddof=1)

            if S > 0:
                rs_list.append(R / S)

        return np.mean(rs_list) if rs_list else None


# =============================================================================
# 峰谷周期规则性分析器
# =============================================================================

class PeakValleyAnalyzer:
    """
    峰谷周期规则性分析

    通过分析峰谷间隔的规则性来判断周期性:
    - 规则周期: 峰谷间隔的CV < 0.3
    - 不规则波动: 峰谷间隔的CV > 0.5

    使用Bry-Boschan算法的简化版本来识别转折点。
    """

    def __init__(self, min_phase_length: int = 1):
        """
        Args:
            min_phase_length: 最小相位长度（年）
        """
        self.min_phase_length = min_phase_length

    def analyze(self, y: np.ndarray) -> Dict[str, Any]:
        """
        分析峰谷模式

        Args:
            y: 时间序列数据

        Returns:
            包含峰谷信息的字典
        """
        n = len(y)

        if n < 3:
            return {
                "peaks": [],
                "valleys": [],
                "peak_valley_ratio": 1.0,
                "cycle_regularity": 0.0,
                "avg_cycle_length": 0.0,
                "is_regular_cycle": False,
            }

        # 识别局部极值点
        peaks = []
        valleys = []

        for i in range(1, n - 1):
            if y[i] > y[i-1] and y[i] > y[i+1]:
                peaks.append(i)
            elif y[i] < y[i-1] and y[i] < y[i+1]:
                valleys.append(i)

        # 检查端点
        if n >= 2:
            if y[0] > y[1]:
                peaks.insert(0, 0)
            elif y[0] < y[1]:
                valleys.insert(0, 0)

            if y[-1] > y[-2]:
                peaks.append(n - 1)
            elif y[-1] < y[-2]:
                valleys.append(n - 1)

        # 计算峰谷比
        if peaks and valleys:
            peak_values = y[peaks]
            valley_values = y[valleys]

            # 处理负值情况
            if np.min(valley_values) <= 0:
                # 使用中位数比率
                peak_valley_ratio = float(np.median(peak_values) - np.median(valley_values) + 1)
            else:
                peak_valley_ratio = float(np.mean(peak_values) / np.mean(valley_values))
        else:
            peak_valley_ratio = 1.0

        # 计算周期规则性
        # 合并峰谷索引并排序
        turning_points = sorted(peaks + valleys)

        if len(turning_points) >= 2:
            intervals = np.diff(turning_points)
            avg_interval = np.mean(intervals)
            interval_std = np.std(intervals)

            # 周期规则性 = 1 - CV(间隔)
            if avg_interval > 0:
                cycle_cv = interval_std / avg_interval
                cycle_regularity = max(0, 1 - cycle_cv)
            else:
                cycle_regularity = 0.0

            avg_cycle_length = float(avg_interval * 2)  # 完整周期约为2倍间隔
        else:
            cycle_regularity = 0.0
            avg_cycle_length = 0.0

        # 判断是否为规则周期
        is_regular_cycle = (
            len(turning_points) >= 3 and
            cycle_regularity > 0.5 and
            peak_valley_ratio > 1.2
        )

        return {
            "peaks": peaks,
            "valleys": valleys,
            "peak_valley_ratio": peak_valley_ratio,
            "cycle_regularity": float(cycle_regularity),
            "avg_cycle_length": avg_cycle_length,
            "is_regular_cycle": is_regular_cycle,
        }


# =============================================================================
# FFT频谱分析器 (保留原有实现，略作优化)
# =============================================================================


class FFTCyclicalAnalyzer:
    """
    FFT频谱分析器

    使用快速傅里叶变换检测时间序列中的周期性模式。

    注意事项：
    - 需要至少6个数据点才能检测3年周期
    - 短序列上FFT效果有限，需结合其他方法
    - 会对数据进行去趋势处理
    """

    def __init__(self, min_period: float = 3.0, max_period: float = 7.0):
        """
        Args:
            min_period: 最小检测周期（年）
            max_period: 最大检测周期（年）
        """
        self.min_period = min_period
        self.max_period = max_period

    def analyze(self, values: np.ndarray) -> FFTResult:
        """
        执行FFT分析

        Args:
            values: 时间序列数据（年度数据）

        Returns:
            FFTResult 分析结果
        """
        n = len(values)

        # 数据点太少，无法可靠检测周期
        if n < 6:
            return FFTResult(
                dominant_period=None,
                period_strength=0.0,
                is_periodic=False,
                secondary_periods=[],
                spectrum_peaks=[],
            )

        # 1. 去趋势处理（去除线性趋势，保留周期成分）
        detrended = self._detrend(values)

        # 2. 应用汉宁窗减少频谱泄漏
        window = np.hanning(n)
        windowed = detrended * window

        # 3. 执行FFT
        fft_result = np.fft.fft(windowed)
        power_spectrum = np.abs(fft_result[:n//2])**2

        # 4. 计算对应的周期（年）
        # 频率 = k/n，周期 = n/k
        frequencies = np.fft.fftfreq(n)[:n//2]
        periods = np.where(frequencies > 0, 1.0 / frequencies, np.inf)

        # 5. 筛选感兴趣的周期范围 (3-7年)
        valid_mask = (periods >= self.min_period) & (periods <= self.max_period)
        valid_periods = periods[valid_mask]
        valid_powers = power_spectrum[valid_mask]

        if len(valid_powers) == 0:
            return FFTResult(
                dominant_period=None,
                period_strength=0.0,
                is_periodic=False,
                secondary_periods=[],
                spectrum_peaks=[],
            )

        # 6. 找到功率谱峰值
        total_power = np.sum(power_spectrum[1:])  # 排除直流分量
        max_idx = np.argmax(valid_powers)
        dominant_period = valid_periods[max_idx]
        dominant_power = valid_powers[max_idx]

        # 7. 计算周期强度（主周期功率占总功率的比例）
        period_strength = dominant_power / total_power if total_power > 0 else 0.0

        # 8. 判断是否有显著周期性
        # 阈值：主周期至少占20%的总功率
        is_periodic = period_strength > 0.20 and n >= 2 * dominant_period

        # 9. 收集所有显著峰值
        spectrum_peaks = []
        for i, (period, power) in enumerate(zip(valid_periods, valid_powers)):
            if power / total_power > 0.10:  # 至少10%功率
                spectrum_peaks.append((float(period), float(power / total_power)))

        # 排序，取前3个
        spectrum_peaks.sort(key=lambda x: -x[1])
        spectrum_peaks = spectrum_peaks[:3]

        secondary_periods = [p for p, _ in spectrum_peaks[1:]]

        return FFTResult(
            dominant_period=float(dominant_period) if is_periodic else None,
            period_strength=float(period_strength),
            is_periodic=is_periodic,
            secondary_periods=secondary_periods,
            spectrum_peaks=spectrum_peaks,
        )

    def _detrend(self, values: np.ndarray) -> np.ndarray:
        """去除线性趋势"""
        n = len(values)
        x = np.arange(n)

        # 最小二乘拟合直线
        slope, intercept = np.polyfit(x, values, 1)
        trend = slope * x + intercept

        return values - trend


class CyclicalPatternDetector:
    """
    企业级周期性模式检测器

    整合多种分析方法的贝叶斯框架：

    1. **先验概率** (Industry Prior)
       - 基于GICS行业分类的周期性先验
       - P(cyclical | industry)

    2. **似然函数** (Data Evidence)
       - HP滤波周期成分幅度
       - ACF周期性模式
       - Hurst指数（均值回复特征）
       - 峰谷规则性
       - FFT频谱分析

    3. **后验概率** (Bayesian Update)
       - P(cyclical | data, industry) ∝ P(data | cyclical) × P(cyclical | industry)

    置信度受限于数据长度（前置条件）。
    """

    def __init__(self, config=None):
        self.config = config or get_default_config()

        # 各分析器
        self.hp_filter = HPFilter(lamb=6.25)  # 年度数据λ=6.25
        self.acf_analyzer = AutocorrelationAnalyzer(significance_level=0.10)
        self.hurst_estimator = HurstExponentEstimator()
        self.peak_valley_analyzer = PeakValleyAnalyzer()
        self.fft_analyzer = FFTCyclicalAnalyzer(min_period=2.0, max_period=8.0)

    def detect(self, values: List[float], industry: str = None) -> CyclicalPatternResult:
        """
        执行综合周期性检测

        Args:
            values: 时间序列数据（年度）
            industry: 行业名称

        Returns:
            CyclicalPatternResult 检测结果
        """
        arr = np.array(values, dtype=float)
        n = len(arr)

        # ========== 0. 前置条件检查 ==========
        preconditions = check_cyclical_preconditions(n)

        if not preconditions.is_valid:
            return self._insufficient_data_result(n, industry, preconditions)

        # ========== 1. 行业先验概率 ==========
        is_known_cyclical = self.config.is_cyclical_industry(industry)
        prior_prob = 0.7 if is_known_cyclical else 0.3  # 先验概率

        thresholds = get_cyclical_thresholds(industry)

        # ========== 2. 执行各项分析 ==========

        # 2.1 基本统计
        mean_val = np.mean(arr)
        std_val = np.std(arr, ddof=1)
        cv = abs(std_val / mean_val) if mean_val != 0 else float('inf')

        # 2.2 HP滤波分析
        hp_result = self.hp_filter.filter(arr)

        # 2.3 自相关分析
        acf_result = self.acf_analyzer.analyze(arr)

        # 2.4 Hurst指数分析
        hurst_result = self.hurst_estimator.estimate(arr)

        # 2.5 峰谷分析
        pv_result = self.peak_valley_analyzer.analyze(arr)

        # 2.6 FFT分析
        fft_result = self.fft_analyzer.analyze(arr)

        # ========== 3. 计算各项证据的似然比 ==========
        likelihood_factors = []

        # 3.1 CV证据
        cv_threshold = thresholds['cv_threshold']
        cv_lr = self._compute_cv_likelihood(cv, cv_threshold)
        likelihood_factors.append(("CV", cv_lr, f"CV={cv:.3f}, threshold={cv_threshold:.3f}"))

        # 3.2 HP滤波周期幅度证据
        hp_lr = self._compute_hp_likelihood(hp_result)
        likelihood_factors.append(("HP_Cycle", hp_lr, f"cycle_amplitude={hp_result.cycle_amplitude:.3f}"))

        # 3.3 ACF证据
        acf_lr = self._compute_acf_likelihood(acf_result)
        likelihood_factors.append(("ACF", acf_lr, f"has_cyclical_acf={acf_result.has_cyclical_acf}"))

        # 3.4 Hurst指数证据
        hurst_lr = self._compute_hurst_likelihood(hurst_result)
        likelihood_factors.append(("Hurst", hurst_lr, f"H={hurst_result.hurst_exponent:.3f} ({hurst_result.interpretation})"))

        # 3.5 峰谷规则性证据
        pv_lr = self._compute_peak_valley_likelihood(pv_result, thresholds['peak_valley_ratio'])
        likelihood_factors.append(("PeakValley", pv_lr, f"ratio={pv_result['peak_valley_ratio']:.3f}, regularity={pv_result['cycle_regularity']:.3f}"))

        # 3.6 FFT证据（短序列权重降低）
        fft_weight = min(n / 10, 1.0)  # 10年以下数据，FFT权重降低
        fft_lr = self._compute_fft_likelihood(fft_result, fft_weight)
        likelihood_factors.append(("FFT", fft_lr, f"period={fft_result.dominant_period}, strength={fft_result.period_strength:.3f}"))

        # ========== 4. 贝叶斯更新计算后验概率 ==========
        # 综合似然比（加权几何平均）
        weights = [0.15, 0.20, 0.15, 0.20, 0.15, 0.15 * fft_weight]  # 各方法权重
        total_weight = sum(weights)
        weights = [w / total_weight for w in weights]

        log_lr = sum(w * np.log(max(lr, 0.01)) for (_, lr, _), w in zip(likelihood_factors, weights))
        combined_lr = np.exp(log_lr)

        # 贝叶斯更新: P(C|D) = P(D|C)P(C) / [P(D|C)P(C) + P(D|¬C)P(¬C)]
        # 简化: posterior_odds = prior_odds × likelihood_ratio
        prior_odds = prior_prob / (1 - prior_prob)
        posterior_odds = prior_odds * combined_lr
        posterior_prob = posterior_odds / (1 + posterior_odds)

        # 应用置信度上限（数据局限）
        confidence = min(posterior_prob, preconditions.confidence_ceiling)

        # ========== 5. 最终判定 ==========
        # 判定阈值：50%概率且至少2个独立证据支持
        supporting_evidence = sum(1 for _, lr, _ in likelihood_factors if lr > 1.2)

        is_cyclical = (
            confidence > 0.5 and
            supporting_evidence >= 2
        ) or (
            is_known_cyclical and
            confidence > 0.4 and
            supporting_evidence >= 1
        )

        # ========== 6. 周期位置判断 ==========
        current_phase, cycle_position = self._determine_cycle_position(
            arr,
            pv_result['peaks'],
            pv_result['valleys'],
            hp_result.cycle
        )

        # ========== 7. 生成警告和报告 ==========
        warnings = []

        # 前置条件警告
        for w in preconditions.warnings:
            warnings.append(TrendWarning(
                code="DATA_LIMITATION",
                level="warning",
                message=w,
                context={"reliability_grade": preconditions.reliability_grade},
            ))

        if is_cyclical:
            # 周期性检测警告
            evidence_summary = ", ".join(name for name, lr, _ in likelihood_factors if lr > 1.2)

            msg = f"Cyclical pattern detected (confidence: {confidence:.1%}, grade: {preconditions.reliability_grade})"
            if is_known_cyclical:
                msg += " [Industry Prior: Cyclical]"
            if fft_result.is_periodic:
                msg += f" [FFT: ~{fft_result.dominant_period:.1f}yr]"
            if hurst_result.interpretation == "mean_reverting":
                msg += f" [Hurst: Mean-Reverting]"

            warnings.append(TrendWarning(
                code="CYCLICAL_PATTERN_DETECTED",
                level="info",
                message=msg,
                context={
                    "posterior_probability": float(confidence),
                    "prior_probability": float(prior_prob),
                    "supporting_evidence": evidence_summary,
                    "reliability_grade": preconditions.reliability_grade,
                    "cycle_position": cycle_position,
                },
            ))

            # 周期底部提示
            if cycle_position == "bottom":
                warnings.append(TrendWarning(
                    code="CYCLE_BOTTOM_DETECTED",
                    level="info",
                    message="处于周期底部区域，基本面下行可能是周期性因素而非结构性问题",
                    context={"cycle_position": cycle_position, "phase": current_phase},
                ))

            # 周期顶部提示
            elif cycle_position == "top":
                warnings.append(TrendWarning(
                    code="CYCLE_TOP_DETECTED",
                    level="warning",
                    message="处于周期顶部区域，当前高点可能难以持续",
                    context={"cycle_position": cycle_position, "phase": current_phase},
                ))

        # ========== 8. 构建置信因子详情 ==========
        confidence_factors = [
            f"Prior: P(cyclical|{industry or 'unknown'})={prior_prob:.2f}",
            f"Data Reliability: Grade {preconditions.reliability_grade} ({n}yr data)",
        ]
        for name, lr, detail in likelihood_factors:
            evidence_type = "+" if lr > 1.2 else ("-" if lr < 0.8 else "~")
            confidence_factors.append(f"[{evidence_type}] {name}: LR={lr:.2f} ({detail})")
        confidence_factors.append(f"Posterior: P(cyclical|data)={confidence:.2%}")

        return CyclicalPatternResult(
            is_cyclical=is_cyclical,
            peak_to_trough_ratio=float(pv_result['peak_valley_ratio']),
            has_middle_peak=len(pv_result['peaks']) > 0,
            has_wave_pattern=len(pv_result['peaks']) > 1 and len(pv_result['valleys']) > 1,
            trend_r_squared=0.0,
            cv=float(cv),
            current_phase=current_phase,
            cycle_position=cycle_position,
            fft_dominant_period=fft_result.dominant_period if fft_result.is_periodic else 0.0,
            industry_cyclical=is_known_cyclical,
            cyclical_confidence=float(confidence),
            peak_to_trough_threshold=float(thresholds['peak_valley_ratio']),
            trend_r_squared_max=0.3,
            cv_threshold=float(thresholds['cv_threshold']),
            industry=industry or "unknown",
            confidence_factors=confidence_factors,
            warnings=warnings,
        )

    # =========================================================================
    # 似然比计算方法
    # =========================================================================

    def _compute_cv_likelihood(self, cv: float, threshold: float) -> float:
        """
        CV似然比: 高CV支持周期性假设

        LR = P(CV|cyclical) / P(CV|not_cyclical)
        """
        # 简化模型: CV服从半正态分布
        # 周期性行业: 期望CV更高
        if cv < threshold * 0.5:
            return 0.3  # 强烈不支持
        elif cv < threshold:
            return 0.7  # 弱不支持
        elif cv < threshold * 1.5:
            return 1.5  # 弱支持
        else:
            return 2.5  # 强支持

    def _compute_hp_likelihood(self, hp_result: HPFilterResult) -> float:
        """
        HP滤波周期成分似然比

        周期成分振幅大 -> 支持周期性
        """
        amplitude = hp_result.cycle_amplitude

        if amplitude < 0.05:
            return 0.4  # 几乎无周期成分
        elif amplitude < 0.10:
            return 0.8
        elif amplitude < 0.20:
            return 1.5
        elif amplitude < 0.30:
            return 2.0
        else:
            return 2.5  # 强周期成分

    def _compute_acf_likelihood(self, acf_result: AutocorrelationResult) -> float:
        """
        ACF似然比

        显著自相关 -> 支持周期性
        """
        if acf_result.has_cyclical_acf:
            # 根据显著滞后期数量调整
            n_significant = len(acf_result.significant_lags)
            return 1.5 + 0.3 * min(n_significant, 3)
        else:
            return 0.7

    def _compute_hurst_likelihood(self, hurst_result: HurstResult) -> float:
        """
        Hurst指数似然比

        H < 0.5 (均值回复) -> 强支持周期性
        H ≈ 0.5 (随机游走) -> 中性
        H > 0.5 (趋势持续) -> 不支持周期性
        """
        H = hurst_result.hurst_exponent
        conf = hurst_result.confidence

        if conf < 0.3:
            return 1.0  # 估计不可靠，保持中性

        if H < 0.35:
            return 2.5  # 强均值回复
        elif H < 0.45:
            return 1.8  # 中等均值回复
        elif H < 0.55:
            return 1.0  # 随机游走，中性
        elif H < 0.65:
            return 0.6  # 弱趋势
        else:
            return 0.3  # 强趋势，不支持周期

    def _compute_peak_valley_likelihood(self, pv_result: Dict, threshold: float) -> float:
        """
        峰谷规则性似然比
        """
        ratio = pv_result['peak_valley_ratio']
        regularity = pv_result['cycle_regularity']

        # 规则性高+峰谷比大 -> 强支持
        if pv_result['is_regular_cycle']:
            return 2.0 + regularity
        elif ratio > threshold and regularity > 0.3:
            return 1.5
        elif ratio > threshold * 0.8:
            return 1.0
        else:
            return 0.6

    def _compute_fft_likelihood(self, fft_result: FFTResult, weight: float) -> float:
        """
        FFT似然比 (受数据长度调制)
        """
        if not fft_result.is_periodic:
            return 1.0 - 0.3 * weight  # 短数据上不检测到也正常

        strength = fft_result.period_strength

        # 周期强度越高，似然比越大
        base_lr = 1.5 + strength * 2.0

        # 短数据上适当降低
        return 1.0 + (base_lr - 1.0) * weight

    # =========================================================================
    # 周期位置判断
    # =========================================================================

    def _determine_cycle_position(
        self,
        arr: np.ndarray,
        peaks: List[int],
        valleys: List[int],
        cycle_component: np.ndarray = None
    ) -> Tuple[str, str]:
        """
        判断周期位置

        综合使用:
        1. 历史分位数
        2. HP滤波周期成分位置
        3. 最近趋势方向

        Returns:
            (current_phase, cycle_position)
        """
        n = len(arr)

        if n < 3:
            return "unknown", "unknown"

        # 1. 当前相位（趋势方向）
        recent_trend = arr[-1] - arr[-2] if n >= 2 else 0
        if recent_trend > 0:
            current_phase = "rising"
        elif recent_trend < 0:
            current_phase = "falling"
        else:
            current_phase = "flat"

        # 2. 历史分位数
        latest = arr[-1]
        percentile = np.sum(arr[:-1] < latest) / (n - 1) * 100 if n > 1 else 50

        # 3. 如果有HP周期成分，也参考它
        if cycle_component is not None and len(cycle_component) > 0:
            cycle_latest = cycle_component[-1]
            cycle_max = np.max(np.abs(cycle_component))
            if cycle_max > 0:
                cycle_position_pct = (cycle_latest / cycle_max + 1) / 2 * 100
            else:
                cycle_position_pct = 50

            # 加权平均
            combined_pct = 0.6 * percentile + 0.4 * cycle_position_pct
        else:
            combined_pct = percentile

        # 4. 判断位置
        if combined_pct >= 80:
            cycle_position = "top"
        elif combined_pct <= 20:
            cycle_position = "bottom"
        elif current_phase == "rising":
            cycle_position = "mid_up"
        elif current_phase == "falling":
            cycle_position = "mid_down"
        else:
            cycle_position = "mid"

        return current_phase, cycle_position

    def _insufficient_data_result(
        self,
        count: int,
        industry: str,
        preconditions: CyclicalPreconditions
    ) -> CyclicalPatternResult:
        """数据不足时的结果"""

        warnings = [
            TrendWarning(
                code="INSUFFICIENT_DATA_FOR_CYCLICAL",
                level="warning",
                message=f"数据不足({count}年)，无法进行可靠的周期性分析",
                context={
                    "data_years": count,
                    "min_required": 5,
                    "reliability_grade": preconditions.reliability_grade,
                },
            )
        ]

        for w in preconditions.warnings:
            warnings.append(TrendWarning(
                code="DATA_LIMITATION",
                level="info",
                message=w,
                context={},
            ))

        return CyclicalPatternResult(
            is_cyclical=False,
            peak_to_trough_ratio=0.0,
            has_middle_peak=False,
            has_wave_pattern=False,
            trend_r_squared=0.0,
            cv=0.0,
            current_phase="unknown",
            cycle_position="unknown",
            fft_dominant_period=0.0,
            industry_cyclical=self.config.is_cyclical_industry(industry),
            cyclical_confidence=0.0,
            peak_to_trough_threshold=3.0,
            trend_r_squared_max=0.3,
            cv_threshold=0.3,
            industry=industry or "unknown",
            confidence_factors=[
                f"Data Reliability: Grade {preconditions.reliability_grade}",
                f"Confidence ceiling: {preconditions.confidence_ceiling:.0%}",
            ],
            warnings=warnings,
        )
