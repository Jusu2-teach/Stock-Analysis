"""
周期性模式检测器 (Cyclical Pattern Detector)
============================================

企业级周期性检测，基于金融计量经济学最佳实践。

核心方法论
----------
1. **Hodrick-Prescott滤波 (HP Filter)**: 分离趋势和周期成分
2. **自相关分析 (ACF)**: 检测序列相关性
3. **峰谷检测 + 周期规则性**
4. **去趋势波动分析 (DFA)**: 区分真周期vs随机游走 (Hurst指数)
5. **贝叶斯框架**: 先验 + 似然 → 后验概率

⚠️ 设计原则 (v3.0):
==================
此探针是 **纯数学工具**，不包含任何业务逻辑：
- ✅ HP滤波、ACF分析、Hurst估计、FFT
- ✅ 贝叶斯概率计算
- ✅ 所有阈值由调用方传入
- ❌ 不知道什么是"钢铁行业"
- ❌ 不调用 get_cyclical_thresholds()

调用方通过参数控制：
- prior_probability: 先验概率 (业务层根据行业设置)
- cv_threshold: CV阈值 (业务层根据行业设置)
- peak_valley_threshold: 峰谷比阈值

学术参考
--------
- Hodrick & Prescott (1997). Journal of Money, Credit and Banking.
- Ravn & Uhlig (2002). Review of Economics and Statistics.
- Hamilton (2018). Review of Economics and Statistics. (HP滤波批评)

作者: AStock Analysis System
日期: 2025-01-07
版本: 3.0 (Pure Math Edition)
"""

import logging
import numpy as np
from scipy import stats
from typing import List, Tuple, Optional, Dict, Any
from dataclasses import dataclass, field

from ..models import CyclicalPatternResult, TrendWarning
from ..config import get_default_config

logger = logging.getLogger(__name__)


# =============================================================================
# 默认阈值 (纯统计学标准，可被调用方覆盖)
# =============================================================================

DEFAULT_CYCLICAL_THRESHOLDS = {
    'cv_threshold': 0.3,           # CV阈值
    'peak_valley_ratio': 2.0,      # 峰谷比阈值
    'r_squared_low': 0.5,          # R²低阈值
    'prior_probability': 0.3,      # 默认先验 (非周期性假设)
}


# =============================================================================
# 前置条件检查器
# =============================================================================

@dataclass
class CyclicalPreconditions:
    """周期性检测的前置条件评估结果"""
    is_valid: bool
    data_years: int
    min_detectable_period: float
    max_reliable_period: float
    confidence_ceiling: float
    warnings: List[str] = field(default_factory=list)

    @property
    def reliability_grade(self) -> str:
        if self.data_years >= 15:
            return "A"
        elif self.data_years >= 10:
            return "B"
        elif self.data_years >= 7:
            return "C"
        elif self.data_years >= 5:
            return "D"
        else:
            return "F"


def check_cyclical_preconditions(n_years: int) -> CyclicalPreconditions:
    """检查周期性检测的前置条件"""
    warnings_list = []

    min_detectable = n_years / 2.0
    max_reliable = n_years / 3.0

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
        warnings_list.append(f"数据不足：{n_years}年数据无法进行可靠的周期性分析")
    elif n_years < 7:
        warnings_list.append(f"数据有限：{n_years}年数据只能检测{max_reliable:.1f}年以下的短周期")
    elif n_years < 10:
        warnings_list.append(f"数据中等：{n_years}年数据可以初步检测商业周期")

    return CyclicalPreconditions(
        is_valid=is_valid,
        data_years=n_years,
        min_detectable_period=min_detectable,
        max_reliable_period=max_reliable,
        confidence_ceiling=confidence_ceiling,
        warnings=warnings_list,
    )


# =============================================================================
# 分析结果数据类
# =============================================================================

@dataclass
class FFTResult:
    dominant_period: Optional[float]
    period_strength: float
    is_periodic: bool
    secondary_periods: List[float]
    spectrum_peaks: List[Tuple[float, float]]


@dataclass
class HPFilterResult:
    trend: np.ndarray
    cycle: np.ndarray
    cycle_amplitude: float
    cycle_volatility: float


@dataclass
class AutocorrelationResult:
    acf_values: np.ndarray
    significant_lags: List[int]
    ljung_box_pvalue: float
    has_cyclical_acf: bool


@dataclass
class HurstResult:
    hurst_exponent: float
    interpretation: str
    confidence: float


# =============================================================================
# Hodrick-Prescott 滤波器
# =============================================================================

class HPFilter:
    """HP滤波器 - 分离趋势和周期成分"""

    def __init__(self, lamb: float = 6.25):
        self.lamb = lamb

    def filter(self, y: np.ndarray) -> HPFilterResult:
        n = len(y)

        if n < 4:
            return HPFilterResult(
                trend=y.copy(),
                cycle=np.zeros_like(y),
                cycle_amplitude=0.0,
                cycle_volatility=0.0,
            )

        D = np.zeros((n-2, n))
        for i in range(n-2):
            D[i, i] = 1
            D[i, i+1] = -2
            D[i, i+2] = 1

        I = np.eye(n)
        A = I + self.lamb * (D.T @ D)

        try:
            trend = np.linalg.solve(A, y)
        except np.linalg.LinAlgError:
            trend = y.copy()

        cycle = y - trend
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
# v9.0: Hamilton (2018) 回归滤波器
# =============================================================================

class HamiltonFilter:
    """Hamilton (2018) 回归滤波器 — HP 滤波的专业替代

    Hamilton J.D. (2018) "Why You Should Never Use the
    Hodrick-Prescott Filter", Review of Economics and Statistics.

    核心问题: HP 滤波在端点产生严重失真 (end-of-sample bias),
    对 n=5-10 的短序列是致命的。HP 的“周期成分”会反映平滑器的人为设计
    而非数据中的真实周期。

    Hamilton 的替代:
        y_t = α + β₁ y_{t-h} + β₂ y_{t-h-1} + ... + β_p y_{t-h-p+1} + ε_t
        cycle_t = y_t - ŷ_t  (残差 = 周期成分)

    对于年度数据: h=2, p=1 (用 t-2 预测 t)
    最小数据: n >= h + p + 2 = 5

    优势:
    1. 无端点失真问题 (纯回归, 不涉及未来信息)
    2. 结果具有统计可解释性 (可用 R², p-value 评价)
    3. 不会发明虚假周期
    """

    def __init__(self, h: int = 2, p: int = 1):
        """h=预测步长, p=滞后阶数"""
        self.h = h
        self.p = p

    def filter(self, y: np.ndarray) -> HPFilterResult:
        n = len(y)
        min_obs = self.h + self.p + 2

        if n < min_obs:
            return HPFilterResult(
                trend=y.copy(),
                cycle=np.zeros_like(y),
                cycle_amplitude=0.0,
                cycle_volatility=0.0,
            )

        # 构建回归矩阵: y_t = α + β₁ y_{t-h} + ... + β_p y_{t-h-p+1} + ε
        start = self.h + self.p - 1
        y_dep = y[start:]  # 因变量 t = start, ..., n-1
        T = len(y_dep)

        X = np.ones((T, 1 + self.p))  # 截距 + p 个滞后值
        for j in range(self.p):
            lag_idx = self.h + j  # y_{t-h-j}
            X[:, 1 + j] = y[start - lag_idx : n - lag_idx]

        # OLS: β = (X'X)^{-1} X'y
        try:
            beta = np.linalg.lstsq(X, y_dep, rcond=None)[0]
        except np.linalg.LinAlgError:
            return HPFilterResult(
                trend=y.copy(), cycle=np.zeros_like(y),
                cycle_amplitude=0.0, cycle_volatility=0.0,
            )

        # 拟合值 = 趋势, 残差 = 周期
        y_hat = X @ beta
        residuals = y_dep - y_hat

        # 填充完整序列 (前 start 个点无法计算, 用原始值作趋势)
        trend = y.copy()
        cycle = np.zeros_like(y)
        trend[start:] = y_hat
        cycle[start:] = residuals

        trend_mean = np.mean(np.abs(trend))
        cycle_amplitude = np.std(cycle[start:]) / trend_mean if trend_mean > 0 else 0.0
        cycle_volatility = np.std(cycle[start:])

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
    """ACF分析器"""

    def __init__(self, max_lag: int = None, significance_level: float = 0.05):
        self.max_lag = max_lag
        self.significance_level = significance_level

    def analyze(self, y: np.ndarray) -> AutocorrelationResult:
        n = len(y)
        max_lag = self.max_lag or min(n // 2, 5)

        if n < 5:
            return AutocorrelationResult(
                acf_values=np.array([1.0]),
                significant_lags=[],
                ljung_box_pvalue=1.0,
                has_cyclical_acf=False,
            )

        y_centered = y - np.mean(y)
        acf_values = np.zeros(max_lag + 1)
        acf_values[0] = 1.0

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

        threshold = stats.norm.ppf(1 - self.significance_level / 2) / np.sqrt(n)
        significant_lags = [k for k in range(1, len(acf_values)) if abs(acf_values[k]) > threshold]

        Q = 0
        for k in range(1, min(max_lag + 1, n)):
            Q += (acf_values[k] ** 2) / (n - k)
        Q *= n * (n + 2)
        ljung_box_pvalue = 1 - stats.chi2.cdf(Q, df=max_lag)

        has_cyclical_acf = (
            len(significant_lags) > 0 and
            ljung_box_pvalue < self.significance_level and
            any(acf_values[k] > threshold for k in significant_lags)
        )

        return AutocorrelationResult(
            acf_values=acf_values,
            significant_lags=significant_lags,
            ljung_box_pvalue=float(ljung_box_pvalue),
            has_cyclical_acf=has_cyclical_acf,
        )


# =============================================================================
# Hurst指数估计器
# =============================================================================

class HurstExponentEstimator:
    """Hurst指数估计器 — v9.0: DFA (Peng 1994) + R/S 融合

    v8.0: 纯 R/S 分析 + Bayesian shrinkage
    v9.0: DFA (Detrended Fluctuation Analysis) 作为主估计,
          R/S 作为辅助, 贝叶斯融合。

    为什么 DFA 优于 R/S:
    1. R/S 在短记忆过程中有正偏差 (Lo 1991): H 系统高伀15-25%
    2. R/S 对序列中的趋势和周期成分不做校正
    3. DFA 通过多项式去趋势消除了非平稳性的影响
    4. DFA 在 n=8-20 的财务年度数据上比 R/S 更可靠 (Kantelhardt 2002)

    References:
        - Peng, C.K. et al. (1994). Physical Review E, 49(2), 1685-1689.
        - Kantelhardt, J.W. et al. (2002). Physica A, 316(1-4), 87-114.
        - Lo, A.W. (1991). Econometrica, 59(5), 1279-1313.
    """

    def estimate(self, y: np.ndarray) -> HurstResult:
        n = len(y)

        if n < 8:
            return HurstResult(hurst_exponent=0.5, interpretation="unknown", confidence=0.0)

        # ========== DFA 估计 (主) ==========
        hurst_dfa, conf_dfa = self._dfa_estimate(y)

        # ========== R/S 估计 (辅) ==========
        hurst_rs, conf_rs = self._rs_estimate(y)

        # ========== 贝叶斯融合 ==========
        # DFA 权重更高 (更可靠), R/S 作为补充
        if conf_dfa > 0.1 and conf_rs > 0.1:
            w_dfa = conf_dfa * 0.7  # DFA 基础权重 70%
            w_rs = conf_rs * 0.3    # R/S 基础权重 30%
            total_w = w_dfa + w_rs
            hurst = (w_dfa * hurst_dfa + w_rs * hurst_rs) / total_w
            confidence = min((conf_dfa + conf_rs) / 2.0 * 1.1, 1.0)  # 融合提升
        elif conf_dfa > 0.1:
            hurst = hurst_dfa
            confidence = conf_dfa
        elif conf_rs > 0.1:
            hurst = hurst_rs
            confidence = conf_rs
        else:
            hurst = 0.5
            confidence = 0.0

        # v8.0+v9.0: 贝叶斯收缩 — 低信心时拉回先验 H=0.5
        hurst_shrunk = confidence * hurst + (1.0 - confidence) * 0.5

        if abs(hurst_shrunk - 0.5) < 0.1:
            interpretation = "random_walk"
        elif hurst_shrunk < 0.5:
            interpretation = "mean_reverting"
        else:
            interpretation = "trending"

        return HurstResult(
            hurst_exponent=float(hurst_shrunk),
            interpretation=interpretation,
            confidence=float(confidence),
        )

    def _dfa_estimate(self, y: np.ndarray) -> Tuple[float, float]:
        """DFA: Detrended Fluctuation Analysis (Peng 1994)

        算法:
        1. 计算累积偏差序列: Y_k = Σ(y_i - ȳ)
        2. 将 Y 分成等长窗口, 在每个窗口内拟合多项式并计算残差方差 F(s)
        3. 拟合 log F(s) ~ H × log(s)
        """
        n = len(y)
        y_mean = np.mean(y)
        Y = np.cumsum(y - y_mean)  # 累积偏差 (profile)

        # 窗口大小: 从 4 到 n//2
        min_s = 4
        max_s = n // 2
        if max_s < min_s:
            return 0.5, 0.0

        scales = []
        fluct = []
        s = min_s
        while s <= max_s:
            n_windows = n // s
            if n_windows < 1:
                break

            # 对每个窗口拟合线性趋势并计算残差
            rms_list = []
            for w in range(n_windows):
                segment = Y[w * s : (w + 1) * s]
                t = np.arange(s)
                # 二次多项式去趋势 (DFA-2, 比 DFA-1 更稳健)
                if s >= 6:
                    coeffs = np.polyfit(t, segment, 2)
                    trend = np.polyval(coeffs, t)
                else:
                    coeffs = np.polyfit(t, segment, 1)
                    trend = np.polyval(coeffs, t)
                rms = np.sqrt(np.mean((segment - trend) ** 2))
                rms_list.append(rms)

            F_s = np.mean(rms_list)
            if F_s > 0:
                scales.append(s)
                fluct.append(F_s)

            s = int(s * 1.5) + 1  # 对数等距缩放

        if len(scales) < 2:
            return 0.5, 0.0

        log_s = np.log(np.array(scales))
        log_f = np.log(np.array(fluct))

        slope, _, r_value, _, _ = stats.linregress(log_s, log_f)
        hurst = max(0.0, min(1.5, float(slope)))  # DFA 可超过 1.0 (非平稳)
        hurst = min(hurst, 1.0)  # 截断到 [0, 1]

        # 信心: 基于拟合优度和数据量
        confidence = float(r_value ** 2) * min(n / 15, 1.0)  # DFA 在 n=15 时可靠

        return hurst, confidence

    def _rs_estimate(self, y: np.ndarray) -> Tuple[float, float]:
        """R/S 分析 (Lo 1991 修正版)"""
        n = len(y)
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
            return 0.5, 0.0

        log_sizes = np.log(chunk_sizes)
        log_rs = np.log(rs_values)
        slope, _, r_value, _, _ = stats.linregress(log_sizes, log_rs)
        hurst = max(0.0, min(1.0, float(slope)))
        confidence = float(r_value ** 2) * min(n / 20, 1.0)

        return hurst, confidence

    def _compute_rs(self, y: np.ndarray, chunk_size: int) -> Optional[float]:
        n = len(y)
        n_chunks = n // chunk_size

        if n_chunks == 0:
            return None

        rs_list = []
        for i in range(n_chunks):
            chunk = y[i * chunk_size : (i + 1) * chunk_size]
            mean_chunk = np.mean(chunk)
            cumdev = np.cumsum(chunk - mean_chunk)
            R = np.max(cumdev) - np.min(cumdev)
            S = np.std(chunk, ddof=1)
            if S > 0:
                rs_list.append(R / S)

        return np.mean(rs_list) if rs_list else None


# =============================================================================
# 峰谷分析器
# =============================================================================

class PeakValleyAnalyzer:
    """峰谷周期规则性分析"""

    def __init__(self, min_phase_length: int = 1):
        self.min_phase_length = min_phase_length

    def analyze(self, y: np.ndarray) -> Dict[str, Any]:
        n = len(y)

        if n < 3:
            return {
                "peaks": [], "valleys": [],
                "peak_valley_ratio": 1.0,
                "cycle_regularity": 0.0,
                "avg_cycle_length": 0.0,
                "is_regular_cycle": False,
            }

        peaks = []
        valleys = []

        for i in range(1, n - 1):
            if y[i] > y[i-1] and y[i] > y[i+1]:
                peaks.append(i)
            elif y[i] < y[i-1] and y[i] < y[i+1]:
                valleys.append(i)

        if n >= 2:
            if y[0] > y[1]:
                peaks.insert(0, 0)
            elif y[0] < y[1]:
                valleys.insert(0, 0)
            if y[-1] > y[-2]:
                peaks.append(n - 1)
            elif y[-1] < y[-2]:
                valleys.append(n - 1)

        if peaks and valleys:
            peak_values = y[peaks]
            valley_values = y[valleys]
            if np.min(valley_values) <= 0:
                peak_valley_ratio = float(np.median(peak_values) - np.median(valley_values) + 1)
            else:
                peak_valley_ratio = float(np.mean(peak_values) / np.mean(valley_values))
        else:
            peak_valley_ratio = 1.0

        turning_points = sorted(peaks + valleys)

        if len(turning_points) >= 2:
            intervals = np.diff(turning_points)
            avg_interval = np.mean(intervals)
            interval_std = np.std(intervals)
            if avg_interval > 0:
                cycle_cv = interval_std / avg_interval
                cycle_regularity = max(0, 1 - cycle_cv)
            else:
                cycle_regularity = 0.0
            avg_cycle_length = float(avg_interval * 2)
        else:
            cycle_regularity = 0.0
            avg_cycle_length = 0.0

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
# FFT频谱分析器
# =============================================================================

class FFTCyclicalAnalyzer:
    """FFT频谱分析器"""

    def __init__(self, min_period: float = 3.0, max_period: float = 7.0):
        self.min_period = min_period
        self.max_period = max_period

    def analyze(self, values: np.ndarray) -> FFTResult:
        n = len(values)

        if n < 6:
            return FFTResult(
                dominant_period=None, period_strength=0.0,
                is_periodic=False, secondary_periods=[], spectrum_peaks=[],
            )

        detrended = self._detrend(values)
        window = np.hanning(n)
        windowed = detrended * window

        fft_result = np.fft.fft(windowed)
        power_spectrum = np.abs(fft_result[:n//2])**2

        frequencies = np.fft.fftfreq(n)[:n//2]
        periods = np.where(frequencies > 0, 1.0 / frequencies, np.inf)

        valid_mask = (periods >= self.min_period) & (periods <= self.max_period)
        valid_periods = periods[valid_mask]
        valid_powers = power_spectrum[valid_mask]

        if len(valid_powers) == 0:
            return FFTResult(
                dominant_period=None, period_strength=0.0,
                is_periodic=False, secondary_periods=[], spectrum_peaks=[],
            )

        total_power = np.sum(power_spectrum[1:])
        max_idx = np.argmax(valid_powers)
        dominant_period = valid_periods[max_idx]
        dominant_power = valid_powers[max_idx]

        period_strength = dominant_power / total_power if total_power > 0 else 0.0
        is_periodic = period_strength > 0.20 and n >= 2 * dominant_period

        spectrum_peaks = []
        for i, (period, power) in enumerate(zip(valid_periods, valid_powers)):
            if power / total_power > 0.10:
                spectrum_peaks.append((float(period), float(power / total_power)))
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
        n = len(values)
        x = np.arange(n)
        slope, intercept = np.polyfit(x, values, 1)
        trend = slope * x + intercept
        return values - trend


# =============================================================================
# 主检测器 (纯数学版)
# =============================================================================

class CyclicalProbe:
    """
    企业级周期性模式探针 (纯数学版)

    Unified interface following ProbeProtocol:
    - compute(values, **kwargs) -> CyclicalPatternResult
    - default() -> CyclicalPatternResult

    v3.0 变更：
    - 移除所有 get_cyclical_thresholds 调用
    - 所有业务相关参数由调用方传入
    - 先验概率由调用方指定
    """

    def __init__(self, config=None):
        self.config = config or get_default_config()
        self.hp_filter = HPFilter(lamb=self.config.hp_filter_lambda)
        self.hamilton_filter = HamiltonFilter(h=2, p=1)  # v9.0
        self.acf_analyzer = AutocorrelationAnalyzer(significance_level=self.config.acf_significance_level)
        self.hurst_estimator = HurstExponentEstimator()
        self.peak_valley_analyzer = PeakValleyAnalyzer()
        self.fft_analyzer = FFTCyclicalAnalyzer(min_period=2.0, max_period=8.0)

    def compute(
        self,
        values: List[float],
        prior_probability: float = 0.3,
        cv_threshold: float = 0.3,
        peak_valley_threshold: float = 2.0,
        industry: str = None,
    ) -> CyclicalPatternResult:
        """
        执行综合周期性检测

        Args:
            values: 时间序列数据（年度）
            prior_probability: 先验概率 P(cyclical)，由调用方根据业务知识设置
                              - 周期性行业可设为 0.7
                              - 防御性行业可设为 0.2
                              - 未知行业默认 0.3
            cv_threshold: CV阈值，用于CV似然比计算
            peak_valley_threshold: 峰谷比阈值
            industry: 行业名称（仅用于报告，不影响计算）

        Returns:
            CyclicalPatternResult 检测结果
        """
        arr = np.array(values, dtype=float)
        n = len(arr)

        # ========== 0. 前置条件检查 ==========
        preconditions = check_cyclical_preconditions(n)

        if not preconditions.is_valid:
            return self._insufficient_data_result(n, industry, preconditions, prior_probability)

        # ========== 1. 执行各项分析 ==========
        mean_val = np.mean(arr)
        std_val = np.std(arr, ddof=1)
        cv = abs(std_val / mean_val) if mean_val != 0 else float('inf')

        hp_result = self.hp_filter.filter(arr)
        # v9.0: Hamilton filter 补充 HP (无端点偏差)
        hamilton_result = self.hamilton_filter.filter(arr)
        # 融合 HP + Hamilton: n≤8 时 Hamilton 主导, n≥15 时 HP 主导
        hp_weight = min(max((n - 8) / 7.0, 0.0), 1.0)  # [0.0, 1.0]
        ham_weight = 1.0 - hp_weight
        # 融合周期振幅
        fused_amplitude = hp_weight * hp_result.cycle_amplitude + ham_weight * hamilton_result.cycle_amplitude
        fused_volatility = hp_weight * hp_result.cycle_volatility + ham_weight * hamilton_result.cycle_volatility
        # 融合周期成分 (用于周期位置判断)
        fused_cycle = hp_weight * hp_result.cycle + ham_weight * hamilton_result.cycle
        hp_result = HPFilterResult(
            trend=hp_weight * hp_result.trend + ham_weight * hamilton_result.trend,
            cycle=fused_cycle,
            cycle_amplitude=float(fused_amplitude),
            cycle_volatility=float(fused_volatility),
        )
        acf_result = self.acf_analyzer.analyze(arr)
        hurst_result = self.hurst_estimator.estimate(arr)
        pv_result = self.peak_valley_analyzer.analyze(arr)
        fft_result = self.fft_analyzer.analyze(arr)

        # ========== 2. 计算各项证据的似然比 ==========
        likelihood_factors = []

        cv_lr = self._compute_cv_likelihood(cv, cv_threshold)
        likelihood_factors.append(("CV", cv_lr, f"CV={cv:.3f}, threshold={cv_threshold:.3f}"))

        hp_lr = self._compute_hp_likelihood(hp_result)
        likelihood_factors.append(("HP_Cycle", hp_lr, f"cycle_amplitude={hp_result.cycle_amplitude:.3f}"))

        acf_lr = self._compute_acf_likelihood(acf_result)
        likelihood_factors.append(("ACF", acf_lr, f"has_cyclical_acf={acf_result.has_cyclical_acf}"))

        hurst_lr = self._compute_hurst_likelihood(hurst_result)
        likelihood_factors.append(("Hurst", hurst_lr, f"H={hurst_result.hurst_exponent:.3f} ({hurst_result.interpretation})"))

        pv_lr = self._compute_peak_valley_likelihood(pv_result, peak_valley_threshold)
        likelihood_factors.append(("PeakValley", pv_lr, f"ratio={pv_result['peak_valley_ratio']:.3f}, regularity={pv_result['cycle_regularity']:.3f}"))

        fft_weight = min(n / 10, 1.0)
        fft_lr = self._compute_fft_likelihood(fft_result, fft_weight)
        likelihood_factors.append(("FFT", fft_lr, f"period={fft_result.dominant_period}, strength={fft_result.period_strength:.3f}"))

        # ========== 3. 贝叶斯更新 ==========
        weights = [0.15, 0.20, 0.15, 0.20, 0.15, 0.15 * fft_weight]
        total_weight = sum(weights)
        weights = [w / total_weight for w in weights]

        log_lr = sum(w * np.log(max(lr, 0.01)) for (_, lr, _), w in zip(likelihood_factors, weights))
        combined_lr = np.exp(log_lr)

        prior_odds = prior_probability / (1 - prior_probability)
        posterior_odds = prior_odds * combined_lr
        posterior_prob = posterior_odds / (1 + posterior_odds)

        confidence = min(posterior_prob, preconditions.confidence_ceiling)

        # ========== 4. 最终判定 ==========
        supporting_evidence = sum(1 for _, lr, _ in likelihood_factors if lr > 1.2)
        is_high_prior = prior_probability > 0.5

        is_cyclical = (
            confidence > 0.5 and supporting_evidence >= 2
        ) or (
            is_high_prior and confidence > 0.4 and supporting_evidence >= 1
        )

        # ========== 5. 周期位置判断 ==========
        current_phase, cycle_position = self._determine_cycle_position(
            arr, pv_result['peaks'], pv_result['valleys'], hp_result.cycle
        )

        # ========== 6. 生成警告 ==========
        warnings = []

        for w in preconditions.warnings:
            warnings.append(TrendWarning(
                code="DATA_LIMITATION",
                level="warning",
                message=w,
                context={"reliability_grade": preconditions.reliability_grade},
            ))

        if is_cyclical:
            evidence_summary = ", ".join(name for name, lr, _ in likelihood_factors if lr > 1.2)
            msg = f"Cyclical pattern detected (confidence: {confidence:.1%}, grade: {preconditions.reliability_grade})"
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
                    "prior_probability": float(prior_probability),
                    "supporting_evidence": evidence_summary,
                    "reliability_grade": preconditions.reliability_grade,
                    "cycle_position": cycle_position,
                },
            ))

            if cycle_position == "bottom":
                warnings.append(TrendWarning(
                    code="CYCLE_BOTTOM_DETECTED",
                    level="info",
                    message="处于周期底部区域",
                    context={"cycle_position": cycle_position, "phase": current_phase},
                ))
            elif cycle_position == "top":
                warnings.append(TrendWarning(
                    code="CYCLE_TOP_DETECTED",
                    level="warning",
                    message="处于周期顶部区域",
                    context={"cycle_position": cycle_position, "phase": current_phase},
                ))

        # ========== 7. 构建置信因子详情 ==========
        confidence_factors = [
            f"Prior: P(cyclical)={prior_probability:.2f}",
            f"Data Reliability: Grade {preconditions.reliability_grade} ({n}yr data)",
        ]
        for name, lr, detail in likelihood_factors:
            evidence_type = "+" if lr > 1.2 else ("-" if lr < 0.8 else "~")
            confidence_factors.append(f"[{evidence_type}] {name}: LR={lr:.2f} ({detail})")
        confidence_factors.append(f"Posterior: P(cyclical|data)={confidence:.2%}")

        # ========== 8. 构建完整结果 (v2.0: 包含所有专业分析输出) ==========
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
            industry_cyclical=is_high_prior,
            cyclical_confidence=float(confidence),
            peak_to_trough_threshold=float(peak_valley_threshold),
            trend_r_squared_max=0.3,
            cv_threshold=float(cv_threshold),
            industry=industry or "unknown",

            # === v2.0 新增: HP滤波完整输出 ===
            hp_cycle_amplitude=float(hp_result.cycle_amplitude),
            hp_cycle_volatility=float(hp_result.cycle_volatility),

            # === v2.0 新增: Hurst指数完整输出 ===
            hurst_exponent=float(hurst_result.hurst_exponent),
            hurst_interpretation=hurst_result.interpretation,
            hurst_confidence=float(hurst_result.confidence),

            # === v2.0 新增: ACF完整输出 ===
            acf_lag1=float(acf_result.acf_values[1]) if len(acf_result.acf_values) > 1 else 0.0,
            acf_has_cyclical_pattern=acf_result.has_cyclical_acf,
            ljung_box_pvalue=float(acf_result.ljung_box_pvalue),

            # === 原有列表字段 ===
            confidence_factors=confidence_factors,
            warnings=warnings,
        )

    # =========================================================================
    # 似然比计算方法 (纯数学)
    # =========================================================================

    def _compute_cv_likelihood(self, cv: float, threshold: float) -> float:
        if cv < threshold * 0.5:
            return 0.3
        elif cv < threshold:
            return 0.7
        elif cv < threshold * 1.5:
            return 1.5
        else:
            return 2.5

    def _compute_hp_likelihood(self, hp_result: HPFilterResult) -> float:
        amplitude = hp_result.cycle_amplitude
        if amplitude < 0.05:
            return 0.4
        elif amplitude < 0.10:
            return 0.8
        elif amplitude < 0.20:
            return 1.5
        elif amplitude < 0.30:
            return 2.0
        else:
            return 2.5

    def _compute_acf_likelihood(self, acf_result: AutocorrelationResult) -> float:
        if acf_result.has_cyclical_acf:
            n_significant = len(acf_result.significant_lags)
            return 1.5 + 0.3 * min(n_significant, 3)
        else:
            return 0.7

    def _compute_hurst_likelihood(self, hurst_result: HurstResult) -> float:
        H = hurst_result.hurst_exponent
        conf = hurst_result.confidence

        if conf < 0.3:
            return 1.0

        if H < 0.35:
            return 2.5
        elif H < 0.45:
            return 1.8
        elif H < 0.55:
            return 1.0
        elif H < 0.65:
            return 0.6
        else:
            return 0.3

    def _compute_peak_valley_likelihood(self, pv_result: Dict, threshold: float) -> float:
        ratio = pv_result['peak_valley_ratio']
        regularity = pv_result['cycle_regularity']

        if pv_result['is_regular_cycle']:
            return 2.0 + regularity
        elif ratio > threshold and regularity > 0.3:
            return 1.5
        elif ratio > threshold * 0.8:
            return 1.0
        else:
            return 0.6

    def _compute_fft_likelihood(self, fft_result: FFTResult, weight: float) -> float:
        if not fft_result.is_periodic:
            return 1.0 - 0.3 * weight

        strength = fft_result.period_strength
        base_lr = 1.5 + strength * 2.0
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
        n = len(arr)

        if n < 3:
            return "unknown", "unknown"

        recent_trend = arr[-1] - arr[-2] if n >= 2 else 0
        if recent_trend > 0:
            current_phase = "rising"
        elif recent_trend < 0:
            current_phase = "falling"
        else:
            current_phase = "flat"

        latest = arr[-1]
        percentile = np.sum(arr[:-1] < latest) / (n - 1) * 100 if n > 1 else 50

        if cycle_component is not None and len(cycle_component) > 0:
            cycle_latest = cycle_component[-1]
            cycle_max = np.max(np.abs(cycle_component))
            if cycle_max > 0:
                cycle_position_pct = (cycle_latest / cycle_max + 1) / 2 * 100
            else:
                cycle_position_pct = 50
            combined_pct = 0.6 * percentile + 0.4 * cycle_position_pct
        else:
            combined_pct = percentile

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
        self, count: int, industry: str,
        preconditions: CyclicalPreconditions, prior_probability: float
    ) -> CyclicalPatternResult:
        warnings = [
            TrendWarning(
                code="INSUFFICIENT_DATA_FOR_CYCLICAL",
                level="warning",
                message=f"数据不足({count}年)，无法进行可靠的周期性分析",
                context={"data_years": count, "min_required": 5},
            )
        ]
        for w in preconditions.warnings:
            warnings.append(TrendWarning(code="DATA_LIMITATION", level="info", message=w, context={}))

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
            industry_cyclical=prior_probability > 0.5,
            cyclical_confidence=0.0,
            peak_to_trough_threshold=2.0,
            trend_r_squared_max=0.3,
            cv_threshold=0.3,
            industry=industry or "unknown",
            # v2.0: 数据不足时的默认值
            hp_cycle_amplitude=0.0,
            hp_cycle_volatility=0.0,
            hurst_exponent=0.5,  # 随机游走假设
            hurst_interpretation="unknown",
            hurst_confidence=0.0,
            acf_lag1=0.0,
            acf_has_cyclical_pattern=False,
            ljung_box_pvalue=1.0,
            confidence_factors=[
                f"Data Reliability: Grade {preconditions.reliability_grade}",
                f"Confidence ceiling: {preconditions.confidence_ceiling:.0%}",
            ],
            warnings=warnings,
        )

    def default(self) -> CyclicalPatternResult:
        """Return default result for insufficient data (ProbeProtocol compliance)."""
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
            industry_cyclical=False,
            cyclical_confidence=0.0,
            peak_to_trough_threshold=2.0,
            trend_r_squared_max=0.3,
            cv_threshold=0.3,
            industry="unknown",
            # v2.0: 默认值
            hp_cycle_amplitude=0.0,
            hp_cycle_volatility=0.0,
            hurst_exponent=0.5,
            hurst_interpretation="unknown",
            hurst_confidence=0.0,
            acf_lag1=0.0,
            acf_has_cyclical_pattern=False,
            ljung_box_pvalue=1.0,
            confidence_factors=[],
            warnings=[TrendWarning(
                code="INSUFFICIENT_DATA",
                level="warning",
                message="Insufficient data",
            )],
        )