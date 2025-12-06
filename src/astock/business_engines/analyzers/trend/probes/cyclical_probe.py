"""
周期性模式检测器 (Cyclical Pattern Detector)
============================================

增强版周期性检测，支持：
1. 传统统计方法（CV、峰谷比）
2. FFT频谱分析（检测3-7年周期）
3. 行业先验知识
4. 周期位置判断（顶部/底部/中部）

金融周期检测的特殊考虑：
- 财务数据通常只有5-10年，很难检测长周期
- FFT在短序列上效果有限，需要结合先验知识
- 周期位置比"是否周期"更有实操价值

作者: AStock Analysis System
日期: 2025-12-06
"""

import logging
import numpy as np
from typing import List, Tuple, Optional, Dict, Any
from dataclasses import dataclass

from ..models import CyclicalPatternResult, TrendWarning
from ..config import get_default_config, get_cyclical_thresholds
from .common import DataQualityChecker

logger = logging.getLogger(__name__)


@dataclass
class FFTResult:
    """FFT分析结果"""
    dominant_period: Optional[float]  # 主导周期（年）
    period_strength: float            # 周期强度 (0-1)
    is_periodic: bool                 # 是否有显著周期性
    secondary_periods: List[float]    # 次要周期列表
    spectrum_peaks: List[Tuple[float, float]]  # (周期, 功率) 列表


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
    周期性模式检测器

    综合使用多种方法检测周期性：
    1. 统计方法（CV、峰谷比）
    2. FFT频谱分析
    3. 行业先验知识

    新增功能：
    - 周期位置判断（cycle_position）
    - 周期置信度评估
    - FFT主导周期估计
    """

    def __init__(self, config=None):
        self.config = config or get_default_config()
        self.fft_analyzer = FFTCyclicalAnalyzer(min_period=3.0, max_period=7.0)

    def detect(self, values: List[float], industry: str = None) -> CyclicalPatternResult:
        arr = np.array(values, dtype=float)

        if len(arr) < self.config.min_periods:
            return self._insufficient_data_result(len(arr), industry)

        # 1. Leverage Industry Priors
        is_known_cyclical = self.config.is_cyclical_industry(industry)

        # 2. Adjust Thresholds based on Prior
        thresholds = get_cyclical_thresholds(industry)
        if is_known_cyclical:
            # Relax thresholds for known cyclical industries to avoid false negatives
            # due to short 5-year window
            thresholds['cv_threshold'] *= 0.8
            thresholds['peak_valley_ratio'] *= 0.8

        mean = np.mean(arr)
        std = np.std(arr, ddof=1)
        cv = abs(std / mean) if mean != 0 else float('inf')

        # ========== 新增: FFT频谱分析 ==========
        fft_result = self.fft_analyzer.analyze(arr)

        # FFT检测到周期性会增加置信度
        fft_confidence_boost = 0.0
        if fft_result.is_periodic:
            fft_confidence_boost = 0.2
            logger.debug(
                f"[{industry}] FFT检测到周期: {fft_result.dominant_period:.1f}年, "
                f"强度: {fft_result.period_strength:.2%}"
            )
        # =====================================

        if len(arr) >= 3:
            peaks_indices = []
            valleys_indices = []

            for i in range(1, len(arr) - 1):
                if arr[i] > arr[i-1] and arr[i] > arr[i+1]:
                    peaks_indices.append(i)
                elif arr[i] < arr[i-1] and arr[i] < arr[i+1]:
                    valleys_indices.append(i)

            if peaks_indices and valleys_indices:
                peaks = arr[peaks_indices]
                valleys = arr[valleys_indices]
                peak_valley_ratio = np.mean(peaks) / np.mean(valleys) if np.mean(valleys) > 0 else float('inf')
            else:
                peak_valley_ratio = 1.0
        else:
            peak_valley_ratio = 1.0
            peaks_indices = []
            valleys_indices = []

        # 3. Enhanced Detection Logic (整合FFT结果)
        is_cyclical = (
            cv > thresholds['cv_threshold'] and
            peak_valley_ratio > thresholds['peak_valley_ratio']
        ) or fft_result.is_periodic  # FFT检测到周期也算周期性

        # If known cyclical, be more aggressive in detection
        if is_known_cyclical and not is_cyclical:
             # If CV is high enough, assume cyclical even if peak/valley ratio is not perfect
             # (e.g. just one big drop)
             if cv > thresholds['cv_threshold'] * 1.2:
                 is_cyclical = True

        # 计算综合置信度
        base_confidence = min(
            (cv / thresholds['cv_threshold']) * 0.4 +
            (peak_valley_ratio / thresholds['peak_valley_ratio']) * 0.4 +
            fft_result.period_strength * 0.2,  # FFT强度贡献
            1.0
        )

        confidence = base_confidence + fft_confidence_boost
        if is_known_cyclical:
            confidence = min(confidence * 1.2, 1.0)
            # Force true if confidence is decent
            if confidence > 0.6:
                is_cyclical = True

        confidence = min(confidence, 1.0)

        # ========== 新增: 周期位置判断 ==========
        current_phase, cycle_position = self._determine_cycle_position(arr, peaks_indices, valleys_indices)
        # =====================================

        warnings = []
        if is_cyclical:
            msg = f"Cyclical pattern detected (confidence: {confidence:.2%})"
            if is_known_cyclical:
                msg += " [Industry Prior]"
            if fft_result.is_periodic:
                msg += f" [FFT: {fft_result.dominant_period:.1f}yr cycle]"

            warnings.append(
                TrendWarning(
                    code="CYCLICAL_PATTERN_DETECTED",
                    level="info",
                    message=msg,
                    context={
                        "cv": float(cv),
                        "peak_valley_ratio": float(peak_valley_ratio),
                        "is_known_cyclical": is_known_cyclical,
                        "fft_dominant_period": fft_result.dominant_period,
                        "fft_period_strength": float(fft_result.period_strength),
                        "cycle_position": cycle_position,
                    },
                )
            )

        # 周期底部警告（可能是买入机会）
        if is_cyclical and cycle_position == "bottom":
            warnings.append(
                TrendWarning(
                    code="CYCLE_BOTTOM_DETECTED",
                    level="info",
                    message="处于周期底部区域，基本面下行可能是周期性因素而非结构性问题",
                    context={"cycle_position": cycle_position, "phase": current_phase},
                )
            )

        # 构建 confidence_factors
        confidence_factors = [
            f"CV={cv:.3f} (threshold={thresholds['cv_threshold']:.3f})",
            f"Peak/Valley={peak_valley_ratio:.3f} (threshold={thresholds['peak_valley_ratio']:.3f})",
            f"Industry Prior={is_known_cyclical}",
        ]
        if fft_result.is_periodic:
            confidence_factors.append(
                f"FFT Dominant Period={fft_result.dominant_period:.1f}yr (strength={fft_result.period_strength:.2%})"
            )

        return CyclicalPatternResult(
            is_cyclical=is_cyclical,
            peak_to_trough_ratio=float(peak_valley_ratio),
            has_middle_peak=len(peaks_indices) > 0,
            has_wave_pattern=len(peaks_indices) > 1 and len(valleys_indices) > 1,
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

    def _determine_cycle_position(
        self,
        arr: np.ndarray,
        peaks_indices: List[int],
        valleys_indices: List[int]
    ) -> Tuple[str, str]:
        """
        判断周期位置

        Args:
            arr: 数据数组
            peaks_indices: 峰值索引列表
            valleys_indices: 谷值索引列表

        Returns:
            (current_phase, cycle_position)
            - current_phase: "rising" | "falling" | "unknown"
            - cycle_position: "top" | "bottom" | "mid_up" | "mid_down" | "unknown"
        """
        n = len(arr)

        # 1. 判断当前相位
        if n >= 2:
            if arr[-1] > arr[-2]:
                current_phase = "rising"
            else:
                current_phase = "falling"
        else:
            current_phase = "unknown"

        # 2. 判断周期位置
        # 基于最新值在历史分位数中的位置
        if n < 3:
            return current_phase, "unknown"

        latest = arr[-1]
        percentile = np.sum(arr[:-1] < latest) / (n - 1) * 100

        # 结合分位数和趋势判断位置
        if percentile >= 80:
            cycle_position = "top"
        elif percentile <= 20:
            cycle_position = "bottom"
        elif current_phase == "rising":
            cycle_position = "mid_up"
        elif current_phase == "falling":
            cycle_position = "mid_down"
        else:
            cycle_position = "unknown"

        # 3. 使用峰谷信息验证
        if peaks_indices and valleys_indices:
            last_peak = peaks_indices[-1] if peaks_indices else -1
            last_valley = valleys_indices[-1] if valleys_indices else -1

            # 如果最近的是谷且当前在上升，很可能刚过底部
            if last_valley > last_peak and current_phase == "rising" and percentile < 50:
                cycle_position = "mid_up"  # 刚从底部回升

            # 如果最近的是峰且当前在下降，可能正在见顶
            if last_peak > last_valley and current_phase == "falling" and percentile > 50:
                cycle_position = "mid_down"  # 刚从顶部回落

        return current_phase, cycle_position

    def _insufficient_data_result(self, count: int, industry: str) -> CyclicalPatternResult:
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
            peak_to_trough_threshold=3.0,
            trend_r_squared_max=0.3,
            cv_threshold=0.3,
            industry=industry or "unknown",
            confidence_factors=[],
            warnings=[
                TrendWarning(
                    code="INSUFFICIENT_DATA",
                    level="info",
                    message=f"Insufficient data points: {count}",
                    context={},
                )
            ],
        )
