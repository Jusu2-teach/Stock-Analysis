"""
PE-Band 历史估值带模型
=========================

基于历史PE数据构建估值带，判断当前估值水平的高低。

核心思想:
- 历史分位数法：计算当前PE在历史数据中的分位数
- 动态估值带：根据基本面变化调整估值中枢
- 均值回归假设：PE会向历史均值回归

理论基础:
- Benjamin Graham: "价格围绕价值波动"
- Mean Reversion: 估值存在均值回归特性
- Quantile Analysis: 分位数分析识别高估/低估区域

版本: 1.0.0
日期: 2026-01-17
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Tuple

import numpy as np

logger = logging.getLogger(__name__)


class ValuationZone(str, Enum):
    """估值区域"""
    EXTREMELY_UNDERVALUED = "extremely_undervalued"  # 极度低估 (<10%)
    UNDERVALUED = "undervalued"                      # 低估 (10-30%)
    FAIRLY_VALUED = "fairly_valued"                  # 合理 (30-70%)
    OVERVALUED = "overvalued"                        # 高估 (70-90%)
    EXTREMELY_OVERVALUED = "extremely_overvalued"    # 极度高估 (>90%)

    @property
    def display_name(self) -> str:
        """中文显示名"""
        names = {
            "extremely_undervalued": "极度低估",
            "undervalued": "低估",
            "fairly_valued": "合理估值",
            "overvalued": "高估",
            "extremely_overvalued": "极度高估",
        }
        return names[self.value]

    @property
    def emoji(self) -> str:
        """表情符号"""
        emojis = {
            "extremely_undervalued": "🔥",
            "undervalued": "📉",
            "fairly_valued": "➡️",
            "overvalued": "📈",
            "extremely_overvalued": "⚠️",
        }
        return emojis[self.value]


@dataclass
class PEBandResult:
    """PE-Band 分析结果

    Attributes:
        current_pe: 当前PE
        historical_mean: 历史均值PE
        historical_median: 历史中位数PE
        percentile: 当前PE在历史中的分位数 (0-100)
        zone: 估值区域
        pe_bands: PE估值带 {percentile: pe_value}
        years_covered: 历史数据覆盖年限
        is_valid: 数据是否有效
        warnings: 警告信息
    """
    current_pe: float
    historical_mean: float
    historical_median: float
    percentile: float
    zone: ValuationZone
    pe_bands: Dict[int, float] = field(default_factory=dict)
    years_covered: int = 0
    is_valid: bool = True
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            "current_pe": self.current_pe,
            "historical_mean": self.historical_mean,
            "historical_median": self.historical_median,
            "percentile": self.percentile,
            "zone": self.zone.value,
            "zone_display": self.zone.display_name,
            "pe_bands": self.pe_bands,
            "years_covered": self.years_covered,
            "is_valid": self.is_valid,
            "warnings": self.warnings,
        }


class PEBandModel:
    """PE-Band 估值模型

    通过历史PE数据构建估值带，识别高估/低估区域。

    估值带定义 (分位数):
        - 0-10%: 极度低估区域
        - 10-30%: 低估区域
        - 30-70%: 合理估值区域
        - 70-90%: 高估区域
        - 90-100%: 极度高估区域

    Examples:
        >>> model = PEBandModel()
        >>> historical_pes = [10, 12, 15, 18, 20, 22, 25, 28, 30, 32]
        >>> result = model.analyze(
        ...     current_pe=16,
        ...     historical_pes=historical_pes
        ... )
        >>> print(f"当前估值: {result.zone.display_name}")
        >>> print(f"历史分位数: {result.percentile:.1f}%")
    """

    def __init__(self, min_years: int = 3):
        """初始化

        Args:
            min_years: 最低历史数据年限要求
        """
        self.min_years = min_years
        self.logger = logging.getLogger(self.__class__.__name__)

    def analyze(
        self,
        current_pe: float,
        historical_pes: List[float],
        filter_outliers: bool = True,
    ) -> PEBandResult:
        """分析当前PE估值水平

        Args:
            current_pe: 当前PE
            historical_pes: 历史PE列表（从旧到新）
            filter_outliers: 是否过滤离群值

        Returns:
            PEBandResult: PE-Band分析结果
        """
        warnings = []

        # 数据验证
        if len(historical_pes) < self.min_years:
            return PEBandResult(
                current_pe=current_pe,
                historical_mean=0.0,
                historical_median=0.0,
                percentile=50.0,
                zone=ValuationZone.FAIRLY_VALUED,
                years_covered=len(historical_pes),
                is_valid=False,
                warnings=[f"历史数据不足 (需要至少{self.min_years}年)"],
            )

        # 清洗数据：移除非正数、NaN、Inf
        clean_pes = self._clean_data(historical_pes)

        if len(clean_pes) < self.min_years:
            return PEBandResult(
                current_pe=current_pe,
                historical_mean=0.0,
                historical_median=0.0,
                percentile=50.0,
                zone=ValuationZone.FAIRLY_VALUED,
                years_covered=len(clean_pes),
                is_valid=False,
                warnings=["有效数据点不足"],
            )

        # 过滤离群值（使用IQR方法）
        if filter_outliers:
            clean_pes, outlier_count = self._filter_outliers(clean_pes)
            if outlier_count > 0:
                warnings.append(f"移除了{outlier_count}个离群PE值")

        # 计算统计指标
        historical_mean = float(np.mean(clean_pes))
        historical_median = float(np.median(clean_pes))

        # 计算当前PE的分位数
        percentile = self._calculate_percentile(current_pe, clean_pes)

        # 判断估值区域
        zone = self._determine_zone(percentile)

        # 构建PE估值带
        pe_bands = self._build_pe_bands(clean_pes)

        # 额外警告
        if current_pe < 0:
            warnings.append("当前PE为负，公司亏损")
        elif current_pe > historical_mean * 3:
            warnings.append("当前PE远超历史均值，可能存在泡沫")

        return PEBandResult(
            current_pe=current_pe,
            historical_mean=historical_mean,
            historical_median=historical_median,
            percentile=percentile,
            zone=zone,
            pe_bands=pe_bands,
            years_covered=len(clean_pes),
            is_valid=True,
            warnings=warnings,
        )

    def _clean_data(self, pes: List[float]) -> List[float]:
        """清洗PE数据

        移除：
        - 负数PE（亏损公司）
        - NaN
        - Inf
        """
        clean = []
        for pe in pes:
            if (
                pe is not None
                and not np.isnan(pe)
                and not np.isinf(pe)
                and pe > 0  # 只保留正数PE
            ):
                clean.append(float(pe))
        return clean

    def _filter_outliers(
        self,
        pes: List[float],
        k: float = 1.5,
    ) -> Tuple[List[float], int]:
        """使用IQR方法过滤离群值

        Args:
            pes: PE列表
            k: IQR倍数（通常使用1.5）

        Returns:
            (filtered_pes, outlier_count)
        """
        if len(pes) < 4:
            return pes, 0

        q1 = np.percentile(pes, 25)
        q3 = np.percentile(pes, 75)
        iqr = q3 - q1

        lower_bound = q1 - k * iqr
        upper_bound = q3 + k * iqr

        filtered = [pe for pe in pes if lower_bound <= pe <= upper_bound]
        outlier_count = len(pes) - len(filtered)

        return filtered, outlier_count

    def _calculate_percentile(self, current_pe: float, historical_pes: List[float]) -> float:
        """计算当前PE在历史数据中的分位数

        Args:
            current_pe: 当前PE
            historical_pes: 历史PE列表

        Returns:
            percentile: 分位数 (0-100)
        """
        # 统计小于等于current_pe的历史PE数量
        count_below = sum(1 for pe in historical_pes if pe <= current_pe)
        percentile = (count_below / len(historical_pes)) * 100

        return percentile

    def _determine_zone(self, percentile: float) -> ValuationZone:
        """根据分位数判断估值区域

        Args:
            percentile: 分位数 (0-100)

        Returns:
            ValuationZone: 估值区域
        """
        if percentile < 10:
            return ValuationZone.EXTREMELY_UNDERVALUED
        elif percentile < 30:
            return ValuationZone.UNDERVALUED
        elif percentile < 70:
            return ValuationZone.FAIRLY_VALUED
        elif percentile < 90:
            return ValuationZone.OVERVALUED
        else:
            return ValuationZone.EXTREMELY_OVERVALUED

    def _build_pe_bands(self, historical_pes: List[float]) -> Dict[int, float]:
        """构建PE估值带

        计算关键分位数对应的PE值：
        - 10%, 25%, 50%, 75%, 90%

        Returns:
            {percentile: pe_value}
        """
        percentiles = [10, 25, 50, 75, 90]
        pe_bands = {}

        for p in percentiles:
            pe_bands[p] = float(np.percentile(historical_pes, p))

        return pe_bands
