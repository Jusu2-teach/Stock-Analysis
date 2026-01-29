"""
宏观经济指标计算 (Macroeconomic Indicators)
==========================================

计算关键宏观经济指标及其趋势：
- GDP增速、CPI/PPI、PMI
- M2增速、社融规模
- 利率环境（国债收益率）
- 汇率水平

设计原则:
- 使用官方数据源（tushare/wind）
- 计算同比/环比/趋势
- 提供领先/同步/滞后指标分类

版本: 1.0.0
日期: 2026-01-17
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from enum import Enum

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class IndicatorType(Enum):
    """指标类型分类"""
    LEADING = "leading"      # 领先指标（如PMI、M2增速）
    COINCIDENT = "coincident"  # 同步指标（如GDP、工业增加值）
    LAGGING = "lagging"      # 滞后指标（如CPI、失业率）


@dataclass(frozen=True)
class MacroIndicatorResult:
    """宏观指标计算结果"""

    # GDP相关
    gdp_growth_yoy: Optional[float] = None  # GDP同比增速(%)
    gdp_growth_qoq: Optional[float] = None  # GDP环比增速(%)
    gdp_trend: Optional[str] = None         # 趋势：accelerating/stable/decelerating

    # 价格指数
    cpi_yoy: Optional[float] = None         # CPI同比(%)
    ppi_yoy: Optional[float] = None         # PPI同比(%)
    inflation_level: Optional[str] = None   # 通胀水平：low/moderate/high

    # PMI（采购经理人指数）
    manufacturing_pmi: Optional[float] = None     # 制造业PMI
    service_pmi: Optional[float] = None           # 服务业PMI
    pmi_trend: Optional[str] = None               # PMI趋势

    # 货币与信贷
    m2_growth_yoy: Optional[float] = None         # M2同比增速(%)
    social_financing_growth: Optional[float] = None  # 社融增速(%)
    credit_environment: Optional[str] = None      # 信贷环境：tight/neutral/loose

    # 利率环境
    bond_yield_10y: Optional[float] = None        # 10年期国债收益率(%)
    bond_yield_1y: Optional[float] = None         # 1年期国债收益率(%)
    yield_curve_slope: Optional[float] = None     # 收益率曲线斜率
    rate_environment: Optional[str] = None        # 利率环境：low/moderate/high

    # 综合判断
    economic_momentum: Optional[str] = None       # 经济动能：strong/moderate/weak
    policy_stance: Optional[str] = None           # 政策立场：easing/neutral/tightening

    # 元数据
    reference_date: Optional[str] = None          # 参考日期
    data_quality: Optional[str] = None            # 数据质量：complete/partial/insufficient
    warnings: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            # GDP
            'gdp_growth_yoy': self.gdp_growth_yoy,
            'gdp_growth_qoq': self.gdp_growth_qoq,
            'gdp_trend': self.gdp_trend,

            # 价格
            'cpi_yoy': self.cpi_yoy,
            'ppi_yoy': self.ppi_yoy,
            'inflation_level': self.inflation_level,

            # PMI
            'manufacturing_pmi': self.manufacturing_pmi,
            'service_pmi': self.service_pmi,
            'pmi_trend': self.pmi_trend,

            # 货币
            'm2_growth_yoy': self.m2_growth_yoy,
            'social_financing_growth': self.social_financing_growth,
            'credit_environment': self.credit_environment,

            # 利率
            'bond_yield_10y': self.bond_yield_10y,
            'bond_yield_1y': self.bond_yield_1y,
            'yield_curve_slope': self.yield_curve_slope,
            'rate_environment': self.rate_environment,

            # 综合
            'economic_momentum': self.economic_momentum,
            'policy_stance': self.policy_stance,

            # 元数据
            'reference_date': self.reference_date,
            'data_quality': self.data_quality,
            'warnings': self.warnings,
        }


class MacroIndicators:
    """宏观经济指标计算器"""

    # 阈值配置
    GDP_HIGH_GROWTH = 6.5      # GDP高增长阈值
    GDP_LOW_GROWTH = 4.0       # GDP低增长阈值

    CPI_HIGH = 3.0             # CPI高通胀阈值
    CPI_LOW = 1.0              # CPI低通胀阈值

    PMI_EXPANSION = 50.0       # PMI扩张阈值
    PMI_STRONG = 52.0          # PMI强劲阈值
    PMI_WEAK = 48.0            # PMI疲弱阈值

    M2_HIGH_GROWTH = 10.0      # M2高增速阈值
    M2_LOW_GROWTH = 6.0        # M2低增速阈值

    YIELD_HIGH = 3.5           # 高利率阈值
    YIELD_LOW = 2.5            # 低利率阈值

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def calculate(
        self,
        data: pd.DataFrame,
        reference_date: Optional[str] = None,
    ) -> MacroIndicatorResult:
        """计算宏观指标

        Args:
            data: DataFrame包含宏观数据，列名示例：
                - date: 日期
                - gdp_yoy: GDP同比增速
                - cpi_yoy: CPI同比
                - pmi: 制造业PMI
                - m2_yoy: M2同比增速
                - bond_yield_10y: 10年期国债收益率
            reference_date: 参考日期（默认使用最新数据）

        Returns:
            MacroIndicatorResult
        """
        self.logger.info(f"计算宏观指标: reference_date={reference_date}")

        if data.empty:
            return self._create_insufficient_result("输入数据为空")

        # 获取参考日期的数据
        if reference_date:
            mask = data['date'] == reference_date
            if not mask.any():
                return self._create_insufficient_result(f"未找到参考日期: {reference_date}")
            current = data[mask].iloc[0]
        else:
            current = data.iloc[-1]
            reference_date = str(current['date'])

        warnings = []

        # GDP分析
        gdp_growth_yoy = self._safe_get(current, 'gdp_yoy')
        gdp_growth_qoq = self._safe_get(current, 'gdp_qoq')
        gdp_trend = self._analyze_gdp_trend(data, reference_date, warnings)

        # 价格指数分析
        cpi_yoy = self._safe_get(current, 'cpi_yoy')
        ppi_yoy = self._safe_get(current, 'ppi_yoy')
        inflation_level = self._classify_inflation(cpi_yoy)

        # PMI分析
        manufacturing_pmi = self._safe_get(current, 'pmi')
        service_pmi = self._safe_get(current, 'service_pmi')
        pmi_trend = self._analyze_pmi_trend(data, reference_date, warnings)

        # 货币与信贷
        m2_growth_yoy = self._safe_get(current, 'm2_yoy')
        social_financing_growth = self._safe_get(current, 'social_financing_yoy')
        credit_environment = self._classify_credit_environment(m2_growth_yoy, social_financing_growth)

        # 利率环境
        bond_yield_10y = self._safe_get(current, 'bond_yield_10y')
        bond_yield_1y = self._safe_get(current, 'bond_yield_1y')
        yield_curve_slope = None
        if bond_yield_10y is not None and bond_yield_1y is not None:
            yield_curve_slope = bond_yield_10y - bond_yield_1y
        rate_environment = self._classify_rate_environment(bond_yield_10y)

        # 综合判断
        economic_momentum = self._assess_economic_momentum(
            gdp_growth_yoy, manufacturing_pmi, m2_growth_yoy
        )
        policy_stance = self._infer_policy_stance(
            m2_growth_yoy, bond_yield_10y, cpi_yoy
        )

        # 数据质量评估
        data_quality = self._assess_data_quality(current)

        return MacroIndicatorResult(
            gdp_growth_yoy=gdp_growth_yoy,
            gdp_growth_qoq=gdp_growth_qoq,
            gdp_trend=gdp_trend,
            cpi_yoy=cpi_yoy,
            ppi_yoy=ppi_yoy,
            inflation_level=inflation_level,
            manufacturing_pmi=manufacturing_pmi,
            service_pmi=service_pmi,
            pmi_trend=pmi_trend,
            m2_growth_yoy=m2_growth_yoy,
            social_financing_growth=social_financing_growth,
            credit_environment=credit_environment,
            bond_yield_10y=bond_yield_10y,
            bond_yield_1y=bond_yield_1y,
            yield_curve_slope=yield_curve_slope,
            rate_environment=rate_environment,
            economic_momentum=economic_momentum,
            policy_stance=policy_stance,
            reference_date=reference_date,
            data_quality=data_quality,
            warnings=warnings,
        )

    def _safe_get(self, row: pd.Series, col: str) -> Optional[float]:
        """安全获取值"""
        if col not in row.index:
            return None
        val = row[col]
        if pd.isna(val):
            return None
        return float(val)

    def _analyze_gdp_trend(
        self, data: pd.DataFrame, reference_date: str, warnings: List[str]
    ) -> Optional[str]:
        """分析GDP趋势"""
        if 'gdp_yoy' not in data.columns:
            warnings.append("GDP数据缺失")
            return None

        # 获取最近3期数据
        gdp_series = data[data['date'] <= reference_date]['gdp_yoy'].dropna()
        if len(gdp_series) < 2:
            return "insufficient_data"

        recent = gdp_series.iloc[-3:].values
        if len(recent) < 2:
            return "insufficient_data"

        # 计算趋势
        if len(recent) >= 3:
            # 连续加速
            if recent[-1] > recent[-2] > recent[-3]:
                return "accelerating"
            # 连续减速
            elif recent[-1] < recent[-2] < recent[-3]:
                return "decelerating"

        # 简单比较
        if recent[-1] > recent[-2]:
            return "improving"
        elif recent[-1] < recent[-2]:
            return "deteriorating"
        else:
            return "stable"

    def _classify_inflation(self, cpi_yoy: Optional[float]) -> Optional[str]:
        """分类通胀水平"""
        if cpi_yoy is None:
            return None

        if cpi_yoy >= self.CPI_HIGH:
            return "high"
        elif cpi_yoy <= self.CPI_LOW:
            return "low"
        else:
            return "moderate"

    def _analyze_pmi_trend(
        self, data: pd.DataFrame, reference_date: str, warnings: List[str]
    ) -> Optional[str]:
        """分析PMI趋势"""
        if 'pmi' not in data.columns:
            warnings.append("PMI数据缺失")
            return None

        pmi_series = data[data['date'] <= reference_date]['pmi'].dropna()
        if len(pmi_series) < 2:
            return "insufficient_data"

        recent = pmi_series.iloc[-3:].values
        current_pmi = recent[-1]

        # 扩张/收缩
        if current_pmi >= self.PMI_STRONG:
            return "strong_expansion"
        elif current_pmi >= self.PMI_EXPANSION:
            return "moderate_expansion"
        elif current_pmi >= self.PMI_WEAK:
            return "mild_contraction"
        else:
            return "significant_contraction"

    def _classify_credit_environment(
        self, m2_yoy: Optional[float], social_financing_yoy: Optional[float]
    ) -> Optional[str]:
        """分类信贷环境"""
        if m2_yoy is None:
            return None

        if m2_yoy >= self.M2_HIGH_GROWTH:
            return "loose"
        elif m2_yoy <= self.M2_LOW_GROWTH:
            return "tight"
        else:
            return "neutral"

    def _classify_rate_environment(self, bond_yield_10y: Optional[float]) -> Optional[str]:
        """分类利率环境"""
        if bond_yield_10y is None:
            return None

        if bond_yield_10y >= self.YIELD_HIGH:
            return "high"
        elif bond_yield_10y <= self.YIELD_LOW:
            return "low"
        else:
            return "moderate"

    def _assess_economic_momentum(
        self,
        gdp_yoy: Optional[float],
        pmi: Optional[float],
        m2_yoy: Optional[float],
    ) -> Optional[str]:
        """评估经济动能"""
        scores = []

        if gdp_yoy is not None:
            if gdp_yoy >= self.GDP_HIGH_GROWTH:
                scores.append(2)
            elif gdp_yoy >= self.GDP_LOW_GROWTH:
                scores.append(1)
            else:
                scores.append(0)

        if pmi is not None:
            if pmi >= self.PMI_STRONG:
                scores.append(2)
            elif pmi >= self.PMI_EXPANSION:
                scores.append(1)
            else:
                scores.append(0)

        if m2_yoy is not None:
            if m2_yoy >= self.M2_HIGH_GROWTH:
                scores.append(2)
            elif m2_yoy >= self.M2_LOW_GROWTH:
                scores.append(1)
            else:
                scores.append(0)

        if not scores:
            return None

        avg_score = np.mean(scores)
        if avg_score >= 1.5:
            return "strong"
        elif avg_score >= 0.5:
            return "moderate"
        else:
            return "weak"

    def _infer_policy_stance(
        self,
        m2_yoy: Optional[float],
        bond_yield: Optional[float],
        cpi_yoy: Optional[float],
    ) -> Optional[str]:
        """推断政策立场"""
        # 简化版：基于货币增速和利率水平
        if m2_yoy is None and bond_yield is None:
            return None

        easing_signals = 0
        tightening_signals = 0

        if m2_yoy is not None:
            if m2_yoy >= self.M2_HIGH_GROWTH:
                easing_signals += 1
            elif m2_yoy <= self.M2_LOW_GROWTH:
                tightening_signals += 1

        if bond_yield is not None:
            if bond_yield <= self.YIELD_LOW:
                easing_signals += 1
            elif bond_yield >= self.YIELD_HIGH:
                tightening_signals += 1

        if cpi_yoy is not None and cpi_yoy >= self.CPI_HIGH:
            tightening_signals += 1

        if easing_signals > tightening_signals:
            return "easing"
        elif tightening_signals > easing_signals:
            return "tightening"
        else:
            return "neutral"

    def _assess_data_quality(self, row: pd.Series) -> str:
        """评估数据质量"""
        key_fields = ['gdp_yoy', 'cpi_yoy', 'pmi', 'm2_yoy', 'bond_yield_10y']
        available = sum(1 for f in key_fields if f in row.index and pd.notna(row[f]))

        if available >= 4:
            return "complete"
        elif available >= 2:
            return "partial"
        else:
            return "insufficient"

    def _create_insufficient_result(self, reason: str) -> MacroIndicatorResult:
        """创建数据不足的结果"""
        return MacroIndicatorResult(
            data_quality="insufficient",
            warnings=[reason],
        )
