"""
═══════════════════════════════════════════════════════════════════════════════
AStock Evaluators v2.0 - 自适应阈值模块
═══════════════════════════════════════════════════════════════════════════════

根据行业类型、公司规模、市场周期动态调整评估阈值。
避免"一刀切"的静态阈值问题。

作者: AStock Team
版本: 2.0.0
═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


class IndustryCategory(Enum):
    """行业分类"""
    TECHNOLOGY = "technology"
    HEALTHCARE = "healthcare"
    CONSUMER_STAPLES = "consumer_staples"
    CONSUMER_DISCRETIONARY = "consumer_discretionary"
    FINANCIALS = "financials"
    INDUSTRIALS = "industrials"
    MATERIALS = "materials"
    ENERGY = "energy"
    UTILITIES = "utilities"
    REAL_ESTATE = "real_estate"
    TELECOM = "telecom"


class SizeTier(Enum):
    """公司规模分级"""
    MEGA_CAP = "mega_cap"      # > 1000亿
    LARGE_CAP = "large_cap"    # 200-1000亿
    MID_CAP = "mid_cap"        # 50-200亿
    SMALL_CAP = "small_cap"    # < 50亿


class MarketCycle(Enum):
    """市场周期阶段"""
    EXPANSION = "expansion"
    PEAK = "peak"
    CONTRACTION = "contraction"
    TROUGH = "trough"


@dataclass(frozen=True)
class ThresholdSet:
    """单个指标的阈值集合"""

    metric: str
    excellent: float
    good: float
    acceptable: float
    poor: float
    veto: float

    def get_grade(self, value: float, higher_is_better: bool = True) -> str:
        """根据值返回等级"""
        if higher_is_better:
            if value >= self.excellent:
                return "excellent"
            elif value >= self.good:
                return "good"
            elif value >= self.acceptable:
                return "acceptable"
            elif value >= self.poor:
                return "poor"
            else:
                return "veto"
        else:
            # 对于"越低越好"的指标
            if value <= self.excellent:
                return "excellent"
            elif value <= self.good:
                return "good"
            elif value <= self.acceptable:
                return "acceptable"
            elif value <= self.poor:
                return "poor"
            else:
                return "veto"

    def scale(self, multiplier: float) -> 'ThresholdSet':
        """按乘数缩放所有阈值"""
        if multiplier is None:
            return None
        return ThresholdSet(
            metric=self.metric,
            excellent=self.excellent * multiplier,
            good=self.good * multiplier,
            acceptable=self.acceptable * multiplier,
            poor=self.poor * multiplier,
            veto=self.veto * multiplier
        )

    def adjust(self, offset: float) -> 'ThresholdSet':
        """按偏移量调整所有阈值"""
        return ThresholdSet(
            metric=self.metric,
            excellent=self.excellent + offset,
            good=self.good + offset,
            acceptable=self.acceptable + offset,
            poor=self.poor + offset,
            veto=self.veto + offset
        )


@dataclass
class AdaptiveContext:
    """自适应调整的上下文"""

    industry: Optional[IndustryCategory] = None
    industry_name: Optional[str] = None  # 原始行业名称
    size_tier: Optional[SizeTier] = None
    market_cap_billion: Optional[float] = None
    market_cycle: Optional[MarketCycle] = None
    data_years: int = 10

    @classmethod
    def from_company_info(
        cls,
        industry_name: str,
        market_cap: float,  # 亿元
        current_cycle: str = "expansion"
    ) -> 'AdaptiveContext':
        """从公司基本信息构建上下文"""
        return cls(
            industry=cls._map_industry(industry_name),
            industry_name=industry_name,
            size_tier=cls._classify_size(market_cap),
            market_cap_billion=market_cap,
            market_cycle=MarketCycle(current_cycle)
        )

    @staticmethod
    def _map_industry(name: str) -> IndustryCategory:
        """将行业名称映射到分类"""
        # 简化映射，实际应使用更完整的映射表
        mapping = {
            "信息技术": IndustryCategory.TECHNOLOGY,
            "软件": IndustryCategory.TECHNOLOGY,
            "半导体": IndustryCategory.TECHNOLOGY,
            "医药": IndustryCategory.HEALTHCARE,
            "医疗": IndustryCategory.HEALTHCARE,
            "食品饮料": IndustryCategory.CONSUMER_STAPLES,
            "家电": IndustryCategory.CONSUMER_DISCRETIONARY,
            "汽车": IndustryCategory.CONSUMER_DISCRETIONARY,
            "银行": IndustryCategory.FINANCIALS,
            "保险": IndustryCategory.FINANCIALS,
            "机械": IndustryCategory.INDUSTRIALS,
            "化工": IndustryCategory.MATERIALS,
            "钢铁": IndustryCategory.MATERIALS,
            "煤炭": IndustryCategory.ENERGY,
            "电力": IndustryCategory.UTILITIES,
            "房地产": IndustryCategory.REAL_ESTATE,
            "通信": IndustryCategory.TELECOM,
        }

        for key, category in mapping.items():
            if key in name:
                return category

        return IndustryCategory.INDUSTRIALS  # 默认

    @staticmethod
    def _classify_size(market_cap: float) -> SizeTier:
        """根据市值分级"""
        if market_cap >= 1000:
            return SizeTier.MEGA_CAP
        elif market_cap >= 200:
            return SizeTier.LARGE_CAP
        elif market_cap >= 50:
            return SizeTier.MID_CAP
        else:
            return SizeTier.SMALL_CAP


class AdaptiveThresholdEngine:
    """
    自适应阈值引擎

    根据行业、规模、市场周期动态计算阈值。
    从 YAML 配置加载基础阈值和调整乘数。

    Example:
        >>> engine = AdaptiveThresholdEngine.with_defaults()
        >>> ctx = AdaptiveContext.from_company_info("医药生物", 500.0)
        >>> thresholds = engine.get_thresholds("roic_level", ctx)
        >>> print(thresholds.excellent)  # 医药行业调整后的阈值
    """

    def __init__(
        self,
        base_thresholds: Dict[str, ThresholdSet],
        industry_multipliers: Dict[str, Dict[str, float]],
        size_multipliers: Dict[str, Dict[str, float]],
        cycle_adjustments: Dict[str, Dict[str, float]]
    ):
        self._base_thresholds = base_thresholds
        self._industry_multipliers = industry_multipliers
        self._size_multipliers = size_multipliers
        self._cycle_adjustments = cycle_adjustments

    @classmethod
    def from_config(cls, config_path: str | Path) -> 'AdaptiveThresholdEngine':
        """从 YAML 配置文件加载"""
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        # 解析基础阈值
        base_thresholds = {}
        for name, values in config.get('base_thresholds', {}).items():
            base_thresholds[name] = ThresholdSet(
                metric=name,
                excellent=values.get('excellent', 0),
                good=values.get('good', 0),
                acceptable=values.get('acceptable', 0),
                poor=values.get('poor', 0),
                veto=values.get('veto', 0)
            )

        # 解析乘数
        industry_multipliers = config.get('industry_multipliers', {})
        size_multipliers = config.get('size_tier_multipliers', {})
        cycle_adjustments = config.get('market_cycle_adjustments', {})

        return cls(
            base_thresholds=base_thresholds,
            industry_multipliers=industry_multipliers,
            size_multipliers=size_multipliers,
            cycle_adjustments=cycle_adjustments
        )

    @classmethod
    def with_defaults(cls) -> 'AdaptiveThresholdEngine':
        """使用内置默认值创建"""
        base_thresholds = {
            "roic_level": ThresholdSet("roic_level", 15.0, 10.0, 7.0, 4.0, 0.0),
            "roic_trend": ThresholdSet("roic_trend", 0.02, 0.01, 0.0, -0.01, -0.03),
            "roe_level": ThresholdSet("roe_level", 20.0, 15.0, 10.0, 5.0, 0.0),
            "revenue_growth": ThresholdSet("revenue_growth", 0.15, 0.10, 0.05, 0.0, -0.10),
            "gross_margin": ThresholdSet("gross_margin", 40.0, 30.0, 20.0, 10.0, 0.0),
            "net_margin": ThresholdSet("net_margin", 15.0, 10.0, 5.0, 2.0, 0.0),
            "ocf_ratio": ThresholdSet("ocf_ratio", 1.2, 1.0, 0.8, 0.5, 0.0),
        }

        # 默认行业乘数
        industry_multipliers = {
            IndustryCategory.TECHNOLOGY.value: {"roic_level": 1.2, "revenue_growth": 1.5},
            IndustryCategory.FINANCIALS.value: {"roic_level": 0.6, "roe_level": 0.8},
            IndustryCategory.UTILITIES.value: {"roic_level": 0.7, "revenue_growth": 0.5},
        }

        size_multipliers = {
            SizeTier.MEGA_CAP.value: {"revenue_growth": 0.7},
            SizeTier.SMALL_CAP.value: {"revenue_growth": 1.3},
        }

        cycle_adjustments = {
            MarketCycle.CONTRACTION.value: {"roic_level": -2.0, "revenue_growth": -0.05},
            MarketCycle.EXPANSION.value: {"roic_level": 0.0, "revenue_growth": 0.0},
        }

        return cls(
            base_thresholds=base_thresholds,
            industry_multipliers=industry_multipliers,
            size_multipliers=size_multipliers,
            cycle_adjustments=cycle_adjustments
        )

    def get_thresholds(
        self,
        metric: str,
        context: AdaptiveContext
    ) -> ThresholdSet:
        """
        获取特定指标的自适应阈值

        调整顺序：
        1. 基础阈值
        2. 行业乘数（乘法）
        3. 规模乘数（乘法）
        4. 周期调整（加法）
        """
        if metric not in self._base_thresholds:
            raise ValueError(f"未知指标: {metric}")

        thresholds = self._base_thresholds[metric]

        # 应用行业乘数
        if context.industry:
            industry_key = context.industry.value
            if industry_key in self._industry_multipliers:
                multiplier = self._industry_multipliers[industry_key].get(metric, 1.0)
                thresholds = thresholds.scale(multiplier)

        # 应用规模乘数
        if context.size_tier:
            size_key = context.size_tier.value
            if size_key in self._size_multipliers:
                multiplier = self._size_multipliers[size_key].get(metric, 1.0)
                thresholds = thresholds.scale(multiplier)

        # 应用周期调整
        if context.market_cycle:
            cycle_key = context.market_cycle.value
            if cycle_key in self._cycle_adjustments:
                offset = self._cycle_adjustments[cycle_key].get(metric, 0.0)
                thresholds = thresholds.adjust(offset)

        return thresholds

    def get_all_thresholds(self, context: AdaptiveContext) -> Dict[str, ThresholdSet]:
        """获取所有指标的自适应阈值"""
        return {
            metric: self.get_thresholds(metric, context)
            for metric in self._base_thresholds
        }

    def evaluate_metric(
        self,
        metric: str,
        value: float,
        context: AdaptiveContext,
        higher_is_better: bool = True
    ) -> tuple[str, ThresholdSet]:
        """
        评估单个指标

        Returns:
            (grade, thresholds_used)
        """
        thresholds = self.get_thresholds(metric, context)
        grade = thresholds.get_grade(value, higher_is_better)
        return grade, thresholds


# ═══════════════════════════════════════════════════════════════════════════════
# 便捷函数
# ═══════════════════════════════════════════════════════════════════════════════

_DEFAULT_ENGINE: Optional[AdaptiveThresholdEngine] = None


def get_default_engine() -> AdaptiveThresholdEngine:
    """获取默认的自适应阈值引擎（单例）"""
    global _DEFAULT_ENGINE
    if _DEFAULT_ENGINE is None:
        _DEFAULT_ENGINE = AdaptiveThresholdEngine.with_defaults()
    return _DEFAULT_ENGINE


def adaptive_evaluate(
    metric: str,
    value: float,
    industry: str = "default",
    market_cap: float = 100.0,
    higher_is_better: bool = True
) -> str:
    """便捷评估函数"""
    engine = get_default_engine()
    context = AdaptiveContext.from_company_info(industry, market_cap)
    grade, _ = engine.evaluate_metric(metric, value, context, higher_is_better)
    return grade
