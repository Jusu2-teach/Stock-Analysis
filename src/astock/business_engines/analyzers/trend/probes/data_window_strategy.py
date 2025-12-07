"""
专业数据窗口策略 (Professional Data Window Strategy)
=====================================================

针对"10年vs5年数据选择"问题的专业解决方案。

核心思想:
    用10年数据的长度获得：
    1. 周期性检测能力
    2. 结构断点识别
    3. 长期均值回归参考

    用5年数据的精度获得：
    1. 当前经营状态
    2. 趋势斜率/CAGR
    3. 质量评分

指标分类处理策略:

    ┌────────────────┬──────────────────┬──────────────────────────────┐
    │  指标类型      │  主窗口          │  扩展窗口用途                 │
    ├────────────────┼──────────────────┼──────────────────────────────┤
    │  ROE/ROIC      │  近5年           │  周期位置判断、长期中枢       │
    │  净利率/毛利率 │  近5年           │  周期性检测、结构断点         │
    │  营收/利润     │  近5年           │  周期性检测、增长持续性       │
    │  资产周转率    │  近5年           │  长期趋势、行业周期           │
    │  负债率        │  近5年（警惕）   │  长期风险演变                 │
    └────────────────┴──────────────────┴──────────────────────────────┘

结构断点后的处理:
    - 如果检测到断点，只用断点后数据计算趋势
    - 但全量数据仍用于周期性分析（作为参考）

作者: AStock Analysis System
日期: 2025-12-07
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum

import numpy as np

from .multi_horizon_probe import (
    MultiHorizonAnalyzer,
    MultiHorizonResult,
    StructuralBreakResult,
    BreakType,
)

logger = logging.getLogger(__name__)


class MetricCategory(Enum):
    """指标类别"""
    EFFICIENCY = "efficiency"      # 效率类: ROE, ROIC, 利润率
    GROWTH = "growth"              # 增长类: 营收增长, 利润增长
    LEVERAGE = "leverage"          # 杠杆类: 资产负债率
    TURNOVER = "turnover"          # 周转类: 资产周转率
    QUALITY = "quality"            # 质量类: 现金流覆盖率


@dataclass
class WindowStrategy:
    """窗口策略配置

    Attributes:
        primary_window: 主窗口年数（用于趋势计算）
        extended_window: 扩展窗口年数（用于辅助分析）
        primary_weight: 主窗口权重
        use_break_detection: 是否启用断点检测
        use_cyclical_analysis: 是否用扩展窗口做周期分析
        description: 策略说明
    """
    primary_window: int
    extended_window: int
    primary_weight: float
    use_break_detection: bool
    use_cyclical_analysis: bool
    description: str


# 各类指标的默认策略
DEFAULT_STRATEGIES: Dict[MetricCategory, WindowStrategy] = {
    MetricCategory.EFFICIENCY: WindowStrategy(
        primary_window=5,
        extended_window=10,
        primary_weight=0.75,
        use_break_detection=True,
        use_cyclical_analysis=True,
        description="效率指标：近5年为主，10年用于周期判断和断点检测"
    ),
    MetricCategory.GROWTH: WindowStrategy(
        primary_window=5,
        extended_window=10,
        primary_weight=0.70,
        use_break_detection=True,
        use_cyclical_analysis=True,
        description="增长指标：近5年为主，10年用于增长持续性验证"
    ),
    MetricCategory.LEVERAGE: WindowStrategy(
        primary_window=5,
        extended_window=10,
        primary_weight=0.80,
        use_break_detection=True,
        use_cyclical_analysis=False,  # 杠杆不需要周期分析
        description="杠杆指标：重点关注近期，警惕长期趋势恶化"
    ),
    MetricCategory.TURNOVER: WindowStrategy(
        primary_window=5,
        extended_window=10,
        primary_weight=0.65,
        use_break_detection=True,
        use_cyclical_analysis=True,
        description="周转指标：周期性较强，扩展窗口权重略高"
    ),
    MetricCategory.QUALITY: WindowStrategy(
        primary_window=5,
        extended_window=10,
        primary_weight=0.70,
        use_break_detection=True,
        use_cyclical_analysis=False,
        description="质量指标：关注近期现金流质量"
    ),
}


def classify_metric(metric_name: str) -> MetricCategory:
    """
    自动分类指标

    Args:
        metric_name: 指标名称

    Returns:
        MetricCategory: 指标类别
    """
    name = metric_name.lower()

    # 效率类
    if any(k in name for k in ["roe", "roic", "margin", "rate", "ratio"]):
        if "debt" in name or "lever" in name:
            return MetricCategory.LEVERAGE
        return MetricCategory.EFFICIENCY

    # 增长类
    if any(k in name for k in ["revenue", "profit", "growth", "sales", "income"]):
        return MetricCategory.GROWTH

    # 杠杆类
    if any(k in name for k in ["debt", "liability", "leverage", "负债"]):
        return MetricCategory.LEVERAGE

    # 周转类
    if any(k in name for k in ["turn", "周转", "efficiency"]):
        return MetricCategory.TURNOVER

    # 质量类
    if any(k in name for k in ["cash", "fcf", "ocf", "现金", "经营"]):
        return MetricCategory.QUALITY

    # 默认为效率类
    return MetricCategory.EFFICIENCY


@dataclass
class ProfessionalAnalysisResult:
    """
    专业分析结果

    整合多时间窗口分析，提供专业级输出。

    Attributes:
        metric_name: 指标名称
        category: 指标类别
        strategy: 使用的窗口策略

        # 核心输出（用于后续评分）
        effective_slope: 有效斜率（加权或断点后）
        effective_cagr: 有效CAGR
        effective_cv: 有效波动系数
        effective_latest: 最新值

        # 时间窗口分析
        multi_horizon: 多时间窗口分析结果

        # 周期性分析（可选）
        cyclical_confidence: 周期性置信度 (0-1)
        cycle_position: 周期位置

        # 断点信息
        has_break: 是否有断点
        break_year: 断点年份（从1开始）

        # 质量评估
        data_quality_grade: 数据质量等级 (A-F)
        analysis_confidence: 分析置信度 (0-1)

        # 建议
        recommendation: 综合建议
        warnings: 警告列表
    """
    metric_name: str
    category: MetricCategory
    strategy: WindowStrategy

    # 核心输出
    effective_slope: float
    effective_cagr: float
    effective_cv: float
    effective_latest: float

    # 时间窗口分析
    multi_horizon: MultiHorizonResult

    # 周期性
    cyclical_confidence: float = 0.0
    cycle_position: str = "unknown"

    # 断点
    has_break: bool = False
    break_year: Optional[int] = None

    # 质量
    data_quality_grade: str = "C"
    analysis_confidence: float = 0.5

    # 建议
    recommendation: str = ""
    warnings: List[str] = field(default_factory=list)


class ProfessionalDataWindowStrategy:
    """
    专业数据窗口策略

    这是本模块的核心类，提供：
    1. 自动指标分类
    2. 策略驱动的窗口选择
    3. 断点感知的趋势计算
    4. 与周期分析的集成

    使用示例:
        >>> strategy = ProfessionalDataWindowStrategy()
        >>> result = strategy.analyze(
        ...     values_10y=[...],  # 10年数据
        ...     metric_name="roe"
        ... )
        >>> print(result.effective_slope)
        >>> print(result.recommendation)
    """

    def __init__(
        self,
        custom_strategies: Optional[Dict[MetricCategory, WindowStrategy]] = None
    ):
        """
        Args:
            custom_strategies: 自定义策略配置（覆盖默认）
        """
        self.strategies = DEFAULT_STRATEGIES.copy()
        if custom_strategies:
            self.strategies.update(custom_strategies)

        self.analyzer = MultiHorizonAnalyzer()

    def analyze(
        self,
        values_10y: List[float],
        metric_name: str,
        industry: str = "",
        category_override: Optional[MetricCategory] = None
    ) -> ProfessionalAnalysisResult:
        """
        执行专业分析

        Args:
            values_10y: 10年数据（或更短，按时间顺序）
            metric_name: 指标名称
            industry: 行业（用于周期分析）
            category_override: 指标类别覆盖

        Returns:
            ProfessionalAnalysisResult: 专业分析结果
        """
        # 1. 确定指标类别和策略
        category = category_override or classify_metric(metric_name)
        strategy = self.strategies[category]

        # 2. 执行多时间窗口分析
        multi_horizon = self.analyzer.analyze(values_10y, metric_name)

        # 3. 提取核心输出
        effective_slope = multi_horizon.effective_slope
        effective_cagr = multi_horizon.effective_cagr

        # CV使用近期窗口
        effective_cv = multi_horizon.recent_analysis.cv
        effective_latest = multi_horizon.recent_analysis.latest_value

        # 4. 处理断点
        has_break = multi_horizon.structural_break.has_break
        break_year = None
        if has_break and multi_horizon.structural_break.break_year_index is not None:
            break_year = multi_horizon.structural_break.break_year_index + 1

        # 5. 周期性分析（如果启用且有足够数据）
        cyclical_confidence = 0.0
        cycle_position = "unknown"

        if strategy.use_cyclical_analysis and len(values_10y) >= 7:
            # 这里调用周期探针（如果需要可以集成）
            # 暂时使用简化版本
            cyclical_confidence, cycle_position = self._quick_cyclical_check(
                values_10y, industry
            )

        # 6. 数据质量评估
        data_quality_grade = multi_horizon.recent_analysis.reliability_grade
        analysis_confidence = self._compute_analysis_confidence(
            multi_horizon, has_break, cyclical_confidence
        )

        # 7. 生成建议
        recommendation, warnings = self._generate_recommendation(
            metric_name, category, multi_horizon,
            has_break, cyclical_confidence, cycle_position
        )

        return ProfessionalAnalysisResult(
            metric_name=metric_name,
            category=category,
            strategy=strategy,
            effective_slope=effective_slope,
            effective_cagr=effective_cagr,
            effective_cv=effective_cv,
            effective_latest=effective_latest,
            multi_horizon=multi_horizon,
            cyclical_confidence=cyclical_confidence,
            cycle_position=cycle_position,
            has_break=has_break,
            break_year=break_year,
            data_quality_grade=data_quality_grade,
            analysis_confidence=analysis_confidence,
            recommendation=recommendation,
            warnings=warnings
        )

    def _quick_cyclical_check(
        self, values: List[float], industry: str
    ) -> Tuple[float, str]:
        """
        快速周期性检查（简化版）

        完整版应调用cyclical_probe
        """
        arr = np.array(values)
        n = len(arr)

        if n < 5:
            return 0.0, "unknown"

        # 计算CV
        mean_val = np.mean(arr)
        cv = np.std(arr) / abs(mean_val) if abs(mean_val) > 0.01 else 0

        # 检查是否有明显峰谷
        max_idx = np.argmax(arr)
        min_idx = np.argmin(arr)

        # 峰谷比
        if arr[min_idx] > 0:
            peak_trough_ratio = arr[max_idx] / arr[min_idx]
        else:
            peak_trough_ratio = 1.0

        # 简单周期性判断
        if cv > 0.25 and peak_trough_ratio > 1.5:
            # 可能是周期性
            cyclical_conf = min(0.6, cv + (peak_trough_ratio - 1) * 0.2)

            # 确定当前位置
            latest = arr[-1]
            mean = np.mean(arr)
            std = np.std(arr)

            if latest > mean + 0.5 * std:
                position = "top"
            elif latest < mean - 0.5 * std:
                position = "bottom"
            elif arr[-1] > arr[-2]:
                position = "mid_up"
            else:
                position = "mid_down"

            return cyclical_conf, position

        return 0.2, "unknown"

    def _compute_analysis_confidence(
        self,
        multi_horizon: MultiHorizonResult,
        has_break: bool,
        cyclical_confidence: float
    ) -> float:
        """计算分析置信度"""
        base_conf = multi_horizon.recent_analysis.confidence_ceiling

        # 有断点会降低置信度
        if has_break:
            base_conf *= 0.85

        # 周期性强会增加不确定性
        if cyclical_confidence > 0.5:
            base_conf *= 0.90

        return base_conf

    def _generate_recommendation(
        self,
        metric_name: str,
        category: MetricCategory,
        multi_horizon: MultiHorizonResult,
        has_break: bool,
        cyclical_confidence: float,
        cycle_position: str
    ) -> Tuple[str, List[str]]:
        """生成建议"""
        parts = []
        warnings = list(multi_horizon.warnings)

        # 基本趋势
        slope = multi_horizon.effective_slope
        if slope > 0.05:
            parts.append(f"📈 {metric_name}趋势向好")
        elif slope < -0.05:
            parts.append(f"📉 {metric_name}趋势下滑")
        else:
            parts.append(f"➖ {metric_name}走势平稳")

        # 断点信息
        if has_break:
            parts.append(f"⚠️ 存在结构断点，已使用断点后数据")
            warnings.append(f"{metric_name}在历史上发生过显著变化")

        # 周期信息
        if cyclical_confidence > 0.5:
            pos_zh = {
                "top": "高位",
                "bottom": "低位",
                "mid_up": "上升中",
                "mid_down": "下降中",
                "unknown": "不确定"
            }
            parts.append(f"🔄 周期位置: {pos_zh.get(cycle_position, '?')}")

            if cycle_position == "top":
                warnings.append(f"{metric_name}可能处于周期高点，注意均值回归")
            elif cycle_position == "bottom":
                parts.append("💡 可能处于周期低点，关注反转机会")

        recommendation = " | ".join(parts)
        return recommendation, warnings


# =============================================================================
# 便捷函数
# =============================================================================

def analyze_with_professional_strategy(
    values: List[float],
    metric_name: str,
    industry: str = ""
) -> ProfessionalAnalysisResult:
    """
    使用专业策略分析

    Args:
        values: 时间序列数据
        metric_name: 指标名称
        industry: 行业

    Returns:
        ProfessionalAnalysisResult: 分析结果

    Example:
        >>> roe_data = [12, 13, 14, 15, 16, 18, 17, 16, 15, 14]
        >>> result = analyze_with_professional_strategy(roe_data, "roe", "银行")
        >>> print(result.effective_slope)
        >>> print(result.recommendation)
    """
    strategy = ProfessionalDataWindowStrategy()
    return strategy.analyze(values, metric_name, industry)


# =============================================================================
# 测试
# =============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("专业数据窗口策略测试")
    print("=" * 70)

    strategy = ProfessionalDataWindowStrategy()

    # 测试1: ROE数据（效率类）
    print("\n1. ROE数据（效率类）:")
    roe_data = [12, 13, 14, 15, 16, 18, 17, 16, 15, 14]
    result = strategy.analyze(roe_data, "roe", "银行")
    print(f"   类别: {result.category.value}")
    print(f"   有效斜率: {result.effective_slope:.4f}")
    print(f"   有效CAGR: {result.effective_cagr:.2%}")
    print(f"   周期置信: {result.cyclical_confidence:.1%}")
    print(f"   周期位置: {result.cycle_position}")
    print(f"   分析置信: {result.analysis_confidence:.1%}")
    print(f"   建议: {result.recommendation}")

    # 测试2: 营收数据（增长类）
    print("\n2. 营收数据（增长类，有断点）:")
    revenue_data = [100, 105, 110, 108, 105, 200, 220, 250, 280, 300]
    result = strategy.analyze(revenue_data, "revenue", "科技")
    print(f"   类别: {result.category.value}")
    print(f"   有断点: {result.has_break}")
    if result.has_break:
        print(f"   断点年份: 第{result.break_year}年后")
    print(f"   有效斜率: {result.effective_slope:.4f}")
    print(f"   建议: {result.recommendation}")
    print(f"   警告: {result.warnings}")

    # 测试3: 资产负债率（杠杆类）
    print("\n3. 资产负债率（杠杆类）:")
    debt_data = [45, 48, 50, 52, 55, 58, 60, 63, 65, 68]
    result = strategy.analyze(debt_data, "debt_ratio", "房地产")
    print(f"   类别: {result.category.value}")
    print(f"   有效斜率: {result.effective_slope:.4f}")
    print(f"   主窗口权重: {result.strategy.primary_weight:.0%}")
    print(f"   建议: {result.recommendation}")

    # 测试4: 只有5年数据
    print("\n4. 只有5年数据:")
    short_data = [15, 16, 17, 18, 19]
    result = strategy.analyze(short_data, "roic", "消费")
    print(f"   数据年数: {len(short_data)}")
    print(f"   质量等级: {result.data_quality_grade}")
    print(f"   有效斜率: {result.effective_slope:.4f}")
    print(f"   建议: {result.recommendation}")

    print("\n✓ 测试完成")
