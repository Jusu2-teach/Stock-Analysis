"""
宏观经济引擎 - Orchestrator注册
================================

将宏观经济模块注册到Orchestrator系统。

提供的方法:
- calculate_macro_indicators: 计算宏观指标
- detect_economic_cycle: 检测经济周期

版本: 1.0.0
日期: 2026-01-17
"""

import logging
from typing import Any, Dict, Optional

import pandas as pd

from orchestrator.decorators.register import register_method

from .indicators import MacroIndicators
from .cycle_detector import EconomicCycleDetector

logger = logging.getLogger(__name__)


@register_method(
    component_type="business_engine",
    engine_type="macro",
    engine_name="calculate_macro_indicators",
    description="计算宏观经济指标"
)
def calculate_macro_indicators(
    data: pd.DataFrame,
    reference_date: Optional[str] = None,
    **kwargs
) -> pd.DataFrame:
    """计算宏观经济指标

    Args:
        data: DataFrame包含宏观数据
        reference_date: 参考日期（默认使用最新）

    Returns:
        DataFrame包含宏观指标结果
    """
    logger.info(f"计算宏观指标: reference_date={reference_date}")

    calculator = MacroIndicators()
    result = calculator.calculate(data, reference_date=reference_date)

    # 转换为DataFrame
    df = pd.DataFrame([result.to_dict()])

    logger.info(
        f"宏观指标计算完成: momentum={result.economic_momentum}, "
        f"policy={result.policy_stance}"
    )

    return df


@register_method(
    component_type="business_engine",
    engine_type="macro",
    engine_name="detect_economic_cycle",
    description="检测经济周期阶段"
)
def detect_economic_cycle(
    macro_data: pd.DataFrame,
    reference_date: Optional[str] = None,
    lookback_months: int = 12,
    **kwargs
) -> pd.DataFrame:
    """检测经济周期

    Args:
        macro_data: DataFrame包含宏观数据
        reference_date: 参考日期（默认使用最新）
        lookback_months: 回溯月数

    Returns:
        DataFrame包含周期检测结果
    """
    logger.info(f"检测经济周期: reference_date={reference_date}")

    detector = EconomicCycleDetector()
    result = detector.detect(
        macro_data,
        reference_date=reference_date,
        lookback_months=lookback_months,
    )

    # 转换为DataFrame
    df = pd.DataFrame([result.to_dict()])

    logger.info(
        f"经济周期检测完成: phase={result.current_phase.display_name}, "
        f"confidence={result.confidence:.1%}"
    )

    return df
