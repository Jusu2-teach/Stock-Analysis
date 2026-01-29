"""
宏观经济模块 (Macroeconomic Module)
===================================

提供宏观经济环境分析能力，补充微观基本面分析：

核心组件:
- indicators.py: 宏观指标计算 (GDP, CPI, PMI, M2等)
- cycle_detector.py: 经济周期检测 (扩张/收缩/衰退/复苏)
- industry_cycle.py: 行业周期分析 (景气度、产能利用率)
- engine.py: Orchestrator注册方法

设计原则:
- 与基本面解耦：独立的宏观视角
- 数据驱动：基于统计模型，非主观判断
- 周期感知：识别宏观经济和行业周期阶段

版本: 1.0.0
日期: 2026-01-17
"""

from .indicators import MacroIndicators, MacroIndicatorResult
from .cycle_detector import EconomicCycleDetector, CyclePhase, CycleResult
from .engine import (
    calculate_macro_indicators,
    detect_economic_cycle,
)

__all__ = [
    # 宏观指标
    "MacroIndicators",
    "MacroIndicatorResult",

    # 周期检测
    "EconomicCycleDetector",
    "CyclePhase",
    "CycleResult",

    # 注册方法
    "calculate_macro_indicators",
    "detect_economic_cycle",
]
