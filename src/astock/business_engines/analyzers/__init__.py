"""
分析器模块 (Analyzers Module)
============================

集成各类财务分析器：
- trend: 趋势分析器 - FFT周期检测、拐点识别、恶化检测
- quality: 质量分析器 - ROIC质量评估、护城河分析 (预留)
- valuation: 估值分析器 - DCF、相对估值 (预留)

作者: AStock Analysis System
日期: 2025-12-06
"""

from .trend import (
    TrendAnalyzer,
    TrendRuleEngine,
    trend_rule_engine,
    MetricAdapter,
    MetricProfile,
    METRIC_PROFILES,
)

__all__ = [
    # 趋势分析
    "TrendAnalyzer",
    "TrendRuleEngine",
    "trend_rule_engine",
    "MetricAdapter",
    "MetricProfile",
    "METRIC_PROFILES",
]
