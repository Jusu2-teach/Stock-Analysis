"""
分析器模块 (Analyzers Module)
============================

集成财务分析器：
- trend: 趋势分析器 - FFT周期检测、拐点识别、恶化检测

架构说明：
- analyzers/ 是纯数学层，只做趋势计算
- 业务配置（行业阈值、周期性判断）在 evaluators/threshold/ 模块

作者: AStock Analysis System
日期: 2025-12-06
"""

from .trend import (
    TrendAnalyzer,
)

# 业务配置应从 evaluators.threshold 模块导入
# from ..evaluators.threshold import get_industry_category, ...

__all__ = [
    # 趋势分析（纯数学层）
    "TrendAnalyzer",
]
