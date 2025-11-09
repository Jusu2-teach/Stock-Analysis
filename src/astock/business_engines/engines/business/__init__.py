"""
业务逻辑层 (Business Logic Layer)
================================

业务分析功能:
- base_analyzer: 业务分析器基类
- industry_analyzer: 行业分析(聚合、平均值)
- outperform_analyzer: 超越行业分析
- trend_analyzer_engine: 趋势分析引擎封装
"""

from .base_analyzer import BaseAnalyzer
from .industry_analyzer import (
    calc_industry_avg,
    IndustryAggregator,
)
from .outperform_analyzer import (
    filter_outperform_industry,
    OutperformFilter,
)

__all__ = [
    'BaseAnalyzer',
    'calc_industry_avg',
    'IndustryAggregator',
    'filter_outperform_industry',
    'OutperformFilter',
]
