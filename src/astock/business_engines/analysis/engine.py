"""
分析引擎 (Analysis Engine)
==========================

提供数据加载和分析功能的统一接口。

模块职责：
- data_loaders.py: 数据加载 (load_xxx)
- data_analyzers.py: 数据分析 (calculate_xxx, filter_xxx)

版本: 2.0.0
日期: 2026-01-17
"""

import logging

from .data_loaders import load_file, load_financial_data
from .data_analyzers import (
    calculate_industry_average,
    filter_outperform_industry,
)
from .financial_context import build_financial_context

logger = logging.getLogger(__name__)

__all__ = [
    # 数据加载
    "load_file",
    "load_financial_data",
    # 数据准备
    "build_financial_context",
    # 数据分析
    "calculate_industry_average",
    "filter_outperform_industry",
]
