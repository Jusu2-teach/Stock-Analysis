"""
业务引擎模块
提供业务分析功能
"""

# 导入引擎以触发注册
from .engines import duckdb

# 简单的注册表，供Orchestrator使用
business_engine_registry = "business_engines"

__all__ = [
    'business_engine_registry'
]