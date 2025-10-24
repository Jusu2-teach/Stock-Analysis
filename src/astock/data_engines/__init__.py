"""
数据引擎模块
提供数据清理功能
"""

# 导入引擎以触发注册
from .engines import pandas

# 简单的注册表，供Orchestrator使用
data_engine_registry = "data_engines"

__all__ = [
    'data_engine_registry'
]