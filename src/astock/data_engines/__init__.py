"""
数据引擎模块
提供数据清理功能
"""

from orchestrator import Registry
from .engines import pandas, polars

# Scan Pandas Engine
Registry.get().scan(
    module=pandas,
    component_type="data_engine",
    engine_type="pandas",
    tags=("pandas", "cleaning")
)

# Scan Polars Engine
Registry.get().scan(
    module=polars,
    component_type="data_engine",
    engine_type="polars",
    tags=("polars", "high-performance")
)

# 简单的注册表，供Orchestrator使用
data_engine_registry = "data_engines"

__all__ = [
    'data_engine_registry'
]