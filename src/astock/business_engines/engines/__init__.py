"""
业务引擎模块
"""

# 自动导入所有 DuckDB 相关引擎模块, 以确保 register_method 装饰器生效
from .duckdb import *  # noqa: F401,F403
from .duckdb_trend import *  # noqa: F401,F403
from .scoring import *  # noqa: F401,F403