"""
业务引擎模块（重构版）
====================

核心模块：
- duckdb_core: DuckDB核心方法（合并 duckdb.py + duckdb_utils.py）
- duckdb_trend: 趋势分析方法
- scoring: 质量评分方法
"""

# 导入模块以触发 @register_method 装饰器
from . import duckdb_core  # noqa: F401
from . import duckdb_trend  # noqa: F401
from . import scoring  # noqa: F401