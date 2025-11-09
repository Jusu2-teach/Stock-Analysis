"""
数据访问层 (Data Access Layer)
============================

纯数据引擎功能,职责清晰分离:
- duckdb_connector: DuckDB连接管理
- query_builder: SQL查询构建
- data_loader: 文件加载

不包含任何业务逻辑
"""

from .duckdb_connector import DuckDBConnector, get_duckdb_connection
from .query_builder import QueryBuilder, quote_identifier
from .data_loader import DataLoader

__all__ = [
    'DuckDBConnector',
    'get_duckdb_connection',
    'QueryBuilder',
    'quote_identifier',
    'DataLoader',
]
