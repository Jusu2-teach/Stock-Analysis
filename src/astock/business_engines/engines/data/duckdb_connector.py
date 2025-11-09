"""
DuckDB 连接管理器
================

职责:
- 管理DuckDB连接生命周期
- 提供连接单例
- 处理连接池(如需要)
"""

import logging
from typing import Optional, Any
from pathlib import Path

logger = logging.getLogger(__name__)


class DuckDBConnector:
    """DuckDB 连接管理器

    提供简洁的连接接口,支持内存和磁盘数据库
    """

    _instance: Optional['DuckDBConnector'] = None
    _duckdb_module = None

    def __init__(self, database: str = ':memory:'):
        """初始化连接器

        Args:
            database: 数据库路径,默认':memory:'为内存数据库
        """
        self.database = database
        self._connection = None

        # 懒加载duckdb模块
        if DuckDBConnector._duckdb_module is None:
            try:
                import duckdb
                DuckDBConnector._duckdb_module = duckdb
            except ImportError:
                raise ImportError(
                    "DuckDB未安装,请运行: pip install duckdb"
                )

    @property
    def connection(self) -> Any:
        """获取DuckDB连接(懒加载)"""
        if self._connection is None:
            self._connection = self._duckdb_module.connect(
                database=self.database
            )
            logger.debug(f"创建DuckDB连接: {self.database}")
        return self._connection

    def close(self):
        """关闭连接"""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            logger.debug("DuckDB连接已关闭")

    def execute(self, query: str) -> Any:
        """执行SQL查询

        Args:
            query: SQL查询语句

        Returns:
            DuckDB查询结果对象
        """
        return self.connection.execute(query)

    def register_dataframe(self, name: str, df: Any):
        """注册DataFrame为临时表

        Args:
            name: 表名
            df: pandas/polars DataFrame
        """
        # 处理polars
        try:
            import polars as pl
            if isinstance(df, pl.DataFrame):
                df = df.to_pandas()
        except Exception:
            pass

        self.connection.register(name, df)
        logger.debug(f"注册DataFrame: {name} ({len(df)}行)")

    @classmethod
    def get_instance(cls, database: str = ':memory:') -> 'DuckDBConnector':
        """获取单例连接器

        Args:
            database: 数据库路径

        Returns:
            DuckDBConnector实例
        """
        if cls._instance is None or cls._instance.database != database:
            cls._instance = cls(database)
        return cls._instance

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close()


def get_duckdb_connection(database: str = ':memory:') -> DuckDBConnector:
    """便捷函数:获取DuckDB连接器

    Args:
        database: 数据库路径,默认':memory:'

    Returns:
        DuckDBConnector实例
    """
    return DuckDBConnector.get_instance(database)
