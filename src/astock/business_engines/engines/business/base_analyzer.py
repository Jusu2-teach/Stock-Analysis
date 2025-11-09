"""
业务分析器基类
=============

提供所有业务分析器的通用功能:
- 数据源管理
- 列验证
- 日志记录
"""

import logging
import pandas as pd
from typing import Any, List, Union, Optional
from pathlib import Path

from ..data import DuckDBConnector, DataLoader, quote_identifier

logger = logging.getLogger(__name__)


class BaseAnalyzer:
    """业务分析器基类

    所有业务分析器应继承此类,复用通用功能
    """

    def __init__(
        self,
        data: Union[str, Path, pd.DataFrame],
        connector: Optional[DuckDBConnector] = None
    ):
        """初始化分析器

        Args:
            data: 输入数据
            connector: DuckDB连接器(可选,默认创建新连接)
        """
        self.connector = connector or DuckDBConnector()
        self.source_sql = DataLoader.load_to_connector(
            data, self.connector
        )
        self.logger = logger

        # 缓存列信息
        self._columns_cache: Optional[List[str]] = None

    @property
    def columns(self) -> List[str]:
        """获取数据源的列名列表(缓存)"""
        if self._columns_cache is None:
            cols_query = f"DESCRIBE SELECT * FROM {self.source_sql}"
            cols_df = self.connector.execute(cols_query).df()
            self._columns_cache = cols_df['column_name'].tolist()
        return self._columns_cache

    def validate_columns(
        self,
        required_cols: List[str],
        raise_error: bool = True
    ) -> List[str]:
        """验证必需列是否存在

        Args:
            required_cols: 必需的列名列表
            raise_error: 是否抛出异常(False时返回缺失列)

        Returns:
            缺失的列名列表

        Raises:
            ValueError: 存在缺失列且raise_error=True
        """
        missing = [col for col in required_cols if col not in self.columns]

        if missing and raise_error:
            raise ValueError(
                f"数据缺少必需列: {', '.join(missing)}\n"
                f"可用列: {', '.join(self.columns)}"
            )

        return missing

    def filter_valid_columns(
        self,
        candidate_cols: List[str]
    ) -> List[str]:
        """过滤出存在的列

        Args:
            candidate_cols: 候选列列表

        Returns:
            存在的列列表
        """
        return [col for col in candidate_cols if col in self.columns]

    def execute(self, sql: str) -> pd.DataFrame:
        """执行SQL并返回DataFrame

        Args:
            sql: SQL查询

        Returns:
            查询结果DataFrame
        """
        self.logger.debug(f"执行SQL:\n{sql}")
        return self.connector.execute(sql).df()

    def get_row_count(self) -> int:
        """获取数据源的行数

        Returns:
            总行数
        """
        sql = f"SELECT COUNT(*) AS cnt FROM {self.source_sql}"
        result = self.connector.execute(sql).df()
        return int(result['cnt'].iloc[0])

    def quote(self, identifier: str) -> str:
        """引用标识符(便捷方法)

        Args:
            identifier: 标识符名称

        Returns:
            引用后的标识符
        """
        return quote_identifier(identifier)

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        if self.connector:
            self.connector.close()
