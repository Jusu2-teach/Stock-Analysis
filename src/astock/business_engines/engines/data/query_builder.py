"""
SQL 查询构建器
=============

职责:
- 构建标准化SQL查询
- 提供fluent API
- 处理标识符引用和参数化
"""

import logging
from typing import List, Optional, Dict, Any, Union
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


def quote_identifier(name: str) -> str:
    """DuckDB标识符引用(双引号包裹,内部双引号转义)

    Args:
        name: 标识符名称

    Returns:
        引用后的标识符

    Examples:
        >>> quote_identifier('column_name')
        '"column_name"'
        >>> quote_identifier('col"with"quotes')
        '"col""with""quotes"'
    """
    if name is None:
        return '""'
    return '"' + str(name).replace('"', '""') + '"'


@dataclass
class QueryBuilder:
    """SQL查询构建器(Fluent API)

    支持链式调用构建复杂查询:

    Examples:
        >>> qb = QueryBuilder('my_table')
        >>> sql = qb.select(['col1', 'col2']) \\
        ...         .where('col1 > 100') \\
        ...         .order_by('col2') \\
        ...         .build()
    """

    source: str  # 数据源(表名/子查询/文件路径)
    _select_cols: List[str] = field(default_factory=list)
    _where_clauses: List[str] = field(default_factory=list)
    _group_by_cols: List[str] = field(default_factory=list)
    _having_clauses: List[str] = field(default_factory=list)
    _order_by_cols: List[str] = field(default_factory=list)
    _limit_value: Optional[int] = None
    _alias: Optional[str] = None

    def select(self, columns: Union[str, List[str]]) -> 'QueryBuilder':
        """添加SELECT列

        Args:
            columns: 列名或列名列表,支持表达式

        Returns:
            self(支持链式调用)
        """
        if isinstance(columns, str):
            self._select_cols.append(columns)
        else:
            self._select_cols.extend(columns)
        return self

    def where(self, condition: str) -> 'QueryBuilder':
        """添加WHERE条件

        Args:
            condition: WHERE条件表达式

        Returns:
            self
        """
        self._where_clauses.append(f"({condition})")
        return self

    def group_by(self, columns: Union[str, List[str]]) -> 'QueryBuilder':
        """添加GROUP BY列

        Args:
            columns: 列名或列名列表

        Returns:
            self
        """
        if isinstance(columns, str):
            self._group_by_cols.append(columns)
        else:
            self._group_by_cols.extend(columns)
        return self

    def having(self, condition: str) -> 'QueryBuilder':
        """添加HAVING条件

        Args:
            condition: HAVING条件表达式

        Returns:
            self
        """
        self._having_clauses.append(f"({condition})")
        return self

    def order_by(self, columns: Union[str, List[str]]) -> 'QueryBuilder':
        """添加ORDER BY列

        Args:
            columns: 列名或列名列表(可包含ASC/DESC)

        Returns:
            self
        """
        if isinstance(columns, str):
            self._order_by_cols.append(columns)
        else:
            self._order_by_cols.extend(columns)
        return self

    def limit(self, n: int) -> 'QueryBuilder':
        """设置LIMIT

        Args:
            n: 限制行数

        Returns:
            self
        """
        self._limit_value = n
        return self

    def alias(self, name: str) -> 'QueryBuilder':
        """设置表别名

        Args:
            name: 别名

        Returns:
            self
        """
        self._alias = name
        return self

    def build(self) -> str:
        """构建最终SQL查询

        Returns:
            SQL查询字符串
        """
        # SELECT子句
        if not self._select_cols:
            select_clause = "SELECT *"
        else:
            select_clause = f"SELECT {', '.join(self._select_cols)}"

        # FROM子句
        from_clause = f"FROM {self.source}"
        if self._alias:
            from_clause += f" AS {quote_identifier(self._alias)}"

        # WHERE子句
        where_clause = ""
        if self._where_clauses:
            where_clause = f"WHERE {' AND '.join(self._where_clauses)}"

        # GROUP BY子句
        group_clause = ""
        if self._group_by_cols:
            group_clause = f"GROUP BY {', '.join(self._group_by_cols)}"

        # HAVING子句
        having_clause = ""
        if self._having_clauses:
            having_clause = f"HAVING {' AND '.join(self._having_clauses)}"

        # ORDER BY子句
        order_clause = ""
        if self._order_by_cols:
            order_clause = f"ORDER BY {', '.join(self._order_by_cols)}"

        # LIMIT子句
        limit_clause = ""
        if self._limit_value is not None:
            limit_clause = f"LIMIT {self._limit_value}"

        # 组装完整SQL
        parts = [
            select_clause,
            from_clause,
            where_clause,
            group_clause,
            having_clause,
            order_clause,
            limit_clause,
        ]

        sql = '\n'.join(part for part in parts if part)
        return sql

    @staticmethod
    def aggregate_query(
        source: str,
        group_cols: Union[str, List[str]],
        aggregations: Dict[str, str],
        where: Optional[str] = None,
        order_by: Optional[Union[str, List[str]]] = None,
    ) -> str:
        """快速构建聚合查询(静态方法)

        Args:
            source: 数据源
            group_cols: 分组列
            aggregations: 聚合定义 {输出列名: 聚合表达式}
            where: WHERE条件(可选)
            order_by: 排序列(可选)

        Returns:
            SQL查询字符串

        Examples:
            >>> sql = QueryBuilder.aggregate_query(
            ...     source='sales',
            ...     group_cols='region',
            ...     aggregations={
            ...         'total_revenue': 'SUM(revenue)',
            ...         'avg_price': 'AVG(price)'
            ...     }
            ... )
        """
        # 标准化分组列
        if isinstance(group_cols, str):
            group_cols = [group_cols]

        # 构建SELECT列
        select_parts = [quote_identifier(col) for col in group_cols]
        select_parts.extend([
            f"{expr} AS {quote_identifier(name)}"
            for name, expr in aggregations.items()
        ])

        # 构建查询
        qb = QueryBuilder(source)
        qb.select(select_parts).group_by(group_cols)

        if where:
            qb.where(where)

        if order_by:
            qb.order_by(order_by)

        return qb.build()

    @staticmethod
    def join_query(
        left_source: str,
        right_source: str,
        on_condition: str,
        join_type: str = 'INNER',
        select_cols: Optional[List[str]] = None,
        where: Optional[str] = None,
        left_alias: str = 'l',
        right_alias: str = 'r',
    ) -> str:
        """快速构建JOIN查询(静态方法)

        Args:
            left_source: 左表源
            right_source: 右表源
            on_condition: JOIN条件
            join_type: JOIN类型(INNER/LEFT/RIGHT/FULL)
            select_cols: SELECT列列表(默认l.*, r.*)
            where: WHERE条件(可选)
            left_alias: 左表别名
            right_alias: 右表别名

        Returns:
            SQL查询字符串
        """
        # 默认选择所有列
        if select_cols is None:
            select_cols = [f"{left_alias}.*", f"{right_alias}.*"]

        # 构建SQL
        sql_parts = [
            f"SELECT {', '.join(select_cols)}",
            f"FROM {left_source} AS {quote_identifier(left_alias)}",
            f"{join_type.upper()} JOIN {right_source} AS {quote_identifier(right_alias)}",
            f"  ON {on_condition}",
        ]

        if where:
            sql_parts.append(f"WHERE {where}")

        return '\n'.join(sql_parts)
