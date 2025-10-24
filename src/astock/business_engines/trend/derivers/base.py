"""
指标派生器基础模块
==================

定义派生器插件的接口规范，所有具体派生器必须遵循此协议。
"""

from typing import Protocol, Set, runtime_checkable


@runtime_checkable
class MetricDeriver(Protocol):
    """
    指标派生器插件接口

    每个派生器负责将基础指标转换为派生指标。
    例如：ROIIC = ΔNOPAT / Δ投入资本

    实现此接口的类将被自动注册到派生器系统中。
    """

    @property
    def metric_name(self) -> str:
        """
        返回派生指标名称（小写）

        Returns:
            派生指标名，如 'roiic', 'roa', 'fcfroic'
        """
        ...

    @property
    def required_columns(self) -> Set[str]:
        """
        返回派生所需的源数据列

        Returns:
            列名集合，如 {'roic', 'invest_capital', 'end_date'}
        """
        ...

    @property
    def description(self) -> str:
        """
        派生器描述（可选，用于文档生成）

        Returns:
            描述字符串
        """
        return f"{self.metric_name.upper()} 派生器"

    def can_derive(self, metric_name: str, available_cols: Set[str]) -> bool:
        """
        判断是否能派生指定指标

        Args:
            metric_name: 请求的指标名
            available_cols: 数据中可用的列名集合

        Returns:
            True 如果可以派生，False 否则
        """
        if metric_name.lower() != self.metric_name.lower():
            return False
        return self.required_columns.issubset(available_cols)

    def derive(
        self,
        con,              # DuckDB 连接对象
        source_sql: str,  # 源数据 SQL 视图名或查询
        group_column: str # 分组列名（如 ts_code）
    ) -> str:
        """
        执行派生逻辑，创建包含派生指标的新视图

        Args:
            con: DuckDB 连接对象
            source_sql: 源数据的 SQL 表达式
            group_column: 分组列名

        Returns:
            新创建的视图名称

        Raises:
            ValueError: 如果源数据不满足要求
        """
        ...
