"""
数据加载器
=========

职责:
- 统一处理文件/DataFrame加载
- 自动检测格式
- 返回标准化的数据源引用
"""

import logging
import pandas as pd
from pathlib import Path
from typing import Union, Tuple, Any

logger = logging.getLogger(__name__)


class DataLoader:
    """数据加载器

    统一处理多种数据源类型:
    - pandas.DataFrame: 注册为临时表
    - polars.DataFrame: 转换为pandas后注册
    - 文件路径(str/Path): 生成read_parquet/read_csv_auto SQL
    """

    @staticmethod
    def normalize_path(path: Union[str, Path]) -> str:
        """标准化文件路径

        Args:
            path: 文件路径

        Returns:
            标准化的路径字符串(正斜杠,转义单引号)
        """
        return str(Path(path)).replace('\\', '/').replace("'", "''")

    @staticmethod
    def get_file_source_sql(path: Union[str, Path]) -> str:
        """根据文件类型生成DuckDB源表SQL

        Args:
            path: 文件路径

        Returns:
            DuckDB源表SQL片段(如: read_parquet('path'))

        Raises:
            FileNotFoundError: 文件不存在
            ValueError: 不支持的文件格式
        """
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"文件不存在: {p}")

        suffix = p.suffix.lower()
        norm_path = DataLoader.normalize_path(p)

        if suffix == '.parquet':
            return f"read_parquet('{norm_path}')"
        elif suffix in ('.csv', '.svc'):
            return f"read_csv_auto('{norm_path}')"
        else:
            raise ValueError(
                f"不支持的文件格式: {suffix}, 仅支持 .parquet, .csv, .svc"
            )

    @staticmethod
    def load_to_connector(
        data: Any,
        connector: Any,
        table_name: str = 'input_df'
    ) -> str:
        """加载数据到DuckDB连接器

        Args:
            data: 输入数据(DataFrame/文件路径)
            connector: DuckDBConnector实例
            table_name: 临时表名(仅DataFrame使用)

        Returns:
            源表SQL引用(表名或read_xxx()函数)

        Raises:
            ValueError: 不支持的数据类型
        """
        # 1. 处理polars DataFrame
        try:
            import polars as pl
            if isinstance(data, pl.DataFrame):
                logger.debug("检测到polars.DataFrame,转换为pandas")
                data = data.to_pandas()
        except Exception:
            pass

        # 2. 处理pandas DataFrame
        if isinstance(data, pd.DataFrame):
            connector.register_dataframe(table_name, data)
            return table_name

        # 3. 处理文件路径
        if isinstance(data, (str, Path)):
            return DataLoader.get_file_source_sql(data)

        # 4. 兜底:尝试to_pandas()
        if hasattr(data, 'to_pandas') and callable(getattr(data, 'to_pandas')):
            try:
                df = data.to_pandas()
                connector.register_dataframe(table_name, df)
                logger.debug(f"通过to_pandas()转换并注册: {table_name}")
                return table_name
            except Exception as e:
                logger.debug(f"to_pandas()失败: {e}")

        raise ValueError(
            f"不支持的数据类型: {type(data)}. "
            "支持类型: pandas.DataFrame, polars.DataFrame, 文件路径(str/Path)"
        )
