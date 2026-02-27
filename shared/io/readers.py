"""
数据读取器 (Data Readers)
=========================

参考设计:
- pandas: 多格式读取
- polars: 高性能读取
- fsspec: 文件系统抽象

提供多种格式的数据读取器。
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl


class BaseReader(ABC):
    """读取器基类"""

    @abstractmethod
    def read(self, path: Union[str, Path], **kwargs) -> Any:
        """读取数据"""

    @classmethod
    def supports(cls, path: Union[str, Path]) -> bool:
        """检查是否支持该文件类型"""
        return False


class CSVReader(BaseReader):
    """CSV 读取器

    支持 pandas 和 polars 两种后端。

    Example:
        reader = CSVReader()
        df = reader.read("data.csv")

        # 使用 polars
        df = reader.read("data.csv", engine="polars")
    """

    def __init__(
        self,
        engine: str = "pandas",
        default_options: Optional[Dict[str, Any]] = None,
    ):
        self.engine = engine
        self.default_options = default_options or {}

    def read(
        self,
        path: Union[str, Path],
        engine: Optional[str] = None,
        **kwargs,
    ) -> Union["pd.DataFrame", "pl.DataFrame"]:
        """读取 CSV 文件

        Args:
            path: 文件路径
            engine: 引擎 ("pandas" 或 "polars")
            **kwargs: 传递给读取函数的参数
        """
        engine = engine or self.engine
        options = {**self.default_options, **kwargs}

        if engine == "polars":
            import polars as pl
            return pl.read_csv(path, **options)
        else:
            import pandas as pd
            return pd.read_csv(path, **options)

    @classmethod
    def supports(cls, path: Union[str, Path]) -> bool:
        return str(path).lower().endswith('.csv')


class ParquetReader(BaseReader):
    """Parquet 读取器

    支持 pandas, polars 和 pyarrow 后端。

    Example:
        reader = ParquetReader()
        df = reader.read("data.parquet")
    """

    def __init__(
        self,
        engine: str = "pandas",
        default_options: Optional[Dict[str, Any]] = None,
    ):
        self.engine = engine
        self.default_options = default_options or {}

    def read(
        self,
        path: Union[str, Path],
        engine: Optional[str] = None,
        columns: Optional[List[str]] = None,
        **kwargs,
    ) -> Union["pd.DataFrame", "pl.DataFrame"]:
        """读取 Parquet 文件

        Args:
            path: 文件路径
            engine: 引擎
            columns: 要读取的列
            **kwargs: 其他参数
        """
        engine = engine or self.engine
        options = {**self.default_options, **kwargs}

        if columns:
            options["columns"] = columns

        if engine == "polars":
            import polars as pl
            return pl.read_parquet(path, **options)
        else:
            import pandas as pd
            return pd.read_parquet(path, **options)

    @classmethod
    def supports(cls, path: Union[str, Path]) -> bool:
        return str(path).lower().endswith('.parquet')


class JSONReader(BaseReader):
    """JSON 读取器

    支持 JSON 和 JSON Lines 格式。

    Example:
        reader = JSONReader()
        df = reader.read("data.json")

        # JSON Lines
        df = reader.read("data.jsonl", lines=True)
    """

    def __init__(
        self,
        engine: str = "pandas",
        default_options: Optional[Dict[str, Any]] = None,
    ):
        self.engine = engine
        self.default_options = default_options or {}

    def read(
        self,
        path: Union[str, Path],
        engine: Optional[str] = None,
        lines: Optional[bool] = None,
        **kwargs,
    ) -> Union["pd.DataFrame", "pl.DataFrame", Dict, List]:
        """读取 JSON 文件

        Args:
            path: 文件路径
            engine: 引擎
            lines: 是否为 JSON Lines 格式
            **kwargs: 其他参数
        """
        engine = engine or self.engine
        options = {**self.default_options, **kwargs}

        # 自动检测 JSON Lines
        if lines is None:
            lines = str(path).lower().endswith('.jsonl')

        if lines:
            options["lines"] = True

        if engine == "polars":
            import polars as pl
            if lines:
                return pl.read_ndjson(path)
            return pl.read_json(path)
        elif engine == "raw":
            import json
            with open(path, 'r', encoding='utf-8') as f:
                if lines:
                    return [json.loads(line) for line in f]
                return json.load(f)
        else:
            import pandas as pd
            return pd.read_json(path, **options)

    @classmethod
    def supports(cls, path: Union[str, Path]) -> bool:
        path_str = str(path).lower()
        return path_str.endswith('.json') or path_str.endswith('.jsonl')


class ExcelReader(BaseReader):
    """Excel 读取器

    Example:
        reader = ExcelReader()
        df = reader.read("data.xlsx", sheet_name="Sheet1")
    """

    def __init__(self, default_options: Optional[Dict[str, Any]] = None):
        self.default_options = default_options or {}

    def read(
        self,
        path: Union[str, Path],
        sheet_name: Union[str, int] = 0,
        **kwargs,
    ) -> "pd.DataFrame":
        """读取 Excel 文件

        Args:
            path: 文件路径
            sheet_name: 工作表名称或索引
            **kwargs: 其他参数
        """
        import pandas as pd

        options = {**self.default_options, **kwargs}
        return pd.read_excel(path, sheet_name=sheet_name, **options)

    @classmethod
    def supports(cls, path: Union[str, Path]) -> bool:
        path_str = str(path).lower()
        return path_str.endswith('.xlsx') or path_str.endswith('.xls')


class SQLReader(BaseReader):
    """SQL 读取器

    支持 SQLite, DuckDB 和其他数据库。

    Example:
        reader = SQLReader(connection_string="sqlite:///data.db")
        df = reader.read("SELECT * FROM users")

        # DuckDB
        reader = SQLReader(connection_string="data.duckdb", engine="duckdb")
        df = reader.read("SELECT * FROM stocks")
    """

    def __init__(
        self,
        connection_string: Optional[str] = None,
        engine: str = "sqlalchemy",
    ):
        self.connection_string = connection_string
        self.engine = engine

    def read(
        self,
        query: str,
        connection: Optional[Any] = None,
        **kwargs,
    ) -> "pd.DataFrame":
        """执行 SQL 查询

        Args:
            query: SQL 查询语句
            connection: 数据库连接（可选）
            **kwargs: 其他参数
        """
        if self.engine == "duckdb":
            import duckdb

            conn = connection or duckdb.connect(self.connection_string or ":memory:")
            return conn.execute(query).fetchdf()
        else:
            import pandas as pd

            conn = connection or self.connection_string
            return pd.read_sql(query, conn, **kwargs)

    @classmethod
    def supports(cls, path: Union[str, Path]) -> bool:
        path_str = str(path).lower()
        return path_str.endswith('.db') or path_str.endswith('.duckdb')


# 便捷函数
def read_csv(
    path: Union[str, Path],
    engine: str = "pandas",
    **kwargs,
) -> Union["pd.DataFrame", "pl.DataFrame"]:
    """读取 CSV 文件

    Args:
        path: 文件路径
        engine: 引擎 ("pandas" 或 "polars")
        **kwargs: 其他参数

    Returns:
        DataFrame
    """
    return CSVReader(engine=engine).read(path, **kwargs)


def read_parquet(
    path: Union[str, Path],
    engine: str = "pandas",
    **kwargs,
) -> Union["pd.DataFrame", "pl.DataFrame"]:
    """读取 Parquet 文件

    Args:
        path: 文件路径
        engine: 引擎
        **kwargs: 其他参数

    Returns:
        DataFrame
    """
    return ParquetReader(engine=engine).read(path, **kwargs)


def read_json(
    path: Union[str, Path],
    engine: str = "pandas",
    **kwargs,
) -> Union["pd.DataFrame", "pl.DataFrame", Dict, List]:
    """读取 JSON 文件

    Args:
        path: 文件路径
        engine: 引擎
        **kwargs: 其他参数

    Returns:
        DataFrame 或原始数据
    """
    return JSONReader(engine=engine).read(path, **kwargs)


def read_excel(
    path: Union[str, Path],
    sheet_name: Union[str, int] = 0,
    **kwargs,
) -> "pd.DataFrame":
    """读取 Excel 文件

    Args:
        path: 文件路径
        sheet_name: 工作表
        **kwargs: 其他参数

    Returns:
        DataFrame
    """
    return ExcelReader().read(path, sheet_name=sheet_name, **kwargs)


def read_sql(
    query: str,
    connection: Any,
    engine: str = "sqlalchemy",
    **kwargs,
) -> "pd.DataFrame":
    """执行 SQL 查询

    Args:
        query: SQL 查询
        connection: 数据库连接
        engine: 引擎
        **kwargs: 其他参数

    Returns:
        DataFrame
    """
    return SQLReader(engine=engine).read(query, connection=connection, **kwargs)
