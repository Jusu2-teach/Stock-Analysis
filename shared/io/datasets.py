"""
数据集实现 (DataSet Implementations)
====================================

参考设计:
- kedro: 数据集抽象
- pandas/polars: 数据处理

提供各种格式的数据集实现。
"""
from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, Optional, Union, TYPE_CHECKING

from .core import DataSet, DataSetConfig

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl


class CSVDataSet(DataSet):
    """CSV 数据集

    Example:
        dataset = CSVDataSet("data/input.csv")
        df = dataset.load()
        dataset.save(df)
    """

    def __init__(
        self,
        path: Union[str, Path],
        load_args: Optional[Dict[str, Any]] = None,
        save_args: Optional[Dict[str, Any]] = None,
        engine: str = "pandas",
    ):
        config = DataSetConfig(
            path=str(path),
            type="csv",
            file_format="csv",
            load_args=load_args or {},
            save_args=save_args or {"index": False},
        )
        super().__init__(path, config)
        self._engine = engine

    def _load(self) -> Union["pd.DataFrame", "pl.DataFrame"]:
        if self._engine == "polars":
            import polars as pl
            return pl.read_csv(self._path, **self._config.load_args)
        else:
            import pandas as pd
            return pd.read_csv(self._path, **self._config.load_args)

    def _save(self, data: Union["pd.DataFrame", "pl.DataFrame"]) -> None:
        if hasattr(data, 'write_csv'):  # polars
            data.write_csv(self._path)
        else:
            data.to_csv(self._path, **self._config.save_args)


class ParquetDataSet(DataSet):
    """Parquet 数据集

    Example:
        dataset = ParquetDataSet("data/input.parquet")
        df = dataset.load()
        dataset.save(df)
    """

    def __init__(
        self,
        path: Union[str, Path],
        load_args: Optional[Dict[str, Any]] = None,
        save_args: Optional[Dict[str, Any]] = None,
        engine: str = "pandas",
    ):
        config = DataSetConfig(
            path=str(path),
            type="parquet",
            file_format="parquet",
            load_args=load_args or {},
            save_args=save_args or {"compression": "snappy"},
        )
        super().__init__(path, config)
        self._engine = engine

    def _load(self) -> Union["pd.DataFrame", "pl.DataFrame"]:
        if self._engine == "polars":
            import polars as pl
            return pl.read_parquet(self._path, **self._config.load_args)
        else:
            import pandas as pd
            return pd.read_parquet(self._path, **self._config.load_args)

    def _save(self, data: Union["pd.DataFrame", "pl.DataFrame"]) -> None:
        if hasattr(data, 'write_parquet'):  # polars
            data.write_parquet(self._path, **self._config.save_args)
        else:
            data.to_parquet(self._path, **self._config.save_args)


class JSONDataSet(DataSet):
    """JSON 数据集

    Example:
        dataset = JSONDataSet("data/input.json")
        df = dataset.load()
        dataset.save(df)
    """

    def __init__(
        self,
        path: Union[str, Path],
        load_args: Optional[Dict[str, Any]] = None,
        save_args: Optional[Dict[str, Any]] = None,
        lines: bool = False,
    ):
        config = DataSetConfig(
            path=str(path),
            type="json",
            file_format="json",
            load_args=load_args or {},
            save_args=save_args or {"orient": "records"},
        )
        super().__init__(path, config)
        self._lines = lines or str(path).endswith('.jsonl')

    def _load(self) -> "pd.DataFrame":
        import pandas as pd

        if self._lines:
            return pd.read_json(self._path, lines=True, **self._config.load_args)
        return pd.read_json(self._path, **self._config.load_args)

    def _save(self, data: "pd.DataFrame") -> None:
        pass

        save_args = self._config.save_args.copy()
        if self._lines:
            save_args["lines"] = True
            save_args["orient"] = "records"

        data.to_json(self._path, **save_args)


class SQLDataSet(DataSet):
    """SQL 数据集

    Example:
        dataset = SQLDataSet(
            query="SELECT * FROM stocks",
            connection="sqlite:///data.db"
        )
        df = dataset.load()
    """

    def __init__(
        self,
        query: str,
        connection: Any,
        table_name: Optional[str] = None,
        load_args: Optional[Dict[str, Any]] = None,
        save_args: Optional[Dict[str, Any]] = None,
    ):
        config = DataSetConfig(
            type="sql",
            load_args=load_args or {},
            save_args=save_args or {"if_exists": "replace", "index": False},
        )
        super().__init__(config=config)
        self._query = query
        self._connection = connection
        self._table_name = table_name

    def _load(self) -> "pd.DataFrame":
        import pandas as pd
        return pd.read_sql(self._query, self._connection, **self._config.load_args)

    def _save(self, data: "pd.DataFrame") -> None:
        if not self._table_name:
            raise ValueError("table_name is required for saving")

        data.to_sql(
            self._table_name,
            self._connection,
            **self._config.save_args
        )

    def exists(self) -> bool:
        # SQL 查询总是可用的
        return True


class PolarsDataSet(DataSet):
    """Polars 专用数据集

    优化的 Polars DataFrame 处理。

    Example:
        dataset = PolarsDataSet("data/input.csv", file_format="csv")
        df = dataset.load()  # 返回 polars.DataFrame
    """

    def __init__(
        self,
        path: Union[str, Path],
        file_format: str = "csv",
        load_args: Optional[Dict[str, Any]] = None,
        save_args: Optional[Dict[str, Any]] = None,
    ):
        config = DataSetConfig(
            path=str(path),
            type="polars",
            file_format=file_format,
            load_args=load_args or {},
            save_args=save_args or {},
        )
        super().__init__(path, config)

    def _load(self) -> "pl.DataFrame":
        import polars as pl

        fmt = self._config.file_format
        args = self._config.load_args

        if fmt == "csv":
            return pl.read_csv(self._path, **args)
        elif fmt == "parquet":
            return pl.read_parquet(self._path, **args)
        elif fmt == "json":
            return pl.read_json(self._path)
        elif fmt == "ndjson":
            return pl.read_ndjson(self._path)
        else:
            raise ValueError(f"Unsupported format: {fmt}")

    def _save(self, data: "pl.DataFrame") -> None:
        fmt = self._config.file_format
        args = self._config.save_args

        if fmt == "csv":
            data.write_csv(self._path)
        elif fmt == "parquet":
            data.write_parquet(self._path, **args)
        elif fmt == "json":
            data.write_json(self._path)
        elif fmt == "ndjson":
            data.write_ndjson(self._path)
        else:
            raise ValueError(f"Unsupported format: {fmt}")


class DuckDBDataSet(DataSet):
    """DuckDB 数据集

    支持 DuckDB 数据库查询和存储。

    Example:
        dataset = DuckDBDataSet(
            path="data.duckdb",
            query="SELECT * FROM stocks"
        )
        df = dataset.load()
    """

    def __init__(
        self,
        path: Union[str, Path],
        query: Optional[str] = None,
        table_name: Optional[str] = None,
    ):
        config = DataSetConfig(
            path=str(path),
            type="duckdb",
        )
        super().__init__(path, config)
        self._query = query
        self._table_name = table_name

    def _load(self) -> "pd.DataFrame":
        import duckdb

        conn = duckdb.connect(str(self._path))
        try:
            if self._query:
                return conn.execute(self._query).fetchdf()
            elif self._table_name:
                return conn.execute(f"SELECT * FROM {self._table_name}").fetchdf()
            else:
                raise ValueError("Either query or table_name is required")
        finally:
            conn.close()

    def _save(self, data: "pd.DataFrame") -> None:
        import duckdb

        if not self._table_name:
            raise ValueError("table_name is required for saving")

        conn = duckdb.connect(str(self._path))
        try:
            conn.execute(
                f"CREATE OR REPLACE TABLE {self._table_name} AS SELECT * FROM data"
            )
        finally:
            conn.close()

    def execute(self, query: str) -> "pd.DataFrame":
        """执行任意查询"""
        import duckdb

        conn = duckdb.connect(str(self._path))
        try:
            return conn.execute(query).fetchdf()
        finally:
            conn.close()


# 工厂函数
def create_dataset(config: Dict[str, Any]) -> DataSet:
    """从配置创建数据集

    Args:
        config: 数据集配置
            - type: 数据集类型 (csv, parquet, json, sql, polars, duckdb)
            - path: 文件路径
            - 其他特定类型的配置

    Returns:
        DataSet 实例
    """
    dataset_type = config.get("type", "csv")
    path = config.get("path")

    if dataset_type == "csv":
        return CSVDataSet(
            path=path,
            load_args=config.get("load_args"),
            save_args=config.get("save_args"),
            engine=config.get("engine", "pandas"),
        )
    elif dataset_type == "parquet":
        return ParquetDataSet(
            path=path,
            load_args=config.get("load_args"),
            save_args=config.get("save_args"),
            engine=config.get("engine", "pandas"),
        )
    elif dataset_type == "json":
        return JSONDataSet(
            path=path,
            load_args=config.get("load_args"),
            save_args=config.get("save_args"),
            lines=config.get("lines", False),
        )
    elif dataset_type == "sql":
        return SQLDataSet(
            query=config["query"],
            connection=config["connection"],
            table_name=config.get("table_name"),
            load_args=config.get("load_args"),
            save_args=config.get("save_args"),
        )
    elif dataset_type == "polars":
        return PolarsDataSet(
            path=path,
            file_format=config.get("file_format", "csv"),
            load_args=config.get("load_args"),
            save_args=config.get("save_args"),
        )
    elif dataset_type == "duckdb":
        return DuckDBDataSet(
            path=path,
            query=config.get("query"),
            table_name=config.get("table_name"),
        )
    else:
        raise ValueError(f"Unknown dataset type: {dataset_type}")
