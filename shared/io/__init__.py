"""
I/O 系统 (Input/Output System)
==============================

参考设计:
- kedro: DataCatalog 统一数据访问
- fsspec: 文件系统抽象
- pandas: 多格式读写
- polars: 高性能数据处理

提供统一的数据输入输出接口。

核心组件:
---------
- DataCatalog: 统一数据目录管理
- DataSet: 数据集抽象基类
- Readers: CSV, Parquet, JSON, SQL 读取器
- Writers: 多格式写入器
- PathManager: 路径解析与管理

基本用法:
---------
    # 读取数据
    from shared.io import read_csv, read_parquet

    df = read_csv("data/input.csv")
    df = read_parquet("data/input.parquet")

    # 写入数据
    from shared.io import write_csv, write_parquet

    write_csv(df, "data/output.csv")
    write_parquet(df, "data/output.parquet")

使用 DataCatalog:
-----------------
    from shared.io import DataCatalog

    catalog = DataCatalog()
    catalog.register("my_data", CSVDataSet("data/my_data.csv"))

    df = catalog.load("my_data")
    catalog.save("my_data", df)

路径管理:
---------
    from shared.io import PathManager

    paths = PathManager(base_dir="data")
    raw_path = paths.get("raw/input.csv")
    output_path = paths.output("result.parquet")
"""

from .core import (
    DataSet,
    DataCatalog,
    DataSetConfig,
)
from .readers import (
    read_csv,
    read_parquet,
    read_json,
    read_excel,
    read_sql,
    CSVReader,
    ParquetReader,
    JSONReader,
    SQLReader,
)
from .writers import (
    write_csv,
    write_parquet,
    write_json,
    write_excel,
    CSVWriter,
    ParquetWriter,
    JSONWriter,
)
from .datasets import (
    CSVDataSet,
    ParquetDataSet,
    JSONDataSet,
    SQLDataSet,
    PolarsDataSet,
    DuckDBDataSet,
)
from .paths import (
    PathManager,
    resolve_path,
    ensure_dir,
    get_data_dir,
)


__all__ = [
    # Core
    "DataSet",
    "DataCatalog",
    "DataSetConfig",

    # Readers
    "read_csv",
    "read_parquet",
    "read_json",
    "read_excel",
    "read_sql",
    "CSVReader",
    "ParquetReader",
    "JSONReader",
    "SQLReader",

    # Writers
    "write_csv",
    "write_parquet",
    "write_json",
    "write_excel",
    "CSVWriter",
    "ParquetWriter",
    "JSONWriter",

    # DataSets
    "CSVDataSet",
    "ParquetDataSet",
    "JSONDataSet",
    "SQLDataSet",
    "PolarsDataSet",
    "DuckDBDataSet",

    # Paths
    "PathManager",
    "resolve_path",
    "ensure_dir",
    "get_data_dir",
]
