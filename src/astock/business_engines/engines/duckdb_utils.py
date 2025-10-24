"""
DuckDB 引擎通用工具函数
======================

提供 duckdb.py 和 duckdb_v2.py 共享的辅助函数，避免代码重复。
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Any, Tuple

logger = logging.getLogger(__name__)


def _q(name: str) -> str:
    """DuckDB 标识符引用（双引号包裹，内部双引号转义）

    Args:
        name: 标识符名称

    Returns:
        引用后的标识符字符串

    Examples:
        >>> _q('column_name')
        '"column_name"'
        >>> _q('col"with"quotes')
        '"col""with""quotes"'
    """
    if name is None:
        return '""'
    return '"' + str(name).replace('"', '""') + '"'


def _get_duckdb_module():
    """获取 duckdb 模块

    Returns:
        duckdb 模块对象，如果未安装则返回 None
    """
    try:
        import duckdb
        return duckdb
    except ImportError:
        logger.warning("duckdb模块未安装，请使用 pip install duckdb 安装")
        return None


def _init_duckdb_and_source(data: Any) -> Tuple[Any, str]:
    """初始化 DuckDB 连接并返回源表引用

    统一处理多种数据源类型：
    - pandas.DataFrame: 注册为临时表
    - polars.DataFrame: 转换为 pandas 后注册
    - 文件路径 (str/Path): 生成 read_parquet/read_csv_auto SQL
    - 其他实现 to_pandas() 的对象: 尝试转换

    Args:
        data: 输入数据，支持 DataFrame、文件路径或其他数据源

    Returns:
        (duckdb_connection, source_sql): DuckDB连接和源表引用SQL片段

    Raises:
        ImportError: duckdb 模块未安装
        FileNotFoundError: 文件路径不存在
        ValueError: 不支持的数据类型或文件格式

    Examples:
        >>> con, src = _init_duckdb_and_source(df)
        >>> result = con.execute(f"SELECT * FROM {src}").df()

        >>> con, src = _init_duckdb_and_source("data.csv")
        >>> result = con.execute(f"SELECT * FROM {src} LIMIT 10").df()
    """
    duckdb = _get_duckdb_module()
    if duckdb is None:
        raise ImportError("需要 duckdb 模块")

    con = duckdb.connect(database=':memory:')

    # 1) 先处理 DataFrame / polars，避免把 DataFrame 误当成路径字符串
    try:
        import polars as pl  # type: ignore
        if isinstance(data, pl.DataFrame):
            logger.debug("接收到 polars.DataFrame，转换为 pandas 注册")
            data = data.to_pandas()
    except Exception as e:
        logger.debug(f"polars 处理失败（忽略）: {e}")

    if isinstance(data, pd.DataFrame):
        con.register('input_df', data)
        return con, 'input_df'

    # 2) 再处理文件路径
    if isinstance(data, (str, Path)):
        p = Path(data)
        if not p.exists():
            raise FileNotFoundError(f"数据文件不存在: {p}")

        suf = p.suffix.lower()
        # 统一使用正斜杠，避免 Windows 反斜杠在 SQL 字符串里造成潜在解析/转义歧义
        norm_path = str(p).replace('\\', '/').replace("'", "''")

        if suf == '.parquet':
            source = f"read_parquet('{norm_path}')"
        elif suf in ('.csv', '.svc'):  # 支持 .svc 作为 csv
            source = f"read_csv_auto('{norm_path}')"
        else:
            raise ValueError(f"不支持的文件格式: {suf}，仅支持 .parquet, .csv, .svc")

        return con, source

    # 3) 兜底：遇到具有 to_pandas 方法的对象尝试转换
    if hasattr(data, 'to_pandas') and callable(getattr(data, 'to_pandas')):
        try:
            pdf = data.to_pandas()
            con.register('input_df', pdf)
            logger.debug("通过 to_pandas() 动态注册输入数据")
            return con, 'input_df'
        except Exception as e:
            logger.debug(f"to_pandas 失败（忽略）: {e}")

    logger.error(f"未支持的输入类型: {type(data)}")
    raise ValueError(
        f"无法识别输入数据类型 {type(data)}，"
        "支持类型：pandas.DataFrame, polars.DataFrame, 文件路径(str/Path)"
    )
