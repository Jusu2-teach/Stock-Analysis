import duckdb
import pandas as pd
from pathlib import Path
from typing import Union, Tuple, Optional, Any
import logging

logger = logging.getLogger(__name__)

def _q(name: str) -> str:
    """DuckDB 标识符引用（双引号包裹，内部双引号转义）"""
    if name is None:
        return '""'
    return '"' + str(name).replace('"', '""') + '"'

def _get_duckdb_module():
    """获取 duckdb 模块"""
    return duckdb

def _init_duckdb_and_source(data: Any) -> Tuple[duckdb.DuckDBPyConnection, str]:
    """
    初始化 DuckDB 连接并返回源表引用

    Args:
        data: 输入数据，支持 DataFrame、文件路径或其他数据源

    Returns:
        Tuple[duckdb.DuckDBPyConnection, str]: Connection and source SQL/table name.
    """
    con = duckdb.connect(database=':memory:')

    # 1) 先处理 DataFrame / polars
    try:
        import polars as pl
        if isinstance(data, pl.DataFrame):
            logger.debug("接收到 polars.DataFrame，转换为 pandas 注册")
            data = data.to_pandas()
    except ImportError:
        pass
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
        # 统一使用正斜杠
        norm_path = str(p).replace('\\', '/').replace("'", "''")

        if suf == '.parquet':
            source = f"read_parquet('{norm_path}')"
        elif suf in ('.csv', '.svc'):
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

    raise ValueError(f"无法识别输入数据类型 {type(data)}")
