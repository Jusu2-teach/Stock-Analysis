"""
数据加载器 (Data Loaders)
=========================

提供统一的数据加载接口，支持：
- CSV/Parquet文件加载
- DuckDB数据源加载
- Pandas DataFrame加载

设计原则:
- 使用DuckDB加速大文件读取
- 统一命名：load_xxx
- 返回标准化的DataFrame

版本: 1.0.0
日期: 2026-01-17
"""

import logging
from pathlib import Path
from typing import Union, Optional

import pandas as pd

from orchestrator.decorators.register import register_method
from shared.performance import method_timing
from ..core.duckdb_utils import _init_duckdb_and_source

logger = logging.getLogger(__name__)


@register_method(
    engine_name="load_file",
    component_type="business_engine",
    engine_type="duckdb",
    description="加载文件到DataFrame (使用DuckDB)"
)
@method_timing(log_threshold_ms=200.0)
def load_file(
    path: Union[str, Path] = None,
    file_path: Union[str, Path] = None,
    **kwargs
) -> pd.DataFrame:
    """加载CSV或Parquet文件到DataFrame

    Args:
        path: 文件路径（优先）
        file_path: 文件路径（备选）

    Returns:
        DataFrame

    Raises:
        ValueError: 路径参数缺失
        FileNotFoundError: 文件不存在
    """
    target_path = path or file_path
    if not target_path:
        raise ValueError("必须提供 'path' 或 'file_path' 参数")

    target_path = Path(target_path)
    if not target_path.exists():
        raise FileNotFoundError(f"文件不存在: {target_path}")

    logger.info(f"加载文件: {target_path}")

    con, source = _init_duckdb_and_source(target_path)
    df = con.execute(f"SELECT * FROM {source}").df()

    logger.info(f"文件加载完成: {len(df)} 行, {len(df.columns)} 列")

    return df


@register_method(
    engine_name="load_financial_data",
    component_type="business_engine",
    engine_type="duckdb",
    description="加载财务数据 (标准化列名)"
)
@method_timing(log_threshold_ms=300.0)
def load_financial_data(
    path: Union[str, Path],
    required_columns: Optional[list] = None,
    **kwargs
) -> pd.DataFrame:
    """加载财务数据并验证必需列

    Args:
        path: 文件路径
        required_columns: 必需的列名列表

    Returns:
        DataFrame

    Raises:
        ValueError: 缺少必需列
    """
    df = load_file(path=path)

    if required_columns:
        missing_cols = set(required_columns) - set(df.columns)
        if missing_cols:
            raise ValueError(f"缺少必需列: {missing_cols}")

    logger.info(f"财务数据加载完成: {len(df)} 行")

    return df
