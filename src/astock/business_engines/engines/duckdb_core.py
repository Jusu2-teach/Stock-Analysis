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


# ============================================================================
# 业务方法（注册到orchestrator）
# ============================================================================


"""
业务引擎 - DuckDB实现
==================

专门负责业务逻辑：财务比率计算、风险分析、投资评估等业务功能
"""

import sys
from pathlib import Path
import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, Union, List

logger = logging.getLogger(__name__)


def load_file(path: str) -> pd.DataFrame:
    """读取文件为 pandas DataFrame (通过 DuckDB)

    仅做无条件读取，不做列裁剪，方便后续 compute_hand 或聚合函数继续处理。
    """
    duckdb = _get_duckdb_module()
    if duckdb is None:
        raise ImportError("需要 duckdb 模块")
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"文件不存在: {p}")
    suf = p.suffix.lower()
    con = duckdb.connect(database=':memory:')
    esc = str(p).replace("'", "''")
    if suf == '.parquet':
        sql = f"SELECT * FROM read_parquet('{esc}')"
    elif suf in ('.csv', '.svc'):
        sql = f"SELECT * FROM read_csv_auto('{esc}')"
    else:
        raise ValueError(f"不支持的文件格式: {suf}")
    return con.execute(sql).df()


def calc_industry_avg(
    data: Union[str, Path, pd.DataFrame],
    group_cols: Union[str, List[str]],
    metrics: Optional[List[str]] = None,
    cast_double: bool = True,
    prefix: str = "industry_",
    suffix: str = "_avg",
    keep_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    """按分组列计算平均值（纯 DuckDB SQL 实现）

    Args:
        data: 输入数据（DataFrame 或文件路径）
        group_cols: 分组列（字符串或列表）
        metrics: 要聚合的指标列（None 时自动检测）
        cast_double: 是否转换为 DOUBLE 类型
        prefix: 输出列名前缀
        suffix: 输出列名后缀
        keep_cols: 需要保留的额外列（通过 ANY_VALUE）

    Returns:
        聚合后的 DataFrame（每组一行）
    """
    con, source_sql = _init_duckdb_and_source(data)

    # 标准化分组列为列表
    group_cols_list = [group_cols] if isinstance(group_cols, str) else list(group_cols)
    if not group_cols_list:
        raise ValueError("group_cols 不能为空")

    # 使用 SQL 获取列信息（更快）
    cols_query = f"DESCRIBE SELECT * FROM {source_sql}"
    cols_info = con.execute(cols_query).df()
    all_cols = cols_info['column_name'].tolist()

    # 验证分组列存在
    missing_groups = [g for g in group_cols_list if g not in all_cols]
    if missing_groups:
        raise ValueError(f"分组列不存在: {missing_groups}")

    # 确定要聚合的指标列
    if metrics is None:
        # 默认候选指标（常见财务指标）
        candidates = [
            'roic', 'roic_avg', 'roe_waa', 'roe_waa_avg', 'roa', 'roa_avg',
            'ocfps', 'ocfps_avg', 'eps', 'eps_avg', 'or_yoy', 'or_yoy_avg',
            'dt_netprofit_yoy', 'dt_netprofit_yoy_avg', 'grossprofit_margin',
            'grossprofit_margin_avg'
        ]
        metrics = [c for c in candidates if c in all_cols and c not in group_cols_list]
        if not metrics:
            raise ValueError("未找到可聚合指标，请显式指定 metrics 参数")
    else:
        # 过滤存在且非分组列的指标
        valid_metrics = [m for m in metrics if m in all_cols and m not in group_cols_list]
        if len(valid_metrics) < len(metrics):
            invalid = set(metrics) - set(valid_metrics)
            logger.warning(f"忽略无效指标: {invalid}")
        metrics = valid_metrics
        if not metrics:
            raise ValueError("没有有效的聚合指标")

    # 构建 SELECT 子句
    select_parts = []

    # 1. 分组列
    select_parts.extend([_q(g) for g in group_cols_list])

    # 2. 保留列（通过 ANY_VALUE）
    keep_cols = keep_cols or ['industry']
    keep_available = [c for c in keep_cols if c in all_cols and c not in group_cols_list]
    select_parts.extend([f"ANY_VALUE({_q(kc)}) AS {_q(kc)}" for kc in keep_available])

    # 3. 聚合列（AVG）
    agg_cols = []
    for m in metrics:
        # 去掉 _avg 后缀作为基础名
        base_name = m[:-4] if m.endswith('_avg') else m
        out_col = f"{prefix}{base_name}{suffix}"

        # 构建聚合表达式
        expr = f"TRY_CAST({_q(m)} AS DOUBLE)" if cast_double else _q(m)
        select_parts.append(f"AVG({expr}) AS {_q(out_col)}")
        agg_cols.append(out_col)

    # 构建完整 SQL
    group_by_clause = ", ".join([_q(g) for g in group_cols_list])
    sql = f"""
        SELECT {', '.join(select_parts)}
        FROM {source_sql}
        GROUP BY {group_by_clause}
        ORDER BY {group_by_clause}
    """

    logger.debug(f"calc_industry_avg SQL:\n{sql}")

    # 执行并返回
    result = con.execute(sql).df()
    logger.info(
        f"calc_industry_avg: 分组={group_cols_list}, 输出行={len(result)}, 聚合列={len(agg_cols)}"
    )

    return result


def filter_outperform_industry(
    a_data: Union[str, Path, pd.DataFrame],
    industry_data: Union[str, Path, pd.DataFrame],
    industry_col: str = "industry",
    company_id_col: str = "ts_code",
    metric_map: Optional[Dict[str, str]] = None,
    require_all: bool = True,
) -> pd.DataFrame:
    """筛选指标超越行业平均的公司（纯 DuckDB SQL 实现）

    Args:
        a_data: 公司数据（含公司级指标）
        industry_data: 行业数据（含行业平均指标）
        industry_col: 行业列名
        company_id_col: 公司标识列名
        metric_map: 指标映射 {公司列: 行业列}，比较规则为 公司列 > 行业列
        require_all: True=所有指标都超越(AND)，False=任意指标超越(OR)

    Returns:
        满足条件的公司数据（保留所有原始列）

    Examples:
        >>> # 筛选 ROIC 和 ROE 都超越行业平均的公司
        >>> result = filter_outperform_industry(
        ...     a_data=company_df,
        ...     industry_data=industry_df,
        ...     metric_map={
        ...         '5yd_ts_code_roic_avg': 'industry_roic_avg',
        ...         '5yd_ts_code_roe_avg': 'industry_roe_avg'
        ...     },
        ...     require_all=True
        ... )
    """
    # 初始化主连接（使用公司数据）
    con, comp_source = _init_duckdb_and_source(a_data)

    # 处理行业数据：复用 _init_duckdb_and_source 的逻辑，但使用同一个连接
    # 由于 _init_duckdb_and_source 会创建新连接，我们需要手动处理
    duckdb = _get_duckdb_module()

    # 处理 Polars（如果有）
    try:
        import polars as pl
        if isinstance(industry_data, pl.DataFrame):
            industry_data = industry_data.to_pandas()
    except Exception:
        pass

    # 处理不同类型的行业数据
    if isinstance(industry_data, pd.DataFrame):
        con.register('industry_table', industry_data)
        ind_source = 'industry_table'
    elif isinstance(industry_data, (str, Path)):
        p = Path(industry_data)
        if not p.exists():
            raise FileNotFoundError(f"行业数据文件不存在: {p}")
        norm_path = str(p).replace('\\', '/').replace("'", "''")
        suf = p.suffix.lower()
        if suf == '.parquet':
            ind_source = f"read_parquet('{norm_path}')"
        elif suf in ('.csv', '.svc'):
            ind_source = f"read_csv_auto('{norm_path}')"
        else:
            raise ValueError(f"不支持的文件格式: {suf}")
    elif hasattr(industry_data, 'to_pandas') and callable(getattr(industry_data, 'to_pandas')):
        # 兜底：其他支持 to_pandas() 的对象
        con.register('industry_table', industry_data.to_pandas())
        ind_source = 'industry_table'
    else:
        raise ValueError(f"不支持的 industry_data 类型: {type(industry_data)}")

    # 验证必需参数
    if not metric_map:
        raise ValueError("必须提供 metric_map 参数")

    # 使用 SQL 检查列是否存在
    comp_cols_query = f"DESCRIBE SELECT * FROM {comp_source}"
    ind_cols_query = f"DESCRIBE SELECT * FROM {ind_source}"

    comp_cols = set(con.execute(comp_cols_query).df()['column_name'].tolist())
    ind_cols = set(con.execute(ind_cols_query).df()['column_name'].tolist())

    # 验证关键列
    if industry_col not in comp_cols:
        raise ValueError(f"公司数据缺少行业列: {industry_col}")
    if industry_col not in ind_cols:
        raise ValueError(f"行业数据缺少行业列: {industry_col}")
    if company_id_col not in comp_cols:
        raise ValueError(f"公司数据缺少公司标识列: {company_id_col}")

    # 验证并过滤指标映射
    valid_mappings = {}
    for comp_col, ind_col in metric_map.items():
        if comp_col not in comp_cols:
            logger.warning(f"忽略公司列（不存在）: {comp_col}")
            continue
        if ind_col not in ind_cols:
            logger.warning(f"忽略行业列（不存在）: {ind_col}")
            continue
        valid_mappings[comp_col] = ind_col

    if not valid_mappings:
        raise ValueError("没有有效的指标映射")

    # 构建 SQL 比较条件
    conditions = []
    for comp_col, ind_col in valid_mappings.items():
        # 使用 COALESCE 处理 NULL 值，NULL 视为不满足条件
        condition = f"""
            (TRY_CAST(c.{_q(comp_col)} AS DOUBLE) >
             TRY_CAST(i.{_q(ind_col)} AS DOUBLE) AND
             c.{_q(comp_col)} IS NOT NULL AND
             i.{_q(ind_col)} IS NOT NULL)
        """
        conditions.append(condition)

    # 根据 require_all 决定使用 AND 还是 OR
    operator = " AND " if require_all else " OR "
    where_clause = operator.join(conditions)

    # 构建完整 SQL（保留公司数据的所有列）
    sql = f"""
        SELECT c.*
        FROM {comp_source} AS c
        INNER JOIN {ind_source} AS i
            ON c.{_q(industry_col)} = i.{_q(industry_col)}
        WHERE {where_clause}
    """

    logger.debug(f"filter_outperform_industry SQL:\n{sql}")

    # 执行查询
    result = con.execute(sql).df()

    # 统计信息
    total_companies = con.execute(f"SELECT COUNT(*) AS cnt FROM {comp_source}").df()['cnt'][0]
    total_industries = con.execute(f"SELECT COUNT(*) AS cnt FROM {ind_source}").df()['cnt'][0]

    logger.info(
        f"filter_outperform_industry: "
        f"输入公司={total_companies}, 行业={total_industries}, "
        f"映射指标={len(valid_mappings)}, 筛选模式={'AND' if require_all else 'OR'}, "
        f"输出公司={len(result)}"
    )

    if result.empty:
        logger.warning("筛选结果为空，建议检查 metric_map 或放宽筛选条件")

    return result
