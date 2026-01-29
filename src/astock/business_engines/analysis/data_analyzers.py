"""
数据分析器 (Data Analyzers)
===========================

提供数据分析和计算功能：
- 行业平均值计算
- 超越行业平均筛选
- 数据聚合和转换

设计原则:
- 使用DuckDB SQL优化性能
- 统一命名：calculate_xxx, filter_xxx, aggregate_xxx
- 严格的参数验证

版本: 1.0.0
日期: 2026-01-17
"""

import logging
from pathlib import Path
from typing import Union, List, Optional, Dict

import pandas as pd

from orchestrator.decorators.register import register_method
from shared.performance import method_timing
from ..core.duckdb_utils import _q, _init_duckdb_and_source

logger = logging.getLogger(__name__)


@register_method(
    engine_name="calculate_industry_average",
    component_type="business_engine",
    engine_type="duckdb",
    description="计算行业平均值"
)
@method_timing(log_threshold_ms=300.0)
def calculate_industry_average(
    data: Union[str, Path, pd.DataFrame],
    group_cols: Union[str, List[str]],
    metrics: Optional[List[str]] = None,
    cast_double: bool = True,
    prefix: str = "industry_",
    suffix: str = "_avg",
    keep_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    """计算按指定列分组的平均值 (纯DuckDB SQL)

    Args:
        data: 数据源 (文件路径或DataFrame)
        group_cols: 分组列 (如 "industry" 或 ["industry", "year"])
        metrics: 要聚合的指标列（None则自动检测）
        cast_double: 是否转换为DOUBLE类型
        prefix: 输出列名前缀
        suffix: 输出列名后缀
        keep_cols: 保留的附加列（使用ANY_VALUE聚合）

    Returns:
        DataFrame包含分组聚合结果

    Raises:
        ValueError: 参数错误或列不存在
    """
    con, source_sql = _init_duckdb_and_source(data)

    # 标准化分组列
    group_cols_list = [group_cols] if isinstance(group_cols, str) else list(group_cols)
    if not group_cols_list:
        raise ValueError("group_cols 不能为空")

    # 获取所有列名
    cols_query = f"DESCRIBE SELECT * FROM {source_sql}"
    cols_info = con.execute(cols_query).df()
    all_cols = cols_info['column_name'].tolist()

    # 验证分组列存在
    missing_groups = [g for g in group_cols_list if g not in all_cols]
    if missing_groups:
        raise ValueError(f"缺少分组列: {missing_groups}")

    # 自动检测或验证指标列
    if metrics is None:
        # 默认财务指标候选
        candidates = [
            'roic', 'roic_avg', 'roe_waa', 'roe_waa_avg', 'roa', 'roa_avg',
            'ocfps', 'ocfps_avg', 'eps', 'eps_avg', 'or_yoy', 'or_yoy_avg',
            'dt_netprofit_yoy', 'dt_netprofit_yoy_avg', 'grossprofit_margin',
            'grossprofit_margin_avg', 'revenue', 'profit', 'ocf',
        ]
        metrics = [c for c in candidates if c in all_cols and c not in group_cols_list]
        if not metrics:
            raise ValueError("未找到可聚合的指标列，请通过 'metrics' 参数指定")
    else:
        # 验证指标列存在
        valid_metrics = [m for m in metrics if m in all_cols and m not in group_cols_list]
        if len(valid_metrics) < len(metrics):
            invalid = set(metrics) - set(valid_metrics)
            logger.warning(f"忽略无效指标: {invalid}")
        metrics = valid_metrics
        if not metrics:
            raise ValueError("没有有效的指标列可以聚合")

    # 构建SELECT子句
    select_parts = []

    # 分组列
    select_parts.extend([_q(g) for g in group_cols_list])

    # 保留列 (使用ANY_VALUE)
    keep_cols = keep_cols or ['industry']
    keep_available = [c for c in keep_cols if c in all_cols and c not in group_cols_list]
    select_parts.extend([f"ANY_VALUE({_q(kc)}) AS {_q(kc)}" for kc in keep_available])

    # 聚合列
    agg_cols = []
    for m in metrics:
        # 移除 _avg 后缀（如果存在）
        base_name = m[:-4] if m.endswith('_avg') else m
        out_col = f"{prefix}{base_name}{suffix}"

        # 类型转换
        expr = f"TRY_CAST({_q(m)} AS DOUBLE)" if cast_double else _q(m)
        select_parts.append(f"AVG({expr}) AS {_q(out_col)}")
        agg_cols.append(out_col)

    # 构建GROUP BY子句
    group_by_clause = ", ".join([_q(g) for g in group_cols_list])

    # 完整SQL
    sql = f"""
        SELECT {', '.join(select_parts)}
        FROM {source_sql}
        GROUP BY {group_by_clause}
        ORDER BY {group_by_clause}
    """

    logger.debug(f"calculate_industry_average SQL:\n{sql}")

    result = con.execute(sql).df()

    logger.info(
        f"行业平均值计算完成: groups={group_cols_list}, "
        f"rows={len(result)}, agg_cols={len(agg_cols)}"
    )

    return result


@register_method(
    engine_name="filter_outperform_industry",
    component_type="business_engine",
    engine_type="duckdb",
    description="筛选超越行业平均的公司"
)
@method_timing(log_threshold_ms=400.0)
def filter_outperform_industry(
    company_data: Union[str, Path, pd.DataFrame],
    industry_data: Union[str, Path, pd.DataFrame],
    industry_col: str = "industry",
    company_id_col: str = "ts_code",
    metric_map: Optional[Dict[str, str]] = None,
    require_all: bool = True,
) -> pd.DataFrame:
    """筛选财务指标超越行业平均的公司

    Args:
        company_data: 公司数据
        industry_data: 行业平均数据
        industry_col: 行业列名
        company_id_col: 公司ID列名
        metric_map: 指标映射 {公司列名: 行业列名}
        require_all: True=所有指标都超越(AND), False=任一指标超越(OR)

    Returns:
        DataFrame包含筛选后的公司

    Raises:
        ValueError: 参数错误或列不存在
    """
    # 初始化DuckDB连接
    con, comp_source = _init_duckdb_and_source(company_data)

    # 注册行业数据到同一连接
    ind_source = "industry_table"
    if isinstance(industry_data, pd.DataFrame):
        con.register(ind_source, industry_data)
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
    elif hasattr(industry_data, 'to_pandas'):
        con.register(ind_source, industry_data.to_pandas())
    else:
        raise ValueError(f"不支持的industry_data类型: {type(industry_data)}")

    # 验证必需参数
    if not metric_map:
        raise ValueError("必须提供 metric_map 参数")

    # 获取列名
    comp_cols = set(
        con.execute(f"DESCRIBE SELECT * FROM {comp_source}").df()['column_name'].tolist()
    )
    ind_cols = set(
        con.execute(f"DESCRIBE SELECT * FROM {ind_source}").df()['column_name'].tolist()
    )

    # 验证关键列存在
    if industry_col not in comp_cols:
        raise ValueError(f"公司数据缺少行业列: {industry_col}")
    if industry_col not in ind_cols:
        raise ValueError(f"行业数据缺少行业列: {industry_col}")
    if company_id_col not in comp_cols:
        raise ValueError(f"公司数据缺少ID列: {company_id_col}")

    # 验证指标映射
    valid_mappings = {}
    for comp_col, ind_col in metric_map.items():
        if comp_col not in comp_cols:
            logger.warning(f"忽略缺失的公司列: {comp_col}")
            continue
        if ind_col not in ind_cols:
            logger.warning(f"忽略缺失的行业列: {ind_col}")
            continue
        valid_mappings[comp_col] = ind_col

    if not valid_mappings:
        raise ValueError("没有有效的指标映射")

    # 构建筛选条件
    conditions = []
    for comp_col, ind_col in valid_mappings.items():
        condition = f"""
            (TRY_CAST(c.{_q(comp_col)} AS DOUBLE) >
             TRY_CAST(i.{_q(ind_col)} AS DOUBLE) AND
             c.{_q(comp_col)} IS NOT NULL AND
             i.{_q(ind_col)} IS NOT NULL)
        """
        conditions.append(condition)

    # AND/OR逻辑
    operator = " AND " if require_all else " OR "
    where_clause = operator.join(conditions)

    # 完整SQL
    sql = f"""
        SELECT c.*
        FROM {comp_source} AS c
        INNER JOIN {ind_source} AS i
            ON c.{_q(industry_col)} = i.{_q(industry_col)}
        WHERE {where_clause}
    """

    logger.debug(f"filter_outperform_industry SQL:\n{sql}")

    result = con.execute(sql).df()

    logger.info(
        f"筛选完成: 映射指标={len(valid_mappings)}, "
        f"模式={'AND' if require_all else 'OR'}, "
        f"结果={len(result)} 行"
    )

    if result.empty:
        logger.warning("筛选结果为空")

    return result
