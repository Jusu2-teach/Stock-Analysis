"""
General Analysis Engine using DuckDB
"""
import sys
from pathlib import Path
import logging
import pandas as pd
from typing import Union, List, Optional, Dict

# orchestrator path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))
from orchestrator.decorators.register import register_method
from ..core.duckdb_utils import _q, _get_duckdb_module, _init_duckdb_and_source

logger = logging.getLogger(__name__)

@register_method(
    engine_name="load_file",
    component_type="business_engine",
    engine_type="duckdb",
    description="Load file into DataFrame using DuckDB"
)
def load_file(path: Union[str, Path] = None, file_path: Union[str, Path] = None, **kwargs) -> pd.DataFrame:
    """
    Load a file (CSV, Parquet) into a pandas DataFrame using DuckDB.
    """
    target_path = path or file_path
    if not target_path:
        raise ValueError("Either 'path' or 'file_path' must be provided")

    con, source = _init_duckdb_and_source(target_path)
    return con.execute(f"SELECT * FROM {source}").df()

@register_method(
    engine_name="calc_industry_avg",
    component_type="business_engine",
    engine_type="duckdb",
    description="Calculate industry averages"
)
def calc_industry_avg(
    data: Union[str, Path, pd.DataFrame],
    group_cols: Union[str, List[str]],
    metrics: Optional[List[str]] = None,
    cast_double: bool = True,
    prefix: str = "industry_",
    suffix: str = "_avg",
    keep_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Calculate average values grouped by specified columns (Pure DuckDB SQL)."""
    con, source_sql = _init_duckdb_and_source(data)

    group_cols_list = [group_cols] if isinstance(group_cols, str) else list(group_cols)
    if not group_cols_list:
        raise ValueError("group_cols cannot be empty")

    cols_query = f"DESCRIBE SELECT * FROM {source_sql}"
    cols_info = con.execute(cols_query).df()
    all_cols = cols_info['column_name'].tolist()

    missing_groups = [g for g in group_cols_list if g not in all_cols]
    if missing_groups:
        raise ValueError(f"Missing group columns: {missing_groups}")

    if metrics is None:
        candidates = [
            'roic', 'roic_avg', 'roe_waa', 'roe_waa_avg', 'roa', 'roa_avg',
            'ocfps', 'ocfps_avg', 'eps', 'eps_avg', 'or_yoy', 'or_yoy_avg',
            'dt_netprofit_yoy', 'dt_netprofit_yoy_avg', 'grossprofit_margin',
            'grossprofit_margin_avg'
        ]
        metrics = [c for c in candidates if c in all_cols and c not in group_cols_list]
        if not metrics:
            raise ValueError("No metrics found to aggregate, please specify 'metrics'")
    else:
        valid_metrics = [m for m in metrics if m in all_cols and m not in group_cols_list]
        if len(valid_metrics) < len(metrics):
            invalid = set(metrics) - set(valid_metrics)
            logger.warning(f"Ignoring invalid metrics: {invalid}")
        metrics = valid_metrics
        if not metrics:
            raise ValueError("No valid metrics to aggregate")

    select_parts = []
    select_parts.extend([_q(g) for g in group_cols_list])

    keep_cols = keep_cols or ['industry']
    keep_available = [c for c in keep_cols if c in all_cols and c not in group_cols_list]
    select_parts.extend([f"ANY_VALUE({_q(kc)}) AS {_q(kc)}" for kc in keep_available])

    agg_cols = []
    for m in metrics:
        base_name = m[:-4] if m.endswith('_avg') else m
        out_col = f"{prefix}{base_name}{suffix}"
        expr = f"TRY_CAST({_q(m)} AS DOUBLE)" if cast_double else _q(m)
        select_parts.append(f"AVG({expr}) AS {_q(out_col)}")
        agg_cols.append(out_col)

    group_by_clause = ", ".join([_q(g) for g in group_cols_list])
    sql = f"""
        SELECT {', '.join(select_parts)}
        FROM {source_sql}
        GROUP BY {group_by_clause}
        ORDER BY {group_by_clause}
    """

    logger.debug(f"calc_industry_avg SQL:\n{sql}")
    result = con.execute(sql).df()
    logger.info(f"calc_industry_avg: groups={group_cols_list}, rows={len(result)}, agg_cols={len(agg_cols)}")
    return result

@register_method(
    engine_name="filter_outperform_industry",
    component_type="business_engine",
    engine_type="duckdb",
    description="Filter companies outperforming industry average"
)
def filter_outperform_industry(
    a_data: Union[str, Path, pd.DataFrame],
    industry_data: Union[str, Path, pd.DataFrame],
    industry_col: str = "industry",
    company_id_col: str = "ts_code",
    metric_map: Optional[Dict[str, str]] = None,
    require_all: bool = True,
) -> pd.DataFrame:
    """Filter companies that outperform industry averages."""
    con, comp_source = _init_duckdb_and_source(a_data)

    # Handle industry_data separately but within same context if possible,
    # or register to same connection.
    # _init_duckdb_and_source creates a NEW connection.
    # We need to register industry_data to 'con'.

    # Re-implement logic to register industry_data to 'con'
    ind_source = "industry_table"

    # Try to load industry_data
    if isinstance(industry_data, pd.DataFrame):
        con.register(ind_source, industry_data)
    elif isinstance(industry_data, (str, Path)):
        p = Path(industry_data)
        if not p.exists():
             raise FileNotFoundError(f"Industry data file not found: {p}")
        norm_path = str(p).replace('\\', '/').replace("'", "''")
        suf = p.suffix.lower()
        if suf == '.parquet':
            ind_source = f"read_parquet('{norm_path}')"
        elif suf in ('.csv', '.svc'):
            ind_source = f"read_csv_auto('{norm_path}')"
        else:
             raise ValueError(f"Unsupported format: {suf}")
    elif hasattr(industry_data, 'to_pandas'):
         con.register(ind_source, industry_data.to_pandas())
    else:
         raise ValueError(f"Unsupported industry_data type: {type(industry_data)}")

    if not metric_map:
        raise ValueError("metric_map is required")

    comp_cols = set(con.execute(f"DESCRIBE SELECT * FROM {comp_source}").df()['column_name'].tolist())
    # For industry source which might be a function call, we need to select * from it to describe
    ind_cols = set(con.execute(f"DESCRIBE SELECT * FROM {ind_source}").df()['column_name'].tolist())

    if industry_col not in comp_cols:
        raise ValueError(f"Company data missing industry column: {industry_col}")
    if industry_col not in ind_cols:
        raise ValueError(f"Industry data missing industry column: {industry_col}")
    if company_id_col not in comp_cols:
        raise ValueError(f"Company data missing ID column: {company_id_col}")

    valid_mappings = {}
    for comp_col, ind_col in metric_map.items():
        if comp_col not in comp_cols:
            logger.warning(f"Ignoring missing company column: {comp_col}")
            continue
        if ind_col not in ind_cols:
            logger.warning(f"Ignoring missing industry column: {ind_col}")
            continue
        valid_mappings[comp_col] = ind_col

    if not valid_mappings:
        raise ValueError("No valid metric mappings found")

    conditions = []
    for comp_col, ind_col in valid_mappings.items():
        condition = f"""
            (TRY_CAST(c.{_q(comp_col)} AS DOUBLE) >
             TRY_CAST(i.{_q(ind_col)} AS DOUBLE) AND
             c.{_q(comp_col)} IS NOT NULL AND
             i.{_q(ind_col)} IS NOT NULL)
        """
        conditions.append(condition)

    operator = " AND " if require_all else " OR "
    where_clause = operator.join(conditions)

    sql = f"""
        SELECT c.*
        FROM {comp_source} AS c
        INNER JOIN {ind_source} AS i
            ON c.{_q(industry_col)} = i.{_q(industry_col)}
        WHERE {where_clause}
    """

    logger.debug(f"filter_outperform_industry SQL:\n{sql}")
    result = con.execute(sql).df()

    logger.info(f"filter_outperform_industry: mapped={len(valid_mappings)}, mode={'AND' if require_all else 'OR'}, result={len(result)}")

    if result.empty:
        logger.warning("Filter result is empty.")

    return result
