"""
超越行业分析器
=============

业务功能:
- 筛选指标超越行业平均的公司
- JOIN+过滤组合
- 灵活的比较规则(AND/OR)
"""

import logging
import pandas as pd
import sys
from pathlib import Path
from typing import Union, List, Optional, Dict

# orchestrator 已移至根目录
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent.parent))
from orchestrator import register_method

from .base_analyzer import BaseAnalyzer
from ..data import DuckDBConnector, DataLoader, quote_identifier

logger = logging.getLogger(__name__)


class OutperformFilter(BaseAnalyzer):
    """超越行业筛选器

    筛选指标超越行业平均的公司
    """

    def __init__(
        self,
        company_data: Union[str, Path, pd.DataFrame],
        industry_data: Union[str, Path, pd.DataFrame],
        connector: Optional[DuckDBConnector] = None
    ):
        """初始化筛选器

        Args:
            company_data: 公司数据(含公司级指标)
            industry_data: 行业数据(含行业平均指标)
            connector: DuckDB连接器(可选)
        """
        super().__init__(company_data, connector)

        # 加载行业数据
        self.industry_source = DataLoader.load_to_connector(
            industry_data, self.connector, 'industry_table'
        )

        # 获取行业数据列信息
        ind_cols_query = f"DESCRIBE SELECT * FROM {self.industry_source}"
        ind_cols_df = self.connector.execute(ind_cols_query).df()
        self.industry_columns = ind_cols_df['column_name'].tolist()

    def filter_outperform(
        self,
        industry_col: str = "industry",
        company_id_col: str = "ts_code",
        metric_map: Optional[Dict[str, str]] = None,
        require_all: bool = True,
    ) -> pd.DataFrame:
        """筛选指标超越行业平均的公司

        Args:
            industry_col: 行业列名
            company_id_col: 公司标识列名
            metric_map: 指标映射 {公司列: 行业列},比较规则为 公司列 > 行业列
            require_all: True=所有指标都超越(AND),False=任意指标超越(OR)

        Returns:
            满足条件的公司数据(保留所有原始列)

        Examples:
            >>> # 筛选ROIC和ROE都超越行业平均的公司
            >>> filter = OutperformFilter(company_df, industry_df)
            >>> result = filter.filter_outperform(
            ...     metric_map={
            ...         '5yd_ts_code_roic_avg': 'industry_roic_avg',
            ...         '5yd_ts_code_roe_avg': 'industry_roe_avg'
            ...     },
            ...     require_all=True
            ... )
        """
        # 验证必需参数
        if not metric_map:
            raise ValueError("必须提供metric_map参数")

        # 验证关键列
        if industry_col not in self.columns:
            raise ValueError(f"公司数据缺少行业列: {industry_col}")
        if industry_col not in self.industry_columns:
            raise ValueError(f"行业数据缺少行业列: {industry_col}")
        if company_id_col not in self.columns:
            raise ValueError(f"公司数据缺少公司标识列: {company_id_col}")

        # 验证并过滤指标映射
        valid_mappings = {}
        for comp_col, ind_col in metric_map.items():
            if comp_col not in self.columns:
                self.logger.warning(f"忽略公司列(不存在): {comp_col}")
                continue
            if ind_col not in self.industry_columns:
                self.logger.warning(f"忽略行业列(不存在): {ind_col}")
                continue
            valid_mappings[comp_col] = ind_col

        if not valid_mappings:
            raise ValueError("没有有效的指标映射")

        # 构建SQL比较条件
        conditions = []
        for comp_col, ind_col in valid_mappings.items():
            # 使用COALESCE处理NULL值,NULL视为不满足条件
            condition = f"""
                (TRY_CAST(c.{self.quote(comp_col)} AS DOUBLE) >
                 TRY_CAST(i.{self.quote(ind_col)} AS DOUBLE) AND
                 c.{self.quote(comp_col)} IS NOT NULL AND
                 i.{self.quote(ind_col)} IS NOT NULL)
            """
            conditions.append(condition)

        # 根据require_all决定使用AND还是OR
        operator = " AND " if require_all else " OR "
        where_clause = operator.join(conditions)

        # 构建完整SQL(保留公司数据的所有列)
        sql = f"""
            SELECT c.*
            FROM {self.source_sql} AS c
            INNER JOIN {self.industry_source} AS i
                ON c.{self.quote(industry_col)} = i.{self.quote(industry_col)}
            WHERE {where_clause}
        """

        # 执行查询
        result = self.execute(sql)

        # 统计信息
        total_companies = self.get_row_count()
        total_industries_sql = f"SELECT COUNT(*) AS cnt FROM {self.industry_source}"
        total_industries = int(
            self.connector.execute(total_industries_sql).df()['cnt'].iloc[0]
        )

        self.logger.info(
            f"filter_outperform_industry: "
            f"输入公司={total_companies}, 行业={total_industries}, "
            f"映射指标={len(valid_mappings)}, 筛选模式={'AND' if require_all else 'OR'}, "
            f"输出公司={len(result)}"
        )

        if result.empty:
            self.logger.warning("筛选结果为空,建议检查metric_map或放宽筛选条件")

        return result


@register_method(
    engine_name="filter_outperform_industry",
    component_type="business_engine",
    engine_type="duckdb",
    description="公司指标超越行业平均筛选器(纯SQL实现,高性能JOIN+过滤)"
)
def filter_outperform_industry(
    a_data: Union[str, Path, pd.DataFrame],
    industry_data: Union[str, Path, pd.DataFrame],
    industry_col: str = "industry",
    company_id_col: str = "ts_code",
    metric_map: Optional[Dict[str, str]] = None,
    require_all: bool = True,
) -> pd.DataFrame:
    """筛选指标超越行业平均的公司(函数式API)

    这是面向函数式编程的便捷接口,内部使用OutperformFilter

    Args:
        a_data: 公司数据(含公司级指标)
        industry_data: 行业数据(含行业平均指标)
        industry_col: 行业列名
        company_id_col: 公司标识列名
        metric_map: 指标映射 {公司列: 行业列},比较规则为 公司列 > 行业列
        require_all: True=所有指标都超越(AND),False=任意指标超越(OR)

    Returns:
        满足条件的公司数据(保留所有原始列)

    Examples:
        >>> # 筛选ROIC和ROE都超越行业平均的公司
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
    with OutperformFilter(a_data, industry_data) as filter_obj:
        return filter_obj.filter_outperform(
            industry_col=industry_col,
            company_id_col=company_id_col,
            metric_map=metric_map,
            require_all=require_all,
        )
