"""
行业聚合分析器
=============

业务功能:
- 按行业计算平均值
- 多维度聚合
- 保留元数据列
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


class IndustryAggregator(BaseAnalyzer):
    """行业聚合分析器

    专注于行业级聚合计算,如平均值、中位数等
    """

    def aggregate_avg(
        self,
        group_cols: Union[str, List[str]],
        metrics: Optional[List[str]] = None,
        cast_double: bool = True,
        prefix: str = "industry_",
        suffix: str = "_avg",
        keep_cols: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        """按分组列计算平均值

        Args:
            group_cols: 分组列(字符串或列表)
            metrics: 要聚合的指标列(None时自动检测)
            cast_double: 是否转换为DOUBLE类型
            prefix: 输出列名前缀
            suffix: 输出列名后缀
            keep_cols: 需要保留的额外列(通过ANY_VALUE)

        Returns:
            聚合后的DataFrame(每组一行)
        """
        # 标准化分组列为列表
        group_cols_list = (
            [group_cols] if isinstance(group_cols, str) else list(group_cols)
        )
        if not group_cols_list:
            raise ValueError("group_cols不能为空")

        # 验证分组列存在
        self.validate_columns(group_cols_list)

        # 确定要聚合的指标列
        if metrics is None:
            # 默认候选指标(常见财务指标)
            candidates = [
                'roic', 'roic_avg', 'roe_waa', 'roe_waa_avg', 'roa', 'roa_avg',
                'ocfps', 'ocfps_avg', 'eps', 'eps_avg', 'or_yoy', 'or_yoy_avg',
                'dt_netprofit_yoy', 'dt_netprofit_yoy_avg', 'grossprofit_margin',
                'grossprofit_margin_avg'
            ]
            metrics = self.filter_valid_columns(candidates)
            # 排除分组列
            metrics = [m for m in metrics if m not in group_cols_list]

            if not metrics:
                raise ValueError("未找到可聚合指标,请显式指定metrics参数")
        else:
            # 过滤存在且非分组列的指标
            valid_metrics = [
                m for m in metrics
                if m in self.columns and m not in group_cols_list
            ]
            if len(valid_metrics) < len(metrics):
                invalid = set(metrics) - set(valid_metrics)
                self.logger.warning(f"忽略无效指标: {invalid}")
            metrics = valid_metrics
            if not metrics:
                raise ValueError("没有有效的聚合指标")

        # 构建SELECT子句
        select_parts = []

        # 1. 分组列
        select_parts.extend([self.quote(g) for g in group_cols_list])

        # 2. 保留列(通过ANY_VALUE)
        keep_cols = keep_cols or ['industry']
        keep_available = self.filter_valid_columns(keep_cols)
        # 排除分组列
        keep_available = [c for c in keep_available if c not in group_cols_list]
        select_parts.extend([
            f"ANY_VALUE({self.quote(kc)}) AS {self.quote(kc)}"
            for kc in keep_available
        ])

        # 3. 聚合列(AVG)
        agg_cols = []
        for m in metrics:
            # 去掉_avg后缀作为基础名
            base_name = m[:-4] if m.endswith('_avg') else m
            out_col = f"{prefix}{base_name}{suffix}"

            # 构建聚合表达式
            expr = (
                f"TRY_CAST({self.quote(m)} AS DOUBLE)"
                if cast_double else self.quote(m)
            )
            select_parts.append(f"AVG({expr}) AS {self.quote(out_col)}")
            agg_cols.append(out_col)

        # 构建完整SQL
        group_by_clause = ", ".join([self.quote(g) for g in group_cols_list])
        sql = f"""
            SELECT {', '.join(select_parts)}
            FROM {self.source_sql}
            GROUP BY {group_by_clause}
            ORDER BY {group_by_clause}
        """

        # 执行查询
        result = self.execute(sql)

        self.logger.info(
            f"calc_industry_avg: 分组={group_cols_list}, "
            f"输出行={len(result)}, 聚合列={len(agg_cols)}"
        )

        return result


@register_method(
    engine_name="calc_industry_avg",
    component_type="business_engine",
    engine_type="duckdb",
    description="按分组列求平均(纯SQL实现,高性能聚合)"
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
    """按分组列计算平均值(函数式API)

    这是面向函数式编程的便捷接口,内部使用IndustryAggregator

    Args:
        data: 输入数据(DataFrame或文件路径)
        group_cols: 分组列(字符串或列表)
        metrics: 要聚合的指标列(None时自动检测)
        cast_double: 是否转换为DOUBLE类型
        prefix: 输出列名前缀
        suffix: 输出列名后缀
        keep_cols: 需要保留的额外列(通过ANY_VALUE)

    Returns:
        聚合后的DataFrame(每组一行)

    Examples:
        >>> # 计算每个行业的ROIC和ROE平均值
        >>> df_avg = calc_industry_avg(
        ...     data='data/company.csv',
        ...     group_cols='industry',
        ...     metrics=['roic', 'roe']
        ... )
    """
    with IndustryAggregator(data) as analyzer:
        return analyzer.aggregate_avg(
            group_cols=group_cols,
            metrics=metrics,
            cast_double=cast_double,
            prefix=prefix,
            suffix=suffix,
            keep_cols=keep_cols,
        )
