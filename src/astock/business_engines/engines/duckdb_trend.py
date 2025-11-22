"""
DuckDB 通用趋势分析方法（重构版）
=================================

提供独立的、可复用的趋势分析方法,支持对任意指标(ROIC、ROE、ROA等)进行:
1. 加权平均计算
2. 线性回归趋势分析
3. 趋势过滤和评分调整
4. 行业差异化参数配置
"""

import sys
from pathlib import Path
import math
import pandas as pd
import logging
from typing import Union, List, Optional

# orchestrator 已移至根目录
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))
# from orchestrator import register_method  <-- Removed
from .duckdb_core import _q, _get_duckdb_module, _init_duckdb_and_source  # 使用新的合并模块
from ..trend.config import (
    INDUSTRY_FILTER_CONFIGS,
    DEFAULT_FILTER_CONFIG,
    ROIIC_INDUSTRY_FILTER_CONFIGS,
    DEFAULT_ROIIC_FILTER_CONFIG,
    get_industry_category,
    get_default_config,
)
from ..trend.trend_analyzer import TrendAnalyzer
from ..trend.trend_settings import TrendAnalyzerConfig
from ..trend.trend_components import (
    ConfigResolver,
    TrendEvaluationResult,
    TrendResultCollector,
    TrendRuleEvaluator,
)
# 🔌 导入插件化派生器系统
from ..trend.derivers import find_deriver, list_available_metrics, check_derivable

logger = logging.getLogger(__name__)


def _describe_roiic_spread(spread: float) -> str:
    if not math.isfinite(spread):
        return ""

    if spread >= 10.0:
        return f"ROIIC Spread {spread:.1f}pp：扩张创造大量价值"
    if spread >= 3.0:
        return f"ROIIC Spread {spread:.1f}pp：扩张创造价值"
    if spread >= 0.0:
        return f"ROIIC Spread {spread:.1f}pp：刚好覆盖资本成本"
    if spread > -5.0:
        return f"ROIIC Spread {spread:.1f}pp：扩张回报偏弱"
    return f"ROIIC Spread {spread:.1f}pp：扩张可能毁灭价值"


def analyze_metric_trend(
    data: Union[str, Path, pd.DataFrame],
    group_cols: Union[str, List[str]],
    metric_name: str,
    prefix: str = "",
    suffix: str = "_trend",
    min_periods: int = 5,
    analyzer_config: Optional[TrendAnalyzerConfig] = None,
) -> pd.DataFrame:
    """
    对指定指标进行通用趋势分析

    核心功能:
    1. 计算加权平均(最近数据权重更高)
    2. 线性回归分析(斜率、R²、p值)
    3. 趋势过滤(可选,根据配置)
    4. 评分调整(可选,根据配置)

    Args:
        data: 输入数据(必须包含多期数据,按时间排序)
        group_cols: 分组列(如 'ts_code', 'industry')
        metric_name: 要分析的指标名(如 'roic', 'roe', 'roa')
        prefix: 输出列名前缀(默认空)
        suffix: 输出列名后缀(默认 '_trend')
        min_periods: 最少需要的期数(默认5)
        analyzer_config: 趋势分析器配置(窗口、权重、探针、参考指标等)

    Returns:
        DataFrame,包含:
        - 原分组列
        - {prefix}{metric_name}_weighted{suffix}: 加权平均值
        - {prefix}{metric_name}_slope{suffix}: 趋势斜率
        - {prefix}{metric_name}_r_squared{suffix}: R²
        - {prefix}{metric_name}_latest{suffix}: 最新期值
        - {prefix}{metric_name}_penalty{suffix}: 扣分(如果启用过滤)

    Example:
    >>> # 分析ROIC趋势(使用配置中心参数)
    >>> df_roic = analyze_metric_trend(
    ...     data='data/polars/5yd_final_industry.csv',
    ...     group_cols='ts_code',
    ...     metric_name='roic'
    ... )
    """

    logger.info("=" * 80)
    logger.info(f"🔍 通用趋势分析启动: {metric_name}")
    logger.info("=" * 80)

    # ========== 1. 加载数据 ==========
    con, source_sql = _init_duckdb_and_source(data)

    # 标准化分组列
    group_cols_list = [group_cols] if isinstance(group_cols, str) else list(group_cols)

    # 检查指标列是否存在
    cols_info = con.execute(f"DESCRIBE SELECT * FROM {source_sql}").df()
    all_cols = cols_info['column_name'].tolist()

    # 🔌 插件化指标派生系统
    if metric_name not in all_cols:
        # 尝试使用插件派生指标
        deriver = find_deriver(metric_name, set(all_cols))

        if deriver:
            logger.info(f"🔌 使用插件 {deriver.__class__.__name__} 派生 {metric_name}")
            source_sql = deriver.derive(con, source_sql, group_cols_list[0])

            # 刷新列信息
            cols_info = con.execute(f"DESCRIBE SELECT * FROM {source_sql}").df()
            all_cols = cols_info['column_name'].tolist()

        # 最终检查：如果仍然不存在，提供详细错误
        if metric_name not in all_cols:
            # 使用 check_derivable 获取详细信息
            can_derive, missing = check_derivable(metric_name, set(all_cols))

            if missing:
                raise ValueError(
                    f"❌ 指标 '{metric_name}' 无法派生，缺少必需列: {', '.join(sorted(missing))}\n"
                    f"当前可用列: {', '.join(sorted(all_cols))}"
                )
            else:
                available = list_available_metrics()
                raise ValueError(
                    f"❌ 指标 '{metric_name}' 不存在且无可用派生器。\n"
                    f"可派生指标: {', '.join(available)}\n"
                    f"当前可用列: {', '.join(sorted(all_cols))}"
                )

    logger.info(f"分组列: {group_cols_list}")
    logger.info(f"分析指标: {metric_name}")
    _trend_config = get_default_config()
    logger.info(f"加权方案: {_trend_config.default_weights.tolist()}")

    metric_lower = metric_name.lower()
    default_config = DEFAULT_ROIIC_FILTER_CONFIG if metric_lower == "roiic" else DEFAULT_FILTER_CONFIG
    industry_configs = ROIIC_INDUSTRY_FILTER_CONFIGS if metric_lower == "roiic" else INDUSTRY_FILTER_CONFIGS

    # ========== 2. 解析过滤配置 ==========
    base_config = {"enable_filter": True}
    base_config.update(default_config)
    logger.info(f"过滤基线配置(默认阈值): {base_config}")

    # ========== 3. 读取数据并排序 ==========
    # 检查是否有 name 和 industry 列（用于输出）
    keep_cols = []
    if 'name' in all_cols:
        keep_cols.append('name')
    if 'industry' in all_cols:
        keep_cols.append('industry')

    # 构建SELECT列表
    select_cols = [_q(group_cols_list[0]), _q(metric_name), 'end_date']
    if keep_cols:
        select_cols.extend([_q(col) for col in keep_cols])

    sql_load = f"""
        SELECT {', '.join(select_cols)}
        FROM {source_sql}
        ORDER BY {_q(group_cols_list[0])}, end_date ASC
    """

    df_full = con.execute(sql_load).df()
    logger.info(f"输入数据: {len(df_full)} 行")
    if keep_cols:
        logger.info(f"保留额外列: {keep_cols}")

    # ========== 4. 分组处理 ==========
    eliminated_count = 0
    config_resolver = ConfigResolver(industry_configs)
    rule_evaluator = TrendRuleEvaluator(logger)
    result_collector = TrendResultCollector()

    grouped = df_full.groupby(group_cols_list[0])

    for group_key, group_df in grouped:
        # 检查数据完整性
        if len(group_df) < min_periods:
            logger.debug(f"跳过 {group_key}: 数据不足{min_periods}期(实际{len(group_df)}期)")
            continue

        # ========== 根据行业动态调整过滤参数 ==========
        current_config, _ = config_resolver.resolve(group_key, base_config, group_df, logger)

        analyzer = TrendAnalyzer(
            group_key=group_key,
            group_df=group_df,
            metric_name=metric_name,
            group_column=group_cols_list[0],
            prefix=prefix,
            suffix=suffix,
            keep_cols=keep_cols,
            logger=logger,
            config=analyzer_config,
        )

        if not analyzer.valid:
            logger.debug(f"跳过 {group_key}: {analyzer.error_reason}")
            continue

        trend_vector = analyzer.build_trend_vector()

        if current_config.get('enable_filter'):
            evaluation = rule_evaluator.evaluate(group_key, metric_name, current_config, trend_vector)

            if not evaluation.passes:
                eliminated_count += 1
                continue
        else:
            evaluation = TrendEvaluationResult(
                passes=True,
                elimination_reason="",
                penalty=0.0,
                penalty_details=[],
                bonus_details=[],
                trend_score=100.0,
                auxiliary_notes=[],
            )

        snapshot = analyzer.build_snapshot(evaluation, trend_vector)
        result_row = analyzer.build_result_row(snapshot, current_config.get('enable_filter', False))
        result_collector.add(result_row)

    # ========== 10. 构建输出 DataFrame ==========
    df_result = result_collector.to_dataframe()

    logger.info("\n" + "=" * 80)
    logger.info(f"📊 {metric_name} 趋势分析完成")
    logger.info("=" * 80)
    logger.info(f"输入分组数: {grouped.ngroups}")
    logger.info(f"输出分组数: {len(df_result)}")

    if base_config.get('enable_filter'):
        logger.info(f"过滤淘汰: {eliminated_count} 组")

        # 行业配置使用统计
        usage_stats = config_resolver.usage_stats()
        if usage_stats:
            logger.info(f"\n🏭 行业差异化参数应用:")
            for industry, count in sorted(usage_stats.items(), key=lambda x: -x[1])[:10]:
                ind_config = industry_configs.get(industry, default_config)
                slope_param = ind_config.get('log_severe_decline_slope', ind_config.get('severe_decline_slope', default_config.get('log_severe_decline_slope', -0.30)))
                min_value = ind_config.get('min_latest_value', default_config.get('min_latest_value'))
                logger.info(f"  {industry}: {count}家 (min={min_value}, log_slope={slope_param:.2f})")

    if len(df_result) > 0:
        logger.info("\n📊 v2.0 趋势统计 (Log斜率):")
        logger.info(f"  平均加权值:   {df_result[f'{prefix}{metric_name}_weighted{suffix}'].mean():.2f}")
        logger.info(f"  平均Log斜率:  {df_result[f'{prefix}{metric_name}_log_slope{suffix}'].mean():.4f} (CAGR: {df_result[f'{prefix}{metric_name}_cagr{suffix}'].mean()*100:.1f}%)")
        logger.info(f"  平均线性斜率: {df_result[f'{prefix}{metric_name}_slope{suffix}'].mean():.2f} (对照)")
        logger.info(f"  平均R²:       {df_result[f'{prefix}{metric_name}_r_squared{suffix}'].mean():.2f}")

        score_col = f"{prefix}{metric_name}_trend_score{suffix}"
        if score_col in df_result.columns:
            logger.info(f"  平均趋势评分: {df_result[score_col].mean():.1f}")

        # 改善vs衰退 (使用Log斜率)
        log_slope_col = f"{prefix}{metric_name}_log_slope{suffix}"
        improving = (df_result[log_slope_col] > 0.10).sum()   # CAGR >10%
        declining = (df_result[log_slope_col] < -0.10).sum()  # CAGR <-10%
        stable = len(df_result) - improving - declining

        logger.info(f"\n  改善趋势(斜率>+1): {improving} ({improving/len(df_result)*100:.1f}%)")
        logger.info(f"  稳定趋势(斜率±1):  {stable} ({stable/len(df_result)*100:.1f}%)")
        logger.info(f"  下滑趋势(斜率<-1): {declining} ({declining/len(df_result)*100:.1f}%)")

        # 扣分统计
    if base_config.get('enable_filter'):
            penalty_col = f"{prefix}{metric_name}_penalty{suffix}"
            penalized = (df_result[penalty_col] > 0).sum()
            if penalized > 0:
                logger.info(f"\n  被扣分: {penalized} 组")
                logger.info(f"  平均扣分: {df_result[df_result[penalty_col]>0][penalty_col].mean():.1f}分")

    logger.info("=" * 80)

    return df_result


if __name__ == "__main__":
    # 测试通用趋势分析
    import sys
    sys.path.append('src')

    # 测试: ROIC趋势分析
    print("\n" + "=" * 80)
    print("测试: ROIC趋势分析")
    print("=" * 80)

    df_roic = analyze_metric_trend(
        data='data/polars/5yd_final_industry.csv',
        group_cols='ts_code',
        metric_name='roic',
        prefix='',
        suffix='',
        min_periods=5,
    )

    print("\nROIC趋势分析结果(前10):")
    print(df_roic.head(10))
    print(f"\n输出列: {df_roic.columns.tolist()}")
