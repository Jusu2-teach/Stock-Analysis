"""
趋势分析 DuckDB 引擎
====================

提供注册到 orchestrator 的趋势分析方法。

作者: AStock Analysis System
日期: 2025-12-19
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd

try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False

# orchestrator path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent.parent))
from orchestrator.decorators.register import register_method

from .core import (
    TrendAnalyzer,
    TrendResultCollector,
    ConfigResolver,
    get_default_metric_probes,
)
from .models import (
    TrendAnalyzerConfig,
    TrendSeriesConfig,
    TrendEvaluationResult,
    TrendSnapshot,
)
from .config import get_default_config
from .derivers import (
    find_deriver,
    check_derivable,
    list_available_metrics,
)

logger = logging.getLogger(__name__)


# ============================================================================
# 辅助函数
# ============================================================================

def _snapshot_to_row(
    snapshot: TrendSnapshot,
    group_df: pd.DataFrame,
    keep_cols: List[str],
) -> Dict[str, Any]:
    """
    将 TrendSnapshot 转换为扁平的字典行

    提取核心趋势分析指标，适合输出到 DataFrame。
    """
    row: Dict[str, Any] = {}

    # 基本标识
    row['ts_code'] = snapshot.group_key
    row['metric_name'] = snapshot.metric_name

    # 从 group_df 提取保留列
    if not group_df.empty:
        latest_row = group_df.iloc[-1]
        for col in keep_cols:
            if col in group_df.columns:
                row[col] = latest_row.get(col)

    # 核心趋势指标
    trend = snapshot.trend
    row['slope'] = trend.slope
    row['log_slope'] = trend.log_slope
    row['r_squared'] = trend.r_squared
    row['p_value'] = trend.p_value
    row['cagr_approx'] = trend.cagr_approx
    row['trend_direction'] = "up" if trend.log_slope > 0 else ("down" if trend.log_slope < 0 else "flat")

    # 波动性指标
    vol = snapshot.volatility
    row['cv'] = vol.cv
    row['std_dev'] = vol.std_dev
    row['volatility_type'] = vol.volatility_type
    row['volatility_regime'] = vol.volatility_regime

    # 退化检测
    det = snapshot.deterioration
    row['has_deterioration'] = det.has_deterioration
    row['deterioration_severity'] = det.severity
    row['total_decline_pct'] = det.total_decline_pct

    # 拐点检测
    infl = snapshot.inflection
    row['has_inflection'] = infl.has_inflection
    row['inflection_type'] = infl.inflection_type

    # 周期性
    cyc = snapshot.cyclical
    row['is_cyclical'] = cyc.is_cyclical
    row['current_phase'] = cyc.current_phase
    row['cycle_position'] = cyc.cycle_position

    # 滚动趋势
    roll = snapshot.rolling
    row['is_accelerating'] = roll.is_accelerating
    row['is_decelerating'] = roll.is_decelerating
    row['recent_3y_slope'] = roll.recent_3y_slope

    # 稳健趋势
    robust = snapshot.robust
    row['robust_slope'] = robust.robust_slope
    row['mk_tau'] = robust.mann_kendall_tau
    row['mk_p_value'] = robust.mann_kendall_p_value

    # 加权平均与最新值
    row['weighted_avg'] = snapshot.weighted_avg
    row['latest_value'] = snapshot.latest_value
    row['latest_vs_weighted_ratio'] = snapshot.latest_vs_weighted_ratio

    # 多时间窗口分析
    row['full_data_years'] = snapshot.full_data_years
    row['trend_window_years'] = snapshot.trend_window_years
    row['has_structural_break'] = snapshot.has_structural_break
    row['break_year_index'] = snapshot.break_year_index
    row['data_regime'] = snapshot.data_regime

    return row


# ============================================================================
# 派生指标计算 (使用 derivers.py 的正式派生器系统)
# ============================================================================

@register_method(
    engine_name="compute_derived_metrics",
    component_type="business_engine",
    engine_type="duckdb",
    description="计算派生指标 (如 ROIIC) - 使用派生器框架"
)
def compute_derived_metrics(
    data: Union[str, Path, pd.DataFrame],
    group_cols: str = 'ts_code',
    sort_cols: str = 'end_date',
    metrics: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    使用派生器框架计算派生指标

    支持的派生指标 (可通过 list_available_metrics() 查询):
    - roiic: 增量投入资本回报率 = ΔNOPAT / ΔInvested_Capital

    Args:
        data: 输入数据 (DataFrame 或文件路径)
        group_cols: 分组列 (默认 'ts_code')
        sort_cols: 排序列 (默认 'end_date')
        metrics: 要计算的派生指标列表 (None=自动检测可计算的指标)

    Returns:
        包含派生指标的 DataFrame

    Example:
        >>> df = compute_derived_metrics(
        ...     data="data/financial.csv",
        ...     group_cols="ts_code",
        ...     metrics=["roiic"]
        ... )
    """
    # 1. 加载数据
    if isinstance(data, (str, Path)):
        path = Path(data)
        if not path.exists():
            raise FileNotFoundError(f"数据文件不存在: {path}")
        if path.suffix.lower() == '.parquet':
            df = pd.read_parquet(path)
        else:
            df = pd.read_csv(path)
    elif isinstance(data, pd.DataFrame):
        df = data.copy()
    else:
        raise ValueError(f"不支持的数据类型: {type(data)}")

    logger.info(f"compute_derived_metrics: 加载数据 {len(df)} 行")

    available_cols = set(df.columns)

    # 2. 确定要计算的指标
    if metrics is None:
        # 自动检测可计算的指标
        metrics = []
        for metric_name in list_available_metrics():
            can_derive, missing = check_derivable(metric_name, available_cols)
            if can_derive:
                metrics.append(metric_name)
        logger.info(f"自动检测可计算指标: {metrics}")
    else:
        # 验证用户指定的指标
        for metric_name in metrics:
            can_derive, missing = check_derivable(metric_name, available_cols)
            if not can_derive:
                if missing:
                    raise ValueError(f"指标 {metric_name} 缺少依赖列: {missing}")
                else:
                    raise ValueError(f"不支持的派生指标: {metric_name}")

    if not metrics:
        logger.warning("没有可计算的派生指标")
        return df

    # 3. 使用 DuckDB 执行派生
    if HAS_DUCKDB:
        return _compute_with_duckdb(df, group_cols, sort_cols, metrics)
    else:
        # 回退到 Pandas 实现
        return _compute_with_pandas(df, group_cols, sort_cols, metrics)


def _compute_with_duckdb(
    df: pd.DataFrame,
    group_cols: str,
    sort_cols: str,
    metrics: List[str]
) -> pd.DataFrame:
    """使用 DuckDB 派生器计算派生指标"""
    con = duckdb.connect(':memory:')

    # 注册原始数据
    con.register('source_data', df)
    current_view = 'source_data'

    available_cols = set(df.columns)

    # 依次应用派生器
    for metric_name in metrics:
        deriver = find_deriver(metric_name, available_cols)
        if deriver:
            try:
                new_view = deriver.derive(con, current_view, group_cols)
                current_view = new_view
                # 更新可用列
                available_cols.add(metric_name)
                logger.info(f"✅ 派生指标 {metric_name} 计算完成")
            except Exception as e:
                logger.warning(f"⚠️ 派生指标 {metric_name} 计算失败: {e}")

    # 导出结果
    result = con.execute(f"SELECT * FROM {current_view}").fetchdf()
    con.close()

    return result


def _compute_with_pandas(
    df: pd.DataFrame,
    group_cols: str,
    sort_cols: str,
    metrics: List[str]
) -> pd.DataFrame:
    """使用 Pandas 回退实现计算派生指标"""
    logger.info("DuckDB 不可用，使用 Pandas 实现")

    # 确保排序
    df = df.sort_values([group_cols, sort_cols])

    # 分组计算
    results = []
    for group_key, group_df in df.groupby(group_cols):
        group_df = group_df.copy()

        for metric_name in metrics:
            if metric_name == 'roiic':
                group_df = _pandas_compute_roiic(group_df)

        results.append(group_df)

    return pd.concat(results, ignore_index=True)


def _pandas_compute_roiic(df: pd.DataFrame) -> pd.DataFrame:
    """Pandas 版 ROIIC 计算"""
    df = df.copy()

    roic = df['roic'].values
    ic = df['invest_capital'].values

    # ROIC 可能是百分比形式，统一处理
    roic_decimal = np.where(np.abs(roic) > 1, roic / 100, roic)
    nopat = roic_decimal * ic

    # 计算增量
    delta_nopat = np.diff(nopat, prepend=np.nan)
    delta_ic = np.diff(ic, prepend=np.nan)

    # 计算 ROIIC
    with np.errstate(divide='ignore', invalid='ignore'):
        roiic = np.where(
            np.abs(delta_ic) > 1e-6,
            (delta_nopat / delta_ic) * 100,
            np.nan
        )

    roiic[0] = np.nan
    df['roiic'] = roiic

    return df


# ============================================================================
# 趋势分析方法
# ============================================================================

@register_method(
    engine_name="analyze_metric_trend",
    component_type="business_engine",
    engine_type="duckdb",
    description="通用指标趋势分析"
)
def analyze_metric_trend(
    data: Union[str, Path, pd.DataFrame],
    group_cols: str = 'ts_code',
    metric_name: str = 'roic',
    prefix: str = "",
    suffix: str = "",
    min_periods: int = 5,
    window_size: Optional[int] = None,
    enable_multi_horizon: bool = True,
    reference_metrics: Optional[List[str]] = None,
    analyzer_config: Optional[Dict[str, Any]] = None,
    filter_config: Optional[Dict[str, Any]] = None,
    industry_configs: Optional[Dict[str, Dict[str, Any]]] = None,
    keep_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    通用指标趋势分析

    对输入数据按分组列分组，对每组的指定指标进行趋势分析。

    Args:
        data: 输入数据 (DataFrame, CSV 文件路径, 或 Parquet 文件路径)
        group_cols: 分组列名 (默认 'ts_code')
        metric_name: 分析指标名 (默认 'roic')
        prefix: 输出列名前缀
        suffix: 输出列名后缀
        min_periods: 最小数据期数要求 (默认 5)
        window_size: 趋势计算窗口大小 (None=使用全部数据, N=只用最近N年)
        enable_multi_horizon: 是否启用多时间窗口分析(断点检测+周期分析)
        reference_metrics: 交叉验证参考指标列表
        analyzer_config: 分析器配置字典
        filter_config: 过滤配置字典 (如 min_latest_value)
        industry_configs: 行业差异化配置
        keep_cols: 保留的额外列

    Returns:
        包含趋势分析结果的 DataFrame

    Example:
        >>> result = analyze_metric_trend(
        ...     data="data/financial.csv",
        ...     group_cols="ts_code",
        ...     metric_name="roic",
        ...     min_periods=5,
        ...     window_size=5,
        ...     reference_metrics=["roe", "roiic"]
        ... )
    """
    # 1. 加载数据
    if isinstance(data, (str, Path)):
        path = Path(data)
        if not path.exists():
            raise FileNotFoundError(f"数据文件不存在: {path}")

        if path.suffix.lower() == '.parquet':
            df = pd.read_parquet(path)
        elif path.suffix.lower() in ('.csv', '.svc'):
            df = pd.read_csv(path)
        else:
            raise ValueError(f"不支持的文件格式: {path.suffix}")
    elif isinstance(data, pd.DataFrame):
        df = data.copy()
    else:
        raise ValueError(f"不支持的数据类型: {type(data)}")

    logger.info(f"analyze_metric_trend: 加载数据 {len(df)} 行, 指标={metric_name}")

    # 2. 验证必要列
    if group_cols not in df.columns:
        raise ValueError(f"缺少分组列: {group_cols}")
    if metric_name not in df.columns:
        raise ValueError(f"缺少指标列: {metric_name}")

    # 3. 构建配置
    series_config = TrendSeriesConfig(
        window_size=window_size,
        enable_multi_horizon=enable_multi_horizon,
    )

    config = TrendAnalyzerConfig(
        series=series_config,
        reference_metrics=reference_metrics or [],
    )

    if analyzer_config:
        # 合并用户配置
        for key, value in analyzer_config.items():
            if hasattr(config, key):
                setattr(config, key, value)

    # 4. 默认保留列
    default_keep_cols = ['industry', 'name', 'end_date', 'ann_date']
    keep_cols = list(set((keep_cols or []) + default_keep_cols))
    keep_cols = [c for c in keep_cols if c in df.columns]

    # 5. 配置解析器
    config_resolver = ConfigResolver(industry_configs or {})

    # 6. 结果收集器
    collector = TrendResultCollector()

    # 7. 分组处理
    grouped = df.groupby(group_cols)
    total_groups = len(grouped)
    processed = 0
    skipped = 0
    failed = 0

    for group_key, group_df in grouped:
        # 检查数据量
        if len(group_df) < min_periods:
            skipped += 1
            logger.debug(f"{group_key}: 数据不足 ({len(group_df)} < {min_periods})")
            continue

        try:
            # 解析配置
            resolved_config, industry = config_resolver.resolve(
                str(group_key),
                {},
                group_df,
                logger
            )

            # 创建分析器
            analyzer = TrendAnalyzer(
                group_key=str(group_key),
                group_df=group_df,
                metric_name=metric_name,
                group_column=group_cols,
                prefix=prefix,
                suffix=suffix,
                keep_cols=keep_cols,
                reference_metrics=reference_metrics,
                logger=logger,
                config=config,
            )

            if not analyzer.valid:
                failed += 1
                logger.debug(f"{group_key}: 分析失败 - {analyzer.error_reason}")
                continue

            # 构建趋势向量
            vector = analyzer.build_trend_vector()

            # 创建默认评估结果（趋势分析层不做业务评估）
            default_evaluation = TrendEvaluationResult(
                passes=True,
                elimination_reason="",
                penalty=0.0,
                penalty_details=[],
                bonus_details=[],
                trend_score=0.0,
            )

            # 构建快照并输出
            snapshot = analyzer.build_snapshot(default_evaluation, vector)

            # 将快照转换为扁平行
            row = _snapshot_to_row(snapshot, analyzer.group_df, keep_cols)
            collector.add(row)
            processed += 1

        except Exception as exc:
            failed += 1
            logger.warning(f"{group_key}: 分析异常 - {exc}")
            continue

    # 8. 输出结果
    result_df = collector.to_dataframe()

    logger.info(
        f"analyze_metric_trend 完成: "
        f"total={total_groups}, processed={processed}, skipped={skipped}, failed={failed}"
    )

    return result_df


@register_method(
    engine_name="analyze_multiple_metrics",
    component_type="business_engine",
    engine_type="duckdb",
    description="批量多指标趋势分析"
)
def analyze_multiple_metrics(
    data: Union[str, Path, pd.DataFrame],
    group_cols: str = 'ts_code',
    metrics: Optional[List[str]] = None,
    min_periods: int = 5,
    window_size: Optional[int] = None,
    enable_multi_horizon: bool = True,
    reference_config: Optional[Dict[str, List[str]]] = None,
    keep_cols: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    批量分析多个指标的趋势

    Args:
        data: 输入数据
        group_cols: 分组列
        metrics: 要分析的指标列表 (None=自动检测)
        min_periods: 最小数据期数
        window_size: 趋势计算窗口
        enable_multi_horizon: 是否启用多时间窗口分析
        reference_config: 交叉验证配置 {metric: [ref_metrics]}
        keep_cols: 保留的额外列

    Returns:
        合并的趋势分析结果 DataFrame
    """
    # 加载数据
    if isinstance(data, (str, Path)):
        path = Path(data)
        if path.suffix.lower() == '.parquet':
            df = pd.read_parquet(path)
        else:
            df = pd.read_csv(path)
    else:
        df = data.copy()

    # 自动检测指标
    if metrics is None:
        # 常见财务指标
        candidate_metrics = [
            'roic', 'roe', 'roa', 'roiic',
            'grossprofit_margin', 'netprofit_margin',
            'eps', 'ocfps', 'total_revenue_ps',
        ]
        metrics = [m for m in candidate_metrics if m in df.columns]
        logger.info(f"自动检测到指标: {metrics}")

    if not metrics:
        raise ValueError("没有找到可分析的指标")

    # 逐指标分析并合并
    result_dfs = []
    for metric in metrics:
        ref_metrics = (reference_config or {}).get(metric, [])
        try:
            metric_result = analyze_metric_trend(
                data=df,
                group_cols=group_cols,
                metric_name=metric,
                min_periods=min_periods,
                window_size=window_size,
                enable_multi_horizon=enable_multi_horizon,
                reference_metrics=ref_metrics,
                keep_cols=keep_cols,
            )
            if not metric_result.empty:
                result_dfs.append(metric_result)
        except Exception as exc:
            logger.warning(f"指标 {metric} 分析失败: {exc}")
            continue

    if not result_dfs:
        return pd.DataFrame()

    # 合并结果 (按分组列)
    merged = result_dfs[0]
    for df in result_dfs[1:]:
        # 找出公共列 (分组列 + keep_cols)
        common_cols = [group_cols] + (keep_cols or [])
        common_cols = [c for c in common_cols if c in merged.columns and c in df.columns]

        # 去掉 df 中的公共列再合并
        df_to_merge = df.drop(columns=[c for c in common_cols if c in df.columns and c != group_cols], errors='ignore')
        merged = merged.merge(df_to_merge, on=group_cols, how='outer', suffixes=('', '_dup'))

        # 清理重复列
        dup_cols = [c for c in merged.columns if c.endswith('_dup')]
        merged = merged.drop(columns=dup_cols, errors='ignore')

    return merged


__all__ = [
    'compute_derived_metrics',
    'analyze_metric_trend',
    'analyze_multiple_metrics',
]
