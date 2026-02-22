"""
趋势分析 DuckDB 引擎
====================

提供注册到 orchestrator 的趋势分析方法。

作者: AStock Analysis System
日期: 2025-12-19
更新: 2025-12-25 - 集成统一命名规范系统
更新: 2026-01-17 - 集成 PDDA 聚合系统
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd

try:
    import duckdb
    HAS_DUCKDB = True
except ImportError:
    HAS_DUCKDB = False

from orchestrator.decorators.register import register_method

# 🌟 统一命名规范系统 (必需依赖)
from shared.naming_convention import MetricRegistry, ColumnBuilder

# 🌟 PDDA 聚合系统 (必需依赖)
from shared.aggregation import AggregatableResult, AggregationMetadata

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
    canonical_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    将 TrendSnapshot 转换为扁平的字典行

    提取核心趋势分析指标，适合输出到 DataFrame。
    所有分析字段使用 {prefix}_{field} 格式，避免合并时列名冲突。

    Args:
        snapshot: 趋势快照对象
        group_df: 分组数据
        keep_cols: 保留的额外列
        canonical_name: 可选的标准业务键名 (用于 metric_name 列)
    """
    row: Dict[str, Any] = {}

    # 基本标识（不加前缀）
    row['ts_code'] = snapshot.group_key
    # 🌟 metric_name 使用 canonical_name (如果提供)，否则使用 snapshot.metric_name
    row['metric_name'] = canonical_name if canonical_name else snapshot.metric_name

    # 指标前缀 (仍使用 snapshot.metric_name 作为列名前缀，保持与原始数据一致)
    prefix = snapshot.metric_name

    # 从 group_df 提取保留列（不加前缀）
    if not group_df.empty:
        latest_row = group_df.iloc[-1]
        for col in keep_cols:
            if col in group_df.columns:
                row[col] = latest_row.get(col)

    # 核心趋势指标
    trend = snapshot.trend
    row[f'{prefix}_slope'] = trend.slope
    row[f'{prefix}_log_slope'] = trend.log_slope
    row[f'{prefix}_r_squared'] = trend.r_squared
    row[f'{prefix}_p_value'] = trend.p_value
    row[f'{prefix}_cagr_approx'] = trend.cagr_approx
    row[f'{prefix}_cagr'] = trend.cagr_approx  # 🌟 标准别名，保持与 FieldRegistry 一致
    row[f'{prefix}_trend_direction'] = "up" if trend.log_slope > 0 else ("down" if trend.log_slope < 0 else "flat")

    # 波动性指标
    vol = snapshot.volatility
    row[f'{prefix}_cv'] = vol.cv
    row[f'{prefix}_std_dev'] = vol.std_dev
    row[f'{prefix}_volatility_type'] = vol.volatility_type
    row[f'{prefix}_volatility_regime'] = vol.volatility_regime

    # 退化检测
    det = snapshot.deterioration
    row[f'{prefix}_has_deterioration'] = det.has_deterioration
    row[f'{prefix}_deterioration_severity'] = det.severity
    row[f'{prefix}_total_decline_pct'] = det.total_decline_pct

    # 拐点检测
    infl = snapshot.inflection
    row[f'{prefix}_has_inflection'] = infl.has_inflection
    row[f'{prefix}_inflection_type'] = infl.inflection_type

    # 周期性
    cyc = snapshot.cyclical
    row[f'{prefix}_is_cyclical'] = cyc.is_cyclical
    row[f'{prefix}_current_phase'] = cyc.current_phase
    row[f'{prefix}_cycle_position'] = cyc.cycle_position

    # 滚动趋势
    roll = snapshot.rolling
    row[f'{prefix}_is_accelerating'] = roll.is_accelerating
    row[f'{prefix}_is_decelerating'] = roll.is_decelerating
    row[f'{prefix}_recent_3y_slope'] = roll.recent_3y_slope

    # 稳健趋势
    robust = snapshot.robust
    row[f'{prefix}_robust_slope'] = robust.robust_slope
    row[f'{prefix}_mk_tau'] = robust.mann_kendall_tau
    row[f'{prefix}_mk_p_value'] = robust.mann_kendall_p_value

    # 加权平均与最新值
    row[f'{prefix}_weighted_avg'] = snapshot.weighted_avg
    row[f'{prefix}_latest_value'] = snapshot.latest_value
    row[f'{prefix}_latest_vs_weighted_ratio'] = snapshot.latest_vs_weighted_ratio

    # 多时间窗口分析
    row[f'{prefix}_full_data_years'] = snapshot.full_data_years
    row[f'{prefix}_trend_window_years'] = snapshot.trend_window_years
    row[f'{prefix}_has_structural_break'] = snapshot.has_structural_break
    row[f'{prefix}_break_year_index'] = snapshot.break_year_index
    row[f'{prefix}_data_regime'] = snapshot.data_regime

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
) -> AggregatableResult[str, pd.DataFrame]:
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

    # =========================================================================
    # 🌟 统一命名规范处理 (核心改进)
    # =========================================================================
    # 支持输入: business_key ('revenue') 或 source_column ('total_revenue_ps')
    # 统一输出: 使用 source_column 读取数据, output_prefix 生成列名

    source_column = metric_name  # 默认: metric_name 就是数据列名
    output_prefix = metric_name  # 默认: metric_name 就是输出前缀
    canonical_name = metric_name  # 默认: metric_name 就是标准名

    # =========================================================================
    # 🌟 统一命名规范处理 (纯净路径，无兼容分支)
    # =========================================================================
    try:
        # 解析为标准指标
        metric_config = MetricRegistry.resolve(metric_name)
        source_column = metric_config.source_column  # 数据列名
        output_prefix = metric_config.output_prefix  # 输出前缀
        canonical_name = metric_config.business_key  # 标准业务键名

        logger.info(
            f"📋 命名规范解析: '{metric_name}' -> "
            f"source='{source_column}', prefix='{output_prefix}', canonical='{canonical_name}'"
        )

        # 验证并给出建议
        if metric_name != canonical_name:
            logger.warning(
                f"⚠️ 建议: 在 YAML 中使用 business_key '{canonical_name}' "
                f"替代 '{metric_name}' 以保持配置统一"
            )
    except ValueError:
        # 未注册的指标，使用原始 metric_name
        logger.info(f"📋 未注册指标 '{metric_name}'，使用原始名称")

    logger.info(f"analyze_metric_trend: 加载数据 {len(df)} 行, 指标={metric_name} (source={source_column})")

    # 2. 验证必要列
    if group_cols not in df.columns:
        raise ValueError(f"缺少分组列: {group_cols}")
    if source_column not in df.columns:
        # 尝试给出有用的错误信息
        available_cols = [c for c in df.columns if not c.startswith('_')]
        raise ValueError(
            f"缺少指标列: '{source_column}' (请求的 metric_name: '{metric_name}')\n"
            f"可用列: {available_cols[:20]}..."
        )

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
            # 🌟 使用 source_column 读取数据，output_prefix 生成列名
            analyzer = TrendAnalyzer(
                group_key=str(group_key),
                group_df=group_df,
                metric_name=source_column,  # 使用 source_column 读取数据列
                group_column=group_cols,
                prefix=prefix,
                suffix=suffix,
                keep_cols=keep_cols,
                reference_metrics=reference_metrics,
                logger=logger,
                config=config,
            )

            # 🌟 覆盖输出前缀 (确保使用统一的 output_prefix)
            # 注意: TrendAnalyzer 内部会使用 metric_name 作为前缀
            # 如果 output_prefix 与 source_column 不同，需要特殊处理
            # 当前设计: output_prefix == source_column，所以无需额外处理

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
            # 🌟 传入 canonical_name，使 metric_name 列记录标准业务键名
            row = _snapshot_to_row(snapshot, analyzer.group_df, keep_cols, canonical_name)
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

    # 🌟 PDDA: 统一返回 AggregatableResult (纯净路径，无回退)
    return AggregatableResult(
        key=canonical_name,  # 使用标准业务键名作为聚合键
        value=result_df,
        metadata=AggregationMetadata(
            producer_method="analyze_metric_trend",
            tags={
                "metric_name": metric_name,
                "canonical_name": canonical_name,
                "total_groups": total_groups,
                "processed": processed,
            }
        )
    )


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


# ============================================================================
# Financial Context 探针 — 资产结构 & 风险标志
# ============================================================================

# 数据模式检测标志列
_BALANCE_SHEET_MARKERS = {"total_assets", "fix_assets", "goodwill", "total_liab"}
_INDICATOR_MARKERS = {"nca_to_assets", "debt_to_assets", "tbassets_to_totalassets", "ar_turn"}


def _clamp(val: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, val))


def _fc_from_indicators(df_sorted: pd.DataFrame) -> pd.DataFrame:
    """从 fina_indicator 指标数据构建 financial_context DataFrame

    指标数据均为百分比 0~100，需 ÷100 转为比率。
    输出每个 ts_code 一行，包含 β/δ_fraud 因子所需的全部特征列。
    """
    rows = []

    for ts_code, group in df_sorted.groupby("ts_code"):
        ts_code = str(ts_code)
        row = group.iloc[0]

        def _get(col: str, default: float = float('nan')) -> float:
            v = row.get(col)
            if v is not None and pd.notna(v):
                try:
                    return float(v)
                except (ValueError, TypeError):
                    pass
            return default

        nca_pct = _get("nca_to_assets")
        ca_pct = _get("ca_to_assets")
        tba_pct = _get("tbassets_to_totalassets")
        debt_pct = _get("debt_to_assets")
        ar_turnover = _get("ar_turn")
        assets_to_eqt = _get("assets_to_eqt")

        features: Dict[str, Any] = {"ts_code": ts_code}

        # ── Beta 因子所需字段 ──
        if not pd.isna(nca_pct):
            ratio_nca = _clamp(nca_pct / 100.0)
            features["ratio_nca"] = ratio_nca
        else:
            ratio_nca = float('nan')

        ratio_intang = 0.0
        if not pd.isna(tba_pct):
            ratio_intang = _clamp(1.0 - tba_pct / 100.0)
            features["ratio_intang_asset"] = ratio_intang

        if not pd.isna(ratio_nca):
            features["ratio_hard_asset"] = _clamp(ratio_nca - ratio_intang)

        if not pd.isna(ca_pct):
            features["ratio_working_capital"] = _clamp(ca_pct / 100.0)

        # ── DeltaFraud 因子所需字段 ──
        if not pd.isna(debt_pct):
            features["ratio_debt_to_assets"] = _clamp(debt_pct / 100.0)

        # ratio_receivable_to_revenue: 仅 ar_turn < 1.0 (极端风险)
        ratio_recv = 0.0
        if not pd.isna(ar_turnover) and ar_turnover > 0 and ar_turnover < 1.0:
            ratio_recv = _clamp(1.0 / max(ar_turnover, 0.5))
            features["ratio_receivable_to_revenue"] = ratio_recv

        # ratio_goodwill_to_equity: 保守推算 (仅 15% 可能是商誉)
        if not pd.isna(assets_to_eqt) and assets_to_eqt > 0:
            features["ratio_goodwill_to_equity"] = _clamp(
                ratio_intang * (assets_to_eqt / 100.0) * 0.15
            )
        elif ratio_intang > 0.5:
            features["ratio_goodwill_to_equity"] = ratio_intang * 0.25

        # ── 风险标志 ──
        features["flag_goodwill_risk"] = 1.0 if ratio_intang > 0.85 else 0.0
        features["flag_cash_loan_anomaly"] = 0.0   # 指标模式下禁用
        features["flag_high_receivable"] = 1.0 if ratio_recv > 1.0 else 0.0

        # ── 额外估值特征 ──
        bps = _get("bps")
        roe_val = _get("roe")
        eps_val = _get("eps")
        fcff = _get("fcff_ps")
        roic_val = _get("roic")

        if not pd.isna(bps) and bps > 0 and not pd.isna(roe_val):
            features["valuation_earnings_power"] = abs(roe_val / 100.0 * bps)
        if not pd.isna(eps_val) and not pd.isna(fcff) and abs(eps_val) > 0.01:
            features["valuation_cash_conversion"] = _clamp(fcff / abs(eps_val), -2.0, 3.0)
        if not pd.isna(roic_val):
            features["valuation_spread"] = roic_val / 100.0 - 0.08

        # 数据完整度 (代理模式上限 0.85)
        expected_fields = 11
        actual_fields = sum(1 for k in features if k.startswith(("ratio_", "flag_")))
        features["data_completeness"] = min(0.85, actual_fields / expected_fields)

        # 保留 name/industry (供下游报告使用)
        for meta_col in ["name", "industry"]:
            v = row.get(meta_col)
            if v is not None and pd.notna(v):
                features[meta_col] = str(v)

        rows.append(features)

    return pd.DataFrame(rows)


def _fc_from_balance_sheet(df_sorted: pd.DataFrame) -> pd.DataFrame:
    """从资产负债表数据构建 financial_context DataFrame"""
    from .probes.financial_context_probe import FinancialContextProbe

    probe = FinancialContextProbe()
    rows = []

    for ts_code, group in df_sorted.groupby("ts_code"):
        ts_code = str(ts_code)
        latest_row = group.iloc[0]

        financial_data: Dict[str, Any] = {}
        for col in latest_row.index:
            if col == "ts_code":
                continue
            val = latest_row[col]
            if pd.notna(val):
                try:
                    financial_data[col] = float(val)
                except (ValueError, TypeError):
                    pass

        if not financial_data:
            continue

        try:
            ctx_result = probe.compute(financial_data)
            features = ctx_result.to_features_dict()
            clean = {
                k: v for k, v in features.items()
                if isinstance(v, (int, float)) and v == v and abs(v) != float('inf')
            }
            if clean:
                clean["ts_code"] = ts_code
                rows.append(clean)
        except Exception as e:
            logger.warning(f"FinancialContextProbe failed for {ts_code}: {e}")

    return pd.DataFrame(rows) if rows else pd.DataFrame()


@register_method(
    engine_name="build_financial_context",
    component_type="business_engine",
    engine_type="duckdb",
    description="从原始财务数据构建资产结构 & 风险标志探针 (β/δ_fraud 因子数据源)",
)
def build_financial_context(
    data: Union[str, Path, pd.DataFrame],
) -> AggregatableResult[str, pd.DataFrame]:
    """构建 financial_context 探针数据

    自动检测数据模式 (资产负债表 vs 财务指标)，
    输出标准化的资产结构比率和风险标志 DataFrame。

    通过 PDDA 聚合到 trends 命名空间:
        aggregated_trends["financial_context"] = context_df

    下游因子 (β, δ_fraud) 在 _build_probes_from_dataframes 时
    自动转换为 ProbeInput(probe_name="financial_context")。

    Args:
        data: 原始财务数据 (pd.DataFrame 或文件路径)

    Returns:
        AggregatableResult(key="financial_context", namespace="trends")
    """
    if isinstance(data, (str, Path)):
        path = Path(data)
        df = pd.read_parquet(path) if path.suffix == '.parquet' else pd.read_csv(path)
    else:
        df = data

    if df is None or df.empty or "ts_code" not in df.columns:
        logger.warning("build_financial_context: 空数据或缺少 ts_code 列")
        return AggregatableResult(
            key="financial_context",
            value=pd.DataFrame(),
            namespace="trends",
            metadata=AggregationMetadata(producer_method="build_financial_context"),
        )

    # 按最新期排序
    sort_cols = [c for c in ["end_date", "ann_date"] if c in df.columns]
    df_sorted = df.sort_values(sort_cols, ascending=False) if sort_cols else df

    # 自动检测数据模式
    data_cols = set(df.columns)
    bs_score = len(data_cols & _BALANCE_SHEET_MARKERS)
    ind_score = len(data_cols & _INDICATOR_MARKERS)

    if ind_score > bs_score:
        logger.info(f"Financial Context: 指标数据模式 (indicator={ind_score} vs balance_sheet={bs_score})")
        context_df = _fc_from_indicators(df_sorted)
    else:
        logger.info(f"Financial Context: 资产负债表模式 (balance_sheet={bs_score} vs indicator={ind_score})")
        context_df = _fc_from_balance_sheet(df_sorted)

    logger.info(f"✅ Financial Context: 为 {len(context_df)} 只股票构建资产结构探针")

    return AggregatableResult(
        key="financial_context",
        value=context_df,
        namespace="trends",
        metadata=AggregationMetadata(
            producer_method="build_financial_context",
            tags={
                "data_mode": "indicator" if ind_score > bs_score else "balance_sheet",
                "stock_count": str(len(context_df)),
            }
        ),
    )


__all__ = [
    'compute_derived_metrics',
    'analyze_metric_trend',
    'analyze_multiple_metrics',
    'build_financial_context',
]
