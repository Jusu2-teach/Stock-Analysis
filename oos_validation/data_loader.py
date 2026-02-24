"""
OOS Validation — 数据加载模块

从 data/filter_middle/*.csv 重建 aggregated_trends,
避免重复运行趋势探针 (单次探针运行 ~90s，加载 CSV ~2s)。
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Set

import pandas as pd

logger = logging.getLogger("oos_validation")

# 8 个趋势指标 CSV 映射
METRIC_FILES: Dict[str, str] = {
    "roic": "roic_trend_analysis.csv",
    "roe": "roe_trend_analysis.csv",
    "roiic": "roiic_trend_analysis.csv",
    "revenue": "revenue_trend_analysis.csv",
    "profit": "profit_trend_analysis.csv",
    "gross_margin": "gross_margin_trend_analysis.csv",
    "net_margin": "net_margin_trend_analysis.csv",
    "ocf": "ocf_trend_analysis.csv",
}


def load_aggregated_trends(
    base_dir: Path,
    filter_middle_dir: str = "data/filter_middle",
    raw_data_path: str = "data/polars/10yd_final_industry.csv",
) -> Dict[str, pd.DataFrame]:
    """从缓存 CSV 重建 aggregated_trends dict。

    Args:
        base_dir: 项目根目录
        filter_middle_dir: 趋势分析 CSV 目录 (相对路径)
        raw_data_path: 原始财务数据路径 (用于 financial_context)

    Returns:
        Dict[str, pd.DataFrame] — 与 PDDA 聚合输出一致的结构
        包含 8 个趋势指标 + financial_context
    """
    aggregated: Dict[str, pd.DataFrame] = {}
    filter_dir = base_dir / filter_middle_dir

    for metric_name, filename in METRIC_FILES.items():
        csv_path = filter_dir / filename
        if not csv_path.exists():
            raise FileNotFoundError(
                f"趋势分析 CSV 不存在: {csv_path}\n"
                "请先运行完整 pipeline: python -m pipeline run -c workflow/analysis.yaml"
            )
        aggregated[metric_name] = pd.read_csv(csv_path)
        logger.debug(f"  加载 {metric_name}: {len(aggregated[metric_name])} 行")

    # financial_context 未存储为 CSV，需从原始数据重建
    raw_path = base_dir / raw_data_path
    if not raw_path.exists():
        raise FileNotFoundError(f"原始数据不存在: {raw_path}")

    from src.astock.business_engines.trend.engine import build_financial_context

    raw_data = pd.read_csv(raw_path)
    fc_result = build_financial_context(raw_data)
    aggregated["financial_context"] = fc_result.value

    n_companies = len(aggregated["roic"])
    logger.info(
        f"✅ 加载完成: {len(aggregated)} 个指标, {n_companies} 家公司"
    )
    return aggregated


def filter_by_companies(
    aggregated_trends: Dict[str, pd.DataFrame],
    ts_codes: Set[str],
) -> Dict[str, pd.DataFrame]:
    """过滤 aggregated_trends 到指定公司子集。

    用于 Bootstrap 策略: 从全量公司中随机抽样后过滤。
    """
    filtered: Dict[str, pd.DataFrame] = {}
    for key, df in aggregated_trends.items():
        if df is not None and "ts_code" in df.columns:
            filtered[key] = df[df["ts_code"].isin(ts_codes)].reset_index(drop=True)
        else:
            filtered[key] = df
    return filtered


def get_all_ts_codes(aggregated_trends: Dict[str, pd.DataFrame]) -> list:
    """从 aggregated_trends 中提取全部 ts_code (排序)。"""
    all_codes: Set[str] = set()
    for df in aggregated_trends.values():
        if df is not None and "ts_code" in df.columns:
            all_codes.update(df["ts_code"].unique())
    return sorted(all_codes)
