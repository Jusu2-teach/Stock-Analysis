"""
财务上下文构建器 (Financial Context Builder)
==========================================

从原始财务数据构建资产结构比率和风险标志，
供下游 TRUTH 六因子分析 (β/δ_fraud) 消费。

设计原则:
- 数据准备/增强步骤，非趋势分析
- 通过 PDDA 聚合到 trends 命名空间
- 自动检测数据模式 (资产负债表 vs 财务指标)

版本: 1.0.0
日期: 2026-02-22
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, Union

import pandas as pd

from orchestrator.decorators.register import register_method
from shared.aggregation import AggregatableResult, AggregationMetadata

logger = logging.getLogger(__name__)


# ============================================================================
# 数据模式检测
# ============================================================================

_BALANCE_SHEET_MARKERS = {"total_assets", "fix_assets", "goodwill", "total_liab"}
_INDICATOR_MARKERS = {"nca_to_assets", "debt_to_assets", "tbassets_to_totalassets", "ar_turn"}


def _clamp(val: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, val))


# ============================================================================
# 指标模式适配器
# ============================================================================

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


# ============================================================================
# 资产负债表模式适配器
# ============================================================================

def _fc_from_balance_sheet(df_sorted: pd.DataFrame) -> pd.DataFrame:
    """从资产负债表数据构建 financial_context DataFrame"""
    from ..trend.probes.financial_context_probe import FinancialContextProbe

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


# ============================================================================
# 注册方法: build_financial_context
# ============================================================================

@register_method(
    engine_name="build_financial_context",
    component_type="business_engine",
    engine_type="duckdb",
    description="从原始财务数据构建资产结构 & 风险标志 (β/δ_fraud 因子数据源)",
)
def build_financial_context(
    data: Union[str, Path, pd.DataFrame],
) -> AggregatableResult[str, pd.DataFrame]:
    """构建 financial_context 数据

    自动检测数据模式 (资产负债表 vs 财务指标)，
    输出标准化的资产结构比率和风险标志 DataFrame。

    通过 PDDA 聚合到 trends 命名空间:
        aggregated_trends["financial_context"] = context_df

    下游 TRUTH 因子 (β, δ_fraud) 在 _build_probes_from_dataframes 时
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
