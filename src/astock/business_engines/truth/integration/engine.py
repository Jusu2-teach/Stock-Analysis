"""T.R.U.T.H. 与 orchestrator/pipeline 的集成入口 (v3.2).

提供统一的 ``run_truth`` 方法供 orchestrator 注册使用。
内部实现基于重构后的四层 TRUTH 流水线。

版本: 3.2.0
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional
import logging

import pandas as pd

from orchestrator.decorators.register import register_method

from ..domain import (
    FactorId,
    ProbeInput,
    SolverId,
    TruthProfile,
    TruthSignal,
    TruthGrade,
    TruthRunResult,
)
from ..core import create_pipeline, TruthPipeline
from ..core.feature_registry import is_metadata_column
from ..config import TruthConfig, get_default_config

logger = logging.getLogger(__name__)


def _build_probes_from_dataframes(
    probe_frames: Mapping[str, pd.DataFrame],
) -> Dict[str, List[ProbeInput]]:
    """将上游传入的多 DataFrame 探针结果转换为分组的 ProbeInput 列表.

    Args:
        probe_frames: {probe_name: DataFrame} 探针结果
            包括: roic, roe, revenue, profit, ocf, gross_margin, net_margin, roiic
            以及: financial_context (来自 FinancialContextProbe)
    """
    probes_by_ts: Dict[str, List[ProbeInput]] = {}

    for probe_name, df in probe_frames.items():
        if df is None or df.empty:
            continue
        if "ts_code" not in df.columns:
            continue

        col_index: Dict[str, int] = {c: i for i, c in enumerate(df.columns)}
        ts_idx = col_index.get('ts_code')
        if ts_idx is None:
            continue

        # 只保留数值型特征列，排除元数据列
        feature_cols = [
            c
            for c in df.columns
            if c != "ts_code"
            and pd.api.types.is_numeric_dtype(df[c])
            and not is_metadata_column(c)
        ]

        feature_indices = [(c, col_index[c]) for c in feature_cols if c in col_index]

        for row in df.itertuples(index=False, name=None):
            ts_code = str(row[ts_idx])
            features: Dict[str, float] = {}
            for col, idx in feature_indices:
                val = row[idx]
                if pd.notna(val):
                    features[col] = float(val)
            probe = ProbeInput(
                ts_code=ts_code,
                probe_name=probe_name,
                features=features,
            )
            probes_by_ts.setdefault(ts_code, []).append(probe)

    return probes_by_ts


def _profile_to_dict(profile: TruthProfile) -> Dict[str, Any]:
    """将 TruthProfile 转换为字典格式 (用于 pipeline 输出)"""
    return {
        "ts_code": profile.ts_code,
        "name": profile.name,
        "industry": profile.industry,
        # 因子分数
        "factors": {
            fid.value: {
                "score": fr.score,
                "confidence": fr.confidence,
                "components": dict(fr.components),
            }
            for fid, fr in profile.factors.items()
        },
        # 求解器分数和阈值
        "solvers": {
            sid.value: {
                "score": sr.score,
                "confidence": sr.confidence,
                "thresholds": {
                    name: {
                        "value": th.value,
                        "lower": th.lower_bound,
                        "upper": th.upper_bound,
                        "description": th.description,
                    }
                    for name, th in sr.thresholds.items()
                } if sr.thresholds else {},
            }
            for sid, sr in profile.solvers.items()
        },
        # 综合结果
        "final_score": profile.final_score,
        "signal": profile.signal.value if profile.signal else None,
        "grade": profile.grade.value if profile.grade else None,
        "confidence": profile.confidence,
        "data_quality": profile.data_quality,
        # 警告
        "warnings": [
            {
                "code": w.code,
                "level": w.level.value,
                "title": w.title,
                "message": w.message,
            }
            for w in profile.warnings
        ],
        # 动态阈值汇总 (通过 get_all_thresholds() 方法获取)
        "dynamic_thresholds": {
            name: {
                "value": th.value,
                "lower": th.lower_bound,
                "upper": th.upper_bound,
                "unit": th.unit,
                "description": th.description,
            }
            for name, th in profile.get_all_thresholds().items()
        },
    }


def _run_batch(probes_by_ts: Dict[str, List[ProbeInput]], config: TruthConfig) -> TruthRunResult:
    """批量运行 TRUTH 分析"""
    pipeline = create_pipeline(config)
    profiles = []

    for ts_code, probes in probes_by_ts.items():
        profile = pipeline.process(ts_code, probes)
        profiles.append(profile)

    return TruthRunResult(profiles=tuple(profiles))


@register_method(
    engine_name="run_truth",
    component_type="business_engine",
    engine_type="truth",
    description="Run T.R.U.T.H. pipeline with six factors (α/β/γ/δ_fraud/δ_decay/V) and three solvers (Gravity/Velocity/Structure).",
)
def run_truth(
    aggregated_trends: Dict[str, pd.DataFrame],
) -> Dict[str, Any]:
    """TRUTH 入口: 接收多个 probe DataFrame, 返回批量结果.

    🌟 PDDA 纯净路径: 强制使用 aggregated_trends，无回退

    输入:
        aggregated_trends: PDDA自动聚合的趋势数据字典 {metric_name: DataFrame}
            - 必须包含: roic, roe (核心指标)
            - 可选包含: roiic, gross_margin, net_margin, revenue, profit, ocf
            - 可选包含: financial_context (财务上下文)

    注意:
        financial_context 探针必须包含以下字段:
        - ratio_hard_asset, ratio_nca, ratio_intang_asset (β因子)
        - flag_goodwill_risk, flag_cash_loan_anomaly (δ_fraud因子)
        - ratio_goodwill_to_equity, ratio_receivable_to_revenue

    输出:
        {
            "algo_version": "3.2.0",
            "profiles": [...],
            "summary": {...}
        }
    """
    logger.info(f"✅ PDDA: 使用聚合数据，包含 {len(aggregated_trends)} 个指标: {list(aggregated_trends.keys())}")

    probes_by_ts = _build_probes_from_dataframes(aggregated_trends)
    config = get_default_config()
    result = _run_batch(probes_by_ts, config)

    # 构建输出
    profiles_dict = [_profile_to_dict(p) for p in result.profiles]
    summary = _calculate_summary(list(result.profiles))

    return {
        "algo_version": result.algo_version,
        "universe_size": len(result),
        "factor_count": 6,
        "solver_count": 3,
        "profiles": profiles_dict,
        "summary": summary,
    }


def _calculate_summary(profiles: List[TruthProfile]) -> Dict[str, Any]:
    """计算汇总统计"""
    if not profiles:
        return {}

    # 信号分布
    signal_dist = {}
    for profile in profiles:
        sig = profile.signal.value if profile.signal else "unknown"
        signal_dist[sig] = signal_dist.get(sig, 0) + 1

    # 评级分布
    grade_dist = {}
    for profile in profiles:
        grade = profile.grade.value if profile.grade else "unknown"
        grade_dist[grade] = grade_dist.get(grade, 0) + 1

    # 欺诈预警统计
    fraud_alert_count = sum(1 for p in profiles if p.is_fraud_alert)

    # 平均分数
    scores = [p.final_score for p in profiles if p.final_score is not None]
    avg_score = sum(scores) / len(scores) if scores else 0.0

    return {
        "signal_distribution": signal_dist,
        "grade_distribution": grade_dist,
        "fraud_alert_count": fraud_alert_count,
        "average_score": round(avg_score, 4),
        "top_picks_count": sum(1 for p in profiles if p.grade in (TruthGrade.A_PLUS, TruthGrade.A)),
    }


@register_method(
    engine_name="run_truth_single",
    component_type="business_engine",
    engine_type="truth",
    description="Run T.R.U.T.H. analysis for a single stock.",
)
def run_truth_single(ts_code: str, **probe_frames: pd.DataFrame) -> Dict[str, Any]:
    """单支股票的 TRUTH 分析

    Args:
        ts_code: 目标股票代码
        **probe_frames: 探针 DataFrame (需包含该股票)

    Returns:
        单支股票的完整 TruthProfile 字典
    """
    probes_by_ts = _build_probes_from_dataframes(probe_frames)
    probes = probes_by_ts.get(ts_code, [])

    if not probes:
        return {"error": f"No probes found for {ts_code}"}

    config = get_default_config()
    pipeline = create_pipeline(config)
    profile = pipeline.process(ts_code, probes)

    return _profile_to_dict(profile)


__all__ = ["run_truth", "run_truth_single"]
