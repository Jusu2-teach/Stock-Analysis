"""Evaluators Engine Entry Point (v1.0).

提供 orchestrator 注册入口，将探针数据转换为 TrendContext 并执行规则评估。

数据流:
    trend/engine (8个探针)
        ↓ aggregated_trends
    evaluators/engine (本模块)
        ↓ 评估结果
    reporters/comprehensive_generator

架构原则:
    - evaluators 与 truth 是并行独立的两个组件
    - 统一从 trend 接收探针数据
    - 各自独立产出报告

版本: 1.0.0
更新: 2026-01-22 - 初始创建，连接 orchestrator 与 RuleEngine
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from orchestrator.decorators.register import register_method

from .threshold import (
    RuleEngine,
    TrendEvaluator,
    EvaluationResultImpl,
    StrategyResultImpl,
)
from .threshold.domain_models import (
    TrendContext,
    TrendMetrics,
    VolatilityMetrics,
    DeteriorationMetrics,
    InflectionMetrics,
    CyclicalMetrics,
    DataQualityMetrics,
    ReferenceMetric,
    TrendDirection,
    VolatilityRegime,
    CyclePhase,
    DeteriorationSeverity,
)
from .threshold.strategies import (
    HighGrowthStrategy,
    TurnaroundStrategy,
    StableDividendStrategy,
    CyclicalBottomStrategy,
    MoatDefenseStrategy,
)
from .threshold.industry_config import (
    INDUSTRY_CATEGORY_MAP,
    get_industry_category,
    get_category_thresholds,
)

logger = logging.getLogger(__name__)


# ============================================================================
# TrendContext 构建器
# ============================================================================

class TrendContextBuilder:
    """从探针 DataFrame 行构建 TrendContext.

    职责:
        - 解析探针输出的扁平列 → 结构化 TrendContext
        - 处理缺失值和默认值
        - 支持行业自适应阈值

    探针输出列名约定 (来自 trend/engine.py):
        {metric}_slope, {metric}_log_slope, {metric}_r_squared, ...
    """

    def __init__(
        self,
        metric_name: str,
        row: Any,
        column_index: Optional[Dict[str, int]] = None,
        prefix: Optional[str] = None,
    ):
        """
        Args:
            metric_name: 指标名称 (如 'roic', 'roe')
            row: 探针输出的 DataFrame 行
            column_index: 可选，row 为 tuple 时的列名→位置索引
            prefix: 列名前缀 (默认使用 metric_name)
        """
        self._metric_name = metric_name
        self._row = row
        self._column_index = column_index
        self._prefix = prefix or metric_name

    def _get(self, field: str, default: Any = 0.0) -> Any:
        """安全获取列值"""
        col = f"{self._prefix}_{field}"
        return self._get_raw(col, default)

    def _get_raw(self, col_name: str, default: Any = None) -> Any:
        """直接获取列值 (无前缀)"""
        if self._column_index is not None:
            idx = self._column_index.get(col_name)
            if idx is None:
                return default
            val = self._row[idx]
            if pd.isna(val):
                return default
            return val

        # Fallback: pandas.Series-like
        if hasattr(self._row, 'index') and col_name in self._row.index:
            val = self._row[col_name]
            if pd.isna(val):
                return default
            return val
        return default

    def build_trend_metrics(self) -> TrendMetrics:
        """构建趋势指标"""
        return TrendMetrics(
            log_slope=float(self._get("log_slope", 0.0)),
            linear_slope=float(self._get("slope", 0.0)),
            r_squared=float(self._get("r_squared", 0.0)),
            cagr_approx=float(self._get("cagr", 0.0)),
            robust_slope=float(self._get("theilsen_slope", 0.0)),
            recent_3y_slope=float(self._get("recent_3y_slope", 0.0)),
            wls_slope=self._get("wls_slope") if self._get("wls_slope") else None,
            mann_kendall_tau=float(self._get("mk_tau", 0.0)),
            mann_kendall_p_value=float(self._get("mk_p_value", 1.0)),
            trend_acceleration=float(self._get("trend_acceleration", 0.0)),
            is_accelerating=bool(self._get("is_accelerating", False)),
            is_decelerating=bool(self._get("is_decelerating", False)),
        )

    def build_volatility_metrics(self) -> VolatilityMetrics:
        """构建波动性指标"""
        regime_str = str(self._get("volatility_regime", "stable"))
        try:
            regime = VolatilityRegime(regime_str)
        except ValueError:
            regime = VolatilityRegime.STABLE

        return VolatilityMetrics(
            cv=float(self._get("cv", 0.0)),
            std_dev=float(self._get("std_dev", 0.0)),
            detrended_cv=float(self._get("detrended_cv", 0.0)),
            volatility_regime=regime,
            volatility_change_ratio=float(self._get("volatility_change_ratio", 1.0)),
        )

    def build_deterioration_metrics(self) -> DeteriorationMetrics:
        """构建恶化检测指标"""
        severity_str = str(self._get("deterioration_severity", "none"))
        try:
            severity = DeteriorationSeverity(severity_str)
        except ValueError:
            severity = DeteriorationSeverity.NONE

        return DeteriorationMetrics(
            has_deterioration=bool(self._get("has_deterioration", False)),
            severity=severity,
            total_decline_pct=float(self._get("total_decline_pct", 0.0)),
            consecutive_decline_years=int(self._get("consecutive_decline_years", 0)),
            deterioration_probability=float(self._get("deterioration_probability", 0.0)),
            deterioration_pattern=str(self._get("deterioration_pattern", "none")),
            deterioration_acceleration=float(self._get("deterioration_acceleration", 0.0)),
        )

    def build_inflection_metrics(self) -> InflectionMetrics:
        """构建拐点检测指标"""
        return InflectionMetrics(
            has_inflection=bool(self._get("has_break", False)),
            inflection_type=str(self._get("inflection_type", "none")),
            slope_change=float(self._get("slope_change", 0.0)),
            confidence=float(self._get("inflection_confidence", 0.0)),
        )

    def build_cyclical_metrics(self) -> CyclicalMetrics:
        """构建周期性指标"""
        phase_str = str(self._get("cycle_phase", "unknown"))
        try:
            phase = CyclePhase(phase_str)
        except ValueError:
            phase = CyclePhase.UNKNOWN

        return CyclicalMetrics(
            is_cyclical=bool(self._get("is_cyclical", False)),
            current_phase=phase,
            peak_to_trough_ratio=float(self._get("peak_valley_ratio", 1.0)),
            fft_dominant_period=float(self._get("fft_dominant_period", 0.0)),
            cyclical_confidence=float(self._get("cyclical_confidence", 0.0)),
        )

    def build_quality_metrics(self) -> DataQualityMetrics:
        """构建数据质量指标"""
        return DataQualityMetrics(
            has_loss_years=bool(self._get("has_loss_years", False)),
            loss_year_count=int(self._get("loss_year_count", 0)),
            has_near_zero_years=bool(self._get("has_near_zero_years", False)),
            near_zero_count=int(self._get("near_zero_count", 0)),
            latest_value=float(self._get("latest", 0.0)),
            weighted_avg=float(self._get("weighted", 0.0)),
        )

    def build(self) -> TrendContext:
        """构建完整的 TrendContext"""
        ts_code = str(self._get_raw("ts_code", "unknown"))

        return TrendContext(
            ts_code=ts_code,
            metric_name=self._metric_name,
            trend=self.build_trend_metrics(),
            volatility=self.build_volatility_metrics(),
            deterioration=self.build_deterioration_metrics(),
            inflection=self.build_inflection_metrics(),
            cyclical=self.build_cyclical_metrics(),
            quality=self.build_quality_metrics(),
            reference_metrics={},  # 交叉验证在后续添加
        )


# ============================================================================
# 批量评估结果
# ============================================================================

def _build_contexts_from_dataframes(
    aggregated_trends: Dict[str, pd.DataFrame],
) -> Dict[str, List[TrendContext]]:
    """将聚合的探针数据转换为按 ts_code 分组的 TrendContext 列表.

    Args:
        aggregated_trends: {metric_name: DataFrame or AggregatableResult} 探针结果

    Returns:
        {ts_code: [TrendContext, ...]} 按股票分组的上下文
    """
    from shared.aggregation import AggregatableResult

    contexts_by_ts: Dict[str, List[TrendContext]] = {}

    for metric_name, data in aggregated_trends.items():
        # 支持 AggregatableResult 和直接 DataFrame
        if isinstance(data, AggregatableResult):
            df = data.value
        else:
            df = data

        if df is None or (hasattr(df, 'empty') and df.empty):
            continue
        if "ts_code" not in df.columns:
            logger.warning(f"探针 {metric_name} 缺少 ts_code 列，跳过")
            continue

        col_index: Dict[str, int] = {c: i for i, c in enumerate(df.columns)}
        ts_idx = col_index.get('ts_code')
        if ts_idx is None:
            continue

        for row in df.itertuples(index=False, name=None):
            ts_code = str(row[ts_idx])
            builder = TrendContextBuilder(metric_name, row, col_index)
            context = builder.build()

            contexts_by_ts.setdefault(ts_code, []).append(context)

    return contexts_by_ts


def _evaluate_company(
    ts_code: str,
    contexts: List[TrendContext],
    rule_engine: RuleEngine,
    strategies: List[Any],
) -> Dict[str, Any]:
    """评估单个公司.

    Args:
        ts_code: 股票代码
        contexts: 该公司的所有指标 TrendContext
        rule_engine: 规则引擎
        strategies: 策略列表

    Returns:
        公司评估结果字典
    """
    # 按指标存储评估结果
    metric_results: Dict[str, EvaluationResultImpl] = {}
    metric_strategies: Dict[str, List[str]] = {}

    # 对每个指标执行规则评估
    for ctx in contexts:
        eval_result = rule_engine.evaluate(ctx)
        metric_results[ctx.metric_name] = eval_result

        # 执行策略评估
        matched_strategies = []
        for strategy in strategies:
            try:
                result = strategy.evaluate(ctx)
                if result and result.matched:
                    matched_strategies.append(result.name)
            except Exception as e:
                logger.debug(f"策略 {strategy.name} 评估异常: {e}")

        metric_strategies[ctx.metric_name] = matched_strategies

    # 计算综合评分
    total_score = 0.0
    total_weight = 0.0
    passes = True
    elimination_reasons = []
    all_strategies = set()

    # 核心指标权重
    weights = {
        "roic": 1.5,
        "roe": 1.2,
        "roiic": 0.8,
        "gross_margin": 1.0,
        "net_margin": 0.8,
        "revenue": 1.0,
        "profit": 1.0,
        "ocf": 0.8,
    }

    for metric_name, result in metric_results.items():
        weight = weights.get(metric_name, 1.0)
        total_score += result.score * weight
        total_weight += weight

        if not result.passes:
            passes = False
            if result.elimination_reason:
                elimination_reasons.append(f"{metric_name}: {result.elimination_reason}")

        all_strategies.update(metric_strategies.get(metric_name, []))

    composite_score = total_score / total_weight if total_weight > 0 else 0.0

    # 计算评级
    if composite_score >= 90:
        grade = "A"
    elif composite_score >= 80:
        grade = "B"
    elif composite_score >= 70:
        grade = "C"
    elif composite_score >= 60:
        grade = "D"
    else:
        grade = "F"

    return {
        "ts_code": ts_code,
        "passes": passes,
        "composite_score": round(composite_score, 2),
        "grade": grade,
        "elimination_reasons": elimination_reasons,
        "matched_strategies": list(all_strategies),
        "metric_results": {
            name: result.to_dict() for name, result in metric_results.items()
        },
    }


# ============================================================================
# Orchestrator 注册入口
# ============================================================================

@register_method(
    engine_name="run_evaluator",
    component_type="business_engine",
    engine_type="evaluator",
    description="Run rule-based evaluation on probe results (29 rules + 5 strategies)",
)
def run_evaluator(
    aggregated_trends: Dict[str, pd.DataFrame],
) -> Dict[str, Any]:
    """规则评估引擎入口: 对探针结果执行规则评估.

    🌟 PDDA 纯净路径: 强制使用 aggregated_trends

    数据流:
        trend/engine (8个探针) → run_evaluator → 评估结果 → reporters/comprehensive

    输入:
        aggregated_trends: PDDA自动聚合的趋势数据字典 {metric_name: DataFrame}
            - 必须包含: roic, roe (核心指标)
            - 可选包含: roiic, gross_margin, net_margin, revenue, profit, ocf

    输出:
        {
            "algo_version": "1.0.0",
            "universe_size": N,
            "rule_count": 29,
            "strategy_count": 5,
            "evaluations": [...],
            "summary": {...}
        }
    """
    logger.info(
        f"✅ Evaluator: 接收 {len(aggregated_trends)} 个指标: {list(aggregated_trends.keys())}"
    )

    # 构建上下文
    contexts_by_ts = _build_contexts_from_dataframes(aggregated_trends)
    logger.info(f"✅ Evaluator: 构建 {len(contexts_by_ts)} 家公司的 TrendContext")

    # 初始化规则引擎和策略
    rule_engine = RuleEngine()
    strategies = [
        HighGrowthStrategy(),
        TurnaroundStrategy(),
        StableDividendStrategy(),
        CyclicalBottomStrategy(),
        MoatDefenseStrategy(),
    ]

    # 批量评估
    evaluations = []
    for ts_code, contexts in contexts_by_ts.items():
        result = _evaluate_company(ts_code, contexts, rule_engine, strategies)
        evaluations.append(result)

    # 计算汇总统计
    summary = _calculate_summary(evaluations)

    rule_stats = rule_engine.get_rule_statistics()

    return {
        "algo_version": "1.0.0",
        "universe_size": len(evaluations),
        "rule_count": rule_stats.get("total_rules", 29),
        "strategy_count": len(strategies),
        "evaluations": evaluations,
        "summary": summary,
    }


def _calculate_summary(evaluations: List[Dict[str, Any]]) -> Dict[str, Any]:
    """计算汇总统计"""
    if not evaluations:
        return {}

    # 评级分布
    grade_dist = {}
    for e in evaluations:
        grade = e.get("grade", "F")
        grade_dist[grade] = grade_dist.get(grade, 0) + 1

    # 通过/淘汰统计
    pass_count = sum(1 for e in evaluations if e.get("passes", False))
    fail_count = len(evaluations) - pass_count

    # 策略命中统计
    strategy_dist = {}
    for e in evaluations:
        for s in e.get("matched_strategies", []):
            strategy_dist[s] = strategy_dist.get(s, 0) + 1

    # 平均分数
    scores = [e.get("composite_score", 0) for e in evaluations]
    avg_score = sum(scores) / len(scores) if scores else 0.0

    return {
        "grade_distribution": grade_dist,
        "pass_count": pass_count,
        "fail_count": fail_count,
        "strategy_distribution": strategy_dist,
        "average_score": round(avg_score, 2),
        "top_picks_count": grade_dist.get("A", 0) + grade_dist.get("B", 0),
    }


@register_method(
    engine_name="run_evaluator_single",
    component_type="business_engine",
    engine_type="evaluator",
    description="Run rule-based evaluation for a single stock",
)
def run_evaluator_single(
    ts_code: str,
    **probe_frames: pd.DataFrame,
) -> Dict[str, Any]:
    """单支股票的规则评估.

    Args:
        ts_code: 目标股票代码
        **probe_frames: 探针 DataFrame (需包含该股票)

    Returns:
        单支股票的评估结果
    """
    contexts_by_ts = _build_contexts_from_dataframes(probe_frames)
    contexts = contexts_by_ts.get(ts_code, [])

    if not contexts:
        return {"error": f"No contexts found for {ts_code}"}

    rule_engine = RuleEngine()
    strategies = [
        HighGrowthStrategy(),
        TurnaroundStrategy(),
        StableDividendStrategy(),
        CyclicalBottomStrategy(),
        MoatDefenseStrategy(),
    ]

    return _evaluate_company(ts_code, contexts, rule_engine, strategies)


__all__ = ["run_evaluator", "run_evaluator_single"]
