"""
趋势组件与配置解析
==================

封装行业阈值解析、使用次数统计以及规则执行入口。通过 `ConfigResolver`
和 `trend_rule_engine` 把行业差异化配置与趋势向量绑定，为评分阶段提供
统一的上下文。
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import pandas as pd

from .config import INDUSTRY_FILTER_CONFIGS
from .trend_models import (
    TrendContext,
    TrendEvaluationResult,
    TrendRuleConfig,
    TrendVector,
    TrendThresholds,
)
from .trend_rules import trend_rule_engine


class ConfigResolver:
    """Resolve per-group configuration with industry overrides and usage tracking."""

    def __init__(self, industry_configs: Dict[str, Dict]) -> None:
        self.industry_configs = industry_configs
        self._usage_counter: Dict[str, int] = defaultdict(int)

    def resolve(
        self,
        group_key: str,
        base_config: Dict,
        group_df: pd.DataFrame,
        logger: logging.Logger,
    ) -> Tuple[Dict, Optional[str]]:
        resolved = dict(base_config)
        industry: Optional[str] = None

        if resolved.get("enable_filter") and "industry" in group_df.columns and not group_df["industry"].empty:
            industry = group_df["industry"].iloc[0]
            if industry in self.industry_configs:
                resolved.update(self.industry_configs[industry])
                self._usage_counter[industry] += 1
                logger.debug(
                    "%s(%s): 使用行业专属参数 min=%s",
                    group_key,
                    industry,
                    resolved.get("min_latest_value"),
                )

        return resolved, industry

    def usage_stats(self) -> Dict[str, int]:
        return dict(self._usage_counter)


class TrendRuleEvaluator:
    """Encapsulate rule-engine based filtering and scoring."""

    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger

    def evaluate(
        self,
        group_key: str,
        metric_name: str,
        current_config: Dict,
        trend_vector: TrendVector,
    ) -> TrendEvaluationResult:
        rule_config = TrendRuleConfig.from_dict(current_config)
        thresholds = rule_config.thresholds

        if trend_vector.is_cyclical and trend_vector.current_phase == "trough":
            thresholds = TrendThresholds(
                min_latest_value=thresholds.min_latest_value,
                severe_decline=thresholds.severe_decline * 1.5,
                mild_decline=thresholds.mild_decline,
                latest_threshold=thresholds.latest_threshold,
                trend_significance=thresholds.trend_significance,
            )
            self.logger.debug(
                "🔄 周期性调整: %s - 底部阶段,放宽衰退阈值至%.3f",
                group_key,
                thresholds.severe_decline,
            )

        params = rule_config.parameters

        trend_context = TrendContext.from_vector(group_key, metric_name, trend_vector)

        outcome = trend_rule_engine.run(trend_context, params, thresholds, self.logger)

        if not outcome.passes:
            return TrendEvaluationResult(
                passes=False,
                elimination_reason=outcome.elimination_reason,
                penalty=outcome.penalty,
                penalty_details=outcome.penalty_details,
                bonus_details=outcome.bonus_details,
                trend_score=0.0,
                auxiliary_notes=outcome.auxiliary_notes,
            )

        penalty = outcome.penalty
        penalty_details = outcome.penalty_details
        bonus_details = outcome.bonus_details

        max_penalty_threshold = float(params.max_penalty)
        if penalty >= max_penalty_threshold:
            reason = f"累积罚分{penalty:.1f}>={max_penalty_threshold}阈值"
            self.logger.info("❌ 【累积淘汰】%s: 总罚分%.1f", group_key, penalty)
            if penalty_details:
                self.logger.info("   扣分项: %s", "; ".join(penalty_details))
            if bonus_details:
                self.logger.info("   加分项: %s", "; ".join(bonus_details))
            return TrendEvaluationResult(
                passes=False,
                elimination_reason=reason,
                penalty=penalty,
                penalty_details=penalty_details,
                bonus_details=bonus_details,
                trend_score=0.0,
                auxiliary_notes=outcome.auxiliary_notes,
            )

        if penalty > 0 or bonus_details:
            self.logger.debug("✅ 【通过】%s: 罚分%.1f", group_key, penalty)
            if penalty_details:
                self.logger.debug("   扣分项: %s", "; ".join(penalty_details))
            if bonus_details:
                self.logger.debug("   加分项: %s", "; ".join(bonus_details))

        if outcome.auxiliary_notes:
            self.logger.debug("ℹ️ 【ROIIC辅助】%s: %s", group_key, "; ".join(outcome.auxiliary_notes))

        if max_penalty_threshold <= 0:
            trend_score = 0.0
        else:
            trend_score = 100.0 - (penalty / max_penalty_threshold) * 100.0
            trend_score = max(0.0, min(100.0, round(trend_score, 2)))

        return TrendEvaluationResult(
            passes=True,
            elimination_reason="",
            penalty=penalty,
            penalty_details=penalty_details,
            bonus_details=bonus_details,
            trend_score=trend_score,
            auxiliary_notes=outcome.auxiliary_notes,
        )


class TrendResultCollector:
    """Collect per-group trend outputs and provide summary helpers."""

    def __init__(self) -> None:
        self._rows: List[Dict] = []

    def add(self, row: Dict) -> None:
        self._rows.append(row)

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(self._rows)
