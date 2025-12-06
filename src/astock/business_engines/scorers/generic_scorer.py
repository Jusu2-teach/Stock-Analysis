from __future__ import annotations
from typing import Any, Dict, Optional, Union
import pandas as pd
import numpy as np
from ..core.interfaces import IScorer, AnalysisResult, ScoreResult

class GenericQualityScorer(IScorer):
    """
    A generic scorer that evaluates quality based on:
    1. Value (Weighted Average)
    2. Trend (Slope/Score)
    3. Momentum (Latest Value)
    4. Stability (R-Squared)
    """

    def score(self, result: Union[AnalysisResult, pd.DataFrame], config: Dict[str, Any] = None) -> ScoreResult:
        config = config or {}

        if isinstance(result, pd.DataFrame):
            df = result.copy()
            metric = config.get('metric_name')
            metadata = {}
            if not metric:
                # Try to infer from columns
                for col in df.columns:
                    if col.endswith('_weighted_trend'):
                         metric = col.replace('_weighted_trend', '')
                         metadata['suffix'] = '_trend'
                         break
                    elif col.endswith('_weighted'):
                         metric = col.replace('_weighted', '')
                         metadata['suffix'] = ''
                         break

            if not metric:
                metric = 'roic' # Default fallback
        else:
            df = result.data.copy()
            metric = result.metric_name
            metadata = result.metadata or {}

        # Check metadata for prefix/suffix
        prefix = metadata.get('prefix', '')
        suffix = metadata.get('suffix', '_trend') # Default in analyzer is '_trend'

        # Resolve column names dynamically
        col_weighted = f"{prefix}{metric}_weighted{suffix}"
        col_trend_score = f"{prefix}{metric}_trend_score{suffix}"
        col_latest = f"{prefix}{metric}_latest{suffix}"
        col_r2 = f"{prefix}{metric}_r_squared{suffix}"
        col_penalty = f"{prefix}{metric}_penalty{suffix}"

        # Default weights
        weights = config.get('weights', {
            'value': 40,
            'trend': 35,
            'momentum': 15,
            'stability': 10
        })

        # 1. Value Score (Weighted Avg)
        # Config should provide thresholds: e.g. {30: 40, 25: 35, ...}
        value_thresholds = config.get('value_thresholds', {
            30: 40, 25: 35, 20: 30, 15: 25, 12: 20, 10: 15, 8: 10, 6: 5
        })
        df['score_value'] = df[col_weighted].apply(lambda x: self._apply_thresholds(x, value_thresholds))

        # 2. Trend Score
        # Assuming trend score is already 0-100, scale it to component weight
        trend_weight = weights.get('trend', 35)
        if col_trend_score in df.columns:
            df['score_trend'] = (df[col_trend_score].clip(0, 100) / 100.0 * trend_weight).round(2)
        else:
            df['score_trend'] = 0

        # 3. Momentum Score (Latest)
        momentum_thresholds = config.get('momentum_thresholds', {
            25: 15, 20: 12, 15: 10, 12: 8, 10: 6, 8: 4
        })
        df['score_momentum'] = df[col_latest].apply(lambda x: self._apply_thresholds(x, momentum_thresholds))

        # 4. Stability Score (R2)
        stability_thresholds = config.get('stability_thresholds', {
            0.8: 10, 0.6: 7, 0.4: 5, 0.2: 3
        })
        df['score_stability'] = df[col_r2].apply(lambda x: self._apply_thresholds(x, stability_thresholds))

        # 5. Base Score
        df['base_score'] = (
            df['score_value'] +
            df['score_trend'] +
            df['score_momentum'] +
            df['score_stability']
        )

        # 6. Penalties
        # Use the penalty calculated by the analyzer if available
        if col_penalty in df.columns:
            # Map analyzer penalty to score penalty
            # This logic can be customized via config
            df['final_penalty'] = df.apply(
                lambda row: self._calculate_penalty(row, col_penalty, col_weighted, col_latest, config),
                axis=1
            )
        else:
            df['final_penalty'] = 0

        # 7. Final Score
        df['quality_score'] = (df['base_score'] - df['final_penalty']).clip(0, 100)

        # 8. Grade
        df['grade'] = df['quality_score'].apply(self._assign_grade)

        # 9. Recommendation & Risk Label (Simplified for generic)
        df['recommendation'] = df.apply(lambda row: self._assign_recommendation(row, col_trend_score), axis=1)

        return ScoreResult(
            data=df,
            score_col='quality_score',
            grade_col='grade',
            metadata={'config': config}
        )

    def _apply_thresholds(self, value: float, thresholds: Dict[float, float]) -> float:
        # Sort thresholds descending
        sorted_thresholds = sorted(thresholds.items(), key=lambda x: x[0], reverse=True)
        for thresh, score in sorted_thresholds:
            if value >= thresh:
                return score
        return 0

    def _calculate_penalty(self, row, col_penalty, col_weighted, col_latest, config):
        penalty = 0
        analyzer_penalty = row.get(col_penalty, 0)

        # Map analyzer penalty points to score deduction
        # Example: High penalty from analyzer -> High deduction
        if analyzer_penalty >= 15: penalty += 12
        elif analyzer_penalty >= 10: penalty += 8
        elif analyzer_penalty >= 5: penalty += 4

        # Additional generic penalties
        val = row.get(col_weighted, 0)
        latest = row.get(col_latest, 0)

        min_val = config.get('min_value_threshold', 8)
        min_latest = config.get('min_latest_threshold', 6)

        if val < min_val: penalty += 10
        if latest < min_latest: penalty += 8

        return penalty

    def _assign_grade(self, score):
        if score >= 90: return 'S'
        elif score >= 80: return 'A'
        elif score >= 70: return 'B'
        elif score >= 60: return 'C'
        elif score >= 50: return 'D'
        else: return 'F'

    def _assign_recommendation(self, row, col_trend):
        grade = row['grade']
        trend = row.get(col_trend, 0)

        if grade == 'S' and trend >= 80: return '⭐⭐⭐ 强烈推荐'
        if grade in ['S', 'A'] and trend >= 60: return '⭐⭐ 推荐买入'
        if grade in ['A', 'B'] and trend >= 40: return '⭐ 可以关注'
        if grade in ['B', 'C']: return '⚠️ 谨慎观察'
        return '❌ 规避风险'
