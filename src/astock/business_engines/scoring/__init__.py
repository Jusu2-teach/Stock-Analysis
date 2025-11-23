"""
质量评分模块（重构版）
====================
"""
from .generic_scorer import GenericQualityScorer
from .engine import score_quality

__all__ = ['GenericQualityScorer', 'score_quality']
