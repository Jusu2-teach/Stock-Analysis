"""
评分器模块 (Scorers Module)
==========================

提供通用质量评分功能：
- GenericQualityScorer: 通用质量评分器
- score_quality: 质量评分函数（已注册到Registry）

作者: AStock Analysis System
日期: 2025-12-06
"""
from .generic_scorer import GenericQualityScorer
from .engine import score_quality

__all__ = ['GenericQualityScorer', 'score_quality']
