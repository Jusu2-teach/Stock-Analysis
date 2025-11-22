"""
质量评分模块（重构版）
====================

简化后的文件结构：
- quality.py: 质量评分（原 quality_score.py）
"""
from .quality import calculate_quality_score, generate_quality_report

__all__ = ['calculate_quality_score', 'generate_quality_report']
