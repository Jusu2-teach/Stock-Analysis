"""
报告生成模块（重构版）
====================

简化后的文件结构：
- generator.py: 趋势分析报告生成器（原 trend_report_generator.py）
"""
from .generator import generate_trend_analysis_report

__all__ = ['generate_trend_analysis_report']
