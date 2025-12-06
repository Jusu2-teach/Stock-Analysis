"""
报告生成器模块 (Reporters Module)
================================

提供多种报告生成功能：
- generator.py: 趋势分析报告生成器
- comprehensive_generator.py: 综合报告生成器
- generic_reporter.py: 通用报告器

作者: AStock Analysis System
日期: 2025-12-06
"""
from .generator import generate_trend_analysis_report

__all__ = ['generate_trend_analysis_report']
