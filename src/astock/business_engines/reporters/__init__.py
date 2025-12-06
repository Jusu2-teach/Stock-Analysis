"""
报告生成器模块 (Reporters Module)
================================

提供报告生成功能：
- comprehensive_generator.py: 综合报告生成器 (主要使用)
- generic_reporter.py: 通用报告器
- engine.py: 报告引擎入口

作者: AStock Analysis System
日期: 2025-12-06
"""
from .comprehensive_generator import ComprehensiveReportGenerator
from .generic_reporter import GenericReporter

__all__ = ['ComprehensiveReportGenerator', 'GenericReporter']
