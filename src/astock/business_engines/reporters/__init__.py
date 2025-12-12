"""
报告生成器模块 (Reporters Module)
================================

提供报告生成功能：
- comprehensive_generator.py: 综合报告生成器（规则驱动，有阈值）
- truth_report_generator.py: T.R.U.T.H.报告生成器（数据驱动，无阈值）
- generic_reporter.py: 通用报告器
- engine.py: 报告引擎入口

两套报告系统：
1. 🔵 规则驱动报告 (report_comprehensive) - 基于预设阈值
2. 🟢 T.R.U.T.H.报告 (report_truth) - 纯数据说话，六维基因+三大求解器

作者: AStock Analysis System
日期: 2025-12-06
更新: 2025-01-XX - 添加T.R.U.T.H.报告生成器
"""
from .comprehensive_generator import ComprehensiveReportGenerator
from .truth_report_generator import TruthReportGenerator
from .generic_reporter import GenericReporter

__all__ = ['ComprehensiveReportGenerator', 'TruthReportGenerator', 'GenericReporter']
