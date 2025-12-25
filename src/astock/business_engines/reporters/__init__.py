"""
报告生成器模块 (Reporters Module) v2.2
=====================================

提供报告生成功能：
- comprehensive_generator.py: 综合报告生成器（规则驱动，有阈值）
- truth_report_generator.py: T.R.U.T.H.报告生成器（数据驱动，无阈值）
- engine.py: 报告引擎入口（@register_method 注册点）

两套报告系统：
1. 🔵 规则驱动报告 (report_comprehensive) - 基于预设阈值
2. 🟢 T.R.U.T.H.报告 (report_truth/report_truth_single) - 纯数据说话，六维基因+三大求解器

作者: AStock Analysis System
版本: v2.2 - 移除未使用的 GenericReporter
"""
from .comprehensive_generator import ComprehensiveReportGenerator
from .truth_report_generator import TruthReportGenerator

__all__ = ['ComprehensiveReportGenerator', 'TruthReportGenerator']
