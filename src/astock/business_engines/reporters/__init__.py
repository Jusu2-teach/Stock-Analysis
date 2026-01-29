"""
报告生成器模块 (Reporters Module) v2.2
=====================================

提供报告生成功能：
- comprehensive_generator.py: 综合报告生成器（规则驱动，有阈值）
- engine.py: 报告引擎入口（@register_method 注册点，包含 TRUTH 报告）

作者: AStock Analysis System
版本: v2.2 - 移除未使用的 GenericReporter
"""
from .comprehensive_generator import ComprehensiveReportGenerator

__all__ = [
	"ComprehensiveReportGenerator",
]
