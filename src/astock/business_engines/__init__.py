"""
业务引擎模块（重构版 v4.0）
========================

提供业务分析功能：
- trend: 趋势分析探针（纯数学计算）
- evaluators: 规则评估引擎（29条规则 + 5种策略）
- truth: T.R.U.T.H. 计算引擎（六大基因 + 三大求解器）
- reporters: 报告生成（综合报告 + T.R.U.T.H. 报告）
- analysis: 通用分析 (DuckDB Engine)

架构说明:
    ┌─────────────────┐
    │  trend/engine   │  (8个探针)
    └────────┬────────┘
             │
    ┌────────┴────────┐
    ▼                 ▼
┌──────────┐    ┌──────────┐
│evaluators│    │  truth   │  ← 并行独立
│(规则引擎)│    │(基因求解)│
└────┬─────┘    └────┬─────┘
     │               │
     ▼               ▼
┌──────────┐    ┌──────────┐
│report_   │    │report_   │  ← 各自独立报告
│comprehen.│    │truth     │
└──────────┘    └──────────┘
"""

# 简单的注册表，供Orchestrator使用
business_engine_registry = "business_engines"

__all__ = [
    'business_engine_registry'
]