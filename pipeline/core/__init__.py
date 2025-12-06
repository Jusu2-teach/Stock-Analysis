"""Pipeline Core Module - 核心组件导出

提供 Pipeline 系统的核心抽象和服务层组件。

Public API:
- PipelineContext: 执行上下文（状态共享）
- StepSpec, StepOutput: 步骤配置数据类
- DependencyGraph: 专业级依赖图管理
- ExecutionPlan: 层次化执行计划
- ExecuteManager: Pipeline 执行管理器

Services (高级用户):
- ConfigService: 配置解析
- FlowExecutor: 流程执行
- HookManager: 事件钩子
"""
from __future__ import annotations

# Context & Data Classes
from .context import (
    PipelineContext,
    StepSpec,
    StepOutput,
)

# Dependency Management
from .dependency_graph import (
    DependencyGraph,
    DependencyType,
    DependencyEdge,
    DependencySource,
    DataDependencySource,
    ExplicitDependencySource,
    ExecutionPlan,
    ExecutionLayer,
    CyclicDependencyError,
    MissingDependencyError,
)

# Manager
from .execute_manager import ExecuteManager

__all__ = [
    # Context
    'PipelineContext',
    'StepSpec',
    'StepOutput',
    # Dependency Graph
    'DependencyGraph',
    'DependencyType',
    'DependencyEdge',
    'DependencySource',
    'DataDependencySource',
    'ExplicitDependencySource',
    'ExecutionPlan',
    'ExecutionLayer',
    'CyclicDependencyError',
    'MissingDependencyError',
    # Manager
    'ExecuteManager',
]
