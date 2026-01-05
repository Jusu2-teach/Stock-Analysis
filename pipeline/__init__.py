#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AStock Pipeline System
=====================

Configuration-driven workflow execution system with intelligent orchestration.

Core Components:
- ExecuteManager: Pipeline 执行管理器
- PipelineContext: 共享执行上下文
- DependencyGraph: 专业级依赖图管理
- HookManager: 事件钩子系统

Engine Services (专供引擎层使用):
- CacheService: 缓存管理（指纹计算、签名持久化）
- EventPublisher: 事件发布（统一 EventBus 封装）

Core Services (Pipeline 核心服务):
- ConfigService: 配置加载与步骤解析
- FlowExecutor: 流程执行协调
- ResultAssembler: 结果组装

Engines:
- PrefectEngine: Hybrid Prefect+Kedro 运行时

Usage Example:
    from pipeline import ExecuteManager, create_pipeline

    # Basic usage
    manager = ExecuteManager()
    manager.load_config('pipeline.yaml')
    result = manager.execute_pipeline()

    # Access dependency graph
    graph = manager.ctx.get_dependency_graph()
    print(graph.to_mermaid())

Author: Your Name
Version: 1.0.0 (与 pyproject.toml 对齐)
"""
from __future__ import annotations

# Core Components
from .core.execute_manager import ExecuteManager
from .core.context import PipelineContext, StepSpec, StepOutput
from .core.dependency_graph import (
    DependencyGraph,
    DependencyType,
    DependencyEdge,
    ExecutionPlan,
    ExecutionLayer,
    CyclicDependencyError,
    MissingDependencyError,
)

# Engines
from .engines.prefect_engine import PrefectEngine

# Services (Advanced)
from .core.services.hook_manager import HookManager

# Engine Services (专供引擎使用)
from .engine_services import CacheService, EventPublisher

PREFECT_AVAILABLE = True  # always true in trimmed hybrid mode

# 与项目版本/作者信息对齐（见 pyproject.toml）
__version__ = "1.0.0"
__author__ = "Your Name"

# Export main classes and functions
__all__ = [
    # Core
    'ExecuteManager',
    'PipelineContext',
    'StepSpec',
    'StepOutput',
    # Dependency Graph
    'DependencyGraph',
    'DependencyType',
    'DependencyEdge',
    'ExecutionPlan',
    'ExecutionLayer',
    'CyclicDependencyError',
    'MissingDependencyError',
    # Engines
    'PrefectEngine',
    # Services
    'HookManager',
    # Engine Services
    'CacheService',
    'EventPublisher',
    # Functions
    'create_pipeline',
    'get_system_info',
]


def create_pipeline(config_path: str, **kwargs) -> ExecuteManager:
    """创建并初始化 Pipeline 管理器

    Args:
        config_path: YAML 配置文件路径
        **kwargs: 传递给 ExecuteManager 的额外参数

    Returns:
        已加载配置的 ExecuteManager 实例
    """
    mgr = ExecuteManager(config_path, **kwargs)
    mgr.load_config(config_path)
    return mgr


def get_system_info() -> dict:
    """获取系统信息

    Returns:
        包含版本、作者、可用引擎等信息的字典
    """
    return {
        'version': __version__,
        'author': __author__,
        'engines': {
            'hybrid_prefect_kedro': True,
        },
        'features': [
            'dependency_graph',
            'execution_plan',
            'hook_system',
            'mermaid_visualization',
            'graphviz_export',
        ]
    }