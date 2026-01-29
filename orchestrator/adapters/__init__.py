"""Orchestrator Adapters - 为 Pipeline 提供的适配器
===================================================

本模块提供 orchestrator 到 pipeline 的适配器实现。

架构说明：
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                     orchestrator/adapters/ (本模块)                      │
    │                                                                          │
    │   ┌──────────────────────────────────────────────────────────────────┐  │
    │   │  RegistryMethodResolver                                           │  │
    │   │                                                                  │  │
    │   │  ┌─────────────────────┐         ┌─────────────────────┐        │  │
    │   │  │ Pipeline 协议       │  ◀────  │ Orchestrator 实现    │        │  │
    │   │  │ MethodResolverProto │   适配   │ Registry            │        │  │
    │   │  └─────────────────────┘         └─────────────────────┘        │  │
    │   └──────────────────────────────────────────────────────────────────┘  │
    │                                                                          │
    └─────────────────────────────────────────────────────────────────────────┘

为什么放在 orchestrator 目录？
    - 适配器是 orchestrator 对外提供的"服务接口"
    - orchestrator 知道 pipeline 的协议（单向依赖）
    - pipeline 完全不知道 orchestrator 的存在（零耦合）

使用方式：
    # 1. 直接使用
    from orchestrator.adapters import RegistryMethodResolver

    resolver = RegistryMethodResolver()
    info = resolver.resolve("business_engine", "duckdb", "analyze_metric")

    # 2. 通过 DI 注入
    from pipeline.core.container import get_container, Lifecycle
    from pipeline.protocols import MethodResolverProtocol
    from orchestrator.adapters import RegistryMethodResolver

    container = get_container()
    container.register(
        MethodResolverProtocol,
        RegistryMethodResolver,
        Lifecycle.SINGLETON,
    )

    # 3. 传递给 FlowRunner
    from pipeline.execution import FlowRunner

    runner = FlowRunner(
        container=container,
        method_resolver=RegistryMethodResolver(),
    )

版本: 2.0.0
"""

from .registry_adapter import RegistryMethodResolver

__all__ = ["RegistryMethodResolver"]
