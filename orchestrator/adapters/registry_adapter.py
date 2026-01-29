"""Registry Adapter - 将 orchestrator.Registry 适配为 Pipeline 的 MethodResolver
==================================================================================

这是一个 **适配器模式** 实现：
- 实现 Pipeline 定义的 MethodResolverProtocol
- 内部委托给 orchestrator.Registry

架构位置：
    ┌──────────────────────────────────────────────────────────────┐
    │                         Pipeline                              │
    │  ┌────────────────────────────────────────────────────────┐  │
    │  │  只知道 MethodResolverProtocol 协议                      │  │
    │  │  完全不知道 orchestrator 的存在                          │  │
    │  └────────────────────────────────────────────────────────┘  │
    └──────────────────────────────────────────────────────────────┘
                            ▲
                            │ 实现协议
                            │
    ┌──────────────────────────────────────────────────────────────┐
    │                     Orchestrator                              │
    │  ┌────────────────────────────────────────────────────────┐  │
    │  │  RegistryMethodResolver (本模块)                        │  │
    │  │  - 实现 Pipeline 的 MethodResolverProtocol              │  │
    │  │  - 内部使用 Registry                                    │  │
    │  └────────────────────────────────────────────────────────┘  │
    │  ┌────────────────────────────────────────────────────────┐  │
    │  │  Registry (方法注册中心)                                 │  │
    │  └────────────────────────────────────────────────────────┘  │
    └──────────────────────────────────────────────────────────────┘

版本: 2.0.0
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional, TYPE_CHECKING

# Pipeline 的协议（只依赖接口）
from pipeline.protocols import MethodResolverProtocol, MethodInfo

# Orchestrator 的实现（同一模块，直接导入）
from ..registry import Registry

if TYPE_CHECKING:
    from ..models import MethodRegistration

logger = logging.getLogger(__name__)


class RegistryMethodResolver(MethodResolverProtocol):
    """Registry 方法解析器适配器

    将 orchestrator.Registry 适配为 Pipeline 的 MethodResolverProtocol。

    特性：
    - 懒加载：首次使用时才获取 Registry 实例
    - 缓存转换：避免重复创建 MethodInfo
    - 完整映射：支持 Registry 的所有查询能力

    Usage:
        # 方式 1: 直接使用
        resolver = RegistryMethodResolver()
        info = resolver.resolve("business_engine", "duckdb", "analyze_metric")

        # 方式 2: 传递给 FlowRunner
        from pipeline.execution import FlowRunner

        runner = FlowRunner(
            container=container,
            method_resolver=RegistryMethodResolver(),
        )
    """

    def __init__(self, registry: Optional[Registry] = None):
        """初始化适配器

        Args:
            registry: Registry 实例（可选，默认使用单例）
        """
        self._registry = registry
        self._info_cache: Dict[str, MethodInfo] = {}

    @property
    def registry(self) -> Registry:
        """懒加载获取 Registry"""
        if self._registry is None:
            self._registry = Registry.get()

        # Ensure modules are loaded exactly once for production usage.
        # This keeps Pipeline decoupled from business_engines import side effects.
        if not self._registry.index.by_full_key:
            loaded = self._registry.auto_load(hot_reload=False)
            logger.info(
                f"[orchestrator.adapter] auto_load modules={loaded} registered_methods={len(self._registry.index.by_full_key)}"
            )
        return self._registry

    def _convert_to_method_info(self, reg: 'MethodRegistration') -> MethodInfo:
        """将 MethodRegistration 转换为 MethodInfo

        这是适配的核心：将 orchestrator 的数据结构转换为 pipeline 的数据结构。
        """
        # 检查缓存
        cache_key = reg.full_key
        if cache_key in self._info_cache:
            return self._info_cache[cache_key]

        # 创建 MethodInfo
        info = MethodInfo(
            name=reg.engine_name,
            component=reg.component_type,
            engine=reg.engine_type,
            callable=reg.callable,  # 修正: MethodRegistration 使用 callable 而非 func
            description=reg.description,
            version=reg.version,
            priority=reg.priority,
            tags=list(reg.tags) if reg.tags else [],
            metadata={
                'module_path': reg.module_path,
                'signature': reg.signature,
                'deprecated': reg.deprecated,
                'full_key': reg.full_key,
            }
        )

        # 缓存
        self._info_cache[cache_key] = info
        return info

    def resolve(
        self,
        component: str,
        engine: str,
        method: str,
    ) -> Optional[MethodInfo]:
        """解析方法

        委托给 Registry.index 进行查找。
        """
        # 构造完整键
        full_key = f"{component}::{engine}::{method}"

        # 从 Registry 的索引中查找
        reg = self.registry.index.get_full(full_key)

        if reg is None:
            # 尝试使用 select（允许策略选择）
            try:
                reg = self.registry.select(component, method)
            except Exception:
                return None

        return self._convert_to_method_info(reg) if reg else None

    def can_resolve(self, method: str) -> bool:
        """检查方法是否可解析

        遍历所有组件类型查找方法。
        """
        for comp_type in self.registry.index.by_component:
            if method in self.registry.index.by_component[comp_type]:
                return True
        return False

    def list_methods(
        self,
        component: Optional[str] = None,
        engine: Optional[str] = None,
    ) -> Dict[str, MethodInfo]:
        """列出方法

        委托给 Registry.list_methods()。
        """
        # 获取 Registry 的方法列表
        registry_methods = self.registry.list_methods(
            component_type=component,
            engine_type=engine,
        )

        # 转换为 MethodInfo
        result = {}
        for full_key, info_dict in registry_methods.items():
            # 从索引获取完整的 MethodRegistration
            reg = self.registry.index.get_full(full_key)
            if reg:
                result[full_key] = self._convert_to_method_info(reg)

        return result

    def resolve_callable(
        self,
        component: str,
        engine: str,
        method: str,
    ) -> Optional[Callable]:
        """快捷方法：直接解析为可调用对象"""
        info = self.resolve(component, engine, method)
        return info.callable if info else None

    # =========================================================================
    # 扩展方法（利用 Registry 的高级功能）
    # =========================================================================

    def select_with_strategy(
        self,
        component: str,
        method: str,
        *,
        strategy: str = "default",
        preferred_engine: Optional[str] = None,
    ) -> Optional[MethodInfo]:
        """使用策略选择方法

        这是 Registry 的高级功能，支持：
        - 优先级选择
        - 版本选择
        - 引擎偏好
        """
        try:
            reg = self.registry.select(
                component,
                method,
                strategy=strategy,
                preferred_engine=preferred_engine,
            )
            return self._convert_to_method_info(reg)
        except Exception:
            return None

    def execute(
        self,
        component: str,
        method: str,
        *args,
        strategy: str = "default",
        preferred_engine: Optional[str] = None,
        **kwargs,
    ) -> Any:
        """直接执行方法（委托给 Registry）

        这是一个便捷方法，适用于简单场景。
        对于需要中间件、事件等完整执行链的场景，
        应该使用 TaskExecutor。
        """
        return self.registry.execute(
            component,
            method,
            *args,
            strategy=strategy,
            preferred_engine=preferred_engine,
            **kwargs,
        )

    def clear_cache(self) -> None:
        """清除转换缓存"""
        self._info_cache.clear()

    def refresh(self) -> None:
        """刷新 Registry 并清除缓存"""
        self.registry.refresh()
        self.clear_cache()
