"""
Protocols - 组件间接口契约
=========================

定义各组件的抽象接口，实现编译时类型检查 + 运行时解耦。

使用 Protocol 而非 ABC 的优势：
1. 结构化子类型（鸭子类型的静态检查）
2. 无需显式继承
3. 避免循环导入

使用示例：

    # 类型注解使用 Protocol
    def execute_pipeline(orchestrator: OrchestratorProtocol):
        result = orchestrator.execute('business', 'analyze')

    # 实际传入任何符合接口的对象
    from orchestrator import AStockOrchestrator
    execute_pipeline(AStockOrchestrator())
"""
from __future__ import annotations
from typing import (
    Any, Dict, List, Optional, Callable, Protocol,
    runtime_checkable, TypeVar, Generic
)


T = TypeVar('T')


# ============================================================================
# Orchestrator 接口
# ============================================================================

@runtime_checkable
class RegistryProtocol(Protocol):
    """Registry 接口契约

    定义方法注册中心必须实现的接口。
    """

    def register(
        self,
        component_type: str,
        method_name: str,
        func: Callable,
        *,
        engine_type: str = "",
        engine_name: str = "",
        version: str = "",
        priority: int = 0,
        **kwargs
    ) -> bool:
        """注册方法"""
        ...

    def select(
        self,
        component_type: str,
        method_name: str,
        *,
        strategy: str = "default",
        preferred_engine: Optional[str] = None
    ) -> Any:
        """选择最佳方法实现"""
        ...

    def execute(
        self,
        component_type: str,
        method_name: str,
        *args,
        **kwargs
    ) -> Any:
        """执行方法"""
        ...

    def describe(
        self,
        component_type: str,
        method_name: str
    ) -> Dict[str, Any]:
        """描述方法信息"""
        ...


@runtime_checkable
class OrchestratorProtocol(Protocol):
    """Orchestrator 接口契约

    定义编排器必须实现的接口。
    """

    registry: RegistryProtocol

    def execute(
        self,
        component_type: str,
        method_name: str,
        *args,
        **kwargs
    ) -> Any:
        """执行组件方法"""
        ...

    def describe(
        self,
        component_type: str,
        method_name: str
    ) -> Dict[str, Any]:
        """获取方法描述"""
        ...

    def get_component_methods(self, component_type: str) -> List[str]:
        """获取组件的所有方法"""
        ...

    def add_middleware(self, middleware: Callable) -> None:
        """添加中间件"""
        ...


# ============================================================================
# Pipeline 接口
# ============================================================================

@runtime_checkable
class PipelineContextProtocol(Protocol):
    """PipelineContext 接口契约"""

    config: Dict[str, Any]
    steps: Dict[str, Any]
    execution_order: List[str]

    def register_reference(self, ref: str, value: Any) -> str:
        """注册引用值"""
        ...

    def get_reference(self, ref: str) -> Optional[Any]:
        """获取引用值"""
        ...


@runtime_checkable
class ExecutorProtocol(Protocol):
    """Pipeline 执行器接口契约"""

    ctx: PipelineContextProtocol

    def load_config(self, path: str) -> Dict[str, Any]:
        """加载配置"""
        ...

    def execute_pipeline(self) -> Dict[str, Any]:
        """执行 Pipeline"""
        ...


# ============================================================================
# Engine 接口
# ============================================================================

@runtime_checkable
class DataEngineProtocol(Protocol):
    """数据引擎接口契约

    所有数据处理引擎（pandas/polars/duckdb）应符合此接口。
    """

    def load(self, source: str, **options) -> Any:
        """加载数据"""
        ...

    def transform(self, data: Any, operations: List[Dict]) -> Any:
        """转换数据"""
        ...

    def save(self, data: Any, target: str, **options) -> None:
        """保存数据"""
        ...


@runtime_checkable
class CacheProtocol(Protocol):
    """缓存接口契约"""

    def get(self, key: str) -> Optional[Any]:
        """获取缓存"""
        ...

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """设置缓存"""
        ...

    def delete(self, key: str) -> bool:
        """删除缓存"""
        ...

    def exists(self, key: str) -> bool:
        """检查是否存在"""
        ...


# ============================================================================
# Hook/Event 接口
# ============================================================================

@runtime_checkable
class EventBusProtocol(Protocol):
    """事件总线接口契约"""

    def on(self, event: str, handler: Callable, **kwargs) -> Callable:
        """注册事件处理器"""
        ...

    def off(self, event: str, handler: Callable = None) -> bool:
        """注销事件处理器"""
        ...

    def emit(self, event: Any, **kwargs) -> Any:
        """发布事件"""
        ...


@runtime_checkable
class HookManagerProtocol(Protocol):
    """Hook 管理器接口契约（兼容旧代码）"""

    def register(self, event: str, func: Callable) -> Callable:
        """注册钩子"""
        ...

    def emit(self, event: str, *args, **kwargs) -> int:
        """触发事件"""
        ...


# ============================================================================
# 方法句柄接口
# ============================================================================

@runtime_checkable
class MethodHandleProtocol(Protocol):
    """方法句柄接口契约"""

    component: str
    method: str

    def resolve(self, orchestrator: OrchestratorProtocol) -> str:
        """解析引擎"""
        ...

    def explain(self) -> Dict[str, Any]:
        """解释选择原因"""
        ...


# ============================================================================
# 工厂接口
# ============================================================================

class ComponentFactory(Protocol[T]):
    """组件工厂接口

    用于延迟创建组件实例。
    """

    def create(self, **config) -> T:
        """创建组件实例"""
        ...


__all__ = [
    # Orchestrator
    'RegistryProtocol',
    'OrchestratorProtocol',
    # Pipeline
    'PipelineContextProtocol',
    'ExecutorProtocol',
    # Engine
    'DataEngineProtocol',
    'CacheProtocol',
    # Event
    'EventBusProtocol',
    'HookManagerProtocol',
    # Handle
    'MethodHandleProtocol',
    # Factory
    'ComponentFactory',
]
