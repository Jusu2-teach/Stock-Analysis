"""Pipeline Core - Dependency Injection Container
=================================================

企业级依赖注入容器，替代单例模式滥用。

设计原则：
- 显式依赖声明
- 生命周期管理
- 线程安全
- 可测试性
- 条件注册
- 服务激活事件

版本: 2.1.0
"""

from __future__ import annotations

import logging
import threading
import weakref
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    Generic,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
)

logger = logging.getLogger(__name__)

T = TypeVar('T')

# =============================================================================
# 服务生命周期回调
# =============================================================================

ServiceActivatedCallback = Callable[[Any], None]
ServiceDeactivatedCallback = Callable[[Any], None]


# =============================================================================
# 生命周期定义
# =============================================================================

class Lifecycle(Enum):
    """依赖生命周期"""

    SINGLETON = auto()   # 单例: 整个应用生命周期内只创建一次
    SCOPED = auto()      # 作用域: 每个 FlowRun 一个实例
    TRANSIENT = auto()   # 瞬态: 每次请求创建新实例


# =============================================================================
# 服务描述符
# =============================================================================

@dataclass(frozen=True)
class ServiceDescriptor(Generic[T]):
    """服务描述符 (不可变)

    描述如何创建和管理一个服务。
    实例存储在 Container 的 _singleton_instances 中，而非描述符内。

    Attributes:
        service_type: 服务类型 (接口或抽象类)
        implementation: 实现类型或工厂函数
        lifecycle: 生命周期
        condition: 条件函数，返回 False 则不注册 (懒评估)
        on_activated: 服务实例创建后的回调
        on_deactivated: 服务实例销毁前的回调
        tags: 服务标签 (用于批量查询)
    """
    service_type: Type[T]
    implementation: Union[Type[T], Callable[['Container'], T]]
    lifecycle: Lifecycle = Lifecycle.SINGLETON
    condition: Optional[Callable[[], bool]] = None
    on_activated: Optional[ServiceActivatedCallback] = None
    on_deactivated: Optional[ServiceDeactivatedCallback] = None
    tags: FrozenSet[str] = field(default_factory=frozenset)

    def is_factory(self) -> bool:
        """是否为工厂函数"""
        return callable(self.implementation) and not isinstance(self.implementation, type)

    def should_register(self) -> bool:
        """检查条件是否满足"""
        if self.condition is None:
            return True
        try:
            return self.condition()
        except Exception:
            return False


# =============================================================================
# 作用域上下文
# =============================================================================

class Scope:
    """依赖注入作用域

    管理 SCOPED 生命周期的服务实例。

    Usage:
        with container.create_scope() as scope:
            service = scope.resolve(MyService)
            # scope 内多次 resolve 返回同一实例
    """

    def __init__(self, container: 'Container', scope_id: str = None):
        self._container = container
        self._scope_id = scope_id or f"scope-{id(self)}"
        self._instances: Dict[Type, Any] = {}
        self._lock = threading.RLock()
        self._disposed = False

    @property
    def scope_id(self) -> str:
        return self._scope_id

    def resolve(self, service_type: Type[T]) -> T:
        """解析服务"""
        if self._disposed:
            raise RuntimeError(f"Scope {self._scope_id} has been disposed")

        with self._lock:
            # 检查作用域缓存
            if service_type in self._instances:
                return self._instances[service_type]

            # 委托给容器解析
            instance = self._container._resolve_in_scope(service_type, self)

            # 缓存 SCOPED 实例
            descriptor = self._container._get_descriptor(service_type)
            if descriptor and descriptor.lifecycle == Lifecycle.SCOPED:
                self._instances[service_type] = instance

            return instance

    def dispose(self) -> None:
        """释放作用域资源"""
        with self._lock:
            self._disposed = True

            # 调用实例的 dispose 方法 (如果有)
            for instance in self._instances.values():
                if hasattr(instance, 'dispose'):
                    try:
                        instance.dispose()
                    except Exception as e:
                        logger.warning(f"Error disposing {type(instance).__name__}: {e}")

            self._instances.clear()
            logger.debug(f"Disposed scope {self._scope_id}")

    def __enter__(self) -> 'Scope':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.dispose()


# =============================================================================
# 依赖注入容器
# =============================================================================

class Container:
    """依赖注入容器

    企业级 IoC 容器，支持：
    - 三种生命周期 (Singleton/Scoped/Transient)
    - 工厂函数注册
    - 作用域管理
    - 线程安全

    Usage:
        # 创建容器
        container = Container()

        # 注册服务
        container.register(ICatalog, DataCatalog, Lifecycle.SINGLETON)
        container.register(ICollector, Collector, Lifecycle.SCOPED)
        container.register_factory(EventBus, lambda c: EventBus())

        # 解析服务
        catalog = container.resolve(ICatalog)

        # 使用作用域
        with container.create_scope() as scope:
            collector = scope.resolve(ICollector)

    Example (Pipeline 集成):
        # 在 FlowRunner 中
        container = Container.default()

        with container.create_scope(flow_id) as scope:
            catalog = scope.resolve(DataCatalog)
            collector = scope.resolve(Collector)

            runner = FlowRunner(
                catalog=catalog,
                aggregator=collector,
            )
            runner.run(flow_spec)
    """

    # 全局默认容器
    _default: Optional['Container'] = None
    _default_lock = threading.Lock()

    def __init__(self):
        self._descriptors: Dict[Type, ServiceDescriptor] = {}
        self._singleton_instances: Dict[Type, Any] = {}  # 单例实例存储 (分离自描述符)
        self._lock = threading.RLock()
        self._scopes: weakref.WeakValueDictionary[str, Scope] = weakref.WeakValueDictionary()
        # 解析栈用于检测循环依赖 - 使用线程本地存储确保并发安全
        self._local = threading.local()
        # 全局生命周期回调
        self._global_on_activated: List[ServiceActivatedCallback] = []
        self._global_on_deactivated: List[ServiceDeactivatedCallback] = []

    def _get_resolution_stack(self) -> List[Type]:
        """获取当前线程的解析栈（线程安全）"""
        if not hasattr(self._local, 'resolution_stack'):
            self._local.resolution_stack = []
        return self._local.resolution_stack

    def on_service_activated(self, callback: ServiceActivatedCallback) -> 'Container':
        """注册全局服务激活回调

        Args:
            callback: 回调函数，接收新创建的服务实例

        Returns:
            self (链式调用)
        """
        self._global_on_activated.append(callback)
        return self

    def on_service_deactivated(self, callback: ServiceDeactivatedCallback) -> 'Container':
        """注册全局服务销毁回调

        Args:
            callback: 回调函数，接收即将销毁的服务实例

        Returns:
            self (链式调用)
        """
        self._global_on_deactivated.append(callback)
        return self

    # -------------------------------------------------------------------------
    # 注册 API
    # -------------------------------------------------------------------------

    def register(
        self,
        service_type: Type[T],
        implementation: Type[T] = None,
        lifecycle: Lifecycle = Lifecycle.SINGLETON,
        *,
        condition: Callable[[], bool] = None,
        on_activated: ServiceActivatedCallback = None,
        on_deactivated: ServiceDeactivatedCallback = None,
        tags: List[str] = None,
    ) -> 'Container':
        """注册服务

        Args:
            service_type: 服务类型 (接口)
            implementation: 实现类型 (默认为 service_type 自身)
            lifecycle: 生命周期
            condition: 条件函数，返回 False 则跳过注册 (用于环境判断)
            on_activated: 服务实例创建后的回调
            on_deactivated: 服务实例销毁前的回调
            tags: 服务标签 (用于批量查询/操作)

        Returns:
            self (链式调用)

        Examples:
            # 条件注册 (仅在生产环境注册)
            container.register(
                ICache, RedisCache,
                condition=lambda: os.getenv('ENV') == 'production'
            )

            # 生命周期回调
            container.register(
                IDatabase, PostgresDB,
                on_activated=lambda db: db.connect(),
                on_deactivated=lambda db: db.disconnect()
            )
        """
        impl = implementation or service_type

        descriptor = ServiceDescriptor(
            service_type=service_type,
            implementation=impl,
            lifecycle=lifecycle,
            condition=condition,
            on_activated=on_activated,
            on_deactivated=on_deactivated,
            tags=frozenset(tags) if tags else frozenset(),
        )

        # 检查条件
        if not descriptor.should_register():
            logger.debug(
                f"Skipped registration of {service_type.__name__}: condition not met"
            )
            return self

        with self._lock:
            self._descriptors[service_type] = descriptor

        logger.debug(
            f"Registered {service_type.__name__} -> {impl.__name__} "
            f"({lifecycle.name})"
        )
        return self

    def register_factory(
        self,
        service_type: Type[T],
        factory: Callable[['Container'], T],
        lifecycle: Lifecycle = Lifecycle.SINGLETON,
        *,
        condition: Callable[[], bool] = None,
        on_activated: ServiceActivatedCallback = None,
        on_deactivated: ServiceDeactivatedCallback = None,
        tags: List[str] = None,
    ) -> 'Container':
        """使用工厂函数注册服务

        Args:
            service_type: 服务类型
            factory: 工厂函数 (接收 Container 参数)
            lifecycle: 生命周期
            condition: 条件函数
            on_activated: 激活回调
            on_deactivated: 销毁回调
            tags: 服务标签

        Returns:
            self (链式调用)
        """
        descriptor = ServiceDescriptor(
            service_type=service_type,
            implementation=factory,
            lifecycle=lifecycle,
            condition=condition,
            on_activated=on_activated,
            on_deactivated=on_deactivated,
            tags=frozenset(tags) if tags else frozenset(),
        )

        if not descriptor.should_register():
            logger.debug(
                f"Skipped factory registration of {service_type.__name__}: condition not met"
            )
            return self

        with self._lock:
            self._descriptors[service_type] = descriptor

        logger.debug(f"Registered factory for {service_type.__name__} ({lifecycle.name})")
        return self

    def register_instance(
        self,
        service_type: Type[T],
        instance: T,
        *,
        tags: List[str] = None,
    ) -> 'Container':
        """注册已有实例 (作为单例)

        Args:
            service_type: 服务类型
            instance: 实例
            tags: 服务标签

        Returns:
            self (链式调用)
        """
        with self._lock:
            self._descriptors[service_type] = ServiceDescriptor(
                service_type=service_type,
                implementation=type(instance),
                lifecycle=Lifecycle.SINGLETON,
                tags=frozenset(tags) if tags else frozenset(),
            )
            self._singleton_instances[service_type] = instance

        logger.debug(f"Registered instance for {service_type.__name__}")
        return self

    def resolve_by_tag(self, tag: str) -> List[Any]:
        """根据标签解析所有匹配的服务

        Args:
            tag: 服务标签

        Returns:
            匹配的服务实例列表

        Examples:
            # 注册带标签的服务
            container.register(ServiceA, tags=['middleware'])
            container.register(ServiceB, tags=['middleware', 'logging'])

            # 批量解析
            middlewares = container.resolve_by_tag('middleware')
        """
        instances = []
        with self._lock:
            for service_type, descriptor in self._descriptors.items():
                if tag in descriptor.tags:
                    try:
                        if descriptor.lifecycle != Lifecycle.SCOPED:
                            instances.append(self.resolve(service_type))
                    except Exception as e:
                        logger.warning(f"Failed to resolve {service_type.__name__} by tag: {e}")
        return instances

    # -------------------------------------------------------------------------
    # 解析 API
    # -------------------------------------------------------------------------

    def resolve(self, service_type: Type[T]) -> T:
        """解析服务

        对于 SCOPED 生命周期，需要在作用域内解析。

        Args:
            service_type: 服务类型

        Returns:
            服务实例

        Raises:
            KeyError: 服务未注册
            RuntimeError: SCOPED 服务在作用域外解析
        """
        descriptor = self._get_descriptor(service_type)
        if descriptor is None:
            raise KeyError(f"Service {service_type.__name__} is not registered")

        if descriptor.lifecycle == Lifecycle.SCOPED:
            raise RuntimeError(
                f"Scoped service {service_type.__name__} must be resolved within a scope. "
                f"Use container.create_scope() context manager."
            )

        return self._create_instance(descriptor)

    def try_resolve(self, service_type: Type[T], default: T = None) -> Optional[T]:
        """尝试解析服务，失败返回默认值"""
        try:
            return self.resolve(service_type)
        except (KeyError, RuntimeError):
            return default

    def _resolve_in_scope(self, service_type: Type[T], scope: Scope) -> T:
        """在作用域内解析服务"""
        descriptor = self._get_descriptor(service_type)
        if descriptor is None:
            raise KeyError(f"Service {service_type.__name__} is not registered")

        return self._create_instance(descriptor, scope)

    def _get_descriptor(self, service_type: Type) -> Optional[ServiceDescriptor]:
        """获取服务描述符"""
        with self._lock:
            return self._descriptors.get(service_type)

    def _create_instance(
        self,
        descriptor: ServiceDescriptor[T],
        scope: Scope = None,
    ) -> T:
        """创建服务实例"""
        service_type = descriptor.service_type
        resolution_stack = self._get_resolution_stack()

        # 循环依赖检测（线程安全）
        if service_type in resolution_stack:
            cycle = " -> ".join(t.__name__ for t in resolution_stack)
            cycle += f" -> {service_type.__name__}"
            raise RuntimeError(
                f"Circular dependency detected: {cycle}"
            )

        resolution_stack.append(service_type)
        try:
            # 单例: 使用缓存的实例
            if descriptor.lifecycle == Lifecycle.SINGLETON:
                if service_type in self._singleton_instances:
                    return self._singleton_instances[service_type]

                with self._lock:
                    # 双重检查锁定
                    if service_type in self._singleton_instances:
                        return self._singleton_instances[service_type]

                    instance = self._instantiate(descriptor)
                    self._fire_activated(descriptor, instance)
                    self._singleton_instances[service_type] = instance
                    return instance

            # 瞬态: 每次创建新实例
            if descriptor.lifecycle == Lifecycle.TRANSIENT:
                instance = self._instantiate(descriptor)
                self._fire_activated(descriptor, instance)
                return instance

            # 作用域: 在 Scope 中已经处理缓存
            instance = self._instantiate(descriptor)
            self._fire_activated(descriptor, instance)
            return instance
        finally:
            resolution_stack.pop()

    def _fire_activated(self, descriptor: ServiceDescriptor, instance: Any) -> None:
        """触发服务激活回调"""
        # 描述符级别回调
        if descriptor.on_activated:
            try:
                descriptor.on_activated(instance)
            except Exception as e:
                logger.warning(
                    f"on_activated callback failed for {descriptor.service_type.__name__}: {e}"
                )

        # 全局回调
        for callback in self._global_on_activated:
            try:
                callback(instance)
            except Exception as e:
                logger.warning(f"Global on_activated callback failed: {e}")

    def _fire_deactivated(self, descriptor: ServiceDescriptor, instance: Any) -> None:
        """触发服务销毁回调"""
        # 描述符级别回调
        if descriptor.on_deactivated:
            try:
                descriptor.on_deactivated(instance)
            except Exception as e:
                logger.warning(
                    f"on_deactivated callback failed for {descriptor.service_type.__name__}: {e}"
                )

        # 全局回调
        for callback in self._global_on_deactivated:
            try:
                callback(instance)
            except Exception as e:
                logger.warning(f"Global on_deactivated callback failed: {e}")

    def _instantiate(self, descriptor: ServiceDescriptor[T]) -> T:
        """实例化服务

        支持三种模式：
        1. 工厂函数
        2. 自动依赖注入 (通过类型标注)
        3. 无参构造函数
        """
        import inspect

        impl = descriptor.implementation

        if descriptor.is_factory():
            # 工厂函数
            return impl(self)

        # 类型实例化 - 分析构造函数并注入依赖
        try:
            sig = inspect.signature(impl.__init__)
            params = sig.parameters

            # 跳过 'self' 参数
            param_list = list(params.values())[1:]

            if not param_list:
                # 无参构造
                return impl()

            # 尝试自动注入依赖
            kwargs = {}
            for param in param_list:
                param_name = param.name
                param_type = param.annotation

                # 跳过无类型标注的参数 (有默认值的可以跳过)
                if param_type is inspect.Parameter.empty:
                    if param.default is not inspect.Parameter.empty:
                        continue  # 有默认值，跳过
                    else:
                        raise TypeError(
                            f"Cannot auto-inject parameter '{param_name}' in {impl.__name__}: "
                            f"no type annotation and no default value"
                        )

                # 特殊处理: 如果参数类型是 Container
                if param_type is Container or (
                    isinstance(param_type, type) and issubclass(param_type, Container)
                ):
                    kwargs[param_name] = self
                    continue

                # 尝试从容器解析依赖
                if self.is_registered(param_type):
                    kwargs[param_name] = self.resolve(param_type)
                elif param.default is not inspect.Parameter.empty:
                    # 有默认值，使用默认值
                    continue
                else:
                    raise TypeError(
                        f"Cannot resolve dependency '{param_name}' of type {param_type} "
                        f"in {impl.__name__}: not registered in container"
                    )

            return impl(**kwargs)

        except TypeError as e:
            # 如果解析失败，尝试无参构造
            try:
                return impl()
            except TypeError:
                raise TypeError(
                    f"Cannot instantiate {impl.__name__}. Error: {e}"
                ) from e

    # -------------------------------------------------------------------------
    # 作用域 API
    # -------------------------------------------------------------------------

    def create_scope(self, scope_id: str = None) -> Scope:
        """创建新的作用域

        Args:
            scope_id: 作用域 ID (如 flow_run_id)

        Returns:
            作用域上下文管理器
        """
        scope = Scope(self, scope_id)
        self._scopes[scope.scope_id] = scope
        logger.debug(f"Created scope {scope.scope_id}")
        return scope

    def get_scope(self, scope_id: str) -> Optional[Scope]:
        """获取已存在的作用域"""
        return self._scopes.get(scope_id)

    # -------------------------------------------------------------------------
    # 工具方法
    # -------------------------------------------------------------------------

    def is_registered(self, service_type: Type) -> bool:
        """检查服务是否已注册"""
        return service_type in self._descriptors

    def get_services_by_tag(self, tag: str) -> List[Type]:
        """获取具有指定标签的服务类型列表"""
        with self._lock:
            return [
                st for st, d in self._descriptors.items()
                if tag in d.tags
            ]

    def get_registered_services(self) -> Dict[str, str]:
        """获取所有已注册的服务"""
        return {
            st.__name__: f"{d.implementation.__name__ if isinstance(d.implementation, type) else 'factory'} ({d.lifecycle.name})"
            for st, d in self._descriptors.items()
        }

    def clear(self) -> None:
        """清空容器"""
        with self._lock:
            # 释放单例实例 (从 _singleton_instances 而非 descriptor)
            for service_type, instance in list(self._singleton_instances.items()):
                # 触发销毁回调
                descriptor = self._descriptors.get(service_type)
                if descriptor:
                    self._fire_deactivated(descriptor, instance)

                # 调用 dispose 方法
                if hasattr(instance, 'dispose'):
                    try:
                        instance.dispose()
                    except Exception as e:
                        logger.warning(
                            f"Failed to dispose {service_type.__name__}: {e}"
                        )

            self._singleton_instances.clear()
            self._descriptors.clear()

        logger.debug("Container cleared")

    # -------------------------------------------------------------------------
    # 全局默认容器
    # -------------------------------------------------------------------------

    @classmethod
    def default(cls) -> 'Container':
        """获取默认容器 (懒加载)"""
        if cls._default is None:
            with cls._default_lock:
                if cls._default is None:
                    cls._default = cls._create_default_container()
        return cls._default

    @classmethod
    def set_default(cls, container: 'Container') -> None:
        """设置默认容器 (用于测试)"""
        with cls._default_lock:
            cls._default = container

    @classmethod
    def reset_default(cls) -> None:
        """重置默认容器"""
        with cls._default_lock:
            if cls._default:
                cls._default.clear()
            cls._default = None

    @classmethod
    def _create_default_container(cls) -> 'Container':
        """创建默认容器并注册 Pipeline 核心服务"""
        from ..catalog import DataCatalog
        from ..aggregation import (
            Collector,
            AggregationScope,
            ScopeManager,
            Injector,
            LineageTracker,
        )
        from ..events import EventBus
        from ..cache import MemoryCacheBackend, CacheBackend, CacheBackendRouter

        container = cls()

        # =====================================================================
        # Singleton 服务 - 整个应用生命周期内只创建一次
        # =====================================================================

        container.register_factory(
            EventBus,
            lambda c: EventBus.instance(),  # 使用单例
            Lifecycle.SINGLETON,
        )

        container.register_factory(
            DataCatalog,
            lambda c: _create_catalog_with_events(c),
            Lifecycle.SINGLETON,
        )

        container.register_factory(
            CacheBackend,
            lambda c: MemoryCacheBackend(max_size=1000),
            Lifecycle.SINGLETON,
        )

        # 动态缓存路由：按 CachePolicy.backend + namespace 选择后端
        container.register_factory(
            CacheBackendRouter,
            lambda c: CacheBackendRouter(
                base_cache_dir='.cache/pipeline',
                memory_max_size=1000,
                tiered_l1_max_size=100,
            ),
            Lifecycle.SINGLETON,
        )

        container.register_factory(
            ScopeManager,
            lambda c: ScopeManager.instance(),  # 使用单例
            Lifecycle.SINGLETON,
        )

        container.register_factory(
            Injector,
            lambda c: Injector(scope=None),  # 需要 scope 时再设置
            Lifecycle.SINGLETON,
        )

        container.register_factory(
            LineageTracker,
            lambda c: LineageTracker.instance(),  # 使用单例
            Lifecycle.SINGLETON,
        )

        # =====================================================================
        # Singleton 服务 - Collector 和 AggregationScope
        # 注意: 实际作用域通过 ScopeManager.create() 和 set_scope() 动态管理
        # =====================================================================

        container.register_factory(
            AggregationScope,
            lambda c: AggregationScope(flow_id=None),  # flow_id 在 scope 创建时设置
            Lifecycle.SINGLETON,
        )

        container.register_factory(
            Collector,
            lambda c: Collector(scope=None),  # scope 在使用时通过 set_scope 设置
            Lifecycle.SINGLETON,
        )

        logger.info("Created default Pipeline container with core services")
        return container


def _create_catalog_with_events(container: 'Container'):
    """创建 DataCatalog 并自动绑定 EventBus。

    放在模块内（而不是 lambda 里写复杂逻辑），便于调试与测试。
    """
    from ..catalog import DataCatalog
    from ..events import EventBus

    catalog = DataCatalog()
    try:
        bus = container.resolve(EventBus)
        catalog.set_event_bus(bus)
    except Exception:
        # 不强制要求事件系统可用（测试/精简场景），但默认会成功绑定
        pass
    return catalog


# =============================================================================
# 便捷函数
# =============================================================================

def get_container() -> Container:
    """获取默认容器"""
    return Container.default()


def resolve(service_type: Type[T]) -> T:
    """从默认容器解析服务"""
    return Container.default().resolve(service_type)


@contextmanager
def create_scope(scope_id: str = None):
    """从默认容器创建作用域"""
    with Container.default().create_scope(scope_id) as scope:
        yield scope
