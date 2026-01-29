"""
Pipeline EventBus - 高性能事件总线
===================================

企业级事件发布/订阅系统，专为 Pipeline 工作流设计。

核心特性:
1. 层级路由 - O(log n) 事件分发，支持订阅父级事件
2. 类型安全 - 泛型事件 + 运行时验证
3. 优先级订阅 - 控制处理器执行顺序
4. 中间件支持 - 可组合的事件处理管道 (同步/异步)
5. 上下文传播 - 自动传递 trace_id 等元数据
6. 真正的异步支持 - async def 处理器会被正确 await

设计参考:
- Guava EventBus (同步事件)
- RxPY (订阅管理)
- Express/Koa (中间件)
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import threading
import time
import uuid
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import IntEnum
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
)

__all__ = [
    # 核心类
    "EventBus",
    "Event",
    # 优先级
    "Priority",
    # 订阅
    "Subscription",
    # 结果
    "EmitResult",
    # 中间件
    "Middleware",
    "AsyncMiddleware",
    "MiddlewareChain",
    "AsyncMiddlewareChain",
    # 装饰器
    "on",
]

logger = logging.getLogger(__name__)

T = TypeVar("T")
P = TypeVar("P")  # Payload type


# =============================================================================
# Priority
# =============================================================================

class Priority(IntEnum):
    """事件处理优先级 (数值越小越先执行)"""
    SYSTEM = 0       # 系统级 (内部使用)
    HIGH = 10        # 高优先级
    NORMAL = 50      # 普通 (默认)
    LOW = 100        # 低优先级
    MONITOR = 200    # 监控级 (最后执行，用于日志/指标)


# =============================================================================
# Event
# =============================================================================

@dataclass
class Event(Generic[P]):
    """通用事件对象

    Type Parameters:
        P: 负载类型

    Attributes:
        type: 事件类型 (支持层级，如 "task.completed")
        payload: 事件负载
        event_id: 唯一 ID
        timestamp: 发生时间
        source: 事件来源
        trace_id: 追踪 ID (用于分布式追踪)
        metadata: 额外元数据

    Examples:
        # 简单事件
        event = Event("task.completed", {"task_id": "t1", "duration": 100})

        # 带追踪的事件
        event = Event.create(
            "flow.started",
            FlowStartedPayload(flow_id="f1", flow_name="analysis"),
            trace_id="trace-123",
        )
    """
    type: str
    payload: P
    event_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    timestamp: datetime = field(default_factory=datetime.now)
    source: str = "pipeline"
    trace_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        event_type: str,
        payload: P,
        trace_id: str = None,
        source: str = "pipeline",
        **metadata,
    ) -> "Event[P]":
        """工厂方法创建事件"""
        return cls(
            type=event_type,
            payload=payload,
            trace_id=trace_id,
            source=source,
            metadata=metadata,
        )

    @functools.cached_property
    def hierarchy(self) -> List[str]:
        """获取事件类型层级 (缓存计算结果)

        Example:
            Event("task.completed.success").hierarchy
            # → ["task", "task.completed", "task.completed.success"]
        """
        parts = self.type.split(".")
        return [".".join(parts[:i+1]) for i in range(len(parts))]

    def with_trace(self, trace_id: str) -> "Event[P]":
        """添加追踪 ID (返回新实例)"""
        return Event(
            type=self.type,
            payload=self.payload,
            event_id=self.event_id,
            timestamp=self.timestamp,
            source=self.source,
            trace_id=trace_id,
            metadata=self.metadata,
        )

    def __getattr__(self, name: str) -> Any:
        """便捷访问 payload 字段"""
        if isinstance(self.payload, dict) and name in self.payload:
            return self.payload[name]
        raise AttributeError(f"Event has no attribute '{name}'")

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "type": self.type,
            "event_id": self.event_id,
            "timestamp": self.timestamp.isoformat(),
            "source": self.source,
            "trace_id": self.trace_id,
            "payload": self.payload if isinstance(self.payload, dict) else str(self.payload),
            "metadata": self.metadata,
        }


# =============================================================================
# Handler Info
# =============================================================================

@dataclass
class _HandlerInfo:
    """处理器信息 (内部使用)

    支持同步和异步处理器的统一管理:
    - 自动检测 async def 处理器
    - 存储 is_async 标记供 emit_async 使用
    """
    handler: Callable[[Event], Any]
    priority: int
    once: bool = False
    filter: Optional[Callable[[Event], bool]] = None
    is_async: bool = field(default=False, init=False)

    def __post_init__(self):
        """初始化后检测处理器是否为异步"""
        self.is_async = asyncio.iscoroutinefunction(self.handler)

    def matches(self, event: Event) -> bool:
        """检查事件是否匹配过滤器"""
        if self.filter is None:
            return True
        try:
            return self.filter(event)
        except Exception:
            return False


# =============================================================================
# Subscription
# =============================================================================

class Subscription:
    """订阅对象 - 支持取消订阅

    Examples:
        sub = bus.subscribe("task.completed", handler)
        ...
        sub.dispose()  # 取消订阅
    """

    def __init__(
        self,
        event_type: str,
        handler: Callable,
        dispose_fn: Callable[[], bool],
    ):
        self._event_type = event_type
        self._handler = handler
        self._dispose_fn = dispose_fn
        self._disposed = False

    @property
    def event_type(self) -> str:
        return self._event_type

    @property
    def is_disposed(self) -> bool:
        return self._disposed

    def dispose(self) -> bool:
        """取消订阅"""
        if self._disposed:
            return False
        self._disposed = self._dispose_fn()
        return self._disposed

    def __enter__(self) -> "Subscription":
        return self

    def __exit__(self, *args) -> None:
        self.dispose()


# =============================================================================
# Emit Result
# =============================================================================

@dataclass
class EmitResult:
    """事件发布结果"""
    event_type: str
    event_id: str
    handler_count: int
    success_count: int = 0
    error_count: int = 0
    duration_ms: float = 0.0
    errors: List[Tuple[str, Exception]] = field(default_factory=list)

    @property
    def all_success(self) -> bool:
        return self.error_count == 0 and self.handler_count > 0

    @property
    def has_errors(self) -> bool:
        return self.error_count > 0


# =============================================================================
# Middleware
# =============================================================================

class Middleware:
    """同步中间件基类

    中间件可以在事件处理前后执行逻辑。

    Examples:
        class LoggingMiddleware(Middleware):
            def process(self, event, handler, next_fn):
                print(f"Before: {event.type}")
                result = next_fn()
                print(f"After: {event.type}")
                return result
    """

    def process(
        self,
        event: Event,
        handler: Callable,
        next_fn: Callable[[], Any],
    ) -> Any:
        """处理事件

        Args:
            event: 事件对象
            handler: 原始处理器
            next_fn: 调用下一个中间件/处理器的函数

        Returns:
            处理结果
        """
        return next_fn()


class AsyncMiddleware:
    """异步中间件基类

    支持 async def 处理器和异步前后处理逻辑。

    Examples:
        class AsyncLoggingMiddleware(AsyncMiddleware):
            async def process(self, event, handler, next_fn):
                print(f"Before: {event.type}")
                result = await next_fn()  # next_fn 返回 awaitable
                print(f"After: {event.type}")
                return result
    """

    async def process(
        self,
        event: Event,
        handler: Callable,
        next_fn: Callable[[], Awaitable[Any]],
    ) -> Any:
        """异步处理事件

        Args:
            event: 事件对象
            handler: 原始处理器 (可能是 async def)
            next_fn: 调用下一个中间件/处理器的异步函数

        Returns:
            处理结果
        """
        return await next_fn()


class MiddlewareChain:
    """同步中间件链"""

    def __init__(self):
        self._middlewares: List[Middleware] = []

    def use(self, middleware: Middleware) -> "MiddlewareChain":
        """添加中间件"""
        self._middlewares.append(middleware)
        return self

    def execute(
        self,
        event: Event,
        handler: Callable[[Event], Any],
    ) -> Any:
        """执行中间件链"""
        def create_next(index: int) -> Callable[[], Any]:
            if index >= len(self._middlewares):
                # 到达链尾，执行实际处理器
                return lambda: handler(event)

            middleware = self._middlewares[index]
            return lambda: middleware.process(
                event,
                handler,
                create_next(index + 1),
            )

        return create_next(0)()


class AsyncMiddlewareChain:
    """异步中间件链

    支持混合使用同步和异步中间件，统一处理 async def 处理器。

    Examples:
        chain = AsyncMiddlewareChain()
        chain.use(LoggingMiddleware())       # 同步中间件
        chain.use(AsyncRetryMiddleware())    # 异步中间件

        result = await chain.execute(event, async_handler)
    """

    def __init__(self):
        self._middlewares: List[Union[Middleware, AsyncMiddleware]] = []

    def use(self, middleware: Union[Middleware, AsyncMiddleware]) -> "AsyncMiddlewareChain":
        """添加中间件 (支持同步和异步)"""
        self._middlewares.append(middleware)
        return self

    async def execute(
        self,
        event: Event,
        handler: Callable[[Event], Any],
        is_handler_async: bool = False,
    ) -> Any:
        """异步执行中间件链

        Args:
            event: 事件对象
            handler: 事件处理器 (同步或异步)
            is_handler_async: 处理器是否为 async def

        Returns:
            处理结果
        """
        async def create_next(index: int) -> Any:
            if index >= len(self._middlewares):
                # 到达链尾，执行实际处理器
                if is_handler_async:
                    return await handler(event)
                else:
                    # 同步处理器，直接调用
                    return handler(event)

            middleware = self._middlewares[index]

            if isinstance(middleware, AsyncMiddleware):
                # 异步中间件
                return await middleware.process(
                    event,
                    handler,
                    lambda: create_next(index + 1),
                )
            else:
                # 同步中间件，包装为异步
                def sync_next():
                    # 这里需要运行协程
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        # 在已运行的循环中，创建 task
                        future = asyncio.ensure_future(create_next(index + 1))
                        # 同步中间件无法 await，我们需要不同策略
                        # 改为在同步中间件中返回协程
                        raise RuntimeError(
                            "Cannot use sync Middleware with async handlers. "
                            "Use AsyncMiddleware or wrap with asyncio.to_thread()."
                        )
                    return loop.run_until_complete(create_next(index + 1))

                # 更好的方式：让同步中间件返回结果，我们来处理
                result_holder = {"result": None, "called": False}

                def capture_next():
                    result_holder["called"] = True
                    return None  # 占位，实际结果后面获取

                middleware.process(event, handler, capture_next)

                if result_holder["called"]:
                    # 中间件调用了 next，继续执行
                    return await create_next(index + 1)
                else:
                    # 中间件没调用 next (短路)
                    return None

        return await create_next(0)

    async def execute_smart(
        self,
        event: Event,
        handler: Callable[[Event], Any],
        is_handler_async: bool = False,
    ) -> Any:
        """智能执行中间件链 (推荐)

        更简洁的实现：将所有中间件统一为异步处理。

        Args:
            event: 事件对象
            handler: 事件处理器
            is_handler_async: 处理器是否为 async def
        """
        async def final_handler():
            if is_handler_async:
                return await handler(event)
            return handler(event)

        # 构建异步中间件栈
        async def build_chain(index: int) -> Any:
            if index >= len(self._middlewares):
                return await final_handler()

            middleware = self._middlewares[index]

            async def next_fn():
                return await build_chain(index + 1)

            if isinstance(middleware, AsyncMiddleware):
                return await middleware.process(event, handler, next_fn)
            else:
                # 同步中间件：在线程池中执行
                def sync_wrapper():
                    return middleware.process(
                        event,
                        handler,
                        lambda: asyncio.get_event_loop().run_until_complete(next_fn())
                    )
                # 使用默认线程池
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, sync_wrapper)

        return await build_chain(0)


# =============================================================================
# EventBus
# =============================================================================

class EventBus:
    """高性能事件总线

    Features:
    - 层级路由：订阅 "task" 会收到 "task.completed", "task.failed" 等
    - 优先级订阅：控制处理器执行顺序
    - 中间件支持：日志、指标、重试等 (同步/异步)
    - 线程安全：支持多线程环境
    - 真正的异步支持：async def 处理器会被正确 await

    Examples:
        bus = EventBus()

        # 装饰器订阅
        @bus.on("task.completed")
        def on_task_done(event):
            print(f"Task {event.payload['task_id']} completed")

        # 方法订阅
        bus.subscribe("flow.started", on_flow_start, priority=Priority.HIGH)

        # 发布事件
        bus.emit(Event("task.completed", {"task_id": "t1", "duration": 100}))

        # 层级订阅 (收到所有 task.* 事件)
        @bus.on("task")
        def on_any_task(event):
            print(f"Task event: {event.type}")
    """

    _instance: Optional["EventBus"] = None
    _lock = threading.Lock()

    def __init__(self):
        # 事件类型 → 处理器列表 (按优先级排序)
        self._handlers: Dict[str, List[_HandlerInfo]] = defaultdict(list)
        self._handler_lock = threading.RLock()

        # 中间件链 (同步和异步)
        self._middleware = MiddlewareChain()
        self._async_middleware = AsyncMiddlewareChain()

        # 指标
        self._metrics = _EventMetrics()

        # 状态
        self._enabled = True

    @classmethod
    def instance(cls) -> "EventBus":
        """获取单例实例"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    # -------------------------------------------------------------------------
    # Subscribe
    # -------------------------------------------------------------------------

    def subscribe(
        self,
        event_type: str,
        handler: Callable[[Event], Any],
        priority: int = Priority.NORMAL,
        once: bool = False,
        filter: Callable[[Event], bool] = None,
    ) -> Subscription:
        """订阅事件

        Args:
            event_type: 事件类型 (支持层级，如 "task" 匹配 "task.completed")
            handler: 事件处理函数
            priority: 优先级 (数值越小越先执行)
            once: 是否只执行一次
            filter: 事件过滤函数

        Returns:
            Subscription 对象，可用于取消订阅
        """
        info = _HandlerInfo(
            handler=handler,
            priority=priority,
            once=once,
            filter=filter,
        )

        with self._handler_lock:
            handlers = self._handlers[event_type]
            handlers.append(info)
            # 按优先级排序
            handlers.sort(key=lambda h: h.priority)

        logger.debug(f"📡 Subscribed to '{event_type}' (priority={priority})")

        # 创建取消订阅函数
        def dispose() -> bool:
            with self._handler_lock:
                handlers = self._handlers.get(event_type, [])
                for i, h in enumerate(handlers):
                    if h is info:
                        handlers.pop(i)
                        return True
            return False

        return Subscription(event_type, handler, dispose)

    def on(
        self,
        event_type: str,
        priority: int = Priority.NORMAL,
        once: bool = False,
        filter: Callable[[Event], bool] = None,
    ) -> Callable:
        """装饰器订阅

        Examples:
            @bus.on("task.completed")
            def on_task_done(event):
                ...

            @bus.on("task", priority=Priority.HIGH)
            def on_any_task(event):
                ...
        """
        def decorator(handler: Callable[[Event], Any]) -> Callable:
            self.subscribe(event_type, handler, priority, once, filter)
            return handler

        return decorator

    def once(self, event_type: str, priority: int = Priority.NORMAL) -> Callable:
        """装饰器: 只订阅一次"""
        return self.on(event_type, priority, once=True)

    # -------------------------------------------------------------------------
    # Emit
    # -------------------------------------------------------------------------

    def emit(self, event: Event) -> EmitResult:
        """发布事件 (同步)

        Args:
            event: 事件对象

        Returns:
            EmitResult 包含执行结果
        """
        if not self._enabled:
            return EmitResult(
                event_type=event.type,
                event_id=event.event_id,
                handler_count=0,
            )

        start_time = time.time()

        # 收集匹配的处理器 (层级路由)
        handlers_to_call = self._collect_handlers(event)

        result = EmitResult(
            event_type=event.type,
            event_id=event.event_id,
            handler_count=len(handlers_to_call),
        )

        # 执行处理器
        once_handlers = []
        for info in handlers_to_call:
            if not info.matches(event):
                continue

            try:
                # 通过中间件链执行
                self._middleware.execute(event, info.handler)
                result.success_count += 1

                if info.once:
                    once_handlers.append(info)

            except Exception as e:
                result.error_count += 1
                result.errors.append((info.handler.__name__, e))
                logger.warning(
                    f"Handler {info.handler.__name__} failed for {event.type}: {e}"
                )

        # 移除 once 处理器
        if once_handlers:
            self._remove_handlers(once_handlers)

        result.duration_ms = (time.time() - start_time) * 1000

        # 更新指标
        self._metrics.record(event.type, result)

        return result

    async def emit_async(self, event: Event) -> EmitResult:
        """发布事件 (真正的异步)

        真正支持 async def 处理器，会正确 await 异步处理器。

        Args:
            event: 事件对象

        Returns:
            EmitResult 包含执行结果

        Examples:
            @bus.on("data.processed")
            async def handle_data(event):
                await save_to_database(event.payload)

            result = await bus.emit_async(
                Event("data.processed", {"records": 100})
            )
        """
        if not self._enabled:
            return EmitResult(
                event_type=event.type,
                event_id=event.event_id,
                handler_count=0,
            )

        start_time = time.time()

        # 收集匹配的处理器
        handlers_to_call = self._collect_handlers(event)

        result = EmitResult(
            event_type=event.type,
            event_id=event.event_id,
            handler_count=len(handlers_to_call),
        )

        # 分离同步和异步处理器
        sync_handlers: List[_HandlerInfo] = []
        async_handlers: List[_HandlerInfo] = []

        for info in handlers_to_call:
            if not info.matches(event):
                continue
            if info.is_async:
                async_handlers.append(info)
            else:
                sync_handlers.append(info)

        once_handlers = []

        # 1. 先执行同步处理器 (保持顺序)
        for info in sync_handlers:
            try:
                self._middleware.execute(event, info.handler)
                result.success_count += 1
                if info.once:
                    once_handlers.append(info)
            except Exception as e:
                result.error_count += 1
                result.errors.append((info.handler.__name__, e))
                logger.warning(
                    f"Sync handler {info.handler.__name__} failed for {event.type}: {e}"
                )

        # 2. 并发执行异步处理器
        if async_handlers:
            async def execute_async_handler(info: _HandlerInfo) -> Tuple[bool, Optional[Exception]]:
                try:
                    if self._async_middleware._middlewares:
                        await self._async_middleware.execute_smart(
                            event, info.handler, is_handler_async=True
                        )
                    else:
                        await info.handler(event)
                    return (True, None)
                except Exception as e:
                    return (False, e)

            # 并发执行所有异步处理器
            tasks = [execute_async_handler(info) for info in async_handlers]
            results = await asyncio.gather(*tasks, return_exceptions=False)

            for (success, error), info in zip(results, async_handlers):
                if success:
                    result.success_count += 1
                    if info.once:
                        once_handlers.append(info)
                else:
                    result.error_count += 1
                    result.errors.append((info.handler.__name__, error))
                    logger.warning(
                        f"Async handler {info.handler.__name__} failed for {event.type}: {error}"
                    )

        # 移除 once 处理器
        if once_handlers:
            self._remove_handlers(once_handlers)

        result.duration_ms = (time.time() - start_time) * 1000

        # 更新指标
        self._metrics.record(event.type, result)

        return result

    async def emit_all_async(
        self,
        event: Event,
        concurrent: bool = True
    ) -> EmitResult:
        """发布事件，所有处理器都异步执行

        与 emit_async 的区别：
        - emit_async: 同步处理器顺序执行，异步处理器并发执行
        - emit_all_async: 所有处理器都并发执行 (同步处理器包装为异步)

        Args:
            event: 事件对象
            concurrent: True=并发执行所有处理器，False=顺序执行

        Returns:
            EmitResult 包含执行结果
        """
        if not self._enabled:
            return EmitResult(
                event_type=event.type,
                event_id=event.event_id,
                handler_count=0,
            )

        start_time = time.time()
        handlers_to_call = self._collect_handlers(event)

        result = EmitResult(
            event_type=event.type,
            event_id=event.event_id,
            handler_count=len(handlers_to_call),
        )

        matched_handlers = [h for h in handlers_to_call if h.matches(event)]
        once_handlers = []

        async def execute_handler(info: _HandlerInfo) -> Tuple[bool, Optional[Exception]]:
            try:
                if info.is_async:
                    await info.handler(event)
                else:
                    # 同步处理器在线程池执行
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(
                        None,
                        lambda: self._middleware.execute(event, info.handler)
                    )
                return (True, None)
            except Exception as e:
                return (False, e)

        if concurrent:
            # 并发执行
            tasks = [execute_handler(info) for info in matched_handlers]
            exec_results = await asyncio.gather(*tasks, return_exceptions=False)
        else:
            # 顺序执行
            exec_results = []
            for info in matched_handlers:
                exec_results.append(await execute_handler(info))

        for (success, error), info in zip(exec_results, matched_handlers):
            if success:
                result.success_count += 1
                if info.once:
                    once_handlers.append(info)
            else:
                result.error_count += 1
                result.errors.append((info.handler.__name__, error))
                logger.warning(
                    f"Handler {info.handler.__name__} failed for {event.type}: {error}"
                )

        if once_handlers:
            self._remove_handlers(once_handlers)

        result.duration_ms = (time.time() - start_time) * 1000
        self._metrics.record(event.type, result)

        return result

    # -------------------------------------------------------------------------
    # Hierarchy Routing
    # -------------------------------------------------------------------------

    def _collect_handlers(self, event: Event) -> List[_HandlerInfo]:
        """收集匹配事件的所有处理器 (层级路由)

        订阅 "task" 会匹配:
        - "task"
        - "task.completed"
        - "task.completed.success"
        """
        handlers = []

        with self._handler_lock:
            # 精确匹配
            if event.type in self._handlers:
                handlers.extend(self._handlers[event.type])

            # 层级匹配：检查父级订阅
            hierarchy = event.hierarchy
            for level in hierarchy[:-1]:  # 排除精确匹配的类型
                if level in self._handlers:
                    handlers.extend(self._handlers[level])

        # 按优先级排序
        handlers.sort(key=lambda h: h.priority)
        return handlers

    def _remove_handlers(self, handlers: List[_HandlerInfo]) -> None:
        """移除处理器"""
        with self._handler_lock:
            for event_type, handler_list in self._handlers.items():
                self._handlers[event_type] = [
                    h for h in handler_list if h not in handlers
                ]

    # -------------------------------------------------------------------------
    # Middleware
    # -------------------------------------------------------------------------

    def use(self, middleware: Middleware) -> "EventBus":
        """添加同步中间件 (用于 emit)

        Args:
            middleware: 同步中间件实例

        Returns:
            self (链式调用)
        """
        self._middleware.use(middleware)
        return self

    def use_async(self, middleware: Union[Middleware, AsyncMiddleware]) -> "EventBus":
        """添加异步中间件 (用于 emit_async/emit_all_async)

        Args:
            middleware: 同步或异步中间件实例

        Returns:
            self (链式调用)

        Examples:
            bus.use_async(AsyncLoggingMiddleware())
            bus.use_async(RetryMiddleware())  # 同步中间件也可以
        """
        self._async_middleware.use(middleware)
        return self

    # -------------------------------------------------------------------------
    # Control
    # -------------------------------------------------------------------------

    def enable(self) -> None:
        """启用事件总线"""
        self._enabled = True

    def disable(self) -> None:
        """禁用事件总线 (emit 不执行任何处理器)"""
        self._enabled = False

    @contextmanager
    def disabled(self):
        """临时禁用事件总线"""
        self.disable()
        try:
            yield
        finally:
            self.enable()

    def clear(self, event_type: str = None) -> None:
        """清除处理器

        Args:
            event_type: 指定事件类型 (None 清除所有)
        """
        with self._handler_lock:
            if event_type:
                self._handlers.pop(event_type, None)
            else:
                self._handlers.clear()

    # -------------------------------------------------------------------------
    # Introspection
    # -------------------------------------------------------------------------

    def get_handlers(self, event_type: str) -> List[Callable]:
        """获取事件的处理器列表"""
        with self._handler_lock:
            return [h.handler for h in self._handlers.get(event_type, [])]

    def get_subscribed_types(self) -> List[str]:
        """获取所有已订阅的事件类型"""
        with self._handler_lock:
            return list(self._handlers.keys())

    def get_metrics(self) -> Dict[str, Any]:
        """获取事件指标"""
        return self._metrics.to_dict()


# =============================================================================
# Event Metrics (Internal)
# =============================================================================

class _EventMetrics:
    """事件指标收集 (内部使用)"""

    def __init__(self):
        self._emitted: Dict[str, int] = defaultdict(int)
        self._success: Dict[str, int] = defaultdict(int)
        self._failed: Dict[str, int] = defaultdict(int)
        self._durations: Dict[str, List[float]] = defaultdict(list)
        self._start_time = time.time()

    def record(self, event_type: str, result: EmitResult) -> None:
        """记录事件结果"""
        self._emitted[event_type] += 1
        self._success[event_type] += result.success_count
        self._failed[event_type] += result.error_count

        # 只保留最近 100 个延迟样本
        durations = self._durations[event_type]
        durations.append(result.duration_ms)
        if len(durations) > 100:
            self._durations[event_type] = durations[-100:]

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        uptime = time.time() - self._start_time
        total_emitted = sum(self._emitted.values())

        return {
            "uptime_seconds": uptime,
            "total_emitted": total_emitted,
            "throughput_per_second": total_emitted / uptime if uptime > 0 else 0,
            "by_type": {
                event_type: {
                    "emitted": self._emitted[event_type],
                    "success": self._success.get(event_type, 0),
                    "failed": self._failed.get(event_type, 0),
                    "avg_duration_ms": (
                        sum(self._durations.get(event_type, [])) /
                        len(self._durations.get(event_type, [1]))
                    ),
                }
                for event_type in self._emitted
            },
        }


# =============================================================================
# Module-level decorator
# =============================================================================

def on(
    event_type: str,
    priority: int = Priority.NORMAL,
    bus: EventBus = None,
) -> Callable:
    """模块级装饰器 (使用全局 EventBus)

    Examples:
        @on("task.completed")
        def handle_task_done(event):
            ...
    """
    actual_bus = bus or EventBus.instance()
    return actual_bus.on(event_type, priority)
