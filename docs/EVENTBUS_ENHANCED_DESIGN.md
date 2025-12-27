# 🚀 AStock EventBus 增强架构设计方案

> **版本**: 6.0.0
> **日期**: 2025-12-27
> **状态**: ✅ 已实现
> **实现位置**: `shared/event_bus_v6/`
> **参考**: Pluggy, Guava EventBus, Reactor Core, RxPY, Spring Events

---

## 📊 当前实现分析

### ✅ 现有优势
| 特性 | 实现 | 评价 |
|------|------|------|
| 单例模式 | `EventBus.get()` | ✅ 线程安全 |
| 优先级队列 | `EventPriority` 枚举 | ✅ 5级优先级 |
| 事件过滤 | `filter_fn` 参数 | ✅ 条件订阅 |
| Wrapper 拦截器 | `call_with_wrappers` | ✅ 类似 Pluggy |
| 统计监控 | `_stats` + `_event_log` | ✅ 调试友好 |
| 通配符匹配 | `pipeline.*` 模式 | ✅ 灵活订阅 |

### ⚠️ 待增强领域
| 领域 | 现状 | 业界最佳实践 |
|------|------|--------------|
| 类型安全 | 弱类型 Event | **Pluggy**: HookSpec 签名验证 |
| 异步支持 | 可选 `emit_async` | **Reactor**: 原生 async/await |
| 死信处理 | ❌ 无 | **Guava**: DeadEvent 重投递 |
| Historic 模式 | ❌ 无 | **Pluggy**: `call_historic` |
| 背压控制 | ❌ 无 | **Reactor**: Backpressure 策略 |
| 取消订阅 | 简单移除 | **RxPY**: Disposable 模式 |
| 可观测性 | 基础统计 | **OpenTelemetry**: Tracing |
| 中间件管道 | Wrapper 模式 | **Koa/Express**: 洋葱模型增强 |

---

## 🏗️ 增强架构设计

### 核心设计原则

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    AStock EventBus v6.0 Architecture                        │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        Core Layer                                    │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐   │   │
│  │  │EventBus │  │ Channel │  │ Topic   │  │Scheduler│  │  Store  │   │   │
│  │  │ (核心)  │  │ (分区)  │  │ (路由)  │  │ (调度)  │  │ (持久) │   │   │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘  └─────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│  ┌─────────────────────────────────┼───────────────────────────────────┐   │
│  │                        Middleware Pipeline                           │   │
│  │  ┌───────┐    ┌───────┐    ┌───────┐    ┌───────┐    ┌───────┐     │   │
│  │  │Tracing│ -> │Logging│ -> │Validate│ -> │Retry  │ -> │Timeout│     │   │
│  │  └───────┘    └───────┘    └───────┘    └───────┘    └───────┘     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│  ┌─────────────────────────────────┼───────────────────────────────────┐   │
│  │                        Handler Layer                                 │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │   │
│  │  │ Sync Handler │  │Async Handler │  │Stream Handler│               │   │
│  │  │  (同步处理)   │  │  (异步处理)  │  │  (流式处理)  │               │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🔧 增强模块设计

### 1️⃣ 类型安全的 HookSpec 系统 (参考 Pluggy)

```python
# shared/event_bus_v6/specs.py

from typing import TypeVar, Generic, Protocol, runtime_checkable
from dataclasses import dataclass

E = TypeVar('E', bound='Event')

@runtime_checkable
class EventSpec(Protocol[E]):
    """事件规格协议 - 定义事件的类型签名"""

    @property
    def event_type(self) -> str: ...

    def validate(self, event: E) -> bool: ...


@dataclass(frozen=True)
class HookSpec:
    """钩子规格定义 (类似 Pluggy HookspecMarker)"""
    name: str
    firstresult: bool = False      # 首个结果即返回
    historic: bool = False         # 历史事件重放
    warn_on_impl: bool = False     # 实现警告

    # 参数签名验证
    required_args: tuple = ()
    optional_args: tuple = ()


class HookSpecRegistry:
    """钩子规格注册表"""
    _specs: dict[str, HookSpec] = {}

    @classmethod
    def define(cls, name: str, **opts) -> HookSpec:
        """定义钩子规格"""
        spec = HookSpec(name=name, **opts)
        cls._specs[name] = spec
        return spec

    @classmethod
    def validate_handler(cls, event_type: str, handler: Callable) -> tuple[bool, list[str]]:
        """验证处理器签名是否符合规格"""
        if event_type not in cls._specs:
            return True, []

        spec = cls._specs[event_type]
        sig = inspect.signature(handler)
        errors = []

        for arg in spec.required_args:
            if arg not in sig.parameters:
                errors.append(f"Missing required argument: {arg}")

        return len(errors) == 0, errors


# 预定义规格
PIPELINE_NODE_SPEC = HookSpecRegistry.define(
    "pipeline.node.execute",
    firstresult=False,
    required_args=('step_name', 'inputs'),
    optional_args=('context',)
)

REGISTRY_METHOD_SPEC = HookSpecRegistry.define(
    "registry.method.registered",
    historic=True,  # 新订阅者会收到历史注册事件
    required_args=('component', 'method', 'engine_type')
)
```

### 2️⃣ 死信处理 (参考 Guava DeadEvent)

```python
# shared/event_bus_v6/dead_letter.py

@dataclass
class DeadEvent(Event):
    """死信事件 - 无订阅者的事件"""
    original_event: Event
    reason: str = "no_subscribers"
    attempted_at: str = field(default_factory=lambda: datetime.now().isoformat())
    retry_count: int = 0

    @property
    def event_type(self) -> str:
        return "system.dead_letter"


class DeadLetterQueue:
    """死信队列"""

    def __init__(self, max_size: int = 1000, ttl_seconds: int = 3600):
        self._queue: deque[DeadEvent] = deque(maxlen=max_size)
        self._ttl = ttl_seconds
        self._handlers: list[Callable[[DeadEvent], None]] = []

    def enqueue(self, event: Event, reason: str = "no_subscribers"):
        """入队死信"""
        dead_event = DeadEvent(
            original_event=event,
            reason=reason,
            source="dead_letter_queue"
        )
        self._queue.append(dead_event)

        # 通知死信处理器
        for handler in self._handlers:
            try:
                handler(dead_event)
            except Exception as e:
                logger.warning(f"Dead letter handler failed: {e}")

    def on_dead_letter(self, handler: Callable[[DeadEvent], None]):
        """注册死信处理器"""
        self._handlers.append(handler)

    def retry_all(self, bus: 'EventBus') -> int:
        """重试所有死信"""
        count = 0
        while self._queue:
            dead = self._queue.popleft()
            if dead.retry_count < 3:  # 最多重试3次
                dead.retry_count += 1
                bus.emit(dead.original_event)
                count += 1
        return count
```

### 3️⃣ Historic 模式 (参考 Pluggy call_historic)

```python
# shared/event_bus_v6/historic.py

class HistoricEventStore:
    """历史事件存储 - 支持新订阅者接收历史事件"""

    def __init__(self, max_events_per_type: int = 100):
        self._history: dict[str, deque[Event]] = defaultdict(
            lambda: deque(maxlen=max_events_per_type)
        )
        self._historic_types: set[str] = set()

    def mark_historic(self, event_type: str):
        """标记事件类型为历史模式"""
        self._historic_types.add(event_type)

    def is_historic(self, event_type: str) -> bool:
        return event_type in self._historic_types

    def store(self, event: Event):
        """存储历史事件"""
        if self.is_historic(event.event_type):
            self._history[event.event_type].append(event)

    def replay(self, event_type: str, handler: Callable):
        """为新订阅者重放历史事件"""
        if event_type in self._history:
            for event in self._history[event_type]:
                try:
                    handler(event)
                except Exception as e:
                    logger.warning(f"Historic replay failed: {e}")
```

### 4️⃣ 异步增强 (参考 Reactor Core)

```python
# shared/event_bus_v6/async_support.py

import asyncio
from typing import AsyncIterator
from contextlib import asynccontextmanager

class AsyncEventBus:
    """异步事件总线"""

    def __init__(self, bus: 'EventBus'):
        self._bus = bus
        self._async_handlers: dict[str, list[Callable]] = defaultdict(list)
        self._streams: dict[str, asyncio.Queue] = {}

    async def emit_async(self, event: Event, timeout: float = 30.0) -> EmitResult:
        """异步发布事件"""
        handlers = self._async_handlers.get(event.event_type, [])

        tasks = [
            asyncio.create_task(self._call_async_handler(h, event))
            for h in handlers
        ]

        if not tasks:
            # 回退到同步处理
            return self._bus.emit(event)

        done, pending = await asyncio.wait(
            tasks,
            timeout=timeout,
            return_when=asyncio.ALL_COMPLETED
        )

        # 取消超时任务
        for task in pending:
            task.cancel()

        errors = []
        success = 0
        for task in done:
            try:
                task.result()
                success += 1
            except Exception as e:
                errors.append((task.get_name(), e))

        return EmitResult(
            event_type=event.event_type,
            handler_count=len(tasks),
            success_count=success,
            error_count=len(errors),
            total_time_ms=0,  # TODO: 计时
            errors=errors
        )

    def subscribe_stream(self, event_type: str) -> asyncio.Queue:
        """订阅事件流 (Reactive Streams 风格)"""
        if event_type not in self._streams:
            queue = asyncio.Queue(maxsize=1000)
            self._streams[event_type] = queue

            # 注册转发处理器
            self._bus.on(event_type, lambda e: queue.put_nowait(e))

        return self._streams[event_type]

    async def iter_events(self, event_type: str) -> AsyncIterator[Event]:
        """异步迭代事件流"""
        queue = self.subscribe_stream(event_type)
        while True:
            event = await queue.get()
            yield event

    @asynccontextmanager
    async def batch_context(self, event_types: list[str], timeout: float = 60.0):
        """批量等待多个事件 (解决 Guava 提到的问题)"""
        received = {}
        queues = {et: self.subscribe_stream(et) for et in event_types}

        async def collector():
            tasks = {
                et: asyncio.create_task(q.get())
                for et, q in queues.items()
            }
            done, _ = await asyncio.wait(
                tasks.values(),
                timeout=timeout,
                return_when=asyncio.ALL_COMPLETED
            )
            for et, task in tasks.items():
                if task in done:
                    received[et] = task.result()

        yield received, collector
```

### 5️⃣ 中间件管道 (参考 Koa/Express)

```python
# shared/event_bus_v6/middleware.py

from abc import ABC, abstractmethod
from typing import Callable, Any, Optional
from contextlib import contextmanager
import time

class Middleware(ABC):
    """中间件基类"""

    @abstractmethod
    def __call__(self, event: Event, next_fn: Callable) -> Any:
        """处理事件并调用下一个中间件"""
        pass


class TracingMiddleware(Middleware):
    """OpenTelemetry 风格追踪中间件"""

    def __init__(self, service_name: str = "astock"):
        self.service_name = service_name
        self._traces: dict[str, dict] = {}

    def __call__(self, event: Event, next_fn: Callable) -> Any:
        trace_id = f"{event.event_id}_{time.time_ns()}"
        span = {
            'trace_id': trace_id,
            'event_type': event.event_type,
            'start_time': time.perf_counter(),
            'service': self.service_name,
            'attributes': {'source': event.source}
        }
        self._traces[trace_id] = span

        try:
            result = next_fn(event)
            span['status'] = 'ok'
            return result
        except Exception as e:
            span['status'] = 'error'
            span['error'] = str(e)
            raise
        finally:
            span['end_time'] = time.perf_counter()
            span['duration_ms'] = (span['end_time'] - span['start_time']) * 1000


class RetryMiddleware(Middleware):
    """重试中间件"""

    def __init__(self, max_retries: int = 3, delay: float = 0.1):
        self.max_retries = max_retries
        self.delay = delay

    def __call__(self, event: Event, next_fn: Callable) -> Any:
        last_error = None
        for attempt in range(self.max_retries + 1):
            try:
                return next_fn(event)
            except Exception as e:
                last_error = e
                if attempt < self.max_retries:
                    time.sleep(self.delay * (2 ** attempt))  # 指数退避
        raise last_error


class TimeoutMiddleware(Middleware):
    """超时中间件"""

    def __init__(self, timeout_seconds: float = 30.0):
        self.timeout = timeout_seconds

    def __call__(self, event: Event, next_fn: Callable) -> Any:
        import signal

        def handler(signum, frame):
            raise TimeoutError(f"Event handling timeout: {event.event_type}")

        # 设置超时
        old_handler = signal.signal(signal.SIGALRM, handler)
        signal.alarm(int(self.timeout))

        try:
            return next_fn(event)
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)


class MiddlewarePipeline:
    """中间件管道"""

    def __init__(self):
        self._middlewares: list[Middleware] = []

    def use(self, middleware: Middleware) -> 'MiddlewarePipeline':
        """添加中间件"""
        self._middlewares.append(middleware)
        return self

    def execute(self, event: Event, final_handler: Callable) -> Any:
        """执行中间件链"""
        def build_chain(middlewares: list[Middleware], handler: Callable) -> Callable:
            if not middlewares:
                return handler

            current = middlewares[0]
            rest = middlewares[1:]

            def chained(e: Event) -> Any:
                return current(e, build_chain(rest, handler))

            return chained

        chain = build_chain(self._middlewares, final_handler)
        return chain(event)
```

### 6️⃣ Disposable 订阅模式 (参考 RxPY)

```python
# shared/event_bus_v6/subscription.py

from abc import ABC, abstractmethod
from typing import Callable
import weakref

class Disposable(ABC):
    """可销毁订阅"""

    @abstractmethod
    def dispose(self) -> None:
        """取消订阅"""
        pass

    @property
    @abstractmethod
    def is_disposed(self) -> bool:
        """是否已销毁"""
        pass


class Subscription(Disposable):
    """订阅对象"""

    def __init__(
        self,
        event_type: str,
        handler: Callable,
        unsubscribe_fn: Callable[[], None]
    ):
        self._event_type = event_type
        self._handler = handler
        self._unsubscribe = unsubscribe_fn
        self._disposed = False

    def dispose(self) -> None:
        if not self._disposed:
            self._unsubscribe()
            self._disposed = True

    @property
    def is_disposed(self) -> bool:
        return self._disposed

    def __enter__(self) -> 'Subscription':
        return self

    def __exit__(self, *args) -> None:
        self.dispose()


class CompositeDisposable(Disposable):
    """组合订阅 - 批量管理多个订阅"""

    def __init__(self):
        self._subscriptions: list[Disposable] = []
        self._disposed = False

    def add(self, subscription: Disposable) -> 'CompositeDisposable':
        if self._disposed:
            subscription.dispose()
        else:
            self._subscriptions.append(subscription)
        return self

    def dispose(self) -> None:
        if not self._disposed:
            for sub in self._subscriptions:
                sub.dispose()
            self._subscriptions.clear()
            self._disposed = True

    @property
    def is_disposed(self) -> bool:
        return self._disposed
```

---

## 🎯 增强后的 EventBus 主类

```python
# shared/event_bus_v6/bus.py

class EventBusV6:
    """增强版事件总线 v6.0"""

    _instance: Optional['EventBusV6'] = None
    _lock = threading.RLock()

    def __init__(self, config: Optional['EventBusConfig'] = None):
        self.config = config or EventBusConfig()

        # 核心组件
        self._handlers: dict[str, list[HandlerInfo]] = defaultdict(list)
        self._middleware = MiddlewarePipeline()
        self._dead_letter = DeadLetterQueue()
        self._historic_store = HistoricEventStore()
        self._async_bus = AsyncEventBus(self)

        # 统计与监控
        self._stats = EventBusStats()
        self._tracer = TracingMiddleware()

        # 默认中间件
        if self.config.enable_tracing:
            self._middleware.use(self._tracer)
        if self.config.enable_retry:
            self._middleware.use(RetryMiddleware())

    @classmethod
    def get(cls) -> 'EventBusV6':
        """获取单例"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    # ==================== 订阅 API ====================

    def on(
        self,
        event: str,
        handler: Callable = None,
        *,
        priority: EventPriority = EventPriority.NORMAL,
        once: bool = False,
        filter_fn: Optional[Callable] = None,
        source: str = "",
        replay_historic: bool = True
    ) -> Union[Callable, Subscription]:
        """注册事件处理器，返回 Subscription 对象"""

        def register(fn: Callable) -> Subscription:
            # HookSpec 验证
            valid, errors = HookSpecRegistry.validate_handler(event, fn)
            if not valid and self.config.strict_spec:
                raise ValueError(f"Handler signature mismatch: {errors}")

            info = HandlerInfo(
                fn=fn,
                priority=priority,
                is_once=once,
                filter_fn=filter_fn,
                source=source
            )

            with self._lock:
                self._handlers[event].append(info)
                self._sort_handlers(event)

            # 历史事件重放
            if replay_historic and self._historic_store.is_historic(event):
                self._historic_store.replay(event, fn)

            # 返回 Subscription 对象
            def unsubscribe():
                self.off(event, fn)

            return Subscription(event, fn, unsubscribe)

        if handler is not None:
            return register(handler)
        return register

    def subscribe(self, event: str, **kwargs) -> Subscription:
        """显式订阅方法（别名）"""
        return self.on(event, **kwargs)

    # ==================== 发布 API ====================

    def emit(self, event: Union[str, Event], **kwargs) -> EmitResult:
        """同步发布事件"""
        if isinstance(event, str):
            event = SimpleEvent(event_type=event, **kwargs)

        # 存储历史事件
        self._historic_store.store(event)

        handlers = self._get_matching_handlers(event.event_type)

        # 无订阅者 -> 死信
        if not handlers and self.config.enable_dead_letter:
            self._dead_letter.enqueue(event)
            return EmitResult(event.event_type, 0, 0, 0, 0.0)

        # 通过中间件管道执行
        def execute_handlers(e: Event) -> EmitResult:
            return self._execute_handlers(e, handlers)

        return self._middleware.execute(event, execute_handlers)

    async def emit_async(self, event: Event, **kwargs) -> EmitResult:
        """异步发布事件"""
        return await self._async_bus.emit_async(event, **kwargs)

    def emit_historic(self, event: Event):
        """发布历史事件（新订阅者会收到）"""
        self._historic_store.mark_historic(event.event_type)
        return self.emit(event)

    # ==================== 流式 API ====================

    def stream(self, event_type: str) -> asyncio.Queue:
        """获取事件流"""
        return self._async_bus.subscribe_stream(event_type)

    async def iter(self, event_type: str) -> AsyncIterator[Event]:
        """异步迭代事件"""
        async for event in self._async_bus.iter_events(event_type):
            yield event

    # ==================== 中间件 API ====================

    def use(self, middleware: Middleware) -> 'EventBusV6':
        """添加中间件"""
        self._middleware.use(middleware)
        return self

    # ==================== 死信 API ====================

    def on_dead_letter(self, handler: Callable[[DeadEvent], None]):
        """注册死信处理器"""
        self._dead_letter.on_dead_letter(handler)

    def retry_dead_letters(self) -> int:
        """重试死信"""
        return self._dead_letter.retry_all(self)
```

---

## 📊 与现有实现的对比

| 特性 | 当前 v5.0 | 增强 v6.0 | 改进 |
|------|-----------|-----------|------|
| 类型安全 | ❌ 弱类型 | ✅ HookSpec 验证 | +签名检查 |
| 死信处理 | ❌ 丢弃 | ✅ DeadLetterQueue | +可恢复 |
| 历史重放 | ❌ 无 | ✅ HistoricEventStore | +新订阅者不丢消息 |
| 异步支持 | ⚠️ 基础 | ✅ AsyncEventBus + Stream | +Reactive 风格 |
| 中间件 | ✅ Wrapper | ✅ MiddlewarePipeline | +更灵活 |
| 订阅管理 | ⚠️ 手动 off | ✅ Subscription + Disposable | +资源自动释放 |
| 可观测性 | ⚠️ 基础统计 | ✅ TracingMiddleware | +OpenTelemetry |
| 重试机制 | ❌ 无 | ✅ RetryMiddleware | +指数退避 |
| 批量等待 | ❌ 无 | ✅ batch_context | +多事件聚合 |

---

## 🚀 迁移策略

### Phase 1: 兼容层 (向后兼容)
```python
# shared/event_bus.py 保持不变
# shared/event_bus_v6/ 新增

# 兼容适配器
class EventBusCompat:
    """v5 -> v6 兼容层"""

    def __init__(self):
        self._v6 = EventBusV6.get()
        self._v5_api = EventBus.get()

    def __getattr__(self, name):
        # 优先使用 v6，回退到 v5
        if hasattr(self._v6, name):
            return getattr(self._v6, name)
        return getattr(self._v5_api, name)
```

### Phase 2: 渐进式迁移
1. 新代码使用 `EventBusV6`
2. 旧代码通过兼容层继续工作
3. 逐步替换关键路径

### Phase 3: 完全切换
- 移除 v5 实现
- EventBusV6 重命名为 EventBus

---

## 📁 推荐目录结构

```
shared/
├── event_bus.py              # 当前实现 (保留)
├── events.py                 # 事件定义 (保留)
├── event_bus_v6/             # 增强实现
│   ├── __init__.py           # 导出
│   ├── bus.py                # 核心 EventBusV6
│   ├── specs.py              # HookSpec 系统
│   ├── dead_letter.py        # 死信处理
│   ├── historic.py           # 历史事件
│   ├── async_support.py      # 异步支持
│   ├── middleware.py         # 中间件管道
│   ├── subscription.py       # Disposable 订阅
│   ├── config.py             # 配置
│   └── compat.py             # 兼容层
```

---

## 🎯 实施建议

1. **优先实现**: DeadLetterQueue + HistoricEventStore (影响数据可靠性)
2. **其次实现**: MiddlewarePipeline + TracingMiddleware (提升可观测性)
3. **最后实现**: AsyncEventBus + Subscription (提升开发体验)

需要我开始实现哪个模块？
