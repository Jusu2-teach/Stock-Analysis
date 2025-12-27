<p align="center">
  <h1 align="center">🚀 EventBus</h1>
  <p align="center">
    <strong>企业级事件总线 | Enterprise Event Bus</strong>
  </p>
  <p align="center">
    整合多种优秀开源设计模式的生产级事件总线实现
  </p>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/version-6.0.0-blue.svg" alt="Version">
  <img src="https://img.shields.io/badge/python-3.9+-green.svg" alt="Python">
  <img src="https://img.shields.io/badge/license-MIT-orange.svg" alt="License">
  <img src="https://img.shields.io/badge/type_checked-mypy-blue.svg" alt="Type Checked">
  <img src="https://img.shields.io/badge/thread_safe-yes-brightgreen.svg" alt="Thread Safe">
</p>

---

## 📋 目录

- [特性概览](#-特性概览)
- [设计参考](#-设计参考)
- [快速开始](#-快速开始)
- [架构设计](#-架构设计)
- [核心概念](#-核心概念)
- [API 参考](#-api-参考)
- [中间件系统](#-中间件系统)
- [高级特性](#-高级特性)
- [系统集成](#-系统集成)
- [最佳实践](#-最佳实践)
- [配置选项](#️-配置选项)
- [调试与监控](#-调试与监控)
- [模块结构](#-模块结构)
- [变更日志](#-变更日志)
- [贡献指南](#-贡献指南)

---

## ✨ 特性概览

| 特性 | 描述 | 状态 |
|------|------|:----:|
| **发布/订阅** | 完全解耦的事件通信 | ✅ |
| **优先级队列** | 5级优先级控制执行顺序 | ✅ |
| **中间件管道** | 洋葱模型，可插拔扩展 | ✅ |
| **死信队列** | 无订阅者事件的捕获与重试 | ✅ |
| **历史重放** | 新订阅者接收历史事件 | ✅ |
| **类型安全** | HookSpec 签名验证 | ✅ |
| **订阅管理** | RxPY 风格 Disposable | ✅ |
| **异步支持** | async/await 原生支持 | ✅ |
| **熔断器** | 防止级联故障 | ✅ |
| **线程安全** | 生产级并发支持 | ✅ |

---

## 📦 设计参考

本项目借鉴了业界最佳实践：

| 特性 | 参考来源 | 说明 |
|------|----------|------|
| **HookSpec** | [pytest/pluggy](https://github.com/pytest-dev/pluggy) | 类型安全的事件规格定义 |
| **DeadLetterQueue** | [Google Guava EventBus](https://github.com/google/guava) | 无订阅者事件的死信处理 |
| **HistoricEvents** | [pluggy call_historic](https://pluggy.readthedocs.io/) | 历史事件存储与重放 |
| **Middleware** | [Express.js](https://expressjs.com/) / [Koa](https://koajs.com/) | 可插拔的中间件管道 (洋葱模型) |
| **Subscription** | [RxPY](https://github.com/ReactiveX/RxPY) | 可取消的订阅管理 (Disposable) |
| **CircuitBreaker** | [Netflix Hystrix](https://github.com/Netflix/Hystrix) | 熔断器模式 |
| **Async** | [Project Reactor](https://projectreactor.io/) | 响应式异步事件处理 |

---

## 🚀 快速开始

### 安装

EventBus 是 AStock 系统的内置组件，无需单独安装：

```python
from shared import EventBus
# 或者
from shared.event_bus import EventBus
```

### 30秒上手

```python
from shared import EventBus
from dataclasses import dataclass

# 1️⃣ 定义事件
@dataclass
class UserCreatedEvent:
    event_type: str = "user.created"
    user_id: str = ""
    username: str = ""

# 2️⃣ 获取事件总线
bus = EventBus.get()

# 3️⃣ 注册处理器
@bus.on("user.created")
def on_user_created(event):
    print(f"✅ 新用户: {event.username}")

# 4️⃣ 发布事件
result = bus.emit(UserCreatedEvent(user_id="001", username="张三"))
print(f"📤 触发了 {result.handler_count} 个处理器")
```

输出：
```
✅ 新用户: 张三
📤 触发了 1 个处理器
```

---

## 🏗️ 架构设计

### 整体架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         EventBus Architecture                               │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                        Core Layer (核心层)                           │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐   │   │
│  │  │EventBus │  │ Handlers│  │  Topic  │  │Scheduler│  │  Stats  │   │   │
│  │  │ (核心)  │  │ (处理器)│  │ (路由)  │  │ (调度)  │  │ (统计) │   │   │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘  └─────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│  ┌─────────────────────────────────┼───────────────────────────────────┐   │
│  │              Middleware Pipeline (中间件管道 - 洋葱模型)              │   │
│  │  ┌───────┐    ┌───────┐    ┌───────┐    ┌───────┐    ┌───────┐     │   │
│  │  │Logging│ -> │Tracing│ -> │Metrics│ -> │ Retry │ -> │Timeout│     │   │
│  │  └───────┘    └───────┘    └───────┘    └───────┘    └───────┘     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│  ┌─────────────────────────────────┼───────────────────────────────────┐   │
│  │                  Feature Modules (功能模块)                          │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐               │   │
│  │  │DeadLetterQueue│ │HistoricStore │  │ Subscription │               │   │
│  │  │  (死信队列)   │  │ (历史重放)   │  │  (订阅管理)  │               │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘               │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 事件处理流程

#### 1. 事件发布流程 (`emit`)

```
┌──────────┐     ┌───────────────┐     ┌──────────────┐     ┌──────────────┐
│  Caller  │────▶│   EventBus    │────▶│  Middleware  │────▶│   Handlers   │
│          │     │    emit()     │     │   Pipeline   │     │              │
└──────────┘     └───────────────┘     └──────────────┘     └──────────────┘
     │                  │                     │                     │
     │  emit(event)     │                     │                     │
     ├─────────────────▶│                     │                     │
     │                  │                     │                     │
     │                  │  1. 验证 event_type │                     │
     │                  │  2. 查找 handlers   │                     │
     │                  │  3. 检查是否有订阅者 │                     │
     │                  │                     │                     │
     │                  │ [无订阅者?]         │                     │
     │                  │────▶ DeadLetterQueue                      │
     │                  │                     │                     │
     │                  │ [有订阅者]          │                     │
     │                  ├────────────────────▶│                     │
     │                  │                     │                     │
     │                  │                     │ Logging → Tracing → │
     │                  │                     │ Metrics → Retry  ───┤
     │                  │                     │                     │
     │                  │                     │                     ▼
     │                  │                     │              ┌────────────┐
     │                  │                     │              │ Handler 1  │
     │                  │                     │              │ Handler 2  │
     │                  │                     │              │ Handler 3  │
     │                  │                     │              └────────────┘
     │                  │                     │                     │
     │                  │◀────────────────────┼─────────────────────┤
     │                  │                     │                     │
     │◀─────────────────┤ EmitResult          │                     │
     │   {handler_count,│                     │                     │
     │    success_count,│                     │                     │
     │    errors}       │                     │                     │
```

#### 2. 中间件执行流程 (洋葱模型)

```
                            Request Flow
                     ────────────────────────▶

    ┌─────────────────────────────────────────────────────────────┐
    │                                                             │
    │  ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐    │
    │  │         │   │         │   │         │   │         │    │
    │  │ Logging │──▶│ Tracing │──▶│ Metrics │──▶│ Handler │    │
    │  │         │   │         │   │         │   │         │    │
    │  │  pre()  │   │  pre()  │   │  pre()  │   │ execute │    │
    │  │         │◀──│         │◀──│         │◀──│         │    │
    │  │ post()  │   │ post()  │   │ post()  │   │         │    │
    │  │         │   │         │   │         │   │         │    │
    │  └─────────┘   └─────────┘   └─────────┘   └─────────┘    │
    │                                                             │
    └─────────────────────────────────────────────────────────────┘

                     ◀────────────────────────
                            Response Flow
```

#### 3. 历史事件重放

```
Timeline:
─────────────────────────────────────────────────────────────────────────────▶
     T1              T2              T3              T4
     │               │               │               │
     ▼               ▼               ▼               ▼
┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐
│emit(E1) │    │emit(E2) │    │emit(E3) │    │ 新订阅者 │
│         │    │         │    │         │    │ 注册    │
└─────────┘    └─────────┘    └─────────┘    └─────────┘
     │               │               │               │
     ▼               ▼               ▼               │
┌─────────────────────────────────────────┐         │
│         HistoricEventStore              │         │
│  ┌─────┐  ┌─────┐  ┌─────┐             │◀────────┤
│  │ E1  │  │ E2  │  │ E3  │             │  replay │
│  └─────┘  └─────┘  └─────┘             │ historic│
└─────────────────────────────────────────┘         │
                      │                             │
                      │   重放 E1, E2, E3           │
                      └────────────────────────────▶│
                                                    ▼
                                            ┌───────────────┐
                                            │  新订阅者收到  │
                                            │  所有历史事件  │
                                            └───────────────┘
```

#### 4. 系统集成全景图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         AStock System Event Flow                            │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   shared    │     │orchestrator │     │  pipeline   │     │   plugins   │
│  EventBus   │     │  Registry   │     │  Executor   │     │  Monitors   │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │                   │
 ══════╪═══════════════════╪═══════════════════╪═══════════════════╪══════════
       │            System Startup             │                   │
 ══════╪═══════════════════╪═══════════════════╪═══════════════════╪══════════
       │                   │                   │                   │
       │◀──────────────────┤                   │                   │
       │  emit(SystemReady)│                   │                   │
       │───────────────────┼──────────────────▶│──────────────────▶│
       │                   │                   │   [初始化完成]     │
       │                   │                   │                   │
 ══════╪═══════════════════╪═══════════════════╪═══════════════════╪══════════
       │          Method Registration          │                   │
 ══════╪═══════════════════╪═══════════════════╪═══════════════════╪══════════
       │                   │                   │                   │
       │◀──────────────────┤                   │                   │
       │emit(MethodRegistered)                 │                   │
       │  component: "business"                │                   │
       │  method: "analyze_truth"              │                   │
       │───────────────────┼──────────────────▶│──────────────────▶│
       │                   │                   │  [更新可用方法]    │
       │                   │                   │                   │
 ══════╪═══════════════════╪═══════════════════╪═══════════════════╪══════════
       │           Pipeline Execution          │                   │
 ══════╪═══════════════════╪═══════════════════╪═══════════════════╪══════════
       │                   │                   │                   │
       │◀──────────────────┼───────────────────┤                   │
       │                   │  emit(PipelineStarted)                │
       │───────────────────┼───────────────────┼──────────────────▶│
       │                   │                   │    [启动计时器]    │
       │                   │                   │                   │
       │                   │  ┌────────────────┴────────────────┐  │
       │◀──────────────────┼──┤  for each step:                 │  │
       │                   │  │    emit(NodeStarted)            │──┼──▶[记录]
       │                   │  │    execute_step()               │  │
       │◀──────────────────┼──┤    emit(NodeCompleted)          │──┼──▶[指标]
       │                   │  └────────────────┬────────────────┘  │
       │                   │                   │                   │
       │◀──────────────────┼───────────────────┤                   │
       │                   │  emit(PipelineCompleted)              │
       │───────────────────┼───────────────────┼──────────────────▶│
       │                   │                   │   [生成报告]      │
```

---

## 🎯 核心概念

### 事件 (Event)

事件是系统中的基本通信单元，使用 `dataclass` 定义：

```python
from dataclasses import dataclass, field
import time

@dataclass
class MyEvent:
    """自定义事件"""
    event_type: str = "my.namespace.event"  # 必须：事件类型标识
    data: str = ""                           # 业务数据
    timestamp: float = field(default_factory=time.time)  # 时间戳
```

**事件命名规范：**

```python
# ✅ 推荐：使用点分命名空间
"user.created"
"pipeline.node.started"
"data.transform.completed"

# ❌ 避免：不清晰的命名
"userCreated"
"evt1"
"handle_data"
```

### 处理器 (Handler)

处理器是响应事件的函数：

```python
# 装饰器方式
@bus.on("user.created")
def handle_user_created(event):
    print(f"处理用户: {event.user_id}")

# 函数调用方式
bus.on("user.created", lambda e: print(e))

# 带优先级
@bus.on("data.saved", priority=EventPriority.HIGH)
def high_priority_handler(event):
    pass

# 一次性处理器
@bus.once("app.started")
def run_once(event):
    pass
```

### 优先级 (Priority)

```python
from shared.event_bus import EventPriority

class EventPriority(Enum):
    SYSTEM = 0      # 系统级（日志、监控）最先执行
    HIGH = 25       # 高优先级
    NORMAL = 50     # 默认优先级
    LOW = 75        # 低优先级
    LAST = 100      # 最后执行（清理、统计）
```

---

## 📚 API 参考

### EventBus 类

#### 获取实例

```python
# 获取单例
bus = EventBus.get()

# 带配置
config = EventBusConfig(enable_middleware=True)
bus = EventBus.get(config)

# 重置单例（测试用）
EventBus.reset()
```

#### 订阅方法

| 方法 | 说明 | 返回值 |
|------|------|--------|
| `on(event_type, handler, **kwargs)` | 注册处理器 | `Subscription` |
| `once(event_type, handler, **kwargs)` | 注册一次性处理器 | `Subscription` |
| `off(event_type, handler)` | 注销处理器 | `bool` |
| `subscribe(event_type, handler, **kwargs)` | 订阅（返回可取消对象） | `Subscription` |

```python
# on() - 永久订阅
@bus.on("event.type")
def handler(event): ...

# once() - 单次订阅
@bus.once("event.type")
def one_time_handler(event): ...

# subscribe() - 返回可取消订阅
sub = bus.subscribe("event.type", handler)
sub.dispose()  # 取消订阅

# off() - 手动注销
bus.off("event.type", handler)
```

#### 发布方法

| 方法 | 说明 | 返回值 |
|------|------|--------|
| `emit(event, **kwargs)` | 发布事件 | `EmitResult` |
| `emit_async(event, **kwargs)` | 异步发布 | `AsyncEmitResult` |

```python
# 同步发布
result = bus.emit(MyEvent(data="hello"))
print(f"处理器数: {result.handler_count}")
print(f"成功数: {result.success_count}")
print(f"错误: {result.errors}")

# 异步发布
result = await bus.emit_async(MyEvent())
```

#### 查询方法

| 方法 | 说明 | 返回值 |
|------|------|--------|
| `has_handlers(event_type)` | 检查是否有处理器 | `bool` |
| `handler_count(event_type)` | 获取处理器数量 | `int` |
| `get_stats()` | 获取统计信息 | `EventBusStats` |

#### 管理方法

| 方法 | 说明 |
|------|------|
| `use(middleware)` | 添加中间件 |
| `clear(event_type=None)` | 清空处理器 |
| `cleanup()` | 清理资源 |

### EmitResult 类

```python
@dataclass
class EmitResult:
    event_type: str          # 事件类型
    handler_count: int       # 处理器总数
    success_count: int       # 成功数
    error_count: int         # 错误数
    total_time_ms: float     # 总耗时
    errors: List[tuple]      # 错误列表 [(handler_name, exception)]
```

### EventBusStats 类

```python
stats = bus.get_stats()

stats.total_handlers      # 总处理器数
stats.total_emits         # 总发布次数
stats.total_handlers_called  # 总调用次数
stats.total_errors        # 总错误数
stats.uptime_seconds      # 运行时间
stats.event_types         # 事件类型列表
```

---

## 🔧 中间件系统

### 内置中间件

| 中间件 | 说明 | 用途 |
|--------|------|------|
| `LoggingMiddleware` | 日志记录 | 调试、审计 |
| `TracingMiddleware` | 分布式追踪 | APM、链路追踪 |
| `MetricsMiddleware` | 性能指标 | 监控、告警 |
| `RetryMiddleware` | 自动重试 | 容错、可靠性 |
| `TimeoutMiddleware` | 超时控制 | 防止阻塞 |
| `ValidationMiddleware` | 事件验证 | 数据校验 |
| `CircuitBreakerMiddleware` | 熔断器 | 防止雪崩 |

### 使用中间件

```python
from shared.event_bus import (
    EventBus, EventBusConfig,
    LoggingMiddleware,
    MetricsMiddleware,
    RetryMiddleware,
    CircuitBreakerMiddleware
)

# 启用中间件支持
config = EventBusConfig(enable_middleware=True)
bus = EventBus.get(config)

# 添加中间件（按顺序执行）
bus.use(LoggingMiddleware(log_level="DEBUG"))
bus.use(MetricsMiddleware())
bus.use(RetryMiddleware(max_retries=3, delay=0.1))
bus.use(CircuitBreakerMiddleware(failure_threshold=5))
```

### 自定义中间件

```python
from shared.event_bus import Middleware, MiddlewareContext

class CustomMiddleware(Middleware):
    """自定义中间件示例"""

    def __init__(self, name: str = "custom"):
        self.name = name

    def process(self, ctx: MiddlewareContext, next_fn):
        # ========== 前置处理 ==========
        print(f"[{self.name}] 开始处理: {ctx.event.event_type}")
        start_time = time.time()

        try:
            # ========== 调用下一个中间件 ==========
            result = next_fn(ctx)

            # ========== 后置处理（成功）==========
            duration = (time.time() - start_time) * 1000
            print(f"[{self.name}] 处理成功: {duration:.2f}ms")

            return result

        except Exception as e:
            # ========== 后置处理（失败）==========
            print(f"[{self.name}] 处理失败: {e}")
            raise

# 使用
bus.use(CustomMiddleware(name="audit"))
```

### 熔断器详解

```
熔断器状态转换:

┌─────────┐                    ┌─────────┐                    ┌─────────┐
│ CLOSED  │ ── 失败达阈值 ───▶ │  OPEN   │ ── 超时后 ────────▶│HALF_OPEN│
│ (关闭)  │                    │ (打开)  │                    │(半开)   │
│ 正常工作 │◀── 请求成功 ───── │ 拒绝请求 │◀── 请求失败 ──────│ 试探恢复│
└─────────┘                    └─────────┘                    └─────────┘
     ▲                                                              │
     │                        请求成功                               │
     └──────────────────────────────────────────────────────────────┘
```

```python
from shared.event_bus import CircuitBreakerMiddleware

breaker = CircuitBreakerMiddleware(
    failure_threshold=5,    # 5次失败后熔断
    reset_timeout=30.0,     # 30秒后尝试恢复
    half_open_max=3         # 半开状态最多尝试3次
)
bus.use(breaker)

# 查看状态
print(breaker.state)  # 'closed' | 'open' | 'half_open'
```

---

## 🌟 高级特性

### 死信队列 (Dead Letter Queue)

当事件没有订阅者时，会进入死信队列：

```python
from shared.event_bus import EventBusConfig, DeadEvent

# 启用死信队列
config = EventBusConfig(enable_dead_letter=True)
bus = EventBus.get(config)

# 监听死信事件
@bus.on("dead_event")
def handle_dead_event(dead: DeadEvent):
    print(f"⚠️ 无人处理: {dead.original_event.event_type}")
    print(f"   原因: {dead.reason}")
    print(f"   时间: {dead.attempted_at}")

# 访问死信队列
dlq = bus.dead_letter_queue

# 查看所有死信
for dead in dlq.get_all():
    print(dead.original_event)

# 重试死信
results = dlq.retry_all()
print(f"成功: {results['success']}, 失败: {results['failed']}")
```

### 历史事件重放 (Historic Events)

新订阅者可以接收历史事件：

```python
from shared.event_bus import EventBusConfig

# 启用历史事件
config = EventBusConfig(
    enable_historic=True,
    historic_max_size=1000,      # 最多存储 1000 条
    historic_ttl_seconds=3600    # 1小时过期
)
bus = EventBus.get(config)

# 发布事件（存入历史）
bus.emit(ConfigLoadedEvent(config={"key": "value"}))

# 后注册的处理器可以重放历史
@bus.on("config.loaded", replay_historic=True)
def late_subscriber(event):
    print(f"收到历史事件: {event.config}")  # ✅ 能收到之前的事件
```

### HookSpec 类型安全

使用 HookSpec 定义类型安全的事件规格：

```python
from shared.event_bus import HookSpec, HookSpecRegistry

# 定义事件规格
class DataHooks(HookSpec):
    """数据处理钩子规格"""

    @HookSpec.hook
    def on_data_loaded(self, data_id: str, row_count: int) -> None:
        """数据加载完成"""
        ...

    @HookSpec.hook(historic=True)
    def on_schema_changed(self, schema: dict) -> None:
        """Schema 变更（支持历史重放）"""
        ...

# 注册规格
registry = HookSpecRegistry()
registry.register(DataHooks)

# 实现规格
class DataProcessor:
    def on_data_loaded(self, data_id: str, row_count: int):
        print(f"处理数据: {data_id}, {row_count} 行")

registry.add_implementation(DataProcessor())

# 调用
registry.call("on_data_loaded", data_id="D001", row_count=1000)
```

### 订阅管理 (Subscription)

RxPY 风格的可取消订阅：

```python
from shared.event_bus import CompositeDisposable

# 单个订阅
sub = bus.subscribe("user.action", handle_action)
sub.dispose()  # 取消
print(sub.is_disposed)  # True

# 组合订阅（批量管理）
subs = CompositeDisposable()
subs.add(bus.subscribe("event.a", handler_a))
subs.add(bus.subscribe("event.b", handler_b))
subs.add(bus.subscribe("event.c", handler_c))
subs.dispose()  # 一次性取消所有

# 生命周期管理
class MyService:
    def __init__(self):
        self._subs = CompositeDisposable()

    def start(self):
        self._subs.add(bus.subscribe("data.update", self._handle))

    def stop(self):
        self._subs.dispose()  # 清理所有订阅
```

### 异步支持

```python
from shared.event_bus import AsyncEventBus, to_async, run_sync
import asyncio

# 异步事件总线
async def main():
    async_bus = AsyncEventBus()

    @async_bus.on("async.event")
    async def async_handler(event):
        await asyncio.sleep(0.1)
        print(f"异步处理: {event}")

    result = await async_bus.emit(MyEvent())
    print(f"完成: {result.handler_count} 个处理器")

asyncio.run(main())

# 工具函数
@to_async
def sync_handler(event):
    return process_data(event)

result = run_sync(async_handler(event))
```

---

## 🔗 系统集成

### 在 Orchestrator 中

```python
from shared import EventBus
from shared.event_bus import MethodRegisteredEvent

bus = EventBus.get()

def register_method(component, method, func, engine):
    # ... 注册逻辑 ...

    # 发布事件
    bus.emit(MethodRegisteredEvent(
        component=component,
        method=method,
        engine=engine
    ))
```

### 在 Pipeline 中

```python
from shared.event_bus import (
    PipelineStartedEvent,
    PipelineCompletedEvent,
    NodeCompletedEvent
)

class PipelineExecutor:
    def execute(self):
        bus.emit(PipelineStartedEvent(pipeline_name=self.name))

        for node in self.nodes:
            result = node.execute()
            bus.emit(NodeCompletedEvent(
                node_name=node.name,
                duration_ms=result.duration
            ))

        bus.emit(PipelineCompletedEvent(
            pipeline_name=self.name,
            total_duration_ms=total_time
        ))
```

### 在插件中

```python
from shared import EventBus

class PrometheusPlugin:
    """Prometheus 指标收集插件"""

    def __init__(self):
        bus = EventBus.get()

        # 订阅多种事件
        bus.on("pipeline.started", self._on_pipeline_start)
        bus.on("pipeline.completed", self._on_pipeline_complete)
        bus.on("node.completed", self._record_duration)
        bus.on("error.*", self._record_error)  # 通配符

    def _on_pipeline_start(self, event):
        self.metrics.pipeline_starts.inc()

    def _on_pipeline_complete(self, event):
        self.metrics.pipeline_duration.observe(event.duration_ms)

    def _record_duration(self, event):
        self.metrics.node_duration.labels(
            node=event.node_name
        ).observe(event.duration_ms)
```

---

## 📝 最佳实践

### ✅ 推荐做法

```python
# 1. 使用 dataclass 定义事件
@dataclass
class OrderCreatedEvent:
    event_type: str = "order.created"
    order_id: str = ""
    amount: float = 0.0

# 2. 处理器快速返回，耗时任务异步处理
@bus.on("data.received")
def handle_data(event):
    task_queue.enqueue(process_large_file, event.file)  # ✅ 异步

# 3. 使用订阅管理生命周期
class Service:
    def __init__(self):
        self._subs = CompositeDisposable()

    def start(self):
        self._subs.add(bus.subscribe("event.a", self._handle))

    def stop(self):
        self._subs.dispose()

# 4. 使用点分命名空间
"user.profile.updated"
"payment.transaction.completed"

# 5. 合理使用优先级
@bus.on("data.saved", priority=EventPriority.HIGH)
def validate_first(event): ...

@bus.on("data.saved", priority=EventPriority.LOW)
def notify_later(event): ...
```

### ❌ 避免做法

```python
# 1. 不要在处理器中执行耗时同步操作
@bus.on("data.received")
def bad_handler(event):
    process_large_file(event.file)  # ❌ 阻塞！

# 2. 不要忘记清理订阅
sub = bus.subscribe("event", handler)
# ... 忘记 sub.dispose() 导致内存泄漏

# 3. 不要使用不清晰的事件名
"evt1"  # ❌
"handleUserData"  # ❌

# 4. 不要在处理器中抛出未捕获异常（除非故意中断）
@bus.on("event")
def bad_handler(event):
    raise Exception("oops")  # ❌ 会影响其他处理器
```

---

## ⚙️ 配置选项

```python
from shared.event_bus import EventBusConfig

config = EventBusConfig(
    # ===== 中间件配置 =====
    enable_middleware=True,           # 启用中间件

    # ===== 死信队列配置 =====
    enable_dead_letter=True,          # 启用死信队列
    dead_letter_max_size=1000,        # 死信队列最大容量

    # ===== 历史事件配置 =====
    enable_historic=True,             # 启用历史事件
    historic_max_size=500,            # 每种事件最多存储数量
    historic_ttl_seconds=3600,        # 历史事件过期时间（秒）

    # ===== 性能配置 =====
    max_handlers_per_event=100,       # 每个事件最大处理器数
    emit_timeout_seconds=30.0,        # 发布超时时间

    # ===== 调试配置 =====
    debug=False,                      # 调试模式
    trace_enabled=False               # 启用追踪
)

bus = EventBus.get(config)
```

---

## 🔍 调试与监控

### 查看统计信息

```python
stats = bus.get_stats()

print(f"📊 EventBus 统计")
print(f"  总处理器: {stats.total_handlers}")
print(f"  总发布: {stats.total_emits}")
print(f"  总调用: {stats.total_handlers_called}")
print(f"  总错误: {stats.total_errors}")
print(f"  运行时间: {stats.uptime_seconds:.2f}s")
print(f"  事件类型: {stats.event_types}")
```

### 调试命令

```python
# 查看所有注册的处理器
for event_type, handlers in bus._handlers.items():
    print(f"{event_type}: {len(handlers)} 个处理器")
    for h in handlers:
        print(f"  - {h.name} (priority={h.priority})")

# 查看中间件列表
if bus._middleware_pipeline:
    print("中间件:")
    for mw in bus._middleware_pipeline.list_middlewares():
        print(f"  - {mw}")

# 查看死信队列
if bus.dead_letter_queue:
    print(f"死信队列: {len(bus.dead_letter_queue.get_all())} 条")
    for dead in bus.dead_letter_queue.get_all():
        print(f"  - {dead.original_event.event_type}")

# 查看历史事件
if bus.historic_store:
    for event_type, events in bus.historic_store._history.items():
        print(f"{event_type}: {len(events)} 条历史")
```

---

## 📁 模块结构

```
shared/event_bus/
│
├── __init__.py          # 📦 主入口，导出所有公共 API
│
├── bus.py               # 🎯 核心 EventBus 实现
│                        #    - EventBusV6 类
│                        #    - 单例模式
│                        #    - 订阅/发布逻辑
│
├── config.py            # ⚙️ 配置
│                        #    - EventBusConfig dataclass
│
├── models.py            # 📋 数据模型
│                        #    - HandlerInfo
│                        #    - EmitResult
│                        #    - EventBusStats
│                        #    - EventPriority
│
├── events.py            # 📨 预定义事件类型
│                        #    - MethodRegisteredEvent
│                        #    - PipelineStartedEvent
│                        #    - NodeCompletedEvent
│                        #    - 等等...
│
├── specs.py             # 🎯 HookSpec 类型安全系统
│                        #    - HookSpec
│                        #    - HookSpecRegistry
│
├── middleware.py        # 🔧 中间件管道
│                        #    - MiddlewarePipeline
│                        #    - LoggingMiddleware
│                        #    - TracingMiddleware
│                        #    - RetryMiddleware
│                        #    - CircuitBreakerMiddleware
│                        #    - 等等...
│
├── subscription.py      # 🔄 订阅管理
│                        #    - Subscription
│                        #    - Disposable
│                        #    - CompositeDisposable
│
├── dead_letter.py       # 📬 死信队列
│                        #    - DeadEvent
│                        #    - DeadLetterQueue
│
├── historic.py          # 📜 历史事件存储
│                        #    - HistoricEventStore
│                        #    - HistoricEntry
│
├── async_support.py     # ⚡ 异步支持
│                        #    - AsyncEventBus
│                        #    - to_async
│                        #    - run_sync
│
└── demo.py              # 🎪 功能演示
```

---

## 🎯 预定义事件

| 事件类 | event_type | 触发时机 |
|--------|------------|----------|
| `MethodRegisteredEvent` | `method.registered` | @register_method 执行 |
| `MethodExecutedEvent` | `method.executed` | Registry.execute() 完成 |
| `MethodSelectedEvent` | `method.selected` | Registry.select() 返回 |
| `PipelineStartedEvent` | `pipeline.started` | Pipeline 开始执行 |
| `PipelineCompletedEvent` | `pipeline.completed` | Pipeline 执行完成 |
| `PipelineErrorEvent` | `pipeline.error` | Pipeline 执行出错 |
| `NodeStartedEvent` | `node.started` | 单个节点开始 |
| `NodeCompletedEvent` | `node.completed` | 单个节点完成 |
| `DataLoadedEvent` | `data.loaded` | 数据加载完成 |
| `DataTransformedEvent` | `data.transformed` | 数据转换完成 |
| `CacheHitEvent` | `cache.hit` | 缓存命中 |
| `CacheInvalidatedEvent` | `cache.invalidated` | 缓存失效 |
| `SystemReadyEvent` | `system.ready` | 系统就绪 |
| `ComponentLoadedEvent` | `component.loaded` | 组件加载 |
| `ErrorEvent` | `error` | 通用错误 |
| `MetricEvent` | `metric` | 指标收集 |

---

## 📝 变更日志

### v6.0.0 (2025-12-27)

**🎉 重大更新**

- ✨ 整合多种开源最佳实践
- ✨ 新增 HookSpec 类型安全系统 (参考 pluggy)
- ✨ 新增死信队列 DeadLetterQueue (参考 Guava)
- ✨ 新增历史事件重放 HistoricEventStore (参考 pluggy)
- ✨ 新增中间件管道 MiddlewarePipeline (参考 Express/Koa)
- ✨ 新增订阅管理 Subscription/Disposable (参考 RxPY)
- ✨ 新增熔断器 CircuitBreakerMiddleware (参考 Hystrix)
- ✨ 新增异步支持 AsyncEventBus (参考 Reactor)
- 🔧 移除兼容层，统一 API
- 📚 完善文档和示例

### v5.0.0 (2025-12-25)

- ✨ 完全集成到 AStock 系统
- 🔧 清理旧代码，移除 HookBus
- ✨ 新增通配符匹配支持

### v4.0.0 (2025-12-20)

- ✨ 新增优先级队列
- ✨ 新增事件过滤
- ✨ 新增 Wrapper 支持

---

## 🤝 贡献指南

### 开发环境

```bash
# 克隆仓库
git clone https://github.com/your-org/AStock-Analysis.git
cd AStock-Analysis

# 安装依赖
pip install -r requirements.txt

# 运行测试
python -m pytest tests/test_event_bus.py -v
```

### 提交规范

```
feat: 添加新功能
fix: 修复 bug
docs: 更新文档
refactor: 重构代码
test: 添加测试
chore: 其他修改
```

### 代码规范

- 使用 `black` 格式化代码
- 使用 `mypy` 进行类型检查
- 所有公共 API 必须有 docstring
- 新功能必须有对应测试

---

## 📄 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](../../LICENSE) 文件

---

<p align="center">
  <sub>Built with ❤️ by AStock Team</sub>
</p>
