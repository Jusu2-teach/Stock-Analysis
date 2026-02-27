"""Pipeline Core - Unified Middleware System
===========================================

统一中间件基础设施，整合执行中间件和事件中间件的通用模式。

设计原则：
- 洋葱模型: 前置处理 → 下一层 → 后置处理
- 类型安全: 泛型支持
- 可组合: 中间件链
- 可观测: 计时、追踪

架构概览：
```
    ┌───────────────────────────────────────────────────┐
    │              Middleware<TPayload, TContext>        │
    │                    (Abstract Base)                 │
    │  - process(payload, context, next) → Result       │
    │  - before(payload, context)                       │
    │  - after(payload, context, result)                │
    │  - on_error(payload, context, error)              │
    └───────────────────────────────────────────────────┘
                            │
         ┌──────────────────┼──────────────────┐
         ▼                  ▼                  ▼
    Execution          Event              Custom
    Middleware       Middleware          Middleware
    (TaskRun)        (PipelineEvent)     (Any)
```

版本: 2.0.0
"""

from __future__ import annotations

import abc
import logging
import time
import traceback
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Protocol,
    Tuple,
    TypeVar,
    Union,
    runtime_checkable,
)

logger = logging.getLogger(__name__)


# =============================================================================
# 类型定义
# =============================================================================

TPayload = TypeVar('TPayload')      # 负载类型 (event, task_run, etc.)
TContext = TypeVar('TContext')      # 上下文类型
TResult = TypeVar('TResult')        # 结果类型


class MiddlewareAction(Enum):
    """中间件动作"""
    CONTINUE = auto()    # 继续执行
    ABORT = auto()       # 中止执行
    RETRY = auto()       # 重试
    SKIP = auto()        # 跳过后续


# =============================================================================
# 通用上下文基类
# =============================================================================

@dataclass
class BaseContext:
    """通用中间件上下文基类

    Attributes:
        context_id: 上下文唯一标识
        timestamp: 创建时间戳
        start_time: 开始处理时间 (用于计时)
        metadata: 扩展元数据
        errors: 错误列表
        should_continue: 是否继续执行
    """
    context_id: str = field(default_factory=lambda: str(uuid.uuid4())[:12])
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    start_time: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    should_continue: bool = True

    # 中间件执行追踪
    middleware_times: Dict[str, float] = field(default_factory=dict)

    def elapsed_ms(self) -> float:
        """已耗时（毫秒）"""
        return (time.time() - self.start_time) * 1000

    def set_meta(self, key: str, value: Any) -> None:
        """设置元数据"""
        self.metadata[key] = value

    def get_meta(self, key: str, default: Any = None) -> Any:
        """获取元数据"""
        return self.metadata.get(key, default)

    def abort(self, reason: str = "") -> None:
        """中止执行"""
        self.should_continue = False
        if reason:
            self.errors.append(reason)

    def record_middleware_time(self, name: str, duration_ms: float) -> None:
        """记录中间件耗时"""
        self.middleware_times[name] = duration_ms

    def add_error(self, error: Union[str, Exception]) -> None:
        """添加错误"""
        if isinstance(error, Exception):
            self.errors.append(f"{type(error).__name__}: {error}")
        else:
            self.errors.append(error)


# =============================================================================
# 中间件协议 (Protocol)
# =============================================================================

# 下一步处理函数签名
NextFn = Callable[[TPayload, TContext], TResult]


@runtime_checkable
class MiddlewareProtocol(Protocol[TPayload, TContext, TResult]):
    """中间件协议

    定义中间件必须实现的接口。
    """

    @property
    def name(self) -> str:
        """中间件名称"""
        ...

    @property
    def order(self) -> int:
        """执行顺序 (越小越先执行)"""
        ...

    def process(
        self,
        payload: TPayload,
        context: TContext,
        next_fn: NextFn[TPayload, TContext, TResult],
    ) -> TResult:
        """处理负载"""
        ...


# =============================================================================
# 中间件基类
# =============================================================================

class MiddlewareBase(abc.ABC, Generic[TPayload, TContext, TResult]):
    """通用中间件基类

    提供洋葱模型的标准实现，子类可以选择性覆盖：
    - before(): 前置处理
    - after(): 后置处理
    - on_error(): 错误处理

    或完全覆盖 process() 实现自定义逻辑。

    Example:
        class LoggingMiddleware(MiddlewareBase[TaskRun, ExecContext, Any]):
            name = "logging"
            order = 10

            def before(self, payload, context) -> Tuple[TaskRun, ExecContext, MiddlewareAction]:
                print(f"Starting: {payload.name}")
                return payload, context, MiddlewareAction.CONTINUE

            def after(self, payload, context, result) -> Any:
                print(f"Completed: {payload.name}")
                return result
    """

    # 子类应覆盖
    name: str = "base_middleware"
    order: int = 100

    def process(
        self,
        payload: TPayload,
        context: TContext,
        next_fn: NextFn[TPayload, TContext, TResult],
    ) -> Optional[TResult]:
        """处理负载 (模板方法)

        标准洋葱模型：before → next → after

        Args:
            payload: 负载数据
            context: 上下文
            next_fn: 下一个中间件

        Returns:
            处理结果，ABORT 时返回 None
        """
        start_time = time.time()

        try:
            # 前置处理
            payload, context, action = self.before(payload, context)

            if action == MiddlewareAction.ABORT:
                return self.get_abort_result(payload, context)

            if action == MiddlewareAction.SKIP:
                return self.get_skip_result(payload, context)

            # 调用下一个中间件
            result = next_fn(payload, context)

            # 后置处理
            result = self.after(payload, context, result)

            return result

        except Exception as e:
            # 错误处理
            action = self.on_error(payload, context, e)

            if action == MiddlewareAction.RETRY:
                # 子类负责实际重试逻辑
                raise
            elif action == MiddlewareAction.CONTINUE:
                # 继续执行，返回 None 或默认值
                return self.get_error_result(payload, context, e)
            else:
                # 默认重新抛出
                raise

        finally:
            # 记录耗时
            if isinstance(context, BaseContext):
                duration = (time.time() - start_time) * 1000
                context.record_middleware_time(self.name, duration)

    def before(
        self,
        payload: TPayload,
        context: TContext,
    ) -> Tuple[TPayload, TContext, MiddlewareAction]:
        """前置处理

        Args:
            payload: 负载数据
            context: 上下文

        Returns:
            (可能修改的 payload, context, 动作)
        """
        return payload, context, MiddlewareAction.CONTINUE

    def after(
        self,
        payload: TPayload,
        context: TContext,
        result: TResult,
    ) -> TResult:
        """后置处理

        Args:
            payload: 负载数据
            context: 上下文
            result: 上一步结果

        Returns:
            可能修改的结果
        """
        return result

    def on_error(
        self,
        payload: TPayload,
        context: TContext,
        error: Exception,
    ) -> MiddlewareAction:
        """错误处理

        Args:
            payload: 负载数据
            context: 上下文
            error: 发生的异常

        Returns:
            动作 (ABORT/RETRY/CONTINUE)
        """
        return MiddlewareAction.ABORT

    def get_abort_result(
        self,
        payload: TPayload,
        context: TContext,
    ) -> Optional[TResult]:
        """获取中止时的默认结果

        子类可覆盖以返回特定的中止结果。
        """
        return None

    def get_skip_result(
        self,
        payload: TPayload,
        context: TContext,
    ) -> Optional[TResult]:
        """获取跳过时的默认结果

        子类可覆盖以返回特定的跳过结果。
        """
        return None

    def get_error_result(
        self,
        payload: TPayload,
        context: TContext,
        error: Exception,
    ) -> Optional[TResult]:
        """获取错误时的默认结果

        子类可覆盖以返回特定的错误结果。
        """
        return None

    def __call__(
        self,
        payload: TPayload,
        context: TContext,
        next_fn: NextFn[TPayload, TContext, TResult],
    ) -> Optional[TResult]:
        """使中间件可调用，支持函数式调用风格"""
        return self.process(payload, context, next_fn)


# =============================================================================
# 函数式中间件
# =============================================================================

class FunctionMiddleware(MiddlewareBase[TPayload, TContext, TResult]):
    """函数包装中间件

    将普通函数包装为中间件。

    Usage:
        def log_middleware(payload, context, next_fn):
            print(f"Before: {payload}")
            result = next_fn(payload, context)
            print(f"After: {payload}")
            return result

        middleware = FunctionMiddleware(log_middleware, name="log", order=10)
    """

    def __init__(
        self,
        fn: Callable[[TPayload, TContext, NextFn], TResult],
        name: Optional[str] = None,
        order: int = 100,
    ):
        self._fn = fn
        self.name = name or getattr(fn, '__name__', 'anonymous')
        self.order = order

    def process(
        self,
        payload: TPayload,
        context: TContext,
        next_fn: NextFn[TPayload, TContext, TResult],
    ) -> TResult:
        """直接调用包装的函数"""
        return self._fn(payload, context, next_fn)


# =============================================================================
# 中间件链
# =============================================================================

class MiddlewareChain(Generic[TPayload, TContext, TResult]):
    """中间件链

    管理和执行中间件链。

    特性：
    - 按 order 排序
    - 洋葱模型执行
    - 支持动态添加/移除

    Example:
        chain = MiddlewareChain()
        chain.use(LoggingMiddleware())
        chain.use(TimingMiddleware())
        chain.use(ValidationMiddleware())

        def final_handler(payload, context):
            return do_work(payload)

        result = chain.execute(payload, context, final_handler)
    """

    def __init__(self):
        self._middlewares: List[MiddlewareBase[TPayload, TContext, TResult]] = []
        self._sorted = True

    def use(
        self,
        middleware: Union[
            MiddlewareBase[TPayload, TContext, TResult],
            Callable[[TPayload, TContext, NextFn], TResult],
        ],
        *,
        name: str = None,
        order: int = None,
    ) -> 'MiddlewareChain':
        """添加中间件

        Args:
            middleware: 中间件实例或函数
            name: 名称 (仅函数时有效)
            order: 顺序 (仅函数时有效)

        Returns:
            self (链式调用)
        """
        if callable(middleware) and not isinstance(middleware, MiddlewareBase):
            middleware = FunctionMiddleware(
                middleware,
                name=name,
                order=order or 100,
            )

        if order is not None:
            middleware.order = order

        self._middlewares.append(middleware)
        self._sorted = False

        logger.debug(f"Added middleware: {middleware.name} (order={middleware.order})")
        return self

    def remove(self, name: str) -> bool:
        """移除中间件

        Args:
            name: 中间件名称

        Returns:
            是否成功移除
        """
        original_len = len(self._middlewares)
        self._middlewares = [m for m in self._middlewares if m.name != name]
        removed = len(self._middlewares) < original_len

        if removed:
            logger.debug(f"Removed middleware: {name}")

        return removed

    def clear(self) -> None:
        """清空所有中间件"""
        self._middlewares.clear()
        self._sorted = True

    def execute(
        self,
        payload: TPayload,
        context: TContext,
        final_handler: Callable[[TPayload, TContext], TResult],
    ) -> TResult:
        """执行中间件链

        Args:
            payload: 负载数据
            context: 上下文
            final_handler: 最终处理函数

        Returns:
            处理结果
        """
        # 确保排序
        if not self._sorted:
            self._middlewares.sort(key=lambda m: m.order)
            self._sorted = True

        if not self._middlewares:
            return final_handler(payload, context)

        # 构建洋葱结构
        def build_chain(index: int) -> NextFn:
            if index >= len(self._middlewares):
                # 最内层: 最终处理函数
                return lambda p, c: final_handler(p, c)

            middleware = self._middlewares[index]
            next_fn = build_chain(index + 1)

            return lambda p, c: middleware.process(p, c, next_fn)

        chain = build_chain(0)
        return chain(payload, context)

    @property
    def middlewares(self) -> List[MiddlewareBase]:
        """获取所有中间件 (按顺序)"""
        if not self._sorted:
            self._middlewares.sort(key=lambda m: m.order)
            self._sorted = True
        return list(self._middlewares)

    def __len__(self) -> int:
        return len(self._middlewares)

    def __iter__(self):
        return iter(self.middlewares)


# =============================================================================
# 装饰器支持
# =============================================================================

def middleware(
    name: str = None,
    order: int = 100,
) -> Callable:
    """中间件装饰器

    将函数装饰为中间件。

    Usage:
        @middleware(name="timing", order=10)
        def timing_middleware(payload, context, next_fn):
            start = time.time()
            result = next_fn(payload, context)
            print(f"Elapsed: {time.time() - start:.3f}s")
            return result
    """
    def decorator(fn: Callable) -> FunctionMiddleware:
        return FunctionMiddleware(fn, name=name, order=order)

    return decorator


# =============================================================================
# 预置中间件
# =============================================================================

class PassthroughMiddleware(MiddlewareBase[TPayload, TContext, TResult]):
    """直通中间件

    不做任何处理，直接传递给下一个中间件。
    用于测试或占位。
    """
    name = "passthrough"
    order = 1000

    def before(self, payload, context):
        return payload, context, MiddlewareAction.CONTINUE


class ErrorHandlerMiddleware(MiddlewareBase[TPayload, TContext, TResult]):
    """错误处理中间件

    捕获异常并根据策略处理。
    """
    name = "error_handler"
    order = 5

    def __init__(
        self,
        log_errors: bool = True,
        reraise: bool = True,
    ):
        self._log_errors = log_errors
        self._reraise = reraise

    def on_error(self, payload, context, error):
        if self._log_errors:
            logger.error(f"Error in middleware chain: {error}")
            logger.debug(traceback.format_exc())

        if isinstance(context, BaseContext):
            context.add_error(error)

        if self._reraise:
            return MiddlewareAction.ABORT
        else:
            return MiddlewareAction.CONTINUE


class TimingMiddleware(MiddlewareBase[TPayload, TContext, TResult]):
    """计时中间件

    记录整个处理链的耗时。
    """
    name = "timing"
    order = 1

    def __init__(self, threshold_ms: float = 1000):
        """
        Args:
            threshold_ms: 超过此阈值记录警告日志
        """
        self._threshold_ms = threshold_ms

    def before(self, payload, context):
        if isinstance(context, BaseContext):
            context.set_meta('_timing_start', time.time())
        return payload, context, MiddlewareAction.CONTINUE

    def after(self, payload, context, result):
        if isinstance(context, BaseContext):
            start = context.get_meta('_timing_start', time.time())
            elapsed_ms = (time.time() - start) * 1000
            context.set_meta('total_elapsed_ms', elapsed_ms)

            if elapsed_ms > self._threshold_ms:
                logger.warning(
                    f"Slow operation detected: {elapsed_ms:.1f}ms "
                    f"(threshold: {self._threshold_ms}ms)"
                )

        return result


# =============================================================================
# 工厂函数
# =============================================================================

def create_chain(*middlewares) -> MiddlewareChain:
    """创建中间件链

    Args:
        *middlewares: 中间件列表

    Returns:
        配置好的中间件链
    """
    chain = MiddlewareChain()
    for m in middlewares:
        chain.use(m)
    return chain
