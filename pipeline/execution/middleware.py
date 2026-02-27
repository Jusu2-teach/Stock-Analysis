"""Pipeline Execution - Middleware System
=========================================

任务执行中间件，继承自 core/middleware 统一基础设施。

架构关系：
    core/middleware.MiddlewareBase        (泛型统一基类)
                │
                ├── events/middleware.EventMiddleware  (事件专用)
                │       └── LoggingMiddleware, TracingMiddleware, ...
                │
                └── execution/middleware.ExecutionMiddlewareBase  (任务执行专用)  ← 当前文件
                        └── RetryMiddleware, CacheMiddleware, ...

设计原则：
- 洋葱模型 (每个中间件包裹下一个)
- 继承统一基类，确保架构一致性
- 单一职责

版本: 2.0.0
"""

from __future__ import annotations

import logging
import time
import traceback
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from ..core.run import TaskRun
from ..core.state import TaskState
from ..core.policy import RetryPolicy, CachePolicy
from ..core.policy import TimeoutPolicy
from ..core.middleware import (
    MiddlewareBase as CoreMiddlewareBase,
    MiddlewareAction,
    BaseContext,
)

# 导入 CacheBackend 并重新导出以保持统一接口
from ..cache.backends import CacheBackend
from ..cache.router import CacheBackendRouter

logger = logging.getLogger(__name__)


__all__ = [
    # Base
    "ExecutionMiddlewareBase",
    "MiddlewareContext",
    "NextMiddleware",
    "MiddlewareAction",
    # 内置中间件
    "ExecutionMiddleware",
    "LoggingMiddleware",
    "TimingMiddleware",
    "TimeoutMiddleware",
    "RetryMiddleware",
    "CacheMiddleware",
    "ValidationMiddleware",
    "ErrorHandlingMiddleware",
    # Chain
    "ExecutionMiddlewareChain",
    # Cache
    "CacheBackend",
]


# =============================================================================
# 执行中间件上下文
# =============================================================================

@dataclass
class MiddlewareContext(BaseContext):
    """执行中间件上下文

    继承自 BaseContext，添加任务执行专用字段。

    Attributes:
        task_run: 任务运行时状态
        inputs: 任务输入
        callable: 实际执行的函数
        result: 执行结果 (由中间件填充)
        error: 执行错误 (由中间件填充)
        skip_execution: 是否跳过实际执行
        skip_cached: 是否跳过缓存命中的任务 (True=命中则跳过，False=即使命中也执行)
    """
    task_run: TaskRun = None
    inputs: Dict[str, Any] = field(default_factory=dict)
    callable: Callable[..., Any] = None
    result: Any = None
    error: Optional[Exception] = None
    skip_execution: bool = False
    skip_cached: bool = True  # P2: 显式声明，而不是通过 metadata 隐式传递

    @property
    def task_name(self) -> str:
        """任务名称 (便捷属性)"""
        return self.task_run.name if self.task_run else ""


# 类型别名
NextMiddleware = Callable[[MiddlewareContext], None]


class ExecutionMiddlewareBase(CoreMiddlewareBase[MiddlewareContext, MiddlewareContext, None]):
    """执行中间件基类

    继承自 core/middleware.MiddlewareBase，专用于任务执行。

    子类可以选择两种实现方式:
    1. 覆盖 handle() - 传统方式，接收 (ctx, next)
    2. 覆盖 before()/after()/on_error() - 模板方法

    Example (传统方式):
        class LoggingMiddleware(ExecutionMiddlewareBase):
            def handle(self, ctx: MiddlewareContext, next: NextMiddleware) -> None:
                print(f"Before: {ctx.task_name}")
                next(ctx)  # 调用下一个中间件
                print(f"After: {ctx.task_name}")
    """

    # 子类覆盖
    name: str = "base_middleware"
    order: int = 100

    def handle(self, ctx: MiddlewareContext, next: NextMiddleware) -> None:
        """处理请求 (传统 API)

        默认实现委托给 process()，子类可覆盖此方法。

        Args:
            ctx: 中间件上下文
            next: 下一个中间件
        """
        # 适配 core 的 process() 签名
        def adapted_next(payload, context):
            next(payload)
            return None

        self.process(ctx, ctx, adapted_next)

    def process(
        self,
        payload: MiddlewareContext,
        context: MiddlewareContext,
        next_fn: Callable,
    ) -> None:
        """处理请求 (core 统一 API)

        标准洋葱模型：before → next → after
        """
        start_time = time.time()

        try:
            # 前置处理
            payload, context, action = self.before(payload, context)

            if action == MiddlewareAction.ABORT:
                return None

            if action == MiddlewareAction.SKIP:
                return None

            # 调用下一个中间件
            next_fn(payload, context)

            # 后置处理
            self.after(payload, context, None)

        except Exception as e:
            action = self.on_error(payload, context, e)
            if action != MiddlewareAction.CONTINUE:
                raise

        finally:
            duration = (time.time() - start_time) * 1000
            context.record_middleware_time(self.name, duration)


# =============================================================================
# 内置中间件
# =============================================================================

class ExecutionMiddleware(ExecutionMiddlewareBase):
    """执行中间件 (最内层)

    实际执行任务函数。
    """
    name = "execution"
    order = 1000  # 最后执行

    def handle(self, ctx: MiddlewareContext, next: NextMiddleware) -> None:
        if ctx.skip_execution:
            return

        try:
            ctx.result = ctx.callable(**ctx.inputs)
        except Exception as e:
            ctx.error = e
            raise


class LoggingMiddleware(ExecutionMiddlewareBase):
    """日志中间件

    记录任务执行的开始和结束。
    """
    name = "logging"
    order = 10  # 早执行，记录开始时间

    def __init__(self, logger: Optional[logging.Logger] = None):
        self._logger = logger or logging.getLogger(__name__)

    def handle(self, ctx: MiddlewareContext, next: NextMiddleware) -> None:
        self._logger.info(f"▶ Starting task: {ctx.task_name}")
        start_time = time.time()

        try:
            next(ctx)
            elapsed = (time.time() - start_time) * 1000
            self._logger.info(
                f"✓ Completed task: {ctx.task_name} ({elapsed:.1f}ms)"
            )
        except Exception as e:
            elapsed = (time.time() - start_time) * 1000
            self._logger.error(
                f"✗ Failed task: {ctx.task_name} ({elapsed:.1f}ms) - {e}"
            )
            raise


class TimingMiddleware(ExecutionMiddlewareBase):
    """计时中间件

    记录任务执行时间到元数据。
    """
    name = "timing"
    order = 5  # 最早执行，包裹所有其他中间件

    def handle(self, ctx: MiddlewareContext, next: NextMiddleware) -> None:
        start_time = time.time()

        try:
            next(ctx)
        finally:
            elapsed_ms = (time.time() - start_time) * 1000
            ctx.metadata['duration_ms'] = elapsed_ms


class TimeoutMiddleware(ExecutionMiddlewareBase):
    """超时中间件

    提供两种超时模式：
    1. 抢占式超时 (preemptive=True，默认)：使用线程池实现真正的超时中断
       - 优点：可以在超时后立即返回，不必等待任务完成
       - 缺点：无法中断 CPU 密集型任务或不释放 GIL 的 C 扩展
       - 适用：I/O 密集型任务、网络请求、可中断的计算
       - **注意**：超时后原任务仍在后台运行，需通过 `pending_tasks` 属性追踪

    2. 后置检测 (preemptive=False)：执行完成后检测是否超时
       - 优点：简单可靠，无线程开销
       - 缺点：无法真正中断任务
       - 适用：需要确保任务完成的场景

    Note:
        Python GIL 限制意味着纯 CPU 密集型任务无法被真正中断。
        抢占式模式下，超时后原任务仍在后台运行直到完成或进程退出。
        可通过 `pending_tasks` 属性获取仍在运行的僵尸任务数量。
    """

    name = "timeout"
    order = 15

    def __init__(self, preemptive: bool = True):
        """
        Args:
            preemptive: 是否使用抢占式超时（True 使用线程池，False 使用后置检测）
        """
        self._preemptive = preemptive
        # P6: 追踪超时后仍在运行的僵尸任务
        self._pending_futures: list = []  # 存储 (future, task_name, timeout_time) 元组

    @property
    def pending_tasks(self) -> int:
        """返回因超时而被放弃但仍在后台运行的任务数量"""
        # 清理已完成的任务
        self._pending_futures = [
            (f, name, t) for f, name, t in self._pending_futures if not f.done()
        ]
        return len(self._pending_futures)

    def get_pending_task_names(self) -> list:
        """返回仍在运行的僵尸任务名称列表"""
        self._pending_futures = [
            (f, name, t) for f, name, t in self._pending_futures if not f.done()
        ]
        return [name for _, name, _ in self._pending_futures]

    def handle(self, ctx: MiddlewareContext, next: NextMiddleware) -> None:
        policy: TimeoutPolicy = ctx.task_run.spec.policies.timeout

        if not policy.enabled:
            next(ctx)
            return

        timeout_s = float(policy.timeout_seconds or 0)
        if timeout_s <= 0:
            next(ctx)
            return

        if self._preemptive:
            self._handle_preemptive(ctx, next, policy, timeout_s)
        else:
            self._handle_post_check(ctx, next, policy, timeout_s)

    def _handle_preemptive(
        self,
        ctx: MiddlewareContext,
        next: NextMiddleware,
        policy: TimeoutPolicy,
        timeout_s: float,
    ) -> None:
        """抢占式超时：使用线程池实现真正的超时"""
        import concurrent.futures

        # P6: 使用实例级线程池而非上下文管理器，以便追踪僵尸任务
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        future = executor.submit(next, ctx)

        try:
            future.result(timeout=timeout_s)
            executor.shutdown(wait=False)
        except concurrent.futures.TimeoutError:
            # 记录僵尸任务以便追踪
            self._pending_futures.append((future, ctx.task_name, time.time()))
            executor.shutdown(wait=False)  # 不等待完成

            ctx.metadata['timeout_exceeded'] = True
            ctx.metadata['timeout_seconds'] = timeout_s
            ctx.metadata['timeout_mode'] = 'preemptive'
            ctx.metadata['pending_background_tasks'] = self.pending_tasks

            message = (
                f"Task '{ctx.task_name}' timed out after {timeout_s:.3f}s "
                f"(preemptive mode - task running in background, "
                f"total pending: {self.pending_tasks})"
            )

            if policy.soft_timeout:
                logger.warning(message)
                return

            raise TimeoutError(message)

    def _handle_post_check(
        self,
        ctx: MiddlewareContext,
        next: NextMiddleware,
        policy: TimeoutPolicy,
        timeout_s: float,
    ) -> None:
        """后置检测超时：执行完成后检查是否超时"""
        start_time = time.time()

        next(ctx)

        elapsed_s = time.time() - start_time

        if elapsed_s > timeout_s:
            ctx.metadata['timeout_exceeded'] = True
            ctx.metadata['timeout_seconds'] = timeout_s
            ctx.metadata['elapsed_seconds'] = elapsed_s
            ctx.metadata['timeout_mode'] = 'post_check'

            message = (
                f"Task '{ctx.task_name}' exceeded timeout: "
                f"{elapsed_s:.3f}s > {timeout_s:.3f}s (post-check mode)"
            )

            if policy.soft_timeout:
                logger.warning(message)
                return

            raise TimeoutError(message)

class RetryMiddleware(ExecutionMiddlewareBase):
    """重试中间件

    根据重试策略自动重试失败的任务。

    Args:
        strict_state_transitions: 状态转换失败时是否抛出异常（默认 False，仅警告）
    """
    name = "retry"
    order = 100  # 靠后执行，包裹实际执行

    def __init__(self, strict_state_transitions: bool = False):
        self._strict_state_transitions = strict_state_transitions

    def handle(self, ctx: MiddlewareContext, next: NextMiddleware) -> None:
        policy: RetryPolicy = ctx.task_run.spec.policies.retry

        if not policy.enabled:
            next(ctx)
            return

        last_error: Optional[Exception] = None

        for attempt in range(1, policy.max_attempts + 1):
            ctx.task_run.attempt = attempt

            # 从 RETRYING 恢复到 RUNNING（与状态机定义保持一致）
            if ctx.task_run.state == TaskState.RETRYING:
                ctx.task_run.state_machine.transition_to(
                    TaskState.RUNNING,
                    trigger="retry_policy",
                    metadata={'attempt': attempt},
                )

            try:
                next(ctx)
                return  # 成功
            except Exception as e:
                last_error = e

                # 检查是否应该重试
                if not policy.should_retry(e):
                    raise

                # 检查是否还有重试机会
                if attempt >= policy.max_attempts:
                    raise

                # 状态机对齐：RUNNING -> FAILED -> RETRYING
                try:
                    ctx.task_run.state_machine.transition_to(
                        TaskState.FAILED,
                        trigger="retry_policy",
                        metadata={'attempt': attempt, 'error': str(e)},
                    )
                except Exception as state_err:
                    # 记录到元数据以便事后分析
                    ctx.metadata.setdefault('state_transition_errors', []).append({
                        'attempt': attempt,
                        'transition': 'RUNNING->FAILED',
                        'error': str(state_err),
                    })

                    # 严格模式：状态转换失败时抛出异常
                    if self._strict_state_transitions:
                        raise RuntimeError(
                            f"State transition RUNNING->FAILED failed for task '{ctx.task_name}' "
                            f"(attempt {attempt}): {state_err}"
                        ) from state_err

                    # 非严格模式：仅警告，不阻断重试流程
                    logger.warning(
                        f"State transition RUNNING->FAILED failed for task '{ctx.task_name}' "
                        f"(attempt {attempt}): {state_err}"
                    )

                # 计算延迟
                delay = policy.get_delay(attempt + 1)
                logger.warning(
                    f"Task {ctx.task_name} failed (attempt {attempt}/{policy.max_attempts}), "
                    f"retrying in {delay:.1f}s: {e}"
                )

                # 等待
                if delay > 0:
                    time.sleep(delay)

                # 标记重试
                ctx.task_run.mark_retrying()

        if last_error:
            raise last_error


class CacheMiddleware(ExecutionMiddlewareBase):
    """缓存中间件

    检查缓存命中，跳过实际执行。
    """
    name = "cache"
    order = 50  # 在验证之后，重试之前

    def __init__(
        self,
        cache_backend: Optional['CacheBackend'] = None,
        cache_router: Optional[CacheBackendRouter] = None,
    ):
        # 兼容：cache_backend 为历史固定后端
        # 推荐：cache_router 按 CachePolicy.backend 动态选择后端
        self._cache = cache_backend
        self._router = cache_router

    def set_cache(self, cache: 'CacheBackend') -> None:
        self._cache = cache

    def set_router(self, router: CacheBackendRouter) -> None:
        self._router = router

    def handle(self, ctx: MiddlewareContext, next: NextMiddleware) -> None:
        if self._cache is None and self._router is None:
            next(ctx)
            return

        policy: CachePolicy = ctx.task_run.spec.policies.cache

        if not policy.enabled:
            next(ctx)
            return

        # Runner 级开关：是否在缓存命中时跳过执行
        # - True: 命中即跳过（默认）
        # - False: 忽略命中，始终执行（但仍允许写回缓存）
        # P2/P3: 使用显式字段，若未设置则从 metadata 兼容读取（过渡期）
        skip_cached = ctx.skip_cached

        # 按任务策略选择后端（使 CachePolicy.backend 真正生效）
        cache_backend: Optional[CacheBackend] = None
        if self._router is not None:
            # namespace: 优先使用 key_prefix（可在 YAML 中作为隔离维度）；为空则走默认空间
            namespace = policy.key_prefix or None
            cache_backend = self._router.get(policy.backend, namespace=namespace)
        else:
            cache_backend = self._cache

        if cache_backend is None:
            next(ctx)
            return

        # 计算缓存键
        cache_key = self._compute_cache_key(ctx, policy)

        # 检查缓存（仅在允许跳过时才读取并短路）
        if skip_cached:
            cached_result = cache_backend.get(cache_key)
            if cached_result is not None:
                logger.debug(f"Cache hit for task: {ctx.task_name}")
                ctx.result = cached_result
                ctx.skip_execution = True
                ctx.task_run.mark_cached(cached_result, cache_key)
                ctx.metadata['cache_hit'] = True
                # 继续调用 next() 让后续中间件（如 TimingMiddleware）有机会执行 after 逻辑
                next(ctx)
                return

        # 执行任务
        next(ctx)

        # 存储到缓存
        if ctx.result is not None and ctx.error is None:
            cache_backend.set(cache_key, ctx.result, ttl=policy.ttl_seconds)
            ctx.metadata['cache_key'] = cache_key

    def _compute_cache_key(
        self,
        ctx: MiddlewareContext,
        policy: CachePolicy,
    ) -> str:
        """计算缓存键"""
        import hashlib
        import json

        # 收集用于缓存键的参数
        params = {}
        for key, value in ctx.inputs.items():
            if policy.include_params and key not in policy.include_params:
                continue
            if key in policy.exclude_params:
                continue

            try:
                params[key] = self._make_hashable(value)
            except (TypeError, ValueError) as e:
                # 无法哈希的值使用字符串表示
                logger.debug(f"Cannot hash param '{key}': {e}, using str()")
                params[key] = str(value)

        # 构建键
        key_parts = [
            policy.key_prefix,
            ctx.task_name,
            ctx.task_run.spec.method,
            json.dumps(params, sort_keys=True, default=str),
        ]

        key_str = ":".join(str(p) for p in key_parts if p)
        return hashlib.md5(key_str.encode()).hexdigest()

    def _make_hashable(self, value: Any) -> Any:
        """将值转换为可哈希形式"""
        if isinstance(value, dict):
            return tuple(sorted(
                (k, self._make_hashable(v))
                for k, v in value.items()
            ))
        if isinstance(value, list):
            return tuple(self._make_hashable(v) for v in value)
        return value


class ValidationMiddleware(ExecutionMiddlewareBase):
    """验证中间件

    验证输入参数。
    """
    name = "validation"
    order = 20  # 早执行，尽早发现输入错误

    def handle(self, ctx: MiddlewareContext, next: NextMiddleware) -> None:
        spec = ctx.task_run.spec

        # 检查必需输入
        for inp in spec.inputs:
            if inp.required and inp.name not in ctx.inputs:
                if inp.source is None:  # 非引用参数
                    raise ValueError(
                        f"Missing required input '{inp.name}' for task '{ctx.task_name}'"
                    )

        next(ctx)


class ErrorHandlingMiddleware(ExecutionMiddlewareBase):
    """错误处理中间件

    捕获和格式化错误。
    """
    name = "error_handling"
    order = 1  # 最早执行，捕获所有错误

    def handle(self, ctx: MiddlewareContext, next: NextMiddleware) -> None:
        try:
            next(ctx)
        except Exception as e:
            # 记录错误详情
            ctx.error = e
            ctx.metadata['error_type'] = type(e).__name__
            ctx.metadata['error_message'] = str(e)
            ctx.metadata['error_traceback'] = traceback.format_exc()
            raise


# =============================================================================
# 中间件链
# =============================================================================

class ExecutionMiddlewareChain:
    """执行中间件链

    管理和执行中间件，提供可配置的请求处理流水线。

    Example:
        chain = ExecutionMiddlewareChain()
        chain.use(LoggingMiddleware())
        chain.use(RetryMiddleware())
        chain.use(CacheMiddleware(cache))
        chain.use(ExecutionMiddleware())  # 最后添加执行中间件

        chain.execute(ctx)
    """

    def __init__(self) -> None:
        self._middlewares: List[ExecutionMiddlewareBase] = []

    def use(self, middleware: ExecutionMiddlewareBase) -> 'ExecutionMiddlewareChain':
        """添加中间件到链末尾"""
        self._middlewares.append(middleware)
        return self

    def insert(self, index: int, middleware: ExecutionMiddlewareBase) -> 'ExecutionMiddlewareChain':
        """在指定位置插入中间件"""
        self._middlewares.insert(index, middleware)
        return self

    def remove(self, middleware_type: type) -> bool:
        """移除指定类型的中间件

        Returns:
            是否成功移除
        """
        for i, m in enumerate(self._middlewares):
            if isinstance(m, middleware_type):
                self._middlewares.pop(i)
                return True
        return False

    def execute(self, ctx: MiddlewareContext) -> None:
        """执行中间件链

        Raises:
            RuntimeError: 中间件链为空时
        """
        if not self._middlewares:
            raise RuntimeError("No middlewares in chain")

        # 语义一致性：按 order 排序执行（同 order 保持插入顺序）
        middlewares = sorted(self._middlewares, key=lambda m: m.order)

        # 构建执行链
        def build_chain(index: int) -> NextMiddleware:
            if index >= len(middlewares):
                return lambda ctx: None  # 终止

            middleware = middlewares[index]
            next_fn = build_chain(index + 1)

            return lambda ctx: middleware.handle(ctx, next_fn)

        chain = build_chain(0)
        chain(ctx)

    @classmethod
    def default(
        cls,
        cache_backend: Optional['CacheBackend'] = None,
        cache_router: Optional[CacheBackendRouter] = None,
    ) -> 'ExecutionMiddlewareChain':
        """创建默认中间件链

        Args:
            cache_backend: 可选的缓存后端，如提供则添加 CacheMiddleware

        Returns:
            配置好的中间件链
        """
        chain = cls()
        chain.use(ErrorHandlingMiddleware())
        chain.use(LoggingMiddleware())
        chain.use(TimingMiddleware())
        chain.use(TimeoutMiddleware())
        chain.use(ValidationMiddleware())
        if cache_router is not None:
            chain.use(CacheMiddleware(cache_router=cache_router))
        elif cache_backend is not None:
            chain.use(CacheMiddleware(cache_backend))
        chain.use(RetryMiddleware())
        chain.use(ExecutionMiddleware())
        return chain

    def __len__(self) -> int:
        return len(self._middlewares)

    def __iter__(self):
        return iter(self._middlewares)
