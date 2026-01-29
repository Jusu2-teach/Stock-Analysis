"""
Pipeline Execution - Runner, Executor, and Middleware.

Components:
    - runner.py: FlowRunner - orchestrates entire flow execution
    - executor.py: TaskExecutor - executes individual tasks
    - middleware.py: Middleware chain for cross-cutting concerns
"""

from .middleware import (
    ExecutionMiddlewareBase,
    ExecutionMiddlewareChain,
    MiddlewareContext,
    NextMiddleware,
    # Built-in middlewares
    ExecutionMiddleware,
    LoggingMiddleware,
    TimingMiddleware,
    RetryMiddleware,
    CacheMiddleware,
    ValidationMiddleware,
    ErrorHandlingMiddleware,
    # Cache backend
    CacheBackend,
)

from .executor import (
    TaskExecutor,
    ExecutorConfig,
)

from ..protocols import (
    MethodResolverProtocol,
    MethodInfo,
)

from .runner import (
    FlowRunner,
    RunnerConfig,
    DryRunResult,
)

__all__ = [
    # Middleware
    "ExecutionMiddlewareBase",
    "ExecutionMiddlewareChain",
    "MiddlewareContext",
    "NextMiddleware",
    "ExecutionMiddleware",
    "LoggingMiddleware",
    "TimingMiddleware",
    "RetryMiddleware",
    "CacheMiddleware",
    "ValidationMiddleware",
    "ErrorHandlingMiddleware",
    "CacheBackend",
    # Executor
    "TaskExecutor",
    "ExecutorConfig",
    # Protocols
    "MethodResolverProtocol",
    "MethodInfo",
    # Runner
    "FlowRunner",
    "RunnerConfig",
    "DryRunResult",
]
