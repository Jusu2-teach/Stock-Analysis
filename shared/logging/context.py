"""
日志上下文管理 (Log Context Management)
=======================================

参考设计:
- structlog: contextvars + bind/unbind
- loguru: contextualize
- OpenTelemetry: span context

提供线程安全的上下文绑定机制。
"""
from __future__ import annotations
from contextvars import ContextVar
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, Generator, Optional
import threading
import uuid


# 线程安全的上下文存储
_context_var: ContextVar[Dict[str, Any]] = ContextVar('log_context', default={})
_lock = threading.Lock()


@dataclass
class LogContextData:
    """日志上下文数据"""
    request_id: Optional[str] = None
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    component: Optional[str] = None
    operation: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典，排除 None 值"""
        result = {}
        for key in ['request_id', 'trace_id', 'span_id', 'user_id',
                    'session_id', 'component', 'operation']:
            value = getattr(self, key)
            if value is not None:
                result[key] = value
        result.update(self.extra)
        return result


def get_context() -> Dict[str, Any]:
    """获取当前上下文"""
    return _context_var.get().copy()


def bind_context(**kwargs) -> None:
    """绑定上下文数据

    Example:
        bind_context(request_id="abc123", user_id="user1")
    """
    current = _context_var.get().copy()
    current.update(kwargs)
    _context_var.set(current)


def unbind_context(*keys: str) -> None:
    """解除绑定指定的上下文键

    Example:
        unbind_context('request_id', 'user_id')
    """
    current = _context_var.get().copy()
    for key in keys:
        current.pop(key, None)
    _context_var.set(current)


def clear_context() -> None:
    """清除所有上下文"""
    _context_var.set({})


class LogContext:
    """日志上下文管理器

    提供作用域级别的上下文绑定，退出时自动恢复。

    Example:
        with LogContext(request_id="abc123"):
            logger.info("Processing")  # 自动携带 request_id

        # request_id 已移除

    支持嵌套:
        with LogContext(request_id="abc"):
            with LogContext(step="load"):
                logger.info("Loading")  # 携带 request_id + step
    """

    def __init__(
        self,
        request_id: Optional[str] = None,
        trace_id: Optional[str] = None,
        auto_request_id: bool = False,
        **kwargs
    ):
        """初始化上下文

        Args:
            request_id: 请求 ID
            trace_id: 追踪 ID
            auto_request_id: 是否自动生成 request_id
            **kwargs: 其他上下文数据
        """
        self._bindings: Dict[str, Any] = {}
        self._previous: Dict[str, Any] = {}

        if request_id:
            self._bindings['request_id'] = request_id
        elif auto_request_id:
            self._bindings['request_id'] = uuid.uuid4().hex[:12]

        if trace_id:
            self._bindings['trace_id'] = trace_id

        self._bindings.update(kwargs)

    def __enter__(self) -> 'LogContext':
        """进入上下文"""
        self._previous = get_context()
        bind_context(**self._bindings)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """退出上下文，恢复之前状态"""
        _context_var.set(self._previous)

    def bind(self, **kwargs) -> 'LogContext':
        """动态添加绑定"""
        self._bindings.update(kwargs)
        bind_context(**kwargs)
        return self

    @property
    def request_id(self) -> Optional[str]:
        """获取当前 request_id"""
        return self._bindings.get('request_id')


@contextmanager
def log_context(**kwargs) -> Generator[Dict[str, Any], None, None]:
    """函数式上下文管理器

    Example:
        with log_context(step="processing"):
            do_work()
    """
    with LogContext(**kwargs) as ctx:
        yield ctx._bindings


class ContextBinder:
    """上下文绑定器

    用于更复杂的上下文管理场景。

    Example:
        binder = ContextBinder()
        binder.bind(user_id="user1")
        binder.bind(session_id="sess1")

        # 稍后
        binder.unbind_all()
    """

    def __init__(self):
        self._bound_keys: list[str] = []

    def bind(self, **kwargs) -> 'ContextBinder':
        """绑定上下文"""
        bind_context(**kwargs)
        self._bound_keys.extend(kwargs.keys())
        return self

    def unbind(self, *keys: str) -> 'ContextBinder':
        """解除指定绑定"""
        unbind_context(*keys)
        for key in keys:
            if key in self._bound_keys:
                self._bound_keys.remove(key)
        return self

    def unbind_all(self) -> None:
        """解除所有绑定"""
        unbind_context(*self._bound_keys)
        self._bound_keys.clear()

    def __enter__(self) -> 'ContextBinder':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.unbind_all()


# Pipeline 专用上下文
class PipelineLogContext(LogContext):
    """Pipeline 日志上下文

    专门用于工作流执行的上下文。

    Example:
        with PipelineLogContext(workflow="analysis", step="load_data"):
            execute_step()
    """

    def __init__(
        self,
        workflow: str,
        step: Optional[str] = None,
        **kwargs
    ):
        super().__init__(
            component="pipeline",
            workflow=workflow,
            step=step,
            **kwargs
        )


# Registry 专用上下文
class RegistryLogContext(LogContext):
    """Registry 日志上下文"""

    def __init__(
        self,
        engine_type: str,
        method_name: Optional[str] = None,
        **kwargs
    ):
        super().__init__(
            component="registry",
            engine_type=engine_type,
            method_name=method_name,
            **kwargs
        )
