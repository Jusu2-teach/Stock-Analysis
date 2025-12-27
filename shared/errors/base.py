"""
错误基类 (Base Error Classes)
=============================

参考设计:
- Django: 分层异常 + 错误上下文
- FastAPI: 结构化异常信息
- requests: 简洁的异常链
- Sentry: 丰富的上下文数据
- Rich: 美化输出

核心特性:
1. 上下文携带 - 附加调试所需的任意数据
2. 错误链 - 保留原始异常信息
3. 可序列化 - 支持 JSON 序列化
4. EventBus 集成 - 自动发布错误事件
5. 格式化输出 - 人类可读的错误展示
"""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional, Type, TypeVar
import traceback
import json
import uuid

from .codes import ErrorCode, ErrorSeverity

T = TypeVar('T', bound='AStockError')


@dataclass
class ErrorContext:
    """错误上下文

    携带错误发生时的环境信息，便于调试和追踪。

    参考 Sentry 的 Context 设计。
    """
    # 唯一标识
    error_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])

    # 时间信息
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    # 来源信息
    source: str = ""           # 错误来源模块
    component: str = ""        # 组件名称
    operation: str = ""        # 操作名称

    # 附加数据
    data: Dict[str, Any] = field(default_factory=dict)

    # 追踪信息
    trace_id: Optional[str] = None    # 分布式追踪 ID
    span_id: Optional[str] = None     # Span ID

    # 用户信息（如适用）
    user_id: Optional[str] = None

    # 堆栈信息
    stack_trace: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'error_id': self.error_id,
            'timestamp': self.timestamp,
            'source': self.source,
            'component': self.component,
            'operation': self.operation,
            'data': self.data,
            'trace_id': self.trace_id,
            'span_id': self.span_id,
            'user_id': self.user_id,
            'stack_trace': self.stack_trace,
        }

    def to_json(self) -> str:
        """序列化为 JSON"""
        return json.dumps(self.to_dict(), ensure_ascii=False, default=str)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ErrorContext':
        """从字典创建"""
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})


class AStockError(Exception):
    """AStock 统一错误基类

    所有业务错误都应继承此类。

    Features:
    - 错误码支持
    - 上下文携带
    - 错误链
    - JSON 序列化
    - EventBus 集成

    Example:
        raise AStockError(
            "Something went wrong",
            code=ErrorCode.SYSTEM_INTERNAL_ERROR,
            context={"key": "value"}
        )
    """

    # 默认错误码（子类可覆盖）
    default_code: ErrorCode = ErrorCode.SYSTEM_INTERNAL_ERROR

    def __init__(
        self,
        message: str = "",
        *,
        code: Optional[ErrorCode] = None,
        context: Optional[Dict[str, Any] | ErrorContext] = None,
        cause: Optional[Exception] = None,
        emit_event: bool = True,
    ):
        """初始化错误

        Args:
            message: 错误消息
            code: 错误码（默认使用类的 default_code）
            context: 错误上下文（字典或 ErrorContext）
            cause: 原因异常（用于错误链）
            emit_event: 是否发布错误事件到 EventBus
        """
        self._code = code or self.default_code
        self._message = message or self._code.default_message

        # 构建上下文
        if isinstance(context, ErrorContext):
            self._context = context
        else:
            self._context = ErrorContext(data=context or {})

        # 捕获堆栈
        if self._context.stack_trace is None:
            self._context.stack_trace = traceback.format_exc()

        # 错误链
        self._cause = cause
        if cause:
            self.__cause__ = cause

        # 初始化父类
        super().__init__(self._message)

        # 发布错误事件
        if emit_event:
            self._emit_error_event()

    @property
    def code(self) -> ErrorCode:
        """错误码"""
        return self._code

    @property
    def error_code(self) -> str:
        """错误码字符串"""
        return self._code.code

    @property
    def message(self) -> str:
        """错误消息"""
        return self._message

    @property
    def severity(self) -> ErrorSeverity:
        """错误严重程度"""
        return self._code.severity

    @property
    def context(self) -> ErrorContext:
        """错误上下文"""
        return self._context

    @property
    def cause(self) -> Optional[Exception]:
        """原因异常"""
        return self._cause

    @property
    def error_id(self) -> str:
        """错误唯一 ID"""
        return self._context.error_id

    def with_context(self: T, **kwargs) -> T:
        """添加上下文数据（链式调用）

        Example:
            raise MyError("failed").with_context(step="load", file="data.csv")
        """
        self._context.data.update(kwargs)
        return self

    def with_cause(self: T, cause: Exception) -> T:
        """设置原因异常（链式调用）

        Example:
            try:
                risky_operation()
            except Exception as e:
                raise MyError("operation failed").with_cause(e)
        """
        self._cause = cause
        self.__cause__ = cause
        return self

    def with_source(self: T, source: str, component: str = "", operation: str = "") -> T:
        """设置来源信息（链式调用）"""
        self._context.source = source
        if component:
            self._context.component = component
        if operation:
            self._context.operation = operation
        return self

    def with_trace(self: T, trace_id: str, span_id: Optional[str] = None) -> T:
        """设置追踪信息（链式调用）"""
        self._context.trace_id = trace_id
        if span_id:
            self._context.span_id = span_id
        return self

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典（便于序列化）"""
        result = {
            'error_id': self.error_id,
            'code': self.error_code,
            'message': self.message,
            'severity': self.severity.name,
            'category': self._code.category,
            'context': self._context.to_dict(),
        }

        if self._cause:
            result['cause'] = {
                'type': type(self._cause).__name__,
                'message': str(self._cause),
            }

        return result

    def to_json(self) -> str:
        """序列化为 JSON"""
        return json.dumps(self.to_dict(), ensure_ascii=False, default=str)

    def _emit_error_event(self):
        """发布错误事件到 EventBus"""
        try:
            from shared import EventBus, ErrorEvent
            EventBus.get().emit(ErrorEvent(
                error_code=self.error_code,
                error_message=self.message,
                severity=self.severity.name,
                context=self._context.data,
                source=self._context.source or self.__class__.__name__,
            ))
        except ImportError:
            pass  # EventBus 未加载，静默忽略
        except Exception:
            pass  # 防止错误处理中再次出错

    def __str__(self) -> str:
        """字符串表示"""
        return f"[{self.error_code}] {self.message}"

    def __repr__(self) -> str:
        """详细表示"""
        return (
            f"{self.__class__.__name__}("
            f"code={self.error_code!r}, "
            f"message={self.message!r}, "
            f"error_id={self.error_id!r})"
        )


def format_error(error: AStockError, verbose: bool = False) -> str:
    """格式化错误输出

    Args:
        error: AStock 错误对象
        verbose: 是否显示详细信息

    Returns:
        格式化的错误字符串
    """
    lines = [
        f"╔══════════════════════════════════════════════════════════════",
        f"║ Error: {error.error_code}",
        f"║ ID: {error.error_id}",
        f"║ Severity: {error.severity.name}",
        f"╠══════════════════════════════════════════════════════════════",
        f"║ Message: {error.message}",
    ]

    if error.context.data:
        lines.append(f"╠══════════════════════════════════════════════════════════════")
        lines.append(f"║ Context:")
        for key, value in error.context.data.items():
            lines.append(f"║   {key}: {value}")

    if error.cause:
        lines.append(f"╠══════════════════════════════════════════════════════════════")
        lines.append(f"║ Caused by: {type(error.cause).__name__}: {error.cause}")

    if verbose and error.context.stack_trace:
        lines.append(f"╠══════════════════════════════════════════════════════════════")
        lines.append(f"║ Stack Trace:")
        for line in error.context.stack_trace.split('\n'):
            if line.strip():
                lines.append(f"║   {line}")

    lines.append(f"╚══════════════════════════════════════════════════════════════")

    return '\n'.join(lines)


# 类型别名
ErrorType = Type[AStockError]
