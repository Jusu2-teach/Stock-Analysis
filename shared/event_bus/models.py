"""
EventBus 核心模型定义
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Callable, Optional, Any, List, TYPE_CHECKING
from enum import Enum, IntEnum, auto
from datetime import datetime
import uuid

if TYPE_CHECKING:
    from ..events import Event


class EventPriority(IntEnum):
    """事件处理器优先级

    数值越小越先执行（类似 CSS z-index）。
    """
    SYSTEM = 0      # 系统级（日志、监控）最先执行
    HIGH = 25       # 高优先级
    NORMAL = 50     # 默认优先级
    LOW = 75        # 低优先级
    LAST = 100      # 最后执行（清理、统计）


class HandlerType(Enum):
    """处理器类型"""
    SYNC = auto()       # 同步处理器
    ASYNC = auto()      # 异步处理器
    WRAPPER = auto()    # 拦截器
    STREAM = auto()     # 流处理器


@dataclass
class HandlerInfo:
    """处理器信息"""
    handler: Callable  # 处理器函数
    event_type: str = ""  # 事件类型
    priority: int = 0  # 整数优先级（数值越小越先执行）
    once: bool = False  # 是否只执行一次
    handler_type: HandlerType = HandlerType.SYNC
    filter_fn: Optional[Callable[['Event'], bool]] = None
    name: str = ""
    source: str = ""  # 注册来源（组件名）
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())

    # 执行统计
    call_count: int = 0
    error_count: int = 0
    total_time_ms: float = 0.0
    last_called: Optional[str] = None

    def __post_init__(self):
        if not self.name and hasattr(self.handler, '__name__'):
            self.name = self.handler.__name__

    def mark_called(self, duration_ms: float = 0.0):
        """标记被调用"""
        self.call_count += 1
        self.total_time_ms += duration_ms
        self.last_called = datetime.now().isoformat()

    def mark_error(self):
        """标记错误"""
        self.error_count += 1

    @property
    def is_wrapper(self) -> bool:
        return self.handler_type == HandlerType.WRAPPER

    @property
    def is_async(self) -> bool:
        return self.handler_type == HandlerType.ASYNC

    @property
    def avg_time_ms(self) -> float:
        if self.call_count == 0:
            return 0.0
        return self.total_time_ms / self.call_count


@dataclass
class EmitResult:
    """事件发布结果"""
    event_type: str = ""
    handler_count: int = 0
    success_count: int = 0
    error_count: int = 0
    duration_ms: float = 0.0  # 改名为 duration_ms 保持一致
    errors: List[str] = field(default_factory=list)  # 错误消息列表
    results: List[Any] = field(default_factory=list)  # 处理器返回值

    # 扩展信息
    event_id: str = ""
    success: bool = True
    is_dead_letter: bool = False
    is_historic: bool = False
    middleware_time_ms: float = 0.0

    @property
    def partial_success(self) -> bool:
        return self.success_count > 0 and self.error_count > 0

    def __repr__(self) -> str:
        status = "✓" if self.success else ("⚠" if self.partial_success else "✗")
        return (
            f"EmitResult({status} {self.event_type}: "
            f"{self.success_count}/{self.handler_count} handlers, "
            f"{self.duration_ms:.2f}ms)"
        )


@dataclass
class EventBusStats:
    """EventBus 统计信息"""
    # 核心统计
    total_handlers: int = 0
    total_emits: int = 0
    total_handlers_invoked: int = 0  # 别名
    total_handlers_called: int = 0
    total_errors: int = 0

    # 事件类型列表
    event_types: list = field(default_factory=list)

    # 运行时间
    uptime_seconds: float = 0.0

    # 组件统计
    dead_letter_stats: Any = None
    historic_stats: Any = None
    subscription_stats: Any = None
    middleware_list: list = field(default_factory=list)

    # 性能统计
    total_time_ms: float = 0.0
    max_handler_time_ms: float = 0.0
    avg_handler_time_ms: float = 0.0

    # 按事件类型统计
    events_by_type: dict = field(default_factory=dict)

    def __post_init__(self):
        """初始化后处理"""
        # 同步别名
        if self.total_handlers_called:
            self.total_handlers_invoked = self.total_handlers_called

    def record_emit(self, result: EmitResult):
        """记录发布结果"""
        self.total_emits += 1
        self.total_handlers_called += result.handler_count
        self.total_handlers_invoked = self.total_handlers_called
        self.total_errors += result.error_count
        self.total_time_ms += result.total_time_ms

        # 按类型统计
        if result.event_type not in self.events_by_type:
            self.events_by_type[result.event_type] = {
                'count': 0,
                'errors': 0,
                'time_ms': 0.0
            }

        stats = self.events_by_type[result.event_type]
        stats['count'] += 1
        stats['errors'] += result.error_count
        stats['time_ms'] += result.total_time_ms

    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            'total_handlers': self.total_handlers,
            'total_emits': self.total_emits,
            'total_handlers_called': self.total_handlers_called,
            'total_errors': self.total_errors,
            'event_types': self.event_types,
            'uptime_seconds': self.uptime_seconds,
            'total_time_ms': self.total_time_ms,
            'events_by_type': self.events_by_type,
        }
