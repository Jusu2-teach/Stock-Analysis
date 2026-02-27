"""
Pipeline Events Types - 预定义事件类型
======================================

包含 Pipeline 工作流所需的所有事件类型定义。

设计原则:
1. 扁平化 - 只保留必要的事件类型
2. 负载类型化 - 每个事件有明确的负载结构
3. 便捷创建 - 类方法快速构建事件

事件层级:
- flow.* - 工作流级别
- task.* - 任务级别
- data.* - 数据操作
- cache.* - 缓存操作
- system.* - 系统级别
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .bus import Event

__all__ = [
    # 负载类型
    "FlowPayload",
    "TaskPayload",
    "DataPayload",
    "CachePayload",
    "SystemPayload",
    "ErrorPayload",
    # 事件创建
    "FlowEvents",
    "TaskEvents",
    "DataEvents",
    "CacheEvents",
    "SystemEvents",
    # 事件类型常量
    "EventType",
]


# =============================================================================
# Payload Types
# =============================================================================

@dataclass
class FlowPayload:
    """工作流事件负载"""
    flow_id: str
    flow_name: str
    step_count: int = 0
    current_step: int = 0
    duration_ms: float = 0.0
    status: str = "running"
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TaskPayload:
    """任务事件负载"""
    task_id: str
    task_name: str
    flow_id: Optional[str] = None
    method: Optional[str] = None
    duration_ms: float = 0.0
    status: str = "running"
    progress: float = 0.0  # 0.0 ~ 1.0
    result: Any = None
    error: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataPayload:
    """数据事件负载"""
    key: str
    namespace: str = "default"
    operation: str = "produce"  # produce | consume | aggregate
    data_type: Optional[str] = None
    row_count: Optional[int] = None
    source: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CachePayload:
    """缓存事件负载"""
    key: str
    operation: str  # hit | miss | set | invalidate
    ttl_seconds: Optional[int] = None
    size_bytes: Optional[int] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SystemPayload:
    """系统事件负载"""
    component: str
    message: str
    level: str = "info"  # debug | info | warning | error
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ErrorPayload:
    """错误事件负载"""
    error_type: str
    message: str
    source: Optional[str] = None
    stack_trace: Optional[str] = None
    recoverable: bool = True
    context: Dict[str, Any] = field(default_factory=dict)


# =============================================================================
# Event Type Constants
# =============================================================================

class EventType:
    """事件类型常量

    使用常量避免字符串拼写错误。

    Examples:
        bus.subscribe(EventType.TASK_COMPLETED, handler)
    """
    # Flow
    FLOW_STARTED = "flow.started"
    FLOW_COMPLETED = "flow.completed"
    FLOW_FAILED = "flow.failed"
    FLOW_PROGRESS = "flow.progress"

    # Task
    TASK_STARTED = "task.started"
    TASK_COMPLETED = "task.completed"
    TASK_FAILED = "task.failed"
    TASK_PROGRESS = "task.progress"
    TASK_SKIPPED = "task.skipped"
    TASK_RETRYING = "task.retrying"

    # Data
    DATA_PRODUCED = "data.produced"
    DATA_CONSUMED = "data.consumed"
    DATA_AGGREGATED = "data.aggregated"

    # Cache
    CACHE_HIT = "cache.hit"
    CACHE_MISS = "cache.miss"
    CACHE_SET = "cache.set"
    CACHE_INVALIDATED = "cache.invalidated"

    # System
    SYSTEM_STARTED = "system.started"
    SYSTEM_SHUTDOWN = "system.shutdown"
    SYSTEM_WARNING = "system.warning"
    SYSTEM_ERROR = "system.error"


# =============================================================================
# Event Factories
# =============================================================================

class FlowEvents:
    """工作流事件工厂

    Examples:
        event = FlowEvents.started("flow-1", "analysis", step_count=10)
        bus.emit(event)
    """

    @staticmethod
    def started(
        flow_id: str,
        flow_name: str,
        step_count: int = 0,
        trace_id: str = None,
    ) -> Event[FlowPayload]:
        """创建工作流开始事件"""
        return Event.create(
            EventType.FLOW_STARTED,
            FlowPayload(flow_id=flow_id, flow_name=flow_name, step_count=step_count),
            trace_id=trace_id,
        )

    @staticmethod
    def completed(
        flow_id: str,
        flow_name: str,
        duration_ms: float,
        trace_id: str = None,
    ) -> Event[FlowPayload]:
        """创建工作流完成事件"""
        return Event.create(
            EventType.FLOW_COMPLETED,
            FlowPayload(
                flow_id=flow_id,
                flow_name=flow_name,
                duration_ms=duration_ms,
                status="completed",
            ),
            trace_id=trace_id,
        )

    @staticmethod
    def failed(
        flow_id: str,
        flow_name: str,
        error: str,
        trace_id: str = None,
    ) -> Event[FlowPayload]:
        """创建工作流失败事件"""
        return Event.create(
            EventType.FLOW_FAILED,
            FlowPayload(
                flow_id=flow_id,
                flow_name=flow_name,
                status="failed",
                extra={"error": error},
            ),
            trace_id=trace_id,
        )

    @staticmethod
    def progress(
        flow_id: str,
        flow_name: str,
        current_step: int,
        step_count: int,
        trace_id: str = None,
    ) -> Event[FlowPayload]:
        """创建工作流进度事件"""
        return Event.create(
            EventType.FLOW_PROGRESS,
            FlowPayload(
                flow_id=flow_id,
                flow_name=flow_name,
                current_step=current_step,
                step_count=step_count,
            ),
            trace_id=trace_id,
        )


class TaskEvents:
    """任务事件工厂

    Examples:
        event = TaskEvents.completed("task-1", "Analyze_ROIC", duration_ms=1234)
        bus.emit(event)
    """

    @staticmethod
    def started(
        task_id: str,
        task_name: str,
        flow_id: str = None,
        method: str = None,
        trace_id: str = None,
    ) -> Event[TaskPayload]:
        """创建任务开始事件"""
        return Event.create(
            EventType.TASK_STARTED,
            TaskPayload(
                task_id=task_id,
                task_name=task_name,
                flow_id=flow_id,
                method=method,
                status="running",
            ),
            trace_id=trace_id,
        )

    @staticmethod
    def completed(
        task_id: str,
        task_name: str,
        duration_ms: float,
        result: Any = None,
        trace_id: str = None,
    ) -> Event[TaskPayload]:
        """创建任务完成事件"""
        return Event.create(
            EventType.TASK_COMPLETED,
            TaskPayload(
                task_id=task_id,
                task_name=task_name,
                duration_ms=duration_ms,
                status="completed",
                result=result,
            ),
            trace_id=trace_id,
        )

    @staticmethod
    def failed(
        task_id: str,
        task_name: str,
        error: str,
        trace_id: str = None,
    ) -> Event[TaskPayload]:
        """创建任务失败事件"""
        return Event.create(
            EventType.TASK_FAILED,
            TaskPayload(
                task_id=task_id,
                task_name=task_name,
                status="failed",
                error=error,
            ),
            trace_id=trace_id,
        )

    @staticmethod
    def progress(
        task_id: str,
        task_name: str,
        progress: float,
        trace_id: str = None,
    ) -> Event[TaskPayload]:
        """创建任务进度事件"""
        return Event.create(
            EventType.TASK_PROGRESS,
            TaskPayload(
                task_id=task_id,
                task_name=task_name,
                progress=progress,
            ),
            trace_id=trace_id,
        )

    @staticmethod
    def skipped(
        task_id: str,
        task_name: str,
        reason: str,
        trace_id: str = None,
    ) -> Event[TaskPayload]:
        """创建任务跳过事件"""
        return Event.create(
            EventType.TASK_SKIPPED,
            TaskPayload(
                task_id=task_id,
                task_name=task_name,
                status="skipped",
                extra={"reason": reason},
            ),
            trace_id=trace_id,
        )


class DataEvents:
    """数据事件工厂

    Examples:
        event = DataEvents.produced("roic", "trends", row_count=100)
        bus.emit(event)
    """

    @staticmethod
    def produced(
        key: str,
        namespace: str = "default",
        data_type: str = None,
        row_count: int = None,
        source: str = None,
        trace_id: str = None,
    ) -> Event[DataPayload]:
        """创建数据产出事件"""
        return Event.create(
            EventType.DATA_PRODUCED,
            DataPayload(
                key=key,
                namespace=namespace,
                operation="produce",
                data_type=data_type,
                row_count=row_count,
                source=source,
            ),
            trace_id=trace_id,
        )

    @staticmethod
    def consumed(
        key: str,
        namespace: str = "default",
        consumer: str = None,
        trace_id: str = None,
    ) -> Event[DataPayload]:
        """创建数据消费事件"""
        return Event.create(
            EventType.DATA_CONSUMED,
            DataPayload(
                key=key,
                namespace=namespace,
                operation="consume",
                extra={"consumer": consumer} if consumer else {},
            ),
            trace_id=trace_id,
        )

    @staticmethod
    def aggregated(
        namespace: str,
        keys: List[str],
        trace_id: str = None,
    ) -> Event[DataPayload]:
        """创建数据聚合事件"""
        return Event.create(
            EventType.DATA_AGGREGATED,
            DataPayload(
                key=f"aggregated_{namespace}",
                namespace=namespace,
                operation="aggregate",
                extra={"keys": keys},
            ),
            trace_id=trace_id,
        )


class CacheEvents:
    """缓存事件工厂

    Examples:
        event = CacheEvents.hit("task:analyze_roic:v1")
        bus.emit(event)
    """

    @staticmethod
    def hit(key: str, trace_id: str = None) -> Event[CachePayload]:
        """创建缓存命中事件"""
        return Event.create(
            EventType.CACHE_HIT,
            CachePayload(key=key, operation="hit"),
            trace_id=trace_id,
        )

    @staticmethod
    def miss(key: str, trace_id: str = None) -> Event[CachePayload]:
        """创建缓存未命中事件"""
        return Event.create(
            EventType.CACHE_MISS,
            CachePayload(key=key, operation="miss"),
            trace_id=trace_id,
        )

    @staticmethod
    def set(
        key: str,
        ttl_seconds: int = None,
        size_bytes: int = None,
        trace_id: str = None,
    ) -> Event[CachePayload]:
        """创建缓存设置事件"""
        return Event.create(
            EventType.CACHE_SET,
            CachePayload(
                key=key,
                operation="set",
                ttl_seconds=ttl_seconds,
                size_bytes=size_bytes,
            ),
            trace_id=trace_id,
        )

    @staticmethod
    def invalidated(key: str, trace_id: str = None) -> Event[CachePayload]:
        """创建缓存失效事件"""
        return Event.create(
            EventType.CACHE_INVALIDATED,
            CachePayload(key=key, operation="invalidate"),
            trace_id=trace_id,
        )


class SystemEvents:
    """系统事件工厂

    Examples:
        event = SystemEvents.started("pipeline", "Pipeline v2.0 started")
        bus.emit(event)
    """

    @staticmethod
    def started(
        component: str,
        message: str,
        trace_id: str = None,
    ) -> Event[SystemPayload]:
        """创建系统启动事件"""
        return Event.create(
            EventType.SYSTEM_STARTED,
            SystemPayload(component=component, message=message),
            trace_id=trace_id,
        )

    @staticmethod
    def shutdown(
        component: str,
        message: str,
        trace_id: str = None,
    ) -> Event[SystemPayload]:
        """创建系统关闭事件"""
        return Event.create(
            EventType.SYSTEM_SHUTDOWN,
            SystemPayload(component=component, message=message),
            trace_id=trace_id,
        )

    @staticmethod
    def warning(
        component: str,
        message: str,
        trace_id: str = None,
    ) -> Event[SystemPayload]:
        """创建系统警告事件"""
        return Event.create(
            EventType.SYSTEM_WARNING,
            SystemPayload(component=component, message=message, level="warning"),
            trace_id=trace_id,
        )

    @staticmethod
    def error(
        component: str,
        message: str,
        error: Exception = None,
        trace_id: str = None,
    ) -> Event[SystemPayload]:
        """创建系统错误事件"""
        extra = {"error_type": type(error).__name__} if error else {}
        return Event.create(
            EventType.SYSTEM_ERROR,
            SystemPayload(
                component=component,
                message=message,
                level="error",
                extra=extra,
            ),
            trace_id=trace_id,
        )
