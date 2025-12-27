"""
Events - 标准化事件定义
======================

所有跨组件通信的事件类型定义。

事件命名规范：
- 使用点分隔的层级命名：{domain}.{entity}.{action}
- 例如：pipeline.node.started, registry.method.registered

事件分类：
1. 注册事件 (registry.*)     - 方法注册/发现
2. Pipeline 事件 (pipeline.*) - 流水线执行
3. 系统事件 (system.*)       - 启动/关闭/健康
"""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional, List
from abc import ABC
import uuid


@dataclass
class Event(ABC):
    """事件基类

    所有事件必须继承此类，确保统一的元数据结构。
    """
    # 元数据（自动填充）
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    source: str = ""  # 发布者标识

    @property
    def event_type(self) -> str:
        """事件类型（由子类定义）"""
        raise NotImplementedError


# ============================================================================
# 注册相关事件 (registry.*)
# ============================================================================

@dataclass
class MethodRegisteredEvent(Event):
    """方法注册事件

    当新方法通过 @register_method 注册到 Registry 时触发。
    """
    component: str = ""
    method: str = ""
    engine_type: str = ""
    engine_name: str = ""
    version: str = ""
    priority: int = 0
    full_key: str = ""

    @property
    def event_type(self) -> str:
        return "registry.method.registered"


@dataclass
class MethodExecutedEvent(Event):
    """方法执行事件

    当通过 Orchestrator 执行方法时触发。
    """
    component: str = ""
    method: str = ""
    engine: str = ""
    duration_ms: float = 0.0
    success: bool = True
    error: Optional[str] = None
    args_summary: str = ""  # 参数摘要（避免敏感数据）

    @property
    def event_type(self) -> str:
        return "registry.method.executed"


@dataclass
class MethodSelectedEvent(Event):
    """方法选择事件

    当 Registry 选择最佳方法实现时触发。
    """
    component: str = ""
    method: str = ""
    selected_engine: str = ""
    candidates_count: int = 0
    strategy: str = ""

    @property
    def event_type(self) -> str:
        return "registry.method.selected"


# ============================================================================
# Pipeline 相关事件 (pipeline.*)
# ============================================================================

@dataclass
class PipelineStartedEvent(Event):
    """Pipeline 启动事件"""
    pipeline_name: str = ""
    config_path: str = ""
    total_steps: int = 0
    execution_order: List[str] = field(default_factory=list)

    @property
    def event_type(self) -> str:
        return "pipeline.flow.started"


@dataclass
class PipelineCompletedEvent(Event):
    """Pipeline 完成事件"""
    pipeline_name: str = ""
    status: str = ""  # 'success' | 'failed' | 'partial'
    duration_sec: float = 0.0
    executed_steps: int = 0
    cached_steps: int = 0
    failed_steps: int = 0
    error: Optional[str] = None

    @property
    def event_type(self) -> str:
        return "pipeline.flow.completed"


@dataclass
class NodeStartedEvent(Event):
    """节点启动事件"""
    step_name: str = ""
    pipeline_name: str = ""
    inputs: List[str] = field(default_factory=list)
    outputs: List[str] = field(default_factory=list)
    signature: str = ""

    @property
    def event_type(self) -> str:
        return "pipeline.node.started"


@dataclass
class NodeCompletedEvent(Event):
    """节点完成事件"""
    step_name: str = ""
    pipeline_name: str = ""
    status: str = ""  # 'success' | 'failed' | 'cached'
    duration_ms: float = 0.0
    output_count: int = 0
    error: Optional[str] = None
    metrics: Dict[str, Any] = field(default_factory=dict)

    @property
    def event_type(self) -> str:
        return "pipeline.node.completed"


@dataclass
class CacheHitEvent(Event):
    """缓存命中事件"""
    step_name: str = ""
    signature: str = ""
    outputs: List[str] = field(default_factory=list)
    saved_time_ms: float = 0.0  # 估算节省的时间

    @property
    def event_type(self) -> str:
        return "pipeline.cache.hit"


@dataclass
class CacheInvalidatedEvent(Event):
    """缓存失效事件"""
    step_name: str = ""
    reason: str = ""  # 'signature_changed' | 'ttl_expired' | 'manual'
    old_signature: str = ""
    new_signature: str = ""

    @property
    def event_type(self) -> str:
        return "pipeline.cache.invalidated"


@dataclass
class PipelineErrorEvent(Event):
    """Pipeline 错误事件"""
    step_name: str = ""
    error: str = ""
    traceback: str = ""
    context: Dict[str, Any] = field(default_factory=dict)

    @property
    def event_type(self) -> str:
        return "pipeline.error"


@dataclass
class RegistryRefreshedEvent(Event):
    """注册表刷新事件"""
    mode: str = ""  # 'full' | 'incremental'
    method_count: int = 0

    @property
    def event_type(self) -> str:
        return "registry.refreshed"


# ============================================================================
# 系统级事件 (system.*)
# ============================================================================

@dataclass
class SystemReadyEvent(Event):
    """系统就绪事件

    当所有组件初始化完成时触发。
    """
    components: List[str] = field(default_factory=list)
    registered_methods: int = 0
    version: str = ""

    @property
    def event_type(self) -> str:
        return "system.ready"


@dataclass
class ComponentLoadedEvent(Event):
    """组件加载事件"""
    component_name: str = ""
    component_type: str = ""  # 'orchestrator' | 'pipeline' | 'business'
    methods_count: int = 0
    load_time_ms: float = 0.0

    @property
    def event_type(self) -> str:
        return "system.component.loaded"


@dataclass
class ErrorEvent(Event):
    """错误事件

    通用错误事件，用于跨组件错误通知。
    """
    error_type: str = ""
    message: str = ""
    component: str = ""
    stack_trace: str = ""
    context: Dict[str, Any] = field(default_factory=dict)

    @property
    def event_type(self) -> str:
        return "system.error"


@dataclass
class MetricEvent(Event):
    """指标事件

    用于发布性能/业务指标。
    """
    metric_name: str = ""
    value: float = 0.0
    unit: str = ""
    tags: Dict[str, str] = field(default_factory=dict)

    @property
    def event_type(self) -> str:
        return "system.metric"


# ============================================================================
# 数据相关事件 (data.*)
# ============================================================================

@dataclass
class DataLoadedEvent(Event):
    """数据加载事件"""
    dataset_name: str = ""
    source: str = ""  # 'tushare' | 'akshare' | 'file'
    row_count: int = 0
    column_count: int = 0
    duration_ms: float = 0.0

    @property
    def event_type(self) -> str:
        return "data.loaded"


@dataclass
class DataTransformedEvent(Event):
    """数据转换事件"""
    dataset_name: str = ""
    transformation: str = ""
    input_rows: int = 0
    output_rows: int = 0
    duration_ms: float = 0.0

    @property
    def event_type(self) -> str:
        return "data.transformed"


__all__ = [
    # Base
    'Event',
    # Registry Events
    'MethodRegisteredEvent',
    'MethodExecutedEvent',
    'MethodSelectedEvent',
    'RegistryRefreshedEvent',
    # Pipeline Events
    'PipelineStartedEvent',
    'PipelineCompletedEvent',
    'PipelineErrorEvent',
    'NodeStartedEvent',
    'NodeCompletedEvent',
    'CacheHitEvent',
    'CacheInvalidatedEvent',
    # System Events
    'SystemReadyEvent',
    'ComponentLoadedEvent',
    'ErrorEvent',
    'MetricEvent',
    # Data Events
    'DataLoadedEvent',
    'DataTransformedEvent',
]
