"""HookManager: 事件钩子系统 (完全基于 EventBus)

基于统一 EventBus 架构的事件管理系统。

使用:
   from pipeline.core.services.hook_manager import HookManager
   hooks = HookManager.get()

   # 注册处理器
   @hooks.on('node.started')
   def handler(event): ...

   # 发布事件
   hooks.emit('node.started', step_name='load_data', inputs=['data.csv'])

事件类型:
   - pipeline.flow.started   : 流程启动
   - pipeline.flow.completed : 流程完成
   - pipeline.node.started   : 节点启动
   - pipeline.node.completed : 节点完成
   - pipeline.cache.hit      : 缓存命中
   - pipeline.error          : 错误发生
"""
from __future__ import annotations
from typing import Callable, Dict, Any, ClassVar, Optional, List
import threading
import logging

# 统一事件总线（必需依赖）
from shared import (
    EventBus,
    EventPriority,
    NodeStartedEvent,
    NodeCompletedEvent,
    CacheHitEvent,
    PipelineStartedEvent,
    PipelineCompletedEvent,
    PipelineErrorEvent,
)


logger = logging.getLogger(__name__)


class HookManager:
    """事件钩子管理器 (EventBus 包装器)

    提供简化的 API 包装统一 EventBus，用于 Pipeline 组件。
    """
    _instance: ClassVar[Optional['HookManager']] = None
    _lock: ClassVar[threading.Lock] = threading.Lock()

    # 事件名映射: 短名称 -> 完整 EventBus 事件名
    EVENT_ALIASES: ClassVar[Dict[str, str]] = {
        'before_flow': 'pipeline.flow.started',
        'after_flow': 'pipeline.flow.completed',
        'before_node': 'pipeline.node.started',
        'after_node': 'pipeline.node.completed',
        'on_cache_hit': 'pipeline.cache.hit',
        'on_failure': 'pipeline.error',
        # 新式命名
        'flow.started': 'pipeline.flow.started',
        'flow.completed': 'pipeline.flow.completed',
        'node.started': 'pipeline.node.started',
        'node.completed': 'pipeline.node.completed',
        'cache.hit': 'pipeline.cache.hit',
        'error': 'pipeline.error',
    }

    def __init__(self):
        self._event_bus = EventBus.get()
        self._logger = logging.getLogger(__name__)

    @classmethod
    def get(cls) -> 'HookManager':
        """获取单例实例（线程安全）"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = HookManager()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """重置单例（用于测试）"""
        with cls._lock:
            cls._instance = None

    def _resolve_event_name(self, event: str) -> str:
        """解析事件名称到完整格式"""
        return self.EVENT_ALIASES.get(event, event)

    def on(self, event: str, *, priority: EventPriority = EventPriority.NORMAL) -> Callable:
        """注册事件处理器

        Args:
            event: 事件名称 (支持别名)
            priority: 处理优先级

        Returns:
            装饰器函数

        Example:
            @hooks.on('node.completed')
            def handler(event):
                print(f"Node {event.step_name} completed")
        """
        full_event = self._resolve_event_name(event)
        return self._event_bus.on(full_event, priority=priority)

    def register(self, event: str, func: Callable) -> Callable:
        """注册事件处理器（函数式调用）

        Args:
            event: 事件名称
            func: 处理函数

        Returns:
            原函数
        """
        full_event = self._resolve_event_name(event)
        self._event_bus.on(full_event)(func)
        return func

    def emit(self, event: str, *args, **kwargs) -> None:
        """发布事件

        自动将参数转换为标准化事件对象。

        Args:
            event: 事件名称
            *args: 位置参数
            **kwargs: 关键字参数
        """
        # 转换为标准化事件对象
        event_obj = self._create_event(event, *args, **kwargs)
        if event_obj:
            self._event_bus.emit(event_obj)

    def _create_event(self, event: str, *args, **kwargs) -> Optional[Any]:
        """根据事件类型创建标准化事件对象"""
        try:
            if event in ('before_node', 'node.started'):
                step_name = args[0] if args else kwargs.get('step_name', '')
                ctx = args[1] if len(args) > 1 else kwargs.get('context', {})
                return NodeStartedEvent(
                    step_name=step_name,
                    inputs=ctx.get('inputs', []) if isinstance(ctx, dict) else [],
                    outputs=ctx.get('planned_outputs', []) if isinstance(ctx, dict) else [],
                    signature=ctx.get('signature', '') if isinstance(ctx, dict) else '',
                    source='pipeline'
                )

            elif event in ('after_node', 'node.completed'):
                step_name = args[0] if args else kwargs.get('step_name', '')
                ctx = args[1] if len(args) > 1 else kwargs.get('context', {})
                metrics = args[2] if len(args) > 2 else kwargs.get('metrics', {})
                return NodeCompletedEvent(
                    step_name=step_name,
                    status='failed' if ctx.get('failed') else 'success',
                    duration_ms=(ctx.get('duration_sec') or 0) * 1000,
                    metrics=metrics if isinstance(metrics, dict) else {},
                    source='pipeline'
                )

            elif event in ('on_cache_hit', 'cache.hit'):
                step_name = args[0] if args else kwargs.get('step_name', '')
                metrics = args[1] if len(args) > 1 else kwargs.get('metrics', {})
                return CacheHitEvent(
                    step_name=step_name,
                    signature=metrics.get('signature', '') if isinstance(metrics, dict) else '',
                    outputs=metrics.get('outputs', []) if isinstance(metrics, dict) else [],
                    source='pipeline'
                )

            elif event in ('before_flow', 'flow.started'):
                ctx = args[0] if args else kwargs
                return PipelineStartedEvent(
                    pipeline_name=ctx.get('pipeline_name', '') if isinstance(ctx, dict) else '',
                    source='pipeline'
                )

            elif event in ('after_flow', 'flow.completed'):
                result = args[0] if args else kwargs
                return PipelineCompletedEvent(
                    status=result.get('status', '') if isinstance(result, dict) else '',
                    duration_sec=(result.get('duration_sec') or 0) if isinstance(result, dict) else 0,
                    executed_steps=result.get('executed_steps', 0) if isinstance(result, dict) else 0,
                    source='pipeline'
                )

            elif event in ('on_failure', 'error'):
                step_name = args[0] if args else kwargs.get('step_name', '')
                ctx = args[1] if len(args) > 1 else kwargs.get('context', {})
                error = ctx.get('error', '') if isinstance(ctx, dict) else str(ctx)
                return PipelineErrorEvent(
                    step_name=step_name,
                    error=error,
                    source='pipeline'
                )

            # 未知事件类型
            self._logger.debug(f"未知事件类型: {event}")
            return None

        except Exception as e:
            self._logger.warning(f"创建事件对象失败 ({event}): {e}")
            return None

    def get_stats(self) -> Dict[str, Any]:
        """获取事件统计"""
        return self._event_bus.get_stats()

    def list_handlers(self, event: str = None) -> Dict[str, List[str]]:
        """列出注册的处理器"""
        if event:
            full_event = self._resolve_event_name(event)
            return self._event_bus.list_handlers(full_event)
        return self._event_bus.list_handlers()


__all__ = ["HookManager"]
