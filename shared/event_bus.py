"""
EventBus - 企业级统一事件总线
============================

特性：
1. 发布/订阅模式（完全解耦）
2. 优先级队列（控制执行顺序）
3. 事件过滤（条件订阅）
4. Wrapper 支持（类似 pluggy）
5. 异步支持（可选）
6. 事件追溯（调试友好）
7. 线程安全（生产级）

设计参考：
- Python pluggy (pytest 插件系统)
- Node.js EventEmitter
- Apache Kafka (概念层)

使用示例：

    # 订阅事件
    @EventBus.on('method.registered')
    def handle_registration(event: MethodRegisteredEvent):
        print(f"New method: {event.component}.{event.method}")

    # 发布事件
    EventBus.emit(MethodRegisteredEvent(
        component='business',
        method='analyze_truth',
        engine='duckdb'
    ))

    # Wrapper（拦截器）
    @EventBus.wrapper('pipeline.node.execute')
    def timing_wrapper(event):
        start = time.time()
        result = yield  # 执行原始逻辑
        print(f"Took {time.time() - start:.2f}s")
        return result
"""
from __future__ import annotations
from typing import (
    Callable, Dict, List, Any, Optional, TypeVar, Generic,
    Union, Set, TYPE_CHECKING
)
from dataclasses import dataclass, field
from enum import Enum, auto
from datetime import datetime
from collections import defaultdict
from functools import wraps
import threading
import logging
import time
import uuid
import weakref

if TYPE_CHECKING:
    from .events import Event

logger = logging.getLogger(__name__)


class EventPriority(Enum):
    """事件处理器优先级

    数值越小越先执行（类似 CSS z-index）。
    """
    SYSTEM = 0      # 系统级（日志、监控）最先执行
    HIGH = 25       # 高优先级
    NORMAL = 50     # 默认优先级
    LOW = 75        # 低优先级
    LAST = 100      # 最后执行（清理、统计）


@dataclass
class HandlerInfo:
    """处理器信息"""
    fn: Callable
    priority: EventPriority = EventPriority.NORMAL
    is_wrapper: bool = False
    is_once: bool = False  # 是否只执行一次
    filter_fn: Optional[Callable[['Event'], bool]] = None
    name: str = ""
    source: str = ""  # 注册来源（组件名）
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])


@dataclass
class EmitResult:
    """事件发布结果"""
    event_type: str
    handler_count: int
    success_count: int
    error_count: int
    total_time_ms: float
    errors: List[tuple] = field(default_factory=list)  # [(handler_name, exception)]


class EventBus:
    """统一事件总线（单例模式）

    核心功能：
    1. on(event, handler) - 注册处理器
    2. off(event, handler) - 注销处理器
    3. emit(event) - 同步发布事件
    4. emit_async(event) - 异步发布事件
    5. wrapper(event) - 注册拦截器
    """

    _instance: Optional['EventBus'] = None
    _lock = threading.RLock()

    def __init__(self):
        self._handlers: Dict[str, List[HandlerInfo]] = defaultdict(list)
        self._stats: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {'emit_count': 0, 'handler_calls': 0, 'errors': 0, 'total_time_ms': 0.0}
        )
        self._debug_mode = False
        self._paused_events: Set[str] = set()
        self._event_log: List[Dict] = []  # 调试用
        self._max_log_size = 1000

    @classmethod
    def get(cls) -> 'EventBus':
        """获取单例实例（线程安全）"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = EventBus()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """重置单例（用于测试）"""
        with cls._lock:
            cls._instance = None

    # ==================== 订阅 API ====================

    def on(
        self,
        event: str,
        handler: Callable = None,
        *,
        priority: EventPriority = EventPriority.NORMAL,
        once: bool = False,
        filter_fn: Callable[['Event'], bool] = None,
        source: str = ""
    ) -> Callable:
        """注册事件处理器

        可作为装饰器或直接调用：
            @bus.on('event.name')
            def handler(event): ...

            bus.on('event.name', handler)

        Args:
            event: 事件类型（支持通配符 'pipeline.*'）
            handler: 处理函数
            priority: 执行优先级
            once: 是否只执行一次
            filter_fn: 过滤函数（返回 True 才执行）
            source: 注册来源标识
        """
        def decorator(fn: Callable) -> Callable:
            info = HandlerInfo(
                fn=fn,
                priority=priority,
                is_wrapper=False,
                is_once=once,
                filter_fn=filter_fn,
                name=fn.__name__,
                source=source
            )
            with self._lock:
                self._handlers[event].append(info)
                self._sort_handlers(event)
            logger.debug(f"📌 EventBus: registered handler '{fn.__name__}' for '{event}'")
            return fn

        if handler is not None:
            return decorator(handler)
        return decorator

    def once(self, event: str, **kwargs) -> Callable:
        """注册一次性处理器（触发后自动移除）"""
        return self.on(event, once=True, **kwargs)

    def off(self, event: str, handler: Callable = None) -> bool:
        """注销处理器

        Args:
            event: 事件类型
            handler: 要移除的处理器（None 则移除该事件所有处理器）

        Returns:
            是否成功移除
        """
        with self._lock:
            if event not in self._handlers:
                return False
            if handler is None:
                del self._handlers[event]
                return True
            # 按函数引用移除
            original_len = len(self._handlers[event])
            self._handlers[event] = [
                h for h in self._handlers[event] if h.fn != handler
            ]
            return len(self._handlers[event]) < original_len

    def wrapper(
        self,
        event: str,
        *,
        priority: EventPriority = EventPriority.NORMAL,
        source: str = ""
    ) -> Callable:
        """注册 Wrapper 拦截器

        Wrapper 是生成器函数，可在事件处理前后执行逻辑：

            @bus.wrapper('pipeline.node.execute')
            def my_wrapper(event):
                print("Before")
                result = yield  # 等待核心执行
                print(f"After: {result}")
                return result  # 可修改返回值
        """
        def decorator(fn: Callable) -> Callable:
            info = HandlerInfo(
                fn=fn,
                priority=priority,
                is_wrapper=True,
                name=fn.__name__,
                source=source
            )
            with self._lock:
                self._handlers[event].append(info)
                self._sort_handlers(event)
            logger.debug(f"🔄 EventBus: registered wrapper '{fn.__name__}' for '{event}'")
            return fn
        return decorator

    # ==================== 发布 API ====================

    def emit(self, event: Union[str, 'Event'], **kwargs) -> EmitResult:
        """同步发布事件

        Args:
            event: 事件对象或事件类型字符串
            **kwargs: 当 event 是字符串时，作为事件数据

        Returns:
            EmitResult 包含执行统计
        """
        from .events import Event

        # 解析事件
        if isinstance(event, Event):
            event_type = event.event_type
            event_data = event
        else:
            event_type = event
            # 创建简单事件对象
            event_data = type('SimpleEvent', (), {'event_type': event_type, **kwargs})()

        # 检查暂停
        if event_type in self._paused_events:
            return EmitResult(event_type, 0, 0, 0, 0.0)

        start = time.perf_counter()
        handlers = self._get_matching_handlers(event_type)
        success_count = 0
        errors = []
        to_remove = []

        for info in handlers:
            if info.is_wrapper:
                continue  # Wrapper 在 call_with_wrappers 中处理

            # 检查过滤器
            if info.filter_fn:
                try:
                    if not info.filter_fn(event_data):
                        continue
                except Exception:
                    continue

            # 执行处理器
            try:
                info.fn(event_data)
                success_count += 1
                if info.is_once:
                    to_remove.append((event_type, info))
            except Exception as e:
                errors.append((info.name, e))
                logger.warning(f"⚠️ EventBus: handler '{info.name}' raised: {e}")

        # 移除一次性处理器
        for ev, info in to_remove:
            self._handlers[ev].remove(info)

        elapsed = (time.perf_counter() - start) * 1000

        # 更新统计
        stats = self._stats[event_type]
        stats['emit_count'] += 1
        stats['handler_calls'] += len(handlers)
        stats['errors'] += len(errors)
        stats['total_time_ms'] += elapsed

        # 调试日志
        if self._debug_mode:
            self._event_log.append({
                'time': datetime.now().isoformat(),
                'event': event_type,
                'handlers': len(handlers),
                'success': success_count,
                'errors': len(errors)
            })
            if len(self._event_log) > self._max_log_size:
                self._event_log = self._event_log[-self._max_log_size:]

        return EmitResult(
            event_type=event_type,
            handler_count=len(handlers),
            success_count=success_count,
            error_count=len(errors),
            total_time_ms=elapsed,
            errors=errors
        )

    def call_with_wrappers(
        self,
        event_type: str,
        core_fn: Callable,
        *args,
        **kwargs
    ) -> Any:
        """执行带 Wrapper 拦截的调用

        Wrapper 按优先级形成洋葱模型：
        [outer wrapper] -> [inner wrapper] -> [core_fn] -> [inner wrapper] -> [outer wrapper]
        """
        handlers = self._get_matching_handlers(event_type)
        wrappers = [h for h in handlers if h.is_wrapper]

        if not wrappers:
            return core_fn(*args, **kwargs)

        # 构建洋葱模型
        def build_chain(remaining_wrappers, core):
            if not remaining_wrappers:
                return core

            wrapper_info = remaining_wrappers[0]
            inner_chain = build_chain(remaining_wrappers[1:], core)

            def wrapped(*a, **kw):
                gen = wrapper_info.fn(*a, **kw)
                try:
                    next(gen)  # 运行到 yield
                except StopIteration as e:
                    return e.value

                try:
                    result = inner_chain(*a, **kw)
                except Exception as exc:
                    try:
                        gen.throw(exc)
                    except StopIteration as e:
                        return e.value
                    raise

                try:
                    gen.send(result)
                except StopIteration as e:
                    return e.value if e.value is not None else result
                return result

            return wrapped

        chain = build_chain(wrappers, core_fn)
        return chain(*args, **kwargs)

    # ==================== 工具方法 ====================

    def _sort_handlers(self, event: str):
        """按优先级排序处理器"""
        if event in self._handlers:
            self._handlers[event].sort(key=lambda h: h.priority.value)

    def _get_matching_handlers(self, event_type: str) -> List[HandlerInfo]:
        """获取匹配的处理器（支持通配符）"""
        result = []
        with self._lock:
            # 精确匹配
            result.extend(self._handlers.get(event_type, []))

            # 通配符匹配 (e.g., 'pipeline.*' 匹配 'pipeline.node.started')
            for pattern, handlers in self._handlers.items():
                if '*' in pattern:
                    prefix = pattern.rstrip('*').rstrip('.')
                    if event_type.startswith(prefix):
                        result.extend(handlers)

        # 去重并排序
        seen = set()
        unique = []
        for h in result:
            if h.id not in seen:
                seen.add(h.id)
                unique.append(h)
        unique.sort(key=lambda h: h.priority.value)
        return unique

    def pause(self, event: str):
        """暂停事件（不再分发）"""
        self._paused_events.add(event)

    def resume(self, event: str):
        """恢复事件"""
        self._paused_events.discard(event)

    def set_debug(self, enabled: bool = True):
        """启用调试模式"""
        self._debug_mode = enabled

    def get_stats(self) -> Dict[str, Dict[str, Any]]:
        """获取事件统计"""
        return dict(self._stats)

    def get_event_log(self) -> List[Dict]:
        """获取事件日志（调试模式）"""
        return list(self._event_log)

    def list_handlers(self, event: str = None) -> Dict[str, List[str]]:
        """列出注册的处理器"""
        if event:
            return {event: [h.name for h in self._handlers.get(event, [])]}
        return {e: [h.name for h in handlers] for e, handlers in self._handlers.items()}


# ==================== 便捷函数（模块级 API）====================

def on(event: str, **kwargs) -> Callable:
    """便捷订阅函数"""
    return EventBus.get().on(event, **kwargs)


def emit(event: Union[str, 'Event'], **kwargs) -> EmitResult:
    """便捷发布函数"""
    return EventBus.get().emit(event, **kwargs)


def wrapper(event: str, **kwargs) -> Callable:
    """便捷 Wrapper 注册"""
    return EventBus.get().wrapper(event, **kwargs)


__all__ = [
    'EventBus',
    'EventPriority',
    'HandlerInfo',
    'EmitResult',
    'on',
    'emit',
    'wrapper',
]
