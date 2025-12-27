"""
EventBus V6 主模块
==================

整合所有增强功能的统一事件总线。

Features:
1. HookSpec 类型安全（参考 Pluggy）
2. DeadLetterQueue 死信处理（参考 Guava）
3. HistoricEventStore 历史重放（参考 Pluggy call_historic）
4. MiddlewarePipeline 中间件管道（参考 Express/Koa）
5. Subscription 订阅管理（参考 RxPY Disposable）
6. AsyncEventBus 异步支持（参考 Reactor Core）

使用示例：

    # 获取单例
    bus = EventBusV6.get()
    
    # 基本订阅
    @bus.on("user.created")
    def on_user_created(event):
        print(f"User created: {event.user_id}")
    
    # 带取消功能的订阅
    subscription = bus.subscribe("order.placed", handler)
    subscription.dispose()  # 取消订阅
    
    # 中间件
    bus.use(LoggingMiddleware())
    bus.use(RetryMiddleware(max_retries=3))
    
    # 发布事件
    bus.emit(UserCreatedEvent(user_id="123"))
"""
from __future__ import annotations
import asyncio
from dataclasses import dataclass, field
from typing import (
    Callable, Optional, List, Any, Dict, Set, TypeVar,
    Union, TYPE_CHECKING, overload
)
from datetime import datetime
from collections import defaultdict
import logging
import threading
import time
import fnmatch
import functools

# 本地导入
from .config import EventBusConfig
from .models import HandlerInfo, EmitResult, EventPriority, EventBusStats
from .specs import HookSpec, HookSpecRegistry
from .dead_letter import DeadEvent, DeadLetterQueue
from .historic import HistoricEventStore
from .middleware import (
    Middleware, MiddlewarePipeline, MiddlewareContext,
    LoggingMiddleware, TracingMiddleware, RetryMiddleware,
    TimeoutMiddleware, MetricsMiddleware, CircuitBreakerMiddleware
)
from .subscription import (
    Subscription, CompositeDisposable, SubscriptionManager,
    Disposable
)

logger = logging.getLogger(__name__)

E = TypeVar('E')
Handler = Callable[[Any], Any]


class EventBusV6:
    """EventBus V6 - 增强版事件总线
    
    整合多种设计模式的生产级事件总线实现。
    """
    
    _instance: Optional['EventBusV6'] = None
    _creation_lock = threading.Lock()
    
    def __init__(self, config: Optional[EventBusConfig] = None):
        """初始化事件总线
        
        Args:
            config: 可选的配置对象
        """
        self._config = config or EventBusConfig.from_env()
        
        # 核心组件
        self._handlers: Dict[str, List[HandlerInfo]] = defaultdict(list)
        self._lock = threading.RLock()
        
        # 增强组件
        self._dead_letter_queue = DeadLetterQueue(
            max_size=self._config.dead_letter_max_size,
            ttl_seconds=self._config.dead_letter_ttl_seconds,
            max_retries=self._config.dead_letter_max_retries
        ) if self._config.enable_dead_letter else None
        
        self._historic_store = HistoricEventStore(
            max_events_per_type=self._config.historic_max_events_per_type
        ) if self._config.enable_historic else None
        
        self._middleware_pipeline = MiddlewarePipeline() if self._config.enable_middleware else None
        
        self._subscription_manager = SubscriptionManager()
        
        # 统计
        self._total_emits = 0
        self._total_handlers_called = 0
        self._total_errors = 0
        self._start_time = time.time()
        
        logger.info(
            f"🚌 EventBusV6 initialized "
            f"(middleware={self._config.enable_middleware}, "
            f"dead_letter={self._config.enable_dead_letter}, "
            f"historic={self._config.enable_historic})"
        )
    
    # ========================================================================
    # 单例管理
    # ========================================================================
    
    @classmethod
    def get(cls, config: Optional[EventBusConfig] = None) -> 'EventBusV6':
        """获取单例实例
        
        Args:
            config: 首次创建时的配置
            
        Returns:
            EventBusV6 实例
        """
        if cls._instance is None:
            with cls._creation_lock:
                if cls._instance is None:
                    cls._instance = cls(config)
        return cls._instance
    
    @classmethod
    def reset(cls):
        """重置单例（用于测试）"""
        with cls._creation_lock:
            if cls._instance:
                cls._instance.clear()
            cls._instance = None
    
    @classmethod
    def configure(cls, config: EventBusConfig) -> 'EventBusV6':
        """配置并获取实例"""
        cls.reset()
        return cls.get(config)
    
    # ========================================================================
    # 订阅 API
    # ========================================================================
    
    def on(
        self,
        event_type: str,
        handler: Optional[Handler] = None,
        *,
        priority: int = 0,
        once: bool = False
    ) -> Union[Callable[[Handler], Handler], Subscription]:
        """注册事件处理器
        
        可以作为装饰器使用：
            @bus.on("user.created")
            def handler(event): ...
            
            @bus.on("user.created", priority=10)
            def high_priority_handler(event): ...
        
        或直接调用（返回 Subscription）：
            subscription = bus.on("user.created", handler)
            subscription.dispose()  # 取消订阅
        
        Args:
            event_type: 事件类型（支持通配符 * ）
            handler: 处理器函数
            priority: 优先级（越大越先执行）
            once: 是否只执行一次
            
        Returns:
            装饰器函数或 Subscription 对象
        """
        def decorator(fn: Handler) -> Handler:
            return self._register_handler(
                event_type, fn, priority=priority, once=once
            )
        
        if handler is not None:
            self._register_handler(
                event_type, handler, priority=priority, once=once
            )
            return self.subscribe(event_type, handler, priority=priority)
        
        return decorator
    
    def subscribe(
        self,
        event_type: str,
        handler: Handler,
        *,
        priority: int = 0,
        once: bool = False,
        replay_historic: bool = True
    ) -> Subscription:
        """订阅事件（返回可取消的订阅）
        
        Args:
            event_type: 事件类型
            handler: 处理器
            priority: 优先级
            once: 是否只执行一次
            replay_historic: 是否重放历史事件
            
        Returns:
            Subscription 对象
        """
        # 注册处理器
        self._register_handler(event_type, handler, priority=priority, once=once)
        
        # 创建取消函数
        def unsubscribe() -> bool:
            return self.off(event_type, handler)
        
        # 创建订阅
        subscription = self._subscription_manager.create(
            event_type=event_type,
            handler=handler,
            unsubscribe_fn=unsubscribe,
            priority=priority
        )
        
        # 历史事件重放
        if replay_historic and self._historic_store:
            if self._historic_store.is_historic(event_type):
                self._historic_store.replay(event_type, handler)
            elif '*' in event_type:
                self._historic_store.replay_pattern(event_type, handler)
        
        return subscription
    
    def _register_handler(
        self,
        event_type: str,
        handler: Handler,
        priority: int = 0,
        once: bool = False
    ) -> Handler:
        """内部注册处理器"""
        # HookSpec 验证
        if self._config.enable_validation:
            valid, errors = HookSpecRegistry.validate_handler(
                event_type, handler
            )
            if not valid:
                if self._config.validation_strict:
                    raise ValueError(
                        f"Handler validation failed for '{event_type}': {errors}"
                    )
                else:
                    logger.warning(
                        f"⚠️ Handler validation warning for '{event_type}': {errors}"
                    )
        
        # 包装 once 处理器
        actual_handler = handler
        if once:
            @functools.wraps(handler)
            def once_wrapper(event):
                result = handler(event)
                self.off(event_type, once_wrapper)
                return result
            actual_handler = once_wrapper
        
        # 创建 HandlerInfo
        info = HandlerInfo(
            handler=actual_handler,
            event_type=event_type,
            priority=priority,
            once=once
        )
        
        with self._lock:
            self._handlers[event_type].append(info)
            # 按优先级排序（优先级高的在前）
            self._handlers[event_type].sort(key=lambda h: -h.priority)
        
        logger.debug(
            f"📌 Handler registered: {event_type} -> {handler.__name__} "
            f"(priority={priority}, once={once})"
        )
        
        return handler
    
    def off(self, event_type: str, handler: Handler) -> bool:
        """注销处理器
        
        Args:
            event_type: 事件类型
            handler: 处理器函数
            
        Returns:
            是否成功注销
        """
        with self._lock:
            if event_type not in self._handlers:
                return False
            
            original_count = len(self._handlers[event_type])
            self._handlers[event_type] = [
                h for h in self._handlers[event_type]
                if h.handler != handler
            ]
            
            removed = original_count - len(self._handlers[event_type])
            
            if removed > 0:
                logger.debug(f"🔌 Handler unregistered: {event_type}")
            
            return removed > 0
    
    def once(
        self,
        event_type: str,
        handler: Optional[Handler] = None,
        *,
        priority: int = 0
    ) -> Union[Callable[[Handler], Handler], Subscription]:
        """注册一次性处理器"""
        return self.on(event_type, handler, priority=priority, once=True)
    
    # ========================================================================
    # 发布 API
    # ========================================================================
    
    def emit(self, event: Any, **kwargs) -> EmitResult:
        """发布事件
        
        Args:
            event: 事件对象
            **kwargs: 额外参数（会合并到事件属性）
            
        Returns:
            发布结果
        """
        event_type = getattr(event, 'event_type', type(event).__name__)
        start = time.time()
        
        result = EmitResult(event_type=event_type)
        
        # 存储历史事件
        if self._historic_store:
            self._historic_store.store(event)
        
        # 收集处理器
        handlers = self._get_handlers(event_type)
        result.handler_count = len(handlers)
        
        if not handlers:
            logger.debug(f"📭 No handlers for event: {event_type}")
            
            # 死信处理
            if self._dead_letter_queue:
                self._dead_letter_queue.enqueue(event, "no_subscribers")
            
            return result
        
        # 执行处理器
        if self._middleware_pipeline:
            # 通过中间件管道执行
            try:
                self._middleware_pipeline.execute(
                    event,
                    lambda e, ctx: self._execute_handlers(e, handlers, result)
                )
            except Exception as e:
                result.errors.append(str(e))
                self._total_errors += 1
        else:
            # 直接执行
            self._execute_handlers(event, handlers, result)
        
        result.duration_ms = (time.time() - start) * 1000
        result.success = result.error_count == 0
        
        self._total_emits += 1
        self._total_handlers_called += result.success_count
        
        logger.debug(
            f"📤 Event emitted: {event_type} "
            f"({result.success_count}/{result.handler_count} handlers, "
            f"{result.duration_ms:.2f}ms)"
        )
        
        return result
    
    def _get_handlers(self, event_type: str) -> List[HandlerInfo]:
        """获取匹配的处理器（包括通配符）"""
        handlers: List[HandlerInfo] = []
        
        with self._lock:
            # 精确匹配
            if event_type in self._handlers:
                handlers.extend(self._handlers[event_type])
            
            # 通配符匹配
            for pattern, pattern_handlers in self._handlers.items():
                if '*' in pattern and pattern != event_type:
                    if self._match_pattern(pattern, event_type):
                        handlers.extend(pattern_handlers)
        
        # 排序
        handlers.sort(key=lambda h: -h.priority)
        return handlers
    
    def _match_pattern(self, pattern: str, event_type: str) -> bool:
        """通配符模式匹配"""
        # 支持 'namespace.*' 和 'namespace.**' 格式
        if pattern.endswith('.**'):
            prefix = pattern[:-3]
            return event_type.startswith(prefix)
        elif pattern.endswith('.*'):
            prefix = pattern[:-2]
            parts = event_type.split('.')
            pattern_parts = prefix.split('.')
            if len(parts) != len(pattern_parts) + 1:
                return False
            return '.'.join(parts[:-1]) == prefix
        else:
            return fnmatch.fnmatch(event_type, pattern)
    
    def _execute_handlers(
        self,
        event: Any,
        handlers: List[HandlerInfo],
        result: EmitResult
    ) -> Any:
        """执行所有处理器"""
        for info in handlers:
            try:
                res = info.handler(event)
                info.mark_called()
                result.success_count += 1
                result.results.append(res)
                
            except Exception as e:
                result.error_count += 1
                result.errors.append(f"{info.handler.__name__}: {e}")
                self._total_errors += 1
                logger.warning(
                    f"⚠️ Handler error: {info.handler.__name__}: {e}"
                )
        
        return result.results
    
    def emit_sync(self, event: Any) -> EmitResult:
        """同步发布（等待所有处理器完成）
        
        与 emit() 相同，这里为了 API 兼容性。
        """
        return self.emit(event)
    
    # ========================================================================
    # 中间件 API
    # ========================================================================
    
    def use(self, middleware: Middleware) -> 'EventBusV6':
        """添加中间件
        
        Args:
            middleware: 中间件实例
            
        Returns:
            self，支持链式调用
        """
        if self._middleware_pipeline:
            self._middleware_pipeline.use(middleware)
        else:
            logger.warning("⚠️ Middleware is disabled in config")
        return self
    
    def middleware(
        self,
        fn: Optional[Callable] = None,
        *,
        name: Optional[str] = None,
        order: int = 100
    ) -> Callable:
        """装饰器方式添加中间件
        
        @bus.middleware
        def my_middleware(event, context, next_fn):
            # 前置处理
            result = next_fn(event, context)
            # 后置处理
            return result
        """
        def decorator(func: Callable) -> Callable:
            if self._middleware_pipeline:
                self._middleware_pipeline.use_fn(func, name, order)
            return func
        
        if fn is not None:
            return decorator(fn)
        return decorator
    
    # ========================================================================
    # 死信处理 API
    # ========================================================================
    
    def on_dead_letter(
        self,
        handler: Optional[Callable[[DeadEvent], None]] = None
    ) -> Union[Callable, 'EventBusV6']:
        """注册死信处理器
        
        @bus.on_dead_letter
        def handle_dead(dead_event: DeadEvent):
            logger.warning(f"Dead letter: {dead_event.original_type}")
        """
        def decorator(fn: Callable[[DeadEvent], None]) -> Callable:
            if self._dead_letter_queue:
                self._dead_letter_queue.on_dead_letter(fn)
            return fn
        
        if handler is not None:
            decorator(handler)
            return self
        return decorator
    
    def retry_dead_letters(self) -> int:
        """重试所有死信
        
        Returns:
            恢复的数量
        """
        if not self._dead_letter_queue:
            return 0
        return self._dead_letter_queue.retry_all(self.emit)
    
    @property
    def dead_letter_queue(self) -> Optional[DeadLetterQueue]:
        """获取死信队列"""
        return self._dead_letter_queue
    
    # ========================================================================
    # 历史事件 API
    # ========================================================================
    
    def mark_historic(self, event_type: str, ttl_seconds: Optional[int] = None):
        """标记事件类型为历史模式
        
        Args:
            event_type: 事件类型
            ttl_seconds: 可选的 TTL
        """
        if self._historic_store:
            self._historic_store.mark_historic(event_type, ttl_seconds)
        
        # 同步到 HookSpec
        if HookSpecRegistry.has_spec(event_type):
            spec = HookSpecRegistry.get(event_type)
            # HookSpec 是不可变的，这里只是记录
            logger.debug(f"📜 Event type marked as historic: {event_type}")
    
    def get_history(self, event_type: str, limit: Optional[int] = None) -> List[Any]:
        """获取历史事件"""
        if self._historic_store:
            return self._historic_store.get_history(event_type, limit)
        return []
    
    @property
    def historic_store(self) -> Optional[HistoricEventStore]:
        """获取历史存储"""
        return self._historic_store
    
    # ========================================================================
    # HookSpec API
    # ========================================================================
    
    def define_spec(
        self,
        name: str,
        *,
        firstresult: bool = False,
        historic: bool = False,
        required_args: tuple = (),
        optional_args: tuple = (),
        return_type: Optional[type] = None,
        description: str = ""
    ) -> HookSpec:
        """定义事件规格
        
        Args:
            name: 事件类型名称
            firstresult: 是否只取第一个非 None 结果
            historic: 是否为历史事件
            required_args: 必需参数
            optional_args: 可选参数
            return_type: 返回类型
            description: 描述
            
        Returns:
            HookSpec 对象
        """
        spec = HookSpecRegistry.define(
            name,
            firstresult=firstresult,
            historic=historic,
            required_args=required_args,
            optional_args=optional_args,
            return_type=return_type,
            description=description
        )
        
        # 如果是历史事件，同步到 historic_store
        if historic and self._historic_store:
            self._historic_store.mark_historic(name)
        
        return spec
    
    # ========================================================================
    # 查询 API
    # ========================================================================
    
    def has_handlers(self, event_type: str) -> bool:
        """检查是否有处理器"""
        return len(self._get_handlers(event_type)) > 0
    
    def handler_count(self, event_type: Optional[str] = None) -> int:
        """获取处理器数量"""
        if event_type:
            return len(self._get_handlers(event_type))
        
        with self._lock:
            return sum(len(h) for h in self._handlers.values())
    
    def list_event_types(self) -> List[str]:
        """列出所有事件类型"""
        with self._lock:
            return list(self._handlers.keys())
    
    def list_handlers(self, event_type: str) -> List[str]:
        """列出某类型的所有处理器名称"""
        handlers = self._get_handlers(event_type)
        return [h.handler.__name__ for h in handlers]
    
    # ========================================================================
    # 管理 API
    # ========================================================================
    
    def clear(self, event_type: Optional[str] = None):
        """清空处理器
        
        Args:
            event_type: 可选的事件类型，None 表示清空所有
        """
        with self._lock:
            if event_type:
                self._handlers.pop(event_type, None)
            else:
                self._handlers.clear()
        
        logger.info(
            f"🧹 Handlers cleared"
            + (f" for {event_type}" if event_type else "")
        )
    
    def cleanup(self):
        """清理过期资源"""
        if self._dead_letter_queue:
            self._dead_letter_queue.cleanup_expired()
        
        if self._historic_store:
            self._historic_store.cleanup_expired()
        
        self._subscription_manager.cleanup_disposed()
    
    def get_stats(self) -> EventBusStats:
        """获取统计信息"""
        with self._lock:
            handler_count = sum(len(h) for h in self._handlers.values())
            event_types = list(self._handlers.keys())
        
        return EventBusStats(
            total_handlers=handler_count,
            total_emits=self._total_emits,
            total_handlers_called=self._total_handlers_called,
            total_errors=self._total_errors,
            event_types=event_types,
            uptime_seconds=time.time() - self._start_time,
            dead_letter_stats=(
                self._dead_letter_queue.get_stats()
                if self._dead_letter_queue else None
            ),
            historic_stats=(
                self._historic_store.get_stats()
                if self._historic_store else None
            ),
            subscription_stats=self._subscription_manager.get_stats(),
            middleware_list=(
                self._middleware_pipeline.list_middlewares()
                if self._middleware_pipeline else []
            )
        )
    
    @property
    def config(self) -> EventBusConfig:
        """获取配置"""
        return self._config


# ============================================================================
# 便捷函数
# ============================================================================

def get_bus(config: Optional[EventBusConfig] = None) -> EventBusV6:
    """获取 EventBus 实例的便捷函数"""
    return EventBusV6.get(config)


def emit(event: Any, **kwargs) -> EmitResult:
    """发布事件的便捷函数"""
    return EventBusV6.get().emit(event, **kwargs)


def on(
    event_type: str,
    handler: Optional[Handler] = None,
    **kwargs
) -> Union[Callable, Subscription]:
    """订阅事件的便捷函数"""
    return EventBusV6.get().on(event_type, handler, **kwargs)


def off(event_type: str, handler: Handler) -> bool:
    """取消订阅的便捷函数"""
    return EventBusV6.get().off(event_type, handler)
