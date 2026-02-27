"""
异步支持模块
============

参考 Project Reactor 和 asyncio 设计，提供：
1. 异步事件发布
2. 异步处理器支持
3. 并发控制
4. 背压处理

使用示例：

    bus = AsyncEventBus.get()
    
    # 异步订阅
    @bus.on_async("user.created")
    async def async_handler(event):
        await some_async_operation(event)
    
    # 异步发布（等待所有处理器完成）
    await bus.emit_async(event)
    
    # 并行发布（不等待）
    bus.emit_concurrent(event)
"""
from __future__ import annotations
import asyncio
from dataclasses import dataclass, field
from typing import (
    Callable, Optional, List, Any, Dict, Coroutine, TypeVar,
    Union
)
from concurrent.futures import ThreadPoolExecutor
import logging
import threading
import time
import functools

logger = logging.getLogger(__name__)

E = TypeVar('E')
AsyncHandler = Callable[[Any], Coroutine[Any, Any, Any]]
SyncHandler = Callable[[Any], Any]
Handler = Union[AsyncHandler, SyncHandler]


@dataclass
class AsyncEmitResult:
    """异步发布结果"""
    event_type: str
    handler_count: int
    success_count: int = 0
    error_count: int = 0
    duration_ms: float = 0.0
    errors: List[str] = field(default_factory=list)
    results: List[Any] = field(default_factory=list)
    
    @property
    def all_success(self) -> bool:
        return self.error_count == 0 and self.success_count > 0


class AsyncEventBus:
    """异步事件总线
    
    支持异步处理器和并发事件处理。
    """
    
    _instance: Optional['AsyncEventBus'] = None
    _lock = threading.Lock()
    
    def __init__(
        self,
        max_concurrent: int = 100,
        thread_pool_size: int = 4,
        emit_timeout: float = 30.0
    ):
        """初始化异步事件总线
        
        Args:
            max_concurrent: 最大并发数
            thread_pool_size: 线程池大小（用于同步处理器）
            emit_timeout: 发布超时（秒）
        """
        self._async_handlers: Dict[str, List[AsyncHandler]] = {}
        self._sync_handlers: Dict[str, List[SyncHandler]] = {}
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._max_concurrent = max_concurrent
        self._emit_timeout = emit_timeout
        self._executor = ThreadPoolExecutor(max_workers=thread_pool_size)
        self._lock = threading.RLock()
        
        # 统计
        self._total_emits = 0
        self._total_errors = 0
    
    @classmethod
    def get(cls) -> 'AsyncEventBus':
        """获取单例实例"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance
    
    @classmethod
    def reset(cls):
        """重置单例（用于测试）"""
        with cls._lock:
            if cls._instance:
                cls._instance._executor.shutdown(wait=False)
            cls._instance = None
    
    def _get_semaphore(self) -> asyncio.Semaphore:
        """获取或创建信号量"""
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self._max_concurrent)
        return self._semaphore
    
    def on_async(
        self,
        event_type: str,
        handler: Optional[AsyncHandler] = None
    ) -> Union[Callable, 'AsyncEventBus']:
        """注册异步处理器
        
        可以作为装饰器使用：
            @bus.on_async("event.type")
            async def handler(event): ...
        
        或直接调用：
            bus.on_async("event.type", handler)
        """
        def decorator(fn: AsyncHandler) -> AsyncHandler:
            with self._lock:
                if event_type not in self._async_handlers:
                    self._async_handlers[event_type] = []
                self._async_handlers[event_type].append(fn)
            
            logger.debug(f"📌 Async handler registered: {event_type} -> {fn.__name__}")
            return fn
        
        if handler is not None:
            decorator(handler)
            return self
        
        return decorator
    
    def on_sync(
        self,
        event_type: str,
        handler: Optional[SyncHandler] = None
    ) -> Union[Callable, 'AsyncEventBus']:
        """注册同步处理器（会在线程池中执行）"""
        def decorator(fn: SyncHandler) -> SyncHandler:
            with self._lock:
                if event_type not in self._sync_handlers:
                    self._sync_handlers[event_type] = []
                self._sync_handlers[event_type].append(fn)
            
            logger.debug(f"📌 Sync handler registered: {event_type} -> {fn.__name__}")
            return fn
        
        if handler is not None:
            decorator(handler)
            return self
        
        return decorator
    
    def off_async(self, event_type: str, handler: AsyncHandler) -> bool:
        """注销异步处理器"""
        with self._lock:
            if event_type in self._async_handlers:
                try:
                    self._async_handlers[event_type].remove(handler)
                    return True
                except ValueError:
                    pass
        return False
    
    def off_sync(self, event_type: str, handler: SyncHandler) -> bool:
        """注销同步处理器"""
        with self._lock:
            if event_type in self._sync_handlers:
                try:
                    self._sync_handlers[event_type].remove(handler)
                    return True
                except ValueError:
                    pass
        return False
    
    async def emit(self, event: Any) -> AsyncEmitResult:
        """异步发布事件
        
        等待所有处理器完成。
        
        Args:
            event: 事件对象
            
        Returns:
            发布结果
        """
        event_type = getattr(event, 'event_type', type(event).__name__)
        start = time.time()
        
        result = AsyncEmitResult(
            event_type=event_type,
            handler_count=0
        )
        
        # 收集处理器
        tasks: List[Coroutine] = []
        
        with self._lock:
            async_handlers = list(self._async_handlers.get(event_type, []))
            sync_handlers = list(self._sync_handlers.get(event_type, []))
            
            # 通配符匹配
            for pattern, handlers in self._async_handlers.items():
                if self._match_wildcard(pattern, event_type):
                    async_handlers.extend(handlers)
            
            for pattern, handlers in self._sync_handlers.items():
                if self._match_wildcard(pattern, event_type):
                    sync_handlers.extend(handlers)
        
        result.handler_count = len(async_handlers) + len(sync_handlers)
        
        if result.handler_count == 0:
            logger.debug(f"📭 No handlers for async event: {event_type}")
            return result
        
        # 创建任务
        semaphore = self._get_semaphore()
        
        for handler in async_handlers:
            tasks.append(self._run_async_handler(handler, event, semaphore))
        
        for handler in sync_handlers:
            tasks.append(self._run_sync_handler(handler, event, semaphore))
        
        # 执行所有任务
        try:
            done, pending = await asyncio.wait(
                [asyncio.create_task(t) for t in tasks],
                timeout=self._emit_timeout
            )
            
            # 处理结果
            for task in done:
                try:
                    res = task.result()
                    result.success_count += 1
                    result.results.append(res)
                except Exception as e:
                    result.error_count += 1
                    result.errors.append(str(e))
            
            # 取消超时任务
            for task in pending:
                task.cancel()
                result.error_count += 1
                result.errors.append("Timeout")
                
        except Exception as e:
            result.error_count += 1
            result.errors.append(f"Emit failed: {e}")
        
        result.duration_ms = (time.time() - start) * 1000
        
        self._total_emits += 1
        if result.error_count > 0:
            self._total_errors += 1
        
        logger.debug(
            f"📤 Async emit completed: {event_type} "
            f"({result.success_count}/{result.handler_count} success, "
            f"{result.duration_ms:.2f}ms)"
        )
        
        return result
    
    async def _run_async_handler(
        self,
        handler: AsyncHandler,
        event: Any,
        semaphore: asyncio.Semaphore
    ) -> Any:
        """执行异步处理器"""
        async with semaphore:
            return await handler(event)
    
    async def _run_sync_handler(
        self,
        handler: SyncHandler,
        event: Any,
        semaphore: asyncio.Semaphore
    ) -> Any:
        """在线程池中执行同步处理器"""
        async with semaphore:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                self._executor,
                handler,
                event
            )
    
    def emit_nowait(self, event: Any):
        """发布事件但不等待（fire and forget）
        
        Args:
            event: 事件对象
        """
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(self.emit(event))
            else:
                loop.run_until_complete(self.emit(event))
        except RuntimeError:
            # 没有事件循环，创建新的
            asyncio.run(self.emit(event))
    
    def _match_wildcard(self, pattern: str, event_type: str) -> bool:
        """通配符匹配"""
        if '*' not in pattern:
            return False
        
        prefix = pattern.rstrip('*').rstrip('.')
        return event_type.startswith(prefix) and pattern != event_type
    
    def clear(self, event_type: Optional[str] = None):
        """清空处理器"""
        with self._lock:
            if event_type:
                self._async_handlers.pop(event_type, None)
                self._sync_handlers.pop(event_type, None)
            else:
                self._async_handlers.clear()
                self._sync_handlers.clear()
    
    def get_stats(self) -> dict:
        """获取统计"""
        with self._lock:
            async_count = sum(len(h) for h in self._async_handlers.values())
            sync_count = sum(len(h) for h in self._sync_handlers.values())
        
        return {
            'async_handlers': async_count,
            'sync_handlers': sync_count,
            'total_handlers': async_count + sync_count,
            'total_emits': self._total_emits,
            'total_errors': self._total_errors,
            'max_concurrent': self._max_concurrent,
        }


# ============================================================================
# 工具函数
# ============================================================================

def to_async(fn: SyncHandler) -> AsyncHandler:
    """将同步函数转换为异步函数"""
    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))
    return wrapper


def run_sync(coro: Coroutine) -> Any:
    """在同步上下文中运行协程"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # 已有运行中的循环，创建任务
            import concurrent.futures
            future = concurrent.futures.Future()
            
            async def run_and_set():
                try:
                    result = await coro
                    future.set_result(result)
                except Exception as e:
                    future.set_exception(e)
            
            asyncio.create_task(run_and_set())
            return future.result(timeout=30)
        else:
            return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)
