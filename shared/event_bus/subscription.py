"""
订阅管理模块
============

参考 RxPY 的 Disposable 模式，提供：
1. 可取消的订阅
2. 资源自动释放
3. 组合订阅
4. 生命周期管理

使用示例：

    bus = EventBusV6.get()
    
    # 基本订阅（返回 Subscription）
    subscription = bus.on("user.created", handler)
    
    # 取消订阅
    subscription.dispose()
    
    # 组合订阅
    composite = CompositeDisposable()
    composite.add(bus.on("event.a", handler_a))
    composite.add(bus.on("event.b", handler_b))
    composite.dispose()  # 一次性取消所有
    
    # 使用上下文管理器
    with bus.subscribe("temp.event", handler):
        # 订阅有效
        pass
    # 自动取消订阅
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable, Optional, List, Any, Dict, TypeVar, Generic, Set
from datetime import datetime
from contextlib import contextmanager
import logging
import threading
import time
import weakref
import uuid

logger = logging.getLogger(__name__)


class Disposable(ABC):
    """可释放资源的抽象基类
    
    类似 RxPY 的 Disposable，提供统一的资源释放接口。
    """
    
    @abstractmethod
    def dispose(self) -> bool:
        """释放资源
        
        Returns:
            是否成功释放
        """
        pass
    
    @property
    @abstractmethod
    def is_disposed(self) -> bool:
        """是否已释放"""
        pass
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dispose()
        return False


class EmptyDisposable(Disposable):
    """空的 Disposable（什么都不做）"""
    
    _is_disposed: bool = False
    
    def dispose(self) -> bool:
        self._is_disposed = True
        return True
    
    @property
    def is_disposed(self) -> bool:
        return self._is_disposed


@dataclass
class Subscription(Disposable):
    """订阅对象
    
    代表一个事件订阅，可以用于取消订阅。
    
    Attributes:
        id: 订阅 ID
        event_type: 事件类型
        handler: 处理器函数
        created_at: 创建时间
        priority: 优先级
        metadata: 元数据
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:12])
    event_type: str = ""
    handler: Optional[Callable] = None
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    priority: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # 内部状态
    _disposed: bool = field(default=False, repr=False)
    _unsubscribe_fn: Optional[Callable[[], bool]] = field(default=None, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    
    # 统计
    _call_count: int = field(default=0, repr=False)
    _last_called: Optional[str] = field(default=None, repr=False)
    
    def dispose(self) -> bool:
        """取消订阅"""
        with self._lock:
            if self._disposed:
                return False
            
            self._disposed = True
            
            if self._unsubscribe_fn:
                try:
                    result = self._unsubscribe_fn()
                    logger.debug(f"🔌 Subscription disposed: {self.event_type} [{self.id}]")
                    return result
                except Exception as e:
                    logger.warning(f"⚠️ Subscription dispose failed: {e}")
                    return False
            
            return True
    
    @property
    def is_disposed(self) -> bool:
        return self._disposed
    
    def mark_called(self):
        """标记被调用"""
        self._call_count += 1
        self._last_called = datetime.now().isoformat()
    
    @property
    def call_count(self) -> int:
        return self._call_count
    
    @property
    def last_called(self) -> Optional[str]:
        return self._last_called
    
    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            'id': self.id,
            'event_type': self.event_type,
            'handler': self.handler.__name__ if self.handler else None,
            'created_at': self.created_at,
            'priority': self.priority,
            'is_disposed': self._disposed,
            'call_count': self._call_count,
            'last_called': self._last_called,
        }


class CompositeDisposable(Disposable):
    """组合 Disposable
    
    管理多个 Disposable，可以一次性释放所有。
    """
    
    def __init__(self):
        self._disposables: List[Disposable] = []
        self._disposed = False
        self._lock = threading.Lock()
    
    def add(self, disposable: Disposable) -> 'CompositeDisposable':
        """添加 Disposable
        
        Args:
            disposable: 要添加的 Disposable
            
        Returns:
            self，支持链式调用
        """
        with self._lock:
            if self._disposed:
                # 如果已释放，直接释放新添加的
                disposable.dispose()
            else:
                self._disposables.append(disposable)
        return self
    
    def remove(self, disposable: Disposable) -> bool:
        """移除 Disposable（不释放）"""
        with self._lock:
            try:
                self._disposables.remove(disposable)
                return True
            except ValueError:
                return False
    
    def dispose(self) -> bool:
        """释放所有"""
        with self._lock:
            if self._disposed:
                return False
            
            self._disposed = True
            disposables = list(self._disposables)
            self._disposables.clear()
        
        # 在锁外释放，避免死锁
        success = True
        for d in disposables:
            try:
                d.dispose()
            except Exception as e:
                logger.warning(f"⚠️ CompositeDisposable: dispose failed: {e}")
                success = False
        
        logger.debug(f"🔌 CompositeDisposable disposed: {len(disposables)} items")
        return success
    
    @property
    def is_disposed(self) -> bool:
        return self._disposed
    
    @property
    def count(self) -> int:
        return len(self._disposables)
    
    def clear(self):
        """清空但不释放"""
        with self._lock:
            self._disposables.clear()


class SerialDisposable(Disposable):
    """串行 Disposable
    
    持有单个 Disposable，设置新值时自动释放旧值。
    """
    
    def __init__(self):
        self._current: Optional[Disposable] = None
        self._disposed = False
        self._lock = threading.Lock()
    
    @property
    def disposable(self) -> Optional[Disposable]:
        return self._current
    
    @disposable.setter
    def disposable(self, value: Optional[Disposable]):
        with self._lock:
            old = self._current
            self._current = value
            
            if self._disposed and value:
                value.dispose()
        
        # 在锁外释放旧值
        if old:
            old.dispose()
    
    def dispose(self) -> bool:
        with self._lock:
            if self._disposed:
                return False
            
            self._disposed = True
            current = self._current
            self._current = None
        
        if current:
            current.dispose()
        
        return True
    
    @property
    def is_disposed(self) -> bool:
        return self._disposed


class SingleAssignmentDisposable(Disposable):
    """单次赋值 Disposable
    
    只能设置一次内部 Disposable。
    """
    
    def __init__(self):
        self._disposable: Optional[Disposable] = None
        self._disposed = False
        self._lock = threading.Lock()
    
    @property
    def disposable(self) -> Optional[Disposable]:
        return self._disposable
    
    @disposable.setter
    def disposable(self, value: Disposable):
        with self._lock:
            if self._disposable is not None:
                raise RuntimeError("SingleAssignmentDisposable already assigned")
            
            self._disposable = value
            
            if self._disposed:
                value.dispose()
    
    def dispose(self) -> bool:
        with self._lock:
            if self._disposed:
                return False
            
            self._disposed = True
            d = self._disposable
        
        if d:
            d.dispose()
        
        return True
    
    @property
    def is_disposed(self) -> bool:
        return self._disposed


class RefCountDisposable(Disposable):
    """引用计数 Disposable
    
    只有当引用计数归零时才真正释放。
    """
    
    def __init__(self, disposable: Disposable):
        self._disposable = disposable
        self._ref_count = 0
        self._primary_disposed = False
        self._lock = threading.Lock()
    
    def acquire(self) -> Disposable:
        """获取一个引用
        
        Returns:
            一个 InnerDisposable，释放时减少引用计数
        """
        with self._lock:
            if self._primary_disposed:
                return EmptyDisposable()
            
            self._ref_count += 1
            return _InnerRefCountDisposable(self)
    
    def release(self):
        """释放一个引用"""
        with self._lock:
            self._ref_count -= 1
            should_dispose = (
                self._primary_disposed and 
                self._ref_count == 0
            )
        
        if should_dispose:
            self._disposable.dispose()
    
    def dispose(self) -> bool:
        with self._lock:
            if self._primary_disposed:
                return False
            
            self._primary_disposed = True
            should_dispose = self._ref_count == 0
        
        if should_dispose:
            self._disposable.dispose()
        
        return True
    
    @property
    def is_disposed(self) -> bool:
        return self._primary_disposed


class _InnerRefCountDisposable(Disposable):
    """RefCountDisposable 的内部引用"""
    
    def __init__(self, parent: RefCountDisposable):
        self._parent = parent
        self._disposed = False
    
    def dispose(self) -> bool:
        if self._disposed:
            return False
        
        self._disposed = True
        self._parent.release()
        return True
    
    @property
    def is_disposed(self) -> bool:
        return self._disposed


# ============================================================================
# 订阅管理器
# ============================================================================

class SubscriptionManager:
    """订阅管理器
    
    管理所有订阅的生命周期。
    """
    
    def __init__(self):
        self._subscriptions: Dict[str, Subscription] = {}  # id -> Subscription
        self._by_type: Dict[str, Set[str]] = {}  # event_type -> subscription_ids
        self._lock = threading.RLock()
        
        # 统计
        self._total_created = 0
        self._total_disposed = 0
    
    def create(
        self,
        event_type: str,
        handler: Callable,
        unsubscribe_fn: Callable[[], bool],
        priority: int = 0,
        metadata: Optional[dict] = None
    ) -> Subscription:
        """创建订阅
        
        Args:
            event_type: 事件类型
            handler: 处理器
            unsubscribe_fn: 取消订阅函数
            priority: 优先级
            metadata: 元数据
            
        Returns:
            Subscription 对象
        """
        subscription = Subscription(
            event_type=event_type,
            handler=handler,
            priority=priority,
            metadata=metadata or {}
        )
        subscription._unsubscribe_fn = unsubscribe_fn
        
        with self._lock:
            self._subscriptions[subscription.id] = subscription
            
            if event_type not in self._by_type:
                self._by_type[event_type] = set()
            self._by_type[event_type].add(subscription.id)
            
            self._total_created += 1
        
        logger.debug(
            f"📌 Subscription created: {event_type} [{subscription.id}] "
            f"priority={priority}"
        )
        
        return subscription
    
    def remove(self, subscription_id: str) -> bool:
        """移除订阅记录"""
        with self._lock:
            if subscription_id not in self._subscriptions:
                return False
            
            sub = self._subscriptions.pop(subscription_id)
            
            if sub.event_type in self._by_type:
                self._by_type[sub.event_type].discard(subscription_id)
            
            self._total_disposed += 1
        
        return True
    
    def get(self, subscription_id: str) -> Optional[Subscription]:
        """获取订阅"""
        return self._subscriptions.get(subscription_id)
    
    def get_by_type(self, event_type: str) -> List[Subscription]:
        """获取某类型的所有订阅"""
        with self._lock:
            ids = self._by_type.get(event_type, set())
            return [
                self._subscriptions[id] 
                for id in ids 
                if id in self._subscriptions
            ]
    
    def dispose_by_type(self, event_type: str) -> int:
        """取消某类型的所有订阅
        
        Returns:
            取消的数量
        """
        subscriptions = self.get_by_type(event_type)
        count = 0
        
        for sub in subscriptions:
            if sub.dispose():
                count += 1
        
        return count
    
    def dispose_all(self) -> int:
        """取消所有订阅
        
        Returns:
            取消的数量
        """
        with self._lock:
            subscriptions = list(self._subscriptions.values())
        
        count = 0
        for sub in subscriptions:
            if sub.dispose():
                count += 1
        
        logger.info(f"🔌 All subscriptions disposed: {count}")
        return count
    
    def cleanup_disposed(self) -> int:
        """清理已释放的订阅记录
        
        Returns:
            清理的数量
        """
        with self._lock:
            disposed_ids = [
                id for id, sub in self._subscriptions.items()
                if sub.is_disposed
            ]
            
            for id in disposed_ids:
                sub = self._subscriptions.pop(id, None)
                if sub and sub.event_type in self._by_type:
                    self._by_type[sub.event_type].discard(id)
        
        return len(disposed_ids)
    
    def get_stats(self) -> dict:
        """获取统计"""
        with self._lock:
            type_counts = {
                t: len(ids) for t, ids in self._by_type.items()
            }
        
        return {
            'total_active': len(self._subscriptions),
            'total_created': self._total_created,
            'total_disposed': self._total_disposed,
            'by_type': type_counts,
        }
    
    def list_subscriptions(self) -> List[dict]:
        """列出所有订阅"""
        with self._lock:
            return [sub.to_dict() for sub in self._subscriptions.values()]
