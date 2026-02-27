"""
死信处理模块
============

参考 Google Guava EventBus 的 DeadEvent 设计，提供：
1. 无订阅者事件的捕获
2. 死信队列管理
3. 重试机制
4. 监控告警

使用示例：

    bus = EventBusV6.get()
    
    # 注册死信处理器
    @bus.on_dead_letter
    def handle_dead_letter(dead_event: DeadEvent):
        logger.warning(f"Dead letter: {dead_event.original_event}")
    
    # 重试死信
    recovered = bus.retry_dead_letters()
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Callable, Optional, List, Any, TYPE_CHECKING
from datetime import datetime
from collections import deque
import logging
import threading
import time

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


@dataclass
class DeadEvent:
    """死信事件
    
    当事件没有订阅者时，会被包装为 DeadEvent。
    参考 Guava EventBus 的 DeadEvent 设计。
    
    Attributes:
        original_event: 原始事件对象
        original_type: 原始事件类型
        reason: 死信原因
        attempted_at: 首次尝试时间
        retry_count: 重试次数
        last_error: 最后一次错误
        metadata: 额外元数据
    """
    original_event: Any
    original_type: str = ""
    reason: str = "no_subscribers"
    attempted_at: str = field(default_factory=lambda: datetime.now().isoformat())
    retry_count: int = 0
    max_retries: int = 3
    last_error: Optional[str] = None
    metadata: dict = field(default_factory=dict)
    
    # 事件元数据
    event_id: str = field(default_factory=lambda: f"dead_{int(time.time() * 1000)}")
    source: str = "dead_letter_queue"
    
    def __post_init__(self):
        if not self.original_type and hasattr(self.original_event, 'event_type'):
            self.original_type = self.original_event.event_type
    
    @property
    def event_type(self) -> str:
        """死信事件类型"""
        return "system.dead_letter"
    
    @property
    def can_retry(self) -> bool:
        """是否可以重试"""
        return self.retry_count < self.max_retries
    
    def increment_retry(self, error: Optional[str] = None):
        """增加重试计数"""
        self.retry_count += 1
        if error:
            self.last_error = error
    
    def to_dict(self) -> dict:
        """转换为字典"""
        return {
            'event_id': self.event_id,
            'original_type': self.original_type,
            'reason': self.reason,
            'attempted_at': self.attempted_at,
            'retry_count': self.retry_count,
            'max_retries': self.max_retries,
            'last_error': self.last_error,
            'can_retry': self.can_retry,
        }


class DeadLetterQueue:
    """死信队列
    
    管理无订阅者的事件，提供重试和监控功能。
    
    Features:
    1. 自动捕获无订阅者事件
    2. 可配置的队列大小和 TTL
    3. 重试机制（指数退避）
    4. 死信处理器回调
    5. 统计监控
    """
    
    def __init__(
        self,
        max_size: int = 1000,
        ttl_seconds: int = 3600,
        max_retries: int = 3
    ):
        """初始化死信队列
        
        Args:
            max_size: 队列最大容量
            ttl_seconds: 事件存活时间（秒）
            max_retries: 最大重试次数
        """
        self._queue: deque[DeadEvent] = deque(maxlen=max_size)
        self._ttl = ttl_seconds
        self._max_retries = max_retries
        self._handlers: List[Callable[[DeadEvent], None]] = []
        self._lock = threading.RLock()
        
        # 统计
        self._total_enqueued = 0
        self._total_retried = 0
        self._total_recovered = 0
        self._total_expired = 0
    
    def enqueue(
        self,
        event: Any,
        reason: str = "no_subscribers",
        metadata: Optional[dict] = None
    ) -> DeadEvent:
        """入队死信
        
        Args:
            event: 原始事件
            reason: 死信原因
            metadata: 额外元数据
            
        Returns:
            创建的 DeadEvent
        """
        dead_event = DeadEvent(
            original_event=event,
            reason=reason,
            max_retries=self._max_retries,
            metadata=metadata or {}
        )
        
        with self._lock:
            self._queue.append(dead_event)
            self._total_enqueued += 1
        
        logger.warning(
            f"💀 Dead letter enqueued: {dead_event.original_type} "
            f"(reason: {reason}, queue size: {len(self._queue)})"
        )
        
        # 通知处理器
        self._notify_handlers(dead_event)
        
        return dead_event
    
    def _notify_handlers(self, dead_event: DeadEvent):
        """通知所有死信处理器"""
        for handler in self._handlers:
            try:
                handler(dead_event)
            except Exception as e:
                logger.error(f"Dead letter handler failed: {e}")
    
    def on_dead_letter(self, handler: Callable[[DeadEvent], None]):
        """注册死信处理器
        
        Args:
            handler: 处理函数，接收 DeadEvent 参数
        """
        self._handlers.append(handler)
        logger.debug(f"📌 Dead letter handler registered: {handler.__name__}")
    
    def off_dead_letter(self, handler: Callable[[DeadEvent], None]) -> bool:
        """注销死信处理器"""
        try:
            self._handlers.remove(handler)
            return True
        except ValueError:
            return False
    
    def retry_one(self, emit_fn: Callable[[Any], Any]) -> Optional[DeadEvent]:
        """重试一个死信
        
        Args:
            emit_fn: 发布函数
            
        Returns:
            重试的 DeadEvent，如果队列为空返回 None
        """
        with self._lock:
            if not self._queue:
                return None
            
            dead_event = self._queue.popleft()
        
        if not dead_event.can_retry:
            logger.warning(
                f"💀 Dead letter expired (max retries): {dead_event.original_type}"
            )
            self._total_expired += 1
            return dead_event
        
        try:
            dead_event.increment_retry()
            self._total_retried += 1
            
            # 重新发布
            emit_fn(dead_event.original_event)
            
            self._total_recovered += 1
            logger.info(
                f"✅ Dead letter recovered: {dead_event.original_type} "
                f"(attempt {dead_event.retry_count})"
            )
            
        except Exception as e:
            dead_event.increment_retry(str(e))
            
            # 如果还能重试，放回队列
            if dead_event.can_retry:
                with self._lock:
                    self._queue.appendleft(dead_event)
            else:
                logger.error(
                    f"💀 Dead letter failed permanently: {dead_event.original_type} "
                    f"(error: {e})"
                )
                self._total_expired += 1
        
        return dead_event
    
    def retry_all(self, emit_fn: Callable[[Any], Any]) -> int:
        """重试所有死信
        
        Args:
            emit_fn: 发布函数
            
        Returns:
            成功恢复的数量
        """
        recovered = 0
        initial_size = len(self._queue)
        
        for _ in range(initial_size):
            dead = self.retry_one(emit_fn)
            if dead and dead.retry_count > 0 and not dead.can_retry:
                # 已恢复或已过期
                pass
            elif dead:
                recovered += 1
        
        logger.info(f"🔄 Dead letter retry completed: {recovered}/{initial_size} recovered")
        return recovered
    
    def peek(self, count: int = 10) -> List[DeadEvent]:
        """查看队列中的死信（不移除）
        
        Args:
            count: 返回数量
            
        Returns:
            死信列表
        """
        with self._lock:
            return list(self._queue)[:count]
    
    def clear(self) -> int:
        """清空队列
        
        Returns:
            清除的数量
        """
        with self._lock:
            count = len(self._queue)
            self._queue.clear()
        return count
    
    def cleanup_expired(self) -> int:
        """清理过期死信
        
        Returns:
            清理的数量
        """
        now = datetime.now()
        expired = 0
        
        with self._lock:
            while self._queue:
                dead = self._queue[0]
                try:
                    created = datetime.fromisoformat(dead.attempted_at)
                    age = (now - created).total_seconds()
                    
                    if age > self._ttl:
                        self._queue.popleft()
                        expired += 1
                        self._total_expired += 1
                    else:
                        break
                except Exception:
                    break
        
        if expired:
            logger.info(f"🧹 Cleaned up {expired} expired dead letters")
        
        return expired
    
    @property
    def size(self) -> int:
        """队列大小"""
        return len(self._queue)
    
    @property
    def is_empty(self) -> bool:
        """队列是否为空"""
        return len(self._queue) == 0
    
    def get_stats(self) -> dict:
        """获取统计信息"""
        return {
            'queue_size': self.size,
            'total_enqueued': self._total_enqueued,
            'total_retried': self._total_retried,
            'total_recovered': self._total_recovered,
            'total_expired': self._total_expired,
            'recovery_rate': (
                self._total_recovered / self._total_retried
                if self._total_retried > 0 else 0.0
            ),
            'handlers_count': len(self._handlers),
        }
