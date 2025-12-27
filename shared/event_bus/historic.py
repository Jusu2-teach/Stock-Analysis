"""
历史事件存储模块
================

参考 pytest/pluggy 的 call_historic 设计，提供：
1. 历史事件存储
2. 新订阅者自动重放
3. 按类型管理
4. 容量控制

使用示例：

    bus = EventBusV6.get()
    
    # 标记事件类型为历史模式
    bus.mark_historic("registry.method.registered")
    
    # 发布历史事件
    bus.emit(MethodRegisteredEvent(...))
    
    # 后续订阅者会自动收到历史事件
    @bus.on("registry.method.registered")
    def late_subscriber(event):
        # 会收到之前发布的所有历史事件
        pass
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Callable, Optional, List, Any, Dict, Set, TYPE_CHECKING
from datetime import datetime
from collections import deque, defaultdict
import logging
import threading
import time

if TYPE_CHECKING:
    from ..events import Event

logger = logging.getLogger(__name__)


@dataclass
class HistoricEntry:
    """历史事件条目"""
    event: Any
    event_type: str
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    replay_count: int = 0  # 被重放的次数
    
    def mark_replayed(self):
        """标记被重放"""
        self.replay_count += 1


class HistoricEventStore:
    """历史事件存储
    
    支持新订阅者接收历史事件，类似 Pluggy 的 call_historic 功能。
    
    Features:
    1. 按事件类型存储历史事件
    2. 新订阅者自动重放
    3. 可配置的存储容量
    4. 过期清理机制
    """
    
    def __init__(
        self,
        max_events_per_type: int = 100,
        default_ttl_seconds: int = 3600 * 24  # 默认24小时
    ):
        """初始化历史存储
        
        Args:
            max_events_per_type: 每种事件类型的最大存储数量
            default_ttl_seconds: 默认存活时间
        """
        self._max_per_type = max_events_per_type
        self._default_ttl = default_ttl_seconds
        self._history: Dict[str, deque[HistoricEntry]] = defaultdict(
            lambda: deque(maxlen=max_events_per_type)
        )
        self._historic_types: Set[str] = set()
        self._type_ttl: Dict[str, int] = {}  # 按类型的 TTL
        self._lock = threading.RLock()
        
        # 统计
        self._total_stored = 0
        self._total_replays = 0
    
    def mark_historic(self, event_type: str, ttl_seconds: Optional[int] = None):
        """标记事件类型为历史模式
        
        Args:
            event_type: 事件类型
            ttl_seconds: 可选的自定义 TTL
        """
        self._historic_types.add(event_type)
        if ttl_seconds is not None:
            self._type_ttl[event_type] = ttl_seconds
        logger.debug(f"📜 Event type marked as historic: {event_type}")
    
    def unmark_historic(self, event_type: str):
        """取消历史模式标记"""
        self._historic_types.discard(event_type)
        self._type_ttl.pop(event_type, None)
    
    def is_historic(self, event_type: str) -> bool:
        """检查事件类型是否为历史模式"""
        return event_type in self._historic_types
    
    def store(self, event: Any) -> bool:
        """存储历史事件
        
        Args:
            event: 事件对象
            
        Returns:
            是否成功存储（仅历史类型事件会存储）
        """
        if not hasattr(event, 'event_type'):
            return False
        
        event_type = event.event_type
        
        if not self.is_historic(event_type):
            return False
        
        entry = HistoricEntry(
            event=event,
            event_type=event_type
        )
        
        with self._lock:
            self._history[event_type].append(entry)
            self._total_stored += 1
        
        logger.debug(
            f"📜 Historic event stored: {event_type} "
            f"(total: {len(self._history[event_type])})"
        )
        return True
    
    def replay(
        self,
        event_type: str,
        handler: Callable,
        filter_fn: Optional[Callable[[Any], bool]] = None
    ) -> int:
        """为新订阅者重放历史事件
        
        Args:
            event_type: 事件类型
            handler: 处理器函数
            filter_fn: 可选的过滤函数
            
        Returns:
            重放的事件数量
        """
        if event_type not in self._history:
            return 0
        
        replayed = 0
        
        with self._lock:
            entries = list(self._history[event_type])
        
        for entry in entries:
            try:
                # 应用过滤器
                if filter_fn and not filter_fn(entry.event):
                    continue
                
                handler(entry.event)
                entry.mark_replayed()
                replayed += 1
                self._total_replays += 1
                
            except Exception as e:
                logger.warning(
                    f"⚠️ Historic replay failed for {event_type}: {e}"
                )
        
        if replayed:
            logger.debug(
                f"📜 Replayed {replayed} historic events for {event_type}"
            )
        
        return replayed
    
    def replay_pattern(
        self,
        pattern: str,
        handler: Callable,
        filter_fn: Optional[Callable[[Any], bool]] = None
    ) -> int:
        """使用通配符模式重放历史事件
        
        Args:
            pattern: 事件类型模式（如 'pipeline.*'）
            handler: 处理器函数
            filter_fn: 可选的过滤函数
            
        Returns:
            重放的事件数量
        """
        if '*' not in pattern:
            return self.replay(pattern, handler, filter_fn)
        
        prefix = pattern.rstrip('*').rstrip('.')
        total_replayed = 0
        
        with self._lock:
            matching_types = [
                t for t in self._history.keys()
                if t.startswith(prefix)
            ]
        
        for event_type in matching_types:
            total_replayed += self.replay(event_type, handler, filter_fn)
        
        return total_replayed
    
    def get_history(
        self,
        event_type: str,
        limit: Optional[int] = None
    ) -> List[Any]:
        """获取历史事件列表
        
        Args:
            event_type: 事件类型
            limit: 可选的返回数量限制
            
        Returns:
            事件列表
        """
        with self._lock:
            if event_type not in self._history:
                return []
            
            entries = list(self._history[event_type])
            if limit:
                entries = entries[-limit:]
            
            return [e.event for e in entries]
    
    def get_latest(self, event_type: str) -> Optional[Any]:
        """获取最新的历史事件"""
        history = self.get_history(event_type, limit=1)
        return history[0] if history else None
    
    def clear(self, event_type: Optional[str] = None) -> int:
        """清空历史事件
        
        Args:
            event_type: 可选的事件类型，None 表示清空所有
            
        Returns:
            清除的数量
        """
        with self._lock:
            if event_type:
                count = len(self._history.get(event_type, []))
                self._history.pop(event_type, None)
            else:
                count = sum(len(q) for q in self._history.values())
                self._history.clear()
        
        logger.info(f"🧹 Cleared {count} historic events")
        return count
    
    def cleanup_expired(self) -> int:
        """清理过期的历史事件
        
        Returns:
            清理的数量
        """
        now = datetime.now()
        total_cleaned = 0
        
        with self._lock:
            for event_type, entries in list(self._history.items()):
                ttl = self._type_ttl.get(event_type, self._default_ttl)
                cleaned = 0
                
                while entries:
                    try:
                        entry = entries[0]
                        created = datetime.fromisoformat(entry.timestamp)
                        age = (now - created).total_seconds()
                        
                        if age > ttl:
                            entries.popleft()
                            cleaned += 1
                        else:
                            break
                    except Exception:
                        break
                
                total_cleaned += cleaned
        
        if total_cleaned:
            logger.info(f"🧹 Cleaned up {total_cleaned} expired historic events")
        
        return total_cleaned
    
    def get_stats(self) -> dict:
        """获取统计信息"""
        with self._lock:
            type_counts = {
                t: len(q) for t, q in self._history.items()
            }
        
        return {
            'historic_types': list(self._historic_types),
            'type_counts': type_counts,
            'total_stored': self._total_stored,
            'total_replays': self._total_replays,
            'total_events': sum(type_counts.values()),
        }
    
    def list_historic_types(self) -> List[str]:
        """列出所有历史事件类型"""
        return list(self._historic_types)
