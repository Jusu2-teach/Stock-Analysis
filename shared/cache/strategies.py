"""
缓存淘汰策略 (Eviction Strategies)
===================================

参考设计:
- cachetools: LRU, TTL, LFU
- Caffeine (Java): Window TinyLFU

支持多种缓存淘汰策略。
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional

from .core import CacheEntry


class EvictionStrategy(ABC):
    """淘汰策略基类"""

    @abstractmethod
    def should_evict(self, entry: CacheEntry) -> bool:
        """判断是否应该淘汰"""

    @abstractmethod
    def select_victim(self, entries: Dict[str, CacheEntry]) -> Optional[str]:
        """选择淘汰目标"""

    def on_access(self, entry: CacheEntry) -> None:
        """访问时回调"""

    def on_insert(self, entry: CacheEntry) -> None:
        """插入时回调"""


class LRUStrategy(EvictionStrategy):
    """最近最少使用 (LRU) 策略

    淘汰最久未访问的条目。
    """

    def __init__(self, maxsize: int = 1000):
        self.maxsize = maxsize

    def should_evict(self, entry: CacheEntry) -> bool:
        return entry.is_expired

    def select_victim(self, entries: Dict[str, CacheEntry]) -> Optional[str]:
        if not entries:
            return None

        # 选择最久未访问的
        oldest_key = None
        oldest_time = float('inf')

        for key, entry in entries.items():
            if entry.last_accessed < oldest_time:
                oldest_time = entry.last_accessed
                oldest_key = key

        return oldest_key

    def on_access(self, entry: CacheEntry) -> None:
        entry.touch()


class TTLStrategy(EvictionStrategy):
    """生存时间 (TTL) 策略

    淘汰过期的条目。
    """

    def __init__(self, default_ttl: float = 3600):
        self.default_ttl = default_ttl

    def should_evict(self, entry: CacheEntry) -> bool:
        return entry.is_expired

    def select_victim(self, entries: Dict[str, CacheEntry]) -> Optional[str]:
        # 优先选择已过期的
        for key, entry in entries.items():
            if entry.is_expired:
                return key

        # 否则选择最快过期的
        if not entries:
            return None

        return min(
            entries.keys(),
            key=lambda k: entries[k].expires_at or float('inf')
        )


class LFUStrategy(EvictionStrategy):
    """最不经常使用 (LFU) 策略

    淘汰访问次数最少的条目。
    """

    def __init__(self, maxsize: int = 1000):
        self.maxsize = maxsize

    def should_evict(self, entry: CacheEntry) -> bool:
        return entry.is_expired

    def select_victim(self, entries: Dict[str, CacheEntry]) -> Optional[str]:
        if not entries:
            return None

        # 选择访问次数最少的
        return min(
            entries.keys(),
            key=lambda k: entries[k].access_count
        )

    def on_access(self, entry: CacheEntry) -> None:
        entry.access_count += 1


class SizeBasedStrategy(EvictionStrategy):
    """基于大小的策略

    当总大小超过限制时淘汰。
    """

    def __init__(self, max_size_bytes: int = 100 * 1024 * 1024):  # 100MB
        self.max_size_bytes = max_size_bytes

    def should_evict(self, entry: CacheEntry) -> bool:
        return entry.is_expired

    def select_victim(self, entries: Dict[str, CacheEntry]) -> Optional[str]:
        if not entries:
            return None

        # 计算总大小
        total_size = sum(e.size_bytes for e in entries.values())

        if total_size <= self.max_size_bytes:
            return None

        # 选择最大的条目
        return max(
            entries.keys(),
            key=lambda k: entries[k].size_bytes
        )


class CombinedStrategy(EvictionStrategy):
    """组合策略

    结合多种策略的判断。
    """

    def __init__(self, strategies: List[EvictionStrategy]):
        self.strategies = strategies

    def should_evict(self, entry: CacheEntry) -> bool:
        return any(s.should_evict(entry) for s in self.strategies)

    def select_victim(self, entries: Dict[str, CacheEntry]) -> Optional[str]:
        # 按策略顺序尝试选择
        for strategy in self.strategies:
            victim = strategy.select_victim(entries)
            if victim:
                return victim
        return None

    def on_access(self, entry: CacheEntry) -> None:
        for strategy in self.strategies:
            strategy.on_access(entry)


@dataclass
class EvictionConfig:
    """淘汰配置"""
    strategy: str = "lru"  # lru, ttl, lfu, size
    maxsize: int = 1000
    default_ttl: float = 3600
    max_size_bytes: int = 100 * 1024 * 1024

    def create_strategy(self) -> EvictionStrategy:
        """根据配置创建策略"""
        if self.strategy == "lru":
            return LRUStrategy(maxsize=self.maxsize)
        elif self.strategy == "ttl":
            return TTLStrategy(default_ttl=self.default_ttl)
        elif self.strategy == "lfu":
            return LFUStrategy(maxsize=self.maxsize)
        elif self.strategy == "size":
            return SizeBasedStrategy(max_size_bytes=self.max_size_bytes)
        else:
            return LRUStrategy(maxsize=self.maxsize)
