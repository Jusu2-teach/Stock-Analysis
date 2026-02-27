"""
缓存核心 (Cache Core)
======================

参考设计:
- cachetools: 策略模式
- functools.lru_cache: 简洁 API
- Django cache: 统一接口

提供统一的缓存接口和基础设施。
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Generic, Optional, TypeVar
import hashlib
import threading
import time

T = TypeVar('T')


@dataclass(frozen=True)
class CacheKey:
    """缓存键

    支持复杂键的生成和标准化。
    """
    namespace: str
    key: str
    version: str = "v1"

    def __str__(self) -> str:
        return f"{self.namespace}:{self.version}:{self.key}"

    @classmethod
    def from_args(
        cls,
        func_name: str,
        args: tuple,
        kwargs: dict,
        namespace: str = "",
    ) -> 'CacheKey':
        """从函数参数生成缓存键"""
        # 构建键内容
        key_parts = [func_name]

        for arg in args:
            key_parts.append(_hash_value(arg))

        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}={_hash_value(v)}")

        key_str = ":".join(key_parts)

        return cls(
            namespace=namespace or func_name,
            key=hashlib.md5(key_str.encode()).hexdigest()[:16],
        )


@dataclass
class CacheEntry(Generic[T]):
    """缓存条目"""
    key: CacheKey
    value: T
    created_at: float = field(default_factory=time.time)
    expires_at: Optional[float] = None
    access_count: int = 0
    last_accessed: float = field(default_factory=time.time)
    size_bytes: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_expired(self) -> bool:
        """检查是否过期"""
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at

    @property
    def ttl_remaining(self) -> Optional[float]:
        """剩余生存时间（秒）"""
        if self.expires_at is None:
            return None
        remaining = self.expires_at - time.time()
        return max(0, remaining)

    def touch(self) -> None:
        """更新访问信息"""
        self.access_count += 1
        self.last_accessed = time.time()


class CacheBackend(ABC):
    """缓存后端基类"""

    @abstractmethod
    def get(self, key: CacheKey) -> Optional[CacheEntry]:
        """获取缓存条目"""

    @abstractmethod
    def set(self, entry: CacheEntry) -> None:
        """设置缓存条目"""

    @abstractmethod
    def delete(self, key: CacheKey) -> bool:
        """删除缓存条目"""

    @abstractmethod
    def exists(self, key: CacheKey) -> bool:
        """检查键是否存在"""

    @abstractmethod
    def clear(self) -> None:
        """清空缓存"""

    @abstractmethod
    def size(self) -> int:
        """缓存条目数量"""

    def get_many(self, keys: list[CacheKey]) -> Dict[CacheKey, CacheEntry]:
        """批量获取"""
        result = {}
        for key in keys:
            entry = self.get(key)
            if entry:
                result[key] = entry
        return result

    def set_many(self, entries: list[CacheEntry]) -> None:
        """批量设置"""
        for entry in entries:
            self.set(entry)

    def delete_many(self, keys: list[CacheKey]) -> int:
        """批量删除"""
        deleted = 0
        for key in keys:
            if self.delete(key):
                deleted += 1
        return deleted


class Cache:
    """统一缓存接口

    提供高级缓存操作，支持多后端和策略。

    Example:
        cache = Cache()

        # 基础操作
        cache.set("key", "value", ttl=300)
        value = cache.get("key", default="fallback")

        # 批量操作
        cache.set_many({"a": 1, "b": 2})
        values = cache.get_many(["a", "b"])

        # 带命名空间
        user_cache = cache.namespace("user")
        user_cache.set("123", user_data)
    """

    _default: Optional['Cache'] = None
    _lock = threading.Lock()

    def __init__(
        self,
        backend: Optional[CacheBackend] = None,
        default_ttl: Optional[float] = None,
        namespace: str = "default",
        key_prefix: str = "",
    ):
        from .backends import MemoryBackend

        self._backend = backend or MemoryBackend()
        self._default_ttl = default_ttl
        self._namespace = namespace
        self._key_prefix = key_prefix
        self._stats = CacheStats()

    @classmethod
    def get_default(cls) -> 'Cache':
        """获取默认缓存实例"""
        if cls._default is None:
            with cls._lock:
                if cls._default is None:
                    cls._default = Cache()
        return cls._default

    @classmethod
    def set_default(cls, cache: 'Cache') -> None:
        """设置默认缓存实例"""
        with cls._lock:
            cls._default = cache

    def _make_key(self, key: str) -> CacheKey:
        """构建缓存键"""
        if self._key_prefix:
            key = f"{self._key_prefix}:{key}"
        return CacheKey(namespace=self._namespace, key=key)

    def get(self, key: str, default: T = None) -> Optional[T]:
        """获取缓存值"""
        cache_key = self._make_key(key)
        entry = self._backend.get(cache_key)

        if entry is None:
            self._stats.misses += 1
            return default

        if entry.is_expired:
            self._backend.delete(cache_key)
            self._stats.misses += 1
            return default

        entry.touch()
        self._stats.hits += 1
        return entry.value

    def set(
        self,
        key: str,
        value: T,
        ttl: Optional[float] = None,
        **metadata
    ) -> None:
        """设置缓存值

        Args:
            key: 缓存键
            value: 缓存值
            ttl: 生存时间（秒），None 表示永不过期
            **metadata: 附加元数据
        """
        cache_key = self._make_key(key)

        ttl = ttl or self._default_ttl
        expires_at = time.time() + ttl if ttl else None

        entry = CacheEntry(
            key=cache_key,
            value=value,
            expires_at=expires_at,
            size_bytes=_estimate_size(value),
            metadata=metadata,
        )

        self._backend.set(entry)
        self._stats.sets += 1

    def delete(self, key: str) -> bool:
        """删除缓存"""
        cache_key = self._make_key(key)
        deleted = self._backend.delete(cache_key)
        if deleted:
            self._stats.deletes += 1
        return deleted

    def exists(self, key: str) -> bool:
        """检查键是否存在"""
        cache_key = self._make_key(key)
        entry = self._backend.get(cache_key)
        return entry is not None and not entry.is_expired

    def get_or_set(
        self,
        key: str,
        default_factory: Callable[[], T],
        ttl: Optional[float] = None,
    ) -> T:
        """获取或设置（原子操作）

        如果键不存在，调用 default_factory 生成值并缓存。
        """
        value = self.get(key)
        if value is not None:
            return value

        value = default_factory()
        self.set(key, value, ttl=ttl)
        return value

    def get_many(self, keys: list[str]) -> Dict[str, Any]:
        """批量获取"""
        cache_keys = [self._make_key(k) for k in keys]
        entries = self._backend.get_many(cache_keys)

        result = {}
        for key, entry in entries.items():
            if not entry.is_expired:
                entry.touch()
                result[entry.key.key] = entry.value
                self._stats.hits += 1
            else:
                self._stats.misses += 1

        self._stats.misses += len(keys) - len(result)
        return result

    def set_many(self, mapping: Dict[str, Any], ttl: Optional[float] = None) -> None:
        """批量设置"""
        ttl = ttl or self._default_ttl
        expires_at = time.time() + ttl if ttl else None

        entries = []
        for key, value in mapping.items():
            cache_key = self._make_key(key)
            entries.append(CacheEntry(
                key=cache_key,
                value=value,
                expires_at=expires_at,
                size_bytes=_estimate_size(value),
            ))

        self._backend.set_many(entries)
        self._stats.sets += len(entries)

    def delete_many(self, keys: list[str]) -> int:
        """批量删除"""
        cache_keys = [self._make_key(k) for k in keys]
        deleted = self._backend.delete_many(cache_keys)
        self._stats.deletes += deleted
        return deleted

    def clear(self) -> None:
        """清空缓存"""
        self._backend.clear()
        self._stats.clears += 1

    def namespace(self, ns: str) -> 'Cache':
        """创建子命名空间"""
        return Cache(
            backend=self._backend,
            default_ttl=self._default_ttl,
            namespace=f"{self._namespace}:{ns}",
            key_prefix=self._key_prefix,
        )

    @property
    def stats(self) -> 'CacheStats':
        """获取统计信息"""
        return self._stats

    @property
    def size(self) -> int:
        """缓存条目数量"""
        return self._backend.size()


@dataclass
class CacheStats:
    """缓存统计"""
    hits: int = 0
    misses: int = 0
    sets: int = 0
    deletes: int = 0
    clears: int = 0

    @property
    def hit_rate(self) -> float:
        """命中率"""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0

    def reset(self) -> None:
        """重置统计"""
        self.hits = 0
        self.misses = 0
        self.sets = 0
        self.deletes = 0
        self.clears = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'hits': self.hits,
            'misses': self.misses,
            'sets': self.sets,
            'deletes': self.deletes,
            'clears': self.clears,
            'hit_rate': f"{self.hit_rate:.2%}",
        }


def get_default_cache() -> Cache:
    """获取默认缓存"""
    return Cache.get_default()


def set_default_cache(cache: Cache) -> None:
    """设置默认缓存"""
    Cache.set_default(cache)


def _hash_value(value: Any) -> str:
    """计算值的哈希"""
    try:
        # 尝试直接哈希
        return str(hash(value))[:8]
    except TypeError:
        # 不可哈希的对象，使用 repr
        return hashlib.md5(repr(value).encode()).hexdigest()[:8]


def _estimate_size(value: Any) -> int:
    """估算值的内存大小"""
    import sys

    try:
        # 特殊处理 DataFrame
        if hasattr(value, 'memory_usage'):
            return int(value.memory_usage(deep=True).sum())

        return sys.getsizeof(value)
    except Exception:
        return 0
