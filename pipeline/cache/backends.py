"""Pipeline Cache - Cache Backends
==================================

缓存后端实现。

版本: 2.0.0
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import pickle
import tempfile
import time
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


# =============================================================================
# 缓存后端接口
# =============================================================================

class CacheBackend(ABC):
    """缓存后端基类"""

    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """获取缓存值"""
        raise NotImplementedError

    @abstractmethod
    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """设置缓存值"""
        raise NotImplementedError

    @abstractmethod
    def delete(self, key: str) -> bool:
        """删除缓存"""
        raise NotImplementedError

    @abstractmethod
    def exists(self, key: str) -> bool:
        """检查键是否存在"""
        raise NotImplementedError

    @abstractmethod
    def clear(self) -> int:
        """清空缓存，返回清除的条目数"""
        raise NotImplementedError

    @abstractmethod
    def get_stats(self) -> Dict[str, Any]:
        """获取缓存统计"""
        raise NotImplementedError


# =============================================================================
# 空缓存（显式禁用）
# =============================================================================

class NullCacheBackend(CacheBackend):
    """空缓存后端

    用于显式禁用缓存（backend='none'）。
    所有 get 都 miss；set/delete/clear 为 no-op。
    """

    def get(self, key: str) -> Optional[Any]:
        return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        return None

    def delete(self, key: str) -> bool:
        return False

    def exists(self, key: str) -> bool:
        return False

    def clear(self) -> int:
        return 0

    def get_stats(self) -> Dict[str, Any]:
        return {
            'backend': 'none',
            'entries': 0,
        }


# =============================================================================
# 内存缓存
# =============================================================================

@dataclass
class CacheEntry:
    """缓存条目"""
    value: Any
    created_at: float
    ttl: Optional[int] = None
    last_accessed: float = field(default_factory=time.time)  # LRU 追踪

    def is_expired(self) -> bool:
        if self.ttl is None:
            return False
        return time.time() - self.created_at > self.ttl

    def touch(self) -> None:
        """更新最后访问时间（用于 LRU）"""
        self.last_accessed = time.time()


class MemoryCacheBackend(CacheBackend):
    """内存缓存后端

    基于 OrderedDict 的内存缓存，支持 TTL 和真正的 LRU 淘汰。

    LRU 实现原理：
    - 使用 OrderedDict 维护插入/访问顺序
    - 每次 get() 命中时，将条目移动到末尾（最近访问）
    - 淘汰时删除头部条目（最久未访问）
    """

    def __init__(self, max_size: int = 1000):
        from collections import OrderedDict
        self._cache: OrderedDict[str, CacheEntry] = OrderedDict()
        self._max_size = max_size
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            entry = self._cache.get(key)

            if entry is None:
                self._misses += 1
                return None

            if entry.is_expired():
                del self._cache[key]
                self._misses += 1
                return None

            # LRU: 移动到末尾（最近访问）
            self._cache.move_to_end(key)
            entry.touch()
            self._hits += 1
            return entry.value

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        with self._lock:
            # 如果键已存在，先删除（保证新插入在末尾）
            if key in self._cache:
                del self._cache[key]

            # LRU 淘汰：当达到容量上限时
            while len(self._cache) >= self._max_size:
                self._evict_lru()

            self._cache[key] = CacheEntry(
                value=value,
                created_at=time.time(),
                ttl=ttl,
            )

    def delete(self, key: str) -> bool:
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def exists(self, key: str) -> bool:
        with self._lock:
            entry = self._cache.get(key)
            return entry is not None and not entry.is_expired()

    def clear(self) -> int:
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            return count

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            total_requests = self._hits + self._misses
            hit_rate = self._hits / total_requests if total_requests > 0 else 0.0

            return {
                'backend': 'memory',
                'entries': len(self._cache),
                'max_size': self._max_size,
                'hits': self._hits,
                'misses': self._misses,
                'hit_rate': hit_rate,
            }

    def _evict_lru(self) -> None:
        """淘汰最久未使用的条目（真正的 LRU）

        策略：
        1. 优先淘汰已过期的条目
        2. 如果没有过期条目，淘汰 OrderedDict 头部（最久未访问）
        """
        if not self._cache:
            return

        # 策略1: 首先淘汰过期条目
        expired_keys = [k for k, v in self._cache.items() if v.is_expired()]
        for k in expired_keys:
            del self._cache[k]
            if len(self._cache) < self._max_size:
                return

        # 策略2: 淘汰最久未访问的条目（OrderedDict 头部）
        if self._cache:
            # popitem(last=False) 删除并返回头部元素（最旧的）
            oldest_key = next(iter(self._cache))
            del self._cache[oldest_key]
            logger.debug(f"LRU evicted cache entry: {oldest_key[:16]}...")


# =============================================================================
# 文件缓存
# =============================================================================

class FileCacheBackend(CacheBackend):
    """文件缓存后端

    基于文件系统的持久化缓存。
    """

    def __init__(self, cache_dir: str = ".cache/pipeline"):
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._metadata_file = self._cache_dir / "metadata.json"
        self._metadata: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._load_metadata()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            file_path = self._get_file_path(key)

            if not file_path.exists():
                return None

            # 检查 TTL
            meta = self._metadata.get(key, {})
            ttl = meta.get('ttl')
            created_at = meta.get('created_at', 0)

            if ttl is not None:
                if time.time() - created_at > ttl:
                    self.delete(key)
                    return None

            try:
                with open(file_path, 'rb') as f:
                    return pickle.load(f)
            except (pickle.UnpicklingError, EOFError, IOError) as e:
                # 缓存文件损坏，自动清理
                logger.warning(f"Corrupted cache file '{key}', removing: {e}")
                self._cleanup_corrupted(key, file_path)
                return None
            except Exception as e:
                logger.warning(f"Failed to load cache '{key}': {e}")
                return None

    def _cleanup_corrupted(self, key: str, file_path: Path) -> None:
        """清理损坏的缓存文件"""
        try:
            if file_path.exists():
                file_path.unlink()
            self._metadata.pop(key, None)
            self._save_metadata()
            logger.info(f"Cleaned up corrupted cache: {key}")
        except Exception as e:
            logger.error(f"Failed to cleanup corrupted cache '{key}': {e}")

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        with self._lock:
            file_path = self._get_file_path(key)

            try:
                with open(file_path, 'wb') as f:
                    pickle.dump(value, f)

                self._metadata[key] = {
                    'created_at': time.time(),
                    'ttl': ttl,
                }
                self._save_metadata()
            except Exception as e:
                logger.warning(f"Failed to save cache '{key}': {e}")

    def delete(self, key: str) -> bool:
        with self._lock:
            file_path = self._get_file_path(key)

            if file_path.exists():
                file_path.unlink()
                self._metadata.pop(key, None)
                self._save_metadata()
                return True
            return False

    def exists(self, key: str) -> bool:
        file_path = self._get_file_path(key)
        return file_path.exists()

    def clear(self) -> int:
        with self._lock:
            count = 0
            for file_path in self._cache_dir.glob("*.cache"):
                file_path.unlink()
                count += 1

            self._metadata.clear()
            self._save_metadata()
            return count

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            cache_files = list(self._cache_dir.glob("*.cache"))
            total_size = sum(f.stat().st_size for f in cache_files)

            return {
                'backend': 'file',
                'cache_dir': str(self._cache_dir),
                'entries': len(cache_files),
                'total_size_bytes': total_size,
            }

    def _get_file_path(self, key: str) -> Path:
        """生成缓存文件路径"""
        safe_key = hashlib.md5(key.encode()).hexdigest()
        return self._cache_dir / f"{safe_key}.cache"

    def _load_metadata(self) -> None:
        """加载元数据"""
        if self._metadata_file.exists():
            try:
                with open(self._metadata_file, 'r') as f:
                    self._metadata = json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                logger.warning(f"Failed to load cache metadata: {e}")
                self._metadata = {}

    def _save_metadata(self) -> None:
        """保存元数据（原子写入，防止竞争条件）

        使用临时文件 + os.replace 实现原子写入，
        确保并发场景下元数据文件不会损坏。
        """
        try:
            # 创建临时文件在同一目录，确保 os.replace 原子性
            fd, temp_path = tempfile.mkstemp(
                dir=self._cache_dir,
                suffix='.tmp',
                prefix='metadata_'
            )
            try:
                with os.fdopen(fd, 'w') as f:
                    json.dump(self._metadata, f)
                # 原子替换：在同一文件系统内是原子操作
                os.replace(temp_path, self._metadata_file)
            except Exception:
                # 清理临时文件
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
                raise
        except Exception as e:
            logger.warning(f"Failed to save cache metadata: {e}")


# =============================================================================
# 复合缓存
# =============================================================================

class TieredCacheBackend(CacheBackend):
    """分层缓存

    L1: 内存 (快速, 热数据)
    L2: 文件 (持久化, 冷数据)

    特性：
    - 读时提升: L2 命中后自动提升到 L1
    - 写穿透: 同时写入 L1 和 L2
    - 删除级联: 同时从 L1 和 L2 删除
    - 预热支持: 启动时可预热 L1
    - 统计追踪: 分层命中率统计

    Example:
        cache = TieredCacheBackend(
            l1_max_size=100,
            l2_cache_dir=".cache/pipeline",
            warmup_keys=["key1", "key2"],  # 预热
        )

        cache.set("key", value)
        value = cache.get("key")  # L1 命中
    """

    def __init__(
        self,
        l1_max_size: int = 100,
        l2_cache_dir: str = ".cache/pipeline",
        warmup_keys: Optional[list] = None,
        write_through: bool = True,
    ):
        """
        Args:
            l1_max_size: L1 内存缓存最大条目数
            l2_cache_dir: L2 文件缓存目录
            warmup_keys: 启动时预热的键列表
            write_through: 是否写穿透 (同时写 L1 和 L2)
        """
        self._l1 = MemoryCacheBackend(max_size=l1_max_size)
        self._l2 = FileCacheBackend(cache_dir=l2_cache_dir)
        self._write_through = write_through
        self._lock = threading.RLock()

        # 统计
        self._l1_hits = 0
        self._l2_hits = 0
        self._misses = 0
        self._promotions = 0  # L2 → L1 提升次数

        # 预热
        if warmup_keys:
            self._warmup(warmup_keys)

    def _warmup(self, keys: list) -> int:
        """预热 L1 缓存

        Args:
            keys: 要预热的键列表

        Returns:
            成功预热的键数量
        """
        warmed = 0
        for key in keys:
            value = self._l2.get(key)
            if value is not None:
                self._l1.set(key, value)
                warmed += 1
                logger.debug(f"Warmed up cache key: {key}")

        if warmed > 0:
            logger.info(f"Cache warmup completed: {warmed}/{len(keys)} keys")

        return warmed

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            # L1 查找 (热数据)
            value = self._l1.get(key)
            if value is not None:
                self._l1_hits += 1
                return value

            # L2 查找 (冷数据)
            value = self._l2.get(key)
            if value is not None:
                self._l2_hits += 1
                # 提升到 L1 (读时提升策略)
                self._l1.set(key, value)
                self._promotions += 1
                return value

            self._misses += 1
            return None

    def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        with self._lock:
            # 总是写入 L1
            self._l1.set(key, value, ttl)

            # 写穿透模式: 同时写入 L2
            if self._write_through:
                self._l2.set(key, value, ttl)

    def delete(self, key: str) -> bool:
        with self._lock:
            l1_deleted = self._l1.delete(key)
            l2_deleted = self._l2.delete(key)
            return l1_deleted or l2_deleted

    def exists(self, key: str) -> bool:
        return self._l1.exists(key) or self._l2.exists(key)

    def clear(self) -> int:
        with self._lock:
            l1_count = self._l1.clear()
            l2_count = self._l2.clear()

            # 重置统计
            self._l1_hits = 0
            self._l2_hits = 0
            self._misses = 0
            self._promotions = 0

            return l1_count + l2_count

    def flush_to_l2(self) -> int:
        """将 L1 中所有数据刷新到 L2

        用于程序退出前持久化热数据。

        Returns:
            刷新的条目数
        """
        flushed = 0
        with self._lock:
            # 访问 L1 内部缓存数据（OrderedDict）
            with self._l1._lock:
                for key, entry in list(self._l1._cache.items()):
                    if not entry.is_expired():
                        self._l2.set(key, entry.value, entry.ttl)
                        flushed += 1
            if flushed > 0:
                logger.info(f"Flushed {flushed} entries from L1 to L2")
        return flushed

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            total_requests = self._l1_hits + self._l2_hits + self._misses

            return {
                'backend': 'tiered',
                'write_through': self._write_through,
                'l1': self._l1.get_stats(),
                'l2': self._l2.get_stats(),
                'performance': {
                    'l1_hits': self._l1_hits,
                    'l2_hits': self._l2_hits,
                    'misses': self._misses,
                    'promotions': self._promotions,
                    'total_requests': total_requests,
                    'l1_hit_rate': (
                        self._l1_hits / total_requests if total_requests > 0 else 0.0
                    ),
                    'overall_hit_rate': (
                        (self._l1_hits + self._l2_hits) / total_requests
                        if total_requests > 0 else 0.0
                    ),
                },
            }

    def demote_cold_entries(self, cold_threshold_seconds: float = 300.0) -> int:
        """降级冷数据

        将 L1 中最后访问时间超过阈值的条目移除 (保留在 L2)。

        Args:
            cold_threshold_seconds: 冷数据阈值（秒），默认 5 分钟未访问视为冷数据

        Returns:
            降级的条目数
        """
        demoted = 0
        now = time.time()
        keys_to_demote = []

        with self._lock:
            # 识别冷数据
            with self._l1._lock:
                for key, entry in self._l1._cache.items():
                    if now - entry.last_accessed > cold_threshold_seconds:
                        keys_to_demote.append(key)

            # 降级：从 L1 移除（L2 已有数据则保留）
            for key in keys_to_demote:
                self._l1.delete(key)
                demoted += 1

        if demoted > 0:
            logger.debug(f"Demoted {demoted} cold entries from L1")
        return demoted


# =============================================================================
# 工厂函数
# =============================================================================

def create_cache_backend(
    backend_type: str = "memory",
    **kwargs,
) -> CacheBackend:
    """创建缓存后端

    Args:
        backend_type: 后端类型 ('memory', 'file', 'tiered', 'none', 'redis')
        **kwargs: 后端配置参数

    Returns:
        缓存后端实例
    """
    if backend_type == "memory":
        return MemoryCacheBackend(
            max_size=kwargs.get('max_size', 1000),
        )
    elif backend_type == "file":
        return FileCacheBackend(
            cache_dir=kwargs.get('cache_dir', '.cache/pipeline'),
        )
    elif backend_type == "tiered":
        return TieredCacheBackend(
            l1_max_size=kwargs.get('l1_max_size', 100),
            l2_cache_dir=kwargs.get('cache_dir', '.cache/pipeline'),
        )
    elif backend_type == "none":
        return NullCacheBackend()
    elif backend_type == "redis":
        raise NotImplementedError(
            "Redis cache backend is not implemented yet. "
            "Use backend_type='memory'/'file'/'tiered' or backend_type='none'."
        )
    else:
        raise ValueError(f"Unknown cache backend: {backend_type}")
