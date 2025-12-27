"""
缓存后端 (Cache Backends)
==========================

参考设计:
- cachetools: 内存缓存
- diskcache: 磁盘持久化
- Django cache: 多级缓存

支持多种存储后端。
"""
from __future__ import annotations
from pathlib import Path
from typing import Any, Dict, List, Optional
import threading
import time
import pickle
import hashlib
import shutil

from .core import CacheBackend, CacheKey, CacheEntry


class MemoryBackend(CacheBackend):
    """内存缓存后端

    基于字典的内存缓存，支持 LRU 淘汰。

    参考 cachetools 的 LRUCache 实现。
    """

    def __init__(
        self,
        maxsize: int = 1000,
        ttl: Optional[float] = None,
    ):
        self._cache: Dict[str, CacheEntry] = {}
        self._maxsize = maxsize
        self._default_ttl = ttl
        self._lock = threading.RLock()
        self._access_order: List[str] = []  # LRU 顺序

    def get(self, key: CacheKey) -> Optional[CacheEntry]:
        key_str = str(key)
        with self._lock:
            entry = self._cache.get(key_str)
            if entry:
                # 更新 LRU 顺序
                if key_str in self._access_order:
                    self._access_order.remove(key_str)
                self._access_order.append(key_str)
            return entry

    def set(self, entry: CacheEntry) -> None:
        key_str = str(entry.key)
        with self._lock:
            # 检查是否需要淘汰
            while len(self._cache) >= self._maxsize and self._access_order:
                oldest = self._access_order.pop(0)
                self._cache.pop(oldest, None)

            self._cache[key_str] = entry

            # 更新 LRU 顺序
            if key_str in self._access_order:
                self._access_order.remove(key_str)
            self._access_order.append(key_str)

    def delete(self, key: CacheKey) -> bool:
        key_str = str(key)
        with self._lock:
            if key_str in self._cache:
                del self._cache[key_str]
                if key_str in self._access_order:
                    self._access_order.remove(key_str)
                return True
            return False

    def exists(self, key: CacheKey) -> bool:
        key_str = str(key)
        with self._lock:
            return key_str in self._cache

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self._access_order.clear()

    def size(self) -> int:
        with self._lock:
            return len(self._cache)

    def cleanup_expired(self) -> int:
        """清理过期条目"""
        cleaned = 0
        with self._lock:
            expired_keys = [
                k for k, v in self._cache.items() if v.is_expired
            ]
            for key in expired_keys:
                del self._cache[key]
                if key in self._access_order:
                    self._access_order.remove(key)
                cleaned += 1
        return cleaned


class DiskBackend(CacheBackend):
    """磁盘缓存后端

    参考 diskcache 的设计，使用文件系统持久化。

    目录结构:
    cache_dir/
        namespace/
            ab/
                cd/
                    abcd1234.cache  # 缓存文件
    """

    def __init__(
        self,
        cache_dir: str | Path = ".cache",
        max_size_mb: int = 100,
    ):
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._max_size_bytes = max_size_mb * 1024 * 1024
        self._lock = threading.RLock()

    def _get_path(self, key: CacheKey) -> Path:
        """获取缓存文件路径"""
        key_str = str(key)
        key_hash = hashlib.md5(key_str.encode()).hexdigest()

        # 使用 2 级目录减少单目录文件数
        return (
            self._cache_dir
            / key.namespace
            / key_hash[:2]
            / key_hash[2:4]
            / f"{key_hash}.cache"
        )

    def get(self, key: CacheKey) -> Optional[CacheEntry]:
        path = self._get_path(key)

        with self._lock:
            if not path.exists():
                return None

            try:
                with open(path, 'rb') as f:
                    entry = pickle.load(f)
                return entry
            except Exception:
                # 损坏的缓存文件
                path.unlink(missing_ok=True)
                return None

    def set(self, entry: CacheEntry) -> None:
        path = self._get_path(entry.key)

        with self._lock:
            # 确保目录存在
            path.parent.mkdir(parents=True, exist_ok=True)

            try:
                with open(path, 'wb') as f:
                    pickle.dump(entry, f)
            except Exception:
                pass  # 写入失败，静默处理

    def delete(self, key: CacheKey) -> bool:
        path = self._get_path(key)

        with self._lock:
            if path.exists():
                path.unlink()
                return True
            return False

    def exists(self, key: CacheKey) -> bool:
        return self._get_path(key).exists()

    def clear(self) -> None:
        with self._lock:
            if self._cache_dir.exists():
                shutil.rmtree(self._cache_dir)
                self._cache_dir.mkdir(parents=True, exist_ok=True)

    def size(self) -> int:
        """缓存文件数量"""
        count = 0
        for _ in self._cache_dir.rglob("*.cache"):
            count += 1
        return count

    def disk_usage(self) -> int:
        """磁盘使用量（字节）"""
        total = 0
        for path in self._cache_dir.rglob("*.cache"):
            total += path.stat().st_size
        return total

    def cleanup_expired(self) -> int:
        """清理过期缓存"""
        cleaned = 0
        with self._lock:
            for path in self._cache_dir.rglob("*.cache"):
                try:
                    with open(path, 'rb') as f:
                        entry = pickle.load(f)
                    if entry.is_expired:
                        path.unlink()
                        cleaned += 1
                except Exception:
                    # 损坏的文件也删除
                    path.unlink(missing_ok=True)
                    cleaned += 1
        return cleaned

    def cleanup_by_size(self) -> int:
        """按大小清理（删除最旧的文件直到低于限制）"""
        cleaned = 0

        with self._lock:
            current_size = self.disk_usage()

            if current_size <= self._max_size_bytes:
                return 0

            # 按修改时间排序
            files = sorted(
                self._cache_dir.rglob("*.cache"),
                key=lambda p: p.stat().st_mtime
            )

            for path in files:
                if current_size <= self._max_size_bytes * 0.9:  # 保留 10% 余量
                    break

                try:
                    size = path.stat().st_size
                    path.unlink()
                    current_size -= size
                    cleaned += 1
                except Exception:
                    pass

        return cleaned


class TieredBackend(CacheBackend):
    """多级缓存后端

    组合多个后端，形成层级缓存。
    读取时从高层开始，写入时写入所有层。

    Example:
        tiered = TieredBackend([
            MemoryBackend(maxsize=100),  # L1: 内存
            DiskBackend(".cache"),        # L2: 磁盘
        ])
    """

    def __init__(self, backends: List[CacheBackend]):
        if not backends:
            raise ValueError("At least one backend is required")
        self._backends = backends

    def get(self, key: CacheKey) -> Optional[CacheEntry]:
        """从高层到低层查找"""
        entry = None
        found_at = -1

        for i, backend in enumerate(self._backends):
            entry = backend.get(key)
            if entry and not entry.is_expired:
                found_at = i
                break

        if entry is None:
            return None

        # 回填到更高层
        if found_at > 0:
            for i in range(found_at):
                self._backends[i].set(entry)

        return entry

    def set(self, entry: CacheEntry) -> None:
        """写入所有层"""
        for backend in self._backends:
            backend.set(entry)

    def delete(self, key: CacheKey) -> bool:
        """从所有层删除"""
        deleted = False
        for backend in self._backends:
            if backend.delete(key):
                deleted = True
        return deleted

    def exists(self, key: CacheKey) -> bool:
        """检查任意层是否存在"""
        for backend in self._backends:
            if backend.exists(key):
                return True
        return False

    def clear(self) -> None:
        """清空所有层"""
        for backend in self._backends:
            backend.clear()

    def size(self) -> int:
        """返回第一层大小"""
        return self._backends[0].size()


class NullBackend(CacheBackend):
    """空后端（用于禁用缓存）"""

    def get(self, key: CacheKey) -> Optional[CacheEntry]:
        return None

    def set(self, entry: CacheEntry) -> None:
        pass

    def delete(self, key: CacheKey) -> bool:
        return False

    def exists(self, key: CacheKey) -> bool:
        return False

    def clear(self) -> None:
        pass

    def size(self) -> int:
        return 0
