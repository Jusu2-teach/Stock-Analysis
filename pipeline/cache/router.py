"""Pipeline Cache - Backend Router

按 CachePolicy.backend 动态选择缓存后端，并可按 namespace 隔离后端实例。

设计目标：
- 让 CachePolicy.backend 从“字段存在”变为“真正选择后端”
- 避免全局单一 CacheBackend 带来的语义模糊
- 复用后端实例（避免每次任务执行都新建文件/内存后端）
"""

from __future__ import annotations

import re
import threading
from pathlib import Path
from typing import Dict, Optional, Tuple

from .backends import CacheBackend, NullCacheBackend, create_cache_backend


class CacheBackendRouter:
    """缓存后端路由器

    - 按 backend_type (memory/file/tiered/none) 选择后端
    - 可按 namespace 隔离实例（特别是 file/tiered 的目录隔离）

    说明：namespace 为空时会回退到 "default"。
    """

    def __init__(
        self,
        base_cache_dir: str = ".cache/pipeline",
        memory_max_size: int = 1000,
        tiered_l1_max_size: int = 100,
    ):
        self._base_cache_dir = Path(base_cache_dir)
        self._memory_max_size = memory_max_size
        self._tiered_l1_max_size = tiered_l1_max_size

        self._lock = threading.RLock()
        self._backends: Dict[Tuple[str, str], CacheBackend] = {}
        self._null_backend = NullCacheBackend()

    def get(self, backend_type: str, namespace: Optional[str] = None) -> CacheBackend:
        """获取（或创建）指定后端。

        Args:
            backend_type: memory/file/tiered/none
            namespace: 可选命名空间，用于实例隔离
        """
        ns = self._normalize_namespace(namespace)
        bt = (backend_type or "memory").strip().lower()

        if bt == "none":
            return self._null_backend

        key = (bt, ns)
        with self._lock:
            existing = self._backends.get(key)
            if existing is not None:
                return existing

            cache_dir = str(self._base_cache_dir) if ns == "default" else str(self._base_cache_dir / ns)

            if bt == "memory":
                backend = create_cache_backend("memory", max_size=self._memory_max_size)
            elif bt == "file":
                backend = create_cache_backend("file", cache_dir=cache_dir)
            elif bt == "tiered":
                backend = create_cache_backend(
                    "tiered",
                    l1_max_size=self._tiered_l1_max_size,
                    cache_dir=cache_dir,
                )
            elif bt == "redis":
                # 显式透出未实现（与 create_cache_backend 行为一致）
                backend = create_cache_backend("redis")
            else:
                backend = create_cache_backend(bt)

            self._backends[key] = backend
            return backend

    @staticmethod
    def _normalize_namespace(namespace: Optional[str]) -> str:
        raw = (namespace or "").strip()
        if not raw:
            return "default"

        # 目录安全：仅保留常见可读字符，其余替换为 '_'
        safe = re.sub(r"[^a-zA-Z0-9._-]+", "_", raw)
        return safe or "default"
