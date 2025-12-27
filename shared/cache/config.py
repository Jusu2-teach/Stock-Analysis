"""
缓存配置 (Cache Configuration)
===============================

支持从文件、环境变量加载缓存配置。
"""
from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional
import os
import json

from .core import Cache, set_default_cache
from .backends import MemoryBackend, DiskBackend, TieredBackend


@dataclass
class BackendConfig:
    """后端配置"""
    type: Literal["memory", "disk", "tiered", "null"]

    # Memory backend options
    maxsize: int = 1000

    # Disk backend options
    cache_dir: str = ".cache"
    max_size_mb: int = 100


@dataclass
class CacheConfig:
    """缓存配置

    Example:
        config = CacheConfig(
            default_ttl=3600,
            backends=[
                BackendConfig(type="memory", maxsize=500),
                BackendConfig(type="disk", cache_dir=".cache"),
            ]
        )
        config.apply()
    """
    default_ttl: Optional[float] = None
    namespace: str = "default"
    backends: List[BackendConfig] = field(default_factory=list)

    def __post_init__(self):
        if not self.backends:
            # 默认内存缓存
            self.backends = [BackendConfig(type="memory")]

    def apply(self) -> Cache:
        """应用配置并返回缓存实例"""
        if len(self.backends) == 1:
            backend = self._create_backend(self.backends[0])
        else:
            # 多后端：创建分层缓存
            backends = [self._create_backend(cfg) for cfg in self.backends]
            backend = TieredBackend(backends)

        cache = Cache(
            backend=backend,
            default_ttl=self.default_ttl,
            namespace=self.namespace,
        )

        set_default_cache(cache)
        return cache

    def _create_backend(self, cfg: BackendConfig):
        """创建后端"""
        if cfg.type == "memory":
            return MemoryBackend(maxsize=cfg.maxsize)
        elif cfg.type == "disk":
            return DiskBackend(
                cache_dir=cfg.cache_dir,
                max_size_mb=cfg.max_size_mb,
            )
        elif cfg.type == "null":
            from .backends import NullBackend
            return NullBackend()
        else:
            return MemoryBackend()


def load_cache_config(
    config_path: Optional[str | Path] = None,
    env_prefix: str = "ASTOCK_CACHE_",
) -> CacheConfig:
    """加载缓存配置

    环境变量:
    - ASTOCK_CACHE_BACKEND: memory, disk, tiered
    - ASTOCK_CACHE_TTL: 默认 TTL（秒）
    - ASTOCK_CACHE_DIR: 磁盘缓存目录
    - ASTOCK_CACHE_MAXSIZE: 内存缓存大小
    """
    config = CacheConfig()

    # 从文件加载
    if config_path:
        path = Path(config_path)
        if path.exists():
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                config = _config_from_dict(data)
            except Exception:
                pass

    # 环境变量覆盖
    env_ttl = os.environ.get(f"{env_prefix}TTL")
    if env_ttl:
        config.default_ttl = float(env_ttl)

    env_backend = os.environ.get(f"{env_prefix}BACKEND")
    env_dir = os.environ.get(f"{env_prefix}DIR")
    env_maxsize = os.environ.get(f"{env_prefix}MAXSIZE")

    if env_backend:
        backends = []

        if env_backend == "memory":
            maxsize = int(env_maxsize) if env_maxsize else 1000
            backends.append(BackendConfig(type="memory", maxsize=maxsize))

        elif env_backend == "disk":
            cache_dir = env_dir or ".cache"
            backends.append(BackendConfig(type="disk", cache_dir=cache_dir))

        elif env_backend == "tiered":
            maxsize = int(env_maxsize) if env_maxsize else 500
            cache_dir = env_dir or ".cache"
            backends.append(BackendConfig(type="memory", maxsize=maxsize))
            backends.append(BackendConfig(type="disk", cache_dir=cache_dir))

        if backends:
            config.backends = backends

    return config


def _config_from_dict(data: Dict[str, Any]) -> CacheConfig:
    """从字典创建配置"""
    backends = []
    for b in data.get('backends', []):
        backends.append(BackendConfig(**b))

    return CacheConfig(
        default_ttl=data.get('default_ttl'),
        namespace=data.get('namespace', 'default'),
        backends=backends,
    )


# 预设配置
PRESET_DEVELOPMENT = CacheConfig(
    default_ttl=300,  # 5 分钟
    backends=[
        BackendConfig(type="memory", maxsize=1000),
    ],
)

PRESET_PRODUCTION = CacheConfig(
    default_ttl=3600,  # 1 小时
    backends=[
        BackendConfig(type="memory", maxsize=500),
        BackendConfig(type="disk", cache_dir=".cache", max_size_mb=500),
    ],
)

PRESET_TESTING = CacheConfig(
    default_ttl=60,
    backends=[
        BackendConfig(type="memory", maxsize=100),
    ],
)
