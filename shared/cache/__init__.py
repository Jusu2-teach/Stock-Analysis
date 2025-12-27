"""
AStock 统一缓存系统 (Unified Cache System)
==========================================

集百家之长的缓存模块：
- cachetools: TTL/LRU/LFU 策略
- diskcache: 磁盘持久化
- joblib: 计算结果缓存
- Redis: 分布式缓存接口

核心特性:
1. 多级缓存 - 内存 → 磁盘 → 远程
2. 多种策略 - LRU, TTL, LFU
3. 装饰器支持 - @cached 自动缓存函数结果
4. DataFrame 优化 - 针对 pandas/polars 数据的高效序列化
5. EventBus 集成 - 缓存事件通知

Usage:
    from shared.cache import Cache, cached, TTLCache

    # 使用全局缓存
    cache = Cache.get_default()
    cache.set("key", value, ttl=300)
    value = cache.get("key")

    # 装饰器缓存
    @cached(ttl=3600)
    def expensive_computation(x):
        ...

    # DataFrame 缓存
    @cached(backend="disk", serializer="parquet")
    def load_data():
        return pd.read_csv("large.csv")
"""
__version__ = "1.0.0"

from .core import (
    Cache,
    CacheBackend,
    CacheKey,
    CacheEntry,
    get_default_cache,
    set_default_cache,
)

from .backends import (
    MemoryBackend,
    DiskBackend,
    TieredBackend,
)

from .strategies import (
    EvictionStrategy,
    LRUStrategy,
    TTLStrategy,
    LFUStrategy,
    SizeBasedStrategy,
)

from .serializers import (
    Serializer,
    PickleSerializer,
    JSONSerializer,
    ParquetSerializer,
    DataFrameSerializer,
)

from .decorators import (
    cached,
    cached_property,
    invalidate_cache,
)

from .config import (
    CacheConfig,
    load_cache_config,
)

__all__ = [
    # Core
    'Cache',
    'CacheBackend',
    'CacheKey',
    'CacheEntry',
    'get_default_cache',
    'set_default_cache',

    # Backends
    'MemoryBackend',
    'DiskBackend',
    'TieredBackend',

    # Strategies
    'EvictionStrategy',
    'LRUStrategy',
    'TTLStrategy',
    'LFUStrategy',
    'SizeBasedStrategy',

    # Serializers
    'Serializer',
    'PickleSerializer',
    'JSONSerializer',
    'ParquetSerializer',
    'DataFrameSerializer',

    # Decorators
    'cached',
    'cached_property',
    'invalidate_cache',

    # Config
    'CacheConfig',
    'load_cache_config',
]