"""
PDDA Collection Engine - 收集引擎
==================================

负责收集和存储可聚合数据。

核心功能：
1. 自动收集实现 Aggregatable 协议的数据
2. 支持多种收集策略（Dict, List, Stream等）
3. 线程安全的存储
4. 缓存和过期管理

设计原则：
- 策略模式：可插拔的收集策略
- 零配置：自动选择合适的策略
- 高性能：最小化内存拷贝
"""

from __future__ import annotations
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Generic, TypeVar
from threading import Lock
from datetime import datetime, timedelta

from .protocols import Aggregatable, AggregationMetadata

__all__ = [
    'CollectionStrategy',
    'DictCollectorStrategy',
    'ListCollectorStrategy',
    'UniversalCollector',
]

logger = logging.getLogger(__name__)

K = TypeVar('K')
V = TypeVar('V')


class CollectionStrategy(ABC, Generic[K, V]):
    """
    收集策略抽象基类

    定义如何收集和存储可聚合数据
    """

    @abstractmethod
    def collect(self, item: Aggregatable[K, V]) -> bool:
        """
        收集单个数据项

        Args:
            item: 可聚合数据项

        Returns:
            是否成功收集
        """
        ...

    @abstractmethod
    def get_all(self) -> Any:
        """
        获取所有收集的数据

        Returns:
            聚合后的数据（格式取决于策略）
        """
        ...

    @abstractmethod
    def clear(self):
        """清空收集的数据"""
        ...

    @abstractmethod
    def size(self) -> int:
        """返回收集的数据项数量"""
        ...


class DictCollectorStrategy(CollectionStrategy[K, V]):
    """
    字典收集策略

    将数据收集到字典中：{key: value}
    这是最常用的策略，适用于大多数场景。

    特性：
    - Key 相同时覆盖旧值
    - O(1) 查找性能
    - 线程安全
    """

    def __init__(self, allow_overwrite: bool = True):
        """
        Args:
            allow_overwrite: 是否允许覆盖已存在的 key
        """
        self._data: Dict[K, V] = {}
        self._metadata: Dict[K, AggregationMetadata] = {}
        self._lock = Lock()
        self._allow_overwrite = allow_overwrite

    def collect(self, item: Aggregatable[K, V]) -> bool:
        """收集到字典"""
        try:
            key = item.get_aggregation_key()
            value = item.get_aggregation_value()
            metadata = item.get_metadata()

            with self._lock:
                # 检查是否允许覆盖
                if not self._allow_overwrite and key in self._data:
                    logger.warning(f"Key {key} 已存在，跳过收集")
                    return False

                self._data[key] = value
                self._metadata[key] = metadata

                logger.debug(f"✓ 已收集: {key}")
                return True

        except Exception as e:
            logger.error(f"✗ 收集失败: {e}")
            return False

    def get_all(self) -> Dict[K, V]:
        """返回字典副本"""
        with self._lock:
            return self._data.copy()

    def get_with_metadata(self) -> Dict[K, tuple[V, AggregationMetadata]]:
        """返回数据和元数据"""
        with self._lock:
            return {
                k: (v, self._metadata.get(k))
                for k, v in self._data.items()
            }

    def clear(self):
        """清空数据"""
        with self._lock:
            self._data.clear()
            self._metadata.clear()

    def size(self) -> int:
        """返回数据项数量"""
        with self._lock:
            return len(self._data)

    def has_key(self, key: K) -> bool:
        """检查 key 是否存在"""
        with self._lock:
            return key in self._data


class ListCollectorStrategy(CollectionStrategy[K, V]):
    """
    列表收集策略

    将数据收集到列表中：[(key, value), ...]
    适用于需要保留顺序或允许重复 key 的场景。

    特性：
    - 保留插入顺序
    - 允许重复 key
    - 线程安全
    """

    def __init__(self, max_size: Optional[int] = None):
        """
        Args:
            max_size: 最大容量（None 表示无限制）
        """
        self._data: List[tuple[K, V]] = []
        self._metadata: List[AggregationMetadata] = []
        self._lock = Lock()
        self._max_size = max_size

    def collect(self, item: Aggregatable[K, V]) -> bool:
        """收集到列表"""
        try:
            key = item.get_aggregation_key()
            value = item.get_aggregation_value()
            metadata = item.get_metadata()

            with self._lock:
                # 检查容量限制
                if self._max_size and len(self._data) >= self._max_size:
                    logger.warning(f"达到最大容量 {self._max_size}，跳过收集")
                    return False

                self._data.append((key, value))
                self._metadata.append(metadata)

                logger.debug(f"✓ 已收集: {key} (total: {len(self._data)})")
                return True

        except Exception as e:
            logger.error(f"✗ 收集失败: {e}")
            return False

    def get_all(self) -> List[tuple[K, V]]:
        """返回列表副本"""
        with self._lock:
            return self._data.copy()

    def get_as_dict(self) -> Dict[K, V]:
        """转换为字典（后出现的覆盖先出现的）"""
        with self._lock:
            return dict(self._data)

    def clear(self):
        """清空数据"""
        with self._lock:
            self._data.clear()
            self._metadata.clear()

    def size(self) -> int:
        """返回数据项数量"""
        with self._lock:
            return len(self._data)


class UniversalCollector:
    """
    通用收集器

    自动管理所有可聚合数据的收集。

    特性：
    - 自动选择收集策略
    - 支持多个独立的收集空间
    - 线程安全
    - 缓存过期管理

    使用示例：
        collector = UniversalCollector()

        # 收集数据
        result = AggregatableResult(key="roic", value=df)
        collector.collect(result)

        # 获取所有数据
        all_data = collector.get_all()
    """

    def __init__(
        self,
        default_strategy: CollectionStrategy = None,
        enable_cache: bool = True,
        cache_ttl: int = 3600
    ):
        """
        Args:
            default_strategy: 默认收集策略
            enable_cache: 是否启用缓存
            cache_ttl: 缓存过期时间（秒）
        """
        self._strategy = default_strategy or DictCollectorStrategy()
        self._enable_cache = enable_cache
        self._cache_ttl = cache_ttl

        # 缓存时间戳
        self._last_collected: Dict[Any, datetime] = {}
        self._lock = Lock()

        logger.debug(f"UniversalCollector 初始化: strategy={type(self._strategy).__name__}")

    def collect(self, item: Any) -> bool:
        """
        收集数据项

        Args:
            item: 数据项（必须实现 Aggregatable 协议）

        Returns:
            是否成功收集
        """
        # 检查是否实现 Aggregatable
        if not isinstance(item, Aggregatable):
            logger.warning(f"数据项未实现 Aggregatable 协议: {type(item)}")
            return False

        # 检查元数据
        metadata = item.get_metadata()

        # 检查是否自动收集
        if not metadata.auto_collect:
            logger.debug(f"跳过收集（auto_collect=False）: {item.get_aggregation_key()}")
            return False

        # 检查缓存
        if self._enable_cache and metadata.cache_enabled:
            if not self._check_cache_validity(item):
                logger.debug(f"使用缓存数据: {item.get_aggregation_key()}")
                return True

        # 收集数据
        success = self._strategy.collect(item)

        if success:
            # 更新缓存时间戳
            with self._lock:
                self._last_collected[item.get_aggregation_key()] = datetime.now()

        return success

    def collect_batch(self, items: List[Any]) -> int:
        """
        批量收集

        Args:
            items: 数据项列表

        Returns:
            成功收集的数量
        """
        success_count = 0
        for item in items:
            if self.collect(item):
                success_count += 1
        return success_count

    def get_all(self) -> Any:
        """获取所有收集的数据"""
        return self._strategy.get_all()

    def clear(self):
        """清空所有数据"""
        self._strategy.clear()
        with self._lock:
            self._last_collected.clear()
        logger.debug("已清空收集器")

    def size(self) -> int:
        """返回收集的数据项数量"""
        return self._strategy.size()

    def set_strategy(self, strategy: CollectionStrategy):
        """切换收集策略"""
        old_data = self._strategy.get_all()
        self._strategy = strategy

        # 迁移数据（如果可能）
        if isinstance(old_data, dict):
            for key, value in old_data.items():
                try:
                    # 重新包装为 Aggregatable
                    from .protocols import AggregatableResult
                    item = AggregatableResult(key=key, value=value)
                    strategy.collect(item)
                except:
                    pass

    def _check_cache_validity(self, item: Aggregatable) -> bool:
        """
        检查缓存是否有效

        Returns:
            True 表示需要重新收集，False 表示使用缓存
        """
        key = item.get_aggregation_key()

        with self._lock:
            last_time = self._last_collected.get(key)

            if last_time is None:
                return True  # 从未收集，需要收集

            # 检查是否过期
            metadata = item.get_metadata()
            ttl = metadata.cache_ttl

            elapsed = (datetime.now() - last_time).total_seconds()
            return elapsed >= ttl

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            'size': self.size(),
            'strategy': type(self._strategy).__name__,
            'cache_enabled': self._enable_cache,
            'cached_keys': len(self._last_collected),
        }
