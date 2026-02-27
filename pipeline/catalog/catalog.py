"""Pipeline Catalog - Data Catalog
==================================

统一数据目录，管理所有数据条目。

设计原则：
- 单一数据来源
- 血缘自动追踪
- 线程安全

版本: 2.0.0
"""

from __future__ import annotations

import logging
import threading
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    TypeVar,
    Union,
)

from .entry import DataEntry, EntryKey, EntryStatus, DatasetType, LineageInfo
from ..events import EventBus, DataEvents

logger = logging.getLogger(__name__)


# =============================================================================
# 类型定义
# =============================================================================

T = TypeVar('T')


@dataclass
class CatalogStats:
    """目录统计信息"""
    total_entries: int = 0
    available_entries: int = 0
    expired_entries: int = 0
    memory_bytes: int = 0  # 估算
    namespaces: int = 0


# =============================================================================
# 数据目录
# =============================================================================

class DataCatalog:
    """数据目录

    统一管理 Pipeline 中所有数据的中心。

    核心功能：
    - 数据存储和检索
    - 血缘追踪
    - 命名空间隔离
    - 自动过期

    Example:
        catalog = DataCatalog()

        # 存储数据
        catalog.save("analyze_roic.trend_result", result_df,
                     lineage=LineageInfo(source_task="analyze_roic"))

        # 检索数据
        df = catalog.load("analyze_roic.trend_result")

        # 按命名空间查询
        trends = catalog.get_by_namespace("trends")

    Note:
        不再支持单例模式，请使用 Container 依赖注入:
        container = get_container()
        catalog = container.resolve(DataCatalog)
    """

    def __init__(self):
        self._entries: Dict[str, DataEntry] = {}
        self._namespaces: Dict[str, Set[str]] = defaultdict(set)
        self._lock = threading.RLock()
        self._event_bus: Optional[EventBus] = None

    def set_event_bus(self, bus: EventBus) -> None:
        """设置事件总线"""
        self._event_bus = bus

    # -------------------------------------------------------------------------
    # 基本操作
    # -------------------------------------------------------------------------

    def save(
        self,
        key: Union[str, EntryKey],
        value: Any,
        *,
        dataset_type: DatasetType = DatasetType.MEMORY,
        lineage: Optional[LineageInfo] = None,
        metadata: Optional[Dict[str, Any]] = None,
        ttl_seconds: Optional[int] = None,
        namespace: str = "default",
    ) -> DataEntry:
        """保存数据

        Args:
            key: 数据键
            value: 数据值
            dataset_type: 数据集类型
            lineage: 血缘信息
            metadata: 额外元数据
            ttl_seconds: 生存时间
            namespace: 命名空间

        Returns:
            创建的数据条目
        """
        key_str = str(key) if isinstance(key, EntryKey) else key

        with self._lock:
            entry = self._entries.get(key_str)

            if entry:
                # 更新现有条目
                entry.set_value(value, lineage)
                if metadata:
                    entry.metadata.update(metadata)
                if ttl_seconds:
                    entry.ttl_seconds = ttl_seconds
            else:
                # 创建新条目
                entry = DataEntry(
                    key=key_str,
                    value=value,
                    dataset_type=dataset_type,
                    status=EntryStatus.AVAILABLE,
                    lineage=lineage,
                    metadata=metadata or {},
                    ttl_seconds=ttl_seconds,
                )
                self._entries[key_str] = entry

            # 添加到命名空间索引
            self._namespaces[namespace].add(key_str)

        logger.debug(f"Catalog saved: {key_str}")

        # 可观测性：发出数据产出事件
        if self._event_bus is not None:
            try:
                source = lineage.source_task if lineage else None
                self._event_bus.emit(DataEvents.produced(
                    key=key_str,
                    namespace=namespace,
                    source=source,
                ))
            except Exception:
                logger.debug("Failed to emit DataEvents.produced", exc_info=True)

        return entry

    def load(
        self,
        key: Union[str, EntryKey],
        default: T = None,
    ) -> Optional[T]:
        """加载数据

        Args:
            key: 数据键
            default: 默认值 (如果不存在或不可用)

        Returns:
            数据值或默认值
        """
        key_str = str(key) if isinstance(key, EntryKey) else key

        with self._lock:
            entry = self._entries.get(key_str)

            if entry and entry.is_available():
                entry.mark_accessed()
                value = entry.value
                # 可观测性：发出数据消费事件
                if self._event_bus is not None:
                    try:
                        self._event_bus.emit(DataEvents.consumed(
                            key=key_str,
                            namespace=entry.namespace,
                            consumer=None,
                        ))
                    except Exception:
                        logger.debug("Failed to emit DataEvents.consumed", exc_info=True)
                return value

        return default

    def exists(self, key: Union[str, EntryKey]) -> bool:
        """检查键是否存在"""
        key_str = str(key) if isinstance(key, EntryKey) else key
        with self._lock:
            entry = self._entries.get(key_str)
            return entry is not None and entry.is_available()

    def delete(self, key: Union[str, EntryKey]) -> bool:
        """删除条目

        Returns:
            是否成功删除
        """
        key_str = str(key) if isinstance(key, EntryKey) else key

        with self._lock:
            if key_str in self._entries:
                del self._entries[key_str]

                # 从命名空间索引移除
                for ns_keys in self._namespaces.values():
                    ns_keys.discard(key_str)

                return True

        return False

    def get_entry(self, key: Union[str, EntryKey]) -> Optional[DataEntry]:
        """获取条目 (含元数据)"""
        key_str = str(key) if isinstance(key, EntryKey) else key
        with self._lock:
            return self._entries.get(key_str)

    # -------------------------------------------------------------------------
    # 任务集成
    # -------------------------------------------------------------------------

    def save_task_outputs(
        self,
        task_name: str,
        outputs: Dict[str, Any],
        run_id: str = "",
        upstream_keys: Optional[List[str]] = None,
    ) -> Dict[str, DataEntry]:
        """保存任务输出

        便捷方法，自动创建血缘信息。

        Args:
            task_name: 任务名称
            outputs: 输出字典 {output_name: value}
            run_id: 运行 ID
            upstream_keys: 上游数据键列表

        Returns:
            创建的条目字典 {output_name: entry}
        """
        entries = {}

        for output_name, value in outputs.items():
            key = f"{task_name}.{output_name}"
            lineage = LineageInfo(
                source_task=task_name,
                source_outputs=(output_name,),
                upstream_entries=tuple(upstream_keys or []),
                run_id=run_id,
            )

            entry = self.save(key, value, lineage=lineage)
            entries[output_name] = entry

        return entries

    def load_task_inputs(
        self,
        input_sources: Dict[str, str],
    ) -> Dict[str, Any]:
        """加载任务输入

        根据输入源映射加载数据。

        Args:
            input_sources: {param_name: source_key}

        Returns:
            {param_name: value}
        """
        result = {}

        for param_name, source_key in input_sources.items():
            value = self.load(source_key)
            if value is not None:
                result[param_name] = value
            else:
                logger.warning(f"Input not found: {source_key}")

        return result

    # -------------------------------------------------------------------------
    # 命名空间操作
    # -------------------------------------------------------------------------

    def get_by_namespace(self, namespace: str) -> Dict[str, Any]:
        """获取命名空间中的所有数据

        Args:
            namespace: 命名空间名称

        Returns:
            {key: value} 字典
        """
        result = {}

        with self._lock:
            keys = self._namespaces.get(namespace, set())
            for key in keys:
                entry = self._entries.get(key)
                if entry and entry.is_available():
                    result[key] = entry.value

        return result

    def list_namespaces(self) -> List[str]:
        """列出所有命名空间"""
        with self._lock:
            return list(self._namespaces.keys())

    def clear_namespace(self, namespace: str) -> int:
        """清空命名空间

        Returns:
            删除的条目数
        """
        count = 0

        with self._lock:
            keys = list(self._namespaces.get(namespace, set()))
            for key in keys:
                if key in self._entries:
                    del self._entries[key]
                    count += 1

            if namespace in self._namespaces:
                del self._namespaces[namespace]

        return count

    # -------------------------------------------------------------------------
    # 血缘查询
    # -------------------------------------------------------------------------

    def get_lineage(self, key: Union[str, EntryKey]) -> Optional[LineageInfo]:
        """获取数据血缘"""
        entry = self.get_entry(key)
        return entry.lineage if entry else None

    def get_upstream(self, key: Union[str, EntryKey]) -> List[str]:
        """获取上游数据键"""
        lineage = self.get_lineage(key)
        if lineage:
            return list(lineage.upstream_entries)
        return []

    def get_downstream(self, key: Union[str, EntryKey]) -> List[str]:
        """获取下游数据键"""
        key_str = str(key) if isinstance(key, EntryKey) else key
        downstream = []

        with self._lock:
            for entry_key, entry in self._entries.items():
                if entry.lineage and key_str in entry.lineage.upstream_entries:
                    downstream.append(entry_key)

        return downstream

    def get_full_lineage(self, key: Union[str, EntryKey]) -> Dict[str, Any]:
        """获取完整血缘图

        递归追踪所有上游数据。
        """
        key_str = str(key) if isinstance(key, EntryKey) else key
        visited = set()

        def trace(k: str) -> Dict[str, Any]:
            if k in visited:
                return {'key': k, 'cycle': True}
            visited.add(k)

            entry = self._entries.get(k)
            if not entry or not entry.lineage:
                return {'key': k, 'upstream': []}

            upstream = [trace(u) for u in entry.lineage.upstream_entries]
            return {
                'key': k,
                'source_task': entry.lineage.source_task,
                'upstream': upstream,
            }

        with self._lock:
            return trace(key_str)

    # -------------------------------------------------------------------------
    # 维护操作
    # -------------------------------------------------------------------------

    def cleanup_expired(self) -> int:
        """清理过期条目

        Returns:
            清理的条目数
        """
        expired_keys = []

        with self._lock:
            for key, entry in self._entries.items():
                if entry.ttl_seconds is not None:
                    elapsed = (datetime.now() - entry.created_at).total_seconds()
                    if elapsed > entry.ttl_seconds:
                        expired_keys.append(key)

            for key in expired_keys:
                self._entries[key].status = EntryStatus.EXPIRED
                del self._entries[key]

                for ns_keys in self._namespaces.values():
                    ns_keys.discard(key)

        if expired_keys:
            logger.info(f"Cleaned up {len(expired_keys)} expired entries")

        return len(expired_keys)

    def get_stats(self) -> CatalogStats:
        """获取目录统计"""
        with self._lock:
            total = len(self._entries)
            available = sum(1 for e in self._entries.values() if e.is_available())
            expired = sum(1 for e in self._entries.values()
                         if e.status == EntryStatus.EXPIRED)

            return CatalogStats(
                total_entries=total,
                available_entries=available,
                expired_entries=expired,
                namespaces=len(self._namespaces),
            )

    def clear(self) -> None:
        """清空所有数据"""
        with self._lock:
            self._entries.clear()
            self._namespaces.clear()

    def list_keys(self, pattern: Optional[str] = None) -> List[str]:
        """列出所有键

        Args:
            pattern: 可选的过滤模式 (支持 * 通配符)

        Returns:
            匹配的键列表
        """
        import fnmatch

        with self._lock:
            keys = list(self._entries.keys())

            if pattern:
                keys = [k for k in keys if fnmatch.fnmatch(k, pattern)]

            return sorted(keys)

    def to_dict(self) -> Dict[str, Any]:
        """导出为字典"""
        with self._lock:
            return {
                key: entry.to_dict()
                for key, entry in self._entries.items()
            }

    # -------------------------------------------------------------------------
    # 上下文管理
    # -------------------------------------------------------------------------

    @contextmanager
    def transaction(self) -> Iterator['DataCatalog']:
        """事务上下文

        在事务中的操作可以回滚。
        注意: 当前实现是简化版本。
        """
        # 保存快照
        with self._lock:
            snapshot_entries = dict(self._entries)
            snapshot_namespaces = {k: set(v) for k, v in self._namespaces.items()}

        try:
            yield self
        except Exception:
            # 回滚
            with self._lock:
                self._entries = snapshot_entries
                self._namespaces = snapshot_namespaces
            raise
