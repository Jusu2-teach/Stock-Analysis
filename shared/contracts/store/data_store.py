"""
PGCS Store: Data Store
======================

通用数据存储。

设计原则:
- 单一真相源 + 多视图索引
- 零业务耦合：不包含任何领域特定概念
- 复用 contracts 组件：fingerprint, Metadata, Lineage
- 支持持久化

架构:
    _store (主存储)          key -> DataEntry
        │
        ├── _ref_index       ref -> key (引用路径索引)
        │
        └── _hash_index      hash -> key (哈希索引)
"""

from __future__ import annotations

import hashlib
import json
import pickle
import threading
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Tuple, List, Callable

from ..utils.fingerprint import fingerprint as compute_fingerprint
from ..metadata.base import Metadata
from ..metadata.lineage import Lineage, LineageNode, NodeType
from .data_entry import DataEntry


class DataStore:
    """
    PGCS 通用数据存储

    单一真相源 + 多视图索引的数据存储。

    特性:
    - 主键访问: store.get('key')
    - 引用访问: store.get_by_ref('path.to.data')
    - 哈希访问: store.get_by_hash('abc123')
    - 内置血缘追踪（复用 contracts.Lineage）
    - 支持持久化到磁盘

    Example:
        store = DataStore()

        # 存储
        store.put('key1', my_data, ref='path.to.data')

        # 获取
        data = store.get('key1')
        data = store.get_by_ref('path.to.data')

        # 遍历
        for key, value in store.items():
            print(key, value)

        # 持久化
        store.save_to_disk(Path('./cache'))
        store.load_from_disk(Path('./cache'))
    """

    def __init__(self, auto_fingerprint: bool = True):
        """
        初始化数据存储

        Args:
            auto_fingerprint: 是否自动计算数据指纹
        """
        self._store: Dict[str, DataEntry] = {}
        self._ref_index: Dict[str, str] = {}      # ref -> key
        self._hash_index: Dict[str, str] = {}     # hash -> key
        self._lineage = Lineage()                 # 复用 contracts.Lineage
        self._auto_fingerprint = auto_fingerprint
        self._lock = threading.Lock()

    # ==================== 属性 ====================

    @property
    def lineage(self) -> Lineage:
        """获取血缘图（复用 contracts.Lineage）"""
        return self._lineage

    # ==================== 存储 ====================

    def put(
        self,
        key: str,
        value: Any,
        ref: str = '',
        fingerprint: str = '',
        **meta_kwargs,
    ) -> DataEntry:
        """
        存储数据

        Args:
            key: 主键
            value: 数据对象
            ref: 引用路径（可选，用于 get_by_ref）
            fingerprint: 数据指纹（可选，如不提供则自动计算）
            **meta_kwargs: 元数据

        Returns:
            DataEntry
        """
        # 自动计算指纹
        if not fingerprint and self._auto_fingerprint:
            fingerprint = self._compute_fingerprint(value)

        # 自动添加创建时间
        if 'created_at' not in meta_kwargs:
            meta_kwargs['created_at'] = datetime.now().isoformat()

        # 创建条目
        entry = DataEntry(
            key=key,
            value=value,
            ref=ref,
            fingerprint=fingerprint,
            metadata=Metadata(**meta_kwargs),
        )

        with self._lock:
            # 存储
            self._store[key] = entry

            # 更新引用索引
            if ref:
                self._ref_index[ref] = key

            # 更新哈希索引
            if fingerprint:
                self._hash_index[fingerprint] = key

        return entry

    def put_many(self, entries: List[Tuple[str, Any, str]]) -> List[DataEntry]:
        """
        批量存储

        Args:
            entries: [(key, value, ref), ...]

        Returns:
            List[DataEntry]
        """
        results = []
        for item in entries:
            key, value = item[0], item[1]
            ref = item[2] if len(item) > 2 else ''
            results.append(self.put(key, value, ref=ref))
        return results

    # ==================== 获取 ====================

    def get(self, key: str) -> Optional[Any]:
        """
        通过主键获取数据

        Args:
            key: 主键

        Returns:
            数据对象，如不存在返回 None
        """
        entry = self._store.get(key)
        return entry.value if entry else None

    def get_by_ref(self, ref: str) -> Optional[Any]:
        """
        通过引用路径获取数据

        Args:
            ref: 引用路径

        Returns:
            数据对象，如不存在返回 None
        """
        key = self._ref_index.get(ref)
        return self.get(key) if key else None

    def get_by_hash(self, hash_key: str) -> Optional[Any]:
        """
        通过哈希获取数据

        Args:
            hash_key: 哈希值

        Returns:
            数据对象，如不存在返回 None
        """
        key = self._hash_index.get(hash_key)
        return self.get(key) if key else None

    def get_entry(self, key: str) -> Optional[DataEntry]:
        """
        获取完整条目

        Args:
            key: 主键

        Returns:
            DataEntry，如不存在返回 None
        """
        return self._store.get(key)

    def get_fingerprint(self, key: str) -> Optional[str]:
        """
        获取数据指纹

        Args:
            key: 主键

        Returns:
            指纹字符串，如不存在返回 None
        """
        entry = self._store.get(key)
        return entry.fingerprint if entry else None

    def get_metadata(self, key: str) -> Optional[Metadata]:
        """
        获取元数据

        Args:
            key: 主键

        Returns:
            Metadata，如不存在返回 None
        """
        entry = self._store.get(key)
        return entry.metadata if entry else None

    # ==================== 删除 ====================

    def remove(self, key: str) -> bool:
        """
        删除数据

        Args:
            key: 主键

        Returns:
            是否删除成功
        """
        with self._lock:
            entry = self._store.pop(key, None)
            if entry:
                # 清理索引
                if entry.ref and entry.ref in self._ref_index:
                    del self._ref_index[entry.ref]
                if entry.fingerprint and entry.fingerprint in self._hash_index:
                    del self._hash_index[entry.fingerprint]
                return True
            return False

    def clear(self):
        """清空存储"""
        with self._lock:
            self._store.clear()
            self._ref_index.clear()
            self._hash_index.clear()

    # ==================== 查询 ====================

    def has(self, key: str) -> bool:
        """检查主键是否存在"""
        return key in self._store

    def has_ref(self, ref: str) -> bool:
        """检查引用是否存在"""
        return ref in self._ref_index

    def __contains__(self, key: str) -> bool:
        return self.has(key)

    def __len__(self) -> int:
        return len(self._store)

    # ==================== 迭代 ====================

    def keys(self) -> Iterator[str]:
        """迭代所有主键"""
        return iter(self._store.keys())

    def values(self) -> Iterator[Any]:
        """迭代所有值"""
        for entry in self._store.values():
            yield entry.value

    def items(self) -> Iterator[Tuple[str, Any]]:
        """迭代所有键值对"""
        for key, entry in self._store.items():
            yield key, entry.value

    def entries(self) -> Iterator[DataEntry]:
        """迭代所有条目"""
        return iter(self._store.values())

    def refs(self) -> Iterator[str]:
        """迭代所有引用路径"""
        return iter(self._ref_index.keys())

    # ==================== 过滤 ====================

    def filter(self, predicate: Callable[[DataEntry], bool]) -> List[DataEntry]:
        """
        过滤条目

        Args:
            predicate: 过滤函数

        Returns:
            符合条件的条目列表
        """
        return [entry for entry in self._store.values() if predicate(entry)]

    def find_by_metadata(self, key: str, value: Any) -> List[DataEntry]:
        """
        通过元数据查找

        Args:
            key: 元数据键
            value: 元数据值

        Returns:
            符合条件的条目列表
        """
        return self.filter(lambda e: e.get_meta(key) == value)

    # ==================== 持久化 ====================

    def save_to_disk(
        self,
        base_path: Path,
        serializer: Optional[Callable[[Any, Path], None]] = None,
    ) -> int:
        """
        保存到磁盘

        Args:
            base_path: 基础路径
            serializer: 自定义序列化函数（可选）

        Returns:
            保存的条目数
        """
        base_path = Path(base_path)
        base_path.mkdir(parents=True, exist_ok=True)
        datasets_dir = base_path / 'datasets'
        datasets_dir.mkdir(exist_ok=True)

        index = {}
        saved = 0

        for key, entry in self._store.items():
            try:
                # 序列化数据
                data_file = datasets_dir / f"{self._safe_filename(key)}.pkl"
                if serializer:
                    serializer(entry.value, data_file)
                else:
                    with open(data_file, 'wb') as f:
                        pickle.dump(entry.value, f)

                # 记录索引
                index[key] = entry.to_dict()
                index[key]['file'] = str(data_file.relative_to(base_path))
                saved += 1

            except Exception as e:
                # 跳过无法序列化的数据
                continue

        # 保存索引
        (base_path / 'store_index.json').write_text(
            json.dumps(index, indent=2, ensure_ascii=False),
            encoding='utf-8',
        )

        # 保存血缘图
        (base_path / 'lineage.json').write_text(
            json.dumps(self._lineage.to_dict(), indent=2, ensure_ascii=False),
            encoding='utf-8',
        )

        return saved

    def load_from_disk(
        self,
        base_path: Path,
        deserializer: Optional[Callable[[Path], Any]] = None,
    ) -> int:
        """
        从磁盘加载

        Args:
            base_path: 基础路径
            deserializer: 自定义反序列化函数（可选）

        Returns:
            加载的条目数
        """
        base_path = Path(base_path)
        index_file = base_path / 'store_index.json'
        if not index_file.exists():
            return 0

        index = json.loads(index_file.read_text(encoding='utf-8'))
        loaded = 0

        for key, meta in index.items():
            try:
                data_file = base_path / meta['file']
                if not data_file.exists():
                    continue

                # 反序列化数据
                if deserializer:
                    value = deserializer(data_file)
                else:
                    with open(data_file, 'rb') as f:
                        value = pickle.load(f)

                # 创建条目
                entry = DataEntry.from_dict(meta, value)

                with self._lock:
                    self._store[key] = entry
                    if entry.ref:
                        self._ref_index[entry.ref] = key
                    if entry.fingerprint:
                        self._hash_index[entry.fingerprint] = key

                loaded += 1

            except Exception as e:
                continue

        # 加载血缘图
        lineage_file = base_path / 'lineage.json'
        if lineage_file.exists():
            try:
                lineage_data = json.loads(lineage_file.read_text(encoding='utf-8'))
                for node_data in lineage_data.get('nodes', []):
                    self._lineage.add_node(
                        node_id=node_data['id'],
                        name=node_data['name'],
                        node_type=NodeType(node_data.get('type', 'transform')),
                        **node_data.get('metadata', {}),
                    )
                for edge in lineage_data.get('edges', []):
                    self._lineage.connect(edge['from'], edge['to'])
            except Exception:
                pass

        return loaded

    # ==================== 内部方法 ====================

    def _compute_fingerprint(self, value: Any) -> str:
        """计算数据指纹"""
        return compute_fingerprint(value, length=16)

    @staticmethod
    def _safe_filename(key: str) -> str:
        """将 key 转换为安全的文件名"""
        # 替换不安全字符
        return key.replace('/', '_').replace('\\', '_').replace(':', '_')


class SingletonDataStore(DataStore):
    """
    单例数据存储

    适用于需要全局唯一存储的场景。

    Example:
        store = SingletonDataStore.get()
        store.put('key', value)

        # 在其他地方
        same_store = SingletonDataStore.get()
        same_store.get('key')  # 相同的数据
    """

    _instance: Optional['SingletonDataStore'] = None
    _singleton_lock = threading.Lock()

    @classmethod
    def get(cls) -> 'SingletonDataStore':
        """获取单例实例"""
        if cls._instance is None:
            with cls._singleton_lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls):
        """重置单例（用于测试）"""
        with cls._singleton_lock:
            if cls._instance is not None:
                cls._instance.clear()
            cls._instance = None
