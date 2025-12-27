"""
PGCS Metadata: Base
===================

元数据管理系统。

设计原则:
- 元数据是键值对的集合
- 支持嵌套和类型化
- 支持合并和继承
- 完全通用
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Dict, List, Iterator
from copy import deepcopy
import json


@dataclass
class Metadata:
    """
    元数据容器

    支持嵌套、类型化、合并的元数据。

    Example:
        meta = Metadata(
            version='1.0',
            author='test',
            tags=['important', 'reviewed'],
        )

        # 访问
        meta.get('version')  # '1.0'
        meta['author']  # 'test'

        # 合并
        meta2 = Metadata(priority=10)
        merged = meta.merge(meta2)
    """
    _data: Dict[str, Any] = field(default_factory=dict)

    def __init__(self, **kwargs):
        self._data = kwargs

    def get(self, key: str, default: Any = None) -> Any:
        """获取值"""
        return self._data.get(key, default)

    def set(self, key: str, value: Any):
        """设置值"""
        self._data[key] = value

    def has(self, key: str) -> bool:
        """检查键是否存在"""
        return key in self._data

    def remove(self, key: str):
        """删除键"""
        self._data.pop(key, None)

    def keys(self) -> List[str]:
        """获取所有键"""
        return list(self._data.keys())

    def values(self) -> List[Any]:
        """获取所有值"""
        return list(self._data.values())

    def items(self) -> Iterator[tuple[str, Any]]:
        """迭代键值对"""
        return iter(self._data.items())

    def merge(self, other: 'Metadata', override: bool = True) -> 'Metadata':
        """
        合并元数据

        Args:
            other: 另一个元数据
            override: 是否覆盖已存在的键

        Returns:
            新的合并后的元数据
        """
        result = deepcopy(self._data)

        for key, value in other._data.items():
            if key not in result or override:
                result[key] = deepcopy(value)

        return Metadata(**result)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return deepcopy(self._data)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Metadata':
        """从字典创建"""
        return cls(**data)

    def to_json(self) -> str:
        """转换为 JSON"""
        return json.dumps(self._data, default=str)

    @classmethod
    def from_json(cls, json_str: str) -> 'Metadata':
        """从 JSON 创建"""
        return cls(**json.loads(json_str))

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __setitem__(self, key: str, value: Any):
        self._data[key] = value

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"Metadata({self._data})"


class MetadataStore:
    """
    元数据存储

    管理多个命名元数据集合。

    Example:
        store = MetadataStore()

        # 存储
        store.set('user.profile', Metadata(name='test'))

        # 获取
        meta = store.get('user.profile')

        # 查询
        results = store.find(lambda m: m.get('name') == 'test')
    """

    def __init__(self):
        self._store: Dict[str, Metadata] = {}

    def set(self, key: str, metadata: Metadata):
        """设置元数据"""
        self._store[key] = metadata

    def get(self, key: str) -> Optional[Metadata]:
        """获取元数据"""
        return self._store.get(key)

    def get_or_create(self, key: str) -> Metadata:
        """获取或创建元数据"""
        if key not in self._store:
            self._store[key] = Metadata()
        return self._store[key]

    def remove(self, key: str):
        """删除元数据"""
        self._store.pop(key, None)

    def has(self, key: str) -> bool:
        """检查是否存在"""
        return key in self._store

    def keys(self) -> List[str]:
        """获取所有键"""
        return list(self._store.keys())

    def find(self, predicate: callable) -> List[tuple[str, Metadata]]:
        """
        查找元数据

        Args:
            predicate: 谓词函数 (Metadata) -> bool

        Returns:
            [(key, Metadata), ...]
        """
        results = []
        for key, meta in self._store.items():
            if predicate(meta):
                results.append((key, meta))
        return results

    def find_by_value(self, meta_key: str, value: Any) -> List[str]:
        """
        按元数据值查找

        Args:
            meta_key: 元数据键
            value: 元数据值

        Returns:
            匹配的存储键列表
        """
        return [
            key for key, meta in self._store.items()
            if meta.get(meta_key) == value
        ]

    def clear(self):
        """清空存储"""
        self._store.clear()

    def to_dict(self) -> Dict[str, Dict[str, Any]]:
        """转换为字典"""
        return {
            key: meta.to_dict()
            for key, meta in self._store.items()
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Dict[str, Any]]) -> 'MetadataStore':
        """从字典创建"""
        store = cls()
        for key, meta_dict in data.items():
            store.set(key, Metadata.from_dict(meta_dict))
        return store


__all__ = [
    'Metadata',
    'MetadataStore',
]
