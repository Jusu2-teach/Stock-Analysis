"""
PGCS Store: Data Entry
======================

通用数据条目定义。

设计原则:
- 纯数据结构，无业务逻辑
- 复用 contracts.Metadata 存储元数据
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Dict

from ..metadata.base import Metadata


@dataclass
class DataEntry:
    """
    数据存储条目

    通用的数据条目，存储数据及其元数据。

    Attributes:
        key: 主键
        value: 数据对象
        ref: 引用路径（用于多视图索引）
        fingerprint: 数据指纹
        metadata: 元数据（复用 contracts.Metadata）

    Example:
        entry = DataEntry(
            key='my_key',
            value={'foo': 'bar'},
            ref='path.to.data',
            fingerprint='abc123',
            metadata=Metadata(producer='step1', created_at='2024-01-01'),
        )

        # 访问元数据
        entry.metadata.get('producer')  # 'step1'
    """
    key: str
    value: Any
    ref: str = ''
    fingerprint: str = ''
    metadata: Metadata = field(default_factory=Metadata)

    def __post_init__(self):
        # 确保 metadata 是 Metadata 实例
        if isinstance(self.metadata, dict):
            self.metadata = Metadata(**self.metadata)

    def get_meta(self, key: str, default: Any = None) -> Any:
        """获取元数据值"""
        return self.metadata.get(key, default)

    def set_meta(self, key: str, value: Any):
        """设置元数据值"""
        self.metadata.set(key, value)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'key': self.key,
            'ref': self.ref,
            'fingerprint': self.fingerprint,
            'metadata': self.metadata.to_dict() if hasattr(self.metadata, 'to_dict') else dict(self.metadata._data),
            # 注意: value 不序列化，因为可能是复杂对象
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any], value: Any = None) -> 'DataEntry':
        """从字典创建"""
        return cls(
            key=data['key'],
            value=value,
            ref=data.get('ref', ''),
            fingerprint=data.get('fingerprint', ''),
            metadata=Metadata(**data.get('metadata', {})),
        )
