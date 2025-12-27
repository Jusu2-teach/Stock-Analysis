"""
PGCS Serialization: Base
========================

序列化器基础设施。

设计原则:
- 序列化器是可插拔的后端
- 支持上下文传递 (精度、格式等)
- 完全通用，不绑定任何格式
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Dict, Type, TypeVar, Generic, TYPE_CHECKING
from abc import ABC, abstractmethod

if TYPE_CHECKING:
    from ..core.schema import Schema


T = TypeVar('T')


@dataclass
class SerializationContext:
    """
    序列化上下文

    Attributes:
        include_none: 是否包含 None 值
        use_alias: 是否使用别名
        precision: 浮点数精度
        date_format: 日期格式
        encoding: 字符串编码
        extras: 额外配置
    """
    include_none: bool = False
    use_alias: bool = True
    precision: Optional[int] = None
    date_format: str = '%Y-%m-%d'
    datetime_format: str = '%Y-%m-%dT%H:%M:%S'
    encoding: str = 'utf-8'
    extras: Dict[str, Any] = field(default_factory=dict)

    def with_extras(self, **kwargs) -> 'SerializationContext':
        """创建带额外配置的新上下文"""
        new_extras = {**self.extras, **kwargs}
        return SerializationContext(
            include_none=self.include_none,
            use_alias=self.use_alias,
            precision=self.precision,
            date_format=self.date_format,
            datetime_format=self.datetime_format,
            encoding=self.encoding,
            extras=new_extras,
        )


class Serializer(ABC, Generic[T]):
    """
    序列化器抽象基类

    所有序列化后端必须实现此接口。

    Example:
        class XMLSerializer(Serializer[str]):
            def serialize(self, data, context):
                # 转换为 XML 字符串
                ...

            def deserialize(self, data, schema_cls, context):
                # 从 XML 解析
                ...
    """

    @abstractmethod
    def serialize(
        self,
        data: Any,
        context: Optional[SerializationContext] = None,
    ) -> T:
        """
        序列化数据

        Args:
            data: 要序列化的数据 (Schema 实例或字典)
            context: 序列化上下文

        Returns:
            序列化后的数据
        """
        pass

    @abstractmethod
    def deserialize(
        self,
        data: T,
        schema_cls: Type['Schema'],
        context: Optional[SerializationContext] = None,
    ) -> 'Schema':
        """
        反序列化数据

        Args:
            data: 序列化的数据
            schema_cls: 目标 Schema 类
            context: 序列化上下文

        Returns:
            Schema 实例
        """
        pass


class SerializerRegistry:
    """
    序列化器注册表

    管理不同格式的序列化器。
    """
    _serializers: Dict[str, Serializer] = {}

    @classmethod
    def register(cls, name: str, serializer: Serializer):
        """注册序列化器"""
        cls._serializers[name] = serializer

    @classmethod
    def get(cls, name: str) -> Optional[Serializer]:
        """获取序列化器"""
        return cls._serializers.get(name)

    @classmethod
    def list_names(cls) -> list:
        """列出所有注册的序列化器名称"""
        return list(cls._serializers.keys())

    @classmethod
    def unregister(cls, name: str):
        """注销序列化器"""
        cls._serializers.pop(name, None)


__all__ = [
    'Serializer',
    'SerializationContext',
    'SerializerRegistry',
]
