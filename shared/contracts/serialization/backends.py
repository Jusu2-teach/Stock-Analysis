"""
PGCS Serialization: Built-in Backends
=====================================

内置序列化后端。
"""

from __future__ import annotations

import json
from typing import Any, Optional, Dict, Type, TYPE_CHECKING

from .base import Serializer, SerializationContext, SerializerRegistry

if TYPE_CHECKING:
    from ..core.schema import Schema


class DictSerializer(Serializer[Dict[str, Any]]):
    """
    字典序列化器

    将 Schema 实例转换为 Python 字典。
    """

    def serialize(
        self,
        data: Any,
        context: Optional[SerializationContext] = None,
    ) -> Dict[str, Any]:
        ctx = context or SerializationContext()

        # 如果是 Schema 实例
        if hasattr(data, '__fields__') and hasattr(data, 'to_dict'):
            result = {}
            for name, field in data.__fields__.items():
                value = getattr(data, name, None)

                # 跳过 None
                if value is None and not ctx.include_none:
                    continue

                # 使用别名
                key = name
                if ctx.use_alias and field.descriptor.alias:
                    key = field.descriptor.alias

                # 递归序列化
                result[key] = self._serialize_value(value, ctx)

            return result

        # 如果已经是字典
        if isinstance(data, dict):
            return {
                k: self._serialize_value(v, ctx)
                for k, v in data.items()
                if v is not None or ctx.include_none
            }

        raise TypeError(f"Cannot serialize {type(data)}")

    def _serialize_value(self, value: Any, ctx: SerializationContext) -> Any:
        """递归序列化值"""
        if value is None:
            return None

        if isinstance(value, (str, int, bool)):
            return value

        if isinstance(value, float):
            if ctx.precision is not None:
                return round(value, ctx.precision)
            return value

        if isinstance(value, (list, tuple)):
            return [self._serialize_value(v, ctx) for v in value]

        if isinstance(value, dict):
            return {k: self._serialize_value(v, ctx) for k, v in value.items()}

        if hasattr(value, '__fields__'):
            return self.serialize(value, ctx)

        # 其他类型转字符串
        return str(value)

    def deserialize(
        self,
        data: Dict[str, Any],
        schema_cls: Type['Schema'],
        context: Optional[SerializationContext] = None,
    ) -> 'Schema':
        ctx = context or SerializationContext()

        # 构建 alias -> name 映射
        alias_map = {}
        if ctx.use_alias:
            for name, field in schema_cls.__fields__.items():
                alias = field.descriptor.alias
                if alias:
                    alias_map[alias] = name

        # 转换数据
        kwargs = {}
        for key, value in data.items():
            name = alias_map.get(key, key)
            if name in schema_cls.__fields__:
                kwargs[name] = value

        return schema_cls(**kwargs)


class JSONSerializer(Serializer[str]):
    """
    JSON 序列化器

    将 Schema 实例转换为 JSON 字符串。
    """

    def __init__(
        self,
        indent: Optional[int] = None,
        ensure_ascii: bool = False,
        sort_keys: bool = False,
    ):
        self.indent = indent
        self.ensure_ascii = ensure_ascii
        self.sort_keys = sort_keys
        self._dict_serializer = DictSerializer()

    def serialize(
        self,
        data: Any,
        context: Optional[SerializationContext] = None,
    ) -> str:
        dict_data = self._dict_serializer.serialize(data, context)
        return json.dumps(
            dict_data,
            indent=self.indent,
            ensure_ascii=self.ensure_ascii,
            sort_keys=self.sort_keys,
            default=str,
        )

    def deserialize(
        self,
        data: str,
        schema_cls: Type['Schema'],
        context: Optional[SerializationContext] = None,
    ) -> 'Schema':
        dict_data = json.loads(data)
        return self._dict_serializer.deserialize(dict_data, schema_cls, context)


# 注册内置序列化器
SerializerRegistry.register('dict', DictSerializer())
SerializerRegistry.register('json', JSONSerializer())
SerializerRegistry.register('json_pretty', JSONSerializer(indent=2))


__all__ = [
    'DictSerializer',
    'JSONSerializer',
]
