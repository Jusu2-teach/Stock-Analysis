"""
PGCS Core: Type System
======================

通用类型信息和类型适配器。

设计原则:
- 不包含任何业务类型定义
- 提供类型内省和转换的抽象接口
- 支持用户自定义类型扩展
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import (
    Any, Optional, Dict, List, Type, TypeVar, Generic,
    Callable, Union, get_type_hints, get_origin, get_args,
)
from abc import ABC, abstractmethod
import sys

T = TypeVar('T')


@dataclass(frozen=True)
class TypeInfo:
    """
    类型信息描述符

    封装 Python 类型的元信息，支持泛型和 Union 类型。

    Attributes:
        origin: 原始类型 (如 list, dict, Optional 的 Union)
        args: 类型参数 (如 List[int] 中的 int)
        raw: 原始类型注解
        is_optional: 是否为 Optional 类型
        is_generic: 是否为泛型类型

    Example:
        info = TypeInfo.from_annotation(List[int])
        assert info.origin == list
        assert info.args == (int,)
    """
    origin: Optional[Type] = None
    args: tuple = field(default_factory=tuple)
    raw: Any = None
    is_optional: bool = False
    is_generic: bool = False

    @classmethod
    def from_annotation(cls, annotation: Any) -> 'TypeInfo':
        """从类型注解创建 TypeInfo"""
        if annotation is None:
            return cls(raw=type(None))

        origin = get_origin(annotation)
        args = get_args(annotation)

        # 检查是否为 Optional (Union[X, None])
        is_optional = False
        if origin is Union:
            non_none_args = [a for a in args if a is not type(None)]
            if len(non_none_args) < len(args):
                is_optional = True
                # 简化为非 None 类型
                if len(non_none_args) == 1:
                    origin = get_origin(non_none_args[0]) or non_none_args[0]
                    args = get_args(non_none_args[0]) or ()

        is_generic = origin is not None

        return cls(
            origin=origin or annotation if not is_generic else origin,
            args=args,
            raw=annotation,
            is_optional=is_optional,
            is_generic=is_generic,
        )

    @property
    def python_type(self) -> Type:
        """获取 Python 基础类型"""
        if self.origin is not None:
            return self.origin
        if isinstance(self.raw, type):
            return self.raw
        return type(self.raw) if self.raw is not None else object

    def is_subtype_of(self, other: Type) -> bool:
        """检查是否为指定类型的子类型"""
        try:
            return issubclass(self.python_type, other)
        except TypeError:
            return False

    def to_json_schema_type(self) -> str:
        """转换为 JSON Schema 类型"""
        type_map = {
            int: 'integer',
            float: 'number',
            str: 'string',
            bool: 'boolean',
            list: 'array',
            dict: 'object',
            type(None): 'null',
        }
        return type_map.get(self.python_type, 'string')


class TypeAdapter(ABC, Generic[T]):
    """
    类型适配器抽象基类

    提供类型转换的可插拔接口。用户可以注册自定义适配器
    来处理特殊类型的序列化和反序列化。

    Example:
        class DateTimeAdapter(TypeAdapter[datetime]):
            def to_primitive(self, value: datetime) -> str:
                return value.isoformat()

            def from_primitive(self, value: str) -> datetime:
                return datetime.fromisoformat(value)

        # 注册
        TypeAdapterRegistry.register(datetime, DateTimeAdapter())
    """

    @abstractmethod
    def to_primitive(self, value: T) -> Any:
        """将类型转换为原始值 (JSON 兼容)"""
        pass

    @abstractmethod
    def from_primitive(self, value: Any) -> T:
        """从原始值恢复类型"""
        pass

    def validate(self, value: Any) -> bool:
        """验证值是否为有效类型"""
        return True


class IdentityAdapter(TypeAdapter[T]):
    """恒等适配器 - 不做任何转换"""

    def to_primitive(self, value: T) -> T:
        return value

    def from_primitive(self, value: T) -> T:
        return value


class TypeAdapterRegistry:
    """
    类型适配器注册表

    全局注册自定义类型适配器。
    """
    _adapters: Dict[Type, TypeAdapter] = {}
    _default = IdentityAdapter()

    @classmethod
    def register(cls, type_: Type[T], adapter: TypeAdapter[T]):
        """注册类型适配器"""
        cls._adapters[type_] = adapter

    @classmethod
    def get(cls, type_: Type[T]) -> TypeAdapter[T]:
        """获取类型适配器"""
        return cls._adapters.get(type_, cls._default)

    @classmethod
    def unregister(cls, type_: Type):
        """注销类型适配器"""
        cls._adapters.pop(type_, None)

    @classmethod
    def clear(cls):
        """清空所有注册"""
        cls._adapters.clear()


# 内置类型适配器
class FloatAdapter(TypeAdapter[float]):
    """浮点数适配器 - 处理精度"""

    def __init__(self, precision: int = 6):
        self.precision = precision

    def to_primitive(self, value: float) -> float:
        return round(value, self.precision)

    def from_primitive(self, value: Any) -> float:
        return float(value)


class IntAdapter(TypeAdapter[int]):
    """整数适配器"""

    def to_primitive(self, value: int) -> int:
        return int(value)

    def from_primitive(self, value: Any) -> int:
        return int(value)


class StrAdapter(TypeAdapter[str]):
    """字符串适配器"""

    def __init__(self, encoding: str = 'utf-8'):
        self.encoding = encoding

    def to_primitive(self, value: str) -> str:
        return str(value)

    def from_primitive(self, value: Any) -> str:
        if isinstance(value, bytes):
            return value.decode(self.encoding)
        return str(value)


# 注册内置适配器
TypeAdapterRegistry.register(float, FloatAdapter())
TypeAdapterRegistry.register(int, IntAdapter())
TypeAdapterRegistry.register(str, StrAdapter())


__all__ = [
    'TypeInfo',
    'TypeAdapter',
    'TypeAdapterRegistry',
    'IdentityAdapter',
    'FloatAdapter',
    'IntAdapter',
    'StrAdapter',
]
