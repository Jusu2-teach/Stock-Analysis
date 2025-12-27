"""
PGCS Core: Field
================

声明式字段定义系统。

设计原则:
- 字段定义完全通用，不包含任何业务概念
- 通过 validators 列表实现可组合的验证
- 通过 metadata 字典实现可扩展的元数据
- 使用描述符协议实现属性访问拦截
"""

from __future__ import annotations

from dataclasses import dataclass, field as dataclass_field
from typing import (
    Any, Optional, Dict, List, Type, TypeVar, Generic,
    Callable, Union, TYPE_CHECKING,
)
from copy import deepcopy
import hashlib
import json

from .types import TypeInfo, TypeAdapter, TypeAdapterRegistry

if TYPE_CHECKING:
    from ..validation.base import Validator


T = TypeVar('T')


@dataclass
class FieldDescriptor:
    """
    字段描述符 - 字段的完整元信息

    这是字段的"身份证"，包含所有配置信息。
    与 Field 分离以支持序列化和传递。

    Attributes:
        name: 字段名称
        type_info: 类型信息
        default: 默认值
        default_factory: 默认值工厂函数
        validators: 验证器列表
        metadata: 自定义元数据
        alias: 序列化别名
        description: 字段描述
        deprecated: 是否已废弃
        internal: 是否为内部字段
    """
    name: str = ''
    type_info: Optional[TypeInfo] = None
    default: Any = None
    default_factory: Optional[Callable[[], Any]] = None
    validators: List['Validator'] = dataclass_field(default_factory=list)
    metadata: Dict[str, Any] = dataclass_field(default_factory=dict)
    alias: Optional[str] = None
    description: str = ''
    deprecated: bool = False
    internal: bool = False

    @property
    def has_default(self) -> bool:
        """是否有默认值"""
        return self.default is not None or self.default_factory is not None

    def get_default(self) -> Any:
        """获取默认值"""
        if self.default_factory is not None:
            return self.default_factory()
        return deepcopy(self.default)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'name': self.name,
            'type': str(self.type_info.raw) if self.type_info else None,
            'default': self.default,
            'alias': self.alias,
            'description': self.description,
            'deprecated': self.deprecated,
            'metadata': self.metadata,
        }

    def fingerprint(self) -> str:
        """生成字段指纹"""
        content = json.dumps(self.to_dict(), sort_keys=True, default=str)
        return hashlib.md5(content.encode()).hexdigest()[:8]


class Field:
    """
    PGCS 声明式字段

    核心字段定义类，使用描述符协议实现属性访问拦截。
    完全通用，不包含任何业务概念。

    Features:
    - 类型推断: 自动从类型注解推断
    - 验证组合: validators 列表支持多验证器
    - 元数据扩展: metadata 字典支持任意扩展
    - 序列化支持: alias 和 adapter 支持灵活序列化

    Example:
        from shared.contracts import Field, validators as V

        class User:
            name: str = Field(
                description='用户名',
                validators=[V.required(), V.max_length(100)],
            )
            age: int = Field(
                default=0,
                validators=[V.range_check(0, 150)],
            )
            tags: List[str] = Field(
                default_factory=list,
                metadata={'searchable': True},
            )
    """

    __slots__ = (
        '_descriptor',
        '_name',
        '_owner',
    )

    def __init__(
        self,
        default: Any = None,
        *,
        default_factory: Optional[Callable[[], Any]] = None,
        validators: Optional[List['Validator']] = None,
        metadata: Optional[Dict[str, Any]] = None,
        alias: Optional[str] = None,
        description: str = '',
        deprecated: bool = False,
        internal: bool = False,
        **extra_metadata,
    ):
        """
        初始化字段

        Args:
            default: 默认值
            default_factory: 默认值工厂 (用于可变默认值)
            validators: 验证器列表
            metadata: 自定义元数据字典
            alias: 序列化别名
            description: 字段描述
            deprecated: 是否已废弃
            internal: 是否为内部字段
            **extra_metadata: 额外元数据 (合并到 metadata)
        """
        # 合并元数据
        final_metadata = metadata.copy() if metadata else {}
        final_metadata.update(extra_metadata)

        self._descriptor = FieldDescriptor(
            default=default,
            default_factory=default_factory,
            validators=validators or [],
            metadata=final_metadata,
            alias=alias,
            description=description,
            deprecated=deprecated,
            internal=internal,
        )
        self._name: Optional[str] = None
        self._owner: Optional[Type] = None

    def __set_name__(self, owner: Type, name: str):
        """描述符协议 - 绑定到类"""
        self._name = name
        self._owner = owner
        self._descriptor.name = name

        # 尝试获取类型注解
        try:
            hints = getattr(owner, '__annotations__', {})
            if name in hints:
                self._descriptor.type_info = TypeInfo.from_annotation(hints[name])
        except Exception:
            pass

    def __get__(self, obj: Any, objtype: Optional[Type] = None) -> Any:
        """描述符协议 - 获取值"""
        if obj is None:
            return self

        attr_name = f'_field_{self._name}'
        if hasattr(obj, attr_name):
            return getattr(obj, attr_name)

        return self._descriptor.get_default()

    def __set__(self, obj: Any, value: Any):
        """描述符协议 - 设置值"""
        # 运行验证器
        for validator in self._descriptor.validators:
            result = validator.validate(value, self._descriptor)
            if not result.is_valid:
                raise ValueError(
                    f"Field '{self._name}' validation failed: {result.message}"
                )

        setattr(obj, f'_field_{self._name}', value)

    @property
    def descriptor(self) -> FieldDescriptor:
        """获取字段描述符"""
        return self._descriptor

    @property
    def name(self) -> Optional[str]:
        """获取字段名"""
        return self._name

    @property
    def metadata(self) -> Dict[str, Any]:
        """获取元数据"""
        return self._descriptor.metadata

    def add_validator(self, validator: 'Validator') -> 'Field':
        """添加验证器 (链式调用)"""
        self._descriptor.validators.append(validator)
        return self

    def with_metadata(self, **kwargs) -> 'Field':
        """添加元数据 (链式调用)"""
        self._descriptor.metadata.update(kwargs)
        return self

    def validate(self, value: Any) -> tuple[bool, Optional[str]]:
        """
        验证值

        Returns:
            (is_valid, error_message)
        """
        from ..validation.base import ValidationContext

        ctx = ValidationContext(field=self._descriptor)

        for validator in self._descriptor.validators:
            result = validator.validate(value, self._descriptor, ctx)
            if not result.is_valid:
                return False, result.message

        return True, None

    def serialize(self, value: Any) -> Any:
        """序列化值"""
        if self._descriptor.type_info:
            adapter = TypeAdapterRegistry.get(self._descriptor.type_info.python_type)
            return adapter.to_primitive(value)
        return value

    def deserialize(self, value: Any) -> Any:
        """反序列化值"""
        if self._descriptor.type_info:
            adapter = TypeAdapterRegistry.get(self._descriptor.type_info.python_type)
            return adapter.from_primitive(value)
        return value


def field(
    default: Any = None,
    **kwargs,
) -> Field:
    """
    便捷函数: 创建字段

    Example:
        class User:
            name: str = field(description='用户名')
    """
    return Field(default, **kwargs)


__all__ = [
    'Field',
    'FieldDescriptor',
    'field',
]
