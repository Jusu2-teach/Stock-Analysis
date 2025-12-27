"""
PGCS Core: Schema
=================

契约 Schema 定义系统。

设计原则:
- Schema 是字段的容器，提供批量操作能力
- 使用装饰器或元类自动收集字段
- 支持继承和组合
- 完全通用，不包含业务概念
"""

from __future__ import annotations

from dataclasses import dataclass, field as dataclass_field
from typing import (
    Any, Optional, Dict, List, Type, TypeVar, Iterator,
    Callable, ClassVar, get_type_hints,
)
from abc import ABCMeta
import hashlib
import json
from datetime import datetime

from .field import Field, FieldDescriptor
from .types import TypeInfo


T = TypeVar('T', bound='Schema')


@dataclass
class SchemaInfo:
    """
    Schema 元信息

    Attributes:
        name: Schema 名称
        version: 版本号
        description: 描述
        author: 作者
        tags: 标签列表
        deprecated: 是否已废弃
        created_at: 创建时间
    """
    name: str = ''
    version: str = '1.0.0'
    description: str = ''
    author: str = ''
    tags: List[str] = dataclass_field(default_factory=list)
    deprecated: bool = False
    created_at: str = dataclass_field(default_factory=lambda: datetime.now().isoformat())


class SchemaMeta(ABCMeta):
    """
    Schema 元类

    自动收集类中定义的 Field 实例，构建字段注册表。
    """

    def __new__(
        mcs,
        name: str,
        bases: tuple,
        namespace: dict,
        **kwargs,
    ):
        cls = super().__new__(mcs, name, bases, namespace)

        # 收集字段
        fields: Dict[str, Field] = {}

        # 从基类继承字段
        for base in bases:
            if hasattr(base, '__fields__'):
                fields.update(base.__fields__)

        # 收集当前类定义的字段
        for attr_name, attr_value in namespace.items():
            if isinstance(attr_value, Field):
                fields[attr_name] = attr_value

        cls.__fields__ = fields

        # 初始化 schema info
        if not hasattr(cls, '__schema_info__'):
            cls.__schema_info__ = SchemaInfo(name=name)

        return cls


class Schema(metaclass=SchemaMeta):
    """
    PGCS 契约 Schema 基类

    所有用户定义的 Schema 应继承此类。提供:
    - 自动字段收集
    - 批量验证
    - 序列化/反序列化
    - JSON Schema 生成
    - 指纹签名

    Example:
        from shared.contracts import Schema, Field, validators as V

        class UserSchema(Schema):
            class Meta:
                name = 'user'
                version = '1.0.0'

            name: str = Field(validators=[V.required()])
            age: int = Field(default=0)

        # 或使用装饰器
        @Schema.define(name='user', version='1.0.0')
        class UserSchema:
            name: str = Field(validators=[V.required()])
            age: int = Field(default=0)
    """

    __fields__: ClassVar[Dict[str, Field]] = {}
    __schema_info__: ClassVar[SchemaInfo]

    def __init__(self, **kwargs):
        """
        初始化 Schema 实例

        Args:
            **kwargs: 字段值
        """
        # 设置字段值
        for name, field in self.__fields__.items():
            value = kwargs.get(name, field.descriptor.get_default())
            setattr(self, name, value)

    @classmethod
    def define(
        cls,
        name: str = '',
        version: str = '1.0.0',
        description: str = '',
        author: str = '',
        tags: Optional[List[str]] = None,
        deprecated: bool = False,
    ) -> Callable[[Type], Type[T]]:
        """
        Schema 定义装饰器

        Example:
            @Schema.define(name='user', version='1.0.0')
            class UserSchema:
                name: str = Field()
        """
        def decorator(target_cls: Type) -> Type[T]:
            # 创建新类，继承 Schema
            namespace = dict(target_cls.__dict__)
            namespace.pop('__dict__', None)
            namespace.pop('__weakref__', None)

            new_cls = SchemaMeta(
                target_cls.__name__,
                (Schema,),
                namespace,
            )

            # 设置 schema info
            new_cls.__schema_info__ = SchemaInfo(
                name=name or target_cls.__name__,
                version=version,
                description=description or target_cls.__doc__ or '',
                author=author,
                tags=tags or [],
                deprecated=deprecated,
            )

            return new_cls

        return decorator

    @classmethod
    def fields(cls) -> Dict[str, Field]:
        """获取所有字段"""
        return cls.__fields__.copy()

    @classmethod
    def field_names(cls) -> List[str]:
        """获取所有字段名"""
        return list(cls.__fields__.keys())

    @classmethod
    def get_field(cls, name: str) -> Optional[Field]:
        """获取指定字段"""
        return cls.__fields__.get(name)

    @classmethod
    def field_descriptors(cls) -> Dict[str, FieldDescriptor]:
        """获取所有字段描述符"""
        return {
            name: field.descriptor
            for name, field in cls.__fields__.items()
        }

    @classmethod
    def fields_with_metadata(cls, key: str, value: Any = None) -> Dict[str, Field]:
        """
        按元数据筛选字段

        Args:
            key: 元数据键
            value: 元数据值 (None 表示只检查键存在)
        """
        result = {}
        for name, field in cls.__fields__.items():
            if key in field.metadata:
                if value is None or field.metadata[key] == value:
                    result[name] = field
        return result

    @classmethod
    def validate_data(cls, data: Dict[str, Any]) -> tuple[bool, List[str]]:
        """
        验证数据字典

        Returns:
            (is_valid, error_messages)
        """
        errors = []

        for name, field in cls.__fields__.items():
            if name in data:
                is_valid, message = field.validate(data[name])
                if not is_valid:
                    errors.append(f"{name}: {message}")

        return len(errors) == 0, errors

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        """从字典创建实例"""
        return cls(**data)

    def to_dict(self, include_none: bool = False) -> Dict[str, Any]:
        """
        转换为字典

        Args:
            include_none: 是否包含 None 值
        """
        result = {}
        for name in self.__fields__:
            value = getattr(self, name, None)
            if value is not None or include_none:
                result[name] = value
        return result

    def serialize(self) -> Dict[str, Any]:
        """序列化 (应用字段的 alias 和 adapter)"""
        result = {}
        for name, field in self.__fields__.items():
            value = getattr(self, name, None)
            key = field.descriptor.alias or name
            result[key] = field.serialize(value)
        return result

    @classmethod
    def deserialize(cls: Type[T], data: Dict[str, Any]) -> T:
        """反序列化"""
        kwargs = {}

        # 构建 alias -> name 映射
        alias_map = {
            (f.descriptor.alias or name): name
            for name, f in cls.__fields__.items()
        }

        for key, value in data.items():
            name = alias_map.get(key, key)
            if name in cls.__fields__:
                kwargs[name] = cls.__fields__[name].deserialize(value)

        return cls(**kwargs)

    @classmethod
    def to_json_schema(cls) -> Dict[str, Any]:
        """生成 JSON Schema"""
        properties = {}
        required = []

        for name, field in cls.__fields__.items():
            desc = field.descriptor

            prop: Dict[str, Any] = {
                'description': desc.description,
            }

            # 类型
            if desc.type_info:
                prop['type'] = desc.type_info.to_json_schema_type()

            # 默认值
            if desc.has_default:
                prop['default'] = desc.default

            properties[name] = prop

            # 检查是否必填 (无默认值且不是 optional)
            if not desc.has_default:
                if desc.type_info and not desc.type_info.is_optional:
                    required.append(name)

        return {
            '$schema': 'https://json-schema.org/draft/2020-12/schema',
            'title': cls.__schema_info__.name,
            'description': cls.__schema_info__.description,
            'type': 'object',
            'properties': properties,
            'required': required,
        }

    @classmethod
    def fingerprint(cls) -> str:
        """生成 Schema 指纹"""
        schema = cls.to_json_schema()
        content = json.dumps(schema, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()[:12]

    def __iter__(self) -> Iterator[tuple[str, Any]]:
        """迭代字段"""
        for name in self.__fields__:
            yield name, getattr(self, name, None)

    def __repr__(self) -> str:
        fields_str = ', '.join(
            f"{name}={getattr(self, name, None)!r}"
            for name in self.__fields__
        )
        return f"{self.__class__.__name__}({fields_str})"


__all__ = [
    'Schema',
    'SchemaMeta',
    'SchemaInfo',
]
