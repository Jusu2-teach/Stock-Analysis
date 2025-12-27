"""
PGCS (Portable Generic Contract System)
========================================

一个独立、通用、可复用的数据契约框架。

设计理念:
- 零业务耦合: 不包含任何领域特定概念
- 可插拔架构: 类型、验证器、序列化器均可扩展
- 声明式定义: 基于描述符和装饰器的优雅 API
- 类型安全: 运行时验证 + 静态类型提示

核心组件:
- Field: 声明式字段定义
- Schema: 契约 Schema 基类
- Validator: 可组合的验证器
- Serializer: 可插拔的序列化后端
- Registry: 全局契约注册中心
- Router: 通用路由解析器

灵感来源:
- Pydantic: 声明式验证
- marshmallow: Schema 设计
- Apache Arrow: 元数据系统
- Kedro: Transcoding 模式
- attrs: 简洁的类定义

Example:
    from shared.contracts import Field, Schema, required, max_length, range_check, pattern

    @Schema.define(name='user', version='1.0')
    class UserSchema:
        name: str = Field(validators=[required(), max_length(100)])
        age: int = Field(validators=[range_check(0, 150)])
        email: str = Field(validators=[pattern(r'^[\\w.-]+@[\\w.-]+$')])
"""

__version__ = '2.0.0'

# Core - 核心抽象
from .core.field import Field, FieldDescriptor
from .core.schema import Schema, SchemaMeta
from .core.types import TypeInfo, TypeAdapter

# Validation - 验证系统
from .validation.base import Validator, ValidationResult, ValidationContext
from .validation.validators import (
    required,
    optional,
    range_check,
    min_value,
    max_value,
    pattern,
    max_length,
    min_length,
    choices,
    custom,
)

# Serialization - 序列化系统
from .serialization.base import Serializer, SerializationContext
from .serialization.backends import JSONSerializer, DictSerializer

# Registry - 注册中心
from .registry.schema_registry import SchemaRegistry, get_registry, CompatibilityMode

# Router - 路由系统
from .router.base import Router, Route, RoutePattern
from .router.parser import RouteParser, DelimiterParser, TemplateParser

# Metadata - 元数据
from .metadata.base import Metadata, MetadataStore
from .metadata.lineage import Lineage, LineageNode

# Store - 数据存储
from .store import DataStore, SingletonDataStore, DataEntry, ReferenceResolver, ReferenceNotFoundError

# Utilities
from .utils.fingerprint import fingerprint
from .utils.compat import ensure_compatibility

__all__ = [
    # Core
    'Field',
    'FieldDescriptor',
    'Schema',
    'SchemaMeta',
    'TypeInfo',
    'TypeAdapter',

    # Validation
    'Validator',
    'ValidationResult',
    'ValidationContext',
    'required',
    'optional',
    'range_check',
    'min_value',
    'max_value',
    'pattern',
    'max_length',
    'min_length',
    'choices',
    'custom',

    # Serialization
    'Serializer',
    'SerializationContext',
    'JSONSerializer',
    'DictSerializer',

    # Registry
    'SchemaRegistry',
    'get_registry',
    'CompatibilityMode',

    # Router
    'Router',
    'Route',
    'RoutePattern',
    'RouteParser',
    'DelimiterParser',
    'TemplateParser',

    # Metadata
    'Metadata',
    'MetadataStore',
    'Lineage',
    'LineageNode',

    # Store
    'DataStore',
    'SingletonDataStore',
    'DataEntry',
    'ReferenceResolver',
    'ReferenceNotFoundError',

    # Utils
    'fingerprint',
    'ensure_compatibility',
]
