# PGCS (Portable Generic Contract System)

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Type Hints](https://img.shields.io/badge/type%20hints-yes-brightgreen.svg)](https://www.python.org/dev/peps/pep-0484/)

**一个独立、通用、可复用的数据契约框架。**

PGCS 是一个零业务耦合的数据契约系统，提供声明式 Schema 定义、可组合验证器、可插拔序列化后端等能力。设计灵感来自 Pydantic、marshmallow、Apache Arrow、Kedro 和 attrs。

---

## 📚 目录

- [核心设计理念](#-核心设计理念)
- [快速开始](#-快速开始)
- [模块架构](#-模块架构)
- [API 参考](#-api-参考)
- [高级用法](#-高级用法)
- [扩展指南](#-扩展指南)
- [设计决策与权衡](#-设计决策与权衡)
- [已知限制](#-已知限制)
- [版本历史](#-版本历史)

---

## 🎯 核心设计理念

### 1. 零业务耦合

框架代码 **完全不包含任何业务概念**。所有领域特定的定义（如字段类型、枚举、工厂函数）都由用户在业务层实现。

```python
# ❌ 错误: 框架层不应该有这些
class GeneTarget(Enum):  # 业务概念
    ALPHA = auto()

def alpha_field(**kw) -> Field:  # 业务工厂
    return Field(metadata={'gene': 'alpha'}, **kw)

# ✅ 正确: 用户在业务层定义
# 框架只提供通用的 Field、Schema、metadata 机制
```

### 2. 可插拔架构

- **TypeAdapter**: 自定义类型的序列化/反序列化
- **Validator**: 可组合的验证逻辑 (`&`, `|`, `~` 操作符)
- **Serializer**: 可替换的序列化后端 (Dict, JSON, 可扩展)
- **Router**: 参数化路由模式

### 3. 声明式 API

```python
class UserSchema(Schema):
    name: str = Field(
        validators=[required(), max_length(100)],
        description='用户名',
    )
    age: int = Field(
        default=0,
        validators=[range_check(0, 150)],
    )
```

### 4. 类型安全

- 完整的 `typing` 类型提示
- 运行时类型验证
- 支持泛型和 Union 类型

---

## 🚀 快速开始

### 安装

PGCS 是一个纯 Python 模块，无外部依赖。

```bash
# 项目内部使用
from shared.contracts import Field, Schema, required, range_check
```

### 基本示例

```python
from shared.contracts import (
    Field, Schema,
    required, range_check, max_length, pattern
)

# 定义 Schema
class UserSchema(Schema):
    name: str = Field(
        validators=[required(), max_length(100)],
        description='用户名',
    )
    email: str = Field(
        validators=[pattern(r'^[\w.-]+@[\w.-]+\.\w+$', message='Invalid email')],
    )
    age: int = Field(
        default=0,
        validators=[range_check(0, 150)],
    )

# 创建实例
user = UserSchema(name='Alice', email='alice@example.com', age=25)

# 序列化
data = user.to_dict()
# {'name': 'Alice', 'email': 'alice@example.com', 'age': 25}

# 验证
is_valid, errors = UserSchema.validate_data({'name': '', 'age': 200})
# (False, ['name: This field is required', 'age: Value 200 exceeds maximum 150'])

# JSON Schema 导出
json_schema = UserSchema.to_json_schema()
```

---

## 📦 模块架构

```
shared/contracts/
├── __init__.py              # 公共 API 导出
├── README.md                # 本文档
│
├── core/                    # 核心抽象
│   ├── types.py             # TypeInfo, TypeAdapter, TypeAdapterRegistry
│   ├── field.py             # Field, FieldDescriptor
│   └── schema.py            # Schema, SchemaMeta, SchemaInfo
│
├── validation/              # 验证系统
│   ├── base.py              # Validator, ValidationResult, ValidationContext
│   └── validators.py        # 内置验证器 (required, range_check, pattern, ...)
│
├── serialization/           # 序列化系统
│   ├── base.py              # Serializer, SerializationContext
│   └── backends.py          # DictSerializer, JSONSerializer
│
├── registry/                # 注册中心
│   └── schema_registry.py   # SchemaRegistry, CompatibilityMode
│
├── router/                  # 路由系统
│   ├── base.py              # Router, Route, RoutePattern
│   └── parser.py            # RouteParser, DelimiterParser, TemplateParser
│
├── metadata/                # 元数据系统
│   ├── base.py              # Metadata, MetadataStore
│   └── lineage.py           # Lineage, LineageNode
│
├── store/                   # 数据存储与引用解析
│   ├── data_entry.py        # DataEntry: key/value/ref/fingerprint/metadata
│   ├── data_store.py        # DataStore: 统一数据存储 + ref/hash 索引
│   └── reference.py         # ReferenceResolver: 引用解析与路由集成
│
└── utils/                   # 工具函数
    ├── fingerprint.py       # fingerprint(), content_hash()
    └── compat.py            # ensure_compatibility(), CompatibilityReport
```

> 与 Pipeline 的集成：`pipeline.core.context.PipelineContext` 通过
> `DataStore` 和 `ReferenceResolver` 将字符串引用
> `steps.{step}.outputs.parameters.{param}` 映射到实际数据对象，
> 实现 **单一真相源 + 可追踪的引用/血缘**。

---

## 📖 API 参考

### Core

#### `Field`

声明式字段定义，使用 Python 描述符协议。

```python
Field(
    default=None,              # 默认值
    *,
    default_factory=None,      # 默认值工厂 (用于可变类型)
    validators=None,           # 验证器列表
    metadata=None,             # 自定义元数据字典
    alias=None,                # 序列化别名
    description='',            # 字段描述
    deprecated=False,          # 是否已废弃
    internal=False,            # 是否为内部字段
    **extra_metadata,          # 额外元数据 (合并到 metadata)
)
```

**关键特性:**
- `metadata` 字典支持任意用户扩展
- `**extra_metadata` 允许简写: `Field(gene='alpha')` 等价于 `Field(metadata={'gene': 'alpha'})`
- 验证器在 `__set__` 时自动执行

#### `Schema`

契约 Schema 基类，使用元类自动收集字段。

```python
class MySchema(Schema):
    field1: str = Field(...)
    field2: int = Field(...)

# 或使用装饰器
@Schema.define(name='my_schema', version='1.0.0')
class MySchema:
    field1: str = Field(...)
```

**类方法:**
- `fields()` - 获取所有字段
- `field_names()` - 获取字段名列表
- `get_field(name)` - 获取指定字段
- `field_descriptors()` - 获取所有字段描述符
- `fields_with_metadata(key, value)` - 按元数据筛选字段
- `validate_data(data)` - 验证数据字典
- `from_dict(data)` - 从字典创建实例
- `to_json_schema()` - 生成 JSON Schema
- `fingerprint()` - 生成 Schema 指纹

**实例方法:**
- `to_dict(include_none=False)` - 转换为字典
- `serialize()` - 序列化 (应用 alias 和 adapter)

#### `TypeInfo`

类型元信息，支持泛型和 Union 类型。

```python
info = TypeInfo.from_annotation(Optional[List[int]])
info.origin        # list
info.args          # (int,)
info.is_optional   # True
info.is_generic    # True
info.python_type   # list
info.to_json_schema_type()  # 'array'
```

### Validation

#### 验证器组合

```python
# AND 组合: 两个都必须通过
validator = required() & range_check(0, 100)

# OR 组合: 任一通过即可
validator = pattern(r'^\d+$') | pattern(r'^0x[a-f0-9]+$')

# NOT 取反
validator = ~pattern(r'admin')  # 不能包含 'admin'
```

#### 内置验证器

| 验证器 | 说明 | 示例 |
|--------|------|------|
| `required()` | 必填 | `required(message='必填字段')` |
| `optional(inner)` | 可选包装 | `optional(range_check(0, 100))` |
| `range_check(min, max)` | 范围检查 | `range_check(0, 100, inclusive=True)` |
| `min_value(v)` | 最小值 | `min_value(0)` |
| `max_value(v)` | 最大值 | `max_value(100)` |
| `min_length(n)` | 最小长度 | `min_length(1)` |
| `max_length(n)` | 最大长度 | `max_length(255)` |
| `length(min, max)` | 长度范围 | `length(1, 100)` |
| `pattern(regex)` | 正则匹配 | `pattern(r'^\d{4}$')` |
| `choices(list)` | 枚举选项 | `choices(['A', 'B', 'C'])` |
| `type_check(type)` | 类型检查 | `type_check(str, strict=True)` |
| `custom(func)` | 自定义函数 | `custom(lambda x: x > 0)` |

#### 预置验证器

```python
from shared.contracts.validation.validators import email, url, uuid

# 直接使用
field = Field(validators=[email])
```

### Serialization

#### `SerializationContext`

```python
ctx = SerializationContext(
    include_none=False,      # 是否包含 None 值
    use_alias=True,          # 是否使用字段别名
    precision=6,             # 浮点数精度
    date_format='%Y-%m-%d',  # 日期格式
    encoding='utf-8',        # 编码
)
```

#### 序列化器

```python
from shared.contracts import DictSerializer, JSONSerializer

# 字典序列化
serializer = DictSerializer()
data = serializer.serialize(user, ctx)

# JSON 序列化
json_serializer = JSONSerializer(indent=2)
json_str = json_serializer.serialize(user)
```

### Registry

#### `SchemaRegistry`

全局 Schema 注册中心，支持版本管理和兼容性检查。

```python
from shared.contracts import get_registry, CompatibilityMode

registry = get_registry()

# 注册
registry.register(UserSchema, tags=['user', 'core'])

# 查找
schema = registry.get('UserSchema')
schemas = registry.get_by_tag('user')

# 兼容性检查
is_compat, issues = registry.check_compatibility(
    NewUserSchema,
    mode=CompatibilityMode.BACKWARD
)

# 观察者模式
registry.add_observer(lambda name, schema: print(f'Registered: {name}'))
```

#### 兼容性模式

| 模式 | 说明 |
|------|------|
| `NONE` | 不检查 |
| `BACKWARD` | 向后兼容 (新版本可读旧数据) |
| `FORWARD` | 向前兼容 (旧版本可读新数据) |
| `FULL` | 完全兼容 (双向) |
| `STRICT` | 严格模式 (指纹必须匹配) |

### Router

#### 参数化路由

```python
from shared.contracts import Router

router = Router()

# 注册模式 ({param} 语法)
router.add_pattern('{source}_{field}@{target}', name='field_route')
router.add_pattern('{source}@{target}', name='simple_route')

# 匹配
route = router.match('probe_slope@gene')
route.params  # {'source': 'probe', 'field': 'slope', 'target': 'gene'}
route.pattern # 'field_route'

# 构建
path = router.build('{source}_{field}@{target}', source='probe', field='slope', target='gene')
# 'probe_slope@gene'
```

#### 解析器

```python
from shared.contracts import DelimiterParser, TemplateParser

# 分隔符解析
parser = DelimiterParser(
    separators=['_', '@'],
    segment_names=['source', 'field', 'target']
)
result = parser.parse('probe_slope@gene')
result.params  # {'source': 'probe', 'field': 'slope', 'target': 'gene'}

# 模板解析
parser = TemplateParser('{source}_{field}@{target}')
result = parser.parse('probe_slope@gene')
```

### Metadata

#### `Lineage` (数据血缘)

```python
from shared.contracts import Lineage

lineage = Lineage()

# 添加节点
lineage.add_node('raw', '原始数据', source='tushare')
lineage.add_node('cleaned', '清洗数据')
lineage.add_node('features', '特征数据')

# 连接
lineage.connect('raw', 'cleaned')
lineage.connect('cleaned', 'features')

# 查询
upstream = lineage.get_upstream('features', recursive=True)
path = lineage.get_path('raw', 'features')  # ['raw', 'cleaned', 'features']

# 可视化
print(lineage.visualize_ascii())
```

---

## 🔧 高级用法

### 1. 自定义类型适配器

```python
from datetime import datetime
from shared.contracts import TypeAdapter, TypeAdapterRegistry

class DateTimeAdapter(TypeAdapter[datetime]):
    def to_primitive(self, value: datetime) -> str:
        return value.isoformat()

    def from_primitive(self, value: str) -> datetime:
        return datetime.fromisoformat(value)

# 注册
TypeAdapterRegistry.register(datetime, DateTimeAdapter())
```

### 2. 自定义验证器

```python
from shared.contracts import Validator, ValidationResult

class UniqueValidator(Validator):
    def __init__(self, existing: set):
        self.existing = existing

    def validate(self, value, field=None, context=None) -> ValidationResult:
        if value in self.existing:
            return ValidationResult.error(f"Value '{value}' already exists")
        return ValidationResult.ok()

# 使用
unique_names = UniqueValidator({'Alice', 'Bob'})
field = Field(validators=[unique_names])
```

### 3. 业务层字段工厂

```python
# 在业务代码中 (不是框架代码!)
from enum import Enum, auto
from shared.contracts import Field, range_check

class GeneTarget(Enum):
    ALPHA = auto()
    BETA = auto()

def gene_field(target: GeneTarget, weight: float = 1.0, **kw) -> Field:
    """业务层的字段工厂"""
    return Field(
        validators=[range_check(0, 1)],
        metadata={
            'gene_target': target.name,
            'weight': weight,
        },
        **kw
    )

# 使用
class GeneSchema(Schema):
    alpha = gene_field(GeneTarget.ALPHA, weight=0.3)
    beta = gene_field(GeneTarget.BETA, weight=0.7)
```

### 4. 按元数据筛选字段

```python
# 获取所有 gene 字段
gene_fields = GeneSchema.fields_with_metadata('gene_target')

# 获取特定 gene 类型的字段
alpha_fields = GeneSchema.fields_with_metadata('gene_target', 'ALPHA')
```

---

## 🧩 扩展指南

### 添加新的序列化后端

```python
from shared.contracts import Serializer, SerializerRegistry

class XMLSerializer(Serializer[str]):
    def serialize(self, data, context=None) -> str:
        # 实现 XML 序列化
        ...

    def deserialize(self, data, schema_cls, context=None):
        # 实现 XML 反序列化
        ...

# 注册
SerializerRegistry.register('xml', XMLSerializer())
```

### 添加新的验证器

```python
# 在 validation/validators.py 中添加

class IPAddressValidator(Validator):
    def validate(self, value, field=None, context=None) -> ValidationResult:
        import ipaddress
        try:
            ipaddress.ip_address(value)
            return ValidationResult.ok()
        except ValueError:
            return ValidationResult.error("Invalid IP address")

def ip_address() -> Validator:
    return IPAddressValidator()
```

---

## ⚖️ 设计决策与权衡

### 1. 描述符 vs dataclass

**选择:** 使用 Python 描述符协议

**原因:**
- 更灵活的属性访问拦截
- 支持验证器在赋值时执行
- 不需要 `@dataclass` 装饰器的限制

**权衡:**
- 实现复杂度略高
- 需要手动管理 `_field_xxx` 属性存储

### 2. 元类 vs 装饰器

**选择:** 同时支持 (元类 + `@Schema.define` 装饰器)

**原因:**
- 元类自动收集字段，无需显式注册
- 装饰器提供更好的 IDE 支持和可读性

### 3. 验证器组合

**选择:** 操作符重载 (`&`, `|`, `~`)

**原因:**
- 类似 SQLAlchemy 的表达式风格
- 代码简洁直观

**权衡:**
- 需要熟悉操作符语义
- 调试时堆栈较深

### 4. 全局注册 vs 实例注册

**选择:** 单例 + 线程安全

**原因:**
- Schema 通常是全局定义的
- 避免重复注册和不一致

**权衡:**
- 测试时需要 `reset()` 清理状态

---

## ⚠️ 已知限制

### 1. Field 类型注解问题

**问题:** `Field(str, ...)` 第一个参数是 `default`，不是类型

**正确用法:**
```python
# ✅ 类型来自注解
name: str = Field(description='Name')

# ❌ 错误: 第一个参数是 default，不是类型
name = Field(str, description='Name')  # str 会被当作默认值!
```

### 2. fingerprint() JSON 序列化

**问题:** Schema 中有不可序列化的默认值时 `fingerprint()` 会失败

**解决:** 确保默认值是 JSON 可序列化的，或使用 `default_factory`

### 3. 循环导入

**问题:** `TYPE_CHECKING` 下的导入需要小心

**解决:** 已使用 `if TYPE_CHECKING:` 延迟导入

### 4. Lineage 节点访问

**问题:** `lineage.nodes` 应该是公开属性而不是 `_nodes`

**状态:** 可通过 `lineage.get_node(id)` 访问单个节点

### 5. SchemaRegistry.get() 返回类型

**问题:** 返回 `RegisteredSchema` 而不是 `dict`

**使用:**
```python
reg = registry.get('MySchema')
if reg:
    schema_cls = reg.schema_cls
    version = reg.latest_version
```

---

## 📋 版本历史

### v2.0.0 (当前)

**重大变更:**
- 完全重构为零业务耦合架构
- 移除所有 hardcoded 业务概念 (`GeneTarget`, `alpha_field` 等)
- 新增模块化包结构

**新功能:**
- 可组合验证器 (`&`, `|`, `~`)
- 参数化路由系统
- 数据血缘追踪
- Schema 兼容性检查
- 序列化上下文

### v1.0.0

- 初始版本 (已废弃)

---

## 📄 License

MIT License

---

## 🤝 贡献

欢迎提交 Issue 和 PR。请确保:

1. 遵循零业务耦合原则
2. 添加类型提示
3. 编写测试用例
4. 更新文档
