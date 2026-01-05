# PGCS (Portable Generic Contract System) 架构

## 状态分析

### 当前现状

| 组件 | 位置 | 状态 |
|------|------|------|
| **EventBus** | `shared/event_bus/` | ✅ **已集成** - pipeline、plugins、truth_engine 广泛使用 |
| **Contracts (PGCS)** | `shared/contracts/` | ✅ **已集成** - DataStore、ReferenceResolver 已用于 pipeline |
| **Store** | `shared/contracts/store/` | ✅ **新增** - 统一数据存储组件 |

### 集成完成情况

| 组件 | 状态 | 说明 |
|------|------|------|
| `DataStore` | ✅ 已完成 | 统一数据存储，三种索引（key/ref/hash） |
| `DataEntry` | ✅ 已完成 | 数据条目，含元数据和血缘 |
| `ReferenceResolver` | ✅ 已完成 | 引用解析器，使用 Router |
| `Router` 集成 | ✅ 已完成 | 替换硬编码正则 |
| `PipelineContext` 集成 | ✅ 已完成 | 使用 DataStore |
| `KedroEngine` 集成 | ✅ 已完成 | 使用 DataStore + EventPublisher |

### EventBus 使用情况（参考模式）

```
pipeline/engines/kedro_engine.py    → EventPublisher 封装事件发布
pipeline/core/execute_manager.py    → EventBus.get(), emit PipelineStartedEvent/PipelineCompletedEvent
pipeline/plugins/*.py               → @bus.on() 订阅各类事件
src/astock/business_engines/truth/  → EventBus.get(), emit DataLoadedEvent/DataTransformedEvent
```

**关键模式**: shared/ 提供通用框架 + 预定义事件类型，组件直接 import 使用。

### PGCS 核心组件

```
shared/contracts/
├── core/           # Field, Schema, TypeInfo - 声明式定义
├── validation/     # Validator, required(), range_check() - 验证器
├── serialization/  # JSONSerializer, DictSerializer - 序列化
├── registry/       # SchemaRegistry - Schema 注册中心
├── router/         # Router, RouteParser - 路由解析
├── metadata/       # Lineage, LineageNode - 数据血缘
├── utils/          # fingerprint, compat - 工具函数
└── store/          # ✅ 新增：DataStore, DataEntry, ReferenceResolver
```

---

## 集成目标

将 PGCS 引入系统实现：

1. ✅ **Schema 定义**: 用 `Field` + `Schema` 声明式定义数据契约
2. ✅ **跨步骤引用**: 用 `ReferenceResolver`（内部基于 Router）替换硬编码正则
3. **数据验证**: 用 `Validator` 在步骤间传递时验证数据 (待完成)
4. ✅ **血缘追踪**: 用 `Lineage` 记录数据流转路径 (DataEntry 已支持)

---

## 集成方案

### 方案一：轻量集成（已完成）

**目标**: 通过 `DataStore` + `ReferenceResolver` 统一管理跨步骤引用，彻底移除 `REF_PATTERN` + 手写正则解析。

当前实现要点：

```python
# pipeline/core/context.py
from shared.contracts.store import DataStore, ReferenceResolver

@dataclass
class PipelineContext:
    config: Dict[str, Any] = field(default_factory=dict)
    steps: Dict[str, StepSpec] = field(default_factory=dict)
    execution_order: List[str] = field(default_factory=list)
    _data_store: DataStore | None = field(default=None, repr=False)
    _resolver: ReferenceResolver | None = field(default=None, repr=False)

    def __post_init__(self):
        self._data_store = DataStore()
        self._resolver = ReferenceResolver(self._data_store)
        self._resolver.register_pattern(
            template='steps.{step}.outputs.parameters.{param}',
            handler='step_output',
        )

    def register_reference(self, ref: str, value: Any) -> str:
        """将步骤输出注册到 DataStore，并绑定 ref（steps.X.outputs.parameters.Y）"""
        parsed = self.resolver.parse_ref(ref) or {}
        step_id = parsed.get('step')
        param_id = parsed.get('param')
        key = self.dataset_name(step_id, param_id)
        entry = self.data_store.put(key, value, ref=ref, producer_step=step_id)
        return entry.fingerprint

    def resolve_references(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """递归解析参数中的 {"__ref__": "steps.X.outputs.parameters.Y"} 结构"""
        return self.resolver.resolve_params(params)
```

**收益**:
- 跨步骤数据只存放在 `DataStore` 一处（单一真相源）
- 引用路径统一使用 `steps.{step}.outputs.parameters.{param}` 模板
- 通过指纹(hash) 支持缓存与血缘追踪

---

### 方案二：Schema 验证（进阶）

**目标**: 步骤输出的类型安全验证

在 Business Engine 中定义 Schema (业务代码，非 shared):
```python
# src/astock/business_engines/trend/schemas.py
from shared.contracts import Schema, Field, required, range_check

@Schema.define(name='trend_analysis', version='1.0')
class TrendAnalysisSchema:
    """趋势分析结果 Schema"""
    ts_code: str = Field(validators=[required()])
    slope: float = Field(validators=[range_check(-1.0, 1.0)])
    r_squared: float = Field(validators=[range_check(0.0, 1.0)])
    trend_label: str = Field(validators=[required()])
```

在 Pipeline 执行时验证:
```python
# pipeline/engines/kedro_engine.py
from shared.contracts import get_registry

def _store_output(self, step_name: str, output_name: str, data: Any):
    # 查找是否有对应 Schema
    registry = get_registry()
    schema = registry.get(f"{step_name}.{output_name}")

    if schema:
        # 验证数据符合 Schema
        result = schema.validate(data)
        if not result.valid:
            raise ValueError(f"Output {output_name} failed validation: {result.errors}")

    self.global_catalog[f"{step_name}.{output_name}"] = data
```

---

### 方案三：数据血缘追踪（完整）

**目标**: 记录数据从源头到最终输出的完整路径

```python
# pipeline/engines/kedro_engine.py
from shared.contracts import Lineage, LineageNode

class KedroEngine:
    def __init__(self):
        self._lineage = Lineage()

    def _execute_step(self, step: Dict) -> Any:
        step_name = step['name']

        # 创建血缘节点
        node = LineageNode(
            id=step_name,
            inputs=[ref for ref in self._get_input_refs(step)],
            outputs=[f"{step_name}.{out}" for out in step.get('outputs', {}).get('parameters', [])],
            metadata={'timestamp': datetime.now().isoformat()}
        )
        self._lineage.add_node(node)

        result = self._run_step_logic(step)
        return result

    def get_lineage_graph(self) -> Dict:
        """导出完整的数据血缘图"""
        return self._lineage.to_dict()
```

---

## 实施路线图

```
Phase 1 (Week 1): Router 集成
├── 替换 kedro_engine.py 中的 REF_PATTERN
├── 添加单元测试验证路由解析
└── 更新 YAML 工作流文档

Phase 2 (Week 2): Schema 验证
├── 在 business_engines 中定义核心 Schema
├── 添加可选的输出验证机制
└── 集成到 execute_manager.py

Phase 3 (Week 3): 血缘追踪
├── 集成 Lineage 到 Pipeline
├── 添加血缘可视化导出
└── 与 EventBus 事件关联
```

---

## 设计原则

1. **shared/ 保持通用**
   - Field, Schema, Router, Lineage 是通用框架
   - 不包含任何业务特定的 Schema 定义

2. **业务代码引用 shared/**
   ```python
   # ✅ 正确: 业务代码引用 shared
   # src/astock/business_engines/trend/schemas.py
   from shared.contracts import Schema, Field

   # ❌ 错误: shared 引用业务代码
   # shared/contracts/schemas/trend.py  # 不应该存在!
   ```

3. **遵循 EventBus 模式**
   - EventBus 提供框架 + 标准事件类型
   - Contracts 提供框架 + 标准验证器
   - 组件直接 import 使用

---

## 文件变更清单

| 文件 | 变更类型 | 说明 |
|------|----------|------|
| `pipeline/engines/kedro_engine.py` | 修改 | 引入 Router 替换 REF_PATTERN |
| `src/astock/business_engines/*/schemas.py` | 新增 | 各引擎的 Schema 定义 |
| `pipeline/core/execute_manager.py` | 修改 | 可选的 Schema 验证 |
| `workflow/*.yaml` | 可选 | 添加 schema 引用字段 |
