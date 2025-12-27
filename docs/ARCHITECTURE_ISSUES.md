# AStock-Analysis 架构问题分析报告

## 概述

基于对整个系统代码的深入分析，发现以下架构层面的问题。按严重程度分类：

---

## 🔴 严重问题

### 1. ✅ 数据存储重复（三套并行存储）- **已解决**

**问题描述：**

当前系统存在**三个独立的数据存储**，存储相同的步骤输出数据：

| 存储位置 | 类型 | Key 格式 | 使用方 |
|---------|------|----------|-------|
| `KedroEngine.global_catalog` | `Dict[str, Any]` | `{step}__{output}` | kedro_engine.py, prefect_engine.py, io_manager.py |
| `PipelineContext.reference_values` | `Dict[str, Any]` | `steps.{step}.outputs.parameters.{output}` | runtime_param_service.py |
| `PipelineContext.global_registry` | `Dict[str, Any]` | MD5 hash | runtime_param_service.py (备用) |

**解决方案：**

✅ 统一到 `shared/contracts/store/DataStore`：
- 单一真相源存储
- 三种索引：key、ref、hash
- PipelineContext 和 KedroEngine 都使用同一个 DataStore 实例
- 详见：[shared/contracts/store/data_store.py](../shared/contracts/store/data_store.py)

---

### 2. ✅ 引用解析逻辑重复（三处独立实现）- **已解决**

**问题描述：**

相同的引用解析逻辑在三个地方独立实现：

| 位置 | 使用的正则/逻辑 |
|------|----------------|
| `kedro_engine.py:_resolve_refs_via_catalog()` | 导入 `REF_PATTERN`，手动解析 |
| `runtime_param_service.py:resolve()` | 直接查 `ctx.reference_values` |
| `config_service.py` | 导入 `REF_PATTERN` 计算依赖 |

**解决方案：**

✅ 统一到 `shared/contracts/store/ReferenceResolver`：
- 使用 contracts Router 进行路由匹配
- 支持 `{"__ref__": "..."}` 模式自动解析
- KedroEngine 和 PipelineContext 都使用同一个 ReferenceResolver
- 详见：[shared/contracts/store/reference.py](../shared/contracts/store/reference.py)

**代码证据：**

```python
# kedro_engine.py:130-160 (简化)
def _resolve_refs_via_catalog(obj):
    from pipeline.core.context import REF_PATTERN
    pattern = REF_PATTERN
    def walk(v):
        if isinstance(v, dict) and '__ref__' in v:
            ref = v['__ref__']
            m = pattern.match(ref)
            if m:
                step_id = m.group('step')
                out_id = m.group('param')
                ds_name = f"{step_id}__{out_id}".replace('-', '_')
                val = self.global_catalog[ds_name]  # 从 global_catalog 取
                ...
    return walk(obj)

# runtime_param_service.py:40-55 (简化)
def resolve(self, params: Dict[str, Any]) -> Dict[str, Any]:
    def walk(v: Any):
        if isinstance(v, dict) and '__ref__' in v:
            ref = v['__ref__']
            h = v.get('hash')
            if h and h in self.ctx.global_registry:
                return self.ctx.global_registry[h]  # 从 global_registry 取
            if ref in self.ctx.reference_values:
                return self.ctx.reference_values[ref]  # 从 reference_values 取
    ...
```

**影响：**
- 两处解析可能不一致
- 修改一处忘记另一处
- 违反 DRY 原则

**建议：**
统一为单一引用解析服务：
```python
class ReferenceResolver:
    def resolve(self, ref_or_dict) -> Any:
        """统一的引用解析入口"""
        pass
```

---

### 3. ✅ KedroEngine 职责过重（God Object）- **部分解决**

**问题描述：**

`KedroEngine` 承担了太多职责：

```python
class KedroEngine:
    # 数据存储
    self.global_catalog = {}
    self.data_catalog = DataCatalog()

    # 缓存管理
    self.node_signatures = {}
    self.dataset_fingerprints = {}
    self._persist_node_state()
    self._load_persistent_cache()

    # 血缘追踪
    self.lineage = {}
    self.dataset_producers = {}

    # 指标收集
    self.node_metrics = {}

    # 事件发布
    self._event_bus.emit(...)

    # 方法执行
    self.execute_manager.orchestrator.execute_with_engine(...)
```

**解决方案：**

✅ 已提取：
- `DataStore` → `shared/contracts/store/data_store.py`
- `ReferenceResolver` → `shared/contracts/store/reference.py`
- `CacheService` → `pipeline/services/cache_service.py`
- `EventPublisher` → `pipeline/services/event_publisher.py`

⏳ 待提取：
- `LineageService` - 血缘追踪
- `MetricsService` - 指标收集

---

## 🟡 中等问题

### 4. ✅ EventBus 事件发布分散 - **已解决**

**问题描述：**

事件发布散落在多个文件中，没有统一的发布点：

```
pipeline/engines/kedro_engine.py     → NodeStarted, NodeCompleted, CacheHit, PipelineError
pipeline/core/execute_manager.py     → PipelineStarted, PipelineCompleted (但实际未发布!)
orchestrator/registry/registry.py    → MethodRegistered, RegistryRefreshed
src/astock/.../truth_engine.py       → DataLoaded, DataTransformed
```

**解决方案：**

✅ `ExecuteManager.execute_pipeline()` 现在发布 `PipelineStartedEvent` 和 `PipelineCompletedEvent`
✅ `EventPublisher` 服务统一封装事件发布逻辑
- 详见：[pipeline/services/event_publisher.py](../pipeline/services/event_publisher.py)

---

### 5. 配置解析与依赖计算重复

**问题描述：**

`ConfigService._parse_steps()` 和 `DependencyGraph` 都需要解析参数中的引用：

```python
# config_service.py - 解析引用计算依赖
class ConfigService:
    def _parse_steps(self):
        ...
        # 需要解析 parameters 中的 "steps.X.outputs.parameters.Y" 来确定依赖

# dependency_graph.py - DataDependencySource 也解析引用
class DataDependencySource:
    def extract_dependencies(self, node_name, node_config, all_nodes):
        # 从 inputs 推导数据依赖
```

**问题：**
- 两处都需要遍历参数查找引用
- 使用不同的方式（一个用 REF_PATTERN，一个用 inputs 列表）

---

### 6. Prefect 和 Kedro 引擎职责重叠

**问题描述：**

`PrefectEngine` 和 `KedroEngine` 都有类似的逻辑：

| 功能 | PrefectEngine | KedroEngine |
|------|---------------|-------------|
| 缓存判定 | ✅ (line 576) | ✅ (line 230) |
| 数据存储 | ✅ (`global_catalog`) | ✅ (`global_catalog`) |
| 引用解析 | ✅ (通过 IOManager) | ✅ (`_resolve_refs_via_catalog`) |
| 事件发布 | ❌ | ✅ |

**代码证据：**

```python
# prefect_engine.py:576 - 缓存判定
if last_sig == node_signature and outs_list and all(o in self.kedro_engine.global_catalog for o in outs_list):
    result = tuple(self.kedro_engine.global_catalog[o] for o in outs_list)

# kedro_engine.py:230 - 缓存判定 (几乎相同)
if planned_outputs and all(o in self.global_catalog for o in planned_outputs) and last_sig == node_signature:
    return tuple(self.global_catalog[o] for o in planned_outputs)
```

**建议：**
明确分工：
- **PrefectEngine**: 只负责工作流编排、重试、监控
- **KedroEngine**: 只负责节点执行、数据管理

---

## 🟢 轻微问题

### 7. 命名不一致

| 概念 | 出现的名称 |
|------|-----------|
| 步骤输出 | `outputs`, `parameters`, `dataset`, `artifact` |
| 步骤名称 | `step_name`, `step`, `name`, `id`, `step_id` |
| 数据集 key | `{step}__{output}`, `{step}.{output}`, `steps.{step}.outputs.parameters.{output}` |

**建议：**
统一术语表：
```python
# 统一命名
STEP_OUTPUT_KEY = "{step}__{output}"
STEP_REF_KEY = "steps.{step}.outputs.parameters.{output}"
```

---

### 8. shared/contracts 未被使用

**问题描述：**

`shared/contracts/` 包含完整的 PGCS 框架（Schema, Field, Router, Lineage），但项目中**没有任何实际使用**：

```python
# 只在以下位置出现：
# - demo_pgcs_professional.py (演示)
# - docs/*.md (文档)
# - shared/contracts/ 内部
```

**建议：**
要么：
1. 开始使用（如替换 REF_PATTERN）
2. 要么移除（减少维护负担）

---

## 架构改进路线图

### Phase 1: 数据存储统一 (优先级最高)

```
Before:
┌────────────────┐  ┌────────────────────────┐  ┌────────────────────┐
│ global_catalog │  │ ctx.reference_values   │  │ ctx.global_registry│
│ step__output   │  │ steps.step.outputs...  │  │ md5_hash           │
└────────────────┘  └────────────────────────┘  └────────────────────┘

After:
┌────────────────────────────────────────────────────────────────────┐
│                      UnifiedDataCatalog                            │
│  _store: Dict[str, Any]  # 单一存储                                │
│  _ref_index: {ref -> key}                                          │
│  _hash_index: {hash -> key}                                        │
│                                                                    │
│  get(key) / get_by_ref(ref) / get_by_hash(hash)                   │
│  set(key, value, ref=None)                                         │
└────────────────────────────────────────────────────────────────────┘
```

### Phase 2: 引用解析统一

```python
# 新建: pipeline/core/reference_resolver.py
class ReferenceResolver:
    """统一的引用解析服务"""

    REF_PATTERN = re.compile(r"^steps\.(?P<step>[^.]+)\.outputs\.parameters\.(?P<param>[^.]+)$")

    def __init__(self, catalog: UnifiedDataCatalog):
        self._catalog = catalog

    def resolve(self, value: Any) -> Any:
        """递归解析引用"""
        if isinstance(value, dict) and '__ref__' in value:
            return self._resolve_ref(value['__ref__'])
        if isinstance(value, dict):
            return {k: self.resolve(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self.resolve(v) for v in value]
        return value

    def _resolve_ref(self, ref: str) -> Any:
        return self._catalog.get_by_ref(ref)
```

### Phase 3: KedroEngine 拆分

```
KedroEngine (瘦身后 <150 行)
│
├── inject: UnifiedDataCatalog
├── inject: ReferenceResolver
├── inject: CacheService
├── inject: LineageTracker
├── inject: MetricsCollector
│
└── execute_node() # 只做协调
```

### Phase 4: 事件发布规范化

```python
# 定义事件发布契约
class EventPublisher(Protocol):
    def on_pipeline_start(self, name: str, steps: int): ...
    def on_pipeline_complete(self, status: str, duration: float): ...
    def on_node_start(self, step: str, inputs: List[str]): ...
    def on_node_complete(self, step: str, status: str, duration: float): ...

# ExecuteManager 实现发布
class ExecuteManager:
    def execute_pipeline(self):
        self._publish_pipeline_start()
        try:
            result = self._flow_executor.run(...)
            self._publish_pipeline_complete('success')
        except:
            self._publish_pipeline_complete('failed')
```

---

## 总结

| 类别 | 问题数 | 关键问题 |
|------|--------|----------|
| 🔴 严重 | 3 | 数据存储重复、引用解析重复、KedroEngine 过重 |
| 🟡 中等 | 3 | EventBus 发布分散、配置解析重复、引擎职责重叠 |
| 🟢 轻微 | 2 | 命名不一致、contracts 未使用 |

**最高优先级修复：**
1. 统一数据存储（消除三套并行存储）
2. 统一引用解析（消除重复实现）
3. 修复 Pipeline 级别事件未发布的问题
