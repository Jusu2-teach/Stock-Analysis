# Pipeline 架构重构设计方案 (v2)

## 概述

本文档针对 [ARCHITECTURE_ISSUES.md](ARCHITECTURE_ISSUES.md) 中发现的问题，提出专业的解决方案。

**v2 重要更新**：
1. 数据存储直接扩展 `contracts`，而非新建独立模块
2. KedroEngine 瘦身 = Extract Class 重构（移动代码，不是删除）

---

## ✅ 实施状态

| 组件 | 状态 | 文件路径 |
|------|------|---------|
| `DataStore` | ✅ 已完成 | `shared/contracts/store/data_store.py` |
| `DataEntry` | ✅ 已完成 | `shared/contracts/store/data_entry.py` |
| `ReferenceResolver` | ✅ 已完成 | `shared/contracts/store/reference.py` |
| `CacheService` | ✅ 已完成 | `pipeline/services/cache_service.py` |
| `EventPublisher` | ✅ 已完成 | `pipeline/services/event_publisher.py` |
| `PipelineContext` 集成 | ✅ 已完成 | `pipeline/core/context.py` |
| `KedroEngine` 集成 | ✅ 已完成 | `pipeline/engines/kedro_engine.py` |
| `IOManager` 更新 | ✅ 已完成 | `pipeline/io/io_manager.py` |
| `ExecuteManager` 事件发布 | ✅ 已完成 | `pipeline/execute_manager.py` |

---

## 回答关键问题

### Q1: 为什么不直接用 contracts，而是新建 catalog？

**答案：应该直接扩展 contracts！**

你的问题完全正确。我重新审视了 contracts 组件：

| contracts 已有 | 功能 |
|---------------|------|
| `fingerprint()` | 数据指纹计算 |
| `Metadata` | 元数据存储 |
| `Lineage` | 血缘追踪 |
| `Router` | 引用路由解析 |
| `Schema/Field` | 数据结构定义 |

| contracts 缺少 | 说明 |
|---------------|------|
| `DataStore` | 存储实际数据对象的组件 |

**修正方案**：在 `shared/contracts/store/` 新增 DataStore 组件，而不是创建独立的 `shared/catalog/` 模块。

### Q2: KedroEngine 瘦身是什么意思？要删代码吗？

**答案：不是删代码，是把代码移动到独立的类中！**

| 术语 | 含义 |
|------|------|
| **瘦身** | 把代码从一个大类移动到多个小类 |
| **Extract Class** | 重构模式名称：提取类 |
| **依赖注入** | 大类通过构造函数接收小类实例 |

**代码去向**：

```
KedroEngine (760行)
    │
    ├── 数据存储代码 (~100行) ───────▶ DataStore (contracts/store/)
    │
    ├── 引用解析代码 (~80行) ────────▶ ReferenceResolver (contracts/store/)
    │
    ├── 缓存逻辑代码 (~200行) ───────▶ CacheService (pipeline/services/)
    │
    ├── 事件发布代码 (~50行) ────────▶ EventPublisher (pipeline/services/)
    │
    └── 核心执行逻辑 (~250行) ───────▶ 保留在 KedroEngine (变成协调者)
```

**代码总量不变**，只是从一个 760 行的文件分散到 5 个 ~150 行的文件。

---

## 一、统一数据管理架构

### 1.1 问题回顾

当前存在三套并行数据存储：

```
KedroEngine.global_catalog       → Dict[str, Any]  key: step__output
PipelineContext.reference_values → Dict[str, Any]  key: steps.step.outputs.parameters.output
PipelineContext.global_registry  → Dict[str, Any]  key: MD5 hash
```

### 1.2 扩展后的 contracts 结构

```
shared/contracts/
├── core/              # Schema, Field (已有)
├── validation/        # Validator (已有)
├── serialization/     # Serializer (已有)
├── registry/          # SchemaRegistry (已有)
├── router/            # Router, RoutePattern (已有)
├── metadata/          # Metadata, Lineage (已有)
├── utils/             # fingerprint (已有)
│
└── store/             # ✅ 已实现：数据存储
    ├── __init__.py
    ├── data_store.py      # DataStore 类
    ├── data_entry.py      # DataEntry 数据条目
    └── reference.py       # ReferenceResolver 引用解析
```

### 1.3 DataStore 实现

```python
# shared/contracts/store/data_entry.py
"""数据条目定义"""
from dataclasses import dataclass, field
from typing import Any, Optional
from ..metadata.base import Metadata


@dataclass
class DataEntry:
    """数据存储条目

    复用 contracts.Metadata 存储元数据。
    """
    key: str                           # 主键: "step__output"
    value: Any                         # 数据对象
    ref: str                           # 引用路径: "steps.X.outputs.parameters.Y"
    fingerprint: str                   # 数据指纹
    metadata: Metadata = field(default_factory=Metadata)

    @property
    def producer_step(self) -> Optional[str]:
        return self.metadata.get('producer_step')
```

```python
# shared/contracts/store/data_store.py
"""统一数据存储"""
from typing import Any, Dict, Optional
import threading

from ..utils.fingerprint import fingerprint as compute_fingerprint
from ..metadata.base import Metadata
from ..metadata.lineage import Lineage
from .data_entry import DataEntry


class DataStore:
    """PGCS 数据存储

    统一的数据存储，替代原来分散的三套存储：
    - KedroEngine.global_catalog
    - PipelineContext.reference_values
    - PipelineContext.global_registry

    复用 contracts 组件：
    - fingerprint: 数据指纹
    - Metadata: 元数据
    - Lineage: 血缘追踪

    Example:
        store = DataStore.get()  # 单例

        # 存储
        store.put("Load_Data", "Raw", df)

        # 获取（三种方式）
        df = store.get("Load_Data__Raw")                              # by key
        df = store.get_by_ref("steps.Load_Data.outputs.parameters.Raw")  # by ref
        df = store.get_by_hash("a1b2c3d4")                           # by hash
    """

    _instance: Optional['DataStore'] = None
    _lock = threading.Lock()

    def __init__(self):
        self._store: Dict[str, DataEntry] = {}
        self._ref_index: Dict[str, str] = {}      # ref -> key
        self._hash_index: Dict[str, str] = {}     # hash -> key
        self._lineage = Lineage()                 # 复用 contracts.Lineage

    @classmethod
    def get(cls) -> 'DataStore':
        """获取单例实例"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @property
    def lineage(self) -> Lineage:
        """获取血缘图（复用 contracts.Lineage）"""
        return self._lineage

    def put(self, step: str, output: str, value: Any, **meta_kwargs) -> DataEntry:
        """存储数据"""
        from datetime import datetime

        key = f"{step}__{output}".replace('-', '_')
        ref = f"steps.{step}.outputs.parameters.{output}"
        fp = compute_fingerprint(value, length=16)
        hash_key = self._hash_ref(ref)

        meta_kwargs.setdefault('producer_step', step)
        meta_kwargs.setdefault('created_at', datetime.now().isoformat())
        metadata = Metadata(**meta_kwargs)

        entry = DataEntry(key=key, value=value, ref=ref, fingerprint=fp, metadata=metadata)

        # 存储 + 更新索引
        self._store[key] = entry
        self._ref_index[ref] = key
        self._hash_index[hash_key] = key

        return entry

    def get(self, key: str) -> Optional[Any]:
        """通过 key 获取"""
        entry = self._store.get(key)
        return entry.value if entry else None

    def get_by_ref(self, ref: str) -> Optional[Any]:
        """通过引用路径获取"""
        key = self._ref_index.get(ref)
        return self.get(key) if key else None

    def get_by_hash(self, hash_key: str) -> Optional[Any]:
        """通过哈希获取"""
        key = self._hash_index.get(hash_key)
        return self.get(key) if key else None

    @staticmethod
    def _hash_ref(ref: str) -> str:
        import hashlib
        return hashlib.md5(ref.encode()).hexdigest()[:16]
```

```python
# shared/contracts/store/reference.py
"""引用解析器"""
from typing import Any, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .data_store import DataStore

from ..router.base import Router, RoutePattern


class ReferenceResolver:
    """统一引用解析器

    复用 contracts.Router 进行路由匹配。

    Example:
        resolver = ReferenceResolver(store)
        resolved = resolver.resolve({
            "data": {"__ref__": "steps.Load_Data.outputs.parameters.Raw"}
        })
    """

    def __init__(self, store: 'DataStore'):
        self._store = store
        self._router = Router()
        self._router.register(RoutePattern(
            template='steps.{step}.outputs.parameters.{param}',
            handler='step_output'
        ))

    def resolve(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """递归解析参数中的引用"""
        return {k: self._walk(v) for k, v in params.items()}

    def _walk(self, value: Any) -> Any:
        if isinstance(value, dict):
            if '__ref__' in value:
                return self._resolve_ref(value['__ref__'])
            return {k: self._walk(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._walk(item) for item in value]
        return value

    def _resolve_ref(self, ref: str) -> Any:
        result = self._store.get_by_ref(ref)
        if result is None:
            raise ReferenceNotFoundError(ref)
        return result


class ReferenceNotFoundError(Exception):
    """引用未找到"""
    def __init__(self, ref: str):
        super().__init__(f"引用未找到: {ref}")
```

---

## 二、KedroEngine 瘦身（Extract Class 重构）

### 2.1 代码移动图示

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        KedroEngine (当前 760 行)                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  # 数据存储相关 (~100行)                                                     │
│  self.global_catalog = {}                                                   │
│  self.global_catalog[ds] = val                                              │
│                                    ─────────────────▶  DataStore            │
│                                                        (contracts/store)    │
│                                                                             │
│  # 缓存相关 (~200行)                                                         │
│  self.node_signatures = {}                                                  │
│  def _load_persistent_cache(self): ...                                      │
│  def _persist_node_state(self): ...                                         │
│  def _fingerprint_object(self): ...       ─────────────────▶  CacheService  │
│                                                                             │
│  # 事件发布 (~50行)                                                          │
│  self._event_bus.emit(NodeStartedEvent...)                                  │
│  self._event_bus.emit(NodeCompletedEvent...)─────────────────▶ EventPublisher│
│                                                                             │
│  # 引用解析 (~80行)                                                          │
│  def _resolve_refs_via_catalog(self): ...                                   │
│                                            ─────────────────▶ ReferenceResolver│
│                                                               (contracts/store)│
│                                                                             │
│  # 核心执行逻辑 (~250行)                                                     │
│  def execute_node(self): ...              ─────────────────▶  保留在KedroEngine
│  def _create_kedro_node(self): ...                            (变成协调者)   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 提取的服务类

```python
# pipeline/services/cache_service.py
"""从 kedro_engine.py 提取的缓存逻辑"""

class CacheService:
    """缓存服务

    原来在 kedro_engine.py 的代码：
    - _load_persistent_cache()
    - _persist_node_state()
    - _fingerprint_object()
    - compute_signature()
    """

    def __init__(self, store: DataStore, cache_dir: Path):
        self._store = store
        self._cache_dir = cache_dir
        self._signatures: Dict[str, str] = {}

    def compute_signature(self, step: str, methods: List[str], params: Dict) -> str:
        """计算节点签名（从 kedro_engine 移动过来的代码）"""
        ...

    def is_cached(self, step: str, signature: str, outputs: List[str]) -> bool:
        """判断是否缓存命中（从 kedro_engine 移动过来的代码）"""
        ...
```

```python
# pipeline/services/event_publisher.py
"""从 kedro_engine.py 提取的事件发布逻辑"""

class EventPublisher:
    """事件发布服务

    原来在 kedro_engine.py 的代码：
    - self._event_bus.emit(NodeStartedEvent...)
    - self._event_bus.emit(NodeCompletedEvent...)
    - self._event_bus.emit(CacheHitEvent...)
    """

    def __init__(self):
        self._bus = EventBus.get()

    def on_node_started(self, step: str, inputs: List[str], outputs: List[str]):
        """发布节点开始事件（从 kedro_engine 移动过来的代码）"""
        ...
```

### 2.3 瘦身后的 KedroEngine

```python
# pipeline/engines/kedro_engine.py (瘦身后 ~150 行)

class KedroEngine:
    """Kedro 执行引擎（瘦身后）

    职责：协调各服务完成节点执行

    依赖注入：
    - store: DataStore (数据存储)
    - resolver: ReferenceResolver (引用解析)
    - cache: CacheService (缓存管理)
    - events: EventPublisher (事件发布)
    """

    def __init__(
        self,
        execute_manager,
        store: DataStore = None,
        cache: CacheService = None,
        events: EventPublisher = None,
    ):
        # 依赖注入（默认使用单例）
        self._store = store or DataStore.get()
        self._resolver = ReferenceResolver(self._store)
        self._cache = cache or CacheService(self._store, Path('.pipeline/cache'))
        self._events = events or EventPublisher()

    def execute_node(self, node_config: Dict) -> Any:
        """执行节点（协调者角色）"""
        step_name = node_config['name']

        # 1. 解析引用（委托给 resolver）
        params = self._resolver.resolve(node_config.get('parameters', {}))

        # 2. 检查缓存（委托给 cache）
        signature = self._cache.compute_signature(step_name, ...)
        if self._cache.is_cached(step_name, signature, ...):
            return self._cache.load_cached(step_name, ...)

        # 3. 发布事件（委托给 events）
        self._events.on_node_started(step_name, ...)

        # 4. 执行（保留在本类）
        result = self._execute_methods(node_config, params)

        # 5. 存储（委托给 store）
        self._store.put(step_name, output_name, result)

        # 6. 发布完成事件
        self._events.on_node_completed(step_name, ...)

        return result
```

---

## 三、总结对比

### 代码行数对比

| 组件 | 重构前 | 重构后 | 说明 |
|------|--------|--------|------|
| `KedroEngine` | 760 行 | ~150 行 | 变成协调者 |
| `DataStore` (新) | 0 | ~120 行 | 在 contracts/store |
| `ReferenceResolver` (新) | 0 | ~60 行 | 在 contracts/store |
| `CacheService` (新) | 0 | ~100 行 | 从 kedro 提取 |
| `EventPublisher` (新) | 0 | ~60 行 | 从 kedro 提取 |
| **总代码** | 760 行 | ~490 行 | 消除重复后减少 |

### 重复代码消除

| 重复问题 | 解决方案 |
|---------|---------|
| 3 套数据存储 | → 1 个 DataStore |
| 3 处引用解析 | → 1 个 ReferenceResolver |
| 事件发布分散 | → 1 个 EventPublisher |

### 文件结构变化

```
重构前:
pipeline/engines/kedro_engine.py     # 760 行 God Object
pipeline/core/context.py             # 包含 reference_values, global_registry

重构后:
shared/contracts/store/              # 新增目录
├── __init__.py
├── data_store.py                    # 统一数据存储
├── data_entry.py                    # 数据条目
└── reference.py                     # 引用解析

pipeline/services/                   # 新增目录
├── __init__.py
├── cache_service.py                 # 提取：缓存逻辑
└── event_publisher.py               # 提取：事件发布

pipeline/engines/kedro_engine.py     # 瘦身：150 行协调者
pipeline/core/context.py             # 移除重复存储，使用 DataStore
```

---

## 四、实施步骤

### 阶段 1：扩展 contracts（低风险）

1. 创建 `shared/contracts/store/` 目录
2. 实现 `DataEntry`, `DataStore`, `ReferenceResolver`
3. 更新 `shared/contracts/__init__.py` 导出新组件
4. 编写单元测试

### 阶段 2：Extract Class（中等风险）

1. 创建 `pipeline/services/` 目录
2. 从 `kedro_engine.py` 提取代码到 `CacheService`
3. 从 `kedro_engine.py` 提取代码到 `EventPublisher`
4. 修改 `KedroEngine` 使用依赖注入
5. 保留兼容性属性（`global_catalog`, `lineage`）

### 阶段 3：统一存储（需要谨慎）

1. 修改 `PipelineContext` 使用 `DataStore`
2. 修改 `PrefectEngine` 使用 `DataStore`
3. 移除 `reference_values` 和 `global_registry`
4. 全面回归测试

---

## 五、关键设计原则

1. **复用优于新建**：优先扩展 contracts，不造轮子
2. **移动优于删除**：Extract Class 是移动代码，不是删代码
3. **渐进式重构**：分阶段实施，每阶段可独立验证
4. **保持兼容性**：通过兼容属性支持旧代码逐步迁移
