# 🚀 AStock 统一事件总线架构设计

> **版本**: 5.0.0
> **日期**: 2025-12-25
> **状态**: ✅ 完全集成，旧代码已清理

---

## 📊 集成状态

| 组件 | 状态 | 事件类型 | 清理状态 |
|------|------|----------|----------|
| orchestrator | ✅ 完成 | MethodRegisteredEvent, MethodExecutedEvent | ✅ 移除旧 HookBus |
| pipeline | ✅ 完成 | NodeStartedEvent, NodeCompletedEvent, CacheHitEvent | ✅ HookManager 改为 EventBus 包装器 |
| src (astock) | ✅ 完成 | DataLoadedEvent, DataTransformedEvent | ✅ 移除可选依赖检查 |
| plugins | ✅ 完成 | 订阅 pipeline.* 事件 | ✅ 使用纯 EventBus API |

**清理内容 (2025-12-25)**:
- ❌ 删除 `orchestrator/registry/hooks.py` (旧 HookBus)
- ❌ 删除旧 HookManager 实现 (292 行 → 220 行 EventBus 包装器)
- ❌ 移除所有 `_HAS_EVENT_BUS` 可选依赖检查
- ❌ 移除所有 `_ORCHESTRATOR_AVAILABLE` 标志
- ✅ KedroEngine 直接使用 EventBus
- ✅ 插件使用无参数 `register()` 接口

---

## 📊 架构总览

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              AStock System Architecture                          │
│                           (Event-Driven Microkernel)                            │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│                                shared (核心层)                                   │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────────────┐  │
│  │    EventBus      │  │     Events       │  │        Protocols             │  │
│  │  ┌────────────┐  │  │  ┌────────────┐  │  │  ┌────────────────────────┐  │  │
│  │  │ on()       │  │  │  │ Registry.* │  │  │  │ OrchestratorProtocol   │  │  │
│  │  │ emit()     │  │  │  │ Pipeline.* │  │  │  │ RegistryProtocol       │  │  │
│  │  │ wrapper()  │  │  │  │ System.*   │  │  │  │ ExecutorProtocol       │  │  │
│  │  │ off()      │  │  │  │ Data.*     │  │  │  │ EventBusProtocol       │  │  │
│  │  └────────────┘  │  │  └────────────┘  │  │  └────────────────────────┘  │  │
│  └──────────────────┘  └──────────────────┘  └──────────────────────────────┘  │
│                                    ▲                                            │
│                                    │ 零依赖                                      │
└────────────────────────────────────┼────────────────────────────────────────────┘
                                     │
        ┌────────────────────────────┼────────────────────────────┐
        │                            │                            │
        ▼                            ▼                            ▼
┌───────────────────┐    ┌───────────────────┐    ┌───────────────────┐
│   orchestrator    │    │     pipeline      │    │       src         │
│                   │    │                   │    │     (astock)      │
│  ┌─────────────┐  │    │  ┌─────────────┐  │    │  ┌─────────────┐  │
│  │  Registry   │──┼────┼──│  Executor   │──┼────┼──│ DataHub     │  │
│  │  Executor   │  │    │  │  Context    │  │    │  │ Engines     │  │
│  │  HookBus    │  │    │  │  HookMgr    │  │    │  │ Business    │  │
│  └─────────────┘  │    │  └─────────────┘  │    │  └─────────────┘  │
│        │          │    │        │          │    │        │          │
│        ▼          │    │        ▼          │    │        ▼          │
│   发布事件:        │    │   发布事件:        │    │   发布事件:        │
│   • method.reg    │    │   • node.start    │    │   • data.loaded   │
│   • method.exec   │    │   • node.done     │    │   • @register     │
│   订阅事件:        │    │   • cache.hit     │    │                   │
│   • system.ready  │    │   订阅事件:        │    │                   │
│                   │    │   • method.reg    │    │                   │
└───────────────────┘    └───────────────────┘    └───────────────────┘
```

---

## 🔄 事件流设计

### 1. 系统启动流程

```
┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────┐
│  main   │────▶│ shared  │────▶│ orch.   │────▶│ pipeline│
└─────────┘     └─────────┘     └─────────┘     └─────────┘
     │               │               │               │
     │  import       │               │               │
     ├──────────────▶│               │               │
     │               │               │               │
     │  EventBus.get()               │               │
     ├──────────────▶│               │               │
     │               │               │               │
     │               │  emit(SystemReadyEvent)       │
     │               │◀──────────────┤               │
     │               │               │               │
     │               │  ─────────────┼──────────────▶│
     │               │  broadcast to all subscribers │
     │               │               │               │
```

### 2. 方法注册流程

```python
# src/astock/business_engines/truth/truth_engine.py
@register_method(component="business", method="analyze_truth")
def analyze_truth(...):
    ...

# 触发事件链：
# 1. @register_method 执行
# 2. Registry.register() 调用
# 3. EventBus.emit(MethodRegisteredEvent(...))
# 4. 所有订阅者收到通知
```

```
┌───────────────┐         ┌───────────────┐         ┌───────────────┐
│  @register    │         │   Registry    │         │   EventBus    │
│   decorator   │         │               │         │               │
└───────┬───────┘         └───────┬───────┘         └───────┬───────┘
        │                         │                         │
        │  register(func)         │                         │
        ├────────────────────────▶│                         │
        │                         │                         │
        │                         │  emit(MethodRegistered) │
        │                         ├────────────────────────▶│
        │                         │                         │
        │                         │                         │  broadcast
        │                         │                         ├──────────▶ [监控]
        │                         │                         ├──────────▶ [日志]
        │                         │                         ├──────────▶ [Pipeline刷新]
        │                         │                         │
```

### 3. Pipeline 执行流程

```
┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────┐
│Executor │────▶│ Engine  │────▶│EventBus │────▶│Plugins  │
└─────────┘     └─────────┘     └─────────┘     └─────────┘
     │               │               │               │
     │  run_flow()   │               │               │
     ├──────────────▶│               │               │
     │               │               │               │
     │               │  emit(PipelineStarted)        │
     │               ├──────────────▶│               │
     │               │               │  ────────────▶│ [Prometheus]
     │               │               │               │
     │  for node:    │               │               │
     │               │  emit(NodeStarted)            │
     │               ├──────────────▶│               │
     │               │               │  ────────────▶│ [Logging]
     │               │               │               │
     │               │  execute()    │               │
     │               ├───────────────│───────────────│──▶ orchestrator
     │               │               │               │
     │               │  emit(NodeCompleted)          │
     │               ├──────────────▶│               │
     │               │               │  ────────────▶│ [Metrics]
     │               │               │               │
     │               │  emit(PipelineCompleted)      │
     │               ├──────────────▶│               │
```

---

## 📋 事件目录

### Registry 事件 (`registry.*`)

| 事件 | 触发时机 | Payload |
|------|----------|---------|
| `registry.method.registered` | 方法注册后 | component, method, engine, version |
| `registry.method.executed` | 方法执行后 | component, method, duration, success |
| `registry.method.selected` | 方法选择后 | component, method, strategy, candidates |

### Pipeline 事件 (`pipeline.*`)

| 事件 | 触发时机 | Payload |
|------|----------|---------|
| `pipeline.flow.started` | Flow 开始 | pipeline_name, steps, config |
| `pipeline.flow.completed` | Flow 结束 | status, duration, stats |
| `pipeline.node.started` | 节点开始 | step_name, inputs, signature |
| `pipeline.node.completed` | 节点结束 | step_name, status, duration |
| `pipeline.cache.hit` | 缓存命中 | step_name, signature, saved_time |
| `pipeline.cache.invalidated` | 缓存失效 | step_name, reason |

### System 事件 (`system.*`)

| 事件 | 触发时机 | Payload |
|------|----------|---------|
| `system.ready` | 系统启动完成 | components, methods_count |
| `system.component.loaded` | 组件加载 | component_name, type |
| `system.error` | 全局错误 | error_type, message, stack |
| `system.metric` | 指标发布 | metric_name, value, tags |

### Data 事件 (`data.*`)

| 事件 | 触发时机 | Payload |
|------|----------|---------|
| `data.loaded` | 数据加载完成 | dataset, source, rows |
| `data.transformed` | 数据转换完成 | dataset, operation, rows |

---

## 🔌 集成指南

### Step 1: 在 orchestrator 中集成

```python
# orchestrator/registry/registry.py
from shared import EventBus, MethodRegisteredEvent

class Registry:
    def register(self, ...):
        # ... 原有逻辑 ...

        # 发布事件
        EventBus.get().emit(MethodRegisteredEvent(
            component=component_type,
            method=method_name,
            engine_type=engine_type,
            version=version,
            source='orchestrator'
        ))
```

### Step 2: 在 pipeline 中集成

```python
# pipeline/core/services/hook_manager.py
from shared import EventBus, EventBusProtocol

class HookManager:
    """适配器：将旧 HookManager API 映射到 EventBus"""

    def __init__(self):
        self._bus = EventBus.get()
        # 事件名映射
        self._event_map = {
            'before_node': 'pipeline.node.started',
            'after_node': 'pipeline.node.completed',
            'on_cache_hit': 'pipeline.cache.hit',
        }

    def emit(self, event: str, *args, **kwargs):
        # 转换为标准事件
        std_event = self._event_map.get(event, f'pipeline.{event}')
        return self._bus.emit(std_event, **kwargs)
```

### Step 3: 在 src 中使用

```python
# src/astock/business_engines/truth/truth_engine.py
from shared import EventBus, DataLoadedEvent

def load_financial_data(...):
    data = tushare.get_fina_indicator(...)

    # 发布事件
    EventBus.get().emit(DataLoadedEvent(
        dataset_name='fina_indicator',
        source='tushare',
        row_count=len(data),
        column_count=len(data.columns)
    ))

    return data
```

### Step 4: 插件订阅

```python
# pipeline/plugins/prometheus_plugin.py
from shared import EventBus, NodeCompletedEvent

@EventBus.get().on('pipeline.node.completed')
def record_metrics(event: NodeCompletedEvent):
    node_duration.labels(step=event.step_name).observe(event.duration_ms / 1000)

@EventBus.get().on('pipeline.*')  # 通配符订阅所有 pipeline 事件
def log_all_pipeline_events(event):
    logger.info(f"[Pipeline] {event.event_type}: {event}")
```

---

## 📊 优势对比

| 维度 | 当前架构 | 事件总线架构 |
|------|----------|--------------|
| **组件耦合** | 直接导入 | 🟢 完全解耦 |
| **扩展性** | 需修改源码 | 🟢 订阅即扩展 |
| **可测试性** | 需 Mock 多层 | 🟢 事件断言 |
| **调试追踪** | 断点调试 | 🟢 事件日志 |
| **异步支持** | 需重构 | 🟢 天然支持 |
| **性能监控** | 分散实现 | 🟢 统一采集 |
| **类型安全** | 部分 | 🟢 Protocol 契约 |

---

## 🏗️ 迁移计划

### Phase 1: 基础设施 (已完成 ✅)

- [x] 创建 `shared/` 模块
- [x] 实现 `EventBus` 核心
- [x] 定义标准事件类型
- [x] 定义 Protocol 接口

### Phase 2: Orchestrator 集成

- [ ] Registry 发布 `method.registered` 事件
- [ ] Executor 发布 `method.executed` 事件
- [ ] 适配旧 HookBus → EventBus

### Phase 3: Pipeline 集成

- [ ] HookManager 适配 EventBus
- [ ] KedroEngine 使用新事件
- [ ] PrefectEngine 使用新事件

### Phase 4: src 集成

- [ ] DataHub 发布数据事件
- [ ] BusinessEngines 发布分析事件

### Phase 5: 高级功能

- [ ] 异步事件支持
- [ ] 事件持久化（审计）
- [ ] 分布式事件（Redis/Kafka）

---

## 📁 目录结构

```
AStock-Analysis/
├── shared/                    # 🆕 核心共享层
│   ├── __init__.py           # 统一导出
│   ├── event_bus.py          # 事件总线实现
│   ├── events.py             # 标准事件定义
│   └── protocols.py          # 接口契约
│
├── orchestrator/              # 编排层（订阅 + 发布）
│   ├── registry/
│   │   └── registry.py       # → emit(MethodRegistered)
│   └── orchestrator.py       # → emit(MethodExecuted)
│
├── pipeline/                  # 执行层（订阅 + 发布）
│   ├── core/
│   │   └── services/
│   │       └── hook_manager.py  # → 适配到 EventBus
│   └── engines/
│       └── kedro_engine.py   # → emit(NodeCompleted)
│
└── src/astock/               # 业务层（发布）
    └── business_engines/
        └── truth/            # → emit(DataLoaded)
```

---

## 🎯 结论

**事件总线架构确实更专业**，它提供：

1. **完全解耦** - 发布者和订阅者互不知道对方存在
2. **可扩展** - 新功能通过订阅事件实现，无需修改现有代码
3. **可观测** - 所有交互都是事件，天然可追踪
4. **可测试** - Mock EventBus 即可隔离测试

**建议**：采用渐进式迁移，先实现 `shared` 层，然后逐步集成各组件。
