# AStock-Analysis 系统执行流程完整梳理

## 一、整体架构图

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              用户执行命令                                         │
│          python -m pipeline.main run -c workflow/analysis.yaml                  │
└───────────────────────────────────┬─────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           1. CLI 层 (pipeline/cli.py)                            │
│  - 解析命令行参数 (--resume, --only, --exclude, --debug)                          │
│  - 初始化 ExecuteManager                                                         │
│  - 调用 manager.execute_pipeline()                                               │
└───────────────────────────────────┬─────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    2. ExecuteManager (pipeline/core/execute_manager.py)          │
│  ┌────────────────────────────────────────────────────────────────────────────┐ │
│  │ 初始化:                                                                     │ │
│  │  - self.orchestrator = AStockOrchestrator() ← 自动发现注册所有业务方法         │ │
│  │  - self._event_bus = EventBus.get() ← 获取全局事件总线单例                    │ │
│  │  - self.ctx = PipelineContext() ← 共享执行上下文                              │ │
│  │  - self._load_plugins() ← 加载 pipeline/plugins/*.py 插件                    │ │
│  └────────────────────────────────────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────────────────────────────────────┐ │
│  │ 执行流程:                                                                   │ │
│  │  1. load_config() → 解析 YAML 配置到 ctx.config                             │ │
│  │  2. _build_auto_kedro_config() → 生成 Kedro 节点配置                         │ │
│  │  3. FlowExecutor.run() → 调用 Prefect 引擎执行                               │ │
│  └────────────────────────────────────────────────────────────────────────────┘ │
└───────────────────────────────────┬─────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    3. PrefectEngine (pipeline/engines/prefect_engine.py)         │
│  - Prefect 负责工作流编排、监控、重试                                              │
│  - 将 Kedro 管道视为黑箱 Task 执行                                                │
│  - 内部调用 KedroEngine 执行具体节点                                              │
└───────────────────────────────────┬─────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    4. KedroEngine (pipeline/engines/kedro_engine.py)             │
│  这是核心执行引擎，负责实际业务逻辑调用                                             │
│                                                                                 │
│  ┌────────────────────────────────────────────────────────────────────────────┐ │
│  │ 核心属性:                                                                   │ │
│  │  - self.global_catalog = {} ← 存储所有步骤输出 (Dict[str, Any])              │ │
│  │  - self._event_bus = EventBus.get() ← 事件总线                              │ │
│  │  - self.node_signatures = {} ← 节点指纹 (用于缓存判定)                        │ │
│  │  - self.lineage = {} ← 数据血缘追踪                                          │ │
│  └────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                 │
│  ┌────────────────────────────────────────────────────────────────────────────┐ │
│  │ 节点执行流程 (execute_node):                                                 │ │
│  │                                                                             │ │
│  │  1️⃣ 引用解析 (_resolve_refs_via_catalog)                                   │ │
│  │     - 使用 REF_PATTERN 正则解析 "steps.X.outputs.parameters.Y"               │ │
│  │     - 从 global_catalog 获取上游步骤输出                                      │ │
│  │                                                                             │ │
│  │  2️⃣ 缓存判定                                                                │ │
│  │     - 计算节点签名 = method + params + upstream_fingerprints                 │ │
│  │     - 若签名匹配且输出存在 → 跳过执行，发布 CacheHitEvent                       │ │
│  │                                                                             │ │
│  │  3️⃣ 发布 NodeStartedEvent                                                   │ │
│  │                                                                             │ │
│  │  4️⃣ 方法执行 (支持方法链)                                                    │ │
│  │     for method_name in method_list:                                         │ │
│  │       - 通过 registry.index 查找 Registration                                │ │
│  │       - 调用 orchestrator.execute_with_engine()                             │ │
│  │                                                                             │ │
│  │  5️⃣ 输出存储                                                                │ │
│  │     - 存入 global_catalog[step_name__output_name] = result                  │ │
│  │     - 注册到 ctx.reference_values (供后续步骤引用)                            │ │
│  │     - 持久化到 .pipeline/cache/                                              │ │
│  │                                                                             │ │
│  │  6️⃣ 发布 NodeCompletedEvent                                                 │ │
│  └────────────────────────────────────────────────────────────────────────────┘ │
└───────────────────────────────────┬─────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    5. Orchestrator (orchestrator/orchestrator.py)                │
│                                                                                 │
│  ┌────────────────────────────────────────────────────────────────────────────┐ │
│  │ 初始化 (auto_discover=True):                                                │ │
│  │  - registry.auto_load() 扫描以下模块:                                        │ │
│  │    • src/astock/business_engines/analysis/engine.py                         │ │
│  │    • src/astock/business_engines/trend/engine.py                            │ │
│  │    • src/astock/business_engines/truth/truth_engine.py                      │ │
│  │    • src/astock/business_engines/reporters/engine.py                        │ │
│  │  - 收集所有 @register_method 装饰的函数                                       │ │
│  └────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                 │
│  ┌────────────────────────────────────────────────────────────────────────────┐ │
│  │ execute_with_engine(component, engine, method, **params):                   │ │
│  │  1. registry.index.by_component[component][method][engine] → Registration   │ │
│  │  2. registration.callable(**params) → 执行实际业务函数                        │ │
│  │  3. 发布 MethodExecutedEvent                                                │ │
│  └────────────────────────────────────────────────────────────────────────────┘ │
└───────────────────────────────────┬─────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                6. Business Engines (src/astock/business_engines/)               │
│                                                                                 │
│  实际的业务逻辑函数，通过 @register_method 装饰器注册                               │
│                                                                                 │
│  ┌────────────────────────────────────────────────────────────────────────────┐ │
│  │ 示例 (analysis/engine.py):                                                  │ │
│  │                                                                             │ │
│  │  @register_method(                                                          │ │
│  │      engine_name="load_file",                                               │ │
│  │      component_type="business_engine",                                      │ │
│  │      engine_type="duckdb",                                                  │ │
│  │  )                                                                          │ │
│  │  def load_file(path: str) -> pd.DataFrame:                                  │ │
│  │      con, source = _init_duckdb_and_source(path)                            │ │
│  │      return con.execute(f"SELECT * FROM {source}").df()                     │ │
│  └────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                 │
│  主要引擎:                                                                        │
│  - analysis/engine.py: load_file, calc_industry_avg, filter_outperform          │
│  - trend/engine.py: analyze_metric_trend (8种数学探针)                           │
│  - truth/truth_engine.py: T.R.U.T.H. 六维基因分析                                │
│  - reporters/engine.py: 报告生成                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 二、数据流详解

### 2.1 YAML 配置示例 (workflow/analysis.yaml)

```yaml
pipeline:
  name: "DuckDB财务基线筛选管道"

  steps:
    # Step 1: 加载数据
    - name: "Load_Financial_Data"
      component: "business_engine"
      engine: "duckdb"
      method: ["load_file"]
      parameters:
        path: "data/polars/10yd_final_industry.csv"
      outputs:
        parameters:
          - name: Raw_Data

    # Step 2: 分析趋势 (引用 Step 1 的输出)
    - name: "Analyze_ROIC_Trend"
      component: "business_engine"
      engine: "duckdb"
      method: ["analyze_metric_trend"]
      parameters:
        data: "steps.Load_Financial_Data.outputs.parameters.Raw_Data"  # 跨步骤引用
        metric_name: 'roic'
        min_periods: 5
      outputs:
        parameters:
          - name: ROIC_Trend_Result
```

### 2.2 数据流转过程

```
                    YAML 配置
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   PipelineContext.config                            │
│  存储完整的 YAML 解析结果                                            │
└───────────────────────────────────┬─────────────────────────────────┘
                                    │
        ┌───────────────────────────┴───────────────────────────┐
        │                                                       │
        ▼                                                       ▼
┌───────────────────────┐                         ┌───────────────────────┐
│   Step 1: Load_Data   │                         │  Step 2: Analyze...   │
│                       │                         │                       │
│  parameters:          │                         │  parameters:          │
│    path: "data/..."   │                         │    data: "steps.Load_ │
│                       │                         │    Financial_Data..." │
│  outputs:             │                         │                       │
│    - Raw_Data         │───────────────────────▶│  (引用解析)            │
└───────────────────────┘                         └───────────────────────┘
        │                                                       │
        ▼                                                       ▼
┌───────────────────────────────────────────────────────────────────────┐
│                     KedroEngine.global_catalog                        │
│                                                                       │
│  {                                                                    │
│    "Load_Financial_Data__Raw_Data": <DataFrame>,                      │
│    "Analyze_ROIC_Trend__ROIC_Trend_Result": <DataFrame>,              │
│    ...                                                                │
│  }                                                                    │
└───────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────────────────────────────────┐
│              PipelineContext.data_store（PGCS DataStore）              │
│                                                                       │
│  - 通过 DataStore 统一存储所有步骤输出（按 key/hash/ref 三种索引）       │
│  - 典型 key: "Load_Financial_Data__Raw_Data"                          │
│  - 典型 ref: "steps.Load_Financial_Data.outputs.parameters.Raw_Data"  │
│  - ReferenceResolver 负责将 ref 解析为实际数据                         │
└───────────────────────────────────────────────────────────────────────┘
```

### 2.3 引用解析机制

```python
# pipeline/core/context.py - 引用模式通过 ReferenceResolver 定义
from shared.contracts.store import DataStore, ReferenceResolver

class PipelineContext:
  def __post_init__(self):
    self._data_store = DataStore()
    self._resolver = ReferenceResolver(self._data_store)
    self._resolver.register_pattern(
      template='steps.{step}.outputs.parameters.{param}',
      handler='step_output',
    )

  def register_reference(self, ref: str, value: Any) -> str:
    """将步骤输出注册到 DataStore，并绑定 ref（steps.X.outputs.parameters.Y）"""
    entry = self.data_store.put(key, value, ref=ref, producer_step=step_id)
    return entry.fingerprint

  def resolve_references(self, params: Dict[str, Any]) -> Dict[str, Any]:
    """递归解析参数中的 {"__ref__": "steps.X.outputs.parameters.Y"} 结构"""
    return self.resolver.resolve_params(params)
```

---

## 三、EventBus 事件流

### 3.1 事件定义 (shared/event_bus/events.py)

| 事件类型 | 触发时机 | 携带数据 |
|---------|---------|---------|
| `PipelineStartedEvent` | Pipeline 开始执行 | pipeline_name, total_steps |
| `PipelineCompletedEvent` | Pipeline 执行完成 | status, duration_sec, executed_steps |
| `NodeStartedEvent` | 节点开始执行 | step_name, inputs, outputs, signature |
| `NodeCompletedEvent` | 节点执行完成 | step_name, status, duration_ms, metrics |
| `CacheHitEvent` | 缓存命中跳过执行 | step_name, signature, outputs |
| `PipelineErrorEvent` | 执行出错 | step_name, error, traceback |
| `MethodRegisteredEvent` | 方法注册 | component, method, engine_type |

### 3.2 事件发布位置

```
┌─────────────────────────────────────────────────────────────────────┐
│                       事件发布位置                                    │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  pipeline/core/execute_manager.py                                   │
│    └─ PipelineStartedEvent, PipelineCompletedEvent, SystemReadyEvent│
│                                                                     │
│  pipeline/engines/kedro_engine.py                                   │
│    └─ NodeStartedEvent, NodeCompletedEvent, CacheHitEvent           │
│    └─ PipelineErrorEvent (执行失败时)                                │
│                                                                     │
│  orchestrator/registry/registry.py                                  │
│    └─ MethodRegisteredEvent, RegistryRefreshedEvent                 │
│                                                                     │
│  src/astock/business_engines/truth/truth_engine.py                  │
│    └─ DataLoadedEvent, DataTransformedEvent                         │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### 3.3 事件订阅 (插件系统)

```python
# pipeline/plugins/logging_plugin.py
def register():
    bus = EventBus.get()

    @bus.on('pipeline.node.started')
    def on_node_start(event):
        print(f"[PLUGIN] -> node {event.step_name}")

    @bus.on('pipeline.node.completed')
    def on_node_complete(event):
        print(f"[PLUGIN] <- node {event.step_name} duration={event.duration_ms:.0f}ms")
```

---

## 四、缓存机制

### 4.1 缓存判定逻辑

```python
# kedro_engine.py 中的缓存逻辑

# 1. 计算节点签名
signature_components = [
    "|".join(method_list),           # 方法链
    method_meta_str,                  # 方法版本信息
    str(param_items),                 # 参数
    "|".join(sorted(upstream_fps))    # 上游数据指纹
]
node_signature = "#".join(signature_components)

# 2. 判定缓存命中
if (
    planned_outputs                                      # 有输出
    and all(o in self.global_catalog for o in planned_outputs)  # 输出存在
    and last_sig == node_signature                       # 签名匹配
    and not ttl_expired                                  # 未过期
):
    # 缓存命中 → 跳过执行
    self._event_bus.emit(CacheHitEvent(...))
    return cached_outputs
```

### 4.2 持久化缓存

```
.pipeline/cache/
├── datasets/                  # 数据集缓存 (pickle)
│   ├── Load_Financial_Data__Raw_Data.pkl
│   └── Analyze_ROIC_Trend__ROIC_Trend_Result.pkl
├── node_signatures.json       # 节点签名记录
└── datasets_index.json        # 数据集索引
```

---

## 五、完整执行流程时序图

```
User                CLI              ExecuteManager        PrefectEngine        KedroEngine          Orchestrator         BusinessEngine
  │                  │                     │                    │                    │                     │                    │
  │ run -c yaml      │                     │                    │                    │                     │                    │
  ├─────────────────▶│                     │                    │                    │                     │                    │
  │                  │ new(config)         │                    │                    │                     │                    │
  │                  ├────────────────────▶│                    │                    │                     │                    │
  │                  │                     │ new()              │                    │                     │                    │
  │                  │                     ├───────────────────────────────────────────────────────────────▶│                    │
  │                  │                     │                    │                    │                     │ auto_load()        │
  │                  │                     │                    │                    │                     ├───────────────────▶│
  │                  │                     │                    │                    │                     │ @register_method   │
  │                  │                     │                    │                    │                     │◀───────────────────┤
  │                  │                     │◀─────────────────────────────────────────────────────────────────────────────────────┤
  │                  │                     │                    │                    │                     │                    │
  │                  │ execute_pipeline()  │                    │                    │                     │                    │
  │                  ├────────────────────▶│                    │                    │                     │                    │
  │                  │                     │ load_config()      │                    │                     │                    │
  │                  │                     ├────┐               │                    │                     │                    │
  │                  │                     │    │ parse YAML    │                    │                     │                    │
  │                  │                     │◀───┘               │                    │                     │                    │
  │                  │                     │                    │                    │                     │                    │
  │                  │                     │ FlowExecutor.run() │                    │                     │                    │
  │                  │                     ├───────────────────▶│                    │                     │                    │
  │                  │                     │                    │ build_hybrid_flow()│                     │                    │
  │                  │                     │                    ├───────────────────▶│                     │                    │
  │                  │                     │                    │                    │                     │                    │
  │                  │                     │              ┌─────┴─────────────────────┴─────────────────────┴────────────────────┴────┐
  │                  │                     │              │                    FOR EACH STEP                                          │
  │                  │                     │              ├─────┬─────────────────────┬─────────────────────┬────────────────────┬────┤
  │                  │                     │                    │ execute_node()      │                     │                    │
  │                  │                     │                    ├────────────────────▶│                     │                    │
  │                  │                     │                    │                    │ emit(NodeStarted)   │                    │
  │                  │                     │                    │                    ├────────────────────▶│                    │
  │                  │                     │                    │                    │                     │                    │
  │                  │                     │                    │                    │ execute_with_engine()                    │
  │                  │                     │                    │                    ├────────────────────▶│                    │
  │                  │                     │                    │                    │                     │ callable(**params) │
  │                  │                     │                    │                    │                     ├───────────────────▶│
  │                  │                     │                    │                    │                     │     result         │
  │                  │                     │                    │                    │                     │◀───────────────────┤
  │                  │                     │                    │                    │◀────────────────────┤                    │
  │                  │                     │                    │                    │                     │                    │
  │                  │                     │                    │                    │ store to catalog    │                    │
  │                  │                     │                    │                    ├────┐                │                    │
  │                  │                     │                    │                    │    │                │                    │
  │                  │                     │                    │                    │◀───┘                │                    │
  │                  │                     │                    │                    │                     │                    │
  │                  │                     │                    │                    │ emit(NodeCompleted) │                    │
  │                  │                     │                    │                    ├────────────────────▶│                    │
  │                  │                     │              └─────┴─────────────────────┴─────────────────────┴────────────────────┴────┘
  │                  │                     │                    │                    │                     │                    │
  │                  │                     │◀───────────────────┤                    │                     │                    │
  │                  │◀────────────────────┤                    │                    │                     │                    │
  │◀─────────────────┤                     │                    │                    │                     │                    │
  │   结果            │                     │                    │                    │                     │                    │
```

---

## 六、关键代码位置速查

| 功能 | 文件 | 关键函数/类 |
|------|------|------------|
| CLI 入口 | pipeline/cli.py | `AStockCLI.cmd_run()` |
| 管理器 | pipeline/core/execute_manager.py | `ExecuteManager` |
| 配置解析 | pipeline/core/services/config_service.py | `ConfigService.load_config()` |
| Flow 执行 | pipeline/core/services/flow_executor.py | `FlowExecutor.run()` |
| Prefect 编排 | pipeline/engines/prefect_engine.py | `PrefectEngine.build_hybrid_flow()` |
| Kedro 执行 | pipeline/engines/kedro_engine.py | `KedroEngine._create_kedro_node()` |
| 引用解析 | pipeline/core/context.py | `REF_PATTERN`, `PipelineContext` |
| 编排器 | orchestrator/orchestrator.py | `AStockOrchestrator` |
| 注册表 | orchestrator/registry/registry.py | `Registry` |
| 方法注册 | orchestrator/decorators/register.py | `@register_method` |
| 事件总线 | shared/event_bus/bus.py | `EventBus` |
| 事件定义 | shared/event_bus/events.py | 各种 `*Event` 类 |
| 业务引擎 | src/astock/business_engines/*/engine.py | 各业务函数 |

---

## 七、数据存储路径

```
data/
├── 10yd_base/                    # 原始财务数据 (10年)
│   └── YYYYMMDD_fina_indicator.csv
├── polars/
│   └── 10yd_final_industry.csv   # 聚合后的数据 (带行业标签)
├── filter_middle/                # 趋势分析中间结果
│   ├── roic_trend_analysis.csv
│   ├── roe_trend_analysis.csv
│   └── ...
└── comprehensive_analysis_report.md  # 最终报告

.pipeline/
├── cache/                        # 执行缓存
│   ├── datasets/                 # 数据集 pickle
│   ├── node_signatures.json      # 节点签名
│   └── datasets_index.json       # 索引
└── failures/                     # 失败记录 (用于 --resume)
```
