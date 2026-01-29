# Pipeline 2.0 - 企业级工作流执行引擎

> 🚀 高性能、类型安全、可扩展的 Python 工作流编排框架

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Version 2.1.0](https://img.shields.io/badge/version-2.1.0-green.svg)](./CHANGELOG.md)
[![Tests 46/46](https://img.shields.io/badge/tests-46%2F46%20passed-brightgreen.svg)](../test_pipeline_complete.py)
[![Last Run](https://img.shields.io/badge/last%20run-148s%20%7C%2022%20tasks-blue.svg)](../workflow/analysis.yaml)

---

## 📑 目录

- [概述](#-概述)
- [架构设计](#-架构设计)
- [快速开始](#-快速开始)
- [核心模块](#-核心模块)
  - [Core - 核心模型](#1-core---核心模型)
  - [Events - 事件总线](#2-events---事件总线)
  - [Aggregation - PDDA 数据聚合](#3-aggregation---pdda-数据聚合)
  - [Container - 依赖注入](#4-container---依赖注入)
  - [Catalog - 数据目录](#5-catalog---数据目录)
  - [Cache - 缓存系统](#6-cache---缓存系统)
  - [Config - 配置加载](#7-config---配置加载)
  - [Execution - 执行引擎](#8-execution---执行引擎)
  - [Protocols - 协议层](#9-protocols---协议层)
- [YAML 工作流配置](#-yaml-工作流配置)
- [高级用法](#-高级用法)
- [最佳实践](#-最佳实践)
- [API 参考](#-api-参考)
- [故障排查](#-故障排查)

---

## 🎯 概述

Pipeline 2.0 是专为 **AStock Analysis** 项目设计的企业级工作流执行引擎。它提供：

| 特性 | 描述 |
|------|------|
| **声明式工作流** | YAML 定义任务依赖、参数、输出 |
| **智能依赖解析** | 自动解析 `steps.X.outputs.Y` 引用 |
| **PDDA 数据聚合** | Producer-Driven Data Aggregation 零配置聚合 |
| **层级事件系统** | 支持通配符订阅 (`task.*`) 和优先级 |
| **依赖注入** | 三种生命周期 (Singleton/Scoped/Transient) |
| **血缘追踪** | 自动记录数据流向，支持 Mermaid 可视化 |
| **多级缓存** | Memory → File → DuckDB 分层缓存 |
| **类型安全** | 完整泛型支持 + 运行时验证 |

### 设计参考

- **Apache Airflow** - DAG 任务编排
- **Dagster** - 软件定义资产 (Software-Defined Assets)
- **Prefect** - 函数式任务定义
- **Luigi** - 依赖解析
- **Kedro** - 数据管道

---

## 🏗️ 架构设计

### 整体架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Pipeline 2.0 Architecture                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   workflow/*.yaml                    ← 声明式工作流配置                   │
│        │                                                                 │
│        ▼                                                                 │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐              │
│   │   Config    │────▶│    Core     │────▶│  Execution  │              │
│   │   Loader    │     │  (Spec/DAG) │     │   Runner    │              │
│   └─────────────┘     └─────────────┘     └─────────────┘              │
│                              │                   │                       │
│                              ▼                   ▼                       │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐              │
│   │  Container  │◀───▶│   Events    │◀───▶│   Catalog   │              │
│   │    (DI)     │     │    Bus      │     │   (Data)    │              │
│   └─────────────┘     └─────────────┘     └─────────────┘              │
│         │                   │                   │                       │
│         ▼                   ▼                   ▼                       │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐              │
│   │ Aggregation │     │  Middleware │     │    Cache    │              │
│   │   (PDDA)    │     │   Chain     │     │  (Tiered)   │              │
│   └─────────────┘     └─────────────┘     └─────────────┘              │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 模块依赖图

```
                    ┌──────────────┐
                    │  protocols/  │  ← 协议定义层 (无依赖)
                    └──────┬───────┘
                           │
            ┌──────────────┼──────────────┐
            ▼              ▼              ▼
     ┌──────────┐   ┌──────────┐   ┌──────────┐
     │  core/   │   │ events/  │   │  cache/  │
     │(Spec/DAG)│   │  (Bus)   │   │(Backends)│
     └────┬─────┘   └────┬─────┘   └────┬─────┘
          │              │              │
          └──────────────┼──────────────┘
                         ▼
              ┌─────────────────────┐
              │    aggregation/     │
              │ (Scope/Collector)   │
              └──────────┬──────────┘
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
       ┌──────────┐ ┌──────────┐ ┌──────────┐
       │ catalog/ │ │ config/  │ │execution/│
       │ (Data)   │ │ (YAML)   │ │ (Runner) │
       └──────────┘ └──────────┘ └──────────┘
```

### 数据流

```
YAML Config ──parse──▶ FlowSpec ──build──▶ DAG ──plan──▶ ExecutionPlan
                           │                                   │
                           ▼                                   ▼
                      TaskSpec[]                        ExecutionLayer[]
                           │                                   │
                           └──────────▶ TaskExecutor ◀─────────┘
                                              │
                              ┌───────────────┼───────────────┐
                              ▼               ▼               ▼
                         EventBus        DataCatalog      Collector
                              │               │               │
                              ▼               ▼               ▼
                         task.* events    Data Entries    Aggregated Results
```

---

## 🚀 快速开始

### 安装

```bash
# 项目已包含 pipeline 模块，无需额外安装
cd AStock-Analysis
pip install -r requirements.txt
```

### CLI 命令行 (推荐)

```bash
# 执行完整工作流
python -m pipeline run -c workflow/analysis.yaml

# 仅执行指定任务 (自动包含依赖)
python -m pipeline run -c workflow/analysis.yaml --only Analyze_ROIC_Trend

# 从指定任务恢复执行 (跳过上游已完成任务)
python -m pipeline run -c workflow/analysis.yaml --resume-from Run_Evaluators

# 排除某些任务
python -m pipeline run -c workflow/analysis.yaml --exclude Generate_Truth_Report

# 验证 YAML 配置 (不执行)
python -m pipeline validate -c workflow/analysis.yaml

# 可视化依赖图
python -m pipeline graph -c workflow/analysis.yaml
python -m pipeline graph -c workflow/analysis.yaml --format dot -o dag.dot

# 查看已注册的方法
python -m pipeline engines

# 缓存管理
python -m pipeline cache --stats
python -m pipeline cache --clear
```

### CLI 完整参数

| 命令 | 参数 | 说明 |
|------|------|------|
| `run` | `-c, --config` | YAML 配置文件路径 (必需) |
| | `--only TASK [TASK...]` | 仅执行指定任务及其依赖 |
| | `--exclude TASK [TASK...]` | 排除指定任务 |
| | `--resume-from TASK` | 从指定任务恢复执行 |
| | `--soft-fail` | 任务失败时继续执行后续任务 |
| | `--parallel` | 启用并行执行 |
| | `--workers N` | 并行 worker 数量 (默认: 4) |
| | `--dry-run` | 预演模式，仅验证不执行 |
| | `--no-skip-cached` | 不跳过缓存命中的任务 |
| | `-o, --output FILE` | 输出执行结果到 JSON 文件 |
| `validate` | `-c, --config` | YAML 配置文件路径 |
| | `--strict` | 启用严格验证模式 |
| `graph` | `-c, --config` | YAML 配置文件路径 |
| | `--format` | 输出格式: `ascii` (默认) / `dot` / `json` |
| | `-o, --output FILE` | 输出到文件 |
| `engines` | `--format` | 输出格式: `table` (默认) / `json` |
| `cache` | `--clear` | 清除所有缓存 |
| | `--stats` | 显示缓存统计信息 |

### Python API

```python
from pipeline import load_flow, FlowRunner
from pipeline.core.container import get_container

# 1. 加载工作流
spec = load_flow("workflow/analysis.yaml")

# 2. 获取容器
container = get_container()

# 3. 创建运行器并执行
runner = FlowRunner(container=container)
result = runner.run(spec)

# 4. 检查结果
print(f"状态: {result.state}")  # FlowState.SUCCESS
```

### 从字符串加载

```python
from pipeline import load_flow_string

yaml_content = """
name: my_flow
version: "1.0.0"
tasks:
  - name: hello
    component: business_engine
    engine: duckdb
    method: [say_hello]
    parameters:
      message: "Hello, Pipeline!"
"""

spec = load_flow_string(yaml_content)
```

---

## 📦 核心模块

### 1. Core - 核心模型

> 📁 `pipeline/core/`

#### FlowSpec & TaskSpec

定义工作流和任务的规范。

```python
from pipeline.core.spec import FlowSpec, TaskSpec, TaskInputSpec, TaskOutputSpec

# 创建任务规范
task = TaskSpec(
    name="analyze_roic",
    component="business_engine",
    engine="duckdb",
    method=["analyze_metric_trend"],
    parameters={
        "metric_name": "roic",
        "window_size": 5,
    },
    inputs=[
        TaskInputSpec(name="data", source="steps.load_data.outputs.parameters.raw")
    ],
    outputs=[
        TaskOutputSpec(name="trend_result")
    ],
    depends_on=["load_data"],  # 显式依赖
)

# 创建流程规范
flow = FlowSpec(
    name="financial_analysis",
    version="1.0.0",
    description="财务数据分析流程",
    tasks=[task],
)
```

#### State 状态机

```python
from pipeline.core.state import TaskState, FlowState

# 任务状态
TaskState.PENDING    # 等待执行
TaskState.RUNNING    # 正在执行
TaskState.SUCCESS    # 执行成功
TaskState.FAILED     # 执行失败
TaskState.SKIPPED    # 跳过执行
TaskState.CACHED     # 缓存命中
TaskState.RETRYING   # 等待重试
TaskState.CANCELLED  # 已取消

# 状态转换图
#   PENDING ──┬──→ RUNNING ──┬──→ SUCCESS
#             │              ├──→ FAILED ──→ RETRYING ──→ RUNNING
#             │              └──→ CANCELLED
#             ├──→ SKIPPED
#             └──→ CACHED
```

#### DAG 依赖图

```python
from pipeline.core.dag import DAG
from pipeline.core.spec import FlowSpec

# 从 FlowSpec 构建 DAG
dag = DAG.from_flow_spec(flow_spec)

# 获取执行计划 (分层)
plan = dag.get_execution_plan()

for layer in plan:
    print(f"Layer {layer.level}: {list(layer.tasks)}")
    # Layer 0: ['load_data']
    # Layer 1: ['analyze_roic', 'analyze_roe']  # 可并行
    # Layer 2: ['generate_report']

# 获取执行顺序 (串行)
order = plan.get_execution_order()
# ['load_data', 'analyze_roic', 'analyze_roe', 'generate_report']

# 查询依赖关系
dag.get_upstream("analyze_roic")    # {'load_data'}
dag.get_downstream("load_data")     # {'analyze_roic', 'analyze_roe'}
```

#### RetryPolicy 重试策略

```python
from pipeline.core.policy import RetryPolicy

policy = RetryPolicy(
    max_attempts=3,           # 最大尝试次数
    delay_seconds=1.0,        # 初始延迟
    backoff="exponential",    # none/linear/exponential/fibonacci
    backoff_multiplier=2.0,   # 退避系数
    max_delay_seconds=60.0,   # 最大延迟
    jitter_seconds=0.5,       # 随机抖动
)

# 计算第 N 次重试的延迟
delay = policy.get_delay(attempt=2)  # 2.0 秒 (1.0 * 2^1)
```

---

### 2. Events - 事件总线

> 📁 `pipeline/events/`

企业级事件发布/订阅系统，支持层级路由和优先级。

#### 基础用法

```python
from pipeline.events.bus import EventBus, Event, Priority

# 创建事件总线
bus = EventBus()

# 订阅事件
def on_task_completed(event: Event):
    print(f"任务 {event.payload['task_name']} 完成")

bus.subscribe("task.completed", on_task_completed)

# 发布事件
event = Event(type="task.completed", payload={"task_name": "analyze_roic"})
bus.emit(event)
```

#### 优先级订阅

```python
from pipeline.events.bus import Priority

# 高优先级 (先执行)
bus.subscribe("task.*", high_priority_handler, priority=Priority.HIGH)

# 普通优先级 (默认)
bus.subscribe("task.*", normal_handler, priority=Priority.NORMAL)

# 低优先级 (后执行)
bus.subscribe("task.*", low_priority_handler, priority=Priority.LOW)

# 监控级 (最后执行，用于日志/指标)
bus.subscribe("task.*", monitor_handler, priority=Priority.MONITOR)
```

#### 层级路由 (通配符)

```python
# 订阅所有 task 事件
bus.subscribe("task.*", lambda e: print(f"Task event: {e.type}"))

# 订阅所有 flow 事件
bus.subscribe("flow.*", lambda e: print(f"Flow event: {e.type}"))

# 发布 task.completed.success 会触发:
# - "task.completed.success" 的订阅者
# - "task.completed.*" 的订阅者
# - "task.*" 的订阅者
```

#### 装饰器 API

```python
# 使用装饰器订阅
@bus.on("task.completed")
def handle_completion(event: Event):
    print(f"Task completed: {event.payload}")

# 只订阅一次
@bus.once("flow.started")
def handle_first_flow(event: Event):
    print("First flow started!")
```

#### 中间件

```python
from pipeline.events.middleware import LoggingMiddleware, MetricsMiddleware

# 添加日志中间件
bus.use(LoggingMiddleware())

# 添加指标中间件
bus.use(MetricsMiddleware())
```

#### 预定义事件类型

```python
from pipeline.events.types import FlowEvents, TaskEvents, DataEvents

# 创建标准事件
event = TaskEvents.completed(
    task_id="task-123",
    task_name="analyze_roic",
    duration_ms=1500.0,
)

# 事件类型常量
FlowEvents.STARTED      # "flow.started"
FlowEvents.COMPLETED    # "flow.completed"
TaskEvents.STARTED      # "task.started"
TaskEvents.COMPLETED    # "task.completed"
DataEvents.LOADED       # "data.loaded"
DataEvents.SAVED        # "data.saved"
```

---

### 3. Aggregation - PDDA 数据聚合

> 📁 `pipeline/aggregation/`

**PDDA (Producer-Driven Data Aggregation)** - 零配置数据聚合框架。

#### 核心概念

```
┌─────────────────────────────────────────────────────────────────┐
│                        PDDA Architecture                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   Producer Tasks                        Consumer Tasks           │
│   ┌─────────────┐                      ┌─────────────┐          │
│   │ analyze_A() │──┐                ┌──│ run_eval()  │          │
│   └─────────────┘  │  Collector     │  └─────────────┘          │
│   ┌─────────────┐  │  ┌───────┐     │                           │
│   │ analyze_B() │──┼─▶│ Scope │─────┤  @inject                  │
│   └─────────────┘  │  └───────┘     │  aggregated_trends: Dict  │
│   ┌─────────────┐  │                │  ┌─────────────┐          │
│   │ analyze_C() │──┘                └──│ gen_report()│          │
│   └─────────────┘                      └─────────────┘          │
│                                                                  │
│   return AggregatableResult            自动注入聚合数据          │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

#### AggregatableResult

```python
from pipeline.aggregation.core import AggregatableResult
import pandas as pd

# 基础创建
result = AggregatableResult(key="roic", value=df)

# 链式 API (推荐)
result = (
    AggregatableResult
    .of("roic", df)
    .in_namespace("trends")
    .with_metadata(producer="analyze_roic", tags={"version": "1.0"})
)

# 泛型支持
result: AggregatableResult[str, pd.DataFrame] = AggregatableResult.of("roic", df)
```

#### ScopeManager & AggregationScope

```python
from pipeline.aggregation.core import ScopeManager, AggregationScope

# 创建作用域管理器
manager = ScopeManager()

# 使用上下文管理器创建作用域
with manager.create(flow_id="flow-123") as scope:
    # 存储数据
    scope.set("trends", "roic", roic_df)
    scope.set("trends", "roe", roe_df)

    # 获取数据
    roic = scope.get("trends", "roic")

    # 获取整个命名空间
    all_trends = scope.get_namespace("trends")
    # {"roic": roic_df, "roe": roe_df}
```

#### Collector 流式 API

```python
from pipeline.aggregation.core import Collector

with manager.create(flow_id="flow-123") as scope:
    collector = Collector(scope)

    # 流式 API
    (collector
        .namespace("trends")
        .producer("analyze_trends")
        .put("roic", roic_df)
        .put("roe", roe_df)
        .put("roiic", roiic_df)
        .done())

    # 批量存储
    collector.put_many({
        "gross_margin": gm_df,
        "net_margin": nm_df,
    })

    # 存储 AggregatableResult
    collector.put_result(AggregatableResult.of("revenue", rev_df))
```

#### Injector 自动注入

```python
from pipeline.aggregation.inject import Injector, inject, Aggregated

# 创建注入器
injector = Injector(collector)

# 使用装饰器自动注入
@inject(injector)
def run_evaluator(
    aggregated_trends: Aggregated[str, pd.DataFrame] = None,
    config: dict = None,
):
    """aggregated_trends 会自动注入 "trends" 命名空间的所有数据"""
    for metric_name, df in aggregated_trends.items():
        process(df)
```

#### 冲突策略

```python
from pipeline.aggregation.core import ConflictStrategy, AggregationScope

scope = AggregationScope(
    flow_id="flow-123",
    conflict_strategy=ConflictStrategy.ERROR,  # 默认: 抛出异常
)

# 可选策略
ConflictStrategy.ERROR    # 键冲突时抛异常
ConflictStrategy.REPLACE  # 新值覆盖旧值
ConflictStrategy.KEEP     # 保留旧值
ConflictStrategy.MERGE    # 尝试合并 (DataFrame concat, dict update)
```

#### 血缘追踪

```python
from pipeline.aggregation.lineage import LineageTracker, DataLineage

tracker = LineageTracker()

# 开始追踪
tracker.start_flow("flow-123")

# 记录生产
tracker.track_produce("Load_Data", "raw", "data", {"rows": 1000})

# 记录消费
tracker.track_consume("Analyze", sources=["raw.data"])

# 获取血缘图
lineage = tracker.get_lineage()

# 导出为 Mermaid
print(lineage.to_mermaid())
# graph LR
#   Load_Data -->|produces| raw.data
#   raw.data -->|consumed by| Analyze

# 结束追踪
tracker.end_flow()
```

---

### 4. Container - 依赖注入

> 📁 `pipeline/core/container.py`

企业级 IoC (控制反转) 容器。

#### 生命周期

```python
from pipeline.core.container import Container, Lifecycle

container = Container()

# Singleton - 整个应用生命周期内只创建一次
container.register(EventBus, lifecycle=Lifecycle.SINGLETON)

# Scoped - 每个 FlowRun 一个实例
container.register(Collector, lifecycle=Lifecycle.SCOPED)

# Transient - 每次请求创建新实例
container.register(TaskExecutor, lifecycle=Lifecycle.TRANSIENT)
```

#### 服务注册

```python
# 类型注册
container.register(IEventBus, EventBus, Lifecycle.SINGLETON)

# 工厂注册
container.register_factory(
    DataCatalog,
    lambda c: DataCatalog(event_bus=c.resolve(EventBus)),
    Lifecycle.SINGLETON,
)

# 链式注册
(container
    .register(EventBus)
    .register(DataCatalog)
    .register(ScopeManager))
```

#### 服务解析

```python
# 解析单例
bus = container.resolve(EventBus)
bus2 = container.resolve(EventBus)
assert bus is bus2  # 同一实例

# 使用作用域
with container.create_scope() as scope:
    collector = scope.resolve(Collector)
    collector2 = scope.resolve(Collector)
    assert collector is collector2  # 同一作用域内相同

with container.create_scope() as scope2:
    collector3 = scope2.resolve(Collector)
    assert collector3 is not collector  # 不同作用域不同
```

#### 全局容器

```python
from pipeline.core.container import get_container

# 获取预配置的全局容器
container = get_container()

# 预注册的服务
bus = container.resolve(EventBus)          # Singleton
catalog = container.resolve(DataCatalog)   # Singleton
scope_mgr = container.resolve(ScopeManager) # Singleton
```

---

### 5. Catalog - 数据目录

> 📁 `pipeline/catalog/`

统一的数据存储和检索中心。

#### DataCatalog

```python
from pipeline.catalog.catalog import DataCatalog
from pipeline.catalog.entry import DatasetType

catalog = DataCatalog()

# 保存数据
catalog.save(
    key="analyze_roic.trend_result",
    value=result_df,
    dataset_type=DatasetType.MEMORY,
    namespace="trends",
    ttl_seconds=3600,  # 1小时过期
)

# 加载数据
df = catalog.load("analyze_roic.trend_result")
df = catalog.load("nonexistent", default=pd.DataFrame())  # 默认值

# 检查存在
if catalog.exists("analyze_roic.trend_result"):
    ...

# 删除
catalog.delete("analyze_roic.trend_result")

# 按命名空间查询
all_trends = catalog.get_by_namespace("trends")
```

#### DataEntry

```python
from pipeline.catalog.entry import DataEntry, DatasetType, EntryStatus, LineageInfo

entry = DataEntry(
    key="my_data",
    value=df,
    dataset_type=DatasetType.MEMORY,
    status=EntryStatus.AVAILABLE,
    lineage=LineageInfo(
        source_task="analyze_roic",
        source_outputs=("trend_result",),
    ),
    metadata={"rows": 1000, "columns": 5},
)

# 数据集类型
DatasetType.MEMORY    # 内存
DatasetType.CSV       # CSV 文件
DatasetType.PARQUET   # Parquet 文件
DatasetType.DUCKDB    # DuckDB 表
DatasetType.JSON      # JSON 文件

# 状态
EntryStatus.PENDING   # 待填充
EntryStatus.AVAILABLE # 可用
EntryStatus.EXPIRED   # 已过期
EntryStatus.ERROR     # 错误
```

---

### 6. Cache - 缓存系统

> 📁 `pipeline/cache/`

分层缓存系统，支持 Memory → File 两级缓存。

#### MemoryCacheBackend

```python
from pipeline.cache.backends import MemoryCacheBackend

cache = MemoryCacheBackend(max_size=1000)

# 基础操作
cache.set("key", value, ttl=3600)  # TTL 秒
value = cache.get("key")
cache.delete("key")
cache.exists("key")
cache.clear()

# 统计
stats = cache.get_stats()
# {"size": 100, "hits": 500, "misses": 50, "hit_rate": 0.909}
```

#### FileCacheBackend

```python
from pipeline.cache.backends import FileCacheBackend

cache = FileCacheBackend(cache_dir=".cache/pipeline")

# 自动序列化/反序列化
cache.set("large_df", df)
df = cache.get("large_df")
```

#### TieredCacheBackend

```python
from pipeline.cache.backends import TieredCacheBackend

cache = TieredCacheBackend(
    l1_max_size=100,              # L1 内存缓存大小
    l2_cache_dir=".cache/pipeline", # L2 文件缓存目录
    warmup_keys=["hot_key_1"],    # 启动时预热
    write_through=True,           # 写穿透
)

# 读时提升: L2 命中后自动提升到 L1
# 写穿透: 同时写入 L1 和 L2
cache.set("key", value)
value = cache.get("key")  # 优先 L1，miss 则查 L2 并提升
```

---

### 7. Config - 配置加载

> 📁 `pipeline/config/`

YAML 配置解析和引用解析。

#### 加载工作流

```python
from pipeline.config.loader import load_flow, load_flow_string, YAMLLoader

# 从文件加载
spec = load_flow("workflow/analysis.yaml")

# 从字符串加载
spec = load_flow_string(yaml_content)

# 使用 Loader 类
loader = YAMLLoader()
spec = loader.load("workflow/analysis.yaml")
```

#### 引用解析

```python
from pipeline.config.resolver import ReferenceResolver

resolver = ReferenceResolver()

# 解析引用表达式
ref = resolver.parse("steps.load_data.outputs.parameters.raw_data")
# ref.source_task = "load_data"
# ref.output_name = "raw_data"

# 检查是否为引用
is_ref = resolver.is_reference("steps.X.outputs.Y")  # True
```

---

### 8. Execution - 执行引擎

> 📁 `pipeline/execution/`

工作流执行核心。

#### FlowRunner

```python
from pipeline.execution.runner import FlowRunner, RunnerConfig

# 配置
config = RunnerConfig(
    parallel=True,       # 并行执行同层任务
    soft_fail=False,     # 任务失败是否继续
    dry_run=False,       # 预演模式
    max_workers=4,       # 最大并行数
)

# 创建运行器
runner = FlowRunner(
    container=container,
    config=config,
)

# 执行
result = runner.run(flow_spec)

# 预演 (不实际执行)
dry_result = runner.dry_run(flow_spec)
print(dry_result.execution_plan)
```

#### TaskExecutor

```python
from pipeline.execution.executor import TaskExecutor

executor = TaskExecutor(
    container=container,
    method_resolver=resolver,
)

# 执行单个任务
task_result = executor.execute(task_spec, context)
```

#### 执行中间件

```python
from pipeline.execution.middleware import ExecutionMiddlewareBase, ExecutionMiddlewareChain

class TimingMiddleware(ExecutionMiddlewareBase):
    def before_task(self, task, context):
        context['start_time'] = time.time()
        return context

    def after_task(self, task, result, context):
        duration = time.time() - context['start_time']
        print(f"Task {task.name} took {duration:.2f}s")
        return result

# 使用中间件链
chain = ExecutionMiddlewareChain()
chain.use(TimingMiddleware())
chain.use(LoggingMiddleware())
```

---

### 9. Protocols - 协议层

> 📁 `pipeline/protocols/`

基于 `typing.Protocol` 的接口定义，支持结构化子类型。

#### 核心协议

```python
from pipeline.protocols.core import (
    ExecutableProtocol,
    ExecutionResult,
    ExecutionStatus,
)

class MyTask(ExecutableProtocol):
    def execute(self, context: Dict[str, Any]) -> ExecutionResult:
        # 业务逻辑
        return ExecutionResult(
            status=ExecutionStatus.SUCCESS,
            data={"output": result},
        )
```

#### 领域协议

```python
from pipeline.protocols.domain import (
    TaskProtocol,
    TaskInfo,
    IOProtocol,
    InputSpec,
    OutputSpec,
)

# 任务信息
info = TaskInfo(
    name="analyze_roic",
    description="分析 ROIC 趋势",
    version="1.0.0",
)

# 输入输出规范
input_spec = InputSpec(name="data", required=True)
output_spec = OutputSpec(name="result")
```

#### 集成协议

```python
from pipeline.protocols.integration import (
    MethodResolverProtocol,
    MethodInfo,
    StorageBackendProtocol,
)

# 方法信息
method_info = MethodInfo(
    name="analyze_metric_trend",
    component="business_engine",
    engine="duckdb",
    description="8种探针多维趋势分析",
)
```

---

## 📝 YAML 工作流配置

### 完整示例

```yaml
# workflow/analysis.yaml
name: DuckDB财务基线筛选管道
version: "2.0.0"
description: A股财务数据分析工作流

# 全局配置
config:
  parallel: true
  soft_fail: false
  cache:
    enabled: true
    backend: tiered

# 任务定义
tasks:
  # ============================================
  # 数据加载
  # ============================================
  - name: Load_Financial_Data
    component: business_engine
    engine: duckdb
    method:
      - load_file
    parameters:
      file_path: "data/polars/10yd_final_industry.csv"
    outputs:
      parameters:
        - name: Raw_Data

  # ============================================
  # 趋势分析 (8个探针)
  # ============================================
  - name: Analyze_ROIC_Trend
    component: business_engine
    engine: duckdb
    method:
      - analyze_metric_trend
    depends_on:
      - Load_Financial_Data
    parameters:
      # 引用上游输出
      data: "steps.Load_Financial_Data.outputs.parameters.Raw_Data"
      metric_name: roic
      min_periods: 5
      window_size: 5
    outputs:
      parameters:
        - name: ROIC_Trend_Result

  - name: Analyze_ROE_Trend
    component: business_engine
    engine: duckdb
    method:
      - analyze_metric_trend
    depends_on:
      - Load_Financial_Data
    parameters:
      data: "steps.Load_Financial_Data.outputs.parameters.Raw_Data"
      metric_name: roe
    outputs:
      parameters:
        - name: ROE_Trend_Result

  # ============================================
  # 评估器 (消费聚合数据)
  # ============================================
  - name: Run_Evaluators
    component: business_engine
    engine: evaluator
    method:
      - run_comprehensive_evaluation
    depends_on:
      - Analyze_ROIC_Trend
      - Analyze_ROE_Trend
    parameters:
      # PDDA 自动注入 aggregated_trends
      config:
        enable_veto_rules: true
        enable_bonus_rules: true
    outputs:
      parameters:
        - name: Evaluation_Result

  # ============================================
  # 报告生成
  # ============================================
  - name: Generate_Report
    component: business_engine
    engine: reporting
    method:
      - generate_comprehensive_report
    depends_on:
      - Run_Evaluators
    parameters:
      evaluation_result: "steps.Run_Evaluators.outputs.parameters.Evaluation_Result"
      output_path: "data/comprehensive_analysis_report.md"
```

### 参数引用语法

```yaml
# 完整语法
data: "steps.{task_name}.outputs.parameters.{param_name}"

# 示例
data: "steps.Load_Financial_Data.outputs.parameters.Raw_Data"
config: "steps.Init_Config.outputs.parameters.Config"
```

### 重试配置

```yaml
- name: Fetch_Remote_Data
  component: business_engine
  engine: http
  method:
    - fetch_data
  retry:
    max_attempts: 3
    delay_seconds: 1.0
    backoff: exponential
```

---

## 🔧 高级用法

### 环境变量配置

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `ASTOCK_DEBUG` | `0` | 设为 `1` 启用详细调试日志 |
| `ASTOCK_VALIDATION_MODE` | `warn` | 注册签名校验: `strict`/`warn`/`off` |
| `PIPELINE_METHOD_RESOLVER` | `orchestrator.adapters.RegistryMethodResolver` | 自定义方法解析器类路径 |

```powershell
# PowerShell 设置环境变量
$env:ASTOCK_DEBUG = "1"
$env:PIPELINE_METHOD_RESOLVER = "mypackage.resolvers.CustomResolver"
python -m pipeline run -c workflow/analysis.yaml
```

### 自定义中间件

```python
from pipeline.execution.middleware import ExecutionMiddlewareBase

class MetricsMiddleware(ExecutionMiddlewareBase):
    def __init__(self, metrics_client):
        self.metrics = metrics_client

    def before_task(self, task, context):
        context['_start'] = time.time()
        self.metrics.increment(f"task.{task.name}.started")
        return context

    def after_task(self, task, result, context):
        duration = time.time() - context['_start']
        self.metrics.timing(f"task.{task.name}.duration", duration)

        if result.status == ExecutionStatus.SUCCESS:
            self.metrics.increment(f"task.{task.name}.success")
        else:
            self.metrics.increment(f"task.{task.name}.failure")

        return result

    def on_error(self, task, error, context):
        self.metrics.increment(f"task.{task.name}.error")
        raise error
```

### 自定义事件处理

```python
from pipeline.events.bus import EventBus, Event, Priority

bus = EventBus()

# 错误告警
@bus.on("task.failed", priority=Priority.HIGH)
def alert_on_failure(event: Event):
    send_alert(
        title=f"Task Failed: {event.payload['task_name']}",
        body=str(event.payload['error']),
    )

# 性能监控
@bus.on("task.completed", priority=Priority.MONITOR)
def track_performance(event: Event):
    record_metric(
        name=f"task_duration_{event.payload['task_name']}",
        value=event.payload['duration_ms'],
    )
```

### 条件执行

```python
from pipeline.core.spec import TaskSpec

task = TaskSpec(
    name="send_alert",
    component="notification",
    engine="email",
    method=["send"],
    # 条件: 仅在上游失败时执行
    condition="steps.critical_task.state == 'failed'",
)
```

### 并行执行控制

```yaml
config:
  parallel: true
  max_workers: 8

tasks:
  # 这些任务会并行执行 (无依赖关系)
  - name: Analyze_A
    ...
  - name: Analyze_B
    ...
  - name: Analyze_C
    ...
```

---

## ✅ 最佳实践

### 1. 任务设计

```python
# ✅ 好: 单一职责、明确输入输出
def analyze_metric_trend(data: pd.DataFrame, metric_name: str) -> AggregatableResult:
    result = compute_trend(data, metric_name)
    return AggregatableResult.of(metric_name, result).in_namespace("trends")

# ❌ 差: 做太多事情
def do_everything(data):
    load_data()
    analyze()
    save_report()
    send_email()
```

### 2. 依赖声明

```yaml
# ✅ 好: 通过参数引用自动推断依赖
parameters:
  data: "steps.load_data.outputs.parameters.raw"

# ✅ 也好: 显式声明
depends_on:
  - load_data

# ❌ 差: 硬编码路径
parameters:
  data: "data/temp/load_data_output.csv"
```

### 3. 命名空间隔离

```python
# ✅ 好: 使用命名空间分组
collector.namespace("trends").put("roic", df)
collector.namespace("evaluations").put("scores", scores)

# ❌ 差: 全部放 default
collector.put("roic_trend", df)
collector.put("roe_trend", df)
collector.put("scores", scores)
```

### 4. 错误处理

```python
# ✅ 好: 返回明确的结果
def my_task(...) -> ExecutionResult:
    try:
        result = process()
        return ExecutionResult(status=ExecutionStatus.SUCCESS, data=result)
    except ValueError as e:
        return ExecutionResult(status=ExecutionStatus.FAILED, error=str(e))

# ✅ 好: 配置重试策略
retry:
  max_attempts: 3
  backoff: exponential
```

### 5. 缓存策略

```yaml
# ✅ 好: 稳定计算启用缓存
- name: Heavy_Computation
  cache:
    enabled: true
    ttl_hours: 24

# ❌ 差: 实时数据不应缓存
- name: Fetch_Live_Prices
  cache:
    enabled: true  # 会导致数据过期
```

---

## 📖 API 参考

### 主要导出

```python
from pipeline import (
    # Config
    load_flow,
    load_flow_string,

    # Core
    FlowSpec,
    TaskSpec,
    TaskState,
    FlowState,
    DAG,
    RetryPolicy,

    # Events
    EventBus,
    Event,
    Priority,

    # Aggregation
    AggregatableResult,
    ScopeManager,
    Collector,
    Injector,
    inject,

    # Execution
    FlowRunner,
    RunnerConfig,
    TaskExecutor,

    # Cache
    MemoryCacheBackend,
    FileCacheBackend,
    TieredCacheBackend,

    # Catalog
    DataCatalog,
    DataEntry,
)

from pipeline.core.container import get_container, Container, Lifecycle
```

### 版本

```python
import pipeline
print(pipeline.__version__)  # "2.0.0"
```

---

## 🔍 故障排查

### 常见问题

#### 1. 循环依赖

```
CyclicDependencyError: Cyclic dependency detected: A -> B -> C -> A
```

**解决**: 检查 YAML 中的 `depends_on` 和参数引用，打破循环。

```bash
# 可视化依赖图
python -m pipeline graph -c workflow/analysis.yaml
```

#### 2. 缺失依赖

```
MissingDependencyError: Task 'analyze' depends on non-existent task 'load'
```

**解决**: 确保被引用的任务名称正确且存在。

#### 3. Scoped 服务在作用域外解析

```
RuntimeError: Scoped service Collector must be resolved within a scope.
```

**解决**: 使用 `container.create_scope()`:

```python
with container.create_scope() as scope:
    collector = scope.resolve(Collector)
```

#### 4. 键冲突

```
KeyConflictError: Key conflict in 'trends.roic': already produced by 'task_a'
```

**解决**: 使用不同的键，或配置冲突策略:

```python
scope = AggregationScope(conflict_strategy=ConflictStrategy.REPLACE)
```

### 调试技巧

```bash
# 启用调试日志
$env:ASTOCK_DEBUG="1"
python -m pipeline run -c workflow/analysis.yaml

# 查看已注册方法
python -m pipeline engines

# 验证配置
python -m pipeline validate -c workflow/analysis.yaml

# 生成依赖图 (支持 ascii/dot/json 格式)
python -m pipeline graph -c workflow/analysis.yaml --format dot -o dag.dot

# 清理缓存
python -m pipeline cache --clear
python -m pipeline cache --stats  # 查看缓存统计

# 任务过滤执行
python -m pipeline run -c workflow/analysis.yaml --only Analyze_ROIC_Trend Analyze_ROE_Trend
python -m pipeline run -c workflow/analysis.yaml --exclude Generate_Report
python -m pipeline run -c workflow/analysis.yaml --resume-from Run_Evaluators

# 输出执行结果到 JSON
python -m pipeline run -c workflow/analysis.yaml -o result.json
```

---

## 📊 性能指标

| 场景 | 性能 |
|------|------|
| YAML 解析 | < 50ms |
| DAG 构建 (100 tasks) | < 10ms |
| 事件发布/订阅 | < 0.1ms |
| 内存缓存 get/set | < 0.01ms |
| 作用域创建/销毁 | < 1ms |

### 实测工作流性能 (analysis.yaml)

| 指标 | 数值 |
|------|------|
| 任务总数 | 22 |
| 总耗时 | ~148 秒 |
| 平均单任务 | ~6.7 秒 |
| 数据行数 | ~4000 公司 × 10 年 |
| 输出文件 | 8 CSV + 2 Markdown 报告 |

---

## 📜 版本历史

### v2.1.0 (2026-01-29)

- ✨ CLI 增强：`--only`, `--exclude`, `--resume-from` 任务过滤
- ✨ 新增 `--no-skip-cached` 选项控制缓存行为
- ✨ 支持 `PIPELINE_METHOD_RESOLVER` 环境变量自定义方法解析器
- 🔧 优化 YAML orchestration 配置合并逻辑
- 📊 实测 22 任务工作流完成时间：148 秒

### v2.0.0 (2026-01-25)

- ✨ 全新 Events 模块 (层级路由、优先级、中间件)
- ✨ 全新 Aggregation 模块 (PDDA、血缘追踪)
- ✨ 依赖注入容器 (三种生命周期)
- ✨ 分层缓存系统
- ✨ 协议层 (Protocols)
- 🔧 重构所有模块，代码量减少 40%

### v1.0.0

- 初始版本

---

## 📄 License

MIT License - 详见 [LICENSE](../LICENSE)

---

<div align="center">

**Made with ❤️ for AStock Analysis**

[报告 Bug](https://github.com/Jusu2-teach/Stock-Analysis/issues) ·
[功能请求](https://github.com/Jusu2-teach/Stock-Analysis/issues)

</div>
