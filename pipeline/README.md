# AStock Pipeline 架构指南

> **版本**: 4.1 (2025-12) | **Python Package**: `v2.0.0`
> **状态**: Production Ready
> **架构核心**: 依赖图驱动调度 + Prefect-Kedro 混合执行 + MethodHandle 延迟绑定
> **关键词**: 显式依赖、拓扑排序、层次并行、可观测、可扩展

---

## 🆕 v2.0.0 新特性

| 特性 | 描述 |
|------|------|
| **依赖图可视化** | 支持 Mermaid、GraphViz、文本格式导出，CLI `graph` 命令 |
| **Context 状态管理** | `reset()`, `clone()`, `get_stats()` 方法，支持上下文复用与统计 |
| **HookManager 增强** | 事件统计、调试模式、动态注销，完整生命周期管理 |
| **模块导出优化** | `pipeline.core` 统一 API 入口，类型安全导入 |

---

## 📑 目录

1. [设计理念](#1-设计理念)
2. [架构总览](#2-架构总览)
3. [核心组件](#3-核心组件)
4. [依赖图系统](#4-依赖图系统-dependencygraph)
5. [混合执行引擎](#5-混合执行引擎-prefect--kedro)
6. [配置模型](#6-配置模型-yaml-schema)
7. [数据流与引用](#7-数据流与引用解析)
8. [缓存与签名](#8-缓存与签名机制)
9. [Hook 与插件](#9-hook-与插件体系)
10. [指标与血缘](#10-指标与血缘追踪)
11. [失败恢复](#11-失败恢复与软失败)
12. [扩展指南](#12-扩展指南)
13. [常用命令](#13-常用命令)
14. [设计决策记录](#14-设计决策记录-adr)

---

## 1. 设计理念

### 核心原则

| 原则 | 说明 | 实现 |
|------|------|------|
| **显式优先** | 依赖关系必须可追溯、可理解 | 支持 `depends_on` 显式声明 |
| **单一职责** | 每个组件只做一件事 | 服务分层、DependencyGraph 独立 |
| **开闭原则** | 对扩展开放，对修改关闭 | DependencySource 策略模式 |
| **依赖反转** | 高层不依赖底层实现 | PipelineContext 共享状态 |
| **智能抽象** | 仅在必要时引入复杂性 | MethodHandle 延迟绑定 |

### 设计目标

```
┌─────────────────────────────────────────────────────────────────┐
│                      设计目标金字塔                               │
├─────────────────────────────────────────────────────────────────┤
│                        可观测性                                  │
│                    ┌──────────────┐                             │
│                    │ 指标 / 血缘   │                             │
│                    └──────────────┘                             │
│                  可扩展性   │   可靠性                            │
│              ┌────────────┴────────────┐                        │
│              │ 插件体系 / 多引擎 / 策略   │                        │
│              └─────────────────────────┘                        │
│                      性能 (并行执行)                              │
│         ┌────────────────────────────────────┐                  │
│         │    层次并行 / 缓存 / 智能跳过         │                  │
│         └────────────────────────────────────┘                  │
│                    正确性 (依赖保证)                              │
│    ┌─────────────────────────────────────────────────┐          │
│    │   拓扑排序 / 循环检测 / 依赖验证 / 签名校验        │          │
│    └─────────────────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. 架构总览

### 系统分层

```
┌──────────────────────────────────────────────────────────────────────┐
│                           CLI Layer                                   │
│  pipeline/main.py: run | status | metrics | cache | engines          │
└────────────────────────────────┬─────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        Orchestration Layer                            │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐       │
│  │ ExecuteManager  │  │  ConfigService  │  │  FlowExecutor   │       │
│  │ (生命周期管理)    │  │  (配置解析)      │  │  (流程触发)      │       │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘       │
│           │                    │                    │                 │
│           └────────────────────┼────────────────────┘                 │
│                                │                                      │
│  ┌─────────────────────────────▼─────────────────────────────────┐   │
│  │                    PipelineContext                             │   │
│  │  config | steps | global_catalog | metrics | lineage           │   │
│  └───────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────┐
│                         Engine Layer                                  │
│  ┌───────────────────────┐      ┌───────────────────────┐            │
│  │     PrefectEngine     │      │     KedroEngine       │            │
│  │  - Flow 构建与调度      │─────▶│  - 节点执行            │            │
│  │  - 层次依赖管理         │      │  - 缓存与签名          │            │
│  │  - 任务重试与监控       │      │  - 数据目录管理        │            │
│  └───────────────────────┘      └───────────────────────┘            │
└──────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌──────────────────────────────────────────────────────────────────────┐
│                        Business Layer                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐       │
│  │  data_engine    │  │ business_engine │  │   orchestrator  │       │
│  │  (数据读写)      │  │  (业务逻辑)      │  │   (方法注册)     │       │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘       │
└──────────────────────────────────────────────────────────────────────┘
```

### 组件交互时序

```
User Request
     │
     ▼
┌────────────┐    load_config()     ┌──────────────┐
│    CLI     │──────────────────────▶│ConfigService │
└────────────┘                       └──────┬───────┘
     │                                      │
     │                                      │ parse_steps()
     │                                      ▼
     │                               ┌──────────────┐
     │                               │DependencyGraph│
     │                               │  - 拓扑排序    │
     │                               │  - 层次划分    │
     │                               └──────┬───────┘
     │                                      │
     │  execute()                           │ ExecutionPlan
     ▼                                      ▼
┌────────────┐    run_flow()        ┌──────────────┐
│ExecuteMgr  │──────────────────────▶│ FlowExecutor │
└────────────┘                       └──────┬───────┘
                                            │
                                            ▼
                                    ┌──────────────┐
                                    │PrefectEngine │
                                    │  Layer 0 ────▶ Layer 1 ────▶ Layer N
                                    └──────┬───────┘
                                            │
                                            ▼
                                    ┌──────────────┐
                                    │ KedroEngine  │
                                    │  - 签名计算   │
                                    │  - 缓存检查   │
                                    │  - 方法执行   │
                                    └──────────────┘
```

---

## 3. 核心组件

### 3.1 ExecuteManager

**职责**: Pipeline 执行的中枢神经，管理生命周期和服务编排。

```python
class ExecuteManager:
    """Pipeline 执行管理器

    核心功能:
    - 解析 YAML steps → 生成 Kedro 风格节点描述
    - 解析跨步引用 (steps.<step>.outputs.parameters.<name>)
    - 通过 PrefectEngine (内部封装 KedroEngine) 执行
    - 提供缓存/软失败/血缘/指标结果
    """

    def __init__(self, config_path, orchestrator):
        self.ctx = PipelineContext()           # 共享上下文
        self._config_service = ConfigService()  # 配置解析
        self._flow_executor = FlowExecutor()    # 流程执行
        self._result_assembler = ResultAssembler()  # 结果汇总
        self._cache_stats_service = CacheStatsService()  # 缓存统计
```

### 3.2 ConfigService

**职责**: 配置加载、解析、拓扑排序、节点构建。

```python
class ConfigService:
    """配置服务 (专业级实现)

    核心流程:
    1. load_config() → 解析 YAML
    2. _parse_steps() → 构建 StepSpec
    3. _build_dependency_graph() → 创建依赖图
    4. _compute_execution_order() → 拓扑排序
    """

    def get_execution_plan(self) -> ExecutionPlan:
        """获取执行计划，包含层次信息和关键路径"""
        return self._dependency_graph.build_execution_plan()
```

### 3.3 PipelineContext

**职责**: 跨服务共享状态，单一事实来源。

```python
@dataclass
class PipelineContext:
    """Pipeline 执行上下文"""
    config: Dict[str, Any]           # 原始 YAML 配置
    steps: Dict[str, StepSpec]       # 解析后的步骤规范
    execution_order: List[str]       # 拓扑排序后的执行顺序
    global_catalog: Dict[str, Any]   # 全局数据目录
    node_metrics: Dict[str, Any]     # 节点执行指标
    lineage: Dict[str, Any]          # 数据血缘信息
```

#### 🆕 v2.0.0 Context 新方法

```python
# 获取步骤数量
count = ctx.get_step_count()  # 15

# 获取统计信息
stats = ctx.get_stats()
# {
#   'step_count': 15,
#   'metric_count': 10,
#   'lineage_count': 10,
#   'catalog_size': 8,
#   'has_dependency_graph': True
# }

# 重置上下文 (保留结构)
ctx.reset()

# 克隆上下文 (深拷贝)
ctx_copy = ctx.clone()

# 存储/获取依赖图
ctx.set_dependency_graph(graph)
graph = ctx.get_dependency_graph()
```

---

## 4. 依赖图系统 (DependencyGraph)

### 设计架构

```
┌─────────────────────────────────────────────────────────────────┐
│                      DependencyGraph                             │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                   核心数据结构                            │    │
│  │  - adjacency: Dict[str, Set[str]]  # 邻接表              │    │
│  │  - edges: Set[DependencyEdge]      # 边集合              │    │
│  │  - nodes: Set[str]                 # 节点集合             │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                   │
│                              ▼                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                   核心算法                               │    │
│  │  - Kahn's Algorithm (拓扑排序)                          │    │
│  │  - DFS (循环检测)                                        │    │
│  │  - BFS (关键路径分析)                                    │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                              │
         ┌────────────────────┼────────────────────┐
         ▼                    ▼                    ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐
│DataDependency   │  │ExplicitDependency│  │ResourceDependency│
│Source           │  │Source            │  │Source (扩展)     │
│                 │  │                  │  │                  │
│从 inputs/outputs│  │从 depends_on    │  │从共享资源         │
│推导依赖         │  │声明解析依赖      │  │推导依赖          │
└─────────────────┘  └─────────────────┘  └─────────────────┘
         │                    │                    │
         └────────────────────┼────────────────────┘
                              ▼
                    ┌─────────────────┐
                    │ ExecutionPlan   │
                    │                 │
                    │ - layers        │
                    │ - critical_path │
                    │ - max_parallelism│
                    └─────────────────┘
```

### 依赖类型

```python
class DependencyType(Enum):
    """依赖类型枚举"""
    DATA = auto()       # 数据依赖：通过输入/输出数据集推导
    EXPLICIT = auto()   # 显式依赖：通过 depends_on 声明
    RESOURCE = auto()   # 资源依赖：共享资源（如数据库连接）
    TEMPORAL = auto()   # 时序依赖：时间窗口约束
```

### 🆕 依赖图可视化导出

v2.0.0 新增依赖图可视化能力，支持多种格式导出：

```python
from pipeline.core import DependencyGraph

# 构建依赖图
graph = DependencyGraph.from_step_configs(steps)

# Mermaid 格式 (适合 Markdown 文档)
mermaid_code = graph.to_mermaid()
# 输出:
# graph TD
#     Load_Data --> Process_Data
#     Process_Data --> Store_Result

# GraphViz DOT 格式 (适合高质量图片)
dot_code = graph.to_graphviz()

# 保存为文件
graph.save_visualization("deps.md", format="mermaid")
graph.save_visualization("deps.dot", format="graphviz")
graph.save_visualization("deps.txt", format="text")

# 获取摘要信息
summary = graph.get_summary()
# {
#   'total_nodes': 20,
#   'total_edges': 25,
#   'total_layers': 5,
#   'max_parallelism': 8,
#   'critical_path': ['Load_Data', 'Process', 'Store', 'Report'],
#   'critical_path_length': 4
# }
```

**CLI 命令**:
```bash
# 查看依赖图摘要
python -m pipeline.main graph -c config.yaml --summary

# 导出 Mermaid 格式
python -m pipeline.main graph -c config.yaml -f mermaid -o deps.md

# 导出 GraphViz 格式
python -m pipeline.main graph -c config.yaml -f graphviz -o deps.dot
```

### 层次执行示例

```
Layer 0 (无依赖):
├── Load_Financial_Data

Layer 1 (依赖 Layer 0):
├── Analyze_ROIC_Trend
├── Analyze_ROIIC_Trend
├── Analyze_Revenue_Trend
├── Analyze_Profit_Trend
├── Analyze_GrossMargin_Trend
├── Analyze_NetMargin_Trend
├── Analyze_ROE_Trend
└── Analyze_OCF_Trend

Layer 2 (依赖 Layer 1):
├── Score_ROIC_Quality

Layer 3 (依赖 Layer 1/2):
├── store_ROIC_Trend
├── store_ROIIC_Trend
├── store_Revenue_Trend
├── store_Profit_Trend
├── store_GrossMargin_Trend
├── store_NetMargin_Trend
├── store_ROE_Trend
├── store_OCF_Trend
└── store_ROIC_Quality

Layer 4 (depends_on 显式依赖所有 store):
└── Generate_Comprehensive_Report
```

### 核心算法: Kahn's Algorithm

```python
def topological_sort(self) -> List[str]:
    """使用 Kahn's 算法进行拓扑排序

    时间复杂度: O(V + E)
    空间复杂度: O(V)

    Returns:
        排序后的节点列表

    Raises:
        CyclicDependencyError: 检测到循环依赖
    """
    in_degree = {node: 0 for node in self.nodes}
    for edge in self.edges:
        in_degree[edge.to_node] += 1

    queue = deque([n for n in self.nodes if in_degree[n] == 0])
    result = []

    while queue:
        node = queue.popleft()
        result.append(node)
        for neighbor in self.adjacency[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(result) != len(self.nodes):
        cycle = self._find_cycle()
        raise CyclicDependencyError(cycle)

    return result
```

---

## 5. 混合执行引擎 (Prefect + Kedro)

### 架构理念

```
┌───────────────────────────────────────────────────────────────────┐
│                   Prefect-Kedro 混合引擎                           │
│                                                                   │
│   🎯 Prefect 职责:                 🏗️ Kedro 职责:                  │
│   - 工作流编排                      - 数据处理逻辑                  │
│   - 任务调度                        - 数据血缘追踪                  │
│   - 重试与监控                      - 缓存与签名                    │
│   - 并发控制                        - 方法链执行                    │
│                                                                   │
│   📊 优势结合:                                                     │
│   - Prefect 的可视化监控 + Kedro 的数据工程最佳实践                   │
│   - ConcurrentTaskRunner 支持层内并行                              │
│   - soft_fail 单任务失败不影响整体                                  │
│   - layer_metrics 输出每层任务数与耗时                              │
└───────────────────────────────────────────────────────────────────┘
```

### PrefectEngine 执行流程

```python
def _build_node_level_flow(self, config, kedro_nodes):
    """构建节点级 Flow

    1. 使用 DependencyGraph 构建统一依赖图
    2. 合并数据依赖和显式依赖
    3. 构建层次化执行计划
    4. 为每层创建 Prefect Task
    """

    # 构建依赖图
    graph = DependencyGraph.from_node_configs(
        nodes=node_configs,
        sources=[DataDependencySource(), ExplicitDependencySource()]
    )

    # 获取执行层次
    execution_plan = graph.build_execution_plan()

    # 按层执行
    for layer in execution_plan.layers:
        layer_tasks = [create_task(node) for node in layer.nodes]
        await gather(*layer_tasks)  # 层内并行
```

### KedroEngine 节点执行

```python
class KedroEngine:
    """Kedro 引擎 - 负责单节点执行

    核心功能:
    - 签名计算与缓存检查
    - 方法链执行 (多个 method 按序执行)
    - MethodHandle 延迟引擎解析
    - 输出注册与血缘记录
    """

    def execute_node(self, node_config):
        # 1. 计算签名
        signature = self._compute_signature(node_config)

        # 2. 检查缓存
        if self._check_cache(signature):
            return cached_result

        # 3. 执行方法链
        for method in node_config['methods']:
            result = self._execute_method(method, params)

        # 4. 注册输出
        self._register_outputs(result)

        # 5. 记录血缘
        self._record_lineage(inputs, outputs)
```

---

## 6. 配置模型 (YAML Schema)

### 完整配置结构

```yaml
pipeline:
  name: "Pipeline 名称"

  # 编排配置
  orchestration:
    granularity: node          # 粒度: node | pipeline
    task_runner: sequential    # 运行器: sequential | concurrent
    max_workers: 4             # 并发数
    soft_fail: true            # 软失败模式
    retry_count: 1             # 重试次数
    retry_delay: 10            # 重试延迟(秒)
    timeout: 1200              # 超时(秒)

  # 步骤定义
  steps:
    - name: "步骤名称"
      component: "business_engine"   # 组件类型
      engine: "duckdb"               # 引擎: duckdb | polars | pandas | auto
      method: ["method_name"]        # 方法列表

      # 参数配置
      parameters:
        param1: "value1"
        param2: "steps.other_step.outputs.parameters.result"  # 引用语法

      # 输出配置
      outputs:
        parameters:
          - name: Output_Name

      # 显式依赖
      depends_on:
        - "other_step_1"
        - "other_step_2"
```

### 参数引用语法

```yaml
# 基本引用格式
parameters:
  data: "steps.<step_name>.outputs.parameters.<param_name>"

# 示例
parameters:
  data: "steps.Load_Financial_Data.outputs.parameters.Raw_Data"
  config: "steps.Analyze_Trend.outputs.parameters.Config"
```

### 引擎选择策略

| 引擎 | 适用场景 | 特点 |
|------|----------|------|
| `duckdb` | SQL 分析、大数据聚合 | 高性能 OLAP |
| `polars` | 数据转换、IO 操作 | 向量化计算 |
| `pandas` | 兼容性要求高 | 生态完善 |
| `auto` | 动态决策 | MethodHandle 延迟绑定 |

---

## 7. 数据流与引用解析

### 引用解析流程

```
                    YAML 配置
                        │
                        ▼
         ┌──────────────────────────┐
         │    Reference Scanner     │
         │                          │
         │  扫描所有参数值中的引用:    │
         │  steps.X.outputs.params.Y │
         └────────────┬─────────────┘
                      │
                      ▼
         ┌──────────────────────────┐
         │   Dependency Builder     │
         │                          │
         │  构建 step → step 依赖图   │
         └────────────┬─────────────┘
                      │
                      ▼
         ┌──────────────────────────┐
         │   Output Auto-Complete   │
         │                          │
         │  自动补全被引用但未声明的   │
         │  输出参数                  │
         └────────────┬─────────────┘
                      │
                      ▼
         ┌──────────────────────────┐
         │   Runtime Resolution     │
         │                          │
         │  执行时从 global_catalog  │
         │  解析实际值                │
         └──────────────────────────┘
```

### IOManager 数据流

```python
class IOManager:
    """输入/输出管理器

    职责:
    - 参数绑定与解析
    - 输出捕获与注册
    - 数据集管理
    """

    def bind_parameters(self, params, catalog):
        """绑定参数，解析引用"""
        resolved = {}
        for key, value in params.items():
            if is_reference(value):
                resolved[key] = catalog[extract_ref(value)]
            else:
                resolved[key] = value
        return resolved
```

---

## 8. 缓存与签名机制

### 签名计算

```
节点签名 = hash(
    method_chain_joined      # 方法链 (method1,method2,...)
    + method_meta_joined     # 方法元数据 (engine:version:priority)
    + sorted(param_items)    # 参数键值对
    + sorted(upstream_fps)   # 上游指纹
)
```

### 缓存命中条件

```python
def is_cache_hit(self, signature):
    """判断缓存是否命中

    命中条件:
    1. 所有计划输出数据集已存在
    2. 旧签名 == 新签名
    3. (可选) TTL 未过期
    """
    return (
        self._outputs_exist(signature) and
        self._signature_matches(signature) and
        self._ttl_valid(signature)
    )
```

### 签名差异检测

```
⚠️ 签名变化检测:
  - methods: clean_basic → clean_basic,enrich
  - params.threshold: 0.5 → 0.8
  - upstream.Load_Data: abc123 → def456
```

---

## 9. Hook 与插件体系

### 事件生命周期

```
Pipeline 执行
    │
    ├─▶ before_flow
    │       │
    │       ▼
    │   ┌─────────┐
    │   │ Layer 0 │
    │   └────┬────┘
    │        │
    │        ├─▶ before_node (node_1)
    │        │       │
    │        │       ├─▶ on_cache_hit / on_cache_miss
    │        │       │
    │        │       ├─▶ on_method_execute
    │        │       │
    │        │       └─▶ after_node (node_1)
    │        │
    │        ├─▶ before_node (node_2)
    │        │       ...
    │        │
    │   ┌─────────┐
    │   │ Layer N │
    │   └────┬────┘
    │        │
    └─▶ after_flow
            │
            ▼
       on_failure (如果有错误)
```

### 🆕 v2.0.0 HookManager 增强

```python
from pipeline.core.services.hook_manager import HookManager

# 支持的事件常量
HookManager.SUPPORTED_EVENTS
# {'before_flow', 'after_flow', 'before_node', 'after_node',
#  'on_cache_hit', 'on_cache_miss', 'on_failure', 'on_method_execute'}

# 开启调试模式 (详细日志)
hooks.set_debug(True)

# 注销特定处理器
hooks.unregister('after_node', handler_func)

# 获取某事件的所有处理器
handlers = hooks.get_handlers('before_flow')

# 获取统计信息
stats = hooks.get_stats()
# {
#   'total_handlers': 8,
#   'total_calls': 42,
#   'calls_by_event': {'before_flow': 1, 'after_node': 20, ...}
# }

# 清空所有处理器
hooks.clear()

# 重置统计 (类方法)
HookManager.reset()
```

### 插件开发

```python
# pipeline/plugins/my_plugin.py

def register(hooks):
    """注册插件钩子"""

    def on_before_flow(ctx):
        print(f"🚀 Pipeline 开始: {ctx.config['pipeline']['name']}")

    def on_after_node(step_name, ctx, metrics):
        duration = metrics.get('duration_sec', 0)
        cached = metrics.get('cached', False)
        status = '✅ 缓存命中' if cached else f'⏱️ {duration:.2f}s'
        print(f"  {step_name}: {status}")

    def on_failure(step_name, error, ctx):
        print(f"❌ 步骤失败: {step_name} - {error}")

    hooks.register('before_flow', on_before_flow)
    hooks.register('after_node', on_after_node)
    hooks.register('on_failure', on_failure)
```

### 禁用插件

```bash
# 环境变量
export PIPELINE_DISABLE_PLUGINS=plugin_a,plugin_b

# 或配置文件
echo "plugin_a,plugin_b" > .pipeline_disable_plugins
```

---

## 10. 指标与血缘追踪

### 节点指标

```python
node_metrics[step_name] = {
    'duration_sec': 1.23,
    'cached': False,
    'signature': 'abc123...',
    'outputs': [
        {'name': 'Result_Data', 'type': 'DataFrame', 'shape': (100, 10)}
    ],
    'memory_mb': 256,
    'start_time': '2025-12-06T10:00:00',
    'end_time': '2025-12-06T10:00:01'
}
```

### 血缘追踪

```python
lineage[step_name] = {
    'inputs': ['Raw_Data', 'Config'],
    'outputs': ['Processed_Data'],
    'primary_output': 'Processed_Data',
    'cached': False,
    'duration_sec': 1.23,
    'signature': 'abc123...',
    'upstream_steps': ['Load_Data'],
    'downstream_steps': ['Store_Result']
}
```

### 指标导出

```bash
# JSON 格式
python pipeline/main.py metrics -c workflow/config.yaml --format json

# Markdown 表格
python pipeline/main.py metrics -c workflow/config.yaml --format markdown

# Prometheus 指标 (插件)
# 自动推送到 Prometheus Gateway
```

---

## 11. 失败恢复与软失败

### 软失败模式 (soft_fail)

```yaml
orchestration:
  soft_fail: true  # 启用软失败
```

```
执行流程 (soft_fail=true):
    │
    ├── Step A: ✅ 成功
    │
    ├── Step B: ❌ 失败 (记录错误，继续执行)
    │
    ├── Step C: ⚠️ 跳过 (依赖 Step B)
    │
    └── Step D: ✅ 成功 (不依赖 Step B)
```

### 断点续传 (--resume)

```bash
# 从上次失败点继续
python pipeline/main.py run -c config.yaml --resume
```

```
恢复流程:
1. 读取 .pipeline/failures/<step>.json
2. 分析失败步骤的依赖关系
3. 重建最小子图
4. 跳过已成功的步骤
5. 从失败点重新执行
```

### 失败快照

```json
// .pipeline/failures/Analyze_Trend.json
{
  "step_name": "Analyze_Trend",
  "error_type": "ValueError",
  "error_message": "Invalid metric name",
  "traceback": "...",
  "timestamp": "2025-12-06T10:00:00",
  "parameters": {...},
  "upstream_outputs": {...}
}
```

---

## 12. 扩展指南

### 添加新业务方法

```python
# src/astock/business_engines/engines/my_engine.py

from orchestrator.decorators.register import register_method

@register_method(
    engine_name="my_analysis",
    component_type="business_engine",
    engine_type="duckdb",
    description="自定义分析方法"
)
def my_analysis(data, param1, param2):
    """执行自定义分析

    Args:
        data: 输入数据
        param1: 参数1
        param2: 参数2

    Returns:
        分析结果 DataFrame
    """
    # 业务逻辑
    return result
```

### 添加新依赖源

```python
# pipeline/core/dependency_sources/resource_source.py

from pipeline.core.dependency_graph import DependencySource, DependencyEdge, DependencyType

class ResourceDependencySource(DependencySource):
    """资源依赖源 - 共享资源约束"""

    def extract_dependencies(self, node_name, node_config, all_nodes):
        resource = node_config.get('resource')
        if not resource:
            return []

        edges = []
        for name, cfg in all_nodes.items():
            if name != node_name and cfg.get('resource') == resource:
                edges.append(DependencyEdge(
                    from_node=name,
                    to_node=node_name,
                    dep_type=DependencyType.RESOURCE,
                    metadata={'resource': resource}
                ))
        return edges
```

### 添加新引擎实现

```python
# 在同一 component 下增加新引擎文件
# src/astock/data_engines/engines/arrow_engine.py

@register_method(
    engine_name="load_arrow",
    component_type="data_engine",
    engine_type="arrow",
    priority=20,  # 更高优先级
    description="Arrow 格式数据加载"
)
def load_arrow(path):
    import pyarrow as pa
    return pa.ipc.open_file(path).read_all()
```

---

## 13. 常用命令

### 执行命令

```bash
# 基本执行
python pipeline/main.py run -c workflow/duckdb_screen.yaml

# 只执行特定步骤
python pipeline/main.py run -c config.yaml --only step1,step2

# 排除步骤
python pipeline/main.py run -c config.yaml --exclude step3

# 断点续传
python pipeline/main.py run -c config.yaml --resume

# 强制重新执行 (忽略缓存)
python pipeline/main.py run -c config.yaml --force
```

### 🆕 依赖图可视化 (v2.0.0)

```bash
# 查看依赖图摘要
python pipeline/main.py graph -c config.yaml --summary

# 导出 Mermaid 格式 (适合 Markdown)
python pipeline/main.py graph -c config.yaml -f mermaid -o deps.md

# 导出 GraphViz 格式 (适合高质量图片)
python pipeline/main.py graph -c config.yaml -f graphviz -o deps.dot

# 导出文本格式 (适合终端查看)
python pipeline/main.py graph -c config.yaml -f text

# 生成 GraphViz 图片 (需安装 graphviz)
dot -Tpng deps.dot -o deps.png
```

### 状态与诊断

```bash
# 系统状态
python pipeline/main.py status

# 查看可用引擎
python pipeline/main.py engines

# 查看执行指标
python pipeline/main.py metrics -c config.yaml

# JSON 格式输出
python pipeline/main.py metrics -c config.yaml --format json
```

### 缓存管理

```bash
# 查看缓存计划
python pipeline/main.py cache plan -c config.yaml

# 预热缓存
python pipeline/main.py cache warm -c config.yaml

# 清理缓存
python pipeline/main.py cache clear

# 清理特定步骤缓存
python pipeline/main.py cache clear --steps step1,step2
```

---

## 14. 设计决策记录 (ADR)

### ADR-001: 选择 Prefect + Kedro 混合架构

**背景**: 需要一个支持复杂依赖、可视化监控、且有良好数据工程实践的框架。

**决策**: 采用 Prefect 做调度 + Kedro 做数据处理的混合架构。

**原因**:
- Prefect: 优秀的调度、监控、重试能力
- Kedro: 成熟的数据目录、血缘追踪、测试框架
- 混合: 取两者之长，避免单一框架的局限

### ADR-002: 引入 DependencyGraph 统一依赖管理

**背景**: 原有实现中依赖管理分散在 ConfigService 和 PrefectEngine，导致 `depends_on` 无法正确传递。

**决策**: 创建独立的 DependencyGraph 模块，统一管理所有依赖关系。

**原因**:
- 单一职责: 依赖管理独立于配置解析和执行
- 可测试: 独立模块易于单元测试
- 可扩展: 通过 DependencySource 策略模式支持新依赖类型

### ADR-003: 采用显式依赖声明

**背景**: 隐式依赖推断难以调试，且在复杂场景下容易出错。

**决策**: 要求通过 `depends_on` 显式声明非数据依赖。

**原因**:
- 可预测: 执行顺序完全由配置决定
- 可调试: 依赖关系可直接从配置文件读取
- 可验证: 循环依赖可在解析阶段发现

---

## 📚 相关文档

- [DEPENDENCY_GRAPH_ARCHITECTURE.md](../docs/DEPENDENCY_GRAPH_ARCHITECTURE.md) - 依赖图详细设计
- [ORCHESTRATOR_ARCHITECTURE.md](../docs/ORCHESTRATOR_ARCHITECTURE.md) - 调度器架构
- [Business Engines README](../src/astock/business_engines/README.md) - 业务引擎文档

---

## 📄 许可证

MIT License

---

**维护者**: AStock Team
**最后更新**: 2025-12-07
**Python Package Version**: 2.0.0

---

## 3. 演进关键点

| 方向 | 旧状态 | 新状态 |
|------|--------|--------|
| 自动输入聚合 | InputInferenceService 自动推断 | 已删除 (完全显式 inputs 参数) |
| primary_policy | 输出/输入裁剪 | 移除，保留完整输入上下文 |
| 引擎绑定 | 提前固定或直接指定 | 引入 MethodHandle 延迟解析 (engine=auto) |
| metadata_provider | 外部元数据构建签名 | 使用 predict_signature 内部预测指纹 |
| 多方法执行 | 简单串行 | 统一通过方法链 + per-method handle (预测+解析)|
| 缓存签名 | 方法链 + 参数 + 上游指纹 | 增加实现预测指纹 (engine:version:priority) |
| 可观察 | 零散日志 | 结构化 metrics/lineage + cache diff 警示 |

---
## 4. 核心执行链路
1. 读取 YAML (`-c pipeline/configs/xxx.yaml`).
2. ConfigService: 解析 steps → 引用扫描 → 拓扑排序 → 生成 auto nodes (含 handles)。
3. PrefectEngine: 根据 granularity=node 构建 Flow (每 step 一个 Prefect task)。
4. 执行单节点：
   - 解析参数引用（reference → 上游输出/参数）
   - 预测每个方法的实现签名 `predict_signature()`
   - 组装节点签名 (methods|predicted|params|upstream_fps)
   - 缓存命中则跳过，miss 则执行：对方法链逐个：
     * 若 engine=auto：`MethodHandle.resolve()` 解析实际引擎
     * 绑定参数 → 调用 orchestrator.execute_with_engine()
   - 捕获输出 → 注册 global_catalog → 记录 lineage/node_metrics
5. Flow 结束 → 汇总 metrics / lineage → 输出结果结构。

---
## 5. 配置模型 (示例)
```yaml
pipeline:
  name: Demo
  orchestration:
    granularity: node
    task_runner: concurrent
    max_workers: 4
    soft_fail: true
  steps:
    - name: load_base
      component: data_engine
      engine: pandas        # 或 auto
      methods: [load_raw]
      parameters:
        path: data/raw.csv
    - name: clean
      component: data_engine
      engine: auto          # 由 handle 延迟挑选真实引擎
      methods: [clean_basic, enrich]
      parameters:
        source_ref: {__ref__: steps.load_base.outputs.parameters.raw_df}
    - name: aggregate
      component: business_engine
      engine: duckdb
      methods: [aggregate_kpi]
      parameters:
        df: {__ref__: steps.clean.outputs.parameters.enriched}
```
注意：
- methods 可为单字符串或列表。
- outputs 可省略：系统根据下游引用自动补全。
- 参数引用统一结构 `{__ref__: <ref_string>}`。

---
## 6. 引擎解析: MethodHandle 机制
针对每个 (component, method)，当该 step 配置为 `engine=auto`：
- 创建 `MethodHandle(component, method, prefer='auto')`。
- 执行时：
  1. `predict_signature()`：描述候选（describe）→ 过滤 deprecated → priority/版本排序 → 选中候选 → 组成指纹；不写入 `_resolved_engine`。
  2. 真实执行前：`resolve()` 再次（或使用 fastpath）确定 `engine_type`，生成 explain 结构。
- 支持快速路径：若最近预测时间在 TTL/5 秒内，可直接采用预测结果 (env: `ASTOCK_HANDLE_PREDICT_FASTPATH=0` 可关闭)。
- `invalidate()` 可用于动态注册后人工失效。

Explain 结构示例：
```json
{
  "component": "data_engine",
  "method": "clean_basic",
  "strategy": "default_priority_version",
  "selected": {"engine_type": "pandas", "version": "1.0", "priority": 10, "reason": "rule=priority_version"},
  "candidates": [...],
  "ts": 1730000000.123
}
```

---
## 7. 缓存签名与预测指纹
节点签名构成：
```
<method_chain_joined>|<method_meta_joined>|<sorted(param_items)>|<sorted(upstream_name:fingerprint)>
```
其中 `method_meta_joined` 由每个方法的 `predict_signature()` 产物拼接：
```
method@engine:version:priority;method2@engine:version:priority
```
缓存命中规则：
1. 所有计划数据集输出已存在。
2. 旧签名 == 新签名。
3. (可选) TTL 未过期 (step 可设置 cache_ttl)。

签名差异检测：若输出存在但签名变化，日志输出差异片段（methods/params/upstream）。

---
## 8. 输入/输出 & 引用解析
- 引用语法：`steps.<step>.outputs.parameters.<param_name>`。
- 在参数结构中以 `{"__ref__": "steps.load.outputs.parameters.raw"}` 表达。
- ConfigService 扫描所有参数值 → 构建依赖图。
- 若下游引用的输出上游未显式声明 outputs，将自动补全。
- IOManager (内部) 负责最终参数绑定与输出捕获；已去除 primary_policy / 自动输入推断逻辑。

---
## 9. 指标与血缘
`node_metrics[step]`：
```
{
  duration_sec, cached, signature, outputs: [{name, type, shape/len/...}]
}
```
`lineage[step]`：
```
{
  inputs, outputs, primary_output, cached, duration_sec, signature
}
```
缓存统计：hit / miss / hit_rate 由 CacheStatsService 汇总。

---
## 10. 失败快照 与 resume
- 失败时生成 `.pipeline/failures/<step>.json`。
- `--resume`：读取失败列表，重建需要的上游子图（基于 step 依赖）。
- `soft_fail: true`：节点失败不终止 Flow，后续依赖节点被标记跳过。

---
## 11. Hook 与插件
事件：`before_flow` `after_flow` `before_node` `after_node` `on_cache_hit` `on_failure`。
插件：位于 `pipeline/plugins/`，定义 `register(hooks)` 函数。
禁用：`PIPELINE_DISABLE_PLUGINS=plugin_a,plugin_b` 或 `.pipeline_disable_plugins` 文件。

---
## 12. 服务分层
| 服务 | 作用 |
|------|------|
| ExecuteManager | 生命周期与聚合中枢 |
| ConfigService | 解析 steps / 依赖图 / 生成 nodes |
| FlowExecutor | Prefect 流构建与运行 |
| KedroEngine | 节点执行 + 缓存 + 指纹 + 方法链 orchestrate |
| RuntimeParamService | 运行期动态参数解析 |
| ResultAssembler | 汇总 lineage / metrics / 输出注册 |
| CacheStatsService | 缓存统计汇总 |
| HookManager | 事件广播 |

---
## 13. 扩展点
### 新增方法
1. 在 `src/astock/<domain>/engines/<engine>.py` 增加函数。
2. Orchestrator 自动扫描注册（约定式导入）。
3. YAML 中引用 component + engine + methods。

### 新增引擎实现
- 同一 component 下增加新 `<engine>.py`，注册函数。
- 提升优先级：在注册装饰器里设置 `priority` 更高或 version 更新。

### 新增插件
`pipeline/plugins/my_plugin.py`:
```python
def register(hooks):
    def after_node(step, ctx, metrics):
        ...
    hooks.register('after_node', after_node)
```

### MethodHandle 高级用法 (调试)
```python
for node in em.config['pipeline']['kedro_pipelines']['__auto__']['nodes']:
    for h in node.get('handles', []):
        try:
            h.resolve(em.orchestrator)
            print(h.method, h.explain())
        except: pass
```

---
## 14. 已移除 / 废弃特性
| 特性 | 状态 | 替代 | 原因 |
|------|------|------|------|
| InputInferenceService | 已删除 | 显式参数列表 | 不可预测 / 难调试 |
| primary_policy | 已删除 | 全量输入 | 简化心智模型 |
| metadata_provider | 已删除 | MethodHandle.predict_signature | 去中心化 + 减少外部依赖 |
| dataset_primary_map | 已删除 | primary_output 字段 | 统一输出模型 |
| auto_inputs env 系列 | 废弃 | 无 | 移除隐式魔法 |

---
## 15. 常用运行示例
| 任务 | 命令 |
|------|------|
| 执行 | `python -m pipeline.main run -c pipeline/configs/pipeline.yaml` |
| 仅部分步骤 | `... run -c cfg.yaml --only stepA,stepB` |
| 排除步骤 | `... run -c cfg.yaml --exclude stepX` |
| 查看指标 | `python -m pipeline.main metrics -c cfg.yaml` |
| JSON 指标 | `... metrics -c cfg.yaml --format json` |
| Markdown 指标 | `... metrics -c cfg.yaml --format markdown` |
| 缓存计划 | `python -m pipeline.main cache plan -c cfg.yaml` |
| 预热缓存 | `python -m pipeline.main cache warm -c cfg.yaml` |
| 清理缓存 | `python -m pipeline.main cache clear` |
| 查看引擎 | `python -m pipeline.main engines` |
| 系统状态 | `python -m pipeline.main status` |

---
## 16. 未来增强 (Roadmap)
| 项目 | 描述 |
|------|------|
| Fastpath 统计 | 观察预测快速路径命中率 |
| Explain 导出 | handles_explain.json 自动生成 |
| 更细粒度缓存 | 方法级局部缓存（链内复用） |
| OpenTelemetry | Trace 节点 span + 属性注入 |
| Metrics 推送 | Prometheus / OTLP 双输出 |
| 并行策略 | 根据拓扑 & 历史耗时自适应调度权重 |
| 扩展安全 | 可插拔输出序列化策略 (parquet/arrow) |
| 失败策略 | 更多 classify（可重试 vs 不可重试） |

---
若引入新概念：
1. 先更新本 README
2. 补充示例 YAML
3. 增加最小测试/运行验证
4. 保持签名兼容或版本化迁移策略

> Keep it explicit. Keep it explainable. Make smart optional but safe.
