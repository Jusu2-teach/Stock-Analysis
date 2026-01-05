# 🔄 Pipeline 架构文档

> **目标读者**：新手开发者
> **用途**：理解 Pipeline 是什么、如何编排工作流、如何执行

---

## 🎯 一句话概括

**Pipeline 是一个"工作流编排引擎"** —— 把多个方法按依赖关系串联成流水线，自动处理数据传递、缓存、重试、并行执行。

---

## 🏗️ 架构设计

### 核心思想

```
┌─────────────────────────────────────────────────────────────┐
│                      YAML 配置文件                            │
│  steps:                                                     │
│    step1: get_data → output: raw_data                      │
│    step2: clean_data(raw_data) → output: clean_data       │
│    step3: analyze(clean_data) → output: result            │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ↓
┌─────────────────────────────────────────────────────────────┐
│                   ExecuteManager                            │
│  解析 YAML → 构建节点 → 拓扑排序 → 执行工作流                │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ↓
┌─────────────────────────────────────────────────────────────┐
│                   服务层（解耦设计）                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ConfigService │  │FlowExecutor  │  │Result        │     │
│  │配置解析      │  │流程执行      │  │Assembler     │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ↓
┌─────────────────────────────────────────────────────────────┐
│                   执行引擎层                                  │
│  ┌──────────────┐         ┌──────────────┐                 │
│  │KedroEngine   │ 封装于  │PrefectEngine │                 │
│  │节点执行      │ ←────── │流程封装      │                 │
│  │缓存管理      │         │失败重试      │                 │
│  └──────────────┘         └──────────────┘                 │
└─────────────────────────┬───────────────────────────────────┘
                          │
                          ↓
┌─────────────────────────────────────────────────────────────┐
│                   Orchestrator                              │
│  动态调用已注册的方法（get_data, clean_data, analyze...）   │
└─────────────────────────────────────────────────────────────┘
```

---

## 📂 目录结构

```
pipeline/
├── __init__.py
├── main.py                    # CLI 入口（python -m pipeline.main）
│
├── configs/
│   ├── pipeline.yaml          # Pipeline 配置示例
│   └── tushare_fina.yaml      # 特定场景配置
│
├── core/
│   ├── execute_manager.py     # 主管理器（协调所有服务）
│   ├── context.py             # PipelineContext（共享状态容器）
│   │
│   ├── protocols/             # 接口抽象（避免循环依赖）
│   │   └── method_handle_protocol.py
│   │
│   ├── handles/               # 方法句柄（延迟绑定）
│   │   └── method_handle.py
│   │
│   └── services/              # 服务层（职责分离）
│       ├── config_service.py           # 配置解析、拓扑排序
│       ├── flow_executor.py            # 流程执行
│       ├── result_assembler.py         # 结果组装
│       ├── runtime_param_service.py    # 参数解析
│       ├── cache_stats_service.py      # 缓存统计
│       └── hook_manager.py             # Hook 事件管理
│
└── engines/
    ├── kedro_engine.py        # Kedro 执行引擎（核心）
    └── prefect_engine.py      # Prefect 流程引擎（封装 Kedro）
```

---

## 🔄 工作流程

### 1️⃣ 配置定义（用户）

用户通过 YAML 定义工作流：

```yaml
# pipeline/configs/my_pipeline.yaml
pipeline:
  name: stock_analysis
  description: 股票数据分析流水线

  steps:
    # Step 1: 获取数据
    step1:
      component: datahub
      methods:
        - get_data
      parameters:
        stock_code: "000001.SZ"
      outputs:
        - name: raw_data
          type: dataset  # dataset = DataFrame，会被 Kedro 管理

    # Step 2: 清洗数据（依赖 step1 的输出）
    step2:
      component: data_engines
      methods:
        - clean_data
      parameters:
        data: "steps.step1.outputs.parameters.raw_data"  # 引用 step1 输出
      outputs:
        - name: clean_data
          type: dataset

    # Step 3: 分析数据（依赖 step2 的输出）
    step3:
      component: business_engines
      methods:
        - analyze_trend
      parameters:
        data: "steps.step2.outputs.parameters.clean_data"
      outputs:
        - name: result
          type: parameter  # parameter = 简单值（dict/list/str）
```

**关键概念**：

| 字段 | 说明 | 示例 |
|------|------|------|
| **component** | 组件类型（对应 orchestrator） | datahub, data_engines |
| **methods** | 方法列表（链式调用） | [get_data] 或 [method1, method2] |
| **parameters** | 输入参数（支持引用其他步骤输出） | stock_code: "000001.SZ" |
| **outputs** | 输出定义（dataset/parameter） | name: raw_data, type: dataset |

---

### 2️⃣ 解析配置（ConfigService）

**职责**：
- 加载 YAML 配置
- 解析步骤依赖关系
- 拓扑排序（确定执行顺序）
- 构建 Kedro 节点配置

**流程**：

```python
# ConfigService.load_config()
config = yaml.safe_load(open("pipeline.yaml"))

# ConfigService._parse_steps()
steps = {
    "step1": {...},
    "step2": {
        "depends_on": ["step1"],  # 依赖 step1（通过引用自动检测）
        ...
    },
    "step3": {
        "depends_on": ["step2"],  # 依赖 step2
        ...
    }
}

# ConfigService._compute_execution_order()
execution_order = ["step1", "step2", "step3"]  # 拓扑排序结果
```

**依赖检测**：自动识别引用关系

```yaml
parameters:
  data: "steps.step1.outputs.parameters.raw_data"
         ^^^^^^ 自动检测到依赖 step1
```

---

### 3️⃣ 构建节点（ConfigService + MethodHandle）

**职责**：将每个 step 转换为 Kedro 节点配置

**MethodHandle 机制**：

> **为什么需要 MethodHandle？**
> 因为在配置解析阶段，orchestrator 的方法还没有注册完成（循环依赖问题）。MethodHandle 实现"延迟绑定"：配置时只记录方法信息，执行时才真正调用 orchestrator。

```python
# ConfigService.build_auto_nodes()
for step_name, step_spec in steps.items():
    # 为每个方法创建 MethodHandle（延迟绑定）
    method_handles = [
        create_method_handle(
            component=step_spec.component,
            method=method_name,
            prefer="auto"  # 让 orchestrator 自动选择最优实现
        )
        for method_name in step_spec.methods
    ]

    # 构建 Kedro 节点配置
    node_config = {
        "name": step_name,
        "component": step_spec.component,
        "methods": step_spec.methods,
        "method_handles": method_handles,  # 延迟绑定
        "inputs": [...],
        "outputs": [...],
        "parameters": {...}
    }
```

---

### 4️⃣ 执行流程（FlowExecutor + Engines）

#### 执行层次

```
FlowExecutor (流程协调)
    ↓
PrefectEngine (失败重试、软失败)
    ↓
KedroEngine (节点执行、缓存、血缘)
    ↓
MethodHandle.execute() (调用 orchestrator)
    ↓
Orchestrator.execute() (动态调用注册的方法)
```

#### 执行流程

```python
# 1. FlowExecutor.run()
result = flow_executor.run(auto_info, manager)

# 2. PrefectEngine 创建 Flow
flow = prefect.flow(
    name=pipeline_name,
    retries=2,           # 失败重试 2 次
    retry_delay_seconds=5
)(orchestrator_pipeline_flow)

# 3. PrefectEngine 执行（内部调用 KedroEngine）
result = flow(kedro_engine, pipeline_name, parameters)

# 4. KedroEngine 执行节点
for node in pipeline.nodes:
    # 缓存判断
    if cache_hit:
        logger.info("缓存命中，跳过执行")
        continue

    # 执行节点
    result = node.run(inputs)

    # 保存到 catalog
    catalog.save(output_name, result)

# 5. MethodHandle.execute() 调用实际方法
def execute_node(*args, **kwargs):
    for method_handle in method_handles:
        # 解析引擎（auto 模式）
        engine = method_handle.resolve(orchestrator)

        # 调用 orchestrator
        result = orchestrator.execute(
            component=component,
            method=method_name,
            *resolved_args,
            **resolved_kwargs
        )
    return result
```

---

### 5️⃣ 缓存机制（KedroEngine）

**核心思想**：如果输入和方法没变，直接复用之前的输出。

**缓存签名计算**：

```python
# 节点签名 = 方法链 + 参数 + 上游输出指纹
signature = hash([
    "get_data",                    # 方法名
    "datahub::tushare::get_data",  # 方法实现（引擎:版本:优先级）
    {"stock_code": "000001.SZ"},   # 参数
    "upstream_data:abc123"         # 上游数据指纹
])
```

**缓存判断**：

```python
# 1. 计算当前签名
new_signature = compute_signature(methods, params, upstream_data)

# 2. 对比上次签名
old_signature = cache.get(step_name)

# 3. 判断
if new_signature == old_signature and output_exists:
    logger.info("✅ 缓存命中，跳过执行")
    return cached_output
else:
    logger.info("❌ 缓存失效，重新执行")
    result = execute_node()
    cache.save(step_name, new_signature)
```

**缓存失效原因**：
- ✅ 输入数据变化（上游输出指纹变化）
- ✅ 参数变化（`stock_code` 从 "000001" 改为 "000002"）
- ✅ 方法实现变化（从 tushare v1.0 升级到 v2.0）
- ✅ 方法链变化（从 `[get_data]` 改为 `[get_data, filter_data]`）

---

### 6️⃣ 结果组装（ResultAssembler）

**职责**：汇总执行结果、解析引用、格式化输出

```python
# ResultAssembler.assemble()
result = {
    "status": "success",              # 执行状态
    "duration": "5.23s",              # 执行时长
    "nodes_run": 3,                   # 执行节点数
    "execution_order": ["step1", "step2", "step3"],
    "outputs": {
        "step1": {"raw_data": DataFrame(...)},
        "step2": {"clean_data": DataFrame(...)},
        "step3": {"result": {...}}
    },
    "cache_stats": {
        "hits": 1,                    # 缓存命中数
        "misses": 2,                  # 缓存未命中数
        "saved_time": "2.1s"          # 节省时间
    }
}
```

---

## 🧩 核心组件详解

### ExecuteManager（主管理器）

**职责**：协调所有服务，提供统一入口

**关键方法**：
```python
class ExecuteManager:
    def __init__(self, config_path, orchestrator):
        # 初始化上下文
        self.ctx = PipelineContext()

        # 初始化服务层
        self._config_service = ConfigService(self.ctx, self.logger)
        self._flow_executor = FlowExecutor(self.ctx, ...)
        self._result_assembler = ResultAssembler(self.ctx, ...)

    def run(self, config_path=None, **runtime_params):
        """执行 Pipeline"""
        # 1. 加载配置
        config = self._config_service.load_config(config_path)

        # 2. 构建节点
        auto_info = self._config_service.build_auto_nodes(...)

        # 3. 执行流程
        result = self._flow_executor.run(auto_info, self)

        # 4. 返回结果
        return result
```

---

### PipelineContext（共享状态）

**职责**：所有服务共享的状态容器（避免传递大量参数），并通过 PGCS DataStore 统一管理跨步骤数据与引用解析。

**数据结构（简化）**：
```python
@dataclass
class PipelineContext:
  # 配置与步骤
  config: Dict[str, Any] = field(default_factory=dict)
  steps: Dict[str, StepSpec] = field(default_factory=dict)
  execution_order: List[str] = field(default_factory=list)

  # 运行时状态（依赖图、执行计划等）
  _runtime_state: Dict[str, Any] = field(default_factory=dict)

  # 统一数据存储与引用解析
  _data_store: DataStore | None = field(default=None, repr=False)
  _resolver: ReferenceResolver | None = field(default=None, repr=False)

  def __post_init__(self):
    if self._data_store is None:
      self._data_store = DataStore()
    if self._resolver is None:
      self._resolver = ReferenceResolver(self._data_store)
      self._resolver.register_pattern(
        template='steps.{step}.outputs.parameters.{param}',
        handler='step_output',
      )
```

**使用方式**：
```python
# 所有服务共享同一个 context
ctx = PipelineContext()

config_service = ConfigService(ctx, logger)
flow_executor = FlowExecutor(ctx, ...)
result_assembler = ResultAssembler(ctx, ...)

# 服务间通过 context 共享数据
config_service.load_config(path)       # 写入 ctx.config / ctx.steps / ctx.execution_order
flow_executor.run(...)                 # 读取 ctx.config，并通过 ctx.resolver 解析引用
```

---

### MethodHandle（方法句柄）

**职责**：延迟绑定 orchestrator 方法

**问题背景**：
```python
# ❌ 循环依赖问题
# ExecuteManager 依赖 Orchestrator
# Orchestrator 初始化需要时间（自动发现方法）
# ConfigService 在解析时无法直接调用 orchestrator

# ✅ 解决方案：MethodHandle
# 解析时：只记录方法信息（component, method）
# 执行时：才真正调用 orchestrator
```

**工作原理**：
```python
# 1. 创建（解析阶段）
handle = create_method_handle(
    component="datahub",
    method="get_data",
    prefer="auto"
)

# 2. 执行（运行阶段）
result = handle.execute(orchestrator, stock_code="000001.SZ")

# 内部实现
class MethodHandle:
    def execute(self, orchestrator, *args, **kwargs):
        # 动态调用 orchestrator
        return orchestrator.execute(
            self.component,
            self.method,
            *args,
            **kwargs
        )
```

---

### KedroEngine（执行引擎）

**职责**：
- 创建 Kedro 节点
- 管理数据目录（DataCatalog）
- 缓存管理（签名计算、判断、持久化）
- 执行节点

**关键方法**：
```python
class KedroEngine:
    def create_pipeline(self, pipeline_name, config):
        """创建 Kedro Pipeline"""
        nodes = [self._create_kedro_node(node_config)
                 for node_config in config.nodes]
        return Pipeline(nodes)

    def _create_kedro_node(self, node_config):
        """创建单个节点"""
        def execute_node(*args, **kwargs):
            # 1. 缓存判断
            # 2. 执行方法（通过 MethodHandle）
            # 3. 保存输出
            # 4. 更新缓存

        return Node(
            func=execute_node,
            inputs=[...],
            outputs=[...],
            name=node_config.name
        )

    def run(self, pipeline_name, parameters):
        """执行 Pipeline"""
        runner = SequentialRunner()
        return runner.run(self.pipelines[pipeline_name], self.data_catalog)
```

---

### PrefectEngine（流程引擎）

**职责**：
- 封装 Kedro Pipeline 为 Prefect Flow
- 提供失败重试
- 软失败处理（某些步骤失败不影响其他步骤）

**工作原理**：
```python
# 1. 定义 Flow 函数
@flow(name="pipeline", retries=2)
def orchestrator_pipeline_flow(kedro_engine, pipeline_name, params):
    return kedro_engine.run(pipeline_name, params)

# 2. 执行 Flow
flow = orchestrator_pipeline_flow
result = flow(kedro_engine, "my_pipeline", {...})
```

---

## 📊 完整执行流程图

```
┌────────────────────────────────────────────────────────────┐
│ 1. 用户调用                                                 │
│    manager = ExecuteManager("pipeline.yaml")               │
│    result = manager.run()                                  │
└─────────────────────────┬──────────────────────────────────┘
                          │
                          ↓
┌────────────────────────────────────────────────────────────┐
│ 2. ConfigService.load_config()                             │
│    - 加载 YAML                                             │
│    - 解析 steps                                            │
│    - 拓扑排序 → execution_order                            │
│    - 写入 ctx.config, ctx.steps, ctx.execution_order      │
└─────────────────────────┬──────────────────────────────────┘
                          │
                          ↓
┌────────────────────────────────────────────────────────────┐
│ 3. ConfigService.build_auto_nodes()                        │
│    - 为每个 step 创建 MethodHandle                         │
│    - 构建 Kedro 节点配置                                   │
│    - 返回 auto_info                                        │
└─────────────────────────┬──────────────────────────────────┘
                          │
                          ↓
┌────────────────────────────────────────────────────────────┐
│ 4. FlowExecutor.run()                                      │
│    - 创建 PrefectEngine                                    │
│    - 创建 KedroEngine                                      │
│    - 执行 Prefect Flow                                     │
└─────────────────────────┬──────────────────────────────────┘
                          │
                          ↓
┌────────────────────────────────────────────────────────────┐
│ 5. PrefectEngine.run_pipeline()                            │
│    - 定义 Flow（带重试）                                   │
│    - 调用 KedroEngine.run()                                │
└─────────────────────────┬──────────────────────────────────┘
                          │
                          ↓
┌────────────────────────────────────────────────────────────┐
│ 6. KedroEngine.run()                                       │
│    - 按 execution_order 执行节点                           │
│    - 每个节点：缓存判断 → 执行 → 保存输出                 │
└─────────────────────────┬──────────────────────────────────┘
                          │
                          ↓
┌────────────────────────────────────────────────────────────┐
│ 7. MethodHandle.execute()                                  │
│    - 解析引擎（auto 模式）                                 │
│    - 调用 orchestrator.execute()                           │
└─────────────────────────┬──────────────────────────────────┘
                          │
                          ↓
┌────────────────────────────────────────────────────────────┐
│ 8. Orchestrator.execute()                                  │
│    - 从 Registry 查找方法                                  │
│    - 使用 Strategy 选择实现                                │
│    - 执行方法（如 tushare.get_data）                       │
└─────────────────────────┬──────────────────────────────────┘
                          │
                          ↓
┌────────────────────────────────────────────────────────────┐
│ 9. 返回结果                                                 │
│    - 组装结果（ResultAssembler）                           │
│    - 返回给用户                                             │
└────────────────────────────────────────────────────────────┘
```

---

## 🎓 新手上手指南

### 场景 1：创建简单 Pipeline

```yaml
# my_pipeline.yaml
pipeline:
  name: simple_pipeline

  steps:
    step1:
      component: datahub
      methods: [get_data]
      parameters:
        stock_code: "000001.SZ"
      outputs:
        - name: data
          type: dataset
```

```python
# 执行
from pipeline.core.execute_manager import ExecuteManager

manager = ExecuteManager(config_path="my_pipeline.yaml")
result = manager.run()
print(result["outputs"]["step1"]["data"])
```

---

### 场景 2：步骤间数据传递

```yaml
steps:
  step1:
    component: datahub
    methods: [get_data]
    parameters:
      stock_code: "000001.SZ"
    outputs:
      - name: raw_data
        type: dataset

  step2:
    component: data_engines
    methods: [clean_data]
    parameters:
      data: "steps.step1.outputs.parameters.raw_data"  # 引用 step1 输出
    outputs:
      - name: clean_data
        type: dataset
```

---

### 场景 3：方法链（链式调用）

```yaml
step1:
  component: data_engines
  methods:
    - load_data      # 先执行
    - filter_data    # 然后执行（输入是 load_data 的输出）
    - transform_data # 最后执行（输入是 filter_data 的输出）
  parameters:
    file: "data.csv"
  outputs:
    - name: result
      type: dataset
```

---

### 场景 4：运行时参数覆盖

```python
# 配置文件定义默认参数
# parameters:
#   stock_code: "000001.SZ"

# 运行时覆盖
manager = ExecuteManager(config_path="my_pipeline.yaml")
result = manager.run(stock_code="000002.SZ")  # 运行时覆盖
```

---

### 场景 5：查看缓存统计

```python
result = manager.run()

# 查看缓存统计
cache_stats = result["cache_stats"]
print(f"缓存命中: {cache_stats['hits']}")
print(f"缓存未命中: {cache_stats['misses']}")
print(f"节省时间: {cache_stats['saved_time']}")
```

---

## 🔍 常见问题

### Q1: Pipeline 和 Orchestrator 有什么区别？

| 维度 | Orchestrator | Pipeline |
|------|--------------|----------|
| **职责** | 方法注册与调用 | 工作流编排 |
| **粒度** | 单个方法 | 多个方法的组合 |
| **使用场景** | 简单调用 | 复杂流水线 |
| **依赖关系** | 无（独立调用） | 有（步骤间依赖） |
| **缓存** | 无 | 有（自动缓存） |

**类比**：
- **Orchestrator**：函数库（提供函数）
- **Pipeline**：脚本编排（调用多个函数，处理依赖）

---

### Q2: 为什么需要 MethodHandle？

**问题**：
```python
# ExecuteManager 需要在初始化时构建节点
# 但此时 orchestrator 还没加载完所有方法（auto_discover）
# 直接调用会失败
```

**解决**：
```python
# MethodHandle 实现"延迟绑定"
# 配置解析时：只记录 (component, method)
# 执行时：才调用 orchestrator.execute()
```

---

### Q3: 缓存什么时候会失效？

缓存基于**签名匹配**，以下情况会失效：

1. **输入数据变化**
   ```yaml
   parameters:
     stock_code: "000001.SZ"  # 改为 "000002.SZ" 会失效
   ```

2. **上游输出变化**
   ```yaml
   parameters:
     data: "steps.step1.outputs.parameters.raw_data"
     # step1 重新执行，输出变化，step2 缓存失效
   ```

3. **方法实现变化**
   ```python
   # tushare.get_data 从 v1.0 升级到 v2.0
   # 签名中包含版本信息，会导致缓存失效
   ```

4. **方法链变化**
   ```yaml
   methods: [get_data]         # 改为 [get_data, filter_data]
   # 方法列表变化，缓存失效
   ```

---

### Q4: 如何调试 Pipeline 执行？

```python
# 1. 启用 DEBUG 日志
import logging
logging.basicConfig(level=logging.DEBUG)

# 2. 查看执行顺序
result = manager.run()
print(result["execution_order"])  # ['step1', 'step2', 'step3']

# 3. 查看每个步骤的输出
for step_name, outputs in result["outputs"].items():
    print(f"{step_name}: {outputs}")

# 4. 查看缓存统计
print(result["cache_stats"])
```

---

### Q5: 如何禁用缓存？

```python
# 方法 1：删除缓存文件
import os
if os.path.exists(".pipeline_cache.db"):
    os.remove(".pipeline_cache.db")

# 方法 2：修改 KedroEngine 代码（临时）
# 在 _create_kedro_node 中强制 cache_hit = False
```

---

## 📝 总结

| 概念 | 作用 | 类比 |
|------|------|------|
| **Pipeline** | 工作流编排引擎 | 流水线 |
| **ExecuteManager** | 主管理器 | 工厂车间主管 |
| **ConfigService** | 配置解析 | 图纸解读员 |
| **FlowExecutor** | 流程执行 | 流水线启动器 |
| **KedroEngine** | 节点执行 | 工位操作员 |
| **MethodHandle** | 延迟绑定 | 预约单（执行时才真正调用） |
| **PipelineContext** | 共享状态 | 公告板（所有人共享信息） |
| **缓存** | 跳过重复计算 | 成品仓库（相同输入直接取货） |

**核心优势**：
- ✅ **自动依赖管理**：通过引用自动检测依赖关系
- ✅ **智能缓存**：相同输入自动复用结果，加速执行
- ✅ **失败重试**：Prefect 提供自动重试机制
- ✅ **方法链支持**：一个步骤可以链式调用多个方法
- ✅ **运行时参数**：支持运行时覆盖配置参数
- ✅ **血缘追踪**：记录数据流转路径
- ✅ **易于调试**：详细的日志和执行统计

**设计哲学**：
- 🎯 **职责分离**：每个服务负责一件事（单一职责原则）
- 🔌 **解耦设计**：服务间通过 Context 共享状态（依赖注入）
- 🔧 **延迟绑定**：MethodHandle 解决循环依赖问题
- 📦 **分层架构**：配置层 → 服务层 → 引擎层 → 方法层

---

**下一步**：
- 📖 阅读 [Orchestrator 架构文档](./ORCHESTRATOR_ARCHITECTURE.md) 了解方法注册机制
- 🛠️ 尝试编写自己的 Pipeline 配置文件
- 🔍 查看 `pipeline/configs/` 目录下的示例配置
