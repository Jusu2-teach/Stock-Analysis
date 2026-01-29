# AStock Analysis System

![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![Architecture](https://img.shields.io/badge/Architecture-Four--Layer-green)
![Engine](https://img.shields.io/badge/Engine-DuckDB%20%7C%20Polars-orange)
![Version](https://img.shields.io/badge/Version-5.0-brightgreen)

**AStock Analysis** 是一个现代化、企业级的 **A股基本面量化分析系统**。它采用 **Orchestrator（调度层）+ Pipeline（编排层）+ Business Engines（业务层）+ Shared（共享层）** 的四层解耦架构，专注于通过**多年财务数据的趋势分析**，筛选优质企业、识别困境反转、预警风险公司。

**核心创新**：引入 **T.R.U.T.H. 系统**（Trend-Reality Unified Truth Hashing）—— 一个零配置、去标签化的六维基因量化框架，彻底抛弃传统行业标签和预设阈值，实现公司特征的自动化度量。

---

## 📑 目录

- [系统功能](#-系统功能)
- [核心架构](#-核心架构)
- [四层架构详解](#-四层架构详解)
- [T.R.U.T.H. 系统](#-truth-系统)
- [Workflow 工作流详解](#-workflow-工作流详解)
- [数据流与组件交互](#-数据流与组件交互)
- [快速开始](#-快速开始)
- [目录结构](#-目录结构)
- [核心组件说明](#-核心组件说明)
- [扩展开发](#-扩展开发)

---

## 🎯 系统功能

### 核心分析能力

| 功能模块 | 描述 | 实现位置 |
|---------|------|----------|
| **趋势分析引擎** | 基于对数回归、Mann-Kendall检验、HP滤波等8种探针，分析财务指标的长期趋势 | `analyzers/trend/` |
| **T.R.U.T.H. 六维基因** | α(周期性)、β(资本密度)、γ(成长动能)、δ_fraud(欺诈熵)、δ_decay(衰退熵)、V(验证因子) | `truth/core/genes/` |
| **三大物理求解器** | 重力求解器(ROIC阈值)、速度求解器(增长边界)、结构求解器(护城河侵蚀) | `truth/core/solvers/` |
| **多因子评分** | 成长因子(30%) + 质量因子(40%) + 安全因子(30%) 的综合评分体系 | `reporters/` |
| **策略识别** | 自动识别5种投资策略：高成长、困境反转、稳定分红、周期底部、护城河防御 | `reporters/comprehensive_generator.py` |

### 输出报告

- 📊 **优质公司精选** - 按超大型/大型/中型分类展示 GARP 精选
- 🏰 **白马护城河** - 高ROE、高ROIC、高毛利率的行业龙头
- 🚀 **困境反转机会** - 基本面触底回升的潜力股
- ⚠️ **风险警示** - 财务指标异常、存在欺诈风险的公司
- 🧬 **T.R.U.T.H. 基因报告** - 公司六维基因图谱与动态阈值分析

---

## 🌟 核心架构

系统由四层独立且松耦合的模块组成：

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           🎼 Orchestrator (调度层)                           │
│         @register_method 装饰器 · 自动发现 · 策略路由 · 版本管理              │
│                                                                             │
│   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐       │
│   │  Registry   │  │  Discovery  │  │  Executor   │  │  Strategies │       │
│   │  (方法注册)  │  │  (自动发现)  │  │  (执行器)   │  │  (路由策略)  │       │
│   └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘       │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            🚀 Pipeline (编排层)                              │
│          YAML配置驱动 · Prefect+Kedro混合引擎 · 断点续传 · 指纹缓存           │
│                                                                             │
│   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐       │
│   │ExecuteManager│ │DependencyGraph│ │ KedroEngine │  │PrefectEngine│       │
│   │  (门面类)    │  │  (依赖图)    │  │ (数据处理)  │  │  (任务编排)  │       │
│   └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘       │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        🧬 Business Engines (业务层)                          │
│                      零侵入性 · 纯净业务逻辑 · 即插即用                        │
│                                                                             │
│   ┌──────────────────┐   ┌──────────────────┐   ┌──────────────────┐       │
│   │  📈 Analyzers     │   │  🧪 T.R.U.T.H.   │   │  📊 Reporters    │       │
│   │  趋势分析探针系统   │   │  六维基因系统     │   │  报告生成器       │       │
│   │                  │   │                  │   │                  │       │
│   │ • DuckDB引擎     │   │ • 六维基因计算    │   │ • 综合报告       │       │
│   │ • Polars引擎     │   │ • 三大求解器     │   │ • T.R.U.T.H.报告 │       │
│   │ • 8种数学探针    │   │ • 因果验证       │   │ • CSV/JSON/MD   │       │
│   └──────────────────┘   └──────────────────┘   └──────────────────┘       │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           📦 Shared (共享层)                                 │
│                    统一命名规范 · EventBus事件总线 · 跨模块协议                │
│                                                                             │
│   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐       │
│   │ EventBus    │  │NamingConven │  │  Protocols  │  │   Events    │       │
│   │ (事件总线)   │  │ (命名规范)   │  │  (协议定义)  │  │  (事件类型)  │       │
│   └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘       │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 📐 四层架构详解

### 1️⃣ Orchestrator（调度层）—— 方法注册中心

**职责**：统一管理所有业务方法的注册、发现、路由和执行。

**核心组件**：

| 组件 | 文件 | 职责 |
|------|------|------|
| **Registry** | `registry/registry.py` | 单例模式的方法注册表，维护三层索引 |
| **Discovery** | `registry/discovery.py` | 自动扫描并加载业务模块 |
| **Strategies** | `registry/strategies.py` | 5种方法选择策略（版本优先/优先级优先等） |
| **Executor** | `registry/executor.py` | 方法执行器，处理参数绑定和结果返回 |

**注册方法示例**：

```python
from orchestrator.decorators.register import register_method

@register_method(
    component_type="business_engine",  # 组件类型: datahub | data_engine | business_engine
    engine_type="duckdb",              # 引擎类型: duckdb | polars | tushare
    engine_name="analyze_metric_trend", # 方法名（YAML中引用）
    version="1.0.0",
    priority=10,
    description="趋势分析方法"
)
def analyze_metric_trend(data: pd.DataFrame, **params) -> pd.DataFrame:
    """业务方法实现"""
    ...
```

**三层索引结构**：

```
Registry
└── component_type (business_engine)
    └── engine_name (analyze_metric_trend)
        └── engine_type (duckdb)
            └── MethodRegistration (版本、优先级、callable等)
```

---

### 2️⃣ Pipeline（编排层）—— 工作流执行引擎

**职责**：解析YAML配置，构建依赖图，编排执行业务方法。

**核心组件**：

| 组件 | 文件 | 职责 |
|------|------|------|
| **ExecuteManager** | `core/execute_manager.py` | 门面类，协调所有服务 |
| **ConfigService** | `core/services/config_service.py` | YAML配置解析，节点构建 |
| **DependencyGraph** | `core/dependency_graph.py` | 依赖图管理，拓扑排序，循环检测 |
| **KedroEngine** | `engines/kedro_engine.py` | 数据处理引擎，指纹缓存，血缘追踪 |
| **PrefectEngine** | `engines/prefect_engine.py` | 任务编排引擎，层级并行，重试机制 |
| **HookManager** | `core/services/hook_manager.py` | 事件钩子，插件系统 |

**混合引擎架构**：

```
               Prefect (编排层)
                     │
     ┌───────────────┼───────────────┐
     │               │               │
  Layer 1         Layer 2         Layer 3
  [Task A]     [Task B, C]      [Task D]
     │               │               │
     └───────────────┼───────────────┘
                     │
               Kedro (数据层)
         • DataCatalog 管理
         • 指纹签名缓存
         • 血缘追踪
```

---

### 3️⃣ Business Engines（业务层）—— 纯净业务逻辑

**职责**：实现具体的业务分析逻辑，完全独立于框架代码。

**核心模块**：

```
src/astock/business_engines/
├── analyzers/              # 📈 趋势分析引擎
│   └── trend/
│       ├── core.py         # TrendAnalyzer 核心类
│       ├── duckdb_engine.py # DuckDB 实现 (已注册到 Orchestrator)
│       ├── models.py       # 数据模型 (TrendSnapshot, TrendResult等)
│       └── probes/         # 8种数学探针
│
├── truth/                  # 🧬 T.R.U.T.H. 系统
│   ├── processor.py        # 主处理器
│   ├── adapter.py          # ProbeAdapter (探针→基因映射)
│   └── core/
│       ├── genes/          # 六维基因计算
│       └── solvers/        # 三大物理求解器
│
└── reporters/              # 📊 报告生成器
  ├── comprehensive_generator.py   # 综合分析报告
  └── truth_report_generator.py    # T.R.U.T.H. 基因报告
```

---

### 4️⃣ Shared（共享层）—— 跨模块基础设施

**职责**：提供跨模块共享的基础设施，包括事件总线、命名规范、协议定义。

**核心组件**：

| 组件 | 文件 | 职责 |
|------|------|------|
| **EventBus** | `event_bus.py` | 发布/订阅事件总线，解耦模块通信 |
| **NamingConvention** | `naming_convention.py` | 统一指标命名规范，三层映射 |
| **Events** | `events.py` | 标准化事件类型定义 |

**EventBus 使用示例**：

```python
from shared import EventBus, NodeCompletedEvent, EventPriority

# 订阅事件
@EventBus.on('pipeline.node.completed', priority=EventPriority.NORMAL)
def on_node_complete(event: NodeCompletedEvent):
    print(f"Node {event.step_name} completed in {event.duration_ms}ms")

# 发布事件
EventBus.emit(NodeCompletedEvent(
    step_name="Analyze_ROIC_Trend",
    status="success",
    duration_ms=1234.5,
    source="pipeline.kedro"
))
```

---

## 🧬 T.R.U.T.H. 系统

### 设计哲学

**T.R.U.T.H.**（Trend-Reality Unified Truth Hashing）是本系统的核心创新，彻底抛弃传统行业标签和预设阈值。

| 传统方法 | T.R.U.T.H.方法 |
|---------|---------------|
| 公司 → 行业标签 → 查表获取阈值 | 公司 → 计算6维基因 → **动态生成阈值** |
| `if industry == "白酒": threshold = 15%` | 基因决定阈值：`T = f(α,β,γ,δ,V)` |
| 多元化公司无法处理 | **自动处理**：基因反映混合特征 |
| 需要维护行业配置表 | **零配置**：全自动计算 |

### 六维基因定义

```
┌────────────────────────────────────────────────────────────────┐
│                    六维基因图谱 (6D Genome)                     │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│   属性基因 (公司特征描述)                                        │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐                    │
│   │ α 周期性  │  │ β 轻重    │  │ γ 动能   │                    │
│   │ 0~1     │  │ 0~1      │  │ 0~1     │                    │
│   │ 0=稳定  │  │ 0=轻资产  │  │ 0=停滞  │                    │
│   │ 1=强周期 │  │ 1=重资产  │  │ 1=高增长 │                    │
│   └──────────┘  └──────────┘  └──────────┘                    │
│                                                                │
│   风险/验证基因                                                  │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐                    │
│   │ δ_fraud  │  │ δ_decay  │  │ V 验证   │                    │
│   │ 欺诈熵   │  │ 衰退熵   │  │ 照妖镜   │                    │
│   │ 0=诚实  │  │ 0=健康   │  │ 0=虚假  │                    │
│   │ 1=高风险 │  │ 1=快速衰退│  │ 1=真实  │                    │
│   └──────────┘  └──────────┘  └──────────┘                    │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

### 三大物理求解器

| 求解器 | 上帝方程 | 功能 | 输入基因 |
|--------|---------|------|----------|
| 🌍 **重力求解器** | I | ROIC/ROE 动态阈值 | α, β |
| 🚀 **速度求解器** | II | 增长边界评估 | γ |
| 🧬 **结构求解器** | III | 护城河侵蚀检测 | δ_decay, β |

**重力求解器公式**（简化版）：

```
T_roic = R_f + β_premium × β + growth_discount × γ - α_豁免
       = 无风险利率 + 资本密度惩罚 + 成长折扣 - 周期豁免
```

---

## 📋 Workflow 工作流详解

### YAML 配置结构

工作流定义在 `workflow/analysis.yaml`，核心结构如下：

```yaml
pipeline:
  name: "DuckDB财务基线筛选管道"
  orchestration:
    granularity: node           # 执行粒度
    soft_fail: true             # 软失败模式（单节点失败不中断）
    task_runner: "sequential"   # 任务运行器
    max_workers: 4              # 最大并行数

  steps:
    - name: "Step_Name"
      component: "business_engine"        # 组件类型
      engine: "duckdb"                    # 引擎类型
      method: ["method_name"]             # 方法名（对应 @register_method）
      parameters:
        data: "steps.Previous_Step.outputs.parameters.Output_Name"  # 引用语法
        metric_name: 'roic'
      outputs:
        parameters:
          - name: Output_Name
```

### 完整工作流数据流

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        analysis.yaml 完整数据流                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌──────────────────────┐                                                  │
│   │ Load_Financial_Data  │  ← 加载 10yd_final_industry.csv                  │
│   └──────────┬───────────┘                                                  │
│              │ Raw_Data (DataFrame)                                         │
│              ▼                                                              │
│   ┌──────────────────────────────────────────────────────────────────┐     │
│   │              8 个并行趋势分析步骤 (同层可并行)                      │     │
│   │  ROIC | ROE | Revenue | Profit | GrossMargin | NetMargin | OCF   │     │
│   └──────────────────────────────────────────────────────────────────┘     │
│              │ 8个 Trend_Result DataFrames                                  │
│              ▼                                                              │
│   ┌──────────────────────────────────────────────────────────────────┐     │
│   │                  Process_Truth_System                             │     │
│   │  1. DataFrame → ProbeOutputs 转换                                 │     │
│   │  2. ProbeAdapter.adapt() → GenomeInput                           │     │
│   │  3. compute_genome_from_probes() → CompanyGenome (6D)            │     │
│   │  4. 三大求解器执行 → 动态阈值                                      │     │
│   │  5. 因果网络验证                                                   │     │
│   └──────────────────────────────────────────────────────────────────┘     │
│              │ BatchProcessResult                                          │
│              ▼                                                              │
│   ┌──────────────────────────────────────────────────────────────────┐     │
│   │  Generate_Comprehensive_Report  │  Generate_Truth_Report         │     │
│   │  → comprehensive_analysis.md    │  → truth_analysis_report.md    │     │
│   └──────────────────────────────────────────────────────────────────┘     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 步骤间引用语法

```yaml
parameters:
  # 引用上一步骤的输出
  data: "steps.Load_Financial_Data.outputs.parameters.Raw_Data"

  # 引用格式: steps.{StepName}.outputs.parameters.{OutputName}
```

---

## 🔗 数据流与组件交互

### 组件交互时序

```
CLI → Pipeline → Orchestrator → Business Engine
 │        │            │              │
 │  parse YAML         │              │
 │  build DependencyGraph             │
 │        │            │              │
 │  for each step:     │              │
 │  resolve method ────────►Registry.select()
 │        │            │              │
 │  execute method ───────────────────►实际执行
 │        │            │              │
 │  emit event (EventBus)             │
 │◄───────────────────────────────────┤
```

### 缓存与指纹机制

KedroEngine 实现了智能缓存：
1. 计算节点签名: `signature = hash(inputs) + hash(method_code)`
2. 检查 `.pipeline/cache/datasets_index.json`
3. 缓存命中 → 直接加载 pkl；未命中 → 执行并保存

---

## 🚀 快速开始

### 环境要求

- Python 3.9+
- 依赖包: 见 `requirements.txt`

### 安装

```bash
# 克隆仓库
git clone https://github.com/Jusu2-teach/Stock-Analysis.git
cd Stock-Analysis

# 安装依赖
pip install -r requirements.txt
```

### 运行分析

```bash
# 1. 运行完整的趋势分析工作流
python -m pipeline run -c workflow/analysis.yaml

# 2. 断点续传（跳过已完成步骤）
python -m pipeline run -c workflow/analysis.yaml --resume

# 3. 查看执行状态
python pipeline/main.py status

# 4. 清除缓存重新执行
python pipeline/main.py cache --clear

# 5. 查看已注册的引擎方法
python pipeline/main.py engines
```

### 输出文件

| 文件 | 说明 |
|------|------|
| `data/comprehensive_analysis_report.md` | 综合分析报告（规则驱动） |
| `data/truth_analysis_report.md` | T.R.U.T.H. 基因报告 |
| `data/filter_middle/*.csv` | 中间趋势分析结果 |
| `.pipeline/cache/` | 执行缓存目录 |

---

## � 目录结构

```text
AStock-Analysis/
├── orchestrator/               # 🎼 调度与注册中心
│   ├── __init__.py
│   ├── config.py               # 全局配置
│   ├── errors.py               # 异常定义
│   ├── models.py               # 数据模型
│   ├── orchestrator.py         # 主调度器
│   ├── protocols.py            # 协议接口
│   ├── decorators/
│   │   └── register.py         # @register_method 装饰器
│   └── registry/
│       ├── registry.py         # 方法注册表（单例）
│       ├── discovery.py        # 自动发现
│       ├── executor.py         # 方法执行器
│       ├── strategies.py       # 5种选择策略
│       ├── index.py            # 索引管理
│       └── metrics.py          # 性能指标
│
├── pipeline/                   # 🚀 流程执行引擎
│   ├── main.py                 # CLI 入口
│   ├── cli.py                  # 命令行定义
│   ├── core/
│   │   ├── execute_manager.py  # 门面类
│   │   ├── dependency_graph.py # 依赖图
│   │   └── services/
│   │       ├── config_service.py   # YAML 解析
│   │       ├── state_service.py    # 状态管理
│   │       └── hook_manager.py     # 事件钩子
│   ├── engines/
│   │   ├── kedro_engine.py     # Kedro 数据引擎
│   │   └── prefect_engine.py   # Prefect 任务引擎
│   ├── io/                     # I/O 适配器
│   └── plugins/                # 插件系统
│
├── src/astock/                 # 🧬 业务逻辑层
│   └── business_engines/
│       ├── analyzers/          # 趋势分析器
│       │   └── trend/
│       │       ├── core.py     # TrendAnalyzer
│       │       ├── duckdb_engine.py  # DuckDB 实现 (已注册)
│       │       ├── models.py   # 数据模型
│       │       └── probes/     # 8种数学探针
│       │           ├── log_slope.py
│       │           ├── rolling.py
│       │           ├── inflection.py
│       │           └── deterioration.py
│       ├── truth/              # T.R.U.T.H. 系统
│       │   ├── processor.py    # 主处理器
│       │   ├── adapter.py      # ProbeAdapter
│       │   └── core/
│       │       ├── genes/      # 六维基因计算
│       │       │   ├── alpha.py    # α 周期性
│       │       │   ├── beta.py     # β 资本密度
│       │       │   ├── gamma.py    # γ 成长动能
│       │       │   └── delta.py    # δ 风险因子
│       │       └── solvers/    # 三大物理求解器
│       │           ├── gravity.py      # 重力求解器
│       │           ├── velocity.py     # 速度求解器
│       │           └── structure.py    # 结构求解器
│       └── reporters/          # 报告生成器
│           ├── comprehensive_generator.py
│           └── truth_generator.py
│
├── shared/                     # 📦 跨模块共享
│   ├── event_bus.py            # EventBus 事件总线
│   ├── naming_convention.py    # 统一命名规范
│   ├── events.py               # 事件类型定义
│   └── protocols.py            # 协议定义
│
├── workflow/                   # 📋 YAML 工作流配置
│   ├── analysis.yaml           # 主分析流程（推荐）
│   ├── tushare_basic.yaml      # 基础数据获取
│   └── tushare_fina.yaml       # 财务数据获取
│
├── data/                       # 📁 数据目录
│   ├── polars/                 # 输入数据
│   │   ├── 5yd_final_industry.csv
│   │   └── 10yd_final_industry.csv
│   ├── filter_middle/          # 中间结果
│   └── astock.duckdb           # DuckDB 数据库
│
├── docs/                       # 📚 文档
│   ├── ORCHESTRATOR_ARCHITECTURE.md
│   ├── PIPELINE_ARCHITECTURE.md
│   ├── TRUTH_SYSTEM_DESIGN.md
│   └── PROBE_ARCHITECTURE_REFACTORING.md
│
├── .github/
│   └── copilot-instructions.md # AI Agent 开发指南
│
├── requirements.txt
└── pyproject.toml
```

---

## 📊 数据要求

### 输入数据文件

系统默认读取 `data/polars/10yd_final_industry.csv`，支持 **≥3年** 的时序财务数据。

### 必需字段

| 字段名 | 类型 | 说明 | 用途 |
|--------|------|------|------|
| `ts_code` | string | 股票代码 | 唯一标识 |
| `name` | string | 公司名称 | 显示用 |
| `industry` | string | 所属行业 | 分类分析 |
| `end_date` | string/int | 报告期末日期 | 时序标识 |
| `roic` | float | 投入资本回报率 (%) | **核心筛选指标** |
| `roe` | float | 净资产收益率 (%) | 股东回报 |
| `grossprofit_margin` | float | 毛利率 (%) | 护城河指标 |
| `netprofit_margin` | float | 净利率 (%) | 盈利能力 |
| `eps` | float | 每股收益 (元) | 盈利增长 |
| `total_revenue_ps` | float | 每股营收 (元) | 营收增长 |
| `ocfps` | float | 每股经营现金流 | 盈利质量验证 |
| `invest_capital` | float | 投入资本 (元) | 规模分类 |

---

## 🔧 核心组件说明

### 命名规范系统 (naming_convention.py)

三层映射结构：

```python
from shared.naming_convention import MetricRegistry, ColumnBuilder

# 1. 业务键 → 指标定义
metric = MetricRegistry.get('roic')
# → MetricDefinition(business_key='roic', source_column='roic', output_prefix='roic')

# 2. 构建输出列名
col = ColumnBuilder.analysis_column('roic', 'slope')
# → 'roic_slope'

# 3. 完整流程
def process_metric(df, metric_name):
    metric = MetricRegistry.get(metric_name)
    source_col = metric.source_column      # 输入列名
    output_col = ColumnBuilder.analysis_column(metric.output_prefix, 'slope')  # 输出列名
```

### 注册装饰器 (@register_method)

```python
@register_method(
    component_type="business_engine",  # datahub | data_engine | business_engine
    engine_type="duckdb",              # duckdb | polars | tushare
    engine_name="analyze_metric_trend", # 方法名 (YAML引用)
    version="1.0.0",
    priority=10,
    description="趋势分析方法"
)
def analyze_metric_trend(data: pd.DataFrame, **params) -> pd.DataFrame:
    ...
```

---

## 🔌 扩展开发

### 添加新的分析指标

1. **定义指标** (`shared/naming_convention.py`)：

```python
METRIC_CONFIGS = {
    "new_metric": {
        "source_column": "new_metric",
        "output_prefix": "new_metric",
        "display_name": "新指标",
        "description": "新指标描述"
    }
}
```

2. **添加工作流步骤** (`workflow/analysis.yaml`)：

```yaml
- name: "Analyze_NewMetric_Trend"
  component: "business_engine"
  engine: "duckdb"
  method: ["analyze_metric_trend"]
  parameters:
    data: "steps.Load_Financial_Data.outputs.parameters.Raw_Data"
    metric_name: 'new_metric'
  outputs:
    parameters:
      - name: NewMetric_Trend_Result
```

3. **更新报告生成器** (`reporters/comprehensive_generator.py`)：

```python
self.metrics_config["new_metric"] = {
    "file": "new_metric_trend_analysis.csv",
    "prefix": "new_metric",
    "name": "新指标"
}
```

### 添加新的业务方法

```python
# src/astock/business_engines/analyzers/my_analyzer.py

from orchestrator.decorators.register import register_method

@register_method(
    component_type="business_engine",
    engine_type="duckdb",
    engine_name="my_custom_analysis",
    version="1.0.0"
)
def my_custom_analysis(data: pd.DataFrame, param1: str, param2: int = 10) -> pd.DataFrame:
    """
    自定义分析方法
    """
    # 业务逻辑
    return result_df
```

---

## ⚠️ 开发注意事项

1. **避免循环依赖**: 业务代码 (`src/astock`) 不应导入 `orchestrator` 或 `pipeline` 核心模块
2. **使用延迟导入**: `@register_method` 内部使用 lazy import
3. **DataFrame 传递**: Pipeline 步骤间通过 `steps.{StepName}.outputs.parameters.{OutputName}` 引用
4. **验证模式**: 设置 `ASTOCK_VALIDATION_MODE=strict|warn|off` 控制注册时的签名验证
5. **数据引擎选择**: 优先使用 DuckDB (SQL) 或 Polars (向量化) 处理大数据集

---

## 📚 相关文档

| 文档 | 说明 |
|------|------|
| [ORCHESTRATOR_ARCHITECTURE.md](docs/ORCHESTRATOR_ARCHITECTURE.md) | 调度层架构设计 |
| [PIPELINE_ARCHITECTURE.md](docs/PIPELINE_ARCHITECTURE.md) | 编排层架构设计 |
| [TRUTH_SYSTEM_DESIGN.md](docs/TRUTH_SYSTEM_DESIGN.md) | T.R.U.T.H. 系统设计 |
| [PROBE_ARCHITECTURE_REFACTORING.md](docs/PROBE_ARCHITECTURE_REFACTORING.md) | 探针系统重构 |
| [EVENT_BUS_ARCHITECTURE.md](docs/EVENT_BUS_ARCHITECTURE.md) | EventBus 架构 |
| [copilot-instructions.md](.github/copilot-instructions.md) | AI Agent 开发指南 |

---

## 📄 License

MIT License

---

## 🙏 致谢

- [Tushare](https://tushare.pro/) - A股数据源
- [DuckDB](https://duckdb.org/) - 高性能分析数据库
- [Polars](https://pola.rs/) - 高性能数据处理库
- [Prefect](https://www.prefect.io/) - 工作流编排引擎
- [Kedro](https://kedro.org/) - 数据管道框架
