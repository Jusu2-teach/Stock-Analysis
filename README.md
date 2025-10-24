# 🧠 AStock - 智能股票分析系统

🚀 **企业级股票分析管道平台** | **Prefect-Kedro 混合架构** | **完全动态组件发现**

[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/)
[![Prefect](https://img.shields.io/badge/prefect-3.4.20-purple.svg)](https://prefect.io/)
[![Kedro](https://img.shields.io/badge/kedro-1.0.0-orange.svg)](https://kedro.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

> **企业级股票分析平台，基于完全动态的Orchestrator架构**

## 📋 快速入门指南 (5分钟上手)

### 1️⃣ 安装系统 (2分钟)

### 3️⃣ 验证安装 (30秒)

```bash
# 检查系统状态
python pipeline/main.py status

# 查看可用组件
python pipeline/main.py engines
```

### 4️⃣ 运行第一个管道 (2分钟)

```bash
# 获取2024年财务指标数据
python pipeline/main.py run -c pipeline/configs/tushare_fina.yaml
```

**预期结果:**
- ✅ 获取7,000+只股票数据
- ✅ 生成CSV文件: `data/20241231_fina_indicator.csv`
- ✅ 包含109个财务指标

### 5️⃣ 查看数据 (30秒)

```bash
# 验证生成的数据
python -c "
import pandas as pd
df = pd.read_csv('data/20241231_fina_indicator.csv')
print(f'📊 成功获取 {len(df)} 只股票的财务数据')
print(f'💰 包含 {len(df.columns)} 个财务指标')
print('🎯 前5只股票:')
print(df[['ts_code', 'eps', 'roe', 'roa']].head())
"
```

---

## 🎯 项目概述

AStock是一个现代化的股票分析系统，采用**Prefect-Kedro混合架构**，实现了"prefect将一个完整的kedro pipeline视为一个单一的黑箱task"的设计理念。系统提供了完全动态的组件发现、智能的工作流编排和强大的数据处理能力。
## 运行模式 (Hybrid Only)

当前版本已精简为单一 Hybrid (Prefect + Kedro) 模式：
- YAML steps -> 自动生成 kedro 风格节点 -> Prefect Flow 调度 -> KedroEngine 节点执行
- 支持: 缓存 (签名匹配跳过)、软失败(soft_fail)、血缘(lineage)
- 任何 engine 参数将被忽略，仅用于兼容旧代码调用

### 缓存调试
首次运行生成 .pipeline/cache 内容；再次运行若参数 / 上游指纹 / 方法链一致会命中：
```
[CACHE CHECK] step=load_xxx ...
🧩 Cache hit: load_xxx (signature matched) -> skip execution
```
测试或强制重算可调用:
```python
from pipeline.core.execute_manager import ExecuteManager
ExecuteManager.clear_cache()
```

- 🏗️ **混合架构**: Prefect负责工作流编排，Kedro负责数据管道处理
- 🔍 **智能组件发现**: 自动发现和注册所有数据源、处理引擎和业务逻辑
- 🔌 **零硬编码**: 完全动态的接口生成，新组件自动可用
- ⚡ **即插即用**: `brain.component.method()` 统一调用风格
- 📊 **多数据类型支持**: 完美支持DataFrame、Dict、List等各种Python数据类型
- 🔥 **多方法支持**: 一个step可包含多个methods，大幅简化配置文件
- 💎 **企业级**: 专业架构，支持大规模生产环境
- 🎛️ **配置驱动**: 通过YAML配置文件定义复杂的数据处理管道
- 📈 **高性能**: 支持并发执行、错误处理、监控和日志记录
- 🔧 **高度可扩展**: 模块化设计，轻松添加新的数据源和处理逻辑

## 🏛️ 系统架构

```
📦 AStock 系统架构 (v4.0+)
├── 🎯 Pipeline 系统 (根目录)         # 工作流编排系统（独立）
│   ├── 工作流管理                    # 任务调度和并发控制
│   ├── 执行引擎                     # Prefect-Kedro混合执行器
│   └── 监控系统                     # 任务状态监控和日志
├── 🧠 Orchestrator 系统 (根目录)     # 组件编排系统（独立，与 Pipeline 平级）
│   ├── 组件发现                     # 自动组件注册和管理
│   ├── 方法注册                     # 智能方法映射系统
│   ├── 策略选择                     # 多版本/多引擎策略
│   └── 执行管理                     # 统一的执行接口
└── 🔧 Component Layer (src/astock)  # 组件实现层
    ├── 📥 DataHub (多方法)           # 数据资源管理器
    ├── ⚙️ DataEngines (多方法)       # 数据处理引擎
    └── 🏢 BusinessEngines (多方法)   # 业务逻辑引擎
```

**📍 重要变更 (v4.0+)**: `orchestrator` 已从 `src/astock/orchestrator` 移至根目录，与 `pipeline` 成为平级的独立系统。这体现了两者的平等关系：
- **Pipeline**: 工作流编排和调度
- **Orchestrator**: 组件注册和方法调用

## 🏢 Business Engine 模块

### 📈 趋势分析系统概览
- 新版趋势分析体系由 `TrendAnalyzer`、`ConfigResolver`、`TrendRuleEvaluator`、`TrendResultCollector` 与 `trend_rule_engine` 五大构件协同完成。
- 每个构件职责单一：`TrendAnalyzer` 负责指标序列计算，`ConfigResolver` 管理行业差异化参数，`TrendRuleEvaluator` 承担规则引擎调用，`TrendResultCollector` 聚合输出，`trend_rule_engine` 则定义全部评分/淘汰逻辑。
- 通过 `analyze_metric_trend` 将这些“积木”按顺序拼装，实现高度模块化的主流程，便于后续扩展与测试。

### 🔄 执行流程
1. **数据加载与标准化**：DuckDB 查询取出分组序列，并确定 `keep_cols`（如 `name`、`industry`）。
2. **分组分析 (`TrendAnalyzer`)**：对每个分组生成加权平均、Log 斜率、波动率、拐点、近期恶化、周期性、滚动趋势等上下文信息；失败分组自动降级跳过。
3. **行业参数解析 (`ConfigResolver`)**：基于基础配置 + 行业覆盖生成当前分组的阈值；同时记录行业使用频次用于运行日志。
4. **规则评估 (`TrendRuleEvaluator`)**：将分析上下文喂给 `TrendRuleEngine`（包含 veto/penalty/bonus 规则）；完成周期谷底阈值放宽、累积罚分封顶等业务判定。
5. **结果写出 (`TrendResultCollector`)**：将分组输出行追加至内存缓存，最后统一转为 DataFrame 返回上游流程使用。

### 🧩 核心组件说明
- **TrendAnalyzer (`trend_analyzer.py`)**：封装所有数值计算，提供 `build_trend_vector`、`build_snapshot` 与 `build_result_row`，保证主流程无需关心具体指标详解。异常时落入安全默认值，确保结果结构稳定。
- **ConfigResolver (`trend_components.py`)**：合并基础配置与行业覆盖，支持统计使用情况，方便诊断哪个行业触发了差异化阈值。
- **TrendRuleEvaluator (`trend_components.py`)**：组合 `TrendRuleConfig`、上下文与规则引擎，输出统一的 `TrendEvaluationResult`（通过/淘汰、罚分、加分、原因）。
- **TrendResultCollector (`trend_components.py`)**：轻量化收集器，负责维护输出行列表并在最后转换为 DataFrame。
- **TrendRuleEngine (`trend_rules.py`)**：一组声明式规则（veto、penalty、bonus）决定淘汰、罚分与奖励策略；对周期、拐点、恶化、趋势加速度等维度做统一裁决。

### ⚙️ 行业配置与规则解耦
- 默认参数来自 `DEFAULT_FILTER_CONFIG`，行业覆盖表 `INDUSTRY_FILTER_CONFIGS` 可在 `config.py` 中扩展或外部注入。
- `TrendRuleConfig.from_dict` 将配置字典转换为类型化的阈值/参数对象，确保规则引擎始终读取一致的数值空间。
- 通过 `ConfigResolver` 的使用统计，可快速检查哪些行业触发了专属规则，有利于排查阈值设置是否合理。

### 📊 结果数据结构
- 输出列以 `{prefix}{metric_name}_<field>{suffix}` 方式命名，既兼容多指标并行分析，又便于下游消费。
- 主体指标涵盖加权平均、Log 斜率、R²、CAGR、波动率、拐点、恶化、周期、滚动趋势等多个维度，同时保留行业阈值曝光字段以支持诊断。
- 若启用过滤，会额外写出罚分与扣分明细列，帮助复盘规则命中情况。

### ✅ 测试与扩展
- `tests/test_trend_components.py` 覆盖行业参数合并、规则通过/淘汰路径以及结果收集行为，可作为后续新增规则或配置策略的回归基线。
- 新增业务要求通常只需扩展对应组件（如添加新规则或诊断字段），主流程保持稳定，降低回归风险。

---

## 🚀 详细安装指南

### 📋 环境要求

- **Python**: 3.8+ (推荐 3.12+)
- **操作系统**: Windows/Linux/macOS
- **内存**: 4GB+ RAM (推荐8GB)
- **磁盘空间**: 2GB+ 可用空间
- **网络**: 互联网连接（用于数据源API访问）

### 🔧 安装方法


一键安装脚本会自动完成：
- ✅ Python版本检查
- ✅ 虚拟环境创建
- ✅ pip升级
- ✅ 依赖包安装
- ✅ 系统验证
- ✅ 激活脚本生成

#### 🔧 方法2: 手动安装

##### 1️⃣ 项目获取
```bash
# 克隆项目
git clone https://github.com/your-repo/astock-analysis.git
cd astock-analysis
```

##### 2️⃣ Python虚拟环境配置

> ⚠️ **重要**: 强烈建议使用虚拟环境以避免依赖冲突

**Windows 用户:**
```powershell
# 创建虚拟环境
python -m venv .venv

# 激活虚拟环境
.venv\Scripts\activate

# 验证虚拟环境已激活（命令提示符前会显示 (.venv)）
where python
# 应该显示: <项目路径>\.venv\Scripts\python.exe
```

**Linux/macOS 用户:**
```bash
# 创建虚拟环境

Remove-Item -Recurse -Force .venv
python -m venv .venv

# 激活虚拟环境
source .venv/bin/activate

# 验证虚拟环境已激活
which python
# 应该显示: <项目路径>/.venv/bin/python
```

##### 3️⃣ 依赖包安装

```bash
# 确保虚拟环境已激活后安装依赖
pip install --upgrade pip  # 升级pip到最新版本
pip install -r requirements.txt

# 安装可能缺少的关键依赖
pip install tushare>=1.4.0  # 确保tushare已安装
```

#### 4️⃣ 数据源配置

##### 配置 Tushare Pro API:
1. 访问 [Tushare Pro官网](https://tushare.pro/) 注册账号
2. 获取API Token
3. 修改 `src/astock/datahub/tushare.py` 中的token或通过环境变量设置

```python
# 在代码中直接配置
pro = ts.pro_api('your_tushare_token_here')

# 或通过环境变量配置
export TUSHARE_TOKEN='your_tushare_token_here'
```

#### 5️⃣ 系统验证

```bash
# 验证Python环境
python --version

# 验证系统状态
python pipeline/main.py status
```

**✅ 期望输出：**
```
✅ SUCCESS: Pipeline manager initialized
🧠 Brain Status:
   Version: 3.0-intelligent
   Status: active
   Methods: 13+

🔧 Components (3+):
   • business_engine
   • data_engine
   • datahub
```

#### 6️⃣ 验证数据源连接

```bash
# 测试 tushare 连接
python -c "import tushare as ts; print('Tushare version:', ts.__version__); pro = ts.pro_api('your_token'); print('✅ Tushare连接成功')"

# 测试 akshare 连接
python -c "import akshare as ak; print('Akshare version:', ak.__version__); print('✅ Akshare连接成功')"
```

### 🎯 安装后快速启动

#### 使用激活脚本 (推荐):
**Windows:**
```cmd
# 双击运行或命令行执行
activate_astock.bat
```

**Linux/macOS:**
```bash
# 运行激活脚本
./activate_astock.sh
```

#### 手动激活虚拟环境:
**Windows:**
```cmd
.venv\Scripts\activate
```

**Linux/macOS:**
```bash
source .venv/bin/activate
```

---

## 🎯 常用命令参考

### 系统管理
```bash
python pipeline/main.py status          # 系统状态
python pipeline/main.py engines         # 可用引擎
python pipeline/main.py engines -v      # 详细引擎信息
```

### 配置管理
```bash
python pipeline/main.py validate -c config.yaml    # 验证配置
python pipeline/main.py flow -c config.yaml        # 查看数据流
python pipeline/main.py template -o new_config.yaml # 生成模板
```

### 管道执行
```bash
python pipeline/main.py run -c pipeline/configs/tushare_fina.yaml     # 财务指标
python pipeline/main.py run -c pipeline/configs/pipeline.yaml         # 股票分析
python pipeline/main.py run -c pipeline/configs/demo_multi_io.yaml -e prefect --granularity node  # 节点级 Prefect
```

---

## 📖 使用指南

### 🔥 核心功能示例

### 📡 引用语法与数据集命名

管道 steps 之间参数传递使用统一引用格式：

```
steps.<上游step名>.outputs.parameters.<输出名>
```

在 Kedro / Prefect 内部会被转换为数据集名称：

```
<上游step名>__<输出名>
```

例如：`steps.1234.outputs.parameters.1234data` → 数据集 `1234__1234data`。

### 🧩 granularity 粒度模式

在 `pipeline.orchestration.granularity` 设置执行粒度：

| 值 | 行为 | 适用场景 |
|----|------|----------|
| pipeline | 整个自动生成的 Kedro pipeline 作为单个 Prefect 任务 | 简单/快速运行、统一重试 |
| node | 每个 Kedro 节点映射为单独 Prefect 任务，拓扑分层调度 | 细粒度监控、选择性重试、并发 |

CLI 可临时覆盖：
```
python pipeline/main.py run -c config.yaml -e prefect --granularity node
```

### 🧠 运行时引用解析逻辑
1. 构建阶段保留 `{"__ref__": <ref>, "hash": <md5>}` 占位，不提前解析。
2. 节点执行前尝试：
   - 从 KedroEngine.global_catalog / DataCatalog 取对应数据集。
   - 命中后立即回填到 ExecuteManager.global_registry（hash → value）。
3. 下游节点解析失败会抛出 `ReferenceResolutionError`，提示检查输出名。

### ⚡ 节点级缓存 (node granularity)
节点任务会组装签名：`<上游输入指纹链>#<节点名称>`，命中后复用上次产出的数据集并标记日志：`🧩 (NodeCache) 命中`。

### 🛡️ soft_fail 机制
在 orchestration 中配置：
```yaml
pipeline:
  orchestration:
    soft_fail: true
```
或在 node 粒度启用后仍可通过 CLI 覆盖 granularity。开启后：
* 失败节点标记为 failed + soft_fail，不终止流程。
* 依赖该失败节点的下游会被自动跳过 (skipped)。

### 🧪 故障演示
在 `demo_multi_io.yaml` 中已提供注释的 `fail_demo` 节点，可去掉注释制造引用失败并观察 soft_fail 行为（先启用 soft_fail）。

### 📊 结果结构差异
| 模式 | 关键字段 | 说明 |
|------|----------|------|
| kedro | lineage, node_metrics | 由 KedroEngine 汇总 |
| prefect (pipeline) | task_results, layer_metrics | 任务层级统计 |
| prefect (node) | node_results, cached_nodes, layer_metrics, lineage, node_metrics | 节点级调度+缓存+血缘 |

### 🧵 并发
node 模式下可设置：
```yaml
pipeline:
  orchestration:
    granularity: node
    task_runner: concurrent
    max_workers: 4
```

同一拓扑层中的节点会并发执行（例如多个仅依赖同一个上游的存储或分析节点）。

### 🔒 参数绑定与严格模式 (2025-10 最新)

当前版本已移除所有“隐式别名/魔法”参数注入逻辑，彻底遵循：

> YAML 中 `parameters` 写什么，方法就收到什么；没有 `data/df/dataset` 等保留字。

#### 1. 已移除的历史行为
| 旧行为 | 当前状态 | 理由 |
|--------|----------|------|
| 自动把上游结果注入 `data` / `df` / `dataset` 参数 | 删除 | 造成三方库(例如 `write_csv`) 收到未知 kw 导致报错 |
| 单输入时自动把上游结果绑定为第一个参数 | 改为“受控启发式” | 仅在非严格模式且满足唯一必填参数条件才注入 |
| 多输入别名推断(InputInferenceService) | 删除 | 隐式推断不透明，易产生歧义 |
| primary_policy / metadata_provider | 删除 | 简化核心，统一走显式定义 |

#### 2. 现在的绑定规则
1. 读取方法真实函数签名（必填位置 / 关键字参数 / 默认值）
2. YAML `parameters` 中的键严格一一匹配函数参数名
3. 不再为缺失参数尝试别名扩展
4. 仅当且仅当满足以下全部条件，系统才会“自动”把上游唯一结果当作该方法的唯一必填参数传入（启发式）：
   - 当前方法还缺失的必填参数个数 = 1
   - YAML 未提供该参数
   - 上游聚合结果数量 = 1
   - 运行时环境变量未启用严格模式（见下）

否则：缺啥报错，杜绝沉默注入。

#### 3. 严格模式开关
设置环境变量 `ASTOCK_STRICT_PARAMS=1` 可完全关闭上述启发式注入；此时：
* 任意缺失的必填参数 -> 立即抛出绑定错误
* 不再尝试将上游结果塞入任何参数

Windows (PowerShell):
```powershell
$env:ASTOCK_STRICT_PARAMS=1
python pipeline/main.py run -c pipeline/configs/tushare_fina.yaml
```

Linux / macOS:
```bash
export ASTOCK_STRICT_PARAMS=1
python pipeline/main.py run -c pipeline/configs/tushare_fina.yaml
```

取消：
```powershell
Remove-Item Env:ASTOCK_STRICT_PARAMS
```

#### 4. 迁移提示
如果你之前依赖 `data/df/dataset` 自动注入：
```yaml
parameters:
  path: data/out.csv   # 旧：依赖隐式 data -> store(data=上游)
```
请改为显式：
```yaml
parameters:
  path: data/out.csv
  data: steps.上游名字.outputs.parameters.XXX   # 或在多方法链中按需引用
```

#### 5. 调试建议
| 场景 | 建议 |
|------|------|
| 绑定失败 | 确认参数名是否与函数定义一致（区分大小写） |
| 怀疑注入 | 打开严格模式验证是否仍可运行 |
| 想看引用是否解析 | 运行加 `--verbose` (若实现) 或查看节点日志中 inputs= 数量 |

#### 6. 设计理念
保持“显式优于隐式”：流水线 YAML 是唯一事实来源 (single source of truth)，调试与复现成本最低；启发式只是便利选项（且可一键关闭）。

> 若你正在开发自定义方法：请使用清晰的参数名，不依赖历史别名，以保证在严格模式下仍可工作。

---

---

#### 1. 财务指标数据获取
```yaml
# pipeline/configs/tushare_fina.yaml
pipeline:
  name: "Tushare财务指标数据测试"
  steps:
    - name: "获取财务指标数据"
      component: "datahub"
      engine: "tushare"
      method:
        - "fina_indicator_vip"
      parameters:
        period: "20241231"  # 2024年年报

    - name: "存储财务指标数据"
      component: "datahub"
      engine: "tushare"
      method:
        - "store"
      parameters:
        path: "data/20241231_fina_indicator.csv"
      depends_on: ["获取财务指标数据"]
```

#### 2. 股票基础数据获取
```yaml
pipeline:
  name: "股票基础数据获取"
  steps:
    - name: "获取股票列表"
      component: "datahub"
      engine: "tushare"
      method: "stock_basic"

    - name: "获取日线数据"
      component: "datahub"
      engine: "tushare"
      method: "daily"
      parameters:
        ts_code: "000001.SZ"
        start_date: "20240101"
        end_date: "20241231"
      depends_on: ["获取股票列表"]
```

### 🔧 高级配置示例

#### 获取股票基础数据
```yaml
# my_config.yaml
pipeline:
  name: "股票数据获取"
  steps:
    - name: "获取股票列表"
      component: "datahub"
      engine: "tushare"
      method: "stock_basic"

    - name: "存储数据"
      component: "datahub"
      engine: "tushare"
      method: "store"
      parameters:
        path: "data/stock_list.csv"
      depends_on: ["获取股票列表"]
```

#### 多步数据处理
```yaml
pipeline:
  name: "股票分析流水线"
  steps:
    - name: "数据获取"
      component: "datahub"
      engine: "akshare"
      method: "balance_sheet"

    - name: "数据清理"
      component: "data_engine"
      engine: "pandas"
      method: "data_cleaning"
      depends_on: ["数据获取"]

    - name: "财务分析"
      component: "business_engine"
      engine: "duckdb"
      method: "financial_ratios"
      depends_on: ["数据清理"]
```

#### 多方法执行
```yaml
steps:
  - name: "数据处理"
    component: "data_engine"
    engine: "pandas"
    method:
      - "data_cleaning"      # 1. 数据清理
      - "data_validation"    # 2. 数据验证
      - "data_transformation" # 3. 数据转换
```

#### 并行处理
```yaml
steps:
  - name: "数据获取A"
    component: "datahub"
    engine: "tushare"
    method: "stock_basic"

  - name: "数据获取B"
    component: "datahub"
    engine: "akshare"
    method: "balance_sheet"
    # 无depends_on，与步骤A并行执行

  - name: "数据合并"
    component: "data_engine"
    engine: "pandas"
    method: "merge_data"
    depends_on: ["数据获取A", "数据获取B"]
```

---

## 📊 支持的数据源

### 1. Tushare Pro (推荐)
- **股票基础信息**: `stock_basic`
- **日线数据**: `daily`
- **财务数据**: `income`, `balancesheet`
- **财务指标**: `fina_indicator_vip`
- **期权数据**: `opt_basic`

### 2. AKShare (免费)
- **资产负债表**: `balance_sheet`
- **北向资金**: `hsgt_board_rank`
- **行业板块数据**
- **更多功能持续扩展中...**

---

## 🚨 常见问题解决

### Q: 提示"tushare模块未安装"
**A:** 确保在虚拟环境中运行
```bash
# Windows
.venv\Scripts\activate
pip install tushare

# Linux/macOS
source .venv/bin/activate
pip install tushare
```

### Q: 管道执行失败
**A:** 检查配置文件和依赖关系
```bash
python pipeline/main.py validate -c your_config.yaml
python pipeline/main.py flow -c your_config.yaml
```

### Q: 数据获取失败
**A:** 检查网络连接和API配置
```bash
# 测试tushare连接
python -c "import tushare as ts; pro = ts.pro_api('your_token'); print(pro.stock_basic().head())"

# 测试akshare连接
python -c "import akshare as ak; print(ak.stock_zh_a_spot_em().head())"
```

### Q: "tushare模块未安装" 错误
```bash
# 解决方案：确保在虚拟环境中安装
.venv\Scripts\activate  # Windows
# source .venv/bin/activate  # Linux/macOS
pip install tushare>=1.4.0
```

### Q: 虚拟环境未正确激活
```bash
# 检查当前Python路径
python -c "import sys; print(sys.executable)"
# 应该指向 .venv 目录中的Python

# 重新激活虚拟环境
.venv\Scripts\activate  # Windows
```

### Q: 依赖版本冲突
```bash
# 清理并重新安装依赖
pip freeze > old_requirements.txt  # 备份当前环境
pip uninstall -r old_requirements.txt -y
pip install -r requirements.txt
```

### Q: 网络连接问题
```bash
# 使用国内镜像源加速
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple/
```

---

## 🛠️ 开发指南

### 添加新的数据源

1. 在 `src/astock/datahub/` 创建新文件
2. 实现数据获取方法
3. 使用 `@register_method` 装饰器注册
4. 系统会自动发现并集成

```python
# src/astock/datahub/my_datasource.py
# 新版 (v4.0+) 模块化编排：orchestrator 已移至根目录
from orchestrator import register_method

@register_method(
  component_type="datahub",      # 组件类别
  engine_type="my_datasource",   # 引擎/实现类型 (旧 engine_name / method_name 统一为 engine_type + engine_name)
  engine_name="get_data",        # 对外暴露方法名 (调用: orchestrator.datahub.get_data(...))
  version="1.0.0",
  priority=0,
  description="示例数据源实现"
)
def get_data(symbol: str) -> dict:
  # 实现数据获取逻辑
  return {"data": "example"}
```

### 添加新的处理引擎

```python
# src/astock/data_engines/engines/my_engine.py
# orchestrator 已移至根目录
from orchestrator import register_method

@register_method(
  component_type="data_engines",  # 与目录名保持一致 (data_engines)
  engine_type="my_engine",        # 引擎分类 (如 pandas / polars / my_engine)
  engine_name="process_data",     # 方法名
  version="0.1.0",
  description="自定义数据处理引擎示例"
)
def process_data(df):
  # 实现数据处理逻辑，这里直接透传
  return df
```

### 🔁 运行时调用示例 (v4.0+)

```python
# orchestrator 已移至根目录，与 pipeline 平级
from orchestrator import AStockOrchestrator

o = AStockOrchestrator(auto_discover=True)
data = o.datahub.get_data("000001.SZ")
res  = o.data_engines.process_data(data)
```

### 🧩 策略与引擎选择

```python
# 默认策略: priority > version (latest) > 非 deprecated
o.execute("datahub", "get_data")

# 指定策略
o.execute("datahub", "get_data", _strategy="latest")

# 指定引擎 (engine_type 精确选择)
o.execute("datahub", "get_data", _engine_type="my_datasource")
```

### 🧪 输入风格控制 (ASTOCK_INPUT_STYLE)

统一输入参数风格，避免历史“单元素列表”伪多输入混淆：

环境变量: `ASTOCK_INPUT_STYLE`

| 值 | 语义 | 规则 |
|----|------|------|
| strict_single (默认) | 严格单对象 | 禁止以单元素 list/tuple 作为唯一位置参数，除非函数首参类型注解为 list/Iterable |
| allow_list | 放宽 | 不做校验 (兼容模式) |
| enforce_list | 强制列表 | 要求首个位置参数必须是 list/tuple |

示例 (Windows PowerShell):
```powershell
$Env:ASTOCK_INPUT_STYLE = "strict_single"
python -m pipeline.main run -c workflow\tushare_fina.yaml
```

若触发校验异常，可临时切换：
```powershell
$Env:ASTOCK_INPUT_STYLE = "allow_list"
```

### 🛠 迁移说明 (v3 -> v4)

| 变更项 | v3 (旧) | v4 (新) |
|--------|--------|---------|
| 注册装饰器导入 | from astock.orchestrator.core import register_method | from orchestrator import register_method |
| Orchestrator 导入 | from astock.orchestrator import AStockOrchestrator | from orchestrator import AStockOrchestrator |
| 内部核心 | intelligent_registry 单文件 | 模块化 registry/index/strategies/loader/... |
| 自动发现函数 | auto_load_all_components | registry.auto_load() (由 orchestrator 自动调用) |
| 方法选择 | execute_method / select_registration | execute / 策略参数 _strategy / _engine_type |
| 输入参数隐式别名 | data/df/dataset 自动注入 | 完全移除，需显式命名 |
| 输入风格 | 可混用单对象 / 单元素列表 | 受 ASTOCK_INPUT_STYLE 控制 |
| 热刷新 | refresh_components | registry.refresh() |

> v4 去除了所有旧兼容路径 (legacy core/intelligent_registry 已物理删除)，确保语义统一、行为可预测。orchestrator 现已移至根目录，与 pipeline 系统平级。若代码仍引用 `astock.orchestrator`，请改为 `from orchestrator import AStockOrchestrator`。

---

## 📝 项目重命名说明

### 🎯 重命名完成状态

项目已从 **equity-analysis** 成功重命名为 **astock-analysis**：

#### ✅ 已更新的标识：

- **项目名称**: `astock-analysis`
- **包名称**: `astock-analysis`
- **文件夹名**: `AStock-Analysis`
- **系统名称**: AStock 智能股票分析系统
- **仓库名**: `astock-analysis`

#### ✅ 已更新的文件：

1. **pyproject.toml** - 项目配置和依赖
2. **README.md** - 项目文档
3. **.gitignore** - Git忽略规则

#### 🔄 Git仓库配置

如果使用Git，请更新远程仓库URL：

```bash
# 更新远程仓库URL
git remote set-url origin https://github.com/yourusername/astock-analysis.git

# 验证远程仓库配置
git remote -v
```

---

## 📝 更新日志

### v4.0.0 (2025-10-06)
模块化 Orchestrator 重构：
* 🧱 拆分为 models / registry(index, strategies, metrics, hooks, loader, executor) / orchestrator facade / decorator
* 🔥 删除 monolith intelligent_registry 与所有隐式参数/别名桥接
* 🎯 引入统一输入风格校验 (ASTOCK_INPUT_STYLE)
* ⚙️ 策略模式抽象 (default / latest / stable / priority / engine override)
* 📊 内建执行指标采集 (成功/失败/耗时)
* 🧪 准备添加单元测试基座 (后续 v4.x)

### v3.1.0 (2025-10-06)
显式参数绑定重构 & 严格模式引入：
* 🔥 移除历史隐式别名注入 (data/df/dataset) 及单输入自动绑定副作用
* 🧹 删除遗留 InputInferenceService / primary_policy / metadata_provider 相关代码
* 🧵 新增启发式单唯一必填参数自动注入（可通过 ASTOCK_STRICT_PARAMS=1 禁用）
* 🛡️ 严格模式环境变量 `ASTOCK_STRICT_PARAMS` 上线，保证生产环境零隐式行为
* 🐛 修复因隐式注入导致的 `write_csv() got unexpected keyword argument 'df'` 错误
* 📘 README 增补《参数绑定与严格模式》章节，明确迁移与设计理念
* ⚙️ MethodHandle 强化：预测缓存、线程安全锁、invalidate 支持
* 🚫 移除过时的自动多输入推断逻辑，统一显式引用

### v3.0.0 (2024-10-04)
- ✅ 新增Tushare Pro API完整集成
- ✅ 实现fina_indicator_vip财务指标获取
- ✅ 修复虚拟环境配置问题
- ✅ 完善安装和配置文档
- ✅ 支持2024年最新财务数据获取
- ✅ 项目重命名为astock-analysis

### v2.0.0
- ✅ Prefect-Kedro混合架构
- ✅ 动态组件发现
- ✅ 多方法支持

### v1.0.0
- ✅ 基础系统框架
- ✅ AKShare数据源集成

---

## 🤝 贡献指南

1. Fork 本项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

## 📄 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情

## 🆘 支持与联系

- **问题反馈**: [GitHub Issues](https://github.com/your-repo/astock-analysis/issues)
- **功能建议**: [GitHub Discussions](https://github.com/your-repo/astock-analysis/discussions)
- **文档**: [项目Wiki](https://github.com/your-repo/astock-analysis/wiki)

## 📚 更多资源

- **快速入门**: 本文档顶部5分钟快速入门部分
- **配置示例**: pipeline/configs/
- **源码结构**: src/astock/
- **帮助命令**: `python pipeline/main.py --help`

---

**🎉 恭喜！您已经成功入门AStock系统！**

**⭐ 如果这个项目对您有帮助，请给我们一个Star！**