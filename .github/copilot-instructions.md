# AStock Analysis - AI Agent 开发指南

## 项目概述
A股基本面量化分析系统。**四层架构**：Orchestrator (方法注册中心) → Pipeline (YAML工作流引擎) → Business Engines (业务逻辑) → Shared (共享组件/事件总线)。

## 🏗️ 架构与数据流
```
workflow/*.yaml                           # YAML 定义工作流步骤
    ↓ ConfigService 解析
pipeline/core/execute_manager.py          # 执行管理器门面类
    ↓ Registry.select() 查找方法
orchestrator/registry/registry.py         # @register_method 方法注册中心
    ↓ 调用业务方法
src/astock/business_engines/*/engine.py   # 业务逻辑实现
    ↓
输出: data/filter_middle/*.csv, data/*_report.md
```

## 🔧 添加业务方法 (核心模式)

**1. 在 `src/astock/business_engines/*/engine.py` 使用装饰器注册：**
```python
from orchestrator.decorators.register import register_method

@register_method(
    component_type="business_engine",  # datahub | data_engine | business_engine
    engine_type="duckdb",              # duckdb | polars | tushare | reporting | truth
    engine_name="my_analysis",         # YAML 中 method: ["my_analysis"]
    version="1.0.0",
    priority=10
)
def my_analysis(data: pd.DataFrame, **params) -> pd.DataFrame:
    ...
```

**2. 在 `workflow/analysis.yaml` 添加步骤：**
```yaml
- name: "My_Step"
  component: "business_engine"
  engine: "duckdb"
  method: ["my_analysis"]
  parameters:
    data: "steps.Load_Financial_Data.outputs.parameters.Raw_Data"  # 跨步骤引用
  outputs:
    parameters:
      - name: My_Result  # 供下游引用
```

## 📐 命名规范 (必须遵循)

**所有指标必须通过 `shared/naming_convention.py` 统一定义**：
```python
from shared.naming_convention import MetricRegistry, ColumnBuilder

# 业务键 → 数据列映射 (已定义: roic, roe, revenue, profit, gross_margin, net_margin, ocf, roiic)
metric = MetricRegistry.get('revenue')  # → source_column: 'total_revenue_ps'

# 构建输出列名: {output_prefix}_{field}
ColumnBuilder.analysis_column('roic', 'slope')  # → 'roic_slope'
```

**添加新指标**: 在 `MetricRegistry._METRICS` 字典中添加 `MetricConfig` 条目。

## 🚀 常用命令
```bash
python pipeline/main.py run -c workflow/analysis.yaml          # 运行工作流
python pipeline/main.py run -c workflow/analysis.yaml --resume # 断点续传
python pipeline/main.py status                                 # 查看状态
python pipeline/main.py engines                                # 查看已注册方法
```

## ⚠️ 关键约束

1. **循环依赖禁止**: `src/astock/` 业务代码禁止导入 `orchestrator/` 或 `pipeline/` (仅 `@register_method` 装饰器除外)
2. **引用语法**: 步骤间数据传递 `steps.{StepName}.outputs.parameters.{OutputName}`
3. **验证模式**: `ASTOCK_VALIDATION_MODE=strict|warn|off` 控制签名验证
4. **数据引擎选择**: 大数据集优先 DuckDB (SQL) 或 Polars (向量化)
5. **EventBus 通信**: 跨层通信使用 `shared/event_bus/`，避免直接依赖

## 📁 关键文件速查

| 任务 | 文件 |
|------|------|
| 添加业务方法 | `src/astock/business_engines/*/engine.py` |
| 修改工作流 | `workflow/analysis.yaml` |
| 添加新指标 | `shared/naming_convention.py` → `MetricRegistry._METRICS` |
| 添加探针 | `src/astock/business_engines/trend/probes/*.py` |
| T.R.U.T.H.系统 | `src/astock/business_engines/truth/` (六维基因 + 三大求解器) |
| 报告生成 | `src/astock/business_engines/reporters/engine.py` |
| 数据文件 | `data/10yd_base/` (原始), `data/filter_middle/` (中间), `data/polars/` (聚合) |

## 🧬 探针系统 (8种数学探针)

每个探针一个文件，位于 `src/astock/business_engines/trend/probes/`:
- `log_trend_probe.py` - OLS对数回归 | `inflection_probe.py` - Mann-Kendall拐点
- `rolling_probe.py` - 滚动窗口 | `deterioration_probe.py` - 恶化检测
- `cyclical_probe.py` - 周期性 | `volatility_probe.py` - 波动性
- `robust_probe.py` - 稳健统计 | `multi_horizon_probe.py` - 多时间窗口

## 🧪 T.R.U.T.H. 系统 (六维基因)

位于 `src/astock/business_engines/truth/`:
- **六维基因**: α(周期性) β(资本密度) γ(成长动能) δ_fraud(欺诈熵) δ_decay(衰退熵) V(验证因子)
- **三大求解器**: `gravity_solver`(ROIC阈值) `velocity_solver`(增长边界) `structure_solver`(护城河)
- **数据流**: Pipeline探针 → `DataFrameToProbeConverter` → `ProbeAdapter` → `compute_genome_from_probes()`

## 🔌 已注册方法示例

| engine_name | 功能 | 位置 |
|-------------|------|------|
| `load_file` | 加载CSV/Parquet | trend/engine.py |
| `analyze_metric_trend` | 指标趋势分析 | trend/engine.py |
| `compute_derived_metrics` | 派生指标(ROIIC) | trend/engine.py |
| `generate_truth_report` | T.R.U.T.H.报告 | truth/truth_engine.py |
| `generate_comprehensive_report` | 综合报告 | reporters/engine.py |

## 📚 详细文档
- [ORCHESTRATOR_ARCHITECTURE.md](docs/ORCHESTRATOR_ARCHITECTURE.md) - 方法注册与路由
- [PIPELINE_ARCHITECTURE.md](docs/PIPELINE_ARCHITECTURE.md) - YAML工作流引擎
- [TRUTH_SYSTEM_DESIGN.md](docs/TRUTH_SYSTEM_DESIGN.md) - T.R.U.T.H.六维基因系统
- [EVENT_BUS_ARCHITECTURE.md](docs/EVENT_BUS_ARCHITECTURE.md) - 事件总线设计
- [PROBE_ARCHITECTURE_REFACTORING.md](docs/PROBE_ARCHITECTURE_REFACTORING.md) - 探针架构
