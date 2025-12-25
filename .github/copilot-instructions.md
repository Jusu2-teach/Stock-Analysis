# AStock Analysis - AI Agent 开发指南

## 项目概述

A股基本面量化分析系统，使用 **Orchestrator + Pipeline + Business Engines** 三层架构。核心目标：通过多年财务数据趋势分析筛选优质企业。

## 🏗️ 架构速览

```
Orchestrator (调度层)  →  方法注册中心，@register_method 自动发现
     ↓
Pipeline (编排层)      →  YAML 驱动工作流，workflow/*.yaml 定义步骤
     ↓
Business Engines       →  零侵入业务代码，src/astock/business_engines/
```

## 📁 核心目录约定

| 目录 | 职责 | 关键文件 |
|------|------|----------|
| `orchestrator/` | 方法注册与策略路由 | `decorators/register.py`, `registry/registry.py` |
| `pipeline/core/` | 工作流执行引擎 | `execute_manager.py`, `services/config_service.py` |
| `src/astock/business_engines/` | 业务逻辑实现 | `analyzers/trend/duckdb_engine.py` |
| `shared/` | 跨模块共享组件 | `event_bus.py`, `naming_convention.py` |
| `workflow/` | YAML 工作流配置 | `analysis.yaml` |

## 🔧 注册业务方法 (核心模式)

所有业务方法必须使用 `@register_method` 装饰器注册到 Orchestrator：

```python
from orchestrator.decorators.register import register_method

@register_method(
    component_type="business_engine",  # datahub | data_engine | business_engine
    engine_type="duckdb",              # 引擎类型: duckdb | polars | tushare
    engine_name="analyze_metric_trend", # 方法名（YAML 中引用）
    version="1.0.0",
    priority=10,
    description="趋势分析方法"
)
def analyze_metric_trend(data: pd.DataFrame, **params) -> pd.DataFrame:
    ...
```

## 📋 YAML 工作流配置

Pipeline 步骤通过 YAML 定义，参考 [workflow/analysis.yaml](workflow/analysis.yaml)：

```yaml
steps:
  - name: "Step_Name"
    component: "business_engine"
    engine: "duckdb"
    method: ["method_name"]  # 对应 @register_method 的 engine_name
    parameters:
      data: "steps.Previous_Step.outputs.parameters.Output_Name"
    outputs:
      parameters:
        - name: Output_Name
```

## 🧬 T.R.U.T.H. 六维基因系统

核心评估框架位于 `src/astock/business_engines/truth/`：
- **六维基因**: α(周期敏感度), β(资本密度), γ(增长弹性), δ(风险因子), V(验证因子)
- **零配置设计**: 基因自动计算阈值，无需预设行业标签
- **探针系统**: `analyzers/trend/probes/` 包含 8 个数学探针 (OLS, Mann-Kendall, HP滤波等)

## 📐 命名规范

使用 [shared/naming_convention.py](shared/naming_convention.py) 统一指标命名：

```python
from shared.naming_convention import MetricRegistry, ColumnBuilder

# 获取指标
metric = MetricRegistry.get('roic')  # → source_column: 'roic'

# 构建输出列名: {metric}_{field}
col = ColumnBuilder.analysis_column('roic', 'slope')  # → 'roic_slope'
```

## 🚀 常用命令

```bash
# 运行完整分析工作流
python pipeline/main.py run -c workflow/analysis.yaml

# 恢复断点执行
python pipeline/main.py run -c workflow/analysis.yaml --resume

# 查看执行状态
python pipeline/main.py status
```

## ⚠️ 开发注意事项

1. **避免循环依赖**: 业务代码 (`src/astock`) 不应导入 `orchestrator` 或 `pipeline` 核心模块
2. **使用延迟导入**: `@register_method` 内部使用 lazy import 避免循环
3. **DataFrame 传递**: Pipeline 步骤间通过 `steps.{StepName}.outputs.parameters.{OutputName}` 引用
4. **验证模式**: 设置 `ASTOCK_VALIDATION_MODE=strict|warn|off` 控制注册时的签名验证
5. **数据引擎**: 优先使用 DuckDB (SQL) 或 Polars (向量化) 处理大数据集

## 📚 架构文档

- [ORCHESTRATOR_ARCHITECTURE.md](docs/ORCHESTRATOR_ARCHITECTURE.md) - 调度层设计
- [PIPELINE_ARCHITECTURE.md](docs/PIPELINE_ARCHITECTURE.md) - 编排层设计
- [TRUTH_SYSTEM_DESIGN.md](docs/TRUTH_SYSTEM_DESIGN.md) - T.R.U.T.H. 系统设计
- [PROBE_ARCHITECTURE_REFACTORING.md](docs/PROBE_ARCHITECTURE_REFACTORING.md) - 探针系统架构
