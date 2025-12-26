# AStock Analysis - AI Agent 开发指南

## 项目概述
A股基本面量化分析系统。**四层架构**：Orchestrator (方法注册) → Pipeline (YAML工作流) → Business Engines (业务逻辑) → Shared (共享组件)。

## 🏗️ 架构与数据流
```
workflow/*.yaml (YAML配置)
    ↓ ConfigService 解析
Pipeline (execute_manager.py)
    ↓ Registry.select() 查找方法
Orchestrator (@register_method 注册)
    ↓ 调用业务方法
Business Engines (src/astock/business_engines/)
    ↓
输出: data/filter_middle/*.csv, data/*_report.md
```

## 🔧 添加业务方法 (核心模式)

**1. 在 `src/astock/business_engines/` 创建方法，使用装饰器注册：**
```python
# src/astock/business_engines/trend/engine.py
from orchestrator.decorators.register import register_method

@register_method(
    component_type="business_engine",  # datahub | data_engine | business_engine
    engine_type="duckdb",              # duckdb | polars | tushare | reporting | truth
    engine_name="analyze_metric_trend", # YAML 中 method: ["analyze_metric_trend"]
    version="1.0.0",
    priority=10
)
def analyze_metric_trend(data: pd.DataFrame, metric_name: str, **params) -> pd.DataFrame:
    ...
```

**2. 在 `workflow/analysis.yaml` 添加步骤：**
```yaml
- name: "Analyze_ROIC_Trend"
  component: "business_engine"
  engine: "duckdb"                      # 对应 engine_type
  method: ["analyze_metric_trend"]      # 对应 engine_name
  parameters:
    data: "steps.Load_Financial_Data.outputs.parameters.Raw_Data"  # 引用语法
    metric_name: 'roic'
  outputs:
    parameters:
      - name: ROIC_Trend_Result         # 供下游步骤引用
```

## 📐 命名规范 (必须遵循)

使用 `shared/naming_convention.py` 统一指标命名：
```python
from shared.naming_convention import MetricRegistry, ColumnBuilder

# 业务键 → 数据列映射
metric = MetricRegistry.get('revenue')  # → source_column: 'total_revenue_ps'
metric = MetricRegistry.get('profit')   # → source_column: 'eps'
metric = MetricRegistry.get('roic')     # → source_column: 'roic'

# 构建输出列名: {output_prefix}_{field}
ColumnBuilder.analysis_column('roic', 'slope')  # → 'roic_slope'
ColumnBuilder.analysis_column('roic', 'cagr')   # → 'roic_cagr'
```

## 🚀 常用命令
```bash
python pipeline/main.py run -c workflow/analysis.yaml          # 运行工作流
python pipeline/main.py run -c workflow/analysis.yaml --resume # 断点续传
python pipeline/main.py status                                 # 查看状态
python pipeline/main.py cache --clear                          # 清除缓存
python pipeline/main.py engines                                # 查看已注册方法
```

## ⚠️ 关键约束

1. **循环依赖**: `src/astock/` 业务代码禁止导入 `orchestrator/` 或 `pipeline/` 核心模块
2. **延迟导入**: `@register_method` 装饰器内部已使用 lazy import，无需额外处理
3. **引用语法**: 步骤间数据传递格式 `steps.{StepName}.outputs.parameters.{OutputName}`
4. **验证模式**: `ASTOCK_VALIDATION_MODE=strict|warn|off` 控制签名验证
5. **数据引擎**: 大数据集优先 DuckDB (SQL) 或 Polars (向量化)

## 📁 关键文件速查

| 任务 | 文件 |
|------|------|
| 添加业务方法 | `src/astock/business_engines/*/engine.py` |
| 修改工作流 | `workflow/analysis.yaml` |
| 添加新指标 | `shared/naming_convention.py` → METRIC_CONFIGS |
| 添加探针 | `src/astock/business_engines/trend/probes/` |
| 修改报告 | `src/astock/business_engines/reporters/` |
| T.R.U.T.H.系统 | `src/astock/business_engines/truth/` |

## 🧬 探针系统 (trend/probes/)

8种数学探针，每个文件一个探针类：
- `log_trend_probe.py` - OLS对数回归
- `rolling_probe.py` - 滚动窗口分析
- `deterioration_probe.py` - 恶化检测
- `inflection_probe.py` - 拐点检测 (Mann-Kendall)
- `cyclical_probe.py` - 周期性分析
- `volatility_probe.py` - 波动性分析
- `robust_probe.py` - 稳健统计
- `multi_horizon_probe.py` - 多时间窗口

## 📚 详细文档
- [ORCHESTRATOR_ARCHITECTURE.md](docs/ORCHESTRATOR_ARCHITECTURE.md)
- [PIPELINE_ARCHITECTURE.md](docs/PIPELINE_ARCHITECTURE.md)
- [TRUTH_SYSTEM_DESIGN.md](docs/TRUTH_SYSTEM_DESIGN.md)
- [PROBE_ARCHITECTURE_REFACTORING.md](docs/PROBE_ARCHITECTURE_REFACTORING.md)
