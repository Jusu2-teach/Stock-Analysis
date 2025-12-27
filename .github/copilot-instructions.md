# AStock Analysis - AI Agent 开发指南

## 架构概览 (四层解耦)
```
workflow/*.yaml → pipeline/core/execute_manager.py → orchestrator/registry → src/astock/business_engines/*/engine.py
```

| 层 | 职责 | 关键文件 |
|----|------|----------|
| **Orchestrator** | `@register_method` 装饰器自动注册，5种路由策略 | `orchestrator/decorators/register.py` |
| **Pipeline** | YAML配置驱动，断点续传(`--resume`)，指纹缓存 | `pipeline/core/execute_manager.py`, `pipeline/core/context.py` |
| **Business Engines** | 纯业务逻辑: 8种探针 + T.R.U.T.H.六维基因 | `src/astock/business_engines/*/engine.py` |
| **Shared** | EventBus跨层通信，Contracts数据契约，MetricRegistry命名 | `shared/` |

## 核心开发模式

### 1. 添加业务方法
```python
# src/astock/business_engines/{module}/engine.py
from orchestrator.decorators.register import register_method

@register_method(
    component_type="business_engine",  # datahub | data_engine | business_engine
    engine_type="duckdb",              # duckdb | polars | tushare | truth | reporting
    engine_name="my_analysis",         # YAML引用名 (默认=函数名)
)
def my_analysis(data: pd.DataFrame, **params) -> pd.DataFrame: ...
```

### 2. 添加工作流步骤 (YAML)
```yaml
# workflow/analysis.yaml
- name: "My_Step"
  component: "business_engine"
  engine: "duckdb"
  method: ["my_analysis"]
  parameters:
    data: "steps.Load_Financial_Data.outputs.parameters.Raw_Data"  # 跨步骤引用
  outputs:
    parameters:
      - name: My_Result
```

### 3. 指标命名 (必须使用)
```python
from shared.naming_convention import MetricRegistry, ColumnBuilder
MetricRegistry.get('roic')                       # 已定义: roic, roe, revenue, profit, gross_margin, net_margin, ocf, roiic
ColumnBuilder.analysis_column('roic', 'slope')   # → 'roic_slope'
# 新增指标: shared/naming_convention.py → MetricRegistry._METRICS
```

### 4. 跨层通信 (EventBus)
```python
from shared.event_bus import EventBus, NodeStartedEvent
bus = EventBus.get()
bus.emit(NodeStartedEvent(step_name="My_Step"))

@bus.on("pipeline.node.completed")
def on_completed(event): ...
```

### 5. 数据存储与引用 (Contracts/Store)
```python
from shared.contracts.store import DataStore, ReferenceResolver

store = DataStore()
store.put('key', data, ref='steps.step.outputs.parameters.output')
data = store.get_by_ref('steps.step.outputs.parameters.output')

resolver = ReferenceResolver(store)
resolved = resolver.resolve({'data': {'__ref__': 'steps.Load.outputs.parameters.Raw'}})
```

## 常用命令
```bash
python pipeline/main.py run -c workflow/analysis.yaml          # 运行工作流
python pipeline/main.py run -c workflow/analysis.yaml --resume # 断点续传
python pipeline/main.py engines                                # 查看已注册方法
python scripts/validate_yaml_naming.py                         # 验证YAML命名规范
ASTOCK_DEBUG=1 python pipeline/main.py run -c workflow/analysis.yaml  # 调试模式
```

## 关键约束
1. **循环依赖禁止**: `src/astock/` 禁止导入 `orchestrator/` 或 `pipeline/` (仅 `@register_method` 除外)
2. **跨层通信**: 使用 `shared/event_bus/` EventBus，不直接依赖
3. **验证模式**: `ASTOCK_VALIDATION_MODE=strict|warn|off` 控制签名验证
4. **大数据集**: 优先 DuckDB (SQL) 或 Polars (向量化)，避免 pandas 全量加载

## 路径速查
| 任务 | 路径 |
|------|------|
| 添加业务方法 | `src/astock/business_engines/*/engine.py` |
| 添加/修改工作流 | `workflow/analysis.yaml` |
| 添加新指标 | `shared/naming_convention.py` → `MetricRegistry._METRICS` |
| 8种数学探针 | `src/astock/business_engines/trend/probes/*.py` |
| T.R.U.T.H.六维基因 | `src/astock/business_engines/truth/core/genes/` |
| 三大求解器 | `src/astock/business_engines/truth/core/solvers/` |
| 数据存储组件 | `shared/contracts/store/` |
| Pipeline上下文 | `pipeline/core/context.py` (PipelineContext, DataStore集成) |
| 引擎专用服务 | `pipeline/engine_services/` (CacheService, EventPublisher) |
| 核心服务 | `pipeline/core/services/` (ConfigService, FlowExecutor, ResultAssembler) |

## Pipeline 服务目录结构 (v3.0)
```
pipeline/
├── engine_services/     # 引擎专用服务 (KedroEngine 使用)
│   ├── cache_service.py    # 缓存管理 (指纹/签名)
│   └── event_publisher.py  # 事件发布 (EventBus封装)
└── core/
    └── services/        # Pipeline 核心服务
        ├── config_service.py      # 配置加载
        ├── flow_executor.py       # 流程执行
        ├── result_assembler.py    # 结果组装
        └── runtime_param_service.py  # 运行时参数
```

## 数据流
```
data/10yd_base/*.csv (原始) → data/polars/*.csv (聚合) → data/filter_middle/*.csv (分析) → data/*_report.md (报告)
```

## 文档索引
- [ORCHESTRATOR_ARCHITECTURE.md](docs/ORCHESTRATOR_ARCHITECTURE.md) - 方法注册与路由
- [PIPELINE_ARCHITECTURE.md](docs/PIPELINE_ARCHITECTURE.md) - YAML工作流引擎
- [TRUTH_SYSTEM_DESIGN.md](docs/TRUTH_SYSTEM_DESIGN.md) - T.R.U.T.H.六维基因
- [CONTRACTS_ARCHITECTURE.md](docs/CONTRACTS_ARCHITECTURE.md) - PGCS数据契约
- [trend/README.md](src/astock/business_engines/trend/README.md) - 8种探针详解
