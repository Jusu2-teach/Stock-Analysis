# AStock Analysis - AI Agent 开发指南

## 架构速览
```text
workflow/*.yaml → pipeline/core/execute_manager.py → orchestrator.AStockOrchestrator → src/astock/business_engines/*
```
- Orchestrator：统一方法注册与路由（`@register_method` + 策略选择），核心在 `orchestrator/orchestrator.py`、`orchestrator/decorators/register.py`、`orchestrator/registry/*`。
- Pipeline：YAML 驱动、依赖图 + Prefect/Kedro 混合执行，入口 `pipeline/cli.py`，核心在 `pipeline/core/execute_manager.py` 与 `pipeline/core/services/*`。
- Business Engines：纯业务逻辑（趋势探针、T.R.U.T.H. 基因、报告），位于 `src/astock/business_engines/*`，按 `analyzers/`、`truth/`、`reporters/` 分层。
- Shared：事件总线、数据契约、命名规范，位于 `shared/*`；通过 EventBus 与 DataStore/ReferenceResolver 串联各层。

## 常见开发任务
- 新增业务方法：在 `src/astock/business_engines/{module}/engine.py`（或具体子模块内）实现函数，并用 `register_method` 注册，例如：
```python
from orchestrator.decorators.register import register_method

@register_method(
    component_type="business_engine",   # 如 duckdb/truth/reporting 等业务引擎
    engine_type="duckdb",
    engine_name="analyze_metric_trend", # 在 workflow 中通过 method 引用
)
def analyze_metric_trend(data, **params): ...
```
- 新增工作流步骤：在 `workflow/*.yaml` 中添加 `steps` 条目，`component/engine/method` 对应 Orchestrator 注册信息，参数通过 `steps.{Step}.outputs.parameters.{Name}` 传递；参考 `workflow/analysis.yaml` 中 `Analyze_ROIC_Trend` 与 `Process_Truth_System` 的写法。
- 指标命名：通过 `shared/naming_convention.py` 中的 `MetricRegistry` 与 `ColumnBuilder` 统一管理（如 `ColumnBuilder.analysis_column("roic", "slope")`）；新增业务指标时优先扩展 `MetricRegistry._METRICS`，保持 `metric_name`（如 `roic/revenue/net_margin`）与底层列映射一致。
- 事件与跨层通信：使用 `shared/event_bus` 中的 `EventBus` 与标准事件（如 `pipeline.node.started`、`pipeline.node.completed`），不要从业务层直接调用 Pipeline/Orchestrator 内部类。
- 数据存储与引用：Pipeline 通过 `shared/contracts/store` 的 `DataStore` 和 `ReferenceResolver` 管理 `steps.X.outputs.parameters.Y` 引用，不要手写字符串解析或自建全局字典；相关设计见 `shared/contracts/README.md` 与 `pipeline/core/context.py`。

## 运行与调试
- 运行主分析工作流：`python -m pipeline run -c workflow/analysis.yaml`（默认顺序执行全部步骤）。
- 断点续跑 / 子集执行：使用 `--resume`、`--only`、`--exclude` 等参数（详见 `pipeline/README.md`），便于只重跑新增/修改节点。
- 调试模式：`ASTOCK_DEBUG=1 python -m pipeline run -c workflow/analysis.yaml`，开启更详细日志与事件输出。
- 查看已注册方法：`python -m pipeline engines`，用于确认 `component_type/engine_type/engine_name` 是否正确暴露给 Orchestrator。
- 可视化依赖图与指标：通过 `python -m pipeline graph ...`、`python -m pipeline metrics ...`，快速理解当前 workflow 的执行结构与性能热点。

## 关键约束与约定
- 分层依赖：`src/astock/*` 不得反向导入 `orchestrator/` 或 `pipeline/`（唯一例外是 `register_method` 装饰器）；`shared/*` 作为基础设施层可被各层引用。
- 数据处理：大数据路径在 `data/10yd_base`、`data/5yd_base`，业务引擎优先使用 DuckDB/Polars 进行批量计算，避免 pandas 一次性加载完整 CSV。
- Orchestrator 校验：通过环境变量 `ASTOCK_VALIDATION_MODE=strict|warn|off`、`ASTOCK_INPUT_STYLE`、`ASTOCK_CONFLICT_MODE` 配置签名与调用校验；默认推荐 `warn` + `strict_single`，以在开发期暴露参数不一致问题。
- Pipeline 数据流：入口通常为 `data/polars/*` 归一化数据，中间结果写入 `data/filter_middle/*_trend_analysis.csv`，最终报告输出到 `data/comprehensive_analysis_report.md` 与 `data/truth_analysis_report.*`。
- 测试与质量：pytest/coverage 已在 `pyproject.toml` 预配置，单元测试应放在根目录 `tests/` 下；`shared/event_bus`、`shared/contracts`、`orchestrator` 等基础模块优先补齐/更新测试。
- 参考文档：深入设计见 `docs/ORCHESTRATOR_ARCHITECTURE.md`、`docs/PIPELINE_ARCHITECTURE.md`、`docs/TRUTH_SYSTEM_DESIGN.md`、`shared/event_bus/README.md`、`src/astock/business_engines/README.md`。

## RD-Agent 子目录说明
- 仓库下的 `RD-Agent/` 为上游开源项目 `microsoft/RD-Agent` 的完整副本，具有独立的 `pyproject.toml`、`requirements` 与 `test/` 结构。
- 如需在 `RD-Agent/` 内改动，请遵循其自带的 README、Makefile 与测试/格式化规范；在实现 AStock 功能时，默认不要大范围重构该子目录，仅在确有需要时局部扩展或调用其能力。
