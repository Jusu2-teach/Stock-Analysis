# AStock Analysis - Copilot 指令

## 架构总览（四层解耦）

```
workflow/*.yaml → pipeline/ → orchestrator/ → src/astock/business_engines/
  (声明式)      (DAG/缓存/PDDA)  (注册/路由)      (纯业务逻辑)
```

**核心数据流**：`trend`（8探针趋势分析）→ PDDA 聚合 → `evaluators`（规则评分）与 `truth`（T.R.U.T.H 六因子）**并行** → `reporters` 生成 Markdown 报告。

## 改代码的正确姿势

| 改动类型 | 改哪里 | 参考文件 |
|---------|--------|---------|
| 业务算法/规则 | `src/astock/business_engines/**` | `trend/engine.py`, `evaluators/engine.py` |
| 新增指标 | `shared/naming_convention.py` 的 `MetricRegistry` | 搜索 `MetricConfig` |
| 工作流配置 | `workflow/analysis.yaml` | 复制现有 step 模板 |
| 框架扩展 | `pipeline/` 或 `orchestrator/` | 各自 README.md |

**方法注册模板**（所有业务方法必须用 `@register_method` 装饰器注册）：

```python
from orchestrator.decorators.register import register_method
from shared.aggregation import AggregatableResult

@register_method(
    component_type="business_engine",  # datahub | data_engine | business_engine
    engine_type="duckdb",              # duckdb | polars | pandas
    engine_name="my_step"              # workflow YAML 中 method: [xxx] 引用的名称
)
def my_step(data, **params) -> AggregatableResult[str, "pd.DataFrame"]:
    return AggregatableResult(key="roic", value=df, namespace="trends")
```

## PDDA 聚合系统

- **生产者**：返回 `AggregatableResult(key=..., value=..., namespace=...)`
- **消费者**：函数签名声明 `aggregated_trends: Dict[str, pd.DataFrame]`，自动注入
- **实现**：`shared/aggregation/` (协议) + `pipeline/aggregation/core.py` (运行时)

## 命名规范（三层映射）

YAML `metric_name: 'roic'` → `MetricRegistry.get('roic').source_column` → 输出列 `ColumnBuilder.analysis_column("roic", "slope")` = `roic_slope`

## 常用命令

```bash
python -m pipeline run -c workflow/analysis.yaml              # 执行完整工作流
python -m pipeline run -c workflow/analysis.yaml --only Analyze_ROIC_Trend  # 单步调试
python -m pipeline validate -c workflow/analysis.yaml         # 校验 YAML 语法
python -m pipeline graph -c workflow/analysis.yaml            # 生成 DAG 可视化
python -m pipeline engines                                    # 列出所有已注册方法
python -m pipeline cache --clear                              # 清理缓存
python test_pipeline_complete.py                              # 集成测试 (46 用例)
```

## 环境变量

| 变量 | 值 | 说明 |
|------|---|------|
| `ASTOCK_VALIDATION_MODE` | `strict\|warn\|off` | 注册签名校验级别 |
| `ASTOCK_DEBUG` | `1` | 开启详细调试日志 |

## 关键路径

| 类型 | 路径 |
|------|------|
| 输入数据 | `data/polars/10yd_final_industry.csv`（10年）、`5yd_final_industry.csv`（5年） |
| 中间产物 | `data/filter_middle/*.csv`（8个探针 CSV） |
| 输出报告 | `data/comprehensive_analysis_report.md`、`data/truth_analysis_report.md` |

## 硬性约束 ⚠️

1. **`evaluators` 与 `truth` 禁止互相 import**——并行独立的两条分析路径
2. **`reporters` 只消费上游结果**——不硬编码业务规则
3. **业务层不反向依赖框架层**——`business_engines/` 仅可 import `@register_method` 装饰器
4. **不要修改 `RD-Agent/`**——只读参考副本

## 8 个趋势探针

`roic`, `roe`, `roiic`, `revenue`(→total_revenue_ps), `profit`(→eps), `gross_margin`, `net_margin`, `ocf`(→ocfps)

## YAML Step 模板

```yaml
- name: "Analyze_XXX_Trend"
  component: "business_engine"
  engine: "duckdb"
  method: ["analyze_metric_trend"]
  parameters:
    data: "steps.Load_Financial_Data.outputs.parameters.Raw_Data"
    group_cols: 'ts_code'
    metric_name: 'roic'           # MetricRegistry 业务键
    min_periods: 5
    window_size: 5                # 不配置=使用全量数据
    reference_metrics: ["roe"]    # 交叉验证
  outputs:
    parameters:
      - name: XXX_Trend_Result
```
