# AStock Analysis - Copilot 指令

## 架构总览（四层解耦）

```
workflow/*.yaml → pipeline/ → orchestrator/ → src/astock/business_engines/
  (声明式YAML)   (DAG/缓存/PDDA)  (注册/路由)      (纯业务逻辑)
```

**核心数据流**：
1. `trend/engine.py`（8探针趋势分析）产出 `AggregatableResult`
2. PDDA 自动聚合到 `aggregated_trends: Dict[str, pd.DataFrame]`
3. `evaluators`（29规则+5策略）与 `truth`（六维基因+三求解器）**并行消费**
4. `reporters` 生成 Markdown 报告

## 改代码指南

| 改动类型 | 位置 | 参考 |
|---------|------|------|
| 业务算法 | `src/astock/business_engines/{trend,evaluators,truth}/` | 各模块 `engine.py` |
| 新增指标 | `shared/naming_convention.py` → `MetricRegistry` | 搜索 `MetricConfig` |
| 工作流 | `workflow/analysis.yaml` | 复制现有 step |
| 框架层 | `pipeline/` 或 `orchestrator/` | 各自 README.md |

## 方法注册模板

```python
from orchestrator.decorators.register import register_method
from shared.aggregation import AggregatableResult

@register_method(
    component_type="business_engine",  # datahub | data_engine | business_engine
    engine_type="duckdb",              # duckdb | polars | pandas | evaluator | truth | reporting
    engine_name="my_step"              # workflow YAML method: [xxx] 引用
)
def my_step(data, **params) -> AggregatableResult[str, pd.DataFrame]:
    # 业务逻辑...
    return AggregatableResult(key="metric_name", value=result_df, namespace="trends")
```

## PDDA 生产者/消费者模式

```python
# 生产者 (trend/engine.py): 返回 AggregatableResult
def analyze_metric_trend(...) -> AggregatableResult[str, pd.DataFrame]:
    return AggregatableResult(key="roic", value=df, namespace="trends")

# 消费者 (evaluators/engine.py): 签名声明即自动注入
def run_evaluator(aggregated_trends: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    roic_df = aggregated_trends["roic"]  # 直接使用
```

## 命名规范（三层映射）

`metric_name: 'roic'` → `MetricRegistry.get('roic').source_column` → `ColumnBuilder.analysis_column("roic", "slope")` = `roic_slope`

## 常用命令

```bash
python -m pipeline run -c workflow/analysis.yaml                              # 完整运行
python -m pipeline run -c workflow/analysis.yaml --only Analyze_ROIC_Trend    # 单步调试
python -m pipeline validate -c workflow/analysis.yaml                         # YAML校验
python -m pipeline engines                                                    # 列出注册方法
python -m pipeline cache --clear                                              # 清缓存
```

## 硬性约束 ⚠️

1. **`evaluators` ↔ `truth` 禁止互相 import**——并行独立分析路径
2. **`reporters` 只消费上游**——不硬编码业务规则
3. **业务层禁止反向依赖框架**——仅可 import `@register_method` 和 `AggregatableResult`
4. **禁止修改 `RD-Agent/`**——只读参考

## 8 个趋势探针

| 业务键 | source_column | 说明 |
|--------|---------------|------|
| `roic` | roic | 投入资本回报率 |
| `roe` | roe | 股东权益回报率 |
| `roiic` | roiic | 增量资本回报率(派生) |
| `revenue` | total_revenue_ps | 每股营收 |
| `profit` | eps | 每股收益 |
| `gross_margin` | grossprofit_margin | 毛利率 |
| `net_margin` | netprofit_margin | 净利率 |
| `ocf` | ocfps | 每股经营现金流 |

## 关键文件

| 用途 | 路径 |
|------|------|
| 输入 | `data/polars/{10yd,5yd}_final_industry.csv` |
| 中间 | `data/filter_middle/*_trend_analysis.csv` |
| 报告 | `data/{comprehensive,truth}_analysis_report.md` |
| 工作流 | `workflow/analysis.yaml` |

## 环境变量

- `ASTOCK_VALIDATION_MODE=strict|warn|off` — 注册签名校验
- `ASTOCK_DEBUG=1` — 详细日志
