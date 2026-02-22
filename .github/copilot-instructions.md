# AStock Analysis - Copilot 指令

> A股基本面量化分析系统：趋势探针 → 规则评估/六维基因 → 报告生成

## 四层架构（严格分层）

```
workflow/*.yaml → pipeline/ → orchestrator/ → src/astock/business_engines/
  (声明式配置)    (DAG执行/PDDA聚合)  (方法注册/路由)     (纯业务逻辑)
```

**数据流**: `trend/`(8探针) → PDDA聚合 → `evaluators/`(29规则) + `truth/`(六维基因) **并行** → `reporters/`

## 常用命令

```bash
python -m pipeline run -c workflow/analysis.yaml                    # 完整运行
python -m pipeline run -c workflow/analysis.yaml --only <StepName>  # 单步调试
python -m pipeline validate -c workflow/analysis.yaml               # YAML校验
python -m pipeline engines                                          # 列出注册方法
python -m pipeline cache --clear                                    # 清缓存
```

## 硬性约束 ⚠️

1. **`evaluators/` ↔ `truth/` 禁止互相 import** — 并行独立分析路径
2. **业务层禁止反向依赖框架** — 仅可 import `@register_method` 和 `AggregatableResult`
3. **`reporters/` 只消费上游** — 不硬编码业务规则
4. **`RD-Agent/` 只读** — 仅作参考

## 新增/修改代码速查

| 改动 | 位置 | 模板 |
|-----|------|------|
| 业务算法 | `src/astock/business_engines/{trend,evaluators,truth}/engine.py` | 搜索 `@register_method` |
| 新指标 | `shared/naming_convention.py` → `MetricRegistry` | 搜索 `MetricConfig` |
| 工作流步骤 | `workflow/analysis.yaml` | 复制现有 step |
| 新探针 | `trend/probes/*_probe.py` | 参考 `log_trend_probe.py` |

## 方法注册（@register_method）

```python
from orchestrator.decorators.register import register_method
from shared.aggregation import AggregatableResult

@register_method(
    component_type="business_engine",  # datahub | data_engine | business_engine
    engine_type="duckdb",              # duckdb | polars | evaluator | truth | reporting
    engine_name="my_step"              # workflow YAML 中 method: [xxx] 引用
)
def my_step(data, **params) -> AggregatableResult[str, pd.DataFrame]:
    return AggregatableResult(key="metric_name", value=result_df, namespace="trends")
```

## PDDA 生产/消费

```python
# 生产者 (trend/engine.py)
def analyze_metric_trend(...) -> AggregatableResult[str, pd.DataFrame]:
    return AggregatableResult(key="roic", value=df, namespace="trends")

# 消费者 (evaluators/engine.py, truth/engine.py) - 签名声明即自动注入
def run_evaluator(aggregated_trends: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    roic_df = aggregated_trends["roic"]
```

## 命名规范（三层映射）

`metric_name: 'roic'` → `MetricRegistry.get('roic').source_column` → `ColumnBuilder.analysis_column("roic", "slope")` → `roic_slope`

## 8 个指标探针

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

## 关键路径

| 用途 | 路径 |
|------|------|
| 输入数据 | `data/polars/{10yd,5yd}_final_industry.csv` |
| 中间结果 | `data/filter_middle/*_trend_analysis.csv` |
| 输出报告 | `data/{comprehensive,truth}_analysis_report.md` |
| 工作流定义 | `workflow/analysis.yaml` |
| 各模块文档 | 各目录 `README.md` |

## 环境变量

- `ASTOCK_VALIDATION_MODE=strict|warn|off` — 注册签名校验
- `ASTOCK_DEBUG=1` — 详细日志
