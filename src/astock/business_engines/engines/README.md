# Business Engine Architecture

AStock's business engine performs the first-pass quality screening for the full market. It turns raw fundamentals into trend-aware scores so downstream valuation work can focus on promising tickers.

---

## Core Principles

- **Trend First** – Build a multi-view trend profile around ROIC, ROIIC, and complementary metrics; rely on data before opinions.
- **Industry-Aware** – All thresholds and penalties are parameterised per industry so capital intensity and cyclicality are respected.
- **Dual Track Decisions** – Combine cumulative penalties with veto rules to catch both chronic underperformance and sudden disasters.
- **Second Chances** – Reward clear acceleration or repair signals so turnaround stories are not filtered out prematurely.

---

## High-Level Flow

```
Raw Financial Data (Kedro / custom pipelines)
        ↓
DuckDB Trend Engine (`duckdb_trend.analyze_metric_trend`)
        ↓
Trend Context Builder (`trend_analyzer.TrendAnalyzer`)
        ↓
Config Resolver (`trend_components.ConfigResolver`)
        ↓
Rule Evaluation (`trend_rules.TrendRuleEvaluator`)
        ↓
Scores, veto reasons, diagnostics → CSV / logs / downstream pipeline
```

---

## Trend Flow

- `pipeline/main.py` 的 `AStockCLI.cmd_run` 读取 `workflow/duckdb_screen.yaml`，应用 `--only/--exclude` 过滤后交给 `ExecuteManager`。
- `ExecuteManager` 通过 `ConfigService` 解析 steps 并交由 Prefect+Kedro 混合执行，参数引用使用 `steps.<name>.outputs.parameters.<field>` 解析。
- Step `Load_Financial_Data` 调用 `business_engine/engines/duckdb.py::load_file` 读取 `data/polars/5yd_final_industry.csv`，产出 `Raw_Data`。
- Step `Analyze_ROIC_Trend` 调用 `engines/duckdb_trend.py::analyze_metric_trend` 生成 `ROIC_Trend_Result`；紧接着 `Score_ROIC_Quality` 用 `engines/scoring.py::score_quality` 评分并写 `roic_quality_report.txt`。
- `data_engine/engines/polars.py::store` 将 ROIC 趋势与评分写入 `data/filter_middle/roic_trend_analysis.csv` 与 `roic_quality_scored.csv`；ROIIC 分析步骤同样复用 `analyze_metric_trend` 并写入 `roiic_trend_analysis.csv`。

---

## Trend Details

- `analyze_metric_trend` 通过 `_init_duckdb_and_source` 把 CSV 映射成 DuckDB 视图，必要时用 `_prepare_derived_metric` 自动派生 ROIIC；继而只保留 `ts_code`, `end_date`, 指标列及 `name/industry` 等辅助列。
- 行业阈值由 `DEFAULT_FILTER_CONFIG` 与 `INDUSTRY_FILTER_CONFIGS`（ROIIC 则使用 ROIIC 对应配置）组合，通过 `ConfigResolver.resolve` 为每个公司应用差异化过滤。
- 对每个 `ts_code` 分组初始化 `TrendAnalyzer`，按 schema 运行多个探针：
        - **加权平均 (`*_weighted`)**：使用 `WEIGHTS`（最近期权重大）计算平滑后的平均水平，用来代表常态 ROIC。
        - **对数趋势 (Log slope / CAGR)**：对 ROIC=20%,18%,15% 等序列取 `ln(1+ROIC/100)` 后做线性回归，得到年化复合增长率；对数化消除了基数大小的偏差，可解释为 CAGR。
        - **线性趋势 (`*_slope`)**：不取对数，直接在原值上做线性回归斜率，作为对数结果的直观对照。
        - **R² (`*_r_squared`)**：来自回归的拟合优度，衡量趋势显著性；接近 1 代表趋势明显，低值说明波动或噪声主导。
        - **滚动趋势 (`*_recent_3y_slope`, `*_trend_acceleration`)**：基于最近 3 年窗口再次拟合，观察短期动量与加速度。
        - **波动与周期 (`*_cv`, `*_is_cyclical`)**：计算变异系数、峰谷比等指标，识别周期性与稳定性，配合行业特征调参。
        - **恶化探针 (`*_deterioration_result`)**：侦测单年断崖、持续下降、负面波动，给出结构化的恶化标签。
- `TrendRuleEvaluator` 根据行业阈值决定是否淘汰、计算罚分与趋势得分；淘汰公司不再写入结果。`TrendResultCollector` 汇总字段（如 `roic_weighted`, `roic_log_slope`, `roic_trend_score`, `roic_penalty`, `roic_penalty_details`, `roic_notes`）。
- 输出 DataFrame 记录趋势统计、罚分统计与行业配置命中次数，供评分与下游分析使用。

---

## Threshold Config

**核心字段含义（与趋势字段对应关系）**
- `min_latest_value` → 对应 `roic_latest` / `roiic_latest`。例如煤炭行业设置为 5%，若趋势结果的 `roic_latest` < 5% 即触发淘汰或罚分。
- `latest_threshold` → 同样对比 `*_latest`。高于该值会在 `penalty_details` 中记录积极说明，低于则标记“最新期弱势”。
- `log_severe_decline_slope` → 对比 `roic_log_slope` / `roiic_log_slope`。值为 -0.30 表示年均跌 30%，若趋势列的 log slope 更陡（例如 -0.4），立即写入 veto 原因并剔除。
- `log_mild_decline_slope` → 也对比 `*_log_slope`。若介于严重和轻度之间，会累加罚分并在 `*_penalty_details` 中注明“轻度衰退”。
- `trend_significance` → 对应 `roic_r_squared` / `roiic_r_squared`。当 R² 低于阈值，`TrendRuleEvaluator` 会降低信任度并抑制正向加分，必要时写入“趋势显著性不足”。
- `penalty_factor` 与 `max_penalty` → 操作 `roic_penalty` / `roiic_penalty` 列，将斜率、跌幅等信号换算成 0-15 分的风险扣分。
- `severe_single_year_decline_pct` → 比较 `roic_latest` 与上一期差值，若跌幅超过阈值会在 `*_penalty_details` 记录“单年巨跌”，并增加 `penalty`。
- `relative_decline_ratio_*` → 对比 `roic_latest` 与 `roic_weighted`（或 ROIIC 同名列）的比值。跌破 70%/60% 触发不同层级的罚分。
- `sustained_decline_threshold` → 再次利用 `*_log_slope`，确认多期持续走弱并写入“持续衰退”标签。
- ROIIC 专用字段：
        - `allow_negative` 与 `negative_tolerance_years` 用于解析 `roiic_history` 序列，如 `roiic_latest < 0` 且超出容忍年限则写入 veto。
        - `roiic_veto_weighted_threshold` / `roiic_veto_latest_threshold` 对应 `roiic_weighted`、`roiic_latest` 列，低于阈值直接淘汰。
        - `roiic_positive_bonus_threshold` 与 `excellent_roiic` 检查 `roiic_trend_score` 与 `roiic_weighted`，满足条件的公司会收到加分并在 `roiic_notes` 中记录“扩张高回报”。

**配置来源**
- `DEFAULT_FILTER_CONFIG`、`DEFAULT_ROIIC_FILTER_CONFIG` 给未明确定义行业的公司使用中性阈值。
- `INDUSTRY_FILTER_CONFIGS`、`ROIIC_INDUSTRY_FILTER_CONFIGS` 针对每个行业覆写上述字段：
        - 成长行业（如软件服务）设置更高的 `min_latest_value` 与更紧的衰退斜率。
        - 周期行业（如煤炭）降低底线但放宽斜率，以区分正常周期波动与系统性崩塌。
        - 重资产行业提高 `negative_tolerance_years`，接受更长的回本周期。

**Runtime Usage**
- `ConfigResolver.resolve` 根据公司所在行业合并默认和特定阈值，产出 `current_config` 并记录使用频次，方便调参复盘。
- `TrendRuleEvaluator.evaluate` 顺序应用阈值：
        - **守门**：检查最新期是否低于 `min_latest_value` 或 ROIIC veto 阈值；若是，直接标记淘汰。
        - **趋势退化**：比较 `log_slope` 与 `log_*_decline_slope`；严重衰退 → veto，轻度衰退 → 累计罚分。
        - **单年异常**：使用 `severe_single_year_decline_pct` / `relative_decline_ratio_*` 侦测断崖式下跌，将原因写入 `penalty_details`。
        - **持续走弱**：当对数斜率低于 `sustained_decline_threshold` 时再加罚分，并保留注释，提醒趋势被拉长的衰退。
        - **ROIIC 评估**：按 `allow_negative`、`negative_tolerance_years` 判断扩张阶段是否可接受负 ROIIC；高于 `roiic_positive_bonus_threshold` 的公司会获得正向加分。
- `TrendEvaluationResult` 会返回 `penalty`、`penalty_details` 与 `passes`。上游趋势 DataFrame 将这些字段写入 `roic_penalty`、`roic_penalty_details` 等列，供评分环节与人工分析引用。

---

## Scoring Stage

- `score_quality` 确保输入包含 `roic_weighted`, `roic_trend_score`, `roic_latest`, `roic_r_squared`；缺失即抛出异常，保证数据契约。
- `calculate_quality_score` 计算四个子分：ROIC 水平 40 分、趋势健康 35 分、最新表现 15 分、稳定性 10 分；趋势得分来源于 `roic_trend_score` 归一化。
- 风险扣分结合趋势罚分阈值（≥15/10/5）以及加权 ROIC、最新 ROIC；趋势得分 <40 追加惩罚，生成 `penalty` 与 `roic_penalty_details`。
- 评分结果输出 `quality_score`, `grade`, `risk_label`, `recommendation`，并按需写入 `roic_quality_scored.csv` 与 `roic_quality_report.txt`。
- `data/filter_middle/roiic_trend_analysis.csv` 暂保留原始 ROIIC 趋势，后续可接入相同评分框架。

---

## Key Modules

| Module | Purpose |
| --- | --- |
| `trend/config/` | Default thresholds, penalty weights, industry presets. |
| `duckdb_trend.py` | Generic trend calculator (weighted averages, log/CAGR slopes, Theil–Sen, deterioration flags, ROIIC derivation). |
| `trend_analyzer.py` | Builds `TrendContext` with all derived indicators for a single group. |
| `trend_components.py` | Resolves industry overrides, composes results, stores usage statistics. |
| `trend_rules.py` | Veto, penalty, and bonus logic; orchestrates the dual-track decision model. |
| `kedro_engine.py` / `prefect_engine.py` | Bind the business engine into Kedro or Prefect orchestration. |

---

## Metrics Layer

### Inputs

`pipeline/main.py` loads cleaned financial statements (currently five-year ROIC series plus invested capital) and supplements them with sector metadata.

### Trend Toolkit

| Metric | Description |
| --- | --- |
| **Weighted Average** | Emphasises recent data for the current level snapshot. |
| **Log / CAGR Slope** | Measures long-term direction with natural base-effect handling. |
| **Robust Slope** | Theil–Sen estimator guards against outliers. |
| **Rolling Slopes** | 3-year slope and acceleration capture near-term momentum. |
| **Recent Deterioration** | Detects sharp year-over-year drops and structural decline. |
| **Volatility & CV** | Flags noisy or cyclical series for rule adjustments. |
| **Cyclical Phase** | Estimates peak/trough states to relax rules in downturns. |
| **Data Quality Flags** | Track loss years, near-zero readings, and cleaning intensity. |
| **ROIIC Derivation** | If `roiic` is absent but `roic` and `invest_capital` exist, the engine creates a temporary DuckDB view estimating NOPAT (`ROIC% × Invested Capital`) and computes ROIIC = ΔNOPAT / ΔInvested Capital. |

All signals are materialised into `TrendContext` for consistent downstream consumption.

---

## Rule Engine

1. **Execution Order** – Rules run in a fixed sequence (veto → penalties → bonuses). Once a veto triggers, evaluation stops.
2. **Structural Decline Veto** – Negative log slope, significant drop from weighted average, and strong statistical fit will exit the name immediately.
3. **Latest Value Penalties** – Compare the most recent reading against industry minima; additive deductions track severity.
4. **Composite Deterioration** – Blends decline speed, single-year crashes, relative drawdowns, and acceleration to capture rolling damage.
5. **Momentum Bonuses** – Accelerating or high CAGR names earn offsets so that growth names can survive mild rule breaches.

Final scores equal `100 - (penalty / max_penalty × 100)` unless vetoed. Detailed reasons and intermediate diagnostics are persisted for auditability.

---

## Workflow Integration

- The DuckDB screening workflow (`workflow/duckdb_screen.yaml`) loads data, analyses ROIC and ROIIC trends, scores ROIC quality, and stores CSV outputs under `data/filter_middle/`.
- ROIIC analysis currently runs with filtering disabled by default, producing a clean trends file ready for future rule integration.
- All steps run through the mixed Prefect+Kedro executor; caching keeps repeated runs efficient while preserving transparency.

---

## Next Enhancements

1. Introduce ROIIC-specific veto/penalty rules once empirical thresholds are validated.
2. Extend rule context with cross-metric comparisons (e.g., ROIC vs ROIIC divergence flags).
3. Surface trend analytics into BI dashboards for faster qualitative review.

- min_latest_value：行业默认 ROIC 下限。
- log_severe_decline_slope / log_mild_decline_slope：对数斜率的严重/轻度衰退阈值。
- 	rend_significance：结构性衰退判定所需最小 R²。
- penalty_factor / max_penalty：扣分力度与淘汰阈值。
- 单年大幅下滑相关阈值与罚分。

### 6.2 行业覆写

INDUSTRY_FILTER_CONFIGS 针对行业特性覆写上述字段，例如：

- **科技成长**（软件服务、半导体等）：提高 min_latest_value、收紧衰退阈值。
- **周期行业**（小金属、钢铁）：放宽最新值要求，但提高波动判定敏感度。
- **重资产**（新型电力、建筑）：降低盈利门槛，同时加强结构性衰退识别。

ConfigResolver.resolve() 会根据公司所属行业合并默认与行业配置，并记录使用频次方便调试。

---

## 7. 输出与调试

### 7.1 输出文件

- data/filter_middle/roic_quality_scored.csv
  - score_trend, score_latest, penalty, 	rend_penalty_details, onus_details。
  - grade,
isk_label,
ecommendation 为后续投资流程提供标签。

### 7.2 日志

- veto 项目以 INFO 级别输出 【一票否决】，便于快速定位淘汰原因。
- 通过的公司在 DEBUG 级别展示扣分与加分明细，建议在调参时开启。

### 7.3 调优建议

1. **抽样复核**：针对高罚分但未淘汰的公司，核查是否符合主观判断。
2. **阈值调整**：修改 `trend/config/filters.py` 或 `trend/config/roiic.py` 中的行业配置，重新运行管线观察保留数变化。
3. **关注成长豁免**：重点审视通过减免留下的公司，判断趋势回升是否可靠。

---

## 8. 运行指南

`ash
# 运行最新筛选流程
python -m pipeline.main run -c workflow/duckdb_screen.yaml
`

执行完成后，查看 data/filter_middle/ 中的结果文件以及日志输出即可了解每家公司的打分与淘汰原因。

---

## 9. 后续演进方向

- **多指标扩展**：在现有结构上引入 ROE、毛利率、自由现金流等指标，实现多维评分。
- **自动调参**：结合历史回测结果自动校准各行业阈值，提高策略收益相关性。
- **风控联动**：与估值模型、财务健康度评估模块串联，形成二次过滤体系。
- **可视化面板**：构建趋势与规则命中情况的仪表板，帮助快速诊断。

---

## 10. 快速复盘 Checklist

- [ ] 指标计算是否成功产出 TrendContext 所需字段？
- [ ] 行业配置是否覆盖全部样本并合理分组？
- [ ] Veto 命中案例是否符合预期？
# AStock Business Engine

AStock 的 business engine 负责把原始财务报表转换成可量化的趋势画像，并为后续的质量评分、组合筛选与估值模型提供结构化输入。该目录包含所有 **趋势分析（Trend）** 相关的核心实现、插件框架以及配置解析逻辑。

---

## 1. 架构概览

```
┌──────────────────────────┐
│  Data Sources (CSV / DB) │
└────────────┬─────────────┘
                                                 │ 1. pipeline.load
┌────────────▼─────────────┐
│  DuckDB Trend Engine      │  ← duckdb_trend.analyze_metric_trend
└────────────┬─────────────┘
                                                 │ 2. metric probes
┌────────────▼─────────────┐
│  TrendAnalyzer            │  ← trend_analyzer.TrendAnalyzer
│    • MetricProbe 执行器   │
│    • Schema build_result  │
└────────────┬─────────────┘
                                                 │ 3. schema flatten
┌────────────▼─────────────┐
│  Trend Schema             │  ← trend_schema.trend_field_schema
└────────────┬─────────────┘
                                                 │ 4. compose context
┌────────────▼─────────────┐
│  Components / Rules       │  ← trend_components / trend_rules
└────────────┬─────────────┘
                                                 │ 5. scoring & store
┌────────────▼─────────────┐
│  Outputs / Pipelines      │  ← CSV, downstream scorers
└──────────────────────────┘
```

关键特性：
- **插件化**：指标计算通过 `MetricProbe` 扩展，易于增删业务逻辑。
- **Schema 驱动**：`trend_field_schema` 统一管控输出列、含义和读取路径。
- **上下文可追踪**：`TrendAnalyzer` 构建完整的 `TrendVector` / `TrendSnapshot`，便于调试与审计。
- **管道整合**：Prefect+Kedro 混合执行，支持缓存与节点级重跑。

> 趋势相关实现已迁移至 `astock.business_engines.trend` 子包，本节所列文件均位于该目录下。

---

## 2. 模块地图

| 模块 | 说明 |
| --- | --- |
| `duckdb_trend.py` | 从 DuckDB 视图读取财报数据，聚合并调用 `TrendAnalyzer`；对 ROIC / ROIIC 等指标进行批量分析。 |
| `trend_analyzer.py` | 执行 MetricProbe 列表、构建趋势上下文、基于 schema 输出标准化行。 |
| `metric_probes.py` | 定义 `MetricProbe` 协议、默认探针集合以及 fatal/soft-fail 处理策略。 |
| `trend_defaults.py` | 提供空结果/回退结构，确保探针失败时输出稳定。 |
| `trend_schema.py` | 描述所有趋势字段的 schema（列名、attr path、说明、单位、分类）。 |
| `trend_components.py` | 处理行业配置、阈值覆写、聚合上下文。 |
| `trend_rules.py` | 评分、罚分、 veto 逻辑（当前主要用于 ROIC 质量评分）。 |
| `trend_models.py` | Pydantic 数据模型，定义内部结构（TrendVector、VolatilityProfile 等）。 |
| `trend/config/` | 默认阈值与行业覆写配置。 |
| `duckdb.py` / `duckdb_utils.py` | 常规 DuckDB 数据引擎工具（行业比较、过滤等）。 |
| `scoring.py` | 质量评分入口，消费趋势分析结果。 |

---

## 3. 执行生命周期

1. **加载数据**：`pipeline/main.py` 调用 `Load_Financial_Data` 节点，读取聚合后的财务 CSV（`data/polars/5yd_final_industry.csv`）。
2. **趋势分析**：`duckdb_trend.analyze_metric_trend` 对每个指标、每个 `ts_code` 分组执行 TrendAnalyzer。
3. **探针分发**：TrendAnalyzer 依次执行 `MetricProbe` 列表（默认包含日志趋势、波动率、拐点、恶化、周期、加速度等）。
4. **结果合并**：探针返回的结构被赋值到 `TrendVector`，随后按照 `trend_field_schema` 展平为 DataFrame 行。
5. **规则/评分**：`scoring.score_quality` 消费趋势结果，应用行业阈值、罚分、豁免等规则，生成质量评分报告。
6. **持久化**：`store_*` 节点通过 `data_engine` (Polars) 输出 CSV，供后续流程或人工分析使用。

---

## 4. Metric Probe 插件层

- **接口**：`MetricProbe` 是一个 `Protocol`，签名 `def run(self, ctx: MetricProbeContext) -> ProbeResult`。
- **上下文**：`MetricProbeContext` 提供原始序列、滚动窗口、行业配置、日志记录器等。
- **错误策略**：
        - 抛出 `FatalMetricProbeError` → 整体指标失败（仅 LogTrend 默认 fatal）。
        - 抛出其他异常 → 记录 warning，回退到 `trend_defaults` 对应的空结果。
- **扩展方法**：
        1. 新建 `class MyProbe(MetricProbe)` 并在 `get_default_metric_probes` 中注册，或在外部构建自定义列表注入 TrendAnalyzer。
        2. 返回的 `ProbeResult` 应包含 Pydantic 模型或 dataclass，确保 schema 可以解析。
        3. 若需要新增输出列，请同步更新 `trend_schema.py`。

默认探针一览：
- `LogTrendProbe`：对数回归，输出斜率、R²、当前水平等。
- `VolatilityProbe`：计算标准差、CV、波动分类等。
- `InflectionProbe`：分段斜率、拐点类型、置信度。
- `DeteriorationProbe`：结构性恶化判断、幅度标签。
- `CyclicalProbe`：判断周期峰谷、波形模式。
- `AccelerationProbe`：近 3 年斜率、加速度、是否加速/减速。

---

## 5. Schema 驱动输出

`trend_schema.trend_field_schema()` 返回 `TrendField` 列表，每个字段包含：
- `column`：输出列名（例如 `roic_recent_slope`）。
- `attr_path`：从 TrendAnalyzer 结果中读取值的路径（使用点号语法）。
- `description`、`unit`、`category`：用于文档与可视化。

`TrendAnalyzer.build_result_row` 遍历 schema，调用 `TrendField.resolve(result)`，自动处理缺失值/回退，并在日志中报告无法解析的路径。

优势：
- 添加新列仅需在 schema 中声明，无需改动核心逻辑。
- 列顺序与命名统一，可持续产出符合 BI/下游规则的表格。
- 结合 `trend_defaults` 确保列完整性，即使探针软失败也有默认值。

---

## 6. 配置与行业覆写

- 全局默认参数集中在 `trend/config/`（如 ROIC 最低阈值、衰退斜率限定等）。
- `trend_components.ConfigResolver` 根据行业标签应用覆写，支持个别指标放宽/收紧标准。
- 若新增行业分类，请更新 `INDUSTRY_FILTER_CONFIGS` 并在测试中验证。

---

## 7. 管道集成

- 入口：`workflow/duckdb_screen.yaml`
        - `Analyze_ROIC_Trend` 与 `Analyze_ROIIC_Trend` 调用同一业务引擎（不同 `metric_name`）。
        - `Score_ROIC_Quality` 用于 ROIC 质量评分，ROIIC 结果暂作为原始趋势输出。
- 执行命令：
        ```pwsh
        .\.venv\Scripts\python.exe -m pipeline.main run -c workflow/duckdb_screen.yaml
        ```
- 输出路径：`data/filter_middle/roic_trend_analysis.csv`、`roiic_trend_analysis.csv`、`roic_quality_scored.csv` 等。

---

## 8. 开发指南

1. **新增指标**：
         - 在 `metric_probes.py` 定义新的 Probe。
         - 添加 `TrendField` 条目描述输出。
         - 在 `duckdb_trend.analyze_metric_trend` 调整 `reference_metrics` 或参数。
2. **调试探针**：使用 `pytest` 或交互式 notebook 调用 `MetricProbe.run`，传入构造好的 `MetricProbeContext`。
3. **日志与监控**：默认 logger 会输出 probe 成功/失败、schema 解析情况，必要时提升到 DEBUG 查看详细数据。
4. **测试**：
         - 单元测试（建议在 `tests/business_engines` 下补充）。
         - 集成验证：运行 `workflow/duckdb_screen.yaml`，核对产出的列和值。

---

## 9. 常见问题

| 问题 | 排查建议 |
| --- | --- |
| CSV 缺列导致探针异常 | 检查 `MetricProbeContext.series` 是否包含所需字段，或在 DuckDB 阶段补齐。 |
| Schema 日志提示 `resolve failed` | 说明 `attr_path` 与结果结构不匹配，确认探针返回模型字段是否一致。 |
| Prefect WARNING: `UnsupportedFieldAttributeWarning` | 来自 Pydantic v2，对业务无影响，可忽略或等待上游修复。 |
| 输出空值过多 | 查看对应探针是否软失败，必要时提升为 fatal 或增加数据清洗。 |

---

## 10. Roadmap

- 扩展 ROIIC 评分与 veto 规则，构建完整双指标决策链。
- 引入多指标（ROE、FCF 等）并共用 MetricProbe 架构。
- 将 schema 元数据导出到文档/BI，形成字段词典。
- 构建 TrendObservation / TrendReport 层，支持自动化叙述与异常检测。

以上内容即时更新于趋势分析重构后（2025-10-16）。如需补充，请在提交前同步修改本 README。
