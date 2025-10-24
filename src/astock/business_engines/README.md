````markdown
# Trend 子系统架构指南

> 目标：帮助新同事快速理解 trend 目录下各模块的职责、调用关系与执行流程，
> 便于扩展新的指标或调试现有规则。

---

## 1. 总览

```
Raw Financial Data
          │  (DuckDB / Pandas / Polars load)
          ▼
Trend Engine Entry (duckdb_trend.analyze_metric_trend)
          │
          ▼
TrendAnalyzer orchestrates probes & rules
          │
          ▼
Trend Results DataFrame → 评分/持久化
```

核心步骤：
- DuckDB 读取原始 CSV/Parquet，并准备 ROIIC 等派生字段；
- TrendAnalyzer 调用 metric probes 生成趋势向量；
- ConfigResolver 注入行业阈值，TrendRuleEngine 做扣分/淘汰；
- Trend Schema/Settings 控制输出字段与序列预处理；
- 评分模块或存储节点消费结构化结果。

---

## 2. 执行链路

1. **数据加载**：`business_engines/engines/duckdb_trend.py` 中的 `analyze_metric_trend` 负责：
    - 通过 `_init_duckdb_and_source` 建立 DuckDB 视图；
    - 根据 `metric_name` 选择/派生列（如 ROIIC 由 ROIC、invest_capital 计算）；
    - 准备 `ts_code`, `end_date`, `industry`, `reference_metrics` 等列。

2. **TrendAnalyzer 初始化**（`trend_analyzer.py`）：
    - 使用 `TrendSeriesConfig` 设定窗口、权重、缺失值处理；
    - 执行默认 probe 列表（来自 `metric_probes.py`）；
    - 生成 `TrendVector`, `TrendSnapshot`，并结合行业配置。

3. **行业阈值解析**（`trend_components.py`）：
    - `ConfigResolver.resolve` 根据 `trend/config` 包中的行业配置，得到当前公司的阈值集合；
    - 记录行业使用次数，供调优统计。

4. **规则执行**（`trend_rules.py`）：
    - `trend_rule_engine` 根据趋势向量、阈值、规则参数计算 `TrendRuleOutcome`；
    - 包含 veto（淘汰）、penalty（扣分）、bonus（加分）、auxiliary_notes（附注）。

5. **结果组装**（`trend_schema.py`）：
    - 使用声明式 `TrendField` 列表将 `TrendSnapshot` 展平为 DataFrame；
    - 输出列与评分模块/存储节点保持契约一致。

6. **评分/存储**：
    - ROIC 结果进入 `business_engines/engines/scoring.py::score_quality`；
    - ROIIC 结果写入 CSV，备用于 ROIIC 评分扩展。

---

## 3. 模块职责地图

| 模块 | 主要职责 | 典型调用方 |
| --- | --- | --- |
| `duckdb_trend.py` | 数据加载、派生、调用 TrendAnalyzer | Pipeline Step `Analyze_*_Trend` |
| `trend_analyzer.py` | 调度 probes、构建 TrendSnapshot/TrendVector | `duckdb_trend.analyze_metric_trend` |
| `metric_probes.py` | 实现 Log 斜率、线性斜率、波动率、周期、恶化、滚动趋势等探针 | TrendAnalyzer |
| `trend_analysis.py` | 提供底层统计例程（加权平均、对数回归、Theil-Sen 等） | Metric Probes |
| `trend_components.py` | 行业阈值解析、使用统计、规则入口 | TrendAnalyzer |
| `trend_rules.py` | 执行 veto/扣分/加分规则，输出处罚明细 | Trend Components |
| `trend_models.py` | 数据类定义（TrendVector, TrendSnapshot, Result 等） | 各模块共享 |
| `trend_defaults.py` | 提供探针失败时的默认结果 | TrendAnalyzer / Metric Probes |
| `trend_schema.py` | 定义输出字段、映射路径、说明 | TrendAnalyzer → DataFrame |
| `trend_settings.py` | 配置 TrendAnalyzer 行为、序列预处理、输出字段集 | 上层配置 |

### 模块协作细节
- `trend_models` 提供所有探针与规则共享的数据结构：`TrendVector` 记录指标向量，`TrendSnapshot` 作为 schema 展平的输入，`TrendRuleOutcome`/`TrendEvaluationResult` 携带罚分结果。
- `metric_probes` 在计算失败时回退至 `trend_defaults` 的占位对象，保证 `TrendAnalyzer` 始终能得到合法的 `TrendVector`。
- `trend_analyzer` 组装 `MetricProbe` 产出的结果和 `trend_components.ConfigResolver` 提供的阈值，随后调用 `trend_rules.trend_rule_engine` 得到最终的 `TrendEvaluationResult`。
- `trend_schema` 使用 `TrendSnapshot` 与规则评估结果中的字段名称将信息映射到 DataFrame 列，既包含 `trend_models` 的核心指标，也包含 `trend_rules` 输出的 `penalty_details`、`auxiliary_notes`。
- `trend_settings.TrendAnalyzerConfig` 作为胶水：它定义序列窗口、输出字段以及可选自定义 probe 列表，驱动 `trend_analyzer` 如何组合 models/defaults/probes/rules/schema。

---

## 4. 调用流程（序列图）

```
duckdb_trend.analyze_metric_trend
  ├─ ConfigResolver.resolve → 行业阈值
  ├─ TrendAnalyzer(...)
  │    ├─ metric_probes (log trend / volatility / cyclical / deterioration / rolling)
  │    │      └─ trend_analysis.* 工具函数
  │    ├─ TrendRuleEvaluator (内部调用 trend_rule_engine)
  │    └─ TrendResultCollector + trend_schema → DataFrame 行
  └─ 返回趋势结果 DF
```

---

## 5. 快速上手

1. **运行现有工作流**
    ```pwsh
    .\.venv\Scripts\python.exe -m pipeline.main run -c workflow/duckdb_screen.yaml
    ```
    输出位于 `data/filter_middle/`。

2. **新增指标 / 参考指标**
    - 在 `metric_probes.py` 添加新探针（如现金流趋势），或扩展 `trend_analysis.py` 的工具函数；
    - 更新 `trend_schema.py` 追加列定义；
    - 在 `duckdb_trend.analyze_metric_trend` 传入 `reference_metrics`。

3. **调节阈值/规则**
    - 修改 `trend/config/filters.py` 或 `trend/config/roiic.py` 中对应行业的阈值字段；
    - 调整 `trend_rules.py` 中的规则参数或新增规则类。

4. **调试建议**
    - 设置 `ASTOCK_DEBUG=1` 观察日志；
    - 通过 `TrendAnalyzer` 返回的 `penalty_details`、`trend_notes` 快速定位淘汰原因；
    - 利用 `trend_models.TrendWarning` 捕获数据质量问题。

---

## 6. 开发规范

- 每个新探针需在失败时返回合理的默认值，避免中断流水线；
- 新增列时必须更新 `trend_schema.py` 与文档，并考虑评分模块兼容性；
- 行业参数调整后建议重新跑 Workflow，对比 `ConfigResolver` 的使用统计；
- 保持模块 docstring（中文）描述清晰，便于团队成员快速理解。

---

## 7. 后续计划

- 引入多指标联动规则（ROIC 与 ROIIC 差异分析）；
- 将 Trend 输出与评分结果推送到可视化面板；
- 结合回测数据自动回调行业阈值；
- 支持更长时间窗口（7 年 / 10 年）并按需调整权重。

````
# Business Engines Overview# Business Engines - 趋势分析引擎






## 1. Why This Layer Exists## 📑 目录



The `business_engines` package sits between raw data pipelines and presentation. Its job is to:1. [模块架构](#模块架构)

2. [核心模块详解](#核心模块详解)

- gather clean metric series (ROIC, ROIIC, ROE, …)3. [趋势分析方法](#趋势分析方法)

- run reusable analysis workflows (trend checks, quality scoring, report synthesis)4. [决策引擎架构](#决策引擎架构)

- expose consistent outputs back to pipelines, notebooks, or dashboards5. [配置体系](#配置体系)

6. [使用示例](#使用示例)

Think of it as **workflow glue**: data enters, engines evaluate, structured results come out.7. [开发指南](#开发指南)



------



## 2. Directory Cheat Sheet## 🏗️ 模块架构



### 目录结构

```
business_engines/
├── README.md
├── engines/                    # 执行型引擎（DuckDB workflow 等）
│   ├── duckdb_trend.py         # 趋势分析主入口
│   ├── duckdb_utils.py         # DuckDB SQL 辅助方法
│   ├── scoring.py              # 评分/兜底 hook
│   └── README.md
├── trend/                      # 通用趋势分析工具箱
│   ├── config/                 # 行业阈值配置子包
│   │   ├── filters.py          # ROIC 行业阈值
│   │   ├── roiic.py            # ROIIC 行业阈值
│   │   └── characteristics.py  # 周期与恶化阈值
│   ├── metric_probes.py        # 信号探针定义
│   ├── trend_analysis.py       # 底层统计例程
│   ├── trend_analyzer.py       # Probe 调度与结果汇总
│   ├── trend_components.py     # ConfigResolver / 规则执行
│   ├── trend_rules.py          # Veto/Penalty/Bonus 规则
│   └── trend_settings.py       # TrendAnalyzerConfig 等配置对象
├── scoring/                    # 其它业务评分模块
└── reporting/                  # 报告生成工具
```

### 系统架构图

If you only care about **running** trend analysis, focus on `engines/` and skim the `trend/` package when you need to adjust behaviour.

```

---┌─────────────────────────────────────────────────────────────┐

│                  AStock 趋势分析系统 v2.3                      │

## 3. Big-Picture Workflow└─────────────────────────────────────────────────────────────┘

                              │

Below is the happy path for the default DuckDB trend engine:                ┌─────────────┴─────────────┐

                │                           │

1. **Entry call** – `analyze_metric_trend()` (in `engines/duckdb_trend.py`) receives:         ┌──────▼──────┐           ┌───────▼────────┐

   - a DuckDB source or Pandas DataFrame         │  数据层 (DuckDB) │           │  配置层 (config)  │

   - group columns (e.g. `ts_code`)         │  - 10年财务数据  │           │  - 25行业配置    │

   - target metric name (`"roic"`, `"roiic"`, …)         │  - ROIC/ROE等   │           │  - 差异化阈值    │

   - optional `TrendAnalyzerConfig` for window size, weights, probes, schema         └──────┬──────┘           └───────┬────────┘

2. **Data preparation** – helper SQL checks for derived columns (ROIIC) and pulls the necessary fields ordered by date.                │                           │

3. **Config resolution** – `ConfigResolver` merges base defaults with industry-specific overrides from `trend/config`.         ┌──────▼──────────────────────────▼────────┐

4. **Per-group analysis** – for each company code:         │         趋势分析核心引擎                    │

   - instantiate `TrendAnalyzer`         │        (duckdb_trend.py)                  │

   - sanitise the metric series using rules defined in `TrendSeriesConfig`         │  ┌─────────────────────────────────┐    │

   - execute probe plug-ins (`metric_probes.py`) to produce dataclass results         │  │ 对数线性回归 (Log-Linear Regression)│   │

   - compute reference metrics (ROIC vs ROIIC) if requested         │  │  y = β₀ + β₁·x                  │    │

5. **Rule evaluation** – `TrendRuleEvaluator` feeds a `TrendVector` into `trend_rules.py` to apply veto, penalty, and bonus logic.         │  │  log(ROIC) ~ year               │    │

6. **Snapshot & output** – `TrendAnalyzer.build_snapshot()` bundles vector + evaluation, and `build_result_row()` flattens everything using the schema from `trend_settings.py`. Rows are collected into a Pandas DataFrame.         │  └─────────────────────────────────┘    │

7. **Pipeline continuation** – calling code persists the DataFrame, triggers downstream scoring, or launches the reporting module.         └──────┬──────────────────────────────────┘

                │

This flow is designed so that you can swap engines (e.g. move to Prefect, Prefect+Kedro hybrid, or future Spark implementations) without touching the probe logic.    ┌───────────┴───────────┐

    │                       │

---┌───▼────────┐      ┌───────▼────────────┐

│  信号检测   │      │   统一决策引擎      │

## 4. Key Concepts (No Jargon Version)│ (trend_    │      │  (Unified Decision │

│ analysis.py)│      │      Engine)       │

| Concept | Plain-English Description | Where to look |└───┬────────┘      └───────┬────────────┘

|--------|---------------------------|---------------|    │                       │

| Metric probe | Small plugin that inspects a number series (trend, volatility, cyclical pattern…). | `trend/metric_probes.py` |    ├─ P0: 基础趋势         │

| Trend series config | Rules for window length, fill strategy, and weighting. | `trend/trend_settings.py` |    ├─ P1: 拐点+恶化        ├─ 优先级1: 一票否决

| Trend analyzer | Glues together probes, computes references, packages results. | `trend/trend_analyzer.py` |    └─ P2: 周期+加速        ├─ 优先级2: 累积罚分

| Trend vector | Lightweight snapshot of the most important metrics; input for rules. | `trend/trend_models.py` |                            └─ 优先级3: 阈值判断

| Rule engine | Applies veto and penalty formulas so the output is “pass/fail + score”. | `trend/trend_rules.py` |                                    │

| Collector | Rows → DataFrame helper used by pipelines. | `trend/trend_components.py` |                            ┌───────▼────────┐

                            │  筛选结果输出   │

---                            │ + 详细评分明细  │

                            └────────────────┘

## 5. Running the Engine Yourself```



### Quick CLI style test (hybrid pipeline)---



```powershell## 🔧 核心模块详解

$env:PYTHONPATH = "src"

.\.venv\Scripts\python.exe -m pipeline.main run -c workflow/duckdb_screen.yaml### 1. config 包 - 配置管理中心 ⭐⭐⭐⭐⭐

Remove-Item Env:PYTHONPATH

```**职责**: 管理25个行业的差异化配置参数



This launches the Prefect+Kedro workflow that already wires in the trend engine. Logs will show each node, the penalty decisions, and summary stats.**核心功能**:



### Minimal Python snippet```python

from astock.business_engines.trend.config import (

```python    get_filter_config,      # 获取行业配置

from pathlib import Path    get_industry_category,  # 获取行业分类

import pandas as pd    INDUSTRY_FILTER_CONFIGS # 所有行业配置

from astock.business_engines.trend import TrendAnalyzer, TrendAnalyzerConfig, TrendSeriesConfig)



# Load any 5-year metric history for a single company# 示例：获取医药生物行业配置

df = pd.read_csv("data/polars/20151231_fina_indicator_clean.csv")config = get_filter_config('医药生物')

config = TrendAnalyzerConfig(series=TrendSeriesConfig(weights=[1, 1, 1, 1, 2]))print(config)

# {

analyzer = TrendAnalyzer(#     'log_severe_decline_slope': -0.25,

    group_key="000001.SZ",#     'log_mild_decline_slope': -0.15,

    group_df=df[df["ts_code"] == "000001.SZ"],#     'r_squared_threshold': 0.65,

    metric_name="roic",#     'max_penalty_threshold': 15,

    group_column="ts_code",#     ...

    prefix="",# }

    suffix="_trend",```

    keep_cols=["name", "industry"],

    config=config,**25个行业分类**:

)

| 大类 | 子行业 (数量) | 特性 |

snapshot = analyzer.build_snapshot(|------|--------------|------|

    evaluation=None,| 🚀 **科技成长** | 软件服务、半导体、元器件、电气设备、IT设备 (5) | 高增长，严格标准 |

    vector=analyzer.build_trend_vector(),| 🏥 **稳定消费** | 生物制药、化学制药、医疗保健、中药、食品饮料、白酒 (6) | 稳定性高，严格标准 |

)| 🏭 **制造工业** | 汽车零部件、汽车整车、机械设备、专用设备、专用机械 (5) | 中等波动，中等标准 |

print(snapshot.vector.log_slope, snapshot.trend.quality.effective)| 🔄 **周期性** | 小金属、钢铁、有色金属、化工、煤炭 (5) | 高波动，宽松标准 |

```| 🏗️ **重资产** | 房地产、建筑装饰、建筑材料、新型电力 (4) | 低波动，宽松标准 |



> In production we always pass a `TrendEvaluationResult` from the rule engine into `build_snapshot()`. Passing `None` above is fine for quick inspection or notebooks.**版本演进**:

- v1.0: 线性斜率参数 (severe_decline_slope, mild_decline_slope)

---- **v2.0**: 对数斜率参数 (log_severe_decline_slope, log_mild_decline_slope)

- **v2.1**: 统一罚分阈值 (max_penalty_threshold ≈ max_penalty = 15)

## 6. Customising Behaviour- **v2.3**: 趋势分析函数全面返回 dataclass，携带数据质量、异常值与风险提示元数据



1. **Weighting / window length** – tweak `TrendSeriesConfig` (allow longer windows, choose `ffill`, set a constant fill value, etc.).---

2. **Output columns** – supply custom `TrendField` definitions through `TrendAnalyzerConfig.output_fields` if you only need a subset.

3. **Probe set** – hand in your own probe list; any object that follows the `MetricProbe` protocol can participate.### 2. duckdb_trend.py - 趋势分析主引擎 ⭐⭐⭐⭐⭐

4. **Rules** – extend `trend_rules.py` or instantiate a different `TrendRuleEngine` to change penalties and vetoes.

**职责**: 执行对数回归分析 + 统一决策引擎

All these knobs live in the trend toolkit so pipelines stay untouched.

**核心类**: `DuckDBTrendAnalyzer`

---

#### 主要方法

## 7. Learning Path for New Contributors

```python

1. **Run the default workflow** – execute the DuckDB pipeline and read the logs.class DuckDBTrendAnalyzer:

2. **Inspect one company** – print `analyzer.trend_result`, `volatility_result`, etc., to see what each probe exposes.    def __init__(self, db_path: str, config: dict):

3. **Review `trend_settings.py`** – understand configurable knobs (windowing, weights, schema overrides).        """初始化分析器"""

4. **Study `trend_rules.py`** – follow how a `TrendVector` becomes penalties, vetoes, and notes.

5. **Look at orchestration** – once the above makes sense, dive into Prefect/Kedro integration for full context.    def run_full_analysis(self, table_name: str, metric: str) -> pd.DataFrame:

        """

With this overview even a new teammate (“小白”) can reason about the business engines architecture, extend probes or rules, and plug new workflows into the existing pipeline.        运行完整的趋势分析


        Parameters:
        -----------
        table_name : str
            DuckDB表名
        metric : str
            分析指标（如'roic', 'roe'）

        Returns:
        --------
        pd.DataFrame
            筛选后的结果，包含所有分析字段
        """
```

#### 分析流程

```python
def run_full_analysis(self):
    # 1. 数据准备
    df_full = self._load_and_prepare_data()

    # 2. 初始化行业差异化参数与评估器
    config_resolver = ConfigResolver(INDUSTRY_FILTER_CONFIGS)
    rule_evaluator = TrendRuleEvaluator(logger)
    collector = TrendResultCollector()

    # 3. 逐公司构建 TrendAnalyzer（封装全部指标 dataclass 计算）
    for group_key, group_df in df_full.groupby(self.group_col):
        analyzer = TrendAnalyzer(...)
        if not analyzer.valid:
            continue

        # 4. 获取 v2.3 统一上下文，驱动规则评估/扣分
    trend_vector = analyzer.build_trend_vector()
        evaluation = rule_evaluator.evaluate(group_key, metric, config, trend_context)

        # 5. dataclass → 行级载荷，含质量/警示元数据
        result_row = analyzer.build_result_row(evaluation, enable_penalty)
        collector.add(result_row)

    # 6. 输出结构化 DataFrame
    return collector.to_dataframe()
```

#### v2.1 核心改进：统一决策引擎

```python
def _unified_decision_engine(self, signals, config):
    """
    v2.1 统一决策引擎

    三层优先级架构：
    1. 优先级1: 一票否决（严重恶化直接淘汰）
    2. 优先级2: 累积罚分（所有规则统一计分）
    3. 优先级3: 阈值判断（罚分>=15淘汰）
    """

    # ========== 阶段1: 信号收集 ==========
    is_mild_decline = signals['p0']['is_mild_decline']
    p1_deterioration = signals['p1']['deterioration_severity']
    p1_inflection = signals['p1']['inflection_type']
    p2_cyclical = signals['p2']['cyclical_phase']
    p2_acceleration = signals['p2']['acceleration_signal']

    # 计算关键比率
    latest_vs_weighted = latest_value / weighted_avg
    is_sustained_decline = (log_slope < -0.15 and latest_value < weighted_avg)

    # ========== 阶段2: 统一决策引擎 ==========

    # 【优先级1】一票否决规则
    if p1_deterioration == 'severe':
        total_decline_pct = (latest - earliest) / earliest * 100

        # Rule 8a: 总跌幅 > 40%
        if total_decline_pct < -40:
            return {
                'passed': False,
                'reason': '一票否决-跌幅过大',
                'veto': True
            }

        # Rule 8b: 最新值 < 70% 加权平均
        if latest_vs_weighted < 0.70:
            return {
                'passed': False,
                'reason': '一票否决-当前值过低',
                'veto': True
            }

    # 【优先级2】累积罚分系统
    penalty = 0
    penalty_details = []
    bonus_details = []

    # Rule 3: 轻度衰退 (P0)
    if is_mild_decline:
        penalty += 5
        penalty_details.append("轻度衰退-5分")

    # Rule 4: 近期恶化 (P1)
    if p1_deterioration == 'severe':
        penalty += 15
        penalty_details.append("严重恶化-15分")
    elif p1_deterioration == 'moderate':
        penalty += 10
        penalty_details.append("中度恶化-10分")

    # Rule 9: 持续衰退重罚 (v2.1新增)
    if is_sustained_decline:
        penalty += 10
        penalty_details.append("持续衰退重罚-10分")

    # Rule 5: 拐点分析 (P1)
    if p1_inflection == 'growth_to_decline':
        penalty += 15
        penalty_details.append("增长转衰退-15分")
    elif p1_inflection == 'decline_to_improvement':
        penalty -= 5
        bonus_details.append("拐点改善+5分")

    # Rule 6: 周期性分析 (P2)
    if p2_cyclical == 'trough':
        penalty -= 5
        bonus_details.append("周期谷底+5分")
    elif p2_cyclical == 'peak':
        penalty += 3
        penalty_details.append("周期高点-3分")

    # Rule 7: 加速度分析 (P2)
    if p2_acceleration == 'accelerating_decline':
        penalty += 5
        penalty_details.append("下降加速-5分")
    elif p2_acceleration == 'accelerating_improvement':
        penalty -= 5
        bonus_details.append("改善加速+5分")

    # 【优先级3】阈值判断
    if penalty >= config['max_penalty_threshold']:
        return {
            'passed': False,
            'reason': '累积淘汰',
            'penalty': penalty,
            'penalty_details': penalty_details,
            'bonus_details': bonus_details,
            'veto': False
        }

    # 通过筛选
    return {
        'passed': True,
        'penalty': penalty,
        'penalty_details': penalty_details,
        'bonus_details': bonus_details
    }
```

#### v2.3 核心改进：TrendAnalyzer 数据模型

- 趋势计算函数 `calculate_log_trend_slope`、`detect_recent_deterioration` 等全面返回 dataclass，`TrendAnalyzer` 将其缓存为属性，避免重复计算并附带 `TrendWarning` 列表。
- `TrendAnalyzer.build_trend_vector()` 输出标准化特征向量（`log_slope`、`cagr_approx`、`current_phase` 等），配合 `build_snapshot()` 生成统一格式的结果对象，`TrendRuleEvaluator` 可一次性读取完成评估与扣分。
- `TrendAnalyzer.build_result_row()` 将 dataclass 元数据扁平化写入结果行，统一包含数据质量、阈值、滚动趋势等字段，最终由 `TrendResultCollector.to_dataframe()` 汇总为结构化 DataFrame。
- `DataQualitySummary` 与 `SerializableResult.to_dict()` 为报表/持久化提供属性访问与字典导出的双重入口。

```python
analyzer = TrendAnalyzer(group_key='000001.SZ', group_df=df, metric_name='roic', ...)
trend = analyzer.trend_result

print(trend.log_slope, trend.quality.effective)
print(analyzer.volatility_result.cv)

result_row = analyzer.build_result_row(evaluation, include_penalty=True)
# 交由 TrendResultCollector.add(result_row) → to_dataframe()
```

**日志输出示例**:

```
✅ 【通过】永兴材料: 罚分8.5
   扣分项: 中度恶化-10分; 轻度衰退-5分
   加分项: 周期谷底+5分; 拐点改善+5分

❌ 【累积淘汰】海康威视: 总罚分20.77
   扣分项: 中度恶化-10分; 持续衰退重罚-10分; 轻度衰退-5分
   加分项: 无

❌ 【一票否决】长春高新: 严重恶化一票否决-跌幅51.9%>40%
```

---

### 3. trend_analysis.py - P0/P1/P2信号检测 ⭐⭐⭐⭐

**职责**: 检测三层趋势信号（P0基础趋势、P1拐点恶化、P2周期加速）

#### P0层：基础趋势分析

`calculate_log_trend_slope(values, check_outliers=True)` 返回 `LogTrendResult`，集中包含对数斜率、线性对照、数据质量与异常值信息：

```python
import math

from astock.business_engines.trend.trend_analysis import calculate_log_trend_slope
from astock.business_engines.trend.trend_models import LogTrendResult

result: LogTrendResult = calculate_log_trend_slope(values)

if result.r_squared > 0.6 and result.log_slope > 0:
    print("长期趋势向上且拟合度良好")

print("原始数据质量:", result.quality.original)
print("有效数据质量:", result.quality.effective)
if math.isfinite(result.robust_slope):
    print(f"鲁棒斜率(Theil-Sen): {result.robust_slope:.4f}")
else:
    print("鲁棒斜率(Theil-Sen): nan")
print("鲁棒斜率95%区间:", result.robust_slope_ci_low, "→", result.robust_slope_ci_high)

if result.outliers and result.outliers.has_outliers:
    print("异常值处理:", result.outliers.risk_level)

if result.warnings:
    for warning in result.warnings:
        print("warning ->", warning.code, warning.message)
```

当 Theil–Sen 鲁棒斜率与对数 OLS 斜率差距显著且拟合度不足 (`R² < 0.7`) 时，会自动产生 `ROBUST_SLOPE_DISCREPANCY` 提示，帮助发现被极端年份扭曲的趋势；若鲁棒斜率计算失败，同样会附带 `ROBUST_SLOPE_FALLBACK` 供排查数据质量。

**对数回归原理**:

```
线性空间:  ROIC = 30% → 25% → 20% → 15% → 10%
           变化: -5个百分点/年（看起来均匀）

对数空间:  log(ROIC) = -1.20 → -1.39 → -1.61 → -1.90 → -2.30
           斜率: -0.275/年（反映真实下降速度）

为什么用对数？
- 相同比例变化具有相同斜率
- 避免异常值过度影响
- 符合复利增长的金融逻辑
```

#### P1层：拐点与恶化检测

- `detect_inflection_point(values)` 返回 `InflectionResult`，对 3 期滑窗斜率进行分段回归，提供 `has_inflection`, `inflection_type`, `slope_change`, `confidence`，并将早期/最近窗口的 R² 暴露为审计依据。
- `detect_recent_deterioration(values, industry)` 返回 `RecentDeteriorationResult`，结合行业差异化阈值计算连跌幅度、严重程度及是否属于高位稳定，所有判定阈值透出到 `decline_threshold_pct/abs` 字段。

```python
from astock.business_engines.trend.trend_analysis import (
    detect_inflection_point,
    detect_recent_deterioration,
)

inflection = detect_inflection_point(values)
if inflection.has_inflection:
    print("拐点:", inflection.inflection_type, "Δslope=", inflection.slope_change)

deterioration = detect_recent_deterioration(values, industry="新能源车")
print("severity:", deterioration.severity, "total decline %:", deterioration.total_decline_pct)
```

#### P2层：周期性与加速度检测

- `detect_cyclical_pattern(values, industry)` 输出 `CyclicalPatternResult`，综合峰谷比、趋势 R²、波动模式和行业因子计算周期置信度，其 `confidence_factors` 为最终报告提供透明解释。
- `calculate_rolling_trend(values)` 输出 `RollingTrendResult`，对比 3 年与 5 年斜率，给出 `trend_acceleration`、加速/减速标记以及 3 年、5 年各自的 R²。

```python
from astock.business_engines.trend.trend_analysis import (
    detect_cyclical_pattern,
    calculate_rolling_trend,
)

cyclical = detect_cyclical_pattern(values, industry="化工")
print("cyclical?", cyclical.is_cyclical, "confidence=", cyclical.cyclical_confidence)
print("phase:", cyclical.current_phase)

rolling = calculate_rolling_trend(values)
print("trend acceleration:", rolling.trend_acceleration)
print("is accelerating?", rolling.is_accelerating)
```

---

### 4. reporting/trend_report_generator.py - 报告生成器

**职责**: 生成8节详细分析报告

```python
from astock.business_engines.reporting import TrendReportGenerator

# 生成报告
generator = TrendReportGenerator(
    input_file='data/filter_middle/roic_trend_analysis.csv'
)
report = generator.generate_full_report()

# 保存报告
with open('data/trend_analysis_report.md', 'w', encoding='utf-8') as f:
    f.write(report)
```

**报告结构**:

1. **Executive Summary** - 高管摘要
2. **P0 Analysis** - 基础趋势分析（10年长期）
3. **P1 Analysis** - 拐点与恶化分析（3-5年短期）
4. **P2 Analysis** - 周期与加速度分析（3年滚动）
5. **Industry Distribution** - 行业分布
6. **Investment Opportunities** - 投资机会
7. **Risk Warnings** - 风险警告
8. **Appendix** - 附录（完整公司列表）

---

## 📐 趋势分析方法

### v2.3 统一结果模型 (Dataclass Protocol)

自 v2.3 起，`trend_analysis.py` 中的全部分析函数均返回强类型的 dataclass，统一了承载字段、元数据与风险提示：

| Dataclass | 主要字段 | 用途 |
|-----------|---------|------|
| `LogTrendResult` | `log_slope`, `robust_slope`, `r_squared`, `cagr_approx`, `robust_slope_ci_low/high`, `quality: DataQualitySummary`, `outliers`, `warnings` | P0 对数趋势核心输出，包含OLS/鲁棒双斜率、原始/清洗数据质量与异常值标记 |
| `VolatilityResult` | `cv`, `std_dev`, `range_ratio`, `volatility_type`, `warnings` | 波动率与均值接近零检测 |
| `InflectionResult` | `has_inflection`, `inflection_type`, `slope_change`, `confidence`, `early_r_squared`, `recent_r_squared` | 拐点信号与置信度 |
| `RecentDeteriorationResult` | `has_deterioration`, `severity`, `year4_to_5_pct`, `total_decline_pct`, `decline_threshold_pct/abs`, `industry` | 近期恶化判定与行业化阈值 |
| `CyclicalPatternResult` | `is_cyclical`, `peak_to_trough_ratio`, `current_phase`, `cyclical_confidence`, `confidence_factors`, `warnings` | 周期性诊断与解释因子 |
| `RollingTrendResult` | `recent_3y_slope`, `full_5y_slope`, `trend_acceleration`, `is_accelerating`, `warnings` | 3 年滚动趋势与加速度 |

所有结果类继承了 `SerializableResult` 混入，提供 `to_dict()` 便捷地导出到 DataFrame/JSON；风险提示以 `TrendWarning` 列表返回，利于日志与可视化展示。

### 分析方法速览

- `calculate_log_trend_slope`：以对数线性回归衡量 5 年窗口的复合增速，配合 Theil–Sen 鲁棒斜率与数据质量检查，目的在于识别长期稳健增长或隐蔽衰退。
    - 判定标准：默认窗口为 5 期，先对序列做 `asinh` 变换并回归；`log_slope <-0.30` 视为严重衰退、`log_slope <-0.15` 视为轻度衰退（行业可覆写）；若拟合度 `R² < 0.2` 或数据质量被评为 `poor` 会附带警告；同时输出 Theil–Sen 鲁棒斜率用于对照。
    - 示例：`log_result.log_slope ≈ 0.12` 代表年化约 12% 的趋势性增长；`log_result.slope = -4.8` 说明按原始数值拟合，每年大约下降 4.8 个百分点，可与对数斜率交叉验证。

- `calculate_volatility_metrics`：统计标准差、变异系数与极差比，判定序列的波动等级，帮助区分“低 R² 因稳定”与“低 R² 因剧烈波动”。
    - 判定标准：`cv`（标准差/均值）是核心指标，默认阈值为 0.12（超稳定）、0.20（稳定）、0.35（中等）、0.55（波动）、>0.55（高波动）；若均值接近零则直接归类为 `extreme_volatility` 并触发警告。
    - 示例：`vol_result.cv = 0.18` ⇒ 波动型为 `stable`，意味着数据围绕均值偏差较小。

- `detect_inflection_point`：对比前后分段斜率判断趋势是否发生反转，用于早期捕捉“恶化→好转”或“好转→恶化”信号。
    - 判定标准：采用 3 期滑窗线性回归，比较早期与最近斜率；若斜率从 ≥+1 下降至 ≤-1 且变化幅度 ≥2 认定为 `growth_to_decline`，反向为 `deterioration_to_recovery`；额外检测斜率差分的符号翻转以捕捉“峰-谷-峰”模式。
    - 示例：`inflection.has_inflection=True` 且 `inflection.inflection_type='growth_to_decline'` 表示趋势已由上升转为下滑。

- `detect_recent_deterioration`：关注最近两年的连续下滑程度，结合行业阈值评定恶化级别，提醒高位回落或结构性风险。
    - 判定标准：要求第 3→4 年与 4→5 年两个区间均跌过行业阈值（默认 -5% 或 -2pct，行业可覆写）；若累计跌幅 >5%/15%/30% 分别记为轻度/中度/严重恶化；若最新值仍高于高位阈值且总体波动 <5% 则判定为高位稳定不触发。
    - 示例：`deterioration.severity='moderate'` 说明近两年跌幅介于 15%-30%，需要关注潜在风险。

- `detect_cyclical_pattern`：融合行业标签、峰谷比、波动模式与 CV 等指标，判定企业是否处于周期性波段，并给出当前所处阶段。
    - 判定标准：先判断是否属于预置周期行业，再校验峰谷比是否高于行业阈值（默认≥3）、趋势 `R²` 是否低于上限、`cv` 是否超过行业给定区间，同时要求一阶差分存在 ≥2 次方向翻转；上述要素按权重组合成置信度，≥0.5 即视为周期性，并根据最新值判定阶段（peak/trough/rising/falling）。
    - 示例：`cyclical.is_cyclical=True` 且 `cyclical.current_phase='trough'` 表示被识别为周期型企业且当前在低谷阶段。

- `calculate_rolling_trend`：比较 3 年与 5 年斜率差异，衡量趋势加速度，用于识别短期加速上行或加速下滑的公司。
    - 判定标准：计算最近 3 年与全部 5 年的 OLS 斜率，`trend_acceleration = slope_3y - slope_5y`；当 >+1 认为趋势在加速改善，<-1 认为趋势在加速恶化，并返回对应布尔标记与警告。
    - 示例：`rolling.trend_acceleration = -1.8`，若小于 -1.0 表示短期下滑速度显著快于长期趋势。

### 淘汰底线与评分阈值

- 一票否决：`TrendRuleEvaluator` 遇到 `severity='severe'` 的近期恶化时，会复查两项底线——近两年累计跌幅是否超过 40%、最新值是否低于加权平均的 70%。任一命中就会输出 `veto=True`，直接剔除公司。
- 累积罚分：其他规则（轻度衰退、拐点转坏、周期高点、加速下滑等）统一叠加到 `penalty`。达到行业配置里的 `max_penalty_threshold`（默认 15 分）即 `passed=False`，视为淘汰。
- 正向抵扣：若识别到周期谷底或趋势加速改善，会回馈 5 分，帮助周期行业在底部阶段进入观察名单而非立即淘汰。

**调用范例**

```python
from astock.business_engines.trend.trend_analysis import calculate_log_trend_slope

values = [12.4, 14.0, 13.8, 15.2, 18.6]
log_result = calculate_log_trend_slope(values)

print(f"log slope = {log_result.log_slope:.4f}")
print(f"data quality = {log_result.quality.effective}")
if log_result.warnings:
    for warning in log_result.warnings:
        print(f"warning[{warning.code}] -> {warning.message}")

row_payload = log_result.to_dict()  # 方便序列化
```

`TrendAnalyzer` 内部已经全面适配 dataclass，避免散乱的字典字段访问；上层决策与报表组件可以直接访问属性或在需要时统一转换为字典。

### 对数线性回归 (Log-Linear Regression)

#### 数学原理

**传统线性回归**:
```
y = β₀ + β₁·x
ROIC = β₀ + β₁·year
```

**对数线性回归**:
```
log(y) = β₀ + β₁·x
log(ROIC) = β₀ + β₁·year

等价于：
ROIC = e^(β₀ + β₁·year)
ROIC = e^β₀ · e^(β₁·year)
```

#### 为什么用对数回归？

| 方面 | 线性回归 | 对数回归 |
|------|----------|----------|
| **相对变化** | 30%→25% 和 10%→5% 视为相同（-5个百分点） | 30%→25% 和 10%→5% 视为不同（不同比例） |
| **异常值处理** | 极端值影响大 | 对数变换降低影响 |
| **金融逻辑** | 线性增长 | 复利增长 |
| **解释性** | 每年变化X个百分点 | 每年变化X% |

#### 实际案例对比

**案例：公司A的ROIC变化**

| 年份 | ROIC | 线性斜率 | log(ROIC) | 对数斜率 |
|------|------|----------|-----------|----------|
| 2020 | 30% | - | -1.204 | - |
| 2021 | 25% | -5个百分点 | -1.386 | -0.182 |
| 2022 | 20% | -5个百分点 | -1.609 | -0.223 |
| 2023 | 15% | -5个百分点 | -1.897 | -0.288 |
| 2024 | 10% | -5个百分点 | -2.303 | -0.406 |

**分析**:
- **线性回归**: 斜率 = -5个百分点/年（看起来均匀）
- **对数回归**: 斜率 = -0.275/年（反映真实加速下降）

对数回归能发现：从20%→15%→10%的下降速度在加快（不是匀速）！

### 三层信号系统

```
P0层: 基础趋势（10年）
  ├─ 对数线性回归
  ├─ 拟合优度检验
  └─ 衰退程度判断

P1层: 短期变化（3-5年）
  ├─ 拐点检测（前5年 vs 后5年）
  └─ 近期恶化（最近3年 vs 10年加权）

P2层: 精细分析（3年滚动）
  ├─ 周期性检测（变异系数）
  └─ 加速度检测（斜率变化率）
```

---

## ⚙️ 决策引擎架构

### v2.1 统一决策引擎

**核心思想**: 先收集所有信号 → 统一决策 → 详细输出

```
┌─────────────────────────────────────────┐
│     阶段1: 信号收集 (Signal Collection)    │
├─────────────────────────────────────────┤
│  P0: is_mild_decline                    │
│  P1: p1_deterioration_signal            │
│      p1_inflection_signal               │
│  P2: p2_cyclical_signal                 │
│      p2_acceleration_signal             │
│  关键比率: latest_vs_weighted_ratio      │
│           is_sustained_decline          │
└─────────────────────────────────────────┘
                    ⬇
┌─────────────────────────────────────────┐
│    阶段2: 统一决策引擎 (Decision Engine)   │
├─────────────────────────────────────────┤
│  【优先级1】一票否决规则                   │
│   Rule 8a: severe + 总跌幅>40%          │
│   Rule 8b: severe + 最新<70%加权         │
│                                         │
│  【优先级2】累积罚分系统                   │
│   Rule 3: 轻度衰退 -5分                 │
│   Rule 4: 近期恶化 -10/-15分            │
│   Rule 9: 持续衰退重罚 -10分 (v2.1新增)  │
│   Rule 5: 拐点 -15/+5分                 │
│   Rule 6: 周期性 -3/+5分                │
│   Rule 7: 加速度 -5/+5分                │
│                                         │
│  【优先级3】阈值判断                      │
│   penalty >= 15 → 淘汰                  │
└─────────────────────────────────────────┘
                    ⬇
┌─────────────────────────────────────────┐
│      阶段3: 详细输出 (Detailed Output)    │
├─────────────────────────────────────────┤
│  扣分项: [...penalty_details...]        │
│  加分项: [...bonus_details...]          │
│  最终决策: ✅通过 / ❌淘汰               │
└─────────────────────────────────────────┘
```

### 决策规则表

| 规则 | 层级 | 条件 | 罚分/加分 | 优先级 |
|------|------|------|-----------|--------|
| **Rule 8a** | P1 | severe恶化 + 总跌幅>40% | 一票否决 | 1 |
| **Rule 8b** | P1 | severe恶化 + 最新<70%加权 | 一票否决 | 1 |
| **Rule 8c** | P0/P1 | 结构性衰退 (log斜率低 + 最新<85%加权 + 总跌幅>25%) | 一票否决 | 1 |
| **Rule 3** | P0 | 轻度衰退 (log_slope < -0.15) | -5分 | 2 |
| **Rule 4** | P1 | severe恶化 | -15分 | 2 |
| **Rule 4** | P1 | moderate恶化 | -10分 | 2 |
| **Rule 9** | P1 | 持续衰退 (log_slope < -0.15 且 最新<加权) | -10分 | 2 |
| **Rule 5a** | P1 | 增长转衰退 | -15分 | 2 |
| **Rule 5b** | P1 | 衰退转改善 | +5分 | 2 |
| **Rule 6a** | P2 | 周期谷底 | +5分 | 2 |
| **Rule 6b** | P2 | 周期高点 | -3分 | 2 |
| **Rule 7a** | P2 | 下降加速 (< -1.0) | -5分 | 2 |
| **Rule 7b** | P2 | 改善加速 (> 1.0) | +5分 | 2 |
| **阈值判断** | - | penalty >= 15 | 淘汰 | 3 |

### v2.1 vs v2.0 对比

| 方面 | v2.0 (分散决策) | v2.1 (统一引擎) |
|------|----------------|----------------|
| **架构** | 规则分散执行 | 三阶段统一流程 |
| **决策顺序** | 顺序相关 | 顺序无关（除一票否决） |
| **透明度** | 无详细记录 | penalty_details完整追溯 |
| **一票否决** | 无 | Rule 8 & 8c (连锁+结构衰退) |
| **持续衰退** | 无特殊处理 | Rule 9 重罚 (v2.1新增) |
| **罚分阈值** | 20 | 15 (v2.1降低) |
| **P2阈值** | 2.0 | 1.0 (v2.1降低) |
| **可维护性** | 难以修改 | 模块化，易扩展 |
| **可测试性** | 难以单元测试 | 各层独立测试 |

---

## 🎛️ 配置体系

### 行业差异化配置原理

**核心思想**: 不同行业有不同的增长特性和波动性

```python
# 示例：三类行业对比

# 1. 稳定消费类（医药生物）- 严格标准
{
    'log_severe_decline_slope': -0.25,  # 不容忍显著衰退
    'log_mild_decline_slope': -0.15,
    'r_squared_threshold': 0.65,        # 要求趋势稳定
}

# 2. 周期性行业（钢铁）- 宽松标准
{
    'log_severe_decline_slope': -0.65,  # 容忍大幅波动
    'log_mild_decline_slope': -0.35,
    'r_squared_threshold': 0.45,        # 允许波动性
}

# 3. 科技成长（软件服务）- 严格标准
{
    'log_severe_decline_slope': -0.20,  # 不容忍显著衰退
    'log_mild_decline_slope': -0.10,
    'r_squared_threshold': 0.60,
}
```

### 配置参数详解

| 参数 | 含义 | 典型范围 | 影响 |
|------|------|----------|------|
| `log_severe_decline_slope` | 严重衰退对数斜率阈值 | -0.20 ~ -0.65 | 越负越宽松 |
| `log_mild_decline_slope` | 轻度衰退对数斜率阈值 | -0.10 ~ -0.35 | 越负越宽松 |
| `r_squared_threshold` | 拟合优度阈值 | 0.45 ~ 0.70 | 越高要求趋势越稳定 |
| `max_penalty_threshold` | 罚分淘汰阈值 | 15 (统一) | 越低越严格 |
| `min_latest_value` | 最新值最低要求 | 5.0 ~ 15.0 | 行业差异大 |

### 如何调整配置

**场景1: 行业过于严格，优质公司被误杀**

```python
# 编辑 trend/config/filters.py
INDUSTRY_FILTER_CONFIGS = {
    "你的行业": {
        "log_severe_decline_slope": -0.30,  # 从-0.25放宽到-0.30
        "log_mild_decline_slope": -0.20,    # 从-0.15放宽到-0.20
        "r_squared_threshold": 0.55,        # 从0.65降低到0.55
    }
}
```

**场景2: 行业过于宽松，问题公司通过**

```python
INDUSTRY_FILTER_CONFIGS = {
    "你的行业": {
        "log_severe_decline_slope": -0.20,  # 从-0.25收紧到-0.20
        "log_mild_decline_slope": -0.10,    # 从-0.15收紧到-0.10
        "max_penalty_threshold": 12,        # 从15降低到12
    }
}
```

---

## 💻 使用示例

### 基础使用

```python
from astock.business_engines.engines import DuckDBTrendAnalyzer
from astock.business_engines.trend.config import get_filter_config

# 1. 初始化分析器
analyzer = DuckDBTrendAnalyzer(
    db_path='data/financial_data.duckdb',
    config=get_filter_config('医药生物')
)

# 2. 运行分析
results = analyzer.run_full_analysis(
    table_name='financial_indicators',
    metric='roic'
)

# 3. 查看结果
print(f"筛选前: {len(data)} 家公司")
print(f"筛选后: {len(results)} 家公司")
print(f"淘汰率: {(1 - len(results)/len(data)) * 100:.1f}%")
```

### 高级使用：批量行业分析

```python
from astock.business_engines.engines import DuckDBTrendAnalyzer
from astock.business_engines.trend.config import INDUSTRY_FILTER_CONFIGS

# 批量分析所有行业
results_by_industry = {}

for industry in INDUSTRY_FILTER_CONFIGS.keys():
    config = get_filter_config(industry)
    analyzer = DuckDBTrendAnalyzer(db_path='data/db.duckdb', config=config)

    results = analyzer.run_full_analysis(
        table_name='financial_indicators',
        metric='roic'
    )

    results_by_industry[industry] = results
    print(f"{industry}: {len(results)} 家通过")
```

### 生成报告

```python
from astock.business_engines.reporting import TrendReportGenerator

# 生成完整报告
generator = TrendReportGenerator(
    input_file='data/filter_middle/roic_trend_analysis.csv'
)

report = generator.generate_full_report()

# 保存
with open('data/trend_analysis_report.md', 'w', encoding='utf-8') as f:
    f.write(report)

print("报告已生成: data/trend_analysis_report.md")
```

---

## 🛠️ 开发指南

### 添加新规则

```python
# 在 duckdb_trend.py 的统一决策引擎中添加

def _unified_decision_engine(self, signals, config):
    # ... 现有代码 ...

    # 【新规则】示例：ROE同步验证
    if signals['roe_trend']['is_decline']:
        penalty += 3
        penalty_details.append("ROE同步下降-3分")

    # ... 后续代码 ...
```

### 添加新信号

```python
# 在 trend_analysis.py 中添加新的信号检测函数

def detect_p3_signals(df: pd.DataFrame) -> dict:
    """
    P3层：自定义信号检测
    """
    # 实现你的检测逻辑
    signal = your_detection_logic(df)

    return {
        'signal_type': signal,
        'signal_value': value,
    }
```

### 测试新功能

```python
import pytest
from astock.business_engines.engines import DuckDBTrendAnalyzer

def test_new_rule():
    # 准备测试数据
    test_data = create_test_data()

    # 运行分析
    analyzer = DuckDBTrendAnalyzer(...)
    result = analyzer._unified_decision_engine(test_data)

    # 验证结果
    assert result['penalty'] == expected_penalty
    assert 'your_rule' in result['penalty_details']
```

---

## 📚 参考资料

### 核心文档

- **[TREND_ANALYSIS_SYSTEM_GUIDE.md](../../../docs/TREND_ANALYSIS_SYSTEM_GUIDE.md)** - 系统完整指南
- **[V2.1_REFACTORING_GUIDE.md](../../../docs/V2.1_REFACTORING_GUIDE.md)** - v2.1重构说明
- **[V2.1_IMPROVEMENT_REPORT.md](../../../docs/V2.1_IMPROVEMENT_REPORT.md)** - v2.1改进报告
- **[WHY_P1_P2_NOT_ENOUGH.md](../../../docs/WHY_P1_P2_NOT_ENOUGH.md)** - 为什么需要v2.1

### 学术参考

- **对数回归**: [Wikipedia - Log-linear Model](https://en.wikipedia.org/wiki/Log-linear_model)
- **趋势分析**: Greene, W. H. (2012). Econometric Analysis. Pearson.
- **时间序列**: Box, G. E. P., & Jenkins, G. M. (1976). Time Series Analysis.

---

## 🏆 最佳实践

### 1. 配置调优

```python
# 保守策略（降低误纳率）
config = {
    'max_penalty_threshold': 12,  # 更严格
    'log_mild_decline_slope': -0.12,
}

# 激进策略（降低误拒率）
config = {
    'max_penalty_threshold': 18,  # 更宽松
    'log_severe_decline_slope': -0.30,
}
```

### 2. 监控日志

```bash
# 查看被一票否决的公司
Get-Content logs/latest.log | Select-String "一票否决"

# 查看通过公司的详细评分
Get-Content logs/latest.log | Select-String "【通过】"
```

### 3. 定期回测

```python
# 每季度运行回测
results = backtest_trend_analysis(
    start_date='2020-01-01',
    end_date='2024-12-31',
    metrics=['roic', 'roe']
)

# 分析误纳率和误拒率
analyze_accuracy(results)
```

---

**文档版本**: v2.1
**更新日期**: 2025-10-11
**维护者**: AStock Team
