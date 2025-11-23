# 趋势分析引擎 (Trend Analysis Engine)

## 1. 概述 (Overview)

趋势分析引擎是 AStock 系统的核心业务组件之一，负责对上市公司的财务指标（如 ROIC, 营收增长率等）进行深度时序分析。它不仅计算简单的增长率，还通过多维度的探针（Probes）来评估趋势的质量、稳健性、周期性和潜在风险。

本模块采用 **Orchestrator-Probe (协调者-探针)** 架构，实现了高内聚、低耦合的设计。

---

## 2. 架构设计 (Architecture)

### 2.1 模块交互图谱

本引擎采用分层架构，各层职责单一且相互独立：

```mermaid
graph TD
    Input[原始数据序列] --> Analyzer[TrendAnalyzer (核心分析器)]

    subgraph Probes [探针层 (纯数学计算)]
        Analyzer --> LogProbe[LogTrendProbe]
        Analyzer --> InflectionProbe[InflectionProbe]
        Analyzer --> RobustProbe[RobustProbe]
        Analyzer --> OtherProbes[...]
    end

    Probes --> Vector[TrendVector (特征向量)]

    Vector --> Evaluator[TrendEvaluator (评估器)]

    subgraph Logic [业务逻辑层]
        Evaluator --> Rules[Rules Engine (评分/排雷)]
        Evaluator --> Strategies[Strategy Engine (选优/打标签)]
    end

    Rules --> Score[基础得分 & 否决结果]
    Strategies --> Tags[策略标签 & 加分]

    Score & Tags --> Output[最终分析报告]
```

### 2.2 核心组件详解

*   **协调者 (Orchestrator)**: `core.py`
    *   **TrendAnalyzer**: 整个分析流程的总指挥。它负责准备数据、按顺序调用各个探针、收集结果，并最终生成分析报告。它**不包含任何业务规则**，只负责算数。
    *   **TrendEvaluator**: 负责将算出来的数字变成业务结论。它依次调用规则引擎和策略引擎。

*   **探针层 (Probes)**: `probes/`
    *   **独立性**: 每个探针只负责计算一类指标（如斜率、波动率），完全不知道“股票”或“ROIC”是什么，只处理 `List[float]`。
    *   **LogTrendProbe**: 计算对数趋势斜率、CAGR 和 R²。
    *   **RobustProbe**: 使用 Theil-Sen 和 Mann-Kendall 算法进行抗噪的稳健趋势分析。
    *   **InflectionProbe**: 使用分段线性回归识别趋势拐点（如 V 型反转）。
    *   **CyclicalProbe**: 结合行业先验知识和波形分析，检测周期性特征。

*   **规则引擎 (Rule Engine)**: `rules.py`
    *   **职责**: **守门员**。负责定义什么是“不及格”。
    *   **逻辑**: 例如“斜率 < -0.1 且 R² > 0.5 -> 判定为严重衰退”。
    *   **解耦**: 规则只依赖 `TrendVector` 中的数据，不依赖具体的计算过程。

*   **策略引擎 (Strategy Engine)**: `strategies.py`
    *   **职责**: **星探**。负责定义什么是“特长生”。
    *   **逻辑**: 例如“斜率 > 0.15 且 加速度 > 0 -> 判定为高增长”。
    *   **独立**: 策略独立于规则运行，即使规则扣了分，策略依然可以给予加分或打标签。

---

## 3. 核心算法与特性 (Key Algorithms)

### 3.1 自适应权重 (Adaptive Weighting)
*   **问题**: 传统固定权重（如 `[0.1, 0.2, 0.3, 0.4]`）在面对剧烈波动时容易被最新一年的异常值误导。
*   **方案**: 系统自动计算变异系数 (CV)。如果 CV > 0.25（高波动），自动将权重“压平”（Flatten），降低最新年份的权重占比，从而避免“错杀”。

### 3.2 智能拐点检测 (Smart Inflection Detection)
*   **算法**: 分段线性回归 (Segmented Linear Regression)。
*   **能力**: 自动寻找最佳分割点（Best Split Point），识别趋势的结构性变化。
*   **识别类型**:
    *   `deterioration_to_recovery`: 困境反转 (V型)
    *   `growth_to_decline`: 盛极而衰 (倒V型)
    *   `acceleration`: 加速增长

### 3.3 稳健趋势分析 (Robust Trend Analysis)
*   **算法**: Theil-Sen 估算器 + Mann-Kendall 趋势检验。
*   **优势**: 非参数统计方法，对异常值不敏感。即使 5 年数据中有 1 年因为一次性损益导致暴跌，该算法仍能准确识别出原本的增长趋势。

### 3.5 策略标签系统 (Strategy Tagging System)
*   **定位**: 在基础评分之上，进行“优中选优”的二次筛选。
*   **功能**: 自动识别符合特定投资风格的股票，并打上标签。
*   **内置策略 (专业级)**:
    *   **高增长/优质护城河 (High Growth)**:
        *   **针对效率指标 (ROIC/毛利)**: 寻找"护城河"。要求高位企稳 (如 ROIC > 15% 且不恶化)。
        *   **针对规模指标 (营收/利润)**: 寻找"瞪羚"。要求高速奔跑 (如 CAGR > 20% 且加速)。
    *   **困境反转 (Turnaround)**:
        *   筛选从亏损/低谷中走出，且呈现 V 型反转或极速修复特征的股票。
        *   拒绝"死猫跳"，要求最新值必须回到安全线以上 (如净利率 > 2%)。
*   **输出**: 结果表中会自动增加 `{metric}_is_high_growth` 和 `{metric}_is_turnaround` 等布尔列，方便直接筛选。

---

## 4. 输出字段说明 (Output Fields)

分析结果包含以下几类关键指标：

### 核心指标
*   `trend_score`: 综合趋势得分 (0-100)。
*   `cagr`: 复合年均增长率近似值。
*   `weighted_avg`: 自适应加权平均值。

### 策略标签 (New)
*   `{metric}_strategies`: 命中的策略列表 (如 "high_growth,turnaround")。
*   `{metric}_is_high_growth`: 是否为高增长优质股 (0/1)。
*   `{metric}_is_turnaround`: 是否为困境反转股 (0/1)。
*   `{metric}_strategy_reasons`: 具体的命中原因说明。

### 诊断指标
*   `robust_slope`: 排除异常值后的稳健斜率。
*   `cv`: 变异系数（衡量波动率）。
*   `inflection_type`: 拐点形态（如 "growth_to_decline"）。
*   `is_cyclical`: 是否判定为周期性股。
*   `deterioration_severity`: 恶化严重程度 ("mild", "moderate", "severe")。

### 质量指标
*   `data_quality`: 数据质量评级 ("good", "poor", "has_loss")。
*   `r_squared`: 趋势拟合优度。

---

## 5. 扩展指南 (Extension Guide)

### 添加新策略 (New Strategy)
1.  在 `strategies.py` 中创建一个新类（如 `HighGrossMarginStrategy`）。
2.  实现 `evaluate(context)` 方法，定义您的筛选逻辑。
3.  在 `get_default_strategies()` 函数中注册该类。
4.  **无需修改核心代码**，系统会自动运行新策略并生成对应的输出列。

### 添加新探针
1.  在 `probes/` 目录下创建新文件（如 `my_probe.py`）。
2.  实现 `compute(values, context)` 方法。
3.  在 `core.py` 的 `get_default_metric_probes` 列表中注册新探针。
4.  在 `models.py` 的 `TrendVector` 中添加对应结果字段。

## 6. 实战指南：如何引入新指标 (How to Add New Metrics)

本引擎是**指标无关 (Metric-Agnostic)** 的。这意味着您可以用它分析 ROIC，也可以分析毛利率、净利率、营收增长率等任何时序数据。

### 步骤 1: 确保数据源存在
确保您的 DuckDB 或 CSV 数据源中包含该指标的列。例如，如果您想分析 `gross_margin`（毛利率），请确保输入表中有一列叫 `gross_margin`。

### 步骤 2: 配置 Pipeline
在您的 Pipeline 配置文件（如 `workflow/duckdb_screen.yaml`）中添加一个新的分析任务。

```yaml
# 示例：添加毛利率趋势分析
- name: "analyze_gross_margin_trend"
  engine: "trend"
  params:
    metric: "gross_margin"  # 指定要分析的列名
    group_column: "code"
    # 可选：覆盖默认阈值
    config:
      min_latest_value: 20.0  # 毛利率通常较高，门槛设为20%
```

### 步骤 3: 自动生效
一旦配置完成并运行 Pipeline，系统会自动执行以下操作：
1.  **计算趋势**: 计算毛利率的斜率、波动率、拐点等。
2.  **应用规则**: 检查毛利率是否严重衰退、是否低于 20% 等。
3.  **应用策略**:
    *   自动判断该股票的毛利率是否符合 **"High Growth"**（高增长）策略。
    *   自动判断该股票的毛利率是否符合 **"Turnaround"**（困境反转）策略。

### 步骤 4: (进阶) 定制策略门槛
如果您发现默认的策略门槛（如 `latest_value > 15`）不适合新指标（例如净利率通常只有 5%），您可以修改 `strategies.py`，使其支持参数化配置，或者添加特定指标的策略类。

```python
# 示例：在 strategies.py 中为净利率定制策略
class NetMarginGrowthStrategy:
    def evaluate(self, context):
        if context.metric_name == "net_margin" and context.latest_value > 5:
            return StrategyResult(...)
```
