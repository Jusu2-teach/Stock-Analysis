# AStock 趋势分析引擎 (Trend Analysis Engine) 架构文档

> **版本**: 3.0 (2025-11)  
> **定位**: 专业级基本面量化选股系统的核心引擎  
> **核心理念**: "用成长的速度衡量扩张，用质量的高度衡量护城河，用交叉验证识别造假"

---

## 1. 系统总览 (System Overview)

本系统不仅仅是一个简单的"计算增长率"的工具，而是一个模拟顶级基本面分析师思维过程的**自动化决策引擎**。它旨在解决传统量化回测中常见的"幸存者偏差"、"财务造假陷阱"和"伪增长"问题。

### 核心能力
1.  **多维趋势识别**: 结合 OLS (普通最小二乘)、Theil-Sen (稳健回归) 和 Mann-Kendall (趋势检验)，在 A 股高波动环境下精准识别趋势。
2.  **三表交叉验证**: 打通利润表、现金流量表和资产负债表，识别"有利润无现金"或"低效扩张"的风险。
3.  **双向决策机制**: 
    *   **规则引擎 (Rules)**: 负责"排雷"（负面剔除），使用一票否决制。
    *   **策略引擎 (Strategies)**: 负责"选优"（正面筛选），寻找皇冠上的明珠。
4.  **自适应阈值**: 针对不同行业（如周期股 vs 消费股）和不同指标（如营收 vs 毛利）自动调整评估标准。

---

## 2. 核心架构与工作流 (Architecture & Workflow)

系统采用 **管道-过滤器 (Pipeline-Filter)** 架构，数据流向如下：

```mermaid
graph TD
    A[原始财务数据] --> B(数据预处理 & 清洗)
    B --> C{特征提取层 (Probes)}
    C --> D[趋势向量 (TrendVector)]
    D --> E{规则引擎 (Rule Engine)}
    E -- 触发一票否决 --> F[淘汰 (Rejected)]
    E -- 通过/扣分 --> G{策略引擎 (Strategy Engine)}
    G -- 匹配策略 --> H[优质标的 (Selected)]
    G -- 未匹配 --> I[普通标的 (Neutral)]
```

### 详细处理流程

#### 第一阶段：数据预处理 (Data Preprocessing)
*   **模块**: `core.py`
*   **职责**: 
    *   处理缺失值 (NaN)、无穷大 (Inf)。
    *   执行 `min_periods` 检查（默认 5 年），剔除上市时间过短的公司（避免次新股陷阱）。
    *   加载 **参考指标 (Reference Metrics)**：例如在分析"净利润"时，同时加载"经营现金流"数据，为后续交叉验证做准备。

#### 第二阶段：特征提取 (Feature Extraction - Probes)
*   **模块**: `probes/`
*   **核心探针**:
    1.  **LogTrendProbe**: 计算对数线性回归斜率 (CAGR)，作为基础增长率。
    2.  **RobustTrendProbe**: 使用 **Theil-Sen 估算器** 计算稳健斜率，并进行 **Mann-Kendall 检验**。
        *   *作用*: 过滤掉由单年非经常性损益导致的"伪高增长"。
    3.  **CyclicalProbe**: 识别周期性特征（峰谷比、波形），防止在周期顶点误判为高增长。
    4.  **DeteriorationProbe**: 检测近期（近1-2年）是否出现断崖式下跌。

#### 第三阶段：规则引擎 (Rule Engine - The Negative Filter)
*   **模块**: `rules.py`
*   **职责**: **"排雷"**。寻找任何可能导致亏损的瑕疵。
*   **机制**: 
    *   **一票否决 (Veto)**: 触发即淘汰。例如：ROIIC 长期为负、财务造假嫌疑。
    *   **扣分 (Penalty)**: 瑕疵累积。例如：增速放缓、波动过大。
*   **关键规则**:
    *   `rule_earnings_quality_divergence`: **含金量检验**。利润增长但现金流恶化 -> 重罚。
    *   `rule_sustainable_growth_check`: **内生增长检验**。营收增速远超 ROE -> 判定为低效烧钱扩张。
    *   `rule_roiic_capital_destruction`: **价值毁灭检验**。投入资本回报率长期为负 -> 一票否决。

#### 第四阶段：策略引擎 (Strategy Engine - The Positive Selector)
*   **模块**: `strategies.py`
*   **职责**: **"选优"**。在幸存者中寻找特定类型的优质公司。
*   **核心策略**:
    1.  **HighGrowthStrategy (高增长策略)**:
        *   要求：营收/利润高速增长 (>20%) + 趋势显著 (Mann-Kendall p<0.1) + ROE 及格。
        *   *拒绝*: "有毒增长" (高增长但 ROE<5%)。
    2.  **TurnaroundStrategy (困境反转策略)**:
        *   要求：基本面触底回升 + 稳健斜率转正 + 杜绝纯消息面炒作。
        *   *拒绝*: "伪反转" (营收回升但毛利率暴跌)。

---

## 3. 关键技术细节 (Technical Deep Dive)

### 3.1 为什么需要"参考指标" (Reference Metrics)?
单一指标具有欺骗性。本系统引入 `TrendContext.reference_metrics` 实现多维校验：

| 主分析指标 | 参考指标 | 校验目的 | 逻辑 |
| :--- | :--- | :--- | :--- |
| **净利润 (EPS)** | **经营现金流 (OCFPS)** | **含金量** | 利润涨、现金流跌 = 纸面富贵 (可能造假) |
| **营收 (Revenue)** | **ROE** | **可持续性** | 营收涨、ROE低 = 烧钱换增长 (股东受损) |
| **ROIIC** | **ROIC** | **边际效益** | 增量回报 < 存量回报 = 边际效益递减 |

### 3.2 为什么使用 Theil-Sen 和 Mann-Kendall?
A 股常有"单年暴雷"或"资产处置收益"导致的脉冲式波动。
*   **OLS (普通回归)**: 对异常值极其敏感。一年利润翻 10 倍会拉高整个 5 年的斜率。
*   **Theil-Sen (中位数斜率)**: 忽略异常值，只看"大多数年份"的趋势。
*   **Mann-Kendall**: 统计学检验。告诉我们"这个增长趋势是真实的，还是随机波动产生的"。

### 3.3 自适应阈值 (Adaptive Thresholds)
系统不搞"一刀切"。在 `strategies.py` 中：
*   **规模类指标 (Scale)**: 如营收。追求 **速度** (Growth > 20%)。
*   **效率类指标 (Efficiency)**: 如毛利率、ROE。追求 **高度与稳定性** (Value > 40%, Stable)。
    *   *逻辑*: 要求毛利率每年增长 20% 是荒谬的；要求毛利率维持在 50% 才是好公司。

---

## 4. 开发指南 (Developer Guide)

### 如何添加一个新的"排雷规则"?
1.  在 `src/astock/business_engines/trend/rules.py` 中定义函数：
    ```python
    def rule_my_new_check(context, params, thresholds):
        # 访问数据
        if context.latest_value < 0:
            return RuleResult("my_rule", "penalty", "负值扣分", 10.0)
        return None
    ```
2.  在 `src/astock/business_engines/trend/core.py` 的 `DEFAULT_TREND_RULES` 列表中注册。

### 如何添加一个新的"选股策略"?
1.  在 `src/astock/business_engines/trend/strategies.py` 中继承 `BaseStrategy`：
    ```python
    class MySuperStrategy(BaseStrategy):
        name = "super_stock"
        def evaluate(self, context):
            if context.log_slope > 0.5:
                return StrategyResult(self.name, True, "超级成长")
            return StrategyResult(self.name, False)
    ```
2.  在 `get_default_strategies()` 中注册。

---

## 5. 配置指南 (Configuration)

主要配置文件位于 `workflow/duckdb_screen.yaml`。

**启用交叉验证的配置示例**:
```yaml
    - name: "Analyze_Profit_Trend"
      component: "business_engine"
      engine: "duckdb"
      method: ["analyze_metric_trend"]
      parameters:
        metric_name: 'eps'
        # 关键：在此处指定参考指标
        reference_metrics: ["ocfps", "roe"] 
```

---

## 6. 常见问题 (FAQ)

**Q: 为什么我的股票明明增长很快，却被淘汰了?**
A: 检查日志。可能是触发了 `earnings_quality_divergence` (有利润无现金) 或 `high_volatility_instability` (波动过大，增长不显著)。

**Q: "Insufficient data" 警告是什么意思?**
A: 系统默认要求至少 5 年数据 (`min_periods=5`)。对于上市不满 5 年的公司，为了保证趋势分析的统计有效性，会自动跳过。这是有意设计的保护机制。

**Q: 怎么调整扣分力度?**
A: 在 `config.py` 或 YAML 参数中调整 `penalty_factor` (默认 20)。

---

**维护者**: Jusu2-teach
**最后更新**: 2025-11-29
