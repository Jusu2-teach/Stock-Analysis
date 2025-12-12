# 报告系统架构说明

> **版本**: 4.0 (2025-12)
> **定位**: 全方位公司评价系统
> **核心理念**: "探针分析 → 六大基因 → 三大求解器 → 投资决策"

---

## 🔄 数据流：从探针分析到 T.R.U.T.H. 报告

### 第一步：探针分析输出（8个指标）

每个 `Analyze_XXX_Trend` 步骤输出的 DataFrame 包含大量趋势分析字段：

```
┌─────────────────────────────────────────────────────────────────┐
│ 探针输出字段 (以ROIC为例)                                        │
├─────────────────────────────────────────────────────────────────┤
│ roic_cagr              → 年化复合增长率                          │
│ roic_r_squared         → 趋势拟合度 (0-1)                        │
│ roic_cv                → 变异系数 (波动性)                       │
│ roic_detrended_cv      → 去趋势后波动                            │
│ roic_cyclical_confidence → 周期性置信度                          │
│ roic_trend_acceleration → 趋势加速度                             │
│ roic_deterioration_probability → 恶化概率                        │
│ roic_has_inflection    → 是否有拐点                              │
│ roic_inflection_type   → 拐点类型 (peak/trough/none)             │
│ roic_recent_slope      → 近期斜率                                │
│ roic_data_quality      → 数据质量评级                            │
│ ...更多字段...                                                   │
└─────────────────────────────────────────────────────────────────┘
```

### 第二步：六大基因提取（核心映射逻辑）

每个指标的探针字段会被映射到六大基因：

```python
# truth_report_generator.py: _extract_single_metric_genome()

┌────────────────────────────────────────────────────────────────────┐
│ 探针字段 → 六大基因                                                │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  α (周期性)  ← detrended_cv, cyclical_confidence                  │
│              意义: 公司受经济周期影响程度                          │
│                                                                    │
│  β (资本密度) ← cv (变异系数)                                      │
│              意义: 经营稳定性，波动越小越稳定                      │
│                                                                    │
│  γ (成长动能) ← cagr, r_squared, trend_acceleration               │
│              意义: 增长速度和可持续性                              │
│                                                                    │
│  δ_fraud (欺诈熵) ← cv太低(<0.02), r_squared太高(>0.98)           │
│              意义: 数据是否"太完美"可疑                            │
│                                                                    │
│  δ_decay (衰退熵) ← deterioration_probability, has_inflection,    │
│                    inflection_type, recent_slope                   │
│              意义: 是否正在走下坡路                                │
│                                                                    │
│  V (验证)    ← data_quality                                        │
│              意义: 数据可信度                                      │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

### 第三步：加权聚合（8个指标 → 1个公司基因组）

```python
# _aggregate_company_genome()

┌─────────────────────────────────────────────────────────────────┐
│ 8个指标的基因加权聚合                                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   ROIC基因 (权重20%)  ──┐                                        │
│   ROE基因 (权重15%)   ──┤                                        │
│   ROIIC基因 (权重15%) ──┤                                        │
│   毛利率基因 (权重10%)──┤──→ 加权平均 ──→ CompanyGenome          │
│   净利率基因 (权重10%)──┤       ↓                                │
│   营收基因 (权重10%)  ──┤    (α, β, γ, δ_fraud, δ_decay, V)     │
│   利润基因 (权重10%)  ──┤                                        │
│   现金流基因 (权重10%)──┘                                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 第四步：三大求解器（基因 → 投资决策）

```python
# _run_solvers()

┌─────────────────────────────────────────────────────────────────┐
│ 三大物理求解器                                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  CompanyGenome ──→ 🔴 重力求解器 (gravity_solver)                │
│  (α,β,γ,δ,V)      │  输入: α(周期性), β(资本密度)               │
│                   │  输出: T_threshold (动态ROIC阈值)            │
│                   │  解释: 周期性强的公司需要更高的ROIC门槛       │
│                   │                                             │
│               ──→ 🟢 速度求解器 (velocity_solver)                │
│                   │  输入: γ(成长动能), β(资本密度)              │
│                   │  输出: max_sustainable_growth (可持续增长率) │
│                   │  解释: 计算不透支未来的最大增长速度           │
│                   │                                             │
│               ──→ 🔵 结构求解器 (structure_solver)               │
│                   │  输入: δ_fraud, δ_decay, V                  │
│                   │  输出: expected_slope (预期趋势斜率)         │
│                   │  解释: 预测未来走势，是上升还是下降           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 完整数据流图

```
 ┌─────────────────────────────────────────────────────────────────────────┐
 │                        Pipeline 执行流程                                │
 └─────────────────────────────────────────────────────────────────────────┘

 数据源                探针分析                  T.R.U.T.H.处理
 ────────────────────────────────────────────────────────────────────────

 10年财务数据          8个指标趋势分析            六大基因          三大求解器
      │                    │                       │                  │
      ▼                    ▼                       ▼                  ▼
 ┌─────────┐        ┌──────────────┐        ┌───────────┐      ┌──────────┐
 │ CSV数据 │───────→│ ROIC探针    │──┐     │           │      │ 重力求解 │
 │ 1913家  │        │ ROE探针     │  │     │  α 周期性 │      │ T阈值    │
 │ 公司    │        │ ROIIC探针   │  │     │  β 资本   │      └────┬─────┘
 └─────────┘        │ 毛利率探针  │  ├────→│  γ 成长   │──────────→│
                    │ 净利率探针  │  │     │  δ 欺诈   │      ┌────┴─────┐
                    │ 营收探针    │  │     │  δ 衰退   │      │ 速度求解 │
                    │ 利润探针    │  │     │  V 验证   │      │ 增长边界 │
                    │ 现金流探针  │──┘     │           │      └────┬─────┘
                    └──────────────┘        └───────────┘           │
                          │                      │            ┌────┴─────┐
                          │                      │            │ 结构求解 │
                          ▼                      ▼            │ 预期斜率 │
                    每个探针输出:          加权聚合成:         └────┬─────┘
                    - cagr                CompanyGenome              │
                    - r_squared           每家公司一个               ▼
                    - cv                                       ┌──────────┐
                    - deterioration_prob                       │  最终    │
                    - has_inflection                           │  投资    │
                    - ...                                      │  决策    │
                                                               └──────────┘
```

---

## 🧬 六大基因详解

| 基因 | 名称 | 来源字段 | 含义 | 范围 |
|------|------|----------|------|------|
| α | 周期性 | detrended_cv, cyclical_confidence | 受经济周期影响程度 | 0-1 |
| β | 资本密度 | cv (变异系数) | 经营稳定性 | 0-1 |
| γ | 成长动能 | cagr, r_squared, trend_acceleration | 增长速度和质量 | 0-1 |
| δ_fraud | 欺诈熵 | cv太低, r_squared太高 | 数据造假可能性 | 0-1 |
| δ_decay | 衰退熵 | deterioration_prob, inflection | 走下坡路风险 | 0-1 |
| V | 验证 | data_quality | 数据可信度 | 0-1 |

---

## ⚙️ 三大求解器详解

### 🔴 重力求解器 (Gravity Solver)

**物理类比**：越是周期性强、资本密集的行业，需要更高的"逃逸速度"（ROIC门槛）

**公式**：
```
T_threshold = base_threshold × (1 + α × cyclical_factor) × (1 + β × capital_factor)
```

**输出**：动态ROIC阈值，周期性公司阈值更高

---

### 🟢 速度求解器 (Velocity Solver)

**物理类比**：增长像速度，不能无限加速，有物理极限

**公式**：
```
max_sustainable_growth = γ × growth_potential / (1 + β × drag_factor)
```

**输出**：可持续增长率上限，防止增长透支

---

### 🔵 结构求解器 (Structure Solver)

**物理类比**：建筑有结构完整性，财务数据也有"结构健康度"

**公式**：
```
expected_slope = base_slope × V × (1 - δ_fraud) × (1 - δ_decay)
```

**输出**：预期未来趋势斜率，正=上升，负=下降

---

## 📁 文件结构

```
reporters/
├── __init__.py                    # 导出模块
├── engine.py                      # 报告引擎入口（注册方法）
├── comprehensive_generator.py     # 🔵 规则驱动报告（基于预设阈值）
├── truth_report_generator.py      # 🟢 T.R.U.T.H.数据驱动报告（无阈值）
├── generic_reporter.py            # 通用报告生成器
└── README.md                      # 本文档
```

---

## 🔑 两套独立报告系统

| 特性 | 规则驱动报告 | T.R.U.T.H.报告 |
|------|-------------|---------------|
| 文件 | `comprehensive_generator.py` | `truth_report_generator.py` |
| 方法 | `report_comprehensive` | `report_truth` |
| 阈值 | 预设固定阈值 | 动态计算阈值 |
| 适用 | 快速筛选 | 深度分析 |
| 核心 | 4层过滤体系 | 6基因+3求解器 |

---

## 💻 代码示例

### 使用方式

```python
from src.astock.business_engines.reporters.engine import report_truth, report_comprehensive

# 直接传入探针分析结果（无需读取CSV文件）
result = report_truth(
    roic_data=roic_df,
    roe_data=roe_df,
    roiic_data=roiic_df,
    gross_margin_data=gross_margin_df,
    net_margin_data=net_margin_df,
    revenue_data=revenue_df,
    profit_data=profit_df,
    ocf_data=ocf_df,
    output_path='data/truth_analysis_report.md'
)
```

### 一家公司的完整处理流程

```python
# 假设分析贵州茅台 (600519.SH)

# 1. 从8个探针DataFrame获取该公司数据
roic_row = roic_data.loc['600519.SH']  # roic_cagr=0.15, roic_r_squared=0.85...

# 2. 提取每个指标的六大基因
roic_genes = {
    'alpha': 0.2,      # 周期性低（茅台不太受周期影响）
    'beta': 0.1,       # 波动小（稳定）
    'gamma': 0.6,      # 中等成长
    'delta_fraud': 0.0, # 无欺诈信号
    'delta_decay': 0.1, # 轻微衰退（增速放缓）
    'verification': 1.0 # 数据可信
}

# 3. 8个指标加权聚合
company_genome = CompanyGenome(
    ts_code='600519.SH',
    company_name='贵州茅台',
    alpha=0.18,  # 加权平均
    beta=0.12,
    gamma=0.55,
    delta_fraud=0.02,
    delta_decay=0.15,
    verification=0.95,
)

# 4. 三大求解器计算
gravity_result = gravity_solver(genome)   # T_threshold = 12%
velocity_result = velocity_solver(genome) # max_growth = 18%
structure_result = structure_solver(genome) # expected_slope = +0.03

# 5. 输出投资建议
# "茅台ROIC需要 > 12%才值得投资，可持续增长率18%，预期继续上升"
```

---

## 📊 Pipeline 集成

在 `workflow/analysis.yaml` 中配置：

```yaml
# 🟢 T.R.U.T.H. 数据驱动报告
- name: "Generate_Truth_Report"
  component: "business_engine"
  engine: "reporting"
  method: ["report_truth"]
  parameters:
    # 直接接收探针分析结果
    roic_data: "steps.Analyze_ROIC_Trend.outputs.parameters.ROIC_Trend_Result"
    roe_data: "steps.Analyze_ROE_Trend.outputs.parameters.ROE_Trend_Result"
    roiic_data: "steps.Analyze_ROIIC_Trend.outputs.parameters.ROIIC_Trend_Result"
    gross_margin_data: "steps.Analyze_GrossMargin_Trend.outputs.parameters.GrossMargin_Trend_Result"
    net_margin_data: "steps.Analyze_NetMargin_Trend.outputs.parameters.NetMargin_Trend_Result"
    revenue_data: "steps.Analyze_Revenue_Trend.outputs.parameters.Revenue_Trend_Result"
    profit_data: "steps.Analyze_Profit_Trend.outputs.parameters.Profit_Trend_Result"
    ocf_data: "steps.Analyze_OCF_Trend.outputs.parameters.OCF_Trend_Result"
    output_path: "data/truth_analysis_report.md"
```

---

## 📝 总结

1. **探针分析** → 输出每个指标的30+个统计特征
2. **基因映射** → 把这些特征映射到6个基因维度
3. **加权聚合** → 8个指标聚合成1个公司基因组
4. **求解器计算** → 基因组输入三大求解器，输出投资决策阈值

---

## 📅 更新日志

### V4.0 (2025-12-11)
- ✨ **数据直传**：报告直接接收探针DataFrame，无需读取CSV文件
- ✨ **清理兼容模式**：删除所有文件读取逻辑，简化代码
- ✨ **文档更新**：添加完整的数据流说明和六大基因/三大求解器详解

### V3.0 (2025-12-07)
- ✨ **架构重构**：聚焦3类公司筛选，所有技术方法都作为内部工具
- ✨ **4层过滤体系**：基础质量→交叉验证→拐点检测→综合分类
- ✨ **T.R.U.T.H.系统独立**：创建独立的truth_report_generator.py

### V2.0 (2025-11)
- 多板块报告（优质公司/白马护城河/困境反转/风险警示）
- 综合评分模型（成长/质量/安全因子）

### V1.0 (2025-10)
- 初始版本，单指标CSV输出

---

**维护者**: Jusu2-teach
**最后更新**: 2025-12-11
