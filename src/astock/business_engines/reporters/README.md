# 报告系统架构说明

> **版本**: 5.0 (2025-01)
> **定位**: 全方位公司评价系统
> **核心理念**: "探针分析 → T.R.U.T.H. 处理 → 专业基因映射 → 投资决策"

---

## 🔄 数据流架构（v5.0 更新）

### 新架构：独立 T.R.U.T.H. 处理步骤

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    analysis.yaml Pipeline 数据流                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐                       │
│  │Analyze_ROIC │   │Analyze_ROE  │   │Analyze_ROIIC│  ...8个探针           │
│  └──────┬──────┘   └──────┬──────┘   └──────┬──────┘                       │
│         │                 │                 │                               │
│         └────────────────┬┴─────────────────┘                               │
│                          ↓                                                  │
│         ┌────────────────────────────────┐                                  │
│         │    Process_Truth_System        │  ← 新增独立处理步骤              │
│         │    (business_engine: truth)    │                                  │
│         │                                │                                  │
│         │  🧬 专业基因-指标映射          │                                  │
│         │  ⚙️ 三大求解器执行             │                                  │
│         │  🔗 因果网络验证               │                                  │
│         └────────────┬───────────────────┘                                  │
│                      ↓                                                      │
│         ┌────────────────────────────────┐                                  │
│         │   Generate_Truth_Report        │                                  │
│         │   (使用处理后的专业数据)        │                                  │
│         └────────────────────────────────┘                                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 专业基因-指标映射关系

```
┌─────────────────────────────────────────────────────────────────────────────┐
│              专业基因-指标映射表 (TruthProcessor)                            │
├─────────┬───────────────────┬─────────────┬─────────────────────────────────┤
│  基因   │  核心指标         │  聚合策略   │  计算逻辑                        │
├─────────┼───────────────────┼─────────────┼─────────────────────────────────┤
│  α      │  ROIC, ROE        │  max        │  周期性最强的那个决定公司周期性 │
│ (周期性)│                   │             │                                 │
├─────────┼───────────────────┼─────────────┼─────────────────────────────────┤
│  β      │  ROIC, OCF        │  加权       │  ROIC波动(40%)+OCF波动(30%)     │
│(资本密度)│  利润, 营收       │             │  +DOL经营杠杆(30%)              │
├─────────┼───────────────────┼─────────────┼─────────────────────────────────┤
│  γ      │  营收, 利润, OCF  │  调和平均   │  惩罚不平衡增长                  │
│(成长动能)│                   │             │  营收/利润/现金流应同步增长     │
├─────────┼───────────────────┼─────────────┼─────────────────────────────────┤
│  δ_f    │  利润 vs 现金流   │  逻辑OR     │  任一背离即触发欺诈预警          │
│(欺诈熵) │  ROE vs ROIC      │  (max)      │  利润↑但现金流↓ = 红旗          │
├─────────┼───────────────────┼─────────────┼─────────────────────────────────┤
│  δ_d    │  ROIC, ROE        │  max        │  最严重的衰退信号决定整体风险    │
│(衰退熵) │  毛利率, 净利率   │             │                                 │
├─────────┼───────────────────┼─────────────┼─────────────────────────────────┤
│  V      │  OCF              │  单一       │  现金流是最终验证                │
│(验证)   │                   │             │                                 │
└─────────┴───────────────────┴─────────────┴─────────────────────────────────┘
```

### 求解器数据流

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      三大求解器数据流                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ 重力求解器 (gravity_solver)                                          │   │
│  │ ├─ 输入: ROIC指标的 α(周期性), β(资本密度)                           │   │
│  │ ├─ 输出: 动态ROIC阈值 T_threshold                                   │   │
│  │ └─ 逻辑: 周期性强、资本密度高 → 需要更高ROIC门槛                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ 速度求解器 (velocity_solver)                                         │   │
│  │ ├─ 输入: 营收/利润/现金流指标的 γ(成长动能)                          │   │
│  │ ├─ 输出: 增长边界 T_growth_bound, 增长分类                          │   │
│  │ └─ 逻辑: 评估可持续增长速度，防止过高预期                           │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ 结构求解器 (structure_solver)                                        │   │
│  │ ├─ 输入: 毛利率斜率                                                  │   │
│  │ ├─ 输出: 护城河侵蚀预警                                             │   │
│  │ └─ 逻辑: 毛利率持续下滑 = 竞争优势受损                              │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 因果网络验证

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       因果网络验证层                                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│      营收增长 ──────→ 利润增长 ──────→ 现金流增长                           │
│          │               │                 │                                │
│          │               │                 │                                │
│          ↓               ↓                 │                                │
│         ROE ←────────── ROIC               │                                │
│          │                                 │                                │
│          │                                 │                                │
│          ↓                                 │                                │
│      周期性波动                             │                                │
│                                            │                                │
│  验证规则:                                                                  │
│  1. 营收增长但利润不增长 → 因果异常                                         │
│  2. 利润增长但现金流下降 → 因果断裂（欺诈信号）                             │
│  3. ROE远高于ROIC → 杠杆风险                                                │
│  4. ROIC周期但营收不周期 → 周期异常                                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 📊 探针分析输出（8个指标）

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

---

## 🔧 使用模式说明（仅专业模式）

报告系统在 v5.0 起只支持 **单一官方链路**：

1. 通过 `Process_Truth_System` 步骤调用 `business_engine: truth`，
   接收 8 个趋势探针分析结果，生成 `Truth_Processed_Results`。
2. 报告引擎 `report_truth` / `report_truth_single` 只接受
   `truth_processed` 作为入口，不再直接消费探针 DataFrame。

推荐的 analysis.yaml 片段如下：

```yaml
# Step 1: T.R.U.T.H. 处理
- name: "Process_Truth_System"
  component: "business_engine"
  engine: "truth"
  method: ["process_truth"]
  parameters:
    roic_data: "steps.Analyze_ROIC_Trend.outputs.parameters.ROIC_Trend_Result"
    roe_data: "steps.Analyze_ROE_Trend.outputs.parameters.ROE_Trend_Result"
    roiic_data: "steps.Analyze_ROIIC_Trend.outputs.parameters.ROIIC_Trend_Result"
    gross_margin_data: "steps.Analyze_GrossMargin_Trend.outputs.parameters.GrossMargin_Trend_Result"
    net_margin_data: "steps.Analyze_NetMargin_Trend.outputs.parameters.NetMargin_Trend_Result"
    revenue_data: "steps.Analyze_Revenue_Trend.outputs.parameters.Revenue_Trend_Result"
    profit_data: "steps.Analyze_Profit_Trend.outputs.parameters.Profit_Trend_Result"
    ocf_data: "steps.Analyze_OCF_Trend.outputs.parameters.OCF_Trend_Result"
  outputs:
    parameters:
      - name: Truth_Processed_Results

# Step 2: T.R.U.T.H. 报告
- name: "Generate_Truth_Report"
  component: "business_engine"
  engine: "reporting"
  method: ["report_truth"]
  parameters:
    truth_processed: "steps.Process_Truth_System.outputs.parameters.Truth_Processed_Results"
    output_path: "data/truth_analysis_report.md"
```

> 注意：旧版本中“直接传探针 DataFrame” 的兼容模式已废弃，
> 报告系统不再从 CSV 或中间 DataFrame 直接构建 T.R.U.T.H.，
> 以保证 T.R.U.T.H. 口径与 TruthProcessor 完全一致。
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
├── engine.py                      # 报告引擎入口（@register_method 注册点）
├── comprehensive_generator.py     # 🔵 规则驱动报告（基于预设阈值）
├── truth_report_generator.py      # 🟢 T.R.U.T.H.数据驱动报告（无阈值）
└── README.md                      # 本文档
```

---

## 🔑 两套独立报告系统

| 特性 | 规则驱动报告 | T.R.U.T.H.报告 |
|------|-------------|---------------|
| 文件 | `comprehensive_generator.py` | `truth_report_generator.py` |
| 方法 | `report_comprehensive` | `report_truth` / `report_truth_single` |
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
