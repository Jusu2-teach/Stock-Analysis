# T.R.U.T.H. 六维基因系统 v3.0

> **T**ransparent **R**isk-adjusted **U**nified **T**hreshold **H**euristic
>
> 透明风险调整统一阈值启发式系统

## 🏗️ 系统架构总览

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        T.R.U.T.H. 系统架构 v3.0                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                    Pipeline 探针分析层                                │   │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐                     │   │
│  │  │  ROIC   │ │   ROE   │ │  ROIIC  │ │ 毛利率  │                     │   │
│  │  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘                     │   │
│  │  ┌────┴────┐ ┌────┴────┐ ┌────┴────┐ ┌────┴────┐                     │   │
│  │  │ 净利率  │ │  营收   │ │  利润   │ │  OCF    │   8个探针DataFrame  │   │
│  │  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘                     │   │
│  └───────┼──────────┼──────────┼──────────┼─────────────────────────────┘   │
│          │          │          │          │                                 │
│          ▼          ▼          ▼          ▼                                 │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                 DataFrameToProbeConverter (桥接层)                    │   │
│  │                                                                       │   │
│  │  DataFrame行 ──→ LogTrendResult, VolatilityResult,                   │   │
│  │                  CyclicalPatternResult, RecentDeteriorationResult... │   │
│  │                                      ↓                                │   │
│  │                              ProbeOutputs                             │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                      │                                      │
│                                      ▼                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                   ProbeAdapter.adapt() (适配层)                       │   │
│  │                                                                       │   │
│  │  MultiIndicatorProbeOutputs ──→ GenomeInput                          │   │
│  │  (roic, gross_margin, revenue, ocf, net_profit)                      │   │
│  │                           ↓                                          │   │
│  │  AlphaGeneInput, BetaGeneInput, GammaGeneInput,                      │   │
│  │  DeltaFraudInput, DeltaDecayInput, VFactorInput                      │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                      │                                      │
│                                      ▼                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │              compute_genome_from_probes() (核心计算层)                │   │
│  │  ┌────────────────────────────────────────────────────────────────┐  │   │
│  │  │  core/genes/                                                    │  │   │
│  │  │  ├─ alpha.py:       compute_alpha_from_probes()   → α          │  │   │
│  │  │  ├─ beta.py:        compute_beta_from_probes()    → β          │  │   │
│  │  │  ├─ gamma.py:       compute_gamma_from_probes()   → γ          │  │   │
│  │  │  ├─ delta_fraud.py: compute_delta_fraud_from_probes() → δ_f    │  │   │
│  │  │  ├─ delta_decay.py: compute_delta_decay_from_probes() → δ_d    │  │   │
│  │  │  └─ verification.py: compute_verification_from_probes() → V    │  │   │
│  │  └────────────────────────────────────────────────────────────────┘  │   │
│  │                           ↓                                          │   │
│  │                     CompanyGenome                                    │   │
│  │              (α, β, γ, δ_fraud, δ_decay, V)                          │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                      │                                      │
│                                      ▼                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                      三大求解器 (Solvers)                             │   │
│  │  ┌─────────────────┬─────────────────┬─────────────────┐             │   │
│  │  │ gravity_solver  │ velocity_solver │ structure_solver│             │   │
│  │  │ (重力求解器)    │ (速度求解器)    │ (结构求解器)    │             │   │
│  │  ├─────────────────┼─────────────────┼─────────────────┤             │   │
│  │  │ 输入: α, β      │ 输入: γ, V      │ 输入: genome    │             │   │
│  │  │ 输出: ROIC阈值  │ 输出: 增长边界  │ 输出: 斜率预测  │             │   │
│  │  └─────────────────┴─────────────────┴─────────────────┘             │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                      │                                      │
│                                      ▼                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                      TruthProcessResult (输出)                        │   │
│  │  - genome: CompanyGenome (六维基因)                                   │   │
│  │  - solver_results: 三大求解器结果                                     │   │
│  │  - final_score: 综合评分                                              │   │
│  │  - signal: STRONG_BUY | BUY | NEUTRAL | CAUTION | SELL               │   │
│  │  - grade: A | B+ | B | C | D                                         │   │
│  │  - warnings: 风险预警列表                                             │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🔄 Workflow 集成

### analysis.yaml 中的 T.R.U.T.H. 处理步骤

```yaml
# ═══════════════════════════════════════════════════════════════════════════
# 🧬 T.R.U.T.H. 处理系统 - 专业基因-指标映射
# ═══════════════════════════════════════════════════════════════════════════

# Step 1: T.R.U.T.H. 处理（专业基因提取 + 求解器）
- name: "Process_Truth_System"
  component: "business_engine"
  engine: "truth"
  method: ["process_truth"]
  parameters:
    # 接收8个探针分析结果
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

# Step 2: T.R.U.T.H. 报告生成
- name: "Generate_Truth_Report"
  component: "business_engine"
  engine: "reporting"
  method: ["report_truth"]
  parameters:
    truth_processed: "steps.Process_Truth_System.outputs.parameters.Truth_Processed_Results"
    output_path: "data/truth_analysis_report.md"
```

### 完整数据流

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Pipeline 完整数据流                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ① Load_Financial_Data                                                      │
│     │  读取: data/polars/10yd_final_industry.csv                            │
│     ▼                                                                       │
│  ② 8个探针分析步骤 (并行处理)                                                │
│     ├─ Analyze_ROIC_Trend    → roic_trend_analysis.csv                      │
│     ├─ Analyze_ROE_Trend     → roe_trend_analysis.csv                       │
│     ├─ Analyze_ROIIC_Trend   → roiic_trend_analysis.csv                     │
│     ├─ Analyze_GrossMargin   → gross_margin_trend_analysis.csv              │
│     ├─ Analyze_NetMargin     → net_margin_trend_analysis.csv                │
│     ├─ Analyze_Revenue       → revenue_trend_analysis.csv                   │
│     ├─ Analyze_Profit        → profit_trend_analysis.csv                    │
│     └─ Analyze_OCF           → ocf_trend_analysis.csv                       │
│     │                                                                       │
│     ▼                                                                       │
│  ③ Generate_Comprehensive_Report (规则驱动报告)                              │
│     │  输出: data/comprehensive_analysis_report.md                          │
│     │                                                                       │
│  ④ Process_Truth_System  ← 【T.R.U.T.H. 核心处理】                          │
│     │  - 8个DataFrame → ProbeOutputs → GenomeInput                          │
│     │  - compute_genome_from_probes() 专业基因计算                           │
│     │  - 三大求解器执行                                                      │
│     │  输出: Truth_Processed_Results                                        │
│     │                                                                       │
│     ▼                                                                       │
│  ⑤ Generate_Truth_Report                                                    │
│     │  输出: data/truth_analysis_report.md                                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🧬 核心理念：信号熔炉

T.R.U.T.H. 系统将公司财务数据通过六维基因提炼为一个**动态阈值**，用于判断公司是否值得投资。

### 上帝方程

```
T = R_f + k₁β - k₂α - k₃(γ × E × V) + k₄δ_fraud + k₅δ_decay
```

**物理含义**：
- `T` = 动态阈值（公司需要达到的最低ROIC）
- `R_f` = 无风险利率基准（约3%）
- `β` = 重资产惩罚（钢铁、航空需要更高回报）
- `α` = 周期性惩罚（周期股在底部应该更宽容）
- `γ × E × V` = 真成长奖励（高质量成长降低门槛）
- `δ_fraud` = 欺诈熵惩罚（财务异常提高门槛）
- `δ_decay` = 衰退熵惩罚（基本面恶化提高门槛）

---

## 🔬 六维基因详解

### 1. α 周期性基因 (Cyclicality Gene)

**功能**：区分真周期股与趋势性高波动股

**v2.0 熔炼方程**：

```python
α = cv_normalized × cyclical_confidence × hurst_factor + 0.2×pt_score + 0.1×arch_score
```

**核心进化：Hurst指数门控**

```python
hurst_factor = 1 - |hurst - 0.5| × 2

# Hurst = 0.5 (随机游走) → factor = 1.0  ✓ 真周期
# Hurst = 0.3 (均值回归) → factor = 0.6  ✓ 强周期
# Hurst = 0.8 (趋势性)   → factor = 0.4  ✗ 成长股降权
```

**解决的问题**：
- ❌ v1.0：英伟达等高成长股因波动大被误判为周期股
- ✅ v2.0：Hurst>0.5的趋势性波动被降权，只有真正的均值回归才是周期

**输入字段**：
| 字段 | 来源 | 说明 |
|------|------|------|
| `detrended_cv` | volatility_probe | 去趋势后变异系数 |
| `cyclical_confidence` | cyclical_probe | 周期置信度 |
| `peak_to_trough_ratio` | volatility_probe | 峰谷比 |
| `arch_effect_strength` | arch_probe | ARCH效应强度 |
| `hurst_exponent` | cyclical_probe | Hurst指数 (v2.0新增) |

---

### 2. β 资本密度基因 (Capital Intensity Gene)

**功能**：检测重资产公司和"隐性重资产"公司

**v2.0 熔炼方程**：

```python
β = 0.6 × β_static + 0.4 × Norm(DOL × CV_amplification)
```

**核心进化：DOL经营杠杆检测隐性重资产**

```python
DOL = profit_log_slope / revenue_log_slope  # 经营杠杆系数
CV_amplification = profit_cv / revenue_cv    # 波动放大率
implied_capital_intensity = DOL × CV_amplification

# 警报触发：implied_capital_intensity > 2.0 → 隐性重资产！
```

**物理含义**：
- DOL > 1.5：营收增10%，利润增15%以上 → 高固定成本
- CV放大 > 1：利润波动 > 营收波动 → 重资产特征
- 两者乘积 > 2：看似轻资产，实际经营风险高

**解决的问题**：
- ❌ v1.0：没有直接的固定资产/总资产数据
- ✅ v2.0：通过DOL间接检测隐性重资产

**输入字段**：
| 字段 | 来源 | 说明 |
|------|------|------|
| `roic_detrended_cv` | roic.volatility_probe | ROIC去趋势CV |
| `ocf_cv` | ocf.volatility_probe | 现金流CV |
| `roic_log_slope` | roic.log_trend_probe | ROIC对数斜率 |
| `revenue_log_slope` | revenue.log_trend_probe | 营收对数斜率 |
| `profit_cv` | profit.volatility_probe | 利润CV (v2.0新增) |
| `revenue_cv` | revenue.volatility_probe | 营收CV (v2.0新增) |
| `profit_log_slope` | profit.log_trend_probe | 利润对数斜率 (v2.0新增) |

---

### 3. γ 成长动能基因 (Growth Momentum Gene)

**功能**：量化公司成长性，抗噪且支持断点重置

**v2.0 熔炼方程**：

```python
γ = Norm(slope) × (1 + 0.5×max(0,accel)) × √R²
```

**核心进化：稳健斜率 + 断点重置 + R²惩罚**

```python
# 斜率优先级
if has_structural_break and break_confidence > 0.7:
    slope = post_break_slope  # 断点后斜率（最新趋势）
elif robust_slope is not None:
    slope = robust_slope      # 稳健斜率（抗异常值）
else:
    slope = weighted_cagr     # 回退到CAGR

# R²惩罚：低R²=噪声大=不可信
gamma = gamma_base × accel_factor × √R²
```

**解决的问题**：
- ❌ v1.0：容易被单年非经常性损益骗过
- ❌ v1.0：转型公司（如苹果2007）历史斜率无意义
- ✅ v2.0：稳健斜率抗异常值，断点后斜率捕捉转型

**输入字段**：
| 字段 | 来源 | 说明 |
|------|------|------|
| `revenue_cagr` | revenue.log_trend_probe | 营收CAGR |
| `profit_cagr` | profit.log_trend_probe | 利润CAGR |
| `acceleration` | trend_probe | 成长加速度 |
| `r_squared` | log_trend_probe | 拟合R² (v2.0新增) |
| `robust_slope` | robust_trend_probe | 稳健斜率 (v2.0新增) |
| `has_structural_break` | breakpoint_probe | 是否有断点 (v2.0新增) |
| `break_confidence` | breakpoint_probe | 断点置信度 (v2.0新增) |
| `post_break_slope` | breakpoint_probe | 断点后斜率 (v2.0新增) |

---

### 4. δ_fraud 欺诈熵基因 (Fraud Entropy Gene)

**功能**：检测财务造假特征，触发熔断机制

**v2.0 熔炼方程**：

```python
δ_fraud = max(传统财务熵, 麦道夫异常分数)
```

**核心进化：麦道夫特征检测 (Too Smooth = 造假)**

```python
# 麦道夫特征：太完美反而是造假信号
too_smooth_margin = 1.0 if margin_cv < 0.01 else 0.0    # 毛利率太稳定
too_perfect_revenue = 1.0 if revenue_r² > 0.99 else 0.0  # 营收增长太线性
cash_manipulation = 1.0 if ocf_has_arch_effect else 0.0  # 现金流有ARCH效应

madoff_score = 0.5×too_smooth + 0.35×too_perfect + 0.15×cash_manip

# 熔断触发
if madoff_score >= 0.67:
    circuit_break = True  # 强制熔断
```

**物理含义**：
- 真实企业的毛利率会波动（原材料、竞争、季节性）
- 如果毛利率CV < 1%，几乎不可能是真实的
- 营收R² > 0.99 = 增长太完美 = 人工调节痕迹

**解决的问题**：
- ❌ v1.0：只看OCF/利润背离，容易漏判
- ✅ v2.0：麦道夫式"太完美"造假一眼识别

**输入字段**：
| 字段 | 来源 | 说明 |
|------|------|------|
| `ocf_profit_divergence` | comparison_probe | OCF与利润背离度 |
| `receivable_growth_ratio` | trend_probe | 应收增速/营收增速 |
| `inventory_growth_ratio` | trend_probe | 存货增速/营收增速 |
| `ocf_volatility_type` | volatility_probe | 现金流波动类型 |
| `margin_cv` | margin_probe | 毛利率CV (v2.0新增) |
| `revenue_r_squared` | log_trend_probe | 营收R² (v2.0新增) |
| `ocf_has_arch_effect` | arch_probe | OCF是否有ARCH (v2.0新增) |

---

### 5. δ_decay 衰退熵基因 (Decay Entropy Gene)

**功能**：检测基本面恶化，提供逃顶能力

**v2.0 熔炼方程**：

```python
δ_decay = 0.35×P_det + 0.20×years + 0.20×Norm(-slope_recent) + 0.15×Trigger + 0.10×pattern
```

**核心进化：拐点预警系统**

```python
# 拐点触发器
Trigger = I(inflection_type == "NegativeReversal") × confidence

# 硬触发规则：逃顶能力
if inflection_type == "NegativeReversal" and confidence > 0.8:
    δ_decay = max(δ_decay, 0.7)  # 强制提高阈值
```

**物理含义**：
- `NegativeReversal` = 顶部反转信号
- 一旦检测到高置信度顶部反转，立即提高门槛
- 具备**逃顶能力**，不再"跌了3年才发现衰退"

**解决的问题**：
- ❌ v1.0：只看历史数据，滞后3年才发现问题
- ✅ v2.0：Inflection探针实现拐点预警

**输入字段**：
| 字段 | 来源 | 说明 |
|------|------|------|
| `deterioration_probability` | deterioration_probe | 恶化概率 |
| `consecutive_decline_years` | deterioration_probe | 连续下跌年数 |
| `deterioration_pattern` | deterioration_probe | 恶化模式 |
| `volatility_regime` | volatility_probe | 波动率体制 |
| `inflection_type` | inflection_probe | 拐点类型 (v2.0新增) |
| `inflection_confidence` | inflection_probe | 拐点置信度 (v2.0新增) |
| `recent_3y_slope` | trend_probe | 近3年斜率 (v2.0新增) |

---

### 6. V 真相验证基因 (Verification Gene)

**功能**：验证成长的"真实性"，防止假成长

**v2.0 熔炼方程**：

```python
V = V_raw × V_quality × (1 + Bonus_prepay)
```

**核心进化：体制惩罚 + 预收奖励**

```python
# V_raw: 基础现金流匹配
V_raw = min(1.0, OCF_cagr / Revenue_cagr)

# V_quality: 体制质量乘数
V_quality = {
    'ultra_stable': 1.3,    # 极稳现金流：奖励
    'stable': 1.2,          # 稳定：奖励
    'moderate': 1.0,        # 中性
    'volatile': 0.8,        # 波动：体制惩罚
    'highly_volatile': 0.6  # 剧烈波动：重度惩罚
}

# Bonus_prepay: 预收款奖励（强势地位信号）
Bonus_prepay = min(预收/营收 × 系数, 0.25)
```

**物理含义**：
- V 高 → 利润有现金流支撑 = 真成长
- V 低 → 利润与现金流背离 = 假成长
- 体制惩罚：极端波动的现金流不可信
- 预收奖励：高预收款 = 下游强势（茅台、格力）

**输入字段**：
| 字段 | 来源 | 说明 |
|------|------|------|
| `ocf_cagr` | ocf.log_trend_probe | 现金流CAGR |
| `revenue_cagr` | revenue.log_trend_probe | 营收CAGR |
| `ocf_volatility_type` | volatility_probe | 现金流波动类型 |
| `advance_receipts` | 财务数据 | 预收账款 |
| `latest_revenue` | 财务数据 | 最新营收 |

---

## 📊 三层过滤哲学

T.R.U.T.H. v2.0 遵循"信号熔炉"三层过滤：

### Layer 1: 去噪 (Denoising)
- 稳健斜率替代普通斜率
- Hurst指数区分真周期vs趋势波动
- R²惩罚低可信度拟合

### Layer 2: 校准 (Calibration)
- 断点重置捕捉转型公司
- DOL检测隐性重资产
- 拐点预警实现逃顶

### Layer 3: 增强 (Enhancement)
- 麦道夫特征检测"太完美"造假
- 体制惩罚/奖励动态调整
- 硬触发规则防止漏判

---

## 🚀 使用方式

### 方式一：通过 Workflow（推荐）

在 `workflow/analysis.yaml` 中已配置好完整的处理流程：

```bash
# 运行完整分析管道
python -m pipeline.main workflow/analysis.yaml
```

### 方式二：直接调用 TruthProcessor

```python
import pandas as pd
from src.astock.business_engines.truth.processor import TruthProcessor

# 初始化处理器
processor = TruthProcessor()

# 准备探针数据（从 Pipeline 分析结果）
probe_data = {
    'roic': pd.read_csv('data/filter_middle/roic_trend_analysis.csv'),
    'roe': pd.read_csv('data/filter_middle/roe_trend_analysis.csv'),
    'gross_margin': pd.read_csv('data/filter_middle/gross_margin_trend_analysis.csv'),
    'revenue': pd.read_csv('data/filter_middle/revenue_trend_analysis.csv'),
    'profit': pd.read_csv('data/filter_middle/profit_trend_analysis.csv'),
    'ocf': pd.read_csv('data/filter_middle/ocf_trend_analysis.csv'),
}

# 处理单个公司
ts_code = "600519.SH"  # 贵州茅台
result = processor.process_company(ts_code, probe_data, "贵州茅台")

# 获取处理结果
print(f"ts_code: {result.ts_code}")
print(f"六维基因:")
g = result.genome
print(f"  α (周期性): {g.alpha:.4f}")
print(f"  β (资本密度): {g.beta:.4f}")
print(f"  γ (成长动能): {g.gamma:.4f}")
print(f"  δ_fraud (欺诈熵): {g.delta_fraud:.4f}")
print(f"  δ_decay (衰退熵): {g.delta_decay:.4f}")
print(f"  V (真相验证): {g.verification:.4f}")
print(f"最终评分: {result.final_score:.2f}")
print(f"信号: {result.signal}")
print(f"评级: {result.grade}")
print(f"处理流程: {result.processing_notes}")
```

### 方式三：批量处理

```python
# 批量处理所有公司
batch_result = processor.process_batch(probe_data)

# 获取结果 DataFrame
df_results = processor.get_results_dataframe(batch_result)
print(f"处理了 {len(df_results)} 家公司")
print(df_results[['ts_code', 'alpha', 'gamma', 'final_score', 'signal', 'grade']].head())
```

---

## 📁 文件结构

```
truth/
├── __init__.py              # 模块导出
├── README.md                # 本文档
├── GENE_EVOLUTION_ANALYSIS.md  # 基因进化分析报告
│
├── adapter.py               # ProbeAdapter - 探针适配器
│                            # ProbeOutputs → GenomeInput
│
├── processor.py             # TruthProcessor - 核心处理器 ⭐
│                            # 包含 DataFrameToProbeConverter (桥接层)
│                            # DataFrame → ProbeOutputs → GenomeInput → CompanyGenome
│
├── truth_engine.py          # process_truth() - Pipeline注册入口
│                            # @register_method 注册到 orchestrator
│
├── config.py                # TruthConfig - 配置参数
├── models.py                # CompanyGenome, TruthResult 等数据模型
├── calibrator.py            # 阈值校准器
├── clusterer.py             # 聚类分析
├── visualizer.py            # 可视化
│
└── core/
    ├── genes/               # 六维基因实现 ⭐
    │   ├── __init__.py      # 模块导出 + compute_genome_from_probes
    │   ├── genome_assembler.py  # 基因组装器（调用下面6个函数）
    │   ├── alpha.py         # α 周期性基因 - compute_alpha_from_probes()
    │   ├── beta.py          # β 资本密度基因 - compute_beta_from_probes()
    │   ├── gamma.py         # γ 成长动能基因 - compute_gamma_from_probes()
    │   ├── delta_fraud.py   # δ_fraud 欺诈熵基因 - compute_delta_fraud_from_probes()
    │   ├── delta_decay.py   # δ_decay 衰退熵基因 - compute_delta_decay_from_probes()
    │   └── verification.py  # V 真相验证基因 - compute_verification_from_probes()
    │
    └── solvers/             # 三大求解器 ⭐
        ├── __init__.py      # 模块导出
        ├── gravity_solver.py     # 重力求解器 - 计算动态ROIC阈值
        ├── velocity_solver.py    # 速度求解器 - 计算增长边界
        └── structure_solver.py   # 结构求解器 - 斜率预测
```

---

## 🔗 基因-指标映射关系

### 专业映射表

| 基因 | 核心指标 | 聚合策略 | 计算逻辑 |
|------|----------|----------|----------|
| α (周期性) | ROIC, ROE | max | 选择周期性最强的指标 |
| β (资本密度) | ROIC, OCF | 加权 | ROIC波动 + OCF波动 + DOL |
| γ (成长动能) | 营收, 利润, OCF | 调和平均 | 惩罚不平衡增长 |
| δ_fraud (欺诈熵) | 利润, OCF | 逻辑OR | 任一异常即触发 |
| δ_decay (衰退熵) | 所有效率指标 | max | 最严重的衰退信号 |
| V (验证因子) | OCF | 单一 | 现金流验证利润真实性 |

### 求解器输入输出

| 求解器 | 核心输入 | 输出 | 用途 |
|--------|----------|------|------|
| gravity_solver | α, β | final_threshold | 动态ROIC阈值 |
| velocity_solver | γ, V | max_sustainable_growth | 增长边界评估 |
| structure_solver | genome | expected_slope | 斜率预测（护城河检测）|

### 因果网络验证

```
    营收增长 ──────→ 利润增长 ──────→ 现金流增长
        │               │
        ▼               ▼
       ROE ←────────── ROIC
        │
        ▼
    周期性波动 → α
```

---

## 📝 版本历史

| 版本 | 日期 | 更新内容 |
|------|------|----------|
| v3.0 | 2025-01 | 架构重构：DataFrameToProbeConverter桥接层、专业函数调用链、三大求解器集成 |
| v2.0 | 2025-01 | 六维基因进化：Hurst门控、DOL检测、断点重置、麦道夫特征、拐点预警、体制惩罚 |
| v1.0 | 2024-12 | 初始版本：六维基因框架建立 |

---

*T.R.U.T.H. - 让投资决策透明化*
