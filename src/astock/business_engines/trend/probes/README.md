# 趋势分析探针系统 (Probes)

> **核心理念**：将任意财务指标（ROIC、ROE、毛利率、营收等）通过标准化的探针处理，提取出**趋势特征**、**波动特征**、**周期特征**、**拐点特征**等多维度信息，为后续的投资决策提供量化基础。

---

## 📊 系统概览

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        原始财务指标时间序列                               │
│          [2019, 2020, 2021, 2022, 2023] 或 [2014, ..., 2023]            │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          Probes 探针处理层                               │
│  ┌──────────────┬──────────────┬──────────────┬──────────────┐         │
│  │ LogTrend     │ Volatility   │ Cyclical     │ Deterioration│         │
│  │ 对数趋势探针  │ 波动率探针   │ 周期性探针    │ 恶化检测探针  │         │
│  └──────────────┴──────────────┴──────────────┴──────────────┘         │
│  ┌──────────────┬──────────────┬──────────────┬──────────────┐         │
│  │ Inflection   │ Robust       │ Rolling      │ MultiHorizon │         │
│  │ 拐点检测探针  │ 稳健趋势探针  │ 滚动趋势探针  │ 多窗口分析   │         │
│  └──────────────┴──────────────┴──────────────┴──────────────┘         │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           探针输出结果                                   │
│  • 斜率/CAGR/R²     • CV/去趋势CV    • 周期性置信度   • 恶化概率         │
│  • Mann-Kendall τ   • ARCH效应       • 周期位置       • 连续下跌年数     │
│  • 拐点位置/类型    • 波动体制       • 断点检测       • 加速度           │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🔬 探针详解

### 1. LogTrendProbe（对数趋势探针）

**文件**: `log_trend_probe.py`

**功能**: 计算时间序列的**对数趋势**，提取年化增长率和趋势强度。

#### 核心算法

1. **自适应变换**：
   - 正值序列 → `log(x)` 对数变换
   - 含负值序列 → `arcsinh(x)` 双曲正弦变换（保持单调性，处理负值）

2. **多方法融合**：
   - **OLS** (普通最小二乘)：基础斜率估计
   - **WLS** (加权最小二乘)：近期数据权重更大（指数衰减）
   - **Bootstrap** 置信区间：小样本可靠性估计

#### 输出参数

| 参数名 | 类型 | 含义 | 业务解读 |
|--------|------|------|----------|
| `log_slope` | float | 对数斜率 | 趋势的陡峭程度，>0 上升，<0 下降 |
| `cagr_approx` | float | 复合年增长率 | `exp(log_slope) - 1`，如 0.08 = 8%年增长 |
| `r_squared` | float | 决定系数 | 趋势的可靠性，>0.7 趋势明确，<0.3 无明显趋势 |
| `p_value` | float | 显著性 | <0.05 趋势统计显著 |
| `std_err` | float | 标准误差 | 斜率估计的不确定性 |
| `crosses_zero` | bool | 是否穿越零点 | True = 有亏损年份 |
| `wls_slope` | float | 加权斜率 | 强调近期趋势（WLS） |
| `slope_ci_lower/upper` | float | 斜率置信区间 | Bootstrap 95% CI |

#### 使用示例

```python
from .probes import LogTrendProbe

roic_series = [0.15, 0.16, 0.18, 0.17, 0.19]  # 5年ROIC
probe = LogTrendProbe()
result = probe.compute(roic_series)

print(f"CAGR: {result.cagr_approx:.1%}")  # 如 "CAGR: 6.2%"
print(f"趋势强度: {result.r_squared:.2f}")  # 如 "趋势强度: 0.85"
```

---

### 2. VolatilityProbe（波动率探针）

**文件**: `volatility_probe.py`

**功能**: 评估指标的**稳定性**和**波动特征**。

#### 核心算法

1. **变异系数 (CV)**: `std / |mean|`，标准化的波动度量
2. **去趋势CV**: 去除线性趋势后的残差波动
3. **ARCH效应检测**: 波动是否具有时间聚集性
4. **波动体制识别**: 前后期波动率变化

#### 输出参数

| 参数名 | 类型 | 含义 | 业务解读 |
|--------|------|------|----------|
| `cv` | float | 变异系数 | <0.15 超稳定，0.15-0.30 稳定，>0.50 高波动 |
| `detrended_cv` | float | 去趋势CV | 排除趋势影响后的纯波动 |
| `std_dev` | float | 标准差 | 绝对波动幅度 |
| `volatility_type` | str | 波动分类 | `ultra_stable`, `stable`, `moderate`, `volatile` |
| `has_arch_effect` | bool | ARCH效应 | True = 波动聚集（大波动后跟大波动） |
| `arch_correlation` | float | ARCH相关系数 | >0.4 表示显著聚集效应 |
| `volatility_regime` | str | 波动体制 | `stable`, `increasing_vol`, `decreasing_vol` |
| `volatility_change_ratio` | float | 波动变化比 | 后期/前期波动率，>1.5 波动加剧 |

#### 稳定性分级标准

```
┌─────────────┬────────────┬─────────────────────────────────┐
│  CV 范围    │   分类      │   典型公司类型                   │
├─────────────┼────────────┼─────────────────────────────────┤
│  < 0.12     │ ultra_stable│ 茅台、海天、公用事业              │
│  0.12-0.20  │ stable      │ 消费龙头、医药白马                │
│  0.20-0.35  │ moderate    │ 一般制造业、零售                  │
│  0.35-0.55  │ volatile    │ 科技成长、周期行业                │
│  > 0.55     │ extreme     │ 强周期（钢铁、航运、证券）         │
└─────────────┴────────────┴─────────────────────────────────┘
```

---

### 3. CyclicalProbe（周期性探针）

**文件**: `cyclical_probe.py`

**功能**: 检测指标是否具有**商业周期特征**，并判断当前所处的周期位置。

#### 核心算法

1. **Hodrick-Prescott滤波**: 分离趋势与周期成分
2. **自相关分析 (ACF)**: 检测周期性自相关
3. **峰谷检测**: 识别历史高低点及间隔
4. **Hurst指数**: 区分真周期 vs 随机游走
5. **行业先验贝叶斯更新**: 结合行业特性调整判断

#### 输出参数

| 参数名 | 类型 | 含义 | 业务解读 |
|--------|------|------|----------|
| `is_cyclical` | bool | 是否周期性 | True = 判定为周期性公司 |
| `cyclical_confidence` | float | 周期置信度 | 0-1，>0.6 可信，<0.4 不确定 |
| `cycle_position` | str | 周期位置 | `bottom`, `mid_up`, `top`, `mid_down`, `unknown` |
| `current_phase` | str | 当前阶段 | `expansion`, `peak`, `contraction`, `trough` |
| `peak_to_trough_ratio` | float | 峰谷比 | 最高值/最低值，>3 强周期 |
| `fft_dominant_period` | float | 主导周期(年) | FFT检测的周期长度 |
| `hurst_exponent` | float | Hurst指数 | <0.5 均值回复，>0.5 趋势持续 |
| `industry_cyclical` | bool | 行业是否周期 | 基于GICS行业分类的先验 |

#### 周期位置判断逻辑

```
                   ★ TOP (顶部)
                  /           \
                 /             \
     MID_UP    /               \    MID_DOWN
   (上升中期) /                 \  (下降中期)
             /                   \
            /                     \
           ★ BOTTOM ─────────────★ BOTTOM
              (底部)              (底部)
```

**投资意义**：
- `bottom` (底部): 周期股最佳买点，但需确认拐点
- `mid_up` (上升中期): 持有，警惕见顶信号
- `top` (顶部): 考虑减仓，回避追高
- `mid_down` (下降中期): 观望，等待底部确认

---

### 4. DeteriorationProbe（恶化检测探针）

**文件**: `deterioration_probe.py`

**功能**: 检测指标的**近期恶化趋势**，识别基本面拐点风险。

#### 核心算法

1. **连续下跌统计**: 统计连续下降年数
2. **恶化加速度**: 下跌是否在加速
3. **贝叶斯概率计算**: 综合多信号量化恶化置信度
4. **高位回调豁免**: 区分正常回调与真实恶化

#### 输出参数

| 参数名 | 类型 | 含义 | 业务解读 |
|--------|------|------|----------|
| `has_deterioration` | bool | 是否恶化 | 规则判断结果 |
| `severity` | str | 严重程度 | `none`, `mild`, `moderate`, `severe` |
| `consecutive_decline_years` | int | 连续下跌年数 | ≥3 年需警惕 |
| `deterioration_acceleration` | float | 恶化加速度 | >0 加速恶化，<0 恶化放缓 |
| `deterioration_probability` | float | 恶化概率 | 贝叶斯后验，>0.7 高风险 |
| `deterioration_pattern` | str | 恶化模式 | 见下表 |
| `year4_to_5_pct` | float | 最近一年变化% | 最新年vs前一年 |
| `total_decline_pct` | float | 累计下跌% | 从最高点的累计跌幅 |
| `is_high_level_stable` | bool | 高位稳定 | True = 虽有下跌但仍在高位 |

#### 恶化模式分类

```
┌──────────────────┬───────────────────────────────────────┐
│  模式            │  特征描述                              │
├──────────────────┼───────────────────────────────────────┤
│  none            │  无恶化迹象                            │
│  mild_decline    │  轻微下滑，可能是正常波动               │
│  accelerating    │  加速恶化，越跌越快 ⚠️                  │
│  sustained       │  持续恶化，连续多年下跌 ⚠️              │
│  cliff_fall      │  断崖式下跌，单年暴跌 🔴                │
└──────────────────┴───────────────────────────────────────┘
```

---

### 5. InflectionProbe（拐点检测探针）

**文件**: `inflection_probe.py`

**功能**: 检测趋势的**结构性变化点**（从上升转为下降，或反之）。

#### 核心算法

1. **分段线性回归**: 寻找最优分段点
2. **CUSUM变点检测**: 累积和检验均值偏移
3. **EWMA趋势偏移**: 检测渐进式趋势转变
4. **F检验显著性**: 验证拐点统计显著性

#### 输出参数

| 参数名 | 类型 | 含义 | 业务解读 |
|--------|------|------|----------|
| `has_inflection` | bool | 是否有拐点 | True = 检测到结构性变化 |
| `inflection_type` | str | 拐点类型 | 见下表 |
| `early_slope` | float | 前期斜率 | 拐点前的趋势 |
| `recent_slope` | float | 后期斜率 | 拐点后的趋势 |
| `slope_change` | float | 斜率变化 | recent - early |
| `confidence` | float | 置信度 | 0-1，>0.5 可信 |
| `cusum_change_point` | int | CUSUM变点位置 | 年份索引 |

#### 拐点类型

```
┌────────────────────┬────────────────────────────────────┐
│  类型              │  含义                               │
├────────────────────┼────────────────────────────────────┤
│  positive_reversal │  由跌转涨 ✅ 利好信号               │
│  negative_reversal │  由涨转跌 ⚠️ 警惕信号               │
│  acceleration      │  上涨加速                           │
│  deceleration      │  上涨减速（可能见顶）                │
│  no_clear_pattern  │  无明显模式                         │
└────────────────────┴────────────────────────────────────┘
```

---

### 6. RobustTrendProbe（稳健趋势探针）

**文件**: `robust_probe.py`

**功能**: 使用**非参数统计方法**评估趋势，对异常值具有抵抗能力。

#### 核心算法

1. **Theil-Sen估算器**: 基于中位数斜率，breakdown point = 29.3%
2. **Mann-Kendall检验**: 非参数单调趋势检验

#### 输出参数

| 参数名 | 类型 | 含义 | 业务解读 |
|--------|------|------|----------|
| `robust_slope` | float | 稳健斜率 | Theil-Sen估计，抗异常值 |
| `robust_slope_ci_low/high` | float | 置信区间 | 非参数95% CI |
| `mann_kendall_tau` | float | MK τ系数 | [-1,1]，方向和强度 |
| `mann_kendall_p_value` | float | MK显著性 | <0.05 趋势显著 |

#### OLS vs Theil-Sen 对比

```
场景：ROIC序列 [15%, 16%, 8%, 17%, 18%]（第3年是异常值）

OLS斜率: 0.2%（被异常值拉低）
Theil-Sen斜率: 1.5%（忽略异常值，反映真实趋势）

结论：当两者差异大时，应以 Theil-Sen 为准
```

---

### 7. RollingProbe（滚动趋势探针）

**文件**: `rolling_probe.py`

**功能**: 计算**多窗口趋势**，检测趋势加速/减速。

#### 输出参数

| 参数名 | 类型 | 含义 | 业务解读 |
|--------|------|------|----------|
| `recent_3y_slope` | float | 近3年斜率 | 最新趋势方向 |
| `early_3y_slope` | float | 前3年斜率 | 历史趋势方向 |
| `full_5y_slope` | float | 全5年斜率 | 整体趋势 |
| `trend_acceleration` | float | 趋势加速度 | recent - early |
| `acceleration_confidence` | float | 加速度置信度 | min(R²_recent, R²_early) |
| `is_accelerating` | bool | 是否加速上涨 | 趋势在加强 |
| `is_decelerating` | bool | 是否减速/逆转 | 趋势在减弱 |

#### 趋势动量信号

```
┌─────────────────────┬─────────────────────────────────────┐
│  信号               │  含义                                │
├─────────────────────┼─────────────────────────────────────┤
│  加速上涨           │  recent > early > 0，势头强劲        │
│  减速上涨           │  recent < early，均>0，动能衰减       │
│  加速下跌           │  recent < early < 0，恶化加剧 ⚠️     │
│  减速下跌           │  recent > early，均<0，跌势放缓 ✅    │
└─────────────────────┴─────────────────────────────────────┘
```

---

### 8. MultiHorizonProbe（多时间窗口探针）

**文件**: `multi_horizon_probe.py`

**功能**: 解决**10年vs5年数据选择**问题，智能整合多时间窗口信息。

#### 核心思想

```
┌────────────────────────────────────────────────────────────┐
│  问题：                                                    │
│  • 5年太短：无法可靠检测3-7年商业周期                        │
│  • 10年太长：公司可能已发生结构性变化，稀释近期信号           │
│                                                            │
│  解决方案：                                                 │
│  • 近5年窗口：计算趋势、增长率（反映当前状态）               │
│  • 全10年窗口：周期性检测、结构断点（提供长期视角）          │
│  • 智能权重：根据是否有断点动态调整                          │
└────────────────────────────────────────────────────────────┘
```

#### 输出参数

| 参数名 | 类型 | 含义 |
|--------|------|------|
| `recent_analysis` | HorizonAnalysis | 近5年分析结果 |
| `extended_analysis` | HorizonAnalysis | 全10年分析结果 |
| `effective_analysis` | HorizonAnalysis | 有效窗口（断点后） |
| `structural_break` | StructuralBreakResult | 断点检测结果 |
| `effective_slope` | float | 综合加权斜率 |
| `effective_cagr` | float | 综合加权CAGR |
| `data_regime` | str | 数据体制 |
| `recent_weight` | float | 近期权重 (0-1) |

---

### 9. StructuralBreakDetector（结构断点检测器）

**文件**: `multi_horizon_probe.py`

**功能**: 识别公司**本质变化点**（如并购、战略转型、行业变革）。

#### 输出参数

| 参数名 | 类型 | 含义 |
|--------|------|------|
| `has_break` | bool | 是否存在显著断点 |
| `break_type` | BreakType | 断点类型 |
| `break_year_index` | int | 断点位置（年份索引） |
| `break_significance` | float | 断点显著性 (F统计量) |
| `p_value` | float | 统计显著性 |
| `recommended_window_start` | int | 推荐数据起始年 |
| `pre_break_stats` | dict | 断点前统计特征 |
| `post_break_stats` | dict | 断点后统计特征 |

#### 断点类型

```
┌────────────────────┬────────────────────────────────────────┐
│  类型              │  含义                                   │
├────────────────────┼────────────────────────────────────────┤
│  NONE              │  无断点，数据连续                        │
│  LEVEL_SHIFT       │  水平位移（均值突变）                    │
│  TREND_CHANGE      │  趋势变化（斜率突变）                    │
│  VOLATILITY_CHANGE │  波动率变化                              │
│  REGIME_SWITCH     │  全面体制转换（多指标同时变化）          │
└────────────────────┴────────────────────────────────────────┘
```

---

### 10. （已合并）ProfessionalDataWindowStrategy → MultiHorizonProbe

v2.0 起，原 `ProfessionalDataWindowStrategy` 已完全合并进 `MultiHorizonProbe`，
统一由 MultiHorizonProbe + MetricCategory + WindowStrategy 实现“专业数据窗口策略”。

#### 指标分类策略（由 MultiHorizonProbe 实现）

```
┌────────────────┬──────────────────┬──────────────────────────────┐
│  指标类型      │  主窗口          │  扩展窗口用途                 │
├────────────────┼──────────────────┼──────────────────────────────┤
│  ROE/ROIC      │  近5年(75%)      │  周期位置判断、长期中枢       │
│  净利率/毛利率 │  近5年(75%)      │  周期性检测、结构断点         │
│  营收/利润     │  近5年(70%)      │  周期性检测、增长持续性       │
│  资产周转率    │  近5年(65%)      │  长期趋势、行业周期           │
│  负债率        │  近5年(80%)      │  长期风险演变                 │
└────────────────┴──────────────────┴──────────────────────────────┘
```

---

## 🔄 数据流与输出汇总

### 完整处理流程

```
原始指标序列
    │
    ├──► LogTrendCalculator ──► log_slope, cagr, r_squared
    │
    ├──► VolatilityCalculator ──► cv, detrended_cv, volatility_type
    │
    ├──► CyclicalPatternDetector ──► is_cyclical, cycle_position, cyclical_confidence
    │
    ├──► DeteriorationDetector ──► deterioration_probability, consecutive_years
    │
    ├──► InflectionDetector ──► has_inflection, inflection_type
    │
    ├──► RobustTrendProbe ──► robust_slope, mann_kendall_tau
    │
    ├──► RollingTrendCalculator ──► trend_acceleration, is_accelerating
    │
    └──► MultiHorizonAnalyzer ──► effective_slope, structural_break
```

### 核心输出参数总表

| 维度 | 参数 | 来源探针 | 用途 |
|------|------|----------|------|
| **趋势** | `log_slope` | LogTrend | 趋势方向和强度 |
| | `cagr_approx` | LogTrend | 年化增长率 |
| | `r_squared` | LogTrend | 趋势可靠性 |
| | `robust_slope` | Robust | 抗异常值斜率 |
| **波动** | `cv` | Volatility | 波动程度 |
| | `detrended_cv` | Volatility | 去趋势波动 |
| | `volatility_regime` | Volatility | 波动体制变化 |
| **周期** | `is_cyclical` | Cyclical | 周期性判断 |
| | `cycle_position` | Cyclical | 当前周期位置 |
| | `cyclical_confidence` | Cyclical | 周期性置信度 |
| **恶化** | `deterioration_probability` | Deterioration | 恶化概率 |
| | `consecutive_decline_years` | Deterioration | 连续下跌年数 |
| | `deterioration_pattern` | Deterioration | 恶化模式 |
| **拐点** | `has_inflection` | Inflection | 是否有拐点 |
| | `inflection_type` | Inflection | 拐点类型 |
| **动量** | `trend_acceleration` | Rolling | 趋势加速度 |
| | `is_accelerating` | Rolling | 是否加速 |
| **断点** | `has_break` | MultiHorizon | 是否有断点 |
| | `break_type` | MultiHorizon | 断点类型 |

---

## 📈 与 T.R.U.T.H. 系统集成

Probes 探针的输出直接用于 T.R.U.T.H. 系统的**六维基因测序**：

```
┌─────────────────────────────────────────────────────────────────┐
│                    Probes → 六维基因映射                         │
├─────────────────────────────────────────────────────────────────┤
│  α (周期性基因)   ← volatility_probe + cyclical_probe           │
│                    detrended_cv, peak_to_trough_ratio           │
│                                                                  │
│  β (资本密度基因) ← volatility_probe                             │
│                    roic波动率, 毛利率稳定性                       │
│                                                                  │
│  γ (成长动能基因) ← log_trend_probe + rolling_probe              │
│                    cagr, trend_acceleration                      │
│                                                                  │
│  δ_fraud (欺诈熵) ← 财务异常检测（独立模块）                      │
│                                                                  │
│  δ_decay (衰退熵) ← deterioration_probe                          │
│                    deterioration_probability, pattern            │
│                                                                  │
│  V (验证因子)     ← OCF/营收增速比较                              │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ 使用示例

### 完整分析流程

```python
from .probes import (
    LogTrendProbe,
    VolatilityProbe,
    CyclicalProbe,
    DeteriorationProbe,
    InflectionProbe,
    RobustTrendProbe,
    RollingProbe,
)

# 示例数据：5年ROIC
roic = [0.15, 0.14, 0.12, 0.10, 0.08]

# 1. 趋势分析
log_result = LogTrendProbe().compute(roic)
print(f"CAGR: {log_result.cagr_approx:.1%}")
print(f"R²: {log_result.r_squared:.2f}")

# 2. 波动分析
vol_result = VolatilityProbe().compute(roic)
print(f"CV: {vol_result.cv:.2f}")
print(f"波动类型: {vol_result.volatility_type}")

# 3. 周期性分析
cyc_result = CyclicalProbe().compute(roic)
print(f"周期性: {cyc_result.is_cyclical}")
print(f"周期位置: {cyc_result.cycle_position}")

# 4. 恶化检测
det_result = DeteriorationProbe().compute(roic)
print(f"恶化概率: {det_result.deterioration_probability:.1%}")
print(f"恶化模式: {det_result.deterioration_pattern}")

# 5. 拐点检测
inf_result = InflectionProbe().compute(roic)
print(f"有拐点: {inf_result.has_inflection}")
print(f"拐点类型: {inf_result.inflection_type}")

# 6. 稳健趋势
rob_result = RobustTrendProbe().compute(roic)
print(f"稳健斜率: {rob_result.robust_slope:.4f}")
print(f"Mann-Kendall τ: {rob_result.mann_kendall_tau:.2f}")

# 7. 滚动趋势
roll_result = RollingProbe().compute(roic)
print(f"趋势加速度: {roll_result.trend_acceleration:.4f}")
print(f"加速中: {roll_result.is_accelerating}")
```

---

## 📚 学术参考

1. **Hodrick-Prescott Filter**: Hodrick & Prescott (1997), JMCB
2. **Theil-Sen Estimator**: Sen (1968), JASA
3. **Mann-Kendall Test**: Mann (1945), Econometrica
4. **CUSUM**: Page (1954), Biometrika
5. **Structural Break**: Bai & Perron (1998), Econometrica
6. **Hurst Exponent**: Hurst (1951), Transactions of ASCE

---

## 📝 版本历史

- **v3.0** (2025-12-25): 文档规范化
  - 统一类名为 `*Probe` 后缀
  - 统一方法名为 `compute()`
  - 修正使用示例代码
  - 与实际代码完全对齐

- **v2.0** (2025-12): 专业增强版
  - 新增多时间窗口分析
  - 新增结构断点检测
  - 贝叶斯恶化概率计算
  - ARCH效应检测
  - 行业差异化阈值

- **v1.0** (2025-01): 初始版本
  - 基础探针实现
