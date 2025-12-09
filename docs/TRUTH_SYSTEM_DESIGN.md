# T.R.U.T.H. 系统终极设计方案

> **Trend-Reality Unified Truth Hashing System**
>
> 版本: v2025.Ultimate
> 状态: 设计确认稿 ✅
> 作者: AStock Analysis Team
> 日期: 2025-12-09

---

## 📋 目录

1. [设计哲学](#一设计哲学)
2. [架构总览](#二架构总览)
3. [Layer 0: 时间衰减引擎](#三layer-0-时间衰减引擎)
4. [Layer 1: 六维基因测序](#四layer-1-六维基因测序)
5. [Layer 2: 物理求解器](#五layer-2-物理求解器)
6. [Layer 3: 双层自适应校准](#六layer-3-双层自适应校准)
7. [探针映射表](#七探针映射表)
8. [实战推演](#八实战推演)
9. [关键决策（已确认）](#九关键决策已确认)
10. [实现路线图](#十实现路线图)

---

## 一、设计哲学

### 1.1 核心理念：彻底去标签化

> **革命性变化**：T.R.U.T.H.系统彻底抛弃静态行业标签和预设阈值

| 传统方法 | T.R.U.T.H.方法 |
|---------|---------------|
| 公司 → 行业标签 → 查表获取阈值 | 公司 → 计算6维基因 → **动态生成阈值** |
| `if industry == "白酒": threshold = 15%` | 基因决定阈值：`T = f(α,β,γ,δ,V)` |
| 多元化公司无法处理 | **自动处理**：基因反映混合特征 |
| 需要维护行业配置表 | **零配置**：全自动计算 |

**精确含义**：
- ❌ 不再需要：`_ROIC_FILTER_CONFIGS["白酒"] = {"min_roic": 0.15}`
- ❌ 不再需要：判断公司属于哪个行业
- ❌ 不再需要：对多元化公司做特殊处理
- ✅ 保留行业作为**弱先验**（仅占基因计算的5%权重）

### 1.2 哲学原则

| 原则 | 传统做法 | T.R.U.T.H.做法 |
|-----|---------|---------------|
| **去标签化** | "这是周期股，用周期股标准" | "数据显示α=0.8，自动应用周期豁免" |
| **物理同构** | "ROIC要大于8%" | "ROIC必须克服资金重力+资本密度惩罚" |
| **双盲验证** | "营收增长30%是好公司" | "营收增长但V=0.2（OCF差）→假成长" |
| **时空加权** | 5年平均 | EWMA半衰期1.5年，近期权重更高 |

### 1.3 彻底解决的问题

```
❓ "茅台是白酒还是消费品？用哪个阈值？"
   → 不重要，基因会计算出茅台的特征：α=0.1, β=0.1, V=1.2

❓ "美的是家电还是机器人？"
   → 不重要，基因反映其混合特征：α=0.4, β=0.5, γ=0.6

❓ "比亚迪是汽车还是电池？"
   → 不重要，基因自动捕捉其周期成长混合属性
```

---

## 二、架构总览

```
┌─────────────────────────────────────────────────────────────────────┐
│                    T.R.U.T.H. v2025.Ultimate                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Layer 0: 时间衰减引擎 (Time Decay Engine)                    │   │
│  │ ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━│   │
│  │ • EWMA 半衰期 1.5年                                         │   │
│  │ • Bootstrap 500次重采样                                     │   │
│  │ • 指数衰减权重: [0.1, 0.15, 0.2, 0.25, 0.3]                 │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              ▼                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Layer 1: 六维基因测序器 (6D Gene Sequencer)                  │   │
│  │ ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━│   │
│  │                                                             │   │
│  │  ┌────────┐ ┌────────┐ ┌────────┐                          │   │
│  │  │ α 周期 │ │ β 轻重 │ │ γ 动能 │  ← 属性基因              │   │
│  │  └────────┘ └────────┘ └────────┘                          │   │
│  │  ┌────────┐ ┌────────┐ ┌────────┐                          │   │
│  │  │δ_fraud │ │δ_decay │ │ V 验证 │  ← 风险/验证基因          │   │
│  │  │ 熔断项 │ │ 惩罚项 │ │ 照妖镜 │                          │   │
│  │  └────────┘ └────────┘ └────────┘                          │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              ▼                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Layer 2: 三大物理求解器 (Physics Solvers)                    │   │
│  │ ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━│   │
│  │                                                             │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │   │
│  │  │ 🌍 重力求解器 │  │ 🚀 速度求解器 │  │ 🧬 结构求解器 │      │   │
│  │  │ ROIC/ROE     │  │ 营收/利润增速 │  │ 毛利率/周转率│      │   │
│  │  │ 对抗资金成本 │  │ 对抗GDP摩擦  │  │ 对抗熵增     │      │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘      │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              ▼                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Layer 3: 双层自适应校准 (Dual Calibration)                   │   │
│  │ ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━│   │
│  │                                                             │   │
│  │  T_final = T_theory + Δ_industry + Δ_size + Δ_confidence   │   │
│  │                                                             │   │
│  │  • Δ_industry: 行业残差修正（景气度调整）                    │   │
│  │  • Δ_size: 市值分层修正（大票-1.5%, 微盘+3%）               │   │
│  │  • Δ_confidence: 置信度折扣（5年数据max 55%）               │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              ▼                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                    📊 最终输出                               │   │
│  │ ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━│   │
│  │ • 公司基因图谱 (6D Genome)                                  │   │
│  │ • 动态阈值 (T_final per metric)                            │   │
│  │ • 通过/失败判定 + 置信度                                    │   │
│  │ • 风险预警 + 投资建议                                       │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 三、Layer 0: 时间衰减引擎

### 3.1 核心原理

**问题**：5年前的数据和今年的数据同等权重是否合理？

**答案**：不合理。2025年的财务数据应该比2020年的数据重要得多。

### 3.2 实现方案

我们的探针系统**已经内置**了时间衰减：

| 组件 | 实现位置 | 衰减方式 |
|-----|---------|---------|
| 加权平均 | `TrendAnalysisConfig.default_weights` | `[0.1, 0.15, 0.2, 0.25, 0.3]` |
| Log斜率 | `log_trend_probe.py` | WLS指数衰减权重 `decay=0.15` |
| 滚动趋势 | `rolling_probe.py` | 近3年单独计算 |
| 贝叶斯更新 | `deterioration_probe.py` | 连续恶化年数加权 |

### 3.3 配置建议

```python
TIME_DECAY_CONFIG = {
    "half_life_years": 1.5,              # 半衰期1.5年
    "ewma_alpha": 0.5,                   # EWMA平滑因子 = 1 - exp(-ln(2)/1.5)
    "recent_window_weight": 0.7,         # 近3年权重70%
    "bootstrap_iterations": 500,         # Bootstrap重采样次数
}
```

---

## 四、Layer 1: 六维基因测序

### 4.1 基因定义与探针映射

#### 基因 α (Cyclicality 周期性)

> **定义**：业绩对宏观经济的敏感弹性

| 数据来源 | 探针 | 字段 | 权重 |
|---------|-----|------|-----|
| 去趋势波动率 | `volatility_probe` | `detrended_cv` | 0.35 |
| R²低 → 周期特征 | `log_trend_probe` | `1 - r_squared` | 0.25 |
| 峰谷比 | `cyclical_probe` | `peak_to_trough_ratio` | 0.20 |
| 周期置信度 | `cyclical_probe` | `cyclical_confidence` | 0.15 |
| 行业先验 | `config.py` | `industry_category` | 0.05 |

**计算公式**：
```python
α = (
    0.35 * normalize(detrended_cv) +
    0.25 * (1 - r_squared) +
    0.20 * normalize(peak_to_trough_ratio) +
    0.15 * cyclical_confidence +
    0.05 * industry_cyclical_prior
)
```

#### 基因 β (Heaviness 资本密度)

> **定义**：赚取下一块钱利润所需的"重"度

**⚠️ 注意**：我们的探针系统目前**不直接计算**这个基因，需要从原始财务数据获取。

| 数据来源 | 计算方式 | 权重 |
|---------|---------|-----|
| 硬资产占比 | `(固定资产+在建)/总资产` | 0.40 |
| 资本维持压力 | `Capex/折旧摊销` | 0.30 |
| 资产周转率逆序 | `1/总资产周转率` | 0.30 |

**建议**：创建新探针 `asset_structure_probe.py` 来计算β。

#### 基因 γ (Growth 原始动能)

> **定义**：业务表面上的扩张加速度（尚未验证真伪）

| 数据来源 | 探针 | 字段 | 权重 |
|---------|-----|------|-----|
| 营收CAGR | `log_trend_probe` (revenue) | `cagr` | 0.40 |
| 加速度 | `rolling_probe` (revenue) | `trend_acceleration` | 0.35 |
| 利润增速 | `log_trend_probe` (profit) | `cagr` | 0.25 |

**计算公式**：
```python
γ = (
    0.40 * normalize(revenue_cagr) +
    0.35 * normalize(trend_acceleration) +
    0.25 * normalize(profit_cagr)
)
```

#### 基因 δ_fraud (Risk 欺诈熵) —— 熔断项

> **定义**：财务报表的物理真实性

**⚠️ 关键**：这是**一票否决项**，触发则直接熔断。

| 风险信号 | 检测方式 | 熔断阈值 |
|---------|---------|---------|
| 存贷双高 | 高现金(>30%资产) + 高负债(>60%) | 同时满足 |
| Z-Score预警 | Altman Z-Score < 1.8 | 触发 |
| 非标审计意见 | 会计师红牌 | 触发 |
| OCF与利润背离 | `OCF/净利润 < 0.3` 连续3年 | 触发 |

**建议**：创建新探针 `fraud_detection_probe.py`。

#### 基因 δ_decay (Risk 衰退熵) —— 惩罚项

> **定义**：商业模式的恶化趋势

| 数据来源 | 探针 | 字段 | 权重 |
|---------|-----|------|-----|
| 恶化概率 | `deterioration_probe` | `deterioration_probability` | 0.35 |
| 连续恶化年数 | `deterioration_probe` | `consecutive_decline_years` | 0.25 |
| 恶化模式 | `deterioration_probe` | `deterioration_pattern` | 0.20 |
| 毛利率斜率 | `log_trend_probe` (gross_margin) | `log_slope` | 0.20 |

**计算公式**：
```python
δ_decay = (
    0.35 * deterioration_probability +
    0.25 * normalize(consecutive_decline_years) +
    0.20 * pattern_severity_score +  # accelerating=1.0, chronic=0.7, etc.
    0.20 * max(0, -gross_margin_slope / 0.1)  # 负斜率惩罚
)
```

#### 基因 V (Verification 真相验证) —— 照妖镜

> **定义**：成长的含金量

| 数据来源 | 探针 | 字段 | 计算方式 |
|---------|-----|------|---------|
| OCF增速 | `log_trend_probe` (ocf) | `cagr` | 分子 |
| 营收增速 | `log_trend_probe` (revenue) | `cagr` | 分母 |
| 现金流质量 | `volatility_probe` (ocf) | `volatility_type` | 调整因子 |

**计算公式**：
```python
V_raw = min(1.0, ocf_cagr / max(revenue_cagr, 0.01))

# 预收款奖励（如有合同负债数据）
V_bonus = 0.5 * (合同负债 / 营收)

V = V_raw * (1 + V_bonus)

# 如果OCF波动极大，V打折
if ocf_volatility_type == "high_volatility":
    V *= 0.8
```

### 4.2 基因归一化规则

所有基因归一化到 **[0, 1]** 区间：

| 基因 | 0.0 含义 | 1.0 含义 |
|-----|---------|---------|
| α | 纯防御/公用事业 | 纯周期猛兽 |
| β | 印钞型轻资产 | 苦力型重资产 |
| γ | 衰退/停滞 | 爆发增长 |
| δ_fraud | 报表可信 | 高度可疑（熔断）|
| δ_decay | 健康 | 模式崩塌 |
| V | 假成长（现金流差）| 真成长+预收奖励 |

---

## 五、Layer 2: 物理求解器

### 5.1 重力求解器 (Gravity Solver)

> **适用指标**：ROIC, ROE, ROIIC
> **物理隐喻**：资本回报必须克服"资金重力"（无风险利率+风险溢价）

#### 上帝方程 I

$$
T_{roic} = R_f + k_1 \beta - k_2 \alpha - k_3 (\gamma \cdot E \cdot V) + k_4 \delta_{decay}
$$

| 参数 | 含义 | 建议值 |
|-----|------|-------|
| $R_f$ | 资金成本底线 | 3%（当前国债收益率）|
| $k_1$ | 重资产惩罚系数 | 0.08 |
| $k_2$ | 周期豁免系数 | 0.04 |
| $k_3$ | 真成长奖励系数 | 0.06 |
| $k_4$ | 衰退惩罚系数 | 0.10 |
| $E$ | 市场情绪因子 | 0.5~1.5（牛熊市）|

**解读**：
- `+k₁β`：重资产公司必须有更高回报才值得投资
- `-k₂α`：周期股在底部允许低回报（周期豁免）
- `-k₃(γEV)`：**真成长奖励**。只有 γ(成长) × E(牛市) × V(真钱) 同时高时，才允许低当期回报
- `+k₄δ_decay`：恶化中的公司需要更高门槛

#### 实战示例

**案例A：茅台**
```
α = 0.1 (防御)
β = 0.1 (轻资产)
γ = 0.3 (稳定增长)
V = 1.2 (高预收)
δ_decay = 0.1 (健康)

T_roic = 3% + 0.8% - 0.4% - 2.2% + 1.0% = 2.2%
实际ROIC ≈ 30% >> 2.2% → PASS
```

**案例B：某光伏组件厂**
```
α = 0.9 (强周期)
β = 0.6 (中等重资产)
γ = 0.2 (增速放缓)
V = 0.2 (现金流差)
δ_decay = 0.9 (存货积压)

T_roic = 3% + 4.8% - 3.6% - 0.2% + 9.0% = 13.0%
实际ROIC ≈ 8% < 13.0% → FAIL
```

### 5.2 速度求解器 (Velocity Solver)

> **适用指标**：营收增速、利润增速
> **物理隐喻**：增长必须克服"GDP摩擦力"

#### 上帝方程 II

$$
T_{growth} = GDP_g + k_1 (\gamma \cdot E \cdot V) - k_2 (\alpha \cdot (1-S))
$$

| 参数 | 含义 | 建议值 |
|-----|------|-------|
| $GDP_g$ | GDP增速基准 | 5%（当前中国GDP）|
| $k_1$ | 成长溢价系数 | 0.15 |
| $k_2$ | 周期豁免系数 | 0.10 |
| $S$ | 宏观景气度 | 0~1（PMI映射）|

**解读**：
- 所有公司的及格线是GDP增速
- 真成长(γ×V高) + 牛市(E高) 时，可以要求更高增速
- 强周期(α高) + 经济萧条(S低) 时，允许负增长

### 5.3 结构求解器 (Structure Solver)

> **适用指标**：毛利率趋势、周转率趋势
> **物理隐喻**：对抗熵增。**不考核绝对值，只考核斜率！**

#### 上帝方程 III

$$
T_{slope} = -0.02 + k_1 (1-\beta) - k_2 \delta_{decay}
$$

| 参数 | 含义 | 建议值 |
|-----|------|-------|
| $-0.02$ | 自然波动容忍 | 每年2%下滑不算恶化 |
| $k_1$ | 轻资产严查系数 | 0.03 |
| $k_2$ | 衰退惩罚系数 | 0.02 |

**解读**：
- **轻资产严查**：软件/白酒/消费品公司（β低）毛利率一旦下跌就是大问题
- **重资产宽容**：钢铁/化工等（β高）毛利率波动是常态

**实战应用**：
```python
# 从 log_trend_probe 获取毛利率斜率
gm_slope = df['grossprofit_margin_log_slope']

# 计算动态阈值
T_slope = -0.02 + 0.03 * (1 - β) - 0.02 * δ_decay

# 判断
if gm_slope < T_slope:
    risk = "毛利率恶化预警"
```

---

## 六、Layer 3: 双层自适应校准

### 6.1 总公式

$$
T_{final} = T_{theory} + \Delta_{industry} + \Delta_{size} + \Delta_{confidence}
$$

### 6.2 行业残差修正 (Δ_industry)

> **原理**：让数据说话，根据行业实际表现动态调整

**计算方法**：
```python
# 1. 计算行业Top20%公司的实际中位数
actual_median = industry_data[industry_data['rank_pct'] <= 0.2]['roic'].median()

# 2. 计算行业理论均值（基于理论阈值）
theory_mean = T_theory  # 由上帝方程计算

# 3. 残差修正（学习率λ控制调整速度）
λ = 0.3  # 学习率
Δ_industry = (actual_median - theory_mean) * λ
```

**效果**：
- 煤炭景气时 → `Δ_industry > 0` → 自动提高煤炭股门槛
- 光伏内卷时 → `Δ_industry < 0` → 自动降低光伏股门槛

### 6.3 市值分层修正 (Δ_size)

> **原理**：大票确定性高可容忍低回报，小票风险高需要高补偿

| 市值分位 | 规模名称 | Δ_size |
|---------|---------|--------|
| Top 10% | 超级大盘 | -1.5% |
| 10%-30% | 大盘 | -0.5% |
| 30%-70% | 中盘 | 0% |
| 70%-90% | 小盘 | +1.5% |
| Bottom 10% | 微盘 | +3.0% |

### 6.4 置信度折扣 (Δ_confidence)

> **原理**：数据年限短时，结论的置信度有上限

**我们已有的配置**：
```python
CYCLICALITY_CONFIDENCE_CONFIG = {
    "confidence_ceiling_by_years": {
        5: 0.55,   # 5年数据置信度上限55%
        7: 0.70,
        10: 0.85,
        15: 0.95,
    }
}
```

**应用方式**：
```python
confidence_ceiling = get_cyclicality_confidence_ceiling(n_years)

# 对于低置信度数据，收紧阈值（更保守）
Δ_confidence = 0.02 * (1 - confidence_ceiling)  # 置信度55%时，阈值+0.9%
```

---

## 七、探针映射表

### 7.1 现有探针 → 基因映射

| 探针 | 输出字段 | 映射到基因 | 权重 |
|-----|---------|-----------|-----|
| `log_trend_probe` | `log_slope`, `r_squared`, `cagr` | γ, α | 核心 |
| `volatility_probe` | `detrended_cv`, `cv`, `arch_effect` | α, δ_decay | 核心 |
| `cyclical_probe` | `cyclical_confidence`, `peak_to_trough` | α | 核心 |
| `deterioration_probe` | `deterioration_probability`, `pattern` | δ_decay | 核心 |
| `rolling_probe` | `trend_acceleration`, `recent_3y_slope` | γ | 辅助 |
| `robust_probe` | `robust_slope`, `mk_tau` | 验证用 | 辅助 |
| `inflection_probe` | `inflection_type`, `confidence` | 拐点预警 | 辅助 |
| `multi_horizon_probe` | `break_type`, `data_regime` | 断点预警 | 辅助 |

### 7.2 缺失探针（建议新增）

| 建议探针 | 用途 | 映射基因 |
|---------|-----|---------|
| `asset_structure_probe` | 资本密度分析 | β |
| `fraud_detection_probe` | 欺诈风险检测 | δ_fraud |
| `cash_verification_probe` | 现金流验证 | V |
| `inventory_probe` | 存货周转分析 | δ_decay (增强) |

---

## 八、实战推演

### 8.1 场景A：2025年 某光伏组件龙头

**输入数据**（从探针获取）：
```yaml
# 来自各探针的原始输出
roic:
  log_slope: -0.15
  detrended_cv: 0.45
  cyclical_confidence: 0.85
  deterioration_probability: 0.75
  consecutive_decline_years: 2

revenue:
  cagr: 0.08
  trend_acceleration: -0.05

ocf:
  cagr: -0.10
  volatility_type: "high_volatility"

gross_margin:
  log_slope: -0.08
```

**Step 1: 基因测序**
```python
α = 0.35*0.45 + 0.25*(1-0.4) + 0.20*norm(3.5) + 0.15*0.85 + 0.05*1.0 = 0.82
β = 0.55  # 假设从资产结构获取
γ = 0.40*norm(0.08) + 0.35*norm(-0.05) + 0.25*norm(0.03) = 0.25
δ_decay = 0.35*0.75 + 0.25*norm(2) + 0.20*0.8 + 0.20*0.8 = 0.73
V = min(1.0, -0.10/0.08) * 0.8 = 0  # 现金流差，V≈0
```

**Step 2: 重力求解器**
```python
T_roic = 3% + 0.08*0.55 - 0.04*0.82 - 0.06*(0.25*0.8*0) + 0.10*0.73
       = 3% + 4.4% - 3.3% - 0% + 7.3%
       = 11.4%
```

**Step 3: 双层校准**
```python
Δ_industry = -2%  # 光伏行业整体差
Δ_size = -0.5%    # 大盘股
Δ_confidence = +0.4%  # 5年数据

T_final = 11.4% - 2% - 0.5% + 0.4% = 9.3%
```

**最终判决**：
```
实际ROIC: 6%
阈值: 9.3%
判定: ❌ FAIL

原因分析:
1. 存货恶化 (δ_decay=0.73) 大幅推高阈值
2. 现金流差 (V=0) 无法获得成长奖励
3. 虽然行业修正-2%，但不足以抵消恶化惩罚
```

### 8.2 场景B：2025年 某潮流玩具公司

**输入数据**：
```yaml
roic:
  log_slope: 0.12
  detrended_cv: 0.15
  cyclical_confidence: 0.25
  deterioration_probability: 0.10

revenue:
  cagr: 0.35
  trend_acceleration: 0.08

ocf:
  cagr: 0.25
  volatility_type: "stable"

gross_margin:
  log_slope: 0.02
```

**Step 1: 基因测序**
```python
α = 0.18  # 低周期性
β = 0.12  # 极轻资产
γ = 0.75  # 高成长
δ_decay = 0.12  # 健康
V = min(1.0, 0.25/0.35) * 1.0 * 1.2 = 0.86  # 假设有预收奖励
```

**Step 2: 重力求解器**
```python
T_roic = 3% + 0.08*0.12 - 0.04*0.18 - 0.06*(0.75*1.0*0.86) + 0.10*0.12
       = 3% + 1.0% - 0.7% - 3.9% + 1.2%
       = 0.6%
```

**Step 3: 结构求解器**（毛利率）
```python
T_slope = -0.02 + 0.03*(1-0.12) - 0.02*0.12
        = -0.02 + 0.026 - 0.002
        = 0.004  # 要求毛利率不能下滑

实际 gross_margin_slope = 0.02 > 0.004 → PASS
```

**最终判决**：
```
实际ROIC: 18%
阈值: 0.6%
判定: ✅ PASS

原因分析:
1. 轻资产 (β=0.12) + 高V因子 (0.86) → 大幅降低阈值
2. 真成长奖励项 -3.9% 起了关键作用
3. 毛利率结构检验也通过
```

---

## 九、关键决策（已确认）

> 以下决策基于金融工程、计量经济学和实战风控的专业知识做出。

### ✅ 决策1：β基因（资本密度）的实现方案

**专业分析**：

β基因衡量"赚取下一块钱利润的重度"，本质上反映的是**经营杠杆(Operating Leverage)**。

从计量经济学角度，有三种测量方法：

| 方法 | 数据需求 | 优点 | 缺点 |
|-----|---------|-----|-----|
| 资产结构法 | 固定资产/总资产 | 直接、直观 | 需要额外数据源 |
| 经营杠杆法 | EBIT弹性 | 理论严谨 | 计算复杂 |
| **波动代理法** | ROIC时序波动 | **现有探针可用** | 间接测量 |

**专业论证**：
重资产公司的核心特征是**高固定成本+低边际成本**，这导致：
1. 经营杠杆高 → ROIC对收入变化极度敏感
2. 周期波动大 → ROIC的 `detrended_cv` 高
3. 但这与周期性(α)不同：周期性是外因（宏观），资本密度是内因（成本结构）

**确认方案：混合法**

```python
# β 基因计算（不依赖额外数据源）
def compute_beta_gene(roic_metrics, ocf_metrics, revenue_metrics):
    """
    资本密度基因：用现有探针数据推断

    核心洞察：
    - 重资产公司：ROIC波动 > OCF波动（固定成本放大利润波动）
    - 轻资产公司：ROIC波动 ≈ OCF波动（成本结构灵活）
    """
    # 1. ROIC波动率 vs OCF波动率 的比值
    roic_cv = roic_metrics['detrended_cv']
    ocf_cv = ocf_metrics['cv'] if ocf_metrics['cv'] < float('inf') else 1.0
    leverage_ratio = min(roic_cv / max(ocf_cv, 0.01), 3.0) / 3.0  # 归一化到[0,1]

    # 2. 营收-利润敏感度（经营杠杆代理）
    # 利润斜率 / 营收斜率 > 1 说明高经营杠杆
    rev_slope = revenue_metrics['log_slope']
    profit_slope = roic_metrics['log_slope']  # 用ROIC斜率代理利润
    if abs(rev_slope) > 0.01:
        sensitivity = min(abs(profit_slope / rev_slope), 2.0) / 2.0
    else:
        sensitivity = 0.5

    # 3. 综合（权重：波动比70% + 敏感度30%）
    β = 0.7 * leverage_ratio + 0.3 * sensitivity
    return β
```

**结论**：✅ **不需要新增探针**，用现有数据推断β。

---

### ✅ 决策2：δ_fraud（欺诈熵）熔断机制

**专业分析**：

欺诈检测在学术界有三大经典方法：

| 方法 | 代表模型 | 适用场景 | 我们的可行性 |
|-----|---------|---------|-------------|
| 比率异常法 | Beneish M-Score | 检测盈余操纵 | 需要8个财务比率 |
| 破产预警法 | Altman Z-Score | 检测财务困境 | 需要5个比率 |
| **现金流验证法** | Sloan应计质量 | 检测盈利质量 | ✅ **我们有OCF探针** |

**关键洞察**：
1. 真正的"欺诈"是系统性风险，普通量化系统难以检测（需要审计意见等定性数据）
2. 我们能做的是**检测"盈利质量问题"**，这是欺诈的前兆
3. Sloan (1996) 证明：应计利润(Accruals)高的公司，未来回报显著更低

**确认方案：盈利质量熔断（简化但有效）**

```python
# δ_fraud 熔断机制
def compute_fraud_gene(roic_metrics, ocf_metrics, profit_metrics, n_years=5):
    """
    欺诈/质量风险基因

    基于 Sloan (1996) 应计质量理论 + 我们的探针数据
    """
    # 1. OCF/利润背离度（核心指标）
    # 连续多年 OCF增速 << 利润增速 是危险信号
    ocf_cagr = ocf_metrics['cagr']
    profit_cagr = profit_metrics['cagr']

    if profit_cagr > 0.05:  # 利润正增长时才检查
        divergence = max(0, profit_cagr - ocf_cagr) / max(profit_cagr, 0.01)
    else:
        divergence = 0  # 利润负增长，不适用此检测

    # 2. OCF波动异常（波动极大可能是操纵）
    ocf_volatility_penalty = 1.0 if ocf_metrics['volatility_type'] == 'extreme_volatility' else 0.0

    # 3. 连续恶化（可能在掩盖问题）
    if roic_metrics.get('consecutive_decline_years', 0) >= 3:
        chronic_warning = 0.3
    else:
        chronic_warning = 0.0

    # 综合
    δ_fraud = 0.6 * divergence + 0.2 * ocf_volatility_penalty + 0.2 * chronic_warning

    # 熔断逻辑
    MELTDOWN_THRESHOLD = 0.7
    is_meltdown = δ_fraud > MELTDOWN_THRESHOLD

    return δ_fraud, is_meltdown, {
        'divergence': divergence,
        'volatility_penalty': ocf_volatility_penalty,
        'chronic_warning': chronic_warning,
    }
```

**熔断条件**：
- δ_fraud > 0.7 → **直接熔断**，不参与评分
- 触发时输出："⚠️ 盈利质量风险：OCF与利润持续背离"

**结论**：✅ **不需要新增探针**，用OCF探针实现简化版熔断。

---

### ✅ 决策3：市场情绪因子 E 的处理

**专业分析**：

市场情绪因子E在上帝方程中的作用是**调节成长溢价的时效性**：
- 牛市(E>1)：市场愿意给成长股更高估值，允许当期低回报换未来增长
- 熊市(E<1)：市场要求即时回报，成长溢价失效

| 方案 | 实现复杂度 | 准确性 | 依赖外部数据 |
|-----|-----------|-------|-------------|
| 大盘PE分位数 | 高 | 高 | 是 |
| **固定E=1** | **低** | 中 | **否** |
| 滚动波动率VIX | 中 | 高 | 是 |

**关键洞察**：
1. 我们的系统是**基本面分析**，不是择时系统
2. E因子的价值在于"牛市容忍亏损换增长"，但这本身就是风险
3. **保守原则**：固定E=1意味着我们不为"牛市泡沫"买单

**确认方案：固定E=1.0（保守中性）**

```python
# 市场情绪因子
E = 1.0  # 固定为中性市场

# 如果未来需要动态化，预留接口：
def get_market_sentiment() -> float:
    """
    获取市场情绪因子 (0.5 ~ 1.5)

    当前实现：固定返回1.0（中性）
    未来可扩展：接入PE分位数、VIX等
    """
    return 1.0
```

**理由**：
- 避免在牛市高位给予过度宽容
- 保持系统的**逆向投资特性**
- 符合"让数据说话"的哲学（E是市场噪音，不是基本面）

**结论**：✅ **固定E=1.0**，未来可扩展。

---

### ✅ 决策4：上帝方程参数校准方法

**专业分析**：

参数校准在计量经济学中有三大流派：

| 方法 | 学术名称 | 优点 | 缺点 |
|-----|---------|-----|-----|
| 历史回测 | OLS/MLE估计 | 最优拟合 | 过拟合风险 |
| 经验值+残差修正 | Ridge回归思想 | 平衡偏差-方差 | 需要先验 |
| **贝叶斯自适应** | 贝叶斯更新 | **自我进化** | 计算复杂 |

**关键洞察**：
1. 我们的目标不是"预测股价"，而是"识别好公司"
2. 过度校准会导致**过拟合**：在历史数据上完美，在未来失效
3. 参数的**稳健性**比**精确性**更重要

**确认方案：经验值 + 贝叶斯残差自适应**

```python
# 上帝方程参数（稳健经验值）
GOD_EQUATION_PARAMS = {
    # 重力求解器
    'R_f': 0.03,          # 无风险利率3%（可配置）
    'k1_beta': 0.08,      # 重资产惩罚：β每增加0.1，阈值+0.8%
    'k2_alpha': 0.04,     # 周期豁免：α每增加0.1，阈值-0.4%
    'k3_growth': 0.06,    # 真成长奖励：γ×E×V每增加0.1，阈值-0.6%
    'k4_decay': 0.10,     # 衰退惩罚：δ_decay每增加0.1，阈值+1.0%

    # 速度求解器
    'GDP_growth': 0.05,   # GDP基准5%
    'k1_growth_premium': 0.15,
    'k2_cycle_exempt': 0.10,

    # 结构求解器
    'natural_decay': -0.02,  # 允许每年2%自然波动
    'k1_light_asset': 0.03,  # 轻资产严查
    'k2_decay_penalty': 0.02,
}

# 贝叶斯残差自适应
class AdaptiveCalibrator:
    """
    自适应参数校准器

    原理：用行业Top20%公司的实际表现修正理论阈值
    学习率λ控制调整速度，防止过拟合
    """
    def __init__(self, learning_rate=0.3):
        self.λ = learning_rate
        self.industry_residuals = {}  # 缓存行业残差

    def update_residual(self, industry: str, actual_median: float, theory_mean: float):
        """贝叶斯更新行业残差"""
        new_residual = actual_median - theory_mean

        if industry in self.industry_residuals:
            # 指数移动平均更新
            old = self.industry_residuals[industry]
            self.industry_residuals[industry] = (1 - self.λ) * old + self.λ * new_residual
        else:
            self.industry_residuals[industry] = new_residual

    def get_calibrated_threshold(self, T_theory: float, industry: str, size_percentile: float) -> float:
        """获取校准后的阈值"""
        Δ_industry = self.industry_residuals.get(industry, 0)
        Δ_size = self._size_adjustment(size_percentile)
        return T_theory + Δ_industry + Δ_size

    def _size_adjustment(self, percentile: float) -> float:
        """市值分层修正"""
        if percentile <= 0.1:
            return -0.015  # 超大盘：-1.5%
        elif percentile <= 0.3:
            return -0.005  # 大盘：-0.5%
        elif percentile <= 0.7:
            return 0.0     # 中盘：0%
        elif percentile <= 0.9:
            return 0.015   # 小盘：+1.5%
        else:
            return 0.03    # 微盘：+3.0%
```

**结论**：✅ **经验值为主，残差自适应为辅**，防止过拟合。

---

### ✅ 决策5：与现有系统的集成方式

**专业分析**：

三种集成架构对比：

| 方案 | 架构 | 优点 | 缺点 |
|-----|-----|-----|-----|
| 并行独立 | T.R.U.T.H. ∥ 现有系统 | 风险隔离 | 维护两套系统 |
| 完全替换 | T.R.U.T.H. → 报告 | 统一简洁 | 迁移风险大 |
| **预筛选层** | T.R.U.T.H. → 现有系统 → 报告 | **渐进演进** | 需要适配 |

**关键洞察**：
1. 现有系统（`comprehensive_generator.py`）已经很成熟
2. T.R.U.T.H.的核心价值是**动态阈值**，不是评分逻辑
3. **最小侵入性**原则：不破坏现有功能，只增强

**确认方案：T.R.U.T.H.作为"基因注入层"**

```
数据流：
┌──────────┐    ┌───────────────┐    ┌──────────────────┐    ┌────────┐
│ 探针系统 │ -> │ T.R.U.T.H.     │ -> │ comprehensive    │ -> │ 报告   │
│ (现有)   │    │ 基因测序       │    │ generator (现有) │    │        │
└──────────┘    │ 动态阈值生成   │    │ + 基因增强       │    └────────┘
               └───────────────┘    └──────────────────┘
```

**具体实现**：

```python
# truth_engine.py (新模块)
class TruthEngine:
    """T.R.U.T.H. 基因测序引擎"""

    def sequence_genome(self, company_metrics: Dict) -> CompanyGenome:
        """对公司进行6维基因测序"""
        return CompanyGenome(
            α=self._compute_alpha(company_metrics),
            β=self._compute_beta(company_metrics),
            γ=self._compute_gamma(company_metrics),
            δ_fraud=self._compute_delta_fraud(company_metrics),
            δ_decay=self._compute_delta_decay(company_metrics),
            V=self._compute_verification(company_metrics),
        )

    def compute_dynamic_thresholds(self, genome: CompanyGenome) -> DynamicThresholds:
        """根据基因计算动态阈值"""
        return DynamicThresholds(
            T_roic=self._gravity_solver(genome),
            T_growth=self._velocity_solver(genome),
            T_structure=self._structure_solver(genome),
        )

# comprehensive_generator.py (修改)
class ComprehensiveReportGenerator:
    def __init__(self, ...):
        ...
        self.truth_engine = TruthEngine()  # 注入T.R.U.T.H.引擎

    def generate_report(self):
        # 1. 现有：加载数据
        df = self.load_and_merge_data()

        # 2. 新增：基因测序
        for idx, row in df.iterrows():
            genome = self.truth_engine.sequence_genome(row)
            thresholds = self.truth_engine.compute_dynamic_thresholds(genome)

            # 注入基因到DataFrame
            df.loc[idx, 'gene_alpha'] = genome.α
            df.loc[idx, 'gene_beta'] = genome.β
            # ... 其他基因

            # 注入动态阈值
            df.loc[idx, 'dynamic_T_roic'] = thresholds.T_roic

        # 3. 现有：评分和筛选（使用动态阈值）
        ...
```

**结论**：✅ **基因注入层**，最小侵入性，渐进演进。

---

### ✅ 决策6：6维基因可视化

**专业分析**：

可视化在量化分析中的价值：
1. **可解释性(Explainability)**：让用户理解为什么公司被筛选/淘汰
2. **异常检测**：雷达图形状异常一眼可见
3. **投资者沟通**：专业报告必备

**确认方案：生成基因雷达图 + 文字解读**

```python
import matplotlib.pyplot as plt
import numpy as np

def plot_genome_radar(genome: CompanyGenome, company_name: str) -> plt.Figure:
    """
    生成6维基因雷达图
    """
    # 数据准备
    categories = ['α 周期性', 'β 资本密度', 'γ 成长动能',
                  'δf 欺诈风险', 'δd 衰退风险', 'V 真相验证']
    values = [genome.α, genome.β, genome.γ,
              genome.δ_fraud, genome.δ_decay, genome.V]

    # 雷达图
    angles = np.linspace(0, 2 * np.pi, len(categories), endpoint=False).tolist()
    values += values[:1]  # 闭合
    angles += angles[:1]

    fig, ax = plt.subplots(figsize=(8, 8), subplot_kw=dict(polar=True))
    ax.fill(angles, values, color='steelblue', alpha=0.25)
    ax.plot(angles, values, color='steelblue', linewidth=2)
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(categories, fontsize=12)
    ax.set_ylim(0, 1)
    ax.set_title(f'{company_name} 基因图谱', fontsize=16, fontweight='bold')

    return fig

def generate_genome_interpretation(genome: CompanyGenome) -> str:
    """
    生成基因解读文字
    """
    lines = []

    # 周期性解读
    if genome.α > 0.7:
        lines.append("⚡ **强周期特征**：业绩波动大，需关注周期位置")
    elif genome.α < 0.3:
        lines.append("🛡️ **防御特征**：业绩稳定，适合长期持有")

    # 资本密度解读
    if genome.β > 0.7:
        lines.append("🏭 **重资产模式**：固定成本高，经营杠杆大")
    elif genome.β < 0.3:
        lines.append("💰 **轻资产模式**：现金流优质，印钞机属性")

    # 成长性解读
    if genome.γ > 0.7:
        lines.append("🚀 **高成长期**：扩张加速，需验证现金流")
    elif genome.γ < 0.3:
        lines.append("📉 **成熟/衰退期**：增长放缓，关注分红和回购")

    # 风险解读
    if genome.δ_fraud > 0.5:
        lines.append("⚠️ **质量预警**：盈利与现金流背离，需警惕")
    if genome.δ_decay > 0.5:
        lines.append("📉 **恶化预警**：趋势向下，可能是价值陷阱")

    # 验证解读
    if genome.V > 0.8:
        lines.append("✅ **真成长**：现金流验证增长，含金量高")
    elif genome.V < 0.3:
        lines.append("❌ **假成长**：增长未转化为现金，需谨慎")

    return '\n'.join(lines)
```

**报告输出示例**：

```
┌────────────────────────────────────────────┐
│          贵州茅台 (600519) 基因图谱         │
├────────────────────────────────────────────┤
│                                            │
│           α=0.12 (防御)                    │
│              ╱╲                            │
│             ╱  ╲                           │
│    β=0.08 ╱    ╲ γ=0.35                   │
│           ╲    ╱                           │
│            ╲  ╱                            │
│    δf=0.05 ╲╱  V=1.15                      │
│           δd=0.08                          │
│                                            │
├────────────────────────────────────────────┤
│ 基因解读:                                  │
│ 🛡️ 防御特征：业绩稳定，适合长期持有         │
│ 💰 轻资产模式：现金流优质，印钞机属性       │
│ ✅ 真成长：现金流验证增长，含金量高         │
│                                            │
│ 动态阈值: T_roic = 2.1%                    │
│ 实际ROIC: 31.2%                            │
│ 判定: ✅ PASS (超额 29.1%)                 │
└────────────────────────────────────────────┘
```

**结论**：✅ **生成雷达图 + 文字解读**，增强可解释性。

---

## 十、实现路线图

### Phase 1: 核心引擎（1周）

```
优先级: P0
目标: 实现基因测序 + 三大求解器

任务:
├── truth_engine.py (新建)
│   ├── CompanyGenome 数据类
│   ├── compute_alpha() - 周期性基因
│   ├── compute_beta() - 资本密度基因（用波动代理法）
│   ├── compute_gamma() - 成长动能基因
│   ├── compute_delta_fraud() - 欺诈熵基因
│   ├── compute_delta_decay() - 衰退熵基因
│   ├── compute_verification() - 真相验证基因
│   ├── gravity_solver() - 重力求解器
│   ├── velocity_solver() - 速度求解器
│   └── structure_solver() - 结构求解器
└── 单元测试
```

### Phase 2: 自适应校准（3天）

```
优先级: P1
目标: 实现双层校准

任务:
├── adaptive_calibrator.py (新建)
│   ├── AdaptiveCalibrator 类
│   ├── 行业残差更新
│   ├── 市值分层修正
│   └── 置信度折扣
└── 集成到 truth_engine.py
```

### Phase 3: 系统集成（3天）

```
优先级: P1
目标: 注入到现有系统

任务:
├── comprehensive_generator.py (修改)
│   ├── 初始化 TruthEngine
│   ├── 基因测序循环
│   ├── 动态阈值注入
│   └── 报告输出增强
└── 端到端测试
```

### Phase 4: 可视化（2天）

```
优先级: P2
目标: 基因雷达图

任务:
├── genome_visualizer.py (新建)
│   ├── plot_genome_radar()
│   ├── generate_genome_interpretation()
│   └── export_to_report()
└── 报告模板更新
```

### 里程碑

| 日期 | 里程碑 | 交付物 |
|-----|-------|-------|
| Week 1 | 核心引擎完成 | `truth_engine.py` + 测试通过 |
| Week 1.5 | 校准器完成 | `adaptive_calibrator.py` |
| Week 2 | 系统集成 | 端到端可运行 |
| Week 2.5 | 可视化完成 | 报告含基因雷达图 |

---

## 十一、附录：配置迁移指南

### 从静态配置到T.R.U.T.H.

**废弃的配置**（将被动态阈值替代）：

```python
# ❌ 不再需要
_ROIC_FILTER_CONFIGS = {
    "白酒": {"min_roic": 0.15, "min_slope": -0.02},
    "钢铁": {"min_roic": 0.04, "min_slope": -0.06},
    ...
}

# ❌ 不再需要
_INDUSTRY_CATEGORY_MAP = {
    "白酒": "defensive",
    "钢铁": "cyclical",
    ...
}
```

**保留的配置**（作为弱先验）：

```python
# ✅ 保留，但仅作为贝叶斯先验（权重5%）
INDUSTRY_CYCLICAL_PRIOR = {
    "钢铁": 0.9,    # 90%先验周期性
    "白酒": 0.1,    # 10%先验周期性
    "default": 0.5, # 中性先验
}
```

**新增的配置**：

```python
# ✅ 新增：上帝方程参数
GOD_EQUATION_PARAMS = {
    'R_f': 0.03,
    'k1_beta': 0.08,
    'k2_alpha': 0.04,
    'k3_growth': 0.06,
    'k4_decay': 0.10,
    ...
}

# ✅ 新增：市值分层
SIZE_ADJUSTMENTS = {
    'mega': -0.015,   # Top 10%
    'large': -0.005,  # 10-30%
    'mid': 0.0,       # 30-70%
    'small': 0.015,   # 70-90%
    'micro': 0.03,    # Bottom 10%
}
```

---

> 📝 **版本历史**
> - v0.1 (2025-12-09): 初始设计稿
> - v1.0 (2025-12-09): **决策确认版** - 6个关键决策已确认
