# 三大求解器进化分析报告

## 🔍 当前架构审计

### 三大求解器职责

| 求解器 | 职责 | 输出 | 使用的基因 |
|--------|------|------|------------|
| **Gravity** | 计算动态ROIC阈值 | T_roic | α, β, γ, V, δ_decay |
| **Velocity** | 计算增长边界 | T_growth | α, γ, V |
| **Structure** | 预测斜率走势 | T_slope | β, δ_decay |

### 当前上帝方程

**Gravity (方程 I):**
```
T_roic = R_f + k₁β - k₂α - k₃(γ×E×V) + k₄δ_decay
```

**Velocity (方程 II):**
```
T_growth = GDP_g + k₁(γ×E×V) - k₂(α×(1-S))
```

**Structure (方程 III):**
```
T_slope = -0.02 + k₁(1-β) - k₂×δ_decay
```

---

## 🚨 发现的问题

### 问题1: δ_fraud 未被任何求解器使用！

**现状**: `δ_fraud` 欺诈熵基因只用于触发熔断，但不参与阈值计算。

**问题**: 即使未触发熔断（δ_fraud < 0.58），有财务疑点的公司也应该被要求更高回报。

**建议**: 在 Gravity Solver 中加入欺诈风险溢价。

### 问题2: V因子使用方式不够精细

**现状**: V 只作为乘数 `γ×E×V`，全有或全无。

**问题**: V=0.8 和 V=0.4 对阈值影响是线性的，但实际应该有"真假分界线"。

**建议**: 引入V的非线性变换，V<0.5时应该有更强的惩罚。

### 问题3: Structure Solver 没用到 α 和 γ

**现状**: 斜率预测只用了 β 和 δ_decay。

**问题**:
- 周期股(高α)的斜率预测应该考虑周期位置
- 高成长股(高γ)的斜率可能持续向好

**建议**: 扩展 Structure Solver 的输入。

### 问题4: Velocity Solver 的周期位置信号(S)过于简化

**现状**: `cycle_position_signal = genome.alpha`（直接用α代替S）

**问题**: α是周期敏感度，不是周期位置！高α只说明这是周期股，不说明现在在哪个位置。

**建议**: 从 δ_decay 或新增探针获取真正的周期位置。

### 问题5: v2.0新特性未被利用

**问题**: 六大基因v2.0增加了很多新信息（硬触发、拐点预警、麦道夫特征），但求解器还是v1.0逻辑。

---

## 🔧 优化方案

### 优化1: Gravity Solver v2.0 - 增加δ_fraud

**原方程:**
```
T = R_f + k₁β - k₂α - k₃(γEV) + k₄δ_decay
```

**新方程:**
```
T = R_f + k₁β - k₂α - k₃(γEV×f(δ_fraud)) + k₄δ_decay + k₅δ_fraud
```

其中:
- `k₅δ_fraud`: 欺诈风险溢价（正号，增加阈值）
- `f(δ_fraud)`: 欺诈惩罚因子，削弱成长奖励
  - `f(x) = max(0, 1 - 2×x)` 当 δ_fraud > 0.5 时，成长奖励归零

**代码实现:**
```python
# k₅δ_fraud: 欺诈风险溢价
fraud_premium = solver.k5_fraud * genome.delta_fraud

# 欺诈惩罚因子（削弱成长奖励）
fraud_penalty_factor = max(0, 1 - 2 * genome.delta_fraud)

# 修正后的成长折扣
growth_discount = solver.k3_gamma * genome.gamma * E * genome.verification * fraud_penalty_factor

# 新方程
T = base_rate + beta_premium - alpha_discount - growth_discount + decay_penalty + fraud_premium
```

### 优化2: V因子非线性变换

**问题**: V=0.3 和 V=0.8 对阈值影响是线性的。

**解决方案**: 引入S型变换，强化"真假分界"。

```python
def transform_verification(v: float) -> float:
    """
    V因子非线性变换
    - V < 0.4: 假成长，强烈惩罚
    - V = 0.5: 中性点
    - V > 0.6: 真成长，给予奖励
    """
    # 使用S型函数增强分界效应
    # 中心点0.5，斜率4
    return 1 / (1 + math.exp(-8 * (v - 0.5)))
```

**效果:**
| 原始V | 变换后V_eff | 解释 |
|-------|-------------|------|
| 0.2 | 0.08 | 假成长，几乎无奖励 |
| 0.4 | 0.27 | 可疑，大幅削减 |
| 0.5 | 0.50 | 中性 |
| 0.6 | 0.73 | 偏真，给予奖励 |
| 0.8 | 0.92 | 真成长，接近满分 |

### 优化3: Structure Solver v2.0 - 加入α和γ

**原方程:**
```
T_slope = -0.02 + k₁(1-β) - k₂×δ_decay
```

**新方程:**
```
T_slope = -0.02 + k₁(1-β) + k₃(γ×V) - k₂×δ_decay - k₄×α×sign(cycle_position)
```

其中:
- `+k₃(γ×V)`: 真成长支撑（真成长公司斜率更可能为正）
- `-k₄×α×sign(cycle_position)`: 周期调整
  - 周期顶部(position>0): 预期斜率下降
  - 周期底部(position<0): 预期斜率上升

### 优化4: Velocity Solver v2.0 - 修复周期位置信号

**问题**: 用α代替周期位置S是错误的。

**解决方案**: 从δ_decay推断周期位置。

```python
# 周期位置推断
# δ_decay 高 → 可能在周期顶部（下跌中）
# δ_decay 低 + α 高 → 可能在周期底部（即将反转）
if genome.alpha > 0.5:  # 是周期股
    if genome.delta_decay > 0.6:
        cycle_position = 0.8  # 周期顶部/下行区
    elif genome.delta_decay < 0.3:
        cycle_position = 0.2  # 周期底部/上行区
    else:
        cycle_position = 0.5  # 周期中部
else:
    cycle_position = 0.5  # 非周期股，中性
```

### 优化5: 利用v2.0的硬触发机制

**问题**: 基因的硬触发（熔断、拐点预警）没有传递给求解器。

**解决方案**: 在CompanyGenome中增加标志位。

```python
@dataclass
class CompanyGenome:
    # ... 原有字段 ...

    # v2.0 新增硬触发标志
    fraud_circuit_break: bool = False    # δ_fraud 麦道夫熔断
    decay_hard_trigger: bool = False     # δ_decay 拐点逃顶触发
    alpha_high_trend: bool = False       # α Hurst高趋势性（非真周期）
    beta_hidden_heavy: bool = False      # β 隐性重资产警报
```

求解器利用这些标志:
```python
# Gravity Solver v2.0
if genome.fraud_circuit_break:
    # 欺诈熔断：阈值直接设为上限
    return ThresholdResult(final_threshold=solver.threshold_ceiling, circuit_break=True)

if genome.decay_hard_trigger:
    # 拐点预警：额外增加5%阈值
    decay_penalty += 0.05
```

---

## 📊 新权重建议

### Gravity Solver 系数

| 系数 | 原值 | 新值 | 理由 |
|------|------|------|------|
| k1_beta | 0.08 | 0.08 | 保持（重资产惩罚合理） |
| k2_alpha | 0.04 | 0.05 | 略增（v2.0 α更精准，可给更多豁免） |
| k3_gamma | 0.06 | 0.08 | 增加（配合V的非线性，整体效果不变） |
| k4_decay | 0.10 | 0.08 | 略降（v2.0有硬触发，常规惩罚可降） |
| **k5_fraud** | 无 | **0.06** | **新增**（欺诈风险溢价） |

### Structure Solver 系数

| 系数 | 原值 | 新值 | 理由 |
|------|------|------|------|
| natural_decay | -0.02 | -0.02 | 保持（熵增定律） |
| k1_structure | 0.05 | 0.05 | 保持 |
| k2_structure | 0.08 | 0.08 | 保持 |
| **k3_structure** | 无 | **0.03** | **新增**（成长支撑斜率） |
| **k4_structure** | 无 | **0.02** | **新增**（周期位置调整） |

---

## 🎯 优先级排序

| 优先级 | 优化项 | 影响度 | 复杂度 |
|--------|--------|--------|--------|
| P0 | Gravity加入δ_fraud | ⭐⭐⭐⭐⭐ | ⭐⭐ |
| P0 | 利用v2.0硬触发机制 | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |
| P1 | V因子非线性变换 | ⭐⭐⭐⭐ | ⭐⭐ |
| P1 | 修复Velocity的周期位置 | ⭐⭐⭐⭐ | ⭐⭐⭐ |
| P2 | Structure加入α和γ | ⭐⭐⭐ | ⭐⭐⭐ |

---

## 📝 实施计划

### Phase 1: 核心修复 (P0)
1. 修改 `gravity_solver.py`，加入 k5_fraud
2. 扩展 `CompanyGenome` 增加硬触发标志
3. 在基因计算时填充硬触发标志
4. 求解器利用硬触发

### Phase 2: 精细化 (P1)
1. 实现V因子非线性变换
2. 从δ_decay推断周期位置
3. 修复 `velocity_solver.py`

### Phase 3: 完善 (P2)
1. 扩展 `structure_solver.py`
2. 全面测试
3. 参数调优

---

## ✅ 验证测试案例

### 案例1: 茅台（轻资产、真成长、无欺诈）
- 预期: 低阈值，高成长奖励
- β=0.1, α=0.1, γ=0.7, V=0.9, δ_fraud=0.1, δ_decay=0.1
- T ≈ 3% + 0.8% - 0.5% - 4.5% + 0.8% + 0.6% ≈ **0.2%** (极低阈值，茅台容易过)

### 案例2: 宝钢（重资产、周期股）
- 预期: 高阈值，周期豁免
- β=0.9, α=0.8, γ=0.3, V=0.6, δ_fraud=0.2, δ_decay=0.3
- T ≈ 3% + 7.2% - 4% - 1.2% + 2.4% + 1.2% ≈ **8.6%** (高阈值)

### 案例3: 某造假公司
- 预期: 极高阈值或熔断
- β=0.5, α=0.2, γ=0.6, V=0.3, δ_fraud=0.7, δ_decay=0.4
- fraud_circuit_break = True → **熔断，直接排除**

---

*优化后的求解器将与v2.0基因完美协同，实现更精准的筛选。*
