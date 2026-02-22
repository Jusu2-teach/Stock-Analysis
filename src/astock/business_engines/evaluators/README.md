# AStock Evaluators v2.1

## 因果贝叶斯网络 + 状态机 + 规则引擎评估系统

> **v2.1 更新 (2026-02)**: 全面整合规则引擎、因果诊断、Copula 融合，实现六大组件的完整数据流闭环

### 🎯 设计目标

解决 v1/v2.0 版本的核心问题：
1. **过度工程化** - 23个Python类 → 6个核心模块
2. **规则硬编码** - 硬编码规则 → YAML声明式配置 + 规则引擎
3. **假设独立性** - 忽略指标相关性 → Copula建模
4. **相关≠因果** - 相关性分析 → Pearl因果推断
5. **静态评估** - 静态快照 → HMM状态机建模生命周期
6. **🆕 组件孤岛** - 组件初始化但结果未使用 → 完整数据流整合

---

### 🔗 与 PDDA 的集成

> **重要**: v2.1 版本已针对 PDDA 输出格式进行优化

#### PDDA 数据流

```
原始数据（10年×1800家）
    │
    ▼
trend 层（8个探针）
    │ - 时间衰减已在此完成（WLS，半衰期~5年）
    │ - 输出：每家公司每指标 1 行
    ▼
PDDA 聚合
    │ aggregated_trends = {
    │     "roic": DataFrame[1800行, 40列],
    │     "roe": DataFrame[1800行, 40列],
    │     ...
    │ }
    ▼
Evaluators v2.1（本模块）
    │ - 直接消费 PDDA 单行聚合结果
    │ - 不再重复做时间衰减
    │ - 充分利用 PDDA 的布尔特征
    │ - 🆕 六大组件完整整合
    ▼
评估结果
```

#### PDDA 列名映射

v2.1 引擎通过 `PDDAColumns` 常量类与 trend 层输出对齐：

```python
# engine.py 中的 PDDAColumns 类
PDDAColumns.SLOPE          → "{metric}_slope"
PDDAColumns.ROBUST_SLOPE   → "{metric}_robust_slope"
PDDAColumns.CV             → "{metric}_cv"
PDDAColumns.HAS_DETERIORATION → "{metric}_has_deterioration"
PDDAColumns.LATEST_VALUE   → "{metric}_latest_value"
...
```

---

### 🏗️ 架构总览（v2.1 更新）

```
┌──────────────────────────────────────────────────────────────────────────┐
│                      CausalBayesianEvaluator v2.1                        │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐                │
│  │ PDDAColumns   │  │ adaptive_     │  │ causal_       │                │
│  │ 列名映射       │  │ threshold.py  │  │ graph.py      │                │
│  │ (trend对齐)    │  │ 自适应阈值    │  │ 因果DAG       │                │
│  └──────┬────────┘  └──────┬────────┘  └──────┬────────┘                │
│         │                  │                  │                          │
│  ┌──────▼──────────────────▼──────────────────▼────────┐                │
│  │           _extract_features_from_pdda               │                │
│  │    直接映射 PDDA 单行输出（不做时间衰减）           │                │
│  └─────────────────────────┬───────────────────────────┘                │
│                            │                                             │
│  ┌─────────────────────────▼───────────────────────────┐                │
│  │                  🆕 11步完整评估流程                  │                │
│  │                                                      │                │
│  │  ┌─────────────────────────────────────────────┐    │                │
│  │  │ Step 3: 🆕 RuleEngine (首先执行)            │    │                │
│  │  │   - 一票否决检查                            │    │                │
│  │  │   - 惩罚/奖励规则                           │    │                │
│  │  │   - 策略模式识别                            │    │                │
│  │  └─────────────────────────────────────────────┘    │                │
│  │                         │                           │                │
│  │  ┌──────────────────────▼──────────────────────┐    │                │
│  │  │ Step 4-5: StateMachine + Evidence收集       │    │                │
│  │  │   - HMM状态推断                             │    │                │
│  │  │   - 🆕 正确的键名映射                        │    │                │
│  │  └──────────────────────┬──────────────────────┘    │                │
│  │                         │                           │                │
│  │  ┌──────────────────────▼──────────────────────┐    │                │
│  │  │ Step 6-7: Copula + Dempster-Shafer          │    │                │
│  │  │   - 🆕 Copula 结果影响评分                   │    │                │
│  │  │   - 🆕 DS 动态 target 选择                  │    │                │
│  │  └──────────────────────┬──────────────────────┘    │                │
│  │                         │                           │                │
│  │  ┌──────────────────────▼──────────────────────┐    │                │
│  │  │ Step 8: 🆕 CausalGraph 诊断                 │    │                │
│  │  │   - Pearl do-calculus                       │    │                │
│  │  │   - 🆕 诊断结果影响评分 (±15%)              │    │                │
│  │  └──────────────────────┬──────────────────────┘    │                │
│  │                         │                           │                │
│  │  ┌──────────────────────▼──────────────────────┐    │                │
│  │  │ Step 9-10: 🆕 综合评分 + 综合决策            │    │                │
│  │  │   - 6维度评分整合                           │    │                │
│  │  │   - 4信号源置信度计算                       │    │                │
│  │  └──────────────────────┬──────────────────────┘    │                │
│  │                         │                           │                │
│  └─────────────────────────┼───────────────────────────┘                │
│                            │                                             │
│  ┌─────────────────────────▼───────────────────────────┐                │
│  │              explanation.py                          │                │
│  │     可解释AI - 生成人类可读决策解释                  │                │
│  └─────────────────────────┬───────────────────────────┘                │
│                            │                                             │
│                            ▼                                             │
│                   CompanyEvaluation                                      │
│    {score, decision, factors, rule_result, vetoed, explanation}         │
└──────────────────────────────────────────────────────────────────────────┘
```

---

### 📁 文件结构

```
evaluators/
├── config/                          # YAML配置
│   ├── rules.yaml                  # 29条规则定义（veto/penalty/bonus）
│   ├── adaptive_thresholds.yaml    # 自适应阈值
│   ├── state_machine.yaml          # 状态转移矩阵
│   └── causal_structure.yaml       # 因果图结构
│
├── adaptive_threshold.py            # 自适应阈值（~250行）
├── causal_graph.py                  # 因果推断（~350行）
├── state_machine.py                 # HMM状态机（~350行）
├── copula_fusion.py                 # Copula融合（~300行）
├── dempster_shafer.py              # DS证据理论（~350行）
├── rule_engine.py                   # 🆕 YAML规则引擎（~400行）
├── explanation.py                   # 决策解释（~300行）
├── engine.py                        # 主引擎（~1500行，v2.1 完整整合）
└── __init__.py                      # 公共API
```

**总代码量**: ~3800行（功能更完整，组件全部闭环）

---

### 🆕 v2.1 完整评估流程（11步）

```python
def evaluate_company(ts_code, trend_data, company_info):
    """
    完整的 11 步评估流程
    """
    # 1. 提取 PDDA 特征
    features = _extract_features_from_pdda(company_trends)

    # 2. 创建自适应上下文（行业/市值）
    context = _create_adaptive_context(company_info)

    # 3. 🆕【规则引擎】首先执行，检查一票否决
    rule_result = _run_rule_engine(features, context)
    if rule_result.vetoed:
        return CompanyEvaluation(decision=VETO, veto_reason=...)

    # 4. 推断公司状态（HMM）
    state_inference = _infer_company_state(features)

    # 5. 收集证据（充分利用 PDDA 布尔特征）
    evidences = _collect_evidences(features, context)

    # 6. 🆕 Copula 融合（结果影响评分）
    copula_result = _copula_fusion.fuse(evidences)

    # 7. 🆕 Dempster-Shafer 融合（动态 target）
    ds_result = _ds_evaluate_with_dynamic_target(evidences, features)

    # 8. 🆕 因果诊断（Pearl do-calculus，影响评分）
    causal_diagnosis = _run_causal_diagnosis(features)
    causal_adjustment = _compute_causal_adjustment(causal_diagnosis)

    # 9. 🆕 计算综合评分（6维度整合）
    score, factors = _compute_integrated_score(
        features, state_inference, copula_result,
        ds_result, rule_result, causal_adjustment, context
    )

    # 10. 🆕 做出综合决策（4信号源）
    decision, confidence = _make_integrated_decision(
        score, ds_result, state_inference, copula_result, rule_result
    )

    # 11. 生成解释
    explanation = _generate_explanation(...)

    return CompanyEvaluation(...)
```

---

### 🔬 六大核心组件（全部闭环）

#### 1. 🆕 规则引擎（YAML 声明式）

```yaml
# config/rules.yaml - 29条规则
veto_rules:
  negative_roic:
    condition: "roic_latest < 0"
    description: "ROIC为负，资本回报能力丧失"

penalty_rules:
  declining_margin:
    condition: "gross_margin_trend < -0.02"
    penalty: 15
    description: "毛利率持续恶化"

bonus_rules:
  consistent_growth:
    condition: "revenue_trend > 0.05 and roic_trend > 0.01"
    bonus: 10
    description: "营收增长且资本效率提升"

strategies:
  high_growth:
    conditions:
      - "revenue_trend > 0.15"
      - "roic_trend > 0.02"
    description: "高成长型公司"
```

```python
# engine.py 中的调用
rule_result = self._run_rule_engine(features, context)
# → RuleEngineResult(vetoed=False, total_penalty=15, total_bonus=10, strategies=["high_growth"])
```

#### 2. 因果推断（Pearl do-calculus）

```python
# 不只是检测"ROIC下降"，还能推断"为什么下降"
diagnosis = graph.diagnose(target_metric="roic_trend", observed_data={...})
# → "ROIC下降主要由毛利率恶化引起（贡献度0.45）"

# 🆕 v2.1: 诊断结果影响评分
causal_adjustment = _compute_causal_adjustment(diagnosis)
# → 正向诊断（改善中）+5分，负向诊断（恶化中）-10分
```

#### 3. HMM状态机

```python
# 推断公司所处生命周期阶段
inference = infer_company_state(features)
# → StateInference(state=GROWTH, prob=72%, quality_class=QUALITY)

# 🆕 v2.1: 修复键名映射
# revenue_growth → revenue_trend (正确映射)
# profit_growth → profit_trend
```

#### 4. Copula 相关性建模

```python
# 处理ROIC↔ROE高度相关（ρ=0.75）导致的独立性假设失效
result = copula_fusion.fuse(evidences)
# → effective_evidence_count=3.2（而非名义上的6条）

# 🆕 v2.1: 有效证据数影响评分
efficiency = effective_count / nominal_count
if efficiency < 0.5:
    score_penalty = -(1 - efficiency) * 10  # 证据高度相关惩罚
```

#### 5. Dempster-Shafer 不确定性

```python
# 显式处理"不知道"，区分"支持"vs"反对"vs"不确定"
result = ds_evaluator.evaluate("quality")
# → [Bel=0.75, Pl=0.92]，置信度区间而非单点估计

# 🆕 v2.1: 动态 target 选择
# 正面证据 → target="support"
# 负面证据 → target="oppose"
# 混合证据 → target="quality"
```

#### 6. 🆕 自适应阈值（修复 fallback）

```python
# 按行业/市值动态调整阈值
grade = _get_adaptive_grade(metric, value, context)

# 🆕 v2.1: 修复 fallback 逻辑
# 对于趋势数据，使用合理的分段阈值
# value > 0.03 → "excellent"
# value > 0.01 → "good"
# value > -0.01 → "acceptable"
# value > -0.03 → "poor"
# else → "veto"
```

---

### 📐 评分算法（v2.1 六维度整合）

```
最终分数 = 基础分数
         + 规则引擎调整 (penalty/bonus)
         + 状态机调整 (state_adjustment)
         + 因果诊断调整 (causal_adjustment, ±15%)
         + Copula 效率惩罚 (copula_adjustment)
         + DS 冲突惩罚 (ds_adjustment)
```

### 📊 置信度计算（v2.1 四信号源加权）

```
置信度 = 0.40 × DS置信度
       + 0.25 × 状态机置信度
       + 0.20 × Copula效率
       + 0.15 × 规则引擎置信度
```

---

### 🚀 使用方法

#### 基础用法

```python
from src.astock.business_engines.evaluators import (
    CausalBayesianEvaluator,
    EvaluatorConfig,
    evaluate_single_company
)

# 方式1：使用主引擎（推荐）
config = EvaluatorConfig(
    use_rule_engine=True,        # 启用规则引擎
    use_causal_inference=True,   # 启用因果推断
    use_state_machine=True,      # 启用状态机
    rule_veto_enabled=True,      # 允许规则引擎一票否决
    causal_score_weight=0.15,    # 因果诊断权重
)
evaluator = CausalBayesianEvaluator(config)

result = evaluator.evaluate_company(
    ts_code="000001.SZ",
    trend_data=aggregated_trends,  # 来自 PDDA（每公司每指标 1 行）
    company_info={"name": "平安银行", "industry": "银行", "market_cap": 3000}
)

print(result.score)           # 78.5
print(result.decision)        # DecisionType.QUALITY
print(result.company_state)   # CompanyState.MATURE
print(result.vetoed)          # False (🆕 是否被一票否决)
print(result.rule_result)     # 🆕 RuleEngineResult(...)
print(result.explanation.summary)  # "平安银行被评估为【优质公司】..."

# 方式2：便捷函数
result = evaluate_single_company("000001.SZ", aggregated_trends)
```

#### Pipeline集成

```yaml
# workflow/analysis.yaml
steps:
  - name: Evaluate_Companies
    method: causal_bayesian_evaluator
    depends_on:
      - Analyze_ROIC_Trend
      - Analyze_ROE_Trend
      # ... 其他趋势分析步骤
    params:
      use_causal_inference: true
      use_state_machine: true
      use_rule_engine: true        # 🆕 启用规则引擎
      rule_veto_enabled: true      # 🆕 启用一票否决
      causal_score_weight: 0.15    # 🆕 因果诊断权重
    # PDDA 会自动注入 aggregated_trends 参数
```

---

### ⚙️ 配置说明

#### 核心配置项（EvaluatorConfig）

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `use_adaptive_thresholds` | true | 是否启用自适应阈值 |
| `use_causal_inference` | true | 是否启用因果推断 |
| `use_state_machine` | true | 是否启用状态机 |
| `evidence_correlation_default` | 0.3 | 默认证据相关系数 |
| `ds_conflict_threshold` | 0.7 | DS冲突阈值 |
| `quality_threshold` | 70.0 | 优质公司分数阈值 |
| `average_threshold` | 50.0 | 一般公司分数阈值 |
| `veto_threshold` | 30.0 | 否决分数阈值 |
| 🆕 `use_rule_engine` | true | 是否启用规则引擎 |
| 🆕 `rule_veto_enabled` | true | 是否允许规则引擎一票否决 |
| 🆕 `causal_score_weight` | 0.15 | 因果诊断对评分的影响权重 |
| 🆕 `copula_confidence_weight` | 0.3 | Copula效率对置信度的影响 |

> **注意**: `half_life_years` 和 `min_time_weight` 已移除，因为时间衰减由 trend 层完成。

#### 自定义规则

编辑 `config/rules.yaml`:

```yaml
veto_rules:
  negative_roic:
    enabled: true
    condition: "roic_latest < 0"
    description: "ROIC为负，资本回报能力丧失"

  severe_margin_collapse:
    enabled: true
    condition: "gross_margin_trend < -0.05 and net_margin_trend < -0.03"
    description: "毛利率和净利率同时大幅恶化"

penalty_rules:
  declining_margin:
    condition: "gross_margin_trend < -0.02"
    penalty: 15
    description: "毛利率持续恶化"

bonus_rules:
  consistent_growth:
    condition: "revenue_trend > 0.05 and roic_trend > 0.01"
    bonus: 10
    description: "营收增长且资本效率提升"
```

---

### 📊 输出格式

```python
{
    "evaluations": [
        {
            "ts_code": "000001.SZ",
            "score": 78.5,
            "decision": "quality",
            "confidence": 0.82,
            "company_state": "mature",
            "vetoed": false,                    # 🆕 是否被一票否决
            "veto_reason": "",                  # 🆕 否决原因
            "rule_result": {                    # 🆕 规则引擎结果
                "vetoed": false,
                "total_penalty": 5,
                "total_bonus": 10,
                "triggered_rules": ["consistent_growth"],
                "strategies": ["stable_growth"]
            },
            "factors": [
                {"name": "roic_trend", "value": 0.02, "contribution": 0.35},
                {"name": "rule_engine", "value": 5, "contribution": 0.05},  # 🆕
                {"name": "causal_diagnosis", "value": 3, "contribution": 0.03},  # 🆕
                ...
            ]
        },
        ...
    ],
    "summary": {
        "total_evaluated": 500,
        "quality_count": 45,
        "veto_count": 32,
        "average_score": 52.3
    },
    "quality_companies": ["000001.SZ", "000002.SZ", ...],
    "veto_companies": ["000010.SZ", ...]
}
```

---

### 🔄 版本对比

| 维度 | v1 | v2.0 | v2.1 |
|------|-----|-----|------|
| 代码量 | ~3500行/18文件 | ~2500行/10文件 | ~3800行/10文件 |
| 规则定义 | Python硬编码 | YAML声明式 | YAML + 规则引擎 |
| 相关性处理 | 假设独立 | Copula建模 | Copula + 评分整合 |
| 因果分析 | 无 | Pearl do-calculus | Pearl + 评分调整 |
| 状态建模 | 无 | HMM状态机 | HMM + 键名修复 |
| 不确定性 | 单点估计 | DS区间 | DS动态target |
| 规则引擎 | 无 | 初始化未使用 ❌ | 完整整合 ✅ |
| 组件闭环 | - | 部分孤岛 ❌ | 全部闭环 ✅ |
| 可解释性 | 基础 | 完整因果链 | 因果链+规则链 |

---

### 🐛 v2.1 修复的问题

| 优先级 | 问题 | 修复方案 |
|--------|------|----------|
| **P0** | RuleEngine 初始化但从未调用 | 新增 `_run_rule_engine()`，支持一票否决 |
| **P0** | Copula 结果完全未使用 | 利用 `effective_evidence_count` 计算评分惩罚 |
| **P1** | 因果诊断不影响决策 | 新增 `_compute_causal_adjustment()` 方法 |
| **P1** | DS 证据 target 全部硬编码为 "quality" | 新增 `_ds_evaluate_with_dynamic_target()` |
| **P2** | 状态机键名不匹配 | 在 `_infer_company_state()` 添加键名映射 |
| **P2** | 自适应阈值 fallback 硬编码 | 修复 `_get_adaptive_grade()` 的分段阈值 |

---

### 📚 理论参考

1. **因果推断**: Pearl, J. (2009). *Causality: Models, Reasoning, and Inference*
2. **Dempster-Shafer**: Shafer, G. (1976). *A Mathematical Theory of Evidence*
3. **Copula理论**: Nelsen, R.B. (2006). *An Introduction to Copulas*
4. **隐马尔可夫模型**: Rabiner, L.R. (1989). *A Tutorial on HMM*

---

### 🛠️ 开发指南

#### 添加新的证据源

1. 在 `engine.py` 的 `_collect_evidences()` 中添加:

```python
metric_configs.append(("new_metric", threshold, confidence))
```

2. 在 `copula_fusion.py` 的 `_preset_correlations` 中添加相关系数:

```python
("new_metric", "roic"): 0.4,
```

#### 添加新的规则

编辑 `config/rules.yaml`:

```yaml
# 一票否决规则
veto_rules:
  new_veto_rule:
    enabled: true
    condition: "metric_value < threshold"
    description: "规则描述"

# 惩罚规则
penalty_rules:
  new_penalty_rule:
    condition: "metric_value < threshold"
    penalty: 10  # 扣分值
    description: "规则描述"

# 奖励规则
bonus_rules:
  new_bonus_rule:
    condition: "metric_value > threshold"
    bonus: 5  # 加分值
    description: "规则描述"
```

#### 添加新的状态

编辑 `config/state_machine.yaml`:

```yaml
states:
  new_state:
    name: "新状态"
    characteristics:
      revenue_growth: [0.0, 0.1]
    quality_class: "uncertain"

transition_matrix:
  existing_state:
    new_state: 0.15
```

---

### ⚠️ 注意事项

1. **依赖**: 需要 `numpy`, `scipy`, `pyyaml`, `pandas`
2. **性能**: 单公司评估 ~15ms，批量500公司 ~8s
3. **数据要求**: 需要至少5年历史数据以获得可靠的状态推断
4. **冲突处理**: 当DS冲突系数>0.7时，结果置信度自动降低
5. **规则引擎**: 一票否决优先于所有其他评估逻辑

---

### 📝 更新日志

#### v2.1 (2026-02-03)
- 🔧 修复 RuleEngine 未被调用的问题
- 🔧 修复 Copula 结果未影响评分的问题
- 🔧 修复因果诊断结果未影响决策的问题
- 🔧 修复 DS 证据 target 硬编码问题
- 🔧 修复状态机键名映射问题
- 🔧 修复自适应阈值 fallback 逻辑
- ✨ 新增 `_run_rule_engine()` 方法
- ✨ 新增 `_compute_causal_adjustment()` 方法
- ✨ 新增 `_compute_integrated_score()` 方法
- ✨ 新增 `_make_integrated_decision()` 方法
- ✨ 新增 `_ds_evaluate_with_dynamic_target()` 方法
- ✨ 新增 `_get_adaptive_grade()` 方法
- 📦 新增配置项: `causal_score_weight`, `use_rule_engine`, `rule_veto_enabled`, `copula_confidence_weight`
- 📦 新增数据字段: `rule_result`, `vetoed`, `veto_reason`

#### v2.0 (2025-12)
- 初始版本，引入六大核心组件

---

*AStock Evaluators v2.1 - 让量化评估更智能、更可解释、更完整*
