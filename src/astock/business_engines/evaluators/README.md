# AStock Evaluators v2.0

## 因果贝叶斯网络 + 状态机评估引擎

### 🎯 设计目标

解决 v1 版本的核心问题：
1. **过度工程化** - 23个Python类 → 6个核心模块
2. **规则硬编码** - 硬编码规则 → YAML声明式配置
3. **假设独立性** - 忽略指标相关性 → Copula建模
4. **相关≠因果** - 相关性分析 → Pearl因果推断
5. **静态评估** - 静态快照 → HMM状态机建模生命周期

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
Evaluators v2（本模块）
    │ - 直接消费 PDDA 单行聚合结果
    │ - 不再重复做时间衰减
    │ - 充分利用 PDDA 的布尔特征
    ▼
评估结果
```

#### PDDA 列名映射

v2 引擎通过 `PDDAColumns` 常量类与 trend 层输出对齐：

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

### 🏗️ 架构总览

```
┌─────────────────────────────────────────────────────────────────────┐
│                    CausalBayesianEvaluator                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │ PDDAColumns  │  │ adaptive_    │  │ causal_      │              │
│  │ 列名映射      │  │ threshold.py │  │ graph.py     │              │
│  │ (trend对齐)   │  │ 自适应阈值   │  │ 因果DAG      │              │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘              │
│         │                 │                 │                       │
│  ┌──────▼─────────────────▼─────────────────▼───────┐              │
│  │          _extract_features_from_pdda              │              │
│  │   直接映射 PDDA 单行输出（不做时间衰减）          │              │
│  └──────────────────────┬────────────────────────────┘              │
│                         │                                           │
│  ┌──────────────────────▼────────────────────────────┐              │
│  │  ┌─────────────────┐    ┌─────────────────────┐   │              │
│  │  │ copula_fusion.py│    │ dempster_shafer.py  │   │              │
│  │  │ Copula相关性建模│    │ DS证据融合          │   │              │
│  │  │ (处理ROIC↔ROE)  │    │ (不确定性区间)      │   │              │
│  │  └────────┬────────┘    └──────────┬──────────┘   │              │
│  │           └────────────┬───────────┘              │              │
│  └────────────────────────┼──────────────────────────┘              │
│                           │                                         │
│  ┌────────────────────────▼──────────────────────────┐              │
│  │              state_machine.py                      │              │
│  │     HMM公司生命周期推断                            │              │
│  │  EMERGING → GROWTH → MATURE → CASH_COW → DECLINING │              │
│  └────────────────────────┬──────────────────────────┘              │
│                           │                                         │
│  ┌────────────────────────▼──────────────────────────┐              │
│  │              explanation.py                        │              │
│  │     可解释AI - 生成人类可读决策解释                │              │
│  └────────────────────────┬──────────────────────────┘              │
│                           │                                         │
│                           ▼                                         │
│                  CompanyEvaluation                                  │
│         {score, decision, factors, explanation}                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

### 📁 文件结构

```
evaluators/v2/
├── config/                          # YAML配置
│   ├── rules.yaml                  # 规则定义
│   ├── adaptive_thresholds.yaml    # 自适应阈值
│   ├── state_machine.yaml          # 状态转移矩阵
│   └── causal_structure.yaml       # 因果图结构
│
├── temporal.py                      # 时间衰减（保留，但主流程不使用）
├── adaptive_threshold.py            # 自适应阈值（~250行）
├── causal_graph.py                  # 因果推断（~350行）
├── state_machine.py                 # HMM状态机（~350行）
├── copula_fusion.py                 # Copula融合（~300行）
├── dempster_shafer.py              # DS证据理论（~350行）
├── explanation.py                   # 决策解释（~300行）
├── engine.py                        # 主引擎（~500行）
└── __init__.py                      # 公共API
```

**总代码量**: ~2800行（v1为~3500行，减少20%，但功能更强）

---

### 🔬 五大核心创新

#### 1. 因果推断（Pearl do-calculus）

```python
# 不只是检测"ROIC下降"，还能推断"为什么下降"
graph = create_financial_causal_graph()
diagnosis = graph.diagnose(
    target_metric="roic_trend",
    observed_data={"gross_margin_trend": -0.02, "revenue_trend": 0.10}
)
# → "ROIC下降主要由毛利率恶化引起（贡献度0.45），而非营收问题"
```

#### 2. HMM状态机

```python
# 推断公司所处生命周期阶段
inference = infer_company_state(
    revenue_growth=0.25,
    roic_level=18.0,
    roic_trend=0.03
)
# → StateInference(state=GROWTH, prob=72%, quality_class=QUALITY)
```

#### 3. Copula相关性建模

```python
# 处理ROIC↔ROE高度相关（ρ=0.75）导致的独立性假设失效
fusion = CopulaEvidenceFusion()
result = fusion.fuse(evidences)
# → effective_evidence_count=3.2（而非名义上的6条）
```

#### 4. Dempster-Shafer不确定性

```python
# 显式处理"不知道"，区分"支持"vs"反对"vs"不确定"
evaluator = DSEvidenceEvaluator()
evaluator.add_evidence("roic", belief=0.7, disbelief=0.1, uncertainty=0.2)
result = evaluator.evaluate("quality")
# → [Bel=0.75, Pl=0.92]，置信度区间而非单点估计
```

#### 5. PDDA 特征充分利用

```python
# v2.1 充分利用 PDDA 的布尔特征作为证据
# 不仅是连续趋势值，还包括：
# - has_deterioration → 直接作为否定证据
# - volatility_type → 分类映射为证据强度
# - is_cyclical + cycle_phase → 周期底部是积极信号
# - has_structural_break → 增加不确定性
```

---

### 🚀 使用方法

#### 基础用法

```python
from src.astock.business_engines.evaluators.v2 import (
    CausalBayesianEvaluator,
    evaluate_single_company
)

# 方式1：使用主引擎
evaluator = CausalBayesianEvaluator()
result = evaluator.evaluate_company(
    ts_code="000001.SZ",
    trend_data=aggregated_trends,  # 来自 PDDA（每公司每指标 1 行）
    company_info={"name": "平安银行", "industry": "银行", "market_cap": 3000}
)

print(result.score)           # 78.5
print(result.decision)        # DecisionType.QUALITY
print(result.company_state)   # CompanyState.MATURE
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
    # PDDA 会自动注入 aggregated_trends 参数
```

---

### ⚙️ 配置说明

#### 核心配置项

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

> **注意**: `half_life_years` 和 `min_time_weight` 已移除，因为时间衰减由 trend 层完成。

#### 自定义规则

编辑 `config/rules.yaml`:

```yaml
veto_rules:
  negative_roic:
    enabled: true
    condition: "roic_level < 0"
    description: "ROIC为负，资本回报能力丧失"
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
            "factors": [
                {"name": "roic_trend", "value": 0.02, "contribution": 0.35},
                {"name": "roe_trend", "value": 0.01, "contribution": 0.20},
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

### 🔄 与v1对比

| 维度 | v1 | v2 |
|------|-----|-----|
| 代码量 | ~3500行/18文件 | ~2500行/10文件 |
| 规则定义 | Python硬编码 | YAML声明式 |
| 相关性处理 | 假设独立 | Copula建模 |
| 因果分析 | 无 | Pearl do-calculus |
| 状态建模 | 无 | HMM状态机 |
| 不确定性 | 单点估计 | DS区间 |
| 时间加权 | 无 | 指数衰减 |
| 可解释性 | 基础 | 完整因果链 |

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
2. **性能**: 单公司评估 ~10ms，批量500公司 ~5s
3. **数据要求**: 需要至少5年历史数据以获得可靠的状态推断
4. **冲突处理**: 当DS冲突系数>0.7时，结果置信度自动降低

---

*AStock Evaluators v2.0 - 让量化评估更智能、更可解释*
