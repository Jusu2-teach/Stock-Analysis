# OOS Validation Framework v1.0

> Out-of-Sample 验证框架: 独立验证 AStock 分析系统的稳健性

## 设计理念

系统有 50+ 手工调参的超参数，所有指标 (ρ=0.814, signal=75.5%) 均在同一数据集上测量。
OOS 验证回答一个核心问题: **这些指标是真实信号还是过拟合噪声？**

## 四大验证策略

| 策略 | 原理 | 测试什么 | 通过标准 |
|------|------|----------|----------|
| **Monte Carlo 参数扰动** | 对所有超参数加高斯噪声 | 参数精确值是否关键 | ±5%→ρ≥0.95, ±10%→ρ≥0.90 |
| **公司自举** | 随机抽 80% 公司重跑 | 个别公司是否支配排名 | ρ≥0.90 |
| **因子消融** | 逐一移除因子 | 哪些因子影响最大 | 单因子影响 ≤0.10 |
| **双引擎一致性** | TRUTH vs Evaluator 对比 | 信号来自数据还是模型 | ρ≥0.75 |

## 使用方法

```bash
# 完整运行 (默认配置, ~10-15 分钟)
python -m oos_validation

# 快速模式 (减少迭代, ~5 分钟)
python -m oos_validation --fast

# 仅运行特定策略
python -m oos_validation --only perturb    # 参数扰动
python -m oos_validation --only bootstrap  # 公司自举
python -m oos_validation --only ablation   # 因子消融
python -m oos_validation --only cross      # 双引擎一致性

# 自定义迭代次数和种子
python -m oos_validation --iterations 20 --seed 123

# 详细日志
python -m oos_validation -v
```

## 输出

报告生成在 `data/oos_validation_report.md`，包含:

1. **执行摘要**: OOS 稳健性评分 (0-100) + Pass/Fail 表
2. **参数扰动详情**: 每个噪声水平的 ρ、Jaccard、等级一致率
3. **公司自举详情**: 采样稳定性统计
4. **因子消融详情**: 每个因子/权重的影响程度分析
5. **双引擎一致性**: Spearman/Kendall/Jaccard/分类一致性
6. **结论与建议**: 基于测试结果的具体建议

## 架构

```
oos_validation/
├── __init__.py       # 包初始化
├── __main__.py       # CLI 入口 (python -m oos_validation)
├── runner.py         # 主编排器 (OOSValidator)
├── strategies.py     # 四大验证策略
├── metrics.py        # 统计指标 (Spearman/Kendall/Jaccard/...)
├── data_loader.py    # 从 CSV 重建 aggregated_trends
├── report.py         # Markdown 报告生成
└── README.md         # 本文档
```

## 数据依赖

框架从**缓存的趋势分析 CSV** 加载数据 (而非重跑趋势探针)：

| 文件 | 来源 |
|------|------|
| `data/filter_middle/*_trend_analysis.csv` | 8 个趋势探针输出 |
| `data/polars/10yd_final_industry.csv` | 原始财务数据 (用于 financial_context) |

**前置条件**: 必须先运行完整 pipeline 一次：
```bash
python -m pipeline run -c workflow/analysis.yaml
```

## Python API

```python
from pathlib import Path
from oos_validation.runner import OOSValidator, OOSConfig

config = OOSConfig(
    perturbation_iterations=10,
    bootstrap_iterations=20,
    seed=42,
)
validator = OOSValidator(Path("."), config)
results = validator.run()

# results 包含各策略的详细结果
print(results["perturbation"][0].truth_rhos)
print(results["bootstrap"].truth_rhos)
print(results["ablation"].items)
print(results["cross_engine"].spearman_rho)
```

## OOS 稳健性评分解读

| 评分 | 等级 | 含义 |
|------|------|------|
| 90-100 | 🟢 A | 极其稳健，无过拟合迹象 |
| 75-89 | 🟡 B | 稳健，轻微敏感性属正常 |
| 60-74 | 🟠 C | 轻微过拟合风险 |
| 40-59 | 🔴 D | 中度过拟合 |
| 0-39 | ⛔ F | 严重过拟合，需要改造 |
