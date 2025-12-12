# T.R.U.T.H. 报告引擎接入指南

## 核心思路：两套报告并存

**两套报告系统并行运行，方便后续对比准确性！**

```text
Pipeline 输出两份报告:

                              ┌─→ 🔵 report_comprehensive (规则驱动)
                              │      基于预设阈值（行业/规模）
原始数据 → 多指标探针分析 →─┤      输出: comprehensive_analysis_report.md
                              │
                              └─→ 🟢 report_truth (数据驱动)
                                     无预设阈值，数据说话
                                     输出: truth_analysis_report.json
```

**目的：对比两套模式的准确性**

---

## 现有 Pipeline 数据流

```text
workflow/analysis.yaml:

Step 1: Load_Financial_Data
    │   加载原始财务数据 (10年历史)
    │   字段: ts_code, roic, gross_margin, revenue, ocf...
    ▼
Step 2: Analyze_*_Trend (8个并行)
    │   ├── ROIC 趋势探针
    │   ├── ROIIC 趋势探针
    │   ├── Revenue 趋势探针
    │   ├── Profit 趋势探针
    │   ├── GrossMargin 趋势探针
    │   ├── NetMargin 趋势探针
    │   ├── ROE 趋势探针
    │   └── OCF 趋势探针
    │
    │   每个探针输出:
    │   - log_slope, r_squared, cagr
    │   - detrended_cv, volatility_type
    │   - cyclical_confidence, peak_to_trough
    │   - deterioration_probability
    │   - ...
    ▼
Step 3: store_*_Trend
    │   保存到 data/filter_middle/*.csv
    ▼
Step 4: Generate_*_Report  ← 【这里替换】
    │
    ├── 🔵 现有: report_comprehensive (规则驱动)
    │       engine: "reporting"
    │       method: ["report_comprehensive"]
    │       使用预设阈值 → 加权评分 → 排名
    │
    └── 🟢 新增: report_truth (数据驱动)
            engine: "truth"
            method: ["report_truth"]
            六大基因 → 三大求解器 → 动态阈值
```

---

## 接入方式

### 在 analysis.yaml 中添加 T.R.U.T.H. 报告（两个报告并行）

```yaml
# ========== 综合报告生成 (Cross-Analysis Report) ==========
# 依赖所有 store 步骤完成后才执行

# 🔵 规则驱动报告（保留现有）
- name: "Generate_Comprehensive_Report"
  component: "business_engine"
  engine: "reporting"
  method: ["report_comprehensive"]
  parameters:
    data_dir: "data/filter_middle"
    output_path: "data/comprehensive_analysis_report.md"
  depends_on:
    - "store_ROIC_Trend"
    - "store_ROIIC_Trend"
    - "store_Revenue_Trend"
    - "store_Profit_Trend"
    - "store_GrossMargin_Trend"
    - "store_NetMargin_Trend"
    - "store_ROE_Trend"
    - "store_OCF_Trend"
  outputs:
    parameters:
      - name: Comprehensive_Report_Path

# 🟢 T.R.U.T.H. 数据驱动报告（新增）
- name: "Generate_Truth_Report"
  component: "business_engine"
  engine: "truth"
  method: ["report_truth"]
  parameters:
    data_dir: "data/filter_middle"
    output_path: "data/truth_analysis_report.json"
  depends_on:
    - "store_ROIC_Trend"
    - "store_ROIIC_Trend"
    - "store_Revenue_Trend"
    - "store_Profit_Trend"
    - "store_GrossMargin_Trend"
    - "store_NetMargin_Trend"
    - "store_ROE_Trend"
    - "store_OCF_Trend"
  outputs:
    parameters:
      - name: Truth_Report_Path
```

---

## T.R.U.T.H. 报告引擎内部流程

```text
report_truth(data_dir) 内部:

1. 加载探针CSV
   ├── roic_trend_analysis.csv
   ├── gross_margin_trend_analysis.csv
   ├── revenue_trend_analysis.csv
   └── ocf_trend_analysis.csv

2. 按公司聚合 (per ts_code)
   └── MultiIndicatorProbeOutputs

3. 适配器转换
   └── ProbeAdapter.adapt() → GenomeInput

4. 六大基因计算
   ├── α 周期性基因 (Hurst门控)
   ├── β 资本密度基因 (DOL检测)
   ├── γ 成长动能基因 (稳健斜率)
   ├── δ_fraud 欺诈熵 (麦道夫检测)
   ├── δ_decay 衰退熵 (拐点预警)
   └── V 验证因子 (体制惩罚)

   → CompanyGenome

5. 三大物理求解器 (v3.1)
   ├── gravity_solver → T_threshold (动态阈值)
   ├── velocity_solver → T_growth (增长边界)
   └── structure_solver → T_slope (斜率通道)

6. 生成报告
   └── TruthResult → JSON/CSV/Markdown
```

---

## 需要实现的代码

只需要一个文件：`truth/engine.py` 中注册 `report_truth` 方法

```python
# src/astock/business_engines/truth/engine.py

@register_method(
    engine_name="report_truth",
    component_type="business_engine",
    engine_type="truth",
    description="T.R.U.T.H. 数据驱动报告引擎"
)
def report_truth(
    data_dir: str = "data/filter_middle",
    output_path: str = "data/truth_analysis_report.json"
) -> str:
    """
    T.R.U.T.H. 数据驱动报告生成

    读取探针分析结果 → 六大基因 → 三大求解器 → 动态阈值报告
    """
    # 1. 加载探针CSV
    # 2. 按公司聚合
    # 3. 计算基因组
    # 4. 运行求解器
    # 5. 生成报告
    ...
```

然后在 `__init__.py` 中注册 engine:

```python
# src/astock/business_engines/__init__.py

from .truth import engine as truth_engine

registry.register_multiple_methods_from_module(
    module=truth_engine,
    engine_name="truth",
    engine_type="truth",
    tags=("truth", "data-driven")
)
```

---

## 两种报告对比

| 维度 | 🔵 report_comprehensive | 🟢 report_truth |
|-----|------------------------|-----------------|
| **阈值来源** | 预设（行业/规模表） | 无阈值，动态计算 |
| **核心假设** | 同行业/规模可比 | 每家公司特征独特 |
| **评分方式** | 加权评分（满分100） | 六基因→三求解器→动态范围 |
| **输出形式** | Markdown 报告 | JSON + CSV + Markdown |
| **适用场景** | 快速筛选、批量分析 | 深度研究、个股分析 |
| **后续用途** | 对比准确性基准 | 对比准确性验证 |

---

## 总结

```text
两套报告并存:

原始数据 → 探针分析 → ┬─→ 🔵 规则驱动报告 (有阈值)
                      │      comprehensive_analysis_report.md
                      │
                      └─→ 🟢 数据驱动报告 (无阈值)
                             truth_analysis_report.json

后续对比两套模式的准确性，验证哪种方法更有效！
```

---

*文档版本: v1.1 (简化版)*
*最后更新: 2025-01*
