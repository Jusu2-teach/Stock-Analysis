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
# ========== 综合报告 & T.R.U.T.H. 报告生成 ==========
# 依赖所有趋势分析步骤完成后才执行

# 🔵 规则驱动报告（保留现有）
- name: "Generate_Comprehensive_Report"
  component: "business_engine"
  engine: "reporting"
  method: ["report_comprehensive"]
  parameters:
    roic_data: "steps.Analyze_ROIC_Trend.outputs.parameters.ROIC_Trend_Result"
    roe_data: "steps.Analyze_ROE_Trend.outputs.parameters.ROE_Trend_Result"
    roiic_data: "steps.Analyze_ROIIC_Trend.outputs.parameters.ROIIC_Trend_Result"
    gross_margin_data: "steps.Analyze_GrossMargin_Trend.outputs.parameters.GrossMargin_Trend_Result"
    net_margin_data: "steps.Analyze_NetMargin_Trend.outputs.parameters.NetMargin_Trend_Result"
    revenue_data: "steps.Analyze_Revenue_Trend.outputs.parameters.Revenue_Trend_Result"
    profit_data: "steps.Analyze_Profit_Trend.outputs.parameters.Profit_Trend_Result"
    ocf_data: "steps.Analyze_OCF_Trend.outputs.parameters.OCF_Trend_Result"
    output_path: "data/comprehensive_analysis_report.md"
  outputs:
    parameters:
      - name: Comprehensive_Report_Path

# 🧬 T.R.U.T.H. 处理（专业基因提取 + 求解器）
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

# 🟢 T.R.U.T.H. 数据驱动报告（使用处理后的结果）
- name: "Generate_Truth_Report"
  component: "business_engine"
  engine: "reporting"
  method: ["report_truth"]
  parameters:
    truth_processed: "steps.Process_Truth_System.outputs.parameters.Truth_Processed_Results"
    output_path: "data/truth_analysis_report.json"
  outputs:
    parameters:
      - name: Truth_Report_Path
```

---

## T.R.U.T.H. 报告引擎内部流程

```text
report_truth(truth_processed) 内部:

1. 接收 TruthProcessor 的处理结果
  ├── processed_results: BatchProcessResult
  ├── results_df: 每家公司基因/求解器结果 DataFrame
  └── probe_data: 8 个指标的探针 DataFrame（来自 trend 引擎）

2. 根据 results_df 生成每家公司的 T.R.U.T.H. 评分和信号

3. 使用 probe_data 提供的趋势特征做必要的补充解释（如趋势图、波动模式说明）

4. 组装多层级报告结构
  └── TruthResult → JSON/Markdown
```

---

## 实际实现位置

- 真正的 T.R.U.T.H. 处理入口在 `truth/truth_engine.py`:
  - `process_truth`: 批量处理 8 个探针 DataFrame，输出 `processed_results/results_df/summary/probe_data`。
  - `process_truth_single`: 针对单只股票的深度处理。
- 报告生成入口在 `reporters/engine.py`:
  - `report_truth`: 接收 `truth_processed`（即上面的字典）和可选 `probe_data`，生成 Markdown 报告。

这两个入口都只依赖内存中的 DataFrame，不再从 `data/filter_middle/*.csv` 读取数据。
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
