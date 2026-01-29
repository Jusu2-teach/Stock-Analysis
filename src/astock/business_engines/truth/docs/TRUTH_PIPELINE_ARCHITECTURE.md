# TRUTH 流水线架构

TRUTH 是一个独立的“六因子 + 三求解器”子系统，只通过业务引擎接口
与 workflow/pipeline 交互：

```text
原始数据 → 多指标探针分析 → TRUTH 因子/求解器 → TRUTH 报告

Step 1: Analyze_*_Trend  (trend 引擎, 产生 probes)
Step 2: Run_TRUTH        (truth 引擎, 六因子 + 三求解器)
Step 3: Generate_Truth_Report (reporting 引擎, 报告渲染)
```

---

## Pipeline 数据流

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
Step 3: Run_TRUTH
  │   component: "business_engine"
  │   engine:    "truth"
  │   method:    ["run_truth"]
  │   输入: 8 个 Analyze_*_Trend 的输出 DataFrame
  │   输出: metadata + profiles (六因子 + 三求解器的结果)
  ▼
Step 4: Generate_Truth_Report
    component: "business_engine"
    engine:    "reporting"
    method:    ["report_truth"]
    输入: Run_TRUTH 的 truth_result
    输出: data/truth_analysis_report.md
```

---

## 接入方式

### 在 analysis.yaml 中接入 TRUTH 报告

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

# 🧬 TRUTH 处理
- name: "Run_TRUTH"
  component: "business_engine"
  engine: "truth"
  method: ["run_truth"]
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
      - name: Truth_Result

# 🟢 TRUTH 报告
- name: "Generate_Truth_Report"
  component: "business_engine"
  engine: "reporting"
  method: ["report_truth"]
  parameters:
    truth_result: "steps.Run_TRUTH.outputs.parameters.Truth_Result"
    output_path: "data/truth_analysis_report.md"
  outputs:
    parameters:
      - name: Truth_Report_Path
```

---

## TRUTH 内部流程（简要）

```text
run_truth(**probe_frames):

1. 将 8 个探针 DataFrame (roic_data/roe_data/...) 统一转换为 ProbeBatch
2. 对每只股票:
   - 运行六个因子 F1~F6, 得到 FactorResult 向量
   - 基于因子向量运行三个求解器 S1~S3, 得到 SolverResult 向量
   - 组合成 TruthProfile(ts_code, factors, solvers)
3. 汇总为 TruthRunResult(metadata + profiles)

report_truth(truth_result):

1. 读取 metadata, 输出 run 的版本与覆盖范围
2. 遍历 profiles, 以表格形式展示每只股票的因子/求解器分数
3. 渲染为 Markdown 文件 truth_analysis_report.md
```

---

## 实际实现位置

- TRUTH 处理入口在 `src/astock/business_engines/truth/integration/engine.py`:
  - `run_truth`: 接收多个探针 DataFrame, 统一构建 ProbeBatch, 运行六因子/三求解器, 返回 TruthRunResult 的扁平字典视图。
- 报告生成入口在 `src/astock/business_engines/reporters/engine.py`:
  - `report_truth`: 接收 `truth_result` (run_truth 的返回值), 生成 Markdown 报告。

业务引擎注册在 `src/astock/business_engines/__init__.py` 中通过 Registry.scan 完成。
```

---

## 两种报告对比

| 维度 | 🔵 report_comprehensive | 🟢 report_truth |
|-----|------------------------|-------------------|
| **阈值来源** | 预设（行业/规模表） | 无预设阈值，六因子直接打分 |
| **核心假设** | 同行业/规模可比 | 每家公司特征独特 |
| **评分方式** | 加权评分（满分100） | 六因子 → 三求解器 → 综合向量 |
| **输出形式** | Markdown 报告 | Markdown (可扩展 JSON) |
| **适用场景** | 快速筛选、批量分析 | 深度研究、个股分析 |
| **后续用途** | 对比准确性基准 | 作为新版 TRUTH 主口径 |

---

## 总结

```text
最终形态:

原始数据 → 探针分析 → ┬─→ 🔵 规则驱动报告 (有阈值)
       │      comprehensive_analysis_report.md
       │
      └─→ 🟢 TRUTH 数据驱动报告 (六因子 + 三求解器)
         truth_analysis_report.md
```

---

*文档版本: v1.1 (简化版)*
*最后更新: 2025-01*
