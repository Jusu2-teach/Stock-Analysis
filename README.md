# AStock Analysis System

![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![Architecture](https://img.shields.io/badge/Architecture-Modular-green)
![Engine](https://img.shields.io/badge/Engine-DuckDB%20%7C%20Polars-orange)

AStock Analysis 是一个现代化、高性能的 **A股基本面量化分析系统**。它采用 **Orchestrator (调度器) + Pipeline (编排器) + Business Plugins (业务插件)** 的分层架构，专注于通过**多年财务数据的趋势分析**，筛选优质企业、识别困境反转、预警风险公司。

---

## 🎯 系统功能

### 核心分析能力
| 功能模块 | 描述 |
|---------|------|
| **趋势分析引擎** | 基于对数回归、Mann-Kendall检验等统计方法，分析财务指标的长期趋势 |
| **多因子评分** | 成长因子(30%) + 质量因子(40%) + 安全因子(30%) 的综合评分体系 |
| **规则引擎** | 31条规则 (7条一票否决 + 10条扣分 + 6条交叉验证 + 3条周期性 + 5条加分) |
| **策略识别** | 自动识别5种投资策略：高成长、困境反转、稳定分红、周期底部、护城河防御 |
| **风险预警** | 检测纸面富贵(利润vs现金流背离)、低效扩张(高增长低ROE)等风险 |

### 输出报告
- 📊 **优质公司精选** - 按超大型/大型/中型分类展示
- 🏰 **白马护城河** - 高ROE、高ROIC、高毛利率的行业龙头
- 🚀 **困境反转机会** - 基本面触底回升的潜力股
- ⚠️ **风险警示** - 财务指标异常的公司
- 🏭 **行业景气度** - 全行业横向对比

---

## 🌟 核心架构

系统由三大独立且松耦合的模块组成：

```
┌─────────────────────────────────────────────────────────────┐
│                    Orchestrator (调度层)                     │
│         自动发现 · 统一门面 · 策略路由 · 版本管理              │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     Pipeline (编排层)                        │
│          YAML配置驱动 · 断点续传 · 缓存机制 · 指标监控         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              AStock Business Engines (业务层)                │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ 趋势分析器    │  │  评分引擎     │  │  报告生成器   │      │
│  │ TrendAnalyzer│  │ QualityScorer│  │ReportGenerator│     │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  DuckDB引擎  │  │  Polars引擎   │  │  规则引擎    │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

### 1. Orchestrator (核心调度层)
系统的"大脑"和服务总线。
-   **自动发现 (Auto-Discovery)**: 引入智能 `Scanner`，业务代码无需手动注册，遵循约定即可自动挂载。
-   **统一门面 (Facade)**: 提供统一的 API 调用接口，屏蔽底层实现细节。
-   **策略路由**: 支持多种执行策略（如优先使用最新版本、特定引擎等）。

### 2. Pipeline (流程编排层)
系统的"骨架"和执行引擎。
-   **混合引擎 (Hybrid Engine)**: 结合了 **Prefect** 的任务调度能力和 **Kedro** 的数据流管理能力。
-   **配置驱动**: 通过 YAML 文件定义复杂的工作流 (`workflow/*.yaml`)。
-   **健壮性**: 支持断点续传 (`--resume`)、缓存机制、依赖自动解析和详细的执行指标。

### 3. AStock Business Engines (业务逻辑层)
系统的"血肉"，纯净的业务实现。
-   **零侵入性**: 业务代码（如 `src/astock`）完全独立，不依赖框架代码。
-   **高性能**: 内置 DuckDB 和 Polars 引擎，支持 SQL 级和向量化的高速计算。
-   **即插即用**: 编写普通 Python 函数即可被系统识别为业务能力。

---

## 🚀 快速开始

### 环境要求
- Python 3.9+
- 依赖包: 见 `requirements.txt`

### 安装
```bash
pip install -r requirements.txt
```

### 运行分析
```bash
# 1. 运行完整的趋势分析工作流
python pipeline/main.py run -c workflow/analysis.yaml

# 2. 生成综合分析报告
python -c "from src.astock.business_engines.reporters.comprehensive_generator import ComprehensiveReportGenerator; ComprehensiveReportGenerator().generate_report()"

# 报告输出至: data/filter_middle/comprehensive_analysis_report.md
```

### 其他命令
```bash
# 查看系统状态
python pipeline/main.py status

# 查看可用引擎
python pipeline/main.py engines

# 查看性能指标
python pipeline/main.py metrics -c workflow/duckdb_screen.yaml

# 清理缓存
python pipeline/main.py cache clear
```

---

## 📊 数据要求 (重要)

### 输入数据文件
系统默认读取 `data/polars/5yd_final_industry.csv`，支持 **≥3年** 的时序财务数据。

### 必需字段 (Required Fields)

#### 1️⃣ 标识字段 (必需)
| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| `ts_code` | string | 股票代码 (唯一标识) | `000001.SZ` |
| `name` | string | 公司名称 | `平安银行` |
| `industry` | string | 所属行业 | `银行` |
| `end_date` | string/int | 报告期末日期 (年份标识) | `20231231` |

#### 2️⃣ 核心效率指标 (趋势分析必需)
| 字段名 | 类型 | 说明 | 用途 |
|--------|------|------|------|
| `roic` | float | 投入资本回报率 (%) | **核心筛选指标**，衡量资本效率 |
| `roe` | float | 净资产收益率 (%) | 股东回报视角，交叉验证ROIC |
| `grossprofit_margin` | float | 毛利率 (%) | 护城河指标，产品竞争力 |
| `netprofit_margin` | float | 净利率 (%) | 盈利能力指标 |

#### 3️⃣ 规模增长指标 (趋势分析必需)
| 字段名 | 类型 | 说明 | 用途 |
|--------|------|------|------|
| `eps` | float | 每股收益 (元) | 盈利增长趋势 |
| `total_revenue_ps` | float | 每股营收 (元) | 营收增长趋势 |
| `ocfps` | float | 每股经营现金流 (元) | **盈利质量验证**，检测纸面富贵 |

#### 4️⃣ 资本结构字段 (规模分类必需)
| 字段名 | 类型 | 说明 | 用途 |
|--------|------|------|------|
| `invest_capital` | float | 投入资本 (元) | **规模分类**的核心依据 |

### 可选字段 (Optional Fields)

| 字段名 | 类型 | 说明 | 用途 |
|--------|------|------|------|
| `roiic` | float | 增量投入资本回报率 (%) | 评估新投资效率 (需自行计算) |
| `bps` | float | 每股净资产 (元) | 价值评估参考 |
| `debt_to_assets` | float | 资产负债率 (%) | 财务风险评估 |
| `current_ratio` | float | 流动比率 | 短期偿债能力 |
| `fcff` | float | 自由现金流 (元) | 现金创造能力 |
| `symbol` | string | 股票简码 | 辅助标识 |
| `area` | string | 所在地区 | 地区分析 |
| `list_date` | string | 上市日期 | 上市时长分析 |

### 数据格式示例

```csv
ts_code,name,industry,end_date,roic,roe,grossprofit_margin,netprofit_margin,eps,total_revenue_ps,ocfps,invest_capital
000001.SZ,平安银行,银行,20201231,12.5,15.2,45.3,32.1,1.52,25.6,2.1,150000000000
000001.SZ,平安银行,银行,20211231,13.1,16.0,46.1,33.2,1.68,28.3,2.3,160000000000
000001.SZ,平安银行,银行,20221231,12.8,15.5,44.8,31.5,1.61,27.1,2.0,165000000000
000001.SZ,平安银行,银行,20231231,13.5,16.2,47.2,34.0,1.75,29.5,2.5,170000000000
000001.SZ,平安银行,银行,20241231,14.0,17.0,48.5,35.5,1.88,31.2,2.8,180000000000
```

### 数据年份要求

| 年份数 | 支持情况 | 分析能力 |
|--------|---------|---------|
| 3年 | ✅ 最小支持 | 基础趋势分析 |
| 5年 | ✅ **推荐** | 完整趋势+拐点检测 |
| 6-10年 | ✅ 支持 | 长周期分析 |
| >10年 | ✅ 支持 | 超长周期分析 |

> ⚠️ **重要**: 系统自动适应数据年份，无需修改代码。只要每家公司有 ≥3 年连续数据即可。

---

## 🔧 规模分类标准

系统根据 `invest_capital` (投入资本) 自动分类公司规模：

| 分类 | 投入资本范围 | 标签 | 投资特点 |
|------|-------------|------|---------|
| 微型 (Micro) | < 10亿 | 🔹 | 流动性差，风险极高 |
| 小型 (Small) | 10-50亿 | 🔸 | 成长空间大，波动剧烈 |
| 中型 (Mid) | 50-200亿 | 🔶 | 相对稳健，机构关注 |
| 大型 (Large) | 200-1000亿 | 🔷 | 行业龙头，流动性好 |
| 超大型 (Mega) | > 1000亿 | 💎 | 蓝筹白马，稳定性最高 |

> 📌 报告中仅展示 **中型、大型、超大型** 公司，小型和微型因风险过高被过滤。

---

## 📈 趋势分析方法

### 核心算法
| 算法 | 用途 |
|------|------|
| **对数线性回归** | 计算 log_slope，捕捉复合增长趋势 |
| **Mann-Kendall 检验** | 非参数趋势检验，抗异常值 |
| **Theil-Sen 估计** | 鲁棒斜率估计 |
| **分段回归** | 拐点检测，识别趋势反转 |
| **滚动窗口分析** | 近3年 vs 全周期对比，检测加速/减速 |

### 评分体系

```
综合评分 = 成长分×30% + 质量分×40% + 安全分×30%

成长分: 营收CAGR排名 + 利润CAGR排名 + 趋势加速度
质量分: ROE排名 + ROIC排名 + 毛利率排名 + 稳定性
安全分: 现金流覆盖率 + 现金流趋势 + 负债水平
```

### 策略识别

| 策略 | 识别条件 |
|------|---------|
| **高成长** | CAGR>20%, 趋势斜率>0.15, R²>0.7 |
| **困境反转** | 历史亏损→最新盈利, 近3年斜率转正 |
| **护城河** | ROE>15%, ROIC>12%, 毛利率>30%, 稳定5年 |
| **周期底部** | 周期性行业, 处于历史低位, 趋势见底 |
| **稳定分红** | ROE稳定, 现金流充沛, 波动率低 |

---

## 📂 目录结构

```text
AStock-Analysis/
├── orchestrator/           # [Core] 调度与注册中心
│   ├── registry/           # 组件注册表
│   └── decorators/         # 装饰器
├── pipeline/               # [Core] 流程执行引擎
│   ├── core/               # 核心执行器
│   ├── engines/            # Prefect/Kedro 引擎
│   └── main.py             # CLI 入口
├── src/astock/             # [Plugin] 业务逻辑
│   ├── business_engines/
│   │   ├── analyzers/      # 趋势分析器
│   │   │   └── trend/      # 趋势分析核心
│   │   │       ├── probes/ # 探针 (Log/Rolling/Inflection/Deterioration)
│   │   │       ├── rules.py    # 31条规则引擎
│   │   │       └── strategies.py # 5种策略识别
│   │   ├── reporters/      # 报告生成器
│   │   └── scorers/        # 评分引擎
│   └── data_engines/       # 数据引擎 (DuckDB/Polars)
├── workflow/               # [Config] YAML 工作流
│   ├── duckdb_screen.yaml  # 主分析流程
│   └── tushare_fina.yaml   # 数据获取流程
├── data/
│   ├── polars/             # 原始数据
│   │   └── 5yd_final_industry.csv  # 输入数据文件
│   └── filter_middle/      # 中间结果
│       ├── roic_trend_analysis.csv
│       ├── roe_trend_analysis.csv
│       └── comprehensive_analysis_report.md  # 输出报告
└── requirements.txt
```

---

## 💻 开发指南

### 添加新的分析指标

1. 在 `workflow/duckdb_screen.yaml` 中添加新的分析步骤：
```yaml
- name: "Analyze_NEW_METRIC_Trend"
  component: "business_engine"
  engine: "duckdb"
  method: ["analyze_metric_trend"]
  parameters:
    data: "steps.Load_Financial_Data.outputs.parameters.Raw_Data"
    group_cols: 'ts_code'
    metric_name: 'new_metric'  # CSV中的列名
    min_periods: 3
```

2. 在报告生成器中添加指标配置 (`comprehensive_generator.py`)：
```python
self.metrics_config = {
    ...
    "new_metric": {"file": "new_metric_trend_analysis.csv", "prefix": "new_metric", "name": "新指标"},
}
```

### 自定义规则

在 `src/astock/business_engines/analyzers/trend/rules.py` 中添加新规则：
```python
def my_custom_rule(context: RuleContext, thresholds: ThresholdConfig) -> Optional[RuleResult]:
    """自定义规则"""
    if context.some_condition:
        return RuleResult("my_rule", "penalty", "扣分原因", penalty_value=10)
    return None
```

---

## 📄 License

MIT License

---

## 🙏 致谢

- [Tushare](https://tushare.pro/) - A股数据源
- [DuckDB](https://duckdb.org/) - 高性能分析数据库
- [Polars](https://pola.rs/) - 高性能数据处理库
- [Prefect](https://www.prefect.io/) - 工作流编排引擎
