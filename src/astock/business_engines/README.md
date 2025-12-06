# AStock Business Engines 架构文档

> **版本**: 4.0 (2025-12)
> **定位**: 专业级基本面量化选股系统的业务引擎层
> **核心理念**: "用成长的速度衡量扩张，用质量的高度衡量护城河，用交叉验证识别造假"

---

## 📑 目录

1. [系统概述](#1-系统概述)
2. [架构设计](#2-架构设计)
3. [模块详解](#3-模块详解)
4. [分析器系统](#4-分析器系统-analyzers)
5. [评分引擎](#5-评分引擎-scorers)
6. [报告生成器](#6-报告生成器-reporters)
7. [核心组件](#7-核心组件-core)
8. [趋势分析详解](#8-趋势分析详解)
9. [规则引擎](#9-规则引擎-rule-engine)
10. [策略引擎](#10-策略引擎-strategy-engine)
11. [扩展指南](#11-扩展指南)
12. [配置参考](#12-配置参考)
13. [最佳实践](#13-最佳实践)

---

## 1. 系统概述

### 1.1 什么是 Business Engines?

Business Engines 是 AStock Analysis 系统的**业务逻辑层**，负责实现所有与股票分析相关的核心算法。它不仅仅是一个简单的"计算增长率"的工具，而是一个模拟顶级基本面分析师思维过程的**自动化决策引擎**。

### 1.2 核心能力

| 能力 | 描述 | 实现模块 |
|------|------|----------|
| **多维趋势识别** | OLS + Theil-Sen + Mann-Kendall 组合分析 | `analyzers/trend/` |
| **三表交叉验证** | 利润表 × 现金流量表 × 资产负债表 | `analyzers/trend/rules.py` |
| **双向决策机制** | 规则引擎(排雷) + 策略引擎(选优) | `rules.py` + `strategies.py` |
| **自适应阈值** | 行业差异化 + 指标类型差异化 | `metric_adapter.py` |
| **质量评分** | 多维度加权综合评分 | `scorers/` |
| **智能报告** | 自动生成分析报告 | `reporters/` |

### 1.3 设计原则

```
┌─────────────────────────────────────────────────────────────────┐
│                     Business Engines 设计原则                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   🎯 零侵入性                                                    │
│   ├── 业务代码完全独立，不依赖框架代码                             │
│   └── 可独立测试、独立运行                                        │
│                                                                 │
│   🔌 即插即用                                                    │
│   ├── 通过 @register_method 装饰器自动注册                        │
│   └── 新功能只需编写函数，无需修改框架                             │
│                                                                 │
│   ⚡ 高性能                                                      │
│   ├── DuckDB 引擎: SQL 级 OLAP 分析                              │
│   └── Polars 引擎: 向量化计算                                    │
│                                                                 │
│   🧩 可组合                                                      │
│   ├── Probe (探针) 可自由组合                                    │
│   ├── Rule (规则) 可动态启用/禁用                                │
│   └── Strategy (策略) 可按需配置                                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. 架构设计

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Business Engines                              │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                      Analyzers (分析器)                       │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │   │
│  │  │    Trend     │  │   Quality    │  │  Valuation   │       │   │
│  │  │  趋势分析     │  │   质量分析    │  │   估值分析    │       │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘       │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              │                                      │
│                              ▼                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                      Scorers (评分器)                         │   │
│  │  ┌──────────────┐  ┌──────────────┐                         │   │
│  │  │  Quality     │  │   Generic    │                         │   │
│  │  │  质量评分     │  │   通用评分    │                         │   │
│  │  └──────────────┘  └──────────────┘                         │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              │                                      │
│                              ▼                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                    Reporters (报告生成)                       │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │   │
│  │  │ Comprehensive│  │   Generic    │  │   Custom     │       │   │
│  │  │   综合报告    │  │   通用报告    │  │   自定义报告  │       │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘       │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │                        Core (核心)                            │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │   │
│  │  │ config_loader│  │ duckdb_utils │  │  interfaces  │       │   │
│  │  │   配置加载    │  │  DuckDB工具   │  │   接口定义    │       │   │
│  │  └──────────────┘  └──────────────┘  └──────────────┘       │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 目录结构

```
src/astock/business_engines/
├── __init__.py              # 模块初始化 & 自动扫描配置
├── README.md                # 本文档
│
├── analyzers/               # 分析器模块
│   ├── __init__.py
│   ├── trend/               # 趋势分析
│   │   ├── __init__.py
│   │   ├── config.py        # 行业配置
│   │   ├── core.py          # 核心分析器
│   │   ├── derivers.py      # 派生指标计算
│   │   ├── duckdb_engine.py # DuckDB 实现
│   │   ├── metric_adapter.py # 指标适配器
│   │   ├── metric_profiles.py # 指标配置文件
│   │   ├── models.py        # 数据模型
│   │   ├── rules.py         # 规则定义
│   │   ├── strategies.py    # 策略定义
│   │   ├── README.md        # 趋势分析文档
│   │   └── probes/          # 特征探针
│   │       ├── __init__.py
│   │       ├── base.py      # 探针基类
│   │       ├── trend.py     # 趋势探针
│   │       └── ...
│   ├── quality/             # 质量分析
│   │   └── ...
│   └── valuation/           # 估值分析
│       └── ...
│
├── scorers/                 # 评分引擎
│   ├── __init__.py
│   ├── engine.py            # 评分引擎入口
│   └── generic_scorer.py    # 通用评分器
│
├── reporters/               # 报告生成器
│   ├── __init__.py
│   ├── engine.py            # 报告引擎入口
│   ├── generator.py         # 报告生成器
│   ├── generic_reporter.py  # 通用报告器
│   └── comprehensive_generator.py # 综合报告生成器
│
└── core/                    # 核心工具
    ├── __init__.py
    ├── config_loader.py     # 配置加载器
    ├── duckdb_utils.py      # DuckDB 工具函数
    └── interfaces.py        # 接口定义
```

### 2.3 数据流架构

```
                     原始财务数据 (CSV/Parquet/DuckDB)
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────┐
│                        数据预处理层                               │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │  - 缺失值处理 (NaN/Inf)                                  │   │
│   │  - 数据期数验证 (min_periods)                            │   │
│   │  - 参考指标加载 (reference_metrics)                      │   │
│   │  - 行业分类识别                                          │   │
│   └─────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────┐
│                        特征提取层 (Probes)                        │
│   ┌───────────────┐  ┌───────────────┐  ┌───────────────┐       │
│   │ LogTrendProbe │  │RobustTrendProbe│  │ CyclicalProbe │       │
│   │  对数线性回归  │  │  Theil-Sen    │  │   周期识别    │       │
│   │    CAGR       │  │  Mann-Kendall │  │   峰谷分析    │       │
│   └───────────────┘  └───────────────┘  └───────────────┘       │
│                              │                                   │
│                              ▼                                   │
│                     TrendVector (特征向量)                        │
└──────────────────────────────────────────────────────────────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    ▼                           ▼
┌───────────────────────────┐   ┌───────────────────────────┐
│      规则引擎 (排雷)        │   │      策略引擎 (选优)        │
│   ┌───────────────────┐   │   │   ┌───────────────────┐   │
│   │   一票否决规则     │   │   │   │   高增长策略       │   │
│   │  ├─ 资本毁灭      │   │   │   │   困境反转策略     │   │
│   │  ├─ 连续亏损      │   │   │   │   稳定分红策略     │   │
│   │  └─ 断崖下跌      │   │   │   │   周期底部策略     │   │
│   └───────────────────┘   │   │   └───────────────────┘   │
│   ┌───────────────────┐   │   └───────────────────────────┘
│   │   扣分规则        │   │
│   │  ├─ 含金量验证    │   │
│   │  ├─ 可持续性验证  │   │
│   │  └─ 波动性验证    │   │
│   └───────────────────┘   │
└───────────────────────────┘
                    │                           │
                    └─────────────┬─────────────┘
                                  ▼
┌──────────────────────────────────────────────────────────────────┐
│                        评分与分类                                 │
│   ┌─────────────────────────────────────────────────────────┐   │
│   │  ❌ Rejected (淘汰)   : 触发一票否决                      │   │
│   │  ⭐ Selected (优选)   : 匹配正向策略                      │   │
│   │  ➖ Neutral (普通)    : 无特殊标记                       │   │
│   └─────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
                         📊 分析报告输出
```

---

## 3. 模块详解

### 3.1 模块职责矩阵

| 模块 | 职责 | 输入 | 输出 |
|------|------|------|------|
| `analyzers/trend` | 趋势分析 | 财务数据 + 配置 | TrendResult |
| `analyzers/quality` | 质量分析 | 财务数据 | QualityScore |
| `analyzers/valuation` | 估值分析 | 财务数据 + 市场数据 | ValuationResult |
| `scorers` | 综合评分 | 分析结果 | 评分报告 |
| `reporters` | 报告生成 | 分析结果 + 评分 | Markdown/PDF |
| `core` | 基础设施 | - | 工具函数 |

### 3.2 接口定义

```python
# core/interfaces.py

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Union
import pandas as pd

@dataclass
class AnalysisResult:
    """分析结果基类"""
    data: pd.DataFrame
    metric_name: str
    metadata: Dict[str, Any]

class IAnalyzer(ABC):
    """分析器接口"""

    @abstractmethod
    def analyze(self, data: Union[str, pd.DataFrame], config: dict) -> AnalysisResult:
        """执行分析

        Args:
            data: 输入数据（文件路径或 DataFrame）
            config: 分析配置

        Returns:
            分析结果
        """
        pass

class IScorer(ABC):
    """评分器接口"""

    @abstractmethod
    def score(self, data: pd.DataFrame, config: dict) -> pd.DataFrame:
        """执行评分

        Args:
            data: 分析结果数据
            config: 评分配置

        Returns:
            带评分的数据
        """
        pass

class IReporter(ABC):
    """报告器接口"""

    @abstractmethod
    def generate(self, data: Dict[str, Any], output_path: str) -> str:
        """生成报告

        Args:
            data: 报告数据
            output_path: 输出路径

        Returns:
            报告文件路径
        """
        pass
```

---

## 4. 分析器系统 (Analyzers)

### 4.1 趋势分析器

```python
# analyzers/trend/duckdb_engine.py

@register_method(
    engine_name="analyze_metric_trend",
    component_type="business_engine",
    engine_type="duckdb",
    description="通用指标趋势分析"
)
def analyze_metric_trend(
    data: Union[str, Path, pd.DataFrame],
    group_cols: str = 'ts_code',
    metric_name: str = 'roic',
    prefix: str = "",
    suffix: str = "",
    min_periods: int = 5,
    reference_metrics: List[str] = None,
    analyzer_config: dict = None,
    filter_config: dict = None,
    industry_configs: dict = None
) -> pd.DataFrame:
    """
    通用指标趋势分析

    Args:
        data: 输入数据
        group_cols: 分组列
        metric_name: 分析指标名
        prefix/suffix: 列名前后缀
        min_periods: 最小数据期数
        reference_metrics: 交叉验证参考指标
        analyzer_config: 分析器配置
        filter_config: 过滤配置
        industry_configs: 行业差异化配置

    Returns:
        包含趋势分析结果的 DataFrame
    """
```

### 4.2 探针系统 (Probes)

探针是特征提取的基本单元，采用**组合模式**设计：

```python
# analyzers/trend/probes/base.py

class BaseProbe(ABC):
    """探针基类"""

    @abstractmethod
    def extract(self, data: pd.Series, context: TrendContext) -> Dict[str, float]:
        """提取特征

        Args:
            data: 时间序列数据
            context: 趋势上下文

        Returns:
            特征字典
        """
        pass

# 具体探针实现

class LogTrendProbe(BaseProbe):
    """对数线性趋势探针 - 计算 CAGR"""

    def extract(self, data: pd.Series, context: TrendContext) -> Dict[str, float]:
        log_data = np.log(data.clip(lower=1e-6))
        slope, intercept, r_value, p_value, std_err = stats.linregress(
            range(len(log_data)), log_data
        )
        return {
            'log_slope': slope,
            'r_squared': r_value ** 2,
            'p_value': p_value,
            'cagr': np.exp(slope) - 1
        }

class RobustTrendProbe(BaseProbe):
    """稳健趋势探针 - Theil-Sen + Mann-Kendall"""

    def extract(self, data: pd.Series, context: TrendContext) -> Dict[str, float]:
        # Theil-Sen 稳健回归
        theil_slope = stats.theilslopes(data)[0]

        # Mann-Kendall 趋势检验
        mk_result = mk.original_test(data)

        return {
            'theil_slope': theil_slope,
            'mk_trend': mk_result.trend,
            'mk_p_value': mk_result.p,
            'mk_tau': mk_result.Tau
        }

class CyclicalProbe(BaseProbe):
    """周期性探针 - 识别周期股特征"""

    def extract(self, data: pd.Series, context: TrendContext) -> Dict[str, float]:
        peaks = self._find_peaks(data)
        troughs = self._find_troughs(data)

        return {
            'is_cyclical': len(peaks) >= 2 and len(troughs) >= 2,
            'peak_count': len(peaks),
            'trough_count': len(troughs),
            'amplitude': data.max() - data.min(),
            'current_phase': self._determine_phase(data)
        }
```

---

## 5. 评分引擎 (Scorers)

### 5.1 质量评分

```python
# scorers/engine.py

@register_method(
    engine_name="score_quality",
    component_type="business_engine",
    engine_type="scoring",
    description="质量评分引擎"
)
def score_quality(
    data: pd.DataFrame,
    report_path: str = None,
    weights: Dict[str, float] = None
) -> pd.DataFrame:
    """
    质量评分

    评分维度:
    - 成长性 (Growth): 营收/利润增长率
    - 盈利性 (Profitability): ROE/ROIC/毛利率
    - 稳定性 (Stability): 趋势显著性/波动率
    - 现金流 (Cash Flow): 经营现金流/自由现金流

    Args:
        data: 趋势分析结果
        report_path: 报告输出路径
        weights: 维度权重配置

    Returns:
        带评分的 DataFrame
    """
```

### 5.2 评分模型

```
┌─────────────────────────────────────────────────────────────────┐
│                        质量评分模型                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   总分 = Σ (维度得分 × 维度权重) + 策略加分 - 规则扣分            │
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐  │
│   │  维度得分计算                                            │  │
│   │  ├── 成长性 (30%): log_slope × 100                      │  │
│   │  ├── 盈利性 (25%): weighted_avg / benchmark             │  │
│   │  ├── 稳定性 (25%): r_squared × 100                      │  │
│   │  └── 现金流 (20%): ocf_ratio × 100                      │  │
│   └─────────────────────────────────────────────────────────┘  │
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐  │
│   │  调整项                                                  │  │
│   │  ├── 策略加分: +5 ~ +20 (匹配优质策略)                   │  │
│   │  ├── 规则扣分: -5 ~ -50 (触发扣分规则)                   │  │
│   │  └── 一票否决: 直接标记为 0 分                           │  │
│   └─────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 6. 报告生成器 (Reporters)

### 6.1 综合报告

```python
# reporters/comprehensive_generator.py

@register_method(
    engine_name="report_comprehensive",
    component_type="business_engine",
    engine_type="reporting",
    description="生成综合分析报告"
)
def report_comprehensive(
    data_dir: str,
    output_path: str,
    template: str = None
) -> str:
    """
    生成综合分析报告

    报告内容:
    1. 执行摘要
    2. 各指标趋势分析
    3. 交叉验证结果
    4. 优质标的推荐
    5. 风险提示

    Args:
        data_dir: 分析结果目录
        output_path: 输出路径
        template: 报告模板

    Returns:
        报告文件路径
    """
```

### 6.2 报告结构

```markdown
# 综合分析报告

## 1. 执行摘要
- 分析日期: 2025-12-06
- 样本数量: 5000 家
- 优质标的: 127 家 (2.5%)
- 淘汰标的: 2341 家 (46.8%)

## 2. 指标趋势分析

### 2.1 ROIC (投入资本回报率)
- 平均值: 8.5%
- 中位数: 6.2%
- 优质门槛: >12%

### 2.2 ROE (净资产收益率)
...

## 3. 交叉验证发现
- 利润含金量不足: 234 家
- 低效扩张: 156 家
- 财务异常: 45 家

## 4. 优质标的推荐
| 代码 | 名称 | 策略 | 得分 | 关键特征 |
|------|------|------|------|----------|
| 000001 | XX银行 | 高增长 | 85 | ROIC>15%, CAGR>20% |
...

## 5. 风险提示
- 本报告基于历史数据分析，不构成投资建议
- 市场环境变化可能导致历史规律失效
```

---

## 7. 核心组件 (Core)

### 7.1 DuckDB 工具

```python
# core/duckdb_utils.py

def _get_duckdb_module():
    """获取 DuckDB 模块（懒加载）"""
    global _duckdb
    if _duckdb is None:
        import duckdb
        _duckdb = duckdb
    return _duckdb

def _init_duckdb_and_source(data: Union[str, Path, pd.DataFrame]) -> Tuple:
    """初始化 DuckDB 连接和数据源

    Args:
        data: 数据源（文件路径或 DataFrame）

    Returns:
        (duckdb_connection, table_name)
    """
    duckdb = _get_duckdb_module()
    conn = duckdb.connect(':memory:')

    if isinstance(data, pd.DataFrame):
        conn.register('source_data', data)
        return conn, 'source_data'
    else:
        # 自动识别文件格式
        path = Path(data)
        if path.suffix == '.csv':
            conn.execute(f"CREATE TABLE source_data AS SELECT * FROM read_csv('{path}')")
        elif path.suffix == '.parquet':
            conn.execute(f"CREATE TABLE source_data AS SELECT * FROM read_parquet('{path}')")
        return conn, 'source_data'

def _q(conn, sql: str) -> pd.DataFrame:
    """执行 SQL 查询并返回 DataFrame"""
    return conn.execute(sql).fetchdf()
```

### 7.2 配置加载器

```python
# core/config_loader.py

class ConfigLoader:
    """配置加载器"""

    @staticmethod
    def load_yaml(path: str) -> Dict[str, Any]:
        """加载 YAML 配置"""
        with open(path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)

    @staticmethod
    def merge_configs(*configs: Dict) -> Dict:
        """合并多个配置（后者覆盖前者）"""
        result = {}
        for config in configs:
            result = deep_merge(result, config)
        return result
```

---

## 8. 趋势分析详解

### 8.1 为什么需要多种回归方法?

| 方法 | 优点 | 缺点 | 适用场景 |
|------|------|------|----------|
| **OLS** | 计算快速、直观 | 对异常值敏感 | 稳定增长型 |
| **Theil-Sen** | 对异常值稳健 | 计算较慢 | A股高波动 |
| **Mann-Kendall** | 统计显著性 | 仅判断趋势方向 | 趋势验证 |

### 8.2 交叉验证矩阵

```
┌─────────────────────────────────────────────────────────────────┐
│                      交叉验证配置矩阵                            │
├─────────────────────────────────────────────────────────────────┤
│  主分析指标          参考指标            校验目的                 │
├─────────────────────────────────────────────────────────────────┤
│  净利润 (EPS)       经营现金流 (OCFPS)   含金量验证               │
│                     → 利润涨、现金跌 = 纸面富贵                  │
├─────────────────────────────────────────────────────────────────┤
│  营收 (Revenue)     ROE                  可持续性验证             │
│                     → 营收涨、ROE低 = 烧钱换增长                 │
├─────────────────────────────────────────────────────────────────┤
│  ROIIC              ROIC                 边际效益验证             │
│                     → 增量回报 < 存量回报 = 效益递减              │
├─────────────────────────────────────────────────────────────────┤
│  毛利率             净利率               费用控制验证             │
│                     → 毛利高、净利低 = 费用失控                  │
├─────────────────────────────────────────────────────────────────┤
│  ROE                ROIC + 净利率        杜邦分解验证             │
│                     → 高ROE低ROIC = 杠杆驱动                    │
└─────────────────────────────────────────────────────────────────┘
```

### 8.3 自适应阈值

```python
# analyzers/trend/metric_adapter.py

class MetricAdapter:
    """指标适配器 - 根据指标类型调整评估标准"""

    METRIC_PROFILES = {
        # 规模类指标: 追求增长速度
        'scale': {
            'metrics': ['total_revenue_ps', 'eps', 'bps'],
            'evaluation': 'growth',  # 评估增长率
            'threshold_type': 'rate',
            'excellent': 0.20,  # 年增长 > 20%
            'good': 0.10,
            'acceptable': 0.05
        },
        # 效率类指标: 追求绝对水平
        'efficiency': {
            'metrics': ['roe', 'roic', 'grossprofit_margin', 'netprofit_margin'],
            'evaluation': 'level',  # 评估绝对值
            'threshold_type': 'absolute',
            'excellent': {'roe': 20, 'roic': 15, 'grossprofit_margin': 40},
            'good': {'roe': 12, 'roic': 10, 'grossprofit_margin': 25}
        },
        # 增量类指标: 追求正值
        'incremental': {
            'metrics': ['roiic'],
            'evaluation': 'positive',
            'threshold_type': 'sign',
            'excellent': 15,
            'good': 8
        }
    }
```

---

## 9. 规则引擎 (Rule Engine)

### 9.1 规则分类

```
┌─────────────────────────────────────────────────────────────────┐
│                        规则引擎架构                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐  │
│   │  一票否决规则 (Veto Rules) - 触发即淘汰                    │  │
│   │  ├── rule_roiic_capital_destruction: ROIIC 资本毁灭       │  │
│   │  ├── rule_min_latest_value: 最新值过低/连续亏损           │  │
│   │  ├── rule_extreme_deterioration: 断崖式恶化               │  │
│   │  └── rule_fraud_suspicion: 财务造假嫌疑                   │  │
│   └─────────────────────────────────────────────────────────┘  │
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐  │
│   │  扣分规则 (Penalty Rules) - 累积扣分                      │  │
│   │  ├── rule_earnings_quality_divergence: 含金量不足         │  │
│   │  ├── rule_sustainable_growth_check: 低效扩张             │  │
│   │  ├── rule_high_volatility_instability: 波动过大          │  │
│   │  ├── rule_recent_deterioration: 近期恶化                 │  │
│   │  └── rule_trend_weakness: 趋势不显著                     │  │
│   └─────────────────────────────────────────────────────────┘  │
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐  │
│   │  交叉验证规则 (Cross-Validation Rules)                    │  │
│   │  ├── rule_profit_cash_divergence: 利润-现金流背离         │  │
│   │  ├── rule_revenue_efficiency_check: 营收-效率背离         │  │
│   │  └── rule_margin_consistency: 毛利-净利一致性             │  │
│   └─────────────────────────────────────────────────────────┘  │
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐  │
│   │  周期性规则 (Cyclical Rules) - 周期股特殊处理             │  │
│   │  ├── rule_cyclical_peak_warning: 周期顶点预警            │  │
│   │  ├── rule_cyclical_trough_opportunity: 周期底部机会       │  │
│   │  └── rule_cyclical_phase_adjustment: 周期阶段调整         │  │
│   └─────────────────────────────────────────────────────────┘  │
│                                                                 │
│   ┌─────────────────────────────────────────────────────────┐  │
│   │  加分规则 (Bonus Rules) - 正向激励                        │  │
│   │  ├── rule_consistent_excellence: 持续优秀                │  │
│   │  ├── rule_accelerating_growth: 加速增长                  │  │
│   │  └── rule_turnaround_signal: 反转信号                    │  │
│   └─────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 9.2 规则定义示例

```python
# analyzers/trend/rules.py

def rule_earnings_quality_divergence(
    context: TrendContext,
    params: TrendRuleParameters,
    thresholds: TrendThresholds
) -> Optional[RuleResult]:
    """
    含金量验证规则

    逻辑: 利润增长但现金流恶化 → 重罚

    触发条件:
    1. 利润(EPS)增长率 > 10%
    2. 现金流(OCFPS)增长率 < 0%
    3. 利润-现金流差异 > 15%
    """
    # 获取参考指标
    ocf_stats = _get_reference_metric(context, "ocfps")
    if not ocf_stats:
        return None

    # 计算背离程度
    eps_growth = context.log_slope
    ocf_growth = ocf_stats.get("log_slope", 0)
    divergence = eps_growth - ocf_growth

    # 判断是否触发
    if eps_growth > 0.10 and ocf_growth < 0 and divergence > 0.15:
        severity = min(divergence / 0.30, 1.0)  # 最大严重度 1.0
        penalty = 20 * severity * params.penalty_factor

        return RuleResult(
            rule_name="earnings_quality_divergence",
            action="penalty",
            message=f"含金量不足: 利润增长{eps_growth:.1%}但现金流下降{ocf_growth:.1%}",
            penalty_score=penalty,
            log_level=logging.WARNING,
            log_prefix="【含金量警告】"
        )

    return None
```

---

## 10. 策略引擎 (Strategy Engine)

### 10.1 策略分类

| 策略 | 目标 | 核心条件 | 风险点 |
|------|------|----------|--------|
| **高增长** | 寻找成长股 | CAGR>20% + ROE>12% | 伪增长 |
| **困境反转** | 寻找触底反弹 | 趋势转正 + 基本面改善 | 伪反转 |
| **稳定分红** | 寻找现金牛 | 分红率>30% + ROE稳定 | 增长乏力 |
| **周期底部** | 寻找周期机会 | 处于周期底部 + 行业复苏 | 时机误判 |
| **护城河** | 寻找竞争优势 | 毛利率>40% + ROE>15% | 护城河消退 |

### 10.2 策略定义示例

```python
# analyzers/trend/strategies.py

class HighGrowthStrategy(BaseStrategy):
    """高增长策略"""

    name = "high_growth"
    description = "寻找高速成长的优质公司"

    def evaluate(self, context: TrendContext) -> StrategyResult:
        """
        高增长策略评估

        准入条件:
        1. 营收/利润 CAGR > 20%
        2. 趋势显著 (Mann-Kendall p < 0.1)
        3. ROE > 12% (效率及格)

        排除条件:
        1. "有毒增长": 高增长但 ROE < 5%
        2. 波动过大: r_squared < 0.6
        """
        # 检查增长率
        if context.cagr < 0.20:
            return StrategyResult(self.name, matched=False, reason="增长率不足20%")

        # 检查趋势显著性
        if context.mk_p_value > 0.10:
            return StrategyResult(self.name, matched=False, reason="趋势不显著")

        # 检查 ROE (从参考指标获取)
        roe_stats = context.get_reference_metric("roe")
        if roe_stats and roe_stats.get("latest", 0) < 12:
            return StrategyResult(self.name, matched=False, reason="ROE不足12%")

        # 排除有毒增长
        if roe_stats and roe_stats.get("latest", 0) < 5:
            return StrategyResult(
                self.name,
                matched=False,
                reason="有毒增长: 高增长低ROE",
                warning="⚠️ 烧钱换增长，股东受损"
            )

        # 检查稳定性
        if context.r_squared < 0.6:
            return StrategyResult(self.name, matched=False, reason="增长不稳定")

        # 匹配成功
        return StrategyResult(
            self.name,
            matched=True,
            reason=f"高质量成长: CAGR={context.cagr:.1%}, ROE={roe_stats.get('latest', 0):.1f}%",
            bonus_score=15
        )
```

---

## 11. 扩展指南

### 11.1 添加新分析器

```python
# 1. 创建分析器文件
# analyzers/my_analyzer/engine.py

from orchestrator.decorators.register import register_method
from ..core.interfaces import IAnalyzer, AnalysisResult

@register_method(
    engine_name="my_analysis",
    component_type="business_engine",
    engine_type="duckdb",
    description="我的自定义分析"
)
def my_analysis(data, config: dict) -> pd.DataFrame:
    """自定义分析方法"""
    # 实现分析逻辑
    return result

# 2. 在 __init__.py 中导出
# analyzers/my_analyzer/__init__.py
from .engine import my_analysis

# 3. 在 YAML 中使用
# workflow/my_workflow.yaml
steps:
  - name: "My_Analysis"
    component: "business_engine"
    engine: "duckdb"
    method: ["my_analysis"]
    parameters:
      data: "steps.Load_Data.outputs.parameters.Raw_Data"
      config:
        param1: value1
```

### 11.2 添加新规则

```python
# 1. 在 rules.py 中定义规则函数
def rule_my_custom_check(
    context: TrendContext,
    params: TrendRuleParameters,
    thresholds: TrendThresholds
) -> Optional[RuleResult]:
    """我的自定义规则"""
    if my_condition:
        return RuleResult(
            rule_name="my_custom_check",
            action="penalty",  # 或 "veto"
            message="触发原因描述",
            penalty_score=10.0
        )
    return None

# 2. 在 core.py 的 DEFAULT_TREND_RULES 中注册
DEFAULT_TREND_RULES = [
    # ... 现有规则
    rule_my_custom_check,
]
```

### 11.3 添加新策略

```python
# 1. 在 strategies.py 中定义策略类
class MyCustomStrategy(BaseStrategy):
    name = "my_custom_strategy"
    description = "我的自定义策略"

    def evaluate(self, context: TrendContext) -> StrategyResult:
        if my_conditions_met:
            return StrategyResult(self.name, matched=True, bonus_score=10)
        return StrategyResult(self.name, matched=False)

# 2. 在 get_default_strategies() 中注册
def get_default_strategies() -> List[BaseStrategy]:
    return [
        HighGrowthStrategy(),
        TurnaroundStrategy(),
        MyCustomStrategy(),  # 新增
    ]
```

---

## 12. 配置参考

### 12.1 趋势分析配置

```yaml
# workflow/duckdb_screen.yaml

- name: "Analyze_ROIC_Trend"
  component: "business_engine"
  engine: "duckdb"
  method: ["analyze_metric_trend"]
  parameters:
    data: "steps.Load_Financial_Data.outputs.parameters.Raw_Data"
    group_cols: 'ts_code'
    metric_name: 'roic'
    min_periods: 5
    reference_metrics: ["roe", "roiic"]  # 交叉验证
    analyzer_config:
      use_robust: true          # 使用 Theil-Sen
      use_mann_kendall: true    # 使用 Mann-Kendall
    filter_config:
      min_latest_value: 8       # 最低 ROIC 门槛
      trend_significance: 0.7   # 趋势显著性门槛
```

### 12.2 行业配置

```python
# analyzers/trend/config.py

INDUSTRY_FILTER_CONFIGS = {
    # 消费行业: 高毛利高稳定
    'consumer': {
        'min_latest_value': 12,
        'min_weighted_avg': 10,
        'trend_significance': 0.7,
        'cyclical_tolerance': 0.2
    },
    # 周期行业: 宽容波动
    'cyclical': {
        'min_latest_value': 5,
        'min_weighted_avg': 3,
        'trend_significance': 0.5,
        'cyclical_tolerance': 0.5
    },
    # 科技行业: 高增长导向
    'technology': {
        'min_latest_value': 8,
        'growth_premium': 1.5,
        'trend_significance': 0.6
    }
}
```

---

## 13. 最佳实践

### 13.1 数据准备

```
✅ 推荐做法:
- 至少 5 年财务数据 (min_periods=5)
- 包含行业分类字段 (industry_code)
- 包含完整的交叉验证指标

❌ 避免:
- 上市不满 3 年的公司
- 财务数据不完整的公司
- 重大资产重组的公司
```

### 13.2 参数调优

```
指标类型    │  推荐配置
───────────┼─────────────────────
规模指标    │  growth_threshold > 0.15
效率指标    │  level_threshold = 行业中位数 × 1.2
增量指标    │  positive_threshold > 5%
现金流指标  │  ratio_threshold > 0.8
```

### 13.3 常见问题

**Q: 为什么优质标的太少?**
A: 检查配置参数是否过于严格。建议：
- 降低 `min_latest_value` 门槛
- 放宽 `trend_significance` 要求

**Q: 为什么明明增长的公司被淘汰?**
A: 检查日志中的规则触发记录。常见原因：
- `earnings_quality_divergence`: 利润无现金流支撑
- `sustainable_growth_check`: 增长依赖烧钱
- `high_volatility_instability`: 增长不稳定

**Q: 如何调整行业差异化?**
A: 修改 `config.py` 中的 `INDUSTRY_FILTER_CONFIGS`

---

## 📚 相关文档

- [Pipeline README](../../../pipeline/README.md) - Pipeline 架构文档
- [Orchestrator README](../../../orchestrator/README.md) - 调度器文档
- [Workflow 配置](../../../workflow/duckdb_screen.yaml) - 工作流示例

---

## 📄 许可证

MIT License

---

**维护者**: AStock Team
**最后更新**: 2025-12-06
    1.  **LogTrendProbe**: 计算对数线性回归斜率 (CAGR)，作为基础增长率。
    2.  **RobustTrendProbe**: 使用 **Theil-Sen 估算器** 计算稳健斜率，并进行 **Mann-Kendall 检验**。
        *   *作用*: 过滤掉由单年非经常性损益导致的"伪高增长"。
    3.  **CyclicalProbe**: 识别周期性特征（峰谷比、波形），防止在周期顶点误判为高增长。
    4.  **DeteriorationProbe**: 检测近期（近1-2年）是否出现断崖式下跌。

#### 第三阶段：规则引擎 (Rule Engine - The Negative Filter)
*   **模块**: `rules.py`
*   **职责**: **"排雷"**。寻找任何可能导致亏损的瑕疵。
*   **机制**:
    *   **一票否决 (Veto)**: 触发即淘汰。例如：ROIIC 长期为负、财务造假嫌疑。
    *   **扣分 (Penalty)**: 瑕疵累积。例如：增速放缓、波动过大。
*   **关键规则**:
    *   `rule_earnings_quality_divergence`: **含金量检验**。利润增长但现金流恶化 -> 重罚。
    *   `rule_sustainable_growth_check`: **内生增长检验**。营收增速远超 ROE -> 判定为低效烧钱扩张。
    *   `rule_roiic_capital_destruction`: **价值毁灭检验**。投入资本回报率长期为负 -> 一票否决。

#### 第四阶段：策略引擎 (Strategy Engine - The Positive Selector)
*   **模块**: `strategies.py`
*   **职责**: **"选优"**。在幸存者中寻找特定类型的优质公司。
*   **核心策略**:
    1.  **HighGrowthStrategy (高增长策略)**:
        *   要求：营收/利润高速增长 (>20%) + 趋势显著 (Mann-Kendall p<0.1) + ROE 及格。
        *   *拒绝*: "有毒增长" (高增长但 ROE<5%)。
    2.  **TurnaroundStrategy (困境反转策略)**:
        *   要求：基本面触底回升 + 稳健斜率转正 + 杜绝纯消息面炒作。
        *   *拒绝*: "伪反转" (营收回升但毛利率暴跌)。

---

## 3. 关键技术细节 (Technical Deep Dive)

### 3.1 为什么需要"参考指标" (Reference Metrics)?
单一指标具有欺骗性。本系统引入 `TrendContext.reference_metrics` 实现多维校验：

| 主分析指标 | 参考指标 | 校验目的 | 逻辑 |
| :--- | :--- | :--- | :--- |
| **净利润 (EPS)** | **经营现金流 (OCFPS)** | **含金量** | 利润涨、现金流跌 = 纸面富贵 (可能造假) |
| **营收 (Revenue)** | **ROE** | **可持续性** | 营收涨、ROE低 = 烧钱换增长 (股东受损) |
| **ROIIC** | **ROIC** | **边际效益** | 增量回报 < 存量回报 = 边际效益递减 |

### 3.2 为什么使用 Theil-Sen 和 Mann-Kendall?
A 股常有"单年暴雷"或"资产处置收益"导致的脉冲式波动。
*   **OLS (普通回归)**: 对异常值极其敏感。一年利润翻 10 倍会拉高整个 5 年的斜率。
*   **Theil-Sen (中位数斜率)**: 忽略异常值，只看"大多数年份"的趋势。
*   **Mann-Kendall**: 统计学检验。告诉我们"这个增长趋势是真实的，还是随机波动产生的"。

### 3.3 自适应阈值 (Adaptive Thresholds)
系统不搞"一刀切"。在 `strategies.py` 中：
*   **规模类指标 (Scale)**: 如营收。追求 **速度** (Growth > 20%)。
*   **效率类指标 (Efficiency)**: 如毛利率、ROE。追求 **高度与稳定性** (Value > 40%, Stable)。
    *   *逻辑*: 要求毛利率每年增长 20% 是荒谬的；要求毛利率维持在 50% 才是好公司。

---

## 4. 开发指南 (Developer Guide)

### 如何添加一个新的"排雷规则"?
1.  在 `src/astock/business_engines/trend/rules.py` 中定义函数：
    ```python
    def rule_my_new_check(context, params, thresholds):
        # 访问数据
        if context.latest_value < 0:
            return RuleResult("my_rule", "penalty", "负值扣分", 10.0)
        return None
    ```
2.  在 `src/astock/business_engines/trend/core.py` 的 `DEFAULT_TREND_RULES` 列表中注册。

### 如何添加一个新的"选股策略"?
1.  在 `src/astock/business_engines/trend/strategies.py` 中继承 `BaseStrategy`：
    ```python
    class MySuperStrategy(BaseStrategy):
        name = "super_stock"
        def evaluate(self, context):
            if context.log_slope > 0.5:
                return StrategyResult(self.name, True, "超级成长")
            return StrategyResult(self.name, False)
    ```
2.  在 `get_default_strategies()` 中注册。

---

## 5. 配置指南 (Configuration)

主要配置文件位于 `workflow/duckdb_screen.yaml`。

**启用交叉验证的配置示例**:
```yaml
    - name: "Analyze_Profit_Trend"
      component: "business_engine"
      engine: "duckdb"
      method: ["analyze_metric_trend"]
      parameters:
        metric_name: 'eps'
        # 关键：在此处指定参考指标
        reference_metrics: ["ocfps", "roe"]
```

---

## 6. 常见问题 (FAQ)

**Q: 为什么我的股票明明增长很快，却被淘汰了?**
A: 检查日志。可能是触发了 `earnings_quality_divergence` (有利润无现金) 或 `high_volatility_instability` (波动过大，增长不显著)。

**Q: "Insufficient data" 警告是什么意思?**
A: 系统默认要求至少 5 年数据 (`min_periods=5`)。对于上市不满 5 年的公司，为了保证趋势分析的统计有效性，会自动跳过。这是有意设计的保护机制。

**Q: 怎么调整扣分力度?**
A: 在 `config.py` 或 YAML 参数中调整 `penalty_factor` (默认 20)。

---

**维护者**: Jusu2-teach
**最后更新**: 2025-11-29
