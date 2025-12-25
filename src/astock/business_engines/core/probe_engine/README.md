# Probe Engine

## 概述

`ProbeEngine` 是探针系统的核心执行引擎，负责纯数学计算层的探针编排和执行。

## 架构层次

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Raw Financial Data                           │
│                    (时间序列: ROIC, ROE, Revenue...)                 │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    ProbeEngine (本模块)                              │
│═════════════════════════════════════════════════════════════════════│
│  核心职责:                                                           │
│    1. 探针注册和管理                                                 │
│    2. 探针执行编排                                                   │
│    3. 异常处理和降级                                                 │
│    4. 结果缓存                                                       │
│                                                                      │
│  设计原则:                                                           │
│    ✓ 纯函数式：相同输入 → 相同输出                                   │
│    ✓ 无业务逻辑：不包含阈值判断                                      │
│    ✓ 可组合：探针可自由组合                                          │
│    ✓ 可测试：每个探针独立单元测试                                    │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        ProbeOutputs                                  │
│═════════════════════════════════════════════════════════════════════│
│  统一接口层，包含所有探针的计算结果                                  │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    ▼                              ▼
┌────────────────────────────────┐ ┌────────────────────────────────┐
│     ThresholdEvaluator         │ │         T.R.U.T.H.             │
│  (规则驱动 → Pass/Fail)        │ │  (模型驱动 → 六维基因)         │
└────────────────────────────────┘ └────────────────────────────────┘
```

## 模块结构

```
probe_engine/
├── __init__.py          # 模块入口，导出所有公共API
├── interface.py         # 探针协议定义 (ProbeProtocol, BaseProbe)
├── registry.py          # 探针注册中心 (ProbeRegistry)
├── engine.py            # 探针执行引擎 (ProbeEngine)
└── builders.py          # 输出构建器 (ProbeOutputBuilder, ProbeOutputs)
```

## 核心组件

### 1. ProbeProtocol (interface.py)

探针协议定义，所有探针必须实现：

```python
class ProbeProtocol(Protocol[T]):
    name: str                # 探针唯一名称
    description: str         # 探针描述
    fatal: bool              # 失败是否致命

    def compute(self, values: np.ndarray, **kwargs) -> T:
        """执行探针计算"""
        ...

    def default(self) -> T:
        """返回默认结果"""
        ...

    def validate(self, values: np.ndarray) -> bool:
        """验证输入数据"""
        ...
```

### 2. BaseProbe (interface.py)

探针抽象基类，提供基本实现框架：

```python
class BaseProbe(ABC, Generic[T]):
    def __init__(
        self,
        name: str,
        description: str = "",
        fatal: bool = False,
        min_data_points: int = 3,
    ):
        ...

    # 子类只需实现这两个方法
    @abstractmethod
    def _compute_impl(self, values: np.ndarray, **kwargs) -> T: ...

    @abstractmethod
    def _create_default(self) -> T: ...
```

### 3. ProbeRegistry (registry.py)

探针注册中心：

```python
registry = ProbeRegistry()

# 注册探针
registry.register(LogTrendProbeAdapter())
registry.register(VolatilityProbeAdapter())

# 查询探针
probe = registry.get("log_trend")
trend_probes = registry.get_by_category("trend")

# 获取所有探针
all_probes = registry.all()
```

### 4. ProbeEngine (engine.py)

探针执行引擎：

```python
# 创建引擎
engine = ProbeEngine.with_default_probes()

# 执行所有探针
results = engine.run_all(values)
# returns: {"log_trend": LogTrendResult, "volatility": VolatilityResult, ...}

# 执行选定探针
results = engine.run_selected(["log_trend", "volatility"], values)

# 执行某一分类
results = engine.run_by_category("trend", values)

# 批量执行
batch_results = engine.run_batch({
    "roic": roic_values,
    "roe": roe_values,
    "revenue": revenue_values,
})
```

### 5. ProbeOutputs (builders.py)

统一输出接口：

```python
@dataclass
class ProbeOutputs:
    indicator_name: str

    # 8 个核心探针结果
    log_trend: Optional[LogTrendResult] = None
    volatility: Optional[VolatilityResult] = None
    cyclical: Optional[CyclicalPatternResult] = None
    deterioration: Optional[RecentDeteriorationResult] = None
    rolling: Optional[RollingTrendResult] = None
    robust: Optional[RobustTrendResult] = None
    inflection: Optional[InflectionResult] = None
    multi_horizon: Optional[Any] = None

    # 原始数据
    raw_values: Optional[np.ndarray] = None

    def has_core_probes(self) -> bool: ...
    def missing_probes(self) -> List[str]: ...
```

### 6. ProbeOutputBuilder (builders.py)

流式构建器：

```python
# 链式构建
outputs = (
    ProbeOutputBuilder("roic")
    .with_log_trend(log_trend_result)
    .with_volatility(volatility_result)
    .with_raw_values(values)
    .build()
)

# 从引擎结果构建
results = engine.run_all(values)
outputs = ProbeOutputBuilder.from_engine_results("roic", results, values)
```

## 8 个标准探针

| 探针 | 分类 | 功能 | 关键输出 |
|------|------|------|----------|
| `log_trend` | TREND | 对数趋势分析 | log_slope, r², CAGR, WLS, Bootstrap CI |
| `robust` | ROBUST | 稳健趋势估计 | Theil-Sen slope, Mann-Kendall tau |
| `volatility` | VOLATILITY | 波动性分析 | CV, 去趋势CV, ARCH效应, 体制 |
| `cyclical` | CYCLICAL | 周期性检测 | HP滤波, FFT周期, 周期位置 |
| `deterioration` | DETERIORATION | 恶化检测 | 贝叶斯概率, 连续下跌, 峰值跌幅 |
| `inflection` | INFLECTION | 拐点检测 | CUSUM, 分段回归, 拐点类型 |
| `rolling` | ROLLING | 滚动窗口分析 | 3y/5y斜率, 加速度 |
| `multi_horizon` | MULTI_HORIZON | 多视野分析 | 结构断点, 数据体制 |

## 使用示例

### 基本使用

```python
from astock.business_engines.core import ProbeEngine, ProbeOutputBuilder
import numpy as np

# 准备数据
roic_values = np.array([15.2, 14.8, 16.1, 15.5, 17.2])

# 创建引擎
engine = ProbeEngine.with_default_probes()

# 运行所有探针
results = engine.run_all(roic_values)

# 构建 ProbeOutputs
outputs = ProbeOutputBuilder.from_engine_results("roic", results, roic_values)

# 检查结果
print(f"核心探针完整: {outputs.has_core_probes()}")
print(f"缺失探针: {outputs.missing_probes()}")
print(f"CAGR: {outputs.log_trend.cagr_approx:.2%}")
print(f"CV: {outputs.volatility.cv:.2%}")
```

### 自定义探针

```python
from astock.business_engines.core import BaseProbe
from dataclasses import dataclass

@dataclass
class MyResult:
    value: float
    is_valid: bool

class MyCustomProbe(BaseProbe[MyResult]):
    def __init__(self):
        super().__init__(
            name="my_probe",
            description="My custom analysis",
            min_data_points=3,
        )

    def _compute_impl(self, values: np.ndarray, **kwargs) -> MyResult:
        # 自定义计算逻辑
        return MyResult(value=np.mean(values), is_valid=True)

    def _create_default(self) -> MyResult:
        return MyResult(value=0.0, is_valid=False)

# 注册使用
engine = ProbeEngine()
engine.registry.register(MyCustomProbe())
results = engine.run_all(values)
```

### 批量处理

```python
# 准备多指标数据
data = {
    "roic": roic_values,
    "roe": roe_values,
    "revenue": revenue_values,
}

# 批量执行
batch_results = engine.run_batch(data)

# 构建公司级输出
from astock.business_engines.core import MultiIndicatorProbeOutputBuilder

company_outputs = MultiIndicatorProbeOutputBuilder.from_batch_results(
    company_code="000001.SZ",
    company_name="平安银行",
    batch_results=batch_results,
    raw_data=data,
)
```

### 与评估器集成

```python
from astock.business_engines.evaluators import ThresholdEvaluator

# 创建评估器
evaluator = ThresholdEvaluator.with_default_rules()

# 评估
result = evaluator.evaluate(outputs)
print(f"通过: {result.passes}")
print(f"分数: {result.score:.1f}")
print(f"等级: {result.grade}")
print(f"策略: {result.strategies}")
```

## 配置选项

### ProbeEngineConfig

```python
@dataclass
class ProbeEngineConfig:
    min_data_points: int = 3        # 最小数据点数
    enable_cache: bool = True       # 启用缓存
    cache_ttl_seconds: float = 300  # 缓存过期时间
    parallel_execution: bool = False # 并行执行
    max_workers: int = 4            # 最大工作线程
    timeout_seconds: float = 30     # 单探针超时
    fail_fast: bool = False         # 遇到致命错误立即停止
    collect_stats: bool = True      # 收集执行统计
```

## 性能指标

| 操作 | 目标时间 |
|------|----------|
| 单探针计算 | < 10ms |
| 8探针全量计算 | < 100ms |
| 1000家公司批量 | < 30s |

## 与现有系统的兼容性

ProbeEngine 设计为向后兼容：

1. **复用现有探针实现**: 通过适配器包装 `LogTrendCalculator`, `VolatilityCalculator` 等
2. **保持结果模型不变**: `LogTrendResult`, `VolatilityResult` 等模型保持原样
3. **支持渐进式迁移**: 现有代码可继续使用，逐步切换到新接口

## 相关文档

- [探针架构重构设计](../../../docs/PROBE_ARCHITECTURE_REFACTORING.md)
- [趋势分析 README](../../analyzers/trend/README.md)
- [T.R.U.T.H. 系统设计](../../truth/docs/TRUTH_SYSTEM_DESIGN.md)

---

*版本: 2.0.0*
*创建日期: 2025-01*
*作者: AStock Analysis System*
