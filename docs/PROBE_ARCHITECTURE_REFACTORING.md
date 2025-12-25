# 探针系统重构架构设计

## 一、问题诊断

### 1.1 当前架构分析

```
当前混合架构：
┌─────────────────────────────────────────────────────────────────────┐
│                    analyzers/trend/                                  │
├─────────────────────────────────────────────────────────────────────┤
│  core.py (1252行) - 混合职责：                                       │
│    ├── TrendAnalyzer: 探针编排 + 数据预处理                          │
│    ├── TrendEvaluator: 规则执行 + 策略匹配                           │
│    ├── TrendRuleEngine: 29+ 规则的执行引擎                           │
│    └── TrendResultCollector: 结果收集                                │
├─────────────────────────────────────────────────────────────────────┤
│  probes/ (8个探针) - 纯数学计算 ✓                                    │
│    ├── log_trend_probe.py: OLS + WLS + Bootstrap CI                 │
│    ├── robust_probe.py: Theil-Sen + Mann-Kendall                    │
│    ├── volatility_probe.py: CV + ARCH效应 + 去趋势CV                 │
│    ├── cyclical_probe.py: HP滤波 + FFT + DFA                        │
│    ├── deterioration_probe.py: 贝叶斯恶化概率                        │
│    ├── inflection_probe.py: CUSUM + 分段回归                        │
│    ├── rolling_probe.py: 3年/5年滚动窗口                            │
│    └── multi_horizon_probe.py: 结构断点检测                          │
├─────────────────────────────────────────────────────────────────────┤
│  rules.py (1133行) - 业务规则                                        │
│    ├── Veto Rules: 一票否决规则                                      │
│    ├── Penalty Rules: 扣分规则                                       │
│    └── Bonus Rules: 加分规则                                         │
├─────────────────────────────────────────────────────────────────────┤
│  strategies.py (457行) - 投资策略                                    │
│    ├── HighGrowthStrategy                                            │
│    ├── TurnaroundStrategy                                            │
│    ├── StableDividendStrategy                                        │
│    └── CyclicalBottomStrategy                                        │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                       truth/                                         │
├─────────────────────────────────────────────────────────────────────┤
│  adapter.py (727行) - DataFrame → ProbeOutputs 桥接                  │
│    ├── ProbeOutputs: 单指标探针结果集                                │
│    ├── MultiIndicatorProbeOutputs: 公司级多指标结果集                │
│    ├── GenomeInput: 六维基因输入                                     │
│    └── ProbeAdapter: 探针输出 → 基因输入 映射                        │
├─────────────────────────────────────────────────────────────────────┤
│  processor.py (1580行) - T.R.U.T.H. 处理器                           │
│    ├── DataFrameToProbeConverter: DataFrame → 探针结果               │
│    ├── TruthProcessor: 基因计算 + 求解器                             │
│    └── BatchProcessor: 批量处理                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### 1.2 核心问题

1. **职责混淆**: `core.py` 混合了探针编排和业务评估
2. **代码重复**: 探针调用逻辑在 `TrendAnalyzer` 和 `TruthProcessor` 中各实现一次
3. **接口不统一**: T.R.U.T.H. 使用 `ProbeOutputs`，而 trend 直接使用 `TrendVector`
4. **难以复用**: 纯数学探针被绑定在业务逻辑中

---

## 二、目标架构

### 2.1 三层分离设计

```
目标架构：
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

                          Raw Financial Data
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│           LAYER 1: ProbeEngine (Pure Mathematical Layer)            │
│═════════════════════════════════════════════════════════════════════│
│  core/probe_engine/                                                  │
│    ├── engine.py: ProbeEngine - 探针编排引擎                        │
│    ├── registry.py: ProbeRegistry - 探针注册中心                    │
│    └── interface.py: 探针接口协议                                    │
│                                                                      │
│  输入: np.ndarray (时间序列数据)                                     │
│  输出: ProbeOutputs (纯数学结果，无业务判断)                         │
│                                                                      │
│  核心原则:                                                           │
│    ✓ 纯函数式：相同输入 → 相同输出                                   │
│    ✓ 无业务逻辑：不包含阈值判断、不做 pass/fail 决策                 │
│    ✓ 可缓存：探针结果可被多个评估器复用                              │
│    ✓ 可测试：每个探针独立单元测试                                    │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│           LAYER 2: ProbeOutputs (Unified Interface Layer)           │
│═════════════════════════════════════════════════════════════════════│
│  core/probe_outputs/                                                 │
│    ├── models.py: 探针输出数据模型                                   │
│    ├── adapters.py: DataFrame ↔ ProbeOutputs 转换器                 │
│    └── cache.py: 探针结果缓存管理                                    │
│                                                                      │
│  核心数据结构:                                                       │
│    ProbeOutputs: 单指标所有探针结果                                  │
│    MultiIndicatorProbeOutputs: 公司级多指标结果                      │
│    ProbeOutputsCache: 缓存层 (避免重复计算)                          │
│                                                                      │
│  复用现有:                                                           │
│    - LogTrendResult, VolatilityResult, CyclicalPatternResult...      │
│    - 这些数据模型保持不变，只是统一入口                              │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                   ┌──────────────┴──────────────┐
                   ▼                              ▼
┌────────────────────────────────┐ ┌────────────────────────────────┐
│  LAYER 3A: ThresholdEvaluator  │ │  LAYER 3B: T.R.U.T.H. System   │
│════════════════════════════════│ │════════════════════════════════│
│  evaluators/threshold/         │ │  truth/                        │
│    ├── engine.py: 规则引擎     │ │    ├── processor.py: 基因计算  │
│    ├── rules.py: 29+ 规则      │ │    ├── core/genes.py: 六维基因 │
│    ├── strategies.py: 4 策略   │ │    ├── core/solvers.py: 求解器 │
│    └── models.py: 评估结果     │ │    └── genome.py: 基因组       │
│                                │ │                                │
│  输入: ProbeOutputs            │ │  输入: ProbeOutputs            │
│  输出: ThresholdEvaluation     │ │  输出: TruthResult             │
│                                │ │                                │
│  职责:                         │ │  职责:                         │
│    - 阈值判断                  │ │    - 基因映射                  │
│    - Pass/Veto/Penalty 决策    │ │    - 物理求解器                │
│    - 策略匹配                  │ │    - 行业校准                  │
└────────────────────────────────┘ └────────────────────────────────┘
                   │                              │
                   ▼                              ▼
┌────────────────────────────────┐ ┌────────────────────────────────┐
│  Threshold Report Generator    │ │  T.R.U.T.H. Report Generator   │
│════════════════════════════════│ │════════════════════════════════│
│  reporters/                    │ │  reporters/                    │
│    └── threshold_report.py     │ │    └── truth_report.py         │
│                                │ │                                │
│  输出:                         │ │  输出:                         │
│    - 趋势质量评分              │ │    - 六维基因雷达图            │
│    - Pass/Fail 决策            │ │    - 动态阈值推荐              │
│    - 策略建议                  │ │    - 因果验证结果              │
└────────────────────────────────┘ └────────────────────────────────┘
```

### 2.2 数据流设计

```
完整数据流：

1. 数据输入阶段
   ┌─────────────────────────────────────────────────────────────┐
   │ DataFrame (from Pipeline)                                   │
   │   columns: roic, roe, revenue, ocf, gross_margin, ...       │
   │   index: ts_code (公司代码)                                  │
   └─────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
2. 探针计算阶段 (ProbeEngine)
   ┌─────────────────────────────────────────────────────────────┐
   │ for each metric in [roic, roe, revenue, ...]:               │
   │     values = extract_time_series(df, metric)                │
   │     probes = [                                               │
   │         LogTrendProbe,                                       │
   │         RobustProbe,                                         │
   │         VolatilityProbe,                                     │
   │         CyclicalProbe,                                       │
   │         DeteriorationProbe,                                  │
   │         InflectionProbe,                                     │
   │         RollingProbe,                                        │
   │         MultiHorizonProbe,                                   │
   │     ]                                                        │
   │     results = engine.run_all_probes(values, probes)          │
   └─────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
3. 标准化输出阶段 (ProbeOutputs)
   ┌─────────────────────────────────────────────────────────────┐
   │ ProbeOutputs(                                                │
   │     indicator_name="roic",                                   │
   │     log_trend=LogTrendResult(                                │
   │         log_slope=0.05,                                      │
   │         r_squared=0.85,                                      │
   │         cagr_approx=5.1%,                                    │
   │         wls_slope=0.048,                                     │
   │         bootstrap_ci=(0.03, 0.07),                           │
   │     ),                                                       │
   │     volatility=VolatilityResult(                             │
   │         cv=0.15,                                             │
   │         detrended_cv=0.12,                                   │
   │         has_arch_effect=False,                               │
   │         volatility_regime="stable",                          │
   │     ),                                                       │
   │     cyclical=CyclicalPatternResult(...),                     │
   │     deterioration=RecentDeteriorationResult(...),            │
   │     rolling=RollingTrendResult(...),                         │
   │     robust=RobustTrendResult(...),                           │
   │     inflection=InflectionResult(...),                        │
   │ )                                                            │
   └─────────────────────────────────────────────────────────────┘
                                  │
              ┌───────────────────┴───────────────────┐
              │                                       │
              ▼                                       ▼
4A. 阈值评估 (ThresholdEvaluator)          4B. T.R.U.T.H. 基因计算
   ┌──────────────────────────────┐          ┌──────────────────────────────┐
   │ context = build_context(     │          │ genome_input = adapter.adapt( │
   │     probe_outputs            │          │     multi_probe_outputs       │
   │ )                            │          │ )                             │
   │                              │          │                               │
   │ # 运行 29+ 规则              │          │ genome = compute_genome(      │
   │ for rule in rules:           │          │     genome_input              │
   │     result = rule(context)   │          │ )                             │
   │                              │          │                               │
   │ # 匹配 4 个策略              │          │ # 运行 3 大求解器             │
   │ for strategy in strategies:  │          │ gravity = gravity_solver(...)  │
   │     match = strategy(context)│          │ velocity = velocity_solver(...)│
   │                              │          │ structure = structure_solver() │
   │ return ThresholdEvaluation(  │          │                               │
   │     passes=True/False,       │          │ return TruthResult(           │
   │     penalty=15.0,            │          │     genome=genome,            │
   │     strategies=["high_growth"]│         │     solvers={...},            │
   │ )                            │          │     final_score=85.0,         │
   └──────────────────────────────┘          │ )                             │
              │                               └──────────────────────────────┘
              │                                       │
              ▼                                       ▼
5A. 阈值报告                               5B. T.R.U.T.H. 报告
   ┌──────────────────────────────┐          ┌──────────────────────────────┐
   │ Threshold Quality Report     │          │ T.R.U.T.H. Analysis Report   │
   │ ════════════════════════════│          │ ════════════════════════════│
   │ ✓ ROIC 趋势质量: 85/100      │          │ α: 0.15 (轻周期)             │
   │ ✓ 策略: 高成长型             │          │ β: 0.22 (轻资产)             │
   │ ⚠ 警告: 波动率上升           │          │ γ: 0.08 (稳健增长)           │
   └──────────────────────────────┘          │ δf: 0.02 (低风险)            │
                                             │ δd: 0.05 (健康)              │
                                             │ V: 0.95 (高验证)             │
                                             │                               │
                                             │ 动态ROIC阈值: 10.2%          │
                                             │ 增长速度边界: 8-15%          │
                                             └──────────────────────────────┘
```

---

## 三、模块详细设计

### 3.1 ProbeEngine 模块

```python
# core/probe_engine/engine.py

from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Protocol
import numpy as np

class ProbeProtocol(Protocol):
    """探针协议：所有探针必须实现此接口"""
    name: str

    def compute(self, values: np.ndarray, **kwargs) -> Any:
        """执行探针计算"""
        ...

    def default(self) -> Any:
        """返回默认结果（数据不足时）"""
        ...


@dataclass(frozen=True)
class ProbeEngineConfig:
    """探针引擎配置"""
    min_data_points: int = 3
    enable_cache: bool = True
    parallel_execution: bool = False
    timeout_seconds: float = 30.0


class ProbeEngine:
    """
    探针引擎：纯数学计算层

    核心职责：
    1. 管理探针注册
    2. 执行探针计算
    3. 处理异常和降级
    4. 缓存计算结果

    设计原则：
    - 纯函数式：无副作用
    - 可组合：探针可自由组合
    - 可扩展：新探针即插即用
    """

    def __init__(
        self,
        probes: Optional[List[ProbeProtocol]] = None,
        config: Optional[ProbeEngineConfig] = None,
    ):
        self.config = config or ProbeEngineConfig()
        self._registry: Dict[str, ProbeProtocol] = {}

        if probes:
            for probe in probes:
                self.register(probe)

    def register(self, probe: ProbeProtocol) -> None:
        """注册探针"""
        self._registry[probe.name] = probe

    def run_single(
        self,
        probe_name: str,
        values: np.ndarray,
        **kwargs
    ) -> Any:
        """运行单个探针"""
        if len(values) < self.config.min_data_points:
            return self._registry[probe_name].default()

        return self._registry[probe_name].compute(values, **kwargs)

    def run_all(
        self,
        values: np.ndarray,
        **kwargs
    ) -> Dict[str, Any]:
        """运行所有注册的探针"""
        results = {}
        for name, probe in self._registry.items():
            try:
                results[name] = self.run_single(name, values, **kwargs)
            except Exception as e:
                results[name] = probe.default()
        return results

    def run_selected(
        self,
        probe_names: List[str],
        values: np.ndarray,
        **kwargs
    ) -> Dict[str, Any]:
        """运行选定的探针"""
        results = {}
        for name in probe_names:
            if name in self._registry:
                results[name] = self.run_single(name, values, **kwargs)
        return results
```

### 3.2 ProbeOutputs 统一接口

```python
# core/probe_outputs/models.py

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
import numpy as np

# 复用现有的探针结果模型
from ...analyzers.trend.models import (
    LogTrendResult,
    VolatilityResult,
    CyclicalPatternResult,
    RecentDeteriorationResult,
    RollingTrendResult,
    RobustTrendResult,
    InflectionResult,
)


@dataclass
class ProbeOutputs:
    """
    单指标的探针输出集合 (统一接口)

    这是 ProbeEngine 和 Evaluators 之间的标准契约。
    所有评估器（ThresholdEvaluator, T.R.U.T.H.）都接收此结构。
    """
    indicator_name: str

    # 8 个核心探针结果
    log_trend: Optional[LogTrendResult] = None
    volatility: Optional[VolatilityResult] = None
    cyclical: Optional[CyclicalPatternResult] = None
    deterioration: Optional[RecentDeteriorationResult] = None
    rolling: Optional[RollingTrendResult] = None
    robust: Optional[RobustTrendResult] = None
    inflection: Optional[InflectionResult] = None
    multi_horizon: Optional[Any] = None  # MultiHorizonResult

    # 原始数据（用于降级计算）
    raw_values: Optional[np.ndarray] = None

    # 元信息
    data_quality: str = "unknown"
    effective_years: int = 0

    def has_core_probes(self) -> bool:
        """检查核心探针是否齐全"""
        return all([
            self.log_trend is not None,
            self.volatility is not None,
            self.cyclical is not None,
            self.deterioration is not None,
        ])

    def missing_probes(self) -> List[str]:
        """列出缺失的探针"""
        probes = {
            "log_trend": self.log_trend,
            "volatility": self.volatility,
            "cyclical": self.cyclical,
            "deterioration": self.deterioration,
            "rolling": self.rolling,
            "robust": self.robust,
            "inflection": self.inflection,
        }
        return [name for name, result in probes.items() if result is None]

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典（用于报告生成）"""
        return {
            "indicator_name": self.indicator_name,
            "log_trend": self.log_trend.to_dict() if self.log_trend else None,
            "volatility": self.volatility.to_dict() if self.volatility else None,
            "cyclical": self.cyclical.to_dict() if self.cyclical else None,
            "deterioration": self.deterioration.to_dict() if self.deterioration else None,
            "rolling": self.rolling.to_dict() if self.rolling else None,
            "robust": self.robust.to_dict() if self.robust else None,
            "inflection": self.inflection.to_dict() if self.inflection else None,
        }


@dataclass
class MultiIndicatorProbeOutputs:
    """
    公司级多指标探针输出

    包含一个公司所有指标的探针结果。
    这是 T.R.U.T.H. 基因计算和 ThresholdEvaluator 公司评估的输入。
    """
    company_code: str
    company_name: str = ""

    # 核心财务指标
    roic: Optional[ProbeOutputs] = None
    roe: Optional[ProbeOutputs] = None
    roiic: Optional[ProbeOutputs] = None
    gross_margin: Optional[ProbeOutputs] = None
    net_margin: Optional[ProbeOutputs] = None
    revenue: Optional[ProbeOutputs] = None
    net_profit: Optional[ProbeOutputs] = None
    ocf: Optional[ProbeOutputs] = None
    fcf: Optional[ProbeOutputs] = None

    # 辅助财务数据（用于 δ_fraud 和 V 因子）
    total_assets: float = 0.0
    equity: float = 0.0
    goodwill: float = 0.0
    receivables: float = 0.0

    def get_indicator(self, name: str) -> Optional[ProbeOutputs]:
        """获取指定指标的探针输出"""
        return getattr(self, name.lower(), None)

    def list_available_indicators(self) -> List[str]:
        """列出可用的指标"""
        indicators = [
            "roic", "roe", "roiic", "gross_margin", "net_margin",
            "revenue", "net_profit", "ocf", "fcf"
        ]
        return [i for i in indicators if getattr(self, i) is not None]
```

### 3.3 ThresholdEvaluator 模块

```python
# evaluators/threshold/engine.py

from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Callable
from enum import Enum

from ...core.probe_outputs.models import ProbeOutputs, MultiIndicatorProbeOutputs


class RuleCategory(Enum):
    """规则分类"""
    VETO = "veto"           # 一票否决
    PENALTY = "penalty"     # 扣分规则
    BONUS = "bonus"         # 加分规则
    ADJUSTMENT = "adjustment"  # 调整规则


@dataclass
class ThresholdRule:
    """阈值规则定义"""
    name: str
    category: RuleCategory
    func: Callable
    description: str = ""
    enabled: bool = True


@dataclass
class ThresholdEvaluationResult:
    """阈值评估结果"""
    passes: bool
    score: float

    # 规则执行详情
    veto_triggered: bool = False
    veto_reason: str = ""
    penalties: List[Dict[str, Any]] = field(default_factory=list)
    bonuses: List[Dict[str, Any]] = field(default_factory=list)

    # 策略匹配
    strategies: List[str] = field(default_factory=list)
    strategy_reasons: List[str] = field(default_factory=list)
    strategy_bonus: float = 0.0

    # 警告和建议
    warnings: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)


class ThresholdEvaluator:
    """
    阈值评估器

    职责：
    1. 接收 ProbeOutputs 作为输入
    2. 执行 29+ 业务规则
    3. 匹配投资策略
    4. 输出 Pass/Fail 决策

    与 T.R.U.T.H. 的区别：
    - ThresholdEvaluator: 规则驱动，适合快速筛选
    - T.R.U.T.H.: 模型驱动，适合深度分析
    """

    def __init__(
        self,
        rules: Optional[List[ThresholdRule]] = None,
        strategies: Optional[List[Any]] = None,
        config: Optional[Dict[str, Any]] = None,
    ):
        self.rules = rules or self._default_rules()
        self.strategies = strategies or []
        self.config = config or {}

    def evaluate(
        self,
        probe_outputs: ProbeOutputs,
        context: Optional[Dict[str, Any]] = None,
    ) -> ThresholdEvaluationResult:
        """
        评估单个指标的探针输出

        Args:
            probe_outputs: 探针输出
            context: 额外上下文（如行业信息）

        Returns:
            ThresholdEvaluationResult: 评估结果
        """
        ctx = self._build_context(probe_outputs, context)

        # Phase 1: 运行否决规则
        veto_result = self._run_veto_rules(ctx)
        if veto_result:
            return ThresholdEvaluationResult(
                passes=False,
                score=0.0,
                veto_triggered=True,
                veto_reason=veto_result,
            )

        # Phase 2: 运行扣分规则
        penalties = self._run_penalty_rules(ctx)

        # Phase 3: 运行加分规则
        bonuses = self._run_bonus_rules(ctx)

        # Phase 4: 匹配策略
        strategies, strategy_reasons, strategy_bonus = self._match_strategies(ctx)

        # 计算最终分数
        base_score = 100.0
        penalty_sum = sum(p.get("penalty", 0) for p in penalties)
        bonus_sum = sum(b.get("bonus", 0) for b in bonuses)
        final_score = base_score - penalty_sum + bonus_sum + strategy_bonus
        final_score = max(0.0, min(100.0, final_score))

        return ThresholdEvaluationResult(
            passes=final_score >= self.config.get("pass_threshold", 60.0),
            score=final_score,
            penalties=penalties,
            bonuses=bonuses,
            strategies=strategies,
            strategy_reasons=strategy_reasons,
            strategy_bonus=strategy_bonus,
        )

    def evaluate_company(
        self,
        multi_outputs: MultiIndicatorProbeOutputs,
    ) -> Dict[str, ThresholdEvaluationResult]:
        """评估公司所有指标"""
        results = {}
        for indicator in multi_outputs.list_available_indicators():
            probe_outputs = multi_outputs.get_indicator(indicator)
            if probe_outputs:
                results[indicator] = self.evaluate(probe_outputs)
        return results

    def _build_context(
        self,
        probe_outputs: ProbeOutputs,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """构建规则上下文"""
        # 将 ProbeOutputs 转换为规则所需的上下文格式
        ctx = {
            "metric_name": probe_outputs.indicator_name,
            "log_slope": probe_outputs.log_trend.log_slope if probe_outputs.log_trend else 0.0,
            "r_squared": probe_outputs.log_trend.r_squared if probe_outputs.log_trend else 0.0,
            "cv": probe_outputs.volatility.cv if probe_outputs.volatility else 0.0,
            "detrended_cv": probe_outputs.volatility.detrended_cv if probe_outputs.volatility else 0.0,
            "has_arch_effect": probe_outputs.volatility.has_arch_effect if probe_outputs.volatility else False,
            "is_cyclical": probe_outputs.cyclical.is_cyclical if probe_outputs.cyclical else False,
            "cycle_position": probe_outputs.cyclical.cycle_position if probe_outputs.cyclical else 0.0,
            "deterioration_probability": probe_outputs.deterioration.deterioration_probability if probe_outputs.deterioration else 0.0,
            "recent_3y_slope": probe_outputs.rolling.recent_3y_slope if probe_outputs.rolling else 0.0,
            "trend_acceleration": probe_outputs.rolling.trend_acceleration if probe_outputs.rolling else 0.0,
            # ... 更多字段
        }
        if extra:
            ctx.update(extra)
        return ctx

    def _run_veto_rules(self, ctx: Dict[str, Any]) -> Optional[str]:
        """运行否决规则"""
        for rule in self.rules:
            if rule.category == RuleCategory.VETO and rule.enabled:
                result = rule.func(ctx, self.config)
                if result:
                    return result.message
        return None

    def _run_penalty_rules(self, ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
        """运行扣分规则"""
        penalties = []
        for rule in self.rules:
            if rule.category == RuleCategory.PENALTY and rule.enabled:
                result = rule.func(ctx, self.config)
                if result:
                    penalties.append({
                        "rule": rule.name,
                        "penalty": result.penalty,
                        "reason": result.message,
                    })
        return penalties

    def _run_bonus_rules(self, ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
        """运行加分规则"""
        bonuses = []
        for rule in self.rules:
            if rule.category == RuleCategory.BONUS and rule.enabled:
                result = rule.func(ctx, self.config)
                if result:
                    bonuses.append({
                        "rule": rule.name,
                        "bonus": result.bonus,
                        "reason": result.message,
                    })
        return bonuses

    def _match_strategies(self, ctx: Dict[str, Any]):
        """匹配投资策略"""
        matched = []
        reasons = []
        total_bonus = 0.0
        for strategy in self.strategies:
            result = strategy.evaluate(ctx)
            if result.matched:
                matched.append(result.name)
                reasons.append(result.reason)
                total_bonus += result.score_boost
        return matched, reasons, total_bonus

    def _default_rules(self) -> List[ThresholdRule]:
        """默认规则集（从 rules.py 导入）"""
        # 这里会导入现有的 29+ 规则
        return []
```

---

## 四、实施路线图

### Phase 1: 创建基础设施 (无破坏性)

```
Week 1: 基础层
├── 创建 core/probe_engine/
│   ├── __init__.py
│   ├── engine.py: ProbeEngine 类
│   ├── registry.py: 探针注册
│   └── interface.py: ProbeProtocol
│
├── 创建 core/probe_outputs/
│   ├── __init__.py
│   ├── models.py: ProbeOutputs, MultiIndicatorProbeOutputs
│   ├── adapters.py: 适配器
│   └── builders.py: 构建器
│
└── 编写单元测试
    ├── test_probe_engine.py
    └── test_probe_outputs.py
```

### Phase 2: 迁移 ThresholdEvaluator

```
Week 2: 评估器
├── 创建 evaluators/threshold/
│   ├── __init__.py
│   ├── engine.py: ThresholdEvaluator
│   ├── rules.py: 迁移现有规则
│   ├── strategies.py: 迁移现有策略
│   ├── models.py: 评估结果模型
│   └── config.py: 配置
│
├── 适配 analyzers/trend/core.py
│   └── TrendEvaluator 改为使用 ThresholdEvaluator
│
└── 保持向后兼容
    └── 旧接口调用新实现
```

### Phase 3: 重构 T.R.U.T.H.

```
Week 3: T.R.U.T.H. 统一
├── 修改 truth/adapter.py
│   └── 使用 core/probe_outputs/models.py 的定义
│
├── 修改 truth/processor.py
│   └── DataFrameToProbeConverter 迁移到 ProbeEngine
│
└── 验证
    └── 确保 T.R.U.T.H. 输出不变
```

### Phase 4: 统一报告层

```
Week 4: 报告与文档
├── 更新 reporters/
│   ├── threshold_report_generator.py: 新建
│   └── truth_report_generator.py: 重构
│
├── 更新文档
│   ├── PROBE_ENGINE_README.md
│   ├── THRESHOLD_EVALUATOR_README.md
│   └── 架构图更新
│
└── 集成测试
    └── 端到端流水线测试
```

---

## 五、向后兼容策略

### 5.1 API 保持

```python
# analyzers/trend/core.py - 保持原有 API

class TrendAnalyzer:
    """保持不变的外部接口"""

    def __init__(self, ...):
        # 内部使用 ProbeEngine
        self._probe_engine = ProbeEngine(probes=get_default_probes())

    def build_trend_vector(self) -> TrendVector:
        # 内部实现改变，但返回类型不变
        probe_outputs = self._probe_engine.run_all(self.values_list)
        return self._convert_to_trend_vector(probe_outputs)


class TrendEvaluator:
    """保持不变的外部接口"""

    def __init__(self, ...):
        # 内部使用 ThresholdEvaluator
        self._threshold_evaluator = ThresholdEvaluator(...)

    def evaluate(self, ...) -> TrendEvaluationResult:
        # 代理到新实现
        return self._threshold_evaluator.evaluate(...)
```

### 5.2 弃用警告

```python
import warnings

class TrendRuleEngine:
    """即将弃用，请使用 ThresholdEvaluator"""

    def run(self, ...):
        warnings.warn(
            "TrendRuleEngine 将在 v2.0 中弃用，请使用 ThresholdEvaluator",
            DeprecationWarning,
            stacklevel=2
        )
        # 代理到新实现
        return self._new_impl.run(...)
```

---

## 六、质量保证

### 6.1 测试策略

```
测试金字塔：

        ┌─────────────────┐
        │   E2E Tests     │  Pipeline → Report
        │   (5-10%)       │
        └─────────────────┘
              ▲
        ┌─────────────────┐
        │ Integration     │  ProbeEngine + Evaluator
        │   (20-30%)      │
        └─────────────────┘
              ▲
        ┌─────────────────┐
        │   Unit Tests    │  每个探针、每条规则
        │   (60-70%)      │
        └─────────────────┘
```

### 6.2 性能基准

```python
# 性能要求
PERFORMANCE_TARGETS = {
    "single_probe": 10,      # ms，单探针计算
    "all_probes": 100,       # ms，8探针全量计算
    "batch_1000": 30,        # 秒，1000家公司批量
}
```

---

## 七、文件结构预览

```
src/astock/business_engines/
├── core/                           # 新增：核心共享模块
│   ├── probe_engine/               # 探针引擎
│   │   ├── __init__.py
│   │   ├── engine.py
│   │   ├── registry.py
│   │   └── interface.py
│   │
│   └── probe_outputs/              # 探针输出接口
│       ├── __init__.py
│       ├── models.py
│       ├── adapters.py
│       └── cache.py
│
├── evaluators/                     # 新增：评估器模块
│   ├── __init__.py
│   ├── threshold/                  # 阈值评估器
│   │   ├── __init__.py
│   │   ├── engine.py
│   │   ├── rules.py               # 从 trend/rules.py 迁移
│   │   ├── strategies.py          # 从 trend/strategies.py 迁移
│   │   └── models.py
│   │
│   └── base.py                     # 评估器基类
│
├── analyzers/
│   └── trend/                      # 保持向后兼容
│       ├── core.py                 # 简化，代理到新模块
│       ├── models.py               # 数据模型保持
│       ├── config.py               # 配置保持
│       ├── probes/                 # 探针实现保持
│       │   ├── log_trend_probe.py
│       │   ├── robust_probe.py
│       │   └── ...
│       ├── rules.py                # 弃用，指向 evaluators/threshold/rules.py
│       └── strategies.py           # 弃用，指向 evaluators/threshold/strategies.py
│
├── truth/                          # 重构：使用统一接口
│   ├── adapter.py                  # 使用 core/probe_outputs
│   ├── processor.py                # 使用 ProbeEngine
│   └── ...
│
└── reporters/
    ├── threshold_report_generator.py  # 新增
    └── truth_report_generator.py      # 重构
```

---

## 八、总结

### 重构收益

| 维度 | 重构前 | 重构后 |
|------|--------|--------|
| 代码复用 | 探针逻辑重复实现 | 单一 ProbeEngine |
| 职责清晰 | 混合数学+业务 | 分层解耦 |
| 可测试性 | 难以单独测试规则 | 每层独立测试 |
| 可扩展性 | 新探针需改多处 | 注册即用 |
| 接口一致 | T.R.U.T.H./Trend 不同 | 统一 ProbeOutputs |

### 风险控制

1. **向后兼容**: 保持原有 API，内部重构
2. **渐进式迁移**: 分 4 个阶段，每阶段可独立验证
3. **充分测试**: 单元+集成+E2E 测试覆盖
4. **文档完善**: 架构文档+API文档+迁移指南

---

*文档版本: v1.0*
*创建日期: 2025-01*
*作者: AStock Analysis System*
