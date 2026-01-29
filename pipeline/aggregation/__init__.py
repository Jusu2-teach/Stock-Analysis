"""
Pipeline Aggregation - PDDA 数据聚合框架
========================================

Producer-Driven Data Aggregation (PDDA) 的核心实现。

模块结构:
    - core: 核心类型和存储 (AggregatableResult, AggregationScope, Collector)
    - inject: 智能依赖注入 (Injector, inject 装饰器)
    - lineage: 数据血缘追踪 (DataLineage, LineageTracker)

Quick Start:
    # 生产者
    from pipeline.aggregation import AggregatableResult

    @register_method(...)
    def analyze_metric(data, metric_name: str) -> AggregatableResult:
        result_df = analyze(data, metric_name)
        return AggregatableResult.of(metric_name, result_df).in_namespace("trends")

    # 消费者
    from pipeline.aggregation import inject

    @inject()
    def run_evaluator(aggregated_trends: Dict[str, DataFrame]) -> Dict:
        for metric, df in aggregated_trends.items():
            evaluate(metric, df)

Architecture:
    ┌─────────────────────────────────────────────────────────────────────┐
    │                        AggregationScope                              │
    │                      (FlowRun 级别隔离)                              │
    │                                                                      │
    │   ┌─────────────────────────────────────────────────────────────┐   │
    │   │  Namespace: "trends"                                         │   │
    │   │  ┌──────────┬──────────┬──────────┬──────────┬──────────┐  │   │
    │   │  │  roic    │   roe    │  roiic   │ revenue  │  profit  │  │   │
    │   │  │ (step1)  │ (step2)  │ (step3)  │ (step4)  │ (step5)  │  │   │
    │   │  └──────────┴──────────┴──────────┴──────────┴──────────┘  │   │
    │   └─────────────────────────────────────────────────────────────┘   │
    │                              ▲                                       │
    │                              │                                       │
    │   Producers                  │               Consumer                │
    │   ──────────                 │               ────────                │
    │   analyze_roic()  ───────────┤                                       │
    │   analyze_roe()   ───────────┤                                       │
    │   analyze_roiic() ───────────┼──────────▶  run_evaluator()          │
    │   analyze_revenue()──────────┤             (auto-injected)          │
    │   analyze_profit()───────────┘                                       │
    │                                                                      │
    └─────────────────────────────────────────────────────────────────────┘

Version: 2.0.0
"""

__version__ = "2.0.0"

# =============================================================================
# Core (Result, Scope, Collector)
# =============================================================================

from .core import (
    # 结果类型
    AggregatableResult,
    ResultMetadata,
    # 作用域
    AggregationScope,
    ScopeManager,
    # 收集器
    Collector,
    # 策略
    ConflictStrategy,
    # 异常
    AggregationError,
    KeyConflictError,
    NamespaceNotFoundError,
    ValidationError,
    # 便捷函数
    get_current_scope,
)

# =============================================================================
# Injection
# =============================================================================

from .inject import (
    # 注入器
    Injector,
    # 装饰器
    inject,
    injectable,
    # 类型
    Aggregated,
    InjectionSpec,
    # 异常
    InjectionError,
    MissingDependencyError,
    # 工具
    get_injection_specs,
    describe_injection,
)

# =============================================================================
# Lineage
# =============================================================================

from .lineage import (
    # 数据结构
    DataLineage,
    LineageNode,
    LineageEdge,
    NodeType,
    # 追踪器
    LineageTracker,
    # 查询
    LineageQuery,
    # 装饰器
    track_lineage,
)

# =============================================================================
# Public API
# =============================================================================

__all__ = [
    # === Core ===
    # 结果
    "AggregatableResult",
    "ResultMetadata",
    # 作用域
    "AggregationScope",
    "ScopeManager",
    "get_current_scope",
    # 收集器
    "Collector",
    # 策略
    "ConflictStrategy",
    # 异常
    "AggregationError",
    "KeyConflictError",
    "NamespaceNotFoundError",
    "ValidationError",

    # === Injection ===
    "Injector",
    "inject",
    "injectable",
    "Aggregated",
    "InjectionSpec",
    "InjectionError",
    "MissingDependencyError",
    "get_injection_specs",
    "describe_injection",

    # === Lineage ===
    "DataLineage",
    "LineageNode",
    "LineageEdge",
    "NodeType",
    "LineageTracker",
    "LineageQuery",
    "track_lineage",
]


# =============================================================================
# Module-level Singletons
# =============================================================================

def get_scope_manager() -> ScopeManager:
    """获取全局 ScopeManager 实例"""
    return ScopeManager.instance()


def get_lineage_tracker() -> LineageTracker:
    """获取全局 LineageTracker 实例"""
    return LineageTracker.instance()
