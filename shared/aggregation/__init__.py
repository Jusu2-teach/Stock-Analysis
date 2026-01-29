"""
PDDA (Protocol-Driven Dynamic Aggregation) System
===================================================

一个零配置、约定驱动的通用数据聚合框架。

核心特性：
- 🎯 Protocol-Driven: 基于 Python Protocol (PEP 544) 的类型系统
- 🚀 Zero-Config: 通过类型标注和约定自动工作
- 🔌 Pluggable: 可扩展的策略和钩子系统
- 🌐 Universal: 与业务逻辑完全解耦

架构层次：
- Layer 7: Convention (约定层)
- Layer 6: Type System (类型系统)
- Layer 5: Discovery Engine (发现引擎)
- Layer 4: Collection Engine (收集引擎)
- Layer 3: Injection Engine (注入引擎)
- Layer 2: Decorators (装饰器)
- Layer 1: Event Bus (事件总线)

使用示例：

    # 生产者：返回可聚合类型
    @register_method(...)
    def analyze_metric_trend(...) -> AggregatableResult[str, pd.DataFrame]:
        return AggregatableResult(key="roic", value=df)

    # 消费者：声明需求
    @register_method(...)
    def report_comprehensive(aggregated_trends: Dict[str, pd.DataFrame]):
        # PDDA 自动注入收集的数据
        pass

版本: 2.0.0
作者: AStock Team
"""

from .protocols import (
    Aggregatable,
    AggregatableResult,
    AggregationMetadata,
)

from .conventions import (
    NamingConvention,
    TypeConvention,
    ProtocolConvention,
)

from .discovery import (
    MethodScanner,
    CapabilityInfo,
    ProducerInfo,
    ConsumerInfo,
)

from .collector import (
    UniversalCollector,
    CollectionStrategy,
    DictCollectorStrategy,
    ListCollectorStrategy,
)

from .injector import (
    ParameterResolver,
    DynamicInjector,
    InjectionContext,
)

from .decorators import (
    aggregatable,
    consumer,
    before_collect,
    after_collect,
)

from .manager import (
    AggregationManager,
)

__all__ = [
    # Protocols
    'Aggregatable',
    'AggregatableResult',
    'AggregationMetadata',

    # Conventions
    'NamingConvention',
    'TypeConvention',
    'ProtocolConvention',

    # Discovery
    'MethodScanner',
    'CapabilityInfo',
    'ProducerInfo',
    'ConsumerInfo',

    # Collection
    'UniversalCollector',
    'CollectionStrategy',
    'DictCollectorStrategy',
    'ListCollectorStrategy',

    # Injection
    'ParameterResolver',
    'DynamicInjector',
    'InjectionContext',

    # Decorators
    'aggregatable',
    'consumer',
    'before_collect',
    'after_collect',

    # Manager
    'AggregationManager',
]

__version__ = '2.0.0'
