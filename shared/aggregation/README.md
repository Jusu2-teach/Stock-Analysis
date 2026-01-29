# PDDA (Protocol-Driven Dynamic Aggregation) System

[![Version](https://img.shields.io/badge/version-2.0.0-blue.svg)](https://github.com/yourusername/pdda)
[![Python](https://img.shields.io/badge/python-3.10+-brightgreen.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-yellow.svg)](LICENSE)

一个零配置、约定驱动的通用数据聚合框架。

---

## 🎯 核心特性

- **🚀 零配置**: 通过类型标注和约定自动工作，无需额外配置文件
- **🎯 Protocol-Driven**: 基于 Python Protocol (PEP 544) 的类型系统
- **🔌 可扩展**: 插件化的收集策略和生命周期钩子
- **🌐 通用**: 与业务逻辑完全解耦，可用于任何领域
- **💡 智能**: 自动发现生产者和消费者，自动匹配和注入
- **🔒 类型安全**: 完整的类型提示和运行时验证

---

## 📚 快速开始

### 1. 定义可聚合类型

```python
from shared.aggregation import AggregatableResult

# 方式 1: 使用内置实现
def analyze_metric_trend(...) -> AggregatableResult[str, pd.DataFrame]:
    result_df = pd.DataFrame(...)
    return AggregatableResult(key="roic", value=result_df)

# 方式 2: 实现 Aggregatable 协议
@dataclass
class TrendAnalysisResult(Aggregatable[str, pd.DataFrame]):
    metric_name: str
    result_df: pd.DataFrame

    def get_aggregation_key(self) -> str:
        return self.metric_name

    def get_aggregation_value(self) -> pd.DataFrame:
        return self.result_df
```

### 2. 声明消费需求

```python
# 方式 1: 通过命名约定（最简单）
def report_comprehensive(
    aggregated_trends: Dict[str, pd.DataFrame]  # 自动识别
):
    for metric, df in aggregated_trends.items():
        # 使用聚合数据
        pass

# 方式 2: 使用装饰器（更明确）
from shared.aggregation import consumer

@consumer("aggregated_trends", min_items=3)
def report_comprehensive(aggregated_trends: Dict[str, pd.DataFrame]):
    pass
```

### 3. 集成到 Pipeline

```python
from shared.aggregation import AggregationManager

# 在 ExecuteManager 中初始化
class ExecuteManager:
    def __init__(self):
        self.aggregation_manager = AggregationManager.get()
        self.aggregation_manager.initialize()

    def execute_step(self, step):
        # 使用 PDDA 执行（自动注入）
        result = self.aggregation_manager.execute(
            method_name=step.method,
            func=step.func,
            params=step.params
        )
        return result
```

---

## 🏗️ 架构设计

```
Layer 7: Convention (约定层)
  └─ NamingConvention, TypeConvention, ProtocolConvention

Layer 6: Type System (类型系统)
  └─ Aggregatable[K, V], AggregatableResult

Layer 5: Discovery Engine (发现引擎)
  └─ MethodScanner, ProducerInfo, ConsumerInfo

Layer 4: Collection Engine (收集引擎)
  └─ UniversalCollector, CollectionStrategy

Layer 3: Injection Engine (注入引擎)
  └─ DynamicInjector, ParameterResolver

Layer 2: Decorators (装饰器)
  └─ @aggregatable, @consumer

Layer 1: Event Bus (事件总线)
  └─ 集成现有 EventBus
```

---

## 📖 核心概念

### Aggregatable Protocol

定义"可聚合"的能力：

```python
class Aggregatable(Protocol[K, V]):
    def get_aggregation_key(self) -> K:
        """返回聚合键"""
        ...

    def get_aggregation_value(self) -> V:
        """返回聚合值"""
        ...
```

### Convention Over Configuration

通过约定自动识别：

1. **命名约定**: `aggregated_*`, `*_frames`, `*_results` 等参数名
2. **类型约定**: `Dict[str, X]`, `List[X]` 等聚合类型
3. **协议约定**: 实现 `Aggregatable` 协议的返回值

---

## 🔧 高级用法

### 自定义收集策略

```python
from shared.aggregation import CollectionStrategy

class CustomStrategy(CollectionStrategy):
    def collect(self, item):
        # 自定义收集逻辑
        pass

    def get_all(self):
        # 返回自定义格式
        pass

# 使用自定义策略
manager = AggregationManager.get()
manager.set_collection_strategy(CustomStrategy())
```

### 生命周期钩子

```python
from shared.aggregation import before_collect, after_collect

@before_collect
def validate_data(item):
    # 收集前验证
    if not is_valid(item):
        raise ValueError("数据无效")

@after_collect
def log_collection(item):
    # 收集后记录日志
    logger.info(f"已收集: {item.get_aggregation_key()}")
```

### 装饰器增强

```python
from shared.aggregation import aggregatable

@register_method(...)
@aggregatable(key="metric_name", value="result_df", auto_collect=True)
def analyze_trend(...) -> pd.DataFrame:
    # 返回普通 DataFrame，自动包装为 Aggregatable
    return df
```

---

## 📊 API 参考

### AggregationManager

主要门面类：

- `get()`: 获取单例实例
- `initialize()`: 初始化系统（扫描方法）
- `execute(method_name, func, params)`: 执行方法（自动注入）
- `collect(result)`: 手动收集数据
- `get_collected_data()`: 获取所有收集的数据
- `get_stats()`: 获取统计信息

### MethodScanner

方法扫描器：

- `scan_all_methods()`: 扫描所有已注册方法
- `get_all_producers()`: 获取所有生产者
- `get_all_consumers()`: 获取所有消费者
- `match_producers_to_consumer(consumer_method)`: 匹配生产者

### UniversalCollector

数据收集器：

- `collect(item)`: 收集数据项
- `get_all()`: 获取所有数据
- `clear()`: 清空数据
- `set_strategy(strategy)`: 设置收集策略

---

## 🎨 设计原则

1. **零配置**: 通过约定和类型标注自动工作
2. **零硬编码**: 不假设具体类型名、字段名或参数名
3. **零侵入**: 对业务代码完全透明
4. **完全通用**: 与业务逻辑解耦，可复用
5. **类型安全**: 运行时类型检查和验证
6. **可扩展**: 插件化的策略和钩子

---

## 🔍 调试与监控

### 启用调试日志

```python
import logging
logging.getLogger('shared.aggregation').setLevel(logging.DEBUG)
```

### 获取统计信息

```python
manager = AggregationManager.get()
stats = manager.get_stats()

print(f"生产者数量: {stats['scanner']['total_producers']}")
print(f"消费者数量: {stats['scanner']['total_consumers']}")
print(f"收集的数据项: {stats['collector']['size']}")
```

---

## 📝 最佳实践

1. **优先使用类型标注**: 让 PDDA 自动工作，无需装饰器
2. **遵循命名约定**: 使用 `aggregated_*` 等标准参数名
3. **显式类型提示**: 帮助 IDE 自动补全和类型检查
4. **最小化装饰器**: 仅在需要额外约束时使用
5. **测试覆盖**: 为自定义策略和钩子编写测试

---

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

---

## 📄 许可证

MIT License

---

## 🔗 相关资源

- [Python Protocols (PEP 544)](https://peps.python.org/pep-0544/)
- [Type Hints (PEP 484)](https://peps.python.org/pep-0484/)
- [Dataclasses (PEP 557)](https://peps.python.org/pep-0557/)
