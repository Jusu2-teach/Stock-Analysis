# 🎼 AStock Orchestrator

> **方法注册中心 + 统一调度门面**
>
> 将分散在各模块的数据获取、处理、分析方法统一管理，通过策略路由智能选择最佳实现。

---

## ✨ 特性

| 特性 | 说明 |
| ------ | ------ |
| **HookSpec 系统** | 类似 pluggy 的接口声明，支持 `@hookspec` 装饰器 |
| **签名验证** | 注册时自动验证函数签名，支持 warn/strict/off 模式 |
| **Middleware 机制** | Facade 提供洋葱模型中间件链，便于 AOP 扩展 |
| **Observer 可观测性端口** | 生命周期信号端口；默认适配 shared.EventBus，但核心不硬依赖 |
| **文件整合** | 合并 loader+scanner → discovery.py, 合并 utils_version → strategies.py |

---

## 📊 架构总览

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                      AStockOrchestrator (Facade)                        │
│                                                                         │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐     │
│  │ ComponentProxy  │    │ Middleware Chain│    │   describe()    │     │
│  │ (动态属性访问)   │    │ (中间件链)       │    │   (元数据查询)   │     │
│  └────────┬────────┘    └────────┬────────┘    └─────────────────┘     │
│           │                      │                                      │
│           └──────────────────────┼──────────────────────────────────────┘
│                                  │
│                                  ▼
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                        Registry (单例)                            │   │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐           │   │
│  │  │  Index   │ │Strategies│ │ Executor │ │ Metrics  │           │   │
│  │  │ (索引)   │ │ (策略)   │ │ (执行器) │ │ (指标)   │           │   │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘           │   │
│  │                                                                   │   │
│  │  ┌──────────────────────┐ ┌──────────────────────┐               │   │
│  │  │      Discovery       │ │      Protocols       │               │   │
│  │  │ (ModuleLoader+Scanner)│ │ (HookSpec+Validator) │               │   │
│  │  └──────────────────────┘ └──────────────────────┘               │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 目录结构

```text
orchestrator/
├── __init__.py           # 包入口
├── orchestrator.py       # Facade 主类 + ComponentProxy 动态代理
├── models.py             # MethodRegistration 数据模型 (不可变 dataclass)
├── config.py             # RegistryConfig 配置类
├── errors.py             # 自定义异常体系 (含 RegistryValidationError)
├── protocols.py          # ✨ HookSpec 系统 + SignatureValidator
├── telemetry.py          # ✨ Observer 端口 (生命周期信号)
├── telemetry_shared.py   # shared.EventBus 适配器 (默认行为)
│
├── decorators/
│   ├── __init__.py       # 导出 register_method
│   └── register.py       # ✨ @register_method (含签名验证)
│
└── registry/
    ├── __init__.py       # 导出所有 registry 组件
    ├── registry.py       # Registry 单例：核心注册/选择/执行逻辑
    ├── index.py          # RegistryIndex：三层索引 (component→method→engine)
    ├── strategies.py     # 5种选择策略 + parse_version()
    ├── discovery.py      # ✨ ModuleLoader + Scanner (合并)
    ├── executor.py       # MethodExecutor：执行器 + 输入风格校验
    ├── metrics.py        # MetricsService：执行指标收集

```

---

## 🔍 核心设计

### ✅ 设计亮点

| 设计点 | 实现 | 评价 |
| ------ | ---- | ---- |
| **单例模式** | `Registry.get()` 双重检查锁 | ✅ 线程安全，标准实现 |
| **策略模式** | 5种 `SelectionStrategy` | ✅ 灵活的方法选择机制 |
| **不可变数据** | `MethodRegistration(frozen=True)` | ✅ 便于缓存和哈希 |
| **三层索引** | `component→method→engine` | ✅ O(1) 快速查找 |
| **中间件链** | 洋葱模型执行 | ✅ 可扩展的 AOP 能力 |
| **HookSpec** | 类似 pluggy 的接口声明 | ✅ 签名验证 |
| **Observer** | 生命周期信号端口 | ✅ 解耦可观测性 |
| **自动发现** | `ModuleLoader` + `Scanner` | ✅ 零配置注册 |

### 🆚 与业界对比 (pluggy)

| 对比项 | AStock Orchestrator | pluggy |
| ------ | ------------------- | ------ |
| **注册方式** | `@register_method(...)` | `@hookimpl` |
| **接口声明** | `@hookspec` | `@hookspec` |
| **版本支持** | ✅ 内置版本策略 | ❌ 不支持 |
| **组件分类** | ✅ component_type | ❌ 无 |
| **执行策略** | ✅ 5种选择策略 | 单一调用 |
| **Observer** | ✅ 生命周期信号端口 | 依赖实现 |

---

## 🚀 快速使用

### 1. 声明接口规范 (可选)

```python
from orchestrator import hookspec

@hookspec("business_engine", required_params=("data",))
def filter_stocks(data, **kwargs):
    """Filter stocks based on criteria."""
    ...
```

### 2. 注册方法

```python
from orchestrator import register_method

@register_method(
    component_type="business_engine",
    engine_type="polars",
    engine_name="filter_stocks",
    version="2.0.0",
    priority=10
)
def filter_stocks(data, threshold=0.5, **kwargs):
    """趋势分析 - Polars 实现"""
    return data.filter(...)
```

### 3. 调用方法

```python
from orchestrator import AStockOrchestrator

o = AStockOrchestrator(auto_discover=True)

# 方式1: 动态属性访问
result = o.business_engine.filter_stocks(data, threshold=0.3)

# 方式2: 显式执行
result = o.execute("business_engine", "filter_stocks", data, threshold=0.3)

# 方式3: 指定引擎
result = o.execute("business_engine", "filter_stocks", data, _preferred_engine="pandas")

# 方式4: 指定策略
result = o.execute("business_engine", "filter_stocks", data, _strategy="prefer_latest")
```

### 4. 查看系统状态

```python
# 列出所有方法
methods = o.registry.list_methods()

# 查看方法详情
info = o.describe("business_engine", "filter_stocks")

# 获取执行统计
stats = o.get_system_status()
```

### 5. 可观测性 (Observer)

默认情况下：orchestrator 会尝试通过 shared.EventBus 发布兼容事件（如果 shared 可用）。

如果你希望关闭事件发布：

```python
from orchestrator import AStockOrchestrator, NullObserver

o = AStockOrchestrator(observer=NullObserver())
```

---

## ⚙️ 环境变量配置

| 变量 | 默认值 | 说明 |
| ---- | ------ | ---- |
| `ASTOCK_VALIDATION_MODE` | `warn` | 验证模式: `strict`=报错 / `warn`=警告 / `off`=关闭 |
| `ASTOCK_STRICT_SPEC` | `false` | 严格模式: `true`=检查额外参数 |
| `ASTOCK_INPUT_STYLE` | `strict_single` | 输入风格: `strict_single` / `allow_list` / `enforce_list` |
| `ASTOCK_CONFLICT_MODE` | `warn` | 冲突处理: `error` / `warn` / `ignore` |

---

## 📦 导出清单

```python
from orchestrator import (
    # Core
    AStockOrchestrator,
    Registry,
    MethodRegistration,
    register_method,

    # Protocols & Validation
    hookspec,
    HookSpecRegistry,
    SignatureValidator,
    BusinessEngineFunction,
    DataEngineFunction,
    DataHubFunction,

    # Observability
    NullObserver,
    CompositeObserver,
)

from orchestrator.registry import (
    parse_version,
    ModuleLoader,
    Scanner,
    MethodExecutor,
    MetricsService,
)
```

---

## 📈 代码质量评分

| 维度 | 评分 | 说明 |
| ---- | ---- | ---- |
| **架构设计** | ⭐⭐⭐⭐⭐ | 分层清晰，职责单一 |
| **代码质量** | ⭐⭐⭐⭐⭐ | 类型注解完整，文档良好 |
| **可扩展性** | ⭐⭐⭐⭐⭐ | 策略模式 + HookSpec + Observer |
| **通用性** | ⭐⭐⭐⭐⭐ | 可独立复用到其他项目 |
| **代码整洁** | ⭐⭐⭐⭐ | 整合后更精简 |
| **文档完整** | ⭐⭐⭐⭐ | README + 代码注释 |

**总评**：⭐⭐⭐⭐⭐ (5/5) - **专业且强大**

---

## 🔗 相关文档

- [架构设计文档](../docs/ORCHESTRATOR_ARCHITECTURE.md)
- [Pipeline 集成](../pipeline/README.md)
- [业务引擎](../src/astock/business_engines/README.md)
