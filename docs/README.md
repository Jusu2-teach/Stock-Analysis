# 📚 AStock 架构文档

欢迎阅读 AStock 智能股票分析系统的架构文档！

---

## 📖 文档导航

### 🎯 [Orchestrator 架构文档](./ORCHESTRATOR_ARCHITECTURE.md)

**适合阅读人群**：新手开发者

**内容概述**：
- 什么是 Orchestrator（方法注册与调用中心）
- 架构设计和目录结构
- 工作流程（方法注册 → 方法调用 → 方法选择）
- 核心组件详解（Registry、Index、Strategy、ComponentProxy）
- 完整调用流程图
- 新手上手指南（添加新数据源、新组件、测试特定实现）
- 常见问题解答

**关键概念**：
- **方法注册**：使用 `@register_method` 装饰器
- **动态调用**：`orchestrator.datahub.get_data()`
- **策略选择**：default / latest / stable / priority
- **引擎切换**：灵活切换不同实现（tushare / akshare）

---

### 🔄 [Pipeline 架构文档](./PIPELINE_ARCHITECTURE.md)

**适合阅读人群**：新手开发者

**内容概述**：
- 什么是 Pipeline（工作流编排引擎）
- 架构设计和目录结构
- 工作流程（配置定义 → 解析配置 → 构建节点 → 执行流程）
- 缓存机制（签名计算、缓存判断、失效原因）
- 核心组件详解（ExecuteManager、ConfigService、FlowExecutor、KedroEngine、MethodHandle）
- 完整执行流程图
- 新手上手指南（创建 Pipeline、数据传递、方法链、运行时参数）
- 常见问题解答

**关键概念**：
- **YAML 配置**：定义工作流步骤
- **依赖管理**：自动检测步骤间依赖关系
- **智能缓存**：相同输入自动复用结果
- **方法链**：一个步骤链式调用多个方法
- **延迟绑定**：MethodHandle 解决循环依赖

---

## 🔌 Pipeline 与 Orchestrator 解耦机制

### 核心问题

**循环依赖困境**：
```
Pipeline (ExecuteManager) → 需要调用 → Orchestrator 的方法
         ↓
   但在初始化时构建节点配置（build_auto_nodes）
         ↓
   此时 Orchestrator 还在 auto_discover（加载方法注册）
         ↓
   无法直接调用 orchestrator.execute()
```

### 解决方案：MethodHandle（延迟绑定）

#### 1️⃣ **Protocol 接口抽象**

使用 Protocol 定义接口契约，避免直接导入实现类：

```python
# pipeline/core/protocols/method_handle_protocol.py
from typing import Protocol

class IMethodHandle(Protocol):
    """方法句柄接口（只声明契约，不依赖实现）"""
    component: str
    method: str

    def resolve(self, orchestrator) -> str:
        """解析引擎类型"""
        ...

    def execute(self, orchestrator, *args, **kwargs):
        """执行方法"""
        ...

def create_method_handle(component, method, prefer='auto', fixed_engine=None):
    """工厂方法：延迟导入实现类"""
    from pipeline.core.handles.method_handle import MethodHandle
    return MethodHandle(component, method, prefer=prefer, fixed_engine=fixed_engine)
```

**优势**：
- ✅ ConfigService 只依赖 `IMethodHandle` 接口（Protocol）
- ✅ 不直接导入 `MethodHandle` 实现类
- ✅ 通过工厂方法 `create_method_handle()` 创建实例
- ✅ 避免了循环依赖

---

#### 2️⃣ **配置解析阶段：创建句柄**

```python
# pipeline/core/services/config_service.py
def build_auto_nodes(self):
    """构建节点配置（不执行方法）"""
    from pipeline.core.protocols import create_method_handle

    for step in steps:
        # 为每个方法创建 MethodHandle（仅记录信息）
        handles = []
        for method_name in step.methods:
            if step.engine == 'auto':
                # 自动模式：运行时选择最优引擎
                handle = create_method_handle(
                    component=step.component,
                    method=method_name,
                    prefer='auto'
                )
            else:
                # 固定模式：指定引擎
                handle = create_method_handle(
                    component=step.component,
                    method=method_name,
                    prefer='fixed',
                    fixed_engine=step.engine
                )
            handles.append(handle)

        # 将句柄附加到节点配置
        node_config['handles'] = handles
```

**关键点**：
- ⚠️ 此时**不调用** `orchestrator.execute()`
- ✅ 只创建 MethodHandle 实例（轻量级对象）
- ✅ 记录方法信息：`(component, method, prefer, fixed_engine)`

---

#### 3️⃣ **运行阶段：延迟解析和执行**

```python
# pipeline/engines/kedro_engine.py
def _create_kedro_node(self, node_config):
    """创建 Kedro 节点（包含执行逻辑）"""

    def execute_node(*args, **kwargs):
        """节点执行函数（运行时才调用）"""

        # 1. 从配置中获取 MethodHandle
        method_handles = node_config.get('handles', [])

        # 2. 运行时解析引擎（此时 orchestrator 已完全初始化）
        for handle in method_handles:
            # resolve() 调用 orchestrator.describe() 选择最优引擎
            engine_type = handle.resolve(orchestrator)

            # 3. 通过 orchestrator 执行方法
            result = orchestrator.execute(
                component=handle.component,
                method=handle.method,
                *args,
                **kwargs
            )

        return result

    return Node(func=execute_node, ...)
```

**关键点**：
- ✅ `resolve()` 在**运行时**才调用（orchestrator 已初始化完成）
- ✅ 通过 `orchestrator.describe()` 获取所有候选实现
- ✅ 使用策略选择最优引擎（优先级 > 版本 > 非废弃）

---

#### 4️⃣ **MethodHandle 内部逻辑**

```python
# pipeline/core/handles/method_handle.py
class MethodHandle:
    def __init__(self, component, method, prefer='auto', fixed_engine=None):
        """初始化（轻量级，不做耗时操作）"""
        self.component = component
        self.method = method
        self.prefer = prefer
        self.fixed_engine = fixed_engine
        self._resolved_engine = None  # 延迟解析
        self._ttl = 5.0  # 缓存 5 秒

    def resolve(self, orchestrator) -> str:
        """解析引擎（带缓存）"""
        # 1. 固定引擎：直接返回
        if self.fixed_engine:
            return self.fixed_engine

        # 2. 缓存有效：直接返回
        if self._is_cache_valid():
            return self._resolved_engine

        # 3. 调用 orchestrator.describe() 获取候选
        desc = orchestrator.describe(self.component, self.method)
        implementations = desc.get('implementations', [])

        # 4. 过滤 + 排序
        active = [i for i in implementations if not i.get('deprecated')]
        sorted_impls = sorted(
            active,
            key=lambda x: (x.get('priority', 0), parse_version(x.get('version'))),
            reverse=True
        )

        # 5. 选择最优
        best = sorted_impls[0]
        self._resolved_engine = best['engine_type']
        self._resolved_at = time.time()

        return self._resolved_engine

    def predict_signature(self, orchestrator) -> str:
        """预测缓存签名（用于缓存判断）"""
        # 格式: method@engine:version:priority
        engine = self.resolve(orchestrator)
        desc = orchestrator.describe(self.component, self.method)
        selected = next(i for i in desc['implementations'] if i['engine_type'] == engine)
        return f"{self.method}@{engine}:{selected['version']}:{selected['priority']}"
```

**特性**：
- ✅ **延迟解析**：第一次调用 `resolve()` 才查询 orchestrator
- ✅ **短期缓存**：解析结果缓存 5 秒（避免重复查询）
- ✅ **策略选择**：优先级 > 版本 > 非废弃
- ✅ **签名预测**：用于缓存系统判断方法是否变化

---

### 时序图

```
配置解析阶段 (t=0)
====================
ConfigService.build_auto_nodes()
    ↓
create_method_handle(component, method, prefer='auto')
    ↓
MethodHandle.__init__()  # 只记录信息，不解析
    ↓
return MethodHandle 实例（轻量级）
    ↓
node_config['handles'] = [handle1, handle2, ...]


运行阶段 (t=5s, orchestrator 已初始化)
=======================================
KedroEngine._create_kedro_node()
    ↓
def execute_node():
    handle.resolve(orchestrator)  # ← 此时才解析！
        ↓
    orchestrator.describe(component, method)
        ↓
    Registry.index.method_candidates()
        ↓
    Strategy.select(candidates)
        ↓
    return 'tushare'  # 选中的引擎
        ↓
    orchestrator.execute(component, method, *args)
        ↓
    Registry.execute_with_engine(component, 'tushare', method, *args)
        ↓
    return result
```

---

### 核心优势

| 特性 | 传统方式 | MethodHandle 方式 |
|------|---------|------------------|
| **依赖关系** | 紧耦合（直接调用） | 松耦合（接口抽象） |
| **初始化顺序** | 必须先初始化 orchestrator | 可以并行初始化 |
| **循环依赖** | ❌ 存在 | ✅ 避免 |
| **灵活性** | 写死引擎类型 | 运行时动态选择 |
| **缓存支持** | 需要手动实现 | 内置智能缓存 |
| **测试性** | 难以 Mock | 易于 Mock（接口清晰） |

---

### 实际示例

#### YAML 配置

```yaml
steps:
  step1:
    component: datahub
    methods: [get_data]
    engine: auto  # ← 自动选择最优引擎
    parameters:
      stock_code: "000001.SZ"
```

#### 执行流程

```
1. ConfigService 解析 YAML
   ↓
2. create_method_handle('datahub', 'get_data', prefer='auto')
   ↓
3. MethodHandle 实例存储在 node_config['handles']
   ↓
4. KedroEngine 执行节点
   ↓
5. handle.resolve(orchestrator)
   ├─ orchestrator.describe('datahub', 'get_data')
   ├─ 返回候选: [tushare (v1.0, pri=10), akshare (v2.0, pri=5)]
   ├─ 策略选择: tushare（优先级更高）
   └─ 返回: 'tushare'
   ↓
6. orchestrator.execute('datahub', 'get_data', stock_code="000001.SZ")
   ↓
7. 调用 tushare.get_data("000001.SZ")
   ↓
8. 返回结果
```

---

## 🎯 阅读建议

### 对于新手

1. **先读 Orchestrator**：理解方法如何注册和调用
2. **再读本章节**：理解 Pipeline 如何与 Orchestrator 解耦
3. **最后读 Pipeline**：理解完整的工作流编排
4. **动手实践**：按照文档中的示例代码尝试

### 对于有经验的开发者

1. 快速浏览两个文档的"架构设计"和"核心组件"部分
2. 重点阅读本章节理解**解耦机制**（MethodHandle + Protocol）
3. 查看"完整流程图"了解数据流转
4. 参考"常见问题"解决特定问题

---

## 🔑 核心区别

| 维度 | Orchestrator | Pipeline |
|------|--------------|----------|
| **定位** | 方法注册与调用中心 | 工作流编排引擎 |
| **使用方式** | Python 代码调用 | YAML 配置驱动 |
| **粒度** | 单个方法 | 多个方法组合 |
| **依赖关系** | 无（独立调用） | 有（步骤间依赖） |
| **缓存** | 无 | 有（智能缓存） |
| **典型场景** | 简单数据获取 | 复杂数据分析流水线 |

**简单类比**：
- **Orchestrator**：函数库（提供函数 `get_data()`）
- **Pipeline**：脚本（调用多个函数，处理依赖，`get_data() → clean() → analyze()`）

---

## 🛠️ 快速开始

### 使用 Orchestrator（简单调用）

```python
from orchestrator import AStockOrchestrator

# 初始化
orchestrator = AStockOrchestrator(auto_discover=True)

# 调用方法
data = orchestrator.datahub.get_data("000001.SZ")
result = orchestrator.data_engines.process_data(data)
```

### 使用 Pipeline（复杂流水线）

```yaml
# my_pipeline.yaml
pipeline:
  steps:
    step1:
      component: datahub
      methods: [get_data]
      parameters:
        stock_code: "000001.SZ"
      outputs:
        - name: data
          type: dataset

    step2:
      component: data_engines
      methods: [clean_data]
      parameters:
        data: "steps.step1.outputs.parameters.data"
      outputs:
        - name: result
          type: dataset
```

```python
from pipeline.core.execute_manager import ExecuteManager

manager = ExecuteManager(config_path="my_pipeline.yaml")
result = manager.run()
```

---

## 📞 联系与贡献

如果你在阅读文档时有任何疑问或建议，欢迎：
- 提交 Issue
- 提交 Pull Request
- 联系维护者

---

**最后更新**：2025年11月2日
**文档版本**：v1.0
