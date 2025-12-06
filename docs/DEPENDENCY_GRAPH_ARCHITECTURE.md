# Pipeline 依赖管理架构

## 设计原则

本实现遵循以下专业级软件设计原则：

### 1. SOLID 原则

- **单一职责 (SRP)**: `DependencyGraph` 只负责依赖建模和拓扑排序
- **开闭原则 (OCP)**: 通过 `DependencySource` 接口扩展新的依赖类型
- **依赖反转 (DIP)**: 高层模块依赖抽象接口而非具体实现

### 2. 参考工业标准

- **Apache Airflow**: DAG 依赖模型
- **Prefect**: Flow 层次执行
- **Kedro**: Pipeline 拓扑排序
- **Dagster**: Asset 依赖图

## 核心组件

```
pipeline/core/
├── dependency_graph.py      # 依赖图核心实现
├── context.py               # Pipeline 上下文
└── services/
    └── config_service.py    # 配置服务（使用 DependencyGraph）
```

### DependencyGraph 类

```python
class DependencyGraph:
    """专业级依赖图管理

    提供：
    - 节点和边管理
    - 循环依赖检测
    - 拓扑排序（Kahn 算法）
    - 层次化执行计划
    - 关键路径分析
    """
```

### 依赖类型枚举

```python
class DependencyType(Enum):
    DATA = auto()       # 数据依赖：通过输入/输出数据集推导
    EXPLICIT = auto()   # 显式依赖：通过 depends_on 声明
    RESOURCE = auto()   # 资源依赖：共享资源约束
    TEMPORAL = auto()   # 时序依赖：时间窗口约束
```

### 依赖源策略（Strategy Pattern）

```python
class DependencySource(ABC):
    """依赖源抽象基类"""

    @abstractmethod
    def extract_dependencies(self, node_name, node_config, all_nodes):
        """从节点配置提取依赖边"""
        pass

class DataDependencySource(DependencySource):
    """数据依赖：从 inputs/outputs 推导"""

class ExplicitDependencySource(DependencySource):
    """显式依赖：从 depends_on 字段解析"""
```

### 执行计划

```python
@dataclass
class ExecutionPlan:
    layers: List[ExecutionLayer]    # 层次化节点分组
    total_nodes: int
    critical_path: List[str]        # 最长执行路径

    @property
    def max_parallelism(self) -> int:
        """最大并行度"""

    @property
    def depth(self) -> int:
        """执行深度（层数）"""
```

## 数据流

```
YAML Config
    │
    ▼
┌─────────────────┐
│  ConfigService  │
│  _parse_steps() │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────┐
│  DependencyGraph.from_node_configs()  │
│                                       │
│  ┌───────────────────────────┐       │
│  │ DataDependencySource      │       │
│  │ ExplicitDependencySource  │       │
│  └───────────────────────────┘       │
└────────────────┬────────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│  ExecutionPlan                  │
│  - layers: [Layer0, Layer1, ...]│
│  - critical_path: [A, B, C]     │
│  - max_parallelism: N           │
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│  PrefectEngine                  │
│  _build_node_level_flow()       │
│  - 使用统一的 DependencyGraph   │
│  - 层次化并行执行               │
└─────────────────────────────────┘
```

## YAML 配置示例

```yaml
pipeline:
  steps:
    - name: "Load_Data"
      # ...

    - name: "Analyze_ROIC"
      parameters:
        data: "steps.Load_Data.outputs.parameters.Raw_Data"  # 数据依赖

    - name: "Generate_Report"
      depends_on:           # 显式依赖
        - "Store_ROIC"
        - "Store_Revenue"
```

## 与之前实现的对比

| 方面 | 之前（临时 fix） | 现在（专业实现） |
|------|------------------|------------------|
| 依赖管理 | 分散在多处 | 统一 DependencyGraph |
| 扩展性 | 硬编码 | DependencySource 策略模式 |
| 测试覆盖 | 无 | 16 个单元测试 |
| 循环检测 | 基础检测 | 完整检测 + 路径报告 |
| 执行计划 | 无 | ExecutionPlan + 关键路径 |
| 类型安全 | 弱 | 强类型 + 数据类 |

## 扩展点

1. **新增依赖类型**
   ```python
   class ResourceDependencySource(DependencySource):
       """从共享资源（如 DB 连接池）推导依赖"""
   ```

2. **自定义执行策略**
   ```python
   class PriorityExecutionPlan(ExecutionPlan):
       """基于优先级的执行计划"""
   ```

3. **依赖可视化**
   ```python
   graph.to_mermaid()  # 生成 Mermaid 图
   graph.to_graphviz()  # 生成 DOT 格式
   ```
