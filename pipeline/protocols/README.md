# Pipeline Protocols - 企业级通用协议层

> "协议是系统的契约，而非实现的枷锁"

## 🎯 设计目标

Pipeline Protocols 是一个**与实现无关**的协议层，它定义了工作流系统中所有组件的标准接口。

### 核心理念

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         协议（接口）                                      │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐   │
│  │ Executable   │ │ Resolvable   │ │ Task         │ │ Resource     │   │
│  │ Protocol     │ │ Protocol     │ │ Protocol     │ │ Protocol     │   │
│  └──────┬───────┘ └──────┬───────┘ └──────┬───────┘ └──────┬───────┘   │
│         │                │                │                │           │
└─────────┼────────────────┼────────────────┼────────────────┼───────────┘
          │                │                │                │
          ▼                ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         适配器（实现）                                    │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐   │
│  │ Registry     │ │ K8s Service  │ │ gRPC Service │ │ AWS Lambda   │   │
│  │ Adapter      │ │ Adapter      │ │ Adapter      │ │ Adapter      │   │
│  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

**Pipeline 永远只依赖协议，不依赖任何具体实现。**

## 📚 协议分层

### Layer 1: Core Protocols (核心协议)

最基础的抽象，所有组件都可以组合使用。

| 协议 | 职责 | 参考 |
|------|------|------|
| `ExecutableProtocol` | 定义可执行单元 | Dagster Op, Airflow Operator |
| `ResolvableProtocol` | 定义可解析对象 | Dagster Config, Luigi Parameter |
| `ConfigurableProtocol` | 定义可配置对象 | Dagster Resources, Prefect Block |
| `SerializableProtocol` | 定义可序列化对象 | Airflow XCom, Prefect Result |

```python
from pipeline.protocols.core import (
    ExecutableProtocol, ExecutionResult, ExecutionStatus,
    ResolvableProtocol, ResolveResult,
    ConfigurableProtocol, ConfigSchema,
    SerializableProtocol,
)

# 示例：自定义可执行单元
class MyAnalyzer(ExecutableProtocol[Dict, DataFrame]):
    def execute(self, input: Dict, context=None) -> ExecutionResult[DataFrame]:
        result = analyze(input)
        return ExecutionResult(status=ExecutionStatus.SUCCESS, value=result)
```

### Layer 2: Domain Protocols (领域协议)

面向工作流领域的高级抽象。

| 协议 | 职责 | 参考 |
|------|------|------|
| `TaskProtocol` | 完整任务定义 | Luigi Task, Dagster Op |
| `IOProtocol` | 输入输出管理 | Dagster IOManager |
| `ResourceProtocol` | 外部资源抽象 | Dagster Resources, Airflow Hooks |
| `ContextProtocol` | 执行上下文 | Dagster OpContext, Airflow Context |

```python
from pipeline.protocols.domain import (
    TaskProtocol, TaskInfo, TaskCapabilities,
    IOProtocol, InputSpec, OutputSpec, TargetProtocol,
    ResourceProtocol, ResourceSpec, ResourceLifecycle,
    ContextProtocol, ExecutionContext,
)

# 示例：自定义任务
class MetricAnalysisTask(TaskProtocol[DataFrame, Dict]):
    @property
    def info(self) -> TaskInfo:
        return TaskInfo(
            name="analyze_metric",
            category="business_engine",
            capabilities={TaskCapabilities.CACHEABLE, TaskCapabilities.RETRYABLE}
        )

    def requires(self) -> List[str]:
        return ["load_financial_data"]

    def run(self, input: DataFrame, context=None) -> ExecutionResult[Dict]:
        result = self._analyze(input)
        return ExecutionResult(status=ExecutionStatus.SUCCESS, value=result)
```

### Layer 3: Integration Protocols (集成协议)

连接外部系统的适配器协议。

| 协议 | 职责 | 已有实现 |
|------|------|---------|
| `MethodResolverProtocol` | 方法解析 | `RegistryMethodResolver` |
| `StorageBackendProtocol` | 存储后端 | `FileCacheBackend`, `S3Backend`* |
| `NotificationChannelProtocol` | 通知渠道 | `SlackChannel`*, `EmailChannel`* |
| `MetricCollectorProtocol` | 指标收集 | `PrometheusCollector`* |

(*待实现)

```python
from pipeline.protocols.integration import (
    MethodResolverProtocol, MethodInfo, MethodSelectorProtocol,
    StorageBackendProtocol, StorageResult,
    NotificationChannelProtocol, NotificationPayload,
    MetricCollectorProtocol, MetricValue,
)

# 示例：自定义方法解析器
class K8sMethodResolver(MethodResolverProtocol):
    def resolve(self, component, engine, method) -> Optional[MethodInfo]:
        # 从 Kubernetes Service 发现方法
        service = self._discover_service(component, engine)
        if service:
            return MethodInfo(
                name=method,
                component=component,
                engine=engine,
                callable=self._create_grpc_stub(service, method)
            )
        return None
```

## 🏭 行业最佳实践参考

### Apache Airflow

- **TaskSDK**: 任务定义和装饰器
- **Executor**: 可插拔执行器
- **XCom**: 任务间数据传递
- **Hooks/Connections**: 外部系统连接

### Dagster

- **Op**: 基本执行单元
- **IOManager**: 输入输出管理
- **Resources**: 外部资源抽象
- **Config**: 配置模式验证

### Prefect

- **Task/Flow**: 任务和流程定义
- **TaskRunner**: 可插拔任务运行器
- **Result**: 结果持久化
- **Block**: 可配置组件

### Luigi

- **Task**: 任务定义 (requires/output/run)
- **Target**: 输出目标抽象
- **Parameter**: 参数定义
- **Events**: 事件回调

## 📝 使用指南

### 1. 实现自定义任务

```python
from pipeline.protocols import TaskProtocol, TaskInfo, TaskCapabilities
from pipeline.protocols import ExecutionResult, ExecutionStatus

class MyTask(TaskProtocol[Dict, List]):
    @property
    def info(self) -> TaskInfo:
        return TaskInfo(
            name="my_task",
            category="custom",
            capabilities={TaskCapabilities.CACHEABLE},
        )

    def inputs(self):
        return [InputSpec(name="data", type=Dict)]

    def outputs(self):
        return [OutputSpec(name="result", type=List)]

    def requires(self):
        return ["upstream_task"]

    def run(self, input, context=None):
        result = process(input)
        return ExecutionResult(status=ExecutionStatus.SUCCESS, value=result)
```

### 2. 实现自定义资源

```python
from pipeline.protocols import ResourceProtocol, ResourceSpec

class DatabaseResource(ResourceProtocol[Connection]):
    @property
    def spec(self) -> ResourceSpec:
        return ResourceSpec(
            name="database",
            type=Connection,
            lifecycle=ResourceLifecycle.FLOW,
        )

    def setup(self, config=None):
        return create_connection(config or self.spec.config)

    def teardown(self, resource):
        resource.close()

    def health_check(self, resource):
        return resource.is_connected()
```

### 3. 实现自定义存储后端

```python
from pipeline.protocols import StorageBackendProtocol, StorageResult, StorageOperation

class S3StorageBackend(StorageBackendProtocol[bytes]):
    def __init__(self, bucket: str):
        self.bucket = bucket
        self.s3 = boto3.client('s3')

    def read(self, key: str) -> StorageResult[bytes]:
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            return StorageResult.ok(StorageOperation.READ, obj['Body'].read())
        except Exception as e:
            return StorageResult.fail(StorageOperation.READ, str(e))

    def write(self, key: str, data: bytes, **options) -> StorageResult[bytes]:
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=data)
        return StorageResult.ok(StorageOperation.WRITE)
```

## 🔧 扩展点

协议层设计了以下扩展点：

1. **自定义方法解析器**: 实现 `MethodResolverProtocol` 连接任意服务注册中心
2. **自定义存储后端**: 实现 `StorageBackendProtocol` 连接任意存储系统
3. **自定义通知渠道**: 实现 `NotificationChannelProtocol` 连接任意通知系统
4. **自定义指标收集**: 实现 `MetricCollectorProtocol` 连接任意监控系统
5. **自定义任务类型**: 实现 `TaskProtocol` 创建新的任务类型
6. **自定义资源类型**: 实现 `ResourceProtocol` 管理新的外部资源

## 📄 版本

- **协议版本**: 2.0.0
- **兼容 Pipeline 版本**: 2.0.0+
