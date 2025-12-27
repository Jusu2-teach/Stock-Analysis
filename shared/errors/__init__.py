"""
AStock 统一错误系统 (Unified Error System)
==========================================

集百家之长的错误处理模块：
- Django: 分层异常体系 + 错误上下文
- FastAPI: 结构化异常信息 + HTTP 状态码映射
- requests: 简洁的异常链
- Rich: 美化错误输出
- Sentry: 错误追踪与聚合

核心特性:
1. 错误码体系 - 按模块分类的唯一错误标识
2. 上下文携带 - 附加调试所需的任意数据
3. 错误链 - 保留原始异常信息
4. EventBus 集成 - 自动发布错误事件
5. 可序列化 - 支持 JSON 序列化

Usage:
    from shared.errors import (
        AStockError, ErrorCode, ValidationError,
        error_handler, safe_execute
    )

    # 抛出带上下文的错误
    raise ValidationError("Invalid data").with_context(row=10, column="price")

    # 使用装饰器处理错误
    @error_handler(DataError, fallback=pd.DataFrame())
    def load_data():
        ...
"""
__version__ = "1.0.0"

# 错误码
from .codes import (
    ErrorCode,
    ErrorSeverity,
    ErrorCodeInfo,
    get_error_by_code,
    list_errors_by_category,
    list_errors_by_severity,
)

# 基类
from .base import (
    AStockError,
    ErrorContext,
    ErrorType,
    format_error,
)

# Registry 错误
from .registry import (
    RegistryError,
    MethodNotFoundError,
    DuplicateMethodError,
    RegistryVersionConflictError,
    RegistryInitializationError,
    SignatureValidationError,
    MethodExecutionError,
)

# Pipeline 错误
from .pipeline import (
    PipelineError,
    StepExecutionError,
    WorkflowDefinitionError,
    DependencyResolutionError,
    ParameterResolutionError,
    PipelineTimeoutError,
    StateCheckpointError,
    OutputCollectionError,
)

# 验证错误
from .validation import (
    ValidationError,
    SchemaValidationError,
    TypeValidationError,
    RequiredFieldError,
    RangeValidationError,
    FormatValidationError,
    SignatureMismatchError,
    ContractViolationError,
)

# 数据错误
from .data import (
    DataError,
    FileNotFoundError,
    FileFormatError,
    DataLoadError,
    DataTransformError,
    ColumnNotFoundError,
    EmptyDataError,
    DataIntegrityError,
    DatabaseError,
    SerializationError,
)

# 配置错误
from .config import (
    ConfigError,
    ConfigNotFoundError,
    ConfigParseError,
    ConfigValidationError,
    ConfigKeyError,
    ConfigTypeError,
    EnvironmentVariableError,
    WorkflowConfigError,
)

# 处理器
from .handlers import (
    ErrorHandler,
    ErrorHandlerResult,
    ErrorHandlerChain,
    LoggingErrorHandler,
    RetryHandler,
    FallbackHandler,
    error_handler,
    safe_execute,
)

__all__ = [
    # 错误码
    'ErrorCode',
    'ErrorSeverity',
    'ErrorCodeInfo',
    'get_error_by_code',
    'list_errors_by_category',
    'list_errors_by_severity',

    # 基类
    'AStockError',
    'ErrorContext',
    'ErrorType',
    'format_error',

    # Registry 错误
    'RegistryError',
    'MethodNotFoundError',
    'DuplicateMethodError',
    'RegistryVersionConflictError',
    'RegistryInitializationError',
    'SignatureValidationError',
    'MethodExecutionError',

    # Pipeline 错误
    'PipelineError',
    'StepExecutionError',
    'WorkflowDefinitionError',
    'DependencyResolutionError',
    'ParameterResolutionError',
    'PipelineTimeoutError',
    'StateCheckpointError',
    'OutputCollectionError',

    # 验证错误
    'ValidationError',
    'SchemaValidationError',
    'TypeValidationError',
    'RequiredFieldError',
    'RangeValidationError',
    'FormatValidationError',
    'SignatureMismatchError',
    'ContractViolationError',

    # 数据错误
    'DataError',
    'FileNotFoundError',
    'FileFormatError',
    'DataLoadError',
    'DataTransformError',
    'ColumnNotFoundError',
    'EmptyDataError',
    'DataIntegrityError',
    'DatabaseError',
    'SerializationError',

    # 配置错误
    'ConfigError',
    'ConfigNotFoundError',
    'ConfigParseError',
    'ConfigValidationError',
    'ConfigKeyError',
    'ConfigTypeError',
    'EnvironmentVariableError',
    'WorkflowConfigError',

    # 处理器
    'ErrorHandler',
    'ErrorHandlerResult',
    'ErrorHandlerChain',
    'LoggingErrorHandler',
    'RetryHandler',
    'FallbackHandler',
    'error_handler',
    'safe_execute',
]