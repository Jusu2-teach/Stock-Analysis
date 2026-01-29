"""
Aggregation Inject - 智能依赖注入
==================================

基于类型注解和参数命名约定的自动依赖注入系统。

设计原则：
1. 类型注解驱动：从类型推断命名空间和验证规则
2. 约定优于配置：aggregated_* 前缀自动识别
3. DAG 感知：基于执行顺序确定可用数据
4. 友好错误：缺失数据时提供详细上下文
"""

from __future__ import annotations

import functools
import inspect
import logging
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

from .core import AggregationScope, AggregationError

__all__ = [
    # 主要 API
    "Injector",
    "inject",
    "injectable",
    # 类型别名
    "Aggregated",
    # 配置
    "InjectionSpec",
    # 异常
    "InjectionError",
    "MissingDependencyError",
]

logger = logging.getLogger(__name__)

T = TypeVar("T")


# =============================================================================
# Exceptions
# =============================================================================

class InjectionError(AggregationError):
    """注入异常基类"""
    pass


class MissingDependencyError(InjectionError):
    """缺失依赖异常"""

    def __init__(
        self,
        param_name: str,
        namespace: str,
        available_namespaces: List[str],
        available_keys: List[str] = None,
        consumer: str = None,
    ):
        self.param_name = param_name
        self.namespace = namespace
        self.available_namespaces = available_namespaces
        self.available_keys = available_keys
        self.consumer = consumer

        # 构建友好的错误信息
        msg_parts = [
            f"Cannot inject '{param_name}': namespace '{namespace}' not found."
        ]

        if available_namespaces:
            msg_parts.append(f"Available namespaces: {', '.join(available_namespaces)}")
        else:
            msg_parts.append("No data available in scope.")

        if consumer:
            msg_parts.append(f"Consumer: {consumer}")

        # 提示
        msg_parts.append(
            f"\nHint: Ensure a producer step outputs to namespace '{namespace}' "
            f"before this consumer runs."
        )

        super().__init__("\n".join(msg_parts))


# =============================================================================
# Type Alias for Injection
# =============================================================================

class Aggregated(Generic[T]):
    """类型别名：标记需要注入的聚合数据

    用于类型注解，表示该参数应从 AggregationScope 注入。

    Examples:
        def run_evaluator(
            trends: Aggregated[Dict[str, DataFrame]],  # 注入 "trends" namespace
        ) -> EvalResult:
            ...

    命名空间推断规则：
    1. 显式配置 (通过 @injectable 装饰器)
    2. 参数名匹配: aggregated_trends → "trends"
    3. 参数名直接匹配: trends → "trends"
    """
    pass


# =============================================================================
# Injection Specification
# =============================================================================

@dataclass
class InjectionSpec:
    """注入规格 - 描述如何注入一个参数

    Attributes:
        param_name: 参数名称
        namespace: 目标命名空间
        key: 特定键 (None 表示注入整个命名空间)
        required: 是否必需
        default: 默认值
        transform: 值转换函数
    """
    param_name: str
    namespace: str
    key: Optional[str] = None
    required: bool = True
    default: Any = None
    transform: Optional[Callable[[Any], Any]] = None

    def is_namespace_injection(self) -> bool:
        """是否注入整个命名空间"""
        return self.key is None


# =============================================================================
# Namespace Inference Engine
# =============================================================================

class NamespaceInferrer:
    """命名空间推断器

    从参数名和类型注解推断目标命名空间。
    """

    # 参数名前缀映射
    PREFIX_MAPPINGS: Dict[str, str] = {
        "aggregated_": "",      # aggregated_trends → trends
        "collected_": "",       # collected_metrics → metrics
        "data_": "",            # data_evaluations → evaluations
        "all_": "",             # all_results → results
    }

    # 直接映射 (参数名 → namespace)
    DIRECT_MAPPINGS: Dict[str, str] = {
        "trends": "trends",
        "evaluations": "evaluations",
        "truth": "truth",
        "reports": "reports",
        "metrics": "metrics",
    }

    @classmethod
    def infer(cls, param_name: str, type_hint: Any = None) -> Optional[str]:
        """推断命名空间

        Args:
            param_name: 参数名
            type_hint: 类型注解 (可选)

        Returns:
            推断的命名空间，或 None (无法推断)

        推断优先级:
        1. 前缀匹配: aggregated_trends → "trends"
        2. 直接匹配: trends → "trends"
        3. 类型注解: Aggregated[TrendResult] → 从类型注册表查找
        """
        # 1. 前缀匹配
        for prefix, replacement in cls.PREFIX_MAPPINGS.items():
            if param_name.startswith(prefix):
                return param_name[len(prefix):] + replacement

        # 2. 直接匹配
        if param_name in cls.DIRECT_MAPPINGS:
            return cls.DIRECT_MAPPINGS[param_name]

        # 3. 类型注解推断
        if type_hint is not None:
            origin = get_origin(type_hint)
            if origin is Aggregated or (
                hasattr(type_hint, "__origin__") and
                getattr(type_hint.__origin__, "__name__", "") == "Aggregated"
            ):
                # Aggregated[T] → 从 T 推断
                args = get_args(type_hint)
                if args:
                    inner_type = args[0]
                    # 尝试从类型名推断
                    type_name = getattr(inner_type, "__name__", str(inner_type))
                    # TrendResult → trends, EvalResult → evaluations
                    return cls._type_name_to_namespace(type_name)

        return None

    @classmethod
    def _type_name_to_namespace(cls, type_name: str) -> Optional[str]:
        """从类型名推断命名空间"""
        # 移除常见后缀
        for suffix in ("Result", "Data", "Output", "Dict"):
            if type_name.endswith(suffix):
                type_name = type_name[:-len(suffix)]

        # 转换为小写
        namespace = type_name.lower()

        # 处理复数形式
        if not namespace.endswith("s"):
            namespace += "s"

        return namespace

    @classmethod
    def is_injectable_param(cls, param_name: str, type_hint: Any = None) -> bool:
        """判断参数是否需要注入"""
        # 前缀检查
        for prefix in cls.PREFIX_MAPPINGS:
            if param_name.startswith(prefix):
                return True

        # 直接映射检查
        if param_name in cls.DIRECT_MAPPINGS:
            return True

        # 类型检查
        if type_hint is not None:
            origin = get_origin(type_hint)
            if origin is Aggregated:
                return True

        return False


# =============================================================================
# Injector
# =============================================================================

class Injector:
    """依赖注入器 - 自动注入聚合数据

    核心功能:
    1. 分析函数签名，识别需要注入的参数
    2. 从 AggregationScope 获取数据
    3. 调用函数并返回结果

    Examples:
        injector = Injector(scope)

        # 自动注入
        @injector.inject
        def run_evaluator(aggregated_trends: Dict[str, DataFrame]) -> Dict:
            for metric, df in aggregated_trends.items():
                ...

        # 手动注入
        injected = injector.prepare_injection(func)
        result = func(**injected)
    """

    def __init__(
        self,
        scope: AggregationScope,
        strict: bool = True,
    ):
        """初始化注入器

        Args:
            scope: 聚合作用域
            strict: 严格模式 (缺失必需依赖时抛出异常)
        """
        self._scope = scope
        self._strict = strict
        self._specs_cache: Dict[Callable, List[InjectionSpec]] = {}

    @property
    def scope(self) -> AggregationScope:
        return self._scope

    # -------------------------------------------------------------------------
    # Core API
    # -------------------------------------------------------------------------

    def analyze(self, func: Callable) -> List[InjectionSpec]:
        """分析函数签名，返回注入规格列表

        Args:
            func: 目标函数

        Returns:
            需要注入的参数规格列表
        """
        # 缓存检查
        if func in self._specs_cache:
            return self._specs_cache[func]

        specs = []
        sig = inspect.signature(func)

        # 获取类型注解
        try:
            hints = get_type_hints(func)
        except Exception:
            hints = {}

        for param_name, param in sig.parameters.items():
            type_hint = hints.get(param_name)

            # 检查是否需要注入
            if not NamespaceInferrer.is_injectable_param(param_name, type_hint):
                continue

            # 推断命名空间
            namespace = NamespaceInferrer.infer(param_name, type_hint)
            if namespace is None:
                logger.warning(
                    f"Cannot infer namespace for {param_name}, skipping injection"
                )
                continue

            # 创建规格
            spec = InjectionSpec(
                param_name=param_name,
                namespace=namespace,
                required=(param.default is inspect.Parameter.empty),
                default=param.default if param.default is not inspect.Parameter.empty else None,
            )
            specs.append(spec)

        # 缓存
        self._specs_cache[func] = specs
        return specs

    def prepare_injection(
        self,
        func: Callable,
        extra_kwargs: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """准备注入参数

        Args:
            func: 目标函数
            extra_kwargs: 额外参数 (已提供的参数不会被注入覆盖)

        Returns:
            注入后的参数字典
        """
        extra_kwargs = extra_kwargs or {}
        specs = self.analyze(func)
        injected = {}

        for spec in specs:
            # 跳过已提供的参数
            if spec.param_name in extra_kwargs:
                continue

            # 获取数据
            if spec.is_namespace_injection():
                value = self._scope.get_namespace(spec.namespace)
            else:
                value = self._scope.get(spec.namespace, spec.key)

            # 检查数据可用性
            if not value:  # 空 dict 或 None
                if spec.required and self._strict:
                    raise MissingDependencyError(
                        param_name=spec.param_name,
                        namespace=spec.namespace,
                        available_namespaces=self._scope.namespaces(),
                        consumer=func.__name__,
                    )
                value = spec.default

            # 应用转换
            if value is not None and spec.transform:
                value = spec.transform(value)

            injected[spec.param_name] = value

        return injected

    def call(
        self,
        func: Callable[..., T],
        *args,
        **kwargs,
    ) -> T:
        """调用函数并自动注入依赖

        Args:
            func: 目标函数
            *args: 位置参数
            **kwargs: 关键字参数

        Returns:
            函数返回值
        """
        injected = self.prepare_injection(func, kwargs)
        merged_kwargs = {**injected, **kwargs}
        return func(*args, **merged_kwargs)

    # -------------------------------------------------------------------------
    # Decorator API
    # -------------------------------------------------------------------------

    def inject(self, func: Callable[..., T]) -> Callable[..., T]:
        """装饰器: 自动注入依赖

        Example:
            injector = Injector(scope)

            @injector.inject
            def run_evaluator(aggregated_trends: Dict[str, DataFrame]) -> Dict:
                ...
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return self.call(func, *args, **kwargs)

        # 保存元数据
        wrapper._injection_specs = self.analyze(func)
        wrapper._injector = self

        return wrapper


# =============================================================================
# Module-level Decorator Factory
# =============================================================================

def inject(
    scope: AggregationScope = None,
    strict: bool = True,
) -> Callable:
    """函数装饰器: 自动注入聚合数据

    Args:
        scope: 聚合作用域 (None 时使用当前作用域)
        strict: 严格模式

    Examples:
        @inject()
        def run_evaluator(aggregated_trends: Dict[str, DataFrame]) -> Dict:
            ...

        # 指定作用域
        @inject(scope=my_scope)
        def process(aggregated_metrics):
            ...
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # 获取作用域
            from .core import get_current_scope
            actual_scope = scope or get_current_scope()

            if actual_scope is None:
                raise InjectionError(
                    f"No AggregationScope available for injection in {func.__name__}. "
                    "Use ScopeManager.create() to create a scope."
                )

            injector = Injector(actual_scope, strict=strict)
            return injector.call(func, *args, **kwargs)

        return wrapper

    return decorator


def injectable(
    namespace: str = None,
    key: str = None,
    required: bool = True,
    default: Any = None,
) -> Callable:
    """参数装饰器: 标记参数为可注入

    通常不需要使用此装饰器，因为 Injector 会自动识别参数。
    仅在需要覆盖默认推断行为时使用。

    Args:
        namespace: 目标命名空间 (覆盖推断)
        key: 特定键 (None 表示整个命名空间)
        required: 是否必需
        default: 默认值

    Examples:
        @inject()
        def custom_consumer(
            @injectable(namespace="custom_ns")
            data: Dict[str, Any],
        ):
            ...

    Note:
        Python 不原生支持参数装饰器，这个函数主要用于
        通过 __annotations__ 附加元数据。
    """
    # 这个装饰器主要是为了文档和类型提示
    # 实际的元数据附加需要通过其他方式实现
    def marker(param):
        param._injection_config = {
            "namespace": namespace,
            "key": key,
            "required": required,
            "default": default,
        }
        return param

    return marker


# =============================================================================
# Injection Context (for async support)
# =============================================================================

@dataclass
class InjectionContext:
    """注入上下文 - 支持异步注入

    用于在异步环境中传递注入状态。

    Attributes:
        scope: 聚合作用域
        injector: 注入器实例
        func: 原始目标函数（用于准备注入参数）
        specs: 预分析的注入规格列表
    """
    scope: AggregationScope
    injector: Injector
    func: Callable  # 修复: 保存原始函数引用
    specs: List[InjectionSpec] = field(default_factory=list)

    @classmethod
    def create(cls, scope: AggregationScope, func: Callable) -> "InjectionContext":
        """创建注入上下文"""
        injector = Injector(scope)
        specs = injector.analyze(func)
        return cls(scope=scope, injector=injector, func=func, specs=specs)

    def get_injected_kwargs(self, extra_kwargs: Dict[str, Any] = None) -> Dict[str, Any]:
        """获取注入的参数

        修复: 使用保存的原始函数而非 dummy lambda
        """
        return self.injector.prepare_injection(self.func, extra_kwargs)


# =============================================================================
# Utility Functions
# =============================================================================

def get_injection_specs(func: Callable) -> List[InjectionSpec]:
    """获取函数的注入规格 (不需要 Injector 实例)

    Useful for introspection and documentation.
    """
    specs = []
    sig = inspect.signature(func)

    try:
        hints = get_type_hints(func)
    except Exception:
        hints = {}

    for param_name, param in sig.parameters.items():
        type_hint = hints.get(param_name)

        if NamespaceInferrer.is_injectable_param(param_name, type_hint):
            namespace = NamespaceInferrer.infer(param_name, type_hint)
            if namespace:
                specs.append(InjectionSpec(
                    param_name=param_name,
                    namespace=namespace,
                    required=(param.default is inspect.Parameter.empty),
                ))

    return specs


def describe_injection(func: Callable) -> str:
    """描述函数的注入需求 (用于调试)"""
    specs = get_injection_specs(func)

    if not specs:
        return f"{func.__name__}: No injection required"

    lines = [f"{func.__name__} requires:"]
    for spec in specs:
        required = "required" if spec.required else "optional"
        lines.append(f"  - {spec.param_name} ← namespace '{spec.namespace}' ({required})")

    return "\n".join(lines)
