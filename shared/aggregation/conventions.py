"""
PDDA Convention Layer - 约定系统
================================

定义 PDDA 系统的内置约定，实现"约定优于配置"。

核心理念：
- 零配置：通过约定自动识别聚合模式
- 智能推断：从命名、类型、协议推断行为
- 可扩展：约定可以被覆盖和扩展

约定类型：
1. NamingConvention: 命名约定（参数名模式）
2. TypeConvention: 类型约定（类型标注模式）
3. ProtocolConvention: 协议约定（协议实现检测）
"""

from __future__ import annotations
import re
import inspect
from typing import (
    Any, Callable, get_origin, get_args, get_type_hints, Union,
    Dict, List, Mapping, MutableMapping, Sequence, MutableSequence
)
from .protocols import Aggregatable

__all__ = [
    'NamingConvention',
    'TypeConvention',
    'ProtocolConvention',
]


class NamingConvention:
    """
    命名约定：通过参数名模式识别聚合需求

    核心思想：
        如果参数名符合特定模式，则认为它需要聚合数据。
        这是最直观的约定，无需额外标注。

    内置模式：
        - aggregated_*: aggregated_trends, aggregated_data
        - collected_*: collected_results, collected_metrics
        - *_frames: probe_frames, result_frames, data_frames
        - *_results: trend_results, analysis_results
        - *_data_map: metric_data_map, indicator_data_map
        - *_collection: metric_collection, stock_collection

    使用示例：
        def report(aggregated_trends: Dict[str, pd.DataFrame]):
            # ✅ 参数名 "aggregated_trends" 符合约定，自动注入
            pass

        def analyze(probe_frames: Dict[str, pd.DataFrame]):
            # ✅ 参数名 "probe_frames" 符合约定，自动注入
            pass

    扩展：
        # 添加自定义模式
        NamingConvention.register_pattern(r"^my_aggregated_.*")
    """

    # 内置参数名模式（正则表达式）
    _BUILTIN_PATTERNS = [
        r"^aggregated_.*",      # aggregated_trends, aggregated_data
        r"^collected_.*",       # collected_results, collected_metrics
        r".*_frames$",          # probe_frames, result_frames
        r".*_results$",         # trend_results, analysis_results
        r".*_data_map$",        # metric_data_map
        r".*_collection$",      # metric_collection
        r".*_aggregate$",       # data_aggregate
    ]

    # 用户自定义模式
    _custom_patterns: List[str] = []

    @classmethod
    def is_aggregation_parameter(cls, param_name: str) -> bool:
        """
        判断参数名是否符合聚合约定

        Args:
            param_name: 参数名

        Returns:
            是否符合约定
        """
        # 检查内置模式
        for pattern in cls._BUILTIN_PATTERNS:
            if re.match(pattern, param_name, re.IGNORECASE):
                return True

        # 检查自定义模式
        for pattern in cls._custom_patterns:
            if re.match(pattern, param_name, re.IGNORECASE):
                return True

        return False

    @classmethod
    def register_pattern(cls, pattern: str):
        """
        注册自定义参数名模式

        Args:
            pattern: 正则表达式模式

        示例：
            NamingConvention.register_pattern(r"^my_custom_.*")
        """
        if pattern not in cls._custom_patterns:
            cls._custom_patterns.append(pattern)

    @classmethod
    def clear_custom_patterns(cls):
        """清除所有自定义模式"""
        cls._custom_patterns.clear()

    @classmethod
    def get_all_patterns(cls) -> List[str]:
        """获取所有模式（内置 + 自定义）"""
        return cls._BUILTIN_PATTERNS + cls._custom_patterns


class TypeConvention:
    """
    类型约定：通过类型标注识别聚合需求

    核心思想：
        如果参数类型是聚合容器（Dict, List等），则认为它需要聚合数据。

    支持的类型：
        - Dict[K, V]: 字典类型（最常用）
        - List[T]: 列表类型
        - Mapping/MutableMapping: 映射抽象类型
        - Sequence/MutableSequence: 序列抽象类型

    使用示例：
        def report(trends: Dict[str, pd.DataFrame]):
            # ✅ 类型是 Dict，自动识别为聚合需求
            pass

        def analyze(data: List[AggregatableResult]):
            # ✅ 类型是 List，自动识别为聚合需求
            pass

    扩展：
        # 添加自定义类型
        TypeConvention.register_aggregation_type(MyCustomType)
    """

    # 内置聚合类型
    _BUILTIN_TYPES = [
        dict, Dict,
        list, List,
        Mapping, MutableMapping,
        Sequence, MutableSequence,
    ]

    # 用户自定义类型
    _custom_types: List[type] = []

    @classmethod
    def is_aggregation_type(cls, type_hint: Any) -> bool:
        """
        判断类型是否为聚合类型

        Args:
            type_hint: 类型标注

        Returns:
            是否为聚合类型
        """
        if type_hint is None:
            return False

        # 获取泛型的原始类型
        origin = get_origin(type_hint)

        # 检查内置类型
        if origin in cls._BUILTIN_TYPES:
            return True

        # 如果没有泛型，直接检查类型本身
        if origin is None:
            if type_hint in cls._BUILTIN_TYPES:
                return True

        # 检查自定义类型
        if origin in cls._custom_types or type_hint in cls._custom_types:
            return True

        return False

    @classmethod
    def register_aggregation_type(cls, custom_type: type):
        """
        注册自定义聚合类型

        Args:
            custom_type: 自定义类型

        示例：
            TypeConvention.register_aggregation_type(MyCustomContainer)
        """
        if custom_type not in cls._custom_types:
            cls._custom_types.append(custom_type)

    @classmethod
    def clear_custom_types(cls):
        """清除所有自定义类型"""
        cls._custom_types.clear()

    @classmethod
    def extract_value_type(cls, type_hint: Any) -> Any:
        """
        提取聚合类型的值类型

        示例：
            Dict[str, DataFrame] → DataFrame
            List[AggregatableResult] → AggregatableResult

        Args:
            type_hint: 类型标注

        Returns:
            值类型，如果无法提取则返回 None
        """
        args = get_args(type_hint)
        if not args:
            return None

        origin = get_origin(type_hint)

        # Dict[K, V] → V (第二个参数)
        if origin in (dict, Dict, Mapping, MutableMapping):
            return args[1] if len(args) >= 2 else None

        # List[T] → T (第一个参数)
        if origin in (list, List, Sequence, MutableSequence):
            return args[0] if len(args) >= 1 else None

        return None


class ProtocolConvention:
    """
    协议约定：通过协议实现识别聚合能力

    核心思想：
        - 如果函数返回值实现了 Aggregatable 协议，则是生产者
        - 如果函数参数需要聚合数据，则是消费者

    检测方式：
        1. 类型标注检查：isinstance(type_hint, Aggregatable)
        2. 运行时检查：isinstance(result, Aggregatable)
        3. 装饰器标记：hasattr(func, '_is_aggregatable_producer')

    使用示例：
        def analyze(...) -> AggregatableResult[str, pd.DataFrame]:
            # ✅ 返回类型实现 Aggregatable，自动识别为生产者
            return AggregatableResult(key="roic", value=df)
    """

    @classmethod
    def is_producer(cls, func: Callable) -> bool:
        """
        判断函数是否为生产者

        检测逻辑：
        1. 检查装饰器标记
        2. 检查返回类型是否实现 Aggregatable

        Args:
            func: 函数对象

        Returns:
            是否为生产者
        """
        # 1. 检查装饰器标记
        if hasattr(func, '_is_aggregatable_producer'):
            return getattr(func, '_is_aggregatable_producer')

        # 2. 检查返回类型
        try:
            hints = get_type_hints(func, include_extras=True)
            return_type = hints.get('return')

            if return_type is None:
                return False

            # 检查是否实现 Aggregatable
            return cls._implements_aggregatable(return_type)

        except Exception:
            # 类型提示解析失败，保守处理
            return False

    @classmethod
    def is_consumer(cls, func: Callable) -> bool:
        """
        判断函数是否为消费者

        检测逻辑：
        1. 检查装饰器标记
        2. 检查参数名是否符合约定
        3. 检查参数类型是否为聚合类型

        Args:
            func: 函数对象

        Returns:
            是否为消费者
        """
        # 1. 检查装饰器标记
        if hasattr(func, '_is_aggregatable_consumer'):
            return getattr(func, '_is_aggregatable_consumer')

        # 2. 检查参数
        try:
            sig = inspect.signature(func)
            hints = get_type_hints(func)

            for param_name, param in sig.parameters.items():
                # 跳过特殊参数
                if param_name in ('self', 'cls', 'data', 'args', 'kwargs'):
                    continue

                # 检查命名约定
                if NamingConvention.is_aggregation_parameter(param_name):
                    return True

                # 检查类型约定
                param_type = hints.get(param_name)
                if TypeConvention.is_aggregation_type(param_type):
                    return True

            return False

        except Exception:
            return False

    @classmethod
    def _implements_aggregatable(cls, type_hint: Any) -> bool:
        """
        检查类型是否实现 Aggregatable 协议

        Args:
            type_hint: 类型标注

        Returns:
            是否实现协议
        """
        try:
            # 处理 Union 类型：检查是否有任何一个类型实现 Aggregatable
            origin = get_origin(type_hint)
            if origin is Union:
                # Union[A, B, ...] - 检查每个类型
                args = get_args(type_hint)
                return any(cls._implements_aggregatable(arg) for arg in args)

            # 获取泛型的原始类型
            check_type = origin if origin is not None else type_hint

            # 检查是否是 Aggregatable 或其子类
            if isinstance(check_type, type):
                # 运行时协议检查
                return issubclass(check_type, Aggregatable)

            # 检查类型名称（作为后备方案）
            type_name = getattr(check_type, '__name__', str(check_type))
            if 'Aggregatable' in type_name:
                return True

            return False

        except (TypeError, AttributeError):
            return False

    @classmethod
    def extract_aggregatable_config(cls, func: Callable) -> Dict[str, Any]:
        """
        提取函数的聚合配置

        Args:
            func: 函数对象

        Returns:
            配置字典
        """
        config = {}

        # 从装饰器提取配置
        if hasattr(func, '_aggregatable_config'):
            config.update(getattr(func, '_aggregatable_config'))

        if hasattr(func, '_consumer_config'):
            config.update(getattr(func, '_consumer_config'))

        return config
