"""
PDDA Decorators - 装饰器系统
==============================

提供声明式的聚合能力标注。

核心装饰器：
1. @aggregatable: 标记生产者方法
2. @consumer: 标记消费者方法
3. @before_collect/@after_collect: 生命周期钩子

设计原则：
- 可选使用：装饰器是增强，不是必需
- 零侵入：不改变函数行为
- 类型安全：配合类型标注工作
"""

from __future__ import annotations
import logging
from typing import Callable, List, Type, Optional, TypeVar
from functools import wraps

from .protocols import Aggregatable, AggregatableResult, AggregationMetadata

__all__ = [
    'aggregatable',
    'consumer',
    'before_collect',
    'after_collect',
]

logger = logging.getLogger(__name__)

T = TypeVar('T')


def aggregatable(
    key: str,
    value: str = "self",
    auto_collect: bool = True,
    cache: bool = False,
    cache_ttl: int = 3600,
    priority: int = 0
):
    """
    标记方法为可聚合生产者

    使用此装饰器可以让普通函数返回的数据自动包装为 Aggregatable。

    Args:
        key: 键字段名（从返回值中提取）
        value: 值字段名（"self" 表示整个返回值）
        auto_collect: 是否自动收集
        cache: 是否启用缓存
        cache_ttl: 缓存过期时间（秒）
        priority: 收集优先级

    使用示例：
        @register_method(...)
        @aggregatable(key="metric_name", value="result_df")
        def analyze_trend(...) -> pd.DataFrame:
            df = pd.DataFrame(...)
            df['metric_name'] = "roic"
            return df

        # 返回值自动包装为 AggregatableResult

    注意：
        - 如果函数已返回 Aggregatable 类型，则不会重复包装
        - 键字段必须存在于返回值中
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)

            # 如果已经是 Aggregatable，直接返回
            if isinstance(result, Aggregatable):
                return result

            # 自动包装为 AggregatableResult
            try:
                wrapped = _wrap_as_aggregatable(
                    result,
                    key_field=key,
                    value_field=value,
                    producer_method=func.__name__,
                    auto_collect=auto_collect,
                    cache_enabled=cache,
                    cache_ttl=cache_ttl,
                    priority=priority
                )
                return wrapped

            except Exception as e:
                logger.warning(f"包装失败，返回原始结果: {e}")
                return result

        # 标记函数为生产者
        wrapper._is_aggregatable_producer = True
        wrapper._aggregatable_config = {
            'key': key,
            'value': value,
            'auto_collect': auto_collect,
            'cache': cache,
            'cache_ttl': cache_ttl,
            'priority': priority,
        }

        return wrapper
    return decorator


def consumer(
    *param_names: str,
    required_types: Optional[List[Type]] = None,
    min_items: int = 0,
    required_keys: Optional[List[str]] = None,
    validation: Optional[Callable] = None
):
    """
    标记方法为聚合数据消费者

    使用此装饰器可以显式声明方法需要聚合数据。

    Args:
        *param_names: 需要注入聚合数据的参数名
        required_types: 要求的类型列表
        min_items: 最少数据项数
        required_keys: 必需的键列表
        validation: 自定义验证函数

    使用示例：
        @register_method(...)
        @consumer("aggregated_trends", min_items=3, required_keys=["roic", "roe"])
        def report_comprehensive(aggregated_trends: Dict[str, pd.DataFrame]):
            # aggregated_trends 会自动注入
            pass

    注意：
        - 即使不使用此装饰器，PDDA 也会通过约定识别消费者
        - 此装饰器主要用于添加验证和约束
    """
    def decorator(func: Callable) -> Callable:
        # 不修改函数行为，只添加元数据
        func._is_aggregatable_consumer = True
        func._consumer_config = {
            'param_names': param_names,
            'required_types': required_types or [],
            'min_items': min_items,
            'required_keys': required_keys or [],
            'validation': validation,
        }

        return func
    return decorator


def before_collect(func: Callable) -> Callable:
    """
    标记为收集前钩子

    此函数会在每次收集数据前被调用。

    使用示例：
        @before_collect
        def validate_trend_data(item: AggregatableResult):
            # 验证数据质量
            df = item.get_aggregation_value()
            if df.shape[0] < 5:
                raise ValueError("数据量不足")

    钩子签名：
        def hook(item: Aggregatable) -> None:
            pass
    """
    func._is_before_collect_hook = True
    return func


def after_collect(func: Callable) -> Callable:
    """
    标记为收集后钩子

    此函数会在每次收集数据后被调用。

    使用示例：
        @after_collect
        def log_collection(item: AggregatableResult):
            # 记录收集日志
            logger.info(f"已收集: {item.get_aggregation_key()}")

    钩子签名：
        def hook(item: Aggregatable) -> None:
            pass
    """
    func._is_after_collect_hook = True
    return func


# ═══════════════════════════════════════════════════════════
# 辅助函数
# ═══════════════════════════════════════════════════════════

def _wrap_as_aggregatable(
    data: any,
    key_field: str,
    value_field: str,
    producer_method: str,
    **metadata_kwargs
) -> Aggregatable:
    """
    将普通数据包装为 Aggregatable

    支持：
    - DataFrame: 提取指定列作为 key
    - Dict: 使用指定 key
    - 自定义对象: 使用 getattr

    Args:
        data: 原始数据
        key_field: 键字段名
        value_field: 值字段名（"self" 表示整个数据）
        producer_method: 生产者方法名
        **metadata_kwargs: 元数据参数

    Returns:
        AggregatableResult 实例
    """
    # 提取 key
    if hasattr(data, key_field):
        # 对象属性
        key = getattr(data, key_field)
    elif isinstance(data, dict) and key_field in data:
        # 字典 key
        key = data[key_field]
    elif hasattr(data, '__getitem__'):
        # 可索引对象（DataFrame, Series等）
        try:
            key = data[key_field]
            # 如果是 Series/列，取第一个值
            if hasattr(key, 'iloc'):
                key = key.iloc[0]
        except (KeyError, IndexError):
            raise ValueError(f"无法从数据中提取键: {key_field}")
    else:
        raise ValueError(f"不支持的数据类型: {type(data)}")

    # 提取 value
    if value_field == "self":
        value = data
    elif hasattr(data, value_field):
        value = getattr(data, value_field)
    elif isinstance(data, dict) and value_field in data:
        value = data[value_field]
    elif hasattr(data, '__getitem__'):
        try:
            value = data[value_field]
        except (KeyError, IndexError):
            value = data  # 提取失败，使用整个数据
    else:
        value = data

    # 创建元数据
    metadata = AggregationMetadata(
        producer_method=producer_method,
        **metadata_kwargs
    )

    # 创建 AggregatableResult
    return AggregatableResult(
        key=key,
        value=value,
        metadata=metadata
    )
