"""
缓存装饰器 (Cache Decorators)
==============================

参考设计:
- functools.lru_cache: 简洁 API
- cachetools.cached: 参数化装饰器
- joblib.Memory: 计算缓存

提供便捷的函数结果缓存装饰器。
"""
from __future__ import annotations
from functools import wraps
from typing import Any, Callable, Optional, TypeVar
import hashlib
import inspect

from .core import Cache, get_default_cache

F = TypeVar('F', bound=Callable[..., Any])
T = TypeVar('T')


def cached(
    ttl: Optional[float] = None,
    cache: Optional[Cache] = None,
    key_prefix: str = "",
    ignore_args: Optional[list[str]] = None,
    condition: Optional[Callable[..., bool]] = None,
) -> Callable[[F], F]:
    """函数结果缓存装饰器

    Args:
        ttl: 缓存过期时间（秒）
        cache: 缓存实例（默认使用全局缓存）
        key_prefix: 缓存键前缀
        ignore_args: 忽略的参数名（不参与键计算）
        condition: 条件函数，返回 True 时才缓存

    Example:
        @cached(ttl=3600)
        def expensive_computation(x, y):
            ...

        @cached(ttl=60, ignore_args=['debug'])
        def fetch_data(query, debug=False):
            ...

        @cached(condition=lambda result: result is not None)
        def maybe_none(x):
            ...
    """
    ignore_args = ignore_args or []

    def decorator(func: F) -> F:
        func_name = f"{func.__module__}.{func.__qualname__}"

        @wraps(func)
        def wrapper(*args, **kwargs):
            # 获取缓存实例
            _cache = cache or get_default_cache()

            # 构建缓存键
            cache_key = _build_key(func, func_name, args, kwargs, key_prefix, ignore_args)

            # 尝试从缓存获取
            cached_value = _cache.get(cache_key)
            if cached_value is not None:
                return cached_value

            # 执行函数
            result = func(*args, **kwargs)

            # 检查条件
            if condition is not None and not condition(result):
                return result

            # 缓存结果
            _cache.set(cache_key, result, ttl=ttl)

            return result

        # 添加缓存控制方法
        wrapper.cache_clear = lambda: _clear_cache(cache or get_default_cache(), func_name)
        wrapper.cache_info = lambda: _cache_info(cache or get_default_cache(), func_name)

        return wrapper  # type: ignore

    return decorator


def cached_property(
    ttl: Optional[float] = None,
    cache: Optional[Cache] = None,
) -> Callable[[Callable[[Any], T]], property]:
    """缓存属性装饰器

    类似 functools.cached_property，但支持 TTL。

    Example:
        class MyClass:
            @cached_property(ttl=300)
            def expensive_property(self):
                return compute_something()
    """
    def decorator(method: Callable[[Any], T]) -> property:
        f"_cached_{method.__name__}"

        @property
        @wraps(method)
        def wrapper(self) -> T:
            _cache = cache or get_default_cache()

            # 使用对象 id 作为键的一部分
            cache_key = f"{type(self).__name__}:{id(self)}:{method.__name__}"

            cached_value = _cache.get(cache_key)
            if cached_value is not None:
                return cached_value

            result = method(self)
            _cache.set(cache_key, result, ttl=ttl)

            return result

        return wrapper

    return decorator


def invalidate_cache(
    func: Optional[F] = None,
    cache: Optional[Cache] = None,
    key_prefix: str = "",
) -> None:
    """使缓存失效

    Example:
        @cached()
        def get_user(user_id):
            ...

        # 使特定调用失效
        invalidate_cache(get_user, args=(user_id,))

        # 使所有调用失效
        get_user.cache_clear()
    """
    if func is None:
        # 清空整个缓存
        _cache = cache or get_default_cache()
        _cache.clear()
        return

    func_name = f"{func.__module__}.{func.__qualname__}"
    _clear_cache(cache or get_default_cache(), func_name)


def _build_key(
    func: Callable,
    func_name: str,
    args: tuple,
    kwargs: dict,
    prefix: str,
    ignore_args: list[str],
) -> str:
    """构建缓存键"""
    # 获取参数签名
    sig = inspect.signature(func)
    params = list(sig.parameters.keys())

    # 构建参数字典
    bound_args = {}
    for i, arg in enumerate(args):
        if i < len(params):
            param_name = params[i]
            if param_name not in ignore_args:
                bound_args[param_name] = arg

    for key, value in kwargs.items():
        if key not in ignore_args:
            bound_args[key] = value

    # 计算哈希
    key_parts = [func_name]
    for key in sorted(bound_args.keys()):
        value = bound_args[key]
        key_parts.append(f"{key}={_hash_value(value)}")

    key_str = ":".join(key_parts)
    key_hash = hashlib.md5(key_str.encode()).hexdigest()[:16]

    if prefix:
        return f"{prefix}:{key_hash}"
    return key_hash


def _hash_value(value: Any) -> str:
    """计算值的哈希"""
    try:
        # DataFrame 特殊处理
        if hasattr(value, 'shape'):
            # pandas/polars DataFrame 或 numpy array
            return f"<shape={value.shape}>"

        # 可哈希对象
        return str(hash(value))[:8]
    except TypeError:
        # 不可哈希，使用 repr
        return hashlib.md5(repr(value).encode()).hexdigest()[:8]


def _clear_cache(cache: Cache, func_name: str) -> None:
    """清除函数的所有缓存（简化实现）"""
    # 注意：完整实现需要按前缀清除
    # 这里暂时清除整个缓存
    cache.clear()


def _cache_info(cache: Cache, func_name: str) -> dict:
    """获取缓存信息"""
    stats = cache.stats
    return {
        'hits': stats.hits,
        'misses': stats.misses,
        'hit_rate': stats.hit_rate,
        'size': cache.size,
    }


class CacheRegion:
    """缓存区域

    管理一组相关的缓存键。

    Example:
        user_cache = CacheRegion("users", ttl=300)

        @user_cache.cached()
        def get_user(user_id):
            ...

        # 清除整个区域
        user_cache.invalidate_all()
    """

    def __init__(
        self,
        name: str,
        ttl: Optional[float] = None,
        cache: Optional[Cache] = None,
    ):
        self.name = name
        self.ttl = ttl
        self._cache = cache

    @property
    def cache(self) -> Cache:
        return self._cache or get_default_cache()

    def cached(
        self,
        ttl: Optional[float] = None,
        ignore_args: Optional[list[str]] = None,
    ) -> Callable[[F], F]:
        """装饰器"""
        return cached(
            ttl=ttl or self.ttl,
            cache=self.cache,
            key_prefix=self.name,
            ignore_args=ignore_args,
        )

    def get(self, key: str, default: T = None) -> Optional[T]:
        """获取"""
        return self.cache.get(f"{self.name}:{key}", default)

    def set(self, key: str, value: T, ttl: Optional[float] = None) -> None:
        """设置"""
        self.cache.set(f"{self.name}:{key}", value, ttl=ttl or self.ttl)

    def delete(self, key: str) -> bool:
        """删除"""
        return self.cache.delete(f"{self.name}:{key}")

    def invalidate_all(self) -> None:
        """清除整个区域"""
        # 简化实现
        self.cache.clear()
