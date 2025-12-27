"""
PGCS Store: Reference Resolver
==============================

通用引用解析器。

设计原则:
- 零业务耦合：引用格式完全可配置
- 复用 contracts.Router 进行路由匹配
- 支持嵌套引用解析

引用格式:
- 使用 {"__ref__": "path.to.data"} 标记引用
- 支持通过 hash 解析: {"__ref__": "path", "__hash__": "abc123"}
"""

from __future__ import annotations

from typing import Any, Dict, Optional, List, TYPE_CHECKING

if TYPE_CHECKING:
    from .data_store import DataStore

from ..router.base import Router, RoutePattern


class ReferenceNotFoundError(Exception):
    """引用未找到异常"""

    def __init__(self, ref: str, context: str = ''):
        self.ref = ref
        self.context = context
        message = f"引用未找到: {ref}"
        if context:
            message += f" (context: {context})"
        super().__init__(message)


class ReferenceResolver:
    """
    通用引用解析器

    递归解析数据结构中的引用。

    引用格式:
        {"__ref__": "path.to.data"}
        {"__ref__": "path.to.data", "__hash__": "abc123"}

    Example:
        resolver = ReferenceResolver(store)

        # 解析参数
        params = {
            'input': {'__ref__': 'path.to.input'},
            'config': {'nested': {'__ref__': 'path.to.config'}},
        }
        resolved = resolver.resolve(params)

    自定义路由模式:
        resolver = ReferenceResolver(store)
        resolver.register_pattern(
            template='steps.{step}.outputs.{output}',
            handler='step_output',
        )
    """

    # 引用标记
    REF_KEY = '__ref__'
    HASH_KEY = '__hash__'

    def __init__(self, store: 'DataStore'):
        """
        初始化解析器

        Args:
            store: 数据存储
        """
        self._store = store
        self._router = Router()

    def register_pattern(
        self,
        template: str,
        handler: str = '',
        priority: int = 0,
    ):
        """
        注册路由模式

        Args:
            template: 模式模板，如 'steps.{step}.outputs.{output}'
            handler: 处理器名称
            priority: 优先级
        """
        self._router.add_pattern(
            template=template,
            handler=handler,
            priority=priority,
        )

    def resolve(
        self,
        data: Any,
        strict: bool = True,
        default: Any = None,
    ) -> Any:
        """
        递归解析数据中的引用

        Args:
            data: 要解析的数据
            strict: 是否严格模式（引用不存在时抛异常）
            default: 非严格模式下引用不存在时的默认值

        Returns:
            解析后的数据
        """
        return self._walk(data, strict, default)

    def resolve_ref(self, ref: str, hash_key: str = '') -> Optional[Any]:
        """
        解析单个引用

        Args:
            ref: 引用路径
            hash_key: 哈希值（可选）

        Returns:
            解析后的数据
        """
        # 优先尝试哈希
        if hash_key:
            result = self._store.get_by_hash(hash_key)
            if result is not None:
                return result

        # 通过引用路径
        result = self._store.get_by_ref(ref)
        if result is not None:
            return result

        return None

    def parse_ref(self, ref: str) -> Optional[Dict[str, str]]:
        """
        解析引用字符串，提取参数

        Args:
            ref: 引用字符串

        Returns:
            参数字典，如 {'step': 'Load', 'output': 'Data'}
        """
        route = self._router.match(ref)
        if route and route.is_matched:
            return dict(route.params)
        return None

    def build_ref(self, pattern_template: str, **params) -> str:
        """
        构建引用字符串

        Args:
            pattern_template: 模式模板
            **params: 参数

        Returns:
            引用字符串
        """
        # 查找匹配的模式
        for pattern in self._router._patterns:
            if pattern.template == pattern_template:
                return pattern.build(**params)

        # 简单替换
        result = pattern_template
        for key, value in params.items():
            result = result.replace(f'{{{key}}}', str(value))
        return result

    def _walk(self, value: Any, strict: bool, default: Any) -> Any:
        """递归遍历并解析引用"""
        if isinstance(value, dict):
            # 检查是否是引用
            if self.REF_KEY in value:
                return self._resolve_ref_dict(value, strict, default)
            # 递归处理字典
            return {k: self._walk(v, strict, default) for k, v in value.items()}

        if isinstance(value, list):
            # 递归处理列表
            return [self._walk(item, strict, default) for item in value]

        # 其他类型直接返回
        return value

    def _resolve_ref_dict(self, ref_dict: Dict[str, Any], strict: bool, default: Any) -> Any:
        """解析引用字典"""
        ref = ref_dict[self.REF_KEY]
        hash_key = ref_dict.get(self.HASH_KEY, '')

        result = self.resolve_ref(ref, hash_key)

        if result is None:
            if strict:
                raise ReferenceNotFoundError(ref)
            return default

        return result


class BatchResolver:
    """
    批量引用解析器

    用于批量解析多个引用，支持依赖排序。

    Example:
        batch = BatchResolver(store)
        batch.add('a', {'__ref__': 'path.a'})
        batch.add('b', {'__ref__': 'path.b'})
        results = batch.resolve_all()
    """

    def __init__(self, store: 'DataStore'):
        self._resolver = ReferenceResolver(store)
        self._pending: List[tuple] = []

    def add(self, name: str, data: Any):
        """添加待解析项"""
        self._pending.append((name, data))

    def resolve_all(self, strict: bool = True) -> Dict[str, Any]:
        """
        解析所有项

        Returns:
            {name: resolved_value, ...}
        """
        results = {}
        for name, data in self._pending:
            results[name] = self._resolver.resolve(data, strict=strict)
        return results

    def clear(self):
        """清空待解析列表"""
        self._pending.clear()
