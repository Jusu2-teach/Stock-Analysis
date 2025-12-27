"""
PGCS Router: Base
=================

通用路由系统。

设计原则:
- 路由模式完全可配置
- 支持参数化路由
- 支持优先级匹配
- 完全通用，不包含业务逻辑
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional, Dict, List, Callable, Pattern
from abc import ABC, abstractmethod


@dataclass(frozen=True)
class Route:
    """
    路由匹配结果

    Attributes:
        pattern: 匹配的模式
        params: 提取的参数
        raw: 原始路由字符串
        is_matched: 是否匹配成功
    """
    pattern: str = ''
    params: Dict[str, str] = field(default_factory=dict)
    raw: str = ''
    is_matched: bool = True
    handler: Optional[str] = None

    def get(self, key: str, default: str = '') -> str:
        """获取参数"""
        return self.params.get(key, default)

    def __bool__(self) -> bool:
        return self.is_matched


@dataclass
class RoutePattern:
    """
    路由模式定义

    支持参数化模式:
    - {name} - 命名参数
    - {name:regex} - 带正则的命名参数
    - * - 通配符

    Example:
        pattern = RoutePattern('{source}_{field}@{target}')
        match = pattern.match('probe_slope@gene')
        # match.params = {'source': 'probe', 'field': 'slope', 'target': 'gene'}
    """
    template: str
    handler: str = ''
    priority: int = 0
    description: str = ''
    metadata: Dict[str, Any] = field(default_factory=dict)

    _compiled: Optional[Pattern] = field(default=None, repr=False, compare=False)
    _param_names: List[str] = field(default_factory=list, repr=False, compare=False)

    def __post_init__(self):
        self._compile()

    def _compile(self):
        """编译模式为正则表达式"""
        pattern = self.template
        self._param_names = []

        # 提取参数名并替换为正则
        def replace_param(match):
            full = match.group(0)
            name = match.group(1)
            regex = match.group(2) if match.lastindex >= 2 and match.group(2) else r'[^_@/]+'

            self._param_names.append(name)
            return f'(?P<{name}>{regex})'

        # {name} 或 {name:regex}
        pattern = re.sub(r'\{(\w+)(?::([^}]+))?\}', replace_param, pattern)

        # * 通配符
        pattern = pattern.replace('*', r'[^_@/]+')

        # 转义其他特殊字符
        # pattern = re.escape(pattern)  # 不能这样，会转义我们的正则

        self._compiled = re.compile(f'^{pattern}$')

    def match(self, route: str) -> Optional[Route]:
        """
        匹配路由

        Args:
            route: 路由字符串

        Returns:
            Route 如果匹配，否则 None
        """
        if self._compiled is None:
            return None

        m = self._compiled.match(route)
        if m:
            return Route(
                pattern=self.template,
                params=m.groupdict(),
                raw=route,
                is_matched=True,
                handler=self.handler,
            )
        return None

    def build(self, **params) -> str:
        """
        构建路由字符串

        Args:
            **params: 参数值

        Returns:
            路由字符串
        """
        result = self.template
        for name in self._param_names:
            if name in params:
                # 替换 {name} 或 {name:regex}
                result = re.sub(
                    rf'\{{{name}(?::[^}}]+)?\}}',
                    str(params[name]),
                    result,
                )
        return result


class Router:
    """
    PGCS 通用路由器

    管理路由模式并执行匹配。

    Example:
        router = Router()

        # 注册模式
        router.add_pattern('{source}_{field}@{target}', handler='field_handler')
        router.add_pattern('{source}@{target}', handler='simple_handler')

        # 匹配
        route = router.match('probe_slope@gene')
        print(route.params)  # {'source': 'probe', 'field': 'slope', 'target': 'gene'}
        print(route.handler)  # 'field_handler'
    """

    def __init__(self):
        self._patterns: List[RoutePattern] = []
        self._cache: Dict[str, Route] = {}
        self._handlers: Dict[str, Callable] = {}

    def add_pattern(
        self,
        template: str,
        handler: str = '',
        priority: int = 0,
        description: str = '',
        **metadata,
    ) -> RoutePattern:
        """
        添加路由模式

        Args:
            template: 模式模板
            handler: 处理器名称
            priority: 优先级 (高优先)
            description: 描述
            **metadata: 额外元数据

        Returns:
            RoutePattern
        """
        pattern = RoutePattern(
            template=template,
            handler=handler,
            priority=priority,
            description=description,
            metadata=metadata,
        )
        self._patterns.append(pattern)
        self._patterns.sort(key=lambda p: -p.priority)
        return pattern

    def remove_pattern(self, template: str):
        """移除模式"""
        self._patterns = [p for p in self._patterns if p.template != template]
        self._cache.clear()

    def register_handler(self, name: str, handler: Callable):
        """注册处理器"""
        self._handlers[name] = handler

    def match(self, route: str) -> Route:
        """
        匹配路由

        Args:
            route: 路由字符串

        Returns:
            Route (即使不匹配也返回，检查 is_matched)
        """
        # 缓存检查
        if route in self._cache:
            return self._cache[route]

        # 按优先级尝试匹配
        for pattern in self._patterns:
            result = pattern.match(route)
            if result:
                self._cache[route] = result
                return result

        # 无匹配
        return Route(raw=route, is_matched=False)

    def match_all(self, route: str) -> List[Route]:
        """匹配所有模式"""
        results = []
        for pattern in self._patterns:
            result = pattern.match(route)
            if result:
                results.append(result)
        return results

    def build(self, template: str, **params) -> Optional[str]:
        """
        使用指定模板构建路由

        Args:
            template: 模式模板
            **params: 参数

        Returns:
            路由字符串
        """
        for pattern in self._patterns:
            if pattern.template == template:
                return pattern.build(**params)
        return None

    def dispatch(self, route: str, *args, **kwargs) -> Any:
        """
        分发路由到处理器

        Args:
            route: 路由字符串
            *args, **kwargs: 传递给处理器的参数

        Returns:
            处理器返回值
        """
        match = self.match(route)
        if not match.is_matched:
            raise ValueError(f"No matching route for: {route}")

        if not match.handler:
            raise ValueError(f"No handler for route: {route}")

        handler = self._handlers.get(match.handler)
        if not handler:
            raise ValueError(f"Handler not found: {match.handler}")

        return handler(match, *args, **kwargs)

    def list_patterns(self) -> List[RoutePattern]:
        """列出所有模式"""
        return self._patterns.copy()

    def clear_cache(self):
        """清除缓存"""
        self._cache.clear()


__all__ = [
    'Router',
    'Route',
    'RoutePattern',
]
