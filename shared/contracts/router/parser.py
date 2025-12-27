"""
PGCS Router: Parser
===================

路由字符串解析器。

提供多种解析策略。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Dict, List, Tuple
from abc import ABC, abstractmethod
import re


@dataclass
class ParseResult:
    """解析结果"""
    segments: List[str]
    separators: List[str]
    params: Dict[str, str]
    raw: str
    is_valid: bool = True
    error: str = ''


class RouteParser(ABC):
    """
    路由解析器抽象基类

    定义路由字符串的解析策略。
    """

    @abstractmethod
    def parse(self, route: str) -> ParseResult:
        """解析路由字符串"""
        pass

    @abstractmethod
    def build(self, **params) -> str:
        """构建路由字符串"""
        pass


class DelimiterParser(RouteParser):
    """
    分隔符解析器

    基于分隔符拆分路由。

    Example:
        parser = DelimiterParser(
            separators=['_', '@'],
            segment_names=['source', 'field', 'target']
        )
        result = parser.parse('probe_slope@gene')
        # result.params = {'source': 'probe', 'field': 'slope', 'target': 'gene'}
    """

    def __init__(
        self,
        separators: List[str],
        segment_names: Optional[List[str]] = None,
    ):
        self.separators = separators
        self.segment_names = segment_names or []

        # 构建正则
        sep_pattern = '|'.join(re.escape(s) for s in separators)
        self._split_pattern = re.compile(f'({sep_pattern})')

    def parse(self, route: str) -> ParseResult:
        """解析路由"""
        parts = self._split_pattern.split(route)

        # 分离段和分隔符
        segments = parts[::2]  # 偶数位置是段
        separators = parts[1::2]  # 奇数位置是分隔符

        # 构建参数
        params = {}
        for i, seg in enumerate(segments):
            if i < len(self.segment_names):
                params[self.segment_names[i]] = seg
            else:
                params[f'segment_{i}'] = seg

        return ParseResult(
            segments=segments,
            separators=separators,
            params=params,
            raw=route,
        )

    def build(self, **params) -> str:
        """构建路由"""
        segments = []
        for i, name in enumerate(self.segment_names):
            if name in params:
                segments.append(str(params[name]))
            elif f'segment_{i}' in params:
                segments.append(str(params[f'segment_{i}']))

        # 交替插入分隔符
        result = []
        for i, seg in enumerate(segments):
            result.append(seg)
            if i < len(self.separators):
                result.append(self.separators[i])

        return ''.join(result)


class TemplateParser(RouteParser):
    """
    模板解析器

    基于模板解析路由。

    Example:
        parser = TemplateParser('{source}_{field}@{target}')
        result = parser.parse('probe_slope@gene')
        # result.params = {'source': 'probe', 'field': 'slope', 'target': 'gene'}
    """

    def __init__(self, template: str):
        self.template = template
        self._param_names: List[str] = []
        self._regex = self._compile_template()

    def _compile_template(self) -> re.Pattern:
        """编译模板为正则"""
        pattern = self.template

        def replace_param(match):
            name = match.group(1)
            self._param_names.append(name)
            return f'(?P<{name}>[^_@/]+)'

        pattern = re.sub(r'\{(\w+)\}', replace_param, pattern)
        return re.compile(f'^{pattern}$')

    def parse(self, route: str) -> ParseResult:
        """解析路由"""
        m = self._regex.match(route)

        if not m:
            return ParseResult(
                segments=[],
                separators=[],
                params={},
                raw=route,
                is_valid=False,
                error=f"Route does not match template: {self.template}",
            )

        params = m.groupdict()
        segments = [params.get(name, '') for name in self._param_names]

        return ParseResult(
            segments=segments,
            separators=[],
            params=params,
            raw=route,
        )

    def build(self, **params) -> str:
        """构建路由"""
        result = self.template
        for name in self._param_names:
            if name in params:
                result = result.replace(f'{{{name}}}', str(params[name]))
        return result


class ChainParser(RouteParser):
    """
    链式解析器

    尝试多个解析器直到成功。
    """

    def __init__(self, parsers: List[RouteParser]):
        self.parsers = parsers

    def parse(self, route: str) -> ParseResult:
        """尝试所有解析器"""
        for parser in self.parsers:
            result = parser.parse(route)
            if result.is_valid:
                return result

        return ParseResult(
            segments=[],
            separators=[],
            params={},
            raw=route,
            is_valid=False,
            error="No parser could parse the route",
        )

    def build(self, **params) -> str:
        """使用第一个解析器构建"""
        if self.parsers:
            return self.parsers[0].build(**params)
        return ''


__all__ = [
    'RouteParser',
    'ParseResult',
    'DelimiterParser',
    'TemplateParser',
    'ChainParser',
]
