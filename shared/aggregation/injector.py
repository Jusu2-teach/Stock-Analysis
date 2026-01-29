"""
PDDA Injection Engine - 注入引擎
=================================

负责参数解析和动态注入。

核心功能：
1. 解析函数签名，识别需要注入的参数
2. 从收集器获取数据并绑定到参数
3. 类型适配和验证
4. 执行函数并收集结果

设计原则：
- 透明注入：对业务代码零侵入
- 类型安全：严格的类型检查
- 智能匹配：自动匹配生产者和消费者
"""

from __future__ import annotations
import inspect
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Any, Callable, Optional, get_type_hints
from functools import wraps

from .protocols import Aggregatable
from .discovery import MethodScanner, ConsumerInfo
from .collector import UniversalCollector
from .conventions import NamingConvention, TypeConvention

__all__ = [
    'InjectionContext',
    'ParameterResolver',
    'DynamicInjector',
]

logger = logging.getLogger(__name__)


@dataclass
class InjectionContext:
    """
    注入上下文

    携带注入过程中的元数据
    """
    method_name: str
    func: Callable
    original_params: Dict[str, Any]
    injected_params: Dict[str, Any] = field(default_factory=dict)
    skip_params: List[str] = field(default_factory=list)

    def merge_params(self) -> Dict[str, Any]:
        """合并原始参数和注入参数（原始参数优先）"""
        return {**self.injected_params, **self.original_params}


class ParameterResolver:
    """
    参数解析器

    分析函数签名，确定哪些参数需要注入
    """

    def __init__(self, scanner: MethodScanner):
        self.scanner = scanner

    def resolve(
        self,
        method_name: str,
        func: Callable,
        provided_params: Dict[str, Any]
    ) -> InjectionContext:
        """
        解析函数参数需求

        Args:
            method_name: 方法名
            func: 函数对象
            provided_params: 用户提供的参数

        Returns:
            注入上下文
        """
        context = InjectionContext(
            method_name=method_name,
            func=func,
            original_params=provided_params
        )

        # 获取消费者信息
        consumers = self.scanner.get_consumers_for_method(method_name)

        if not consumers:
            # 不是消费者，无需注入
            return context

        # 分析每个需要注入的参数
        for consumer in consumers:
            param_name = consumer.param_name

            # 用户已提供，跳过注入
            if param_name in provided_params:
                context.skip_params.append(param_name)
                logger.debug(f"参数 {param_name} 已由用户提供，跳过注入")
                continue

            # 标记为需要注入
            context.injected_params[param_name] = None  # 占位，后续填充
            logger.debug(f"参数 {param_name} 需要注入")

        return context

    def get_required_params(self, method_name: str) -> List[str]:
        """获取方法需要注入的参数名列表"""
        consumers = self.scanner.get_consumers_for_method(method_name)
        return [c.param_name for c in consumers]


class DynamicInjector:
    """
    动态注入器

    核心组件：执行参数注入和方法调用

    工作流程：
    1. 解析函数签名（ParameterResolver）
    2. 从收集器获取数据
    3. 类型适配和验证
    4. 注入参数并执行函数
    5. 收集执行结果

    使用示例：
        scanner = MethodScanner()
        scanner.scan_all_methods()

        collector = UniversalCollector()
        injector = DynamicInjector(scanner, collector)

        # 执行方法（自动注入）
        result = injector.inject_and_execute(
            method_name="report_comprehensive",
            func=report_func,
            params={"output_path": "report.md"}
        )
    """

    def __init__(
        self,
        scanner: MethodScanner,
        collector: UniversalCollector
    ):
        """
        Args:
            scanner: 方法扫描器
            collector: 数据收集器
        """
        self.scanner = scanner
        self.collector = collector
        self.resolver = ParameterResolver(scanner)

        logger.debug("DynamicInjector 初始化完成")

    def inject_and_execute(
        self,
        method_name: str,
        func: Callable,
        params: Dict[str, Any]
    ) -> Any:
        """
        注入参数并执行方法

        Args:
            method_name: 方法名
            func: 函数对象
            params: 用户提供的参数

        Returns:
            函数执行结果
        """
        # 1. 解析参数需求
        context = self.resolver.resolve(method_name, func, params)

        # 2. 准备注入数据
        if context.injected_params:
            self._prepare_injection_data(context)

        # 3. 合并参数
        final_params = context.merge_params()

        # 4. 过滤参数（只保留函数需要的）
        final_params = self._filter_params(func, final_params)

        # 5. 执行函数
        logger.debug(f"执行方法: {method_name} with params: {list(final_params.keys())}")
        result = func(**final_params)

        # 6. 收集结果（如果是可聚合类型）
        self.collector.collect(result)

        return result

    def _prepare_injection_data(self, context: InjectionContext):
        """
        准备注入数据

        从收集器获取数据并填充到 context.injected_params
        """
        # 获取所有收集的数据
        all_data = self.collector.get_all()

        # 获取消费者信息
        consumers = self.scanner.get_consumers_for_method(context.method_name)

        for consumer in consumers:
            param_name = consumer.param_name

            # 跳过用户已提供的参数
            if param_name in context.skip_params:
                continue

            # 注入数据
            context.injected_params[param_name] = all_data

            logger.debug(
                f"注入参数 {param_name}: "
                f"{len(all_data) if isinstance(all_data, dict) else 'N/A'} items"
            )

            # 验证注入的数据
            if consumer.min_items > 0:
                if isinstance(all_data, dict) and len(all_data) < consumer.min_items:
                    logger.warning(
                        f"注入的数据项数量不足: {len(all_data)} < {consumer.min_items}"
                    )

            # 检查必需的键
            if consumer.required_keys:
                if isinstance(all_data, dict):
                    missing_keys = set(consumer.required_keys) - set(all_data.keys())
                    if missing_keys:
                        logger.warning(f"缺少必需的键: {missing_keys}")

            # 自定义验证
            if consumer.validation_func:
                try:
                    is_valid = consumer.validation_func(all_data)
                    if not is_valid:
                        logger.warning(f"参数 {param_name} 验证失败")
                except Exception as e:
                    logger.error(f"验证函数执行失败: {e}")

    def _filter_params(self, func: Callable, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        过滤参数：只保留函数签名中的参数

        Args:
            func: 函数对象
            params: 参数字典

        Returns:
            过滤后的参数字典
        """
        try:
            sig = inspect.signature(func)
            valid_params = {}

            for param_name in sig.parameters:
                if param_name in params:
                    valid_params[param_name] = params[param_name]

            return valid_params

        except Exception as e:
            logger.warning(f"参数过滤失败: {e}，使用原始参数")
            return params

    def collect(self, result: Any) -> bool:
        """
        收集方法执行结果

        Args:
            result: 方法返回值

        Returns:
            是否成功收集
        """
        return self.collector.collect(result)

    def clear_collected_data(self):
        """清空收集的数据"""
        self.collector.clear()

    def get_collected_data(self) -> Any:
        """获取所有收集的数据"""
        return self.collector.get_all()

    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            'scanner': self.scanner.get_stats(),
            'collector': self.collector.get_stats(),
        }
