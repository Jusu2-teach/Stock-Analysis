"""
PDDA Discovery Engine - 发现引擎
=================================

自动发现和分析系统中的聚合能力。

核心功能：
1. 扫描所有已注册方法
2. 识别生产者（产生可聚合数据的方法）
3. 识别消费者（需要聚合数据的方法）
4. 构建依赖关系图

设计原则：
- 零配置：完全基于类型标注和约定
- 智能匹配：自动匹配生产者和消费者
- 可扩展：支持自定义检测逻辑
"""

from __future__ import annotations
import inspect
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Set, Optional, Callable, Any, get_type_hints
from collections import defaultdict

from .protocols import Aggregatable
from .conventions import NamingConvention, TypeConvention, ProtocolConvention

__all__ = [
    'CapabilityInfo',
    'ProducerInfo',
    'ConsumerInfo',
    'MethodScanner',
]

logger = logging.getLogger(__name__)


@dataclass
class CapabilityInfo:
    """
    方法能力信息（基类）

    记录方法的基本信息和聚合能力
    """
    method_name: str                    # 方法名
    func: Callable                      # 函数对象
    component_type: str                 # 组件类型 (business_engine, data_engine等)
    engine_type: str                    # 引擎类型 (duckdb, polars等)


@dataclass
class ProducerInfo(CapabilityInfo):
    """
    生产者信息

    记录能够产生可聚合数据的方法
    """
    produces_type: Optional[type] = None     # 产生的类型
    key_type: Optional[type] = None          # 键类型
    value_type: Optional[type] = None        # 值类型

    # 装饰器配置
    key_field: Optional[str] = None          # 键字段名
    value_field: Optional[str] = None        # 值字段名
    auto_collect: bool = True                # 是否自动收集

    def __repr__(self) -> str:
        return (
            f"Producer({self.method_name}, "
            f"produces={self.produces_type.__name__ if self.produces_type else 'Unknown'})"
        )


@dataclass
class ConsumerInfo(CapabilityInfo):
    """
    消费者信息

    记录需要聚合数据的方法
    """
    param_name: str                          # 参数名
    param_type: Optional[type] = None        # 参数类型
    required_value_type: Optional[type] = None  # 要求的值类型

    # 约束条件
    min_items: int = 0                       # 最少项数
    required_keys: List[str] = field(default_factory=list)  # 必需的键

    # 装饰器配置
    validation_func: Optional[Callable] = None  # 自定义验证函数

    def __repr__(self) -> str:
        return (
            f"Consumer({self.method_name}, "
            f"param={self.param_name}, "
            f"type={self.param_type})"
        )


class MethodScanner:
    """
    方法扫描器：发现系统中的聚合能力

    工作流程：
    1. 从 Orchestrator.Registry 获取所有已注册方法
    2. 对每个方法进行能力分析
    3. 构建生产者和消费者索引
    4. 建立匹配关系

    使用示例：
        scanner = MethodScanner()
        scanner.scan_all_methods()

        # 查询生产者
        producers = scanner.get_all_producers()

        # 查询消费者
        consumers = scanner.get_all_consumers()

        # 匹配
        matches = scanner.match_producers_to_consumer("report_comprehensive")
    """

    def __init__(self):
        self._producers: Dict[str, ProducerInfo] = {}
        self._consumers: Dict[str, List[ConsumerInfo]] = defaultdict(list)
        self._scanned = False
        self._registry = None  # 外部注入的 registry 引用

    def scan_all_methods(self, registry=None):
        """
        扫描所有已注册方法

        Args:
            registry: Orchestrator Registry 实例（推荐通过依赖注入传入）
                      如果为 None，将使用延迟导入作为回退方案

        Note:
            推荐从外部传入 registry，避免 shared 层直接依赖 orchestrator 层。
            延迟导入保留作为向后兼容的回退方案。
        """
        try:
            orch_registry = registry

            # 回退方案：如果未传入 registry，尝试延迟导入
            if orch_registry is None:
                try:
                    from orchestrator.registry import Registry
                    orch_registry = Registry.get()
                    logger.debug("⚠️ PDDA: 使用延迟导入获取 Registry（建议通过参数传入）")
                except ImportError as e:
                    logger.error(f"❌ PDDA: 无法获取 Registry: {e}")
                    return

            # 保存引用供后续使用
            self._registry = orch_registry

            logger.info("🔍 PDDA: 开始扫描已注册方法...")

            producer_count = 0
            consumer_count = 0

            # 遍历所有已注册方法
            for full_key, method_reg in orch_registry.index.by_full_key.items():
                method_name = method_reg.engine_name
                func = method_reg.callable  # 使用 callable 属性
                component_type = method_reg.component_type
                engine_type = method_reg.engine_type

                # 如果没有callable，跳过
                if func is None:
                    logger.warning(f"⚠️ PDDA: 方法 {full_key} 没有 callable，跳过")
                    continue

                # 分析生产者能力
                if self._analyze_producer(method_name, func, component_type, engine_type):
                    producer_count += 1

                # 分析消费者能力
                consumer_infos = self._analyze_consumer(method_name, func, component_type, engine_type)
                if consumer_infos:
                    consumer_count += len(consumer_infos)

            self._scanned = True
            logger.info(
                f"✅ PDDA: 扫描完成 - "
                f"发现 {producer_count} 个生产者, {consumer_count} 个消费者"
            )

        except Exception as e:
            logger.error(f"❌ PDDA: 扫描失败: {e}", exc_info=True)

    def _analyze_producer(
        self,
        method_name: str,
        func: Callable,
        component_type: str,
        engine_type: str
    ) -> bool:
        """
        分析方法是否为生产者

        Returns:
            是否为生产者
        """
        # 使用协议约定检测
        if not ProtocolConvention.is_producer(func):
            return False

        try:
            # 提取类型信息
            hints = get_type_hints(func, include_extras=True)
            return_type = hints.get('return')

            # 提取装饰器配置
            config = ProtocolConvention.extract_aggregatable_config(func)

            # 创建生产者信息
            producer_info = ProducerInfo(
                method_name=method_name,
                func=func,
                component_type=component_type,
                engine_type=engine_type,
                produces_type=return_type,
                key_field=config.get('key'),
                value_field=config.get('value'),
                auto_collect=config.get('auto_collect', True),
            )

            # 注册生产者
            self._producers[method_name] = producer_info

            logger.debug(f"  ✓ 生产者: {method_name} → {return_type}")
            return True

        except Exception as e:
            logger.warning(f"  ✗ 分析生产者失败 {method_name}: {e}")
            return False

    def _analyze_consumer(
        self,
        method_name: str,
        func: Callable,
        component_type: str,
        engine_type: str
    ) -> List[ConsumerInfo]:
        """
        分析方法是否为消费者

        Returns:
            消费者信息列表（一个方法可能有多个聚合参数）
        """
        # 使用协议约定检测
        if not ProtocolConvention.is_consumer(func):
            return []

        consumer_infos = []

        try:
            sig = inspect.signature(func)
            hints = get_type_hints(func)

            # 提取装饰器配置
            config = ProtocolConvention.extract_aggregatable_config(func)
            param_names = config.get('param_names', [])

            for param_name, param in sig.parameters.items():
                # 跳过特殊参数
                if param_name in ('self', 'cls', 'data', 'args', 'kwargs'):
                    continue

                # 检查是否为聚合参数
                param_type = hints.get(param_name)

                is_aggregation = False

                # 1. 检查装饰器指定
                if param_names and param_name in param_names:
                    is_aggregation = True

                # 2. 检查命名约定
                elif NamingConvention.is_aggregation_parameter(param_name):
                    is_aggregation = True

                # 3. 检查类型约定
                elif TypeConvention.is_aggregation_type(param_type):
                    is_aggregation = True

                if not is_aggregation:
                    continue

                # 提取值类型
                required_value_type = TypeConvention.extract_value_type(param_type)

                # 创建消费者信息
                consumer_info = ConsumerInfo(
                    method_name=method_name,
                    func=func,
                    component_type=component_type,
                    engine_type=engine_type,
                    param_name=param_name,
                    param_type=param_type,
                    required_value_type=required_value_type,
                    min_items=config.get('min_items', 0),
                    required_keys=config.get('required_keys', []),
                    validation_func=config.get('validation'),
                )

                consumer_infos.append(consumer_info)
                logger.debug(f"  ✓ 消费者: {method_name}.{param_name} → {param_type}")

            # 注册消费者
            if consumer_infos:
                self._consumers[method_name].extend(consumer_infos)

        except Exception as e:
            logger.warning(f"  ✗ 分析消费者失败 {method_name}: {e}")

        return consumer_infos

    def get_all_producers(self) -> Dict[str, ProducerInfo]:
        """获取所有生产者"""
        return self._producers.copy()

    def get_all_consumers(self) -> Dict[str, List[ConsumerInfo]]:
        """获取所有消费者"""
        return dict(self._consumers)

    def get_producer(self, method_name: str) -> Optional[ProducerInfo]:
        """获取指定生产者"""
        return self._producers.get(method_name)

    def get_consumers_for_method(self, method_name: str) -> List[ConsumerInfo]:
        """获取指定方法的所有消费者参数"""
        return self._consumers.get(method_name, [])

    def match_producers_to_consumer(
        self,
        consumer_method: str,
        param_name: Optional[str] = None
    ) -> Dict[str, List[str]]:
        """
        为消费者方法匹配生产者

        Args:
            consumer_method: 消费者方法名
            param_name: 参数名（可选，如果不指定则匹配所有参数）

        Returns:
            {参数名: [生产者方法列表]}
        """
        consumers = self.get_consumers_for_method(consumer_method)

        if not consumers:
            return {}

        # 过滤指定参数
        if param_name:
            consumers = [c for c in consumers if c.param_name == param_name]

        matches = {}

        for consumer in consumers:
            # 简化实现：匹配所有生产者
            # 可以根据类型进行更精确的匹配
            producer_methods = list(self._producers.keys())

            # 过滤：只匹配 auto_collect=True 的生产者
            producer_methods = [
                name for name in producer_methods
                if self._producers[name].auto_collect
            ]

            matches[consumer.param_name] = producer_methods

        return matches

    def is_scanned(self) -> bool:
        """是否已完成扫描"""
        return self._scanned

    def is_producer(self, method_name: str) -> bool:
        """
        检查方法是否为生产者

        Args:
            method_name: 方法名

        Returns:
            True if 方法是生产者
        """
        return method_name in self._producers

    def is_consumer(self, method_name: str) -> bool:
        """
        检查方法是否为消费者

        Args:
            method_name: 方法名

        Returns:
            True if 方法是消费者
        """
        return method_name in self._consumers

    def get_stats(self) -> Dict[str, int]:
        """获取统计信息"""
        return {
            'total_producers': len(self._producers),
            'total_consumers': sum(len(v) for v in self._consumers.values()),
            'scanned': self._scanned,
        }
