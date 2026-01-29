"""
PDDA Aggregation Manager - 聚合管理器
======================================

PDDA 系统的统一门面类，提供简洁的 API。

核心功能：
1. 初始化和配置 PDDA 系统
2. 统一的执行接口
3. 生命周期管理
4. 统计和监控

设计模式：
- Facade 模式：隐藏内部复杂性
- Singleton 模式：全局唯一实例
"""

from __future__ import annotations
import logging
from typing import Dict, Any, Callable, Optional
from threading import Lock

from .protocols import Aggregatable
from .discovery import MethodScanner
from .collector import UniversalCollector, CollectionStrategy
from .injector import DynamicInjector

__all__ = ['AggregationManager']

logger = logging.getLogger(__name__)


class AggregationManager:
    """
    聚合管理器（门面类）

    提供 PDDA 系统的统一入口。

    使用示例：
        # 获取单例
        manager = AggregationManager.get()

        # 初始化（扫描方法）
        manager.initialize()

        # 执行方法（自动注入）
        result = manager.execute(
            method_name="report_comprehensive",
            func=report_func,
            params={"output_path": "report.md"}
        )

        # 获取统计信息
        stats = manager.get_stats()
    """

    _instance: Optional[AggregationManager] = None
    _lock = Lock()

    def __init__(self):
        """初始化管理器"""
        self._scanner = None  # 延迟初始化
        self._collector = UniversalCollector()
        self._injector = None  # 延迟初始化（依赖 scanner）
        self._initialized = False
        self._registry = None  # 保存 registry 引用

        logger.debug("AggregationManager 创建")

    @classmethod
    def get(cls) -> AggregationManager:
        """获取单例实例（线程安全）"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls):
        """重置单例（主要用于测试）"""
        with cls._lock:
            if cls._instance:
                cls._instance._clear_all()
            cls._instance = None

    def initialize(self, registry=None):
        """
        初始化 PDDA 系统

        Args:
            registry: orchestrator.Registry 实例（推荐传入，实现依赖注入）
                      如果未传入，scanner 会使用延迟导入作为回退

        执行：
        1. 扫描所有已注册方法
        2. 构建生产者/消费者索引
        3. 准备收集器和注入器
        """
        if self._initialized:
            logger.warning("PDDA 已初始化，跳过")
            return

        logger.info("🚀 初始化 PDDA 系统...")

        # 保存 registry 引用（如果提供）
        if registry is not None:
            self._registry = registry

        # 初始化 scanner
        self._scanner = MethodScanner()

        # 🆕 传递 registry 给 scanner，实现依赖注入
        # 这样 shared 层不需要直接 import orchestrator
        self._scanner.scan_all_methods(registry=self._registry)

        # 初始化 injector（依赖 scanner）
        self._injector = DynamicInjector(self._scanner, self._collector)

        # 标记为已初始化
        self._initialized = True

        # 输出统计信息
        stats = self.get_stats()
        logger.info(
            f"✅ PDDA 初始化完成 - "
            f"生产者: {stats['scanner']['total_producers']}, "
            f"消费者: {stats['scanner']['total_consumers']}"
        )

    def execute(
        self,
        method_name: str,
        func: Callable,
        params: Dict[str, Any]
    ) -> Any:
        """
        执行方法（自动注入聚合数据）

        Args:
            method_name: 方法名
            func: 函数对象
            params: 用户提供的参数

        Returns:
            函数执行结果
        """
        # 确保已初始化
        if not self._initialized:
            self.initialize()

        # 使用注入器执行
        return self._injector.inject_and_execute(
            method_name=method_name,
            func=func,
            params=params
        )

    def collect(self, result: Any) -> bool:
        """
        手动收集数据

        Args:
            result: 可聚合数据

        Returns:
            是否成功收集
        """
        return self._collector.collect(result)

    def get_collected_data(self) -> Any:
        """获取所有收集的数据"""
        return self._collector.get_all()

    def clear_collected_data(self):
        """清空收集的数据"""
        self._collector.clear()

    def set_collection_strategy(self, strategy: CollectionStrategy):
        """设置收集策略"""
        self._collector.set_strategy(strategy)

    def get_scanner(self) -> MethodScanner:
        """获取方法扫描器（高级用法）"""
        return self._scanner

    def get_collector(self) -> UniversalCollector:
        """获取收集器（高级用法）"""
        return self._collector

    def get_injector(self) -> DynamicInjector:
        """获取注入器（高级用法）"""
        return self._injector

    def get_stats(self) -> Dict[str, Any]:
        """
        获取统计信息

        Returns:
            {
                'initialized': bool,
                'scanner': {...},
                'collector': {...}
            }
        """
        return {
            'initialized': self._initialized,
            'scanner': self._scanner.get_stats() if self._scanner else {'total_producers': 0, 'total_consumers': 0},
            'collector': self._collector.get_stats(),
        }

    def _clear_all(self):
        """清空所有数据（内部方法）"""
        self._collector.clear()
        self._initialized = False

    def clear(self):
        """清空收集的数据（公共方法）"""
        self.clear_collected_data()

    def is_producer(self, method_name: str) -> bool:
        """
        检查方法是否为生产者

        Args:
            method_name: 方法名

        Returns:
            True if 方法是生产者
        """
        if not self._initialized or self._scanner is None:
            return False
        return self._scanner.is_producer(method_name)

    def is_consumer(self, method_name: str) -> bool:
        """
        检查方法是否为消费者

        Args:
            method_name: 方法名

        Returns:
            True if 方法是消费者
        """
        if not self._initialized or self._scanner is None:
            return False
        return self._scanner.is_consumer(method_name)

    def collect_result(self, method_name: str, result: Any) -> bool:
        """
        收集生产者的输出结果

        Args:
            method_name: 方法名
            result: 方法返回值

        Returns:
            是否成功收集

        Raises:
            TypeError: 如果结果不是 Aggregatable 类型
        """
        # 🌟 PDDA 纯净路径: 强制要求 Aggregatable 协议
        if isinstance(result, Aggregatable):
            success = self._collector.collect(result)
            if success:
                logger.info(f"✅ PDDA: 收集生产者结果 {method_name} (Aggregatable)")
            return success

        # 纯净路径: 不支持非 Aggregatable 类型
        raise TypeError(
            f"PDDA: {method_name} 返回类型 {type(result).__name__} 不支持。\n"
            f"生产者必须返回 AggregatableResult 类型。\n"
            f"示例: return AggregatableResult(key='metric_name', value=df)"
        )

    def inject_aggregated_params(
        self,
        method_name: str,
        current_params: Dict[str, Any],
        data_store: Any = None
    ) -> Dict[str, Any]:
        """
        为消费者注入聚合数据

        Args:
            method_name: 方法名
            current_params: 当前参数字典
            data_store: 可选的 DataStore 对象（用于从步骤输出构建聚合数据）

        Returns:
            注入后的参数字典（合并了聚合数据）
        """
        # 确保已初始化
        if not self._initialized or self._scanner is None:
            logger.debug(f"⚠️ PDDA未初始化，跳过注入 method={method_name}")
            return current_params

        # 获取消费者信息（返回 List[ConsumerInfo]）
        consumers = self._scanner.get_consumers_for_method(method_name)
        if not consumers:
            logger.debug(f"⚠️ {method_name} 不是消费者，跳过注入")
            return current_params

        # 尝试从收集器获取数据
        collected_data = self._collector.get_all()

        # 如果收集器为空，尝试从 data_store 构建
        if not collected_data and data_store is not None:
            logger.info(f"🔄 PDDA: 收集器为空，尝试从 DataStore 构建聚合数据")
            collected_data = self._build_from_datastore(data_store)

        if not collected_data:
            logger.warning(f"⚠️ PDDA: 无可用数据，无法注入到 {method_name}")
            return current_params

        # 为每个聚合参数注入数据
        injected_params = current_params.copy()
        for consumer in consumers:
            param_name = consumer.param_name

            # 用户已提供，跳过注入
            if param_name in injected_params:
                logger.debug(f"⏭️  参数 {param_name} 已由用户提供，跳过注入")
                continue

            # 注入收集的数据
            injected_params[param_name] = collected_data
            logger.info(f"✅ PDDA: 注入参数 {param_name} <- {len(collected_data)} items")

        return injected_params

    def _build_from_datastore(self, data_store: Any) -> Dict[str, Any]:
        """
        从 DataStore 构建聚合数据

        使用 MetricRegistry 统一解析指标名称，实现零硬编码。

        Args:
            data_store: Pipeline DataStore 对象

        Returns:
            聚合数据字典 {canonical_metric_name: DataFrame}
        """
        from shared.naming_convention import MetricRegistry

        aggregated = {}

        # 遍历 DataStore 中的所有数据
        if hasattr(data_store, '_store'):
            for key, entry in data_store._store.items():
                # 检查是否是趋势分析输出
                if '_Trend_Result' in key:
                    # 提取 metric_name: "Analyze_ROIC_Trend__ROIC_Trend_Result" -> "ROIC"
                    parts = key.split('__')[0].split('_')
                    if len(parts) >= 3 and parts[0] == 'Analyze' and parts[-1] == 'Trend':
                        raw_metric = '_'.join(parts[1:-1]).lower()

                        # 🆕 统一使用 MetricRegistry.resolve_safe() 解析
                        # MetricRegistry 已集成别名系统，无需在此硬编码
                        config = MetricRegistry.resolve_safe(raw_metric)
                        if config:
                            canonical_name = config.business_key
                        else:
                            # 未注册的指标，使用原始名称
                            canonical_name = raw_metric
                            logger.debug(f"  ⚠️ 指标 '{raw_metric}' 未在 MetricRegistry 注册，使用原名")

                        # DataEntry 对象，取 value 字段
                        value = entry.value if hasattr(entry, 'value') else entry
                        aggregated[canonical_name] = value
                        logger.debug(f"  📦 提取: {key} -> {canonical_name}")

        if aggregated:
            logger.info(f"✅ 从 DataStore 构建聚合数据: {list(aggregated.keys())}")

        return aggregated

    def __repr__(self) -> str:
        stats = self.get_stats()
        return (
            f"AggregationManager("
            f"initialized={self._initialized}, "
            f"producers={stats['scanner']['total_producers']}, "
            f"consumers={stats['scanner']['total_consumers']})"
        )
