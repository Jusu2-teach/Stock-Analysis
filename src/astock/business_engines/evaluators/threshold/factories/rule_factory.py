"""
规则工厂 (Rule Factory)
=======================

负责创建和管理规则实例的工厂类。

设计模式:
- 工厂模式: 集中创建规则实例
- 自动发现: 通过反射自动发现规则类
- 依赖注入: 支持规则配置注入

特性:
- 类型安全: 使用 Protocol 检查规则实现
- 按优先级排序: 自动按规则优先级排序
- 分类管理: 按规则类别 (veto/penalty/bonus/validation) 分组

作者: AStock Analysis System
日期: 2026-01-10
版本: 2.0.0
"""

from typing import List, Dict, Type, Optional
import logging
import inspect
from dataclasses import dataclass

from ..protocols import RuleProtocol, RuleFactoryProtocol
from ..rule_config import RuleCategory, RuleConfig, DEFAULT_CONFIG

logger = logging.getLogger(__name__)


@dataclass
class RuleRegistryEntry:
    """规则注册表项"""
    rule_class: Type[RuleProtocol]
    category: RuleCategory
    priority: int
    enabled: bool
    description: str


class RuleFactory:
    """
    规则工厂

    职责:
    - 创建规则实例
    - 管理规则注册表
    - 按类别和优先级组织规则

    Examples:
        >>> factory = RuleFactory()
        >>> veto_rules = factory.create_veto_rules()
        >>> all_rules = factory.create_all_rules()

        >>> # 自动发现规则
        >>> factory.discover_rules_from_module("veto")
        >>> rules = factory.create_rules_by_category(RuleCategory.VETO)
    """

    def __init__(self, config: Optional[RuleConfig] = None):
        """
        初始化规则工厂

        Args:
            config: 规则配置 (可选，默认使用 DEFAULT_CONFIG)
        """
        self.config = config or DEFAULT_CONFIG
        self._registry: Dict[str, RuleRegistryEntry] = {}
        self._initialized = False

    def _ensure_initialized(self):
        """确保工厂已初始化"""
        if not self._initialized:
            self._register_builtin_rules()
            self._initialized = True

    def _register_builtin_rules(self):
        """注册内置规则"""
        # 导入所有内置规则
        try:
            from ..rules import veto
            self.discover_rules_from_module(veto)
        except ImportError as e:
            logger.warning(f"无法导入 veto 模块: {e}")

        try:
            from ..rules import penalty
            self.discover_rules_from_module(penalty)
        except ImportError as e:
            logger.warning(f"无法导入 penalty 模块: {e}")

        try:
            from ..rules import bonus
            self.discover_rules_from_module(bonus)
        except ImportError as e:
            logger.warning(f"无法导入 bonus 模块: {e}")

        try:
            from ..rules import validation
            self.discover_rules_from_module(validation)
        except ImportError as e:
            logger.warning(f"无法导入 validation 模块: {e}")

    def register_rule(
        self,
        rule_class: Type[RuleProtocol],
        force: bool = False
    ):
        """
        注册规则类

        Args:
            rule_class: 规则类 (必须实现 RuleProtocol)
            force: 是否强制覆盖已存在的规则

        Raises:
            ValueError: 如果规则类不符合 RuleProtocol
            KeyError: 如果规则已存在且 force=False
        """
        # 检查是否实现 RuleProtocol
        if not self._is_rule_protocol(rule_class):
            raise ValueError(
                f"规则类 {rule_class.__name__} 未实现 RuleProtocol"
            )

        # 获取规则元数据
        rule_name = getattr(rule_class, 'name', rule_class.__name__)
        category = getattr(rule_class, 'category', RuleCategory.PENALTY)
        priority = getattr(rule_class, 'priority', 100)
        enabled = getattr(rule_class, 'enabled', True)
        description = getattr(rule_class, 'description', '')

        # 检查是否已存在
        if rule_name in self._registry and not force:
            raise KeyError(f"规则 {rule_name} 已存在，使用 force=True 覆盖")

        # 注册规则
        entry = RuleRegistryEntry(
            rule_class=rule_class,
            category=category,
            priority=priority,
            enabled=enabled,
            description=description
        )
        self._registry[rule_name] = entry

        logger.debug(
            f"注册规则: {rule_name} (category={category.value}, "
            f"priority={priority})"
        )

    def discover_rules_from_module(self, module):
        """
        从模块中自动发现规则类

        Args:
            module: Python 模块对象

        Examples:
            >>> from ..rules import veto
            >>> factory.discover_rules_from_module(veto)
        """
        for name, obj in inspect.getmembers(module):
            if inspect.isclass(obj) and self._is_rule_protocol(obj):
                try:
                    self.register_rule(obj, force=False)
                except KeyError:
                    # 规则已存在，跳过
                    pass

    @staticmethod
    def _is_rule_protocol(obj) -> bool:
        """
        检查对象是否实现 RuleProtocol

        使用鸭子类型检查:
        - 有 name 属性
        - 有 category 属性
        - 有 execute 方法
        """
        if not inspect.isclass(obj):
            return False

        # 检查必需的属性
        if not hasattr(obj, 'name'):
            return False
        if not hasattr(obj, 'category'):
            return False
        if not hasattr(obj, 'execute'):
            return False

        # 检查 execute 方法签名
        execute_method = getattr(obj, 'execute')
        if not callable(execute_method):
            return False

        return True

    def create_veto_rules(self) -> List[RuleProtocol]:
        """
        创建所有否决规则实例

        Returns:
            否决规则列表，按优先级排序
        """
        self._ensure_initialized()
        return self.create_rules_by_category(RuleCategory.VETO)

    def create_penalty_rules(self) -> List[RuleProtocol]:
        """
        创建所有扣分规则实例

        Returns:
            扣分规则列表，按优先级排序
        """
        self._ensure_initialized()
        return self.create_rules_by_category(RuleCategory.PENALTY)

    def create_bonus_rules(self) -> List[RuleProtocol]:
        """
        创建所有加分规则实例

        Returns:
            加分规则列表，按优先级排序
        """
        self._ensure_initialized()
        return self.create_rules_by_category(RuleCategory.BONUS)

    def create_validation_rules(self) -> List[RuleProtocol]:
        """
        创建所有验证规则实例

        Returns:
            验证规则列表，按优先级排序
        """
        self._ensure_initialized()
        return self.create_rules_by_category(RuleCategory.VALIDATION)

    def create_rules_by_category(
        self,
        category: RuleCategory
    ) -> List[RuleProtocol]:
        """
        按类别创建规则实例

        Args:
            category: 规则类别

        Returns:
            规则实例列表，按优先级排序
        """
        rules = []

        for entry in self._registry.values():
            if entry.category == category and entry.enabled:
                try:
                    rule = entry.rule_class()
                    rules.append(rule)
                except Exception as e:
                    logger.error(
                        f"创建规则失败: {entry.rule_class.__name__}, "
                        f"错误: {e}"
                    )

        # 按优先级排序
        rules.sort(key=lambda r: r.priority)

        return rules

    def create_all_rules(self) -> List[RuleProtocol]:
        """
        创建所有规则实例

        Returns:
            所有规则列表，按类别和优先级排序
        """
        self._ensure_initialized()

        return (
            self.create_veto_rules() +
            self.create_penalty_rules() +
            self.create_bonus_rules() +
            self.create_validation_rules()
        )

    def get_rule_by_name(self, name: str) -> Optional[RuleProtocol]:
        """
        根据名称获取规则实例

        Args:
            name: 规则名称

        Returns:
            规则实例，如果不存在返回 None
        """
        self._ensure_initialized()

        entry = self._registry.get(name)
        if entry is None:
            return None

        try:
            return entry.rule_class()
        except Exception as e:
            logger.error(f"创建规则失败: {name}, 错误: {e}")
            return None

    def list_rules(self) -> List[Dict[str, any]]:
        """
        列出所有已注册的规则

        Returns:
            规则信息列表
        """
        self._ensure_initialized()

        result = []
        for name, entry in self._registry.items():
            result.append({
                "name": name,
                "class": entry.rule_class.__name__,
                "category": entry.category.value,
                "priority": entry.priority,
                "enabled": entry.enabled,
                "description": entry.description,
            })

        return sorted(result, key=lambda x: (x['category'], x['priority']))

    def enable_rule(self, name: str):
        """启用规则"""
        if name in self._registry:
            entry = self._registry[name]
            # dataclass 是 frozen 的，需要替换整个对象
            self._registry[name] = RuleRegistryEntry(
                rule_class=entry.rule_class,
                category=entry.category,
                priority=entry.priority,
                enabled=True,
                description=entry.description
            )

    def disable_rule(self, name: str):
        """禁用规则"""
        if name in self._registry:
            entry = self._registry[name]
            self._registry[name] = RuleRegistryEntry(
                rule_class=entry.rule_class,
                category=entry.category,
                priority=entry.priority,
                enabled=False,
                description=entry.description
            )


# ============================================================================
# 全局工厂实例
# ============================================================================

# 全局默认工厂实例
_default_factory: Optional[RuleFactory] = None


def get_default_factory() -> RuleFactory:
    """
    获取全局默认工厂实例

    Returns:
        全局 RuleFactory 实例
    """
    global _default_factory
    if _default_factory is None:
        _default_factory = RuleFactory()
    return _default_factory


def reset_default_factory():
    """重置全局工厂实例 (主要用于测试)"""
    global _default_factory
    _default_factory = None


__all__ = [
    'RuleFactory',
    'RuleRegistryEntry',
    'get_default_factory',
    'reset_default_factory',
]
