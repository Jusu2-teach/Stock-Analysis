"""
工厂模块 (Factories Package)
=============================

提供规则和策略的工厂类。

模块:
- rule_factory: 规则工厂
- strategy_factory: 策略工厂 (TODO)

作者: AStock Analysis System
日期: 2026-01-10
"""

from .rule_factory import (
    RuleFactory,
    RuleRegistryEntry,
    get_default_factory,
    reset_default_factory,
)

__all__ = [
    'RuleFactory',
    'RuleRegistryEntry',
    'get_default_factory',
    'reset_default_factory',
]
