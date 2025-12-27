"""
环境管理 (Environment Management)
==================================

参考设计:
- dynaconf: 多环境支持
- Django: settings 模块

管理开发/测试/生产等多环境配置。
"""
from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import Optional
import os
import threading


class EnvironmentType(str, Enum):
    """环境类型"""
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"

    @classmethod
    def from_string(cls, value: str) -> 'EnvironmentType':
        """从字符串解析"""
        mapping = {
            'dev': cls.DEVELOPMENT,
            'development': cls.DEVELOPMENT,
            'test': cls.TESTING,
            'testing': cls.TESTING,
            'stage': cls.STAGING,
            'staging': cls.STAGING,
            'prod': cls.PRODUCTION,
            'production': cls.PRODUCTION,
        }
        return mapping.get(value.lower(), cls.DEVELOPMENT)


@dataclass
class Environment:
    """环境配置

    Example:
        env = Environment.current()

        if env.is_production:
            # 生产环境逻辑
            pass

        if env.is_debug:
            # 调试逻辑
            pass
    """
    name: EnvironmentType
    debug: bool = False
    testing: bool = False

    @property
    def is_development(self) -> bool:
        return self.name == EnvironmentType.DEVELOPMENT

    @property
    def is_testing(self) -> bool:
        return self.name == EnvironmentType.TESTING or self.testing

    @property
    def is_staging(self) -> bool:
        return self.name == EnvironmentType.STAGING

    @property
    def is_production(self) -> bool:
        return self.name == EnvironmentType.PRODUCTION

    @property
    def is_debug(self) -> bool:
        return self.debug or self.is_development

    @property
    def config_suffix(self) -> str:
        """配置文件后缀"""
        return self.name.value

    @classmethod
    def from_env(cls, env_var: str = "ASTOCK_ENV") -> 'Environment':
        """从环境变量创建"""
        env_name = os.environ.get(env_var, "development")
        env_type = EnvironmentType.from_string(env_name)

        debug = os.environ.get("DEBUG", "").lower() in ("1", "true", "yes")
        testing = os.environ.get("TESTING", "").lower() in ("1", "true", "yes")

        return cls(name=env_type, debug=debug, testing=testing)

    @classmethod
    def development(cls) -> 'Environment':
        return cls(name=EnvironmentType.DEVELOPMENT, debug=True)

    @classmethod
    def testing(cls) -> 'Environment':
        return cls(name=EnvironmentType.TESTING, testing=True)

    @classmethod
    def staging(cls) -> 'Environment':
        return cls(name=EnvironmentType.STAGING)

    @classmethod
    def production(cls) -> 'Environment':
        return cls(name=EnvironmentType.PRODUCTION)


# 全局环境
_current_env: Optional[Environment] = None
_lock = threading.Lock()


def get_environment() -> Environment:
    """获取当前环境"""
    global _current_env

    if _current_env is None:
        with _lock:
            if _current_env is None:
                _current_env = Environment.from_env()

    return _current_env


def set_environment(env: Environment) -> None:
    """设置当前环境"""
    global _current_env

    with _lock:
        _current_env = env


def reset_environment() -> None:
    """重置环境（从环境变量重新读取）"""
    global _current_env

    with _lock:
        _current_env = None


# 便捷函数
def is_development() -> bool:
    return get_environment().is_development


def is_testing() -> bool:
    return get_environment().is_testing


def is_staging() -> bool:
    return get_environment().is_staging


def is_production() -> bool:
    return get_environment().is_production


def is_debug() -> bool:
    return get_environment().is_debug
