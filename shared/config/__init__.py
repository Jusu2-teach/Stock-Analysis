"""
AStock 统一配置系统 (Unified Configuration System)
===================================================

集百家之长的配置模块：
- pydantic-settings: 类型安全 + 验证
- dynaconf: 多环境 + 层叠配置
- hydra: 组合式配置
- python-dotenv: 环境变量加载

核心特性:
1. 类型安全 - 自动验证和转换
2. 多环境 - development/staging/production
3. 层叠加载 - defaults → env → file → override
4. 热重载 - 配置变更监听
5. 加密支持 - 敏感配置加密存储

Usage:
    from shared.config import Config, Settings

    # 加载配置
    config = Config.load("config/app.yaml")

    # 类型化设置
    class AppSettings(Settings):
        debug: bool = False
        database_url: str
        api_key: str = Field(env="API_KEY")

    settings = AppSettings()
    print(settings.debug)
"""
__version__ = "1.0.0"

from .core import (
    Config,
    ConfigValue,
    get_config,
    set_config,
)

from .settings import (
    Settings,
    BaseSettings,
    Field,
    SecretStr,
)

from .loaders import (
    ConfigLoader,
    YAMLLoader,
    JSONLoader,
    EnvLoader,
    DotEnvLoader,
    ChainLoader,
)

from .environment import (
    Environment,
    get_environment,
    set_environment,
)

from .validators import (
    validate_config,
    ConfigValidator,
    ValidationResult,
)

from .providers import (
    ConfigProvider,
    FileProvider,
    EnvironmentProvider,
    DefaultProvider,
)

__all__ = [
    # Core
    'Config',
    'ConfigValue',
    'get_config',
    'set_config',

    # Settings
    'Settings',
    'BaseSettings',
    'Field',
    'SecretStr',

    # Loaders
    'ConfigLoader',
    'YAMLLoader',
    'JSONLoader',
    'EnvLoader',
    'DotEnvLoader',
    'ChainLoader',

    # Environment
    'Environment',
    'get_environment',
    'set_environment',

    # Validators
    'validate_config',
    'ConfigValidator',
    'ValidationResult',

    # Providers
    'ConfigProvider',
    'FileProvider',
    'EnvironmentProvider',
    'DefaultProvider',
]