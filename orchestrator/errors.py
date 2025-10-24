class RegistryError(Exception):
    """基础注册中心异常"""


class RegistryMethodNotFound(RegistryError):
    pass


class RegistryExecutionError(RegistryError):
    pass


class RegistryConflictError(RegistryError):
    pass


class RegistryStrategyError(RegistryError):
    pass
