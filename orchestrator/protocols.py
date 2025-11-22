"""
AStock Orchestrator Protocols
=============================

Defines the standard protocols (interfaces) for business and data engines.
This ensures that all plugins adhere to a consistent contract, making the system
more robust and professional.
"""

from typing import Protocol, Any, Dict, Optional, runtime_checkable

@runtime_checkable
class BusinessEngineFunction(Protocol):
    """Protocol for business logic functions.

    Expected signature:
    def my_func(data: Any, **kwargs) -> Any: ...
    """
    def __call__(self, data: Any, **kwargs) -> Any:
        ...

@runtime_checkable
class DataEngineFunction(Protocol):
    """Protocol for data processing functions.

    Expected signature:
    def my_func(data: Optional[DataFrame], **kwargs) -> DataFrame: ...
    """
    def __call__(self, data: Any, **kwargs) -> Any:
        ...

@runtime_checkable
class DataHubFunction(Protocol):
    """Protocol for data source functions.

    Expected signature:
    def my_func(ts_code: str, **kwargs) -> DataFrame: ...
    """
    def __call__(self, **kwargs) -> Any:
        ...
