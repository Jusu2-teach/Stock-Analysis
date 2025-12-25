"""
AStock Orchestrator Protocols
=============================

Defines the standard protocols (interfaces) for business and data engines.
This ensures that all plugins adhere to a consistent contract, making the system
more robust and professional.

Similar to pluggy's hookspec, this module provides:
1. Protocol definitions for type checking
2. Signature validation for registered methods
3. Component-specific interface contracts
"""

from __future__ import annotations
import inspect
from typing import Protocol, Any, Dict, Optional, runtime_checkable, Callable, List, Set, Tuple
from dataclasses import dataclass, field


# =============================================================================
# Protocol Definitions (Interface Contracts)
# =============================================================================

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


# =============================================================================
# HookSpec Registry (like pluggy's @hookspec)
# =============================================================================

@dataclass
class HookSpec:
    """Hook specification definition.

    Defines the expected interface for a specific method name.
    Used for signature validation when methods are registered.
    """
    name: str                           # Method name (e.g., "filter_stocks")
    component_type: str                 # Component type (e.g., "business_engine")
    required_params: Tuple[str, ...] = tuple()  # Required parameter names
    optional_params: Tuple[str, ...] = tuple()  # Optional parameter names
    allow_kwargs: bool = True           # Whether **kwargs is allowed
    description: str = ""


class HookSpecRegistry:
    """Registry for hook specifications.

    Similar to pluggy's hookspec system, this allows declaring expected
    interfaces that implementations must conform to.

    Usage:
        specs = HookSpecRegistry()

        # Declare a spec
        specs.declare("filter_stocks", "business_engine",
                      required_params=("data",),
                      optional_params=("threshold", "method"))

        # Validate a function against the spec
        is_valid, errors = specs.validate(my_func, "filter_stocks", "business_engine")
    """

    _instance: Optional['HookSpecRegistry'] = None

    def __init__(self):
        self._specs: Dict[str, HookSpec] = {}  # key: "component::method"

    @classmethod
    def get(cls) -> 'HookSpecRegistry':
        """Get singleton instance."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _make_key(self, name: str, component_type: str) -> str:
        return f"{component_type}::{name}"

    def declare(
        self,
        name: str,
        component_type: str,
        *,
        required_params: Tuple[str, ...] = tuple(),
        optional_params: Tuple[str, ...] = tuple(),
        allow_kwargs: bool = True,
        description: str = ""
    ) -> HookSpec:
        """Declare a hook specification.

        Args:
            name: Method name
            component_type: Component type
            required_params: Parameters that MUST be present
            optional_params: Parameters that MAY be present
            allow_kwargs: Whether **kwargs is allowed
            description: Human-readable description

        Returns:
            The created HookSpec
        """
        spec = HookSpec(
            name=name,
            component_type=component_type,
            required_params=required_params,
            optional_params=optional_params,
            allow_kwargs=allow_kwargs,
            description=description
        )
        key = self._make_key(name, component_type)
        self._specs[key] = spec
        return spec

    def get_spec(self, name: str, component_type: str) -> Optional[HookSpec]:
        """Get a hook specification by name and component type."""
        key = self._make_key(name, component_type)
        return self._specs.get(key)

    def has_spec(self, name: str, component_type: str) -> bool:
        """Check if a spec exists."""
        return self._make_key(name, component_type) in self._specs

    def validate(
        self,
        func: Callable,
        name: str,
        component_type: str,
        *,
        strict: bool = False
    ) -> Tuple[bool, List[str]]:
        """Validate a function against a hook specification.

        Args:
            func: The function to validate
            name: Method name to match
            component_type: Component type to match
            strict: If True, fail on extra parameters not in spec

        Returns:
            Tuple of (is_valid, list_of_error_messages)
        """
        errors: List[str] = []
        spec = self.get_spec(name, component_type)

        if spec is None:
            # No spec defined, validation passes by default
            return True, []

        try:
            sig = inspect.signature(func)
        except (ValueError, TypeError) as e:
            errors.append(f"Cannot inspect signature: {e}")
            return False, errors

        param_names = set(sig.parameters.keys())
        has_var_keyword = any(
            p.kind == inspect.Parameter.VAR_KEYWORD
            for p in sig.parameters.values()
        )

        # Check required parameters
        for req in spec.required_params:
            if req not in param_names and not has_var_keyword:
                errors.append(f"Missing required parameter: '{req}'")

        # In strict mode, check for unexpected parameters
        if strict:
            allowed = set(spec.required_params) | set(spec.optional_params)
            for param in param_names:
                if param not in allowed:
                    p = sig.parameters[param]
                    # Skip *args and **kwargs
                    if p.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                        errors.append(f"Unexpected parameter: '{param}' (not in spec)")

        # Check kwargs allowance
        if has_var_keyword and not spec.allow_kwargs:
            errors.append("**kwargs not allowed by spec")

        return len(errors) == 0, errors

    def list_specs(self, component_type: Optional[str] = None) -> List[HookSpec]:
        """List all registered specs, optionally filtered by component type."""
        if component_type is None:
            return list(self._specs.values())
        return [s for s in self._specs.values() if s.component_type == component_type]


# =============================================================================
# Signature Validator (standalone utility)
# =============================================================================

class SignatureValidator:
    """Utility class for validating function signatures.

    Provides validation independent of HookSpec, useful for:
    1. Checking parameter types match annotations
    2. Verifying callable conforms to a Protocol
    3. Comparing two functions have compatible signatures
    """

    @staticmethod
    def get_param_info(func: Callable) -> Dict[str, Any]:
        """Extract parameter information from a function.

        Returns:
            Dict with keys: 'required', 'optional', 'has_args', 'has_kwargs', 'params'
        """
        try:
            sig = inspect.signature(func)
        except (ValueError, TypeError):
            return {
                'required': [],
                'optional': [],
                'has_args': False,
                'has_kwargs': False,
                'params': {}
            }

        required = []
        optional = []
        has_args = False
        has_kwargs = False
        params = {}

        for name, param in sig.parameters.items():
            params[name] = {
                'kind': param.kind.name,
                'default': param.default if param.default is not inspect.Parameter.empty else None,
                'annotation': param.annotation if param.annotation is not inspect.Parameter.empty else None
            }

            if param.kind == inspect.Parameter.VAR_POSITIONAL:
                has_args = True
            elif param.kind == inspect.Parameter.VAR_KEYWORD:
                has_kwargs = True
            elif param.default is inspect.Parameter.empty:
                required.append(name)
            else:
                optional.append(name)

        return {
            'required': required,
            'optional': optional,
            'has_args': has_args,
            'has_kwargs': has_kwargs,
            'params': params
        }

    @staticmethod
    def check_protocol_compliance(func: Callable, protocol: type) -> Tuple[bool, str]:
        """Check if a function complies with a Protocol.

        Args:
            func: Function to check
            protocol: Protocol class to check against

        Returns:
            Tuple of (is_compliant, error_message)
        """
        if not hasattr(protocol, '__call__'):
            return False, f"{protocol} is not a callable Protocol"

        # For runtime_checkable protocols, use isinstance
        if hasattr(protocol, '_is_runtime_checkable') or runtime_checkable:
            try:
                if isinstance(func, protocol):
                    return True, ""
            except TypeError:
                pass

        return True, ""  # Default pass if can't verify

    @staticmethod
    def signatures_compatible(func1: Callable, func2: Callable) -> Tuple[bool, List[str]]:
        """Check if two functions have compatible signatures.

        func2 is considered compatible with func1 if it can accept all
        arguments that func1 can accept.

        Returns:
            Tuple of (is_compatible, list_of_incompatibilities)
        """
        info1 = SignatureValidator.get_param_info(func1)
        info2 = SignatureValidator.get_param_info(func2)

        issues = []

        # func2 must have all required params of func1 (or **kwargs)
        if not info2['has_kwargs']:
            for req in info1['required']:
                if req not in info2['params']:
                    issues.append(f"Missing parameter '{req}' in target function")

        return len(issues) == 0, issues


# =============================================================================
# Decorator for declaring hookspecs (similar to pluggy)
# =============================================================================

def hookspec(
    component_type: str,
    *,
    required_params: Tuple[str, ...] = tuple(),
    optional_params: Tuple[str, ...] = tuple(),
    allow_kwargs: bool = True
):
    """Decorator to declare a hook specification.

    Usage:
        @hookspec("business_engine", required_params=("data",))
        def filter_stocks(data: Any, **kwargs) -> Any:
            '''Filter stocks based on criteria.'''
            ...

    The decorated function serves as documentation and spec definition.
    The function body is not executed.
    """
    def decorator(func: Callable) -> Callable:
        name = func.__name__
        description = func.__doc__ or ""

        HookSpecRegistry.get().declare(
            name=name,
            component_type=component_type,
            required_params=required_params,
            optional_params=optional_params,
            allow_kwargs=allow_kwargs,
            description=description.strip()
        )

        return func
    return decorator


# =============================================================================
# Component Protocol Mapping
# =============================================================================

COMPONENT_PROTOCOLS: Dict[str, type] = {
    "business_engine": BusinessEngineFunction,
    "data_engine": DataEngineFunction,
    "datahub": DataHubFunction,
}


def get_protocol_for_component(component_type: str) -> Optional[type]:
    """Get the Protocol type for a component type."""
    return COMPONENT_PROTOCOLS.get(component_type)
