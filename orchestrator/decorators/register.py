from __future__ import annotations
import inspect
import logging
import os
from typing import Optional, List, Tuple
from ..models import MethodRegistration
from ..protocols import (
    HookSpecRegistry,
    SignatureValidator,
    get_protocol_for_component
)

logger = logging.getLogger(__name__)


def _validate_against_spec(
    func,
    component_type: str,
    engine_name: str
) -> Tuple[bool, List[str]]:
    """Validate function against HookSpec if one exists."""
    spec_registry = HookSpecRegistry.get()

    # Check if there's a spec for this method
    if spec_registry.has_spec(engine_name, component_type):
        strict = os.getenv('ASTOCK_STRICT_SPEC', 'false').lower() == 'true'
        return spec_registry.validate(func, engine_name, component_type, strict=strict)

    return True, []


def _validate_against_protocol(func, component_type: str) -> Tuple[bool, str]:
    """Validate function against Protocol if one exists for component type."""
    protocol = get_protocol_for_component(component_type)
    if protocol is None:
        return True, ""

    return SignatureValidator.check_protocol_compliance(func, protocol)


def register_method(
    component_type: str,
    engine_type: str,
    engine_name: Optional[str] = None,
    version: str = "1.0.0",
    priority: int = 0,
    deprecated: bool = False,
    tags: Optional[List[str]] = None,
    description: str = "",
    *,
    validate_spec: bool = True,
    validate_protocol: bool = True
):
    """
    Decorator to register a method with the Orchestrator Registry.

    Args:
        component_type: Logical component category (datahub/data_engine/business_engine)
        engine_type: Specific implementation type (pandas/polars/tushare)
        engine_name: Exposed method name (defaults to function name)
        version: Semantic version string
        priority: Dispatch priority (higher = preferred)
        deprecated: Whether this method is deprecated
        tags: Business/technical tags
        description: Human-readable description
        validate_spec: Whether to validate against HookSpec (default: True)
        validate_protocol: Whether to validate against Protocol (default: True)

    Validation Behavior:
        - If ASTOCK_VALIDATION_MODE=strict: Validation failures raise errors
        - If ASTOCK_VALIDATION_MODE=warn (default): Validation failures log warnings
        - If ASTOCK_VALIDATION_MODE=off: Skip all validation
    """
    def decorator(func):
        # Lazy import to avoid circular dependency
        from ..registry.registry import Registry

        nonlocal engine_name, description
        if engine_name is None:
            engine_name = func.__name__
        if not description:
            description = func.__doc__ or ""

        # Get validation mode from env
        validation_mode = os.getenv('ASTOCK_VALIDATION_MODE', 'warn').lower()

        if validation_mode != 'off':
            validation_errors = []

            # 1. Validate against HookSpec
            if validate_spec:
                is_valid, spec_errors = _validate_against_spec(func, component_type, engine_name)
                if not is_valid:
                    validation_errors.extend([f"[HookSpec] {e}" for e in spec_errors])

            # 2. Validate against Protocol
            if validate_protocol:
                is_valid, protocol_error = _validate_against_protocol(func, component_type)
                if not is_valid:
                    validation_errors.append(f"[Protocol] {protocol_error}")

            # Handle validation results
            if validation_errors:
                msg = f"Validation errors for {component_type}::{engine_type}::{engine_name}:\n  " + "\n  ".join(validation_errors)

                if validation_mode == 'strict':
                    from ..errors import RegistryValidationError
                    raise RegistryValidationError(msg)
                else:  # warn mode
                    logger.warning(msg)

        module_path = func.__module__
        try:
            sig = str(inspect.signature(func))
        except ValueError:
            sig = "()"

        reg = MethodRegistration(
            component_type=component_type,
            engine_type=engine_type,
            engine_name=engine_name,
            description=description.strip(),
            version=version,
            callable=func,
            tags=tuple(tags or []),
            deprecated=deprecated,
            priority=priority,
            signature=sig,
            module_path=module_path
        )

        Registry.get().register(reg)
        return func
    return decorator
