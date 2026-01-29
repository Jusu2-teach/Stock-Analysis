"""
Pipeline Core - Immutable specifications and mutable runtime state.

Components:
    - spec.py: Immutable pipeline/task definitions (FlowSpec, TaskSpec)
    - run.py: Mutable runtime state (FlowRun, TaskRun)
    - state.py: State machines (TaskState, FlowState)
    - policy.py: Retry and failure policies (RetryPolicy)
    - dag.py: Dependency graph (DAG)
    - container.py: Dependency injection container
    - middleware.py: Generic middleware infrastructure
"""

from pipeline.core.spec import FlowSpec, TaskSpec, TaskInputSpec, TaskOutputSpec
from pipeline.core.run import FlowRun, TaskRun
from pipeline.core.state import TaskState, FlowState, TaskStateMachine, FlowStateMachine
from pipeline.core.policy import RetryPolicy
from pipeline.core.dag import DAG
from pipeline.core.container import Container, Lifecycle, Scope as DIScope
from pipeline.core.middleware import (
    MiddlewareBase,
    MiddlewareChain,
    BaseContext,
    MiddlewareAction,
    FunctionMiddleware,
    middleware,
    create_chain,
)

__all__ = [
    # Spec
    "FlowSpec",
    "TaskSpec",
    "TaskInputSpec",
    "TaskOutputSpec",
    # Run
    "FlowRun",
    "TaskRun",
    # State
    "TaskState",
    "FlowState",
    "TaskStateMachine",
    "FlowStateMachine",
    # Policy
    "RetryPolicy",
    # DAG
    "DAG",
    # Container (DI)
    "Container",
    "Lifecycle",
    "DIScope",
    # Middleware
    "MiddlewareBase",
    "MiddlewareChain",
    "BaseContext",
    "MiddlewareAction",
    "FunctionMiddleware",
    "middleware",
    "create_chain",
]
