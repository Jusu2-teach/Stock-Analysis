"""
Pipeline 错误 (Pipeline Errors)
================================

工作流引擎相关的错误定义。
"""
from typing import Any, Dict, List, Optional

from .base import AStockError
from .codes import ErrorCode


class PipelineError(AStockError):
    """Pipeline 基础错误"""
    default_code = ErrorCode.PIPELINE_STEP_FAILED


class StepExecutionError(PipelineError):
    """步骤执行错误

    当工作流步骤执行失败时抛出。
    """
    default_code = ErrorCode.PIPELINE_STEP_FAILED

    def __init__(
        self,
        step_name: str,
        *,
        step_index: Optional[int] = None,
        error_message: str = "",
        execution_time: Optional[float] = None,
        parameters: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        message = f"Step '{step_name}' execution failed"
        if error_message:
            message += f": {error_message}"

        super().__init__(message, **kwargs)

        self.with_context(
            step_name=step_name,
            step_index=step_index,
            error_message=error_message,
            execution_time=execution_time,
            parameters=parameters,
        )


class WorkflowDefinitionError(PipelineError):
    """工作流定义错误

    当 YAML 工作流定义有问题时抛出。
    """
    default_code = ErrorCode.PIPELINE_INVALID_DEFINITION

    def __init__(
        self,
        workflow_name: str,
        *,
        reason: str = "",
        location: Optional[str] = None,
        **kwargs
    ):
        message = f"Invalid workflow definition '{workflow_name}'"
        if reason:
            message += f": {reason}"
        if location:
            message += f" at {location}"

        super().__init__(message, **kwargs)

        self.with_context(
            workflow_name=workflow_name,
            reason=reason,
            location=location,
        )


class DependencyResolutionError(PipelineError):
    """依赖解析错误

    当步骤依赖无法解析时抛出。
    """
    default_code = ErrorCode.PIPELINE_DEPENDENCY_ERROR

    def __init__(
        self,
        step_name: str,
        *,
        missing_dependency: Optional[str] = None,
        circular_deps: Optional[List[str]] = None,
        available_outputs: Optional[List[str]] = None,
        **kwargs
    ):
        if circular_deps:
            message = f"Circular dependency detected in step '{step_name}': {' -> '.join(circular_deps)}"
        elif missing_dependency:
            message = f"Step '{step_name}' has unresolved dependency: '{missing_dependency}'"
        else:
            message = f"Dependency resolution failed for step '{step_name}'"

        super().__init__(message, **kwargs)

        self.with_context(
            step_name=step_name,
            missing_dependency=missing_dependency,
            circular_deps=circular_deps,
            available_outputs=available_outputs,
        )


class ParameterResolutionError(PipelineError):
    """参数解析错误

    当 steps.X.outputs.parameters.Y 引用解析失败时抛出。
    """
    default_code = ErrorCode.PIPELINE_PARAMETER_MISSING

    def __init__(
        self,
        step_name: str,
        parameter_ref: str,
        *,
        reason: str = "",
        available_refs: Optional[List[str]] = None,
        **kwargs
    ):
        message = f"Failed to resolve parameter '{parameter_ref}' in step '{step_name}'"
        if reason:
            message += f": {reason}"

        super().__init__(message, **kwargs)

        self.with_context(
            step_name=step_name,
            parameter_ref=parameter_ref,
            reason=reason,
            available_refs=available_refs,
        )


class PipelineTimeoutError(PipelineError):
    """Pipeline 超时错误"""
    default_code = ErrorCode.PIPELINE_TIMEOUT

    def __init__(
        self,
        step_name: str,
        timeout_seconds: float,
        *,
        elapsed_time: Optional[float] = None,
        **kwargs
    ):
        message = f"Step '{step_name}' timed out after {timeout_seconds}s"
        if elapsed_time:
            message += f" (elapsed: {elapsed_time:.2f}s)"

        super().__init__(message, **kwargs)

        self.with_context(
            step_name=step_name,
            timeout_seconds=timeout_seconds,
            elapsed_time=elapsed_time,
        )


class StateCheckpointError(PipelineError):
    """状态检查点错误

    断点续传相关的错误。
    """
    default_code = ErrorCode.PIPELINE_STATE_CHECKPOINT_FAILED

    def __init__(
        self,
        checkpoint_path: str,
        *,
        operation: str = "load",  # "load" or "save"
        reason: str = "",
        **kwargs
    ):
        message = f"Failed to {operation} checkpoint '{checkpoint_path}'"
        if reason:
            message += f": {reason}"

        super().__init__(message, **kwargs)

        self.with_context(
            checkpoint_path=checkpoint_path,
            operation=operation,
            reason=reason,
        )


class OutputCollectionError(PipelineError):
    """输出收集错误"""
    default_code = ErrorCode.PIPELINE_OUTPUT_COLLECTION_FAILED

    def __init__(
        self,
        step_name: str,
        output_name: str,
        *,
        expected_type: Optional[str] = None,
        actual_type: Optional[str] = None,
        **kwargs
    ):
        message = f"Failed to collect output '{output_name}' from step '{step_name}'"
        if expected_type and actual_type:
            message += f" (expected {expected_type}, got {actual_type})"

        super().__init__(message, **kwargs)

        self.with_context(
            step_name=step_name,
            output_name=output_name,
            expected_type=expected_type,
            actual_type=actual_type,
        )
