"""Pipeline Execution - Task Executor
=====================================

任务执行器，负责执行单个任务。

设计原则：
- 单一职责：只负责执行任务
- 中间件驱动：横切关注点通过中间件处理
- 依赖注入：通过 Container 获取依赖
- 协议驱动：MethodResolverProtocol 由外部注入
- 可测试：所有依赖可 mock

架构约束：
- Pipeline 只定义 MethodResolverProtocol 接口
- 实现由 orchestrator/adapters 提供 (RegistryMethodResolver)
- 应用入口层负责 DI 绑定

版本: 2.0.0
"""

from __future__ import annotations

import logging
import inspect
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

from ..core.run import TaskRun
from ..core.state import TaskState
from ..core.container import Container, get_container
from ..catalog import DataCatalog
from ..events import EventBus, TaskEvents
from .middleware import ExecutionMiddlewareChain, MiddlewareContext
from ..protocols import MethodResolverProtocol, MethodInfo
from ..aggregation import Collector as AggregationCollector
from ..aggregation.inject import Injector
from ..cache.backends import CacheBackend
from ..cache.router import CacheBackendRouter

logger = logging.getLogger(__name__)


# =============================================================================
# 任务执行器
# =============================================================================

@dataclass
class ExecutorConfig:
    """执行器配置"""
    emit_events: bool = True
    collect_aggregation: bool = True
    validate_inputs: bool = True


class TaskExecutor:
    """任务执行器

    执行单个任务，处理输入/输出、中间件和事件。

    架构说明：
        method_resolver 必须通过 DI 注入，Pipeline 不提供默认实现。
        生产环境使用 orchestrator.adapters.RegistryMethodResolver。

    Example:
        from orchestrator.adapters import RegistryMethodResolver

        resolver = RegistryMethodResolver()
        executor = TaskExecutor(
            container=get_container(),
            method_resolver=resolver,
        )
        result = executor.execute(task_run)
    """

    def __init__(
        self,
        container: Container,
        method_resolver: MethodResolverProtocol,
        middleware_chain: Optional[ExecutionMiddlewareChain] = None,
        config: Optional[ExecutorConfig] = None,
    ):
        """初始化 TaskExecutor

        Args:
            container: 依赖注入容器 (必须)
            method_resolver: 方法解析器 (必须) - 由应用层注入
            middleware_chain: 中间件链 (可选)
            config: 配置 (可选)

        Raises:
            TypeError: 如果 method_resolver 未提供或不符合协议
        """
        if method_resolver is None:
            raise TypeError(
                "method_resolver is required. "
                "Use 'from orchestrator.adapters import RegistryMethodResolver' to get the production implementation."
            )

        self._container = container
        self._config = config or ExecutorConfig()
        self._method_resolver = method_resolver

        # Runner 级开关：是否在缓存命中时跳过执行（默认 True）
        self._skip_cached: bool = True

        # 默认中间件链：自动接入容器中的 CacheBackend（如已注册）
        if middleware_chain is None:
            cache_router: Optional[CacheBackendRouter] = self._container.try_resolve(CacheBackendRouter)
            cache_backend: Optional[CacheBackend] = None

            # 兼容：如果没有注册 router，则使用旧的单一 CacheBackend
            if cache_router is None:
                cache_backend = self._container.try_resolve(CacheBackend)

            self._middleware = ExecutionMiddlewareChain.default(
                cache_backend=cache_backend,
                cache_router=cache_router,
            )
        else:
            self._middleware = middleware_chain

        # 从 Container 解析 Singleton 服务
        self._catalog = self._container.resolve(DataCatalog)
        self._event_bus = self._container.resolve(EventBus)
        self._aggregator = self._container.resolve(AggregationCollector)

        # 当前聚合作用域 (由 FlowRunner 设置)
        self._current_scope = None

    def set_scope(self, scope) -> None:
        """设置当前聚合作用域"""
        self._current_scope = scope

    def set_skip_cached(self, skip_cached: bool) -> None:
        """设置是否跳过缓存命中任务。

        skip_cached=False 表示：即使缓存命中也执行任务（但仍可写回缓存）。
        """
        self._skip_cached = bool(skip_cached)

    def execute(self, task_run: TaskRun, flow_run_id: str = "") -> Any:
        """执行任务

        Args:
            task_run: 任务运行时状态
            flow_run_id: 流程运行 ID (用于事件)

        Returns:
            任务执行结果

        Raises:
            ValueError: 如果方法未找到
            Exception: 任务执行中的异常
        """
        spec = task_run.spec

        # 1. 解析方法 (通过协议)
        method_info = self._method_resolver.resolve(
            spec.component,
            spec.engine,
            spec.method,
        )

        if method_info is None:
            raise ValueError(
                f"Method not found: {spec.component}.{spec.engine}.{spec.method}. "
                f"Check if the method is registered or use 'python -m pipeline engines' to list available methods."
            )

        # 获取可调用对象
        method = method_info.callable

        # 2. 准备输入
        inputs = self._prepare_inputs(task_run)

        # 2.1 聚合注入（基于签名推断；不依赖 YAML 显式配置）
        if self._current_scope is not None:
            strict_injection = bool(self._config.validate_inputs)
            try:
                injected = Injector(self._current_scope, strict=strict_injection).prepare_injection(
                    method,
                    extra_kwargs=inputs,
                )
                inputs.update(injected)
            except Exception:
                if strict_injection:
                    raise
                # 非严格模式下：注入失败不阻断任务，继续执行

        # 2.2 兼容：若任务显式配置了 aggregation policy，则按 policy 注入
        # 但只在方法能接收该参数（或有 **kwargs）时才注入，避免 TypeError。
        aggregation_policy = spec.policies.aggregation
        if aggregation_policy.inject_as_consumer and self._current_scope:
            try:
                sig = inspect.signature(method)
                accepts_kwargs = any(
                    p.kind is inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()
                )
                if accepts_kwargs or aggregation_policy.consumer_param_name in sig.parameters:
                    aggregated = self._aggregator.prepare_consumer_inputs(
                        consumer_task=spec.name,
                        param_name=aggregation_policy.consumer_param_name,
                        namespace=aggregation_policy.namespace,
                        scope=self._current_scope,
                    )
                    inputs.update(aggregated)
            except Exception:
                # 聚合注入失败不阻断任务，继续执行
                logger.debug(f"Aggregation injection failed for task '{spec.name}', continuing without it")

        # 3. 发布开始事件
        if self._config.emit_events and self._event_bus:
            self._event_bus.emit(TaskEvents.started(
                task_id=f"{flow_run_id}:{spec.name}",
                task_name=spec.name,
                flow_id=flow_run_id,
            ))

        # 4. 创建中间件上下文
        ctx = MiddlewareContext(
            task_run=task_run,
            inputs=inputs,
            callable=method,
            skip_cached=self._skip_cached,  # P2/P3: 通过显式字段传递，而非 metadata
        )

        # 5. 标记开始
        task_run.mark_started()

        # 6. 执行中间件链
        try:
            self._middleware.execute(ctx)

            # 7. 处理结果
            result = ctx.result

            # 收集聚合数据
            if self._config.collect_aggregation and self._current_scope:
                self._aggregator.collect_from_task_result(
                    spec.name, result, scope=self._current_scope
                )

            # 保存输出到 Catalog
            self._save_outputs(task_run, result)

            # 标记成功
            if not task_run.cached:  # 缓存命中已经标记过了
                task_run.mark_success(result)

            # 发布完成事件
            if self._config.emit_events and self._event_bus:
                self._event_bus.emit(TaskEvents.completed(
                    task_id=f"{flow_run_id}:{spec.name}",
                    task_name=spec.name,
                    duration_ms=task_run.duration_ms or 0,
                ))

            return result

        except Exception as e:
            # 标记失败
            import traceback
            task_run.mark_failed(str(e), traceback.format_exc())

            # 发布失败事件
            if self._config.emit_events and self._event_bus:
                self._event_bus.emit(TaskEvents.failed(
                    task_id=f"{flow_run_id}:{spec.name}",
                    task_name=spec.name,
                    error=str(e),
                ))

            raise

    def _prepare_inputs(self, task_run: TaskRun) -> Dict[str, Any]:
        """准备任务输入"""
        spec = task_run.spec
        inputs = {}

        # 1. 添加静态参数 (解析引用)
        for key, value in spec.parameters.items():
            resolved = self._resolve_parameter_value(value)
            inputs[key] = resolved

        # 2. 解析数据引用 (inputs 配置)
        for inp in spec.inputs:
            if inp.source:
                # 从 Catalog 加载
                value = self._catalog.load(self._resolve_source(inp.source))
                if value is not None:
                    inputs[inp.name] = value
                elif inp.required and self._config.validate_inputs:
                    raise ValueError(f"Required input not found: {inp.source}")
                elif inp.required:
                    logger.warning(f"Required input not found: {inp.source}")
            elif inp.default is not None:
                inputs[inp.name] = inp.default

        # 记录实际输入
        task_run.inputs = inputs

        return inputs

    def _resolve_parameter_value(
        self, value: Any, _depth: int = 0, _max_depth: int = 50
    ) -> Any:
        """递归解析参数值中的引用

        如果值是 steps.X.outputs.parameters.Y 格式的引用，从 Catalog 加载。
        支持嵌套的 dict 和 list。

        Args:
            value: 待解析的值
            _depth: 当前递归深度（内部使用）
            _max_depth: 最大递归深度，防止恶意/错误配置导致栈溢出

        Raises:
            RecursionError: 当递归深度超过限制时
        """
        # P7: 添加递归深度限制，防止恶意/错误的深嵌套配置
        if _depth > _max_depth:
            raise RecursionError(
                f"Parameter resolution exceeded max depth ({_max_depth}). "
                f"Check for circular references or overly nested structures."
            )

        if isinstance(value, str):
            # 检查是否是引用
            if value.startswith('steps.'):
                from ..core.dag import DataReference

                ref = DataReference.parse(value)
                if ref:
                    resolved_key = f"{ref.source_task}.{ref.output_name}"
                    loaded = self._catalog.load(resolved_key)
                    if loaded is not None:
                        return loaded
                    if self._config.validate_inputs:
                        raise ValueError(f"Reference not found in catalog: {value} -> {resolved_key}")
                    logger.debug(f"Reference not found in catalog: {value} -> {resolved_key}")
            return value
        elif isinstance(value, dict):
            return {
                k: self._resolve_parameter_value(v, _depth + 1, _max_depth)
                for k, v in value.items()
            }
        elif isinstance(value, list):
            return [
                self._resolve_parameter_value(v, _depth + 1, _max_depth)
                for v in value
            ]
        return value

    def _resolve_source(self, source: str) -> str:
        """解析数据源引用

        将 YAML 引用格式转换为 Catalog 键。

        Example:
            "steps.load_data.outputs.parameters.raw_data" -> "load_data.raw_data"
        """
        from ..core.dag import DataReference

        ref = DataReference.parse(source)
        if ref:
            return f"{ref.source_task}.{ref.output_name}"

        # 已经是简单键
        return source

    def _save_outputs(self, task_run: TaskRun, result: Any) -> None:
        """保存任务输出到 Catalog

        输出映射策略：
        1. 若未声明 outputs：使用默认键 "{task_name}.result"
        2. 若 result 是 dict 且键与声明的 outputs 匹配：逐项保存
        3. 若 result 是 dict 但键不匹配：保存整体到主输出
        4. 若 result 非 dict：保存到主输出
        """
        spec = task_run.spec

        if not spec.outputs:
            # 没有声明输出，使用默认键
            key = f"{spec.name}.result"
            self._catalog.save(key, result)
            task_run.outputs['result'] = result
            return

        # P5: 简化输出保存逻辑，避免条件分支过深
        primary_output = spec.get_primary_output()

        # 检查 result 是否是 dict 且包含声明的输出键
        if isinstance(result, dict):
            # 计算匹配的输出数量
            matched_outputs = [out for out in spec.outputs if out.name in result]

            if matched_outputs:
                # 有匹配的输出，逐项保存
                for out in matched_outputs:
                    key = f"{spec.name}.{out.name}"
                    self._catalog.save(key, result[out.name])
                    task_run.outputs[out.name] = result[out.name]
                return

        # 未匹配或非 dict：保存整体到主输出
        if primary_output:
            key = f"{spec.name}.{primary_output}"
            self._catalog.save(key, result)
            task_run.outputs[primary_output] = result
