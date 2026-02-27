"""Pipeline Execution - Flow Runner
===================================

流程运行器，负责编排和执行整个流程。

设计原则：
- 层级执行
- 并行支持
- 状态追踪
- 依赖注入 (通过 Container)

版本: 2.0.0
"""

from __future__ import annotations

import logging
import concurrent.futures
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from ..core.spec import FlowSpec
from ..core.run import FlowRun, TaskRun
from ..core.state import TaskState, FlowState
from ..core.dag import DAG, ExecutionLayer
from ..core.container import Container
from ..catalog import DataCatalog
from ..events import EventBus, FlowEvents
from ..core.policy import FailureStrategy
from .executor import TaskExecutor
from ..protocols import MethodResolverProtocol
from ..aggregation import ScopeManager

logger = logging.getLogger(__name__)


# =============================================================================
# Dry Run 结果
# =============================================================================

@dataclass
class DryRunResult:
    """Dry run 验证结果"""
    valid: bool
    flow_name: str
    total_tasks: int
    total_layers: int
    execution_plan: List[List[str]]  # 每层的任务列表
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    unregistered_methods: List[str] = field(default_factory=list)
    missing_dependencies: Dict[str, List[str]] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'valid': self.valid,
            'flow_name': self.flow_name,
            'total_tasks': self.total_tasks,
            'total_layers': self.total_layers,
            'execution_plan': self.execution_plan,
            'warnings': self.warnings,
            'errors': self.errors,
            'unregistered_methods': self.unregistered_methods,
            'missing_dependencies': self.missing_dependencies,
        }


# =============================================================================
# 运行器配置
# =============================================================================

@dataclass
class RunnerConfig:
    """运行器配置"""
    # 说明：RunnerConfig 的值来源存在优先级
    # 1) CLI/代码显式传参（即 RunnerConfig 字段非 None）
    # 2) YAML FlowSpec.orchestration
    # 3) RunnerConfig 内置默认值
    #
    # 为了能区分“未设置”与“显式设置”，这里使用 Optional[...] + None 作为未设置哨兵。

    # 执行模式
    execution_mode: Optional[str] = None  # sequential, parallel
    max_workers: Optional[int] = None

    # 行为控制
    soft_fail: Optional[bool] = None  # 任务失败是否继续执行
    skip_cached: Optional[bool] = None  # 是否跳过已缓存任务
    dry_run: Optional[bool] = None  # 是否只做验证不执行

    # 过滤
    only_tasks: Optional[Set[str]] = None  # 只执行这些任务
    exclude_tasks: Optional[Set[str]] = None  # 排除这些任务
    resume_from: Optional[str] = None  # 从某任务恢复

    # 事件
    emit_events: Optional[bool] = None

    @classmethod
    def defaults(cls) -> 'RunnerConfig':
        """RunnerConfig 内置默认值（最低优先级）。"""
        return cls(
            execution_mode="sequential",
            max_workers=4,
            soft_fail=False,
            skip_cached=True,
            dry_run=False,
            only_tasks=None,
            exclude_tasks=None,
            resume_from=None,
            emit_events=True,
        )


# =============================================================================
# 流程运行器
# =============================================================================

class FlowRunner:
    """流程运行器

    编排和执行整个工作流。

    核心功能：
    - DAG 构建和拓扑排序
    - 层级执行 (支持并行)
    - 失败处理和恢复
    - 状态追踪
    - 依赖注入 (通过 Container)

    Example:
        runner = FlowRunner(container=get_container())
        flow_run = runner.run(flow_spec)

        # Dry run 验证
        result = runner.dry_run(flow_spec)
        if not result.valid:
            print(f"Errors: {result.errors}")
    """

    def __init__(
        self,
        container: Container,
        method_resolver: MethodResolverProtocol,
        config: Optional[RunnerConfig] = None,
    ):
        """初始化 FlowRunner

        Args:
            container: 依赖注入容器 (必须)
            method_resolver: 方法解析器 (必须) - 由应用层注入
            config: 运行器配置 (可选)

        Raises:
            TypeError: 如果 method_resolver 未提供
        """
        if method_resolver is None:
            raise TypeError(
                "method_resolver is required. "
                "Use 'from orchestrator.adapters import RegistryMethodResolver' to get the production implementation."
            )

        self._container = container
        # _user_config: CLI/代码传入配置（最高优先级，字段级别）
        self._user_config = config
        # _config: 生效配置（运行期根据 orchestration + defaults 解析得到）
        self._config = RunnerConfig.defaults()
        self._method_resolver = method_resolver

        # 从 Container 解析 Singleton 服务
        self._catalog = self._container.resolve(DataCatalog)
        self._event_bus = self._container.resolve(EventBus)
        self._scope_manager = self._container.resolve(ScopeManager)

        # Executor 也使用 Container
        self._executor = TaskExecutor(
            container=self._container,
            method_resolver=self._method_resolver,
        )

    def _resolve_effective_config(self, flow_spec: FlowSpec) -> tuple[RunnerConfig, Dict[str, str]]:
        """解析 RunnerConfig 生效值，并返回每个字段的来源。

        优先级（字段级）：
        1) CLI/代码传参（_user_config 非 None 且字段非 None）
        2) YAML orchestration
        3) RunnerConfig.defaults()
        """
        defaults = RunnerConfig.defaults()
        user = self._user_config or RunnerConfig()
        orch = flow_spec.orchestration

        sources: Dict[str, str] = {}

        def pick(field_name: str, yaml_value: Any) -> Any:
            user_value = getattr(user, field_name)
            if user_value is not None:
                sources[field_name] = "cli/code"
                return user_value
            if yaml_value is not None:
                sources[field_name] = "yaml"
                return yaml_value
            sources[field_name] = "default"
            return getattr(defaults, field_name)

        # P8: orchestration -> RunnerConfig 映射（YAML 来源）
        # 支持的 task_runner 值: sequential, threaded, parallel, async
        yaml_execution_mode_map = {
            'sequential': 'sequential',
            'threaded': 'parallel',
            'parallel': 'parallel',  # P8: 添加 parallel 作为 threaded 的别名
        }
        yaml_execution_mode: Optional[str] = None
        if orch.task_runner:
            if orch.task_runner in yaml_execution_mode_map:
                yaml_execution_mode = yaml_execution_mode_map[orch.task_runner]
            elif orch.task_runner == 'async':
                # 当前 Runner 未实现 async runner；保持可用性，降级为串行
                logger.warning(
                    "orchestration.task_runner='async' is not supported yet; "
                    "falling back to sequential"
                )
                yaml_execution_mode = 'sequential'
            else:
                logger.warning(
                    f"Unknown orchestration.task_runner='{orch.task_runner}'; "
                    f"valid values: {list(yaml_execution_mode_map.keys()) + ['async']}. "
                    f"Falling back to sequential."
                )
                yaml_execution_mode = 'sequential'

        yaml_max_workers: Optional[int] = None
        if orch.max_parallelism and orch.max_parallelism > 0:
            yaml_max_workers = orch.max_parallelism

        effective = RunnerConfig(
            execution_mode=pick('execution_mode', yaml_execution_mode),
            max_workers=pick('max_workers', yaml_max_workers),
            soft_fail=pick('soft_fail', orch.soft_fail),
            skip_cached=pick('skip_cached', None),
            dry_run=pick('dry_run', None),
            only_tasks=pick('only_tasks', None),
            exclude_tasks=pick('exclude_tasks', None),
            resume_from=pick('resume_from', None),
            emit_events=pick('emit_events', None),
        )

        return effective, sources

    def _log_effective_config(
        self,
        flow_spec: FlowSpec,
        effective: RunnerConfig,
        sources: Dict[str, str],
    ) -> None:
        """打印最终生效配置（含来源），避免运维期困惑。"""
        # 只在 run/dry_run 入口调用一次；避免噪音。
        logger.info(
            "Runner config resolved: "
            f"execution_mode={effective.execution_mode}({sources.get('execution_mode')}), "
            f"max_workers={effective.max_workers}({sources.get('max_workers')}), "
            f"soft_fail={effective.soft_fail}({sources.get('soft_fail')}), "
            f"skip_cached={effective.skip_cached}({sources.get('skip_cached')}), "
            f"dry_run={effective.dry_run}({sources.get('dry_run')}), "
            f"emit_events={effective.emit_events}({sources.get('emit_events')}), "
            f"only_tasks={'set' if effective.only_tasks else None}({sources.get('only_tasks')}), "
            f"exclude_tasks={'set' if effective.exclude_tasks else None}({sources.get('exclude_tasks')}), "
            f"resume_from={effective.resume_from}({sources.get('resume_from')})"
        )
        logger.info(
            "Runner orchestration (YAML): "
            f"granularity={flow_spec.orchestration.granularity}, "
            f"task_runner={flow_spec.orchestration.task_runner}, "
            f"max_parallelism={flow_spec.orchestration.max_parallelism}, "
            f"soft_fail={flow_spec.orchestration.soft_fail}"
        )

    def dry_run(self, flow_spec: FlowSpec) -> DryRunResult:
        """执行 Dry Run 验证

        验证流程配置的正确性，不实际执行任务。

        验证内容：
        - DAG 构建是否成功 (检测循环依赖)
        - 方法是否已注册
        - 依赖引用是否有效
        - 执行计划是否可行

        Args:
            flow_spec: 流程规范

        Returns:
            DryRunResult: 验证结果
        """
        # 入口统一：解析并记录生效配置
        effective, sources = self._resolve_effective_config(flow_spec)
        self._config = effective
        self._log_effective_config(flow_spec, effective, sources)

        errors: List[str] = []
        warnings: List[str] = []
        unregistered_methods: List[str] = []
        missing_dependencies: Dict[str, List[str]] = {}

        # 1. 尝试构建 DAG
        try:
            dag = DAG.from_flow_spec(flow_spec)
        except Exception as e:
            errors.append(f"DAG build failed: {e}")
            return DryRunResult(
                valid=False,
                flow_name=flow_spec.name,
                total_tasks=len(flow_spec.tasks),
                total_layers=0,
                execution_plan=[],
                errors=errors,
            )

        # 2. 应用过滤
        if self._config.only_tasks or self._config.exclude_tasks:
            try:
                dag = dag.filter_by_selection(
                    only=self._config.only_tasks,
                    exclude=self._config.exclude_tasks,
                    include_upstream=True,
                )
            except Exception as e:
                errors.append(f"Task filter failed: {e}")

        # 3. 获取执行计划
        try:
            plan = dag.get_execution_plan()
            execution_plan = [[task for task in layer.tasks] for layer in plan]
        except Exception as e:
            errors.append(f"Execution plan failed: {e}")
            execution_plan = []

        # 4. 验证方法注册
        for task in flow_spec.tasks:
            methods = task.method if isinstance(task.method, list) else [task.method]
            for method in methods:
                if not self._method_resolver.can_resolve(method):
                    unregistered_methods.append(f"{task.name}: {method}")

        # 5. 验证依赖引用
        task_names = {task.name for task in flow_spec.tasks}
        for task in flow_spec.tasks:
            # 检查参数中的依赖引用
            missing = []
            for key, value in (task.parameters or {}).items():
                if isinstance(value, str) and value.startswith('steps.'):
                    # 解析 steps.{TaskName}.outputs.parameters.{Param}
                    parts = value.split('.')
                    if len(parts) >= 2:
                        ref_task = parts[1]
                        if ref_task not in task_names:
                            missing.append(f"'{key}' references unknown task '{ref_task}'")
            if missing:
                missing_dependencies[task.name] = missing

        # 6. 检查潜在问题
        if unregistered_methods:
            warnings.append(
                f"Found {len(unregistered_methods)} unregistered method(s). "
                f"Run 'python -m pipeline engines' to check available methods."
            )

        if missing_dependencies:
            for task_name, deps in missing_dependencies.items():
                for dep in deps:
                    errors.append(f"Task '{task_name}': {dep}")

        # 7. 构建结果
        valid = len(errors) == 0

        result = DryRunResult(
            valid=valid,
            flow_name=flow_spec.name,
            total_tasks=len(flow_spec.tasks),
            total_layers=len(execution_plan),
            execution_plan=execution_plan,
            warnings=warnings,
            errors=errors,
            unregistered_methods=unregistered_methods,
            missing_dependencies=missing_dependencies,
        )

        # 8. 记录日志
        if valid:
            logger.info(
                f"Dry run PASSED: '{flow_spec.name}' with {result.total_tasks} tasks "
                f"in {result.total_layers} layers"
            )
        else:
            logger.error(
                f"Dry run FAILED: '{flow_spec.name}' with {len(errors)} error(s)"
            )
            for error in errors:
                logger.error(f"  - {error}")

        if warnings:
            for warning in warnings:
                logger.warning(f"  - {warning}")

        return result

    def run(self, flow_spec: FlowSpec) -> FlowRun:
        """执行流程

        Args:
            flow_spec: 流程规范

        Returns:
            流程运行状态
        """
        # 入口统一：解析并记录生效配置（CLI/代码 > YAML orchestration > defaults）
        effective, sources = self._resolve_effective_config(flow_spec)
        self._config = effective
        self._log_effective_config(flow_spec, effective, sources)

        # 0. Dry run 模式
        if self._config.dry_run:
            result = self.dry_run(flow_spec)
            # 创建空的 FlowRun 返回
            flow_run = FlowRun(spec=flow_spec)
            # 使用专用方法设置状态，避免直接访问私有属性
            flow_run.state_machine.set_dry_run_result(result.valid)
            return flow_run

        # 1. 创建 FlowRun
        flow_run = FlowRun(spec=flow_spec)

        # 2. 构建 DAG
        try:
            dag = DAG.from_flow_spec(flow_spec)
        except Exception as e:
            logger.error(f"Failed to build DAG: {e}")
            flow_run.state_machine.set_dry_run_result(False)
            return flow_run

        # 3. 应用过滤
        if self._config.only_tasks or self._config.exclude_tasks:
            dag = dag.filter_by_selection(
                only=self._config.only_tasks,
                exclude=self._config.exclude_tasks,
                include_upstream=True,
            )

        # 3.1 resume_from（严格语义）：只执行 resume_from 及其下游任务。
        # 上游任务被视为“外部已满足”（例如来自外部数据源/缓存/目录），
        # 如果实际缺失，TaskExecutor 的输入解析/验证会在运行时明确失败。
        if self._config.resume_from:
            resume_task = self._config.resume_from
            if resume_task not in dag.tasks:
                logger.error(f"resume_from task '{resume_task}' not found in selected DAG")
                flow_run.state_machine.force_fail(reason=f"resume_from task '{resume_task}' not found")
                flow_run.mark_finished()
                self._print_summary(flow_run)
                return flow_run

            selected = {resume_task}
            selected.update(dag.get_all_downstream(resume_task))
            logger.info(
                f"Resume mode enabled: resume_from='{resume_task}', "
                f"selected {len(selected)}/{len(dag.tasks)} tasks (task + downstream)"
            )

            # 标记未选中的任务为 skipped，避免“沉默不执行”
            for task_name, task_run in flow_run.task_runs.items():
                if task_name not in selected:
                    if task_run.state.is_runnable():
                        task_run.mark_skipped("Not selected (resume_from)")

            dag = dag.subgraph(selected)

        # 4. 获取执行计划
        plan = dag.get_execution_plan()

        logger.info(
            f"Starting flow '{flow_spec.name}' with {plan.total_tasks} tasks "
            f"in {plan.total_layers} layers"
        )

        # 5. 发布开始事件
        if self._config.emit_events and self._event_bus:
            self._event_bus.emit(FlowEvents.started(
                flow_id=flow_run.run_id,
                flow_name=flow_spec.name,
                step_count=plan.total_tasks,
            ))

        # 6. 标记开始
        flow_run.mark_started()

        # 7. 创建聚合作用域并设置到 Executor
        with self._scope_manager.create(flow_id=flow_run.run_id) as scope:
            self._executor.set_scope(scope)
            # Runner -> Executor 行为开关
            self._executor.set_skip_cached(bool(self._config.skip_cached))

            # 8. 执行（按 orchestration.granularity）
            try:
                if flow_spec.orchestration.granularity == 'task':
                    if self._config.execution_mode == 'parallel':
                        logger.warning(
                            "granularity='task' does not support parallel scheduling; running sequentially"
                        )
                    for task_name in dag.get_topological_order():
                        task_run = flow_run.get_task_run(task_name)
                        if task_run is None:
                            continue

                        if not self._check_dependencies(task_name, flow_run, dag):
                            task_run.mark_skipped("Upstream dependency failed")
                            continue

                        self._execute_sequential([task_run], flow_run.run_id)

                        if not self._should_continue(flow_run):
                            break
                else:
                    for layer in plan:
                        self._execute_layer(layer, flow_run, dag)

                        # 检查是否应该继续
                        if not self._should_continue(flow_run):
                            break

                # 9. 完成
                flow_run.mark_finished()

                # 10. 发布完成事件
                if self._config.emit_events and self._event_bus:
                    stats = flow_run.get_statistics()
                    self._event_bus.emit(FlowEvents.completed(
                        flow_id=flow_run.run_id,
                        flow_name=flow_spec.name,
                        duration_ms=flow_run.total_duration_ms or 0,
                    ))

            except Exception as e:
                # 标记流程失败 (使用公开 API，避免直接访问私有属性)
                flow_run.state_machine.force_fail(reason=str(e))
                flow_run.mark_finished()
                logger.error(f"Flow '{flow_spec.name}' failed: {e}", exc_info=True)

            finally:
                # 清理 executor scope
                self._executor.set_scope(None)

        # 11. 打印摘要
        self._print_summary(flow_run)

        return flow_run

    def _execute_layer(
        self,
        layer: ExecutionLayer,
        flow_run: FlowRun,
        dag: DAG,
    ) -> None:
        """执行一层任务"""
        tasks_to_run = []

        for task_name in layer.tasks:
            task_run = flow_run.get_task_run(task_name)
            if task_run is None:
                continue

            # 检查依赖是否满足
            if not self._check_dependencies(task_name, flow_run, dag):
                task_run.mark_skipped("Upstream dependency failed")
                continue

            tasks_to_run.append(task_run)

        if not tasks_to_run:
            return

        logger.debug(f"Executing layer {layer.level}: {[t.name for t in tasks_to_run]}")

        if self._config.execution_mode == "parallel" and len(tasks_to_run) > 1:
            self._execute_parallel(tasks_to_run, flow_run.run_id)
        else:
            self._execute_sequential(tasks_to_run, flow_run.run_id)

    def _execute_sequential(
        self,
        tasks: List[TaskRun],
        flow_run_id: str,
    ) -> None:
        """串行执行任务"""
        for task_run in tasks:
            try:
                self._executor.execute(task_run, flow_run_id)
            except Exception as e:
                if self._config.soft_fail:
                    logger.warning(f"Task {task_run.name} failed (soft_fail=True): {e}")
                    continue

                strategy = task_run.spec.policies.failure.strategy
                if strategy == FailureStrategy.FAIL_FLOW:
                    raise

                # CONTINUE / SKIP_DOWNSTREAM: 继续其他分支；下游会因依赖检查自动跳过
                logger.warning(
                    f"Task {task_run.name} failed (strategy={strategy.name}), continuing: {e}"
                )

    def _execute_parallel(
        self,
        tasks: List[TaskRun],
        flow_run_id: str,
    ) -> None:
        """并行执行任务

        使用 concurrent.futures.wait 配合 FIRST_EXCEPTION 策略，
        确保任务失败时能够快速停止其他任务。
        """
        import threading
        from concurrent.futures import FIRST_EXCEPTION, ALL_COMPLETED

        cancel_event = threading.Event()
        first_exception: Optional[Exception] = None
        exception_lock = threading.Lock()

        def run_with_cancel_check(task_run: TaskRun) -> None:
            """带取消检查的任务执行包装器"""
            nonlocal first_exception

            # 检查是否已取消
            if cancel_event.is_set():
                task_run.mark_skipped("Cancelled due to sibling task failure")
                return

            try:
                self._executor.execute(task_run, flow_run_id)
            except Exception as e:
                # 记录第一个异常（线程安全）
                with exception_lock:
                    if first_exception is None:
                        first_exception = e
                if not self._config.soft_fail:
                    # 仅在 FAIL_FLOW 策略时触发取消
                    if task_run.spec.policies.failure.strategy == FailureStrategy.FAIL_FLOW:
                        cancel_event.set()  # 信号其他任务停止
                raise

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self._config.max_workers
        ) as executor:
            futures = {
                executor.submit(run_with_cancel_check, task_run): task_run
                for task_run in tasks
            }

            # 使用 wait 而不是 as_completed，可以更好地处理异常
            if self._config.soft_fail:
                # 软失败模式: 等待所有任务完成
                done, not_done = concurrent.futures.wait(
                    futures.keys(),
                    return_when=ALL_COMPLETED,
                )
            else:
                # 严格模式: 第一个异常时立即返回
                done, not_done = concurrent.futures.wait(
                    futures.keys(),
                    return_when=FIRST_EXCEPTION,
                )

            # 处理已完成的任务
            for future in done:
                task_run = futures[future]
                try:
                    future.result()
                except Exception as e:
                    if self._config.soft_fail:
                        logger.warning(f"Task {task_run.name} failed (soft_fail=True): {e}")
                        continue

                    strategy = task_run.spec.policies.failure.strategy
                    if strategy != FailureStrategy.FAIL_FLOW:
                        logger.warning(
                            f"Task {task_run.name} failed (strategy={strategy.name}), continuing: {e}"
                        )
                        continue

                    # FAIL_FLOW: 取消尚未完成的任务
                    for f in not_done:
                        f.cancel()
                    # 等待已启动的任务完成（最多等 5 秒）
                    if not_done:
                        concurrent.futures.wait(not_done, timeout=5.0)
                        # 标记被取消的任务
                        for f in not_done:
                            cancelled_task = futures[f]
                            if cancelled_task.state.is_runnable():
                                cancelled_task.mark_skipped("Cancelled due to sibling task failure")

                    # 抛出第一个捕获的异常
                    with exception_lock:
                        if first_exception:
                            raise first_exception
                    raise

    def _check_dependencies(
        self,
        task_name: str,
        flow_run: FlowRun,
        dag: DAG,
    ) -> bool:
        """检查任务依赖是否满足"""
        dependencies = dag.get_dependencies(task_name)

        for dep_name in dependencies:
            dep_run = flow_run.get_task_run(dep_name)
            if dep_run is None:
                return False

            if not dep_run.state.is_success():
                return False

        return True

    def _should_continue(self, flow_run: FlowRun) -> bool:
        """检查是否应该继续执行"""
        if self._config.soft_fail:
            return True

        # 检查是否有失败任务
        for task_run in flow_run.task_runs.values():
            if task_run.state == TaskState.FAILED:
                return False

        return True

    def _print_summary(self, flow_run: FlowRun) -> None:
        """打印执行摘要"""
        stats = flow_run.get_statistics()
        status_emoji = {
            FlowState.SUCCESS: "✓",
            FlowState.FAILED: "✗",
            FlowState.PARTIAL_SUCCESS: "⚠",
        }.get(flow_run.state, "•")

        logger.info(
            f"\n{status_emoji} Flow '{flow_run.name}' completed: {flow_run.state.name}\n"
            f"  Total: {stats['total']} | Success: {stats['success']} | "
            f"Failed: {stats['failed']} | Cached: {stats['cached']} | "
            f"Skipped: {stats['skipped']}\n"
            f"  Duration: {flow_run.total_duration_ms or 0:.1f}ms"
        )

        # 打印失败任务
        failed = flow_run.get_failed_tasks()
        if failed:
            logger.error(f"  Failed tasks: {', '.join(failed)}")
