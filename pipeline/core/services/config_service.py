"""ConfigService: 负责配置加载/解析/拓扑/节点构建

职责：
1. 加载和解析 YAML 配置文件
2. 构建 StepSpec 规范对象
3. 使用 DependencyGraph 计算执行顺序
4. 生成 Kedro 兼容的节点配置

设计原则：
- 单一职责：只负责配置解析，不执行任何业务逻辑
- 依赖反转：通过 PipelineContext 共享状态
- 开闭原则：通过 DependencySource 扩展依赖解析

重构为依赖 PipelineContext 而非 ExecuteManager，降低耦合。
"""
from __future__ import annotations
from typing import Any, Dict, List, Set
from collections import defaultdict
import yaml
import hashlib
import logging

from ..context import PipelineContext, StepSpec, StepOutput
from ..runtime_models import RetryPolicy, FailurePolicy, FailureStrategy
from ..dependency_graph import (
    DependencyGraph,
    DependencyType,
    DependencySource,
    DependencyEdge,
    ExecutionPlan,
    CyclicDependencyError,
    # ✅ 使用统一的依赖源实现（不再重复定义）
    DataDependencySource,
    ExplicitDependencySource,
)


# ============================================================================
# 🔄 重构说明：
# StepDataDependencySource 和 StepExplicitDependencySource 已移除。
# 现在直接使用 dependency_graph.py 中的 DataDependencySource 和
# ExplicitDependencySource，它们的实现是完全等价的。
# 引用解析统一通过 PipelineContext.resolver (ReferenceResolver) 完成。
# ============================================================================


class ConfigService:
    """配置服务（专业级实现）

    通过 PipelineContext 访问共享状态，使用 DependencyGraph 管理依赖。

    核心流程：
    1. load_config() -> 解析 YAML
    2. _parse_steps() -> 构建 StepSpec
    3. _build_dependency_graph() -> 创建依赖图
    4. _compute_execution_order() -> 拓扑排序
    """

    __slots__ = ('ctx', 'logger', '_dependency_graph')

    def __init__(self, context: PipelineContext, logger: logging.Logger | None = None):
        self.ctx = context
        self.logger = logger or logging.getLogger(__name__)
        self._dependency_graph: DependencyGraph | None = None

    @property
    def dependency_graph(self) -> DependencyGraph | None:
        """获取依赖图（只读访问）"""
        return self._dependency_graph

    # ========== Public API ==========

    def load_config(self, path: str) -> Dict[str, Any]:
        """加载并解析配置文件

        Args:
            path: YAML 配置文件路径

        Returns:
            解析后的配置字典
        """
        with open(path, 'r', encoding='utf-8') as f:
            self.ctx.config = yaml.safe_load(f)
        self.logger.info(f"🧾 已加载配置: {path}")

        self._parse_steps()
        self._build_dependency_graph()
        self._compute_execution_order()

        return self.ctx.config

    def get_execution_plan(self) -> ExecutionPlan:
        """获取执行计划

        Returns:
            ExecutionPlan 实例，包含层次信息和关键路径
        """
        if self._dependency_graph is None:
            raise RuntimeError("依赖图未初始化，请先调用 load_config()")
        return self._dependency_graph.build_execution_plan()

    # ========== Internal: Step Parsing ==========

    def _parse_steps(self):
        """解析配置中的步骤定义"""
        self.ctx.steps.clear()
        pipeline = self.ctx.config.get('pipeline', {}) if self.ctx.config else {}
        raw_steps = pipeline.get('steps') or self.ctx.config.get('steps')
        if not isinstance(raw_steps, list):
            raise ValueError("配置中 pipeline.steps 必须为列表")

        # 流程级默认策略（可被 step 覆盖）
        orchestration_cfg = pipeline.get('orchestration', {}) or {}
        default_retry = self._parse_retry_policy(orchestration_cfg.get('retry'))
        default_failure = self._parse_failure_policy(orchestration_cfg.get('failure'))

        # 预扫描引用
        referenced_map: Dict[str, Set[str]] = defaultdict(set)

        def collect_refs(val: Any):
            if isinstance(val, str):
                # 利用 PipelineContext 的 resolver 模式解析步骤输出引用
                parsed = self.ctx.resolver.parse_ref(val.strip()) or {}
                step = parsed.get('step')
                param = parsed.get('param')
                if step and param:
                    referenced_map[step].add(param)
            elif isinstance(val, list):
                for x in val:
                    collect_refs(x)
            elif isinstance(val, dict):
                for v in val.values():
                    collect_refs(v)

        for raw in raw_steps:
            if not isinstance(raw, dict):
                continue
            cand_params = {}
            if 'arguments' in raw and isinstance(raw['arguments'], dict):
                cand_params.update(raw['arguments'].get('parameters', {}) or {})
            cand_params.update(raw.get('parameters', {}) or {})
            for v in cand_params.values():
                collect_refs(v)

        for idx, raw in enumerate(raw_steps):
            if not isinstance(raw, dict):
                continue
            name = raw.get('name') or f"step_{idx}"
            component = raw['component']
            # 引擎可省略或写 auto -> 由 orchestrator 动态解析
            engine = raw.get('engine', 'auto') or 'auto'
            methods = raw.get('method', [])
            if isinstance(methods, str):
                methods = [methods]
            params = {}
            if 'arguments' in raw and isinstance(raw['arguments'], dict):
                params.update(raw['arguments'].get('parameters', {}) or {})
            params.update(raw.get('parameters', {}) or {})

            outputs: List[StepOutput] = []
            out_section = raw.get('outputs', {})
            param_outputs = out_section.get('parameters', []) if isinstance(out_section, dict) else []
            if isinstance(param_outputs, list):
                for item in param_outputs:
                    if isinstance(item, dict):
                        outputs.append(StepOutput(name=str(item['name']), source_key=item.get('from')))
                    elif isinstance(item, str):
                        outputs.append(StepOutput(name=item))
            elif isinstance(param_outputs, dict):
                for k, v in param_outputs.items():
                    if isinstance(v, dict):
                        outputs.append(StepOutput(name=k, source_key=v.get('from')))
                    else:
                        outputs.append(StepOutput(name=k))

            if not outputs and name in referenced_map:
                auto_outputs = [StepOutput(name=p) for p in sorted(referenced_map[name])]
                outputs.extend(auto_outputs)
                self.logger.info(f"🧩 自动补全隐式 outputs: step={name} -> {[o.name for o in auto_outputs]}")

            # 解析显式依赖声明
            explicit_deps = raw.get('depends_on', [])
            if isinstance(explicit_deps, str):
                explicit_deps = [explicit_deps]

            # per-step 策略（覆盖流程级）
            step_retry_cfg = raw.get('retry')
            step_failure_cfg = raw.get('failure')
            retry_policy = self._parse_retry_policy(step_retry_cfg, default=default_retry)
            failure_policy = self._parse_failure_policy(step_failure_cfg, default=default_failure)

            spec = StepSpec(
                name=name,
                component=component,
                engine=engine,
                methods=methods,
                raw_parameters=self._mark_references(params),
                outputs=outputs,
                depends_on=explicit_deps,
                retry_policy=retry_policy,
                failure_policy=failure_policy,
            )
            self.ctx.steps[name] = spec

    def _mark_references(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """标记参数中的引用

        v3.0: 不再写入 reference_to_hash 字典，哈希值在 DataStore 中自动计算。
        通过 PipelineContext.resolver 识别步骤输出引用模式，
        同时为 config.* / env.* 提供语法糖。
        """

        def walk(val):
            if isinstance(val, str):
                ref = val.strip()

                # 1) steps.* 引用：使用明确的前缀和路径结构，而不是依赖路由器
                #    这里的目标只是“标记这是一个可解析引用”，真正的解析
                #    仍然交给 PipelineContext.resolve_references + DataStore。
                #    规范格式: steps.{step}.outputs.parameters.{param}
                if ref.startswith('steps.'):
                    return {"__ref__": ref}

                # 2) config.* / env.*: 直接按前缀识别并打上 __ref__，
                #    实际解析逻辑在 PipelineContext.resolve_references 中完成。
                if ref.startswith('config.') or ref.startswith('env.'):
                    return {"__ref__": ref}

                # 其它字符串原样返回
                return val
            if isinstance(val, list):
                return [walk(v) for v in val]
            if isinstance(val, dict):
                return {k: walk(v) for k, v in val.items()}
            return val

        return {k: walk(v) for k, v in params.items()}

    # ========== Internal: Policy Parsing ==========+

    def _parse_retry_policy(self, cfg: Any, default: RetryPolicy | None = None) -> RetryPolicy | None:
        """解析重试策略配置

        支持格式：
        - None: 使用默认策略
        - int: 仅指定 max_attempts
        - dict: 显式字段
        """
        if cfg is None:
            return default
        if isinstance(cfg, int):
            if cfg <= 0:
                return default
            return RetryPolicy(max_attempts=cfg)
        if isinstance(cfg, dict):
            return RetryPolicy(
                max_attempts=int(cfg.get('max_attempts', default.max_attempts if default else 1)),
                delay_seconds=float(cfg.get('delay_seconds', default.delay_seconds if default else 0.0)),
                backoff_multiplier=float(cfg.get('backoff_multiplier', default.backoff_multiplier if default else 1.0)),
                jitter_seconds=float(cfg.get('jitter_seconds', default.jitter_seconds if default else 0.0)),
            )
        # 其它类型: 回退到默认
        return default

    def _parse_failure_policy(self, cfg: Any, default: FailurePolicy | None = None) -> FailurePolicy | None:
        """解析失败策略配置

        支持格式：
        - None: 使用默认策略
        - str: 策略枚举别名
        - dict: {strategy, notify_events}
        """
        if cfg is None:
            return default

        def _to_strategy(name: str) -> FailureStrategy:
            key = (name or '').lower()
            if key in {'fail', 'fail_pipeline'}:
                return FailureStrategy.FAIL_PIPELINE
            if key in {'mark_skipped', 'skip', 'skipped'}:
                return FailureStrategy.MARK_SKIPPED
            if key in {'continue', 'keep_going'}:
                return FailureStrategy.CONTINUE
            return default.strategy if default else FailureStrategy.FAIL_PIPELINE

        if isinstance(cfg, str):
            return FailurePolicy(strategy=_to_strategy(cfg), notify_events=(default.notify_events if default else []))

        if isinstance(cfg, dict):
            strategy_name = cfg.get('strategy') or (default.strategy.name if default else 'fail_pipeline')
            events = cfg.get('notify_events') or (default.notify_events if default else [])
            if not isinstance(events, list):
                events = [str(events)]
            return FailurePolicy(strategy=_to_strategy(strategy_name), notify_events=[str(e) for e in events])

        return default

    def _hash_reference(self, ref: str) -> str:
        """生成引用的哈希值"""
        return hashlib.md5(ref.encode('utf-8')).hexdigest()[:16]

    # ========== Internal: Dependency Graph ==========

    def _build_dependency_graph(self) -> None:
        """构建依赖图

        使用专业的 DependencyGraph 类管理依赖关系。
        这是单一职责：依赖图只负责依赖建模和拓扑排序。
        """
        # 将 StepSpec 转换为节点配置格式（用于 DependencySource）
        node_configs = {}
        for name, spec in self.ctx.steps.items():
            # 收集数据集输入
            inputs: List[str] = []
            for pval in spec.raw_parameters.values():
                for ref in self._extract_refs(pval):
                    ref_str = str(ref).strip()
                    # 仅处理标准的 steps.* 引用，避免误伤其它引用类型
                    if not ref_str.startswith('steps.'):
                        continue

                    # 解析规范路径: steps.{step}.outputs.parameters.{param}
                    parts = ref_str.split('.')
                    if len(parts) >= 5 and parts[0] == 'steps' and parts[2] == 'outputs' and parts[3] == 'parameters':
                        step = parts[1]
                        param = parts[4]
                        ds_name = self.ctx.dataset_name(step, param)
                        inputs.append(ds_name)

            # 收集数据集输出
            outputs = [self.ctx.dataset_name(name, o.name) for o in spec.outputs]

            node_configs[name] = {
                'inputs': inputs,
                'outputs': outputs,
                'depends_on': spec.depends_on,
            }

        # 使用依赖源策略创建依赖图（使用统一的 DependencySource 实现）
        self._dependency_graph = DependencyGraph.from_node_configs(
            node_configs,
            sources=[
                DataDependencySource(),      # ✅ 使用统一实现
                ExplicitDependencySource(),  # ✅ 使用统一实现
            ],
            logger=self.logger
        )

        # ✅ 将依赖图存储到上下文中（供其他组件复用，避免重复构建）
        self.ctx.set_dependency_graph(self._dependency_graph)

        # 记录显式依赖（便于调试）
        for name, spec in self.ctx.steps.items():
            if spec.depends_on:
                self.logger.info(f"📌 显式依赖: {name} -> {spec.depends_on}")

    def _compute_execution_order(self) -> None:
        """计算步骤执行顺序

        使用 DependencyGraph 的拓扑排序功能，提供：
        - 循环依赖检测
        - 层次化执行计划
        - 关键路径分析
        """
        if self._dependency_graph is None:
            raise RuntimeError("依赖图未初始化，请先调用 _build_dependency_graph()")

        try:
            plan = self._dependency_graph.build_execution_plan()
            self.ctx.execution_order = plan.flatten()

            # ✅ 使用专用方法存储执行计划（供 Prefect Engine 使用）
            self.ctx.set_execution_plan(plan)

            self.logger.info(f"🧭 执行顺序: {self.ctx.execution_order}")
            self.logger.info(f"📊 执行计划: {plan.depth} 层, 最大并行度 {plan.max_parallelism}")

            if plan.critical_path:
                self.logger.debug(f"🔥 关键路径: {' -> '.join(plan.critical_path)}")

        except CyclicDependencyError as e:
            raise ValueError(f"检测到循环依赖: {e.cycle}") from e

    def _extract_refs(self, val) -> List[str]:
        """递归提取引用标记"""
        refs = []
        if isinstance(val, dict):
            if '__ref__' in val:
                refs.append(val['__ref__'])
            else:
                for v in val.values():
                    refs.extend(self._extract_refs(v))
        elif isinstance(val, list):
            for v in val:
                refs.extend(self._extract_refs(v))
        return refs

    # ========== Internal: Node Config Building ==========

    def build_auto_nodes(self) -> Dict[str, Any]:
        """构建自动节点配置"""
        auto_nodes = []

        for step_name in self.ctx.execution_order:
            spec = self.ctx.steps[step_name]
            resolved_params = dict(spec.raw_parameters)
            node_outs = [self.ctx.dataset_name(spec.name, o.name) for o in spec.outputs]

            # 使用工厂方法创建 MethodHandle（避免循环导入）
            engine_val = spec.engine
            handles = []

            if not spec.methods:
                raise ValueError(f"step 未提供 methods: {spec.name}")

            # 导入工厂方法（接口层，无循环依赖）
            from pipeline.core.protocols import create_method_handle

            for mname in spec.methods:
                if engine_val == 'auto':
                    h = create_method_handle(spec.component, mname, prefer='auto')
                    handles.append(h)
                else:
                    # 显式引擎 -> 固定
                    h = create_method_handle(spec.component, mname, prefer='fixed', fixed_engine=engine_val)
                    handles.append(h)

            # node-level engine 字段
            if engine_val == 'auto':
                engine_val = '<handle:auto>'
                self.logger.info(f"🧷 延迟绑定引擎(多方法支持): step={spec.name} methods={spec.methods}")

            node_cfg = {
                'name': spec.name,
                'component': spec.component,
                'engine': engine_val,
                'method': spec.methods if len(spec.methods) > 1 else spec.methods[0],
                'parameters': resolved_params,
                'outputs': node_outs,
                'primary_output': node_outs[0] if node_outs else None
            }

            if handles:
                node_cfg['handles'] = handles

            # 收集输入依赖
            inputs: List[str] = []
            for pval in spec.raw_parameters.values():
                for ref in self._extract_refs(pval):
                    ref_str = str(ref).strip()
                    if not ref_str.startswith('steps.'):
                        continue

                    parts = ref_str.split('.')
                    if len(parts) >= 5 and parts[0] == 'steps' and parts[2] == 'outputs' and parts[3] == 'parameters':
                        step = parts[1]
                        param = parts[4]
                        ds_in = self.ctx.dataset_name(step, param)
                        if ds_in not in inputs:
                            inputs.append(ds_in)
            if inputs:
                node_cfg['inputs'] = inputs

            # 添加显式依赖（用于 Prefect Engine 拓扑排序）
            if spec.depends_on:
                node_cfg['depends_on'] = spec.depends_on

            auto_nodes.append(node_cfg)

        # 更新配置
        self.ctx.config.setdefault('pipeline', {}).setdefault('kedro_pipelines', {})['__auto__'] = {
            'description': 'auto-generated from steps list',
            'nodes': auto_nodes
        }

        return {'nodes': auto_nodes}


__all__ = ["ConfigService"]
