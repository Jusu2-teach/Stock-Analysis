"""ConfigService: 负责配置加载/解析/拓扑/节点构建

重构为依赖 PipelineContext 而非 ExecuteManager，降低耦合。
"""
from __future__ import annotations
from typing import Any, Dict, List, Set
from collections import defaultdict, deque
import yaml
import hashlib
import re
import logging

from ..context import PipelineContext, StepSpec, StepOutput


class ConfigService:
    """配置服务（解耦版本）

    通过 PipelineContext 访问共享状态，而非直接依赖 ExecuteManager。
    """

    __slots__ = ('ctx', 'logger')

    REF_PATTERN = re.compile(r"^steps\.(?P<step>[^.]+)\.outputs\.parameters\.(?P<param>[^.]+)$")

    def __init__(self, context: PipelineContext, logger: logging.Logger | None = None):
        self.ctx = context
        self.logger = logger or logging.getLogger(__name__)

    # ---- public orchestrated methods ----
    def load_config(self, path: str) -> Dict[str, Any]:
        """加载并解析配置文件"""
        with open(path, 'r', encoding='utf-8') as f:
            self.ctx.config = yaml.safe_load(f)
        self.logger.info(f"🧾 已加载配置: {path}")
        self._parse_steps()
        self._compute_execution_order()
        return self.ctx.config

    # ---- internal pieces ----
    def _parse_steps(self):
        """解析配置中的步骤定义"""
        self.ctx.steps.clear()
        pipeline = self.ctx.config.get('pipeline', {}) if self.ctx.config else {}
        raw_steps = pipeline.get('steps') or self.ctx.config.get('steps')
        if not isinstance(raw_steps, list):
            raise ValueError("配置中 pipeline.steps 必须为列表")
        # 预扫描引用
        referenced_map: Dict[str, Set[str]] = defaultdict(set)

        def collect_refs(val: Any):
            if isinstance(val, str):
                m = self.REF_PATTERN.match(val.strip())
                if m:
                    referenced_map[m.group('step')].add(m.group('param'))
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

            spec = StepSpec(
                name=name,
                component=component,
                engine=engine,
                methods=methods,
                raw_parameters=self._mark_references(params),
                outputs=outputs
            )
            self.ctx.steps[name] = spec

    def _mark_references(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """标记参数中的引用"""
        def walk(val):
            if isinstance(val, str):
                m = self.REF_PATTERN.match(val.strip())
                if m:
                    ref = val.strip()
                    ghash = self._hash_reference(ref)
                    self.ctx.reference_to_hash.setdefault(ref, ghash)
                    return {"__ref__": ref, "hash": ghash}
                return val
            if isinstance(val, list):
                return [walk(v) for v in val]
            if isinstance(val, dict):
                return {k: walk(v) for k, v in val.items()}
            return val
        return {k: walk(v) for k, v in params.items()}

    def _hash_reference(self, ref: str) -> str:
        """生成引用的哈希值"""
        return hashlib.md5(ref.encode('utf-8')).hexdigest()[:16]

    def _compute_execution_order(self):
        """计算步骤执行顺序（拓扑排序）"""
        deps: Dict[str, Set[str]] = defaultdict(set)
        for name, spec in self.ctx.steps.items():
            for pval in spec.raw_parameters.values():
                for ref in self._extract_refs(pval):
                    m = self.REF_PATTERN.match(ref)
                    if m:
                        deps[name].add(m.group('step'))

        in_degree = {name: 0 for name in self.ctx.steps}
        for name, pres in deps.items():
            for pre in pres:
                if pre in in_degree:
                    in_degree[name] += 1

        queue = deque([n for n, d in in_degree.items() if d == 0])
        order: List[str] = []
        while queue:
            cur = queue.popleft()
            order.append(cur)
            for succ, pres in deps.items():
                if cur in pres:
                    in_degree[succ] -= 1
                    if in_degree[succ] == 0:
                        queue.append(succ)

        if len(order) != len(self.ctx.steps):
            missing = set(self.ctx.steps) - set(order)
            raise ValueError(f"检测到循环或缺失依赖: {missing}")

        self.ctx.execution_order = order
        self.logger.info(f"🧭 执行顺序: {order}")

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
            inputs = []
            for pval in spec.raw_parameters.values():
                for ref in self._extract_refs(pval):
                    m = self.REF_PATTERN.match(ref)
                    if m:
                        ds_in = self.ctx.dataset_name(m.group('step'), m.group('param'))
                        if ds_in not in inputs:
                            inputs.append(ds_in)
            if inputs:
                node_cfg['inputs'] = inputs

            auto_nodes.append(node_cfg)

        # 更新配置
        self.ctx.config.setdefault('pipeline', {}).setdefault('kedro_pipelines', {})['__auto__'] = {
            'description': 'auto-generated from steps list',
            'nodes': auto_nodes
        }

        return {'nodes': auto_nodes}


__all__ = ["ConfigService"]
