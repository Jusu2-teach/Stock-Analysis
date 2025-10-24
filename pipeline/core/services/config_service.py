"""ConfigService: 负责配置加载/解析/拓扑/节点构建 (从 ExecuteManager 拆分)

保持无状态核心算法 + 轻状态引用 (通过 manager 访问共享数据结构)，便于后续单元测试。
"""
from __future__ import annotations
from typing import Any, Dict, List, Set, TYPE_CHECKING
from collections import defaultdict, deque
import yaml
import hashlib
import re
from dataclasses import dataclass, field


@dataclass
class StepOutput:
    name: str
    source_key: str | None = None
    global_key: str | None = None


@dataclass
class StepSpec:
    name: str
    component: str
    engine: str
    methods: List[str]
    raw_parameters: Dict[str, Any] = field(default_factory=dict)
    outputs: List[StepOutput] = field(default_factory=list)


if TYPE_CHECKING:  # 避免运行时循环引用
    from pipeline.core.execute_manager import ExecuteManager

class ConfigService:
    REF_PATTERN = re.compile(r"^steps\.(?P<step>[^.]+)\.outputs\.parameters\.(?P<param>[^.]+)$")

    def __init__(self, manager: 'ExecuteManager'):
        self.mgr = manager
        self.logger = manager.logger

    # ---- public orchestrated methods ----
    def load_config(self, path: str) -> Dict[str, Any]:
        with open(path, 'r', encoding='utf-8') as f:
            self.mgr.config = yaml.safe_load(f)
        self.logger.info(f"🧾 已加载配置: {path}")
        self._parse_steps()
        self._compute_execution_order()
        return self.mgr.config

    # ---- internal pieces (ported) ----
    def _parse_steps(self):
        mgr = self.mgr
        mgr.steps.clear()
        pipeline = mgr.config.get('pipeline', {}) if mgr.config else {}
        raw_steps = pipeline.get('steps') or mgr.config.get('steps')
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
            mgr.steps[name] = spec

    def _mark_references(self, params: Dict[str, Any]) -> Dict[str, Any]:
        def walk(val):
            if isinstance(val, str):
                m = self.REF_PATTERN.match(val.strip())
                if m:
                    ref = val.strip()
                    ghash = self._hash_reference(ref)
                    self.mgr.reference_to_hash.setdefault(ref, ghash)
                    return {"__ref__": ref, "hash": ghash}
                return val
            if isinstance(val, list):
                return [walk(v) for v in val]
            if isinstance(val, dict):
                return {k: walk(v) for k, v in val.items()}
            return val
        return {k: walk(v) for k, v in params.items()}

    def _hash_reference(self, ref: str) -> str:
        return hashlib.md5(ref.encode('utf-8')).hexdigest()[:16]

    def _compute_execution_order(self):
        mgr = self.mgr
        deps: Dict[str, Set[str]] = defaultdict(set)
        for name, spec in mgr.steps.items():
            for pval in spec.raw_parameters.values():
                for ref in self._extract_refs(pval):
                    m = self.REF_PATTERN.match(ref)
                    if m:
                        deps[name].add(m.group('step'))
        in_degree = {name: 0 for name in mgr.steps}
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
        if len(order) != len(mgr.steps):
            missing = set(mgr.steps) - set(order)
            raise ValueError(f"检测到循环或缺失依赖: {missing}")
        mgr.execution_order = order
        self.logger.info(f"🧭 执行顺序: {order}")

    def _extract_refs(self, val) -> List[str]:
        refs = []
        if isinstance(val, dict) and '__ref__' in val:
            refs.append(val['__ref__'])
        elif isinstance(val, list):
            for v in val:
                refs.extend(self._extract_refs(v))
        elif isinstance(val, dict):  # second dict case retained for symmetry
            for v in val.values():
                refs.extend(self._extract_refs(v))
        return refs

    def build_auto_nodes(self) -> Dict[str, Any]:
        mgr = self.mgr
        auto_nodes = []
        for step_name in mgr.execution_order:
            spec = mgr.steps[step_name]
            resolved_params = dict(spec.raw_parameters)
            # 已移除自动输入推断，保持原始参数
            node_outs = [mgr._dataset_name(spec.name, o.name) for o in spec.outputs]
            # 模式5推进: 为每个方法创建 MethodHandle（engine=auto -> 延迟；显式 engine -> fixed）
            engine_val = spec.engine
            handles = []
            from pipeline.core.handles.method_handle import MethodHandle  # 局部导入避免循环
            if not spec.methods:
                raise ValueError(f"step 未提供 methods: {spec.name}")
            for mname in spec.methods:
                if engine_val == 'auto':
                    h = MethodHandle(spec.component, mname, prefer='auto')
                    handles.append(h)
                else:
                    # 显式引擎 -> 固定
                    h = MethodHandle(spec.component, mname, prefer='fixed', fixed_engine=engine_val)
                    handles.append(h)
            # node-level engine 字段: 保持兼容（非 auto 时写原值；auto 用占位符）
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
            inputs = []
            for pval in spec.raw_parameters.values():
                for ref in self._extract_refs(pval):
                    m = self.REF_PATTERN.match(ref)
                    if m:
                        ds_in = mgr._dataset_name(m.group('step'), m.group('param'))
                        if ds_in not in inputs:
                            inputs.append(ds_in)
            if inputs:
                node_cfg['inputs'] = inputs
            auto_nodes.append(node_cfg)
        mgr.config.setdefault('pipeline', {}).setdefault('kedro_pipelines', {})['__auto__'] = {
            'description': 'auto-generated from steps list',
            'nodes': auto_nodes
        }
        return {'nodes': auto_nodes}

__all__ = ["ConfigService", "StepSpec", "StepOutput"]
