import inspect
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Callable, Tuple, Union, Protocol

import pandas as pd


class CacheStrategy(Protocol):
    def hit(self, outputs: List[str], global_catalog: Dict[str, Any]) -> bool: ...
    def record(self, outputs: List[str], produced: Dict[str, Any], global_catalog: Dict[str, Any]): ...


class SimplePresenceCache:
    """最简单缓存：所有计划输出存在即视为命中"""
    def hit(self, outputs: List[str], global_catalog: Dict[str, Any]) -> bool:
        return bool(outputs) and all(o in global_catalog for o in outputs)
    def record(self, outputs: List[str], produced: Dict[str, Any], global_catalog: Dict[str, Any]):
        for k, v in produced.items():
            global_catalog[k] = v


@dataclass
class InputSpec:
    name: str
    alias: Optional[str] = None
    required: bool = False
    kind: str = "dataset"  # dataset | param | artifact | model


@dataclass
class OutputSpec:
    name: str
    source_key: Optional[str] = None  # 用于 dict 重映射
    primary: bool = False
    kind: str = "dataset"


@dataclass
class NodeIOConfig:
    step_name: str
    inputs: List[InputSpec] = field(default_factory=list)
    outputs: List[OutputSpec] = field(default_factory=list)
    primary_output: Optional[str] = None
    strict_schema: bool = False
    cache_strategy: CacheStrategy = field(default_factory=SimplePresenceCache)


@dataclass
class ResolvedInputs:
    ordered: List[Any] = field(default_factory=list)         # 位置参数顺序
    mapping: Dict[str, Any] = field(default_factory=dict)    # 按名称映射
    aggregated: List[Any] = field(default_factory=list)      # 自动聚合 inputs
    aggregated_map: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CapturedOutputs:
    produced: Dict[str, Any] = field(default_factory=dict)
    primary_output: Optional[str] = None
    tuple_result: Tuple[Any, ...] = tuple()


class IOManager:
    """集中式 I/O 解析与绑定管理器

    目标：
    1. 抽离硬编码的多输入/多输出处理逻辑
    2. 统一支持 Argo 风格的 *参数* 与 *数据集/工件* 区分（预留）
    3. 支持多种返回类型：单对象 / tuple / dict
    4. primary_output 裁剪策略集中管理
    5. 未来可插拔策略：命名规范、动态选择器、Jinja 模板、缓存策略、物化层插件
    """

    def __init__(self, global_catalog: Dict[str, Any], logger, strict_pipeline: bool = False):
        self.global_catalog = global_catalog
        self.logger = logger
        self.strict_pipeline = strict_pipeline

    # ----------------------------------------------------------------------------
    # 构建配置对象（从原始 node_config 提取）
    # ----------------------------------------------------------------------------
    def build_config(self, node_config: Dict[str, Any]) -> NodeIOConfig:
        step_name = node_config.get('name') or node_config.get('id') or 'unknown_step'

        raw_inputs = node_config.get('inputs') or []
        input_specs: List[InputSpec] = []
        for item in raw_inputs:
            if isinstance(item, dict):
                input_specs.append(InputSpec(
                    name=item.get('name'),
                    alias=item.get('alias'),
                    required=bool(item.get('required', False)),
                    kind=item.get('kind', 'dataset')
                ))
            else:
                input_specs.append(InputSpec(name=item))

        raw_outputs = node_config.get('outputs') or []
        output_specs: List[OutputSpec] = []
        # 兼容列表 + 含字典 + 合成 parameter 数据集 (name: <step>__param__<param>)
        for item in raw_outputs:
            if isinstance(item, dict):
                name = item.get('name')
                kind = item.get('kind', 'dataset')
                src = item.get('source_key') or item.get('from')
                output_specs.append(OutputSpec(name=name, source_key=src, primary=False, kind=kind))
            else:
                name = item
                kind = 'dataset'
                if isinstance(name, str) and '__param__' in name:
                    kind = 'parameter'
                output_specs.append(OutputSpec(name=name, primary=False, kind=kind))
        # primary 解析
        primary_decl = node_config.get('primary_output')
        if primary_decl:
            for spec in output_specs:
                spec.primary = (spec.name == primary_decl)
        else:
            # 选择第一个 dataset 输出为 primary
            for spec in output_specs:
                if spec.kind == 'dataset':
                    spec.primary = True
                    break
        primary = next((s.name for s in output_specs if s.primary), None)

        cfg = NodeIOConfig(
            step_name=step_name,
            inputs=input_specs,
            outputs=output_specs,
            primary_output=primary,
            strict_schema=False
        )
        return cfg

    # ----------------------------------------------------------------------------
    # 输入解析：根据上游传入 *args (Kedro runtime) 与 node_config 描述构造 ResolvedInputs
    # ----------------------------------------------------------------------------
    def resolve_inputs(self, cfg: NodeIOConfig, raw_args: Tuple[Any, ...]) -> ResolvedInputs:
        resolved = ResolvedInputs()
        declared_names = [i.alias or i.name for i in cfg.inputs]
        for idx, val in enumerate(raw_args):
            key = declared_names[idx] if idx < len(declared_names) else f"_arg{idx}"
            resolved.mapping[key] = val
            resolved.ordered.append(val)
        if len(resolved.mapping) > 1:
            resolved.aggregated = list(resolved.mapping.values())
            resolved.aggregated_map = dict(resolved.mapping)
        elif len(resolved.mapping) == 1:
            pass
        return resolved

    # ----------------------------------------------------------------------------
    # 参数绑定：根据函数签名和已解析输入生成最终调用参数
    # ----------------------------------------------------------------------------
    def bind_call_params(self, callable_obj: Callable, base_params: Dict[str, Any], resolved: ResolvedInputs, previous_result: Any = None) -> Dict[str, Any]:
        target = self._unwrap(callable_obj)
        sig = inspect.signature(target)
        params = dict(base_params)
        strict_mode = (str(self.strict_pipeline).lower() == 'true') or (str(
            # 允许通过环境变量显式开启严格模式 (不注入任何隐式别名)
            __import__('os').getenv('ASTOCK_STRICT_PARAMS', '0')
        ) == '1')

        # 链式结果传递（仅限多方法链场景）:
        # 旧逻辑: 注入 data/df/dataset 三种别名并 fallback data
        # 新逻辑: 只在非严格模式下，且 YAML 未显式提供相应参数时，尝试“智能推断一个最可能的单一形参”再注入；
        # 严格模式: 完全不自动注入，必须显式在 YAML 写出。
        if previous_result is not None and not strict_mode:
            # 找尚未提供且无默认值的 POSITIONAL_OR_KEYWORD 形参集合
            candidate_params = [
                (n, p) for n, p in sig.parameters.items()
                if n not in params
                and p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
            ]
            # 若只有 1 个未赋值且无默认值的参数 -> 注入链式结果
            filtered = [c for c in candidate_params if c[1].default is c[1].empty]
            if len(filtered) == 1:
                pname = filtered[0][0]
                params[pname] = previous_result
            # 否则若显式存在 "data" 参数名尚未提供，常见约定优先
            elif 'data' in sig.parameters and 'data' not in params:
                params['data'] = previous_result
            # 其它情况：不注入，保持显式策略
        # 严格模式下 previous_result 完全不注入；用户需在 YAML 用引用显式传递

        # 按名称匹配 declared inputs
        for name, val in resolved.mapping.items():
            if name in sig.parameters and name not in params:
                params[name] = val

        # 移除: 单输入别名(data/df/dataset)自动注入。显式即一切。

        # 多输入：提供聚合 forms
        if resolved.aggregated and 'inputs' in sig.parameters:
            params.setdefault('inputs', resolved.aggregated)
        if resolved.aggregated_map and 'inputs_map' in sig.parameters:
            params.setdefault('inputs_map', resolved.aggregated_map)

        # 如果函数接受 **kwargs 则允许附加；否则裁剪
        has_var_kw = any(p.kind == p.VAR_KEYWORD for p in sig.parameters.values())
        if not has_var_kw:
            params = {k: v for k, v in params.items() if k in sig.parameters}
        return params

    # ----------------------------------------------------------------------------
    # 输出捕获：根据 node_config 规范化 raw_result (dict/tuple/单值)
    # ----------------------------------------------------------------------------
    def capture_outputs(self, cfg: NodeIOConfig, raw_result: Any) -> CapturedOutputs:
        cap = CapturedOutputs(primary_output=cfg.primary_output)
        output_names = [o.name for o in cfg.outputs]
        if not output_names:
            return cap
        # dict 结果：进行键映射（适配 dataset + parameter）
        if isinstance(raw_result, dict) and len(output_names) > 0:
            mapped_vals: List[Any] = []
            raw_keys = list(raw_result.keys())
            used_keys: set = set()

            def infer_key(out_nm: str) -> Optional[str]:
                # 1) 明确映射表
                # legacy output_key_map 已移除
                # 2) 同名
                if out_nm in raw_result:
                    return out_nm
                # 3) 去除常见语义后缀再匹配
                suffixes = ["full", "only", "part", "data", "df", "dataset", "stats", "main"]
                base = out_nm
                for _ in range(2):
                    if '_' in base:
                        tail = base.rsplit('_', 1)[-1]
                        if tail in suffixes:
                            base = base.rsplit('_', 1)[0]
                        else:
                            break
                if base != out_nm and base in raw_result:
                    return base
                # 4) 回退：按未使用顺序挑选
                for k in raw_keys:
                    if k not in used_keys:
                        return k
                return None

            for spec in cfg.outputs:
                out_name = spec.name
                # 优先级：spec.source_key > output_key_map > 推断
                if spec.source_key and spec.source_key in raw_result:
                    source_key = spec.source_key
                elif False:  # 保留占位，output_key_map 已删除
                    source_key = None
                else:
                    source_key = infer_key(out_name)

                val = raw_result.get(source_key) if source_key else None
                if val is None and (not source_key or source_key not in raw_result):
                    self.logger.warning(
                        f"🔑 自动映射未找到合适键 -> 输出 {out_name} (推断源: {source_key}) 使用 None; 原始可用键: {raw_keys}")
                else:
                    used_keys.add(source_key)

                cap.produced[out_name] = val
                # 仅 dataset 输出进入 tuple_result，parameter 不参与 Kedro node output 序列
                if spec.kind == 'dataset':
                    mapped_vals.append(val)
            cap.tuple_result = tuple(mapped_vals) if mapped_vals else tuple()

        # 序列结果（list/tuple）映射到声明的多个输出
        elif isinstance(raw_result, (list, tuple)) and len(output_names) > 1:
            mapped_vals: List[Any] = []
            for idx, out_name in enumerate(output_names):
                if idx < len(raw_result):
                    val = raw_result[idx]
                else:
                    self.logger.warning(f"🔢 多输出位置 {idx} 不存在，使用 None")
                    val = None
                cap.produced[out_name] = val
                # 仍然只将 dataset 输出组装到 tuple_result
                spec_kind = next((s.kind for s in cfg.outputs if s.name == out_name), 'dataset')
                if spec_kind == 'dataset':
                    mapped_vals.append(val)
            cap.tuple_result = tuple(mapped_vals)

        else:
            # 单输出：整个 raw_result 赋给第一个（可能是 dataset 或 parameter）
            first = output_names[0]
            cap.produced[first] = raw_result
            if any(s.kind == 'dataset' for s in cfg.outputs):
                # 仅当存在 dataset 输出时才构造 tuple_result 供 Kedro 使用
                ds_first = next((s.name for s in cfg.outputs if s.kind == 'dataset'), first)
                # 若 first 不是 dataset，且存在 dataset 输出，tuple_result 取该 dataset 的值（此时找不到则 None）
                val_for_tuple = cap.produced.get(ds_first) if ds_first == first else None
                cap.tuple_result = (val_for_tuple,) if ds_first else tuple()
            else:
                cap.tuple_result = tuple()  # 全是 parameter 时返回空 tuple 给 Kedro
        return cap

    # ----------------------------------------------------------------------------
    # 摘要工具
    # ----------------------------------------------------------------------------
    def summarize(self, name: str, obj: Any) -> Dict[str, Any]:
        summary = {'name': name, 'type': type(obj).__name__}
        try:
            if isinstance(obj, pd.DataFrame):
                summary.update({'rows': int(len(obj)), 'cols': int(len(obj.columns)), 'columns_sample': obj.columns[:10].tolist()})
            elif isinstance(obj, (list, tuple)):
                summary['length'] = len(obj)
            elif isinstance(obj, dict):
                summary['keys'] = list(obj.keys())[:15]
        except Exception as e:
            summary['error'] = f'summary_failed: {e}'
        return summary

    # primary_policy 功能已删除：不再需要运行期输入裁剪

    # --- Prefect external input ingestion ---
    def ingest_prefect_inputs(self, step: str, declared_inputs: List[str], task_inputs: Dict[str, Any], logger):
        if not declared_inputs:
            return
        for name in declared_inputs:
            if name in task_inputs:
                self.global_catalog[name] = task_inputs[name]
            else:
                if name not in self.global_catalog:
                    logger.warning(f"[IOManager][Prefect] Step '{step}' 缺失声明输入且全局不存在: {name}")
                else:
                    logger.info(f"[IOManager][Prefect] Step '{step}' 使用全局缓存输入: {name}")

    # ----------------------------------------------------------------------------
    # Helper
    # ----------------------------------------------------------------------------
    def _unwrap(self, func: Callable) -> Callable:
        original = func
        depth = 0
        while hasattr(original, '__wrapped__') and depth < 10:
            original = getattr(original, '__wrapped__')
            depth += 1
        return original

    # ---------------- Schema & 输出 Key 校验提取 ----------------
    def validate_input_schema(self, schema_meta: Dict[str, Any], call_params: Dict[str, Any], strict: bool):
        if not schema_meta or not schema_meta.get('required_columns'):
            return
        required_cols = schema_meta.get('required_columns') or []
        candidate_dfs: List[pd.DataFrame] = []
        for v in call_params.values():
            if isinstance(v, pd.DataFrame):
                candidate_dfs.append(v)
            elif isinstance(v, (list, tuple)) and v and all(isinstance(x, pd.DataFrame) for x in v):
                candidate_dfs.extend(v)
        uniq = []
        seen = set()
        for df in candidate_dfs:
            oid = id(df)
            if oid not in seen:
                uniq.append(df)
                seen.add(oid)
        if not uniq and required_cols and strict:
            raise ValueError("Schema严格模式: 未找到可校验DataFrame")
        missing_any = []
        for df in uniq[:2]:
            miss = [c for c in required_cols if c not in df.columns]
            if miss:
                missing_any.extend(miss)
        if missing_any:
            msg = f"Schema缺列: {sorted(set(missing_any))} (期望:{required_cols})"
            if strict:
                raise ValueError(msg)
            else:
                self.logger.warning(msg)

    def validate_output_schema(self, schema_meta: Dict[str, Any], result: Any, strict: bool, method_name: str):
        if not schema_meta or not schema_meta.get('output_keys') or not isinstance(result, dict):
            return
        expected_keys = schema_meta.get('output_keys') or []
        missing = [k for k in expected_keys if k not in result]
        if missing:
            msg = f"Schema输出缺失({method_name}): {missing} (期望:{expected_keys})"
            if strict:
                raise ValueError(msg)
            else:
                self.logger.warning(msg)


# 未来扩展点占位：命名策略 / 变量渲染 / 分区数据集 / 版本控制 / 缓存策略注册表
## (已移除占位) IONamingStrategy 删除：精简代码基线
