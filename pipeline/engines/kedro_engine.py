import logging
import inspect
from typing import Dict, Any, List, Optional, Callable
import time
import pandas as pd
from pathlib import Path
import json
import pickle
import hashlib
from kedro.pipeline import Pipeline, node
from kedro.io import DataCatalog, MemoryDataset
from kedro.runner import SequentialRunner

from pipeline.io.io_manager import IOManager
from pipeline.core.services.hook_manager import HookManager
import traceback

class KedroEngine:
    def __init__(self, execute_manager):
        self.execute_manager = execute_manager
        self.logger = execute_manager.logger
        self.pipelines = {}
        self.data_catalog = None
        self.global_catalog = {}
        # Phase3: lineage & metrics containers
        self.node_metrics: Dict[str, Dict[str, Any]] = {}
        self.lineage: Dict[str, Dict[str, Any]] = {  # step -> {inputs: [], outputs: []}
            # filled dynamically
        }
        self.dataset_producers: Dict[str, str] = {}  # dataset -> step
    # primary 输出裁剪相关状态已移除
        # Fingerprint / caching (Phase3 enhancement)
        self.dataset_fingerprints: Dict[str, str] = {}  # dataset -> fingerprint string
        self.node_signatures: Dict[str, str] = {}  # step -> last execution signature
        # Persistent cache control
        self.enable_persist = True
        try:
            opts = (execute_manager.ctx.config or {}).get('pipeline', {}).get('__options__', {}) or {}
            cache_opts = opts.get('cache', {}) if isinstance(opts.get('cache'), dict) else {}
            # 默认开启，显式 persist=False 可关闭
            self.enable_persist = cache_opts.get('persist', True)
        except Exception:
            pass
        self.cache_base = Path('.pipeline/cache')
        self.cache_datasets_dir = self.cache_base / 'datasets'
        if self.enable_persist:
            try:
                self._load_persistent_cache()
            except Exception as e:
                self.logger.warning(f"⚠️ 持久化缓存加载失败(忽略继续): {e}")
        self.logger.info("Kedro引擎初始化成功")
        self._initialize_global_catalog()

    def _initialize_global_catalog(self):
        try:
            self.data_catalog = DataCatalog()
            self.global_catalog = {}
            self.logger.info("Kedro数据目录初始化完成")
        except Exception as e:
            self.logger.error(f"数据目录初始化失败: {e}")

    def build_all_pipelines(self, config: Dict[str, Any]):
        kedro_pipelines = config.get("pipeline", {}).get("kedro_pipelines", {})
        for pipeline_name, pipeline_config in kedro_pipelines.items():
            try:
                self.create_pipeline(pipeline_name, pipeline_config)
            except Exception as e:
                self.logger.error(f"管道构建失败 {pipeline_name}: {e}")

    def create_pipeline(self, pipeline_name: str, config: Dict[str, Any]) -> Pipeline:
        nodes = []
        for node_config in config.get("nodes", []):
            try:
                kedro_node = self._create_kedro_node(node_config)
                if kedro_node:
                    nodes.append(kedro_node)
            except Exception as e:
                self.logger.error(f"节点创建失败: {e}")
                continue
        pipeline = Pipeline(nodes, tags={pipeline_name})
        self.pipelines[pipeline_name] = pipeline
        self.logger.info(f"Kedro管道创建成功: {pipeline_name}")
        return pipeline

    def _create_kedro_node(self, node_config: Dict[str, Any]):
        # 智能化配置格式：component + engine + method
        component = node_config.get("component")
        engine = node_config.get("engine")
        method = node_config.get("method")  # 支持字符串或数组
        method_handles = node_config.get("handles")  # Mode5: 多方法句柄列表

        if not component or not engine:
            self.logger.error(f"节点配置必须包含 component, engine: {node_config.get('name')}")
            return None

        if not method:
            self.logger.error(f"节点配置必须包含 method: {node_config.get('name')}")
            return None
        # ---------- I/O manager 构建 ----------
        step_name = node_config.get('name') or node_config.get('id') or (node_config.get('outputs') or ['unknown_step'])[0]
        io_manager = IOManager(self.global_catalog, self.logger, strict_pipeline=bool(self.execute_manager.ctx.config.get('pipeline', {}).get('__strict_schema__')) if self.execute_manager.ctx.config else False)
        io_cfg = io_manager.build_config(node_config)

        def execute_node(*args, **kwargs):
            upstream_map = {}
            start_ts = time.perf_counter()
            try:
                # 方法链列表（在任何依赖其内容的逻辑之前定义）
                method_list = method if isinstance(method, list) else [method]

                base_params = {**node_config.get("parameters", {}), **kwargs}

                # --- 基于 KedroEngine 全局 catalog 的引用解析 ---
                def _resolve_refs_via_catalog(obj):
                    em = self.execute_manager
                    # 使用 config_service 的 REF_PATTERN
                    pattern = getattr(em._config_service, 'REF_PATTERN', None)
                    def walk(v):
                        if isinstance(v, dict) and '__ref__' in v:
                            ref = v['__ref__']
                            h = v.get('hash')
                            try:
                                m = pattern.match(ref) if pattern else None
                            except Exception:
                                m = None
                            if m:
                                step_id = m.group('step')
                                out_id = m.group('param')
                                ds_name = f"{step_id}__{out_id}".replace('-', '_')
                                # 先从 global_catalog 取，其次尝试 data_catalog.load
                                if ds_name in self.global_catalog:
                                    val = self.global_catalog[ds_name]
                                else:
                                    loaded = False
                                    val = None
                                    try:
                                        if self.data_catalog and ds_name in getattr(self.data_catalog, '_data_sets', {}):
                                            val = self.data_catalog.load(ds_name)
                                            self.global_catalog[ds_name] = val
                                            loaded = True
                                    except Exception:
                                        pass
                                    if not loaded and val is None:
                                        raise ValueError(f"catalog 中未找到数据集: {ds_name} (ref={ref})")
                                # 注册到 context 引用表，保证后续通用解析可命中
                                try:
                                    rhash = em._config_service._hash_reference(ref)
                                    em.ctx.reference_to_hash.setdefault(ref, rhash)
                                    em.ctx.reference_values[ref] = val
                                    em.ctx.global_registry[rhash] = val
                                except Exception:
                                    pass
                                return val
                        if isinstance(v, list):
                            return [walk(x) for x in v]
                        if isinstance(v, dict):
                            return {k: walk(val) for k, val in v.items() if k != '__ref__'}
                        return v
                    return {k: walk(val) for k, val in obj.items()}

                try:
                    base_params = _resolve_refs_via_catalog(base_params)
                except Exception as e:
                    self.logger.error(f"参数引用解析失败(step={step_name}, catalog阶段): {e}")
                    raise

                # 统一通过 runtime_param_service 解析动态参数引用
                try:
                    base_params = self.execute_manager.resolve_runtime_params_for_engine(base_params)
                except Exception as e:
                    self.logger.error(f"参数引用解析失败(step={step_name}, runtime阶段): {e}")
                    raise

                # 已移除自动输入聚合逻辑，直接使用 base_params
                # 解析输入（不再执行 primary_policy 裁剪）
                resolved_inputs = io_manager.resolve_inputs(io_cfg, args)
                upstream_map = dict(resolved_inputs.mapping)
                applied_inputs = getattr(resolved_inputs, '_applied_inputs', list(upstream_map.keys()))
                trimmed_inputs = getattr(resolved_inputs, '_trimmed_inputs', [])
                # 预先收集计划输出 (dataset)
                planned_outputs = [o.name for o in io_cfg.outputs if o.kind == 'dataset']
                # 计算当前节点签名 (方法链 + 参数 + 上游指纹)
                upstream_fps = []
                for in_name, in_val in upstream_map.items():
                    fp = self._fingerprint_object(in_val)
                    upstream_fps.append(f"{in_name}:{fp}")
                param_items = sorted(base_params.items())
                # 使用 MethodHandle.predict_signature() 预测每个方法的实现指纹 (engine:version:priority)
                method_meta_parts = []
                try:
                    # method_handles 在 _create_kedro_node 构建时已写入 node_config
                    handle_map = {}
                    if method_handles:
                        for h in method_handles:
                            handle_map[getattr(h, 'method', None)] = h
                    for m in method_list:
                        h = handle_map.get(m)
                        if h is not None:
                            method_meta_parts.append(h.predict_signature(self.execute_manager.orchestrator))
                        else:
                            method_meta_parts.append(f"{m}@unknown:unknown:0")
                    method_meta_str = ';'.join(method_meta_parts)
                except Exception:
                    method_meta_str = ';'.join(f"{m}@unknown:unknown:0" for m in method_list)
                signature_components = ["|".join(method_list), method_meta_str, str(param_items), "|".join(sorted(upstream_fps))]
                node_signature = "#".join(signature_components)

                # --- 缓存判定调试信息 (改为 DEBUG 级别，避免终端噪音) ---
                self.logger.debug(
                    "[CACHE CHECK] step=%s outputs=%s loaded=%s last_sig=%s new_sig=%s",
                    step_name,
                    planned_outputs,
                    {o: o in self.global_catalog for o in planned_outputs},
                    self.node_signatures.get(step_name),
                    node_signature,
                )

                # 缓存命中：输出存在 且 签名相同 -> 跳过执行
                last_sig = self.node_signatures.get(step_name)
                # TTL 失效判定: 允许在 step 配置中加入 cache_ttl(seconds)
                ttl_expired = False
                try:
                    step_cfg_search = None
                    for s in (self.execute_manager.ctx.config.get('pipeline', {}).get('steps') or []):
                        if isinstance(s, dict) and s.get('name') == step_name:
                            step_cfg_search = s
                            break
                    if step_cfg_search and 'cache_ttl' in step_cfg_search:
                        ttl = step_cfg_search.get('cache_ttl')
                        if isinstance(ttl, (int, float)) and ttl > 0:
                            # 简单实现: datasets_index.json 中无单独时间戳, 使用签名文件修改时间
                            sig_file = self.cache_base / 'node_signatures.json'
                            if sig_file.exists():
                                import time as _t, os
                                age = _t.time() - os.path.getmtime(sig_file)
                                if age > ttl:
                                    ttl_expired = True
                except Exception:
                    pass
                if planned_outputs and all(o in self.global_catalog for o in planned_outputs) and last_sig == node_signature and not ttl_expired:
                    self.logger.info(f"🧩 Cache hit: {step_name} (signature matched) -> skip execution")
                    duration = time.perf_counter() - start_ts
                    # 补写 primary 标记（缓存命中也需要）
                    primary_out = node_config.get('primary_output') or (planned_outputs[0] if planned_outputs else None)
                    for ds in planned_outputs:
                        self.dataset_producers.setdefault(ds, step_name)
                    self.node_metrics[step_name] = {
                        'duration_sec': duration,
                        'outputs': [io_manager.summarize(o, self.global_catalog[o]) for o in planned_outputs],
                        'cached': True,
                        'signature': node_signature
                    }
                    self.lineage[step_name] = {
                        'inputs': list(upstream_map.keys()),
                        'applied_inputs': applied_inputs,
                        'trimmed_inputs': trimmed_inputs,
                        'applied_input_count': len(applied_inputs),
                        'trimmed_input_count': len(trimmed_inputs),
                        'outputs': planned_outputs,
                        'primary_output': node_config.get('primary_output') or (planned_outputs[0] if planned_outputs else None),
                        'cached': True,
                        'signature': node_signature,
                        # primary_policy 已移除
                        'duration_sec': duration
                    }
                    for ds in planned_outputs:
                        self.dataset_producers.setdefault(ds, step_name)
                    try:
                        HookManager.get().emit('on_cache_hit', step_name, self.node_metrics[step_name])
                    except Exception:
                        pass
                    return tuple(self.global_catalog[o] for o in planned_outputs) if len(planned_outputs) > 1 else (self.global_catalog[planned_outputs[0]],)
                # 由 IOManager 决定绑定策略

                def _unwrap(func: Callable) -> Callable:
                    original = func
                    depth = 0
                    while hasattr(original, '__wrapped__') and depth < 10:
                        original = getattr(original, '__wrapped__')
                        depth += 1
                    return original

                def build_call_params(reg_callable: Callable, prev_result, user_params) -> Dict[str, Any]:
                    return io_manager.bind_call_params(reg_callable, user_params, resolved_inputs, previous_result=prev_result)

                # method_list 已在上方定义
                # 如果存在旧签名且输出存在但签名不同，输出 diff 说明
                if planned_outputs and all(o in self.global_catalog for o in planned_outputs) and last_sig and last_sig != node_signature and not ttl_expired:
                    self._log_cache_diff(step_name, last_sig, node_signature, upstream_fps)
                if ttl_expired:
                    self.logger.info(f"⏰ Cache TTL expired for step={step_name}, 强制重算")
                self.logger.info(f"🔄 执行方法序列: {method_list}")
                try:
                    HookManager.get().emit('before_node', step_name, {
                        'planned_outputs': planned_outputs,
                        'inputs': list(upstream_map.keys()),
                        'signature': node_signature
                    })
                except Exception:
                    pass
                result = None
                for idx, method_name in enumerate(method_list):
                    self.logger.info(f"  ⚡ 执行方法 {idx+1}/{len(method_list)}: {method_name}")
                    # 若存在句柄列表且 engine 占位符，按方法单独解析
                    effective_engine = engine
                    if method_handles and engine in ('<auto:deferred>', '<handle:auto>'):
                        # 匹配当前方法的 handle（一个方法一个）
                        mh = None
                        for h in method_handles:
                            if getattr(h, 'method', None) == method_name:
                                mh = h
                                break
                        if mh is None:
                            raise ValueError(f"未找到方法句柄: {step_name}.{method_name}")
                        try:
                            # 预测值（用于一致性校验）
                            predicted = None
                            try:
                                predicted_sig = mh.predict_signature(self.execute_manager.orchestrator)
                                # 格式 method@engine:version:priority
                                if '@' in predicted_sig and ':' in predicted_sig:
                                    mpart, rest = predicted_sig.split('@',1)
                                    eng_part = rest.split(':',1)[0]
                                    predicted = eng_part
                            except Exception:
                                predicted = None
                            resolved_engine = mh.resolve(self.execute_manager.orchestrator)
                            effective_engine = resolved_engine
                            if predicted and predicted != resolved_engine:
                                self.logger.warning(f"⚠️ 句柄预测与实际解析不一致: step={step_name} method={method_name} predicted={predicted} actual={resolved_engine}")
                            self.logger.info(f"🧮 句柄解析引擎: step={step_name} method={method_name} -> {resolved_engine}")
                        except Exception as re:
                            raise ValueError(f"MethodHandle 引擎解析失败: {step_name}.{method_name} - {re}") from re
                    # 通过新索引结构解析注册： component -> method -> engine -> registration
                    idx_bucket = self.execute_manager.orchestrator.registry.index.by_component.get(component, {})
                    method_bucket = idx_bucket.get(method_name, {}) if idx_bucket else {}
                    registration = method_bucket.get(effective_engine)
                    if not registration:
                        raise ValueError(f"未注册的方法: {component}::{effective_engine}::{method_name}")
                    callable_obj = registration.callable
                    # 捕获函数参数名供后续 auto 策略判定
                    try:
                        sig = inspect.signature(io_manager._unwrap(callable_obj))
                        # 临时把参数名集合挂在 resolved_inputs 上（后续 IOManager 可利用）
                        setattr(resolved_inputs, 'sig_param_names', list(sig.parameters.keys()))
                    except Exception:
                        setattr(resolved_inputs, 'sig_param_names', [])
                    schema_meta = getattr(callable_obj, '__schema__', None)
                    strict_pipeline = bool(self.execute_manager.ctx.config.get('pipeline', {}).get('__strict_schema__')) if self.execute_manager.ctx.config else False
                    strict_schema = bool(schema_meta.get('strict')) if isinstance(schema_meta, dict) else False
                    effective_strict = strict_pipeline or strict_schema
                    call_params = build_call_params(callable_obj, result, base_params)
                    # -------- 输入列严格校验 (委托 IOManager) --------
                    if schema_meta and schema_meta.get('required_columns'):
                        io_manager.validate_input_schema(schema_meta, call_params, effective_strict)
                    result = self.execute_manager.orchestrator.execute_with_engine(
                        component_type=component,
                        engine_type=effective_engine,
                        method_name=method_name,
                        **call_params
                    )
                    # -------- 输出键严格校验 (委托 IOManager) --------
                    if schema_meta and schema_meta.get('output_keys') and isinstance(result, dict):
                        io_manager.validate_output_schema(schema_meta, result, effective_strict, method_name)
                    self.logger.info(f"  ✅ 方法 {method_name} 执行完成")
                self.logger.info(f"🎯 方法序列执行完成，共 {len(method_list)} 个方法")

                outputs = [o.name for o in io_cfg.outputs if o.kind == 'dataset']
                produced_dataset_names: List[str] = []
                # 收集 parameter 输出名称
                parameter_outputs = [o.name for o in io_cfg.outputs if o.kind == 'parameter']
                if outputs:
                    captured = io_manager.capture_outputs(io_cfg, result)
                    for on, val in captured.produced.items():
                        self.global_catalog[on] = val
                        # 仅记录 dataset 输出（parameter 输出仍保留在 global_catalog 可被引用）
                        spec = next((s for s in io_cfg.outputs if s.name == on), None)
                        if spec and spec.kind == 'dataset':
                            produced_dataset_names.append(on)
                    # parameter 输出摘要
                    param_summary = {pn: io_manager.summarize(pn, self.global_catalog.get(pn)) for pn in parameter_outputs if pn in self.global_catalog}
                    final = captured.tuple_result
                    duration = time.perf_counter() - start_ts
                    self.node_metrics[step_name] = {
                        'duration_sec': duration,
                        'outputs': [io_manager.summarize(on, self.global_catalog[on]) for on in produced_dataset_names],
                        'parameters': param_summary,
                        'cached': False,
                        'signature': node_signature
                    }
                    self.lineage[step_name] = {
                        'inputs': list(upstream_map.keys()),
                        'applied_inputs': applied_inputs,
                        'trimmed_inputs': trimmed_inputs,
                        'applied_input_count': len(applied_inputs),
                        'trimmed_input_count': len(trimmed_inputs),
                        'outputs': produced_dataset_names,
                        'parameters_produced': parameter_outputs,
                        'parameters_used': node_config.get('param_inputs', []),
                        'primary_output': captured.primary_output if captured.primary_output else (produced_dataset_names[0] if produced_dataset_names else None),
                        'cached': False,
                        'signature': node_signature,
                        # primary_policy 已移除
                        'duration_sec': duration
                    }
                    for ds in produced_dataset_names:
                        self.dataset_producers[ds] = step_name
                        # 记录输出指纹
                        self.dataset_fingerprints[ds] = self._fingerprint_object(self.global_catalog[ds])
                        # --- 即时向 ExecuteManager 注册引用 (支持后续节点参数解析) ---
                        if '__' in ds:
                            try:
                                step_id, out_id = ds.split('__', 1)
                                ref = f"steps.{step_id}.outputs.parameters.{out_id}"
                                em = self.execute_manager
                                rhash = em._hash_reference(ref)
                                em.reference_to_hash.setdefault(ref, rhash)
                                em.reference_values[ref] = self.global_catalog[ds]
                                em.global_registry[rhash] = self.global_catalog[ds]
                            except Exception:
                                pass
                    # parameter 输出同样注册（名称不含 __，但可直接构造引用）
                    for pn in parameter_outputs:
                        if pn in self.global_catalog:
                            try:
                                ref = f"steps.{step_name}.outputs.parameters.{pn}"
                                em = self.execute_manager
                                rhash = em._hash_reference(ref)
                                em.reference_to_hash.setdefault(ref, rhash)
                                em.reference_values[ref] = self.global_catalog[pn]
                                em.global_registry[rhash] = self.global_catalog[pn]
                            except Exception:
                                pass
                    # 记录节点签名
                    self.node_signatures[step_name] = node_signature
                    # 持久化节点与数据集（增量）
                    self._persist_node_state(produced_dataset_names)
                    try:
                        HookManager.get().emit('after_node', step_name, {
                            'duration_sec': duration,
                            'produced': produced_dataset_names,
                            'signature': node_signature,
                            'cached': False,
                        }, self.node_metrics[step_name])
                    except Exception:
                        pass
                    return final
                # 无 outputs（兜底）
                duration = time.perf_counter() - start_ts
                self.node_metrics[step_name] = {'duration_sec': duration, 'outputs': [], 'cached': False, 'signature': node_signature}
                self.lineage[step_name] = {
                    'inputs': list(upstream_map.keys()),
                    'applied_inputs': applied_inputs,
                    'trimmed_inputs': trimmed_inputs,
                    'applied_input_count': len(applied_inputs),
                    'trimmed_input_count': len(trimmed_inputs),
                    'outputs': [],
                    'primary_output': None,
                    'cached': False,
                    'signature': node_signature,
                    # primary_policy 已移除
                    'duration_sec': duration
                }
                self.node_signatures[step_name] = node_signature
                self._persist_node_state([])
                # Kedro 期望无 outputs 的节点返回 None/()，避免 DataFrame 等被误判
                try:
                    HookManager.get().emit('after_node', step_name, {
                        'duration_sec': duration,
                        'produced': [],
                        'signature': node_signature,
                        'cached': False,
                    }, self.node_metrics[step_name])
                except Exception:
                    pass
                return None
            except Exception as e:
                # 失败节点 lineage & metrics 记录（即便 soft-fail 上层吞掉，也保留痕迹）
                try:
                    duration = time.perf_counter() - start_ts if start_ts else None
                except Exception:
                    duration = None
                self.node_metrics[step_name] = {
                    'duration_sec': duration,
                    'outputs': [],
                    'parameters': {},
                    'cached': False,
                    'error': str(e)
                }
                if step_name not in self.lineage:
                    self.lineage[step_name] = {
                        'inputs': list(upstream_map.keys()),
                        'applied_inputs': applied_inputs,
                        'trimmed_inputs': trimmed_inputs,
                        'applied_input_count': len(applied_inputs),
                        'trimmed_input_count': len(trimmed_inputs),
                        'outputs': [],
                        'parameters_produced': [],
                        'parameters_used': node_config.get('param_inputs', []),
                        'primary_output': None,
                        'failed': True,
                        'error': str(e),
                        'cached': False,
                        'signature': None,
                        # primary_policy 已移除
                        'duration_sec': duration
                    }
                # 失败快照
                try:
                    fail_dir = Path('.pipeline') / 'failures'
                    fail_dir.mkdir(parents=True, exist_ok=True)
                    snapshot = {
                        'step': step_name,
                        'error': str(e),
                        'traceback': traceback.format_exc(limit=8),
                        'methods': method_list,
                        'parameters': base_params,
                        'inputs': list(upstream_map.keys()),
                        'signature': self.node_metrics.get(step_name, {}).get('signature'),
                    }
                    (fail_dir / f'{step_name}.json').write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding='utf-8')
                except Exception:
                    pass
                try:
                    HookManager.get().emit('after_node', step_name, {
                        'duration_sec': duration,
                        'produced': [],
                        'signature': self.node_metrics.get(step_name, {}).get('signature'),
                        'cached': False,
                        'error': str(e),
                        'failed': True,
                    }, self.node_metrics.get(step_name, {}))
                    HookManager.get().emit('on_failure', step_name, {
                        'error': str(e)
                    })
                except Exception:
                    pass
                self.logger.error(f"节点执行失败: {e}")
                raise

        # 仅把 dataset 类型输出注册给 Kedro，parameter 输出只放入全局目录与占位解析
        try:
            io_manager_tmp = IOManager(self.global_catalog, self.logger)
            io_cfg_tmp = io_manager_tmp.build_config(node_config)
            kedro_outputs = [spec.name for spec in io_cfg_tmp.outputs if spec.kind == 'dataset']
        except Exception:
            raw_outs = node_config.get("outputs", [])
            kedro_outputs = [o for o in raw_outs if isinstance(o, str)]

        # 确保 node_config 内部 outputs 替换为仅 dataset 列表（避免 Kedro 先读取原始 dict 列表时报错）
        node_config['outputs'] = kedro_outputs

        return node(
            func=execute_node,
            inputs=node_config.get("inputs", []),
            outputs=kedro_outputs if kedro_outputs else None,
            name=node_config.get("name"),
            tags=node_config.get("tags", [])
        )

    # ----------------------------------------------------------------------------
    # Caching helpers
    # ----------------------------------------------------------------------------
    def _fingerprint_object(self, obj: Any) -> str:
        """稳定指纹：DataFrame 使用 sha256(shape + 列名 + 前N行样本), 其它对象用类型+repr截断 sha256."""
        try:
            if isinstance(obj, pd.DataFrame):
                h = hashlib.sha256()
                h.update(str(obj.shape).encode())
                h.update("|".join(map(str, obj.columns)).encode())
                # 采样前 30 行（避免超大内存）
                sample = obj.head(30).to_csv(index=False).encode()
                h.update(sample)
                return f"df:{h.hexdigest()}"
            if isinstance(obj, (list, tuple)):
                h = hashlib.sha256()
                h.update(str(type(obj)).encode())
                h.update(str(len(obj)).encode())
                # 采样前 10 个元素的 repr
                for x in list(obj)[:10]:
                    h.update(repr(type(x)).encode())
                return f"seq:{h.hexdigest()}"
            if isinstance(obj, dict):
                h = hashlib.sha256()
                h.update(str(len(obj)).encode())
                for k in sorted(list(obj.keys())[:20]):
                    h.update(str(k).encode())
                    h.update(str(type(obj[k])).encode())
                return f"dict:{h.hexdigest()}"
            # fallback
            h = hashlib.sha256()
            rep = repr(obj)
            if len(rep) > 500:
                rep = rep[:500]
            h.update(rep.encode())
            h.update(str(type(obj)).encode())
            return f"obj:{h.hexdigest()}"
        except Exception:
            return "fingerprint:error"

    def _log_cache_diff(self, step_name: str, old_sig: str, new_sig: str, upstream_fps: List[str]):
        try:
            # 简单拆分签名：method_chain#params_repr#upstream_fp_join
            def split_sig(sig: str):
                parts = sig.split('#', 2)
                while len(parts) < 3:
                    parts.append('')
                return parts
            old_m, old_p, old_u = split_sig(old_sig)
            new_m, new_p, new_u = split_sig(new_sig)
            diffs = []
            if old_m != new_m:
                diffs.append('method_chain')
            if old_p != new_p:
                diffs.append('parameters')
            if old_u != new_u:
                diffs.append('upstream')
            reason = ','.join(diffs) or 'unknown'
            self.logger.info(f"♻️ Cache miss (signature changed) step={step_name} reason=[{reason}] diffs={{methods:{old_m!r}->{new_m!r}, params_changed:{old_p!=new_p}, upstream_changed:{old_u!=new_u}}}")
        except Exception:
            self.logger.debug("无法生成缓存差异说明")

    # ----------------------------------------------------------------------------
    # Persistent cache helpers
    # ----------------------------------------------------------------------------
    def _load_persistent_cache(self):
        if not self.enable_persist:
            return
        sig_file = self.cache_base / 'node_signatures.json'
        idx_file = self.cache_base / 'datasets_index.json'
        if sig_file.exists():
            try:
                self.node_signatures = json.loads(sig_file.read_text(encoding='utf-8'))
            except Exception as e:
                self.logger.warning(f"签名文件读取失败: {e}")
        if idx_file.exists():
            try:
                idx = json.loads(idx_file.read_text(encoding='utf-8'))
                loaded = 0
                for ds, meta in idx.items():
                    file_rel = meta.get('file')
                    if not file_rel:
                        continue
                    fpath = self.cache_base / file_rel
                    if not fpath.exists():
                        continue
                    try:
                        with open(fpath, 'rb') as f:
                            obj = pickle.load(f)
                        self.global_catalog[ds] = obj
                        self.dataset_fingerprints[ds] = meta.get('fingerprint', '')
                        loaded += 1
                    except Exception as e:
                        self.logger.warning(f"数据集 {ds} 载入失败: {e}")
                if loaded:
                    self.logger.info(f"📦 持久化缓存载入: {loaded} datasets, {len(self.node_signatures)} node signatures")
            except Exception as e:
                self.logger.warning(f"数据集索引读取失败: {e}")

    def _persist_node_state(self, produced: List[str]):
        if not self.enable_persist:
            return
        try:
            self.cache_base.mkdir(parents=True, exist_ok=True)
            self.cache_datasets_dir.mkdir(parents=True, exist_ok=True)
            # 写节点签名
            sig_file = self.cache_base / 'node_signatures.json'
            sig_file.write_text(json.dumps(self.node_signatures, ensure_ascii=False, indent=2), encoding='utf-8')
            # 读取旧索引
            idx_file = self.cache_base / 'datasets_index.json'
            if idx_file.exists():
                try:
                    idx = json.loads(idx_file.read_text(encoding='utf-8'))
                except Exception:
                    idx = {}
            else:
                idx = {}
            # 写新产生的数据集
            for ds in produced:
                obj = self.global_catalog.get(ds)
                if obj is None:
                    continue
                fp = self.dataset_fingerprints.get(ds) or self._fingerprint_object(obj)
                self.dataset_fingerprints[ds] = fp
                file_name = f"{ds}.pkl"
                safe_path = self.cache_datasets_dir / file_name
                try:
                    with open(safe_path, 'wb') as f:
                        pickle.dump(obj, f)
                except Exception as e:
                    self.logger.warning(f"数据集 {ds} 持久化失败: {e}")
                rel_path = f"datasets/{file_name}"
                idx[ds] = {
                    'fingerprint': fp,
                    'type': type(obj).__name__,
                    'file': rel_path
                }
            idx_file.write_text(json.dumps(idx, ensure_ascii=False, indent=2), encoding='utf-8')
        except Exception as e:
            self.logger.warning(f"⚠️ 持久化写入失败(忽略): {e}")

    def parse_pipeline_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """解析管道配置，为Kedro构建做准备"""
        try:
            # 直接返回Kedro管道配置部分
            kedro_pipelines = config.get("pipeline", {}).get("kedro_pipelines", {})

            # 构建管道配置对象
            pipeline_configs = {}
            for pipeline_name, pipeline_def in kedro_pipelines.items():
                pipeline_configs[pipeline_name] = type('PipelineConfig', (), {
                    'name': pipeline_def.get('name', pipeline_name),
                    'description': pipeline_def.get('description', ''),
                    'nodes': pipeline_def.get('nodes', []),
                    'depends_on': pipeline_def.get('depends_on', [])
                })()

            self.logger.info(f"✅ Kedro配置解析完成: {len(pipeline_configs)} 个管道")
            return pipeline_configs

        except Exception as e:
            self.logger.error(f"❌ Kedro配置解析失败: {e}")
            raise

    def get_pipeline_execution_order(self, pipeline_configs: Dict[str, Any]) -> List[str]:
        """确定管道执行顺序（拓扑排序）"""
        try:
            # 构建依赖图
            dependencies = {}
            for pipeline_name, config in pipeline_configs.items():
                dependencies[pipeline_name] = getattr(config, 'depends_on', [])

            # 拓扑排序
            result = []
            visited = set()
            temp_visited = set()

            def visit(pipeline_name):
                if pipeline_name in temp_visited:
                    raise ValueError(f"检测到循环依赖: {pipeline_name}")
                if pipeline_name in visited:
                    return

                temp_visited.add(pipeline_name)
                for dep in dependencies.get(pipeline_name, []):
                    if dep in dependencies:  # 确保依赖存在
                        visit(dep)
                temp_visited.remove(pipeline_name)
                visited.add(pipeline_name)
                result.append(pipeline_name)

            for pipeline_name in dependencies:
                if pipeline_name not in visited:
                    visit(pipeline_name)

            self.logger.info(f"✅ 执行顺序确定: {' -> '.join(result)}")
            return result

        except Exception as e:
            self.logger.error(f"❌ 执行顺序确定失败: {e}")
            # 返回简单的按名称排序
            return list(pipeline_configs.keys())
