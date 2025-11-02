#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Advanced Prefect-Kedro Hybrid Engine
===================================

高级混合引擎：Prefect编排Kedro管道

核心理念：
- 🎯 Prefect 负责工作流编排、监控、重试
- 🏗️ Kedro 负责数据处理逻辑、血缘、测试
- 🔗 Prefect 将 Kedro 管道视为黑箱 Task
- 📊 统一的监控、容错、层级耗时统计

架构优势：
- 📈 Prefect 的调度 + Kedro 的数据工程最佳实践
- ♻️ 通过 ConcurrentTaskRunner 支持层内并行；max_workers=1 模拟顺序
- 🛡️ soft_fail 可选：单任务失败不影响整体（依赖自动 skipped）
- 📊 layer_metrics 输出每层任务数与耗时

Author: AStock Team
Version: 2.0.1 - Hybrid Architecture (soft_fail, layer_metrics)
"""

import logging
import inspect
import json
from typing import Dict, Any, List, Optional, Callable, Union
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass

# Prefect-Kedro 混合引擎必需导入
from prefect import flow, task, get_run_logger
from prefect.states import Completed, Failed
from prefect.task_runners import ConcurrentTaskRunner
from pipeline.io.io_manager import IOManager


@dataclass
class HybridTaskConfig:
    """混合任务配置"""
    name: str
    kedro_pipeline: str
    description: str
    retries: int = 3
    retry_delay: int = 30
    timeout: int = 600
    depends_on: List[str] = None


class PrefectEngine:
    """
    高级Prefect-Kedro混合引擎

    将Kedro管道封装为Prefect任务，实现强大的混合架构
    """

    def __init__(self, execute_manager):
        """
        初始化高级Prefect引擎

        Args:
            execute_manager: ExecuteManager实例
        """
        self.execute_manager = execute_manager
        self.logger = execute_manager.logger

        # 引擎状态
        self.kedro_engine = None
        self.current_flow = None
        self.task_registry = {}

        # 初始化
        self.logger.info("✅ Prefect-Kedro混合引擎初始化成功")
        self._initialize_kedro_integration()

    def _initialize_kedro_integration(self):
        """初始化Kedro集成"""
        try:
            # 优先尝试包内相对导入；失败后绝对导入（兼容直接脚本执行）
            KedroEngine = None  # type: ignore
            try:
                from .kedro_engine import KedroEngine as _KE  # type: ignore
                KedroEngine = _KE
            except Exception:
                from pipeline.engines.kedro_engine import KedroEngine as _KE  # type: ignore
                KedroEngine = _KE
            if KedroEngine is None:
                raise ImportError("KedroEngine 未找到")
            self.kedro_engine = KedroEngine(self.execute_manager)
            self.logger.info("🔗 Prefect-Kedro集成初始化完成 (import fallback OK)")
        except Exception as e:
            self.logger.error(f"❌ Kedro集成初始化失败: {e}")

    def parse_hybrid_config(self, config: Dict[str, Any]) -> Dict[str, HybridTaskConfig]:
        """
        解析混合配置:将Kedro管道映射为Prefect任务

        Args:
            config: 完整配置

        Returns:
            混合任务配置字典
        """
        hybrid_tasks = {}

        # 获取Kedro管道配置
        kedro_pipelines = config.get('pipeline', {}).get('kedro_pipelines', {})

        # 获取Prefect配置
        prefect_config = config.get('prefect_flow', {})
        task_config = prefect_config.get('task_config', {})

        for pipeline_name, pipeline_def in kedro_pipelines.items():
            # 为每个Kedro管道创建一个Prefect任务配置
            task_name = f"kedro_pipeline_{pipeline_name}"

            hybrid_task = HybridTaskConfig(
                name=task_name,
                kedro_pipeline=pipeline_name,
                description=pipeline_def.get('description', f"Execute {pipeline_name}"),
                retries=task_config.get('max_retries', 3),
                retry_delay=task_config.get('retry_delay', 30),
                timeout=task_config.get('timeout', 600),
                depends_on=pipeline_def.get('depends_on', [])
            )

            hybrid_tasks[pipeline_name] = hybrid_task

        self.logger.info(f"🎯 解析了 {len(hybrid_tasks)} 个混合任务配置")
        return hybrid_tasks

    def create_kedro_pipeline_task(self, hybrid_config: HybridTaskConfig, soft_fail: bool = False) -> Callable:
        """
        为Kedro管道创建Prefect任务包装器

        这是核心功能：将Kedro管道封装为Prefect任务

        Args:
            hybrid_config: 混合任务配置

        Returns:
            Prefect任务函数
        """
        pipeline_name = hybrid_config.kedro_pipeline

        @task(
            name=hybrid_config.name,
            description=hybrid_config.description,
            retries=hybrid_config.retries,
            retry_delay_seconds=hybrid_config.retry_delay,
            timeout_seconds=hybrid_config.timeout,
            tags=["kedro-pipeline"]  # 独立编排系统标签
        )
        def execute_kedro_pipeline_task(**task_inputs):
            """
            Prefect任务：执行Kedro管道

            这个任务将整个Kedro管道作为黑箱执行
            """
            logger = get_run_logger()
            logger.info(f"🚀 开始执行Kedro管道: {pipeline_name}")

            try:
                # 确保Kedro引擎和管道已准备好
                if pipeline_name not in self.kedro_engine.pipelines:
                    raise ValueError(f"Kedro管道未找到: {pipeline_name}")

                # 获取Kedro管道 & pipeline 定义
                pipeline = self.kedro_engine.pipelines[pipeline_name]
                catalog = self.kedro_engine.data_catalog
                pipeline_def = self.execute_manager.ctx.config.get('pipeline', {}).get('kedro_pipelines', {}).get(pipeline_name, {})
                node_defs = pipeline_def.get('nodes', []) or []
                declared_inputs = set()
                declared_outputs = set()
                for nd in node_defs:
                    # 过滤 node_defs 内部 outputs：只保留 dataset 字符串，写回以减少 Kedro 报错
                    cleaned_outs = []
                    for i in nd.get('inputs', []) or []:
                        if isinstance(i, str):
                            declared_inputs.add(i)
                    for o in nd.get('outputs', []) or []:
                        if isinstance(o, str) and '__param__' not in o:
                            declared_outputs.add(o)
                            cleaned_outs.append(o)
                    nd['outputs'] = cleaned_outs

                # 使用 IOManager 统一处理上游输入注入
                io_manager = IOManager(self.kedro_engine.global_catalog, self.logger)
                io_manager.ingest_prefect_inputs(pipeline_name, list(declared_inputs), task_inputs, self.logger)
                # 同步到 Kedro catalog（仅导入声明需要的）
                for in_name in declared_inputs:
                    if in_name in task_inputs:
                        try:
                            if in_name not in catalog._datasets:  # type: ignore[attr-defined]
                                from kedro.io import MemoryDataset
                                catalog.add(in_name, MemoryDataset())  # type: ignore[attr-defined]
                            catalog.save(in_name, task_inputs[in_name])
                            logger.info(f"📥 注入输入: {in_name}")
                        except Exception as _e:
                            logger.warning(f"保存输入 {in_name} 失败: {_e}")

                # 若声明了输入但未通过 task_inputs 传递，尝试从全局缓存补全
                for miss in declared_inputs:
                    if miss in task_inputs:  # 已由上游传入
                        continue
                    if miss in self.kedro_engine.global_catalog:
                        obj = self.kedro_engine.global_catalog[miss]
                        try:
                            if miss not in catalog._datasets:  # type: ignore[attr-defined]
                                from kedro.io import MemoryDataset
                                catalog.add(miss, MemoryDataset())  # type: ignore[attr-defined]
                            catalog.save(miss, obj)
                            logger.info(f"📥 回填缓存输入: {miss}")
                        except Exception as _e:
                            logger.warning(f"回填输入 {miss} 失败: {_e}")

                # 执行Kedro管道
                from kedro.runner import SequentialRunner
                runner = SequentialRunner()
                runner.run(pipeline, catalog)

                logger.info(f"✅ Kedro管道执行成功: {pipeline_name}")

                # 收集声明输出（优先使用 KedroEngine.global_catalog 中的持久化）
                outputs = {}
                for out in declared_outputs:
                    if out in self.kedro_engine.global_catalog:
                        outputs[out] = self.kedro_engine.global_catalog[out]
                    else:
                        try:
                            if out in catalog._datasets:  # type: ignore[attr-defined]
                                outputs[out] = catalog.load(out)
                        except Exception as e:
                            logger.warning(f"无法加载声明输出 {out}: {e}")

                return {
                    'status': 'completed',
                    'pipeline_name': pipeline_name,
                    'outputs': outputs
                }

            except Exception as e:
                # soft_fail 情况降级为 WARNING，避免误判整体失败
                if soft_fail:
                    logger.warning(f"⚠️ Prefect任务失败(soft_fail已吸收) {pipeline_name}: {e}")
                    return {
                        'status': 'failed',  # 保留标记以统计
                        'pipeline_name': pipeline_name,
                        'error': str(e),
                        'soft_fail': True
                    }
                else:
                    logger.error(f"❌ Prefect任务执行失败 {pipeline_name}: {e}")
                    raise

        return execute_kedro_pipeline_task

    def build_hybrid_flow(self, config: Dict[str, Any]) -> Callable:
        """
        构建混合工作流：Prefect Flow包含多个Kedro Pipeline任务

        Args:
            config: 完整配置

        Returns:
            Prefect Flow函数
        """
        # 确保Kedro管道已构建
        self.kedro_engine.build_all_pipelines(config)

        # 判断是否启用节点级粒度
        orchestration = config.get('pipeline', {}).get('orchestration', {}) or {}
        granularity = orchestration.get('granularity', 'pipeline').lower()
        try:
            self.logger.info(f"🔍 Prefect granularity 检测: raw_orchestration_keys={list(orchestration.keys())} granularity={granularity}")
        except Exception:
            pass
        if granularity == 'node':
            return self._build_node_level_flow(config, orchestration)

        # 解析混合配置
        hybrid_tasks = self.parse_hybrid_config(config)

        # 软失败配置（支持两种路径：pipeline.orchestration.soft_fail 与 pipeline.options.orchestration.soft_fail）
        options_block = config.get('pipeline', {}).get('options', {}) or {}
        opt_orch = options_block.get('orchestration', {}) if isinstance(options_block.get('orchestration', {}), dict) else {}
        soft_fail = bool(orchestration.get('soft_fail', False) or opt_orch.get('soft_fail', False))
        self.logger.info(f"🛡️ Soft-fail 解析结果: {soft_fail} (direct={orchestration.get('soft_fail')}, options={opt_orch.get('soft_fail')})")

        # 创建任务函数
        task_functions = {}
        for pipeline_name, hybrid_config in hybrid_tasks.items():
            task_func = self.create_kedro_pipeline_task(hybrid_config, soft_fail=soft_fail)
            task_functions[pipeline_name] = task_func
            self.task_registry[pipeline_name] = task_func

        # 获取Flow配置
        flow_config = config.get('prefect_flow', {}).get('flow_config', {})

        # 并行执行配置：pipeline.orchestration.task_runner = 'concurrent' | 'sequential'
        orch_cfg = config.get('pipeline', {}).get('orchestration', {})
        task_runner_type = orch_cfg.get('task_runner', 'sequential').lower()
        max_workers = orch_cfg.get('max_workers', 4)
        if task_runner_type == 'concurrent':
            runner = ConcurrentTaskRunner(max_workers=max_workers)
            self.logger.info(f"⚙️ 使用并行任务运行器 ConcurrentTaskRunner(max_workers={max_workers})")
        else:
            # Prefect 3.x 已无 SequentialTaskRunner，使用并发运行器限制为1个worker模拟顺序
            runner = ConcurrentTaskRunner(max_workers=1)
            self.logger.info("⚙️ 使用顺序模拟运行器 (ConcurrentTaskRunner(max_workers=1))")

        # 创建Prefect流程（动态选择task_runner）
        @flow(
            name=config.get('pipeline', {}).get('name', 'AStock混合工作流'),
            description=config.get('pipeline', {}).get('description', 'Prefect编排Kedro管道'),
            log_prints=True,
            retries=orchestration.get('retry_count', 3),
            retry_delay_seconds=orchestration.get('retry_delay', 30),
            timeout_seconds=orchestration.get('timeout', 1800),
            task_runner=runner
        )
        def hybrid_workflow(**flow_inputs):
            """
            混合工作流主函数

            Prefect管理整体编排，Kedro处理数据逻辑
            """
            logger = get_run_logger()
            logger.info("🎯 启动Prefect-Kedro混合工作流")

            # 确定执行顺序
            pipeline_configs = self.kedro_engine.parse_pipeline_config(config)
            execution_order = self.kedro_engine.get_pipeline_execution_order(pipeline_configs)

            # 任务结果存储
            task_results = {}
            pipeline_outputs = {}

            # 依赖层级并行执行：同层（无相互依赖）任务可并行
            # 构建依赖映射
            dependencies = {p: hybrid_tasks[p].depends_on or [] for p in hybrid_tasks}
            remaining = set(execution_order)
            scheduled = {}
            completed = set()

            def ready(pname):
                return all(dep in completed for dep in dependencies.get(pname, []))

            layer_idx = 0
            layer_metrics = []  # 记录每层耗时与任务数量
            while remaining:
                # 当前层：所有依赖已完成且未调度
                current_layer = [p for p in list(remaining) if ready(p)]
                if not current_layer:
                    raise ValueError(f"无法解析依赖（可能循环）: 剩余 {remaining}")
                layer_idx += 1
                logger.info(f"🧩 并行层 {layer_idx}: {current_layer}")

                import time as _time
                layer_start = _time.time()

                # 调度本层所有任务
                layer_futures = {}
                for pipeline_name in current_layer:
                    if pipeline_name not in task_functions:
                        logger.warning(f"跳过未知管道: {pipeline_name}")
                        remaining.remove(pipeline_name)
                        completed.add(pipeline_name)
                        continue
                    # 如果依赖中有 failed 且开启 soft_fail，则跳过此任务
                    dep_failed = any(
                        (dep in task_results and task_results[dep].get('status') == 'failed')
                        for dep in dependencies.get(pipeline_name, [])
                    )
                    if dep_failed and soft_fail:
                        logger.warning(f"⏭️ 软跳过任务 {pipeline_name} 因依赖失败: {dependencies.get(pipeline_name, [])}")
                        task_results[pipeline_name] = {
                            'status': 'skipped',
                            'reason': 'dependency_failed',
                            'dependencies': dependencies.get(pipeline_name, [])
                        }
                        remaining.remove(pipeline_name)
                        completed.add(pipeline_name)
                        continue
                    logger.info(f"📋 调度Kedro管道任务: {pipeline_name}")
                    task_inputs = {}
                    # 准备依赖输出作为输入
                    for dep in dependencies.get(pipeline_name, []):
                        dep_outputs = pipeline_outputs.get(dep, {})
                        for oname, odata in dep_outputs.items():
                            task_inputs[oname] = odata
                    task_func = task_functions[pipeline_name]
                    future = task_func(**task_inputs)
                    scheduled[pipeline_name] = future
                    layer_futures[pipeline_name] = future
                    remaining.remove(pipeline_name)

                # Prefect 3.x 任务调用返回的对象本身就是最终结果(同步执行 task_runner=Concurrent/1)
                # 这里直接使用返回值；如未来切换异步执行可再判断 .result 可用性
                for pipeline_name, fut in layer_futures.items():
                    res = fut  # fut 已是结果字典或普通对象
                    if isinstance(res, dict):
                        status = res.get('status')
                        if status == 'completed':
                            pipeline_outputs[pipeline_name] = res.get('outputs', {})
                            task_results[pipeline_name] = res
                            logger.info(f"✅ Kedro管道任务完成: {pipeline_name}")
                        elif status == 'failed':
                            task_results[pipeline_name] = res
                            if soft_fail:
                                logger.warning(f"⚠️ Kedro管道任务失败(soft_fail保留继续) : {pipeline_name}")
                            else:
                                logger.error(f"❌ Kedro管道任务失败: {pipeline_name}")
                        elif status == 'skipped':
                            task_results[pipeline_name] = res
                            logger.warning(f"⚠️ Kedro管道任务跳过: {pipeline_name}")
                        else:
                            task_results[pipeline_name] = {'status': 'completed', 'raw': res}
                            logger.info(f"✅ Kedro管道任务完成(未知状态包装): {pipeline_name}")
                    else:
                        task_results[pipeline_name] = {'status': 'completed', 'raw': res}
                        logger.info(f"✅ Kedro管道任务完成(非字典返回包装): {pipeline_name}")
                    completed.add(pipeline_name)

                layer_elapsed = _time.time() - layer_start
                layer_metrics.append({
                    'layer': layer_idx,
                    'tasks': current_layer,
                    'task_count': len(current_layer),
                    'elapsed_sec': round(layer_elapsed, 4)
                })
                logger.info(f"⏱️ 层 {layer_idx} 耗时 {layer_elapsed:.3f}s (任务数: {len(current_layer)})")

            logger.info("🎉 混合工作流执行完成 (并行层数: %d)" % layer_idx)
            # 汇总失败/跳过统计
            failed_count = sum(1 for v in task_results.values() if v.get('status') == 'failed')
            skipped_count = sum(1 for v in task_results.values() if v.get('status') == 'skipped')
            overall_status = 'completed'
            if failed_count > 0 and not soft_fail:
                overall_status = 'failed'
            elif failed_count > 0 and soft_fail:
                overall_status = 'completed_with_failures'

            return {
                'status': 'completed',
                'engine': 'prefect-kedro-hybrid',
                'task_results': task_results,
                'execution_order': execution_order,
                'total_pipelines': len(execution_order),
                'layers': layer_idx,
                'layer_metrics': layer_metrics,
                'failed_count': failed_count,
                'skipped_count': skipped_count,
                'overall_status': overall_status,
                'soft_fail': soft_fail
            }

        self.current_flow = hybrid_workflow
        self.logger.info(f"🔗 混合工作流构建完成: {len(task_functions)} 个Kedro管道任务")

        return hybrid_workflow

    def _build_node_level_flow(self, config: Dict[str, Any], orchestration: Dict[str, Any]) -> Callable:
        """构建节点级粒度的 Prefect Flow：每个 Kedro Node 一个 Prefect 任务"""
        from prefect import flow, task, get_run_logger
        from prefect.task_runners import ConcurrentTaskRunner
        # 获取所有管道（当前主要是 __auto__）
        pipelines = self.kedro_engine.pipelines
        if not pipelines:
            raise ValueError("未发现已构建的 Kedro 管道")
        # 仅支持单管道或合并多个管道节点
        all_nodes = []
        for pname, p in pipelines.items():
            for n in p.nodes:
                all_nodes.append((pname, n))
        # 构建数据集 -> 生产节点映射
        dataset_producer = {}
        node_inputs_map = {}
        node_outputs_map = {}
        for _, nd in all_nodes:
            outs = list(nd.outputs) if isinstance(nd.outputs, (list, tuple, set)) else ([nd.outputs] if nd.outputs else [])
            for o in outs:
                dataset_producer[o] = nd.name
            ins = list(nd.inputs) if isinstance(nd.inputs, (list, tuple, set)) else ([nd.inputs] if nd.inputs else [])
            node_inputs_map[nd.name] = ins
            node_outputs_map[nd.name] = outs
        # 依赖：节点依赖于所有其输入数据集的生产节点
        node_deps = {}
        for _, nd in all_nodes:
            deps = set()
            for din in node_inputs_map[nd.name]:
                if din in dataset_producer:
                    deps.add(dataset_producer[din])
            deps.discard(nd.name)
            node_deps[nd.name] = sorted(deps)
        soft_fail = bool(orchestration.get('soft_fail', False))
        task_runner_type = orchestration.get('task_runner', 'sequential').lower()
        max_workers = orchestration.get('max_workers', 4)
        if task_runner_type == 'concurrent':
            runner = ConcurrentTaskRunner(max_workers=max_workers)
            self.logger.info(f"⚙️ (Node) 使用并行任务运行器 ConcurrentTaskRunner(max_workers={max_workers})")
        else:
            runner = ConcurrentTaskRunner(max_workers=1)
            self.logger.info("⚙️ (Node) 使用顺序模拟运行器 (ConcurrentTaskRunner(max_workers=1))")

        # 为每个节点创建 Prefect 任务
        prefect_tasks = {}
        def make_task(node_name, kedro_node):
            @task(name=f"kedro_node_{node_name}", retries=orchestration.get('retry_count', 0), retry_delay_seconds=orchestration.get('retry_delay', 5), tags=["kedro-node"], timeout_seconds=orchestration.get('timeout', 900))
            def _exec_node(**up_inputs):  # noqa
                logger = get_run_logger()
                logger.info(f"🚀 执行Kedro节点: {node_name}")
                # 准备输入（按 kedro_node.inputs 序）
                args = []
                for in_name in node_inputs_map[node_name]:
                    if in_name in self.kedro_engine.global_catalog:
                        args.append(self.kedro_engine.global_catalog[in_name])
                    else:
                        # 尝试从 data_catalog 加载（缓存/持久化）
                        loaded = False
                        if self.kedro_engine.data_catalog and in_name in getattr(self.kedro_engine.data_catalog, '_data_sets', {}):  # type: ignore
                            try:
                                val = self.kedro_engine.data_catalog.load(in_name)  # type: ignore
                                self.kedro_engine.global_catalog[in_name] = val
                                args.append(val)
                                loaded = True
                            except Exception:
                                pass
                        if not loaded:
                            if in_name in up_inputs:
                                args.append(up_inputs[in_name])
                            else:
                                args.append(None)  # 占位
                # --------------- 缓存判定 ---------------
                cached = False
                signature_components = []
                try:
                    upstream_fps = []
                    for in_name, in_val in zip(node_inputs_map[node_name], args):
                        if in_val is not None:
                            upstream_fps.append(f"{in_name}:{self.kedro_engine._fingerprint_object(in_val)}")
                    signature_components.append("|".join(upstream_fps))
                    signature_components.append(node_name)
                    node_signature = "#".join(signature_components)
                    last_sig = self.kedro_engine.node_signatures.get(node_name)
                    outs_list = node_outputs_map[node_name]
                    if last_sig == node_signature and outs_list and all(o in self.kedro_engine.global_catalog for o in outs_list):
                        cached = True
                        result = tuple(self.kedro_engine.global_catalog[o] for o in outs_list) if len(outs_list) > 1 else (self.kedro_engine.global_catalog[outs_list[0]],)
                        logger.info(f"🧩 (NodeCache) 命中: {node_name}")
                    else:
                        result = kedro_node.func(*args)
                        self.kedro_engine.node_signatures[node_name] = node_signature
                except Exception as e:
                    if soft_fail:
                        logger.warning(f"⚠️ 节点执行失败(soft_fail): {node_name}: {e}")
                        return { 'status': 'failed', 'node': node_name, 'error': str(e), 'soft_fail': True }
                    raise
                # 将输出写入 data_catalog (MemoryDataset) 以供下游
                outs = node_outputs_map[node_name]
                produced = {}
                # result 结构: 多输出 -> tuple, 单输出 -> (obj,) per wrapper
                if outs:
                    if isinstance(result, tuple):
                        out_values = list(result)
                    else:
                        out_values = [result]
                    for ds_name, val in zip(outs, out_values):
                        self.kedro_engine.global_catalog[ds_name] = val
                        try:
                            from kedro.io import MemoryDataset
                            if ds_name not in self.kedro_engine.data_catalog._data_sets:  # type: ignore
                                self.kedro_engine.data_catalog.add(ds_name, MemoryDataset())  # type: ignore
                            self.kedro_engine.data_catalog.save(ds_name, val)  # type: ignore
                        except Exception:
                            pass
                        produced[ds_name] = val
                logger.info(f"✅ 节点完成: {node_name} -> {list(produced.keys())} {'(cached)' if cached else ''}")
                return { 'status': 'completed', 'node': node_name, 'outputs': produced, 'cached': cached }
            return _exec_node
        for pname, kedro_node in all_nodes:
            prefect_tasks[kedro_node.name] = make_task(kedro_node.name, kedro_node)

        @flow(
            name=config.get('pipeline', {}).get('name', 'AStock节点级工作流'),
            description='Prefect 节点级编排 Kedro pipeline',
            log_prints=True,
            task_runner=runner,
            retries=orchestration.get('retry_count', 0),
            retry_delay_seconds=orchestration.get('retry_delay', 5),
            timeout_seconds=orchestration.get('timeout', 1800)
        )
        def node_level_flow():
            logger = get_run_logger()
            logger.info("🎯 启动节点级 Prefect-Kedro 工作流")
            # 拓扑层执行
            remaining = set(prefect_tasks.keys())
            completed = set()
            deps = node_deps
            layer_metrics = []
            import time as _time
            layer_idx = 0
            results = {}
            dataset_to_value = {}
            cached_nodes = []
            while remaining:
                ready_nodes = [n for n in list(remaining) if all(d in completed for d in deps.get(n, []))]
                if not ready_nodes:
                    raise ValueError(f"存在循环依赖，剩余: {remaining}")
                layer_idx += 1
                logger.info(f"🧩 Node层 {layer_idx}: {ready_nodes}")
                start_layer = _time.time()
                futures = {}
                for n in ready_nodes:
                    # 聚合其依赖 outputs 作为 kwargs 输入（仅包含数据集）
                    upstream_kwargs = {}
                    for din in node_inputs_map[n]:
                        if din in dataset_to_value:
                            upstream_kwargs[din] = dataset_to_value[din]
                        elif din in self.kedro_engine.global_catalog:
                            upstream_kwargs[din] = self.kedro_engine.global_catalog[din]
                    futures[n] = prefect_tasks[n](**upstream_kwargs)
                    remaining.remove(n)
                # 收集结果
                for n, fut in futures.items():
                    res = fut
                    if isinstance(res, dict):
                        results[n] = res
                        if res.get('status') == 'completed':
                            for ds, val in (res.get('outputs') or {}).items():
                                dataset_to_value[ds] = val
                            if res.get('cached'):
                                cached_nodes.append(n)
                        elif res.get('status') == 'failed' and not soft_fail:
                            logger.error(f"❌ 节点失败终止: {n}")
                            raise RuntimeError(res.get('error'))
                    else:
                        results[n] = {'status': 'completed', 'raw': res}
                    completed.add(n)
                elapsed = _time.time() - start_layer
                layer_metrics.append({'layer': layer_idx, 'nodes': ready_nodes, 'node_count': len(ready_nodes), 'elapsed_sec': round(elapsed, 4)})
            overall_status = 'completed'
            failed_nodes = [n for n, r in results.items() if r.get('status') == 'failed']
            if failed_nodes and not soft_fail:
                overall_status = 'failed'
            elif failed_nodes and soft_fail:
                overall_status = 'completed_with_failures'
            # 合并 KedroEngine lineage / metrics（节点名一致时抽取）
            lineage = getattr(self.kedro_engine, 'lineage', {})
            metrics = getattr(self.kedro_engine, 'node_metrics', {})
            return {
                'status': overall_status,
                'engine': 'prefect-kedro-node',
                'nodes': list(prefect_tasks.keys()),
                'node_results': results,
                'layers': layer_idx,
                'layer_metrics': layer_metrics,
                'failed_nodes': failed_nodes,
                'soft_fail': soft_fail,
                'cached_nodes': cached_nodes,
                'lineage': {k: v for k, v in lineage.items() if k in results},
                'node_metrics': {k: v for k, v in metrics.items() if k in results}
            }
        self.current_flow = node_level_flow
        self.logger.info(f"🔗 节点级混合工作流构建完成: {len(prefect_tasks)} 个节点任务")
        return node_level_flow

    def execute_pipeline(self, execution_graph: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行混合管道系统（为ExecuteManager提供的接口）

        Args:
            execution_graph: 执行图（实际使用config中的混合配置）

        Returns:
            执行结果
        """
        try:
            config = self.execute_manager.ctx.config

            self.logger.info("🚀 启动Prefect-Kedro混合执行")

            # 构建混合工作流
            hybrid_flow = self.build_hybrid_flow(config)

            # 执行工作流
            start_time = datetime.now()

            # 使用Prefect执行
            flow_result = hybrid_flow()

            # 等待结果（同步执行）
            if hasattr(flow_result, 'result'):
                result = flow_result.result()
            else:
                result = flow_result

            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()

            self.logger.info(f"✅ 混合管道执行完成 ({execution_time:.2f}s)")

            # 确保返回标准格式
            if isinstance(result, dict):
                result['execution_time'] = execution_time
                result['start_time'] = start_time.isoformat()
                result['end_time'] = end_time.isoformat()
                # Phase3: enrich with lineage & node metrics if available
                if hasattr(self.kedro_engine, 'lineage'):
                    result['lineage'] = self.kedro_engine.lineage
                if hasattr(self.kedro_engine, 'node_metrics'):
                    result['node_metrics'] = self.kedro_engine.node_metrics
                if hasattr(self.kedro_engine, 'dataset_producers'):
                    result['dataset_producers'] = self.kedro_engine.dataset_producers
                return result
            else:
                return {
                    'status': 'completed',
                    'engine': 'prefect-kedro-hybrid',
                    'result': result,
                    'execution_time': execution_time,
                    'start_time': start_time.isoformat(),
                    'end_time': end_time.isoformat()
                }

        except Exception as e:
            self.logger.error(f"❌ 混合管道执行失败: {e}")
            return {
                'status': 'failed',
                'engine': 'prefect-kedro-hybrid',
                'error': str(e)
            }

    def get_flow_status(self) -> Dict[str, Any]:
        """获取工作流状态"""
        return {
            'available': True,
            'current_flow': self.current_flow.__name__ if self.current_flow else None,
            'registered_tasks': len(self.task_registry),
            'kedro_integration': self.kedro_engine is not None,
            'kedro_pipelines': len(self.kedro_engine.pipelines) if self.kedro_engine else 0
        }

    def visualize_hybrid_workflow(self, output_path: str = None) -> str:
        """
        可视化混合工作流

        Args:
            output_path: 输出文件路径

        Returns:
            可视化描述
        """
        if not self.current_flow:
            return "工作流不可用或未构建"

        try:
            description = []
            description.append("Prefect-Kedro混合工作流可视化")
            description.append("=" * 40)
            description.append(f"工作流名称: {self.current_flow.__name__}")
            description.append(f"Kedro管道数量: {len(self.kedro_engine.pipelines)}")
            description.append(f"Prefect任务数量: {len(self.task_registry)}")
            description.append("")

            # 显示管道结构
            if self.kedro_engine:
                config = self.execute_manager.ctx.config
                pipeline_configs = self.kedro_engine.parse_pipeline_config(config)
                execution_order = self.kedro_engine.get_pipeline_execution_order(pipeline_configs)

                description.append("执行顺序:")
                for i, pipeline_name in enumerate(execution_order, 1):
                    config_obj = pipeline_configs[pipeline_name]
                    description.append(f"{i}. {pipeline_name}")
                    description.append(f"   描述: {config_obj.description}")
                    description.append(f"   节点数: {len(config_obj.nodes)}")
                    if config_obj.depends_on:
                        description.append(f"   依赖: {', '.join(config_obj.depends_on)}")
                    description.append("")

            viz_text = "\\n".join(description)

            # 保存到文件
            if output_path:
                output_file = Path(output_path)
                output_file.parent.mkdir(parents=True, exist_ok=True)
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(viz_text)
                self.logger.info(f"📈 混合工作流可视化已保存: {output_path}")
                return str(output_path)
            else:
                return viz_text

        except Exception as e:
            self.logger.error(f"❌ 工作流可视化失败: {e}")
            return f"可视化失败: {e}"

    def parse_pipeline_config(self, pipeline_config: Dict[str, Any]) -> Dict[str, Any]:
        """解析管道配置，为Prefect优化"""
        try:
            parsed_config = {
                'name': pipeline_config.get('name', 'default_pipeline'),
                'description': pipeline_config.get('description', ''),
                'kedro_pipelines': pipeline_config.get('kedro_pipelines', {}),
                'prefect_settings': {
                    'flow_run_name': f"run_{pipeline_config.get('name', 'default')}_{int(datetime.now().timestamp())}",
                    'task_runner': 'sequential',  # 可以配置为并行runner
                    'retries': pipeline_config.get('retries', 0),
                    'retry_delay': pipeline_config.get('retry_delay', 0)
                }
            }

            self.logger.info(f"✅ Prefect配置解析完成: {parsed_config['name']}")
            return parsed_config

        except Exception as e:
            self.logger.error(f"❌ Prefect配置解析失败: {e}")
            raise


