#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ExecuteManager - Pipeline 执行管理器

职责：
1. 解析 YAML steps -> 规范化内部 StepSpec & 拓扑排序
2. 构建自动 Kedro 风格节点配置（延迟引擎绑定由 MethodHandle 负责）
3. 调用混合执行 FlowExecutor (Prefect 包装 Kedro)
4. 汇总输出 / 缓存 / 血缘 / 指标

架构升级 (v2.2.0):
- 完全基于 EventBus 架构
- Orchestrator 仍为必需依赖
"""
import sys
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
import logging

# 路径注入
sys.path.append(str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Orchestrator 依赖
from orchestrator import AStockOrchestrator

# 统一事件总线
from shared import EventBus, PipelineStartedEvent, PipelineCompletedEvent, SystemReadyEvent

from pipeline.core.context import PipelineContext
from pipeline.core.services.config_service import ConfigService
from pipeline.core.services.result_assembler import ResultAssembler
from pipeline.core.services.runtime_param_service import RuntimeParamService
from pipeline.core.services.flow_executor import FlowExecutor
from pipeline.core.services.cache_stats_service import CacheStatsService


class ExecuteManager:
    """Pipeline 执行管理器（Hybrid 模式）

    功能：
    - 解析 YAML steps -> 生成 Kedro 风格节点描述
    - 解析跨步引用 (steps.<step>.outputs.parameters.<name>)
    - 通过 PrefectEngine (内部封装 KedroEngine) 执行
    - 提供缓存/软失败/血缘/指标结果

    统一 EventBus 架构：
    - 所有事件通过 EventBus 发布
    - 插件通过 EventBus 订阅事件
    """

    def __init__(
        self,
        config_path: Optional[str] = None,
        orchestrator: Optional[Any] = None
    ):
        self.logger = logging.getLogger("AStockExecuteManager")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

        self.config_path = config_path

        # Orchestrator 依赖
        if orchestrator is not None:
            self.orchestrator = orchestrator
        else:
            self.orchestrator = AStockOrchestrator()

        # EventBus 实例
        self._event_bus = EventBus.get()

        # Pipeline 执行上下文（共享状态）
        self.ctx = PipelineContext()

        # 服务层（职责分离 - 全部基于 Context）
        self._config_service = ConfigService(self.ctx, self.logger)
        self._result_assembler = ResultAssembler(self.ctx, self.logger)
        self._runtime_param_service = RuntimeParamService(self.ctx, self.logger)
        self._cache_stats_service = CacheStatsService(self.logger)
        self._flow_executor = FlowExecutor(
            self.ctx,
            self._result_assembler,
            self._cache_stats_service,
            self.logger
        )
        # 插件自动发现
        self._load_plugins()

    # ------------------------------------------------------------------
    # 配置解析
    # ------------------------------------------------------------------
    def _load_plugins(self):
        """加载 pipeline/plugins 目录下的插件"""
        import importlib
        import pkgutil

        plugins_dir = Path(__file__).parent.parent / 'plugins'
        if not plugins_dir.is_dir():
            return

        # 获取禁用插件列表
        disabled = {x.strip() for x in os.getenv('PIPELINE_DISABLE_PLUGINS', '').split(',') if x.strip()}
        disable_file = Path.cwd() / '.pipeline_disable_plugins'
        if disable_file.exists():
            disabled.update(x.strip() for x in disable_file.read_text(encoding='utf-8').split(',') if x.strip())

        # 加载插件
        for module_info in pkgutil.iter_modules([str(plugins_dir)]):
            if module_info.name in disabled:
                self.logger.info(f"🚫 跳过插件: {module_info.name}")
                continue

            try:
                mod = importlib.import_module(f'pipeline.plugins.{module_info.name}')
                if hasattr(mod, 'register'):
                    mod.register()  # 新接口：无参数，插件自己获取 EventBus
                    self.logger.info(f"🔌 已加载插件: {module_info.name}")
            except Exception as e:
                self.logger.warning(f"插件加载失败 {module_info.name}: {e}")

    # ------------------------------------------------------------------
    # 配置解析
    # ------------------------------------------------------------------
    def load_config(self, path: Optional[str] = None) -> Dict[str, Any]:
        """加载配置文件（存储在 context 中）"""
        path = path or self.config_path
        if not path:
            raise ValueError("未提供配置路径")
        return self._config_service.load_config(path)

    def rebuild_after_filter(self):
        """在 steps 过滤后重建内部结构 (parse + topo)。"""
        self.ctx.clear_steps()
        # 直接调用服务内部方法
        self._config_service._parse_steps()
        self._config_service._compute_execution_order()

    # ------------------------------------------------------------------
    # 执行
    # ------------------------------------------------------------------
    def execute_pipeline(self) -> Dict[str, Any]:
        """执行 Pipeline（Hybrid 模式：Prefect + Kedro）

        Returns:
            执行结果字典，包含 status/executed_steps/outputs/metrics 等
        """
        if not self.ctx.config:
            self.load_config()

        auto_info = self._build_auto_kedro_config()
        result = self._flow_executor.run(auto_info, manager=self)
        result['mode'] = 'hybrid'
        return result

    # ------------------ Introspection ---------
    def get_available_engines(self) -> Dict[str, Any]:
        """返回注册中心的组件/方法/引擎元数据（供 CLI 使用）"""
        registry = self.orchestrator.registry
        return {
            'components': list(registry.index.by_component.keys()),
            'methods': registry.list_methods()
        }

    # ------------------ 内部配置构建 ------------------
    def _build_auto_kedro_config(self) -> Dict[str, Any]:
        """构建自动 Kedro 节点配置"""
        return self._config_service.build_auto_nodes()

    def resolve_runtime_params_for_engine(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """解析运行时参数引用（供引擎调用）"""
        return self._runtime_param_service.resolve(params)

    # ------------------ 工具方法 ------------------
    @staticmethod
    def clear_cache(cache_dir: str = '.pipeline/cache') -> None:
        """清除持久化缓存目录"""
        import shutil
        if os.path.isdir(cache_dir):
            shutil.rmtree(cache_dir)