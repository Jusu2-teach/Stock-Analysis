#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""ExecuteManager (精简版)

职责：
1. 解析 YAML steps -> 规范化内部 StepSpec & 拓扑排序
2. 构建自动 Kedro 风格节点配置（延迟引擎绑定由 MethodHandle 负责）
3. 调用混合执行 FlowExecutor (Prefect 包装 Kedro)
4. 汇总输出 / 缓存 / 血缘 / 指标

已移除：历史多引擎模式 / 旧 CLI 兼容层 / 冗余 run/debug 接口
"""
import sys
import os
import re
import yaml
import hashlib
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set
from collections import defaultdict, deque
import logging
from datetime import datetime

# 路径注入
sys.path.append(str(Path(__file__).parent.parent.parent / "src"))
# orchestrator 已移至根目录,添加到搜索路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
try:
    # 使用新版 v4.0 orchestrator facade (已移至根目录)
    from orchestrator import AStockOrchestrator
except ImportError as e:  # pragma: no cover
    print(f"❌ 导入新版 orchestrator 失败: {e}")
    raise


from pipeline.core.services.config_service import ConfigService, StepSpec, StepOutput
from pipeline.core.services.result_assembler import ResultAssembler
from pipeline.core.services.runtime_param_service import RuntimeParamService
from pipeline.core.services.flow_executor import FlowExecutor
from pipeline.core.services.cache_stats_service import CacheStatsService

class ExecuteManager:
    """核心执行管理器 (Hybrid-only)

    功能聚焦：
    - 解析 YAML steps -> 生成自动 kedro 风格节点描述
    - 解析跨步引用 (steps.<step>.outputs.parameters.<name>)
    - 统一经 PrefectEngine (内部封装 KedroEngine) 执行
    - 提供缓存/软失败/血缘/指标 结果对上层暴露
    - 去除全部历史独立 Kedro / 其它引擎模式代码
    """

    REF_PATTERN = re.compile(r"^steps\.(?P<step>[^.]+)\.outputs\.parameters\.(?P<param>[^.]+)$")

    def __init__(self, config_path: Optional[str] = None, orchestrator: 'AStockOrchestrator' = None):
        self.logger = logging.getLogger("AStockExecuteManager")
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        self.config_path = config_path
        self.config: Optional[Dict[str, Any]] = None
        # 只保留一个 orchestrator 实例（可外部注入，未提供则本地实例化）
        self.orchestrator = orchestrator if orchestrator is not None else AStockOrchestrator()
        # 全局引用/注册表
        self.reference_values: Dict[str, Any] = {}
        self.global_registry: Dict[str, Any] = {}
        self.reference_to_hash: Dict[str, str] = {}
        self.steps: Dict[str, StepSpec] = {}
        self.execution_order: List[str] = []
        # services (分层后的职责拆分)
        self._config_service = ConfigService(self)
        self._result_assembler = ResultAssembler(self)
        self._runtime_param_service = RuntimeParamService(self)
        self._flow_executor = FlowExecutor(self)
    # 已移除自动多输入推断服务，保持显式参数/inputs 列表风格
        self._cache_stats_service = CacheStatsService(self)
        # 尝试插件自动发现 (可选目录 pipeline/plugins)
        try:
            from pipeline.core.services.hook_manager import HookManager
            import importlib, pkgutil, pathlib
            plugins_dir = Path(__file__).parent.parent / 'plugins'
            if plugins_dir.is_dir():
                # 支持通过环境变量禁用插件: PIPELINE_DISABLE_PLUGINS=log,prometheus_plugin
                disable_raw = (Path.cwd() / '.pipeline_disable_plugins').read_text(encoding='utf-8').strip() if (Path.cwd() / '.pipeline_disable_plugins').exists() else ''
                disable_env = os.getenv('PIPELINE_DISABLE_PLUGINS', '')
                disabled = {x.strip() for x in (disable_env + ',' + disable_raw).split(',') if x.strip()}
                for m in pkgutil.iter_modules([str(plugins_dir)]):
                    if m.name in disabled:
                        self.logger.info(f"🚫 跳过插件(被禁用): {m.name}")
                        continue
                    try:
                        mod = importlib.import_module(f'pipeline.plugins.{m.name}')
                        if hasattr(mod, 'register'):
                            mod.register(HookManager.get())
                            self.logger.info(f"🔌 已加载插件: {m.name}")
                    except Exception as e:  # pragma: no cover
                        self.logger.warning(f"插件加载失败 {m.name}: {e}")
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 配置解析
    # ------------------------------------------------------------------
    def load_config(self, path: Optional[str] = None) -> Dict[str, Any]:
        path = path or self.config_path
        if not path:
            raise ValueError("未提供配置路径")
        return self._config_service.load_config(path)

    def _hash_reference(self, ref: str) -> str:  # 外部服务仍会用
        return hashlib.md5(ref.encode('utf-8')).hexdigest()[:16]

    def rebuild_after_filter(self):
        """在 steps 过滤后重建内部结构 (parse + topo)。"""
        self.steps.clear()
        self.execution_order.clear()
        # 直接调用服务内部方法
        self._config_service._parse_steps()
        self._config_service._compute_execution_order()

    # ------------------------------------------------------------------
    # 执行
    # ------------------------------------------------------------------
    def execute_pipeline(self, engine: str | None = None) -> Dict[str, Any]:
        """统一入口：仅保留 Hybrid (Prefect+Kedro) 模式。engine 参数忽略。"""
        if self.config is None:
            self.load_config()
        # 构建 auto pipeline 供混合引擎内部使用
        auto_info = self._build_auto_kedro_config()
        result = self._flow_executor.run(auto_info)
        # 统一标识模式
        result['mode'] = 'hybrid'
        return result

    # ------------------ Introspection (for CLI status/engines) ---------
    def get_available_engines(self) -> Dict[str, Any]:
        """返回当前注册中心的组件/方法/引擎元数据 (供 CLI status / engines).

        结构:
        {
          'components': ['compA', 'compB', ...],
          'methods': {
              'component::engine_type::method': {
                  'component_type': ..., 'engine_type': ..., 'engine_name': ..., 'description': ...
              },
              ...
          }
        }
        """
        try:
            registry = self.orchestrator.registry
            methods = registry.list_methods()
            components = list(registry.index.by_component.keys())
            return {
                'components': components,
                'methods': methods
            }
        except Exception as e:  # pragma: no cover
            self.logger.warning(f"get_available_engines failed: {e}")
            return {'components': [], 'methods': {}}

    # ------------------ 自动构建 kedro pipeline 配置 ------------------
    def _dataset_name(self, step: str, output: str) -> str:
        # 统一数据集命名：step__output (避免点号，更适合某些后端)
        return f"{step}__{output}".replace('-', '_')

    def _build_auto_kedro_config(self) -> Dict[str, Any]:
        return self._config_service.build_auto_nodes()

    # 单独 Kedro 模式已移除

    # ------------------ Prefect 运行 ------------------
    # _run_prefect 已拆分到 FlowExecutor

    # ------------------ 从 catalog 注册输出 ------------------
    # _register_catalog_outputs 逻辑已合并进 ResultAssembler.register_catalog

    # 供外部引擎在节点运行时解析参数引用（直接委派 service）
    def resolve_runtime_params_for_engine(self, params: Dict[str, Any]) -> Dict[str, Any]:  # 保留最小入口
        return self._runtime_param_service.resolve(params)


    # 兼容旧接口 run / debug_registry 已移除，外部应统一使用 execute_pipeline()

    # ------------------ 工具方法 ------------------
    @staticmethod
    def clear_cache(cache_dir: str = '.pipeline/cache') -> None:
        """清除持久化缓存目录 (测试或调试使用)。"""
        import shutil, os
        try:
            if os.path.isdir(cache_dir):
                shutil.rmtree(cache_dir)
        except Exception:  # pragma: no cover
            pass


# 入口
def main():  # pragma: no cover
    import argparse
    parser = argparse.ArgumentParser(description="ExecuteManager Runner")
    parser.add_argument('-c', '--config', required=True, help='Pipeline YAML 路径')
    parser.add_argument('--log', default='INFO')
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log.upper(), logging.INFO))
    mgr = ExecuteManager(args.config)
    mgr.load_config()
    result = mgr.execute_pipeline()
    print(result.get('status'), 'executed_steps=', result.get('executed_steps'))


if __name__ == '__main__':  # pragma: no cover
    main()