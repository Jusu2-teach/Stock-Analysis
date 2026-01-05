"""Execution Backend Abstractions

v4.0: 抽象执行后端接口，当前提供 PrefectBackend 实现。

目标：
- 将 FlowExecutor 与具体编排引擎（Prefect）解耦
- 预留未来切换/新增本地/分布式执行后端的能力
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Any, Dict, Tuple
import logging

from .context import PipelineContext


class ExecutionBackend(Protocol):
    """执行后端协议

    负责根据上下文配置构建并运行工作流，返回原始结果与底层引擎对象。
    """

    def run(self, ctx: PipelineContext, auto_info: Dict[str, Any], manager: Any) -> Tuple[Dict[str, Any], Any]:  # pragma: no cover - 协议接口
        """执行工作流

        Args:
            ctx: PipelineContext 实例
            auto_info: 自动构建的信息（预留）
            manager: ExecuteManager 实例

        Returns:
            (result, engine) 元组，其中 engine 通常为 KedroEngine 或其它底层执行引擎
        """
        ...


@dataclass
class PrefectBackend:
    """基于 PrefectEngine 的默认执行后端实现"""

    logger: logging.Logger

    def run(self, ctx: PipelineContext, auto_info: Dict[str, Any], manager: Any) -> Tuple[Dict[str, Any], Any]:
        """使用 PrefectEngine 执行混合工作流"""
        from pipeline.engines.prefect_engine import PrefectEngine

        prefect_engine = PrefectEngine(manager)

        # 构建并执行 Flow
        flow = prefect_engine.build_hybrid_flow(ctx.config)
        flow_result = flow()
        result = flow_result.result() if hasattr(flow_result, 'result') else flow_result

        kedro_engine = getattr(prefect_engine, 'kedro_engine', None)
        return result, kedro_engine


__all__ = ["ExecutionBackend", "PrefectBackend"]
