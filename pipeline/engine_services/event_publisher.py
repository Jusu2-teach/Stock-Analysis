"""
Pipeline Services: Event Publisher
==================================

事件发布服务，从 KedroEngine 提取。

职责:
- 封装 EventBus 事件发布
- 提供统一的事件接口
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from shared import (
    EventBus,
    NodeStartedEvent,
    NodeCompletedEvent,
    CacheHitEvent,
    PipelineStartedEvent,
    PipelineCompletedEvent,
    PipelineErrorEvent,
)


class EventPublisher:
    """
    事件发布服务

    从 KedroEngine 提取的事件发布逻辑，统一事件接口。

    Example:
        publisher = EventPublisher(source='pipeline.kedro')

        # 节点事件
        publisher.on_node_started('Load_Data', inputs=['input.csv'], outputs=['Raw'])
        publisher.on_node_completed('Load_Data', 'success', duration_ms=1500)

        # 缓存事件
        publisher.on_cache_hit('Load_Data', signature='abc123', outputs=['Raw'])

        # Pipeline 事件
        publisher.on_pipeline_started('my_pipeline', ['step1', 'step2'])
        publisher.on_pipeline_completed('my_pipeline', 'success', ['step1', 'step2'])
    """

    def __init__(
        self,
        source: str = 'pipeline',
        logger: Optional[logging.Logger] = None,
    ):
        """
        初始化事件发布器

        Args:
            source: 事件来源标识
            logger: 日志器
        """
        self._bus = EventBus.get()
        self._source = source
        self._logger = logger or logging.getLogger(__name__)

    # ==================== 节点事件 ====================

    def on_node_started(
        self,
        step_name: str,
        inputs: List[str],
        outputs: List[str],
        signature: str = '',
    ):
        """
        发布节点开始事件

        Args:
            step_name: 步骤名称
            inputs: 输入列表
            outputs: 输出列表
            signature: 节点签名
        """
        try:
            self._bus.emit(NodeStartedEvent(
                step_name=step_name,
                inputs=inputs,
                outputs=outputs,
                signature=signature,
                source=self._source,
            ))
        except Exception as e:
            self._logger.debug(f"NodeStartedEvent 发布失败: {e}")

    def on_node_completed(
        self,
        step_name: str,
        status: str,
        duration_ms: float,
        output_count: int = 0,
        error: str = '',
        metrics: Optional[Dict[str, Any]] = None,
    ):
        """
        发布节点完成事件

        Args:
            step_name: 步骤名称
            status: 状态 ('success', 'failed', 'skipped')
            duration_ms: 耗时（毫秒）
            output_count: 输出数量
            error: 错误信息
            metrics: 指标
        """
        try:
            self._bus.emit(NodeCompletedEvent(
                step_name=step_name,
                status=status,
                duration_ms=duration_ms,
                output_count=output_count,
                error=error,
                metrics=metrics or {},
                source=self._source,
            ))
        except Exception as e:
            self._logger.debug(f"NodeCompletedEvent 发布失败: {e}")

    # ==================== 缓存事件 ====================

    def on_cache_hit(
        self,
        step_name: str,
        signature: str,
        outputs: List[str],
    ):
        """
        发布缓存命中事件

        Args:
            step_name: 步骤名称
            signature: 签名
            outputs: 缓存的输出列表
        """
        try:
            self._bus.emit(CacheHitEvent(
                step_name=step_name,
                signature=signature,
                outputs=outputs,
                source=self._source,
            ))
        except Exception as e:
            self._logger.debug(f"CacheHitEvent 发布失败: {e}")

    # ==================== Pipeline 事件 ====================

    def on_pipeline_started(
        self,
        pipeline_name: str,
        step_names: List[str],
        config_path: str = '',
    ):
        """
        发布 Pipeline 开始事件

        Args:
            pipeline_name: Pipeline 名称
            step_names: 步骤名称列表
            config_path: 配置文件路径
        """
        try:
            self._bus.emit(PipelineStartedEvent(
                pipeline_name=pipeline_name,
                config_path=config_path,
                total_steps=len(step_names) if step_names else 0,
                execution_order=step_names or [],
            ))
        except Exception as e:
            self._logger.debug(f"PipelineStartedEvent 发布失败: {e}")

    def on_pipeline_completed(
        self,
        pipeline_name: str,
        status: str,
        executed_steps: List[str],
        duration_ms: float = 0,
    ):
        """
        发布 Pipeline 完成事件

        Args:
            pipeline_name: Pipeline 名称
            status: 状态
            executed_steps: 已执行的步骤列表
            duration_ms: 总耗时（毫秒）
        """
        try:
            self._bus.emit(PipelineCompletedEvent(
                pipeline_name=pipeline_name,
                status=status,
                duration_sec=duration_ms / 1000.0 if duration_ms else 0.0,
                executed_steps=len(executed_steps) if executed_steps else 0,
            ))
        except Exception as e:
            self._logger.debug(f"PipelineCompletedEvent 发布失败: {e}")

    def on_pipeline_error(
        self,
        step_name: str,
        error: str,
        traceback: str = '',
    ):
        """
        发布 Pipeline 错误事件

        Args:
            step_name: 发生错误的步骤
            error: 错误信息
            traceback: 堆栈信息
        """
        try:
            self._bus.emit(PipelineErrorEvent(
                step_name=step_name,
                error=error,
                traceback=traceback,
                source=self._source,
            ))
        except Exception as e:
            self._logger.debug(f"PipelineErrorEvent 发布失败: {e}")
