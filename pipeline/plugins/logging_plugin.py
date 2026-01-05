"""日志插件: 打印节点与流级事件 (基于 EventBus)

自动注册到 EventBus，监听 pipeline 事件并打印日志。
"""
from __future__ import annotations
from shared import EventBus, EventPriority


def register():
    """注册插件到 EventBus"""
    bus = EventBus.get()

    @bus.on('pipeline.flow.started', priority=EventPriority.LOW)
    def on_flow_start(event, **kwargs):
        print(f"[PLUGIN] Flow start: {event.pipeline_name}")

    @bus.on('pipeline.flow.completed', priority=EventPriority.LOW)
    def on_flow_complete(event, **kwargs):
        print(f"[PLUGIN] Flow finished status={event.status} steps={event.executed_steps}")

    @bus.on('pipeline.node.started', priority=EventPriority.LOW)
    def on_node_start(event, **kwargs):
        print(f"[PLUGIN] -> node {event.step_name} inputs={len(event.inputs)}")

    @bus.on('pipeline.node.completed', priority=EventPriority.LOW)
    def on_node_complete(event, **kwargs):
        if event.status == 'failed':
            print(f"[PLUGIN] !! node FAILED {event.step_name}: {event.error}")
        else:
            print(f"[PLUGIN] <- node {event.step_name} duration={event.duration_ms:.0f}ms outputs={event.output_count}")

    @bus.on('pipeline.cache.hit', priority=EventPriority.LOW)
    def on_cache_hit(event, **kwargs):
        print(f"[PLUGIN] (cache hit) {event.step_name}")

    @bus.on('pipeline.error', priority=EventPriority.LOW)
    def on_error(event, **kwargs):
        print(f"[PLUGIN] ERROR in {event.step_name}: {event.error}")
