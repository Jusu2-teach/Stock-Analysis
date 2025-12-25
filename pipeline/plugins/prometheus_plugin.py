"""Prometheus 导出插件 (基于 EventBus)

功能:
- 记录节点执行时长 (Histogram)
- 记录缓存命中 (Counter)
- 记录失败节点 (Counter)
- 暴露简单的 start_http_server (默认 8009)

注意: 仅在安装 prometheus_client 时生效.
"""
from __future__ import annotations
import os

PORT = int(os.getenv("PIPELINE_PROM_PORT", "8009"))

try:
    from prometheus_client import start_http_server, Histogram, Counter
except ImportError:
    def register():
        """prometheus_client 未安装, 跳过插件注册"""
        print("[PROM_PLUGIN] prometheus_client 未安装, 跳过插件注册")
else:
    # 定义指标
    NODE_DURATION = Histogram(
        "pipeline_node_duration_seconds",
        "节点执行耗时(秒)",
        ["node"],
        buckets=(0.01, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 30, 60, 120)
    )
    CACHE_HIT = Counter(
        "pipeline_node_cache_hit_total",
        "节点缓存命中次数",
        ["node"],
    )
    NODE_FAIL = Counter(
        "pipeline_node_fail_total",
        "节点失败次数",
        ["node"],
    )
    SERVER_STARTED = False

    def register():
        """注册插件到 EventBus"""
        global SERVER_STARTED
        from shared import EventBus, EventPriority

        # 启动 HTTP 服务器
        if not SERVER_STARTED:
            try:
                start_http_server(PORT)
                SERVER_STARTED = True
                print(f"[PROM_PLUGIN] Prometheus 指标暴露端口: {PORT}")
            except OSError as e:
                print(f"[PROM_PLUGIN] 启动失败: {e}")

        bus = EventBus.get()

        @bus.on('pipeline.node.completed', priority=EventPriority.LOW)
        def on_node_complete(event):
            if event.status == 'failed':
                NODE_FAIL.labels(event.step_name).inc()
            else:
                duration_sec = event.duration_ms / 1000.0
                NODE_DURATION.labels(event.step_name).observe(duration_sec)

        @bus.on('pipeline.cache.hit', priority=EventPriority.LOW)
        def on_cache_hit(event):
            CACHE_HIT.labels(event.step_name).inc()
