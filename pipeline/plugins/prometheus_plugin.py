"""示例 Prometheus 导出插件 (懒加载, 若无 prometheus_client 则忽略)

功能:
- 记录节点执行时长 (Histogram)
- 记录缓存命中 (Counter)
- 记录失败节点 (Counter)
- 暴露简单的 start_http_server(默认 8009)

注意: 仅在安装 prometheus_client 时生效.
"""
from __future__ import annotations
from typing import Any
import os

PORT = int(os.getenv("PIPELINE_PROM_PORT", "8009"))

try:
    from prometheus_client import start_http_server, Histogram, Counter
except Exception:  # pragma: no cover - 环境无依赖时静默
    def register(_hooks):  # type: ignore
        print("[PROM_PLUGIN] prometheus_client 未安装, 跳过插件注册")
    # 直接返回
else:
    NODE_DURATION = Histogram(
        "pipeline_node_duration_seconds",
        "节点执行耗时(秒)",
        ["node"],
        buckets=(0.01,0.05,0.1,0.2,0.5,1,2,5,10,30,60,120)
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

    def register(hooks):  # hooks: HookManager
        global SERVER_STARTED
        if not SERVER_STARTED:
            try:
                start_http_server(PORT)
                SERVER_STARTED = True
                print(f"[PROM_PLUGIN] Prometheus 指标暴露端口: {PORT}")
            except OSError as e:  # 端口占用等
                print(f"[PROM_PLUGIN] 启动失败: {e}")
        def after_node(step_name: str, ctx: dict, metrics: dict):
            if ctx.get('cached'):
                CACHE_HIT.labels(step_name).inc()
            if ctx.get('failed'):
                NODE_FAIL.labels(step_name).inc()
            else:
                dur = ctx.get('duration_sec')
                if dur is not None:
                    NODE_DURATION.labels(step_name).observe(dur)
        def on_cache_hit(step_name: str, metrics: dict):
            CACHE_HIT.labels(step_name).inc()
        hooks.register('after_node', after_node)
        hooks.register('on_cache_hit', on_cache_hit)
