"""示例插件: 打印节点与流级事件 (可用于教学或调试)

放置在 pipeline/plugins/ 下即会被自动发现并注册。
"""
from __future__ import annotations
from typing import Any

def register(hooks):  # hooks: HookManager 实例
    def before_flow(ctx: dict):
        print(f"[PLUGIN] Flow start at {ctx.get('started_at')}")
    def after_flow(result: dict):
        print(f"[PLUGIN] Flow finished status={result.get('status')} nodes={len(result.get('executed_steps', []))}")
    def before_node(step_name: str, ctx: dict):
        print(f"[PLUGIN] -> node {step_name} inputs={len(ctx.get('inputs', []))}")
    def after_node(step_name: str, ctx: dict, metrics: dict):
        if ctx.get('failed'):
            print(f"[PLUGIN] !! node FAILED {step_name}: {ctx.get('error')}")
        else:
            print(f"[PLUGIN] <- node {step_name} duration={ctx.get('duration_sec')}s cached={ctx.get('cached')}")
    def on_cache_hit(step_name: str, metrics: dict):
        print(f"[PLUGIN] (cache hit) {step_name}")
    hooks.register('before_flow', before_flow)
    hooks.register('after_flow', after_flow)
    hooks.register('before_node', before_node)
    hooks.register('after_node', after_node)
    hooks.register('on_cache_hit', on_cache_hit)
