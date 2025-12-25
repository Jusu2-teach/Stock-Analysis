#!/usr/bin/env python3
"""
EventBus 快速演示
================

展示统一事件总线的核心功能。

运行: python shared/demo.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from shared import (
    EventBus,
    EventPriority,
    MethodRegisteredEvent,
    NodeCompletedEvent,
    SystemReadyEvent,
)


def main():
    print("=" * 60)
    print("🚀 AStock EventBus Demo")
    print("=" * 60)

    # 获取 EventBus 单例
    bus = EventBus.get()
    bus.set_debug(True)

    # ==================== 1. 基础订阅 ====================
    print("\n📌 1. 注册事件处理器")

    @bus.on('registry.method.registered', priority=EventPriority.NORMAL)
    def on_method_registered(event: MethodRegisteredEvent):
        print(f"   [Handler] 新方法注册: {event.component}.{event.method} @ {event.engine_type}")

    @bus.on('registry.method.registered', priority=EventPriority.SYSTEM)
    def log_registration(event: MethodRegisteredEvent):
        print(f"   [Logger] 📝 {event.event_type} - {event.full_key}")

    print("   ✅ 已注册 2 个处理器")

    # ==================== 2. 发布事件 ====================
    print("\n📤 2. 发布 MethodRegisteredEvent")

    result = bus.emit(MethodRegisteredEvent(
        component='business',
        method='analyze_truth',
        engine_type='duckdb',
        engine_name='TruthEngine',
        version='1.0.0',
        full_key='business.analyze_truth.duckdb.TruthEngine.v1.0.0',
        source='demo'
    ))

    print(f"   📊 结果: handlers={result.handler_count}, success={result.success_count}, time={result.total_time_ms:.2f}ms")

    # ==================== 3. 通配符订阅 ====================
    print("\n🌟 3. 通配符订阅 (pipeline.*)")

    @bus.on('pipeline.*')
    def on_any_pipeline_event(event):
        print(f"   [Pipeline Monitor] 捕获: {event.event_type}")

    # 发布多个 pipeline 事件
    bus.emit(NodeCompletedEvent(
        step_name='load_data',
        status='success',
        duration_ms=123.45,
        source='demo'
    ))

    # ==================== 4. 一次性订阅 ====================
    print("\n⚡ 4. 一次性订阅 (once)")

    @bus.once('system.ready')
    def on_system_ready(event: SystemReadyEvent):
        print(f"   [Once] 系统就绪! 组件: {event.components}")

    # 第一次触发 - 会执行
    bus.emit(SystemReadyEvent(components=['orchestrator', 'pipeline'], source='demo'))
    # 第二次触发 - 不会执行（已自动移除）
    bus.emit(SystemReadyEvent(components=['test'], source='demo'))
    print("   ✅ 第二次发布不再触发 once 处理器")

    # ==================== 5. 查看统计 ====================
    print("\n📊 5. 事件统计")
    stats = bus.get_stats()
    for event_type, stat in stats.items():
        if stat['emit_count'] > 0:
            print(f"   {event_type}: emit={stat['emit_count']}, calls={stat['handler_calls']}")

    # ==================== 6. 列出处理器 ====================
    print("\n📋 6. 已注册处理器")
    handlers = bus.list_handlers()
    for event, names in handlers.items():
        if names:
            print(f"   {event}: {names}")

    print("\n" + "=" * 60)
    print("✅ Demo 完成!")
    print("=" * 60)


if __name__ == '__main__':
    main()
