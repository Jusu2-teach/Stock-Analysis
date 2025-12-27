"""
EventBus V6 使用示例与测试
==========================

演示 EventBus V6 的所有主要特性。
"""
import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)

# 导入 EventBus V6
from shared.event_bus_v6 import (
    EventBusV6,
    EventBusConfig,
    LoggingMiddleware,
    TracingMiddleware,
    RetryMiddleware,
    MetricsMiddleware,
    CircuitBreakerMiddleware,
    HookSpecRegistry,
    DeadEvent,
    CompositeDisposable,
    AsyncEventBus,
)


# ============================================================================
# 定义事件
# ============================================================================

@dataclass
class UserCreatedEvent:
    """用户创建事件"""
    event_type: str = "user.created"
    user_id: str = ""
    username: str = ""
    email: str = ""


@dataclass
class OrderPlacedEvent:
    """订单创建事件"""
    event_type: str = "order.placed"
    order_id: str = ""
    user_id: str = ""
    amount: float = 0.0


@dataclass
class PaymentCompletedEvent:
    """支付完成事件"""
    event_type: str = "payment.completed"
    payment_id: str = ""
    order_id: str = ""
    success: bool = True


# ============================================================================
# 示例 1: 基本用法
# ============================================================================

def demo_basic_usage():
    """演示基本的事件发布/订阅"""
    print("\n" + "=" * 60)
    print("示例 1: 基本用法")
    print("=" * 60)

    # 重置以确保干净的状态
    EventBusV6.reset()
    bus = EventBusV6.get()

    # 使用装饰器订阅
    @bus.on("user.created")
    def on_user_created(event: UserCreatedEvent):
        print(f"  📧 发送欢迎邮件给: {event.email}")

    # 直接订阅（返回 Subscription）
    def log_user_created(event: UserCreatedEvent):
        print(f"  📝 记录用户创建: {event.username}")

    subscription = bus.subscribe("user.created", log_user_created)

    # 发布事件
    print("\n发布 UserCreatedEvent:")
    result = bus.emit(UserCreatedEvent(
        user_id="u001",
        username="张三",
        email="zhangsan@example.com"
    ))

    print(f"\n结果: {result.success_count}/{result.handler_count} 处理器成功")

    # 取消订阅
    subscription.dispose()

    print("\n取消订阅后再次发布:")
    result = bus.emit(UserCreatedEvent(
        user_id="u002",
        username="李四",
        email="lisi@example.com"
    ))
    print(f"结果: {result.success_count}/{result.handler_count} 处理器成功")


# ============================================================================
# 示例 2: 中间件管道
# ============================================================================

def demo_middleware():
    """演示中间件功能"""
    print("\n" + "=" * 60)
    print("示例 2: 中间件管道")
    print("=" * 60)

    EventBusV6.reset()

    # 使用生产环境配置
    config = EventBusConfig.production()
    bus = EventBusV6.get(config)

    # 添加中间件
    metrics = MetricsMiddleware()
    bus.use(LoggingMiddleware(level=logging.INFO))
    bus.use(TracingMiddleware())
    bus.use(RetryMiddleware(max_retries=2))
    bus.use(metrics)

    # 订阅
    @bus.on("order.placed")
    def process_order(event: OrderPlacedEvent):
        print(f"  🛒 处理订单: {event.order_id}, 金额: ¥{event.amount}")

    @bus.on("order.placed")
    def notify_warehouse(event: OrderPlacedEvent):
        print(f"  📦 通知仓库发货: {event.order_id}")

    # 发布
    print("\n发布 OrderPlacedEvent:")
    bus.emit(OrderPlacedEvent(
        order_id="ORD-001",
        user_id="u001",
        amount=299.99
    ))

    # 查看指标
    print("\n📊 中间件指标:")
    for event_type, stats in metrics.get_metrics().items():
        print(f"  {event_type}: 调用 {stats['count']} 次, 平均耗时 {stats['avg_latency_ms']:.2f}ms")


# ============================================================================
# 示例 3: 死信处理
# ============================================================================

def demo_dead_letter():
    """演示死信队列"""
    print("\n" + "=" * 60)
    print("示例 3: 死信队列 (Guava 风格)")
    print("=" * 60)

    EventBusV6.reset()

    config = EventBusConfig(enable_dead_letter=True)
    bus = EventBusV6.get(config)

    # 注册死信处理器
    @bus.on_dead_letter
    def handle_dead_letter(dead: DeadEvent):
        print(f"  💀 死信捕获: {dead.original_type} (原因: {dead.reason})")

    # 发布没有订阅者的事件
    print("\n发布无订阅者的事件:")

    @dataclass
    class OrphanEvent:
        event_type: str = "orphan.event"
        message: str = ""

    bus.emit(OrphanEvent(message="这是一个孤儿事件"))

    # 查看死信队列
    dlq = bus.dead_letter_queue
    print(f"\n死信队列状态: {dlq.get_stats()}")

    # 后来注册订阅者
    @bus.on("orphan.event")
    def late_subscriber(event):
        print(f"  🔄 延迟订阅者收到: {event.message}")

    # 重试死信
    print("\n重试死信:")
    recovered = bus.retry_dead_letters()
    print(f"恢复了 {recovered} 个事件")


# ============================================================================
# 示例 4: 历史事件重放
# ============================================================================

def demo_historic_events():
    """演示历史事件重放 (Pluggy call_historic 风格)"""
    print("\n" + "=" * 60)
    print("示例 4: 历史事件重放 (Pluggy 风格)")
    print("=" * 60)

    EventBusV6.reset()

    config = EventBusConfig(enable_historic=True)
    bus = EventBusV6.get(config)

    # 标记为历史事件类型
    bus.mark_historic("system.initialized")

    # 早期发布的事件
    @dataclass
    class SystemInitializedEvent:
        event_type: str = "system.initialized"
        version: str = ""
        timestamp: str = ""

    print("发布历史事件 (此时无订阅者):")
    bus.emit(SystemInitializedEvent(version="1.0.0", timestamp="2024-01-01 10:00:00"))
    bus.emit(SystemInitializedEvent(version="1.1.0", timestamp="2024-01-15 10:00:00"))

    # 后来的订阅者
    print("\n后来的订阅者注册:")

    @bus.on("system.initialized")
    def late_listener(event):
        print(f"  📜 收到历史事件: v{event.version} @ {event.timestamp}")

    # 新事件
    print("\n新事件:")
    bus.emit(SystemInitializedEvent(version="1.2.0", timestamp="2024-02-01 10:00:00"))


# ============================================================================
# 示例 5: HookSpec 类型验证
# ============================================================================

def demo_hookspec():
    """演示 HookSpec 类型验证"""
    print("\n" + "=" * 60)
    print("示例 5: HookSpec 类型验证 (Pluggy 风格)")
    print("=" * 60)

    EventBusV6.reset()
    bus = EventBusV6.get()

    # 定义事件规格
    bus.define_spec(
        "payment.completed",
        required_args=('payment_id', 'order_id'),
        optional_args=('success', 'error_message'),
        description="支付完成事件规格"
    )

    print("已定义事件规格: payment.completed")
    print(f"  必需参数: payment_id, order_id")
    print(f"  可选参数: success, error_message")

    # 注册符合规格的处理器
    @bus.on("payment.completed")
    def handle_payment(event: PaymentCompletedEvent):
        if event.success:
            print(f"  ✅ 支付成功: {event.payment_id}")
        else:
            print(f"  ❌ 支付失败: {event.payment_id}")

    # 发布事件
    print("\n发布 PaymentCompletedEvent:")
    bus.emit(PaymentCompletedEvent(
        payment_id="PAY-001",
        order_id="ORD-001",
        success=True
    ))


# ============================================================================
# 示例 6: 组合订阅管理
# ============================================================================

def demo_composite_subscription():
    """演示组合订阅 (RxPY Disposable 风格)"""
    print("\n" + "=" * 60)
    print("示例 6: 组合订阅管理 (RxPY 风格)")
    print("=" * 60)

    EventBusV6.reset()
    bus = EventBusV6.get()

    # 创建组合订阅
    composite = CompositeDisposable()

    # 添加多个订阅
    composite.add(bus.subscribe("user.created", lambda e: print(f"  👤 用户: {e.username}")))
    composite.add(bus.subscribe("order.placed", lambda e: print(f"  🛒 订单: {e.order_id}")))
    composite.add(bus.subscribe("payment.completed", lambda e: print(f"  💳 支付: {e.payment_id}")))

    print(f"已创建 {composite.count} 个订阅")

    # 发布事件
    print("\n发布事件:")
    bus.emit(UserCreatedEvent(user_id="u001", username="王五", email="wangwu@example.com"))
    bus.emit(OrderPlacedEvent(order_id="ORD-002", user_id="u001", amount=199.99))
    bus.emit(PaymentCompletedEvent(payment_id="PAY-002", order_id="ORD-002"))

    # 一次性取消所有订阅
    print("\n取消所有订阅:")
    composite.dispose()

    # 再次发布（无响应）
    print("\n再次发布（应该无响应）:")
    bus.emit(UserCreatedEvent(user_id="u002", username="赵六", email="zhaoliu@example.com"))
    print("  (无处理器响应)")


# ============================================================================
# 示例 7: 异步事件
# ============================================================================

async def demo_async_events():
    """演示异步事件处理"""
    print("\n" + "=" * 60)
    print("示例 7: 异步事件处理 (Reactor 风格)")
    print("=" * 60)

    AsyncEventBus.reset()
    async_bus = AsyncEventBus.get()

    # 异步处理器
    @async_bus.on_async("data.processed")
    async def async_handler(event):
        print(f"  ⏳ 异步处理开始: {event.event_type}")
        await asyncio.sleep(0.1)  # 模拟异步操作
        print(f"  ✅ 异步处理完成")

    # 同步处理器（会在线程池执行）
    @async_bus.on_sync("data.processed")
    def sync_handler(event):
        print(f"  🔄 同步处理器 (线程池)")
        time.sleep(0.05)

    # 发布
    @dataclass
    class DataProcessedEvent:
        event_type: str = "data.processed"
        data_id: str = ""

    print("\n异步发布事件:")
    result = await async_bus.emit(DataProcessedEvent(data_id="D001"))
    print(f"结果: {result.success_count}/{result.handler_count} 成功, 耗时 {result.duration_ms:.2f}ms")


# ============================================================================
# 示例 8: 熔断器
# ============================================================================

def demo_circuit_breaker():
    """演示熔断器"""
    print("\n" + "=" * 60)
    print("示例 9: 熔断器")
    print("=" * 60)

    EventBusV6.reset()

    config = EventBusConfig(enable_middleware=True)
    bus = EventBusV6.get(config)

    # 添加熔断器（低阈值用于演示）
    breaker = CircuitBreakerMiddleware(
        failure_threshold=2,
        reset_timeout=1.0
    )
    bus.use(breaker)

    # 会失败的处理器
    fail_count = [0]

    @bus.on("risky.operation")
    def risky_handler(event):
        fail_count[0] += 1
        if fail_count[0] <= 3:
            raise RuntimeError(f"模拟失败 #{fail_count[0]}")
        print("  ✅ 操作成功")

    @dataclass
    class RiskyEvent:
        event_type: str = "risky.operation"

    # 触发熔断
    print("\n触发熔断:")
    for i in range(4):
        try:
            result = bus.emit(RiskyEvent())
            if result.errors:
                print(f"  尝试 {i+1}: 失败 - {result.errors[0]}")
        except RuntimeError as e:
            print(f"  尝试 {i+1}: 熔断器状态 = {breaker.state}")

    print(f"\n当前熔断器状态: {breaker.state}")


# ============================================================================
# 运行所有示例
# ============================================================================

def main():
    """运行所有示例"""
    print("\n" + "=" * 60)
    print("EventBus 功能演示")
    print("=" * 60)

    # 基础功能
    demo_basic_usage()
    demo_middleware()

    # 高级功能
    demo_dead_letter()
    demo_historic_events()
    demo_hookspec()

    # 订阅管理
    demo_composite_subscription()

    # 异步
    asyncio.run(demo_async_events())

    # 弹性
    demo_circuit_breaker()

    # 最终统计
    print("\n" + "=" * 60)
    print("最终统计")
    print("=" * 60)

    bus = EventBusV6.get()
    stats = bus.get_stats()
    print(f"  总处理器: {stats.total_handlers}")
    print(f"  总发布次数: {stats.total_emits}")
    print(f"  总错误: {stats.total_errors}")
    print(f"  运行时间: {stats.uptime_seconds:.2f}s")


if __name__ == "__main__":
    main()
