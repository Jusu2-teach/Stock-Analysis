from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional, Tuple


@dataclass(frozen=True)
class MethodRegistration:
    """轻量方法注册模型 (不可变，便于缓存/哈希)

    component_type: 逻辑组件类别 (datahub / data_engine / business_engine / ...)
    engine_type:    具体实现分类 (pandas / polars / tushare / etc.)
    engine_name:    对外暴露的方法名 (用户调用)
    priority:       调度优先级 (越大越先被 default 策略选中)
    deprecated:     是否弃用
    version:        语义版本号 (用于最新/稳定策略)
    tags:           业务/技术标签
    signature:      函数签名字符串(缓存，避免反射热点)
    module_path:    源模块路径
    callable:       真实可调用对象 (lazy 模式下可为 None, 需要 loader 绑定)
    """

    component_type: str
    engine_type: str
    engine_name: str
    description: str = ""
    version: str = "1.0.0"
    callable: Optional[Callable] = None
    tags: Tuple[str, ...] = tuple()
    deprecated: bool = False
    priority: int = 0
    signature: str = ""
    module_path: str = ""
    registered_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def full_key(self) -> str:
        return f"{self.component_type}::{self.engine_type}::{self.engine_name}"

    # （已移除旧兼容别名 callable_method）
