"""
HookSpec 类型安全系统
====================

参考 pytest/pluggy 的 HookSpec 设计，提供：
1. 事件规格定义
2. 处理器签名验证
3. 参数类型检查

使用示例：

    # 定义事件规格
    HookSpecRegistry.define(
        "pipeline.node.execute",
        required_args=('step_name', 'inputs'),
        optional_args=('context',),
        firstresult=False
    )
    
    # 验证处理器
    valid, errors = HookSpecRegistry.validate_handler(
        "pipeline.node.execute",
        my_handler
    )
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import (
    Callable, Dict, List, Optional, Any, Tuple,
    TypeVar, Protocol, runtime_checkable
)
import inspect
import logging

logger = logging.getLogger(__name__)

E = TypeVar('E')


@runtime_checkable
class EventSpec(Protocol[E]):
    """事件规格协议 - 定义事件的类型签名"""
    
    @property
    def event_type(self) -> str:
        """事件类型标识"""
        ...
    
    def validate(self, event: E) -> bool:
        """验证事件是否符合规格"""
        ...


@dataclass(frozen=True)
class HookSpec:
    """钩子规格定义
    
    类似 Pluggy 的 HookspecMarker，定义事件处理器的规格约束。
    
    Attributes:
        name: 事件类型名称
        firstresult: 是否只取第一个非 None 结果
        historic: 是否为历史事件（新订阅者会收到历史事件）
        warn_on_impl: 实现时是否警告
        required_args: 必需参数列表
        optional_args: 可选参数列表
        return_type: 期望的返回类型
        description: 规格描述
    """
    name: str
    firstresult: bool = False
    historic: bool = False
    warn_on_impl: bool = False
    required_args: Tuple[str, ...] = ()
    optional_args: Tuple[str, ...] = ()
    return_type: Optional[type] = None
    description: str = ""
    
    @property
    def all_args(self) -> Tuple[str, ...]:
        """所有参数"""
        return self.required_args + self.optional_args


class HookSpecRegistry:
    """钩子规格注册表
    
    管理所有事件类型的规格定义，提供验证功能。
    """
    
    _specs: Dict[str, HookSpec] = {}
    _strict_mode: bool = False
    
    @classmethod
    def define(
        cls,
        name: str,
        *,
        firstresult: bool = False,
        historic: bool = False,
        warn_on_impl: bool = False,
        required_args: Tuple[str, ...] = (),
        optional_args: Tuple[str, ...] = (),
        return_type: Optional[type] = None,
        description: str = ""
    ) -> HookSpec:
        """定义钩子规格
        
        Args:
            name: 事件类型名称
            firstresult: 是否只取第一个非 None 结果
            historic: 是否为历史事件
            warn_on_impl: 实现时是否警告
            required_args: 必需参数
            optional_args: 可选参数
            return_type: 返回类型
            description: 描述
            
        Returns:
            创建的 HookSpec 对象
        """
        spec = HookSpec(
            name=name,
            firstresult=firstresult,
            historic=historic,
            warn_on_impl=warn_on_impl,
            required_args=required_args,
            optional_args=optional_args,
            return_type=return_type,
            description=description
        )
        cls._specs[name] = spec
        logger.debug(f"📋 HookSpec defined: {name}")
        return spec
    
    @classmethod
    def get(cls, name: str) -> Optional[HookSpec]:
        """获取规格定义"""
        return cls._specs.get(name)
    
    @classmethod
    def has_spec(cls, name: str) -> bool:
        """是否存在规格定义"""
        return name in cls._specs
    
    @classmethod
    def is_historic(cls, name: str) -> bool:
        """是否为历史事件类型"""
        spec = cls._specs.get(name)
        return spec.historic if spec else False
    
    @classmethod
    def is_firstresult(cls, name: str) -> bool:
        """是否为首结果模式"""
        spec = cls._specs.get(name)
        return spec.firstresult if spec else False
    
    @classmethod
    def validate_handler(
        cls,
        event_type: str,
        handler: Callable,
        strict: bool = False
    ) -> Tuple[bool, List[str]]:
        """验证处理器签名是否符合规格
        
        Args:
            event_type: 事件类型
            handler: 处理器函数
            strict: 严格模式（额外检查）
            
        Returns:
            (是否有效, 错误信息列表)
        """
        if event_type not in cls._specs:
            return True, []  # 无规格定义，默认通过
        
        spec = cls._specs[event_type]
        errors: List[str] = []
        
        try:
            sig = inspect.signature(handler)
            param_names = set(sig.parameters.keys())
            
            # 检查必需参数
            for arg in spec.required_args:
                if arg not in param_names:
                    # 检查是否有 **kwargs
                    has_var_keyword = any(
                        p.kind == inspect.Parameter.VAR_KEYWORD
                        for p in sig.parameters.values()
                    )
                    if not has_var_keyword:
                        errors.append(f"Missing required argument: {arg}")
            
            # 严格模式：检查额外参数
            if strict:
                allowed_args = set(spec.all_args) | {'event', 'self', 'cls'}
                extra_args = param_names - allowed_args
                
                # 排除 *args, **kwargs
                for name in list(extra_args):
                    param = sig.parameters[name]
                    if param.kind in (
                        inspect.Parameter.VAR_POSITIONAL,
                        inspect.Parameter.VAR_KEYWORD
                    ):
                        extra_args.discard(name)
                
                if extra_args:
                    errors.append(f"Unexpected arguments: {extra_args}")
            
            # 警告日志
            if spec.warn_on_impl and not errors:
                logger.warning(
                    f"⚠️ Handler '{handler.__name__}' implements deprecated "
                    f"hook '{event_type}'"
                )
                
        except Exception as e:
            errors.append(f"Signature inspection failed: {e}")
        
        return len(errors) == 0, errors
    
    @classmethod
    def validate_event(cls, event: Any) -> Tuple[bool, List[str]]:
        """验证事件对象是否符合规格
        
        Args:
            event: 事件对象
            
        Returns:
            (是否有效, 错误信息列表)
        """
        errors: List[str] = []
        
        if not hasattr(event, 'event_type'):
            errors.append("Event missing 'event_type' property")
            return False, errors
        
        event_type = event.event_type
        if event_type not in cls._specs:
            return True, []  # 无规格定义，默认通过
        
        spec = cls._specs[event_type]
        
        # 检查必需属性
        for arg in spec.required_args:
            if not hasattr(event, arg):
                errors.append(f"Event missing required attribute: {arg}")
        
        return len(errors) == 0, errors
    
    @classmethod
    def list_specs(cls) -> Dict[str, HookSpec]:
        """列出所有规格"""
        return dict(cls._specs)
    
    @classmethod
    def clear(cls):
        """清空规格（用于测试）"""
        cls._specs.clear()
    
    @classmethod
    def set_strict_mode(cls, enabled: bool):
        """设置全局严格模式"""
        cls._strict_mode = enabled


# ============================================================================
# 预定义规格
# ============================================================================

# Registry 事件
REGISTRY_METHOD_REGISTERED = HookSpecRegistry.define(
    "registry.method.registered",
    historic=True,  # 新订阅者会收到历史注册事件
    required_args=('component', 'method', 'engine_type'),
    optional_args=('version', 'priority', 'full_key'),
    description="方法注册事件"
)

REGISTRY_METHOD_EXECUTED = HookSpecRegistry.define(
    "registry.method.executed",
    required_args=('component', 'method', 'engine'),
    optional_args=('duration_ms', 'success', 'error'),
    description="方法执行事件"
)

# Pipeline 事件
PIPELINE_STARTED = HookSpecRegistry.define(
    "pipeline.flow.started",
    required_args=('pipeline_name',),
    optional_args=('config_path', 'total_steps', 'execution_order'),
    description="Pipeline 启动事件"
)

PIPELINE_COMPLETED = HookSpecRegistry.define(
    "pipeline.flow.completed",
    required_args=('pipeline_name', 'status'),
    optional_args=('duration_sec', 'executed_steps', 'failed_steps', 'error'),
    description="Pipeline 完成事件"
)

NODE_STARTED = HookSpecRegistry.define(
    "pipeline.node.started",
    required_args=('step_name',),
    optional_args=('pipeline_name', 'inputs', 'outputs', 'signature'),
    description="节点启动事件"
)

NODE_COMPLETED = HookSpecRegistry.define(
    "pipeline.node.completed",
    required_args=('step_name', 'status'),
    optional_args=('pipeline_name', 'duration_ms', 'output_count', 'error', 'metrics'),
    description="节点完成事件"
)

# 系统事件
SYSTEM_READY = HookSpecRegistry.define(
    "system.ready",
    historic=True,  # 新订阅者会收到系统就绪事件
    optional_args=('components', 'registered_methods', 'version'),
    description="系统就绪事件"
)

SYSTEM_ERROR = HookSpecRegistry.define(
    "system.error",
    required_args=('error_type', 'message'),
    optional_args=('component', 'stack_trace', 'context'),
    description="系统错误事件"
)
