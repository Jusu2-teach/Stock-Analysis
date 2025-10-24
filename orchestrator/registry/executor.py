from __future__ import annotations
from typing import Any, Iterable
import os
from ..errors import RegistryExecutionError
from ..models import MethodRegistration
from .metrics import MetricsService


class MethodExecutor:
    def __init__(self, metrics: MetricsService):
        self.metrics = metrics

    def _validate_input_style(self, reg: MethodRegistration, args: tuple, kwargs: dict):
        """统一输入风格强制校验。

        规则 (ASTOCK_INPUT_STYLE 环境变量控制, 默认 strict_single):
        - strict_single:  仅允许直接按照函数签名传参；禁止将单元素 list/tuple 作为唯一 *args 传入以“模拟”单一数据对象。
                         若检测到这样做且目标函数第一个参数不是 list/Iterable 类型，则抛出错误提示明确风格。
        - allow_list:     放宽限制，不做任何校验 (兼容模式)。
        - enforce_list:   要求所有数据类第一个位置参数必须是 list/tuple (若函数形参第一个不是 Iterable, 则警告)。
        """
        mode = os.getenv('ASTOCK_INPUT_STYLE', 'strict_single').lower()
        if mode == 'allow_list':
            return
        if reg.callable is None:
            return
        try:
            import inspect
            sig = inspect.signature(reg.callable)
            params = list(sig.parameters.values())
        except Exception:
            return

        if not params:
            return

        # 提取第一个形参用于风格判定
        first_param = params[0]
        # 仅在存在位置参数传递时校验
        if args:
            first_arg = args[0]
            if mode == 'strict_single':
                if isinstance(first_arg, (list, tuple)) and len(args) == 1:
                    # 若目标函数第一参数标注为 list/Iterable 则允许
                    ann = first_param.annotation
                    ann_name = getattr(ann, '__name__', str(ann)) if ann is not inspect._empty else ''
                    if ann not in (list, tuple) and 'List' not in ann_name and 'Iterable' not in ann_name:
                        raise RegistryExecutionError(
                            f"输入风格违规(strict_single): {reg.full_key} 收到单元素 list/tuple 作为唯一位置参数. "
                            f"请直接展开传参或设置 ASTOCK_INPUT_STYLE=allow_list 以临时放宽。")
            elif mode == 'enforce_list':
                # 要求首参数必须是 list/tuple
                if not isinstance(first_arg, (list, tuple)):
                    raise RegistryExecutionError(
                        f"输入风格违规(enforce_list): {reg.full_key} 要求第一个位置参数为 list/tuple, 实际为 {type(first_arg).__name__}")

    def execute(self, reg: MethodRegistration, *args, **kwargs) -> Any:
        if reg.callable is None:
            raise RegistryExecutionError(f"callable not bound for {reg.full_key}")
        # 输入风格校验
        self._validate_input_style(reg, args, kwargs)
        result, dt, err = self.metrics.wrap_execute(reg.full_key, reg.engine_name, reg.callable, *args, **kwargs)
        if err:
            raise RegistryExecutionError(str(err)) from err
        return result
