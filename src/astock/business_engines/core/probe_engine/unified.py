"""
统一探针引擎 (Unified Probe Engine)
===================================

专业设计：解决探针接口不统一、硬编码字段等问题。

核心改进：
1. 统一探针协议：所有探针遵循相同的 `compute(values, **kwargs)` 签名
2. 动态结果容器：使用 dict 存储探针结果，而非硬编码字段
3. 元数据驱动适配：探针配置包含完整的方法映射和参数信息
4. 零特殊处理：消除所有 if-else 特殊适配逻辑

设计原则：
- DRY (Don't Repeat Yourself): 所有配置集中管理
- OCP (Open-Closed Principle): 新增探针只需添加配置，无需修改代码
- ISP (Interface Segregation): 探针只关心计算，适配逻辑由框架处理
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Protocol, TypeVar, Union

import numpy as np

logger = logging.getLogger(__name__)

T = TypeVar("T")


# ============================================================================
# 统一探针协议
# ============================================================================

class UnifiedProbeProtocol(Protocol):
    """
    统一探针协议

    所有探针必须遵循的最小接口：
    - name: 唯一标识
    - compute(values, **kwargs): 计算方法，接受数据和任意参数
    - default(**kwargs): 返回默认结果
    """

    @property
    def name(self) -> str:
        """探针唯一名称"""
        ...

    def compute(self, values: np.ndarray, **kwargs) -> Any:
        """执行计算"""
        ...

    def default(self, **kwargs) -> Any:
        """返回默认结果"""
        ...


# ============================================================================
# 探针适配描述符
# ============================================================================

@dataclass(frozen=True)
class ProbeSpec:
    """
    探针规格描述（不可变）

    描述如何将一个现有的 Calculator/Detector/Probe 适配为统一接口。

    Attributes:
        name: 探针唯一名称
        description: 描述
        category: 分类 (trend/volatility/cyclical/deterioration/...)
        min_points: 最小数据点数
        calculator_class: 计算器类
        compute_method: 计算方法名 (calculate/detect/compute/analyze)
        default_factory: 默认结果工厂函数
        extra_kwargs: 额外的默认参数 (如 context)
        kwargs_factory: 动态生成 kwargs 的函数 (可选)
    """
    name: str
    description: str
    category: str
    min_points: int
    calculator_class: type
    compute_method: str
    default_factory: Callable[[], Any]
    extra_kwargs: Dict[str, Any] = field(default_factory=dict)
    kwargs_factory: Optional[Callable[[np.ndarray], Dict[str, Any]]] = None


class UnifiedProbeAdapter:
    """
    统一探针适配器

    根据 ProbeSpec 将任意计算器适配为 UnifiedProbeProtocol。

    核心思想：
    - 配置驱动：所有适配逻辑由 ProbeSpec 描述
    - 零特殊处理：没有 if-else 分支判断
    - 延迟实例化：计算器在首次使用时创建
    """

    def __init__(self, spec: ProbeSpec):
        self._spec = spec
        self._calculator: Optional[Any] = None

    @property
    def name(self) -> str:
        return self._spec.name

    @property
    def description(self) -> str:
        return self._spec.description

    @property
    def category(self) -> str:
        return self._spec.category

    @property
    def min_points(self) -> int:
        return self._spec.min_points

    def _get_calculator(self) -> Any:
        """延迟获取计算器实例"""
        if self._calculator is None:
            self._calculator = self._spec.calculator_class()
        return self._calculator

    def _build_kwargs(self, values: np.ndarray, **user_kwargs) -> Dict[str, Any]:
        """构建完整的 kwargs"""
        # 1. 从 spec 获取默认 extra_kwargs
        kwargs = dict(self._spec.extra_kwargs)

        # 2. 如果有 kwargs_factory，调用它动态生成
        if self._spec.kwargs_factory:
            dynamic_kwargs = self._spec.kwargs_factory(values)
            kwargs.update(dynamic_kwargs)

        # 3. 用户传入的 kwargs 优先级最高
        kwargs.update(user_kwargs)

        return kwargs

    def compute(self, values: np.ndarray, **kwargs) -> Any:
        """
        执行计算

        自动处理：
        1. 数据验证
        2. 参数适配
        3. 方法调用
        """
        # 数据验证
        if values is None or len(values) < self._spec.min_points:
            return self.default(**kwargs)

        if np.all(np.isnan(values)):
            return self.default(**kwargs)

        try:
            calculator = self._get_calculator()
            method = getattr(calculator, self._spec.compute_method)

            # 构建完整的 kwargs
            full_kwargs = self._build_kwargs(values, **kwargs)

            # 转换为 list（某些探针需要 List[float]）
            values_list = values.tolist() if isinstance(values, np.ndarray) else list(values)

            return method(values_list, **full_kwargs)

        except Exception as e:
            logger.warning(f"Probe '{self.name}' failed: {e}")
            return self.default(**kwargs)

    def default(self, **kwargs) -> Any:
        """返回默认结果"""
        return self._spec.default_factory()

    def validate(self, values: np.ndarray) -> bool:
        """验证数据是否满足要求"""
        if values is None:
            return False
        if len(values) < self._spec.min_points:
            return False
        if np.all(np.isnan(values)):
            return False
        return True

    def __repr__(self) -> str:
        return f"UnifiedProbeAdapter({self.name})"


# ============================================================================
# 动态探针输出容器
# ============================================================================

@dataclass
class DynamicProbeOutputs:
    """
    动态探针输出容器

    不再硬编码探针字段，使用字典存储任意数量的探针结果。

    优势：
    - 新增探针无需修改此类
    - 支持任意探针组合
    - 便于序列化和扩展

    Example:
        outputs = DynamicProbeOutputs("roic")
        outputs.set("log_trend", log_trend_result)
        outputs.set("volatility", volatility_result)

        # 获取结果
        log_trend = outputs.get("log_trend")

        # 检查完整性
        missing = outputs.missing(["log_trend", "volatility", "cyclical"])
    """
    indicator_name: str
    _results: Dict[str, Any] = field(default_factory=dict)
    raw_values: Optional[np.ndarray] = None
    data_quality: str = "unknown"
    effective_years: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def set(self, probe_name: str, result: Any) -> "DynamicProbeOutputs":
        """设置探针结果（支持链式调用）"""
        self._results[probe_name] = result
        return self

    def get(self, probe_name: str, default: Any = None) -> Any:
        """获取探针结果"""
        return self._results.get(probe_name, default)

    def __getattr__(self, name: str) -> Any:
        """属性访问兼容（如 outputs.log_trend）"""
        if name.startswith('_') or name in ('indicator_name', 'raw_values', 'data_quality',
                                             'effective_years', 'metadata'):
            return super().__getattribute__(name)
        return self._results.get(name)

    def available(self) -> List[str]:
        """列出可用的探针"""
        return list(self._results.keys())

    def missing(self, required: List[str]) -> List[str]:
        """列出缺失的探针"""
        return [name for name in required if name not in self._results]

    def has(self, probe_name: str) -> bool:
        """检查是否有某探针结果"""
        return probe_name in self._results

    def has_all(self, probe_names: List[str]) -> bool:
        """检查是否有所有指定的探针结果"""
        return all(name in self._results for name in probe_names)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        def safe_to_dict(obj):
            if obj is None:
                return None
            if hasattr(obj, "to_dict"):
                return obj.to_dict()
            if hasattr(obj, "__dict__"):
                return obj.__dict__
            return str(obj)

        return {
            "indicator_name": self.indicator_name,
            "probes": {k: safe_to_dict(v) for k, v in self._results.items()},
            "available": self.available(),
            "data_quality": self.data_quality,
            "effective_years": self.effective_years,
            "metadata": self.metadata,
        }

    def summary(self) -> Dict[str, Any]:
        """获取摘要"""
        return {
            "indicator": self.indicator_name,
            "probe_count": len(self._results),
            "available": self.available(),
            "data_quality": self.data_quality,
            "effective_years": self.effective_years,
        }


@dataclass
class DynamicMultiIndicatorOutputs:
    """
    动态多指标输出容器

    同样不硬编码指标字段。
    """
    company_code: str
    company_name: str = ""
    _indicators: Dict[str, DynamicProbeOutputs] = field(default_factory=dict)
    auxiliary_data: Dict[str, float] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def set_indicator(self, name: str, outputs: DynamicProbeOutputs) -> "DynamicMultiIndicatorOutputs":
        """设置指标输出"""
        self._indicators[name] = outputs
        return self

    def get_indicator(self, name: str) -> Optional[DynamicProbeOutputs]:
        """获取指标输出"""
        return self._indicators.get(name)

    def __getattr__(self, name: str) -> Any:
        """属性访问兼容"""
        if name.startswith('_') or name in ('company_code', 'company_name',
                                             'auxiliary_data', 'metadata'):
            return super().__getattribute__(name)
        return self._indicators.get(name)

    def available_indicators(self) -> List[str]:
        """列出可用的指标"""
        return list(self._indicators.keys())

    def set_auxiliary(self, name: str, value: float) -> "DynamicMultiIndicatorOutputs":
        """设置辅助数据"""
        self.auxiliary_data[name] = value
        return self

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "company_code": self.company_code,
            "company_name": self.company_name,
            "indicators": {k: v.to_dict() for k, v in self._indicators.items()},
            "auxiliary_data": self.auxiliary_data,
            "metadata": self.metadata,
        }


# ============================================================================
# 统一探针引擎
# ============================================================================

class UnifiedProbeEngine:
    """
    统一探针引擎

    职责：
    1. 管理探针适配器
    2. 执行探针计算
    3. 构建输出容器

    Example:
        # 注册探针
        engine = UnifiedProbeEngine()
        engine.register(log_trend_spec)
        engine.register(volatility_spec)

        # 执行计算
        results = engine.run_all(values)

        # 构建输出
        outputs = engine.build_outputs("roic", values)
    """

    def __init__(self):
        self._adapters: Dict[str, UnifiedProbeAdapter] = {}
        self._categories: Dict[str, List[str]] = {}

    def register(self, spec: ProbeSpec) -> "UnifiedProbeEngine":
        """注册探针（支持链式调用）"""
        adapter = UnifiedProbeAdapter(spec)
        self._adapters[spec.name] = adapter

        # 更新分类索引
        if spec.category not in self._categories:
            self._categories[spec.category] = []
        self._categories[spec.category].append(spec.name)

        return self

    def register_many(self, specs: List[ProbeSpec]) -> "UnifiedProbeEngine":
        """批量注册探针"""
        for spec in specs:
            self.register(spec)
        return self

    def get(self, name: str) -> Optional[UnifiedProbeAdapter]:
        """获取探针适配器"""
        return self._adapters.get(name)

    def list_probes(self) -> List[str]:
        """列出所有探针名称"""
        return list(self._adapters.keys())

    def list_by_category(self, category: str) -> List[str]:
        """按分类列出探针"""
        return self._categories.get(category, [])

    def run_single(self, name: str, values: np.ndarray, **kwargs) -> Any:
        """运行单个探针"""
        adapter = self._adapters.get(name)
        if adapter is None:
            raise ValueError(f"Unknown probe: {name}")
        return adapter.compute(values, **kwargs)

    def run_all(self, values: np.ndarray, **kwargs) -> Dict[str, Any]:
        """运行所有探针"""
        results = {}
        for name, adapter in self._adapters.items():
            results[name] = adapter.compute(values, **kwargs)
        return results

    def run_selected(self, names: List[str], values: np.ndarray, **kwargs) -> Dict[str, Any]:
        """运行选定的探针"""
        results = {}
        for name in names:
            adapter = self._adapters.get(name)
            if adapter:
                results[name] = adapter.compute(values, **kwargs)
        return results

    def run_by_category(self, category: str, values: np.ndarray, **kwargs) -> Dict[str, Any]:
        """按分类运行探针"""
        names = self._categories.get(category, [])
        return self.run_selected(names, values, **kwargs)

    def build_outputs(
        self,
        indicator_name: str,
        values: np.ndarray,
        probe_names: Optional[List[str]] = None,
        **kwargs
    ) -> DynamicProbeOutputs:
        """
        构建探针输出

        Args:
            indicator_name: 指标名称
            values: 时间序列数据
            probe_names: 要运行的探针（None = 全部）
            **kwargs: 传递给探针的参数

        Returns:
            DynamicProbeOutputs 实例
        """
        outputs = DynamicProbeOutputs(indicator_name)
        outputs.raw_values = values
        outputs.effective_years = len(values) if values is not None else 0

        # 运行探针
        if probe_names:
            results = self.run_selected(probe_names, values, **kwargs)
        else:
            results = self.run_all(values, **kwargs)

        # 填充结果
        for name, result in results.items():
            outputs.set(name, result)

        return outputs

    def __repr__(self) -> str:
        return f"UnifiedProbeEngine(probes={len(self._adapters)})"


# ============================================================================
# 便捷工厂函数
# ============================================================================

def create_probe_spec(
    name: str,
    calculator_class: type,
    compute_method: str = "compute",
    default_factory: Callable[[], Any] = lambda: None,
    description: str = "",
    category: str = "general",
    min_points: int = 3,
    **extra_kwargs
) -> ProbeSpec:
    """
    便捷函数：创建 ProbeSpec

    Example:
        spec = create_probe_spec(
            name="log_trend",
            calculator_class=LogTrendProbe,
            compute_method="compute",
            default_factory=_empty_log_trend,
            category="trend",
        )
    """
    return ProbeSpec(
        name=name,
        description=description or f"{name} probe",
        category=category,
        min_points=min_points,
        calculator_class=calculator_class,
        compute_method=compute_method,
        default_factory=default_factory,
        extra_kwargs=extra_kwargs,
    )
