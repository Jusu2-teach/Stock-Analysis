"""
指标类型适配器 (Metric Type Adapter)
=====================================

将指标档案（MetricProfile）转换为趋势分析引擎可使用的参数配置。
实现"一套引擎，多种策略"的适配层。

核心功能：
1. 根据指标类型自动选择合适的 Probe 组合
2. 动态调整规则参数（阈值、权重）
3. 生成指标专属的评分函数

使用方式：
    adapter = MetricAdapter(metric_name="roic")
    config = adapter.get_analysis_config()
    probes = adapter.get_recommended_probes()
    rules = adapter.get_adapted_rules()

作者: AStock Analysis System
日期: 2025-12-06
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Callable, Type
import logging

from .metric_profiles import (
    MetricCategory,
    MetricProfile,
    METRIC_PROFILES,
    get_metric_profile,
    detect_metric_category,
)

logger = logging.getLogger(__name__)


@dataclass
class AdaptedConfig:
    """
    适配后的分析配置

    包含趋势分析引擎所需的所有参数
    """
    # 基本信息
    metric_name: str
    category: MetricCategory

    # Probe配置
    use_log_transform: bool
    time_weights: List[float]

    # 规则阈值
    min_latest_value: Optional[float]
    severe_decline_slope: float
    mild_decline_slope: float
    penalty_factor: float
    max_penalty: float

    # CV（变异系数）阈值
    cv_high: float
    cv_low: float

    # 策略参数
    high_growth_threshold: float
    moat_threshold: Optional[float]
    moat_stability_required: bool

    # 交叉验证
    reference_metrics: List[str]
    quality_check_metrics: List[str]

    # 特殊标志
    is_auxiliary: bool
    cyclical_sensitive: bool
    allow_negative: bool


class MetricAdapter:
    """
    指标适配器

    将 MetricProfile 转换为分析引擎可用的配置
    """

    def __init__(self, metric_name: str, profile: Optional[MetricProfile] = None):
        """
        初始化适配器

        Args:
            metric_name: 指标名称
            profile: 可选的预定义档案，不传则自动查找
        """
        self.metric_name = metric_name.lower()
        self.profile = profile or get_metric_profile(metric_name)

    def get_adapted_config(self) -> AdaptedConfig:
        """
        获取适配后的完整配置
        """
        p = self.profile

        return AdaptedConfig(
            metric_name=self.metric_name,
            category=p.category,
            use_log_transform=p.use_log_transform,
            time_weights=p.time_weights.copy(),
            min_latest_value=p.min_latest_value,
            severe_decline_slope=p.severe_decline_slope,
            mild_decline_slope=p.mild_decline_slope,
            penalty_factor=p.penalty_factor,
            max_penalty=min(p.penalty_factor, 25),
            cv_high=p.cv_threshold_high,
            cv_low=p.cv_threshold_low,
            high_growth_threshold=p.high_growth_threshold,
            moat_threshold=p.moat_threshold,
            moat_stability_required=p.moat_stability_required,
            reference_metrics=p.reference_metrics.copy(),
            quality_check_metrics=p.quality_check_metrics.copy(),
            is_auxiliary=p.is_auxiliary,
            cyclical_sensitive=p.cyclical_sensitive,
            allow_negative=p.allow_negative,
        )

    def get_probe_weights(self) -> Dict[str, float]:
        """
        根据指标类型返回各 Probe 的权重配置

        不同指标对各种分析维度的敏感性不同：
        - 效率指标：更看重稳定性（CV）和绝对水平
        - 规模指标：更看重增速和加速度
        - 现金流：更看重正负转换和恶化检测
        - 增量指标：更看重趋势方向，容忍高波动
        """
        category = self.profile.category

        if category == MetricCategory.EFFICIENCY:
            return {
                "log_trend": 0.30,       # 整体趋势
                "robust": 0.25,          # 稳健趋势（抗异常值）
                "volatility": 0.20,      # 稳定性很重要
                "rolling": 0.10,         # 加速度次要
                "deterioration": 0.10,   # 恶化检测
                "inflection": 0.05,      # 拐点检测权重低
            }

        elif category == MetricCategory.SCALE:
            return {
                "log_trend": 0.35,       # 增长率最重要
                "rolling": 0.25,         # 加速度很重要
                "robust": 0.15,          # 稳健辅助
                "deterioration": 0.10,   # 恶化检测
                "volatility": 0.10,      # 波动容忍度高
                "inflection": 0.05,
            }

        elif category == MetricCategory.CASH_FLOW:
            return {
                "deterioration": 0.30,   # 现金流恶化是最大风险
                "log_trend": 0.25,       # 趋势方向
                "rolling": 0.20,         # 变化速度
                "robust": 0.15,          # 稳健估计
                "volatility": 0.05,      # 现金流波动正常
                "inflection": 0.05,
            }

        elif category == MetricCategory.INCREMENTAL:
            return {
                "log_trend": 0.40,       # 趋势方向最重要
                "robust": 0.30,          # 抗异常值
                "rolling": 0.20,         # 变化趋势
                "volatility": 0.05,      # 高波动正常，权重低
                "deterioration": 0.05,
                "inflection": 0.00,      # 不看拐点
            }

        else:  # UNKNOWN
            return {
                "log_trend": 0.30,
                "robust": 0.20,
                "rolling": 0.20,
                "volatility": 0.15,
                "deterioration": 0.10,
                "inflection": 0.05,
            }

    def get_recommended_probes(self) -> List[str]:
        """
        获取推荐启用的 Probe 列表（按重要性排序）
        """
        weights = self.get_probe_weights()
        sorted_probes = sorted(weights.items(), key=lambda x: -x[1])
        # 只返回权重>0的Probe
        return [name for name, weight in sorted_probes if weight > 0]

    def should_skip_rule(self, rule_name: str) -> bool:
        """
        判断某条规则是否应该跳过

        例如：
        - ROIIC作为辅助指标，跳过一票否决规则
        - 效率指标跳过"增速过快"警告
        """
        p = self.profile

        # 辅助指标跳过否决规则
        if p.is_auxiliary:
            veto_rules = [
                "rule_veto_if_terminal_low",
                "rule_veto_if_severe_log_decline",
                "rule_veto_if_double_negative",
                "rule_veto_if_multi_year_negative",
            ]
            if rule_name in veto_rules:
                logger.debug(f"[{self.metric_name}] 辅助指标跳过否决规则: {rule_name}")
                return True

        # 效率指标不适用某些规模指标规则
        if p.category == MetricCategory.EFFICIENCY:
            scale_rules = [
                "rule_bonus_explosive_growth",  # 效率指标不追求爆发增长
            ]
            if rule_name in scale_rules:
                return True

        return False

    def adjust_rule_params(self, rule_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        根据指标类型调整规则参数

        Args:
            rule_name: 规则名称
            params: 原始参数

        Returns:
            调整后的参数
        """
        p = self.profile
        adjusted = params.copy()

        # 根据指标类型调整斜率阈值
        if "severe_slope" in adjusted:
            adjusted["severe_slope"] = p.severe_decline_slope
        if "mild_slope" in adjusted:
            adjusted["mild_slope"] = p.mild_decline_slope

        # 调整扣分系数
        if "penalty_factor" in adjusted:
            adjusted["penalty_factor"] = p.penalty_factor

        # 调整最新值门槛
        if "min_latest_value" in adjusted and p.min_latest_value is not None:
            adjusted["min_latest_value"] = p.min_latest_value

        # 调整CV阈值
        if "cv_threshold" in adjusted:
            adjusted["cv_threshold"] = p.cv_threshold_high

        return adjusted

    def get_veto_config(self) -> Dict[str, Any]:
        """
        获取否决规则配置

        返回适用于该指标的否决条件
        """
        p = self.profile

        config = {
            "enabled": not p.is_auxiliary,  # 辅助指标不启用否决
            "min_latest_value": p.min_latest_value,
            "max_decline_pct": p.max_decline_pct,
            "severe_slope_threshold": p.severe_decline_slope,
            "max_loss_years": p.max_loss_years if p.allow_negative else 0,
        }

        return config

    def get_scoring_adjustments(self) -> Dict[str, float]:
        """
        获取评分调整系数

        用于最终评分时对不同指标的贡献进行加权
        """
        p = self.profile

        # 基础权重（辅助指标降权）
        base_weight = 0.5 if p.is_auxiliary else 1.0

        # 护城河加成（效率指标且有护城河门槛）
        moat_bonus = 0.2 if p.moat_threshold else 0.0

        # 周期敏感度调整（周期敏感指标在周期底部可能需要调整）
        cycle_factor = 0.9 if p.cyclical_sensitive else 1.0

        return {
            "base_weight": base_weight,
            "moat_bonus": moat_bonus,
            "cycle_factor": cycle_factor,
            "final_weight": base_weight * (1 + moat_bonus) * cycle_factor,
        }


class AdapterFactory:
    """
    适配器工厂

    提供批量创建和管理适配器的功能
    """

    _cache: Dict[str, MetricAdapter] = {}

    @classmethod
    def get_adapter(cls, metric_name: str) -> MetricAdapter:
        """
        获取指标适配器（带缓存）
        """
        key = metric_name.lower()
        if key not in cls._cache:
            cls._cache[key] = MetricAdapter(key)
        return cls._cache[key]

    @classmethod
    def get_adapters_for_category(cls, category: MetricCategory) -> List[MetricAdapter]:
        """
        获取某个类别下所有预定义指标的适配器
        """
        adapters = []
        for name, profile in METRIC_PROFILES.items():
            if profile.category == category:
                adapters.append(cls.get_adapter(name))
        return adapters

    @classmethod
    def get_all_adapters(cls) -> Dict[str, MetricAdapter]:
        """
        获取所有预定义指标的适配器
        """
        return {name: cls.get_adapter(name) for name in METRIC_PROFILES.keys()}

    @classmethod
    def clear_cache(cls):
        """清除缓存"""
        cls._cache.clear()


def create_metric_config_for_pipeline(metric_name: str,
                                       custom_overrides: Optional[Dict] = None) -> Dict[str, Any]:
    """
    为Pipeline创建指标配置

    这是供 workflow YAML 或 Python 调用的高级接口。

    Args:
        metric_name: 指标名称
        custom_overrides: 自定义覆盖参数

    Returns:
        可直接传给趋势分析引擎的配置字典
    """
    adapter = AdapterFactory.get_adapter(metric_name)
    config = adapter.get_adapted_config()

    result = {
        "metric_name": config.metric_name,
        "category": config.category.value,

        # 分析参数
        "use_log_transform": config.use_log_transform,
        "time_weights": config.time_weights,

        # 规则阈值
        "min_latest_value": config.min_latest_value,
        "log_severe_decline_slope": config.severe_decline_slope,
        "log_mild_decline_slope": config.mild_decline_slope,
        "penalty_factor": config.penalty_factor,
        "max_penalty": config.max_penalty,

        # 交叉验证
        "reference_metrics": config.reference_metrics,

        # Probe权重
        "probe_weights": adapter.get_probe_weights(),

        # 特殊标志
        "is_auxiliary": config.is_auxiliary,
        "cyclical_sensitive": config.cyclical_sensitive,
    }

    # 应用自定义覆盖
    if custom_overrides:
        result.update(custom_overrides)

    return result


# ==============================================================================
# 导出
# ==============================================================================

__all__ = [
    "AdaptedConfig",
    "MetricAdapter",
    "AdapterFactory",
    "create_metric_config_for_pipeline",
]
