"""
Probe Output Builders
=====================

构建器：将探针执行结果转换为标准化的 ProbeOutputs。

核心职责：
1. 从探针结果字典构建 ProbeOutputs
2. 处理缺失探针的降级策略
3. 验证探针结果完整性
4. 提供流式构建 API

使用示例：
    # 基本使用
    builder = ProbeOutputBuilder("roic")
    builder.with_log_trend(log_trend_result)
    builder.with_volatility(volatility_result)
    outputs = builder.build()

    # 链式调用
    outputs = (
        ProbeOutputBuilder("roic")
        .with_log_trend(log_trend_result)
        .with_volatility(volatility_result)
        .with_raw_values(values)
        .build()
    )

    # 从引擎结果构建
    results = engine.run_all(values)
    outputs = ProbeOutputBuilder.from_engine_results("roic", results, values)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    # 避免循环导入
    pass

logger = logging.getLogger(__name__)


# ============================================================================
# ProbeOutputs 数据模型（统一接口）
# ============================================================================

@dataclass
class ProbeOutputs:
    """
    单指标的探针输出集合（统一接口）

    这是 ProbeEngine 和所有评估器（ThresholdEvaluator, T.R.U.T.H.）
    之间的标准契约。

    Attributes:
        indicator_name: 指标名称（如 "roic", "roe"）
        log_trend: 对数趋势探针结果
        volatility: 波动性探针结果
        cyclical: 周期性探针结果
        deterioration: 恶化检测探针结果
        rolling: 滚动窗口探针结果
        robust: 稳健趋势探针结果
        inflection: 拐点检测探针结果
        multi_horizon: 多视野探针结果
        raw_values: 原始数据
        data_quality: 数据质量等级
        effective_years: 有效年数
    """
    indicator_name: str

    # 8 个核心探针结果
    log_trend: Optional[Any] = None  # LogTrendResult
    volatility: Optional[Any] = None  # VolatilityResult
    cyclical: Optional[Any] = None  # CyclicalPatternResult
    deterioration: Optional[Any] = None  # RecentDeteriorationResult
    rolling: Optional[Any] = None  # RollingTrendResult
    robust: Optional[Any] = None  # RobustTrendResult
    inflection: Optional[Any] = None  # InflectionResult
    multi_horizon: Optional[Any] = None  # MultiHorizonResult

    # 原始数据（用于降级计算）
    raw_values: Optional[np.ndarray] = None

    # 元信息
    data_quality: str = "unknown"
    effective_years: int = 0

    def has_core_probes(self) -> bool:
        """检查核心探针是否齐全"""
        return all([
            self.log_trend is not None,
            self.volatility is not None,
            self.cyclical is not None,
            self.deterioration is not None,
        ])

    def has_all_probes(self) -> bool:
        """检查所有探针是否齐全"""
        return all([
            self.log_trend is not None,
            self.volatility is not None,
            self.cyclical is not None,
            self.deterioration is not None,
            self.rolling is not None,
            self.robust is not None,
            self.inflection is not None,
        ])

    def missing_probes(self) -> List[str]:
        """列出缺失的探针"""
        probes = {
            "log_trend": self.log_trend,
            "volatility": self.volatility,
            "cyclical": self.cyclical,
            "deterioration": self.deterioration,
            "rolling": self.rolling,
            "robust": self.robust,
            "inflection": self.inflection,
        }
        return [name for name, result in probes.items() if result is None]

    def available_probes(self) -> List[str]:
        """列出可用的探针"""
        probes = {
            "log_trend": self.log_trend,
            "volatility": self.volatility,
            "cyclical": self.cyclical,
            "deterioration": self.deterioration,
            "rolling": self.rolling,
            "robust": self.robust,
            "inflection": self.inflection,
            "multi_horizon": self.multi_horizon,
        }
        return [name for name, result in probes.items() if result is not None]

    def get_probe(self, name: str) -> Optional[Any]:
        """获取指定探针结果"""
        return getattr(self, name, None)

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典（用于报告生成）"""
        def safe_to_dict(obj):
            if obj is None:
                return None
            if hasattr(obj, "to_dict"):
                return obj.to_dict()
            return str(obj)

        return {
            "indicator_name": self.indicator_name,
            "log_trend": safe_to_dict(self.log_trend),
            "volatility": safe_to_dict(self.volatility),
            "cyclical": safe_to_dict(self.cyclical),
            "deterioration": safe_to_dict(self.deterioration),
            "rolling": safe_to_dict(self.rolling),
            "robust": safe_to_dict(self.robust),
            "inflection": safe_to_dict(self.inflection),
            "multi_horizon": safe_to_dict(self.multi_horizon),
            "data_quality": self.data_quality,
            "effective_years": self.effective_years,
            "available_probes": self.available_probes(),
            "missing_probes": self.missing_probes(),
        }

    def summary(self) -> Dict[str, Any]:
        """获取摘要信息"""
        return {
            "indicator": self.indicator_name,
            "core_complete": self.has_core_probes(),
            "all_complete": self.has_all_probes(),
            "available": len(self.available_probes()),
            "missing": len(self.missing_probes()),
            "data_quality": self.data_quality,
            "effective_years": self.effective_years,
        }


@dataclass
class MultiIndicatorProbeOutputs:
    """
    公司级多指标探针输出

    包含一个公司所有指标的探针结果。
    这是 T.R.U.T.H. 基因计算和 ThresholdEvaluator 公司评估的输入。

    Attributes:
        company_code: 公司代码
        company_name: 公司名称
        roic, roe, ...: 各指标的 ProbeOutputs
        total_assets, equity, ...: 辅助财务数据
    """
    company_code: str
    company_name: str = ""

    # 核心财务指标
    roic: Optional[ProbeOutputs] = None
    roe: Optional[ProbeOutputs] = None
    roiic: Optional[ProbeOutputs] = None
    gross_margin: Optional[ProbeOutputs] = None
    net_margin: Optional[ProbeOutputs] = None
    revenue: Optional[ProbeOutputs] = None
    net_profit: Optional[ProbeOutputs] = None
    ocf: Optional[ProbeOutputs] = None
    fcf: Optional[ProbeOutputs] = None

    # 辅助财务数据（用于 δ_fraud 和 V 因子）
    total_assets: float = 0.0
    equity: float = 0.0
    goodwill: float = 0.0
    receivables: float = 0.0
    related_party_transactions: float = 0.0
    advance_receipts: float = 0.0
    inventory: float = 0.0

    def get_indicator(self, name: str) -> Optional[ProbeOutputs]:
        """获取指定指标的探针输出"""
        return getattr(self, name.lower(), None)

    def set_indicator(self, name: str, outputs: ProbeOutputs) -> None:
        """设置指定指标的探针输出"""
        if hasattr(self, name.lower()):
            setattr(self, name.lower(), outputs)

    def list_available_indicators(self) -> List[str]:
        """列出可用的指标"""
        indicators = [
            "roic", "roe", "roiic", "gross_margin", "net_margin",
            "revenue", "net_profit", "ocf", "fcf"
        ]
        return [i for i in indicators if getattr(self, i) is not None]

    def list_missing_indicators(self) -> List[str]:
        """列出缺失的指标"""
        indicators = [
            "roic", "roe", "roiic", "gross_margin", "net_margin",
            "revenue", "net_profit", "ocf", "fcf"
        ]
        return [i for i in indicators if getattr(self, i) is None]

    def list_missing(self) -> Dict[str, List[str]]:
        """
        列出各指标缺失的探针

        Returns:
            Dict[指标名, 缺失的探针列表]
            若指标本身缺失，值为 ['all']

        Example:
            {
                'roic': ['rolling', 'robust'],  # roic 缺少 rolling 和 robust
                'roe': ['all'],  # roe 整个指标缺失
            }
        """
        result = {}
        indicators = [
            "roic", "roe", "roiic", "gross_margin", "net_margin",
            "revenue", "net_profit", "ocf", "fcf"
        ]
        for name in indicators:
            outputs = getattr(self, name)
            if outputs is None:
                result[name] = ['all']
            else:
                missing = outputs.missing_probes()
                if missing:
                    result[name] = missing
        return result

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        result = {
            "company_code": self.company_code,
            "company_name": self.company_name,
            "available_indicators": self.list_available_indicators(),
            "missing_indicators": self.list_missing_indicators(),
        }

        for indicator in self.list_available_indicators():
            outputs = getattr(self, indicator)
            if outputs:
                result[indicator] = outputs.to_dict()

        return result

    def summary(self) -> Dict[str, Any]:
        """获取摘要"""
        return {
            "company_code": self.company_code,
            "company_name": self.company_name,
            "indicators_available": len(self.list_available_indicators()),
            "indicators_missing": len(self.list_missing_indicators()),
            "available": self.list_available_indicators(),
        }


# ============================================================================
# 构建器
# ============================================================================

class ProbeOutputBuilder:
    """
    ProbeOutputs 构建器

    提供流式 API 来构建 ProbeOutputs。

    Example:
        outputs = (
            ProbeOutputBuilder("roic")
            .with_log_trend(log_trend_result)
            .with_volatility(volatility_result)
            .with_raw_values(values)
            .build()
        )
    """

    def __init__(self, indicator_name: str):
        self._indicator_name = indicator_name
        self._log_trend: Optional[Any] = None
        self._volatility: Optional[Any] = None
        self._cyclical: Optional[Any] = None
        self._deterioration: Optional[Any] = None
        self._rolling: Optional[Any] = None
        self._robust: Optional[Any] = None
        self._inflection: Optional[Any] = None
        self._multi_horizon: Optional[Any] = None
        self._raw_values: Optional[np.ndarray] = None
        self._data_quality: str = "unknown"
        self._effective_years: int = 0

    def with_log_trend(self, result: Any) -> "ProbeOutputBuilder":
        """添加对数趋势探针结果"""
        self._log_trend = result
        return self

    def with_volatility(self, result: Any) -> "ProbeOutputBuilder":
        """添加波动性探针结果"""
        self._volatility = result
        return self

    def with_cyclical(self, result: Any) -> "ProbeOutputBuilder":
        """添加周期性探针结果"""
        self._cyclical = result
        return self

    def with_deterioration(self, result: Any) -> "ProbeOutputBuilder":
        """添加恶化检测探针结果"""
        self._deterioration = result
        return self

    def with_rolling(self, result: Any) -> "ProbeOutputBuilder":
        """添加滚动窗口探针结果"""
        self._rolling = result
        return self

    def with_robust(self, result: Any) -> "ProbeOutputBuilder":
        """添加稳健趋势探针结果"""
        self._robust = result
        return self

    def with_inflection(self, result: Any) -> "ProbeOutputBuilder":
        """添加拐点检测探针结果"""
        self._inflection = result
        return self

    def with_multi_horizon(self, result: Any) -> "ProbeOutputBuilder":
        """添加多视野探针结果"""
        self._multi_horizon = result
        return self

    def with_raw_values(self, values: np.ndarray) -> "ProbeOutputBuilder":
        """添加原始数据"""
        self._raw_values = values
        if values is not None:
            self._effective_years = len(values)
        return self

    def with_data_quality(self, quality: str) -> "ProbeOutputBuilder":
        """设置数据质量"""
        self._data_quality = quality
        return self

    def with_effective_years(self, years: int) -> "ProbeOutputBuilder":
        """设置有效年数"""
        self._effective_years = years
        return self

    def build(self) -> ProbeOutputs:
        """构建 ProbeOutputs"""
        return ProbeOutputs(
            indicator_name=self._indicator_name,
            log_trend=self._log_trend,
            volatility=self._volatility,
            cyclical=self._cyclical,
            deterioration=self._deterioration,
            rolling=self._rolling,
            robust=self._robust,
            inflection=self._inflection,
            multi_horizon=self._multi_horizon,
            raw_values=self._raw_values,
            data_quality=self._data_quality,
            effective_years=self._effective_years,
        )

    @classmethod
    def from_engine_results(
        cls,
        indicator_name: str,
        results: Dict[str, Any],
        raw_values: Optional[np.ndarray] = None,
    ) -> ProbeOutputs:
        """
        从引擎结果构建 ProbeOutputs

        Args:
            indicator_name: 指标名称
            results: engine.run_all() 返回的结果字典
            raw_values: 原始数据（可选）

        Returns:
            ProbeOutputs
        """
        builder = cls(indicator_name)

        # 映射探针名称到构建器方法
        probe_map = {
            "log_trend": builder.with_log_trend,
            "volatility": builder.with_volatility,
            "cyclical": builder.with_cyclical,
            "deterioration": builder.with_deterioration,
            "rolling": builder.with_rolling,
            "robust": builder.with_robust,
            "inflection": builder.with_inflection,
            "multi_horizon": builder.with_multi_horizon,
        }

        for probe_name, result in results.items():
            if probe_name in probe_map:
                probe_map[probe_name](result)

        if raw_values is not None:
            builder.with_raw_values(raw_values)

        return builder.build()


class MultiIndicatorProbeOutputBuilder:
    """
    MultiIndicatorProbeOutputs 构建器

    用于构建公司级的多指标探针输出。

    Example:
        outputs = (
            MultiIndicatorProbeOutputBuilder("000001.SZ", "平安银行")
            .with_indicator("roic", roic_outputs)
            .with_indicator("roe", roe_outputs)
            .with_auxiliary_data(total_assets=1000000)
            .build()
        )
    """

    def __init__(self, company_code: str, company_name: str = ""):
        self._company_code = company_code
        self._company_name = company_name
        self._indicators: Dict[str, ProbeOutputs] = {}
        self._auxiliary_data: Dict[str, float] = {}

    def with_indicator(
        self,
        name: str,
        outputs: ProbeOutputs,
    ) -> "MultiIndicatorProbeOutputBuilder":
        """添加指标探针输出"""
        self._indicators[name.lower()] = outputs
        return self

    def with_auxiliary_data(self, **kwargs) -> "MultiIndicatorProbeOutputBuilder":
        """添加辅助财务数据"""
        self._auxiliary_data.update(kwargs)
        return self

    def build(self) -> MultiIndicatorProbeOutputs:
        """构建 MultiIndicatorProbeOutputs"""
        result = MultiIndicatorProbeOutputs(
            company_code=self._company_code,
            company_name=self._company_name,
        )

        # 设置各指标
        for name, outputs in self._indicators.items():
            result.set_indicator(name, outputs)

        # 设置辅助数据
        for key, value in self._auxiliary_data.items():
            if hasattr(result, key):
                setattr(result, key, value)

        return result

    @classmethod
    def from_batch_results(
        cls,
        company_code: str,
        company_name: str,
        batch_results: Dict[str, Dict[str, Any]],
        raw_data: Optional[Dict[str, np.ndarray]] = None,
    ) -> MultiIndicatorProbeOutputs:
        """
        从批量引擎结果构建

        Args:
            company_code: 公司代码
            company_name: 公司名称
            batch_results: engine.run_batch() 返回的结果
            raw_data: 原始数据字典 {indicator: values}

        Returns:
            MultiIndicatorProbeOutputs
        """
        builder = cls(company_code, company_name)

        for indicator_name, probe_results in batch_results.items():
            raw_values = None
            if raw_data and indicator_name in raw_data:
                raw_values = raw_data[indicator_name]

            outputs = ProbeOutputBuilder.from_engine_results(
                indicator_name,
                probe_results,
                raw_values,
            )
            builder.with_indicator(indicator_name, outputs)

        return builder.build()
