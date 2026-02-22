"""
═══════════════════════════════════════════════════════════════════════════════
PDDA 列名映射 - evaluators 和 truth 共用
═══════════════════════════════════════════════════════════════════════════════

PDDA (Pipeline-Driven Data Aggregation) 输出的标准列名。
trend 层输出格式: {metric}_{feature}，如 roic_slope, roe_cv

版本: 2.0.0
═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional


class PDDAColumns:
    """
    PDDA 输出列名常量

    trend 层输出格式: {metric}_{feature}
    例如: roic_slope, roic_cv, roic_has_deterioration
    """
    # ═══════════════════════════════════════════════════════════════════════════
    # 趋势特征
    # ═══════════════════════════════════════════════════════════════════════════
    SLOPE = "slope"                      # OLS 斜率
    LOG_SLOPE = "log_slope"              # 对数斜率
    ROBUST_SLOPE = "robust_slope"        # Theil-Sen 稳健斜率
    R_SQUARED = "r_squared"              # 拟合优度
    CAGR = "cagr"                        # 复合增长率
    TREND_DIRECTION = "trend_direction"  # up/down/flat

    # ═══════════════════════════════════════════════════════════════════════════
    # 波动特征
    # ═══════════════════════════════════════════════════════════════════════════
    CV = "cv"                            # 变异系数
    STD_DEV = "std_dev"                  # 标准差
    VOLATILITY_TYPE = "volatility_type"  # stable/moderate/volatile/high_volatility
    VOLATILITY_REGIME = "volatility_regime"  # 波动体制

    # ═══════════════════════════════════════════════════════════════════════════
    # 恶化检测
    # ═══════════════════════════════════════════════════════════════════════════
    HAS_DETERIORATION = "has_deterioration"        # 是否恶化
    DETERIORATION_SEVERITY = "deterioration_severity"  # none/mild/moderate/severe/catastrophic
    TOTAL_DECLINE_PCT = "total_decline_pct"        # 总下降百分比
    DECLINE_PROBABILITY = "decline_probability"    # 下降概率

    # ═══════════════════════════════════════════════════════════════════════════
    # 拐点检测
    # ═══════════════════════════════════════════════════════════════════════════
    HAS_INFLECTION = "has_inflection"      # 是否有拐点
    INFLECTION_TYPE = "inflection_type"    # v_recovery/peak_decline/...
    INFLECTION_POSITION = "inflection_position"  # 拐点位置

    # ═══════════════════════════════════════════════════════════════════════════
    # 周期性
    # ═══════════════════════════════════════════════════════════════════════════
    IS_CYCLICAL = "is_cyclical"            # 是否周期性
    CURRENT_PHASE = "current_phase"        # 当前周期阶段
    CYCLE_POSITION = "cycle_position"      # 周期位置
    CYCLICAL_CONFIDENCE = "cyclical_confidence"  # 周期判断置信度

    # ═══════════════════════════════════════════════════════════════════════════
    # 加速/减速
    # ═══════════════════════════════════════════════════════════════════════════
    IS_ACCELERATING = "is_accelerating"    # 是否加速
    IS_DECELERATING = "is_decelerating"    # 是否减速
    ACCELERATION = "acceleration"          # 加速度值

    # ═══════════════════════════════════════════════════════════════════════════
    # 滚动窗口
    # ═══════════════════════════════════════════════════════════════════════════
    RECENT_3Y_SLOPE = "recent_3y_slope"    # 近3年斜率
    RECENT_5Y_SLOPE = "recent_5y_slope"    # 近5年斜率
    MK_TAU = "mk_tau"                      # Mann-Kendall tau
    MK_P_VALUE = "mk_p_value"              # MK p值

    # ═══════════════════════════════════════════════════════════════════════════
    # 水平指标
    # ═══════════════════════════════════════════════════════════════════════════
    WEIGHTED_AVG = "weighted_avg"          # 加权平均值
    LATEST_VALUE = "latest_value"          # 最新值
    LATEST_VS_WEIGHTED = "latest_vs_weighted_ratio"  # 最新/加权比
    MAX_VALUE = "max_value"                # 历史最大值
    MIN_VALUE = "min_value"                # 历史最小值

    # ═══════════════════════════════════════════════════════════════════════════
    # 数据质量
    # ═══════════════════════════════════════════════════════════════════════════
    FULL_DATA_YEARS = "full_data_years"    # 完整数据年数
    TREND_WINDOW_YEARS = "trend_window_years"  # 趋势窗口年数
    DATA_QUALITY = "data_quality"          # 数据质量评分

    # ═══════════════════════════════════════════════════════════════════════════
    # 结构断点
    # ═══════════════════════════════════════════════════════════════════════════
    HAS_STRUCTURAL_BREAK = "has_structural_break"  # 是否有结构断点
    BREAK_YEAR_INDEX = "break_year_index"  # 断点位置
    DATA_REGIME = "data_regime"            # 数据体制

    # ═══════════════════════════════════════════════════════════════════════════
    # 元数据列
    # ═══════════════════════════════════════════════════════════════════════════
    TS_CODE = "ts_code"
    METRIC_NAME = "metric_name"
    NAME = "name"
    INDUSTRY = "industry"

    @classmethod
    def col(cls, metric: str, feature: str) -> str:
        """生成完整列名: {metric}_{feature}

        Example:
            >>> PDDAColumns.col("roic", PDDAColumns.SLOPE)
            'roic_slope'
        """
        return f"{metric}_{feature}"

    @classmethod
    def is_metadata(cls, column_name: str) -> bool:
        """检查是否为元数据列（非特征列）"""
        metadata = {cls.TS_CODE, cls.METRIC_NAME, cls.NAME, cls.INDUSTRY}
        if column_name in metadata:
            return True
        suffixes = ("_full_data_years", "_trend_window_years", "_data_regime", "_break_year_index")
        return any(column_name.endswith(s) for s in suffixes)


# ═══════════════════════════════════════════════════════════════════════════════
# 结构化探针输入 (类似 truth 的 ProbeInput，但更通用)
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class ProbeData:
    """单个指标的探针数据（不可变）

    设计原则:
        1. frozen=True 保证线程安全
        2. get() 方法提供安全访问
        3. 保留 metric_name 便于追溯

    Example:
        >>> probe = ProbeData(metric_name="roic", features={"slope": 0.05, "cv": 0.2})
        >>> probe.get("slope", 0.0)
        0.05
        >>> probe.get("missing", -1.0)
        -1.0
    """
    metric_name: str
    features: Mapping[str, float] = field(default_factory=dict)

    def get(self, feature: str, default: float = 0.0) -> float:
        """安全获取特征值"""
        return self.features.get(feature, default)

    def get_col(self, feature: str, default: float = 0.0) -> float:
        """通过 PDDAColumns 常量获取特征值

        Example:
            >>> probe.get_col(PDDAColumns.SLOPE, 0.0)
        """
        return self.features.get(feature, default)

    def has(self, feature: str) -> bool:
        """检查是否有某特征"""
        return feature in self.features and self.features[feature] is not None


@dataclass(frozen=True)
class CompanyProbes:
    """单个公司的所有探针数据（不可变）

    Example:
        >>> probes = CompanyProbes(
        ...     ts_code="000001.SZ",
        ...     name="平安银行",
        ...     industry="银行",
        ...     metrics={"roic": ProbeData(...), "roe": ProbeData(...)}
        ... )
        >>> roic_slope = probes.get_feature("roic", "slope", 0.0)
    """
    ts_code: str
    name: str = ""
    industry: str = ""
    metrics: Mapping[str, ProbeData] = field(default_factory=dict)

    def get_probe(self, metric: str) -> Optional[ProbeData]:
        """获取指定指标的探针数据"""
        return self.metrics.get(metric)

    def get_feature(self, metric: str, feature: str, default: float = 0.0) -> float:
        """获取指定指标的指定特征值"""
        probe = self.metrics.get(metric)
        if probe is None:
            return default
        return probe.get(feature, default)

    def has_metric(self, metric: str) -> bool:
        """检查是否有某指标"""
        return metric in self.metrics


# ═══════════════════════════════════════════════════════════════════════════════
# 导出
# ═══════════════════════════════════════════════════════════════════════════════

__all__ = [
    "PDDAColumns",
    "ProbeData",
    "CompanyProbes",
]
