"""
趋势字段 Schema
==============

声明 Trend 输出表的列名、取值路径及元信息，保证 TrendAnalyzer 与评分模
块之间的数据契约一致可追踪。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, List

from .trend_models import TrendSnapshot


@dataclass(frozen=True)
class TrendField:
    """Declarative definition for a trend output column."""

    key: str
    attr_path: str
    description: str
    unit: str = ""
    category: str = "core"

    def resolve(self, snapshot: TrendSnapshot) -> Any:
        value: Any = snapshot
        for part in self.attr_path.split("."):
            value = getattr(value, part)
        return value


def trend_field_schema() -> Iterable[TrendField]:
    """Return the default schema for trend result columns."""

    return [
        # 核心趋势
        TrendField("weighted", "weighted_avg", "5年加权平均", unit="ratio", category="core"),
        TrendField("log_slope", "trend.log_slope", "Log趋势斜率", unit="slope", category="core"),
        TrendField("slope", "trend.slope", "线性斜率", unit="slope", category="core"),
        TrendField("r_squared", "trend.r_squared", "趋势拟合优度", category="core"),
        TrendField("p_value", "trend.p_value", "趋势显著性P值", category="core"),
        TrendField("cagr", "trend.cagr_approx", "CAGR近似", unit="ratio", category="core"),
        TrendField("latest", "latest_value", "最新值", category="core"),
        TrendField("trend_score", "evaluation.trend_score", "趋势评分", category="core"),
        # 数据质量
        TrendField("data_quality", "quality.effective", "有效数据质量标记", category="quality"),
        TrendField("data_quality_original", "quality.original", "原始数据质量", category="quality"),
        TrendField("data_quality_cleaned", "quality.cleaned", "清洗后数据质量", category="quality"),
        TrendField("has_loss_years", "quality.has_loss_years", "是否存在亏损年", category="quality"),
        TrendField("loss_year_count", "quality.loss_year_count", "亏损年计数", category="quality"),
        TrendField("has_near_zero_years", "quality.has_near_zero_years", "是否存在接近0的年份", category="quality"),
        TrendField("near_zero_count", "quality.near_zero_count", "接近0年份数量", category="quality"),
        TrendField("has_loss_years_cleaned", "quality.has_loss_years_cleaned", "清洗后是否亏损", category="quality"),
        TrendField("loss_year_count_cleaned", "quality.loss_year_count_cleaned", "清洗后亏损年数", category="quality"),
        TrendField("has_near_zero_years_cleaned", "quality.has_near_zero_years_cleaned", "清洗后是否接近0", category="quality"),
        TrendField("near_zero_count_cleaned", "quality.near_zero_count_cleaned", "清洗后接近0年数", category="quality"),
        # 波动率
        TrendField("cv", "volatility.cv", "变异系数", category="volatility"),
        TrendField("std_dev", "volatility.std_dev", "标准差", category="volatility"),
        TrendField("range_ratio", "volatility.range_ratio", "极差比例", category="volatility"),
        TrendField("volatility_type", "volatility.volatility_type", "波动类型", category="volatility"),
        TrendField("vol_mean_near_zero", "volatility.mean_near_zero", "均值是否接近0", category="volatility"),
        # 拐点
        TrendField("has_inflection", "inflection.has_inflection", "是否存在拐点", category="inflection"),
        TrendField("inflection_type", "inflection.inflection_type", "拐点类型", category="inflection"),
        TrendField("early_slope", "inflection.early_slope", "早期斜率", category="inflection"),
        TrendField("middle_slope", "inflection.middle_slope", "中段斜率", category="inflection"),
        TrendField("recent_slope", "inflection.recent_slope", "近年斜率", category="inflection"),
        TrendField("slope_change", "inflection.slope_change", "斜率变化幅度", category="inflection"),
        TrendField("inflection_confidence", "inflection.confidence", "拐点置信度", category="inflection"),
        TrendField("inflection_early_r2", "inflection.early_r_squared", "早期拟合优度", category="inflection"),
        TrendField("inflection_recent_r2", "inflection.recent_r_squared", "近期拟合优度", category="inflection"),
        # 恶化
        TrendField("has_deterioration", "deterioration.has_deterioration", "是否存在恶化", category="deterioration"),
        TrendField("deterioration_severity", "deterioration.severity", "恶化程度", category="deterioration"),
        TrendField("year4_to_5_change", "deterioration.year4_to_5_change", "第4-5年变动", category="deterioration"),
        TrendField("year3_to_4_change", "deterioration.year3_to_4_change", "第3-4年变动", category="deterioration"),
        TrendField("total_decline_pct", "deterioration.total_decline_pct", "总跌幅", unit="ratio", category="deterioration"),
        TrendField("year4_to_5_pct", "deterioration.year4_to_5_pct", "第4-5年跌幅比例", unit="ratio", category="deterioration"),
        TrendField("year3_to_4_pct", "deterioration.year3_to_4_pct", "第3-4年跌幅比例", unit="ratio", category="deterioration"),
        TrendField("is_high_level_stable", "deterioration.is_high_level_stable", "高位稳定", category="deterioration"),
        TrendField("deterioration_industry", "deterioration.industry", "恶化判断行业", category="deterioration"),
        # 周期性
        TrendField("is_cyclical", "cyclical.is_cyclical", "是否周期性", category="cyclical"),
        TrendField("peak_to_trough_ratio", "cyclical.peak_to_trough_ratio", "峰谷比", category="cyclical"),
        TrendField("has_middle_peak", "cyclical.has_middle_peak", "是否中段峰值", category="cyclical"),
        TrendField("current_phase", "cyclical.current_phase", "当前周期阶段", category="cyclical"),
        TrendField("industry_cyclical", "cyclical.industry_cyclical", "行业是否周期性", category="cyclical"),
        TrendField("has_wave_pattern", "cyclical.has_wave_pattern", "是否波浪型", category="cyclical"),
        TrendField("trend_r_squared", "cyclical.trend_r_squared", "周期趋势拟合优度", category="cyclical"),
        TrendField("cyclical_cv", "cyclical.cv", "周期CV", category="cyclical"),
        TrendField("cyclical_confidence", "cyclical.cyclical_confidence", "周期置信度", category="cyclical"),
        TrendField("cyclical_industry", "cyclical.industry", "周期判断行业", category="cyclical"),
        # 阈值曝光
        TrendField("decline_threshold_pct", "deterioration.decline_threshold_pct", "跌幅阈值(%)", unit="ratio", category="threshold"),
        TrendField("decline_threshold_abs", "deterioration.decline_threshold_abs", "跌幅阈值(绝对值)", category="threshold"),
        TrendField("peak_to_trough_threshold", "cyclical.peak_to_trough_threshold", "峰谷阈值", category="threshold"),
        TrendField("trend_r_squared_max", "cyclical.trend_r_squared_max", "趋势R²上限", category="threshold"),
        TrendField("cv_threshold", "cyclical.cv_threshold", "CV阈值", category="threshold"),
        # 滚动趋势
        TrendField("recent_3y_slope", "rolling.recent_3y_slope", "近3年斜率", category="rolling"),
        TrendField("recent_3y_r_squared", "rolling.recent_3y_r_squared", "近3年拟合优度", category="rolling"),
        TrendField("trend_acceleration", "rolling.trend_acceleration", "趋势加速度", category="rolling"),
        TrendField("is_accelerating", "rolling.is_accelerating", "是否加速", category="rolling"),
        TrendField("is_decelerating", "rolling.is_decelerating", "是否放缓", category="rolling"),
        TrendField("full_5y_slope", "rolling.full_5y_slope", "5年全样本斜率", category="rolling"),
        TrendField("full_5y_r_squared", "rolling.full_5y_r_squared", "5年全样本拟合优度", category="rolling"),
    ]


__all__ = ["TrendField", "trend_field_schema"]
