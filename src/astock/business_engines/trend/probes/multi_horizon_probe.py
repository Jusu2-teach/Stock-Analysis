"""
多时间窗口分析探针 (Multi-Horizon Analysis Probe)
===================================================

**整合版本 v2.0** - 合并 MultiHorizonProbe + ProfessionalDataWindowStrategy

解决核心问题:
    - 5年太短: 无法可靠检测3-7年商业周期
    - 10年太长: 公司可能发生结构性变化，稀释近期信号

专业解决方案:
    1. 近期窗口 (Recent Horizon, 5年): 计算趋势、增长率、质量评分
    2. 扩展窗口 (Extended Horizon, 10年): 周期性检测、结构断点、长期均值
    3. 结构断点检测: 识别公司本质变化点，自动选择有效数据窗口
    4. 指标智能分类: 根据指标类型自动调整窗口权重和策略
    5. 周期位置分析: 判断当前处于周期高点/低点/上升/下降

核心能力:
    ┌────────────────┬──────────────────┬──────────────────────────────┐
    │  指标类型      │  主窗口权重      │  扩展窗口用途                 │
    ├────────────────┼──────────────────┼──────────────────────────────┤
    │  效率类(ROE)   │  75%近5年        │  周期位置判断、长期中枢       │
    │  增长类(营收)  │  70%近5年        │  周期性检测、增长持续性       │
    │  杠杆类(负债)  │  80%近5年        │  长期风险演变                 │
    │  周转类(周转)  │  65%近5年        │  长期趋势、行业周期           │
    │  质量类(现金)  │  70%近5年        │  现金流质量趋势               │
    └────────────────┴──────────────────┴──────────────────────────────┘

学术参考:
    - Bai, J., & Perron, P. (1998). Estimating and Testing Linear Models with Multiple
      Structural Changes. Econometrica, 66(1), 47-78. [结构断点检测]
    - Zivot, E., & Andrews, D.W.K. (1992). Further Evidence on the Great Crash,
      the Oil-Price Shock, and the Unit-Root Hypothesis. JBES, 10(3), 251-270.
    - Chow, G. C. (1960). Tests of Equality Between Sets of Coefficients in Two
      Linear Regressions. Econometrica, 28(3), 591-605. [Chow断点检验]

设计哲学:
    "用10年数据的长度换取5年数据的可靠性，但要智能识别何时10年已不再适用"

作者: AStock Analysis System
日期: 2025-12-07
更新: 2025-12-25 - 整合 ProfessionalDataWindowStrategy，增强指标分类和策略能力
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional, Tuple, Dict
from enum import Enum
import numpy as np

logger = logging.getLogger(__name__)


# =============================================================================
# 指标分类与策略配置
# =============================================================================

class MetricCategory(Enum):
    """指标类别

    不同类型的财务指标具有不同的特性，需要采用不同的分析策略:
    - 效率类: 受周期影响，关注长期中枢
    - 增长类: 关注持续性，需要更多历史数据验证
    - 杠杆类: 风险指标，重点关注近期变化
    - 周转类: 周期性强，需要周期分析
    - 质量类: 关注现金流真实性
    """
    EFFICIENCY = "efficiency"      # 效率类: ROE, ROIC, 利润率
    GROWTH = "growth"              # 增长类: 营收增长, 利润增长
    LEVERAGE = "leverage"          # 杠杆类: 资产负债率
    TURNOVER = "turnover"          # 周转类: 资产周转率
    QUALITY = "quality"            # 质量类: 现金流覆盖率


@dataclass(frozen=True)
class WindowStrategy:
    """窗口策略配置

    定义不同指标类别使用的分析策略参数。

    Attributes:
        primary_window: 主窗口年数（用于趋势计算）
        extended_window: 扩展窗口年数（用于辅助分析）
        primary_weight: 主窗口权重 (0-1)
        use_break_detection: 是否启用断点检测
        use_cyclical_analysis: 是否启用周期分析
        description: 策略说明
    """
    primary_window: int
    extended_window: int
    primary_weight: float
    use_break_detection: bool
    use_cyclical_analysis: bool
    description: str


# 各类指标的默认策略配置
METRIC_STRATEGIES: Dict[MetricCategory, WindowStrategy] = {
    MetricCategory.EFFICIENCY: WindowStrategy(
        primary_window=5,
        extended_window=10,
        primary_weight=0.75,
        use_break_detection=True,
        use_cyclical_analysis=True,
        description="效率指标：近5年为主(75%)，10年用于周期判断和断点检测"
    ),
    MetricCategory.GROWTH: WindowStrategy(
        primary_window=5,
        extended_window=10,
        primary_weight=0.70,
        use_break_detection=True,
        use_cyclical_analysis=True,
        description="增长指标：近5年为主(70%)，10年用于增长持续性验证"
    ),
    MetricCategory.LEVERAGE: WindowStrategy(
        primary_window=5,
        extended_window=10,
        primary_weight=0.80,
        use_break_detection=True,
        use_cyclical_analysis=False,  # 杠杆不需要周期分析
        description="杠杆指标：重点关注近期(80%)，警惕长期趋势恶化"
    ),
    MetricCategory.TURNOVER: WindowStrategy(
        primary_window=5,
        extended_window=10,
        primary_weight=0.65,
        use_break_detection=True,
        use_cyclical_analysis=True,
        description="周转指标：周期性较强，扩展窗口权重略高(35%)"
    ),
    MetricCategory.QUALITY: WindowStrategy(
        primary_window=5,
        extended_window=10,
        primary_weight=0.70,
        use_break_detection=True,
        use_cyclical_analysis=False,
        description="质量指标：关注近期现金流质量(70%)"
    ),
}


def classify_metric(metric_name: str) -> MetricCategory:
    """
    智能指标分类

    根据指标名称自动识别指标类别，用于选择合适的分析策略。

    Args:
        metric_name: 指标名称（支持中英文）

    Returns:
        MetricCategory: 指标类别

    Examples:
        >>> classify_metric("roe")
        MetricCategory.EFFICIENCY
        >>> classify_metric("revenue_growth")
        MetricCategory.GROWTH
        >>> classify_metric("debt_ratio")
        MetricCategory.LEVERAGE
    """
    name = metric_name.lower()

    # 质量类 (优先匹配，因为 ocf_ratio 应该是质量类)
    if any(k in name for k in ["cash", "fcf", "ocf", "现金", "经营", "quality"]):
        return MetricCategory.QUALITY

    # 杠杆类 (优先匹配 debt_ratio)
    if any(k in name for k in ["debt", "liability", "leverage", "负债", "杠杆"]):
        return MetricCategory.LEVERAGE

    # 周转类
    if any(k in name for k in ["turn", "周转"]):
        return MetricCategory.TURNOVER

    # v4.1 修复: 效率类优先于增长类匹配 (避免 netprofit_margin 被 "profit" 误分为增长类)
    # 效率类 (包含 roe, roic, margin, rate, ratio 等)
    if any(k in name for k in ["roe", "roic", "margin", "rate", "ratio", "净利率", "毛利率", "efficiency", "效率"]):
        return MetricCategory.EFFICIENCY

    # 增长类
    if any(k in name for k in ["revenue", "profit", "growth", "sales", "income", "营收", "利润", "增长"]):
        return MetricCategory.GROWTH

    # 默认为效率类
    return MetricCategory.EFFICIENCY


# =============================================================================
# 数据类定义
# =============================================================================

class BreakType(Enum):
    """结构断点类型"""
    NONE = "none"                      # 无断点
    LEVEL_SHIFT = "level_shift"        # 水平位移（均值变化）
    TREND_CHANGE = "trend_change"      # 趋势变化（斜率变化）
    VOLATILITY_CHANGE = "vol_change"   # 波动率变化
    REGIME_SWITCH = "regime_switch"    # 全面体制转换


@dataclass
class StructuralBreakResult:
    """结构断点检测结果

    Attributes:
        has_break: 是否存在显著断点
        break_type: 断点类型
        break_year_index: 断点位置（0-indexed，在此年之后发生变化）
        break_significance: 断点显著性 (F-statistic或类似度量)
        p_value: 统计显著性
        pre_break_stats: 断点前统计特征
        post_break_stats: 断点后统计特征
        recommended_window: 推荐使用的数据窗口（从第几年开始）
        confidence: 断点检测置信度
        evidence: 支持断点的证据列表
    """
    has_break: bool
    break_type: BreakType
    break_year_index: Optional[int]  # None if no break
    break_significance: float        # F-stat or similar
    p_value: float
    pre_break_stats: Dict[str, float]
    post_break_stats: Dict[str, float]
    recommended_window_start: int    # 建议从第几年开始使用数据
    confidence: float                # 0-1
    evidence: List[str] = field(default_factory=list)


@dataclass
class HorizonAnalysis:
    """单一时间窗口的分析结果

    Attributes:
        window_name: 窗口名称 (recent_5y, full_10y, post_break)
        years: 窗口包含的年数
        start_index: 在原始数据中的起始位置

        # 基本统计
        mean: 均值
        std: 标准差
        cv: 变异系数

        # 趋势统计
        slope: OLS斜率
        robust_slope: Theil-Sen稳健斜率
        r_squared: R²决定系数
        cagr: 复合年增长率

        # 可靠性
        reliability_grade: 可靠性等级 (A/B/C/D/F)
        confidence_ceiling: 置信度上限
    """
    window_name: str
    years: int
    start_index: int

    # 基本统计
    mean: float
    std: float
    cv: float
    latest_value: float

    # 趋势统计
    slope: float
    robust_slope: float
    r_squared: float
    cagr: float

    # 可靠性
    reliability_grade: str
    confidence_ceiling: float


@dataclass
class MultiHorizonResult:
    """多时间窗口综合分析结果 (增强版 v2.0)

    这是本模块的核心输出，整合多个时间窗口的分析，包含:
    - 多窗口统计分析
    - 结构断点检测
    - 指标分类与策略
    - 周期位置分析
    - 智能建议生成

    Attributes:
        recent_analysis: 近5年分析（主要判断依据）
        extended_analysis: 全10年分析（辅助判断）
        effective_analysis: 有效窗口分析（断点后数据）

        structural_break: 结构断点检测结果

        # 综合指标
        effective_slope: 综合加权斜率
        effective_cagr: 综合加权CAGR
        effective_cv: 变异系数
        effective_latest: 最新值
        data_regime: 数据体制 ("stable", "broken", "transitional")

        # 权重分配
        recent_weight: 近期数据权重 (0-1)
        extended_weight: 扩展数据权重 (0-1)

        # 指标分类与策略 (v2.0 新增)
        metric_category: 指标类别
        applied_strategy: 应用的窗口策略

        # 周期分析 (v2.0 新增)
        cyclical_confidence: 周期性置信度 (0-1)
        cycle_position: 周期位置 (top/bottom/mid_up/mid_down/unknown)

        # 质量评估 (v2.0 新增)
        analysis_confidence: 综合分析置信度 (0-1)
        data_quality_grade: 数据质量等级 (A-F)

        # 建议
        recommendation: 分析建议
        warnings: 警告列表
    """
    recent_analysis: HorizonAnalysis
    extended_analysis: Optional[HorizonAnalysis]
    effective_analysis: Optional[HorizonAnalysis]  # 断点后的有效数据

    structural_break: StructuralBreakResult

    # 综合指标
    effective_slope: float
    effective_cagr: float
    effective_cv: float = 0.0
    effective_latest: float = 0.0
    data_regime: str = "stable"  # stable, broken, transitional

    # 权重分配
    recent_weight: float = 0.7
    extended_weight: float = 0.3

    # 指标分类与策略 (v2.0)
    metric_category: Optional[MetricCategory] = None
    applied_strategy: Optional[WindowStrategy] = None

    # 周期分析 (v2.0)
    cyclical_confidence: float = 0.0
    cycle_position: str = "unknown"

    # 质量评估 (v2.0)
    analysis_confidence: float = 0.5
    data_quality_grade: str = "C"

    # 建议
    recommendation: str = ""
    warnings: List[str] = field(default_factory=list)


# =============================================================================
# 结构断点检测器
# =============================================================================

class StructuralBreakDetector:
    """
    结构断点检测器

    实现多种断点检测方法:
    1. Chow Test: 检测已知断点位置的参数变化
    2. CUSUM: 累积和检验，检测趋势变化
    3. 简化Bai-Perron: 搜索最优断点位置

    针对A股特点优化:
    - 最小片段长度: 3年（保证估计稳定性）
    - 显著性阈值: 相对宽松（10%），因为样本量小
    - 关注实质变化而非统计显著性
    """

    def __init__(
        self,
        min_segment_years: int = 3,
        significance_level: float = 0.10,
        min_effect_size: float = 0.30,  # 最小效应量：均值变化30%
    ):
        """
        Args:
            min_segment_years: 最小片段年数
            significance_level: 显著性水平
            min_effect_size: 最小效应量（相对变化）
        """
        self.min_segment = min_segment_years
        self.alpha = significance_level
        self.min_effect = min_effect_size

    def detect(self, values: List[float]) -> StructuralBreakResult:
        """
        检测结构断点

        Args:
            values: 时间序列数据（按时间顺序，旧→新）

        Returns:
            StructuralBreakResult: 断点检测结果
        """
        n = len(values)
        arr = np.array(values, dtype=float)

        # 数据不足
        if n < 2 * self.min_segment:
            return self._no_break_result(arr, n, reason="数据不足以检测断点")

        # 1. 搜索最优断点位置
        best_break = self._find_optimal_break(arr)

        if best_break is None:
            return self._no_break_result(arr, n, reason="未发现显著断点")

        break_idx, break_stat, break_type = best_break

        # 2. 计算断点前后统计特征
        pre_stats = self._compute_segment_stats(arr[:break_idx+1])
        post_stats = self._compute_segment_stats(arr[break_idx+1:])

        # 3. 评估断点显著性和实际影响
        p_value = self._compute_p_value(break_stat, n)
        effect_size = self._compute_effect_size(pre_stats, post_stats)

        # 4. 判断是否采纳断点
        is_significant = p_value < self.alpha
        is_meaningful = effect_size >= self.min_effect
        has_break = is_significant and is_meaningful

        # 5. 确定推荐窗口
        if has_break:
            # 断点后至少需要min_segment年数据
            post_years = n - break_idx - 1
            if post_years >= self.min_segment:
                recommended_start = break_idx + 1
            else:
                # 断点后数据太少，仍使用全部数据但降低置信度
                recommended_start = 0
                has_break = False  # 降级为无实质断点
        else:
            recommended_start = 0

        # 6. 构建证据列表
        evidence = self._build_evidence(
            has_break, break_idx, break_type,
            pre_stats, post_stats, effect_size, p_value
        )

        return StructuralBreakResult(
            has_break=has_break,
            break_type=break_type if has_break else BreakType.NONE,
            break_year_index=break_idx if has_break else None,
            break_significance=break_stat,
            p_value=p_value,
            pre_break_stats=pre_stats,
            post_break_stats=post_stats,
            recommended_window_start=recommended_start,
            confidence=1.0 - p_value if has_break else 0.0,
            evidence=evidence
        )

    def _find_optimal_break(
        self, arr: np.ndarray
    ) -> Optional[Tuple[int, float, BreakType]]:
        """
        搜索最优断点位置

        使用简化的Bai-Perron方法：
        在所有可能位置计算Chow统计量，选择最大的
        """
        n = len(arr)
        best_stat = 0.0
        best_idx = None
        best_type = BreakType.NONE

        # 搜索范围: [min_segment, n - min_segment)
        for k in range(self.min_segment, n - self.min_segment):
            pre = arr[:k+1]
            post = arr[k+1:]

            # 水平断点检验 (均值变化)
            level_stat = self._chow_level_stat(pre, post)

            # 趋势断点检验 (斜率变化)
            trend_stat = self._chow_trend_stat(pre, post)

            # 选择更显著的断点类型
            if level_stat > best_stat:
                best_stat = level_stat
                best_idx = k
                best_type = BreakType.LEVEL_SHIFT

            if trend_stat > best_stat:
                best_stat = trend_stat
                best_idx = k
                best_type = BreakType.TREND_CHANGE

        # 检查是否超过阈值
        # 使用经验阈值: F > 4.0 在小样本中通常显著
        if best_stat < 3.0:
            return None

        return (best_idx, best_stat, best_type)

    def _chow_level_stat(self, pre: np.ndarray, post: np.ndarray) -> float:
        """计算水平断点的Chow统计量（简化版）"""
        n1, n2 = len(pre), len(post)
        if n1 < 2 or n2 < 2:
            return 0.0

        m1, m2 = np.mean(pre), np.mean(post)
        v1, v2 = np.var(pre, ddof=1), np.var(post, ddof=1)

        # 池化方差
        pooled_var = ((n1-1)*v1 + (n2-1)*v2) / (n1 + n2 - 2)
        if pooled_var < 1e-10:
            return 0.0

        # 近似F统计量
        t_stat = (m1 - m2) / np.sqrt(pooled_var * (1/n1 + 1/n2))
        return t_stat ** 2  # F = t²

    def _chow_trend_stat(self, pre: np.ndarray, post: np.ndarray) -> float:
        """计算趋势断点的Chow统计量（简化版）"""
        n1, n2 = len(pre), len(post)
        if n1 < 3 or n2 < 3:
            return 0.0

        # 分别拟合趋势
        t1 = np.arange(n1)
        t2 = np.arange(n2)

        try:
            slope1, intercept1 = np.polyfit(t1, pre, 1)
            slope2, intercept2 = np.polyfit(t2, post, 1)

            # v4.1 修复: 使用回归截距而非均值计算残差 (Chow test 标准实现)
            res1 = pre - (slope1 * t1 + intercept1)
            res2 = post - (slope2 * t2 + intercept2)

            ssr1 = np.sum(res1**2)
            ssr2 = np.sum(res2**2)

            # 全样本拟合
            t_full = np.arange(n1 + n2)
            arr_full = np.concatenate([pre, post])
            slope_full, intercept_full = np.polyfit(t_full, arr_full, 1)
            res_full = arr_full - (slope_full * t_full + intercept_full)
            ssr_full = np.sum(res_full**2)

            # Chow F统计量
            k = 2  # 参数数量 (斜率 + 截距)
            numerator = (ssr_full - ssr1 - ssr2) / k
            denominator = (ssr1 + ssr2) / (n1 + n2 - 2*k)

            if denominator < 1e-10:
                return 0.0

            return numerator / denominator

        except Exception:
            return 0.0

    def _compute_segment_stats(self, segment: np.ndarray) -> Dict[str, float]:
        """计算片段统计特征"""
        if len(segment) == 0:
            return {"mean": 0, "std": 0, "slope": 0, "cv": 0}

        mean_val = float(np.mean(segment))
        std_val = float(np.std(segment, ddof=1)) if len(segment) > 1 else 0.0
        cv = std_val / abs(mean_val) if abs(mean_val) > 1e-10 else 0.0

        # 趋势
        if len(segment) >= 2:
            t = np.arange(len(segment))
            slope, _ = np.polyfit(t, segment, 1)
        else:
            slope = 0.0

        return {
            "mean": mean_val,
            "std": std_val,
            "slope": float(slope),
            "cv": cv,
            "n": len(segment)
        }

    def _compute_p_value(self, f_stat: float, n: int) -> float:
        """
        计算近似p值

        使用F分布的近似，但针对小样本进行调整

        v13.2 升级: 使用 scipy F 分布精确计算 p 值 (替代硬编码映射)
        原硬编码映射在 n=5~10 小样本下误差约 ±0.05,
        现在用 F(k, n-2k) 分布的生存函数获得精确 p 值。
        """
        if f_stat <= 0:
            return 1.0

        # v13.2: 精确 F 分布 p 值 (Chow test: k=2 参数, df2=n-2k)
        k = 2  # 线性回归参数数 (斜率 + 截距)
        df2 = n - 2 * k
        if df2 <= 0:
            # 自由度不足, 退化到保守估计
            return 0.50
        try:
            from scipy.stats import f as f_dist
            p_value = float(f_dist.sf(f_stat, k, df2))
            return max(0.001, min(1.0, p_value))  # clamp 防止数值极端
        except (ImportError, Exception):
            # scipy 不可用时的 fallback (保留原始映射保证鲁棒性)
            if f_stat > 8.0:
                return 0.01
            elif f_stat > 6.0:
                return 0.05
            elif f_stat > 4.0:
                return 0.10
            elif f_stat > 2.5:
                return 0.20
            else:
                return 0.50

    def _compute_effect_size(
        self, pre_stats: Dict[str, float], post_stats: Dict[str, float]
    ) -> float:
        """计算效应量（相对变化）"""
        pre_mean = pre_stats.get("mean", 0)
        post_mean = post_stats.get("mean", 0)

        if abs(pre_mean) < 1e-10:
            return abs(post_mean)

        return abs(post_mean - pre_mean) / abs(pre_mean)

    def _build_evidence(
        self,
        has_break: bool,
        break_idx: Optional[int],
        break_type: BreakType,
        pre_stats: Dict[str, float],
        post_stats: Dict[str, float],
        effect_size: float,
        p_value: float
    ) -> List[str]:
        """构建证据列表"""
        evidence = []

        if not has_break:
            evidence.append("未检测到显著结构断点")
            return evidence

        if break_type == BreakType.LEVEL_SHIFT:
            evidence.append(
                f"第{break_idx+1}年后发生水平位移: "
                f"均值从{pre_stats['mean']:.2f}变为{post_stats['mean']:.2f} "
                f"(变化{effect_size:.1%})"
            )
        elif break_type == BreakType.TREND_CHANGE:
            evidence.append(
                f"第{break_idx+1}年后趋势变化: "
                f"斜率从{pre_stats['slope']:.3f}变为{post_stats['slope']:.3f}"
            )

        evidence.append(f"统计显著性: p={p_value:.3f}")
        evidence.append(f"效应量: {effect_size:.1%}")

        return evidence

    def _no_break_result(
        self, arr: np.ndarray, n: int, reason: str
    ) -> StructuralBreakResult:
        """返回无断点结果"""
        full_stats = self._compute_segment_stats(arr)
        return StructuralBreakResult(
            has_break=False,
            break_type=BreakType.NONE,
            break_year_index=None,
            break_significance=0.0,
            p_value=1.0,
            pre_break_stats=full_stats,
            post_break_stats=full_stats,
            recommended_window_start=0,
            confidence=0.0,
            evidence=[reason]
        )


# =============================================================================
# 多时间窗口分析器 (增强版 v2.0)
# =============================================================================

class MultiHorizonProbe:
    """
    多时间窗口分析探针 (增强版 v2.0)

    整合了原 ProfessionalDataWindowStrategy 的所有能力:
    - 智能指标分类
    - 策略驱动的窗口权重
    - 周期位置分析
    - 专业级建议生成

    Unified interface following ProbeProtocol:
    - compute(values, **kwargs) -> MultiHorizonResult
    - default() -> MultiHorizonResult

    核心设计理念:
    - 近5年 (Recent): 默认70%权重，反映当前经营状态
    - 全10年 (Extended): 默认30%权重，用于周期检测和断点识别
    - 断点后 (Effective): 如检测到断点，使用断点后数据
    - 智能策略: 根据指标类型自动调整权重

    使用场景:
    1. 有10年数据: 自动检测断点，智能选择有效窗口
    2. 只有5年数据: 仅使用recent分析，降低置信度
    3. 不同指标: 自动识别类型，应用最佳策略

    使用示例:
        >>> probe = MultiHorizonProbe(auto_classify=True)
        >>> result = probe.compute([12, 13, 14, 15, 16, 18, 17, 16, 15, 14], "roe")
        >>> print(result.metric_category)  # MetricCategory.EFFICIENCY
        >>> print(result.cycle_position)   # "mid_down"
        >>> print(result.recommendation)
    """

    def __init__(
        self,
        recent_years: int = 5,
        recent_weight: float = 0.70,
        extended_weight: float = 0.30,
        break_threshold: float = 0.30,
        auto_classify: bool = True,
        custom_strategies: Optional[Dict[MetricCategory, WindowStrategy]] = None,
    ):
        """
        Args:
            recent_years: 近期窗口年数
            recent_weight: 近期窗口默认权重 (auto_classify=False时使用)
            extended_weight: 扩展窗口默认权重 (auto_classify=False时使用)
            break_threshold: 断点效应量阈值
            auto_classify: 是否根据指标名称自动分类并应用策略
            custom_strategies: 自定义策略配置（覆盖默认）
        """
        self.recent_years = recent_years
        self.default_recent_weight = recent_weight
        self.default_extended_weight = extended_weight
        self.break_threshold = break_threshold
        self.auto_classify = auto_classify

        # 策略配置
        self.strategies = METRIC_STRATEGIES.copy()
        if custom_strategies:
            self.strategies.update(custom_strategies)

        self.break_detector = StructuralBreakDetector(
            min_segment_years=3,
            significance_level=0.10,
            min_effect_size=break_threshold
        )

    def compute(
        self,
        values: List[float],
        metric_name: str = "unknown",
        category_override: Optional[MetricCategory] = None,
    ) -> MultiHorizonResult:
        """
        执行多时间窗口分析 (增强版)

        Args:
            values: 时间序列数据（按时间顺序，旧→新）
            metric_name: 指标名称（用于自动分类和报告）
            category_override: 手动指定指标类别（覆盖自动分类）

        Returns:
            MultiHorizonResult: 综合分析结果
        """
        n = len(values)
        arr = np.array(values, dtype=float)
        warnings = []

        # 0. 智能指标分类与策略选择
        if self.auto_classify:
            category = category_override or classify_metric(metric_name)
            strategy = self.strategies.get(category, METRIC_STRATEGIES[MetricCategory.EFFICIENCY])
            base_recent_weight = strategy.primary_weight
            base_extended_weight = 1.0 - strategy.primary_weight
        else:
            category = category_override
            strategy = None
            base_recent_weight = self.default_recent_weight
            base_extended_weight = self.default_extended_weight

        # 1. 计算近5年分析（始终执行）
        recent_start = max(0, n - self.recent_years)
        recent_data = arr[recent_start:]
        recent_analysis = self._analyze_horizon(
            recent_data,
            "recent_5y",
            start_index=recent_start
        )

        # 2. 如果有足够数据，执行扩展分析
        extended_analysis = None
        if n > self.recent_years:
            extended_analysis = self._analyze_horizon(
                arr,
                "full_10y",
                start_index=0
            )
        else:
            warnings.append(
                f"数据不足{n}年，无法进行扩展窗口分析"
            )

        # 3. 结构断点检测
        use_break_detection = strategy.use_break_detection if strategy else True
        if use_break_detection and n >= 6:
            structural_break = self.break_detector.detect(values)
        else:
            structural_break = StructuralBreakResult(
                has_break=False,
                break_type=BreakType.NONE,
                break_year_index=None,
                break_significance=0.0,
                p_value=1.0,
                pre_break_stats={},
                post_break_stats={},
                recommended_window_start=0,
                confidence=0.0,
                evidence=["数据不足以检测断点"]
            )

        # 4. 确定有效窗口
        effective_analysis = None
        if structural_break.has_break:
            start = structural_break.recommended_window_start
            if start > 0 and n - start >= 3:
                effective_data = arr[start:]
                effective_analysis = self._analyze_horizon(
                    effective_data,
                    f"post_break_{n-start}y",
                    start_index=start
                )
                warnings.append(
                    f"检测到结构断点在第{start+1}年，建议使用断点后{n-start}年数据"
                )

        # 5. 计算权重和综合指标
        weights = self._compute_weights(
            n, structural_break, recent_analysis, extended_analysis,
            base_recent_weight, base_extended_weight
        )

        effective_slope, effective_cagr = self._compute_effective_metrics(
            recent_analysis,
            extended_analysis,
            effective_analysis,
            weights
        )

        # 提取近期 CV 和最新值
        effective_cv = recent_analysis.cv
        effective_latest = recent_analysis.latest_value

        # 6. 确定数据体制
        data_regime = self._determine_regime(
            structural_break, recent_analysis, extended_analysis
        )

        # 7. 周期性分析 (v2.0 新增)
        use_cyclical = strategy.use_cyclical_analysis if strategy else True
        if use_cyclical and n >= 7:
            cyclical_confidence, cycle_position = self._analyze_cyclical(arr)
        else:
            cyclical_confidence, cycle_position = 0.0, "unknown"

        # 8. 计算综合分析置信度 (v2.0 新增)
        analysis_confidence = self._compute_analysis_confidence(
            recent_analysis, structural_break.has_break, cyclical_confidence
        )
        data_quality_grade = recent_analysis.reliability_grade

        # 9. 生成专业建议 (v2.0 增强)
        recommendation = self._generate_professional_recommendation(
            metric_name=metric_name,
            category=category,
            recent=recent_analysis,
            extended=extended_analysis,
            effective=effective_analysis,
            break_result=structural_break,
            regime=data_regime,
            cyclical_confidence=cyclical_confidence,
            cycle_position=cycle_position,
        )

        return MultiHorizonResult(
            recent_analysis=recent_analysis,
            extended_analysis=extended_analysis,
            effective_analysis=effective_analysis,
            structural_break=structural_break,
            # 综合指标
            effective_slope=effective_slope,
            effective_cagr=effective_cagr,
            effective_cv=effective_cv,
            effective_latest=effective_latest,
            data_regime=data_regime,
            # 权重
            recent_weight=weights[0],
            extended_weight=weights[1],
            # 策略 (v2.0)
            metric_category=category,
            applied_strategy=strategy,
            # 周期 (v2.0)
            cyclical_confidence=cyclical_confidence,
            cycle_position=cycle_position,
            # 质量 (v2.0)
            analysis_confidence=analysis_confidence,
            data_quality_grade=data_quality_grade,
            # 建议
            recommendation=recommendation,
            warnings=warnings
        )

    def _analyze_cyclical(self, arr: np.ndarray) -> Tuple[float, str]:
        """
        周期性分析

        快速判断数据是否具有周期性，以及当前所处位置。

        Args:
            arr: 时间序列数据

        Returns:
            (cyclical_confidence, cycle_position): 周期置信度和位置
        """
        n = len(arr)
        if n < 5:
            return 0.0, "unknown"

        mean_val = np.mean(arr)
        std_val = np.std(arr)
        cv = std_val / abs(mean_val) if abs(mean_val) > 0.01 else 0

        # 检查峰谷
        max_idx = np.argmax(arr)
        min_idx = np.argmin(arr)

        # 峰谷比
        if arr[min_idx] > 0:
            peak_trough_ratio = arr[max_idx] / arr[min_idx]
        else:
            peak_trough_ratio = 1.0

        # 周期性判断
        if cv > 0.25 and peak_trough_ratio > 1.5:
            cyclical_conf = min(0.8, cv + (peak_trough_ratio - 1) * 0.2)

            # 确定当前位置
            latest = arr[-1]
            if latest > mean_val + 0.5 * std_val:
                position = "top"
            elif latest < mean_val - 0.5 * std_val:
                position = "bottom"
            elif len(arr) >= 2 and arr[-1] > arr[-2]:
                position = "mid_up"
            else:
                position = "mid_down"

            return cyclical_conf, position

        return 0.2, "unknown"

    def _compute_analysis_confidence(
        self,
        recent: HorizonAnalysis,
        has_break: bool,
        cyclical_confidence: float
    ) -> float:
        """计算综合分析置信度"""
        base_conf = recent.confidence_ceiling

        # 有断点会降低置信度
        if has_break:
            base_conf *= 0.85

        # 强周期性增加不确定性
        if cyclical_confidence > 0.5:
            base_conf *= 0.90

        return base_conf

    def _generate_professional_recommendation(
        self,
        metric_name: str,
        category: Optional[MetricCategory],
        recent: HorizonAnalysis,
        extended: Optional[HorizonAnalysis],
        effective: Optional[HorizonAnalysis],
        break_result: StructuralBreakResult,
        regime: str,
        cyclical_confidence: float,
        cycle_position: str,
    ) -> str:
        """生成专业级分析建议"""
        parts = []

        # 指标类别标签
        category_labels = {
            MetricCategory.EFFICIENCY: "📊效率",
            MetricCategory.GROWTH: "📈增长",
            MetricCategory.LEVERAGE: "⚖️杠杆",
            MetricCategory.TURNOVER: "🔄周转",
            MetricCategory.QUALITY: "💎质量",
        }
        cat_label = category_labels.get(category, "📊") if category else "📊"

        # 趋势判断
        slope = effective.slope if effective else recent.slope
        if slope > 0.10:
            parts.append(f"{cat_label} 强势向好 (斜率{slope:.1%})")
        elif slope > 0.03:
            parts.append(f"{cat_label} 稳步上升 (斜率{slope:.1%})")
        elif slope > -0.03:
            parts.append(f"{cat_label} 走势平稳")
        elif slope > -0.10:
            parts.append(f"{cat_label} 小幅下滑 (斜率{slope:.1%})")
        else:
            parts.append(f"{cat_label} 显著下滑 (斜率{slope:.1%})")

        # 断点信息
        if regime == "broken":
            parts.append(f"⚠️断点后数据({effective.years if effective else '?'}年)")
        elif regime == "transitional":
            if extended:
                parts.append(f"📊转换期: 近{recent.slope:.2%} vs 长{extended.slope:.2%}")

        # 周期信息
        if cyclical_confidence > 0.5:
            pos_labels = {
                "top": "🔝周期高位",
                "bottom": "🔻周期低位",
                "mid_up": "⬆️上升期",
                "mid_down": "⬇️下降期",
            }
            pos_label = pos_labels.get(cycle_position, "")
            if pos_label:
                parts.append(pos_label)

                # 周期位置警告
                if cycle_position == "top":
                    parts.append("💡注意均值回归")
                elif cycle_position == "bottom":
                    parts.append("💡关注反转机会")

        # 数据可靠性
        parts.append(f"可靠性:{recent.reliability_grade}")

        return " | ".join(parts)

    def _analyze_horizon(
        self,
        data: np.ndarray,
        name: str,
        start_index: int
    ) -> HorizonAnalysis:
        """分析单一时间窗口"""
        n = len(data)

        # 基本统计
        mean_val = float(np.mean(data))
        std_val = float(np.std(data, ddof=1)) if n > 1 else 0.0
        cv = std_val / abs(mean_val) if abs(mean_val) > 1e-10 else 0.0
        latest = float(data[-1])

        # 趋势统计
        if n >= 2:
            t = np.arange(n)
            slope, intercept = np.polyfit(t, data, 1)

            # R²
            y_pred = slope * t + intercept
            ss_res = np.sum((data - y_pred) ** 2)
            ss_tot = np.sum((data - mean_val) ** 2)
            r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

            # Theil-Sen稳健斜率
            robust_slope = self._theil_sen_slope(data)

            # CAGR
            if data[0] > 0 and data[-1] > 0:
                cagr = (data[-1] / data[0]) ** (1.0 / (n - 1)) - 1
            elif data[-1] != 0:
                cagr = slope / abs(mean_val) if abs(mean_val) > 1e-10 else 0.0
            else:
                cagr = 0.0
        else:
            slope = 0.0
            robust_slope = 0.0
            r_squared = 0.0
            cagr = 0.0

        # 可靠性评级
        grade, ceiling = self._get_reliability(n)

        return HorizonAnalysis(
            window_name=name,
            years=n,
            start_index=start_index,
            mean=mean_val,
            std=std_val,
            cv=cv,
            latest_value=latest,
            slope=float(slope),
            robust_slope=robust_slope,
            r_squared=float(r_squared),
            cagr=float(cagr),
            reliability_grade=grade,
            confidence_ceiling=ceiling
        )

    def _theil_sen_slope(self, data: np.ndarray) -> float:
        """计算Theil-Sen稳健斜率"""
        n = len(data)
        if n < 2:
            return 0.0

        slopes = []
        for i in range(n):
            for j in range(i + 1, n):
                slope = (data[j] - data[i]) / (j - i)
                slopes.append(slope)

        return float(np.median(slopes)) if slopes else 0.0

    def _get_reliability(self, n: int) -> Tuple[str, float]:
        """根据数据年数确定可靠性等级"""
        if n >= 10:
            return "A", 0.95
        elif n >= 7:
            return "B", 0.85
        elif n >= 5:
            return "C", 0.70
        elif n >= 3:
            return "D", 0.55
        else:
            return "F", 0.30

    def _compute_weights(
        self,
        n: int,
        break_result: StructuralBreakResult,
        recent: HorizonAnalysis,
        extended: Optional[HorizonAnalysis],
        base_recent_weight: float = 0.70,
        base_extended_weight: float = 0.30,
    ) -> Tuple[float, float]:
        """
        计算动态权重 (增强版)

        基于策略配置和数据特征动态调整权重:
        1. 基础权重来自策略配置（不同指标类别不同）
        2. 存在断点: 增加近期权重
        3. 近期高波动: 增加扩展权重（平滑噪音）
        4. 趋势分歧: 增加近期权重

        Args:
            base_recent_weight: 策略配置的基础近期权重
            base_extended_weight: 策略配置的基础扩展权重
        """
        recent_w = base_recent_weight
        extended_w = base_extended_weight

        # 只有近期数据
        if extended is None:
            return (1.0, 0.0)

        # 存在断点: 大幅提高近期权重
        if break_result.has_break:
            recent_w = max(recent_w, 0.85)
            extended_w = 1.0 - recent_w

        # 近期高波动: 增加扩展权重平滑噪音
        elif recent.cv > 0.30:
            # 但不能低于策略配置的60%
            recent_w = max(0.60, recent_w - 0.10)
            extended_w = 1.0 - recent_w

        # 趋势方向分歧: 可能是转折，更信任近期
        elif (recent.slope > 0 and extended.slope < 0) or \
             (recent.slope < 0 and extended.slope > 0):
            recent_w = max(recent_w, 0.80)
            extended_w = 1.0 - recent_w

        return (recent_w, extended_w)

    def _compute_effective_metrics(
        self,
        recent: HorizonAnalysis,
        extended: Optional[HorizonAnalysis],
        effective: Optional[HorizonAnalysis],
        weights: Tuple[float, float]
    ) -> Tuple[float, float]:
        """计算综合有效指标"""

        # 如果有断点后分析，优先使用
        if effective is not None:
            return (effective.slope, effective.cagr)

        # 加权计算
        if extended is not None:
            slope = weights[0] * recent.slope + weights[1] * extended.slope
            cagr = weights[0] * recent.cagr + weights[1] * extended.cagr
            return (slope, cagr)
        else:
            return (recent.slope, recent.cagr)

    def _determine_regime(
        self,
        break_result: StructuralBreakResult,
        recent: HorizonAnalysis,
        extended: Optional[HorizonAnalysis]
    ) -> str:
        """确定数据体制"""
        if break_result.has_break:
            return "broken"

        if extended is None:
            return "stable"

        # 检查趋势一致性
        if abs(recent.slope - extended.slope) > 0.10:
            return "transitional"

        return "stable"

    def _generate_recommendation(
        self,
        recent: HorizonAnalysis,
        extended: Optional[HorizonAnalysis],
        effective: Optional[HorizonAnalysis],
        break_result: StructuralBreakResult,
        regime: str,
        metric_name: str
    ) -> str:
        """生成分析建议"""
        parts = []

        if regime == "broken":
            parts.append(
                f"⚠️ 检测到结构断点: 建议使用断点后数据({effective.years if effective else '?'}年)"
            )

        if regime == "transitional":
            parts.append(
                f"📊 趋势转换期: 近期斜率{recent.slope:.3f} vs 长期{extended.slope:.3f}"
            )

        # 近期表现
        if recent.slope > 0.10:
            parts.append(f"✅ 近期强势增长 (斜率{recent.slope:.2%})")
        elif recent.slope < -0.10:
            parts.append(f"⚠️ 近期显著下滑 (斜率{recent.slope:.2%})")
        else:
            parts.append(f"➖ 近期走势平稳")

        # 数据可靠性
        parts.append(
            f"📈 数据可靠性: {recent.reliability_grade} "
            f"(置信上限{recent.confidence_ceiling:.0%})"
        )

        return " | ".join(parts)

    def default(self) -> MultiHorizonResult:
        """Return default result for insufficient data (ProbeProtocol compliance)."""
        # Return a minimal result with empty values
        return MultiHorizonResult(
            recent_analysis=None,
            extended_analysis=None,
            effective_analysis=None,
            structural_break=StructuralBreakResult(
                has_break=False,
                break_type=BreakType.NONE,
                break_year_index=None,
                break_significance=0.0,
                p_value=1.0,
                pre_break_stats={},
                post_break_stats={},
                recommended_window_start=0,
                confidence=0.0,
            ),
            effective_slope=0.0,
            effective_cagr=0.0,
            data_regime="unknown",
            recent_weight=1.0,
            extended_weight=0.0,
            recommendation="数据不足，无法提供建议",
            warnings=["Insufficient data"],
        )


# =============================================================================
# 便捷函数
# =============================================================================

def analyze_multi_horizon(
    values: List[float],
    metric_name: str = "unknown",
    auto_classify: bool = True,
) -> MultiHorizonResult:
    """
    便捷函数: 执行多时间窗口分析 (增强版)

    Args:
        values: 时间序列数据
        metric_name: 指标名称（用于自动分类）
        auto_classify: 是否自动分类指标

    Returns:
        MultiHorizonResult: 分析结果

    Example:
        >>> data = [10, 12, 14, 16, 18, 20, 22, 24, 26, 28]
        >>> result = analyze_multi_horizon(data, "revenue")
        >>> print(result.metric_category)  # MetricCategory.GROWTH
        >>> print(result.cycle_position)
        >>> print(result.recommendation)
    """
    analyzer = MultiHorizonProbe(auto_classify=auto_classify)
    return analyzer.compute(values, metric_name)


def detect_structural_break(values: List[float]) -> StructuralBreakResult:
    """
    便捷函数: 检测结构断点

    Args:
        values: 时间序列数据

    Returns:
        StructuralBreakResult: 断点检测结果
    """
    detector = StructuralBreakDetector()
    return detector.detect(values)


# =============================================================================
# 测试代码
# =============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("多时间窗口分析探针测试 (增强版 v2.0)")
    print("=" * 70)

    # 测试1: ROE数据（效率类 - 自动分类）
    print("\n1. ROE数据（自动分类为效率类）:")
    roe_data = [12, 13, 14, 15, 16, 18, 17, 16, 15, 14]
    result = analyze_multi_horizon(roe_data, "roe")
    print(f"   指标类别: {result.metric_category.value if result.metric_category else 'N/A'}")
    print(f"   策略权重: 近期{result.recent_weight:.0%} / 扩展{result.extended_weight:.0%}")
    print(f"   有效斜率: {result.effective_slope:.4f}")
    print(f"   周期置信: {result.cyclical_confidence:.1%}")
    print(f"   周期位置: {result.cycle_position}")
    print(f"   分析置信: {result.analysis_confidence:.1%}")
    print(f"   建议: {result.recommendation}")

    # 测试2: 营收数据（增长类 - 有断点）
    print("\n2. 营收数据（增长类，有断点）:")
    revenue_data = [100, 105, 110, 108, 105, 200, 220, 250, 280, 300]
    result = analyze_multi_horizon(revenue_data, "revenue")
    print(f"   指标类别: {result.metric_category.value if result.metric_category else 'N/A'}")
    print(f"   数据体制: {result.data_regime}")
    print(f"   有断点: {result.structural_break.has_break}")
    if result.structural_break.has_break:
        print(f"   断点位置: 第{result.structural_break.break_year_index+1}年后")
    print(f"   有效斜率: {result.effective_slope:.4f}")
    print(f"   建议: {result.recommendation}")

    # 测试3: 资产负债率（杠杆类 - 高权重近期）
    print("\n3. 资产负债率（杠杆类）:")
    debt_data = [45, 48, 50, 52, 55, 58, 60, 63, 65, 68]
    result = analyze_multi_horizon(debt_data, "debt_ratio")
    print(f"   指标类别: {result.metric_category.value if result.metric_category else 'N/A'}")
    print(f"   策略权重: 近期{result.recent_weight:.0%} / 扩展{result.extended_weight:.0%}")
    print(f"   策略说明: {result.applied_strategy.description if result.applied_strategy else 'N/A'}")
    print(f"   有效斜率: {result.effective_slope:.4f}")
    print(f"   建议: {result.recommendation}")

    # 测试4: 现金流（质量类）
    print("\n4. 现金流（质量类）:")
    ocf_data = [50, 55, 60, 58, 65, 70, 68, 75, 80, 85]
    result = analyze_multi_horizon(ocf_data, "ocf_ratio")
    print(f"   指标类别: {result.metric_category.value if result.metric_category else 'N/A'}")
    print(f"   有效斜率: {result.effective_slope:.4f}")
    print(f"   有效CV: {result.effective_cv:.2f}")
    print(f"   最新值: {result.effective_latest:.1f}")
    print(f"   建议: {result.recommendation}")

    # 测试5: 只有5年数据
    print("\n5. 只有5年数据:")
    short_data = [15, 16, 17, 18, 19]
    result = analyze_multi_horizon(short_data, "roic")
    print(f"   数据年数: 5")
    print(f"   指标类别: {result.metric_category.value if result.metric_category else 'N/A'}")
    print(f"   数据质量: {result.data_quality_grade}")
    print(f"   分析置信: {result.analysis_confidence:.1%}")
    print(f"   有效斜率: {result.effective_slope:.4f}")
    print(f"   警告: {result.warnings}")

    print("\n" + "=" * 70)
    print("✓ 增强版测试完成!")
    print("=" * 70)
