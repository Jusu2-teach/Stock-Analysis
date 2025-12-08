"""
专业级数据驱动质量筛选器 (Professional Data-Driven Quality Filter)
===================================================================

第一性原理：让数据说话！

核心理念：
---------
我们有5年/10年的时间序列数据，数据本身就包含了所有信息：
- 数据特征决定公司类型，而非行业标签
- ROIC连续5年>15%，数据告诉我们这是优质公司
- ROIC大起大落，数据告诉我们这是周期公司
- ROIC连续下滑，数据告诉我们这家公司在恶化

不预设任何行业标准，完全从数据特征出发！

辅助维度：
---------
对于行业差异显著的指标（如负债率、周转率），提供行业上下文：
- 不是用行业标准筛选，而是用行业数据解释
- 负债率60%在新能源是正常的，在软件是危险信号
- 周转率0.2在重资产行业正常，在贸易行业是问题

数据驱动的公司模式分类：
----------------------
1. 【一直优秀】：5年数据都在高位，波动小，数据自证优秀
2. 【高速成长】：斜率陡峭向上，CAGR高，加速增长
3. 【稳健增长】：温和上升，高R²，低波动
4. 【困境反转】：断点检测确认拐点，近期动量转正
5. 【周期波动】：周期置信度高，需关注周期位置
6. 【持续恶化】：恶化概率高，连续下跌，贝叶斯确认
7. 【行业龙头】：行业内排名持续Top，相对优势明显

v2.0 增强：
---------
- 行业敏感指标的上下文解读
- 多维数据交叉验证强化
- 行业内相对位置作为辅助参考
- 数据异常检测与解释

作者: AStock Analysis System
日期: 2025-12-08
"""

import logging
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum

logger = logging.getLogger(__name__)


# ============================================================================
# 数据驱动的公司类型（由数据特征定义，非行业标签）
# ============================================================================

class CompanyPattern(Enum):
    """公司数据模式（完全由数据特征决定）"""

    CONSISTENTLY_EXCELLENT = "consistently_excellent"  # 一直优秀
    HIGH_GROWTH = "high_growth"                        # 高速成长
    STEADY_GROWTH = "steady_growth"                    # 稳健增长
    TURNAROUND = "turnaround"                          # 困境反转
    CYCLICAL = "cyclical"                              # 周期波动
    DETERIORATING = "deteriorating"                    # 持续恶化
    VOLATILE_UNSTABLE = "volatile_unstable"            # 波动不稳定
    INDUSTRY_LEADER = "industry_leader"                # 行业龙头（相对优势）
    AVERAGE = "average"                                # 普通/中等


# ============================================================================
# 行业敏感指标配置（用于解释，不用于筛选！）
# ============================================================================

# 这些指标在不同行业差异很大，需要行业上下文来正确解读
# 数据来源：实际A股数据统计（基于5yd_final_industry.csv）
INDUSTRY_SENSITIVE_METRICS = {
    # 负债率：重资产行业天然高负债
    "debt_to_assets": {
        "sensitivity": "high",  # 行业敏感度
        "industry_context": {
            # 高负债行业（资本密集型）- 数据显示均值50%+
            "新型电力": {"typical_range": (50, 70), "note": "重资产行业，高负债正常，均值60%"},
            "汽车整车": {"typical_range": (55, 70), "note": "重资产+供应链融资，均值63%"},
            "电气设备": {"typical_range": (40, 55), "note": "制造业中等负债，均值46%"},
            "专用机械": {"typical_range": (35, 50), "note": "制造业，均值43%"},
            "小金属": {"typical_range": (30, 50), "note": "资源类，均值39%"},
            "元器件": {"typical_range": (30, 50), "note": "制造业，均值39%"},
            # 低负债行业 - 数据显示均值35%以下
            "生物制药": {"typical_range": (15, 35), "note": "轻资产，高负债是风险信号，均值25%"},
            "医疗保健": {"typical_range": (15, 35), "note": "轻资产，现金流好，均值26%"},
            "软件服务": {"typical_range": (25, 45), "note": "轻资产，不需要太多负债，均值34%"},
            "半导体": {"typical_range": (20, 40), "note": "研发驱动，均值28%"},
            "化学制药": {"typical_range": (25, 40), "note": "中等负债，均值32%"},
            "IT设备": {"typical_range": (25, 45), "note": "中等负债，均值34%"},
        },
        "interpretation": "负债率需结合行业特性解读，重资产行业60%可接受，轻资产行业超40%需警惕"
    },

    # 周转率：商业模式决定
    "assets_turn": {
        "sensitivity": "high",
        "industry_context": {
            # 快周转行业 - 数据显示均值0.6+
            "小金属": {"typical_range": (0.6, 1.0), "note": "贸易属性，快周转，均值0.8"},
            "汽车整车": {"typical_range": (0.6, 1.0), "note": "规模效应，均值0.8"},
            "元器件": {"typical_range": (0.5, 0.9), "note": "制造+贸易，均值0.7"},
            "电气设备": {"typical_range": (0.5, 0.9), "note": "制造业，均值0.7"},
            # 中等周转
            "IT设备": {"typical_range": (0.4, 0.8), "note": "中等周转，均值0.6"},
            "专用机械": {"typical_range": (0.4, 0.7), "note": "制造业，均值0.5"},
            "医疗保健": {"typical_range": (0.4, 0.7), "note": "服务业，均值0.5"},
            "化学制药": {"typical_range": (0.3, 0.6), "note": "研发周期，均值0.5"},
            "半导体": {"typical_range": (0.3, 0.6), "note": "研发密集，均值0.5"},
            "软件服务": {"typical_range": (0.3, 0.6), "note": "轻资产，均值0.5"},
            # 慢周转行业 - 数据显示均值0.4以下
            "生物制药": {"typical_range": (0.2, 0.5), "note": "研发周期长，均值0.4"},
            "新型电力": {"typical_range": (0.1, 0.3), "note": "重资产，慢周转正常，均值0.2"},
        },
        "interpretation": "周转率需结合商业模式，新能源电站0.2正常，贸易公司0.2是问题"
    },

    # ROIC：核心指标，行业差异也大
    # 数据来源：实际统计各行业ROIC均值
    "roic": {
        "sensitivity": "medium",
        "industry_context": {
            # 高ROIC行业 - 均值8%+
            "医疗保健": {"typical_range": (8, 20), "note": "轻资产+高定价权，均值12%"},
            "小金属": {"typical_range": (5, 15), "note": "周期波动大，均值9%"},
            "专用机械": {"typical_range": (5, 15), "note": "制造业典型，均值8%"},
            "电气设备": {"typical_range": (5, 15), "note": "制造业，均值8%"},
            "元器件": {"typical_range": (4, 12), "note": "制造业，均值7%"},
            "半导体": {"typical_range": (4, 12), "note": "周期性强，均值7%"},
            "化学制药": {"typical_range": (4, 12), "note": "研发投入大，均值7%"},
            "IT设备": {"typical_range": (3, 10), "note": "竞争激烈，均值5%"},
            # 低ROIC行业 - 均值5%以下
            "新型电力": {"typical_range": (0, 8), "note": "重资产，回报周期长，均值4%"},
            "软件服务": {"typical_range": (0, 8), "note": "竞争激烈，均值3%"},
            "生物制药": {"typical_range": (-5, 8), "note": "研发亏损常见，均值1%"},
            "汽车整车": {"typical_range": (-10, 8), "note": "重资产+周期性，均值-0.4%"},
        },
        "interpretation": "ROIC是最核心指标，但需理解行业资本密集度差异。医疗12%是正常，汽车5%可能是行业优秀"
    },

    # ROE: 也有行业差异
    "roe": {
        "sensitivity": "medium",
        "industry_context": {
            "医疗保健": {"typical_range": (8, 20), "note": "高ROE行业，均值12%"},
            "小金属": {"typical_range": (5, 15), "note": "周期性，均值9%"},
            "专用机械": {"typical_range": (5, 15), "note": "制造业，均值9%"},
            "电气设备": {"typical_range": (5, 15), "note": "制造业，均值8%"},
            "半导体": {"typical_range": (4, 15), "note": "周期性，均值8%"},
            "化学制药": {"typical_range": (4, 12), "note": "研发型，均值7%"},
            "IT设备": {"typical_range": (2, 10), "note": "竞争激烈，均值5%"},
            "元器件": {"typical_range": (0, 10), "note": "周期性，均值3%"},
            "软件服务": {"typical_range": (-5, 10), "note": "竞争激烈，均值2%"},
            "新型电力": {"typical_range": (-10, 8), "note": "重资产，均值-2%"},
            "生物制药": {"typical_range": (-10, 8), "note": "研发亏损，均值-2%"},
            "汽车整车": {"typical_range": (-15, 5), "note": "周期底部，均值-6%"},
        },
        "interpretation": "ROE需结合负债率看，高ROE+高负债可能是杠杆驱动而非真实盈利能力"
    },
}

# 行业无关的指标：这些指标的好坏标准与行业关系不大
INDUSTRY_AGNOSTIC_METRICS = [
    "roic_trend_slope",       # 趋势方向：上升就是好，与行业无关
    "roic_r_squared",         # 趋势稳定性：高R²就是好
    "roic_cv",                # 波动率：低CV就是稳定
    "consecutive_decline",    # 连续下跌：下跌就是差信号
    "deterioration_prob",     # 恶化概率：高概率就是风险
    "recent_momentum",        # 近期动量：改善就是好信号
]


@dataclass
class DataDrivenProfile:
    """数据驱动的公司画像"""

    # 基本信息
    ts_code: str
    name: str
    industry: str  # 保留行业信息，用于上下文解释

    # 核心数据特征（由数据计算得出）
    pattern: CompanyPattern           # 数据模式
    pattern_confidence: float         # 模式置信度 (0-1)

    # 盈利能力特征
    roic_level: str                   # "excellent" / "good" / "moderate" / "poor"
    roic_mean: float                  # 5年平均ROIC
    roic_min: float                   # 5年最低ROIC
    roic_stability: float             # 稳定性 (1 - CV)

    # 趋势特征
    trend_direction: str              # "up" / "flat" / "down"
    trend_strength: float             # 趋势强度 (斜率绝对值)
    trend_consistency: float          # 趋势一致性 (R²)
    recent_momentum: str              # "accelerating" / "stable" / "decelerating"

    # 风险特征
    has_loss_years: bool              # 是否有亏损年份
    consecutive_decline: int          # 连续下跌年数
    deterioration_probability: float  # 恶化概率
    volatility_level: str             # "low" / "medium" / "high"

    # 综合评估（必须放在有默认值的字段之前）
    quality_score: float              # 综合质量分 (0-100)
    investment_thesis: str            # 投资逻辑

    # 以下字段有默认值
    # 行业上下文（辅助解读，非筛选依据）
    industry_context: Dict[str, Any] = field(default_factory=dict)
    industry_rank_pct: float = 0.0    # 行业内排名百分位 (0-1, 1=最好)

    # 多维交叉验证
    cross_validation: Dict[str, bool] = field(default_factory=dict)

    # 列表类型字段
    key_strengths: List[str] = field(default_factory=list)
    key_risks: List[str] = field(default_factory=list)
    data_warnings: List[str] = field(default_factory=list)  # 数据层面的警告


# ============================================================================
# 数据特征阈值（基于数据本身的绝对标准，与行业无关）
# ============================================================================

# 这些是"好"的绝对标准，不分行业
# 因为：资本是流动的，钱会流向回报高的地方
# WACC大约8%，长期ROIC<8%意味着毁灭价值，这是财务事实，与行业无关

DATA_THRESHOLDS = {
    # ROIC水平判断（基于WACC和资本成本的经济学原理）
    "roic_excellent": 15.0,      # 显著创造价值
    "roic_good": 10.0,           # 稳定创造价值
    "roic_moderate": 6.0,        # 勉强覆盖资本成本
    "roic_poor": 0.0,            # 毁灭价值

    # 趋势判断
    "slope_strong_up": 0.08,     # 强上升趋势
    "slope_mild_up": 0.02,       # 温和上升
    "slope_flat": -0.02,         # 平稳
    "slope_mild_down": -0.08,    # 温和下降
    # < -0.08 为强下降

    # 稳定性判断 (CV = 标准差/均值)
    "cv_very_stable": 0.15,      # 非常稳定
    "cv_stable": 0.25,           # 稳定
    "cv_moderate": 0.40,         # 中等波动
    # > 0.40 为高波动

    # R²趋势一致性
    "r2_strong": 0.7,            # 趋势明确
    "r2_moderate": 0.4,          # 趋势存在但有噪音
    # < 0.4 为趋势不明确

    # 风险阈值
    "deterioration_high": 0.7,   # 高恶化风险
    "deterioration_moderate": 0.4,
}


class DataDrivenQualityFilter:
    """
    专业级数据驱动质量筛选器

    核心原则：
    1. 数据特征说话：趋势、波动、恶化概率等数据决定公司类型
    2. 绝对标准为主：ROIC>15%是好的，这是资本回报的经济学原理
    3. 行业上下文辅助：对于行业敏感指标，提供解读而非改变标准
    4. 多维交叉验证：利润vs现金流、ROEvs ROIC、营收vs利润

    v2.0 增强：
    - 行业敏感指标的上下文解读
    - 行业内排名作为辅助信息
    - 强化交叉验证逻辑
    - 数据异常警告系统
    """

    def __init__(self, industry_data: pd.DataFrame = None):
        """
        初始化筛选器

        Args:
            industry_data: 包含所有公司数据的DataFrame，用于计算行业统计
        """
        self.thresholds = DATA_THRESHOLDS
        self.industry_stats = {}

        # 如果提供了行业数据，预计算行业统计
        if industry_data is not None:
            self._compute_industry_stats(industry_data)

    def _compute_industry_stats(self, df: pd.DataFrame) -> None:
        """预计算各行业的统计数据（用于上下文解读）"""
        if 'industry' not in df.columns:
            return

        metrics_to_compute = ['roic', 'roe', 'gross_margin', 'debt_to_assets', 'assets_turn']

        for industry in df['industry'].unique():
            ind_df = df[df['industry'] == industry]
            self.industry_stats[industry] = {}

            for metric in metrics_to_compute:
                # 尝试多种可能的列名
                col_candidates = [
                    metric,
                    f'{metric}_latest',
                    f'{metric}_weighted',
                ]
                col = None
                for c in col_candidates:
                    if c in ind_df.columns:
                        col = c
                        break

                if col and not ind_df[col].isna().all():
                    vals = ind_df[col].dropna()
                    self.industry_stats[industry][metric] = {
                        'mean': vals.mean(),
                        'std': vals.std(),
                        'median': vals.median(),
                        'q25': vals.quantile(0.25),
                        'q75': vals.quantile(0.75),
                        'min': vals.min(),
                        'max': vals.max(),
                        'count': len(vals),
                    }

    def _get_industry_context(self, row: pd.Series, industry: str) -> Dict[str, Any]:
        """
        获取行业敏感指标的上下文解读

        返回的是【解读信息】，不是【筛选标准】
        """
        context = {}

        for metric, config in INDUSTRY_SENSITIVE_METRICS.items():
            # 获取指标值
            val = self._safe_get(row, f'{metric}_latest', np.nan)
            if np.isnan(val):
                val = self._safe_get(row, metric, np.nan)

            if np.isnan(val):
                continue

            metric_context = {
                'value': val,
                'sensitivity': config['sensitivity'],
            }

            # 添加行业特定上下文
            if industry in config.get('industry_context', {}):
                ind_info = config['industry_context'][industry]
                typical_low, typical_high = ind_info['typical_range']

                metric_context['industry_typical_range'] = ind_info['typical_range']
                metric_context['industry_note'] = ind_info['note']

                # 判断是否在行业典型范围内
                if val < typical_low:
                    metric_context['vs_industry'] = 'below_typical'
                    metric_context['interpretation'] = f'{metric}={val:.1f}低于行业典型范围({typical_low}-{typical_high})'
                elif val > typical_high:
                    metric_context['vs_industry'] = 'above_typical'
                    metric_context['interpretation'] = f'{metric}={val:.1f}高于行业典型范围({typical_low}-{typical_high})'
                else:
                    metric_context['vs_industry'] = 'within_typical'
                    metric_context['interpretation'] = f'{metric}={val:.1f}在行业典型范围内'

            # 添加行业内排名（如果有统计数据）
            if industry in self.industry_stats and metric in self.industry_stats[industry]:
                stats = self.industry_stats[industry][metric]
                # 计算百分位
                if stats['max'] > stats['min']:
                    pct = (val - stats['min']) / (stats['max'] - stats['min'])
                    metric_context['industry_percentile'] = pct
                    metric_context['vs_industry_median'] = val - stats['median']

            context[metric] = metric_context

        return context

    def _cross_validate(self, row: pd.Series) -> Dict[str, bool]:
        """
        多维交叉验证（与pipeline配置一致）

        参考 analysis.yaml 的指标配置:
        - roic: 效率指标，核心筛选指标，交叉验证 roe/roiic
        - roiic: 增量指标，辅助指标，交叉验证 roic
        - total_revenue_ps: 规模指标，交叉验证 roe
        - eps: 规模指标，交叉验证 ocfps（现金含金量）
        - grossprofit_margin: 护城河指标，交叉验证 netprofit_margin
        - netprofit_margin: 盈利能力，交叉验证 grossprofit_margin/ocfps
        - roe: 股东回报，杜邦分解验证 netprofit_margin/roic
        - ocfps: 现金流指标，盈利质量验证
        """
        validations = {}

        # ========== 1. 利润 vs 现金流一致性（eps交叉验证ocfps）==========
        # 纸面富贵检测：利润高增但现金流恶化
        profit_slope = self._safe_get(row, 'eps_log_slope', 0)
        ocf_slope = self._safe_get(row, 'ocfps_log_slope', 0)
        validations['profit_ocf_aligned'] = not (profit_slope > 0.15 and ocf_slope < -0.05)
        validations['paper_profit_warning'] = profit_slope > 0.15 and ocf_slope < -0.05

        # ========== 2. ROE vs ROIC一致性（roe交叉验证roic）==========
        # 杠杆陷阱检测：ROE高但ROIC低，说明是杠杆驱动
        roe = self._safe_get(row, 'roe_latest', np.nan)
        roic = self._safe_get(row, 'roic_latest', np.nan)
        if not np.isnan(roe) and not np.isnan(roic):
            validations['roe_roic_consistent'] = abs(roe - roic) < 10
            validations['high_leverage_warning'] = roe > 15 and roic < 8
        else:
            validations['roe_roic_consistent'] = True  # 数据不足，不判断
            validations['high_leverage_warning'] = False

        # ========== 3. ROIC vs ROIIC一致性（roic交叉验证roiic）==========
        # 存量效率 vs 增量效率：如果ROIC好但ROIIC差，说明新投资效率在下降
        roiic = self._safe_get(row, 'roiic_latest', np.nan)
        if not np.isnan(roic) and not np.isnan(roiic):
            validations['roic_roiic_consistent'] = not (roic > 12 and roiic < 5)
            validations['declining_incremental_returns'] = roic > 10 and roiic < roic * 0.5
        else:
            validations['roic_roiic_consistent'] = True
            validations['declining_incremental_returns'] = False

        # ========== 4. 营收 vs ROE一致性（revenue交叉验证roe）==========
        # 低效扩张检测：营收高增但ROE低迷
        rev_slope = self._safe_get(row, 'total_revenue_ps_log_slope', 0)
        if not np.isnan(roe):
            validations['revenue_efficiency_aligned'] = not (rev_slope > 0.15 and roe < 5)
            validations['inefficient_expansion'] = rev_slope > 0.15 and roe < 5
        else:
            validations['revenue_efficiency_aligned'] = True
            validations['inefficient_expansion'] = False

        # ========== 5. 营收 vs 利润一致性（增收是否增利）==========
        rev_cagr = self._safe_get(row, 'total_revenue_ps_cagr', 0)
        profit_cagr = self._safe_get(row, 'eps_cagr', 0)
        validations['revenue_profit_aligned'] = (rev_cagr * profit_cagr) >= 0

        # ========== 6. 毛利率 vs 净利率一致性（费用控制）==========
        # 费用失控检测：毛利率稳定但净利率暴跌
        gm_slope = self._safe_get(row, 'grossprofit_margin_log_slope', 0)
        nm_slope = self._safe_get(row, 'netprofit_margin_log_slope', 0)
        validations['margin_structure_healthy'] = not (gm_slope > -0.02 and nm_slope < -0.05)
        validations['expense_out_of_control'] = gm_slope > -0.02 and nm_slope < -0.05

        # ========== 7. 趋势 vs 恶化概率一致性 ==========
        slope = self._safe_get(row, 'roic_log_slope', 0)
        deterioration = self._safe_get(row, 'roic_deterioration_probability', 0)
        validations['trend_deterioration_consistent'] = not (slope > 0.05 and deterioration > 0.6)

        # ========== 8. OLS趋势 vs 稳健趋势一致性 ==========
        robust_slope = self._safe_get(row, 'roic_robust_slope', np.nan)
        if not np.isnan(robust_slope):
            # 如果OLS显示上升但稳健检验显示下降，可能有异常值干扰
            validations['trend_robust_consistent'] = not (slope > 0.05 and robust_slope < -0.02)
        else:
            validations['trend_robust_consistent'] = True

        # ========== 9. 最新值 vs 历史趋势一致性 ==========
        roic_latest = self._safe_get(row, 'roic_latest', np.nan)
        roic_weighted = self._safe_get(row, 'roic_weighted', np.nan)
        if not np.isnan(roic_latest) and not np.isnan(roic_weighted) and roic_weighted != 0:
            validations['latest_vs_trend_normal'] = abs(roic_latest - roic_weighted) < abs(roic_weighted) * 0.5
        else:
            validations['latest_vs_trend_normal'] = True

        # ========== 10. 连续下跌检测 ==========
        consecutive_decline = int(self._safe_get(row, 'roic_consecutive_decline_years', 0))
        validations['no_consecutive_decline'] = consecutive_decline < 3

        return validations

    def _generate_data_warnings(
        self,
        row: pd.Series,
        cross_validation: Dict[str, bool],
        industry_context: Dict[str, Any],
    ) -> List[str]:
        """
        生成数据层面的警告

        与pipeline的交叉验证配置一致
        """
        warnings = []

        # ========== 核心交叉验证警告 ==========

        # 1. 纸面富贵（利润vs现金流）
        if cross_validation.get('paper_profit_warning', False):
            warnings.append("🚨 纸面富贵：利润高增但现金流恶化")

        # 2. 杠杆陷阱（ROE vs ROIC）
        if cross_validation.get('high_leverage_warning', False):
            warnings.append("🚨 杠杆陷阱：ROE高但ROIC低，杠杆驱动收益")

        # 3. 增量效率下降（ROIC vs ROIIC）
        if cross_validation.get('declining_incremental_returns', False):
            warnings.append("⚠️ 增量资本效率下降：ROIIC显著低于ROIC")

        # 4. 低效扩张（营收vs ROE）
        if cross_validation.get('inefficient_expansion', False):
            warnings.append("⚠️ 低效扩张：营收高增长但ROE低迷")

        # 5. 费用失控（毛利率vs净利率）
        if cross_validation.get('expense_out_of_control', False):
            warnings.append("⚠️ 费用失控：毛利率稳定但净利率暴跌")

        # 6. 营收利润不一致
        if not cross_validation.get('revenue_profit_aligned', True):
            warnings.append("⚠️ 增收不增利：营收与利润增长方向不一致")

        # 7. 趋势信号矛盾
        if not cross_validation.get('trend_deterioration_consistent', True):
            warnings.append("⚠️ 信号矛盾：上升趋势但恶化概率高")

        # 8. 稳健检验不通过
        if not cross_validation.get('trend_robust_consistent', True):
            warnings.append("⚠️ 趋势异常：OLS趋势与稳健检验不一致，可能有异常值")

        # 9. 最新值偏离
        if not cross_validation.get('latest_vs_trend_normal', True):
            warnings.append("⚠️ 数据异常：最新数据显著偏离历史趋势")

        # 10. 连续下跌
        if not cross_validation.get('no_consecutive_decline', True):
            warnings.append("🚨 持续恶化：连续3年以上下跌")

        # ========== 行业敏感指标警告 ==========
        for metric, ctx in industry_context.items():
            if ctx.get('vs_industry') == 'above_typical':
                if metric == 'debt_to_assets':
                    warnings.append(f"⚠️ 负债率({ctx['value']:.1f}%)高于行业典型水平")
            elif ctx.get('vs_industry') == 'below_typical':
                if metric in ('roic', 'roe', 'gross_margin'):
                    warnings.append(f"📊 {metric}({ctx['value']:.1f})低于行业典型水平（可能是行业特性）")

        return warnings

    def analyze_company(
        self,
        row: pd.Series,
        historical_data: pd.DataFrame = None,
        all_data: pd.DataFrame = None,
    ) -> DataDrivenProfile:
        """
        分析单个公司，生成数据驱动的画像

        Args:
            row: 公司当前数据行（包含趋势分析结果）
            historical_data: 可选的历史数据（用于更深入分析）
            all_data: 可选的全量数据（用于计算行业排名）
        """
        # 基本信息
        ts_code = str(row.get('ts_code', ''))
        name = str(row.get('name', ''))
        industry = str(row.get('industry', ''))

        # ========== 1. 提取核心数据特征 ==========

        # ROIC相关
        roic_latest = self._safe_get(row, 'roic_latest', np.nan)
        roic_weighted = self._safe_get(row, 'roic_weighted', roic_latest)
        roic_log_slope = self._safe_get(row, 'roic_log_slope', 0)
        roic_r_squared = self._safe_get(row, 'roic_r_squared', 0)
        roic_cv = self._safe_get(row, 'roic_cv', 1.0)
        roic_recent_slope = self._safe_get(row, 'roic_recent_3y_slope', roic_log_slope)

        # 恶化相关
        deterioration_prob = self._safe_get(row, 'roic_deterioration_probability', 0)
        consecutive_decline = int(self._safe_get(row, 'roic_consecutive_decline_years', 0))
        has_deterioration = row.get('roic_has_deterioration', False)

        # 周期相关
        is_cyclical = row.get('roic_is_cyclical', False)
        cyclical_confidence = self._safe_get(row, 'roic_cyclical_confidence', 0)
        current_phase = str(row.get('roic_current_phase', ''))

        # 反转相关
        is_turnaround = row.get('roic_is_turnaround', False) or row.get('eps_is_turnaround', False)

        # ROE/盈利相关
        roe_latest = self._safe_get(row, 'roe_latest', np.nan)
        roe_log_slope = self._safe_get(row, 'roe_log_slope', 0)

        # 现金流
        ocf_slope = self._safe_get(row, 'ocfps_log_slope', 0)

        # 成长
        revenue_cagr = self._safe_get(row, 'total_revenue_ps_cagr', 0)
        profit_cagr = self._safe_get(row, 'eps_cagr', 0)

        # ========== 2. 行业上下文解读 ==========
        industry_context = self._get_industry_context(row, industry)

        # ========== 3. 多维交叉验证 ==========
        cross_validation = self._cross_validate(row)

        # ========== 4. 计算行业内排名 ==========
        industry_rank_pct = 0.5  # 默认中位
        if all_data is not None and 'industry' in all_data.columns:
            ind_data = all_data[all_data['industry'] == industry]
            if len(ind_data) > 1:
                roic_col = 'roic_latest' if 'roic_latest' in ind_data.columns else 'roic'
                if roic_col in ind_data.columns:
                    # 计算百分位排名
                    rank = (ind_data[roic_col] < roic_latest).sum() / len(ind_data)
                    industry_rank_pct = rank

        # ========== 5. 判断ROIC水平 ==========
        roic_mean = roic_weighted if not np.isnan(roic_weighted) else roic_latest
        roic_min = roic_latest - roic_cv * roic_latest if roic_cv < 1 else roic_latest * 0.5

        if roic_mean >= self.thresholds["roic_excellent"]:
            roic_level = "excellent"
        elif roic_mean >= self.thresholds["roic_good"]:
            roic_level = "good"
        elif roic_mean >= self.thresholds["roic_moderate"]:
            roic_level = "moderate"
        else:
            roic_level = "poor"

        # ========== 6. 判断趋势方向 ==========
        if roic_log_slope >= self.thresholds["slope_strong_up"]:
            trend_direction = "up"
            trend_strength = roic_log_slope
        elif roic_log_slope >= self.thresholds["slope_mild_up"]:
            trend_direction = "up"
            trend_strength = roic_log_slope
        elif roic_log_slope >= self.thresholds["slope_flat"]:
            trend_direction = "flat"
            trend_strength = abs(roic_log_slope)
        elif roic_log_slope >= self.thresholds["slope_mild_down"]:
            trend_direction = "down"
            trend_strength = abs(roic_log_slope)
        else:
            trend_direction = "down"
            trend_strength = abs(roic_log_slope)

        # ========== 7. 判断动量（加速/减速）==========
        slope_diff = roic_recent_slope - roic_log_slope
        if slope_diff > 0.03:
            recent_momentum = "accelerating"
        elif slope_diff < -0.03:
            recent_momentum = "decelerating"
        else:
            recent_momentum = "stable"

        # ========== 8. 判断波动性 ==========
        if roic_cv <= self.thresholds["cv_very_stable"]:
            volatility_level = "low"
        elif roic_cv <= self.thresholds["cv_stable"]:
            volatility_level = "low"
        elif roic_cv <= self.thresholds["cv_moderate"]:
            volatility_level = "medium"
        else:
            volatility_level = "high"

        # ========== 9. 识别公司模式（核心！）==========
        pattern, pattern_confidence = self._identify_pattern(
            roic_level=roic_level,
            roic_mean=roic_mean,
            roic_min=roic_min,
            trend_direction=trend_direction,
            trend_strength=trend_strength,
            roic_r_squared=roic_r_squared,
            roic_cv=roic_cv,
            recent_momentum=recent_momentum,
            consecutive_decline=consecutive_decline,
            deterioration_prob=deterioration_prob,
            is_cyclical=is_cyclical,
            cyclical_confidence=cyclical_confidence,
            is_turnaround=is_turnaround,
            revenue_cagr=revenue_cagr,
            profit_cagr=profit_cagr,
            ocf_slope=ocf_slope,
            industry_rank_pct=industry_rank_pct,
        )

        # ========== 10. 计算综合质量分 ==========
        quality_score = self._calculate_quality_score(
            roic_level=roic_level,
            roic_mean=roic_mean,
            trend_direction=trend_direction,
            roic_r_squared=roic_r_squared,
            roic_cv=roic_cv,
            deterioration_prob=deterioration_prob,
            consecutive_decline=consecutive_decline,
            ocf_slope=ocf_slope,
            pattern=pattern,
            cross_validation=cross_validation,
        )

        # ========== 11. 生成数据警告 ==========
        data_warnings = self._generate_data_warnings(row, cross_validation, industry_context)

        # ========== 12. 生成投资逻辑 ==========
        thesis, strengths, risks = self._generate_thesis(
            pattern=pattern,
            roic_level=roic_level,
            roic_mean=roic_mean,
            trend_direction=trend_direction,
            recent_momentum=recent_momentum,
            volatility_level=volatility_level,
            consecutive_decline=consecutive_decline,
            deterioration_prob=deterioration_prob,
            revenue_cagr=revenue_cagr,
            profit_cagr=profit_cagr,
            ocf_slope=ocf_slope,
            industry=industry,
            industry_rank_pct=industry_rank_pct,
        )

        return DataDrivenProfile(
            ts_code=ts_code,
            name=name,
            industry=industry,
            pattern=pattern,
            pattern_confidence=pattern_confidence,
            roic_level=roic_level,
            roic_mean=roic_mean,
            roic_min=roic_min,
            roic_stability=max(0, 1 - roic_cv),
            trend_direction=trend_direction,
            trend_strength=trend_strength,
            trend_consistency=roic_r_squared,
            recent_momentum=recent_momentum,
            has_loss_years=roic_min < 0,
            consecutive_decline=consecutive_decline,
            deterioration_probability=deterioration_prob,
            volatility_level=volatility_level,
            industry_context=industry_context,
            industry_rank_pct=industry_rank_pct,
            cross_validation=cross_validation,
            quality_score=quality_score,
            investment_thesis=thesis,
            key_strengths=strengths,
            key_risks=risks,
            data_warnings=data_warnings,
        )

    def _safe_get(self, row: pd.Series, col: str, default: float) -> float:
        """安全获取值"""
        if col not in row.index:
            return default
        val = row[col]
        if pd.isna(val):
            return default
        return float(val)

    def _identify_pattern(
        self,
        roic_level: str,
        roic_mean: float,
        roic_min: float,
        trend_direction: str,
        trend_strength: float,
        roic_r_squared: float,
        roic_cv: float,
        recent_momentum: str,
        consecutive_decline: int,
        deterioration_prob: float,
        is_cyclical: bool,
        cyclical_confidence: float,
        is_turnaround: bool,
        revenue_cagr: float,
        profit_cagr: float,
        ocf_slope: float,
        industry_rank_pct: float = 0.5,
    ) -> Tuple[CompanyPattern, float]:
        """
        识别公司的数据模式

        这是核心逻辑！完全基于数据特征判断
        行业排名作为辅助信息，用于识别行业龙头
        """
        confidence = 0.5  # 基础置信度

        # ========== 模式0：行业龙头（优先判断）==========
        # 条件：行业内排名Top10% + ROIC不差 + 趋势不恶化
        if (industry_rank_pct >= 0.90 and  # Top 10%
            roic_level in ("excellent", "good", "moderate") and
            consecutive_decline <= 1 and
            deterioration_prob < 0.5):

            confidence = 0.7 + 0.2 * industry_rank_pct + 0.1 * (1 - deterioration_prob)
            return CompanyPattern.INDUSTRY_LEADER, min(confidence, 0.95)

        # ========== 模式1：一直优秀 ==========
        # 条件：ROIC高+稳定+没有亏损年+趋势不下降
        if (roic_level in ("excellent", "good") and
            roic_min > 8.0 and
            roic_cv < 0.3 and
            trend_direction != "down" and
            consecutive_decline == 0):

            confidence = 0.7 + 0.1 * (roic_mean / 20) + 0.1 * (1 - roic_cv) + 0.1 * roic_r_squared
            return CompanyPattern.CONSISTENTLY_EXCELLENT, min(confidence, 0.98)

        # ========== 模式2：高速成长 ==========
        # 条件：趋势强劲上升+增长率高+动量加速
        if (trend_direction == "up" and
            trend_strength > 0.08 and
            roic_r_squared > 0.5 and
            (revenue_cagr > 0.15 or profit_cagr > 0.20) and
            recent_momentum in ("accelerating", "stable")):

            confidence = 0.6 + 0.15 * min(trend_strength / 0.15, 1) + 0.15 * roic_r_squared
            return CompanyPattern.HIGH_GROWTH, min(confidence, 0.95)

        # ========== 模式3：稳健增长 ==========
        # 条件：温和上升+高R²+低波动
        steady_slope = trend_strength if trend_direction == "up" else 0
        if (trend_direction in ("up", "flat") and
            steady_slope >= 0 and
            roic_r_squared > 0.6 and
            roic_cv < 0.25 and
            roic_level in ("excellent", "good", "moderate") and
            roic_mean > 6.0):

            confidence = 0.6 + 0.2 * roic_r_squared + 0.1 * (1 - roic_cv)
            return CompanyPattern.STEADY_GROWTH, min(confidence, 0.90)

        # ========== 模式4：困境反转 ==========
        # 条件：有反转信号+近期动量转好
        if (is_turnaround and
            recent_momentum in ("accelerating", "stable") and
            roic_mean > 0):  # 至少还是正的

            confidence = 0.5 + 0.2 * (1 if recent_momentum == "accelerating" else 0)
            return CompanyPattern.TURNAROUND, min(confidence, 0.80)

        # ========== 模式5：周期波动 ==========
        # 条件：周期性特征明显+高波动
        if (is_cyclical and cyclical_confidence > 0.6) or (roic_cv > 0.4 and roic_r_squared < 0.5):
            confidence = 0.5 + 0.3 * cyclical_confidence
            return CompanyPattern.CYCLICAL, min(confidence, 0.85)

        # ========== 模式6：持续恶化 ==========
        # 条件：连续下跌+恶化概率高
        if (consecutive_decline >= 3 or
            deterioration_prob > 0.7 or
            (trend_direction == "down" and trend_strength > 0.1)):

            confidence = 0.6 + 0.2 * deterioration_prob + 0.1 * min(consecutive_decline / 4, 1)
            return CompanyPattern.DETERIORATING, min(confidence, 0.95)

        # ========== 模式7：波动不稳定 ==========
        if roic_cv > 0.5 and roic_r_squared < 0.4:
            confidence = 0.5 + 0.2 * roic_cv
            return CompanyPattern.VOLATILE_UNSTABLE, min(confidence, 0.80)

        # ========== 默认：普通 ==========
        return CompanyPattern.AVERAGE, 0.5

    def _calculate_quality_score(
        self,
        roic_level: str,
        roic_mean: float,
        trend_direction: str,
        roic_r_squared: float,
        roic_cv: float,
        deterioration_prob: float,
        consecutive_decline: int,
        ocf_slope: float,
        pattern: CompanyPattern,
        cross_validation: Dict[str, bool] = None,
    ) -> float:
        """
        计算综合质量分 (0-100)

        增强：考虑交叉验证结果
        """
        score = 50.0  # 基础分

        # ROIC水平加分 (最多+30)
        level_scores = {"excellent": 30, "good": 20, "moderate": 10, "poor": -10}
        score += level_scores.get(roic_level, 0)

        # ROIC绝对值加分 (最多+10)
        score += min(roic_mean / 2, 10)

        # 趋势方向加分 (最多+10)
        if trend_direction == "up":
            score += 10
        elif trend_direction == "flat":
            score += 5
        else:
            score -= 10

        # 趋势一致性加分 (最多+10)
        score += roic_r_squared * 10

        # 稳定性加分 (最多+10)
        score += (1 - min(roic_cv, 1)) * 10

        # 现金流加分 (最多+10)
        if ocf_slope > 0.05:
            score += 10
        elif ocf_slope > 0:
            score += 5
        elif ocf_slope < -0.05:
            score -= 10

        # 风险扣分
        score -= deterioration_prob * 20
        score -= consecutive_decline * 5

        # 交叉验证扣分（数据不一致需要警惕）
        if cross_validation:
            if not cross_validation.get('profit_ocf_aligned', True):
                score -= 8  # 利润与现金流不一致
            if cross_validation.get('high_leverage_warning', False):
                score -= 5  # 高杠杆风险
            if not cross_validation.get('revenue_profit_aligned', True):
                score -= 5  # 营收利润不一致
            if not cross_validation.get('trend_deterioration_consistent', True):
                score -= 3  # 信号矛盾

        # 模式调整
        pattern_adjustments = {
            CompanyPattern.CONSISTENTLY_EXCELLENT: 10,
            CompanyPattern.HIGH_GROWTH: 8,
            CompanyPattern.STEADY_GROWTH: 5,
            CompanyPattern.INDUSTRY_LEADER: 6,  # 行业龙头
            CompanyPattern.TURNAROUND: 0,
            CompanyPattern.CYCLICAL: -5,
            CompanyPattern.DETERIORATING: -20,
            CompanyPattern.VOLATILE_UNSTABLE: -10,
            CompanyPattern.AVERAGE: 0,
        }
        score += pattern_adjustments.get(pattern, 0)

        return max(0, min(100, score))

    def _generate_thesis(
        self,
        pattern: CompanyPattern,
        roic_level: str,
        roic_mean: float,
        trend_direction: str,
        recent_momentum: str,
        volatility_level: str,
        consecutive_decline: int,
        deterioration_prob: float,
        revenue_cagr: float,
        profit_cagr: float,
        ocf_slope: float,
        industry: str,
        industry_rank_pct: float = 0.5,
    ) -> Tuple[str, List[str], List[str]]:
        """生成投资逻辑和优劣势"""

        strengths = []
        risks = []

        # 根据模式生成投资逻辑
        thesis_map = {
            CompanyPattern.CONSISTENTLY_EXCELLENT:
                f"长期稳定的优质公司，ROIC均值{roic_mean:.1f}%，适合长期持有",
            CompanyPattern.HIGH_GROWTH:
                f"高速成长股，营收CAGR {revenue_cagr:.1%}，利润CAGR {profit_cagr:.1%}，处于快速扩张期",
            CompanyPattern.STEADY_GROWTH:
                f"稳健成长型，趋势一致性高，波动率{volatility_level}，适合稳健投资",
            CompanyPattern.INDUSTRY_LEADER:
                f"{industry}行业龙头，行业内排名Top{(1-industry_rank_pct)*100:.0f}%，具备相对竞争优势",
            CompanyPattern.TURNAROUND:
                f"困境反转机会，近期动量{recent_momentum}，需密切关注反转进程",
            CompanyPattern.CYCLICAL:
                f"周期性公司，波动性{volatility_level}，需要关注周期位置",
            CompanyPattern.DETERIORATING:
                f"警惕！持续恶化中，连续下跌{consecutive_decline}年，恶化概率{deterioration_prob:.0%}",
            CompanyPattern.VOLATILE_UNSTABLE:
                f"波动不稳定，难以预测，不建议作为核心持仓",
            CompanyPattern.AVERAGE:
                f"表现平平，无明显亮点",
        }
        thesis = thesis_map.get(pattern, "数据不足，无法判断")

        # 优势
        if roic_level in ("excellent", "good"):
            strengths.append(f"盈利能力强(ROIC {roic_mean:.1f}%)")
        if trend_direction == "up":
            strengths.append("趋势向上")
        if volatility_level == "low":
            strengths.append("经营稳定")
        if ocf_slope > 0.05:
            strengths.append("现金流健康")
        if recent_momentum == "accelerating":
            strengths.append("增长加速")
        if industry_rank_pct >= 0.8:
            strengths.append(f"行业内Top{(1-industry_rank_pct)*100:.0f}%")

        # 风险
        if roic_level == "poor":
            risks.append(f"盈利能力差(ROIC {roic_mean:.1f}%)")
        if consecutive_decline >= 2:
            risks.append(f"连续{consecutive_decline}年下跌")
        if deterioration_prob > 0.5:
            risks.append(f"恶化风险{deterioration_prob:.0%}")
        if volatility_level == "high":
            risks.append("波动性大")
        if ocf_slope < -0.05:
            risks.append("现金流恶化")
        if recent_momentum == "decelerating":
            risks.append("增长放缓")

        return thesis, strengths, risks

    def filter_companies(
        self,
        df: pd.DataFrame,
        patterns: List[CompanyPattern] = None,
        min_score: float = 70.0,
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        筛选公司

        Args:
            df: 原始数据（需包含趋势分析结果列）
            patterns: 要筛选的模式列表，None表示选择优质模式
            min_score: 最低质量分

        Returns:
            (筛选后的df, 统计信息)
        """
        if patterns is None:
            # 默认选择优质模式
            patterns = [
                CompanyPattern.CONSISTENTLY_EXCELLENT,
                CompanyPattern.HIGH_GROWTH,
                CompanyPattern.STEADY_GROWTH,
            ]

        results = []
        for _, row in df.iterrows():
            profile = self.analyze_company(row)
            results.append({
                'ts_code': profile.ts_code,
                'name': profile.name,
                'industry': profile.industry,
                'pattern': profile.pattern.value,
                'pattern_confidence': profile.pattern_confidence,
                'quality_score': profile.quality_score,
                'roic_level': profile.roic_level,
                'roic_mean': profile.roic_mean,
                'trend_direction': profile.trend_direction,
                'volatility_level': profile.volatility_level,
                'investment_thesis': profile.investment_thesis,
                'key_strengths': '; '.join(profile.key_strengths),
                'key_risks': '; '.join(profile.key_risks),
            })

        results_df = pd.DataFrame(results)

        # 筛选
        pattern_values = [p.value for p in patterns]
        filtered = results_df[
            (results_df['pattern'].isin(pattern_values)) &
            (results_df['quality_score'] >= min_score)
        ].copy()

        # 按质量分排序
        filtered = filtered.sort_values('quality_score', ascending=False)

        # 统计信息
        pattern_counts = results_df['pattern'].value_counts().to_dict()
        stats = {
            'total': len(df),
            'selected': len(filtered),
            'pattern_distribution': pattern_counts,
            'avg_score_selected': filtered['quality_score'].mean() if len(filtered) > 0 else 0,
        }

        return filtered, stats


# ============================================================================
# 便捷函数
# ============================================================================

def analyze_and_filter(
    df: pd.DataFrame,
    min_score: float = 70.0,
    include_turnaround: bool = False,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
    """
    分析并筛选公司

    Args:
        df: 包含趋势分析结果的数据框
        min_score: 最低质量分
        include_turnaround: 是否包含反转型公司

    Returns:
        (精选池, 观察池, 统计信息)
    """
    filter_engine = DataDrivenQualityFilter()

    # 精选模式
    elite_patterns = [
        CompanyPattern.CONSISTENTLY_EXCELLENT,
        CompanyPattern.HIGH_GROWTH,
        CompanyPattern.STEADY_GROWTH,
    ]
    if include_turnaround:
        elite_patterns.append(CompanyPattern.TURNAROUND)

    selected, stats = filter_engine.filter_companies(
        df, patterns=elite_patterns, min_score=min_score
    )

    # 观察池：质量分60-70，或反转型
    watch_patterns = [
        CompanyPattern.TURNAROUND,
        CompanyPattern.CYCLICAL,
    ]
    watch, _ = filter_engine.filter_companies(
        df, patterns=watch_patterns, min_score=60.0
    )
    # 排除已在精选池的
    watch = watch[~watch['ts_code'].isin(selected['ts_code'])]

    return selected, watch, stats


def explain_company(row: pd.Series, all_data: pd.DataFrame = None) -> str:
    """
    解释单个公司的数据分析结果

    Args:
        row: 公司数据行
        all_data: 全量数据（用于计算行业排名）

    Returns:
        Markdown格式的分析报告
    """
    filter_engine = DataDrivenQualityFilter(industry_data=all_data)
    profile = filter_engine.analyze_company(row, all_data=all_data)

    pattern_emoji = {
        CompanyPattern.CONSISTENTLY_EXCELLENT: "🏆",
        CompanyPattern.HIGH_GROWTH: "🚀",
        CompanyPattern.STEADY_GROWTH: "📈",
        CompanyPattern.INDUSTRY_LEADER: "👑",
        CompanyPattern.TURNAROUND: "🔄",
        CompanyPattern.CYCLICAL: "🔁",
        CompanyPattern.DETERIORATING: "📉",
        CompanyPattern.VOLATILE_UNSTABLE: "⚡",
        CompanyPattern.AVERAGE: "➖",
    }

    lines = [
        f"## {profile.name} ({profile.ts_code})",
        f"**行业**: {profile.industry}",
        f"**数据模式**: {pattern_emoji.get(profile.pattern, '')} {profile.pattern.value} (置信度{profile.pattern_confidence:.0%})",
        f"**质量评分**: {profile.quality_score:.0f}/100",
        f"**行业内排名**: Top {(1-profile.industry_rank_pct)*100:.0f}%",
        "",
        f"### 投资逻辑",
        f"{profile.investment_thesis}",
        "",
        f"### 核心数据特征（数据说话）",
        f"- **盈利水平**: {profile.roic_level} (ROIC均值{profile.roic_mean:.1f}%)",
        f"- **趋势方向**: {profile.trend_direction} (R²={profile.trend_consistency:.2f})",
        f"- **近期动量**: {profile.recent_momentum}",
        f"- **波动水平**: {profile.volatility_level} (稳定性={profile.roic_stability:.0%})",
        f"- **恶化风险**: {profile.deterioration_probability:.0%}",
        f"- **连续下跌**: {profile.consecutive_decline}年",
    ]

    # 行业上下文解读
    if profile.industry_context:
        lines.append("")
        lines.append("### 📊 行业敏感指标解读")
        for metric, ctx in profile.industry_context.items():
            if 'interpretation' in ctx:
                lines.append(f"- {ctx['interpretation']}")
            if 'industry_note' in ctx:
                lines.append(f"  - 💡 {ctx['industry_note']}")

    # 交叉验证结果
    if profile.cross_validation:
        lines.append("")
        lines.append("### 🔍 数据交叉验证")
        cv = profile.cross_validation
        if cv.get('profit_ocf_aligned', True):
            lines.append("- ✅ 利润与现金流趋势一致")
        else:
            lines.append("- ❌ 利润与现金流趋势不一致")

        if not cv.get('high_leverage_warning', False):
            lines.append("- ✅ 杠杆水平正常")
        else:
            lines.append("- ⚠️ 高杠杆驱动ROE")

        if cv.get('revenue_profit_aligned', True):
            lines.append("- ✅ 营收与利润增长一致")
        else:
            lines.append("- ❌ 营收与利润增长不一致")

    if profile.key_strengths:
        lines.append("")
        lines.append("### ✅ 核心优势")
        for s in profile.key_strengths:
            lines.append(f"- {s}")

    if profile.key_risks:
        lines.append("")
        lines.append("### ⚠️ 核心风险")
        for r in profile.key_risks:
            lines.append(f"- {r}")

    if profile.data_warnings:
        lines.append("")
        lines.append("### 🚨 数据层面警告")
        for w in profile.data_warnings:
            lines.append(f"- {w}")

    return "\n".join(lines)
