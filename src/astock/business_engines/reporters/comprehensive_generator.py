"""
综合趋势分析报告生成器 (Comprehensive Trend Report Generator)
===================================================================

聚焦于找出3类真正无懈可击的优质公司：
1. 🚀 高成长优质公司 (GARP) - 高增长 + 现金流健康 + 无恶化信号
2. 🏰 白马护城河 (Quality Moat) - 高ROE/ROIC稳定 + 高毛利 + 低波动
3. 🔄 困境反转 (Turnaround) - 断点确认 + 质量改善 + 规模保障

评估体系采用4层过滤架构：
- 第1层：基础质量过滤（趋势斜率、R²、波动率）
- 第2层：交叉验证（利润vs现金流、营收vsROE、ROEvsROIC）
- 第3层：拐点检测（断点检测、恶化概率、反转信号）
- 第4层：综合评分&分类

专业增强 v3.0：
- 行业自适应阈值：不同行业使用差异化标准
- 多元化公司处理：识别并特殊处理多元化集团
- 行业相对排名：使用行业内百分位而非绝对阈值

数据输入：直接接收探针分析结果 DataFrame

注：T.R.U.T.H.系统已独立到 truth_report_generator.py

作者: AStock Analysis System
日期: 2025-12-08
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Tuple
from datetime import datetime
import logging

from shared.naming_convention import (
    MetricRegistry,
    FieldRegistry,
    ColumnBuilder,
    METRIC_PREFIX_MAP,
)

logger = logging.getLogger(__name__)

# 规模分类标签
SIZE_LABELS = {
    'micro': '🔹微型',
    'small': '🔸小型',
    'mid': '🔶中型',
    'large': '🔷大型',
    'mega': '💎超大'
}


class ComprehensiveReportGenerator:
    """
    全方位公司评价报告生成器

    专注于找出3类真正的好公司，所有分析工具（断点检测、贝叶斯概率等）
    都是为最终筛选服务的内部工具，不单独展示。

    v3.0 新增：
    - 行业自适应阈值引擎集成
    - 多元化公司识别
    - 行业相对排名计算

    数据输入：直接接收探针DataFrame
    """

    def __init__(self, probe_data: Dict[str, pd.DataFrame]):
        """
        初始化报告生成器

        Args:
            probe_data: 探针数据字典，格式: {'roic': df, 'roe': df, ...}
        """
        if not probe_data or not any(v is not None for v in probe_data.values()):
            raise ValueError("必须提供探针数据 probe_data")

        self._probe_data = probe_data

        # 使用统一命名规范系统构建指标配置（单一真相源）
        self.metrics_config = {
            key: {
                "prefix": MetricRegistry.get_output_prefix(key),
                "name": MetricRegistry.get_display_name(key),
            }
            for key in MetricRegistry.all_keys()
        }
        self.df_merged = pd.DataFrame()

    def _get_col(self, metric_key: str, field: str) -> str:
        """获取特定指标的列名"""
        prefix = self.metrics_config[metric_key]["prefix"]
        return f"{prefix}_{field}"

    def _classify_size(self, invest_capital: float) -> str:
        """
        根据投入资本计算公司规模分类

        规模分类标准 (按投入资本，单位：亿元):
        - micro: < 10亿 (微型)
        - small: 10-50亿 (小型)
        - mid: 50-200亿 (中型)
        - large: 200-1000亿 (大型)
        - mega: > 1000亿 (超大型)
        """
        if pd.isna(invest_capital):
            return 'micro'

        # 转换为亿元
        capital_yi = invest_capital / 1e8

        if capital_yi < 10:
            return 'micro'
        elif capital_yi < 50:
            return 'small'
        elif capital_yi < 200:
            return 'mid'
        elif capital_yi < 1000:
            return 'large'
        else:
            return 'mega'

    def _load_from_probe_data(self) -> pd.DataFrame:
        """从探针数据字典加载并合并数据"""
        logger.info("直接使用探针分析结果（无需读取文件）")

        merged = None
        # 扩展 shared_cols 包含所有公共元数据列
        shared_cols = {
            'ts_code', 'name', 'industry', 'size_class', 'invest_capital',
            'ann_date', 'end_date', 'metric_name',  # 元数据列
        }

        for key, df in self._probe_data.items():
            if df is None or df.empty:
                continue

            # 确保 ts_code 是列而不是索引
            if 'ts_code' not in df.columns and df.index.name == 'ts_code':
                df = df.reset_index()

            # 排除公共列，只保留该指标特有的数据列
            cols_data = [c for c in df.columns if c not in shared_cols and c != 'ts_code']
            logger.info(f"  ✓ {self.metrics_config.get(key, {}).get('name', key)}: {len(df)} 公司, {len(cols_data)} 列")

            if merged is None:
                # 第一个 DataFrame: 包含公共列 + 数据列
                cols_first = [c for c in df.columns if c in shared_cols and c in df.columns] + cols_data
                # 确保 ts_code 在最前面
                if 'ts_code' in df.columns and 'ts_code' not in cols_first:
                    cols_first = ['ts_code'] + cols_first
                merged = df[cols_first].copy()
            else:
                # 后续 DataFrame: 只合并 ts_code + 数据列
                merge_cols = ['ts_code'] + cols_data
                # 确保只选择存在的列
                merge_cols = [c for c in merge_cols if c in df.columns]
                merged = pd.merge(merged, df[merge_cols], on='ts_code', how='outer')

        return merged

    def load_and_merge_data(self) -> pd.DataFrame:
        """加载并合并所有指标数据"""
        merged = self._load_from_probe_data()

        if merged is not None and not merged.empty:
            # 添加规模标签
            if 'invest_capital' in merged.columns:
                merged['size_class'] = merged['invest_capital'].apply(self._classify_size)

            if 'size_class' in merged.columns:
                merged['size_label'] = merged['size_class'].map(SIZE_LABELS)
                if 'invest_capital' in merged.columns:
                    merged['invest_capital_yi'] = merged['invest_capital'] / 1e8
                logger.info(f"规模分布: {merged['size_class'].value_counts().to_dict()}")

            self.df_merged = merged

        return merged

    # =========================================================================
    # 第1层：基础质量计算（专业增强版 v2.0）
    # =========================================================================

    def _calc_growth_score(self, df: pd.DataFrame) -> pd.Series:
        """
        计算成长因子得分 (0-100)

        增强点：
        1. 整合趋势分析引擎的trend_score
        2. 使用稳健斜率(robust_slope)
        3. 考虑趋势加速度
        """
        scores = pd.Series(0.0, index=df.index)

        # 营收CAGR排名 (30分)
        col_rev = self._get_col('revenue', 'cagr')
        if col_rev in df.columns:
            scores += df[col_rev].rank(pct=True, na_option='bottom') * 30

        # 利润CAGR排名 (30分)
        col_prof = self._get_col('profit', 'cagr')
        if col_prof in df.columns:
            scores += df[col_prof].rank(pct=True, na_option='bottom') * 30

        # 近3年斜率（加速度）(15分)
        col_recent = self._get_col('profit', 'recent_3y_slope')
        if col_recent in df.columns:
            scores += df[col_recent].rank(pct=True, na_option='bottom') * 15

        # 【新增】趋势分析引擎评分 (15分)
        col_trend_score = self._get_col('revenue', 'trend_score')
        if col_trend_score in df.columns:
            # trend_score 范围是 0-125，归一化到 0-15
            normalized = df[col_trend_score].clip(0, 100) / 100 * 15
            scores += normalized.fillna(0)

        # 【新增】加速增长奖励 (10分)
        col_accel = self._get_col('revenue', 'is_accelerating')
        if col_accel in df.columns:
            scores += np.where(df[col_accel] == True, 10, 0)

        return scores.clip(0, 100)

    def _calc_quality_score(self, df: pd.DataFrame) -> pd.Series:
        """
        计算质量因子得分 (0-100)

        增强点：
        1. 整合趋势分析引擎的trend_score
        2. 使用Mann-Kendall稳健趋势检验
        3. 考虑周期性调整
        """
        scores = pd.Series(0.0, index=df.index)

        # ROE行业排名 (25分)
        col_roe = self._get_col('roe', 'latest')
        if col_roe in df.columns and 'industry' in df.columns:
            scores += df.groupby('industry')[col_roe].rank(pct=True, na_option='bottom') * 25

        # ROIC行业排名 (25分)
        col_roic = self._get_col('roic', 'latest')
        if col_roic in df.columns and 'industry' in df.columns:
            scores += df.groupby('industry')[col_roic].rank(pct=True, na_option='bottom') * 25

        # 毛利率行业排名 (15分)
        col_gm = self._get_col('gross_margin', 'latest')
        if col_gm in df.columns and 'industry' in df.columns:
            scores += df.groupby('industry')[col_gm].rank(pct=True, na_option='bottom') * 15

        # R²稳定性 (10分)
        col_r2 = self._get_col('roic', 'r_squared')
        if col_r2 in df.columns:
            scores += df[col_r2].rank(pct=True, na_option='bottom') * 10

        # 【新增】ROIC趋势评分 (15分)
        col_roic_score = self._get_col('roic', 'trend_score')
        if col_roic_score in df.columns:
            normalized = df[col_roic_score].clip(0, 100) / 100 * 15
            scores += normalized.fillna(0)

        # 【新增】Mann-Kendall检验奖励 (10分)
        # 显著上升趋势 (tau>0.4, p<0.1) 奖励
        col_mk_tau = self._get_col('roic', 'mk_tau')
        col_mk_p = self._get_col('roic', 'mk_p_value')
        if col_mk_tau in df.columns and col_mk_p in df.columns:
            mk_bonus = np.where(
                (df[col_mk_tau] > 0.4) & (df[col_mk_p] < 0.1),
                10, 0
            )
            scores += mk_bonus

        # 【修复v2.1】去趋势CV奖励 (5分) - 低detrended_cv说明稳定性高
        col_dtcv = self._get_col('roic', 'detrended_cv')
        if col_dtcv in df.columns:
            # detrended_cv < 0.3 为稳定，给予奖励
            dtcv_bonus = np.where(df[col_dtcv] < 0.2, 5,
                         np.where(df[col_dtcv] < 0.3, 3, 0))
            scores = scores + pd.Series(dtcv_bonus, index=df.index).fillna(0)

        # 【修复v2.1】周期置信度调整 - 高周期性公司的质量分需要折扣
        col_cyclical_conf = self._get_col('roic', 'cyclical_confidence')
        col_is_cyclical = self._get_col('roic', 'is_cyclical')
        if col_cyclical_conf in df.columns and col_is_cyclical in df.columns:
            # 高周期置信度的周期公司，质量分打折（周期公司的稳定性不能简单评判）
            cyclical_discount = np.where(
                (df[col_is_cyclical] == True) & (df[col_cyclical_conf] > 0.7),
                -5, 0  # 周期公司扣5分提醒
            )
            scores = scores + pd.Series(cyclical_discount, index=df.index).fillna(0)

        return scores.clip(0, 100)

    def _calc_safety_score(self, df: pd.DataFrame) -> pd.Series:
        """
        计算安全因子得分 (0-100)

        增强点：
        1. 多维度恶化检测
        2. 连续下跌惩罚
        3. 波动率体制检测
        """
        scores = pd.Series(50.0, index=df.index)  # 基础分

        # 现金流趋势 (+/-25分)
        col_ocf = self._get_col('ocf', 'log_slope')
        if col_ocf in df.columns:
            # 渐进评分，不是非此即彼
            ocf_scores = np.where(
                df[col_ocf] > 0.05, 25,  # 强正向
                np.where(df[col_ocf] > 0, 15,  # 弱正向
                np.where(df[col_ocf] > -0.05, 0,  # 轻微负向
                np.where(df[col_ocf] > -0.1, -15, -25)))  # 严重负向
            )
            scores = scores + pd.Series(ocf_scores, index=df.index).fillna(0)

        # 恶化概率惩罚 (最多-30分)
        col_det = self._get_col('roic', 'deterioration_probability')
        if col_det in df.columns:
            # 恶化概率越高，扣分越多
            det_penalty = np.where(
                df[col_det] > 0.8, -30,
                np.where(df[col_det] > 0.6, -20,
                np.where(df[col_det] > 0.4, -10, 0))
            )
            scores = scores + pd.Series(det_penalty, index=df.index).fillna(0)

        # 【新增】连续下跌惩罚 (最多-20分)
        col_consec = self._get_col('roic', 'consecutive_decline_years')
        if col_consec in df.columns:
            consec_penalty = np.where(
                df[col_consec] >= 4, -20,
                np.where(df[col_consec] >= 3, -15,
                np.where(df[col_consec] >= 2, -5, 0))
            )
            scores = scores + pd.Series(consec_penalty, index=df.index).fillna(0)

        # 【新增】波动率体制检测 (最多-10分)
        col_vol_regime = self._get_col('roic', 'volatility_regime')
        if col_vol_regime in df.columns:
            vol_penalty = np.where(df[col_vol_regime] == 'increasing_vol', -10, 0)
            scores = scores + pd.Series(vol_penalty, index=df.index).fillna(0)

        # 【新增】恶化加速惩罚 (最多-15分)
        col_det_accel = self._get_col('roic', 'deterioration_acceleration')
        if col_det_accel in df.columns:
            accel_penalty = np.where(
                df[col_det_accel] > 0.5, -15,
                np.where(df[col_det_accel] > 0.3, -10,
                np.where(df[col_det_accel] > 0.1, -5, 0))
            )
            scores = scores + pd.Series(accel_penalty, index=df.index).fillna(0)

        # 【修复v2.1】波动类型惩罚 (最多-10分)
        col_vol_type = self._get_col('roic', 'volatility_type')
        if col_vol_type in df.columns:
            # extreme_high 波动极高的公司需要惩罚
            vol_type_penalty = np.where(
                df[col_vol_type] == 'extreme_high', -10,
                np.where(df[col_vol_type] == 'high', -5, 0)
            )
            scores = scores + pd.Series(vol_type_penalty, index=df.index).fillna(0)

        # 【修复v2.1】标准差风险检测 (相对于均值)
        col_std = self._get_col('roic', 'std_dev')
        col_latest = self._get_col('roic', 'latest')
        if col_std in df.columns and col_latest in df.columns:
            # 如果标准差 > 最新值的50%，说明波动太大
            std_ratio = df[col_std] / df[col_latest].abs().replace(0, np.nan)
            std_penalty = np.where(std_ratio > 0.5, -5, 0)
            scores = scores + pd.Series(std_penalty, index=df.index).fillna(0)

        return scores.clip(0, 100)

    # =========================================================================
    # 第2层：交叉验证（专业增强版 v2.0）
    # =========================================================================

    def _safe_get(self, row: pd.Series, col: str, default: float = np.nan) -> float:
        """安全获取值，处理NaN和缺失情况"""
        val = row.get(col, default)
        if pd.isna(val):
            return default
        return float(val)

    def _check_cross_validation(self, row: pd.Series) -> Tuple[bool, List[str]]:
        """
        交叉验证：检查财务指标一致性（专业增强版）

        增强点：
        1. 正确处理NaN - 只有当两个指标都有效时才检测
        2. 新增连续下跌检测
        3. 新增恶化加速度检测
        4. 使用稳健斜率进行验证

        Returns:
            (is_valid, risk_list) - 是否通过验证，风险列表
        """
        risks = []

        # 1. 纸面富贵检测：利润高增 vs 现金流恶化
        # 【修复】只有两个值都有效时才检测
        prof_slope = self._safe_get(row, self._get_col('profit', 'log_slope'), np.nan)
        ocf_slope = self._safe_get(row, self._get_col('ocf', 'log_slope'), np.nan)
        if not np.isnan(prof_slope) and not np.isnan(ocf_slope):
            if prof_slope > 0.15 and ocf_slope < -0.05:
                risks.append("纸面富贵")

        # 2. 低效扩张检测：营收高增 vs ROE低迷
        rev_slope = self._safe_get(row, self._get_col('revenue', 'log_slope'), np.nan)
        roe_val = self._safe_get(row, self._get_col('roe', 'latest'), np.nan)
        if not np.isnan(rev_slope) and not np.isnan(roe_val):
            if rev_slope > 0.15 and roe_val < 5:
                risks.append("低效扩张")

        # 3. 杠杆陷阱检测：ROE高 vs ROIC低
        roic_val = self._safe_get(row, self._get_col('roic', 'latest'), np.nan)
        if not np.isnan(roe_val) and not np.isnan(roic_val):
            if roe_val > 15 and roic_val < 8:
                risks.append("杠杆驱动")
        # 【新增】ROE高但ROIC缺失，也标记为可疑
        elif not np.isnan(roe_val) and roe_val > 20 and np.isnan(roic_val):
            risks.append("杠杆可疑(ROIC缺失)")

        # 4. 费用失控检测：毛利率稳定 vs 净利率暴跌
        gm_slope = self._safe_get(row, self._get_col('gross_margin', 'log_slope'), np.nan)
        nm_slope = self._safe_get(row, self._get_col('net_margin', 'log_slope'), np.nan)
        if not np.isnan(gm_slope) and not np.isnan(nm_slope):
            if gm_slope > -0.02 and nm_slope < -0.05:
                risks.append("费用失控")

        # 5. 【v4.0新增】ROIC vs ROIIC 一致性检测（增量资本效率）
        roiic_val = self._safe_get(row, self._get_col('roiic', 'latest'), np.nan)
        if not np.isnan(roic_val) and not np.isnan(roiic_val):
            # 如果ROIC好但ROIIC差，说明新投资效率在下降
            if roic_val > 12 and roiic_val < 5:
                risks.append("增量效率下降")
            # 如果ROIIC显著低于ROIC的50%，也是警告信号
            elif roic_val > 10 and roiic_val < roic_val * 0.5:
                risks.append("新投资回报不足")

        # 6. 【v4.0新增】营收 vs 利润一致性（增收是否增利）
        rev_cagr = self._safe_get(row, self._get_col('revenue', 'cagr'), np.nan)
        prof_cagr = self._safe_get(row, self._get_col('profit', 'cagr'), np.nan)
        if not np.isnan(rev_cagr) and not np.isnan(prof_cagr):
            # 营收增长但利润下降，说明增收不增利
            if rev_cagr > 0.05 and prof_cagr < -0.05:
                risks.append("增收不增利")

        # 7. 连续下跌检测
        consecutive_decline = self._safe_get(row, self._get_col('roic', 'consecutive_decline_years'), 0)
        if consecutive_decline >= 3:
            risks.append(f"连续{int(consecutive_decline)}年下跌")

        # 8. 恶化加速检测
        det_acceleration = self._safe_get(row, self._get_col('roic', 'deterioration_acceleration'), 0)
        if det_acceleration > 0.3:
            risks.append("恶化加速")

        # 9. 【v4.0新增】最新值 vs 历史趋势一致性
        roic_weighted = self._safe_get(row, self._get_col('roic', 'weighted'), np.nan)
        if not np.isnan(roic_val) and not np.isnan(roic_weighted) and roic_weighted != 0:
            # 最新值显著偏离历史趋势（偏差超过50%）
            if abs(roic_val - roic_weighted) > abs(roic_weighted) * 0.5:
                risks.append("数据异常(偏离历史趋势)")

        # 10. 趋势与稳健趋势背离
        log_slope = self._safe_get(row, self._get_col('roic', 'log_slope'), np.nan)
        robust_slope = self._safe_get(row, self._get_col('roic', 'robust_slope'), np.nan)
        if not np.isnan(log_slope) and not np.isnan(robust_slope):
            # 如果OLS斜率显示上升，但稳健斜率显示下降，可能有异常值干扰
            if log_slope > 0.05 and robust_slope < -0.02:
                risks.append("趋势异常(稳健检验不通过)")

        # 11. 异方差检测 - 使用fused_slope和heteroscedasticity_detected验证
        fused_slope = self._safe_get(row, self._get_col('roic', 'fused_slope'), np.nan)
        hetero_detected = row.get(self._get_col('roic', 'heteroscedasticity_detected'), False)
        if not np.isnan(log_slope) and not np.isnan(fused_slope):
            # 如果融合斜率与OLS斜率差距大，说明存在异方差问题
            if abs(log_slope - fused_slope) > 0.08:
                risks.append("异方差警告(斜率不稳定)")
            # 如果明确检测到异方差且斜率显示上升，需要谨慎
            elif hetero_detected and log_slope > 0.05:
                risks.append("异方差检测(趋势需验证)")

        # 9. 【修复v2.1】周期底部错杀检测
        is_cyclical = row.get(self._get_col('roic', 'is_cyclical'), False)
        current_phase = row.get(self._get_col('roic', 'current_phase'), '')
        fft_period = self._safe_get(row, self._get_col('roic', 'fft_dominant_period'), 0)
        if is_cyclical and current_phase in ('trough', 'bottom') and fft_period > 3:
            # 周期底部的公司，某些风险可以豁免
            if "连续" in str(risks) or "恶化加速" in str(risks):
                risks.append(f"⚠️周期底部(周期{fft_period:.1f}年)-可能错杀")

        is_valid = len(risks) == 0
        return is_valid, risks

    # =========================================================================
    # v4.0: 公司模式识别（基于探针数据的模式分类）
    # =========================================================================

    def _identify_company_pattern(self, row: pd.Series) -> str:
        """
        识别公司的数据模式

        基于探针分析结果识别公司模式:
        - consistently_excellent: 一直优秀
        - high_growth: 高速成长
        - steady_growth: 稳健增长
        - cyclical: 周期波动
        - deteriorating: 持续恶化
        - volatile_unstable: 波动不稳
        - average: 普通
        """
        # 提取关键数据特征
        roic_latest = self._safe_get(row, self._get_col('roic', 'latest'), np.nan)
        roic_weighted = self._safe_get(row, self._get_col('roic', 'weighted'), roic_latest)
        roic_log_slope = self._safe_get(row, self._get_col('roic', 'log_slope'), 0)
        roic_r_squared = self._safe_get(row, self._get_col('roic', 'r_squared'), 0)
        roic_cv = self._safe_get(row, self._get_col('roic', 'cv'), 1.0)

        # 恶化相关
        deterioration_prob = self._safe_get(row, self._get_col('roic', 'deterioration_probability'), 0)
        consecutive_decline = int(self._safe_get(row, self._get_col('roic', 'consecutive_decline_years'), 0))

        # 周期相关
        is_cyclical = row.get(self._get_col('roic', 'is_cyclical'), False)
        cyclical_confidence = self._safe_get(row, self._get_col('roic', 'cyclical_confidence'), 0)

        # 成长相关
        revenue_cagr = self._safe_get(row, self._get_col('revenue', 'cagr'), 0)
        profit_cagr = self._safe_get(row, self._get_col('profit', 'cagr'), 0)

        roic_mean = roic_weighted if not np.isnan(roic_weighted) else roic_latest
        roic_min = roic_latest - roic_cv * abs(roic_latest) if roic_cv < 1 else roic_latest * 0.5

        # === 模式识别逻辑 ===

        # 模式1：一直优秀
        if (roic_mean >= 15 and roic_min > 8 and roic_cv < 0.3 and
            roic_log_slope >= -0.02 and consecutive_decline == 0):
            return "consistently_excellent"

        # 模式2：高速成长
        if (roic_log_slope > 0.08 and roic_r_squared > 0.5 and
            (revenue_cagr > 0.15 or profit_cagr > 0.20)):
            return "high_growth"

        # 模式3：稳健增长
        if (roic_log_slope >= 0 and roic_r_squared > 0.6 and roic_cv < 0.25 and roic_mean > 6):
            return "steady_growth"

        # 模式4：周期波动
        if (is_cyclical and cyclical_confidence > 0.6) or (roic_cv > 0.4 and roic_r_squared < 0.5):
            return "cyclical"

        # 模式5：持续恶化
        if (consecutive_decline >= 3 or deterioration_prob > 0.7 or
            (roic_log_slope < -0.1 and roic_r_squared > 0.5)):
            return "deteriorating"

        # 模式6：波动不稳定
        if roic_cv > 0.5 and roic_r_squared < 0.4:
            return "volatile_unstable"

        # 默认：普通
        return "average"

    def _get_pattern_label(self, pattern: str) -> str:
        """获取模式的中文标签"""
        labels = {
            "consistently_excellent": "🌟一直优秀",
            "high_growth": "🚀高速成长",
            "steady_growth": "📈稳健增长",
            "cyclical": "🔄周期波动",
            "deteriorating": "📉持续恶化",
            "volatile_unstable": "⚡波动不稳",
            "average": "➖普通",
            "unknown": "❓未知",
        }
        return labels.get(pattern, "❓未知")

    # =========================================================================
    # 第3层：拐点检测
    # =========================================================================

    def _check_turnaround_signals(self, row: pd.Series) -> Tuple[bool, str]:
        """
        检测困境反转信号

        Returns:
            (is_turnaround, reason)
        """
        # 需要同时满足：
        # 1. 有反转标识
        # 2. 断点确认（如果有断点检测数据）
        # 3. 近期斜率转正

        is_turnaround_prof = row.get(self._get_col('profit', 'is_turnaround'), 0) == 1
        is_turnaround_rev = row.get(self._get_col('revenue', 'is_turnaround'), 0) == 1

        recent_slope = row.get(self._get_col('profit', 'recent_3y_slope'), 0)
        has_break = row.get(self._get_col('roic', 'has_break'), 0) == 1

        # 断点后斜率为正，且近期改善
        if (is_turnaround_prof or is_turnaround_rev) and recent_slope > 0:
            if has_break:
                return True, "断点反转确认"
            else:
                return True, "趋势反转"

        return False, ""

    def _check_deterioration_risk(self, row: pd.Series) -> Tuple[bool, float, List[str]]:
        """
        多维度恶化风险检测（专业增强版）

        增强点：
        1. 检查多个指标的恶化概率，不仅仅是ROIC
        2. 使用连续下跌年数
        3. 使用恶化加速度
        4. 使用Mann-Kendall趋势检验

        Returns:
            (has_risk, max_probability, evidence_list)
        """
        evidence = []
        probabilities = []

        # 1. ROIC恶化概率
        roic_det = self._safe_get(row, self._get_col('roic', 'deterioration_probability'), np.nan)
        if not np.isnan(roic_det):
            probabilities.append(roic_det)
            if roic_det > 0.6:
                evidence.append(f"ROIC恶化概率{roic_det:.0%}")

        # 2. 利润恶化概率
        prof_det = self._safe_get(row, self._get_col('profit', 'deterioration_probability'), np.nan)
        if not np.isnan(prof_det):
            probabilities.append(prof_det)
            if prof_det > 0.6:
                evidence.append(f"利润恶化概率{prof_det:.0%}")

        # 3. 营收恶化概率
        rev_det = self._safe_get(row, self._get_col('revenue', 'deterioration_probability'), np.nan)
        if not np.isnan(rev_det):
            probabilities.append(rev_det)
            if rev_det > 0.7:  # 营收阈值稍高，因为营收波动更常见
                evidence.append(f"营收恶化概率{rev_det:.0%}")

        # 4. 连续下跌年数
        consecutive = self._safe_get(row, self._get_col('roic', 'consecutive_decline_years'), 0)
        if consecutive >= 3:
            evidence.append(f"连续{int(consecutive)}年下跌")
            # 连续下跌也增加隐含恶化概率
            probabilities.append(min(0.5 + consecutive * 0.1, 0.9))

        # 5. Mann-Kendall趋势检验
        mk_tau = self._safe_get(row, self._get_col('roic', 'mk_tau'), np.nan)
        mk_p = self._safe_get(row, self._get_col('roic', 'mk_p_value'), np.nan)
        if not np.isnan(mk_tau) and not np.isnan(mk_p):
            # 显著下降趋势 (tau<-0.4, p<0.1)
            if mk_tau < -0.4 and mk_p < 0.1:
                evidence.append(f"MK检验确认下降(τ={mk_tau:.2f})")
                probabilities.append(0.7)

        # 6. 恶化加速度
        det_accel = self._safe_get(row, self._get_col('roic', 'deterioration_acceleration'), 0)
        if det_accel > 0.3:
            evidence.append(f"恶化加速({det_accel:.2f})")

        # 7. 【修复v2.1】恶化模式
        det_pattern = row.get(self._get_col('roic', 'deterioration_pattern'), 'none')
        if det_pattern in ('accelerating_decline', 'persistent_decline'):
            evidence.append(f"恶化模式:{det_pattern}")
            probabilities.append(0.75)  # 恶化模式增加隐含概率

        # 8. 【修复v2.1】周期性公司特殊处理
        is_cyclical = row.get(self._get_col('roic', 'is_cyclical'), False)
        cyclical_conf = self._safe_get(row, self._get_col('roic', 'cyclical_confidence'), 0)
        if is_cyclical and cyclical_conf > 0.7:
            # 高置信度周期公司，恶化可能是周期性的，降低风险判定
            current_phase = row.get(self._get_col('roic', 'current_phase'), '')
            if current_phase in ('trough', 'bottom', 'recovery'):
                # 周期底部，降低恶化概率判定
                probabilities = [p * 0.7 for p in probabilities]  # 打7折
                evidence.append(f"周期底部调整(置信度{cyclical_conf:.0%})")

        # 综合判断
        max_prob = max(probabilities) if probabilities else 0.0
        has_risk = max_prob > 0.6 or len(evidence) >= 2

        return has_risk, max_prob, evidence

    # =========================================================================
    # 第4层：综合筛选（专业增强版 v2.0）
    # =========================================================================

    def _select_garp_companies(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        筛选高成长优质公司 (GARP)

        必须满足：
        - 营收CAGR > 10%
        - 利润CAGR > 15%
        - 现金流验证通过
        - 无恶化信号
        - 规模 >= 中型
        """
        candidates = df.copy()

        # 获取列名
        col_rev_cagr = self._get_col('revenue', 'cagr')
        col_prof_cagr = self._get_col('profit', 'cagr')
        col_ocf = self._get_col('ocf', 'log_slope')
        col_det = self._get_col('roic', 'deterioration_probability')

        # 统一阈值筛选
        if col_rev_cagr in candidates.columns:
            candidates = candidates[candidates[col_rev_cagr] > 0.10]
        if col_prof_cagr in candidates.columns:
            candidates = candidates[candidates[col_prof_cagr] > 0.15]

        # 现金流健康
        if col_ocf in candidates.columns:
            candidates = candidates[candidates[col_ocf] > -0.05]

        # 无恶化信号
        if col_det in candidates.columns:
            candidates = candidates[(candidates[col_det] < 0.5) | (candidates[col_det].isna())]

        # 规模过滤
        if 'size_class' in candidates.columns:
            candidates = candidates[candidates['size_class'].isin(['mid', 'large', 'mega'])]

        # 交叉验证
        valid_mask = candidates.apply(lambda row: self._check_cross_validation(row)[0], axis=1)
        candidates = candidates[valid_mask]

        return candidates

    def _select_quality_moat_companies(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        筛选白马护城河公司 (Quality Moat)

        必须满足：
        - ROIC > 12%
        - ROE > 15%
        - 毛利率 > 25%
        - R² > 0.5 (趋势稳定)
        - 无杠杆陷阱
        - 规模 >= 中型
        """
        candidates = df.copy()

        # 获取列名
        col_roic = self._get_col('roic', 'latest')
        col_roe = self._get_col('roe', 'latest')
        col_gm = self._get_col('gross_margin', 'latest')
        col_r2 = self._get_col('roic', 'r_squared')

        # 统一阈值筛选
        if col_roic in candidates.columns:
            candidates = candidates[candidates[col_roic] > 12]
        if col_roe in candidates.columns:
            candidates = candidates[candidates[col_roe] > 15]
        if col_gm in candidates.columns:
            candidates = candidates[candidates[col_gm] > 25]

        # 趋势稳定
        if col_r2 in candidates.columns:
            candidates = candidates[(candidates[col_r2] > 0.5) | (candidates[col_r2].isna())]

        # 规模过滤
        if 'size_class' in candidates.columns:
            candidates = candidates[candidates['size_class'].isin(['mid', 'large', 'mega'])]

        # 排除杠杆陷阱
        if col_roe in candidates.columns and col_roic in candidates.columns:
            # ROE/ROIC比值不能太高（杠杆驱动）
            ratio = candidates[col_roe] / candidates[col_roic].replace(0, np.nan)
            candidates = candidates[ratio < 2.0]

        return candidates

    def _select_turnaround_companies(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        筛选困境反转公司 (Turnaround)

        必须满足：
        - 有反转信号（is_turnaround=1）
        - 断点确认（has_break=1）或近期斜率明显转正
        - 毛利率未崩溃
        - 规模 >= 中型（小公司反转风险太高）
        """
        candidates = df.copy()

        # 有反转信号
        col_turn_prof = self._get_col('profit', 'is_turnaround')
        col_turn_rev = self._get_col('revenue', 'is_turnaround')

        if col_turn_prof in candidates.columns and col_turn_rev in candidates.columns:
            turn_mask = (candidates[col_turn_prof] == 1) | (candidates[col_turn_rev] == 1)
            candidates = candidates[turn_mask]
        else:
            # 没有反转列，使用斜率判断
            col_slope = self._get_col('profit', 'log_slope')
            col_recent = self._get_col('profit', 'recent_3y_slope')
            if col_slope in candidates.columns and col_recent in candidates.columns:
                # 长期下跌但近期反转
                candidates = candidates[(candidates[col_slope] < 0) & (candidates[col_recent] > 0.05)]

        # 毛利率保护
        col_gm_slope = self._get_col('gross_margin', 'log_slope')
        if col_gm_slope in candidates.columns:
            candidates = candidates[candidates[col_gm_slope] > -0.03]

        # 规模过滤（反转股必须是中型以上）
        if 'size_class' in candidates.columns:
            candidates = candidates[candidates['size_class'].isin(['mid', 'large', 'mega'])]

        return candidates

    # =========================================================================
    # 报告生成
    # =========================================================================

    def generate_report(self, output_path: str = "data/comprehensive_analysis_report.md") -> str:
        """
        生成综合分析报告（规则驱动，基于预设阈值）

        注：T.R.U.T.H.系统报告已独立到 truth_report_generator.py
        """
        if self.df_merged is None or self.df_merged.empty:
            self.load_and_merge_data()

        if self.df_merged is None or self.df_merged.empty:
            return "❌ 没有加载到任何数据，无法生成报告。"

        df = self.df_merged

        # 计算因子得分
        df['score_growth'] = self._calc_growth_score(df)
        df['score_quality'] = self._calc_quality_score(df)
        df['score_safety'] = self._calc_safety_score(df)

        # 识别公司模式
        df['company_pattern'] = df.apply(
            lambda row: self._identify_company_pattern(row), axis=1
        )
        df['pattern_label'] = df['company_pattern'].apply(self._get_pattern_label)
        logger.info(f"✅ 公司模式识别完成，分布: {df['company_pattern'].value_counts().to_dict()}")

        lines = []

        # === 标题 ===
        lines.append("# 📊 AStock 全方位基本面分析报告")
        lines.append(f"> 🕐 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"> 📈 覆盖公司: {len(df)} 家")
        if 'size_class' in df.columns:
            size_counts = df['size_class'].value_counts()
            lines.append(f"> 📊 规模分布: 超大型 {size_counts.get('mega', 0)} | 大型 {size_counts.get('large', 0)} | 中型 {size_counts.get('mid', 0)} | 小型 {size_counts.get('small', 0)} | 微型 {size_counts.get('micro', 0)}")
        lines.append("> 📋 报告类型: **规则驱动** (基于预设阈值)")
        lines.append("")

        lines.append("---")
        lines.append("")
        lines.append("## 🎯 核心目标")
        lines.append("")
        lines.append("本报告通过**4层过滤体系**，从全市场中筛选出3类真正无懈可击的好公司：")
        lines.append("")
        lines.append("| 类型 | 特征 | 适合投资者 |")
        lines.append("|---|---|---|")
        lines.append("| 🚀 **高成长 (GARP)** | 营收利润双高增 + 现金流健康 | 追求成长的进攻型投资者 |")
        lines.append("| 🏰 **白马护城河** | 高ROE/ROIC稳定 + 高毛利 | 追求稳健的长期投资者 |")
        lines.append("| 🔄 **困境反转** | 触底确认 + 质量改善信号 | 有耐心的逆向投资者 |")
        lines.append("")

        # === 1. 高成长优质公司 ===
        lines.extend(self._section_garp(df))

        # === 2. 白马护城河 ===
        lines.extend(self._section_quality_moat(df))

        # === 3. 困境反转 ===
        lines.extend(self._section_turnaround(df))

        # === 4. 风险警示 ===
        lines.extend(self._section_risk_warnings(df))

        # === 5. 方法论说明 ===
        lines.extend(self._section_methodology())

        # === 保存 ===
        report_content = "\n".join(lines)
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_text(report_content, encoding='utf-8')
        logger.info(f"✅ 报告已生成: {output_path}")
        return report_content

    def _render_garp_table(self, lines: List[str], section_name: str, picks: pd.DataFrame) -> None:
        """渲染 GARP 表格的辅助方法"""
        lines.append(f"### {section_name} GARP精选")
        lines.append("")
        lines.append("| 代码 | 名称 | 行业 | 模式 | 营收CAGR | 利润CAGR | ROIC | 综合评分 | 核心亮点 |")
        lines.append("|---|---|---|---|---|---|---|---|---|")

        for _, row in picks.iterrows():
            rev_cagr = row.get(self._get_col('revenue', 'cagr'), 0)
            prof_cagr = row.get(self._get_col('profit', 'cagr'), 0)
            roic = row.get(self._get_col('roic', 'latest'), 0)
            pattern_label = row.get('pattern_label', '-')

            # 生成亮点
            highlights = []
            if rev_cagr > 0.20: highlights.append("营收爆发")
            if prof_cagr > 0.25: highlights.append("利润高增")
            if roic > 15: highlights.append("资本高效")
            if not highlights: highlights.append("综合优质")

            lines.append(f"| {row['ts_code']} | {row.get('name', '-')} | {row.get('industry', '-')} | {pattern_label} | {rev_cagr:.1%} | {prof_cagr:.1%} | {roic:.1f}% | **{row['composite']:.0f}** | {', '.join(highlights)} |")

        lines.append("")

    def _section_garp(self, df: pd.DataFrame) -> List[str]:
        """高成长优质公司板块"""
        lines = ["## 🚀 高成长优质公司 (GARP精选)", ""]
        lines.append("**筛选标准**：营收CAGR>10% + 利润CAGR>15% + 现金流健康 + 无恶化信号 + 通过交叉验证")
        lines.append("")

        candidates = self._select_garp_companies(df)

        if candidates.empty:
            lines.append("*(当前市场环境下暂无符合严苛标准的高成长公司)*")
            lines.append("")
            return lines

        # 计算综合得分并排序
        candidates['composite'] = (
            0.5 * candidates['score_growth'] +
            0.3 * candidates['score_quality'] +
            0.2 * candidates['score_safety']
        )

        # 按规模分类展示
        if 'size_class' in candidates.columns:
            for size_key, size_name in [('mega', '💎 超大型'), ('large', '🔷 大型'), ('mid', '🔶 中型')]:
                size_df = candidates[candidates['size_class'] == size_key].copy()
                if size_df.empty:
                    continue

                top_picks = size_df.sort_values('composite', ascending=False).head(10)
                self._render_garp_table(lines, size_name, top_picks)
        else:
            # 无规模分类时，直接展示 top 30
            top_picks = candidates.sort_values('composite', ascending=False).head(30)
            self._render_garp_table(lines, "🏆 综合精选", top_picks)

        return lines

    def _section_quality_moat(self, df: pd.DataFrame) -> List[str]:
        """白马护城河板块"""
        lines = ["## 🏰 白马护城河 (Quality精选)", ""]
        lines.append("**筛选标准**：ROIC>12% + ROE>15% + 毛利率>25% + 趋势稳定 + 无杠杆陷阱")
        lines.append("")

        candidates = self._select_quality_moat_companies(df)

        if candidates.empty:
            lines.append("*(当前市场环境下暂无符合严苛标准的白马公司)*")
            lines.append("")
            return lines

        # 计算护城河得分
        candidates['moat_score'] = (
            0.6 * candidates['score_quality'] +
            0.4 * candidates['score_safety']
        )

        # 按规模分类展示
        if 'size_class' in candidates.columns:
            for size_key, size_name in [('mega', '💎 超大型白马'), ('large', '🔷 大型白马'), ('mid', '🔶 中型白马')]:
                size_df = candidates[candidates['size_class'] == size_key].copy()
                if size_df.empty:
                    continue

                top_picks = size_df.sort_values('moat_score', ascending=False).head(10)
                self._render_moat_table(lines, size_name, top_picks)
        else:
            # 无规模分类时，直接展示 top 30
            top_picks = candidates.sort_values('moat_score', ascending=False).head(30)
            self._render_moat_table(lines, "🏆 综合白马精选", top_picks)

        return lines

    def _render_moat_table(self, lines: List[str], section_name: str, picks: pd.DataFrame) -> None:
        """渲染护城河表格的辅助方法"""
        lines.append(f"### {section_name}")
        lines.append("")
        lines.append("| 代码 | 名称 | 行业 | 模式 | ROE | ROIC | 毛利率 | 护城河分 | 护城河特征 |")
        lines.append("|---|---|---|---|---|---|---|---|---|")

        for _, row in picks.iterrows():
            roe = row.get(self._get_col('roe', 'latest'), 0)
            roic = row.get(self._get_col('roic', 'latest'), 0)
            gm = row.get(self._get_col('gross_margin', 'latest'), 0)
            pattern_label = row.get('pattern_label', '-')

            # 护城河特征分析
            moat_chars = []
            if gm > 40: moat_chars.append("定价权强")
            if roic > 20: moat_chars.append("资本壁垒")
            if roe > 20 and roic > 15: moat_chars.append("盈利稳健")
            if not moat_chars: moat_chars.append("综合优质")

            lines.append(f"| {row['ts_code']} | {row.get('name', '-')} | {row.get('industry', '-')} | {pattern_label} | {roe:.1f}% | {roic:.1f}% | {gm:.1f}% | **{row['moat_score']:.0f}** | {', '.join(moat_chars)} |")

        lines.append("")

    def _section_turnaround(self, df: pd.DataFrame) -> List[str]:
        """困境反转板块"""
        lines = ["## 🔄 困境反转机会 (Turnaround精选)", ""]
        lines.append("**筛选标准**：反转信号确认 + 毛利率未崩溃 + 近期改善明显 + 规模≥中型")
        lines.append("")
        lines.append("> ⚠️ **投资提示**：困境反转是高风险高回报策略，建议仓位控制在10%以内")
        lines.append("")

        candidates = self._select_turnaround_companies(df)

        if candidates.empty:
            lines.append("*(当前市场暂无符合标准的困境反转机会)*")
            lines.append("")
            return lines

        # 按近期改善幅度排序
        col_recent = self._get_col('profit', 'recent_3y_slope')
        if col_recent in candidates.columns:
            candidates = candidates.sort_values(col_recent, ascending=False)

        top_picks = candidates.head(15)

        lines.append("| 代码 | 名称 | 行业 | 模式 | 近3年利润斜率 | 毛利率趋势 | 反转信号 | 投资建议 |")
        lines.append("|---|---|---|---|---|---|---|---|")

        for _, row in top_picks.iterrows():
            recent_slope = row.get(col_recent, 0)
            gm_slope = row.get(self._get_col('gross_margin', 'log_slope'), 0)
            has_break = row.get(self._get_col('roic', 'has_break'), 0)
            pattern_label = row.get('pattern_label', '-')

            # 反转信号强度
            if has_break == 1 and recent_slope > 0.1:
                signal = "⭐ 强信号"
                advice = "可重点关注"
            elif recent_slope > 0.05:
                signal = "📈 中信号"
                advice = "持续观察"
            else:
                signal = "📊 弱信号"
                advice = "谨慎参与"

            gm_trend = "↑" if gm_slope > 0 else ("→" if gm_slope > -0.02 else "↓")

            lines.append(f"| {row['ts_code']} | {row['name']} | {row.get('industry', '-')} | {pattern_label} | {recent_slope:+.2f} | {gm_trend} | {signal} | {advice} |")

        lines.append("")
        return lines

    def _section_risk_warnings(self, df: pd.DataFrame) -> List[str]:
        """
        风险警示板块（专业增强版）

        增强点：
        1. 使用多维度恶化检测结果
        2. 显示具体恶化证据
        3. 区分风险严重程度
        """
        lines = ["## ⚠️ 风险警示", ""]
        lines.append("以下公司存在财务指标异常，建议**谨慎对待或规避**：")
        lines.append("")

        risky_companies = []

        # 只检查中大型公司
        if 'size_class' in df.columns:
            check_df = df[df['size_class'].isin(['mid', 'large', 'mega'])]
        else:
            check_df = df

        for _, row in check_df.iterrows():
            is_valid, cross_risks = self._check_cross_validation(row)
            has_det_risk, det_prob, det_evidence = self._check_deterioration_risk(row)

            all_risks = cross_risks.copy()
            # 添加恶化风险证据
            if has_det_risk:
                all_risks.extend(det_evidence)

            if all_risks:
                # 计算风险严重程度
                severity = "高危" if len(all_risks) >= 3 or det_prob > 0.8 else ("中危" if len(all_risks) >= 2 else "关注")

                risky_companies.append({
                    'code': row['ts_code'],
                    'name': row['name'],
                    'industry': row.get('industry', '-'),
                    'size': row.get('size_label', '-'),
                    'risks': all_risks,
                    'det_prob': det_prob,
                    'severity': severity
                })

        if not risky_companies:
            lines.append("✅ **未发现显著风险公司**")
            lines.append("")
            return lines

        # 按风险严重程度和数量排序
        severity_order = {"高危": 0, "中危": 1, "关注": 2}
        risky_companies.sort(key=lambda x: (severity_order.get(x['severity'], 3), -len(x['risks']), -x['det_prob']))

        lines.append("| 代码 | 名称 | 行业 | 规模 | 严重度 | 风险类型 |")
        lines.append("|---|---|---|---|---|---|")

        for item in risky_companies[:30]:
            risks_str = ", ".join(item['risks'][:3])  # 最多显示3个风险
            if len(item['risks']) > 3:
                risks_str += f" 等{len(item['risks'])}项"
            severity_icon = "🔴" if item['severity'] == "高危" else ("🟡" if item['severity'] == "中危" else "⚪")
            lines.append(f"| {item['code']} | {item['name']} | {item['industry']} | {item['size']} | {severity_icon}{item['severity']} | {risks_str} |")

        lines.append("")
        lines.append(f"> 📊 共发现 **{len(risky_companies)}** 家公司存在风险信号")
        high_risk_count = sum(1 for c in risky_companies if c['severity'] == "高危")
        if high_risk_count > 0:
            lines.append(f"> 🔴 其中 **{high_risk_count}** 家为高危公司，建议规避")
        lines.append("")
        return lines

    def _section_methodology(self) -> List[str]:
        """方法论说明（规则驱动版）"""
        lines = ["## 📖 方法论说明", ""]

        lines.append("### 4层过滤体系")
        lines.append("")
        lines.append("```")
        lines.append("第1层：基础质量过滤（整合趋势引擎评分）")
        lines.append("  ├─ 趋势斜率（成长性）：OLS + Theil-Sen稳健斜率")
        lines.append("  ├─ R²拟合度（稳定性）：趋势可靠性指标")
        lines.append("  ├─ 波动率（风险）：CV + 去趋势CV")
        lines.append("  ├─ 趋势评分（trend_score）：规则引擎综合评分")
        lines.append("  └─ Mann-Kendall检验：非参数趋势检验")
        lines.append("")
        lines.append("第2层：交叉验证（NaN安全处理）")
        lines.append("  ├─ 利润 vs 现金流 → 剔除纸面富贵")
        lines.append("  ├─ 营收 vs ROE → 剔除低效扩张")
        lines.append("  ├─ ROE vs ROIC → 剔除杠杆驱动")
        lines.append("  ├─ 毛利率 vs 净利率 → 验证费用控制")
        lines.append("  ├─ 连续下跌年数 → 持续恶化检测")
        lines.append("  ├─ 恶化加速度 → 加速下滑预警")
        lines.append("  ├─ OLS vs 稳健斜率 → 异常值干扰检测")
        lines.append("  ├─ fused_slope异方差 → 斜率稳定性验证")
        lines.append("  └─ FFT周期底部 → 周期公司错杀保护")
        lines.append("")
        lines.append("第3层：拐点检测（多维度恶化检测）")
        lines.append("  ├─ 贝叶斯恶化概率 → 综合多信号量化风险")
        lines.append("  ├─ Mann-Kendall趋势检验 → 统计显著性验证")
        lines.append("  ├─ 结构断点检测 → 识别公司质变点")
        lines.append("  ├─ 恶化模式(deterioration_pattern) → 分类恶化类型")
        lines.append("  ├─ 恶化加速度 → 检测恶化是否加剧")
        lines.append("  ├─ 波动率体制 → 识别波动扩大风险")
        lines.append("  └─ 周期置信度(cyclical_confidence) → 周期公司特殊处理")
        lines.append("")
        lines.append("第4层：综合评分 & 分类")
        lines.append("  ├─ 🚀 高成长 (GARP)：成长60% + 质量25% + 安全15%")
        lines.append("  ├─ 🏰 白马护城河：质量60% + 安全40%")
        lines.append("  └─ 🔄 困境反转：反转信号 + 断点确认 + 毛利保护")
        lines.append("```")
        lines.append("")

        lines.append("### 专业统计方法")
        lines.append("")
        lines.append("| 方法 | 用途 | 优势 |")
        lines.append("|---|---|---|")
        lines.append("| OLS回归 | 计算趋势斜率 | 经典方法，易解释 |")
        lines.append("| Theil-Sen估计 | 稳健斜率 | 抗异常值，最多容忍29%异常 |")
        lines.append("| Mann-Kendall | 趋势显著性 | 非参数检验，不假设分布 |")
        lines.append("| 贝叶斯推断 | 恶化概率 | 综合多信号，量化不确定性 |")
        lines.append("| CUSUM | 断点检测 | 识别结构性变化点 |")
        lines.append("| FFT频谱分析 | 周期检测 | 识别主导周期长度 |")
        lines.append("| 去趋势CV | 真实波动率 | 剔除趋势后的纯波动 |")
        lines.append("| WLS/融合斜率 | 异方差处理 | 近期数据权重更高 |")
        lines.append("")

        lines.append("### 筛选阈值说明")
        lines.append("")
        lines.append("| 指标 | 含义 | GARP阈值 | 白马阈值 | 风险阈值 |")
        lines.append("|---|---|---|---|---|")
        lines.append("| 营收CAGR | 复合年增长率 | >10% | - | - |")
        lines.append("| 利润CAGR | 复合年增长率 | >15% | - | - |")
        lines.append("| ROIC | 投入资本回报率 | - | >12% | - |")
        lines.append("| ROE | 净资产收益率 | - | >15% | - |")
        lines.append("| 毛利率 | 竞争力指标 | - | >25% | - |")
        lines.append("| R² | 趋势稳定性 | - | >0.5 | - |")
        lines.append("| 恶化概率 | 贝叶斯后验 | <50% | - | >60%预警 |")
        lines.append("| 连续下跌 | 下跌年数 | - | - | ≥3年预警 |")
        lines.append("| MK τ | 趋势系数 | - | >0.4奖励 | <-0.4预警 |")
        lines.append("")

        lines.append("### 风险等级定义")
        lines.append("")
        lines.append("| 等级 | 图标 | 定义 | 建议 |")
        lines.append("|---|---|---|---|")
        lines.append("| 高危 | 🔴 | 风险信号≥3项 或 恶化概率>80% | 规避 |")
        lines.append("| 中危 | 🟡 | 风险信号≥2项 | 谨慎 |")
        lines.append("| 关注 | ⚪ | 风险信号=1项 | 观察 |")
        lines.append("")

        lines.append("---")
        lines.append("")
        lines.append("> **免责声明**：本报告仅供投资参考，不构成投资建议。投资有风险，决策需谨慎。")
        lines.append(">")
        lines.append("> 如需T.R.U.T.H.数据驱动报告（无预设阈值），请使用 `report_truth` 方法。")
        lines.append("")

        return lines
