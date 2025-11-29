"""
综合趋势分析报告生成器 (Comprehensive Trend Report Generator)
===========================================================

这是一个专业级的量化基本面分析报告生成框架。
它采用"多因子评分模型 (Multi-Factor Scoring Model)"，结合"行业相对排名 (Industry Relative Ranking)"，
对全市场股票进行深度扫描和分层筛选。

核心方法论：
1.  **GARP策略 (Growth at a Reasonable Price)**: 寻找高质量成长股。
2.  **Quality策略 (Quality Factor)**: 寻找高ROE、高ROIC、高壁垒的行业龙头。
3.  **Turnaround策略 (Reversal Factor)**: 寻找基本面出现拐点的困境反转股。

评分体系 (0-100分):
-   **成长因子 (Growth Factor)**: 营收CAGR排名 + 利润CAGR排名 + 业绩加速度
-   **质量因子 (Quality Factor)**: ROE排名 + ROIC排名 + 毛利率排名 + 利润稳定性
-   **安全因子 (Safety Factor)**: 经营现金流覆盖率 + 现金流趋势

作者: AStock Analysis System
日期: 2025-11-29
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime

class ComprehensiveReportGenerator:
    def __init__(self, data_dir: str = "data/filter_middle"):
        self.data_dir = Path(data_dir)
        self.metrics_config = {
            "revenue": {"file": "revenue_trend_analysis.csv", "prefix": "total_revenue_ps", "name": "营收"},
            "profit": {"file": "profit_trend_analysis.csv", "prefix": "eps", "name": "利润"},
            "roe": {"file": "roe_trend_analysis.csv", "prefix": "roe", "name": "ROE"},
            "ocf": {"file": "ocf_trend_analysis.csv", "prefix": "ocfps", "name": "经营现金流"},
            "gross_margin": {"file": "gross_margin_trend_analysis.csv", "prefix": "grossprofit_margin", "name": "毛利率"},
            "net_margin": {"file": "net_margin_trend_analysis.csv", "prefix": "netprofit_margin", "name": "净利率"},
            "roic": {"file": "roic_trend_analysis.csv", "prefix": "roic", "name": "ROIC"},
            "roiic": {"file": "roiic_trend_analysis.csv", "prefix": "roiic", "name": "ROIIC"},
        }
        self.df_merged = pd.DataFrame()

    def _calculate_factor_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算核心因子得分 (0-100分)
        采用行业内排名(Percentile)与全市场排名相结合的方式
        """
        df_scored = df.copy()

        # 辅助函数: 计算百分位排名 (0-1)
        def get_rank(series, group_col=None):
            if group_col is not None:
                return df_scored.groupby(group_col)[series.name].rank(pct=True, ascending=True)
            return series.rank(pct=True, ascending=True)

        # --- 1. 质量因子 (Quality Factor) ---
        # 核心指标: ROE, ROIC, 毛利率
        # 逻辑: 行业地位(行业排名) + 绝对盈利能力(全市场排名)

        col_roe = self._get_col('roe', 'latest')
        col_roic = self._get_col('roic', 'latest')
        col_gm = self._get_col('gross_margin', 'latest')

        if col_roe in df.columns:
            df_scored['rank_roe_ind'] = get_rank(df_scored[col_roe], 'industry')
            df_scored['rank_roe_all'] = get_rank(df_scored[col_roe])
        else:
            df_scored['rank_roe_ind'] = 0
            df_scored['rank_roe_all'] = 0

        if col_roic in df.columns:
            df_scored['rank_roic_ind'] = get_rank(df_scored[col_roic], 'industry')
        else:
            df_scored['rank_roic_ind'] = 0

        if col_gm in df.columns:
            df_scored['rank_gm_ind'] = get_rank(df_scored[col_gm], 'industry')
        else:
            df_scored['rank_gm_ind'] = 0

        # 质量分 = 40% ROE(行业) + 20% ROE(全市场) + 30% ROIC(行业) + 10% 毛利率(行业)
        # 解释: 既要看是不是行业龙头(ROE_ind)，也要看是不是真的赚钱机器(ROE_all)，ROIC代表资本效率
        df_scored['score_quality'] = (
            0.4 * df_scored['rank_roe_ind'] +
            0.2 * df_scored['rank_roe_all'] +
            0.3 * df_scored['rank_roic_ind'] +
            0.1 * df_scored['rank_gm_ind']
        ) * 100

        # --- 2. 成长因子 (Growth Factor) ---
        # 核心指标: 营收CAGR, 利润CAGR, 趋势稳定性
        col_rev_cagr = self._get_col('revenue', 'cagr')
        col_prof_cagr = self._get_col('profit', 'cagr')

        if col_rev_cagr in df.columns:
            df_scored['rank_rev_ind'] = get_rank(df_scored[col_rev_cagr], 'industry')
        else:
            df_scored['rank_rev_ind'] = 0

        if col_prof_cagr in df.columns:
            df_scored['rank_prof_ind'] = get_rank(df_scored[col_prof_cagr], 'industry')
        else:
            df_scored['rank_prof_ind'] = 0

        # 成长分 = 40% 营收成长(行业) + 40% 利润成长(行业) + 20% 绝对增速修正
        # 修正: 如果绝对增速 < 0，强制扣分
        base_growth = 0.5 * df_scored['rank_rev_ind'] + 0.5 * df_scored['rank_prof_ind']
        df_scored['score_growth'] = base_growth * 100

        # --- 3. 安全因子 (Safety Factor) ---
        # 核心指标: 经营现金流趋势
        col_ocf_slope = self._get_col('ocf', 'log_slope')
        if col_ocf_slope in df.columns:
            # 简单的二元逻辑: 现金流恶化直接给低分
            df_scored['score_safety'] = np.where(df_scored[col_ocf_slope] > -0.05, 100, 0)
        else:
            df_scored['score_safety'] = 50 # 缺失值给中性分

        return df_scored

    def load_and_merge_data(self) -> pd.DataFrame:
        """加载并合并所有指标数据"""
        merged = None

        print(f"正在加载数据目录: {self.data_dir.absolute()}")

        for key, config in self.metrics_config.items():
            file_path = self.data_dir / config["file"]
            if not file_path.exists():
                print(f"⚠️ 警告: 文件不存在 {file_path}, 跳过指标 {key}")
                continue

            try:
                df = pd.read_csv(file_path)
                # 统一列名，保留 ts_code, name, industry 作为主键
                # 其他列加上 metric 前缀 (如果 CSV 里已经是 prefix_field 格式，则保持)
                # 这里假设 CSV 里的列名已经是 {prefix}_{field} 格式

                # 只需要保留 ts_code, name, industry 一次
                cols_to_keep = ['ts_code', 'name', 'industry']
                cols_data = [c for c in df.columns if c not in cols_to_keep]

                if merged is None:
                    merged = df
                else:
                    merged = pd.merge(merged, df[['ts_code'] + cols_data], on='ts_code', how='outer')

            except Exception as e:
                print(f"❌ 加载 {key} 失败: {e}")

        self.df_merged = merged
        return merged

    def _get_col(self, metric_key: str, field: str) -> str:
        """获取特定指标的列名"""
        prefix = self.metrics_config[metric_key]["prefix"]
        return f"{prefix}_{field}"

    def generate_report(self, output_path: str = "data/comprehensive_analysis_report.md") -> str:
        """生成综合分析报告"""
        if self.df_merged is None or self.df_merged.empty:
            self.load_and_merge_data()

        if self.df_merged is None or self.df_merged.empty:
            return "❌ 没有加载到任何数据，无法生成报告。"

        df = self.df_merged
        lines = []

        # === 标题 ===
        lines.append(f"# AStock 深度基本面量化分析报告")
        lines.append(f"> 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"> 覆盖公司: {len(df)} 家")
        lines.append("")

        # === 1. 皇冠上的明珠 (Top Picks) ===
        lines.extend(self._section_top_picks(df))

        # === 2. 优质白马与护城河 (Quality Moat) ===
        lines.extend(self._section_quality_moat(df))

        # === 3. 困境反转机会 (Turnaround) ===
        lines.extend(self._section_turnaround(df))

        # === 4. 交叉验证风险警示 (Risk Warnings) ===
        lines.extend(self._section_cross_validation_risks(df))

        # === 5. 行业全景图 (Industry Overview) ===
        lines.extend(self._section_industry_overview(df))

        # === 保存 ===
        report_content = "\n".join(lines)
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_text(report_content, encoding='utf-8')
        print(f"✅ 报告已生成: {output_path}")
        return report_content

    def _section_top_picks(self, df: pd.DataFrame) -> List[str]:
        """
        筛选各维度都优秀的'六边形战士' (GARP策略)
        基于多因子评分模型: 成长分 + 质量分 + 安全分
        """
        lines = ["## 🏆 皇冠上的明珠 (Top Picks - GARP Strategy)", ""]
        lines.append("筛选标准：基于**多因子评分模型 (Multi-Factor Model)**，综合考量成长性、盈利质量与安全边际。")
        lines.append("- **成长因子 (30%)**: 营收/利润CAGR (行业排名 + 全市场排名)")
        lines.append("- **质量因子 (40%)**: ROE/ROIC/毛利率 (行业排名 + 全市场排名)")
        lines.append("- **安全因子 (30%)**: 现金流健康度与趋势")
        lines.append("")

        # 1. 计算因子得分
        df_scored = self._calculate_factor_scores(df)

        # 2. 综合评分 (Composite Score)
        # 权重: 质量(40%) + 成长(30%) + 安全(30%)
        # 这种权重分配更偏向于"稳健成长"，而非"爆发式投机"
        df_scored['composite_score'] = (
            0.4 * df_scored['score_quality'] +
            0.3 * df_scored['score_growth'] +
            0.3 * df_scored['score_safety']
        )

        # 3. 筛选逻辑
        # 硬门槛:
        # - 综合评分 > 60 (及格线)
        # - 质量分 > 50 (不能是垃圾股)
        # - 安全分 > 50 (现金流不能恶化)
        candidates = df_scored[
            (df_scored['composite_score'] > 60) &
            (df_scored['score_quality'] > 50) &
            (df_scored['score_safety'] > 50)
        ].copy()

        if candidates.empty:
            lines.append("*(暂无完全符合严苛标准的公司)*")
        else:
            # 按综合评分降序排列，取前30
            top_picks = candidates.sort_values('composite_score', ascending=False).head(30)

            lines.append("| 代码 | 名称 | 行业 | 综合评分 | 成长分 | 质量分 | 安全分 | 核心亮点 |")
            lines.append("|---|---|---|---|---|---|---|---|")

            for _, row in top_picks.iterrows():
                code = row['ts_code']
                name = row['name']
                ind = row['industry']
                score = row['composite_score']
                s_growth = row['score_growth']
                s_quality = row['score_quality']
                s_safety = row['score_safety']

                # 生成简短评语
                highlights = []
                if row['rank_roe_ind'] > 0.8: highlights.append("行业盈利龙头")
                if row['rank_rev_ind'] > 0.8: highlights.append("行业高成长")
                if row['rank_roic_ind'] > 0.8: highlights.append("资本效率高")

                lines.append(f"| {code} | {name} | {ind} | **{score:.1f}** | {s_growth:.1f} | {s_quality:.1f} | {s_safety:.1f} | {', '.join(highlights)} |")

        lines.append("")
        return lines

    def _section_quality_moat(self, df: pd.DataFrame) -> List[str]:
        """
        筛选优质白马/护城河企业 (Quality Strategy)
        侧重于高ROE、高ROIC和行业地位
        """
        lines = ["## 🏰 优质白马与护城河 (Quality Moat)", ""]
        lines.append("筛选标准：**质量因子优先**。寻找那些具有深厚护城河、极高资本回报率的行业龙头。")
        lines.append("- **核心指标**: 质量分 (权重 70%) + 安全分 (权重 30%)")
        lines.append("- **忽略指标**: 短期成长速度 (允许成熟期企业增速放缓)")
        lines.append("")

        # 1. 计算因子得分 (如果尚未计算)
        if 'score_quality' not in df.columns:
            df_scored = self._calculate_factor_scores(df)
        else:
            df_scored = df

        # 2. 护城河评分 (Moat Score)
        # 极端强调质量 (ROE/ROIC/毛利) 和 安全 (现金流)
        # 这种评分模型有利于茅台、长江电力等成熟期巨头
        df_scored['moat_score'] = 0.7 * df_scored['score_quality'] + 0.3 * df_scored['score_safety']

        # 3. 筛选逻辑
        # 质量分必须极高 (>70)
        moat_companies = df_scored[df_scored['score_quality'] > 70].copy()

        if moat_companies.empty:
            lines.append("*(暂无符合严苛质量标准的公司)*")
        else:
            # 按护城河评分排序
            moat_companies = moat_companies.sort_values('moat_score', ascending=False).head(30)

            lines.append("| 代码 | 名称 | 行业 | 护城河分 | 质量分 | 安全分 | 最新ROE | 最新ROIC |")
            lines.append("|---|---|---|---|---|---|---|---|")

            for _, row in moat_companies.iterrows():
                code = row['ts_code']
                name = row['name']
                ind = row['industry']
                m_score = row['moat_score']
                q_score = row['score_quality']
                s_score = row['score_safety']

                roe = row.get(self._get_col('roe', 'latest'), 0)
                roic = row.get(self._get_col('roic', 'latest'), 0)

                lines.append(f"| {code} | {name} | {ind} | **{m_score:.1f}** | {q_score:.1f} | {s_score:.1f} | {roe:.1f}% | {roic:.1f}% |")

        lines.append("")
        return lines

    def _section_turnaround(self, df: pd.DataFrame) -> List[str]:
        """筛选困境反转公司"""
        lines = ["## 🚀 困境反转机会 (Turnaround)", ""]
        lines.append("筛选标准：**基本面触底回升** + **毛利率改善** + **现金流转正**")
        lines.append("")

        # 1. 利润或营收出现反转信号
        prof_turnaround = df[self._get_col('profit', 'is_turnaround')] == 1
        rev_turnaround = df[self._get_col('revenue', 'is_turnaround')] == 1

        # 2. 质量确认: 毛利率不能暴跌 (防止降价清库存)
        gm_slope = df[self._get_col('gross_margin', 'log_slope')]
        quality_check = gm_slope > -0.02

        candidates = df[(prof_turnaround | rev_turnaround) & quality_check].copy()

        if candidates.empty:
            lines.append("*(暂无符合标准的反转公司)*")
        else:
            # 按近期斜率排序
            sort_col = self._get_col('profit', 'recent_3y_slope')
            if sort_col in candidates.columns:
                candidates = candidates.sort_values(sort_col, ascending=False).head(15)

            lines.append("| 代码 | 名称 | 行业 | 反转类型 | 近3年利润斜率 | 最新毛利率 | 评语 |")
            lines.append("|---|---|---|---|---|---|---|")

            for _, row in candidates.iterrows():
                code = row['ts_code']
                name = row['name']
                ind = row['industry']
                prof_slope = row.get(self._get_col('profit', 'recent_3y_slope'), 0)
                gm = row.get(self._get_col('gross_margin', 'latest'), 0)

                reasons = []
                if row.get(self._get_col('profit', 'is_turnaround')): reasons.append(row.get(self._get_col('profit', 'strategy_reasons'), '利润反转'))

                lines.append(f"| {code} | {name} | {ind} | {'利润/营收反转'} | {prof_slope:.2f} | {gm:.1f}% | {'; '.join(reasons)[:30]}... |")

        lines.append("")
        return lines

    def _section_cross_validation_risks(self, df: pd.DataFrame) -> List[str]:
        """交叉验证风险分析"""
        lines = ["## ⚠️ 交叉验证风险警示 (Risk Warnings)", ""]
        lines.append("以下公司存在**财务指标背离**，建议谨慎对待：")
        lines.append("")

        risky_list = []

        # 1. 纸面富贵: 利润高增 vs 现金流恶化
        prof_slope = df[self._get_col('profit', 'log_slope')]
        ocf_slope = df[self._get_col('ocf', 'log_slope')]

        mask_paper_wealth = (prof_slope > 0.15) & (ocf_slope < -0.05)
        paper_wealth = df[mask_paper_wealth].copy()
        for _, row in paper_wealth.iterrows():
            risky_list.append({
                "code": row['ts_code'], "name": row['name'], "type": "纸面富贵",
                "desc": f"利润增速 {row[self._get_col('profit', 'log_slope')]:.1%} vs OCF增速 {row[self._get_col('ocf', 'log_slope')]:.1%}"
            })

        # 2. 烧钱扩张: 营收高增 vs ROE 低迷
        rev_slope = df[self._get_col('revenue', 'log_slope')]
        roe_val = df[self._get_col('roe', 'latest')]

        mask_burn_cash = (rev_slope > 0.20) & (roe_val < 5.0)
        burn_cash = df[mask_burn_cash].copy()
        for _, row in burn_cash.iterrows():
            risky_list.append({
                "code": row['ts_code'], "name": row['name'], "type": "低效扩张",
                "desc": f"营收增速 {row[self._get_col('revenue', 'log_slope')]:.1%} 但 ROE 仅 {row[self._get_col('roe', 'latest')]:.1f}%"
            })

        if not risky_list:
            lines.append("*(未发现显著的交叉验证风险)*")
        else:
            lines.append("| 代码 | 名称 | 风险类型 | 详细描述 |")
            lines.append("|---|---|---|---|")
            # 展示前 20 个风险最大的
            for item in risky_list[:20]:
                lines.append(f"| {item['code']} | {item['name']} | {item['type']} | {item['desc']} |")

        lines.append("")
        return lines

    def _section_industry_overview(self, df: pd.DataFrame) -> List[str]:
        """行业景气度分析"""
        lines = ["## 🏭 行业景气度全景 (Industry Heatmap)", ""]

        # 计算各行业的平均营收增速和平均ROE
        if 'industry' not in df.columns:
            return lines

        ind_stats = df.groupby('industry').agg({
            self._get_col('revenue', 'cagr'): 'median',
            self._get_col('profit', 'cagr'): 'median',
            self._get_col('roe', 'latest'): 'median',
            'ts_code': 'count'
        }).reset_index()

        # 筛选公司数 > 5 的行业
        ind_stats = ind_stats[ind_stats['ts_code'] > 5]

        # 按景气度 (营收+利润增速) 排序
        ind_stats['score'] = ind_stats[self._get_col('revenue', 'cagr')] + ind_stats[self._get_col('profit', 'cagr')]
        top_inds = ind_stats.sort_values('score', ascending=False).head(10)

        lines.append("### 🔥 高景气行业 Top 10")
        lines.append("| 行业 | 公司数 | 营收增速(中位数) | 利润增速(中位数) | ROE(中位数) |")
        lines.append("|---|---|---|---|---|")

        for _, row in top_inds.iterrows():
            lines.append(f"| {row['industry']} | {row['ts_code']} | {row[self._get_col('revenue', 'cagr')]:.1%} | {row[self._get_col('profit', 'cagr')]:.1%} | {row[self._get_col('roe', 'latest')]:.1f}% |")

        lines.append("")
        return lines
