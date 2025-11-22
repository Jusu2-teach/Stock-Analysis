"""
企业质量评分与分级系统
======================

为筛选出的公司进行质量评分(0-100分)和分级(S/A/B/C/D/F)
"""

import pandas as pd
import numpy as np
from pathlib import Path


# Component weights (sum to 100)
ROIC_COMPONENT_FULL_SCORE = 40
TREND_COMPONENT_FULL_SCORE = 35
LATEST_COMPONENT_FULL_SCORE = 15
STABILITY_COMPONENT_FULL_SCORE = 10

# Penalty thresholds derived from trend engine outputs
TREND_PENALTY_HIGH_THRESHOLD = 15
TREND_PENALTY_MEDIUM_THRESHOLD = 10
TREND_PENALTY_LOW_THRESHOLD = 5

# Trend score guard rails
TREND_SCORE_STRONG = 80
TREND_SCORE_MODERATE = 60
TREND_SCORE_WEAK = 40


def calculate_quality_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算企业质量综合评分 (0-100分)

    评分维度:
    1. ROIC质量分 (40分) - 加权平均ROIC
    2. 趋势健康分 (35分) - 归一化趋势评分 (0-100 → 0-35)
    3. 最新期活力分 (15分) - 最新期ROIC
    4. 稳定性分 (10分) - R²拟合优度

    风险扣分:
    - 趋势重罚: 趋势引擎罚分≥15 → 扣12分
    - 趋势警报: 趋势引擎罚分≥10 → 扣8分
    - 趋势关注: 趋势引擎罚分≥5 → 扣4分
    - 极弱趋势: 趋势得分<40 → 扣5分
    - 低ROIC: 加权ROIC<8% → 扣10分
    - 最新崩盘: 最新ROIC<6% → 扣8分
    """

    required_columns = {
        'roic_weighted',
        'roic_trend_score',
        'roic_latest',
        'roic_r_squared',
    }

    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(
            "缺少质量评分所需的基础字段: " + ", ".join(sorted(missing))
        )

    result_df = df.copy()

    # ========== 1. ROIC质量分 (40分) ==========
    def score_roic(roic):
        """
        ROIC评分标准:
        ≥30%: 40分 (卓越)
        25-30%: 35分 (优秀+)
        20-25%: 30分 (优秀)
        15-20%: 25分 (良好+)
        12-15%: 20分 (良好)
        10-12%: 15分 (合格+)
        8-10%: 10分 (合格)
        6-8%: 5分 (及格)
        <6%: 0分 (不合格)
        """
        if roic >= 30:
            return 40
        elif roic >= 25:
            return 35
        elif roic >= 20:
            return 30
        elif roic >= 15:
            return 25
        elif roic >= 12:
            return 20
        elif roic >= 10:
            return 15
        elif roic >= 8:
            return 10
        elif roic >= 6:
            return 5
        else:
            return 0

    result_df['score_roic'] = result_df['roic_weighted'].apply(score_roic)

    # ========== 2. 趋势健康分 (35分) ==========
    trend_score_series = result_df['roic_trend_score'].clip(lower=0, upper=100).fillna(0.0)
    result_df['trend_score_raw'] = trend_score_series
    result_df['score_trend'] = (trend_score_series / 100.0 * TREND_COMPONENT_FULL_SCORE).round(2)

    # ========== 3. 最新期活力分 (15分) ==========
    def score_latest(latest_roic):
        """
        最新期ROIC评分:
        ≥25%: 15分
        20-25%: 12分
        15-20%: 10分
        12-15%: 8分
        10-12%: 6分
        8-10%: 4分
        <8%: 0分
        """
        if latest_roic >= 25:
            return 15
        elif latest_roic >= 20:
            return 12
        elif latest_roic >= 15:
            return 10
        elif latest_roic >= 12:
            return 8
        elif latest_roic >= 10:
            return 6
        elif latest_roic >= 8:
            return 4
        else:
            return 0

    result_df['score_latest'] = result_df['roic_latest'].apply(score_latest)

    # ========== 4. 稳定性分 (10分) ==========
    def score_stability(r_squared):
        """
        R²拟合优度评分:
        ≥0.80: 10分 (趋势非常显著)
        0.60-0.80: 7分 (趋势显著)
        0.40-0.60: 5分 (中等趋势)
        0.20-0.40: 3分 (趋势微弱)
        <0.20: 0分 (无明显趋势)
        """
        if r_squared >= 0.80:
            return 10
        elif r_squared >= 0.60:
            return 7
        elif r_squared >= 0.40:
            return 5
        elif r_squared >= 0.20:
            return 3
        else:
            return 0

    result_df['score_stability'] = result_df['roic_r_squared'].apply(score_stability)

    # ========== 5. 计算基础分 ==========
    result_df['base_score'] = (
        result_df['score_roic'] +
        result_df['score_trend'] +
        result_df['score_latest'] +
        result_df['score_stability']
    )

    # ========== 6. 风险扣分 ==========
    def calculate_penalty(row):
        """风险扣分基于趋势罚分、ROIC质量和最新表现."""
        penalty = 0

        roic_penalty = row.get('roic_penalty', 0) or 0
        trend_score = row.get('roic_trend_score', 0) or 0
        roic_weighted = row['roic_weighted']
        roic_latest = row['roic_latest']

        # 引入趋势引擎产生的罚分作为风险加权
        if roic_penalty >= TREND_PENALTY_HIGH_THRESHOLD:
            penalty += 12
        elif roic_penalty >= TREND_PENALTY_MEDIUM_THRESHOLD:
            penalty += 8
        elif roic_penalty >= TREND_PENALTY_LOW_THRESHOLD:
            penalty += 4

        # 若趋势得分极弱,额外扣分
        if trend_score < TREND_SCORE_WEAK:
            penalty += 5

        # ROIC质量扣分
        if roic_weighted < 8:
            penalty += 10

        # 最新期扣分
        if roic_latest < 6:
            penalty += 8

        return penalty

    result_df['penalty'] = result_df.apply(calculate_penalty, axis=1)

    # ========== 7. 最终得分 (0-100) ==========
    result_df['quality_score'] = result_df['base_score'] - result_df['penalty']
    result_df['quality_score'] = result_df['quality_score'].clip(0, 100)

    # ========== 8. 评级 (S/A/B/C/D/F) ==========
    def assign_grade(score):
        """
        评级标准:
        S级: 90-100分 (顶级企业)
        A级: 80-89分 (优秀企业)
        B级: 70-79分 (良好企业)
        C级: 60-69分 (合格企业)
        D级: 50-59分 (及格企业)
        F级: <50分 (不合格企业)
        """
        if score >= 90:
            return 'S'
        elif score >= 80:
            return 'A'
        elif score >= 70:
            return 'B'
        elif score >= 60:
            return 'C'
        elif score >= 50:
            return 'D'
        else:
            return 'F'

    result_df['grade'] = result_df['quality_score'].apply(assign_grade)

    # ========== 9. 风险标签 ==========
    def assign_risk_label(row):
        """趋势罚分与得分结合生成更贴近趋势引擎的风险标签."""
        roic_penalty = row.get('roic_penalty', 0) or 0
        trend_score = row.get('roic_trend_score', 0) or 0
        log_slope = row.get('roic_log_slope', 0) or 0

        if roic_penalty >= TREND_PENALTY_HIGH_THRESHOLD:
            return '🔴 高风险-趋势恶化'
        if roic_penalty >= TREND_PENALTY_MEDIUM_THRESHOLD:
            return '🟠 高风险-明显下滑'
        if trend_score < TREND_SCORE_WEAK:
            return '🟡 警惕-趋势走弱'
        if trend_score < TREND_SCORE_MODERATE:
            return '🟢 关注-轻度波动'
        if trend_score < TREND_SCORE_STRONG:
            return '⚪ 正常'
        if log_slope < 0.15:
            return '🔵 优秀-稳健增长'
        if trend_score >= 95:
            return '⭐ 明星-高速增长'
        return '🔵 优秀-稳健增长'

    result_df['risk_label'] = result_df.apply(assign_risk_label, axis=1)

    # ========== 10. 投资建议 ==========
    def assign_recommendation(row):
        """
        投资建议:
        - 强烈推荐: S级 + 趋势强势 (得分≥80 且罚分低于5)
        - 推荐买入: S/A级 + 趋势稳定 (得分≥60 且罚分<10)
        - 可以关注: A/B级 + 趋势正常 (得分≥40)
        - 谨慎观察: B/C级 + 趋势偏弱
        - 规避风险: 其余组合
        """
        grade = row['grade']
        trend_score = row.get('roic_trend_score', 0) or 0
        roic_penalty = row.get('roic_penalty', 0) or 0

        if grade == 'S' and trend_score >= TREND_SCORE_STRONG and roic_penalty < TREND_PENALTY_LOW_THRESHOLD:
            return '⭐⭐⭐ 强烈推荐'
        if grade in {'S', 'A'} and trend_score >= TREND_SCORE_MODERATE and roic_penalty < TREND_PENALTY_MEDIUM_THRESHOLD:
            return '⭐⭐ 推荐买入'
        if grade in {'A', 'B'} and trend_score >= TREND_SCORE_WEAK:
            return '⭐ 可以关注'
        if grade in {'C', 'B'} and trend_score >= TREND_SCORE_WEAK:
            return '⚠️ 谨慎观察'
        return '❌ 规避风险'

    result_df['recommendation'] = result_df.apply(assign_recommendation, axis=1)

    if 'roic_penalty_details' in result_df.columns and 'trend_penalty_details' not in result_df.columns:
        result_df['trend_penalty_details'] = result_df['roic_penalty_details']

    return result_df


def generate_quality_report(df: pd.DataFrame, output_path: Path | str | None = None) -> str:
    """生成质量评分报告, 可选写入文件."""

    lines: list[str] = []

    def add(line: str = "") -> None:
        lines.append(line)

    add("=" * 80)
    add("企业质量评分报告")
    add("=" * 80)
    add()

    # 评级分布
    add("【评级分布】")
    grade_dist = df['grade'].value_counts().sort_index()
    for grade in ['S', 'A', 'B', 'C', 'D', 'F']:
        count = grade_dist.get(grade, 0)
        pct = count / len(df) * 100
        add(f"  {grade}级: {count:3d}家 ({pct:5.1f}%)")

    add()

    # 风险标签分布
    add("【风险标签分布】")
    risk_dist = df['risk_label'].value_counts()
    for label, count in risk_dist.items():
        pct = count / len(df) * 100
        add(f"  {label}: {count:3d}家 ({pct:5.1f}%)")

    add()

    # 投资建议分布
    add("【投资建议分布】")
    rec_dist = df['recommendation'].value_counts()
    for rec, count in rec_dist.items():
        pct = count / len(df) * 100
        add(f"  {rec}: {count:3d}家 ({pct:5.1f}%)")

    add()

    # 得分统计
    add("【得分统计】")
    add(f"  平均分: {df['quality_score'].mean():.2f}")
    add(f"  中位数: {df['quality_score'].median():.2f}")
    add(f"  最高分: {df['quality_score'].max():.2f}")
    add(f"  最低分: {df['quality_score'].min():.2f}")
    add(f"  标准差: {df['quality_score'].std():.2f}")

    add()

    sorted_df = df.sort_values('quality_score', ascending=False)
    top_n = sorted_df.head(50)
    bottom_n = sorted_df.tail(50)

    add("【Top 50 最高质量企业】")
    for idx, row in enumerate(top_n.itertuples(index=False), start=1):
        add(
            f"  {idx:2d}. {row.ts_code} {getattr(row, 'name', '')} | {getattr(row, 'industry', '')} | "
            f"分数 {row.quality_score:.2f} | 评级 {row.grade} | 推荐 {getattr(row, 'recommendation', '')}"
        )

    add()
    add("【Bottom 50 最低质量企业】")
    for offset, row in enumerate(bottom_n.itertuples(index=False), start=1):
        add(
            f"  {offset:2d}. {row.ts_code} {getattr(row, 'name', '')} | {getattr(row, 'industry', '')} | "
            f"分数 {row.quality_score:.2f} | 评级 {row.grade} | 推荐 {getattr(row, 'recommendation', '')}"
        )

    add()
    add("=" * 80)

    report_text = "\n".join(lines)
    print(report_text)

    if output_path is not None:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(report_text + "\n", encoding='utf-8')

    return report_text


if __name__ == '__main__':
    # 读取数据
    input_file = Path('data/filter_middle/roic_trend_analysis.csv')
    output_file = Path('data/filter_middle/roic_quality_scored.csv')

    print(f"读取数据: {input_file}")
    df = pd.read_csv(input_file)
    print(f"原始数据: {len(df)}家公司")
    print()

    # 计算评分
    print("正在计算质量评分...")
    df_scored = calculate_quality_score(df)

    # 生成报告
    report_file = Path('data/filter_middle/roic_quality_report.txt')
    generate_quality_report(df_scored, output_path=report_file)

    # 按得分排序
    df_scored = df_scored.sort_values('quality_score', ascending=False)

    # Top 20
    print("\n【Top 20 最高质量企业】")
    top20 = df_scored.head(20)[['ts_code', 'name', 'industry', 'quality_score',
                                  'grade', 'risk_label', 'recommendation',
                                  'roic_weighted', 'roic_log_slope', 'roic_latest']]
    print(top20.to_string(index=False))

    # Bottom 20
    print("\n【Bottom 20 最低质量企业】")
    bottom20 = df_scored.tail(20)[['ts_code', 'name', 'industry', 'quality_score',
                                    'grade', 'risk_label', 'recommendation',
                                    'roic_weighted', 'roic_log_slope', 'roic_latest']]
    print(bottom20.to_string(index=False))

    # 保存结果
    print(f"\n保存结果到: {output_file}")
    df_scored.to_csv(output_file, index=False, encoding='utf-8-sig')

    print("\n✅ 完成!")
