"""
趋势分析详细报告生成器 (v2.0)

功能：
- 读取roic_trend_analysis.csv
- 生成包含P0+P1+P2所有指标的详细Markdown报告
- 输出到data/trend_analysis_report.md

作者: AStock Analysis System
日期: 2025-10-11
"""
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Callable, List


INFLECTION_RECOVERY = "deterioration_to_recovery"
INFLECTION_DECLINE = "growth_to_decline"
INFLECTION_LABELS = {
    INFLECTION_RECOVERY: "deterioration_to_recovery",
    INFLECTION_DECLINE: "growth_to_decline",
    'none': 'none',
}

SEVERITY_ORDER = {
    'none': 0,
    'mild': 1,
    'moderate': 2,
    'severe': 3,
}

STABLE_TYPES = {'stable', 'ultra_stable'}
VOLATILE_TYPES = {'volatile', 'high_volatility'}


def generate_trend_analysis_report(
    input_csv: str = 'data/filter_middle/roic_trend_analysis.csv',
    output_path: str = 'data/trend_analysis_report.md',
    metric_prefix: str = 'roic',
    metric_suffix: str = ''
) -> None:
    """
    生成趋势分析详细报告

    Args:
        input_csv: 输入CSV文件路径
        output_path: 输出Markdown报告路径
        metric_prefix: 趋势指标前缀（默认 roic，可自定义为其他指标）
        metric_suffix: 趋势指标后缀（若 analyze_metric_trend 自定义了 suffix，可在此传入）
    """
    print("="*80)
    print("📊 生成趋势分析详细报告 (v2.0)".center(80))
    print("="*80)

    # 读取数据
    print(f"\n📖 读取数据: {input_csv}")
    df = pd.read_csv(input_csv)
    print(f"✅ 成功加载 {len(df)} 家企业数据")

    def col(field: str) -> str:
        return f"{metric_prefix}_{field}{metric_suffix}"

    # 生成报告
    report_lines = []

    # ========== 报告头部 ==========
    report_lines.extend(_generate_header(df))

    # ========== 1. 执行摘要 ==========
    report_lines.extend(_generate_executive_summary(df, col))

    # ========== 2. P0波动率分析 ==========
    report_lines.extend(_generate_p0_volatility_analysis(df, col))

    # ========== 3. P1拐点与恶化分析 ==========
    report_lines.extend(_generate_p1_inflection_analysis(df, col))

    # ========== 4. P2周期性与加速度分析 ==========
    report_lines.extend(_generate_p2_cyclical_analysis(df, col))

    # ========== 5. 行业分布分析 ==========
    report_lines.extend(_generate_industry_analysis(df, col))

    # ========== 6. 投资机会识别 ==========
    report_lines.extend(_generate_investment_opportunities(df, col))

    # ========== 7. 风险警示 ==========
    report_lines.extend(_generate_risk_warnings(df, col))

    # ========== 8. 附录 ==========
    report_lines.extend(_generate_appendix(df, col))

    # 写入文件
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))

    print(f"\n✅ 报告生成成功: {output_path}")
    print(f"📄 报告行数: {len(report_lines)}")
    print("="*80)


def _generate_header(df: pd.DataFrame) -> List[str]:
    """生成报告头部"""
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    return [
        "# 趋势分析详细报告 (v2.0)",
        "",
        f"**生成时间**: {now}  ",
        f"**分析企业数**: {len(df)} 家  ",
        f"**系统版本**: v2.0 (P0+P1+P2)  ",
        "",
        "---",
        ""
    ]


def _generate_executive_summary(df: pd.DataFrame, col: Callable[[str], str]) -> List[str]:
    """生成执行摘要"""
    lines = [
        "## 📊 执行摘要",
        "",
        "### 核心指标概览",
        ""
    ]

    # 计算核心指标
    total = len(df)
    denom = total if total else 1

    # P0指标
    volatility_col = col('volatility_type')
    log_slope_col = col('log_slope')
    has_inflection_col = col('has_inflection')
    inflection_type_col = col('inflection_type')
    has_deterioration_col = col('has_deterioration')
    is_cyclical_col = col('is_cyclical')
    is_accelerating_col = col('is_accelerating')
    is_decelerating_col = col('is_decelerating')

    stable_mask = df[volatility_col].isin(STABLE_TYPES)
    volatile_mask = df[volatility_col].isin(VOLATILE_TYPES)

    stable = stable_mask.sum()
    volatile = volatile_mask.sum()
    moderate = total - stable - volatile

    # P1指标
    has_inflection = df[has_inflection_col].sum()
    has_deterioration = df[has_deterioration_col].sum()

    # P2指标
    is_cyclical = df[is_cyclical_col].sum()
    is_accelerating = df[is_accelerating_col].sum()
    is_decelerating = df[is_decelerating_col].sum()

    # 趋势分类
    positive = (df[log_slope_col] > 0).sum()
    negative = (df[log_slope_col] < 0).sum()

    lines.extend([
        "| 维度 | 指标 | 数量 | 占比 |",
        "|------|------|------|------|",
    f"| **总体** | 通过筛选企业 | {total} | {100.0 if total else 0.0:.1f}% |",
    f"| **趋势方向** | 正向增长 | {positive} | {positive/denom*100:.1f}% |",
    f"| | 负向衰退 | {negative} | {negative/denom*100:.1f}% |",
    f"| **P0 波动率** | 稳定型 (含 ultra stable) | {stable} | {stable/denom*100:.1f}% |",
    f"| | 中等波动 | {moderate} | {moderate/denom*100:.1f}% |",
    f"| | 高波动 (含 high_volatility) | {volatile} | {volatile/denom*100:.1f}% |",
    f"| **P1 拐点** | 检出拐点 | {has_inflection} | {has_inflection/denom*100:.1f}% |",
    f"| | 近期恶化 | {has_deterioration} | {has_deterioration/denom*100:.1f}% |",
    f"| **P2 周期性** | 周期性企业 | {is_cyclical} | {is_cyclical/denom*100:.1f}% |",
    f"| **P2 加速度** | 加速上升 | {is_accelerating} | {is_accelerating/denom*100:.1f}% |",
    f"| | 加速下降 | {is_decelerating} | {is_decelerating/denom*100:.1f}% |",
        "",
        "---",
        ""
    ])

    return lines


def _generate_p0_volatility_analysis(df: pd.DataFrame, col: Callable[[str], str]) -> List[str]:
    """生成P0波动率分析"""
    lines = [
        "## 📈 P0: 波动率分析",
        "",
        "### 波动率类型分布",
        ""
    ]

    # 波动率统计
    volatility_col = col('volatility_type')
    cv_col = col('cv')
    std_col = col('std_dev')

    stable = df[df[volatility_col].isin(STABLE_TYPES)]
    moderate = df[df[volatility_col] == 'moderate']
    volatile = df[df[volatility_col].isin(VOLATILE_TYPES)]

    lines.extend([
        f"- **稳定型企业** ({len(stable)}家): 变异系数分布于稳定区间",
        f"- **中等波动企业** ({len(moderate)}家)",
        f"- **高波动企业** ({len(volatile)}家): 包含 volatile/high_volatility",
        "",
        "### 波动率统计指标",
        "",
        "| 指标 | 均值 | 中位数 | 最小值 | 最大值 |",
        "|------|------|--------|--------|--------|",
        f"| 变异系数 (CV) | {df[cv_col].mean():.3f} | {df[cv_col].median():.3f} | {df[cv_col].min():.3f} | {df[cv_col].max():.3f} |",
        f"| 标准差 | {df[std_col].mean():.3f} | {df[std_col].median():.3f} | {df[std_col].min():.3f} | {df[std_col].max():.3f} |",
        "",
        "### 🌟 最稳定企业 TOP10 (CV最低)",
        ""
    ])

    # 最稳定企业
    top_stable = stable.nsmallest(10, cv_col)
    for idx, (_, row) in enumerate(top_stable.iterrows(), 1):
        name = row.get('name', row['ts_code'])
        cv = row[cv_col]
        slope = row[col('log_slope')]
        industry = row.get('industry', 'N/A')
        lines.append(f"{idx}. **{name}** ({row['ts_code']}) - CV: {cv:.3f}, 斜率: {slope:.3f}, 行业: {industry}")

    lines.extend(["", "---", ""])
    return lines


def _generate_p1_inflection_analysis(df: pd.DataFrame, col: Callable[[str], str]) -> List[str]:
    """生成P1拐点与恶化分析"""
    lines = [
        "## 🔄 P1: 拐点与恶化分析",
        "",
        "### 拐点检测结果",
        ""
    ]

    has_inflection_col = col('has_inflection')
    inflection_type_col = col('inflection_type')
    slope_change_col = col('slope_change')
    inflection_confidence_col = col('inflection_confidence')
    has_deterioration_col = col('has_deterioration')
    deterioration_severity_col = col('deterioration_severity')
    total_decline_col = col('total_decline_pct')

    has_inflection = df[df[has_inflection_col]].copy()
    no_inflection = df[~df[has_inflection_col]].copy()
    total = len(df)
    denom = total if total else 1

    lines.extend([
        f"- **检出拐点**: {len(has_inflection)} 家 ({len(has_inflection)/denom*100:.1f}%)",
        f"- **无拐点**: {len(no_inflection)} 家 ({len(no_inflection)/denom*100:.1f}%)",
        "",
        "### 拐点类型分布",
        ""
    ])

    if not has_inflection.empty:
        inflection_types = has_inflection[inflection_type_col].value_counts()
        for inflection_type, count in inflection_types.items():
            pct = count / len(has_inflection) * 100 if len(has_inflection) else 0.0
            emoji = "📈" if inflection_type == INFLECTION_RECOVERY else "📉"
            lines.append(f"- {emoji} **{inflection_type}**: {count} 家 ({pct:.1f}%)")

        lines.extend([
            "",
            "### 🎯 拐点反转机会 (恶化→好转)",
            ""
        ])

        reversal = has_inflection[has_inflection[inflection_type_col] == INFLECTION_RECOVERY]
        if not reversal.empty:
            top_reversal = reversal.nlargest(10, slope_change_col)
            for idx, (_, row) in enumerate(top_reversal.iterrows(), 1):
                name = row.get('name', row['ts_code'])
                change = row[slope_change_col]
                confidence = row[inflection_confidence_col]
                lines.append(f"{idx}. **{name}** - 斜率改善: {change:.2f}, 置信度: {confidence:.2f}")

    lines.extend([
        "",
        "### ⚠️ 近期恶化预警",
        ""
    ])

    deterioration = df[df[has_deterioration_col]].copy()
    lines.append(f"**检出近期恶化**: {len(deterioration)} 家 ({len(deterioration)/denom*100:.1f}%)")
    lines.append("")

    if not deterioration.empty:
        deterioration['severity_score'] = deterioration[deterioration_severity_col].map(SEVERITY_ORDER).fillna(-1)
        top_deterioration = deterioration.sort_values(
            ['severity_score', total_decline_col], ascending=[False, True]
        ).head(10)
        for idx, (_, row) in enumerate(top_deterioration.iterrows(), 1):
            name = row.get('name', row['ts_code'])
            severity = row[deterioration_severity_col]
            total_decline = row[total_decline_col]
            if row['severity_score'] >= 0:
                lines.append(
                    f"{idx}. **{name}** - 恶化严重度: {severity}, 总跌幅: {total_decline:.1f}%"
                )

    lines.extend(["", "---", ""])
    return lines


def _generate_p2_cyclical_analysis(df: pd.DataFrame, col: Callable[[str], str]) -> List[str]:
    """生成P2周期性与加速度分析"""
    lines = [
        "## 🔄 P2: 周期性与加速度分析",
        "",
        "### 周期性企业识别",
        ""
    ]

    # 周期性统计
    is_cyclical_col = col('is_cyclical')
    current_phase_col = col('current_phase')
    peak_to_trough_col = col('peak_to_trough_ratio')
    log_slope_col = col('log_slope')
    trend_acceleration_col = col('trend_acceleration')
    is_accelerating_col = col('is_accelerating')
    is_decelerating_col = col('is_decelerating')
    recent_3y_slope_col = col('recent_3y_slope')

    cyclical = df[df[is_cyclical_col]]
    non_cyclical = df[~df[is_cyclical_col]]
    total = len(df)
    denom = total if total else 1

    lines.extend([
    f"- **周期性企业**: {len(cyclical)} 家 ({len(cyclical)/denom*100:.1f}%)",
    f"- **非周期性企业**: {len(non_cyclical)} 家 ({len(non_cyclical)/denom*100:.1f}%)",
        "",
        "### 周期阶段分布",
        ""
    ])

    if len(cyclical) > 0:
        phase_counts = cyclical[current_phase_col].value_counts()
        phase_emoji = {
            'trough': '🔽 底部',
            'peak': '🔼 顶部',
            'rising': '📈 上升',
            'falling': '📉 下降',
            'unknown': '❓ 未知'
        }

        for phase, count in phase_counts.items():
            pct = count / len(cyclical) * 100
            emoji = phase_emoji.get(phase, phase)
            lines.append(f"- {emoji}: {count} 家 ({pct:.1f}%)")

        lines.extend([
            "",
            "### 💎 周期底部机会 (峰谷比>3)",
            ""
        ])

        # 周期底部企业
        trough = cyclical[cyclical[current_phase_col] == 'trough']
        if len(trough) > 0:
            top_trough = trough.nlargest(10, peak_to_trough_col)
            for idx, (_, row) in enumerate(top_trough.iterrows(), 1):
                name = row.get('name', row['ts_code'])
                ratio = row[peak_to_trough_col]
                slope = row[log_slope_col]
                industry = row.get('industry', 'N/A')
                lines.append(f"{idx}. **{name}** - 峰谷比: {ratio:.2f}, 斜率: {slope:.3f}, 行业: {industry}")

    lines.extend([
        "",
        "### 🚀 趋势加速度分析",
        ""
    ])

    # 加速度统计
    accelerating = df[df[is_accelerating_col]]
    decelerating = df[df[is_decelerating_col]]
    stable_trend = df[~(df[is_accelerating_col] | df[is_decelerating_col])]

    lines.extend([
        f"- **加速上升**: {len(accelerating)} 家 ({len(accelerating)/len(df)*100:.1f}%)",
        f"- **加速下降**: {len(decelerating)} 家 ({len(decelerating)/len(df)*100:.1f}%)",
        f"- **趋势稳定**: {len(stable_trend)} 家 ({len(stable_trend)/len(df)*100:.1f}%)",
        "",
        "### ⚡ 加速上升企业 TOP10",
        ""
    ])

    if len(accelerating) > 0:
        top_accelerating = accelerating.nlargest(10, trend_acceleration_col)
        for idx, (_, row) in enumerate(top_accelerating.iterrows(), 1):
            name = row.get('name', row['ts_code'])
            acc = row[trend_acceleration_col]
            slope_3y = row[recent_3y_slope_col]
            lines.append(f"{idx}. **{name}** - 加速度: {acc:.2f}, 3年斜率: {slope_3y:.3f}")

    lines.extend(["", "---", ""])
    return lines


def _generate_industry_analysis(df: pd.DataFrame, col: Callable[[str], str]) -> List[str]:
    """生成行业分布分析"""
    lines = [
        "## 🏭 行业分布分析",
        "",
        "### 行业企业数量分布 TOP15",
        ""
    ]

    if 'industry' in df.columns:
        industry_counts = df['industry'].value_counts().head(15)
        denom = len(df) if len(df) else 1

        lines.append("| 排名 | 行业 | 企业数 | 占比 |")
        lines.append("|------|------|--------|------|")

        for idx, (industry, count) in enumerate(industry_counts.items(), 1):
            pct = count / denom * 100
            lines.append(f"| {idx} | {industry} | {count} | {pct:.1f}% |")

        lines.extend([
            "",
            "### 行业平均ROIC趋势斜率 TOP10",
            ""
        ])

        # 行业平均斜率
        industry_slope = df.groupby('industry')[col('log_slope')].agg(['mean', 'count']).reset_index()
        industry_slope = industry_slope[industry_slope['count'] >= 3]  # 至少3家企业
        top_industries = industry_slope.nlargest(10, 'mean')

        lines.append("| 排名 | 行业 | 平均斜率 | 企业数 |")
        lines.append("|------|------|----------|--------|")

        for idx, row in enumerate(top_industries.itertuples(), 1):
            lines.append(f"| {idx} | {row.industry} | {row.mean:.3f} | {int(row.count)} |")
    else:
        lines.append("_行业信息不可用_")

    lines.extend(["", "---", ""])
    return lines


def _generate_investment_opportunities(df: pd.DataFrame, col: Callable[[str], str]) -> List[str]:
    """生成投资机会识别"""
    lines = [
        "## 💎 投资机会识别",
        "",
        "### 机会类型1: 周期底部+加速上升 (最优机会)",
        ""
    ]

    # 周期底部+加速上升
    opportunity1 = df[
        (df[col('is_cyclical')]) &
        (df[col('current_phase')] == 'trough') &
        (df[col('is_accelerating')])
    ]

    lines.append(f"**数量**: {len(opportunity1)} 家")
    lines.append("")

    if len(opportunity1) > 0:
        for idx, (_, row) in enumerate(opportunity1.iterrows(), 1):
            name = row.get('name', row['ts_code'])
            slope = row[col('log_slope')]
            acc = row[col('trend_acceleration')]
            ratio = row[col('peak_to_trough_ratio')]
            lines.append(f"{idx}. **{name}** - 斜率: {slope:.3f}, 加速度: {acc:.2f}, 峰谷比: {ratio:.2f}")
    else:
        lines.append("_暂无符合条件的企业_")

    lines.extend([
        "",
        "### 机会类型2: 拐点反转+低波动 (稳健机会)",
        ""
    ])

    # 拐点反转+低波动
    opportunity2 = df[
        (df[col('has_inflection')]) &
        (df[col('inflection_type')] == INFLECTION_RECOVERY) &
        (df[col('volatility_type')].isin(STABLE_TYPES))
    ].nlargest(10, col('slope_change'))

    lines.append(f"**数量**: {len(opportunity2)} 家 (展示TOP10)")
    lines.append("")

    if len(opportunity2) > 0:
        for idx, (_, row) in enumerate(opportunity2.iterrows(), 1):
            name = row.get('name', row['ts_code'])
            change = row[col('slope_change')]
            cv = row[col('cv')]
            lines.append(f"{idx}. **{name}** - 斜率改善: {change:.2f}, CV: {cv:.3f}")
    else:
        lines.append("_暂无符合条件的企业_")

    lines.extend([
        "",
        "### 机会类型3: 非周期+加速+低波动 (成长机会)",
        ""
    ])

    # 非周期+加速+低波动
    opportunity3 = df[
        (~df[col('is_cyclical')]) &
        (df[col('is_accelerating')]) &
        (df[col('volatility_type')].isin(STABLE_TYPES)) &
        (df[col('log_slope')] > 0)
    ].nlargest(10, col('trend_acceleration'))

    lines.append(f"**数量**: {len(opportunity3)} 家 (展示TOP10)")
    lines.append("")

    if len(opportunity3) > 0:
        for idx, (_, row) in enumerate(opportunity3.iterrows(), 1):
            name = row.get('name', row['ts_code'])
            acc = row[col('trend_acceleration')]
            slope = row[col('log_slope')]
            cv = row[col('cv')]
            lines.append(f"{idx}. **{name}** - 加速度: {acc:.2f}, 斜率: {slope:.3f}, CV: {cv:.3f}")
    else:
        lines.append("_暂无符合条件的企业_")

    lines.extend(["", "---", ""])
    return lines


def _generate_risk_warnings(df: pd.DataFrame, col: Callable[[str], str]) -> List[str]:
    """生成风险警示"""
    lines = [
        "## ⚠️ 风险警示",
        "",
        "### 风险类型1: 周期顶部预警",
        ""
    ]

    # 周期顶部
    risk1 = df[
        (df[col('is_cyclical')]) &
        (df[col('current_phase')] == 'peak')
    ].nlargest(10, col('peak_to_trough_ratio'))

    lines.append(f"**数量**: {len(risk1)} 家 (展示TOP10)")
    lines.append("")

    if len(risk1) > 0:
        for idx, (_, row) in enumerate(risk1.iterrows(), 1):
            name = row.get('name', row['ts_code'])
            ratio = row[col('peak_to_trough_ratio')]
            slope = row[col('log_slope')]
            lines.append(f"{idx}. **{name}** - 峰谷比: {ratio:.2f}, 斜率: {slope:.3f}")
    else:
        lines.append("_暂无风险预警_")

    lines.extend([
        "",
        "### 风险类型2: 加速恶化+近期恶化",
        ""
    ])

    # 加速恶化+近期恶化
    risk2_temp = df[
        (df[col('is_decelerating')]) &
        (df[col('has_deterioration')])
    ].copy()
    risk2_temp['severity_score'] = risk2_temp[col('deterioration_severity')].map(SEVERITY_ORDER).fillna(-1)
    risk2 = risk2_temp.sort_values(
        ['severity_score', col('trend_acceleration')], ascending=[False, True]
    ).head(10)

    lines.append(f"**数量**: {len(risk2_temp)} 家 (展示TOP10)")
    lines.append("")

    if len(risk2) > 0:
        for idx, (_, row) in enumerate(risk2.iterrows(), 1):
            name = row.get('name', row['ts_code'])
            acc = row[col('trend_acceleration')]
            severity = row[col('deterioration_severity')]
            if row['severity_score'] >= 0:
                lines.append(f"{idx}. **{name}** - 加速度: {acc:.2f}, 恶化严重度: {severity}")
    else:
        lines.append("_暂无风险预警_")

    lines.extend([
        "",
        "### 风险类型3: 高波动+负趋势",
        ""
    ])

    # 高波动+负趋势
    risk3 = df[
        (df[col('volatility_type')].isin(VOLATILE_TYPES)) &
        (df[col('log_slope')] < 0)
    ].nsmallest(10, col('log_slope'))

    lines.append(f"**数量**: {len(risk3)} 家 (展示TOP10)")
    lines.append("")

    if len(risk3) > 0:
        for idx, (_, row) in enumerate(risk3.iterrows(), 1):
            name = row.get('name', row['ts_code'])
            cv = row[col('cv')]
            slope = row[col('log_slope')]
            lines.append(f"{idx}. **{name}** - CV: {cv:.3f}, 斜率: {slope:.3f}")
    else:
        lines.append("_暂无风险预警_")

    lines.extend(["", "---", ""])
    return lines


def _generate_appendix(df: pd.DataFrame, col: Callable[[str], str]) -> List[str]:
    """生成附录"""
    lines = [
        "## 📚 附录",
        "",
        "### 指标说明",
        "",
    "#### P0 波动率指标",
    f"- **{col('cv')}**: 变异系数 (标准差/均值)",
    f"- **{col('std_dev')}**: 标准差",
    f"- **{col('volatility_type')}**: 波动类型分类",
        "",
        "#### P1 拐点指标",
    f"- **{col('has_inflection')}**: 是否存在拐点",
    f"- **{col('inflection_type')}**: 拐点类型",
    f"- **{col('early_slope')}**: 前期斜率",
    f"- **{col('recent_slope')}**: 后期斜率",
    f"- **{col('slope_change')}**: 斜率变化量",
    f"- **{col('inflection_confidence')}**: 拐点置信度",
    f"- **{col('has_deterioration')}**: 是否近期恶化",
    f"- **{col('deterioration_severity')}**: 恶化严重度",
        "",
        "#### P2 周期性指标",
    f"- **{col('is_cyclical')}**: 是否周期性企业",
    f"- **{col('peak_to_trough_ratio')}**: 峰谷比",
    f"- **{col('current_phase')}**: 当前周期阶段 (trough/peak/rising/falling)",
    f"- **{col('recent_3y_slope')}**: 最近3年斜率",
    f"- **{col('trend_acceleration')}**: 趋势加速度",
    f"- **{col('is_accelerating')}**: 是否加速上升",
    f"- **{col('is_decelerating')}**: 是否加速下降",
        "",
        "### 数据统计",
        "",
        f"- **总企业数**: {len(df)} 家",
        f"- **分析年份**: 5年 (2020-2024)",
        f"- **报告生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "---",
        "",
        "*本报告由 AStock Analysis System v2.0 自动生成*"
    ]

    return lines


if __name__ == '__main__':
    # 测试运行
    generate_trend_analysis_report()
