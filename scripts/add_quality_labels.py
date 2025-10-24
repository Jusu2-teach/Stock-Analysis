"""
为ROIC趋势分析结果添加质量标签
"""
import pandas as pd
import sys
from pathlib import Path

def add_quality_labels():
    """为每家公司添加质量分类标签"""

    # 读取数据
    df = pd.read_csv('data/filter_middle/roic_trend_analysis.csv')

    print(f"处理 {len(df)} 家公司...")

    def classify_quality(row):
        """
        分类逻辑:
        1. 严重恶化 - 应该被一票否决
        2. 增长转衰退 - 拐点公司,风险高
        3. 中度恶化 - 有下降趋势
        4. 优秀 - 稳定增长
        5. 良好 - 表现良好
        6. 普通 - 一般表现
        7. 需观察 - 有波动但可接受
        """

        # 计算关键指标
        latest_vs_weighted = (row['roic_latest'] / row['roic_weighted']) if row['roic_weighted'] > 0 else 1.0

        # 1. 严重恶化 (应被排除)
        if row['roic_deterioration_severity'] == 'severe':
            if row['roic_total_decline_pct'] > 40 or latest_vs_weighted < 0.7:
                return "严重恶化⚠️"

        # 2. 增长转衰退拐点 (高风险)
        if row['roic_inflection_type'] == 'growth_to_decline':
            if row['roic_penalty'] > 15:
                return "增长转衰退⚠️"

        # 3. 严重恶化但未被绝对指标捕获
        if row['roic_deterioration_severity'] == 'severe':
            return "严重恶化"

        # 4. 中度恶化
        if row['roic_has_deterioration'] and row['roic_deterioration_severity'] == 'moderate':
            if row['roic_penalty'] >= 10:
                return "中度恶化"

        # 5. 优秀 - 稳定增长
        if (row['roic_log_slope'] > 0.10 and
            row['roic_r_squared'] > 0.7 and
            row['roic_volatility_type'] in ['ultra_stable', 'stable'] and
            row['roic_penalty'] == 0):
            return "优秀✨"

        # 6. 良好 - 增长或恢复
        if row['roic_log_slope'] > 0.05 and row['roic_penalty'] < 5:
            if row['roic_inflection_type'] == 'deterioration_to_recovery':
                return "良好(恢复)"
            return "良好"

        # 7. 周期谷底机会
        if (row['roic_is_cyclical'] and
            row['roic_current_phase'] == 'trough' and
            row['roic_is_accelerating']):
            return "周期谷底"

        # 8. 高波动警告
        if row['roic_volatility_type'] == 'high_volatility' and row['roic_cv'] > 0.5:
            return "高波动⚠️"

        # 9. 轻度恶化
        if row['roic_has_deterioration'] and row['roic_deterioration_severity'] == 'mild':
            return "轻度恶化"

        # 10. 需观察
        if row['roic_penalty'] >= 5 and row['roic_penalty'] < 10:
            return "需观察"

        # 11. 普通
        if row['roic_penalty'] < 5:
            return "普通"

        # 12. 默认
        return "待评估"

    # 添加质量标签
    df['quality_label'] = df.apply(classify_quality, axis=1)

    # 统计各类别数量
    print("\n质量分类统计:")
    print("=" * 60)
    quality_counts = df['quality_label'].value_counts().sort_index()
    for label, count in quality_counts.items():
        percentage = (count / len(df)) * 100
        print(f"  {label:20s}: {count:3d} 家 ({percentage:5.1f}%)")

    # 保存结果
    output_file = 'data/filter_middle/roic_trend_analysis_with_labels.csv'
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n✅ 已保存到: {output_file}")

    # 显示一些样例
    print("\n严重恶化案例 (前10家):")
    print("=" * 100)
    severe_cases = df[df['quality_label'].str.contains('严重恶化')].head(10)
    for _, row in severe_cases.iterrows():
        print(f"  {row['name']:10s} ({row['ts_code']:12s}) | "
              f"行业:{row['industry']:10s} | "
              f"罚分:{row['roic_penalty']:6.2f} | "
              f"跌幅:{row['roic_total_decline_pct']:6.1f}% | "
              f"最新/加权:{(row['roic_latest']/row['roic_weighted']*100):5.1f}%")

    print("\n增长转衰退案例 (前10家):")
    print("=" * 100)
    inflection_cases = df[df['quality_label'].str.contains('增长转衰退')].head(10)
    for _, row in inflection_cases.iterrows():
        print(f"  {row['name']:10s} ({row['ts_code']:12s}) | "
              f"行业:{row['industry']:10s} | "
              f"罚分:{row['roic_penalty']:6.2f} | "
              f"早期斜率:{row['roic_early_slope']:6.2f} | "
              f"近期斜率:{row['roic_recent_slope']:7.2f}")

    print("\n优秀案例 (前10家):")
    print("=" * 100)
    excellent_cases = df[df['quality_label'].str.contains('优秀')].head(10)
    for _, row in excellent_cases.iterrows():
        print(f"  {row['name']:10s} ({row['ts_code']:12s}) | "
              f"行业:{row['industry']:10s} | "
              f"斜率:{row['roic_log_slope']:6.4f} | "
              f"R²:{row['roic_r_squared']:5.3f} | "
              f"波动:{row['roic_volatility_type']}")

    # 分析问题
    print("\n\n问题分析:")
    print("=" * 60)

    severe_count = len(df[df['quality_label'].str.contains('严重恶化')])
    inflection_count = len(df[df['quality_label'].str.contains('增长转衰退')])
    moderate_count = len(df[df['quality_label'].str.contains('中度恶化')])

    print(f"\n需要被排除的公司:")
    print(f"  - 严重恶化: {severe_count} 家 (应被一票否决)")
    print(f"  - 增长转衰退: {inflection_count} 家 (高风险拐点)")
    print(f"  - 中度恶化: {moderate_count} 家 (需要更严格筛选)")
    print(f"  合计: {severe_count + inflection_count + moderate_count} 家")

    print(f"\n改进建议:")
    print(f"  1. 实施规则8: 严重恶化一票否决")
    print(f"  2. 实施规则9: 持续衰退重罚+10分")
    print(f"  3. 实施规则10: 降低罚分阈值到15")
    print(f"  4. 调整P2加速度阈值从±2.0到±1.0")

    return df

if __name__ == '__main__':
    add_quality_labels()
