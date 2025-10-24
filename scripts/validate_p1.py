"""P1功能验证脚本 - 拐点识别和近期恶化检测"""
import pandas as pd

df = pd.read_csv('data/filter_middle/roic_trend_analysis.csv')

print("=" * 80)
print("✅ P1功能验证报告 - 拐点识别 & 近期恶化检测")
print("=" * 80)

print(f"\n📊 总公司数: {len(df)} 家")

# ========== 1. 拐点识别分析 ==========
print("\n" + "=" * 80)
print("🔍 拐点识别功能")
print("=" * 80)

inflection_companies = df[df['roic_has_inflection'] == True]
print(f"\n检测到拐点的公司: {len(inflection_companies)} 家 ({len(inflection_companies)/len(df)*100:.1f}%)")

if len(inflection_companies) > 0:
    print(f"\n📈 拐点类型分布:")
    print(inflection_companies['roic_inflection_type'].value_counts())

    # 恶化→好转 (潜在机会)
    recovery = inflection_companies[inflection_companies['roic_inflection_type'] == 'deterioration_to_recovery']
    print(f"\n✨ 恶化→好转 (潜在反转机会): {len(recovery)} 家")
    if len(recovery) > 0:
        print("\n   典型案例 (按斜率变化排序):")
        top_recovery = recovery.nlargest(10, 'roic_slope_change')
        for _, row in top_recovery.iterrows():
            print(f"      {row['name']:10s} | 前期斜率={row['roic_early_slope']:+6.2f} "
                  f"近期斜率={row['roic_recent_slope']:+6.2f} "
                  f"变化={row['roic_slope_change']:+6.2f} "
                  f"置信度={row['roic_inflection_confidence']:.2f} "
                  f"最新={row['roic_latest']:.1f}%")

    # 好转→恶化 (风险警示)
    decline = inflection_companies[inflection_companies['roic_inflection_type'] == 'growth_to_decline']
    print(f"\n⚠️  好转→恶化 (风险警示): {len(decline)} 家")
    if len(decline) > 0:
        print("\n   典型案例 (按斜率变化排序):")
        top_decline = decline.nsmallest(10, 'roic_slope_change')
        for _, row in top_decline.iterrows():
            print(f"      {row['name']:10s} | 前期斜率={row['roic_early_slope']:+6.2f} "
                  f"近期斜率={row['roic_recent_slope']:+6.2f} "
                  f"变化={row['roic_slope_change']:+6.2f} "
                  f"置信度={row['roic_inflection_confidence']:.2f} "
                  f"最新={row['roic_latest']:.1f}%")

# ========== 2. 近期恶化检测 ==========
print("\n" + "=" * 80)
print("🔍 近期恶化检测功能")
print("=" * 80)

deterioration_companies = df[df['roic_has_deterioration'] == True]
print(f"\n检测到近期恶化的公司: {len(deterioration_companies)} 家 ({len(deterioration_companies)/len(df)*100:.1f}%)")

if len(deterioration_companies) > 0:
    print(f"\n📉 恶化严重程度分布:")
    print(deterioration_companies['roic_deterioration_severity'].value_counts())

    # 严重恶化
    severe = deterioration_companies[deterioration_companies['roic_deterioration_severity'] == 'severe']
    print(f"\n🔴 严重恶化 (近2年跌幅>30%): {len(severe)} 家")
    if len(severe) > 0:
        print("\n   典型案例 (按跌幅排序):")
        top_severe = severe.nsmallest(10, 'roic_total_decline_pct')
        for _, row in top_severe.iterrows():
            print(f"      {row['name']:10s} | 年3→4变化={row['roic_year3_to_4_change']:+6.2f} "
                  f"年4→5变化={row['roic_year4_to_5_change']:+6.2f} "
                  f"累计跌幅={row['roic_total_decline_pct']:+6.1f}% "
                  f"最新={row['roic_latest']:.1f}%")

    # 中度恶化
    moderate = deterioration_companies[deterioration_companies['roic_deterioration_severity'] == 'moderate']
    print(f"\n🟡 中度恶化 (近2年跌幅15-30%): {len(moderate)} 家")

    # 轻度恶化
    mild = deterioration_companies[deterioration_companies['roic_deterioration_severity'] == 'mild']
    print(f"\n🟢 轻度恶化 (近2年跌幅<15%): {len(mild)} 家")

# ========== 3. 组合情况分析 ==========
print("\n" + "=" * 80)
print("🔍 组合情况分析")
print("=" * 80)

# 既有拐点又有恶化
both = df[(df['roic_has_inflection'] == True) & (df['roic_has_deterioration'] == True)]
print(f"\n既有拐点又有近期恶化: {len(both)} 家")

# 恶化但出现反转拐点 (最有价值的信号)
recovery_after_decline = df[
    (df['roic_inflection_type'] == 'deterioration_to_recovery') &
    (df['roic_has_deterioration'] == True)
]
print(f"\n💎 恶化后出现反转拐点 (高潜力): {len(recovery_after_decline)} 家")
if len(recovery_after_decline) > 0:
    print("\n   这些公司经历了近期恶化,但最近2年开始反转,可能是周期底部机会!")
    for _, row in recovery_after_decline.head(5).iterrows():
        print(f"      {row['name']:10s} | 跌幅={row['roic_total_decline_pct']:+6.1f}% "
              f"斜率变化={row['roic_slope_change']:+6.2f} "
              f"最新={row['roic_latest']:.1f}%")

# 无拐点无恶化 (稳定企业)
stable = df[(df['roic_has_inflection'] == False) & (df['roic_has_deterioration'] == False)]
print(f"\n✅ 无拐点无恶化 (稳定企业): {len(stable)} 家 ({len(stable)/len(df)*100:.1f}%)")

# ========== 4. P1功能效果总结 ==========
print("\n" + "=" * 80)
print("📊 P1功能效果总结")
print("=" * 80)

print(f"""
拐点识别:
  - 检出率: {len(inflection_companies)/len(df)*100:.1f}%
  - 恶化→好转: {len(recovery) if len(inflection_companies) > 0 else 0} 家
  - 好转→恶化: {len(decline) if len(inflection_companies) > 0 else 0} 家

近期恶化检测:
  - 检出率: {len(deterioration_companies)/len(df)*100:.1f}%
  - 严重恶化: {len(severe) if len(deterioration_companies) > 0 else 0} 家
  - 中度恶化: {len(moderate) if len(deterioration_companies) > 0 else 0} 家
  - 轻度恶化: {len(mild) if len(deterioration_companies) > 0 else 0} 家

关键洞察:
  ✅ P1功能成功识别了 {len(inflection_companies) + len(deterioration_companies)} 个风险/机会信号
  ✅ {len(recovery_after_decline)} 家公司出现"恶化后反转",值得重点关注
  ✅ {len(stable)} 家稳定企业,无明显风险信号
""")

print("=" * 80)
print("✅ P1功能验证完成!")
print("=" * 80)
