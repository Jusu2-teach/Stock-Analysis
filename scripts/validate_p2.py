"""P2功能验证脚本 - 周期性识别 & 3年滚动趋势"""
import pandas as pd

df = pd.read_csv('data/filter_middle/roic_trend_analysis.csv')

print("=" * 80)
print("✅ P2功能验证报告 - 周期性识别 & 3年滚动趋势")
print("=" * 80)

print(f"\n📊 总公司数: {len(df)} 家")

# ========== 1. 周期性识别分析 ==========
print("\n" + "=" * 80)
print("🔄 周期性识别功能")
print("=" * 80)

cyclical_companies = df[df['roic_is_cyclical'] == True]
print(f"\n检测到周期性的公司: {len(cyclical_companies)} 家 ({len(cyclical_companies)/len(df)*100:.1f}%)")

if len(cyclical_companies) > 0:
    print(f"\n📈 当前周期阶段分布:")
    print(cyclical_companies['roic_current_phase'].value_counts())

    # 行业周期性标记
    industry_cyclical = df[df['roic_industry_cyclical'] == True]
    print(f"\n🏭 行业属于周期性: {len(industry_cyclical)} 家 ({len(industry_cyclical)/len(df)*100:.1f}%)")

    # 周期底部企业 (潜在机会)
    trough = cyclical_companies[cyclical_companies['roic_current_phase'] == 'trough']
    print(f"\n💎 周期底部企业 (潜在机会): {len(trough)} 家")
    if len(trough) > 0:
        print("\n   典型案例 (按峰谷比排序):")
        top_trough = trough.nlargest(10, 'roic_peak_to_trough_ratio')
        for _, row in top_trough.iterrows():
            print(f"      {row['name']:10s} | 峰谷比={row['roic_peak_to_trough_ratio']:.2f} "
                  f"行业={row['industry']:8s} "
                  f"最新={row['roic_latest']:.1f}% "
                  f"阶段={row['roic_current_phase']}")

    # 周期顶部企业 (风险警示)
    peak = cyclical_companies[cyclical_companies['roic_current_phase'] == 'peak']
    print(f"\n⚠️  周期顶部企业 (风险警示): {len(peak)} 家")
    if len(peak) > 0:
        print("\n   典型案例 (按峰谷比排序):")
        top_peak = peak.nlargest(5, 'roic_peak_to_trough_ratio')
        for _, row in top_peak.iterrows():
            print(f"      {row['name']:10s} | 峰谷比={row['roic_peak_to_trough_ratio']:.2f} "
                  f"行业={row['industry']:8s} "
                  f"最新={row['roic_latest']:.1f}% "
                  f"阶段={row['roic_current_phase']}")

    # 上升/下降阶段
    rising = cyclical_companies[cyclical_companies['roic_current_phase'] == 'rising']
    falling = cyclical_companies[cyclical_companies['roic_current_phase'] == 'falling']
    print(f"\n📊 其他阶段:")
    print(f"   上升阶段: {len(rising)} 家")
    print(f"   下降阶段: {len(falling)} 家")

# ========== 2. 3年滚动趋势分析 ==========
print("\n" + "=" * 80)
print("⚡ 3年滚动趋势功能")
print("=" * 80)

# 加速上升
accelerating = df[df['roic_is_accelerating'] == True]
print(f"\n⚡ 加速上升企业: {len(accelerating)} 家 ({len(accelerating)/len(df)*100:.1f}%)")
if len(accelerating) > 0:
    print("\n   典型案例 (按加速度排序):")
    top_accelerating = accelerating.nlargest(10, 'roic_trend_acceleration')
    for _, row in top_accelerating.iterrows():
        print(f"      {row['name']:10s} | 5年斜率={row['roic_log_slope']:+6.3f} "
              f"3年斜率={row['roic_recent_3y_slope']:+6.2f} "
              f"加速度={row['roic_trend_acceleration']:+6.2f} "
              f"最新={row['roic_latest']:.1f}%")

# 加速下滑
decelerating = df[df['roic_is_decelerating'] == True]
print(f"\n⚠️  加速下滑企业: {len(decelerating)} 家 ({len(decelerating)/len(df)*100:.1f}%)")
if len(decelerating) > 0:
    print("\n   典型案例 (按加速度排序):")
    top_decelerating = decelerating.nsmallest(10, 'roic_trend_acceleration')
    for _, row in top_decelerating.iterrows():
        print(f"      {row['name']:10s} | 5年斜率={row['roic_log_slope']:+6.3f} "
              f"3年斜率={row['roic_recent_3y_slope']:+6.2f} "
              f"加速度={row['roic_trend_acceleration']:+6.2f} "
              f"最新={row['roic_latest']:.1f}%")

# 稳定趋势
stable_trend = df[(df['roic_is_accelerating'] == False) & (df['roic_is_decelerating'] == False)]
print(f"\n✅ 稳定趋势企业: {len(stable_trend)} 家 ({len(stable_trend)/len(df)*100:.1f}%)")

# ========== 3. 组合情况分析 ==========
print("\n" + "=" * 80)
print("🔍 组合情况分析")
print("=" * 80)

# 周期底部 + 加速上升 (最佳机会)
best_opportunity = df[
    (df['roic_is_cyclical'] == True) &
    (df['roic_current_phase'] == 'trough') &
    (df['roic_is_accelerating'] == True)
]
print(f"\n💎💎 周期底部+加速上升 (最佳机会): {len(best_opportunity)} 家")
if len(best_opportunity) > 0:
    print("\n   这些是周期性行业的底部反转机会!")
    for _, row in best_opportunity.iterrows():
        print(f"      {row['name']:10s} | 峰谷比={row['roic_peak_to_trough_ratio']:.2f} "
              f"加速度={row['roic_trend_acceleration']:+6.2f} "
              f"最新={row['roic_latest']:.1f}%")

# 周期顶部 + 加速下滑 (最高风险)
highest_risk = df[
    (df['roic_is_cyclical'] == True) &
    (df['roic_current_phase'] == 'peak') &
    (df['roic_is_decelerating'] == True)
]
print(f"\n⚠️⚠️  周期顶部+加速下滑 (最高风险): {len(highest_risk)} 家")
if len(highest_risk) > 0:
    print("\n   这些企业处于周期顶部且开始加速下滑,高风险!")
    for _, row in highest_risk.iterrows():
        print(f"      {row['name']:10s} | 峰谷比={row['roic_peak_to_trough_ratio']:.2f} "
              f"加速度={row['roic_trend_acceleration']:+6.2f} "
              f"最新={row['roic_latest']:.1f}%")

# 非周期性 + 加速上升 (稳健增长)
steady_growth = df[
    (df['roic_is_cyclical'] == False) &
    (df['roic_is_accelerating'] == True) &
    (df['roic_cv'] < 0.30)  # 低波动
]
print(f"\n✨ 非周期+加速上升+低波动 (稳健增长): {len(steady_growth)} 家")

# ========== 4. 峰谷比分析 ==========
print("\n" + "=" * 80)
print("📊 峰谷比分析")
print("=" * 80)

print(f"\n峰谷比统计:")
print(f"   平均值: {df['roic_peak_to_trough_ratio'].mean():.2f}")
print(f"   中位数: {df['roic_peak_to_trough_ratio'].median():.2f}")
print(f"   最大值: {df['roic_peak_to_trough_ratio'].max():.2f}")

extreme_volatility = df[df['roic_peak_to_trough_ratio'] > 5.0]
print(f"\n极端波动企业 (峰谷比>5): {len(extreme_volatility)} 家")
if len(extreme_volatility) > 0:
    print("\n   Top 10:")
    top_volatile = extreme_volatility.nlargest(10, 'roic_peak_to_trough_ratio')
    for _, row in top_volatile.iterrows():
        print(f"      {row['name']:10s} | 峰谷比={row['roic_peak_to_trough_ratio']:.2f} "
              f"周期性={row['roic_is_cyclical']} "
              f"阶段={row['roic_current_phase']}")

# ========== 5. P2功能效果总结 ==========
print("\n" + "=" * 80)
print("📊 P2功能效果总结")
print("=" * 80)

print(f"""
周期性识别:
  - 检出率: {len(cyclical_companies)/len(df)*100:.1f}%
  - 周期底部: {len(trough) if len(cyclical_companies) > 0 else 0} 家 (潜在机会)
  - 周期顶部: {len(peak) if len(cyclical_companies) > 0 else 0} 家 (风险警示)
  - 行业周期性标记: {len(industry_cyclical)} 家

3年滚动趋势:
  - 加速上升: {len(accelerating)} 家 ({len(accelerating)/len(df)*100:.1f}%)
  - 加速下滑: {len(decelerating)} 家 ({len(decelerating)/len(df)*100:.1f}%)
  - 稳定趋势: {len(stable_trend)} 家 ({len(stable_trend)/len(df)*100:.1f}%)

关键洞察:
  ✅ P2功能识别了 {len(cyclical_companies) + len(accelerating) + len(decelerating)} 个周期/加速信号
  💎 {len(best_opportunity)} 家"周期底部+加速上升"企业,最佳机会
  ⚠️ {len(highest_risk)} 家"周期顶部+加速下滑"企业,最高风险
  ✨ {len(steady_growth)} 家"非周期+加速+低波动"企业,稳健增长
""")

print("=" * 80)
print("✅ P2功能验证完成!")
print("=" * 80)
