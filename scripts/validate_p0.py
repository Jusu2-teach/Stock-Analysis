"""P0修复效果验证脚本"""
import pandas as pd

df = pd.read_csv('data/filter_middle/roic_trend_analysis.csv')

print("=" * 80)
print("✅ P0修复验证报告 - R²逻辑修正")
print("=" * 80)

print(f"\n📊 筛选后公司总数: {len(df)} 家 (修复前: 362家)")
print(f"   变化: {len(df) - 362:+d} 家 ({(len(df) - 362) / 362 * 100:+.1f}%)")

print(f"\n🔍 波动率类型分布:")
print(df['roic_volatility_type'].value_counts())

print(f"\n📈 波动率统计 (CV - 变异系数):")
print(f"   平均CV: {df['roic_cv'].mean():.3f}")
print(f"   中位CV: {df['roic_cv'].median():.3f}")
print(f"   最小CV: {df['roic_cv'].min():.3f}")
print(f"   最大CV: {df['roic_cv'].max():.3f}")

stable = (df['roic_cv'] < 0.15).sum()
moderate = ((df['roic_cv'] >= 0.15) & (df['roic_cv'] < 0.30)).sum()
volatile = (df['roic_cv'] >= 0.30).sum()

print(f"\n📊 波动率分级:")
print(f"   CV<0.15 (稳定): {stable} 家 ({stable/len(df)*100:.1f}%)")
print(f"   0.15≤CV<0.30 (中等): {moderate} 家 ({moderate/len(df)*100:.1f}%)")
print(f"   CV≥0.30 (波动): {volatile} 家 ({volatile/len(df)*100:.1f}%)")

# 分析低R²企业
low_r2 = df[df['roic_r_squared'] < 0.4]
print(f"\n🎯 低R²企业分析 (R²<0.4): {len(low_r2)} 家")
if len(low_r2) > 0:
    low_r2_stable = low_r2[low_r2['roic_cv'] < 0.15]
    low_r2_volatile = low_r2[low_r2['roic_cv'] > 0.30]
    print(f"   其中稳定型 (CV<0.15): {len(low_r2_stable)} 家")
    print(f"   其中波动型 (CV>0.30): {len(low_r2_volatile)} 家")
    print(f"\n   典型稳定企业 (低R²+低CV):")
    if len(low_r2_stable) > 0:
        for _, row in low_r2_stable.head(5).iterrows():
            print(f"      {row['name']:8s} | R²={row['roic_r_squared']:.3f} CV={row['roic_cv']:.3f} 最新={row['roic_latest']:.1f}%")

# 趋势分析
print(f"\n📉 趋势特征:")
positive_slope = (df['roic_log_slope'] > 0).sum()
negative_slope = (df['roic_log_slope'] < 0).sum()
print(f"   上升趋势: {positive_slope} 家 ({positive_slope/len(df)*100:.1f}%)")
print(f"   下降趋势: {negative_slope} 家 ({negative_slope/len(df)*100:.1f}%)")

print("\n" + "=" * 80)
print("✅ P0修复验证完成")
print("=" * 80)
