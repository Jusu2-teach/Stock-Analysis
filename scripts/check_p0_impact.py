"""检查P0修复的实际影响"""
import pandas as pd

df = pd.read_csv('data/filter_middle/roic_trend_analysis.csv')

print("=" * 80)
print("🔍 P0修复影响分析")
print("=" * 80)

# 关键问题：P0修复应该影响哪些企业？
# 规则2的三种情况：
# A. 低R² + 低CV → 宽松判断
# B. 低R² + 高CV → 严格判断
# C. 高R² → 原逻辑

print("\n📊 当前筛选结果: 324家")

# 分析低R²企业
low_r2 = df[df['roic_r_squared'] < 0.4]
print(f"\n🎯 低R²企业 (R²<0.4): {len(low_r2)} 家")

# 情况A: 低R² + 低CV (应该被豁免保留)
low_r2_low_cv = low_r2[low_r2['roic_cv'] < 0.15]
print(f"\n   情况A (低R²+低CV<0.15): {len(low_r2_low_cv)} 家")
print(f"   ✅ 这些企业应该被豁免保留 (稳定优质)")
if len(low_r2_low_cv) > 0:
    print(f"\n   典型案例:")
    for _, row in low_r2_low_cv.head(5).iterrows():
        print(f"      {row['name']:10s} | R²={row['roic_r_squared']:.3f} CV={row['roic_cv']:.3f} "
              f"Slope={row['roic_log_slope']:+.3f} 最新={row['roic_latest']:.1f}%")

# 情况B: 低R² + 高CV (应该被严格过滤)
low_r2_high_cv = low_r2[low_r2['roic_cv'] > 0.30]
print(f"\n   情况B (低R²+高CV>0.30): {len(low_r2_high_cv)} 家")
print(f"   ⚠️ 这些企业应该被严格判断")
if len(low_r2_high_cv) > 0:
    print(f"\n   典型案例:")
    for _, row in low_r2_high_cv.head(5).iterrows():
        print(f"      {row['name']:10s} | R²={row['roic_r_squared']:.3f} CV={row['roic_cv']:.3f} "
              f"Slope={row['roic_log_slope']:+.3f} 最新={row['roic_latest']:.1f}%")

    # 检查这些高波动企业是否满足"最新值>1.3倍底线"的要求
    print(f"\n   检查: 这些高波动企业的最新值情况")
    min_threshold = 10.0  # 假设底线是10%
    qualified = low_r2_high_cv[low_r2_high_cv['roic_latest'] >= min_threshold * 1.3]
    print(f"   最新值≥{min_threshold * 1.3:.1f}%: {len(qualified)} 家 (通过)")
    print(f"   最新值<{min_threshold * 1.3:.1f}%: {len(low_r2_high_cv) - len(qualified)} 家 (应被淘汰)")

# 高R²企业
high_r2 = df[df['roic_r_squared'] >= 0.4]
print(f"\n🎯 高R²企业 (R²≥0.4): {len(high_r2)} 家")
print(f"   ✅ 使用原有严重衰退逻辑")

print("\n" + "=" * 80)
print("💡 关键洞察:")
print("=" * 80)
print(f"如果P0修复前后公司数量没变化，可能原因:")
print(f"1. 修复前的代码就已经包含类似逻辑")
print(f"2. 数据集中刚好没有触发新规则的边界案例")
print(f"3. 你在我修改前已经手动调整过代码")
print(f"\n建议: 查看git diff对比代码变化")
print("=" * 80)
