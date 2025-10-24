"""
分析长春高新的详细数据
"""
import pandas as pd

df = pd.read_csv('data/filter_middle/roic_trend_analysis.csv')
ccgx = df[df['name'].str.contains('长春高新', na=False)].iloc[0]

print("="*80)
print("长春高新 完整数据分析".center(80))
print("="*80)

print("\n基础信息:")
print(f"  代码: {ccgx['ts_code']}")
print(f"  名称: {ccgx['name']}")
print(f"  行业: {ccgx['industry']}")

print("\n关键指标:")
print(f"  5年斜率: {ccgx['roic_log_slope']:.3f}")
print(f"  R²: {ccgx['roic_r_squared']:.3f}")
print(f"  P值: {ccgx['roic_p_value']:.4f}")
print(f"  最新ROIC: {ccgx['roic_latest']:.2f}%")
print(f"  加权ROIC: {ccgx['roic_weighted']:.2f}%")

print("\nP0 波动率:")
print(f"  变异系数(CV): {ccgx['roic_cv']:.3f}")
print(f"  标准差: {ccgx['roic_std_dev']:.3f}")
print(f"  波动类型: {ccgx['roic_volatility_type']}")

print("\nP1 拐点:")
print(f"  有拐点: {ccgx['roic_has_inflection']}")
print(f"  拐点类型: {ccgx['roic_inflection_type']}")
print(f"  前期斜率: {ccgx['roic_early_slope']:.3f}")
print(f"  后期斜率: {ccgx['roic_recent_slope']:.3f}")
print(f"  斜率变化: {ccgx['roic_slope_change']:.3f}")
print(f"  拐点置信度: {ccgx['roic_inflection_confidence']:.3f}")

print("\nP1 恶化:")
print(f"  有恶化: {ccgx['roic_has_deterioration']}")
print(f"  恶化严重度: {ccgx['roic_deterioration_severity']}")
print(f"  4→5年变化: {ccgx['roic_year4_to_5_change']:.2f}%")
print(f"  3→4年变化: {ccgx['roic_year3_to_4_change']:.2f}%")
print(f"  总跌幅: {ccgx['roic_total_decline_pct']:.2f}%")

print("\nP2 周期性:")
print(f"  周期性: {ccgx['roic_is_cyclical']}")
print(f"  峰谷比: {ccgx['roic_peak_to_trough_ratio']:.2f}")
print(f"  有中间峰: {ccgx['roic_has_middle_peak']}")
print(f"  当前阶段: {ccgx['roic_current_phase']}")
print(f"  行业周期性: {ccgx['roic_industry_cyclical']}")

print("\nP2 加速度:")
print(f"  3年斜率: {ccgx['roic_recent_3y_slope']:.3f}")
print(f"  3年R²: {ccgx['roic_recent_3y_r_squared']:.3f}")
print(f"  趋势加速度: {ccgx['roic_trend_acceleration']:.3f}")
print(f"  加速上升: {ccgx['roic_is_accelerating']}")
print(f"  加速下降: {ccgx['roic_is_decelerating']}")

print("\n筛选结果:")
print(f"  罚分: {ccgx['roic_penalty']:.1f}")

print("\n" + "="*80)
print("💡 被选中原因分析")
print("="*80)

reasons = []
warnings = []

# 分析通过原因
if ccgx['roic_latest'] >= 8.0:
    reasons.append(f"✅ 最新ROIC ({ccgx['roic_latest']:.2f}%) >= 8.0%")
else:
    warnings.append(f"❌ 最新ROIC ({ccgx['roic_latest']:.2f}%) < 8.0%")

if ccgx['roic_log_slope'] >= -0.30:
    reasons.append(f"✅ 5年斜率 ({ccgx['roic_log_slope']:.3f}) >= -0.30 (未严重衰退)")
else:
    warnings.append(f"❌ 5年斜率 ({ccgx['roic_log_slope']:.3f}) < -0.30 (严重衰退)")

if ccgx['roic_penalty'] < 20:
    reasons.append(f"✅ 总罚分 ({ccgx['roic_penalty']:.1f}) < 20")
else:
    warnings.append(f"❌ 总罚分 ({ccgx['roic_penalty']:.1f}) >= 20")

# P1恶化分析
if ccgx['roic_has_deterioration']:
    warnings.append(f"⚠️  检测到近期恶化 (严重度: {ccgx['roic_deterioration_severity']})")

# P1拐点分析
if ccgx['roic_has_inflection']:
    if 'to_worse' in str(ccgx['roic_inflection_type']):
        warnings.append(f"⚠️  拐点恶化: {ccgx['roic_inflection_type']}")
    else:
        reasons.append(f"✅ 拐点好转: {ccgx['roic_inflection_type']}")

# P2加速度分析
if ccgx['roic_is_decelerating']:
    warnings.append(f"⚠️  趋势加速下降 (加速度: {ccgx['roic_trend_acceleration']:.2f})")

print("\n通过筛选的原因:")
for r in reasons:
    print(f"  {r}")

print("\n警示信号:")
for w in warnings:
    print(f"  {w}")

print("\n" + "="*80)
print("🔍 深度分析")
print("="*80)

print("\n问题: 为什么这样的企业会被选中?")
print("\n答案:")
print(f"  1. 虽然有{len(warnings)}个警示信号，但满足了基本准入条件:")
print(f"     - 最新ROIC ({ccgx['roic_latest']:.2f}%) 高于8%阈值")
print(f"     - 5年斜率 ({ccgx['roic_log_slope']:.3f}) 虽然负向，但未达到严重衰退阈值(-0.30)")
print(f"     - 总罚分 ({ccgx['roic_penalty']:.1f}) 低于淘汰线(20分)")
print(f"\n  2. 当前筛选逻辑:")
print(f"     - 基于'罚分制'而非'一票否决制'")
print(f"     - 只要总罚分<20就能通过，即使有多个负面信号")
print(f"     - 恶化、拐点恶化、加速下降会增加罚分，但未超过阈值")

print("\n💡 建议改进方向:")
print("  1. 增加'一票否决'规则:")
print("     - 如果同时满足: 拐点恶化 + 近期恶化 + 加速下降 → 直接淘汰")
print("     - 如果恶化严重度 > 某阈值 → 直接淘汰")
print("  2. 降低罚分阈值:")
print("     - 从20分降低到15分，更严格筛选")
print("  3. 增加趋势方向过滤:")
print("     - 要求5年斜率必须 > 0 (只保留增长企业)")
print("  4. 增加最新值要求:")
print("     - 要求最新ROIC > 加权ROIC (当前状态好于平均)")

print("\n="*80)
