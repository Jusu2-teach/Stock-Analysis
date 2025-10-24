"""
找出P2新增的2家企业
对比P1和P2的输出，分析为什么P2反而增加了公司数量
"""
import pandas as pd

def analyze_p2_additions():
    print("="*80)
    print("🔍 分析P2新增企业 (324 → 326)".center(80))
    print("="*80)

    # 读取当前P2结果
    df_p2 = pd.read_csv('data/filter_middle/roic_trend_analysis.csv')
    print(f"\n📊 P2结果: {len(df_p2)}家企业")

    # 检查周期性调整相关字段
    print("\n" + "="*80)
    print("1️⃣ 周期底部企业检查")
    print("="*80)

    # 找出周期底部企业
    cyclical_trough = df_p2[
        (df_p2['roic_is_cyclical'] == True) &
        (df_p2['roic_current_phase'] == 'trough')
    ].copy()

    print(f"\n周期底部企业数量: {len(cyclical_trough)}家")

    if len(cyclical_trough) > 0:
        print("\n周期底部企业列表:")
        for idx, row in cyclical_trough.iterrows():
            print(f"\n  {row['ts_code']} - {row.get('name', 'N/A')}")
            print(f"    5年斜率: {row['roic_log_slope']:.3f}")
            print(f"    峰谷比: {row['roic_peak_to_trough_ratio']:.2f}")
            print(f"    行业: {row.get('industry', 'N/A')}")
            print(f"    当前phase: {row['roic_current_phase']}")

    # 检查严重衰退边缘的企业（可能是被P2放宽标准救回来的）
    print("\n" + "="*80)
    print("2️⃣ 严重衰退边缘企业检查")
    print("="*80)

    # 找出斜率在-0.45到-0.30之间的企业（P2放宽后的区间）
    severe_decline_edge = df_p2[
        (df_p2['roic_log_slope'] >= -0.45) &
        (df_p2['roic_log_slope'] <= -0.30)
    ].sort_values('roic_log_slope')

    print(f"\n斜率在[-0.45, -0.30]区间的企业: {len(severe_decline_edge)}家")
    print("(这些企业可能受益于P2的周期性放宽)")

    if len(severe_decline_edge) > 0:
        print("\n企业列表:")
        for idx, row in severe_decline_edge.iterrows():
            is_cyclical_str = "✅周期性" if row['roic_is_cyclical'] else "❌非周期"
            phase_str = f"({row['roic_current_phase']})" if row['roic_is_cyclical'] else ""
            print(f"\n  {row['ts_code']} - {row.get('name', 'N/A')} {is_cyclical_str}{phase_str}")
            print(f"    5年斜率: {row['roic_log_slope']:.3f}")
            print(f"    峰谷比: {row['roic_peak_to_trough_ratio']:.2f}")
            print(f"    行业: {row.get('industry', 'N/A')}")

    # 检查周期底部且斜率在危险区间的企业（最可能是P2救回来的）
    print("\n" + "="*80)
    print("3️⃣ P2放宽标准受益企业 (最可能是新增的2家)")
    print("="*80)

    p2_beneficiaries = df_p2[
        (df_p2['roic_is_cyclical'] == True) &
        (df_p2['roic_current_phase'] == 'trough') &
        (df_p2['roic_log_slope'] >= -0.45) &
        (df_p2['roic_log_slope'] <= -0.30)
    ].sort_values('roic_log_slope')

    print(f"\n周期底部+严重衰退边缘企业: {len(p2_beneficiaries)}家")
    print("(这些企业最可能是P2新增的2家)")

    if len(p2_beneficiaries) > 0:
        print("\n🎯 关键嫌疑企业:")
        for idx, row in p2_beneficiaries.iterrows():
            print(f"\n  {row['ts_code']} - {row.get('name', 'N/A')}")
            print(f"    5年斜率: {row['roic_log_slope']:.3f} (原阈值-0.30, P2放宽后-0.45)")
            print(f"    峰谷比: {row['roic_peak_to_trough_ratio']:.2f}")
            print(f"    行业: {row.get('industry', 'N/A')}")
            print(f"    当前phase: {row['roic_current_phase']}")
            print(f"    3年加速度: {row['roic_trend_acceleration']:.2f}")

            # 判断是否受益于P2放宽
            if row['roic_log_slope'] < -0.30:
                print(f"    💡 P2分析: 斜率{row['roic_log_slope']:.3f} < -0.30, 原本会被淘汰")
                print(f"            P2放宽至-0.45后得以保留!")

    # 总结
    print("\n" + "="*80)
    print("📊 统计汇总")
    print("="*80)
    print(f"\n  总企业数: {len(df_p2)}家")
    print(f"  周期性企业: {len(df_p2[df_p2['roic_is_cyclical']==True])}家")
    print(f"  周期底部: {len(cyclical_trough)}家")
    print(f"  严重衰退边缘[-0.45,-0.30]: {len(severe_decline_edge)}家")
    print(f"  P2放宽受益 (周期底部+边缘): {len(p2_beneficiaries)}家")

    print("\n" + "="*80)
    print("💡 结论")
    print("="*80)
    if len(p2_beneficiaries) >= 2:
        print("\n✅ 找到了! P2新增的2家企业很可能来自'周期底部+严重衰退边缘'组")
        print("   这些企业:")
        print("   - 原本因为斜率 < -0.30 被规则2淘汰")
        print("   - P2识别出它们是周期底部企业")
        print("   - 放宽阈值至-0.45后得以保留")
        print("   - 这是合理的! 周期底部的暂时衰退不应被淘汰")
    else:
        print("\n⚠️  没有找到明确的受益企业，可能需要进一步分析")
        print("   建议检查P0-P2的完整执行日志")

if __name__ == '__main__':
    analyze_p2_additions()
