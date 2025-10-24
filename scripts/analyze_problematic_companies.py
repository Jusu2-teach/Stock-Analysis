"""
分析有问题的公司
这些公司尽管ROIC恶化严重，但仍然通过了筛选
"""
import pandas as pd
import sys
from pathlib import Path

# 添加src到路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

def analyze_problematic_companies():
    """分析有问题的公司"""

    # 读取数据
    df = pd.read_csv('data/filter_middle/roic_trend_analysis.csv')

    # 定义问题公司列表
    problematic_companies = {
        '长春高新': '000661.SZ',
        '海康威视': '002415.SZ',
        '立讯精密': '002475.SZ',
        '迈为股份': '300751.SZ',
        '绿联科技': '301606.SZ',
        '屹通新材': '300930.SZ',
        '东方钽业': '000962.SZ',
        '圣邦股份': '300661.SZ',
        '华特达因': '000915.SZ',
        '江顺科技': '001400.SZ',
        '晶盛机电': '300316.SZ',
        '东方电缆': '603606.SH'
    }

    print("=" * 100)
    print("问题公司详细分析".center(100))
    print("=" * 100)
    print()

    # 按严重程度分类
    severe_cases = []  # 严重恶化
    moderate_cases = []  # 中度恶化
    growth_to_decline = []  # 增长到衰退拐点

    for name, code in problematic_companies.items():
        company = df[df['ts_code'] == code]

        if company.empty:
            print(f"⚠️ 未找到 {name} ({code})")
            continue

        company = company.iloc[0]

        # 分类
        if company['roic_deterioration_severity'] == 'severe':
            severe_cases.append((name, code, company))
        elif company['roic_inflection_type'] == 'growth_to_decline':
            growth_to_decline.append((name, code, company))
        else:
            moderate_cases.append((name, code, company))

    # 输出分析
    print("\n" + "=" * 100)
    print("【类别1】严重恶化案例 (deterioration_severity = severe)".center(100))
    print("=" * 100)

    for name, code, company in severe_cases:
        print(f"\n{'─' * 100}")
        print(f"公司: {name} ({code})")
        print(f"行业: {company['industry']}")
        print(f"{'─' * 100}")

        print(f"\n💰 ROIC指标:")
        print(f"  加权平均ROIC: {company['roic_weighted']:.2f}%")
        print(f"  最新ROIC: {company['roic_latest']:.2f}%")
        print(f"  5年斜率: {company['roic_log_slope']:.4f}")
        print(f"  总跌幅: {company['roic_total_decline_pct']:.2f}%")

        print(f"\n📊 恶化情况:")
        print(f"  恶化严重度: {company['roic_deterioration_severity']}")
        print(f"  Year4->5变化: {company['roic_year4_to_5_change']:.2f}%")
        print(f"  Year3->4变化: {company['roic_year3_to_4_change']:.2f}%")

        print(f"\n📉 趋势加速度 (P2):")
        print(f"  3年斜率: {company['roic_recent_3y_slope']:.4f}")
        print(f"  趋势加速度: {company['roic_trend_acceleration']:.4f}")
        print(f"  加速上升: {company['roic_is_accelerating']}")
        print(f"  加速下降: {company['roic_is_decelerating']}")

        print(f"\n⚠️ 罚分:")
        print(f"  总罚分: {company['roic_penalty']:.2f}")

        # 判断为什么通过
        reasons = []
        warnings = []

        if company['roic_latest'] >= 8.0:
            reasons.append(f"✅ 最新ROIC ({company['roic_latest']:.2f}%) >= 8.0%")
        else:
            warnings.append(f"❌ 最新ROIC ({company['roic_latest']:.2f}%) < 8.0%")

        if company['roic_log_slope'] >= -0.30:
            reasons.append(f"✅ 5年斜率 ({company['roic_log_slope']:.4f}) >= -0.30")
        else:
            warnings.append(f"❌ 5年斜率 ({company['roic_log_slope']:.4f}) < -0.30")

        if company['roic_penalty'] < 20:
            reasons.append(f"✅ 罚分 ({company['roic_penalty']:.2f}) < 20")
        else:
            warnings.append(f"❌ 罚分 ({company['roic_penalty']:.2f}) >= 20")

        print(f"\n🔍 通过筛选的原因:")
        for reason in reasons:
            print(f"  {reason}")

        if warnings:
            print(f"\n⚠️ 警告信号:")
            for warning in warnings:
                print(f"  {warning}")

        # 计算最新ROIC占加权平均的比例
        ratio = (company['roic_latest'] / company['roic_weighted']) * 100
        print(f"\n💡 关键问题:")
        print(f"  最新ROIC仅为加权平均的 {ratio:.1f}%")
        print(f"  实际下降了 {company['roic_total_decline_pct']:.1f}%，但因为:")
        print(f"    1. Log变换掩盖了真实跌幅")
        print(f"    2. 罚分 {company['roic_penalty']:.2f} < 20 阈值")
        if not company['roic_is_decelerating']:
            print(f"    3. P2加速度 {company['roic_trend_acceleration']:.2f} > -2.0，未被标记为加速下降")
        print(f"  ⚠️ 这是一个典型的应该被排除的案例!")

    print("\n\n" + "=" * 100)
    print("【类别2】增长到衰退拐点 (inflection_type = growth_to_decline)".center(100))
    print("=" * 100)

    for name, code, company in growth_to_decline:
        print(f"\n{'─' * 100}")
        print(f"公司: {name} ({code})")
        print(f"行业: {company['industry']}")
        print(f"{'─' * 100}")

        print(f"\n💰 ROIC指标:")
        print(f"  加权平均ROIC: {company['roic_weighted']:.2f}%")
        print(f"  最新ROIC: {company['roic_latest']:.2f}%")
        print(f"  5年斜率: {company['roic_log_slope']:.4f}")

        print(f"\n🔄 拐点情况:")
        print(f"  拐点类型: {company['roic_inflection_type']}")
        print(f"  早期斜率: {company['roic_early_slope']:.2f}%/年")
        print(f"  近期斜率: {company['roic_recent_slope']:.2f}%/年")
        print(f"  斜率变化: {company['roic_slope_change']:.2f}%/年")
        print(f"  拐点置信度: {company['roic_inflection_confidence']:.2f}")

        print(f"\n📊 恶化情况:")
        print(f"  有恶化: {company['roic_has_deterioration']}")
        print(f"  恶化严重度: {company['roic_deterioration_severity']}")
        if company['roic_has_deterioration']:
            print(f"  总跌幅: {company['roic_total_decline_pct']:.2f}%")

        print(f"\n📉 趋势加速度 (P2):")
        print(f"  3年斜率: {company['roic_recent_3y_slope']:.4f}")
        print(f"  趋势加速度: {company['roic_trend_acceleration']:.4f}")
        print(f"  加速下降: {company['roic_is_decelerating']}")

        print(f"\n⚠️ 罚分:")
        print(f"  总罚分: {company['roic_penalty']:.2f}")

        ratio = (company['roic_latest'] / company['roic_weighted']) * 100
        print(f"\n💡 关键问题:")
        print(f"  曾经是高增长公司 (早期斜率 {company['roic_early_slope']:.2f}%/年)")
        print(f"  现在已转为衰退 (近期斜率 {company['roic_recent_slope']:.2f}%/年)")
        print(f"  最新ROIC仅为加权平均的 {ratio:.1f}%")
        if company['roic_penalty'] >= 15:
            print(f"  ⚠️ 罚分 {company['roic_penalty']:.2f} 已经很高!")
        print(f"  ⚠️ 增长到衰退的拐点公司应该慎重!")

    print("\n\n" + "=" * 100)
    print("【类别3】中度恶化案例 (其他)".center(100))
    print("=" * 100)

    for name, code, company in moderate_cases:
        print(f"\n{'─' * 100}")
        print(f"公司: {name} ({code})")
        print(f"行业: {company['industry']}")
        print(f"{'─' * 100}")

        print(f"\n💰 ROIC指标:")
        print(f"  加权平均ROIC: {company['roic_weighted']:.2f}%")
        print(f"  最新ROIC: {company['roic_latest']:.2f}%")
        print(f"  5年斜率: {company['roic_log_slope']:.4f}")

        print(f"\n📊 恶化情况:")
        print(f"  有恶化: {company['roic_has_deterioration']}")
        print(f"  恶化严重度: {company['roic_deterioration_severity']}")
        if company['roic_has_deterioration']:
            print(f"  总跌幅: {company['roic_total_decline_pct']:.2f}%")

        print(f"\n⚠️ 罚分:")
        print(f"  总罚分: {company['roic_penalty']:.2f}")

        # 检查是否是周期性行业
        if company['roic_is_cyclical']:
            print(f"\n🔄 周期性:")
            print(f"  是周期性: {company['roic_is_cyclical']}")
            print(f"  当前阶段: {company['roic_current_phase']}")
            print(f"  行业周期性: {company['roic_industry_cyclical']}")
            print(f"  💡 周期性公司在谷底可能获得豁免")

    # 统计总结
    print("\n\n" + "=" * 100)
    print("统计总结".center(100))
    print("=" * 100)

    print(f"\n问题公司总数: {len(problematic_companies)}")
    print(f"  - 严重恶化 (severe): {len(severe_cases)} 家")
    print(f"  - 增长到衰退拐点: {len(growth_to_decline)} 家")
    print(f"  - 中度问题: {len(moderate_cases)} 家")

    print(f"\n罚分统计:")
    all_companies = severe_cases + growth_to_decline + moderate_cases
    penalties = [c[2]['roic_penalty'] for c in all_companies]
    print(f"  平均罚分: {sum(penalties) / len(penalties):.2f}")
    print(f"  最高罚分: {max(penalties):.2f}")
    print(f"  最低罚分: {min(penalties):.2f}")
    print(f"  罚分 >= 15: {sum(1 for p in penalties if p >= 15)} 家")
    print(f"  罚分 >= 10: {sum(1 for p in penalties if p >= 10)} 家")

    print(f"\n主要问题:")
    print(f"  1. 严重恶化案例 ({len(severe_cases)} 家) 应该被一票否决")
    print(f"  2. 增长到衰退拐点 ({len(growth_to_decline)} 家) 风险极高")
    print(f"  3. 罚分阈值20太高，建议降低到15")
    print(f"  4. P2加速度阈值±2.0太宽松，建议±1.0")
    print(f"  5. 缺少绝对值检查: 最新ROIC < 加权平均 × 0.7")

    print("\n" + "=" * 100)


if __name__ == '__main__':
    analyze_problematic_companies()
