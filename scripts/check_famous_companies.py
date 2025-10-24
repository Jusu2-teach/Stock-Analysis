import pandas as pd

print('='*80)
print('知名企业筛选分析')
print('='*80)

# 读取原始数据
df_full = pd.read_csv('data/polars/5yd_final_industry.csv')
df_final = pd.read_csv('data/final_filter_results.csv')

# 知名企业列表
famous_companies = [
    '比亚迪', '宁德时代', '贵州茅台', '格力电器', '美的集团',
    '五粮液', '海康威视', '中国平安', '招商银行', '长江电力',
    '隆基绿能', '药明康德', '迈瑞医疗', '温氏股份', '牧原股份',
    '通威股份', '中兴通讯', '三一重工', '恒力石化', '万科A'
]

print(f'\n原始数据: {len(df_full)} 行, {df_full["ts_code"].nunique()} 家企业')
print(f'最终筛选: {len(df_final)} 家企业')

print('\n' + '='*80)
print('检查知名企业情况')
print('='*80)

found_in_original = []
found_in_final = []
not_found = []

for company in famous_companies:
    in_original = df_full[df_full['name'].str.contains(company, na=False)]
    in_final = df_final[df_final['name'].str.contains(company, na=False)]

    if len(in_original) > 0:
        found_in_original.append(company)
        if len(in_final) > 0:
            found_in_final.append(company)
    else:
        not_found.append(company)

print(f'\n✅ 在原始数据中找到: {len(found_in_original)} 家')
print(f'✅ 通过最终筛选: {len(found_in_final)} 家')
print(f'❌ 未通过筛选: {len(found_in_original) - len(found_in_final)} 家')
print(f'⚠️  原始数据中就没有: {len(not_found)} 家')

if found_in_final:
    print(f'\n【通过筛选的知名企业】')
    for company in found_in_final:
        company_data = df_final[df_final['name'].str.contains(company, na=False)].iloc[0]
        print(f'  ✓ {company:<12} - 排名: {int(company_data["quality_rank"]):<4} 得分: {company_data["quality_score"]:.1f}')

print(f'\n【未通过筛选的知名企业 - 详细分析】')
print('-'*100)

for company in found_in_original:
    if company not in found_in_final:
        # 获取该企业的数据
        company_data = df_full[df_full['name'].str.contains(company, na=False)].iloc[0]

        print(f'\n❌ {company} ({company_data["ts_code"]})')
        print(f'   行业: {company_data["industry"]}')

        # 检查关键指标
        roic = company_data.get('roic', 0)
        roe = company_data.get('roe', 0)
        or_yoy = company_data.get('or_yoy', 0)
        grossprofit_margin = company_data.get('grossprofit_margin', 0)
        debt_to_assets = company_data.get('debt_to_assets', 0)
        current_ratio = company_data.get('current_ratio', 0)

        # 计算5年平均值（如果有的话）
        roic_5y = company_data.get('5yd_ts_code_roic_avg', roic) if pd.notna(company_data.get('5yd_ts_code_roic_avg')) else roic
        margin_5y = company_data.get('5yd_ts_code_grossprofit_margin_avg', grossprofit_margin) if pd.notna(company_data.get('5yd_ts_code_grossprofit_margin_avg')) else grossprofit_margin
        growth_5y = company_data.get('5yd_ts_code_or_yoy_avg', or_yoy) if pd.notna(company_data.get('5yd_ts_code_or_yoy_avg')) else or_yoy

        print(f'   关键指标:')
        print(f'     ROIC (5年avg): {roic_5y:.2f}%')
        print(f'     毛利率 (5年avg): {margin_5y:.2f}%')
        print(f'     增长率 (5年avg): {growth_5y:.2f}%')
        print(f'     负债率: {debt_to_assets:.2f}%')
        print(f'     流动比率: {current_ratio:.2f}')

        # 分析未通过的原因
        print(f'   未通过原因:')
        reasons = []

        # Layer 0 检查
        if margin_5y <= 10:
            reasons.append(f'毛利率 {margin_5y:.1f}% <= 10% (Layer 0 不通过)')
        if debt_to_assets > 70:
            reasons.append(f'负债率 {debt_to_assets:.1f}% > 70% (Layer 0 不通过)')
        if current_ratio < 1.0:
            reasons.append(f'流动比率 {current_ratio:.2f} < 1.0 (Layer 0 不通过)')

        # Layer 1 检查
        if roic_5y <= 6:
            reasons.append(f'ROIC {roic_5y:.1f}% <= 6% (Layer 1 不通过)')

        # 检查 OCF/EPS
        ocfps = company_data.get('ocfps', 0)
        eps = company_data.get('eps', 0)
        if eps != 0:
            ocf_eps_ratio = ocfps / eps
            if ocf_eps_ratio < 0.8 and not (growth_5y > 15 and ocf_eps_ratio >= 0.5):
                reasons.append(f'OCF/EPS {ocf_eps_ratio:.2f} < 0.8 (Layer 1 不通过)')

        if not reasons:
            reasons.append('可能在商业模式分类或其他层级被淘汰')

        for reason in reasons:
            print(f'     • {reason}')

print('\n' + '='*80)
print('核心发现')
print('='*80)
print('\n我们的筛选体系是"价值投资"导向,关注:')
print('  ✓ 高ROIC (资本效率)')
print('  ✓ 高毛利率 (定价权)')
print('  ✓ 低负债率 (财务安全)')
print('  ✓ 强现金流 (盈利质量)')
print('\n很多知名企业虽然"规模大"、"营收高",但可能:')
print('  ❌ ROIC不够高 (低于6%)')
print('  ❌ 毛利率低 (制造业特征)')
print('  ❌ 负债率高 (扩张期)')
print('  ❌ 现金流质量差')
print('\n结论: "知名" ≠ "优秀" (从财务质量角度)')
print('='*80)
