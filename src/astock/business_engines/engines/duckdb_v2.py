"""
DuckDB筛选引擎 v2.0 Enhanced
结合v2.0的有效性和v3.0的优秀设计

核心改进：
1. 引入财务安全底线筛选（一票否决）
2. 升级商业模式分类（5类，多维度）
3. 引入行业相对值评分
4. 实现差异化权重调整
5. 保留v2的三层架构和例外规则
"""

import pandas as pd
import logging
import sys
from pathlib import Path
from typing import Union, Dict, List

# orchestrator 已移至根目录
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent.parent))
from orchestrator.decorators.register import register_method

from .industry_config import (
    UNIVERSAL_RULES_ENHANCED,
    BUSINESS_MODEL_CLASSIFICATION_ENHANCED,
    EXCEPTION_RULES_ENHANCED,
    classify_business_model_enhanced,
    get_company_config_enhanced,
    calculate_quality_score_enhanced,
)

logger = logging.getLogger(__name__)


@register_method(
    engine_name="filter_second_stage_v2",
    component_type="business_engine",
    engine_type="duckdb_v2",
    description="v2.0 专业投资筛选体系 - 4层架构+5类商业模式+差异化评分"
)
def filter_second_stage_v2(
    first_stage_data: Union[str, Path, pd.DataFrame],
    full_data: Union[str, Path, pd.DataFrame] = None,
    latest_end_date: str = "20241231",
    end_date_col: str = "end_date",
) -> pd.DataFrame:
    """
    v2.0 专业投资筛选体系

    四层架构：
    0. 财务安全底线（一票否决）- 新增
    1. 通用铁律（质量下限）
    2. 商业模式分类（5类：规模/技术/品牌/服务/周期）- 升级
    3. 差异化筛选标准 + 例外规则

    参数:
        first_stage_data: 第一次筛选结果（包含5年平均值）
        full_data: 完整的5年数据（用于获取最新一期数据，可选）
        latest_end_date: 最新一期的日期（如：20241231）
        end_date_col: 日期列名（默认：end_date）

    返回:
        最终筛选结果的DataFrame
    """
    # ========== 数据加载 ==========
    if isinstance(first_stage_data, (str, Path)):
        df_first = pd.read_csv(first_stage_data)
    else:
        df_first = first_stage_data.copy() if isinstance(first_stage_data, pd.DataFrame) else first_stage_data

    logger.info("=" * 80)
    logger.info("🚀 专业投资筛选体系 v2.0 Enhanced 启动")
    logger.info("=" * 80)
    logger.info(f"输入企业数: {len(df_first)}")
    logger.info("")
    logger.info("改进点:")
    logger.info("  ✅ 财务安全底线筛选（一票否决）")
    logger.info("  ✅ 商业模式分类升级（5类）")
    logger.info("  ✅ 行业相对值评分")
    logger.info("  ✅ 差异化权重调整")

    # ========== Merge最新期数据 ==========
    # first_stage_data只有聚合列（5yd_ts_code_*），需要merge原始数据获取最新期指标
    if full_data is not None:
        logger.info("\n📊 合并最新期数据...")

        # 加载完整数据
        if isinstance(full_data, (str, Path)):
            df_full = pd.read_csv(full_data)
        else:
            df_full = full_data.copy() if isinstance(full_data, pd.DataFrame) else full_data

        # 获取最新期数据（按end_date排序）
        if end_date_col in df_full.columns:
            df_full[end_date_col] = pd.to_datetime(df_full[end_date_col], format='%Y%m%d', errors='coerce')
            # 每个ts_code取最新一期
            df_latest = df_full.sort_values(end_date_col).groupby('ts_code', as_index=False).last()
            logger.info(f"   提取最新期数据: {len(df_latest)}行")
        else:
            # 如果没有日期列，直接取每个ts_code的最后一条
            df_latest = df_full.groupby('ts_code', as_index=False).last()
            logger.info(f"   提取最新期数据(无日期列): {len(df_latest)}行")

        # 选择需要的最新期列
        latest_cols = ['ts_code', 'name', 'industry', 'roic', 'roe', 'roa', 'debt_to_assets',
                       'current_ratio', 'ocfps', 'eps', 'grossprofit_margin', 'or_yoy',
                       'fixed_assets_ratio', 'total_revenue', 'rd_exp_ratio']
        latest_cols_available = [c for c in latest_cols if c in df_latest.columns]
        df_latest_subset = df_latest[latest_cols_available].copy()

        # Merge到first_stage_data
        df_merged = df_first.merge(df_latest_subset, on='ts_code', how='left', suffixes=('', '_latest'))

        # 处理重复列（如果有）
        for col in ['name', 'industry']:
            if f'{col}_latest' in df_merged.columns:
                df_merged[col] = df_merged[f'{col}_latest'].fillna(df_merged.get(col, ''))
                df_merged.drop(columns=[f'{col}_latest'], inplace=True)

        logger.info(f"   合并后数据: {len(df_merged)}行, {len(df_merged.columns)}列")
        logger.info(f"   ✅ 已添加最新期指标: {[c for c in latest_cols_available if c not in ['ts_code', 'name', 'industry']]}")
    else:
        logger.warning("   ⚠️  未提供full_data，将使用聚合数据估算最新期指标")
        df_merged = df_first.copy()

        # 如果没有最新期数据，用5年平均估算
        if 'roic' not in df_merged.columns and '5yd_ts_code_roic_avg' in df_merged.columns:
            df_merged['roic'] = df_merged['5yd_ts_code_roic_avg']
        if 'debt_to_assets' not in df_merged.columns:
            df_merged['debt_to_assets'] = 50.0  # 默认50%
        if 'current_ratio' not in df_merged.columns:
            df_merged['current_ratio'] = 1.5  # 默认1.5

    # 补充缺失的财务特征字段（用于商业模式分类）
    if 'fixed_assets_ratio' not in df_merged.columns:
        df_merged['fixed_assets_ratio'] = 30.0  # 默认值
    if 'total_revenue' not in df_merged.columns:
        df_merged['total_revenue'] = 50.0  # 默认50亿
    if 'roa' not in df_merged.columns:
        if '5yd_ts_code_roa_avg' in df_merged.columns:
            df_merged['roa'] = df_merged['5yd_ts_code_roa_avg']
        else:
            df_merged['roa'] = df_merged.get('roe', 10) * 0.6  # 估算
    if 'rd_exp_ratio' not in df_merged.columns:
        df_merged['rd_exp_ratio'] = 3.0  # 默认研发占比
    if 'ocfps_to_eps_ratio' not in df_merged.columns:
        # 计算OCF/EPS比率
        if 'ocfps' in df_merged.columns and 'eps' in df_merged.columns:
            df_merged['ocfps_to_eps_ratio'] = df_merged['ocfps'] / df_merged['eps'].replace(0, 1)
        elif '5yd_ts_code_ocfps_avg' in df_merged.columns and '5yd_ts_code_eps_avg' in df_merged.columns:
            ocf = df_merged['5yd_ts_code_ocfps_avg']
            eps = df_merged['5yd_ts_code_eps_avg']
            df_merged['ocfps_to_eps_ratio'] = ocf / eps.replace(0, 1)
        else:
            df_merged['ocfps_to_eps_ratio'] = 1.0  # 默认值

    current_df = df_merged.copy()
    logger.info(f"\n✅ 数据准备完成: {len(current_df)}行, 包含5年平均列+最新期列")

    # ========== 第0层：财务安全底线（新增）==========
    logger.info("")
    logger.info("=" * 80)
    logger.info("🛡️  第0层：财务安全底线（一票否决）")
    logger.info("=" * 80)

    safety_baseline = UNIVERSAL_RULES_ENHANCED['financial_safety_baseline']

    if safety_baseline['enabled']:
        before = len(current_df)

        # 底线1：毛利率>10%
        logger.info(f"\n底线1: 毛利率 > {safety_baseline['grossprofit_margin_min']}%")
        passed_margin = current_df['5yd_ts_code_grossprofit_margin_avg'] > safety_baseline['grossprofit_margin_min']
        failed = current_df[~passed_margin]
        if len(failed) > 0:
            logger.info(f"  ❌ 淘汰 {len(failed)} 家企业（无定价权）")
        current_df = current_df[passed_margin].copy()

        # 底线2：负债率检查（需要先分类）
        logger.info(f"\n底线2: 负债率检查（按商业模式差异化）")

        # 快速分类（仅用于底线检查）
        def get_business_type_for_safety(row):
            """快速判断商业模式（用于底线检查）"""
            industry = row.get('industry', '')
            margin = row.get('5yd_ts_code_grossprofit_margin_avg', 0)
            growth = row.get('5yd_ts_code_or_yoy_avg', 0)

            # 简化判断逻辑
            if margin > 60:
                return "品牌溢价型"
            elif growth > 20 and margin > 30:
                return "技术壁垒型"
            elif growth > 15 and margin > 30:
                return "轻资产服务型"
            elif any(ind in industry for ind in ["汽车", "电气", "机械", "元器件"]):
                return "规模效应型"
            else:
                return "周期资源型"

        # 使用向量化方式避免apply问题
        business_types = []
        for _, row in current_df.iterrows():
            business_types.append(get_business_type_for_safety(row))
        current_df['business_type_temp'] = business_types

        # 应用差异化负债率底线
        debt_passed = []
        for idx, row in current_df.iterrows():
            biz_type = row['business_type_temp']
            debt_max = safety_baseline['debt_to_assets_max'].get(biz_type, 65.0)
            actual_debt = row.get('debt_to_assets', 100)
            debt_passed.append(actual_debt <= debt_max)

        current_df['debt_check'] = debt_passed
        failed = current_df[~current_df['debt_check']]
        if len(failed) > 0:
            logger.info(f"  ❌ 淘汰 {len(failed)} 家企业（负债率过高）:")
            for _, row in failed.head(5).iterrows():
                logger.info(f"     • {row.get('name', row['ts_code'])} ({row['business_type_temp']}): "
                           f"负债率 {row.get('debt_to_assets', 0):.1f}%")

        current_df = current_df[current_df['debt_check']].copy()
        # 安全删除临时列
        cols_to_drop = [c for c in ['business_type_temp', 'debt_check'] if c in current_df.columns]
        if cols_to_drop:
            current_df = current_df.drop(columns=cols_to_drop)

        # 底线3：流动比率检查（按商业模式差异化）
        logger.info(f"\n底线3: 流动比率检查（按商业模式差异化）")

        # 使用向量化方式避免apply问题
        business_types = []
        for _, row in current_df.iterrows():
            business_types.append(get_business_type_for_safety(row))
        current_df['business_type_temp'] = business_types

        # 应用差异化流动比率底线
        cr_passed = []
        for idx, row in current_df.iterrows():
            biz_type = row['business_type_temp']
            cr_min = safety_baseline['current_ratio_min'].get(biz_type, 1.0)
            actual_cr = row.get('current_ratio', 0)
            cr_passed.append(actual_cr >= cr_min)

        current_df['cr_check'] = cr_passed
        failed = current_df[~current_df['cr_check']]
        if len(failed) > 0:
            logger.info(f"  ❌ 淘汰 {len(failed)} 家企业（流动性不足）:")
            for _, row in failed.head(5).iterrows():
                logger.info(f"     • {row.get('name', row['ts_code'])} ({row['business_type_temp']}): "
                           f"流动比率 {row.get('current_ratio', 0):.2f}")

        current_df = current_df[current_df['cr_check']].copy()
        # 安全删除临时列
        cols_to_drop = [c for c in ['business_type_temp', 'cr_check'] if c in current_df.columns]
        if cols_to_drop:
            current_df = current_df.drop(columns=cols_to_drop)

        logger.info(f"\n✅ 财务安全底线筛选完成: {len(current_df)} 家通过 (淘汰 {before - len(current_df)} 家)")

    step0_count = len(current_df)

    # ========== 第一层：通用铁律 ==========
    logger.info("")
    logger.info("=" * 80)
    logger.info("📋 第一层：通用投资铁律（巴菲特核心准则）")
    logger.info("=" * 80)

    # 铁律1：ROIC绝对值下限
    logger.info(f"\n铁律1: ROIC 5年平均 > {UNIVERSAL_RULES_ENHANCED['roic_5y_min_absolute']}%")
    before = len(current_df)
    passed_roic = current_df['5yd_ts_code_roic_avg'] > UNIVERSAL_RULES_ENHANCED['roic_5y_min_absolute']
    failed = current_df[~passed_roic]
    if len(failed) > 0:
        logger.info(f"  ❌ 淘汰 {len(failed)} 家企业（ROIC过低）")
    current_df = current_df[passed_roic].copy()
    logger.info(f"  ✅ 通过: {len(current_df)} 家")
    step1_count = len(current_df)

    # 铁律2：现金流质量（带增长期豁免）
    logger.info(f"\n铁律2: 现金流质量检查（带增长期豁免）")
    logger.info(f"  基准: OCF/EPS > {UNIVERSAL_RULES_ENHANCED['ocfps_to_eps_base'] * 100:.0f}%")

    high_growth_cfg = UNIVERSAL_RULES_ENHANCED['high_growth_exemption']
    if high_growth_cfg['enabled']:
        logger.info(f"  豁免: 增长>{high_growth_cfg['growth_threshold']}%时，OCF/EPS>{high_growth_cfg['ocfps_to_eps_relaxed'] * 100:.0f}%")

    before = len(current_df)
    ocf_passed = []
    high_growth_exempted = 0

    for idx, row in current_df.iterrows():
        ocf_ratio = row.get('ocfps_to_eps_ratio', 0)
        growth = row.get('5yd_ts_code_or_yoy_avg', 0)

        # 检查是否高增长豁免
        if high_growth_cfg['enabled'] and growth > high_growth_cfg['growth_threshold']:
            threshold = high_growth_cfg['ocfps_to_eps_relaxed']
            if ocf_ratio >= threshold:
                ocf_passed.append(True)
                high_growth_exempted += 1
            else:
                ocf_passed.append(False)
        else:
            threshold = UNIVERSAL_RULES_ENHANCED['ocfps_to_eps_base']
            ocf_passed.append(ocf_ratio >= threshold)

    current_df['ocf_check'] = ocf_passed
    failed = current_df[~current_df['ocf_check']]
    if len(failed) > 0:
        logger.info(f"  ❌ 淘汰 {len(failed)} 家企业（现金流质量差）")

    current_df = current_df[current_df['ocf_check']].copy()
    current_df = current_df.drop(columns=['ocf_check'])

    if high_growth_exempted > 0:
        logger.info(f"  💡 高增长豁免: {high_growth_exempted} 家企业")

    logger.info(f"  ✅ 通过: {len(current_df)} 家")
    step2_count = len(current_df)

    # ========== 第二层：商业模式分类（升级版：5类）==========
    logger.info("")
    logger.info("=" * 80)
    logger.info("📊 第二层：商业模式自动分类（升级版：5类）")
    logger.info("=" * 80)

    # 应用增强版分类逻辑
    classifications = []
    for idx, row in current_df.iterrows():
        category, standards = classify_business_model_enhanced(row.to_dict())
        classifications.append({
            'ts_code': row['ts_code'],
            'category': category,
            'category_name': BUSINESS_MODEL_CLASSIFICATION_ENHANCED[category]['description']
        })

    df_class = pd.DataFrame(classifications)
    current_df = current_df.merge(df_class, on='ts_code', how='left')

    # 统计各类别企业数
    logger.info("\n企业分类结果:")
    for cat in ["规模效应型", "技术壁垒型", "品牌溢价型", "轻资产服务型", "周期资源型"]:
        cat_df = current_df[current_df['category'] == cat]
        if len(cat_df) > 0:
            cat_name = BUSINESS_MODEL_CLASSIFICATION_ENHANCED[cat]['description']
            logger.info(f"\n  【{cat}】{cat_name}: {len(cat_df)}家")

            # 显示行业分布
            industry_counts = cat_df['industry'].value_counts()
            for industry, count in industry_counts.head(3).items():
                logger.info(f"     • {industry}: {count}家")

            # 显示典型企业
            top_companies = cat_df.nlargest(3, '5yd_ts_code_roic_avg')
            logger.info(f"     典型企业:")
            for _, row in top_companies.iterrows():
                logger.info(f"       - {row.get('name', row['ts_code'])}: "
                           f"ROIC {row.get('5yd_ts_code_roic_avg', 0):.1f}%, "
                           f"增长 {row.get('5yd_ts_code_or_yoy_avg', 0):.1f}%")

    step3_count = len(current_df)

    # ========== 第三层：差异化筛选 ==========
    logger.info("")
    logger.info("=" * 80)
    logger.info("🎯 第三层：差异化筛选标准（基于商业模式）")
    logger.info("=" * 80)

    filtered_rows = []
    exemption_logs = []

    for idx, row in current_df.iterrows():
        config_result = get_company_config_enhanced(row.to_dict())
        category = config_result['category']
        standards = config_result['standards']
        exemptions = config_result['exemptions']

        company_name = row.get('name', row['ts_code'])

        # 检查各项标准（与v2相同的逻辑）
        checks = []

        # 1. ROIC 5年平均
        roic_5y = row.get('5yd_ts_code_roic_avg', 0)
        check_roic_min = roic_5y >= standards['roic_5y_min']
        checks.append(('ROIC_5y', check_roic_min, f"{roic_5y:.1f}% >= {standards['roic_5y_min']}%"))

        # 2. ROIC稳定性
        roic_current = row.get('roic', 0)
        if roic_5y > 0:
            decline_pct = ((roic_5y - roic_current) / roic_5y * 100)
        else:
            decline_pct = 0

        if 'roic_decline' in exemptions:
            check_roic_stability = True
            checks.append(('ROIC稳定性', True, f"下滑{decline_pct:.1f}% (豁免)"))
        else:
            check_roic_stability = decline_pct <= standards['roic_decline_max_pct']
            checks.append(('ROIC稳定性', check_roic_stability,
                          f"下滑{decline_pct:.1f}% <= {standards['roic_decline_max_pct']}%"))

        # 3. 毛利率
        margin_5y = row.get('5yd_ts_code_grossprofit_margin_avg', 0)
        check_margin = margin_5y >= standards['margin_5y_min']
        checks.append(('毛利率', check_margin, f"{margin_5y:.1f}% >= {standards['margin_5y_min']}%"))

        # 4. 增长率
        growth_5y = row.get('5yd_ts_code_or_yoy_avg', 0)
        if 'growth_rate' in exemptions:
            check_growth = True
            checks.append(('增长率', True, f"{growth_5y:.1f}% (豁免)"))
        else:
            check_growth = growth_5y >= standards['growth_5y_min']
            checks.append(('增长率', check_growth, f"{growth_5y:.1f}% >= {standards['growth_5y_min']}%"))

        # 5. 负债率（已在底线检查）
        debt = row.get('debt_to_assets', 100)
        if 'debt_to_assets' in exemptions:
            check_debt = True
            checks.append(('负债率', True, f"{debt:.1f}% (豁免)"))
        else:
            check_debt = debt <= standards['debt_to_assets_max']
            checks.append(('负债率', check_debt, f"{debt:.1f}% <= {standards['debt_to_assets_max']}%"))

        # 6. 流动比率（已在底线检查）
        current_ratio = row.get('current_ratio', 0)
        if 'current_ratio' in exemptions:
            check_cr = True
            checks.append(('流动比率', True, f"{current_ratio:.2f} (豁免)"))
        else:
            check_cr = current_ratio >= standards['current_ratio_min']
            checks.append(('流动比率', check_cr, f"{current_ratio:.2f} >= {standards['current_ratio_min']}"))

        # 判断是否通过
        all_passed = all([c[1] for c in checks])

        if all_passed:
            filtered_rows.append(row)

            if len(exemptions) > 0:
                exemption_logs.append({
                    'name': company_name,
                    'category': category,
                    'exemptions': exemptions,
                })

    final_df = pd.DataFrame(filtered_rows)
    logger.info(f"\n✅ 差异化筛选完成: {len(final_df)} 家企业通过")

    # 显示例外豁免企业
    if len(exemption_logs) > 0:
        logger.info(f"\n💡 触发例外规则的企业 ({len(exemption_logs)}家):")
        for log in exemption_logs[:5]:
            logger.info(f"  • {log['name']} ({log['category']})")
            logger.info(f"    豁免: {', '.join(log['exemptions'])}")

    # ========== 质量评分（增强版）==========
    logger.info("")
    logger.info("=" * 80)
    logger.info("⭐ 质量评分体系（增强版：行业相对值+差异化权重）")
    logger.info("=" * 80)

    # 计算行业统计（用于相对排名）
    industry_stats = {}
    for industry in final_df['industry'].unique():
        industry_df = final_df[final_df['industry'] == industry]
        margins = industry_df['5yd_ts_code_grossprofit_margin_avg'].values

        for idx, row in industry_df.iterrows():
            margin = row['5yd_ts_code_grossprofit_margin_avg']
            percentile = (margins < margin).sum() / len(margins) * 100
            if industry not in industry_stats:
                industry_stats[industry] = {}
            industry_stats[industry]['margin_percentile'] = percentile

    # 计算质量评分
    final_df['quality_score'] = final_df.apply(
        lambda row: calculate_quality_score_enhanced(row.to_dict(), industry_stats),
        axis=1
    )
    final_df = final_df.sort_values('quality_score', ascending=False)

    # 统计评分分布
    score_ranges = [
        (90, 100, "卓越"),
        (80, 90, "优秀"),
        (70, 80, "良好"),
        (60, 70, "合格"),
        (0, 60, "一般"),
    ]

    logger.info("\n评分分布:")
    for min_score, max_score, label in score_ranges:
        count = len(final_df[(final_df['quality_score'] >= min_score) & (final_df['quality_score'] < max_score)])
        if count > 0:
            logger.info(f"  {label} ({min_score}-{max_score}分): {count}家")

    # Top 10企业
    logger.info(f"\n🏆 Top 10 高质量企业:")
    for i, (_, row) in enumerate(final_df.head(10).iterrows(), 1):
        logger.info(f"  {i}. {row.get('name', row['ts_code'])} ({row['category']})")
        logger.info(f"     评分: {row['quality_score']:.0f}分 | "
                   f"ROIC: {row.get('5yd_ts_code_roic_avg', 0):.1f}% | "
                   f"增长: {row.get('5yd_ts_code_or_yoy_avg', 0):.1f}% | "
                   f"毛利: {row.get('5yd_ts_code_grossprofit_margin_avg', 0):.1f}%")

    # ========== 最终总结 ==========
    logger.info("")
    logger.info("=" * 80)
    logger.info("📊 筛选总结")
    logger.info("=" * 80)
    logger.info(f"输入企业: {len(df_first)}")
    logger.info(f"通过底线0 (财务安全): {step0_count}")
    logger.info(f"通过铁律1 (ROIC): {step1_count}")
    logger.info(f"通过铁律2 (现金流): {step2_count}")
    logger.info(f"分类完成: {step3_count}")
    logger.info(f"最终通过: {len(final_df)}")
    logger.info(f"总淘汰率: {(1 - len(final_df) / len(df_first)) * 100:.1f}%")

    # 按类别统计
    logger.info("\n分类别统计:")
    for cat in ["规模效应型", "技术壁垒型", "品牌溢价型", "轻资产服务型", "周期资源型"]:
        cat_df = final_df[final_df['category'] == cat]
        if len(cat_df) > 0:
            avg_score = cat_df['quality_score'].mean()
            avg_roic = cat_df['5yd_ts_code_roic_avg'].mean()
            logger.info(f"  {cat}: {len(cat_df)}家 (平均分: {avg_score:.1f}, 平均ROIC: {avg_roic:.1f}%)")

    logger.info("=" * 80)

    # 直接返回DataFrame，不保存文件（由pipeline的store_data步骤统一处理）
    return final_df


# ============================================================================
# 对比分析函数
# ============================================================================

def compare_v2_vs_enhanced(
    v2_results: pd.DataFrame,
    enhanced_results: pd.DataFrame,
    target_leaders: List[str] = None
) -> Dict:
    """
    对比v2和v2_enhanced的结果

    参数:
        v2_results: v2筛选结果
        enhanced_results: v2_enhanced筛选结果
        target_leaders: 目标龙头企业列表

    返回:
        对比统计字典
    """
    if target_leaders is None:
        target_leaders = [
            "贵州茅台", "恒瑞医药", "爱尔眼科", "长春高新", "通策医疗",
            "比亚迪", "宁德时代", "迈瑞医疗", "药明康德",
            "同花顺", "金山办公", "中芯国际",
            "长江电力", "中国神华", "紫金矿业",
            "三一重工",
        ]

    logger.info("")
    logger.info("=" * 80)
    logger.info("📊 v2.0 vs v2.0 Enhanced 对比分析")
    logger.info("=" * 80)

    # 基本统计
    logger.info("\n【基本统计】")
    logger.info(f"v2.0 企业数: {len(v2_results)}")
    logger.info(f"v2.0 Enhanced 企业数: {len(enhanced_results)}")
    logger.info(f"变化: {len(enhanced_results) - len(v2_results):+d} ({(len(enhanced_results) / len(v2_results) - 1) * 100:+.1f}%)")

    # 平均指标对比
    logger.info("\n【平均指标】")
    v2_avg_score = v2_results['quality_score'].mean()
    enh_avg_score = enhanced_results['quality_score'].mean()
    logger.info(f"v2.0 平均评分: {v2_avg_score:.1f}")
    logger.info(f"v2.0 Enhanced 平均评分: {enh_avg_score:.1f} ({enh_avg_score - v2_avg_score:+.1f})")

    v2_avg_roic = v2_results['5yd_ts_code_roic_avg'].mean()
    enh_avg_roic = enhanced_results['5yd_ts_code_roic_avg'].mean()
    logger.info(f"v2.0 平均ROIC: {v2_avg_roic:.1f}%")
    logger.info(f"v2.0 Enhanced 平均ROIC: {enh_avg_roic:.1f}% ({enh_avg_roic - v2_avg_roic:+.1f}%)")

    # 龙头覆盖对比
    logger.info("\n【龙头企业覆盖】")
    v2_covered = []
    enh_covered = []

    for leader in target_leaders:
        in_v2 = len(v2_results[v2_results['name'].str.contains(leader, na=False)]) > 0
        in_enh = len(enhanced_results[enhanced_results['name'].str.contains(leader, na=False)]) > 0

        if in_v2:
            v2_covered.append(leader)
        if in_enh:
            enh_covered.append(leader)

    logger.info(f"v2.0 覆盖: {len(v2_covered)}/{len(target_leaders)} ({len(v2_covered)/len(target_leaders)*100:.0f}%)")
    logger.info(f"v2.0 Enhanced 覆盖: {len(enh_covered)}/{len(target_leaders)} ({len(enh_covered)/len(target_leaders)*100:.0f}%)")

    # 新增和流失
    new_leaders = set(enh_covered) - set(v2_covered)
    lost_leaders = set(v2_covered) - set(enh_covered)

    if new_leaders:
        logger.info(f"\n✅ 新增覆盖 ({len(new_leaders)}家): {', '.join(new_leaders)}")
    if lost_leaders:
        logger.info(f"❌ 失去覆盖 ({len(lost_leaders)}家): {', '.join(lost_leaders)}")

    logger.info("=" * 80)

    return {
        'v2_count': len(v2_results),
        'enhanced_count': len(enhanced_results),
        'v2_coverage': len(v2_covered) / len(target_leaders),
        'enhanced_coverage': len(enh_covered) / len(target_leaders),
        'new_leaders': list(new_leaders),
        'lost_leaders': list(lost_leaders),
    }
