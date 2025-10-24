#!/usr/bin/env python
"""生成关注行业统计报告"""

import pandas as pd
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# 读取筛选结果
df = pd.read_csv('data/filter_middle/roic_trend_analysis.csv')

# 关注行业列表
FOCUS_INDUSTRIES = [
    '半导体', '专用机械', '软件服务', '电气设备',
    '新型电力', '医疗保健', '化学制药', '生物制药',
    '汽车整车', '元器件', '小金属', 'IT设备'
]

# 筛选关注行业
df_focus = df[df['industry'].isin(FOCUS_INDUSTRIES)]

print("=" * 80)
print("📊 关注行业 ROIC 统计报告")
print("=" * 80)

# 统计
stats = df_focus.groupby('industry').agg({
    'roic_latest': ['mean', 'min', 'max', 'count']
}).round(2)

stats.columns = ['平均ROIC', '最低ROIC', '最高ROIC', '企业数']
stats = stats.sort_values('平均ROIC', ascending=False)

print("\n行业排名 (按平均ROIC):")
print("-" * 80)
for idx, (industry, row) in enumerate(stats.iterrows(), 1):
    print(f"{idx:2d}. {industry:10s} | "
          f"平均={row['平均ROIC']:6.2f}% | "
          f"范围={row['最低ROIC']:5.1f}%-{row['最高ROIC']:5.1f}% | "
          f"企业数={row['企业数']:.0f}家")

print("-" * 80)
print(f"总计: {stats['企业数'].sum():.0f}家企业")
print(f"平均ROIC: {df_focus['roic_latest'].mean():.2f}%")

# 行业分类统计
from astock.business_engines.trend.config import INDUSTRY_CATEGORIES

print("\n" + "=" * 80)
print("📁 按类别统计:")
print("=" * 80)

for category, industries in INDUSTRY_CATEGORIES.items():
    focus_in_cat = [ind for ind in industries if ind in FOCUS_INDUSTRIES]
    if not focus_in_cat:
        continue

    df_cat = df_focus[df_focus['industry'].isin(focus_in_cat)]

    print(f"\n{category} ({len(focus_in_cat)}个关注行业):")
    print(f"  企业数: {len(df_cat)}家")
    print(f"  平均ROIC: {df_cat['roic_latest'].mean():.2f}%")
    print(f"  包含: {', '.join(focus_in_cat)}")

print("\n" + "=" * 80)
