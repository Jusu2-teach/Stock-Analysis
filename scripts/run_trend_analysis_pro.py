"""
专业趋势分析执行脚本 (Pro Version)
===================================

使用多时间窗口策略分析10年数据：
- 近5年数据计算趋势指标
- 全10年数据用于周期检测和断点识别
- 自动识别公司质变点

用法:
    python scripts/run_trend_analysis_pro.py              # 使用10年数据
    python scripts/run_trend_analysis_pro.py --years=5    # 使用5年数据
    python scripts/run_trend_analysis_pro.py --help       # 查看帮助

输出:
    data/filter_middle/xxx_trend_analysis.csv    - 趋势分析结果
    data/filter_middle/xxx_horizon_analysis.csv  - 多时间窗口详细分析
"""

import argparse
import time
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Any

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np


def parse_args():
    parser = argparse.ArgumentParser(description="专业趋势分析执行脚本")
    parser.add_argument(
        "--years", type=int, default=10, choices=[5, 10],
        help="使用几年的数据 (default: 10)"
    )
    parser.add_argument(
        "--metrics", type=str, default="all",
        help="要分析的指标，逗号分隔 (default: all)"
    )
    parser.add_argument(
        "--use-horizon", action="store_true", default=True,
        help="是否使用多时间窗口分析 (default: True)"
    )
    parser.add_argument(
        "--no-horizon", dest="use_horizon", action="store_false",
        help="禁用多时间窗口分析，使用传统方法"
    )
    return parser.parse_args()


def get_data_file(years: int) -> str:
    """根据年数选择数据文件"""
    if years == 10:
        return "data/polars/10yd_final_industry.csv"
    else:
        return "data/polars/5yd_final_industry.csv"


def analyze_with_multi_horizon(
    df: pd.DataFrame,
    metric_name: str,
    group_col: str = "ts_code"
) -> pd.DataFrame:
    """
    使用多时间窗口策略分析

    Args:
        df: 输入数据框（需要包含ts_code和metric_name列）
        metric_name: 指标名称
        group_col: 分组列

    Returns:
        包含分析结果的DataFrame
    """
    from src.astock.business_engines.analyzers.trend.probes import (
        ProfessionalDataWindowStrategy,
    )

    strategy = ProfessionalDataWindowStrategy()
    results = []

    # 按公司分组
    grouped = df.groupby(group_col)
    total = len(grouped)

    for i, (ts_code, group) in enumerate(grouped):
        if (i + 1) % 500 == 0:
            print(f"      进度: {i+1}/{total}")

        # 按时间排序获取指标值
        group_sorted = group.sort_values("end_date")
        values = group_sorted[metric_name].dropna().tolist()

        if len(values) < 3:
            continue

        # 获取行业信息
        industry = group_sorted["industry"].iloc[0] if "industry" in group_sorted.columns else ""

        try:
            # 执行专业分析
            result = strategy.analyze(values, metric_name, industry)

            # 提取关键信息
            row = {
                "ts_code": ts_code,
                "name": group_sorted["name"].iloc[0] if "name" in group_sorted.columns else "",
                "industry": industry,
                # 核心指标
                f"{metric_name}_effective_slope": result.effective_slope,
                f"{metric_name}_effective_cagr": result.effective_cagr,
                f"{metric_name}_effective_cv": result.effective_cv,
                f"{metric_name}_latest_value": result.effective_latest,
                # 多时间窗口
                f"{metric_name}_recent_slope": result.multi_horizon.recent_analysis.slope,
                f"{metric_name}_recent_cagr": result.multi_horizon.recent_analysis.cagr,
                f"{metric_name}_extended_slope": (
                    result.multi_horizon.extended_analysis.slope
                    if result.multi_horizon.extended_analysis else None
                ),
                # 断点信息
                f"{metric_name}_has_break": result.has_break,
                f"{metric_name}_break_year": result.break_year,
                # 周期信息
                f"{metric_name}_cyclical_conf": result.cyclical_confidence,
                f"{metric_name}_cycle_position": result.cycle_position,
                # 质量
                f"{metric_name}_data_quality": result.data_quality_grade,
                f"{metric_name}_confidence": result.analysis_confidence,
                f"{metric_name}_data_regime": result.multi_horizon.data_regime,
                # 数据长度
                f"{metric_name}_data_years": len(values),
            }
            results.append(row)

        except Exception as e:
            # 静默跳过错误
            pass

    return pd.DataFrame(results)


def analyze_traditional(
    data_file: str,
    metric_name: str,
    output_file: str
) -> Tuple[pd.DataFrame, float]:
    """
    使用传统方法分析（兼容旧版）
    """
    from src.astock.business_engines.analyzers.trend.duckdb_engine import analyze_metric_trend

    start = time.time()
    df = analyze_metric_trend(
        data=data_file,
        group_cols='ts_code',
        metric_name=metric_name,
        prefix="",
        suffix="",
        min_periods=5
    )
    elapsed = time.time() - start

    return df, elapsed


def main():
    args = parse_args()

    # 配置
    data_file = get_data_file(args.years)
    output_dir = Path("data/filter_middle")
    output_dir.mkdir(parents=True, exist_ok=True)

    # 指标配置
    all_metrics = [
        ("roic", "roic"),
        ("roiic", "roiic"),
        ("total_revenue_ps", "revenue"),
        ("eps", "profit"),
        ("grossprofit_margin", "gross_margin"),
        ("netprofit_margin", "net_margin"),
        ("roe", "roe"),
        ("ocfps", "ocf"),
    ]

    # 过滤指标
    if args.metrics != "all":
        selected = [m.strip().lower() for m in args.metrics.split(",")]
        all_metrics = [(m, n) for m, n in all_metrics if m in selected or n in selected]

    total_start = time.time()
    results = {}

    print("=" * 70)
    print("🚀 专业趋势分析执行 (Pro Version)")
    print("=" * 70)
    print(f"📂 数据文件: {data_file}")
    print(f"📅 数据年数: {args.years}年")
    print(f"🔬 分析模式: {'多时间窗口' if args.use_horizon else '传统模式'}")
    print(f"📊 待分析指标: {len(all_metrics)} 个")
    print()

    if args.use_horizon:
        # 加载数据
        print("📖 加载数据...")
        df = pd.read_csv(data_file)
        print(f"   共 {len(df)} 行数据")
        print()

        # 多时间窗口分析
        for i, (metric_col, metric_name) in enumerate(all_metrics, 1):
            print(f"[{i}/{len(all_metrics)}] 分析 {metric_col} (多时间窗口)...")

            try:
                start = time.time()
                result_df = analyze_with_multi_horizon(df, metric_col)
                elapsed = time.time() - start

                # 保存结果
                output_path = output_dir / f"{metric_name}_trend_analysis.csv"
                result_df.to_csv(output_path, index=False)

                results[metric_col] = {
                    "status": "success",
                    "rows": len(result_df),
                    "time": elapsed,
                    "file": str(output_path)
                }

                # 统计断点和周期性
                n_breaks = result_df[f"{metric_col}_has_break"].sum() if f"{metric_col}_has_break" in result_df.columns else 0

                print(f"    ✅ 完成: {len(result_df)} 条, 耗时 {elapsed:.1f}s")
                print(f"       发现断点: {n_breaks} 家公司")

            except Exception as e:
                results[metric_col] = {
                    "status": "failed",
                    "error": str(e)
                }
                print(f"    ❌ 失败: {e}")
                import traceback
                traceback.print_exc()
    else:
        # 传统模式
        for i, (metric_col, metric_name) in enumerate(all_metrics, 1):
            print(f"[{i}/{len(all_metrics)}] 分析 {metric_col} (传统模式)...")

            try:
                output_path = output_dir / f"{metric_name}_trend_analysis.csv"
                result_df, elapsed = analyze_traditional(data_file, metric_col, str(output_path))
                result_df.to_csv(output_path, index=False)

                results[metric_col] = {
                    "status": "success",
                    "rows": len(result_df),
                    "time": elapsed,
                    "file": str(output_path)
                }
                print(f"    ✅ 完成: {len(result_df)} 条, 耗时 {elapsed:.1f}s")

            except Exception as e:
                results[metric_col] = {
                    "status": "failed",
                    "error": str(e)
                }
                print(f"    ❌ 失败: {e}")

    # ROIC 质量评分
    print()
    print("=" * 70)
    print("[额外] 生成 ROIC 质量评分...")
    try:
        from src.astock.business_engines.scorers.engine import score_quality

        roic_df = pd.read_csv(output_dir / "roic_trend_analysis.csv")
        scored_result = score_quality(
            data=roic_df,
            report_path=str(output_dir / "roic_quality_report.txt")
        )
        if hasattr(scored_result, 'data'):
            scored_result.data.to_csv(output_dir / "roic_quality_scored.csv", index=False)
            print(f"    ✅ 评分完成: {len(scored_result.data)} 条记录")
        else:
            scored_result.to_csv(output_dir / "roic_quality_scored.csv", index=False)
            print(f"    ✅ 评分完成: {len(scored_result)} 条记录")
    except Exception as e:
        print(f"    ⚠️ 评分跳过: {e}")

    # 汇总
    total_time = time.time() - total_start
    success = sum(1 for r in results.values() if r["status"] == "success")
    failed = len(results) - success

    print()
    print("=" * 70)
    print("📊 执行汇总")
    print("=" * 70)
    print(f"✅ 成功: {success}/{len(all_metrics)}")
    print(f"❌ 失败: {failed}/{len(all_metrics)}")
    print(f"⏱️  总耗时: {total_time:.1f}s")
    print()

    # 列出生成的文件
    print("📂 生成的文件:")
    for f in sorted(output_dir.glob("*.csv")):
        print(f"    - {f.name}")

    if args.use_horizon:
        print()
        print("💡 提示: 新增字段说明")
        print("    - xxx_effective_slope: 有效斜率（考虑断点后）")
        print("    - xxx_has_break: 是否检测到结构断点")
        print("    - xxx_break_year: 断点发生年份")
        print("    - xxx_cyclical_conf: 周期性置信度")
        print("    - xxx_cycle_position: 周期位置 (top/bottom/mid_up/mid_down)")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
