"""
趋势分析批量执行脚本
=====================

直接执行所有指标的趋势分析，绕过 pipeline 框架。
适用于快速测试或 pipeline 出问题时的备用方案。

用法:
    python scripts/run_trend_analysis.py
"""

import time
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.astock.business_engines.analyzers.trend.duckdb_engine import analyze_metric_trend
from src.astock.business_engines.scorers.engine import score_quality


def main():
    # 配置
    input_file = "data/polars/5yd_final_industry.csv"
    output_dir = Path("data/filter_middle")
    output_dir.mkdir(parents=True, exist_ok=True)

    # 指标配置: (metric_name, output_filename)
    # 注意: reference_metrics 功能待实现，暂不使用
    metrics = [
        ("roic", "roic_trend_analysis.csv"),
        ("roiic", "roiic_trend_analysis.csv"),
        ("total_revenue_ps", "revenue_trend_analysis.csv"),
        ("eps", "profit_trend_analysis.csv"),
        ("grossprofit_margin", "gross_margin_trend_analysis.csv"),
        ("netprofit_margin", "net_margin_trend_analysis.csv"),
        ("roe", "roe_trend_analysis.csv"),
        ("ocfps", "ocf_trend_analysis.csv"),
    ]

    total_start = time.time()
    results = {}

    print("=" * 60)
    print("🚀 开始趋势分析批量执行")
    print("=" * 60)
    print(f"📂 输入文件: {input_file}")
    print(f"📂 输出目录: {output_dir}")
    print(f"📊 待分析指标: {len(metrics)} 个")
    print()

    for i, (metric_name, output_file) in enumerate(metrics, 1):
        print(f"[{i}/{len(metrics)}] 分析 {metric_name}...")

        try:
            start = time.time()
            df = analyze_metric_trend(
                data=input_file,
                group_cols='ts_code',
                metric_name=metric_name,
                prefix="",
                suffix="",
                min_periods=5
            )
            elapsed = time.time() - start

            # 保存结果
            output_path = output_dir / output_file
            df.to_csv(output_path, index=False)

            results[metric_name] = {
                "status": "success",
                "rows": len(df),
                "time": elapsed,
                "file": str(output_path)
            }
            print(f"    ✅ 完成: {len(df)} 条记录, 耗时 {elapsed:.1f}s")

        except Exception as e:
            results[metric_name] = {
                "status": "failed",
                "error": str(e)
            }
            print(f"    ❌ 失败: {e}")

    print()
    print("=" * 60)

    # 生成 ROIC 质量评分
    print("\n[额外] 生成 ROIC 质量评分...")
    try:
        import pandas as pd
        roic_df = pd.read_csv(output_dir / "roic_trend_analysis.csv")
        scored_result = score_quality(
            data=roic_df,
            report_path=str(output_dir / "roic_quality_report.txt")
        )
        # ScoreResult 对象需要访问 .data 属性
        if hasattr(scored_result, 'data'):
            scored_result.data.to_csv(output_dir / "roic_quality_scored.csv", index=False)
            print(f"    ✅ 评分完成: {len(scored_result.data)} 条记录")
        else:
            scored_result.to_csv(output_dir / "roic_quality_scored.csv", index=False)
            print(f"    ✅ 评分完成: {len(scored_result)} 条记录")
    except Exception as e:
        print(f"    ❌ 评分失败: {e}")

    # 汇总
    total_time = time.time() - total_start
    success = sum(1 for r in results.values() if r["status"] == "success")
    failed = len(results) - success

    print()
    print("=" * 60)
    print("📊 执行汇总")
    print("=" * 60)
    print(f"✅ 成功: {success}/{len(metrics)}")
    print(f"❌ 失败: {failed}/{len(metrics)}")
    print(f"⏱️  总耗时: {total_time:.1f}s")
    print()

    # 列出生成的文件
    print("📂 生成的文件:")
    for f in sorted(output_dir.glob("*.csv")):
        print(f"    - {f.name}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
