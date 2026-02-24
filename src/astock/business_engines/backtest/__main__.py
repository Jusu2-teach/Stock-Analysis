"""
CLI 入口: python -m src.astock.business_engines.backtest [OPTIONS]

选项:
    --fast           快速模式 (3年训练窗口)
    --expanding      使用扩展窗口而不是滚动窗口
    --data PATH      数据文件路径 (默认: data/polars/10yd_final_industry.csv)
    --output PATH    报告输出路径 (默认: data/backtest_report.md)
"""

from __future__ import annotations

import argparse
import logging
import sys

def main():
    parser = argparse.ArgumentParser(description="Rolling Window Fundamental Backtest")
    parser.add_argument("--fast", action="store_true", help="Fast mode (3yr train)")
    parser.add_argument("--expanding", action="store_true", help="Use expanding window")
    parser.add_argument("--data", default="data/polars/10yd_final_industry.csv", help="Data path")
    parser.add_argument("--output", default="data/backtest_report.md", help="Report output path")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")

    args = parser.parse_args()

    # 日志配置
    level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    # 抑制 noisy loggers
    for name in ("src.astock.business_engines.trend", "shared"):
        logging.getLogger(name).setLevel(logging.WARNING)

    from .engine import FundamentalBacktester

    bt = FundamentalBacktester(
        data_path=args.data,
        min_train_years=3 if args.fast else 5,
        window_type="expanding" if args.expanding else "rolling",
    )

    report = bt.run()

    if report.windows:
        bt.generate_report_md(report, args.output)
        sys.exit(0)
    else:
        print("ERROR: No valid backtest windows")
        sys.exit(1)


if __name__ == "__main__":
    main()
