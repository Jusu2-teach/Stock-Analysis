"""
OOS Validation Framework — CLI 入口

用法:
    python -m oos_validation                  # 完整运行
    python -m oos_validation --fast           # 快速模式
    python -m oos_validation --only perturb   # 仅扰动策略
    python -m oos_validation --only bootstrap # 仅自举策略
    python -m oos_validation --only ablation  # 仅消融策略
    python -m oos_validation --only cross     # 仅一致性策略
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from .runner import OOSConfig, OOSValidator, get_fast_config


def main():
    parser = argparse.ArgumentParser(
        description="OOS Validation Framework — A股基本面分析系统稳健性验证"
    )
    parser.add_argument(
        "--fast",
        action="store_true",
        help="快速模式 (减少迭代次数)",
    )
    parser.add_argument(
        "--only",
        type=str,
        choices=["perturb", "bootstrap", "ablation", "cross"],
        help="仅运行指定策略",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=None,
        help="覆盖默认迭代次数",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="随机种子 (默认: 42)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/oos_validation_report.md",
        help="报告输出路径",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="详细日志",
    )

    args = parser.parse_args()

    # 配置日志
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    # 抑制引擎内部的冗余日志
    for name in [
        "src.astock.business_engines.truth.engine",
        "src.astock.business_engines.evaluators.engine",
        "src.astock.business_engines.trend.engine",
    ]:
        logging.getLogger(name).setLevel(logging.WARNING)

    # 构建配置
    if args.fast:
        config = get_fast_config()
    else:
        config = OOSConfig()

    config.seed = args.seed
    config.report_path = args.output

    if args.iterations is not None:
        config.perturbation_iterations = args.iterations
        config.bootstrap_iterations = args.iterations

    # --only 开关
    if args.only:
        config.perturbation_enabled = args.only == "perturb"
        config.bootstrap_enabled = args.only == "bootstrap"
        config.ablation_enabled = args.only == "ablation"
        config.cross_engine_enabled = args.only == "cross"

    # 检测项目根目录
    base_dir = Path.cwd()
    if not (base_dir / "data" / "filter_middle").exists():
        # 尝试常见位置
        candidates = [
            Path(__file__).parent.parent,
            Path.cwd(),
        ]
        for candidate in candidates:
            if (candidate / "data" / "filter_middle").exists():
                base_dir = candidate
                break
        else:
            print(
                "❌ 未找到 data/filter_middle/ 目录。"
                "请在项目根目录下运行: python -m oos_validation",
                file=sys.stderr,
            )
            sys.exit(1)

    print(f"项目目录: {base_dir}")
    print(f"配置: fast={args.fast}, only={args.only}, seed={args.seed}")
    print()

    # 运行
    validator = OOSValidator(base_dir, config)
    results = validator.run()

    # 终端摘要
    print()
    print("=" * 60)
    print("OOS 验证完成！")
    print(f"报告: {base_dir / config.report_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
