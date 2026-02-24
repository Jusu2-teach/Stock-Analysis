"""
OOS Validation — 主编排器

用法:
    from oos_validation.runner import OOSValidator, OOSConfig

    validator = OOSValidator(Path("."))
    results = validator.run()
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

from . import strategies
from .data_loader import load_aggregated_trends
from .report import generate_report

logger = logging.getLogger("oos_validation")


@dataclass
class OOSConfig:
    """OOS 验证配置。"""

    # ── 策略开关 ──
    perturbation_enabled: bool = True
    bootstrap_enabled: bool = True
    ablation_enabled: bool = True
    cross_engine_enabled: bool = True

    # ── 参数扰动 ──
    noise_levels: list = field(default_factory=lambda: [0.05, 0.10, 0.20])
    perturbation_iterations: int = 5

    # ── 公司自举 ──
    bootstrap_fraction: float = 0.80
    bootstrap_iterations: int = 10

    # ── Pass/Fail 阈值 ──
    perturbation_5pct_pass: float = 0.95
    perturbation_10pct_pass: float = 0.90
    perturbation_20pct_pass: float = 0.80
    bootstrap_pass: float = 0.90
    ablation_max_impact: float = 0.10  # 单因子移除最大允许 ρ 下降
    cross_engine_pass: float = 0.75

    # ── 输出 ──
    report_path: str = "data/oos_validation_report.md"
    seed: int = 42


def get_fast_config() -> OOSConfig:
    """快速模式: 减少迭代 (用于开发测试)。"""
    return OOSConfig(
        noise_levels=[0.05, 0.10],
        perturbation_iterations=3,
        bootstrap_iterations=5,
    )


class OOSValidator:
    """OOS 验证主编排器。

    工作流:
        1. 从缓存 CSV 加载 aggregated_trends (跳过趋势探针)
        2. 运行基线 TRUTH + Evaluator
        3. 执行四大验证策略
        4. 生成 Markdown 报告
    """

    def __init__(
        self, base_dir: Path, config: Optional[OOSConfig] = None
    ):
        self.base_dir = base_dir
        self.config = config or OOSConfig()
        self.aggregated_trends = None
        self.baseline_truth = None
        self.baseline_eval = None
        self.results: Dict[str, Any] = {}

    def run(self) -> Dict[str, Any]:
        """执行完整 OOS 验证流程。"""
        total_t0 = time.time()

        logger.info("=" * 60)
        logger.info("OOS Validation Framework v1.0")
        logger.info("=" * 60)

        # Step 1: 加载数据
        logger.info("\n[1/6] 加载 aggregated_trends ...")
        t0 = time.time()
        self.aggregated_trends = load_aggregated_trends(self.base_dir)
        logger.info(f"  数据加载完成 ({time.time()-t0:.1f}s)")

        # Step 2: 运行基线
        logger.info("\n[2/6] 运行基线 TRUTH + Evaluator ...")
        t0 = time.time()
        self._run_baseline()
        logger.info(f"  基线完成 ({time.time()-t0:.1f}s)")

        # Step 3: 参数扰动
        if self.config.perturbation_enabled:
            logger.info("\n[3/6] Monte Carlo 参数扰动 ...")
            self.results["perturbation"] = strategies.run_perturbation_strategy(
                self.aggregated_trends,
                self.baseline_truth,
                self.baseline_eval,
                noise_levels=self.config.noise_levels,
                iterations=self.config.perturbation_iterations,
                seed=self.config.seed,
            )
        else:
            logger.info("\n[3/6] 参数扰动 — 已跳过")

        # Step 4: 公司自举
        if self.config.bootstrap_enabled:
            logger.info("\n[4/6] 公司自举重采样 ...")
            self.results["bootstrap"] = strategies.run_bootstrap_strategy(
                self.aggregated_trends,
                self.baseline_truth,
                self.baseline_eval,
                sample_fraction=self.config.bootstrap_fraction,
                iterations=self.config.bootstrap_iterations,
                seed=self.config.seed + 1000,
            )
        else:
            logger.info("\n[4/6] 公司自举 — 已跳过")

        # Step 5: 因子消融
        if self.config.ablation_enabled:
            logger.info("\n[5/6] 因子消融 ...")
            self.results["ablation"] = strategies.run_ablation_strategy(
                self.aggregated_trends,
                self.baseline_truth,
                self.baseline_eval,
            )
        else:
            logger.info("\n[5/6] 因子消融 — 已跳过")

        # Step 6: 双引擎一致性
        if self.config.cross_engine_enabled:
            logger.info("\n[6/6] 双引擎一致性 ...")
            self.results["cross_engine"] = strategies.run_cross_engine_strategy(
                self.baseline_truth,
                self.baseline_eval,
            )
        else:
            logger.info("\n[6/6] 双引擎一致性 — 已跳过")

        # 生成报告
        total_elapsed = time.time() - total_t0
        logger.info(f"\n总耗时: {total_elapsed:.1f}s")

        report = generate_report(self.results, self.config, total_elapsed)
        report_path = self.base_dir / self.config.report_path
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(report, encoding="utf-8")
        logger.info(f"报告已写入: {report_path}")

        return self.results

    def _run_baseline(self):
        """运行基线引擎。"""
        from src.astock.business_engines.truth.engine import run_truth
        from src.astock.business_engines.evaluators.engine import (
            run_causal_bayesian_evaluator,
        )

        self.baseline_truth = run_truth(self.aggregated_trends)
        self.baseline_eval = run_causal_bayesian_evaluator(self.aggregated_trends)

        # 记录基线统计
        truth_scores = strategies.extract_truth_scores(self.baseline_truth)
        eval_scores = strategies.extract_eval_scores(self.baseline_eval)

        from . import metrics as m

        baseline_rho = m.spearman_rho(truth_scores, eval_scores)
        logger.info(f"  基线 TRUTH-Eval ρ = {baseline_rho:.4f}")
        logger.info(
            f"  TRUTH: {len(truth_scores)} profiles | "
            f"Eval: {len(eval_scores)} evaluations | "
            f"quality: {len(self.baseline_eval['quality_companies'])}"
        )
