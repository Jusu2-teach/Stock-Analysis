"""
═══════════════════════════════════════════════════════════════════════════════
Rolling Window Fundamental Quality Persistence Backtest  v1.0
═══════════════════════════════════════════════════════════════════════════════

设计目标:
    验证系统评分的 **真实预测能力** (predictive validity)。
    不是"两个引擎是否一致" (internal consistency, 即 ρ 指标)，
    而是"高评分公司的未来基本面是否真的更好"。

方法论:
    滚动窗口法 (Rolling Window):
    - 窗口 1: Train 2015-2019 → Test 2020
    - 窗口 2: Train 2016-2020 → Test 2021
    - 窗口 3: Train 2017-2021 → Test 2022
    - 窗口 4: Train 2018-2022 → Test 2023
    - 窗口 5: Train 2019-2023 → Test 2024

指标体系:
    1. Factor IC (Information Coefficient):
       Spearman(score_t, future_fundamental_t+1)
       IC > 0.05 = 有预测力, IC > 0.10 = 强预测力

    2. Quality Persistence:
       P(ROIC_t+1 > median | QUALITY_t) / P(ROIC_t+1 > median)
       Lift > 1.5 = 显著选股能力

    3. Long-Short Spread:
       avg(fundamental_top_quintile) - avg(fundamental_bottom_quintile)
       正 spread + 统计显著 = 因子有效

学术参考:
    - Novy-Marx (2013): "The other side of value: The GP factor"
    - Asness, Frazzini & Pedersen (2019): "Quality Minus Junk"
    - Grinold (1989): "The fundamental law of active management"
    - Fama & French (2015): "A five-factor model"

═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# 数据模型
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class WindowResult:
    """单个回测窗口的结果"""
    train_start: int
    train_end: int
    test_year: int
    n_companies: int
    n_quality: int
    n_veto: int

    # Factor IC
    ic_roic: float = 0.0
    ic_roe: float = 0.0
    ic_gm: float = 0.0
    ic_composite: float = 0.0

    # Per-factor IC
    factor_ics: Dict[str, float] = field(default_factory=dict)

    # Quality persistence
    quality_roic_median: float = 0.0
    all_roic_median: float = 0.0
    quality_lift: float = 0.0
    quality_persistence_rate: float = 0.0

    # Long-short spread
    top_quintile_roic: float = 0.0
    bottom_quintile_roic: float = 0.0
    ls_spread: float = 0.0
    ls_t_stat: float = 0.0


@dataclass
class BacktestReport:
    """完整回测报告"""
    windows: List[WindowResult]
    avg_ic_roic: float = 0.0
    avg_ic_roe: float = 0.0
    avg_ic_composite: float = 0.0
    avg_ls_spread: float = 0.0
    avg_quality_lift: float = 0.0
    avg_quality_persistence: float = 0.0

    avg_factor_ics: Dict[str, float] = field(default_factory=dict)
    optimal_weights: Dict[str, float] = field(default_factory=dict)

    ic_t_stat: float = 0.0
    ic_p_value: float = 0.0
    elapsed_seconds: float = 0.0


# ═══════════════════════════════════════════════════════════════════════════════
# 统计工具 (纯 numpy, 不依赖 scipy)
# ═══════════════════════════════════════════════════════════════════════════════

def _rank_array(arr: np.ndarray) -> np.ndarray:
    """排名 (平均排名处理 ties)"""
    temp = np.empty_like(arr, dtype=float)
    order = arr.argsort()
    temp[order] = np.arange(1, len(arr) + 1, dtype=float)
    for v in np.unique(arr):
        mask = arr == v
        if mask.sum() > 1:
            temp[mask] = temp[mask].mean()
    return temp


def spearman_rank_corr(x: np.ndarray, y: np.ndarray) -> Tuple[float, float]:
    """Spearman 秩相关 + 近似 p 值"""
    n = len(x)
    if n < 5:
        return 0.0, 1.0
    rx, ry = _rank_array(x), _rank_array(y)
    d = rx - ry
    rho = 1.0 - 6.0 * np.sum(d ** 2) / (n * (n ** 2 - 1))
    rho = max(-1.0, min(1.0, rho))
    if abs(rho) >= 1.0:
        return float(rho), 0.0
    t_stat = rho * math.sqrt((n - 2) / (1.0 - rho ** 2))
    df = n - 2
    # Student-t 近似 p 值 (Hill 1970 正则化 Beta)
    p_value = 2.0 * (1.0 + t_stat ** 2 / df) ** (-(df + 1) / 2.0)
    # 更精确: 正则化, 但上面已经足够好
    p_value = min(1.0, max(0.0, p_value * math.sqrt(df * math.pi) / 2.0))
    return float(rho), float(p_value)


def welch_t_test(g1: np.ndarray, g2: np.ndarray) -> Tuple[float, float]:
    """Welch's t-test"""
    n1, n2 = len(g1), len(g2)
    if n1 < 2 or n2 < 2:
        return 0.0, 1.0
    m1, m2 = g1.mean(), g2.mean()
    v1, v2 = g1.var(ddof=1), g2.var(ddof=1)
    se = math.sqrt(v1 / n1 + v2 / n2) if (v1 / n1 + v2 / n2) > 0 else 1e-10
    t_stat = (m1 - m2) / se
    num = (v1 / n1 + v2 / n2) ** 2
    den = (v1 / n1) ** 2 / (n1 - 1) + (v2 / n2) ** 2 / (n2 - 1)
    df = num / den if den > 0 else 1
    p_value = 2.0 * (1.0 + t_stat ** 2 / df) ** (-(df + 1) / 2.0)
    p_value = min(1.0, max(0.0, p_value * math.sqrt(df * math.pi) / 2.0))
    return float(t_stat), float(p_value)


# ═══════════════════════════════════════════════════════════════════════════════
# 核心引擎
# ═══════════════════════════════════════════════════════════════════════════════

class FundamentalBacktester:
    """滚动窗口基本面质量持续性回测引擎

    用法:
        bt = FundamentalBacktester(data_path="data/polars/10yd_final_industry.csv")
        report = bt.run()
        bt.print_summary(report)
        bt.generate_report_md(report)
    """

    # 核心指标 → 用于测定"未来质量"
    FUTURE_METRICS = {
        "roic": "roic",
        "roe": "roe",
        "gross_margin": "grossprofit_margin",
        "net_margin": "netprofit_margin",
        "eps": "eps",
        "ocfps": "ocfps",
    }

    # 分析管线中的指标 (business_key → source_column)
    ANALYSIS_METRICS = {
        "roic": "roic",
        "roe": "roe",
        "revenue": "total_revenue_ps",
        "profit": "eps",
        "gross_margin": "grossprofit_margin",
        "net_margin": "netprofit_margin",
        "ocf": "ocfps",
    }

    # TRUTH 因子名 (FactorId.value)
    FACTOR_NAMES = [
        "alpha", "beta", "gamma", "pi_profitability",
        "lambda_leverage", "delta_fraud", "delta_decay", "verification",
    ]

    def __init__(
        self,
        data_path: str = "data/polars/10yd_final_industry.csv",
        min_train_years: int = 5,
        window_type: str = "rolling",
    ):
        self.data_path = Path(data_path)
        self.min_train_years = min_train_years
        self.window_type = window_type
        self._raw_data: Optional[pd.DataFrame] = None

    # ──────────────────────────────────────────────────────────────────────
    # 数据加载
    # ──────────────────────────────────────────────────────────────────────

    def _load_data(self) -> pd.DataFrame:
        if self._raw_data is not None:
            return self._raw_data
        df = pd.read_csv(self.data_path)
        df["year"] = df["end_date"].astype(str).str[:4].astype(int)
        self._raw_data = df
        return df

    def _get_windows(self) -> List[Tuple[List[int], int]]:
        df = self._load_data()
        all_years = sorted(df["year"].unique())
        windows = []
        for i in range(len(all_years) - self.min_train_years):
            if self.window_type == "rolling":
                train_years = all_years[i: i + self.min_train_years]
            else:
                train_years = all_years[: i + self.min_train_years]
            test_year = all_years[i + self.min_train_years]
            windows.append((list(train_years), test_year))
        return windows

    def _subset_data(self, years: List[int]) -> pd.DataFrame:
        df = self._load_data()
        return df[df["year"].isin(years)].copy()

    # ──────────────────────────────────────────────────────────────────────
    # 在子集上运行完整管线 (trend → PDDA → TRUTH)
    # ──────────────────────────────────────────────────────────────────────

    def _run_truth_on_subset(self, train_df: pd.DataFrame) -> Dict[str, Any]:
        """在训练数据子集上运行完整 TRUTH 分析管线

        复用生产管线的三层:
            1. analyze_metric_trend() × 7 指标 → aggregated_trends
            2. build_financial_context() → aggregated_trends["financial_context"]
            3. TRUTH: _build_probes → _process_single × N → _cross_sectional_normalize
        """
        # 延迟导入 (避免顶层循环依赖)
        from ..trend.engine import analyze_metric_trend, build_financial_context
        from ..truth.engine import (
            _build_probes_from_dataframes,
            _process_single,
            _cross_sectional_normalize,
        )
        from ..truth.config import get_default_config as get_truth_config

        # ── Layer 1: 趋势分析 (复用生产函数) ──
        aggregated_trends: Dict[str, pd.DataFrame] = {}
        for business_key, source_col in self.ANALYSIS_METRICS.items():
            if source_col not in train_df.columns:
                continue
            try:
                agg_result = analyze_metric_trend(
                    data=train_df,
                    group_cols="ts_code",
                    metric_name=business_key,
                    min_periods=3,  # 回测窗口较短, 放宽到3年
                    enable_multi_horizon=True,
                )
                df_result = agg_result.value
                if df_result is not None and not df_result.empty:
                    aggregated_trends[business_key] = df_result
            except Exception as e:
                logger.debug(f"  trend {business_key} failed: {e}")
                continue

        if not aggregated_trends:
            return {}

        # ── Layer 1.5: Financial Context ──
        try:
            fc_result = build_financial_context(data=train_df)
            if fc_result.value is not None and not fc_result.value.empty:
                aggregated_trends["financial_context"] = fc_result.value
        except Exception as e:
            logger.debug(f"  financial_context failed: {e}")

        # ── Layer 2+3: TRUTH 因子 → 求解器 → 校准 → 截面标准化 ──
        truth_config = get_truth_config()
        probes_by_ts = _build_probes_from_dataframes(aggregated_trends)

        profiles = []
        for ts_code, probes in probes_by_ts.items():
            try:
                profile = _process_single(ts_code, probes, truth_config)
                profiles.append(profile)
            except Exception:
                continue

        if len(profiles) >= 5:
            profiles = _cross_sectional_normalize(profiles, truth_config)

        # 输出: {ts_code: {final_score, grade, factors: {name: score}}}
        result = {}
        for p in profiles:
            factors_dict = {}
            for fid, fr in p.factors.items():
                factors_dict[fid.value] = fr.score
            result[p.ts_code] = {
                "final_score": p.final_score,
                "grade": p.grade.value if p.grade else None,
                "signal": p.signal.value if p.signal else None,
                "factors": factors_dict,
            }

        return result

    # ──────────────────────────────────────────────────────────────────────
    # 未来基本面数据
    # ──────────────────────────────────────────────────────────────────────

    def _get_future_fundamentals(self, test_year: int) -> pd.DataFrame:
        df = self._load_data()
        test_df = df[df["year"] == test_year].copy()
        cols = ["ts_code"]
        for name, col in self.FUTURE_METRICS.items():
            if col in test_df.columns:
                test_df[f"future_{name}"] = test_df[col]
                cols.append(f"future_{name}")
        return test_df[cols].drop_duplicates("ts_code")

    # ──────────────────────────────────────────────────────────────────────
    # 单窗口评估
    # ──────────────────────────────────────────────────────────────────────

    def _evaluate_window(
        self, train_years: List[int], test_year: int
    ) -> Optional[WindowResult]:
        tag = f"{train_years[0]}-{train_years[-1]}→{test_year}"
        logger.info(f"Window {tag}: running TRUTH on training data...")

        train_df = self._subset_data(train_years)
        future_df = self._get_future_fundamentals(test_year)
        if train_df.empty or future_df.empty:
            logger.warning(f"  {tag}: empty data, skip")
            return None

        truth_results = self._run_truth_on_subset(train_df)
        if not truth_results:
            logger.warning(f"  {tag}: TRUTH returned empty, skip")
            return None

        # 匹配: 训练评分 ↔ 测试年实际基本面
        matched = []
        for ts_code, scores in truth_results.items():
            row_future = future_df[future_df["ts_code"] == ts_code]
            if row_future.empty:
                continue
            row = {
                "ts_code": ts_code,
                "final_score": scores["final_score"],
                "grade": scores["grade"],
                **scores["factors"],
            }
            for col in future_df.columns:
                if col.startswith("future_"):
                    val = row_future.iloc[0][col]
                    if pd.notna(val):
                        row[col] = float(val)
            matched.append(row)

        match_df = pd.DataFrame(matched)
        if len(match_df) < 20:
            logger.warning(f"  {tag}: only {len(match_df)} matched, skip")
            return None

        logger.info(f"  {tag}: matched {len(match_df)} companies")

        # ── Factor IC ──
        ic_roic = self._calc_ic(match_df, "final_score", "future_roic")
        ic_roe = self._calc_ic(match_df, "final_score", "future_roe")
        ic_gm = self._calc_ic(match_df, "final_score", "future_gross_margin")
        ic_vals = [v for v in [ic_roic, ic_roe, ic_gm] if v != 0.0]
        ic_composite = sum(ic_vals) / len(ic_vals) if ic_vals else 0.0

        # ── Per-factor IC → future ROIC ──
        factor_ics = {}
        for fname in self.FACTOR_NAMES:
            if fname in match_df.columns:
                ic = self._calc_ic(match_df, fname, "future_roic")
                # 负向因子 (高分 = 更差) → 翻转 IC
                if fname in ("alpha", "beta", "lambda_leverage", "delta_fraud", "delta_decay"):
                    ic = -ic
                factor_ics[fname] = ic

        # ── Quality persistence ──
        quality_mask = match_df["grade"].isin(["A+", "A", "B+"])
        n_quality = int(quality_mask.sum())
        n_veto = int((match_df["grade"] == "F").sum())
        quality_roic_med, all_roic_med, quality_lift, persistence_rate = (
            self._calc_quality_persistence(match_df, quality_mask)
        )

        # ── Long-Short spread ──
        top_roic, bot_roic, ls_spread, ls_t = self._calc_ls_spread(match_df)

        return WindowResult(
            train_start=train_years[0],
            train_end=train_years[-1],
            test_year=test_year,
            n_companies=len(match_df),
            n_quality=n_quality,
            n_veto=n_veto,
            ic_roic=ic_roic,
            ic_roe=ic_roe,
            ic_gm=ic_gm,
            ic_composite=ic_composite,
            factor_ics=factor_ics,
            quality_roic_median=quality_roic_med,
            all_roic_median=all_roic_med,
            quality_lift=quality_lift,
            quality_persistence_rate=persistence_rate,
            top_quintile_roic=top_roic,
            bottom_quintile_roic=bot_roic,
            ls_spread=ls_spread,
            ls_t_stat=ls_t,
        )

    # ──────────────────────────────────────────────────────────────────────
    # IC / Persistence / L-S 计算
    # ──────────────────────────────────────────────────────────────────────

    @staticmethod
    def _calc_ic(df: pd.DataFrame, score_col: str, future_col: str) -> float:
        if score_col not in df.columns or future_col not in df.columns:
            return 0.0
        valid = df[[score_col, future_col]].dropna()
        if len(valid) < 20:
            return 0.0
        ic, _ = spearman_rank_corr(valid[score_col].values, valid[future_col].values)
        return ic

    @staticmethod
    def _calc_quality_persistence(
        df: pd.DataFrame, quality_mask: pd.Series
    ) -> Tuple[float, float, float, float]:
        if "future_roic" not in df.columns or quality_mask.sum() == 0:
            return 0.0, 0.0, 1.0, 0.0
        q_roic = df.loc[quality_mask, "future_roic"].dropna()
        all_roic = df["future_roic"].dropna()
        if len(q_roic) == 0 or len(all_roic) == 0:
            return 0.0, 0.0, 1.0, 0.0
        q_med = float(q_roic.median())
        a_med = float(all_roic.median())
        lift = q_med / a_med if a_med > 0 else 1.0
        above = (q_roic > a_med).sum()
        persist = above / len(q_roic) if len(q_roic) > 0 else 0.0
        return q_med, a_med, lift, persist

    @staticmethod
    def _calc_ls_spread(df: pd.DataFrame) -> Tuple[float, float, float, float]:
        if "future_roic" not in df.columns:
            return 0.0, 0.0, 0.0, 0.0
        n = len(df)
        q_size = n // 5
        if q_size < 5:
            return 0.0, 0.0, 0.0, 0.0
        sorted_df = df.sort_values("final_score", ascending=False)
        top = sorted_df.head(q_size)["future_roic"].dropna()
        bot = sorted_df.tail(q_size)["future_roic"].dropna()
        if len(top) == 0 or len(bot) == 0:
            return 0.0, 0.0, 0.0, 0.0
        t_mean = float(top.mean())
        b_mean = float(bot.mean())
        spread = t_mean - b_mean
        t_stat, _ = welch_t_test(top.values, bot.values)
        return t_mean, b_mean, spread, t_stat

    # ──────────────────────────────────────────────────────────────────────
    # IC-Weighted 最优权重 (Grinold 1989)
    # ──────────────────────────────────────────────────────────────────────

    def _optimize_weights(self, windows: List[WindowResult]) -> Dict[str, float]:
        factor_ics_all: Dict[str, List[float]] = {}
        for w in windows:
            for f, ic in w.factor_ics.items():
                factor_ics_all.setdefault(f, []).append(ic)

        avg_ics = {f: sum(vs) / len(vs) for f, vs in factor_ics_all.items() if vs}
        total_pos = sum(max(0, ic) for ic in avg_ics.values())
        if total_pos < 0.01:
            n = max(len(avg_ics), 1)
            return {f: 1.0 / n for f in avg_ics}

        min_w = 0.03
        weights = {}
        for f, ic in avg_ics.items():
            weights[f] = max(min_w, ic / total_pos) if ic > 0 else min_w

        total = sum(weights.values())
        return {f: w / total for f, w in weights.items()}

    # ──────────────────────────────────────────────────────────────────────
    # 主入口
    # ──────────────────────────────────────────────────────────────────────

    def run(self) -> BacktestReport:
        t0 = time.time()
        print("=" * 65)
        print("  ROLLING WINDOW FUNDAMENTAL QUALITY PERSISTENCE BACKTEST")
        print("=" * 65)

        windows = self._get_windows()
        print(f"  Windows: {len(windows)}  |  Type: {self.window_type}  |  Train length: {self.min_train_years}yr")
        print()

        results: List[WindowResult] = []
        for i, (train_years, test_year) in enumerate(windows, 1):
            tag = f"{train_years[0]}-{train_years[-1]}→{test_year}"
            print(f"  [{i}/{len(windows)}] {tag} ...", end=" ", flush=True)
            try:
                result = self._evaluate_window(train_years, test_year)
                if result is not None:
                    results.append(result)
                    print(
                        f"OK  n={result.n_companies}  "
                        f"IC(ROIC)={result.ic_roic:+.3f}  "
                        f"L/S={result.ls_spread:+.1f}pp  "
                        f"Lift={result.quality_lift:.2f}x"
                    )
                else:
                    print("SKIP (insufficient data)")
            except Exception as e:
                print(f"FAIL ({e})")
                import traceback
                traceback.print_exc()

        elapsed = time.time() - t0
        print(f"\n  Completed in {elapsed:.0f}s")

        if not results:
            print("  !! NO VALID WINDOWS !!")
            return BacktestReport(windows=[], elapsed_seconds=elapsed)

        # ── Aggregate ──
        n = len(results)
        avg = lambda attr: sum(getattr(w, attr) for w in results) / n

        avg_ic_roic = avg("ic_roic")
        avg_ic_roe = avg("ic_roe")
        avg_ic_comp = avg("ic_composite")
        avg_ls = avg("ls_spread")
        avg_lift = avg("quality_lift")
        avg_persist = avg("quality_persistence_rate")

        # IC t-stat
        ics = [w.ic_roic for w in results]
        if len(ics) >= 2:
            ic_mean = sum(ics) / len(ics)
            ic_std = (sum((x - ic_mean) ** 2 for x in ics) / (len(ics) - 1)) ** 0.5
            ic_t = ic_mean / (ic_std / len(ics) ** 0.5) if ic_std > 0 else 0.0
        else:
            ic_t = 0.0
        # p-value for t with df=n-1
        df_t = max(n - 1, 1)
        ic_p = 2.0 * (1.0 + ic_t ** 2 / df_t) ** (-(df_t + 1) / 2.0)
        ic_p = min(1.0, max(0.0, ic_p * math.sqrt(df_t * math.pi) / 2.0))

        # Per-factor IC aggregation
        all_fics: Dict[str, List[float]] = {}
        for w in results:
            for f, ic in w.factor_ics.items():
                all_fics.setdefault(f, []).append(ic)
        avg_factor_ics = {
            f: sum(vs) / len(vs) for f, vs in all_fics.items() if vs
        }

        optimal_weights = self._optimize_weights(results)

        report = BacktestReport(
            windows=results,
            avg_ic_roic=avg_ic_roic,
            avg_ic_roe=avg_ic_roe,
            avg_ic_composite=avg_ic_comp,
            avg_ls_spread=avg_ls,
            avg_quality_lift=avg_lift,
            avg_quality_persistence=avg_persist,
            avg_factor_ics=avg_factor_ics,
            optimal_weights=optimal_weights,
            ic_t_stat=ic_t,
            ic_p_value=ic_p,
            elapsed_seconds=elapsed,
        )

        self.print_summary(report)
        return report

    # ──────────────────────────────────────────────────────────────────────
    # 报告
    # ──────────────────────────────────────────────────────────────────────

    def print_summary(self, report: BacktestReport):
        print()
        print("=" * 65)
        print("  BACKTEST RESULTS")
        print("=" * 65)
        print(f"  Windows: {len(report.windows)}  |  Elapsed: {report.elapsed_seconds:.0f}s")
        print()
        print("  --- Core Predictive Metrics ---")
        print(f"  IC(ROIC):      {report.avg_ic_roic:+.4f}  {_judge_ic_short(report.avg_ic_roic)}")
        print(f"  IC(ROE):       {report.avg_ic_roe:+.4f}  {_judge_ic_short(report.avg_ic_roe)}")
        print(f"  IC(Composite): {report.avg_ic_composite:+.4f}")
        print(f"  IC t-stat:     {report.ic_t_stat:.2f}   {'✅ signif' if abs(report.ic_t_stat) >= 2.0 else '⚠️ not signif'}")
        print(f"  IC p-value:    {report.ic_p_value:.4f}")
        print(f"  L/S Spread:    {report.avg_ls_spread:+.1f}pp  {'✅' if report.avg_ls_spread >= 3.0 else '❌'}")
        print(f"  Quality Lift:  {report.avg_quality_lift:.2f}x  {'✅' if report.avg_quality_lift >= 1.5 else '❌'}")
        print(f"  Persistence:   {report.avg_quality_persistence:.1%}  {'✅' if report.avg_quality_persistence >= 0.70 else '❌'}")
        print()
        print("  --- Factor IC Ranking (→ future ROIC) ---")
        for f, ic in sorted(report.avg_factor_ics.items(), key=lambda x: x[1], reverse=True):
            print(f"    {f:16s}: {ic:+.4f}  {_judge_ic_short(ic)}")
        print()
        print("  --- IC-Weighted Optimal Weights (Grinold 1989) ---")
        cur_w = {
            "alpha": 0.10, "beta": 0.08, "gamma": 0.14, "pi_profitability": 0.15,
            "lambda_leverage": 0.10, "delta_fraud": 0.15, "delta_decay": 0.16,
            "verification": 0.12,
        }
        for f, w in sorted(report.optimal_weights.items(), key=lambda x: x[1], reverse=True):
            d = w - cur_w.get(f, 0)
            arrow = "↑" if d > 0.02 else ("↓" if d < -0.02 else "≈")
            print(f"    {f:20s}: {w:.1%}  (current {cur_w.get(f, 0):.1%}, {d:+.1%} {arrow})")
        print("=" * 65)

    def generate_report_md(
        self, report: BacktestReport,
        output_path: str = "data/backtest_report.md",
    ) -> str:
        """生成 Markdown 回测报告"""
        L = []  # lines accumulator
        L.append("# 滚动窗口基本面质量持续性回测报告\n")
        L.append(f"- **生成时间**: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        L.append(f"- **窗口数量**: {len(report.windows)}")
        L.append(f"- **窗口类型**: {self.window_type}")
        L.append(f"- **训练长度**: {self.min_train_years}年")
        L.append(f"- **耗时**: {report.elapsed_seconds:.0f}s\n")

        # Core
        L.append("## 核心预测指标\n")
        L.append("| 指标 | 值 | 判定标准 | 判定 |")
        L.append("|------|-----|---------|------|")
        L.append(f"| IC(ROIC) | **{report.avg_ic_roic:.4f}** | ≥0.05 | {_judge_ic_md(report.avg_ic_roic)} |")
        L.append(f"| IC(ROE) | **{report.avg_ic_roe:.4f}** | ≥0.05 | {_judge_ic_md(report.avg_ic_roe)} |")
        L.append(f"| IC(综合) | **{report.avg_ic_composite:.4f}** | ≥0.05 | {_judge_ic_md(report.avg_ic_composite)} |")
        L.append(f"| IC t-stat | {report.ic_t_stat:.2f} | ≥2.0 | {'✅ 显著' if abs(report.ic_t_stat) >= 2.0 else '⚠️ 不显著'} |")
        L.append(f"| IC p-value | {report.ic_p_value:.4f} | ≤0.05 | {'✅' if report.ic_p_value <= 0.05 else '⚠️'} |")
        ls_j = "✅ 显著" if report.avg_ls_spread > 3 else ("⚠️ 中等" if report.avg_ls_spread > 1 else "❌ 弱")
        L.append(f"| 多空ROIC差 | **{report.avg_ls_spread:.1f}pp** | ≥3pp | {ls_j} |")
        lift_j = "✅" if report.avg_quality_lift >= 1.5 else ("⚠️" if report.avg_quality_lift >= 1.2 else "❌")
        L.append(f"| Quality提升比 | **{report.avg_quality_lift:.2f}x** | ≥1.5x | {lift_j} |")
        per_j = "✅" if report.avg_quality_persistence >= 0.70 else ("⚠️" if report.avg_quality_persistence >= 0.50 else "❌")
        L.append(f"| 质量持续率 | **{report.avg_quality_persistence:.1%}** | ≥70% | {per_j} |")
        L.append("")

        # Windows
        L.append("## 逐窗口明细\n")
        L.append("| 窗口 | 公司数 | Quality | IC(ROIC) | IC(ROE) | 多空差 | 提升比 | 持续率 |")
        L.append("|------|--------|---------|----------|---------|--------|--------|--------|")
        for w in report.windows:
            L.append(
                f"| {w.train_start}-{w.train_end}→{w.test_year} "
                f"| {w.n_companies} | {w.n_quality} "
                f"| {w.ic_roic:+.4f} | {w.ic_roe:+.4f} "
                f"| {w.ls_spread:+.1f}pp "
                f"| {w.quality_lift:.2f}x "
                f"| {w.quality_persistence_rate:.0%} |"
            )
        L.append("")

        # Factor IC
        L.append("## 因子预测力排行 (avg IC → future ROIC)\n")
        L.append("| 排名 | 因子 | avg IC | 评价 |")
        L.append("|------|------|--------|------|")
        for rank, (f, ic) in enumerate(
            sorted(report.avg_factor_ics.items(), key=lambda x: x[1], reverse=True), 1
        ):
            L.append(f"| {rank} | {f} | {ic:+.4f} | {_judge_ic_md(ic)} |")
        L.append("")

        # Optimal weights
        L.append("## IC-Weighted 最优权重 (Grinold 1989)\n")
        L.append("| 因子 | 当前权重 | IC最优权重 | 差异 |")
        L.append("|------|---------|-----------|------|")
        cur = {
            "alpha": 0.10, "beta": 0.08, "gamma": 0.14, "pi_profitability": 0.15,
            "lambda_leverage": 0.10, "delta_fraud": 0.15, "delta_decay": 0.16,
            "verification": 0.12,
        }
        for f in sorted(report.optimal_weights.keys()):
            ow = report.optimal_weights[f]
            cw = cur.get(f, 0)
            d = ow - cw
            arrow = "↑" if d > 0.02 else ("↓" if d < -0.02 else "≈")
            L.append(f"| {f} | {cw:.1%} | {ow:.1%} | {d:+.1%} {arrow} |")
        L.append("")

        # Verdict
        L.append("## 总体判定\n")
        if report.avg_ic_roic >= 0.10 and report.avg_ls_spread >= 5.0:
            L.append("**A 级** — 系统具有强预测能力，IC和多空收益差均达到量化基金级别")
        elif report.avg_ic_roic >= 0.05 and report.avg_ls_spread >= 2.0:
            L.append("**B 级** — 中等预测能力，评分与未来基本面存在统计显著正相关")
        elif report.avg_ic_roic >= 0.02:
            L.append("**C 级** — 弱预测能力，信号存在但不够稳定")
        else:
            L.append("**D 级** — 无显著预测能力，需要根本性重构")
        L.append("")

        L.append("### IC 学术参考基准\n")
        L.append("| 因子 | 文献 IC | 来源 |")
        L.append("|------|--------|------|")
        L.append("| Value (B/P) | 0.03-0.05 | Fama-French 1993 |")
        L.append("| Profitability (GP/A) | 0.04-0.08 | Novy-Marx 2013 |")
        L.append("| Quality (QMJ) | 0.05-0.09 | AQR Asness et al 2019 |")
        L.append("| Accruals | 0.03-0.06 | Sloan 1996 |")
        L.append("")
        L.append("> 注: 文献IC基于股票收益率，本系统IC基于未来基本面值（ROIC/ROE）。")
        L.append("> 后者通常更高更稳定，因为基本面持续性强于股价。")
        L.append("> 严格可比需要股价数据。")

        text = "\n".join(L)
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_text(text, encoding="utf-8")
        print(f"\n  Report saved to: {output_path}")
        return text


# ═══════════════════════════════════════════════════════════════════════════════
# 辅助判定函数
# ═══════════════════════════════════════════════════════════════════════════════

def _judge_ic_short(ic: float) -> str:
    if ic >= 0.10:
        return "✅ strong"
    elif ic >= 0.05:
        return "✅ good"
    elif ic >= 0.02:
        return "⚠️ weak"
    return "❌ none"


def _judge_ic_md(ic: float) -> str:
    if ic >= 0.10:
        return "✅ 强预测力"
    elif ic >= 0.05:
        return "✅ 有预测力"
    elif ic >= 0.02:
        return "⚠️ 弱预测力"
    return "❌ 无预测力"


# ═══════════════════════════════════════════════════════════════════════════════
# v11.0: Evaluator IC 回测 + 跨引擎共识元评分 + F-Score + Beneish + IC 衰减
# ═══════════════════════════════════════════════════════════════════════════════

class EvaluatorBacktester(FundamentalBacktester):
    """v11.0: Evaluator 引擎专用回测

    与 TRUTH 回测共享基础设施, 新增:
    1. 在子集上运行 Evaluator 而非 TRUTH
    2. 输出: Evaluator 的 score IC + quality persistence
    """

    def _run_evaluator_on_subset(self, train_df: pd.DataFrame) -> Dict[str, Any]:
        """在训练数据子集上运行 Evaluator"""
        from ..trend.engine import analyze_metric_trend, build_financial_context
        from ..evaluators.engine import CausalBayesianEvaluator, EvaluatorConfig

        aggregated_trends: Dict[str, pd.DataFrame] = {}
        for business_key, source_col in self.ANALYSIS_METRICS.items():
            if source_col not in train_df.columns:
                continue
            try:
                agg_result = analyze_metric_trend(
                    data=train_df, group_cols="ts_code",
                    metric_name=business_key, min_periods=3, enable_multi_horizon=True,
                )
                if agg_result.value is not None and not agg_result.value.empty:
                    aggregated_trends[business_key] = agg_result.value
            except Exception:
                continue

        if not aggregated_trends:
            return {}

        try:
            fc_result = build_financial_context(data=train_df)
            if fc_result.value is not None and not fc_result.value.empty:
                aggregated_trends["financial_context"] = fc_result.value
        except Exception:
            pass

        evaluator = CausalBayesianEvaluator(EvaluatorConfig())
        all_ts_codes = set()
        for df in aggregated_trends.values():
            if df is not None and "ts_code" in df.columns:
                all_ts_codes.update(df["ts_code"].unique())

        # 提取公司信息
        company_info_dict = {}
        for df in aggregated_trends.values():
            if df is not None and not df.empty and "name" in df.columns:
                for _, row in df[["ts_code", "name", "industry"]].drop_duplicates("ts_code").iterrows():
                    ts = row["ts_code"]
                    if ts not in company_info_dict:
                        company_info_dict[ts] = {
                            "ts_code": ts, "name": str(row.get("name", "")),
                            "industry": str(row.get("industry", "")),
                        }
                break

        result = {}
        for ts_code in all_ts_codes:
            try:
                info = company_info_dict.get(ts_code, {"ts_code": ts_code})
                ev = evaluator.evaluate_company(ts_code, aggregated_trends, info)
                result[ts_code] = {
                    "final_score": ev.score,
                    "decision": ev.decision.value,
                    "confidence": ev.confidence,
                }
            except Exception:
                continue
        return result

    def _evaluate_window(self, train_years, test_year):
        tag = f"{train_years[0]}-{train_years[-1]}→{test_year}"
        logger.info(f"[EvalBT] Window {tag}")

        train_df = self._subset_data(train_years)
        future_df = self._get_future_fundamentals(test_year)
        if train_df.empty or future_df.empty:
            return None

        eval_results = self._run_evaluator_on_subset(train_df)
        if not eval_results:
            return None

        matched = []
        for ts_code, scores in eval_results.items():
            row_future = future_df[future_df["ts_code"] == ts_code]
            if row_future.empty:
                continue
            row = {"ts_code": ts_code, "final_score": scores["final_score"],
                   "grade": "A" if scores.get("decision") == "quality" else (
                       "F" if scores.get("decision") == "veto" else "C")}
            for col in future_df.columns:
                if col.startswith("future_"):
                    val = row_future.iloc[0][col]
                    if pd.notna(val):
                        row[col] = float(val)
            matched.append(row)

        match_df = pd.DataFrame(matched)
        if len(match_df) < 20:
            return None

        ic_roic = self._calc_ic(match_df, "final_score", "future_roic")
        ic_roe = self._calc_ic(match_df, "final_score", "future_roe")
        ic_gm = self._calc_ic(match_df, "final_score", "future_gross_margin")
        ic_vals = [v for v in [ic_roic, ic_roe, ic_gm] if v != 0.0]

        quality_mask = match_df["grade"] == "A"
        q_med, a_med, lift, persist = self._calc_quality_persistence(match_df, quality_mask)
        top_r, bot_r, ls_spread, ls_t = self._calc_ls_spread(match_df)

        return WindowResult(
            train_start=train_years[0], train_end=train_years[-1], test_year=test_year,
            n_companies=len(match_df),
            n_quality=int(quality_mask.sum()), n_veto=int((match_df["grade"] == "F").sum()),
            ic_roic=ic_roic, ic_roe=ic_roe, ic_gm=ic_gm,
            ic_composite=sum(ic_vals) / len(ic_vals) if ic_vals else 0.0,
            quality_roic_median=q_med, all_roic_median=a_med,
            quality_lift=lift, quality_persistence_rate=persist,
            top_quintile_roic=top_r, bottom_quintile_roic=bot_r,
            ls_spread=ls_spread, ls_t_stat=ls_t,
        )


# ═══════════════════════════════════════════════════════════════════════════════
# v11.0: Piotroski F-Score (第三独立验证器)
# ═══════════════════════════════════════════════════════════════════════════════

def compute_piotroski_fscore(features: Dict[str, Any]) -> Dict[str, Any]:
    """计算 Piotroski F-Score (9点制)

    可用 7/9 项 (F6/F7 需要未持有的数据):
    F1: ROA > 0 (盈利能力)
    F2: OCF > 0 (现金质量)
    F3: ΔROA > 0 (盈利改善)
    F4: OCF > NI (应计质量, Sloan 1996)
    F5: ΔLeverage < 0 (杠杆降低)
    F8: ΔGross Margin > 0 (护城河改善)
    F9: ΔAsset Turnover > 0 (效率改善)

    Args:
        features: PDDA 提取的特征字典 (与 Evaluator 相同)

    Returns:
        {"fscore": int, "signals": {name: bool}, "max_possible": int}
    """
    signals = {}
    score = 0

    # F1: ROA > 0 (用 ROIC 替代, 更严格)
    roic_level = features.get("roic_level", features.get("fc_profitability_roic_level"))
    if roic_level is not None:
        signals["F1_profitability"] = roic_level > 0
        if signals["F1_profitability"]:
            score += 1

    # F2: OCF > 0
    ocf_level = features.get("ocf_level")
    if ocf_level is not None:
        signals["F2_cash_flow"] = ocf_level > 0
        if signals["F2_cash_flow"]:
            score += 1

    # F3: ΔROA > 0 (ROIC 趋势)
    roic_trend = features.get("roic_trend")
    if roic_trend is not None:
        signals["F3_delta_roa"] = roic_trend > 0
        if signals["F3_delta_roa"]:
            score += 1

    # F4: OCF/NI > 1 (应计质量)
    profit_level = features.get("profit_level")
    if ocf_level is not None and profit_level is not None and profit_level > 0:
        cash_conv = ocf_level / max(profit_level, 0.01)
        signals["F4_accrual_quality"] = cash_conv > 1.0
        if signals["F4_accrual_quality"]:
            score += 1

    # F5: ΔLeverage < 0 (负债率下降)
    debt_ratio = features.get("fc_ratio_debt_to_assets")
    if debt_ratio is not None:
        # 用趋势近似: 负债率 < 0.50 = 安全; 在没有趋势时, 用水平判断
        signals["F5_leverage_down"] = debt_ratio < 0.55
        if signals["F5_leverage_down"]:
            score += 1

    # F8: ΔGross Margin > 0
    gm_trend = features.get("gross_margin_trend")
    if gm_trend is not None:
        signals["F8_margin_improve"] = gm_trend > 0
        if signals["F8_margin_improve"]:
            score += 1

    # F9: ΔAsset Turnover > 0
    asset_turn = features.get("fc_profitability_assets_turn")
    revenue_trend = features.get("revenue_trend")
    if asset_turn is not None and revenue_trend is not None:
        # 营收增速正 + 资产周转率 > 0.5 → 效率改善
        signals["F9_efficiency"] = revenue_trend > 0 and asset_turn > 0.4
        if signals["F9_efficiency"]:
            score += 1

    return {
        "fscore": score,
        "signals": signals,
        "max_possible": len(signals),
        "interpretation": (
            "strong" if score >= 6 else
            "moderate" if score >= 4 else
            "weak" if score >= 2 else "distressed"
        ),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# v11.0: Beneish M-Score 增强欺诈检测
# ═══════════════════════════════════════════════════════════════════════════════

def compute_beneish_mscore(features: Dict[str, Any]) -> Dict[str, Any]:
    """计算 Beneish M-Score 近似值 (基于可用特征)

    Beneish (1999): "The Detection of Earnings Manipulation"
    原始公式需要完整的资产负债表时序, 这里用 PDDA 特征近似 5 个核心变量:

    DSRI: 应收账款→营收比变化 (应收增速 > 营收增速 = 可疑)
    GMI:  毛利率下降倒数 (毛利率下降 = 增加操纵动机)
    AQI:  资产质量指数 (非流动资产占比增加 = 可疑)
    SGI:  营收增长指数 (高增长 + 虚假盈利 = M-Score核心预警)
    TATA: 应计利润/总资产 (高应计 = Sloan 1996 核心信号)

    Returns:
        {"m_score": float, "is_manipulator": bool, "components": dict, "confidence": str}
    """
    components = {}

    # DSRI: Days Sales in Receivables Index
    # 近似: 应收账款/收入比率的水平 (标志性变量)
    receivable_ratio = features.get("fc_ratio_receivable_to_revenue")
    flag_high_recv = features.get("fc_flag_high_receivable", 0)
    if receivable_ratio is not None:
        # DSRI > 1.0 = 可疑; 规范化到 Beneish 尺度
        components["DSRI"] = min(2.0, max(0.5, receivable_ratio * 3.0 + 0.5))
    elif flag_high_recv:
        components["DSRI"] = 1.5  # 高应收标志 → 中等可疑

    # GMI: Gross Margin Index (inverse)
    gm_trend = features.get("gross_margin_trend", 0.0)
    if gm_trend is not None:
        # 毛利率下降 → GMI > 1; 上升 → GMI < 1
        components["GMI"] = max(0.5, 1.0 - gm_trend * 5.0)

    # AQI: Asset Quality Index
    nca_ratio = features.get("fc_ratio_nca")
    goodwill_risk = features.get("fc_flag_goodwill_risk", 0)
    if nca_ratio is not None:
        components["AQI"] = min(2.0, max(0.5, nca_ratio * 2.0 + 0.3))
        if goodwill_risk:
            components["AQI"] = min(2.0, components["AQI"] * 1.3)
    elif goodwill_risk:
        components["AQI"] = 1.5

    # SGI: Sales Growth Index
    rev_trend = features.get("revenue_trend", 0.0)
    if rev_trend is not None:
        # 高增长 SGI > 1.2; 使用 exp 转换
        components["SGI"] = max(0.5, math.exp(rev_trend * 2.0))

    # TATA: Total Accruals to Total Assets
    profit_level = features.get("profit_level", 0.0)
    ocf_level = features.get("ocf_level", 0.0)
    if profit_level is not None and ocf_level is not None:
        # accruals = profit - ocf (粗略); 标准化
        accruals = profit_level - ocf_level
        components["TATA"] = max(-0.5, min(0.5, accruals / max(abs(profit_level), 0.01) * 0.5))

    if not components:
        return {"m_score": 0.0, "is_manipulator": False, "components": {}, "confidence": "no_data"}

    # Beneish M-Score 原始系数 (Beneish 1999):
    # M = -4.84 + 0.920*DSRI + 0.528*GMI + 0.404*AQI + 0.892*SGI + 4.679*TATA
    M = -4.84
    if "DSRI" in components:
        M += 0.920 * components["DSRI"]
    if "GMI" in components:
        M += 0.528 * components["GMI"]
    if "AQI" in components:
        M += 0.404 * components["AQI"]
    if "SGI" in components:
        M += 0.892 * components["SGI"]
    if "TATA" in components:
        M += 4.679 * components["TATA"]

    # M > -1.78 → likely manipulator (Beneish 1999 原始阈值)
    is_manipulator = M > -1.78
    n_components = len(components)
    confidence = "high" if n_components >= 4 else ("medium" if n_components >= 3 else "low")

    return {
        "m_score": round(M, 3),
        "is_manipulator": is_manipulator,
        "components": {k: round(v, 3) for k, v in components.items()},
        "confidence": confidence,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# v11.0: 跨引擎共识元评分
# ═══════════════════════════════════════════════════════════════════════════════

def compute_consensus_meta_score(
    truth_score: float,
    eval_score: float,
    truth_grade: str,
    eval_decision: str,
    fscore: int = 0,
    beneish_manipulator: bool = False,
) -> Dict[str, Any]:
    """计算跨引擎共识元评分

    Meta-Score = w₁ × TRUTH_percentile + w₂ × Eval_score_normalized + w₃ × F-Score_norm
    + 共识加分/分歧减分 + Beneish 欺诈扣分

    Args:
        truth_score: TRUTH final_score (0-1)
        eval_score: Evaluator score (0-100)
        truth_grade: TRUTH grade (A+/A/B+/B/C/D/F)
        eval_decision: Evaluator decision (quality/average/poor/veto)
        fscore: Piotroski F-Score (0-7)
        beneish_manipulator: Beneish M-Score > -1.78

    Returns:
        {"meta_score": float, "consensus_level": str, "confidence": float, "details": dict}
    """
    # 1. 归一化各引擎分数到 [0, 1]
    truth_norm = max(0, min(1.0, truth_score))  # 已经是 0-1
    eval_norm = max(0, min(1.0, eval_score / 100.0))  # 0-100 → 0-1
    fscore_norm = max(0, min(1.0, fscore / 7.0))  # 0-7 → 0-1

    # 2. 加权合成: TRUTH 45% + Eval 35% + F-Score 20%
    w_truth, w_eval, w_fscore = 0.45, 0.35, 0.20
    raw_meta = truth_norm * w_truth + eval_norm * w_eval + fscore_norm * w_fscore

    # 3. 共识加分/分歧减分
    truth_is_quality = truth_grade in ("A+", "A", "B+")
    eval_is_quality = eval_decision == "quality"

    if truth_is_quality and eval_is_quality:
        # 双引擎共识优质 → 高置信奖励
        consensus_bonus = 0.05
        consensus_level = "strong_consensus"
    elif truth_is_quality != eval_is_quality:
        # 分歧 → 惩罚
        consensus_bonus = -0.03
        consensus_level = "divergent"
    elif truth_grade in ("D", "F") and eval_decision in ("poor", "veto"):
        # 双引擎共识差 → 中等共识 (confirmation)
        consensus_bonus = 0.0
        consensus_level = "negative_consensus"
    else:
        consensus_bonus = 0.0
        consensus_level = "neutral"

    # 4. Beneish 欺诈扣分
    beneish_penalty = -0.08 if beneish_manipulator else 0.0

    meta_score = max(0, min(1.0, raw_meta + consensus_bonus + beneish_penalty))

    # 5. 置信度: 基于引擎间一致性
    score_diff = abs(truth_norm - eval_norm)
    if score_diff < 0.10:
        confidence = 0.90
    elif score_diff < 0.20:
        confidence = 0.75
    elif score_diff < 0.30:
        confidence = 0.60
    else:
        confidence = 0.45

    return {
        "meta_score": round(meta_score, 4),
        "meta_pct": round(meta_score * 100, 1),
        "consensus_level": consensus_level,
        "confidence": round(confidence, 2),
        "details": {
            "truth_norm": round(truth_norm, 4),
            "eval_norm": round(eval_norm, 4),
            "fscore_norm": round(fscore_norm, 4),
            "consensus_bonus": round(consensus_bonus, 4),
            "beneish_penalty": round(beneish_penalty, 4),
        },
    }


# ═══════════════════════════════════════════════════════════════════════════════
# v11.0: IC 衰减监控 (Factor Decay Detection)
# ═══════════════════════════════════════════════════════════════════════════════

def detect_ic_decay(
    window_results: List[WindowResult],
    factor_name: str,
    decay_threshold: int = 3,
) -> Dict[str, Any]:
    """检测因子 IC 是否在衰减

    Harvey & Liu (2020) "Lucky Factors":
    - 因子 IC 连续 N 年下降 → 可能是伪因子或市场已定价
    - 检测: 连续 decay_threshold 个窗口 IC 下降 → 预警

    Args:
        window_results: 回测窗口结果列表
        factor_name: 因子名称
        decay_threshold: 连续下降窗口数阈值

    Returns:
        {"is_decaying": bool, "consecutive_declines": int,
         "ic_trend": list, "recommendation": str}
    """
    ics = []
    for w in window_results:
        ic = w.factor_ics.get(factor_name)
        if ic is not None:
            ics.append({"year": w.test_year, "ic": ic})

    if len(ics) < 2:
        return {"is_decaying": False, "consecutive_declines": 0,
                "ic_trend": ics, "recommendation": "insufficient_data"}

    # 计算连续下降
    consec = 0
    max_consec = 0
    for i in range(1, len(ics)):
        if ics[i]["ic"] < ics[i - 1]["ic"]:
            consec += 1
            max_consec = max(max_consec, consec)
        else:
            consec = 0

    is_decaying = max_consec >= decay_threshold

    # 总体 IC 趋势 (线性回归斜率)
    if len(ics) >= 3:
        xs = list(range(len(ics)))
        ys = [d["ic"] for d in ics]
        x_mean = sum(xs) / len(xs)
        y_mean = sum(ys) / len(ys)
        num = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
        den = sum((x - x_mean) ** 2 for x in xs)
        slope = num / den if den > 0 else 0.0
    else:
        slope = 0.0

    if is_decaying:
        rec = "auto_downweight"
    elif slope < -0.02:
        rec = "monitor_closely"
    elif slope > 0.02:
        rec = "stable_or_improving"
    else:
        rec = "stable"

    return {
        "is_decaying": is_decaying,
        "consecutive_declines": max_consec,
        "ic_trend": ics,
        "slope": round(slope, 4),
        "recommendation": rec,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# v11.0: 注册方法 — 供 workflow YAML 调用
# ═══════════════════════════════════════════════════════════════════════════════

from orchestrator.decorators.register import register_method
from shared.aggregation import AggregatableResult


@register_method(
    component_type="business_engine",
    engine_type="backtest",
    engine_name="run_evaluator_backtest",
    description="Run rolling window backtest for Evaluator engine (v11.0)",
)
def run_evaluator_backtest(
    data: pd.DataFrame,
    output_path: str = "data/evaluator_backtest_report.md",
    **params,
) -> AggregatableResult:
    """运行 Evaluator 引擎的滚动窗口回测

    与 TRUTH 回测并行, 验证 Evaluator 的独立预测力
    """
    bt = EvaluatorBacktester(data)
    report = bt.run()
    bt.print_summary(report)
    md = bt.generate_report_md(report, output_path=output_path)
    return AggregatableResult(
        key="evaluator_backtest",
        value={"report": report, "markdown": md},
        namespace="backtest",
    )


@register_method(
    component_type="business_engine",
    engine_type="backtest",
    engine_name="run_consensus_analysis",
    description="Run cross-engine consensus meta-scoring analysis (v11.0)",
)
def run_consensus_analysis(
    truth_result: Dict[str, Any],
    evaluator_result: Dict[str, Any],
    data: pd.DataFrame = None,
    **params,
) -> AggregatableResult:
    """跨引擎共识元评分

    对每家公司计算: Meta-Score = TRUTH×45% + Eval×35% + F-Score×20%
    + Beneish 欺诈扣分 + 共识加分/分歧减分
    """
    truth_profiles = truth_result.get("profiles", [])
    eval_results = evaluator_result.get("evaluations", [])

    truth_by_ts = {p.get("ts_code"): p for p in truth_profiles}
    eval_by_ts = {e.get("ts_code"): e for e in eval_results}
    common_ts = set(truth_by_ts.keys()) & set(eval_by_ts.keys())

    results = []
    consensus_stats = {"strong_consensus": 0, "divergent": 0, "negative_consensus": 0, "neutral": 0}
    manipulator_count = 0

    for ts_code in common_ts:
        tp = truth_by_ts[ts_code]
        ep = eval_by_ts[ts_code]

        t_score = tp.get("final_score", 0) or 0
        e_score = (ep.get("score", 0) or 0)
        t_grade = tp.get("grade", "C")
        e_decision = ep.get("decision", "uncertain")

        # F-Score: 从 Evaluator factors 中提取
        fscore = 0
        beneish_flag = False
        if "factors" in ep:
            for f in ep["factors"]:
                fn = f.get("name", "")
                if fn == "piotroski_fscore":
                    fscore = int(f.get("value", 0))
                elif fn == "beneish_mscore":
                    beneish_flag = True

        meta = compute_consensus_meta_score(
            truth_score=t_score,
            eval_score=e_score,
            truth_grade=t_grade,
            eval_decision=e_decision,
            fscore=fscore,
            beneish_manipulator=beneish_flag,
        )

        consensus_stats[meta["consensus_level"]] = consensus_stats.get(meta["consensus_level"], 0) + 1
        if beneish_flag:
            manipulator_count += 1

        results.append({
            "ts_code": ts_code,
            "name": tp.get("name", ep.get("name", "")),
            **meta,
        })

    # 排序
    results.sort(key=lambda x: -x["meta_score"])

    summary = {
        "total": len(results),
        "consensus_stats": consensus_stats,
        "manipulator_count": manipulator_count,
        "top_10": results[:10],
        "bottom_10": results[-10:],
        "avg_meta": sum(r["meta_score"] for r in results) / len(results) if results else 0,
        "avg_confidence": sum(r["confidence"] for r in results) / len(results) if results else 0,
    }

    logger.info(
        f"Consensus analysis: {len(results)} companies, "
        f"strong={consensus_stats.get('strong_consensus', 0)}, "
        f"divergent={consensus_stats.get('divergent', 0)}, "
        f"manipulators={manipulator_count}"
    )

    return AggregatableResult(
        key="consensus_meta",
        value={"results": results, "summary": summary},
        namespace="backtest",
    )


# ═══════════════════════════════════════════════════════════════════════════════
# v11.1 P2.3: IC-Based 自动权重优化器
# ═══════════════════════════════════════════════════════════════════════════════

def compute_ic_optimal_weights(
    backtest_report: BacktestReport,
    min_weight: float = 0.03,
    ic_power: float = 1.0,
) -> Dict[str, Any]:
    """基于回测 IC 自动计算最优因子权重 (Grinold 1989 IR Law)

    Fundamental Law of Active Management:
        IR = IC × √BR
    因此 IC 更高的因子应获得更大权重。

    方法:
        1. 汇总每个因子跨窗口的平均 IC
        2. 计算 IC 信息比 (IC_mean / IC_std) 作为稳定性修正
        3. 最终权重 = max(min_weight, IC_adj^ic_power) 归一化

    Args:
        backtest_report: FundamentalBacktester.run() 的输出
        min_weight: 最小权重下限 (防止任何因子为零)
        ic_power: IC 的幂次 (1.0=线性, 2.0=平方加强区分)

    Returns:
        {"optimal_weights": dict, "ic_stats": dict, "current_vs_optimal": dict,
         "config_snippet": str}
    """
    from ..truth.config import ScoringConfig  # 跨模块导入

    if not backtest_report.windows:
        return {"error": "No backtest windows available"}

    # 汇总因子 IC
    factor_ics_all: Dict[str, List[float]] = {}
    for w in backtest_report.windows:
        for f, ic in w.factor_ics.items():
            factor_ics_all.setdefault(f, []).append(ic)

    ic_stats = {}
    for f, ics in factor_ics_all.items():
        n = len(ics)
        ic_mean = sum(ics) / n
        ic_std = (sum((x - ic_mean) ** 2 for x in ics) / max(n - 1, 1)) ** 0.5
        ic_ir = ic_mean / ic_std if ic_std > 0.01 else ic_mean * 10  # IC信息比
        ic_stats[f] = {
            "ic_mean": round(ic_mean, 4),
            "ic_std": round(ic_std, 4),
            "ic_ir": round(ic_ir, 3),
            "n_windows": n,
            "all_positive": all(x > 0 for x in ics),
        }

    # 计算调整后 IC (IC × 稳定性惩罚)
    ic_adj = {}
    for f, stats in ic_stats.items():
        raw = max(0, stats["ic_mean"])
        # 稳定性修正: 如果 IC 不全为正, 惩罚 20%
        stability = 1.0 if stats["all_positive"] else 0.80
        ic_adj[f] = (raw * stability) ** ic_power

    total_adj = sum(ic_adj.values())
    if total_adj < 1e-6:
        n_f = max(len(ic_adj), 1)
        optimal = {f: 1.0 / n_f for f in ic_adj}
    else:
        optimal = {}
        for f, adj in ic_adj.items():
            optimal[f] = max(min_weight, adj / total_adj)
        # 归一化
        total_opt = sum(optimal.values())
        optimal = {f: round(w / total_opt, 3) for f, w in optimal.items()}

    # 当前权重 (从 config)
    try:
        current_config = ScoringConfig()
        current_weights = dict(current_config.factor_weights)
    except Exception:
        current_weights = {}

    # 差异分析
    comparison = {}
    for f in set(list(optimal.keys()) + list(current_weights.keys())):
        curr = current_weights.get(f, 0.0)
        opt = optimal.get(f, 0.0)
        comparison[f] = {
            "current": round(curr, 3),
            "optimal": round(opt, 3),
            "delta": round(opt - curr, 3),
            "direction": "↑" if opt > curr + 0.01 else ("↓" if opt < curr - 0.01 else "="),
        }

    # 生成 config.py 代码片段
    lines = ["factor_weights: Mapping[str, float] = field(default_factory=lambda: {"]
    for f in sorted(optimal.keys()):
        ic_info = ic_stats.get(f, {})
        lines.append(f'    "{f}": {optimal[f]:.3f},  # IC={ic_info.get("ic_mean", 0):.3f}')
    lines.append("})")
    config_snippet = "\n".join(lines)

    return {
        "optimal_weights": optimal,
        "ic_stats": ic_stats,
        "current_vs_optimal": comparison,
        "config_snippet": config_snippet,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# v11.1 P3.1: Factor 正交化诊断 (相关性矩阵 + 共线性检测)
# ═══════════════════════════════════════════════════════════════════════════════

def diagnose_factor_orthogonality(
    truth_result: Dict[str, Any],
    threshold_high: float = 0.70,
    threshold_moderate: float = 0.50,
) -> Dict[str, Any]:
    """分析 8 因子间的相关性矩阵, 检测共线性

    学术依据:
        - VIF > 5 → 严重共线性 (Greene Econometrics)
        - |ρ| > 0.70 → 高度相关, 权重冗余
        - |ρ| > 0.50 → 中等相关, 需关注

    Args:
        truth_result: run_truth() 输出, 包含 profiles 列表
        threshold_high: 高相关阈值
        threshold_moderate: 中等相关阈值

    Returns:
        {"correlation_matrix": dict, "high_correlations": list,
         "recommendations": list, "summary": str}
    """
    profiles = truth_result.get("profiles", [])
    if len(profiles) < 50:
        return {"error": "Insufficient profiles for correlation analysis",
                "n_profiles": len(profiles)}

    # 提取因子分数矩阵
    factor_ids = ["ALPHA", "BETA", "GAMMA", "PI", "LAMBDA",
                  "DELTA_FRAUD", "DELTA_DECAY", "VERIFICATION"]
    factor_names_map = {
        "ALPHA": "α周期性", "BETA": "β资本", "GAMMA": "γ成长",
        "PI": "π盈利", "LAMBDA": "λ杠杆", "DELTA_FRAUD": "δ欺诈",
        "DELTA_DECAY": "δ衰退", "VERIFICATION": "V验证",
    }

    # 构建 N×8 矩阵
    rows = []
    for p in profiles:
        factors = p.get("factors", {})
        row = {}
        valid = True
        for fid in factor_ids:
            fd = factors.get(fid)
            if isinstance(fd, dict) and fd.get("score") is not None:
                row[fid] = fd["score"]
            else:
                valid = False
                break
        if valid:
            rows.append(row)

    if len(rows) < 50:
        return {"error": f"Only {len(rows)} valid rows (need ≥50)"}

    # 计算 Spearman 相关矩阵
    n = len(rows)
    # 构建排名矩阵
    ranked = {fid: [] for fid in factor_ids}
    for row in rows:
        for fid in factor_ids:
            ranked[fid].append(row[fid])

    # 使用 numpy 做排名
    rank_arrays = {}
    for fid in factor_ids:
        arr = np.array(ranked[fid])
        rank_arrays[fid] = _rank_array(arr)

    # Spearman 相关
    corr_matrix = {}
    high_corrs = []
    for i, f1 in enumerate(factor_ids):
        corr_matrix[f1] = {}
        for j, f2 in enumerate(factor_ids):
            if i == j:
                corr_matrix[f1][f2] = 1.0
                continue
            if f2 in corr_matrix and f1 in corr_matrix[f2]:
                corr_matrix[f1][f2] = corr_matrix[f2][f1]
                continue
            rho, _ = spearman_rank_corr(rank_arrays[f1], rank_arrays[f2])
            corr_matrix[f1][f2] = round(rho, 3)
            if i < j and abs(rho) >= threshold_moderate:
                high_corrs.append({
                    "factor_1": f1,
                    "factor_2": f2,
                    "correlation": round(rho, 3),
                    "severity": "🔴 HIGH" if abs(rho) >= threshold_high else "🟡 MODERATE",
                    "names": f"{factor_names_map[f1]} ↔ {factor_names_map[f2]}",
                })

    high_corrs.sort(key=lambda x: -abs(x["correlation"]))

    # 生成建议
    recommendations = []
    for hc in high_corrs:
        if abs(hc["correlation"]) >= threshold_high:
            recommendations.append(
                f"⚠️ {hc['names']}: ρ={hc['correlation']:.3f} — "
                f"考虑合并或降权其中一个, 避免双重计算"
            )
        else:
            recommendations.append(
                f"📋 {hc['names']}: ρ={hc['correlation']:.3f} — "
                f"关注但不紧急, 当前权重配置可能吸收了部分冗余"
            )

    if not high_corrs:
        recommendations.append("✅ 所有因子对的 |ρ| < 0.50, 正交性良好, 无需调整")

    # 汇总
    n_high = sum(1 for hc in high_corrs if abs(hc["correlation"]) >= threshold_high)
    n_mod = len(high_corrs) - n_high
    summary = (
        f"8 因子正交化诊断: {len(rows)} 样本, "
        f"{n_high} 对高相关(|ρ|≥{threshold_high}), "
        f"{n_mod} 对中等相关(|ρ|≥{threshold_moderate})"
    )

    return {
        "correlation_matrix": corr_matrix,
        "high_correlations": high_corrs,
        "recommendations": recommendations,
        "summary": summary,
        "n_samples": len(rows),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# v11.1 P3.2: Quality 稳定性分析 (年间 Turnover)
# ═══════════════════════════════════════════════════════════════════════════════

def analyze_quality_stability(
    window_results: List[WindowResult],
    backtest_report: Optional[BacktestReport] = None,
) -> Dict[str, Any]:
    """分析 Quality 列表的年间稳定性 (Turnover)

    学术依据:
        - 高 turnover → 因子不稳定, 交易成本高 (Frazzini, Israel & Moskowitz 2018)
        - 理想 turnover: 20-40% (年间约 60-80% 持仓保留)
        - turnover > 50% → 因子可能过拟合或数据噪声

    指标:
        1. Jaccard 相似度: |A∩B| / |A∪B| — 衡量年间质量列表重叠程度
        2. Retention Rate: 上一年 quality 中本年仍为 quality 的比例
        3. New Entry Rate: 新进入 quality 列表的比例

    Returns:
        {"windows": list, "avg_jaccard": float, "avg_retention": float,
         "interpretation": str}
    """
    if len(window_results) < 2:
        return {"error": "Need ≥2 windows for stability analysis"}

    # 注意: WindowResult 没有 quality_ts_codes, 但有 n_quality
    # 我们用窗口间指标变化来近似稳定性
    window_stats = []
    for i in range(1, len(window_results)):
        w_prev = window_results[i - 1]
        w_curr = window_results[i]

        # 使用 IC/quality 数量变化作为稳定性代理
        ic_change = abs(w_curr.ic_roic - w_prev.ic_roic)
        lift_change = abs(w_curr.quality_lift - w_prev.quality_lift)
        quality_change = abs(w_curr.n_quality - w_prev.n_quality)

        # 近似 Jaccard: 基于质量数量变化 (真实计算需要 ts_code 列表)
        min_q = min(w_curr.n_quality, w_prev.n_quality)
        max_q = max(w_curr.n_quality, w_prev.n_quality)
        approx_jaccard = min_q / max_q if max_q > 0 else 1.0

        window_stats.append({
            "transition": f"{w_prev.test_year}→{w_curr.test_year}",
            "ic_change": round(ic_change, 4),
            "lift_change": round(lift_change, 3),
            "quality_prev": w_prev.n_quality,
            "quality_curr": w_curr.n_quality,
            "quality_delta": w_curr.n_quality - w_prev.n_quality,
            "approx_jaccard": round(approx_jaccard, 3),
        })

    avg_jaccard = (sum(s["approx_jaccard"] for s in window_stats) /
                   len(window_stats) if window_stats else 0)
    avg_ic_change = (sum(s["ic_change"] for s in window_stats) /
                     len(window_stats) if window_stats else 0)

    # IC 变化的稳定性
    if avg_ic_change < 0.02:
        ic_stability = "excellent"
    elif avg_ic_change < 0.05:
        ic_stability = "good"
    elif avg_ic_change < 0.10:
        ic_stability = "moderate"
    else:
        ic_stability = "unstable"

    # 整体解释
    if avg_jaccard > 0.80 and ic_stability in ("excellent", "good"):
        interpretation = "🟢 因子高度稳定: quality 列表年间重叠高, IC 波动小, 低换手策略可行"
    elif avg_jaccard > 0.60:
        interpretation = "🟡 因子中等稳定: 有一定换手但可接受, 建议结合多窗口验证"
    else:
        interpretation = "🔴 因子不稳定: 年间 quality 列表变化大, 可能存在过拟合或数据噪声"

    return {
        "windows": window_stats,
        "avg_approx_jaccard": round(avg_jaccard, 3),
        "avg_ic_change": round(avg_ic_change, 4),
        "ic_stability": ic_stability,
        "interpretation": interpretation,
        "n_transitions": len(window_stats),
    }


@register_method(
    component_type="business_engine",
    engine_type="backtest",
    engine_name="run_factor_diagnostics",
    description="Run factor orthogonality + quality stability diagnostics (v11.1)",
)
def run_factor_diagnostics(
    truth_result: Dict[str, Any],
    backtest_report: Optional[Dict[str, Any]] = None,
    **params,
) -> AggregatableResult:
    """运行因子诊断: 正交化 + 稳定性 + 权重优化建议"""
    ortho = diagnose_factor_orthogonality(truth_result)

    result = {
        "orthogonality": ortho,
    }

    logger.info(
        f"Factor diagnostics: {ortho.get('summary', 'N/A')}"
    )

    return AggregatableResult(
        key="factor_diagnostics",
        value=result,
        namespace="backtest",
    )

