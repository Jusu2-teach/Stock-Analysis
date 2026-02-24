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
