"""
═══════════════════════════════════════════════════════════════════════════════
AStock Evaluators — Causal Bayesian 评估引擎
═══════════════════════════════════════════════════════════════════════════════

核心组件:
    1. PDDA 特征提取 (来自 trend probes 的聚合数据)
    2. 声明式规则引擎 (29 条规则, 5 独立因子组 VETO)
    3. 自适应行业阈值
    4. 加权评分 + 行业 z-score 归一化
    5. 生命周期推断 (确定性函数)
    6. 可解释性报告

Pipeline 集成:
    - 输入: aggregated_trends: Dict[str, pd.DataFrame] (来自 PDDA)
    - 输出: Dict[str, Any] 包含评估结果和解释
═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# 保留的子模块
from .adaptive_threshold import (
    AdaptiveThresholdEngine,
    AdaptiveContext,
)
from .explanation import (
    DecisionExplainer,
    DecisionType,
    Factor,
    ExplanationResult,
)
from .rule_engine import (
    RuleEngine,
    RuleEngineResult,
)

# Orchestrator 注册
try:
    from orchestrator.decorators.register import register_method

    HAS_ORCHESTRATOR = True
except ImportError:
    HAS_ORCHESTRATOR = False

    def register_method(**kwargs):
        def decorator(func):
            return func
        return decorator

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# PDDA 列名映射
# ═══════════════════════════════════════════════════════════════════════════════

from ..pdda_columns import PDDAColumns
from shared.naming_convention import MetricRegistry


# ═══════════════════════════════════════════════════════════════════════════════
# 配置 (v3.0: 删除所有不再使用的选项)
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class EvaluatorConfig:
    """评估器配置 v3.0 — 仅保留有效选项"""

    # 阈值调整
    use_adaptive_thresholds: bool = True

    # 规则引擎
    use_rule_engine: bool = True
    rule_veto_enabled: bool = True

    # 评分权重 — 8 个核心指标
    # v13.1: IC-OPTIMAL WEIGHTS (Grinold 1989, 数据驱动)
    # 由 v13.0 calibrate_evaluator_weights() 回测校准:
    #   - 5 年滚动窗口 × Spearman IC(指标水平, 未来ROIC)
    #   - Grinold 权重 = IC_i / Σ max(0, IC_j), floor=3%
    #
    # IC 证据 (v13.0 回测):
    #   roic:    IC=+0.716  → w=0.199  (核心质量, 仍是最高 IC)
    #   roe:     IC=+0.709  → w=0.197  (v13.1 ↑↑ 0.08→0.20: IC证明ROE预测力被严重低估)
    #   profit:  IC=+0.590  → w=0.164  (v13.1 ↑ 0.10→0.16: EPS预测力强)
    #   net_m:   IC=+0.526  → w=0.146  (v13.1 ↑ 0.10→0.15: 净利率预测力被低估)
    #   ocf:     IC=+0.421  → w=0.117  (v13.1 ↓ 0.14→0.12: 现金流IC不如预期)
    #   revenue: IC=+0.330  → w=0.092  (v13.1 ↓ 0.12→0.09: 营收预测力偏弱)
    #   gross_m: IC=+0.206  → w=0.057  (v13.1 ↓↓ 0.14→0.06: IC远低于手调预期)
    #   roiic:   IC=+0.000  → w=0.029  (v13.1 ↓ 0.10→0.03: 派生指标IC最弱)
    score_weights: Dict[str, float] = field(default_factory=lambda: {
        "roic_trend": 0.199,         # v13.1 IC=+0.716: 投入资本回报率=核心质量
        "roe_trend": 0.197,          # v13.1 ↑↑ 0.08→0.20: IC证实ROE预测力极强
        "profit_trend": 0.164,       # v13.1 ↑ 0.10→0.16: EPS预测力
        "net_margin_trend": 0.146,   # v13.1 ↑ 0.10→0.15: 净利率预测力
        "ocf_trend": 0.117,          # v13.1 ↓ 0.14→0.12: 现金流
        "revenue_trend": 0.092,      # v13.1 ↓ 0.12→0.09: 营收
        "gross_margin_trend": 0.057, # v13.1 ↓↓ 0.14→0.06: 毛利率IC偏弱
        "roiic_trend": 0.029,        # v13.1 ↓ 0.10→0.03: ROIIC派生指标
    })

    # 决策阈值
    quality_threshold: float = 72.0
    average_threshold: float = 50.0

    # 趋势特征到评分指标的映射
    trend_to_score_mapping: Dict[str, str] = field(default_factory=lambda: {
        "roic": "roic_trend",
        "roe": "roe_trend",
        "revenue": "revenue_trend",
        "gross_margin": "gross_margin_trend",
        "net_margin": "net_margin_trend",
        "ocf": "ocf_trend",
        "roiic": "roiic_trend",
        "profit": "profit_trend",
    })


# ═══════════════════════════════════════════════════════════════════════════════
# 生命周期推断 — 确定性函数替代伪 HMM
# ═══════════════════════════════════════════════════════════════════════════════

def _infer_lifecycle(features: Dict[str, Any]) -> Tuple[str, float]:
    """
    确定性生命周期推断 — 替代旧 v2.0 的 500 行伪 HMM

    基于 revenue 趋势 + ROIC 水平 + 波动性 三个维度判断:
    - emerging:         高增长 + 低/负 ROIC
    - growth:           高增长 + 正 ROIC
    - mature:           稳定增长 + 高 ROIC
    - cash_cow:         低增长 + 高 ROIC + 低波动
    - slowing:          减速中 + 正 ROIC
    - declining:        负增长 + ROIC 下降
    - turnaround:       有拐点回升信号
    - distressed:       严重恶化

    Returns: (state_name, confidence)
    """
    rev_trend = features.get("revenue_trend", 0.0)
    roic_level = features.get("roic_level", 8.0)
    roic_trend = features.get("roic_trend", 0.0)
    rev_volatility = features.get("revenue_volatility", 0.2)
    has_inflection = features.get("revenue_has_inflection", False)
    rev_recent = features.get("revenue_recent_trend", 0.0)
    roic_has_det = features.get("roic_has_deterioration", False)

    # 困境反转: 有拐点 + 近期改善
    if has_inflection and rev_recent > rev_trend and rev_recent > 0:
        return "turnaround", 0.70

    # 严重困境
    if roic_level < 2 and roic_trend < -0.05 and roic_has_det:
        return "distressed", 0.80

    # 高增长阶段
    if rev_trend > 0.10:
        if roic_level > 10:
            return "growth", 0.75
        else:
            return "emerging", 0.65

    # 稳定成熟
    if roic_level > 12 and abs(rev_trend) < 0.05:
        if rev_volatility < 0.20:
            return "cash_cow", 0.75
        return "mature", 0.70

    # 减速
    if rev_trend < roic_trend and rev_trend > -0.03 and roic_level > 5:
        return "slowing", 0.60

    # 衰退
    if rev_trend < -0.03 or (roic_trend < -0.03 and roic_has_det):
        return "declining", 0.70

    # 默认成熟
    return "mature", 0.50


# ═══════════════════════════════════════════════════════════════════════════════
# 评估结果
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class CompanyEvaluation:
    """单个公司的评估结果 v3.0"""

    ts_code: str
    name: Optional[str] = None
    industry: Optional[str] = None

    # 核心评估
    score: float = 0.0
    decision: DecisionType = DecisionType.UNCERTAIN
    confidence: float = 0.0

    # 生命周期
    company_state: Optional[str] = None
    state_confidence: float = 0.0

    # 因素分析
    factors: List[Factor] = field(default_factory=list)

    # 规则引擎结果
    rule_result: Optional[RuleEngineResult] = None
    vetoed: bool = False
    veto_reason: str = ""

    # 解释
    explanation: Optional[ExplanationResult] = None

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典 — 保持 API 向后兼容"""
        return {
            "ts_code": self.ts_code,
            "name": self.name,
            "industry": self.industry,
            "score": self.score,
            "decision": self.decision.value,
            "confidence": self.confidence,
            "company_state": self.company_state,
            "state_confidence": self.state_confidence,
            "vetoed": self.vetoed,
            "veto_reason": self.veto_reason,
            "factors": [
                {
                    "name": f.name,
                    "value": f.value,
                    "contribution": f.contribution,
                    "direction": f.direction,
                }
                for f in self.factors
            ],
            # 向后兼容: reporter 不消费这些, 但保留 key 以防万一
            "causal_diagnosis": None,
            "rule_engine": {
                "vetoed": self.rule_result.vetoed if self.rule_result else False,
                "total_penalty": self.rule_result.total_penalty if self.rule_result else 0,
                "total_bonus": self.rule_result.total_bonus if self.rule_result else 0,
                "strategies": self.rule_result.strategies if self.rule_result else [],
            } if self.rule_result else None,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# 主引擎
# ═══════════════════════════════════════════════════════════════════════════════

class CausalBayesianEvaluator:
    """
    v3.0 精简评估引擎

    Architecture:
        ┌─────────────────────────────────────────────────┐
        │           CausalBayesianEvaluator v3.0           │
        ├─────────────────────────────────────────────────┤
        │                                                 │
        │  Step 1: PDDA 特征提取 (40+ 维度/指标)          │
        │              │                                  │
        │  Step 2: 规则引擎 (YAML 声明式)                 │
        │     ├── Veto Rules  → 硬性否决 (≥3 共识)        │
        │     ├── Penalty     → 扣分 (≤50)                │
        │     └── Bonus       → 加分 (≤30)                │
        │              │                                  │
        │  Step 3: 加权评分 (行业自适应阈值)              │
        │              │                                  │
        │  Step 4: 生命周期推断 (确定性函数)              │
        │              │                                  │
        │  Step 5: 综合决策 + 排名                        │
        │              │                                  │
        │  Step 6: 可解释性报告                           │
        │              ▼                                  │
        │       CompanyEvaluation                         │
        └─────────────────────────────────────────────────┘
    """

    def __init__(self, config: Optional[EvaluatorConfig] = None):
        self.config = config or EvaluatorConfig()

        # 配置文件目录
        config_dir = Path(__file__).parent / "config"

        # 自适应阈值引擎
        threshold_config = config_dir / "adaptive_thresholds.yaml"
        if threshold_config.exists():
            self._threshold_engine = AdaptiveThresholdEngine.from_config(threshold_config)
        else:
            self._threshold_engine = AdaptiveThresholdEngine.with_defaults()

        # 规则引擎
        rules_config = config_dir / "rules.yaml"
        if rules_config.exists():
            self._rule_engine = RuleEngine.from_config(rules_config)
        else:
            self._rule_engine = RuleEngine.with_defaults()

        logger.info("CausalBayesianEvaluator v3.0 initialized (lean mode)")

    # ═══════════════════════════════════════════════════════════════════════
    # 公开接口
    # ═══════════════════════════════════════════════════════════════════════

    def evaluate_company(
        self,
        ts_code: str,
        trend_data: Dict[str, pd.DataFrame],
        company_info: Optional[Dict[str, Any]] = None,
    ) -> CompanyEvaluation:
        """
        评估单个公司 — v3.0 六步流程

        1. 提取 PDDA 特征
        2. 规则引擎 (veto/penalty/bonus)
        3. 加权评分
        4. 生命周期推断
        5. 综合决策
        6. 生成解释
        """
        company_info = company_info or {}
        company_trends = self._extract_company_trends(ts_code, trend_data)

        if not company_trends:
            return CompanyEvaluation(
                ts_code=ts_code,
                name=company_info.get("name"),
                decision=DecisionType.UNCERTAIN,
                confidence=0.0,
            )

        # Step 1: PDDA 特征提取
        features = self._extract_features_from_pdda(company_trends)

        # v12.0: 注入行业信息到特征字典 (供交互引擎使用)
        features["_industry"] = company_info.get("industry", "")

        # Step 2: 规则引擎
        context = self._create_adaptive_context(company_info)
        rule_result = self._run_rule_engine(features, context)

        if rule_result.vetoed and self.config.rule_veto_enabled:
            return CompanyEvaluation(
                ts_code=ts_code,
                name=company_info.get("name"),
                industry=company_info.get("industry"),
                score=0.0,
                decision=DecisionType.VETO,
                confidence=0.95,
                vetoed=True,
                veto_reason=rule_result.veto_reason,
                rule_result=rule_result,
                factors=[Factor(
                    name="veto_rule",
                    display_name="一票否决",
                    value=0.0,
                    contribution=-1.0,
                    direction="negative",
                    explanation=rule_result.veto_reason,
                )],
            )

        # Step 3: 加权评分
        score, factors = self._compute_score(features, rule_result, context)

        # Step 4: 生命周期推断
        lifecycle, life_conf = _infer_lifecycle(features)

        # Step 5: 综合决策 (v7.3: 传入features用于风险硬约束)
        decision, confidence = self._make_decision(score, rule_result, life_conf, features)

        # Step 6: 解释
        explanation = self._generate_explanation(
            ts_code, company_info, decision, confidence, factors, score
        )

        return CompanyEvaluation(
            ts_code=ts_code,
            name=company_info.get("name"),
            industry=company_info.get("industry"),
            score=score,
            decision=decision,
            confidence=confidence,
            company_state=lifecycle,
            state_confidence=life_conf,
            factors=factors,
            rule_result=rule_result,
            explanation=explanation,
        )

    # ═══════════════════════════════════════════════════════════════════════
    # Step 1: PDDA 特征提取 (保留 v2.0 逻辑, 经审计确认无问题)
    # ═══════════════════════════════════════════════════════════════════════

    def _extract_company_trends(
        self, ts_code: str, trend_data: Dict[str, pd.DataFrame]
    ) -> Dict[str, pd.DataFrame]:
        """提取单个公司的趋势数据"""
        company_trends = {}
        for metric, df in trend_data.items():
            if df is None or df.empty:
                continue
            if "ts_code" in df.columns:
                company_df = df[df["ts_code"] == ts_code]
                if not company_df.empty:
                    company_trends[metric] = company_df
            else:
                company_trends[metric] = df
        return company_trends

    def _extract_features_from_pdda(
        self, company_trends: Dict[str, pd.DataFrame]
    ) -> Dict[str, Any]:
        """
        从 PDDA 单行聚合结果直接提取特征

        保留 v2.0 全部提取逻辑 (经审计确认完整正确)
        """
        features: Dict[str, Any] = {}
        C = PDDAColumns

        for metric, df in company_trends.items():
            if df.empty:
                continue

            try:
                prefix = MetricRegistry.get_output_prefix(metric)
            except (ValueError, KeyError):
                prefix = metric

            row = df.iloc[0]

            # ===== 趋势特征 =====
            log_slope_col = C.col(prefix, C.LOG_SLOPE)
            slope_col = C.col(prefix, C.SLOPE)
            if log_slope_col in row.index and pd.notna(row[log_slope_col]):
                features[f"{metric}_trend"] = float(row[log_slope_col])
            elif slope_col in row.index and pd.notna(row[slope_col]):
                features[f"{metric}_trend"] = float(row[slope_col])
            else:
                features[f"{metric}_trend"] = 0.0

            r2_col = C.col(prefix, C.R_SQUARED)
            features[f"{metric}_r_squared"] = (
                float(row[r2_col]) if r2_col in row.index and pd.notna(row[r2_col]) else 0.0
            )

            mk_col = C.col(prefix, C.MK_TAU)
            features[f"{metric}_mk_tau"] = (
                float(row[mk_col]) if mk_col in row.index and pd.notna(row[mk_col]) else 0.0
            )

            dir_col = C.col(prefix, C.TREND_DIRECTION)
            features[f"{metric}_direction"] = (
                str(row[dir_col]) if dir_col in row.index else "flat"
            )

            recent_col = C.col(prefix, C.RECENT_3Y_SLOPE)
            features[f"{metric}_recent_trend"] = (
                float(row[recent_col]) if recent_col in row.index and pd.notna(row[recent_col]) else 0.0
            )

            # ===== 水平特征 =====
            latest_col = C.col(prefix, C.LATEST_VALUE)
            features[f"{metric}_level"] = (
                float(row[latest_col]) if latest_col in row.index and pd.notna(row[latest_col]) else 0.0
            )

            weighted_col = C.col(prefix, C.WEIGHTED_AVG)
            features[f"{metric}_weighted_avg"] = (
                float(row[weighted_col]) if weighted_col in row.index and pd.notna(row[weighted_col]) else 0.0
            )

            ratio_col = C.col(prefix, C.LATEST_VS_WEIGHTED)
            features[f"{metric}_latest_vs_weighted"] = (
                float(row[ratio_col]) if ratio_col in row.index and pd.notna(row[ratio_col]) else 1.0
            )

            # ===== 波动特征 =====
            cv_col = C.col(prefix, C.CV)
            features[f"{metric}_volatility"] = (
                float(row[cv_col]) if cv_col in row.index and pd.notna(row[cv_col]) else 0.2
            )

            vol_type_col = C.col(prefix, C.VOLATILITY_TYPE)
            features[f"{metric}_volatility_type"] = (
                str(row[vol_type_col]) if vol_type_col in row.index else "moderate"
            )

            # ===== 恶化检测 =====
            det_col = C.col(prefix, C.HAS_DETERIORATION)
            features[f"{metric}_has_deterioration"] = (
                bool(row[det_col]) if det_col in row.index else False
            )

            sev_col = C.col(prefix, C.DETERIORATION_SEVERITY)
            features[f"{metric}_deterioration_severity"] = (
                str(row[sev_col]) if sev_col in row.index else "none"
            )

            decline_col = C.col(prefix, C.TOTAL_DECLINE_PCT)
            raw_decline = (
                float(row[decline_col]) if decline_col in row.index and pd.notna(row[decline_col]) else 0.0
            )
            features[f"{metric}_decline_pct"] = abs(raw_decline) / 100.0

            # ===== 拐点/周期/加速/结构断点 =====
            infl_col = C.col(prefix, C.HAS_INFLECTION)
            features[f"{metric}_has_inflection"] = (
                bool(row[infl_col]) if infl_col in row.index else False
            )

            cyc_col = C.col(prefix, C.IS_CYCLICAL)
            features[f"{metric}_is_cyclical"] = (
                bool(row[cyc_col]) if cyc_col in row.index else False
            )

            phase_col = C.col(prefix, C.CURRENT_PHASE)
            features[f"{metric}_cycle_phase"] = (
                str(row[phase_col]) if phase_col in row.index else ""
            )

            acc_col = C.col(prefix, C.IS_ACCELERATING)
            features[f"{metric}_is_accelerating"] = (
                bool(row[acc_col]) if acc_col in row.index else False
            )

            dec_col = C.col(prefix, C.IS_DECELERATING)
            features[f"{metric}_is_decelerating"] = (
                bool(row[dec_col]) if dec_col in row.index else False
            )

            break_col = C.col(prefix, C.HAS_STRUCTURAL_BREAK)
            features[f"{metric}_has_break"] = (
                bool(row[break_col]) if break_col in row.index else False
            )

            regime_col = C.col(prefix, C.DATA_REGIME)
            features[f"{metric}_data_regime"] = (
                str(row[regime_col]) if regime_col in row.index else "stable"
            )

        # ═══ v7.4: 从 financial_context 提取资产结构特征 ═══
        # financial_context 由 trend/engine.py::build_financial_context 产生,
        # 包含 ratio_debt_to_assets, ratio_nca 等直接财务比率 (非 PDDA 标准列)
        fc_df = company_trends.get("financial_context")
        if fc_df is not None and not fc_df.empty:
            fc_row = fc_df.iloc[0]
            for fc_col in ["ratio_debt_to_assets", "ratio_nca", "ratio_receivable_to_revenue",
                           "flag_goodwill_risk", "flag_high_receivable",
                           "profitability_assets_turn", "profitability_gp_assets",
                           "profitability_roic_level", "profitability_roe_level"]:
                if fc_col in fc_row.index and pd.notna(fc_row[fc_col]):
                    features[f"fc_{fc_col}"] = float(fc_row[fc_col])

        return features

    # ═══════════════════════════════════════════════════════════════════════
    # Step 2: 规则引擎 (保留 v2.0 逻辑, 经审计确认核心有效)
    # ═══════════════════════════════════════════════════════════════════════

    def _create_adaptive_context(self, company_info: Dict[str, Any]) -> AdaptiveContext:
        """创建自适应阈值上下文"""
        return AdaptiveContext.from_company_info(
            industry_name=company_info.get("industry", "default"),
            market_cap=company_info.get("market_cap", 100.0),
            current_cycle=company_info.get("market_cycle", "expansion"),
        )

    def _run_rule_engine(
        self, features: Dict[str, Any], context: AdaptiveContext
    ) -> RuleEngineResult:
        """
        运行规则引擎 — 核心否决 + 扣分 + 加分

        对 4 个核心指标执行规则, ≥3 共识否决
        """
        if not self._rule_engine:
            return RuleEngineResult(base_score=100.0, final_score=100.0, grade="C")

        # 获取自适应阈值
        thresholds = {}
        try:
            for metric_name in ["roic_level", "roe_level", "gross_margin", "net_margin", "ocf_ratio", "revenue_growth"]:
                ts = self._threshold_engine.get_thresholds(metric_name, context)
                thresholds[f"{metric_name}_high"] = ts.excellent
                thresholds[f"{metric_name}_moat_min"] = ts.good
        except (ValueError, AttributeError):
            pass

        combined_result = RuleEngineResult(base_score=100.0, final_score=100.0, grade="C")
        veto_count = 0
        veto_reasons = []

        # v4.1: 规则引擎扩展至全8个指标 (原仅4个)
        for metric in ["roic", "roe", "gross_margin", "net_margin", "revenue", "ocf", "profit", "roiic"]:
            metric_features = self._build_metric_features(metric, features)

            result = self._rule_engine.evaluate(
                features=metric_features,
                metric_name=metric,
                thresholds=thresholds,
            )

            if result.vetoed:
                veto_count += 1
                veto_reasons.append(f"[{metric}] {result.veto_reason}")
                combined_result.veto_rules.extend(result.veto_rules)

            combined_result.penalty_rules.extend(result.penalty_rules)
            combined_result.bonus_rules.extend(result.bonus_rules)
            combined_result.strategies.extend(result.strategies)
            combined_result.total_penalty += result.total_penalty
            combined_result.total_bonus += result.total_bonus

        # v7.1: 去共线性的独立因子组否决
        # 高度相关指标分组: ROIC↔ROE(ρ≈0.8), Gross↔Net Margin(ρ≈0.6), Revenue↔Profit(ρ≈0.7)
        # 同一组内多个指标触发 veto 仅计 1 票, 避免单一基本面问题同时触发 4-6 票
        VETO_GROUPS = {
            "return":   ["roic", "roe"],          # 回报率组
            "margin":   ["gross_margin", "net_margin"],  # 利润率组
            "growth":   ["revenue", "profit"],     # 营收/利润增长组
            "cash":     ["ocf"],                   # 现金流组 (独立)
            "capital":  ["roiic"],                 # 增量资本效率组 (独立)
        }
        metric_to_group = {m: g for g, ms in VETO_GROUPS.items() for m in ms}
        vetoed_groups = set()
        for reason in veto_reasons:
            # veto_reasons format: "[metric] reason_text"
            metric_tag = reason.split("]")[0].strip("[")
            group = metric_to_group.get(metric_tag)
            if group:
                vetoed_groups.add(group)

        # ≥3 独立组共识否决 (5 组中真正多维度衰退)
        if len(vetoed_groups) >= 3:
            combined_result.vetoed = True
            combined_result.veto_reason = (
                f"多维度共识否决({len(vetoed_groups)}/{len(VETO_GROUPS)}组, "
                f"{veto_count}个指标): " + "; ".join(veto_reasons)
            )

        # Cap
        max_penalty = getattr(self._rule_engine, "_max_penalty", 50.0)
        max_bonus = getattr(self._rule_engine, "_max_bonus", 30.0)
        combined_result.total_penalty = min(combined_result.total_penalty, max_penalty)
        combined_result.total_bonus = min(combined_result.total_bonus, max_bonus)

        if not combined_result.vetoed:
            combined_result.final_score = max(
                0, min(100, 100 - combined_result.total_penalty + combined_result.total_bonus)
            )
            combined_result.grade = self._score_to_grade_str(combined_result.final_score)

        return combined_result

    def _build_metric_features(
        self, metric: str, features: Dict[str, Any]
    ) -> Dict[str, Any]:
        """为单个指标构建规则引擎特征字典"""
        mf: Dict[str, Any] = {}

        trend_key = f"{metric}_trend"
        if trend_key in features:
            mf["log_slope"] = features[trend_key]
            mf[trend_key] = features[trend_key]

        r2_key = f"{metric}_r_squared"
        if r2_key in features:
            mf["r_squared"] = features[r2_key]

        cv_key = f"{metric}_volatility"
        if cv_key in features:
            mf["cv"] = features[cv_key]

        level_key = f"{metric}_level"
        if level_key in features:
            mf["latest_value"] = features[level_key]

        det_key = f"{metric}_has_deterioration"
        if det_key in features:
            mf["has_deterioration"] = features[det_key]

        sev_key = f"{metric}_deterioration_severity"
        if sev_key in features:
            mf["deterioration_severity"] = features[sev_key]

        decline_key = f"{metric}_decline_pct"
        if decline_key in features:
            mf["decline_pct"] = features[decline_key]

        # 峰值下跌
        weighted_key = f"{metric}_weighted_avg"
        level_val = features.get(level_key, 0) if level_key else 0
        weighted_val = features.get(weighted_key, 0)
        if weighted_val > 0 and level_val < weighted_val:
            mf["peak_decline_pct"] = (weighted_val - level_val) / weighted_val
        else:
            mf["peak_decline_pct"] = 0.0

        cyc_key = f"{metric}_is_cyclical"
        if cyc_key in features:
            mf["is_cyclical"] = features[cyc_key]

        phase_key = f"{metric}_cycle_phase"
        if phase_key in features:
            mf["cycle_phase"] = features[phase_key]

        if weighted_key in features and level_key in features:
            level = features.get(level_key, 0) if level_key else 0
            weighted = features.get(weighted_key, level)
            mf["max_value"] = max(level, weighted * 1.5)

        # 连续亏损年数 (用于 consecutive_loss 规则)
        # v4.1.1: 从 raw_values 实际计算, 而非硬编码猜测
        raw_vals_key = f"{metric}_raw_values"
        if raw_vals_key in features and isinstance(features[raw_vals_key], (list, tuple)):
            raw_vals = features[raw_vals_key]
            # 从末尾往前数连续 <0 的年数
            loss_count = 0
            for v in reversed(raw_vals):
                if isinstance(v, (int, float)) and v < 0:
                    loss_count += 1
                else:
                    break
            mf["loss_year_count"] = loss_count
        else:
            # fallback: 用 level + weighted 的符号近似
            if level_val < 0 and weighted_val < 0:
                mf["loss_year_count"] = 4  # 两个均值都为负 → 大概率持续亏损
            elif level_val < 0:
                mf["loss_year_count"] = 1  # 仅当前为负 → 偶发
            else:
                mf["loss_year_count"] = 0

        # 近3年趋势 (用于 bonus 规则)
        recent_key = f"{metric}_recent_trend"
        if recent_key in features:
            mf["recent_3y_slope"] = features[recent_key]

        # 加速
        acc_key = f"{metric}_is_accelerating"
        if acc_key in features:
            mf["is_accelerating"] = features[acc_key]

        # 拐点
        infl_key = f"{metric}_has_inflection"
        if infl_key in features:
            mf["has_inflection"] = features[infl_key]

        # v4.3: 拐点类型 + 斜率变化 (fix: inflection_recovery 规则之前缺少这两个特征)
        infl_type_key = f"{metric}_inflection_type"
        if infl_type_key in features:
            mf["inflection_type"] = features[infl_type_key]
        slope_change_key = f"{metric}_slope_change"
        if slope_change_key in features:
            mf["slope_change"] = features[slope_change_key]

        # OCF 交叉引用 (用于 earnings_ocf_divergence 规则)
        mf["ref_ocf_slope"] = features.get("ocf_trend", 0.0)

        # v4.3: 使用实际数据计算连续下跌年数 (不再用严重度粗略映射)
        consec_key = f"{metric}_consecutive_decline_years"
        if consec_key in features and features[consec_key]:
            mf["consecutive_decline_years"] = int(features[consec_key])
        elif features.get(f"{metric}_has_deterioration", False):
            sev = features.get(f"{metric}_deterioration_severity", "none")
            mf["consecutive_decline_years"] = {
                "severe": 4, "catastrophic": 4, "moderate": 3, "mild": 2,
            }.get(sev, 0)
        else:
            mf["consecutive_decline_years"] = 0

        # v4.3: 最大单年跌幅 (修复: 以前用总跌幅替代单年跌幅，语义不符)
        # decline_pct 已是 abs/100 归一化值，代表总跌幅
        decline = features.get(f"{metric}_decline_pct", 0.0)
        # 对于 5 年数据，单年平均跌幅 ≈ 总跌/年数，但更准确的计算需要 raw_values
        raw_vals_key = f"{metric}_raw_values"
        if raw_vals_key in features and isinstance(features[raw_vals_key], (list, tuple)):
            raw = [v for v in features[raw_vals_key] if isinstance(v, (int, float))]
            if len(raw) >= 2:
                max_drop = 0.0
                for i in range(1, len(raw)):
                    if raw[i-1] != 0:
                        yoy = (raw[i] - raw[i-1]) / abs(raw[i-1])
                        if yoy < max_drop:
                            max_drop = yoy
                mf["max_yoy_decline"] = max_drop  # 负值
            else:
                mf["max_yoy_decline"] = -decline if decline > 0.30 else 0.0
        else:
            mf["max_yoy_decline"] = -decline if decline > 0.30 else 0.0

        # CAGR 近似
        mf["cagr"] = features.get(f"{metric}_trend", 0.0)

        return mf

    # ═══════════════════════════════════════════════════════════════════════
    # Step 3: 加权评分 (v3.0 简化 — 去掉 Copula/DS/因果调整)
    # ═══════════════════════════════════════════════════════════════════════

    def _compute_score(
        self,
        features: Dict[str, Any],
        rule_result: Optional[RuleEngineResult],
        context: AdaptiveContext,
    ) -> Tuple[float, List[Factor]]:
        """
        计算综合评分

        v3.1: 趋势 60% + 绝对水平 40% 融合评分
              加分递减效应防天花板压缩
        """
        # 趋势指标 → 绝对水平指标的映射
        LEVEL_THRESHOLD_KEYS = {
            "roic_trend": "roic_level",
            "roe_trend": "roe_level",
            "revenue_trend": "revenue_growth",
            "gross_margin_trend": "gross_margin",
            "net_margin_trend": "net_margin",
            "ocf_trend": "ocf_ratio",
            "roiic_trend": None,
            "profit_trend": None,
        }

        factors = []
        weighted_sum = 0.0
        total_weight = 0.0
        _tl_divergence_count = 0  # v4.2: 趋势-水平背离计数
        _excellent_level_count = 0  # v4.7: 卓越水平指标计数 (用于稳定性加分)

        # 1. 趋势 + 绝对水平 融合评分
        for metric_score_key, weight in self.config.score_weights.items():
            if metric_score_key not in features:
                continue

            value = features[metric_score_key]
            trend_grade = self._get_adaptive_grade(metric_score_key, value, context)

            grade_scores = {
                "excellent": 95,
                "good": 80,
                "acceptable": 60,
                "poor": 40,
                "veto": 10,
            }
            trend_score = grade_scores.get(trend_grade, 50)

            # v4.7: 预计算绝对水平等级 — 用于 R² 收缩决策和动态权重
            # 核心洞察: 低R²+高绝对水平 = 卓越稳定性(非趋势不可靠)
            # 迈瑞医疗 ROIC=30% 十年稳定, R²=0.12 → v4.5误判为"趋势不可靠"
            metric_base = metric_score_key.replace("_trend", "")
            level_key = LEVEL_THRESHOLD_KEYS.get(metric_score_key)
            level_feature = f"{metric_base}_level"
            level_value = features.get(level_feature)
            level_grade = None
            if level_key and level_value is not None:
                level_grade = self._get_adaptive_grade(level_key, level_value, context)
                if level_grade == "excellent":
                    _excellent_level_count += 1

            # v4.7: Level-aware R² 信度保护
            # 原理: R² 衡量趋势线拟合优度。低 R² 有两种根本不同的含义:
            #   (a) 趋势不可靠(数据噪声大、无明确方向) → 应收缩到中性
            #   (b) 高水平平台稳定(波动极小、斜率≈0) → 稳定性是竞争优势
            # v4.5 未区分(a)(b), 对迈瑞(ROIC 30%)/恒瑞(ROE 14%) 等造成严重误杀
            # v4.7: 绝对水平 excellent → 完全免除收缩; good → 大幅减免
            r2 = features.get(f"{metric_base}_r_squared", 0.5)
            if r2 < 0.20:
                base_shrink = r2 / 0.20  # 0→0, 0.10→0.5, 0.20→1.0
                if level_grade == "excellent":
                    # 绝对水平卓越 + R²低 = 高位平台稳定运营, 不收缩
                    shrink_factor = 1.0
                elif level_grade == "good":
                    # 绝对水平良好 → 保底75%信号保留
                    shrink_factor = max(base_shrink, 0.75)
                else:
                    # 水平一般 → 原始收缩 (趋势确实不可靠)
                    shrink_factor = base_shrink
                trend_score = 50 + (trend_score - 50) * shrink_factor
            elif r2 < 0.40:
                base_shrink = 0.6 + 0.4 * ((r2 - 0.20) / 0.20)  # 0.6→1.0
                if level_grade == "excellent":
                    shrink_factor = 1.0
                elif level_grade == "good":
                    shrink_factor = max(base_shrink, 0.85)
                else:
                    shrink_factor = base_shrink
                trend_score = 50 + (trend_score - 50) * shrink_factor

            # v4.5: 近期趋势逆转检测
            # 审计发现: 东方钽业10年slope正但近3年slope=-0.14(已在下降)
            #          水晶光电10年ROIC方向=down但仍获A+评级
            # 修复: 当近3年趋势与长期趋势严重矛盾时施加惩罚
            recent_slope = features.get(f"{metric_base}_recent_trend", None)
            if recent_slope is not None and value:
                long_sign = 1 if value > 0.01 else (-1 if value < -0.01 else 0)
                recent_sign = 1 if recent_slope > 0.01 else (-1 if recent_slope < -0.01 else 0)
                if long_sign != 0 and recent_sign != 0 and long_sign != recent_sign:
                    # 长期正但近期转负(或反之) → 趋势正在逆转
                    reversal_penalty = min(8, abs(recent_slope - value) * 15)
                    trend_score -= reversal_penalty

            # v4.4: mk_tau 趋势一致性调整
            mk_tau = features.get(f"{metric_base}_mk_tau", 0.0)
            if mk_tau and value:  # 两者都非零
                slope_sign = 1 if value > 0.005 else (-1 if value < -0.005 else 0)
                tau_sign = 1 if mk_tau > 0.05 else (-1 if mk_tau < -0.05 else 0)
                if slope_sign != 0 and tau_sign != 0:
                    if slope_sign == tau_sign:
                        # 一致: 按|mk_tau|强度给予 0~3 分加成
                        trend_score += min(3, abs(mk_tau) * 4)
                    else:
                        # 矛盾: 扣 2~4 分 (非参检验否定参数趋势)
                        trend_score -= min(4, 2 + abs(mk_tau) * 3)

            # v4.7: 动态趋势/水平权重融合 (替代固定 60/40)
            # 核心原则: "维持卓越比从平庸改善更难, 也更有价值"
            #           — 参考 Buffett 竞争优势持续性理论
            # 固定 60/40 的缺陷: ROIC=30% 平稳趋势 → 60%权重的trend≈60分,
            #   40%权重的level=95分 → 总分仅74. 卓越公司被"趋势平庸"拖累.
            # v4.7: 根据水平/趋势组合动态分配权重
            if level_key and level_value is not None and level_grade is not None:
                level_score = grade_scores.get(level_grade, 50)
                if level_grade == "excellent" and trend_grade in ("good", "acceptable"):
                    # 卓越水平 + 平稳趋势 → 水平主导 (护城河信号)
                    trend_w, level_w = 0.40, 0.60
                elif level_grade == "excellent" and trend_grade == "excellent":
                    # 双优 → 均衡 (持续成长的卓越公司)
                    trend_w, level_w = 0.50, 0.50
                elif level_grade in ("poor", "veto") and trend_grade in ("excellent", "good"):
                    # 低水平但强改善 → 趋势主导 (困境反转信号)
                    trend_w, level_w = 0.70, 0.30
                else:
                    # 默认: 55/45 (比原 60/40 更均衡)
                    trend_w, level_w = 0.55, 0.45
                metric_score = trend_w * trend_score + level_w * level_score
            else:
                metric_score = trend_score

            # v4.2: 趋势-水平背离检测 (Evaluator版 fake_growth 检测)
            # 当水平"excellent"但趋势"poor"/"acceptable"时: 历史高位但正在衰退
            # 这类公司不应获得level带来的高分加持, 降低level权重
            if (level_grade in ("excellent", "good")
                    and trend_grade in ("poor", "veto")
                    and level_key and level_value is not None):
                # 降级: 用纯趋势分 (不让高水平兜底)
                metric_score = trend_score
                _tl_divergence_count += 1

            direction = "positive" if value > 0.005 else ("negative" if value < -0.005 else "neutral")
            contribution = (metric_score - 50) / 50 * weight

            factors.append(Factor(
                name=metric_score_key,
                display_name=metric_score_key,
                value=value,
                contribution=contribution,
                direction=direction,
            ))

            weighted_sum += metric_score * weight
            total_weight += weight

        base_score = weighted_sum / total_weight if total_weight > 0 else 50.0

        # v4.5: ROIC 绝对水平门槛 — 趋势好但ROIC太低的公司限制上限
        # 审计发现: 中科曙光(ROIC 8.8%)、东方钽业(8.4%)、海光信息(10.8%)
        # 趋势评分高但资本回报率不足以覆盖资金成本，不应获得 quality 评级
        roic_latest = features.get("roic_level", None)
        if roic_latest is not None and roic_latest < 10.0:
            # ROIC < 10% → 资本效率差，score 上限 68(刚好低于 quality 阈值 72)
            penalty = max(0, (10.0 - roic_latest) * 2.0)  # 8%→-4, 5%→-10
            base_score -= penalty
        elif roic_latest is not None and roic_latest < 12.0:
            # ROIC 10-12% → 边际效率，小幅扣分
            base_score -= (12.0 - roic_latest) * 0.8  # 10%→-1.6, 11%→-0.8

        # v4.2: 趋势-水平背离总结
        if _tl_divergence_count >= 2:
            # 多指标趋势-水平背离: 公司处于"虚假繁荣"状态
            base_score -= 3 * _tl_divergence_count

        # v7.1: 连续盈利质量检测 (Accruals Quality — Sloan 1996)
        # 核心原理: 高应计利润(profit >> OCF)是盈利不可持续的最强信号之一
        # Sloan (1996) 证明: 高 accruals 公司未来回报显著低于低 accruals 公司
        # 当前 earnings_ocf_divergence 规则是二元信号，此处增加连续量化惩罚
        profit_trend = features.get("profit_trend", 0.0)
        ocf_trend = features.get("ocf_trend", 0.0)
        profit_level = features.get("profit_level", 0.0)
        ocf_level = features.get("ocf_level", 0.0)
        # 趋势维度: 利润增长但现金流停滞/下降 → 增量盈利质量差
        if profit_trend > 0.05 and ocf_trend < 0.0:
            accrual_divergence = profit_trend - ocf_trend
            accrual_penalty = min(5.0, accrual_divergence * 8.0)  # 最多 -5 分
            base_score -= accrual_penalty
        # 水平维度: 利润远超现金流 → 存量盈利含金量低
        if profit_level > 0.5 and ocf_level > 0:
            cash_conversion = ocf_level / max(profit_level, 0.01)
            if cash_conversion < 0.50:
                # 现金转化率 <50% → 显著扣分 (A股应收账款操纵常见)
                base_score -= min(4.0, (0.50 - cash_conversion) * 8.0)
            elif cash_conversion < 0.70:
                # 现金转化率 50-70% → 轻微扣分
                base_score -= min(2.0, (0.70 - cash_conversion) * 5.0)

        # v4.2: 多指标恶化检测
        # TRUTH 有 δ_decay/V_factor 检测衰退和虚假成长，Evaluator 需要对等能力
        deterioration_count = sum(
            1 for metric_key in self.config.score_weights
            if features.get(f"{metric_key.replace('_trend', '')}_has_deterioration", False)
        )

        # ══════ v8.1: 动量信号整合 (latest_vs_weighted momentum) ══════
        # latest_vs_weighted = 最新值 / 时间加权平均
        # 该比率自 v3.1 起提取但从未使用 — 这浪费了一个强信号:
        #   ratio >> 1.0: 最近表现远超历史均值 → 强改善动量
        #   ratio << 1.0: 最近表现远低于历史均值 → 恶化动量
        # 实现: 对核心盈利指标(ROIC, ROE, gross_margin, net_margin)计算中位动量
        # 理论基础: Novy-Marx (2013) 证明盈利动量(profitability momentum)
        #          是独立于价格动量的 alpha 来源, IC ≈ 0.04-0.06/month
        # ================================================================
        _momentum_metrics = ["roic", "roe", "gross_margin", "net_margin"]
        _momentum_ratios = []
        for _m in _momentum_metrics:
            _ratio = features.get(f"{_m}_latest_vs_weighted", None)
            if _ratio is not None and 0.1 < _ratio < 10.0:  # 排除明显异常值
                _momentum_ratios.append(_ratio)

        if len(_momentum_ratios) >= 2:
            median_momentum = float(np.median(_momentum_ratios))
            # 双向调整: 正向改善奖励, 负向恶化惩罚
            if median_momentum > 1.15:
                # 核心盈利指标中位数显著高于均值 → 改善动量
                # 1.15 → +1, 1.30 → +2.5, 1.50+ → +3 (封顶)
                momentum_bonus = min(3.0, (median_momentum - 1.15) * 7.0 + 1.0)
                base_score += momentum_bonus
            elif median_momentum < 0.85:
                # 核心盈利指标中位数显著低于均值 → 恶化动量
                # 0.85 → -1, 0.70 → -2.5, 0.50- → -3 (封顶)
                momentum_penalty = min(3.0, (0.85 - median_momentum) * 7.0 + 1.0)
                base_score -= momentum_penalty

        # v4.4: 多指标恶化调降 (降低力度，避免与规则P3三重惩罚)
        # 旧版 -12/-8/-5 加上规则P3逐指标扣分 + base_score已含低趋势，导致三重打击
        # 新版仅作为"多指标联动"信号的边际调整，核心惩罚由规则P3承担
        if deterioration_count >= 5:
            base_score -= 6
        elif deterioration_count >= 4:
            base_score -= 4
        elif deterioration_count >= 3:
            base_score -= 2

        # ══════ v11.0: π 盈利能力因子 (学习自 TRUTH v7.0) ══════
        # Evaluator 最大盲点: 完全没有 GP/Assets (Novy-Marx 2013 最强单因子, IC=0.70)
        # 实现: 从 financial_context 提取 GP/Assets + ROIC水平 + ROE水平 + 资产周转率
        # 计算独立的盈利质量分数, 作为额外加减分 (±8分)
        # 学术依据: AQR QMJ Profitability = GPOA + ROE + ROA + CFOA
        _gp_assets = features.get("fc_profitability_gp_assets")
        _roic_lvl = features.get("fc_profitability_roic_level", features.get("roic_level"))
        _roe_lvl = features.get("fc_profitability_roe_level", features.get("roe_level"))
        _asset_turn = features.get("fc_profitability_assets_turn")

        _pi_components = []
        if _gp_assets is not None:
            # GP/Assets sigmoid: center=0.15, scale=0.10 (与 TRUTH config 对齐)
            _gpa_sig = 1.0 / (1.0 + math.exp(-(_gp_assets - 0.15) / 0.10))
            _pi_components.append(("gp_assets", _gpa_sig, 0.35))
        if _roic_lvl is not None:
            # ROIC level sigmoid: center=10%, scale=5%
            _roic_sig = 1.0 / (1.0 + math.exp(-(_roic_lvl - 10.0) / 5.0))
            _pi_components.append(("roic_level", _roic_sig, 0.30))
        if _roe_lvl is not None:
            # ROE level sigmoid: center=12%, scale=6%
            _roe_sig = 1.0 / (1.0 + math.exp(-(_roe_lvl - 12.0) / 6.0))
            _pi_components.append(("roe_level", _roe_sig, 0.20))
        if _asset_turn is not None:
            # Asset turnover sigmoid: center=0.5, scale=0.3
            _at_sig = 1.0 / (1.0 + math.exp(-(_asset_turn - 0.5) / 0.3))
            _pi_components.append(("asset_turn", _at_sig, 0.15))

        if _pi_components:
            _pi_total_w = sum(w for _, _, w in _pi_components)
            _pi_score = sum(s * w for _, s, w in _pi_components) / _pi_total_w
            # _pi_score ∈ [0, 1], 0.5 = 中性
            # 映射: 0.0→-8, 0.25→-4, 0.50→0, 0.75→+4, 1.0→+8
            _pi_adjustment = (_pi_score - 0.50) * 16.0  # ±8 分
            _pi_adjustment = max(-8.0, min(8.0, _pi_adjustment))
            base_score += _pi_adjustment
            if abs(_pi_adjustment) > 0.5:
                factors.append(Factor(
                    name="pi_profitability",
                    display_name="π 盈利能力因子 (Novy-Marx)",
                    value=_pi_score,
                    contribution=_pi_adjustment / 100,
                    direction="positive" if _pi_adjustment > 0 else "negative",
                    explanation=f"GP/A+ROIC+ROE+周转 → π={_pi_score:.2f} → {_pi_adjustment:+.1f}分",
                ))

        # ═══ v11.0: Piotroski F-Score 第三验证器 ═══
        # Piotroski (2000): "Value Investing: The Use of Historical Financial Statement
        # Information to Separate Winners from Losers"
        # F-Score ≥ 6 → +3分; F-Score ≤ 2 → -4分
        from ..backtest.engine import compute_piotroski_fscore, compute_beneish_mscore
        _fs_result = compute_piotroski_fscore(features)
        _fscore = _fs_result["fscore"]
        _fs_max = _fs_result["max_possible"]

        if _fs_max >= 4:  # 至少有 4 个指标可计算才有意义
            if _fscore >= 6:
                base_score += 3.0
                factors.append(Factor(
                    name="piotroski_fscore",
                    display_name="Piotroski F-Score",
                    value=float(_fscore),
                    contribution=0.03,
                    direction="positive",
                    explanation=f"F={_fscore}/{_fs_max} ({_fs_result['interpretation']}) → +3",
                ))
            elif _fscore >= 5:
                base_score += 1.5
            elif _fscore <= 2:
                base_score -= 4.0
                factors.append(Factor(
                    name="piotroski_fscore",
                    display_name="Piotroski F-Score",
                    value=float(_fscore),
                    contribution=-0.04,
                    direction="negative",
                    explanation=f"F={_fscore}/{_fs_max} ({_fs_result['interpretation']}) → -4",
                ))
            elif _fscore <= 3:
                base_score -= 2.0

        # ═══ v11.0: Beneish M-Score 增强欺诈检测 ═══
        # Beneish (1999): M > -1.78 → likely manipulator
        # 与 TRUTH δ_fraud 因子互补: TRUTH 检测 OCF/NI 背离
        # Beneish 检测 DSRI+GMI+AQI+SGI+TATA 组合模式
        _bn_result = compute_beneish_mscore(features)
        if _bn_result["confidence"] in ("high", "medium"):
            if _bn_result["is_manipulator"]:
                _bn_penalty = -5.0 if _bn_result["confidence"] == "high" else -3.0
                base_score += _bn_penalty
                factors.append(Factor(
                    name="beneish_mscore",
                    display_name="Beneish M-Score 欺诈检测",
                    value=_bn_result["m_score"],
                    contribution=_bn_penalty / 100,
                    direction="negative",
                    explanation=f"M={_bn_result['m_score']:.2f} > -1.78 ({_bn_result['confidence']}) → {_bn_penalty:+.0f}",
                ))

        # v4.7: 卓越稳定性加分 — 多指标同时处于卓越水平 = 宽广护城河
        # 迈瑞医疗: ROIC 30%, ROE 34%, 毛利率 63%, 净利率 32% → 全面卓越
        # 这类公司即使趋势平稳也应获得额外认可
        if _excellent_level_count >= 5 and deterioration_count <= 2:
            base_score += 3  # 全面卓越加分
        elif _excellent_level_count >= 4 and deterioration_count <= 1:
            base_score += 2  # 高度卓越加分

        # 2. 规则引擎调整 (v4.2: 恶化感知型惩罚缩放)
        rule_adjustment = 0.0
        if rule_result and not rule_result.vetoed:
            penalty = rule_result.total_penalty
            bonus = rule_result.total_bonus

            # v4.7: 恶化感知型惩罚缩放 (v4.2→v4.7 增强)
            # 问题: 规则引擎对历史波动(如比亚迪2015-2020)累积高额罚分,
            #       即使当前基本面已显著改善, 仍被历史罚分拖入AVERAGE
            # v4.7: 绝对水平卓越的公司获得额外折扣 ("当前优秀"减免"历史杂音")
            if deterioration_count <= 1:
                penalty_discount = 0.55    # 近乎无恙 → 45%折扣 (v4.2: 0.65)
            elif deterioration_count == 2:
                penalty_discount = 0.70    # 轻度恶化 (v4.2: 0.80)
            elif deterioration_count == 3:
                penalty_discount = 0.85    # 中度恶化 (v4.2: 0.90)
            else:
                penalty_discount = 1.00    # 多指标恶化 → 全额惩罚

            # v4.7: 卓越水平额外折扣
            # 如果多数指标绝对水平卓越, 规则引擎的历史惩罚应进一步减免
            if _excellent_level_count >= 4:
                penalty_discount *= 0.80  # 额外20%折扣

            # 扣分递减 + 恶化感知
            floor_headroom = max(0, base_score - 10)
            effective_penalty = penalty * penalty_discount * min(1.0, floor_headroom / 40.0)
            penalty_adj = -effective_penalty

            # 加分递减: 基础分越高、加分效果越小 (headroom 式衰减)
            headroom = max(0, 100 - base_score)
            effective_bonus = bonus * min(1.0, headroom / 50.0)

            rule_adjustment = penalty_adj + effective_bonus
            if abs(rule_adjustment) > 0.1:
                factors.append(Factor(
                    name="rule_engine",
                    display_name="规则引擎调整",
                    value=rule_adjustment,
                    contribution=rule_adjustment / 100,
                    direction="positive" if rule_adjustment > 0 else "negative",
                ))

        # v4.2: 如果有恶化, 记录因子
        if deterioration_count >= 3:
            factors.append(Factor(
                name="deterioration_quality",
                display_name="恶化质量检测",
                value=float(-deterioration_count),
                contribution=-deterioration_count * 0.02,
                direction="negative",
                explanation=f"{deterioration_count}个指标同时恶化",
            ))

        final_score = float(np.clip(base_score + rule_adjustment, 0, 100))

        # v4.5: ROIC 绝对水平硬上限 — 资本回报率极低时封顶
        # 审计发现: 蜀道装备(ROIC 5.4%)、金利华电(历史负ROIC)、中科曙光(8.8%)
        # 即使其他指标(毛利率/EPS)表现好也不应获得 quality 评级
        # 因为 ROIC 是衡量"资本创造价值能力"的核心指标
        roic_latest_for_cap = features.get("roic_level", None)
        if roic_latest_for_cap is not None:
            if roic_latest_for_cap < 6.0:
                # ROIC < 6%: 连资金成本都覆盖不了 → 硬封顶 58
                final_score = min(final_score, 58.0)
            elif roic_latest_for_cap < 9.0:
                # ROIC 6-9%: 勉强覆盖资金成本 → 硬封顶 68 (低于 quality 72)
                cap = 58.0 + (roic_latest_for_cap - 6.0) / 3.0 * 10.0  # 6%→58, 9%→68
                final_score = min(final_score, cap)

        # ══════ v12.0: Cross-Factor Interaction Engine (5 项交互) ══════
        # 学术依据:
        #   - Asness, Frazzini & Pedersen (2019): 因子交互提升 IC
        #   - Jensen (1986): 杠杆×欺诈非线性放大
        #   - Penman (2013): 高盈利+衰退=假阳性
        #   - Altman (1968): 杠杆+衰退→违约风险指数增加
        #   - Greenwald (2005): 护城河=持续超额盈利×需求稳定

        # 提取 π 分数 (已在上面计算)
        _pi_for_interaction = _pi_score if _pi_components else None

        # 提取周期性代理 (使用 ROIC 波动率)
        _alpha_proxy = features.get("roic_volatility", features.get("gross_margin_volatility", 0.3))
        if _alpha_proxy is not None:
            _alpha_proxy = min(1.0, max(0.0, _alpha_proxy / 0.50))  # 归一化到 [0,1]

        # 杠杆代理
        _lambda_proxy = features.get("fc_ratio_debt_to_assets", 0.40)
        if _lambda_proxy is not None:
            _lambda_proxy = min(1.0, max(0.0, _lambda_proxy))

        # 欺诈代理 (从 Beneish 结果)
        _fraud_proxy = 0.0
        if _bn_result["confidence"] in ("high", "medium") and _bn_result["is_manipulator"]:
            _fraud_proxy = min(1.0, max(0.0, (_bn_result["m_score"] + 1.78) / 2.0 + 0.50))

        # 衰退代理
        _decay_proxy = min(1.0, max(0.0, deterioration_count / 6.0))

        # 增长代理
        _gamma_proxy = features.get("revenue_trend", 0.0)
        if _gamma_proxy is not None:
            _gamma_proxy = min(1.0, max(0.0, (_gamma_proxy + 0.10) / 0.30))

        # 验证代理 (OCF/利润一致性)
        _verify_proxy = 0.5
        if profit_level > 0.3 and ocf_level > 0:
            _verify_proxy = min(1.0, ocf_level / max(profit_level, 0.01))

        # ── Interaction 1: Moat Strength (护城河强度) ──
        # MS = π × (1 - α): 高盈利 + 低周期 = 持久竞争优势
        if _pi_for_interaction is not None and _alpha_proxy is not None:
            _moat = _pi_for_interaction * (1.0 - _alpha_proxy)
            if _moat > 0.50:
                _moat_bonus = min(4.0, (_moat - 0.50) * 8.0)
                final_score += _moat_bonus
            elif _moat < 0.15:
                _moat_penalty = min(3.0, (0.15 - _moat) * 6.0)
                final_score -= _moat_penalty

        # ── Interaction 2: Financial Danger (财务危险) ──
        # FD = λ × δ_fraud: 高杠杆 + 欺诈信号 = 极度危险
        if _lambda_proxy is not None and _fraud_proxy > 0.1:
            _fin_danger = _lambda_proxy * _fraud_proxy
            if _fin_danger > 0.25:
                _fd_penalty = min(8.0, (_fin_danger - 0.25) * 16.0)
                final_score -= _fd_penalty

        # ── Interaction 3: Verified Growth (验证增长) ──
        # VG = γ × V × (1 - δ_decay): 三因子交叉验证
        if _gamma_proxy > 0.2 and _verify_proxy > 0.3:
            _vg = _gamma_proxy * _verify_proxy * (1.0 - _decay_proxy)
            if _vg > 0.25:
                _vg_bonus = min(5.0, (_vg - 0.25) * 10.0)
                final_score += _vg_bonus

        # ── Interaction 4: Dying Franchise (正在死亡的特许权) ──
        # DF = π × δ_decay: 高盈利 + 高衰退 = 曾经辉煌但正在衰亡
        if _pi_for_interaction is not None and _decay_proxy > 0.2:
            _dying = _pi_for_interaction * _decay_proxy
            if _dying > 0.30:
                _dying_penalty = min(6.0, (_dying - 0.30) * 12.0)
                final_score -= _dying_penalty

        # ── Interaction 5: Leverage Amplifier (杠杆放大器) ──
        # LA = λ × δ_decay: 高杠杆 + 衰退 = 财务困境
        if _lambda_proxy is not None and _decay_proxy > 0.2:
            _la = _lambda_proxy * _decay_proxy
            if _la > 0.20:
                _la_penalty = min(5.0, (_la - 0.20) * 10.0)
                final_score -= _la_penalty

        # 记录交互效应
        _interaction_total = 0.0  # 简化: 求和统计
        factors.append(Factor(
            name="cross_factor_interactions",
            display_name="v12.0 因子交互引擎",
            value=final_score - float(np.clip(base_score + rule_adjustment, 0, 100)),
            contribution=0.0,
            direction="neutral",
            explanation="MS+FD+VG+DF+LA 5项非线性交互",
        ))

        # ══════ v12.0: 行业原型权重微调 ══════
        # 不同行业板块的评分偏置调整 (±3分)
        _EVAL_INDUSTRY_BIAS: Dict[str, float] = {
            # 制造周期: 对衰退更敏感, 基准线下移
            "电气设备": -1.0, "元器件": -1.0, "专用机械": -1.0,
            "汽车整车": -1.5, "小金属": -2.0,
            # 科技成长: 允许更高波动, 基准线上移
            "软件服务": 1.0, "半导体": 1.5, "IT设备": 0.5,
            # 医药: 中性(已有 δ_fraud 保护)
            "医疗保健": 0.0, "化学制药": 0.0, "生物制药": 0.5,
            # 新能源: 政策驱动, 周期噪音豁免
            "新型电力": 1.0,
        }
        _eval_industry = features.get("_industry", "")
        _ind_bias = _EVAL_INDUSTRY_BIAS.get(_eval_industry, 0.0)
        final_score += _ind_bias

        final_score = float(np.clip(final_score, 0, 100))

        # ══════ v7.4: 三维硬约束 (对齐 TRUTH v7.3 严格度) ══════
        # 审计发现: 信号一致率从 74.5% 降至 57.5%, 根因是 TRUTH v7.3 大幅收严
        # (δ_decay 乘法惩罚 ×0.50~0.78) 而 Eval 仅做加法扣分 (-2~-6/100)
        # 差距量级: TRUTH 50% 乘法 vs Eval 6% 加法 → 需要新增乘法型硬约束

        # 1. 多指标恶化: 乘法惩罚 (对齐 TRUTH δ_decay 乘法逻辑)
        #    TRUTH: raw_decay > 0.50 → ×0.62, > 0.70 → ×0.50
        #    Eval:  5 deterioration ≈ severe decay → ×0.75 对应
        if deterioration_count >= 6:
            final_score *= 0.70  # 极重度恶化: 6+ 指标
        elif deterioration_count >= 5:
            final_score *= 0.78  # 重度恶化: 5 指标
        elif deterioration_count >= 4:
            final_score *= 0.88  # 中度恶化: 4 指标

        # 2. 杠杆硬约束 (对齐 TRUTH λ 因子, 权重 10%)
        #    TRUTH 有 λ 因子独立评估杠杆, Eval 完全缺失
        #    AQR Safety: leverage (D/A) 是 quality safety 核心维度
        fc_debt = features.get("fc_ratio_debt_to_assets")
        if fc_debt is not None:
            if fc_debt > 0.80:
                # 极高杠杆 (>80%): 封顶 55 (不可 quality)
                final_score = min(final_score, 55.0)
            elif fc_debt > 0.70:
                # 高杠杆 (70-80%): 封顶 65 (勉强 average)
                final_score = min(final_score, 65.0)

        # 3. OCF-利润严重背离封顶 (对齐 TRUTH δ_fraud 熔断逻辑)
        #    TRUTH: δ_fraud > 0.58 → fraud_alert 熔断
        #    Eval v7.3: 已有软扣分, 此处加硬封顶防止极端背离越线
        #    Sloan (1996): high accruals → future earnings reversal
        profit_level = features.get("profit_level", 0.0)
        ocf_level = features.get("ocf_level", 0.0)
        if profit_level > 0.5 and ocf_level > 0:
            cash_conv = ocf_level / max(profit_level, 0.01)
            if cash_conv < 0.25:
                # 极低现金转化率 (<25%): 封顶 58 (严重盈利质量问题)
                final_score = min(final_score, 58.0)

        return final_score, factors

    def _get_adaptive_grade(
        self, metric: str, value: float, context: AdaptiveContext
    ) -> str:
        """
        获取自适应等级

        基于 A 股 2015-2024 实际 log_slope 分位数校准:
        p15≈-0.85, p25≈-0.50, p50≈-0.16, p75≈-0.01, p85≈+0.05
        (roic 为例; 不同指标有不同的标准差, 详见 adaptive_threshold.py)
        """
        try:
            thresholds = self._threshold_engine.get_thresholds(metric, context)
            return thresholds.get_grade(value, higher_is_better=True)
        except (ValueError, KeyError):
            # 通用 fallback: 基于所有指标的中位数分布
            if value > 0.05:
                return "excellent"
            elif value > 0.00:
                return "good"
            elif value > -0.10:
                return "acceptable"
            elif value > -0.40:
                return "poor"
            else:
                return "veto"

    # ═══════════════════════════════════════════════════════════════════════
    # Step 5: 决策 (v3.0 简化)
    # ═══════════════════════════════════════════════════════════════════════

    def _make_decision(
        self,
        score: float,
        rule_result: Optional[RuleEngineResult],
        lifecycle_confidence: float,
        features: Optional[Dict[str, Any]] = None,
    ) -> Tuple[DecisionType, float]:
        """
        v7.3 决策 — 分数排名 + 风险硬约束

        到达此函数的公司已通过 Step 2 规则引擎否决检查。
        v3.0: 纯分数排名 (QUALITY/AVERAGE/POOR)
        v7.3: 新增风险硬约束 — 消除 "高分但高风险" 的因子-评分矛盾
              参考 AQR QMJ Safety + Sloan Accruals 研究
        """
        if score >= self.config.quality_threshold:
            decision = DecisionType.QUALITY
        elif score >= self.config.average_threshold:
            decision = DecisionType.AVERAGE
        else:
            decision = DecisionType.POOR

        # v7.3: 风险硬约束 — QUALITY 级别的额外安全门
        # 这些检查使用 evaluator 自身的 PDDA 特征, 不依赖 TRUTH
        if decision == DecisionType.QUALITY and features:
            downgrade_reasons = []

            # 1. 虚假增长检测 (Sloan 1996 Accruals Anomaly)
            # 利润强势上行但经营现金流下行 = 高应计利润 = 盈利不可持续
            profit_trend = features.get("profit_trend", 0.0)
            ocf_trend = features.get("ocf_trend", 0.0)
            if profit_trend > 0.08 and ocf_trend < -0.02:
                downgrade_reasons.append("accrual_divergence")

            # 2. 多指标同步恶化 (≥4个指标恶化 + quality = 逻辑矛盾)
            deterioration_count = sum(
                1 for key in features
                if key.endswith("_has_deterioration") and features[key]
            )
            if deterioration_count >= 4:
                downgrade_reasons.append(f"multi_deterioration({deterioration_count})")

            # 3. 核心盈利能力不足 (ROIC < 8% 的公司不应获得 quality)
            # 补充 ROIC floor: _compute_score 中已扣分, 但边界情况可能仍越线
            roic_level = features.get("roic_level")
            if roic_level is not None and roic_level < 8.0:
                downgrade_reasons.append(f"roic_insufficient({roic_level:.1f}%)")

            # 4. 趋势全面下行 (所有核心指标方向 = negative → 不应 quality)
            core_metrics = ["roic", "gross_margin", "revenue", "ocf"]
            neg_count = sum(
                1 for m in core_metrics
                if features.get(f"{m}_direction") == "negative"
            )
            if neg_count >= 3:
                downgrade_reasons.append(f"broad_decline({neg_count}/4)")

            # 5. v7.4: 高杠杆 (对齐 TRUTH λ 因子)
            # AQR Safety: D/A > 0.70 的公司不应是 quality
            fc_debt = features.get("fc_ratio_debt_to_assets")
            if fc_debt is not None and fc_debt > 0.70:
                downgrade_reasons.append(f"high_leverage({fc_debt:.0%})")

            # 6. v7.4: 极低现金转化率 (对齐 TRUTH δ_fraud)
            # Sloan (1996): cash_conversion < 0.30 → 高应计利润 → 盈利不可持续
            profit_level = features.get("profit_level", 0.0)
            ocf_level = features.get("ocf_level", 0.0)
            if profit_level > 0.3 and ocf_level > 0:
                cash_conv = ocf_level / max(profit_level, 0.01)
                if cash_conv < 0.30:
                    downgrade_reasons.append(f"low_cash_conversion({cash_conv:.0%})")

            if downgrade_reasons:
                decision = DecisionType.AVERAGE
                # 微降置信度以反映不确定性
                lifecycle_confidence = max(0.40, lifecycle_confidence - 0.10)

        # 置信度: 规则引擎覆盖度 + 分数边界距离
        rule_conf = 0.50
        if rule_result:
            triggered = len(rule_result.penalty_rules) + len(rule_result.bonus_rules)
            if rule_result.strategies:
                rule_conf = min(0.95, 0.65 + 0.05 * len(rule_result.strategies))
            elif triggered > 3:
                rule_conf = 0.80
            elif triggered > 0:
                rule_conf = 0.65

        boundary = (
            self.config.quality_threshold
            if decision == DecisionType.QUALITY
            else self.config.average_threshold
        )
        distance = abs(score - boundary) / 20.0
        score_conf = min(0.95, 0.50 + distance)

        confidence = 0.50 * rule_conf + 0.25 * lifecycle_confidence + 0.25 * score_conf
        return decision, confidence

    # ═══════════════════════════════════════════════════════════════════════
    # Step 6: 解释
    # ═══════════════════════════════════════════════════════════════════════

    def _generate_explanation(
        self,
        ts_code: str,
        company_info: Dict[str, Any],
        decision: DecisionType,
        confidence: float,
        factors: List[Factor],
        score: float,
    ) -> ExplanationResult:
        """生成解释"""
        explainer = DecisionExplainer(
            company_name=company_info.get("name", ts_code),
            industry=company_info.get("industry"),
        )
        return explainer.explain(
            decision=decision,
            confidence=confidence,
            factors=factors,
            score=score,
            state_info=None,
            causal_diagnosis=None,
        )

    # ═══════════════════════════════════════════════════════════════════════
    # 工具函数
    # ═══════════════════════════════════════════════════════════════════════

    @staticmethod
    def _score_to_grade_str(score: float) -> str:
        if score >= 90:
            return "A"
        elif score >= 80:
            return "B"
        elif score >= 70:
            return "C"
        elif score >= 60:
            return "D"
        return "F"


# ═══════════════════════════════════════════════════════════════════════════════
# Pipeline 集成 (API 100% 向后兼容)
# ═══════════════════════════════════════════════════════════════════════════════

@register_method(
    component_type="business_engine",
    engine_type="evaluator",
    engine_name="causal_bayesian_evaluator",
)
def run_causal_bayesian_evaluator(
    aggregated_trends: Dict[str, pd.DataFrame],
    company_list: Optional[List[Dict[str, Any]]] = None,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    运行评估器 — Pipeline 入口

    Args:
        aggregated_trends: PDDA 聚合趋势数据
        company_list: 可选公司信息列表
        config: 可选配置覆盖

    Returns:
        {evaluations, summary, quality_companies, veto_companies}
    """
    logger.info(f"Evaluator v3.0: {len(aggregated_trends)} metrics")

    eval_config = EvaluatorConfig()
    if config:
        for key, value in config.items():
            if hasattr(eval_config, key):
                setattr(eval_config, key, value)

    evaluator = CausalBayesianEvaluator(eval_config)

    # 获取所有 ts_code
    all_ts_codes = set()
    for df in aggregated_trends.values():
        if df is not None and "ts_code" in df.columns:
            all_ts_codes.update(df["ts_code"].unique())

    # 构建公司信息
    company_info_dict: Dict[str, Dict[str, Any]] = {}
    if company_list:
        for info in company_list:
            ts_code = info.get("ts_code")
            if ts_code:
                company_info_dict[ts_code] = info

    # 从 PDDA 聚合数据提取名称/行业
    if not company_info_dict:
        for df in aggregated_trends.values():
            if df is not None and not df.empty and "name" in df.columns:
                cols = ["ts_code", "name", "industry"] if "industry" in df.columns else ["ts_code", "name"]
                for _, row in df[cols].drop_duplicates("ts_code").iterrows():
                    ts = row["ts_code"]
                    if ts not in company_info_dict:
                        company_info_dict[ts] = {
                            "ts_code": ts,
                            "name": str(row.get("name", "") or ""),
                            "industry": str(row.get("industry", "") or ""),
                        }
                break
        if company_info_dict:
            logger.info(f"Extracted {len(company_info_dict)} company names from aggregated_trends")

    # 评估
    evaluations = []
    quality_companies = []
    veto_companies = []

    for ts_code in all_ts_codes:
        company_info = company_info_dict.get(ts_code, {"ts_code": ts_code})
        try:
            result = evaluator.evaluate_company(ts_code, aggregated_trends, company_info)
            evaluations.append(result.to_dict())
            if result.decision == DecisionType.QUALITY:
                quality_companies.append(ts_code)
            elif result.decision == DecisionType.VETO:
                veto_companies.append(ts_code)
        except Exception as e:
            logger.error(f"Error evaluating {ts_code}: {e}")

    # ══════ v12.0: 贝叶斯后验更新 + 行业中性化 ══════
    # v11.0: 简单 z-score 调整 (±5分) — 统计正确但非贝叶斯
    # v12.0: 真正的 Bayesian 后验更新 (这次名副其实):
    #
    # 理论基础:
    #   - James & Stein (1961): 当维度≥3时, 收缩估计量的风险严格
    #     小于最大似然估计 — 也就是说, 往均值收缩永远更好
    #   - Efron & Morris (1973): "Stein's Estimation Rule and Its
    #     Competitors — An Empirical Bayes Approach"
    #   - Vasicek (1973): 在 β 估计中首次应用贝叶斯收缩
    #
    # 实现:
    #   Prior: 行业均值和方差 → Beta(a, b) 分布
    #     μ_prior = μ_industry (行业均分)
    #     σ²_prior = σ²_industry (行业内方差)
    #   Likelihood: 公司自身评分
    #     μ_data = company_score
    #     σ²_data = 1 / confidence (置信度的倒数作为数据方差)
    #   Posterior (正态-正态共轭):
    #     μ_posterior = w × μ_data + (1-w) × μ_prior
    #     w = σ²_prior / (σ²_prior + σ²_data)  ← 精度加权
    #
    # 关键洞察: 数据不确定性高(低置信度)时 → 更多收缩到行业均值
    #           数据确定性高(高置信度)时 → 更多保留原始评分
    # 这解决了"低数据公司被评分噪声误导"的系统性问题
    _MIN_INDUSTRY_SIZE = 8
    _industry_groups = defaultdict(list)
    for _idx, _ev in enumerate(evaluations):
        _ind = _ev.get("industry", "__unknown__") or "__unknown__"
        _industry_groups[_ind].append((_idx, _ev["score"], _ev.get("confidence", 0.5)))

    _industry_stats = {}
    _n_bayes_updated = 0
    _total_shrinkage = 0.0

    # Step 1: 计算行业先验 (Prior)
    for _ind, _members in _industry_groups.items():
        if len(_members) < _MIN_INDUSTRY_SIZE:
            continue
        _scores = [s for _, s, _ in _members]
        _mu_prior = sum(_scores) / len(_scores)
        _var_prior = sum((_s - _mu_prior) ** 2 for _s in _scores) / len(_scores)
        _sigma_prior = _var_prior ** 0.5
        if _sigma_prior < 1e-6:
            continue
        _industry_stats[_ind] = (_mu_prior, _var_prior, _sigma_prior)

    # Step 2: 贝叶斯后验更新 (每个公司)
    for _ind, _members in _industry_groups.items():
        stats = _industry_stats.get(_ind)
        if stats is None:
            # 小行业: 回退到全样本先验
            _all_scores = [_ev["score"] for _ev in evaluations]
            _global_mu = sum(_all_scores) / len(_all_scores) if _all_scores else 50.0
            _global_var = sum((_s - _global_mu) ** 2 for _s in _all_scores) / len(_all_scores) if _all_scores else 400.0
            stats = (_global_mu, _global_var, _global_var ** 0.5)

        _mu_prior, _var_prior, _sigma_prior = stats

        for _i, _s, _conf in _members:
            # ── 数据方差模型 (修正) ──
            # 正态-正态共轭: posterior mean = w×data + (1-w)×prior
            # w = σ²_prior / (σ²_prior + σ²_data)
            #   → w高 = 保留原始分数 (prior不确定)
            #   → w低 = 收缩到行业均值 (data不确定)
            #
            # 置信度高 → σ²_data 小 → w 大 → 保留原始
            # 置信度低 → σ²_data 大 → w 小 → 收缩到先验
            #
            # 公式: σ²_data = σ²_prior × SHRINKAGE × (1-conf)/conf
            # SHRINKAGE = 0.5: 温和收缩, 避免过度平滑
            _SHRINKAGE_FACTOR = 0.5
            _var_data = _var_prior * _SHRINKAGE_FACTOR * (1.0 - _conf) / max(0.10, _conf)

            # James-Stein 收缩权重 (Bayesian 精度加权)
            # w = σ²_prior / (σ²_prior + σ²_data)
            # w 高 → 保留原始分数; w 低 → 收缩到行业均值
            _w = _var_prior / (_var_prior + _var_data) if (_var_prior + _var_data) > 0 else 0.5

            # 后验均值
            _mu_posterior = _w * _s + (1.0 - _w) * _mu_prior

            # 后验方差 (更新置信度)
            _var_posterior = 1.0 / (1.0 / _var_prior + 1.0 / _var_data) if _var_data > 0 else _var_prior
            _posterior_confidence = min(0.95, _conf * (1.0 + 0.10 * (1.0 - _w)))

            # z-score (在行业内的相对位置)
            _z = (_s - _mu_prior) / _sigma_prior if _sigma_prior > 0 else 0.0
            evaluations[_i]["industry_z_score"] = round(_z, 3)
            evaluations[_i]["bayesian_shrinkage_w"] = round(_w, 3)

            # 应用后验分数 (裁剪到 [0, 100])
            _old_score = evaluations[_i]["score"]
            _new_score = float(np.clip(_mu_posterior, 0, 100))
            evaluations[_i]["score"] = _new_score
            evaluations[_i]["confidence"] = round(_posterior_confidence, 4)

            _shrinkage_delta = abs(_new_score - _old_score)
            _total_shrinkage += _shrinkage_delta
            _n_bayes_updated += 1

            # 重新决策 (使用后验分数)
            if evaluations[_i].get("decision") != "veto":  # veto 决策不可逆
                if _new_score >= eval_config.quality_threshold:
                    evaluations[_i]["decision"] = "quality"
                elif _new_score >= eval_config.average_threshold:
                    evaluations[_i]["decision"] = "average"
                else:
                    evaluations[_i]["decision"] = "poor"

    _avg_shrinkage = _total_shrinkage / _n_bayes_updated if _n_bayes_updated > 0 else 0.0
    if _industry_stats:
        logger.info(
            f"v12.0 Bayesian Posterior Update: {len(_industry_stats)} industries, "
            f"{_n_bayes_updated} scores updated, avg_shrinkage={_avg_shrinkage:.2f}pts, "
            f"decisions re-evaluated"
        )

    # 重建 quality/veto 列表 (决策未变, 但确保一致性)
    quality_companies = [e["ts_code"] for e in evaluations if e.get("decision") == "quality"]
    veto_companies = [e["ts_code"] for e in evaluations if e.get("decision") == "veto"]

    summary = {
        "total_evaluated": len(evaluations),
        "quality_count": len(quality_companies),
        "veto_count": len(veto_companies),
        "average_score": float(np.mean([e["score"] for e in evaluations])) if evaluations else 0,
        "average_confidence": float(np.mean([e["confidence"] for e in evaluations])) if evaluations else 0,
    }

    logger.info(f"Evaluation complete: {summary}")

    return {
        "evaluations": evaluations,
        "summary": summary,
        "quality_companies": quality_companies,
        "veto_companies": veto_companies,
    }
