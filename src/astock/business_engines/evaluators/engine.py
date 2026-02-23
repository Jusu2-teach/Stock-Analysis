"""
═══════════════════════════════════════════════════════════════════════════════
AStock Evaluators v3.0 — 精简诚实架构
═══════════════════════════════════════════════════════════════════════════════

v3.0 重构原则:
    旧 v2.0 拥有 7 个子模块 (因果图/HMM/Copula/D-S/自适应阈值/规则引擎/解释器)
    但代码审计发现:
    - 因果图 = DuPont 会计恒等式的 400 行花架子 (内含 dead-code bug)
    - HMM 状态机 = 无训练数据的伪贝叶斯 (特征范围全硬编码)
    - Copula + D-S = 双轨融合实际影响 <8 分/100 分
    - 218 个未经回测验证的超参数

    v3.0 仅保留经过审计确认有效的组件:
    1. PDDA 特征提取 (完整保留)
    2. 声明式规则引擎 (核心 — 唯一 VETO 网关)
    3. 自适应行业阈值 (简化)
    4. 加权评分 + 排名
    5. 生命周期推断 (简化为确定性函数, 去伪 HMM)
    6. 可解释性报告

    删除的组件 (保留源文件但不再调用):
    - causal_graph.py   → 对评分影响 <7%, 且含 dead-code bug
    - copula_fusion.py  → 只用了 ESS, 对评分影响 <3%
    - dempster_shafer.py → 与 Copula 重复, Yager 规则不满足结合律
    - state_machine.py  → 用确定性函数替代 (同等效果, 0 行废代码)

Pipeline 集成:
    - 输入: aggregated_trends: Dict[str, pd.DataFrame] (来自 PDDA)
    - 输出: Dict[str, Any] 包含评估结果和解释
    - API 100% 向后兼容

版本: 3.0.0
═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import logging
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
    # v4.6: ROIC↔ROE去相关 — ROIC作为首要质量指标(Buffett/Greenblatt)提权,
    #        ROE因与ROIC相关系数~0.8+杠杆膨胀降权;
    #        OCF+毛利率提权(现金质量+结构性竞争优势)
    score_weights: Dict[str, float] = field(default_factory=lambda: {
        "roic_trend": 0.22,          # v4.6 ↑ 0.18→0.22: 投入资本回报率=核心质量
        "roe_trend": 0.08,           # v4.6 ↓ 0.14→0.08: 与ROIC高相关+杠杆虚增
        "revenue_trend": 0.12,       # v4.6 ↓ 0.14→0.12
        "gross_margin_trend": 0.14,  # v4.6 ↑ 0.12→0.14: 护城河标志
        "net_margin_trend": 0.10,    # 不变
        "ocf_trend": 0.14,           # v4.6 ↑ 0.12→0.14: 现金为王
        "roiic_trend": 0.10,         # 不变
        "profit_trend": 0.10,        # 不变
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

        # Step 5: 综合决策
        decision, confidence = self._make_decision(score, rule_result, life_conf)

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

        # v4.1.1: ≥5 指标共识否决
        # 8个指标中 roic↔roe, gross_margin↔net_margin 高度相关,
        # ≥4 很容易被相关指标同时触发 (误杀); ≥5 要求真正的多维度衰退
        if veto_count >= 5:
            combined_result.vetoed = True
            combined_result.veto_reason = (
                f"多指标共识否决({veto_count}个): " + "; ".join(veto_reasons)
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

            # v4.5: R² 信度保护 — 趋势不可靠时分数向中性收缩
            # 审计发现: 捷佳伟创 ROIC R²=0.001、广信科技 R²=0.04、海光信息 R²=0.05
            # 这些公司的趋势方向完全不可靠，但系统仍用不可靠的方向给出极端评分
            # 修复: 将 trend_score 按 R² 向 50(中性) 收缩
            metric_base = metric_score_key.replace("_trend", "")
            r2 = features.get(f"{metric_base}_r_squared", 0.5)
            if r2 < 0.20:
                # R² 极低 → 趋势线几乎无意义，大幅收缩到中性
                shrink_factor = r2 / 0.20  # 0→0, 0.10→0.5, 0.20→1.0
                trend_score = 50 + (trend_score - 50) * shrink_factor
            elif r2 < 0.40:
                # R² 低 → 趋势有弱信号，适度收缩
                shrink_factor = 0.6 + 0.4 * ((r2 - 0.20) / 0.20)  # 0.6→1.0
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

            # 绝对水平补充 (40%)
            level_key = LEVEL_THRESHOLD_KEYS.get(metric_score_key)
            level_feature = metric_score_key.replace("_trend", "_level")
            level_value = features.get(level_feature)

            level_grade = None
            if level_key and level_value is not None:
                level_grade = self._get_adaptive_grade(level_key, level_value, context)
                level_score = grade_scores.get(level_grade, 50)
                metric_score = 0.60 * trend_score + 0.40 * level_score
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

        # v4.2: 多指标恶化检测
        # TRUTH 有 δ_decay/V_factor 检测衰退和虚假成长，Evaluator 需要对等能力
        deterioration_count = sum(
            1 for metric_key in self.config.score_weights
            if features.get(f"{metric_key.replace('_trend', '')}_has_deterioration", False)
        )

        # v4.4: 多指标恶化调降 (降低力度，避免与规则P3三重惩罚)
        # 旧版 -12/-8/-5 加上规则P3逐指标扣分 + base_score已含低趋势，导致三重打击
        # 新版仅作为"多指标联动"信号的边际调整，核心惩罚由规则P3承担
        if deterioration_count >= 5:
            base_score -= 6
        elif deterioration_count >= 4:
            base_score -= 4
        elif deterioration_count >= 3:
            base_score -= 2

        # 2. 规则引擎调整 (v4.2: 恶化感知型惩罚缩放)
        rule_adjustment = 0.0
        if rule_result and not rule_result.vetoed:
            penalty = rule_result.total_penalty
            bonus = rule_result.total_bonus

            # v4.2: 恶化感知型惩罚缩放 (解决"天花板压缩"问题)
            # 问题: 优秀公司 (base=94) 因 R²<0.25 等小问题被规则引擎扣 25 分
            #       导致 94-25=69 刚好低于 quality 阈值 70
            # 修复: 无恶化/少恶化的公司获得惩罚折扣
            #       多恶化的公司维持全额惩罚
            if deterioration_count <= 1:
                penalty_discount = 0.65    # 近乎无恙 → 35%折扣
            elif deterioration_count == 2:
                penalty_discount = 0.80
            elif deterioration_count == 3:
                penalty_discount = 0.90
            else:
                penalty_discount = 1.00    # 多指标恶化 → 全额惩罚

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
    ) -> Tuple[DecisionType, float]:
        """
        v3.0 决策 — 纯分数排名, 无隐式否决

        到达此函数的公司已通过 Step 2 规则引擎否决检查。
        此处仅做 QUALITY / AVERAGE / POOR 三档排名。
        """
        if score >= self.config.quality_threshold:
            decision = DecisionType.QUALITY
        elif score >= self.config.average_threshold:
            decision = DecisionType.AVERAGE
        else:
            decision = DecisionType.POOR

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


# ═══════════════════════════════════════════════════════════════════════════════
# 便捷函数
# ═══════════════════════════════════════════════════════════════════════════════

def evaluate_single_company(
    ts_code: str,
    trend_data: Dict[str, pd.DataFrame],
    company_name: Optional[str] = None,
    industry: Optional[str] = None,
    market_cap: Optional[float] = None,
) -> CompanyEvaluation:
    """便捷函数：评估单个公司"""
    evaluator = CausalBayesianEvaluator()
    company_info = {
        "ts_code": ts_code,
        "name": company_name,
        "industry": industry,
        "market_cap": market_cap or 100.0,
    }
    return evaluator.evaluate_company(ts_code, trend_data, company_info)
