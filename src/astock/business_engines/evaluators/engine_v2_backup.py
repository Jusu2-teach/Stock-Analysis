"""
═══════════════════════════════════════════════════════════════════════════════
AStock Evaluators v2.0 - 主引擎
═══════════════════════════════════════════════════════════════════════════════

因果贝叶斯网络 + 状态机评估引擎

设计哲学：
1. 因果推断（Pearl do-calculus）替代简单规则
2. 状态机（HMM）建模公司生命周期
3. Copula 处理证据相关性
4. Dempster-Shafer 融合不确定性证据
5. 时间衰减使近期数据权重更高
6. 自适应阈值根据行业/规模动态调整

Pipeline 集成：
- 输入: aggregated_trends: Dict[str, pd.DataFrame] (来自 PDDA)
- 输出: Dict[str, Any] 包含评估结果和解释

作者: AStock Team
版本: 2.0.0
═══════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# 内部模块
# 注意: temporal 模块保留但不在此使用（已由 trend 层完成时间衰减）
from .adaptive_threshold import (
    AdaptiveThresholdEngine,
    AdaptiveContext,
    IndustryCategory,
    SizeTier
)
from .causal_graph import CausalGraph, create_financial_causal_graph
from .state_machine import (
    CompanyStateMachine,
    CompanyState,
    StateInference,
    get_default_state_machine
)
from .copula_fusion import (
    Evidence,
    CopulaEvidenceFusion,
    CopulaFusionResult,
    get_default_fusion
)
from .dempster_shafer import (
    DSEvidenceEvaluator,
    DSEvaluationResult
)
from .explanation import (
    DecisionExplainer,
    DecisionType,
    Factor,
    ExplanationResult
)
from .rule_engine import (
    RuleEngine,
    RuleEngineResult,
    get_default_rule_engine,
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
# PDDA 列名映射（从共享模块导入）
# ═══════════════════════════════════════════════════════════════════════════════

from ..pdda_columns import PDDAColumns, ProbeData, CompanyProbes
from shared.naming_convention import MetricRegistry


# ═══════════════════════════════════════════════════════════════════════════════
# 配置
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class EvaluatorConfig:
    """评估器配置"""

    # 注意: 时间衰减已由 trend 层完成，此处不再需要
    # half_life_years 和 min_time_weight 已移除

    # 阈值调整
    use_adaptive_thresholds: bool = True

    # 因果推断
    use_causal_inference: bool = True
    causal_config_path: Optional[str] = None
    causal_score_weight: float = 0.15  # 因果诊断对评分的影响权重

    # 状态机
    use_state_machine: bool = True
    state_config_path: Optional[str] = None

    # 规则引擎（新增）
    use_rule_engine: bool = True
    rule_veto_enabled: bool = True  # 是否启用一票否决

    # 证据融合
    evidence_correlation_default: float = 0.45
    ds_conflict_threshold: float = 0.7
    copula_confidence_weight: float = 0.2  # Copula 有效证据数对置信度的调整权重

    # 评分权重（标准化后使用）
    score_weights: Dict[str, float] = field(default_factory=lambda: {
        "roic_trend": 0.17,
        "roe_trend": 0.13,
        "revenue_trend": 0.13,
        "gross_margin_trend": 0.10,
        "net_margin_trend": 0.08,
        "ocf_trend": 0.11,
        "roiic_trend": 0.08,
        "fcf_margin_trend": 0.08,
        "asset_turnover_trend": 0.07,
        "state_bonus": 0.05
    })

    # 决策阈值
    quality_threshold: float = 70.0
    average_threshold: float = 50.0
    veto_threshold: float = 30.0

    # 趋势特征到评分指标的映射（确保键名一致）
    trend_to_score_mapping: Dict[str, str] = field(default_factory=lambda: {
        "roic": "roic_trend",
        "roe": "roe_trend",
        "revenue": "revenue_trend",
        "gross_margin": "gross_margin_trend",
        "net_margin": "net_margin_trend",
        "ocf": "ocf_trend",
        "roiic": "roiic_trend",
        "fcf_margin": "fcf_margin_trend",
        "asset_turnover": "asset_turnover_trend",
    })


# ═══════════════════════════════════════════════════════════════════════════════
# 评估结果
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class CompanyEvaluation:
    """单个公司的评估结果"""

    ts_code: str
    name: Optional[str] = None
    industry: Optional[str] = None

    # 核心评估
    score: float = 0.0
    decision: DecisionType = DecisionType.UNCERTAIN
    confidence: float = 0.0

    # 状态推断
    company_state: Optional[CompanyState] = None
    state_confidence: float = 0.0

    # 因素分析
    factors: List[Factor] = field(default_factory=list)

    # 证据融合
    ds_result: Optional[DSEvaluationResult] = None
    copula_result: Optional[CopulaFusionResult] = None

    # 因果诊断
    causal_diagnosis: Optional[Dict[str, Any]] = None

    # 规则引擎结果（新增）
    rule_result: Optional[RuleEngineResult] = None
    vetoed: bool = False
    veto_reason: str = ""

    # 解释
    explanation: Optional[ExplanationResult] = None

    # 原始数据引用
    trend_data: Optional[Dict[str, pd.DataFrame]] = None

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "ts_code": self.ts_code,
            "name": self.name,
            "industry": self.industry,
            "score": self.score,
            "decision": self.decision.value,
            "confidence": self.confidence,
            "company_state": self.company_state.value if self.company_state else None,
            "state_confidence": self.state_confidence,
            "vetoed": self.vetoed,
            "veto_reason": self.veto_reason,
            "factors": [
                {
                    "name": f.name,
                    "value": f.value,
                    "contribution": f.contribution,
                    "direction": f.direction
                }
                for f in self.factors
            ],
            "causal_diagnosis": self.causal_diagnosis,
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
    因果贝叶斯评估引擎

    整合所有子模块，提供统一的评估接口。

    Architecture:
        ┌─────────────────────────────────────────────────────────────┐
        │                   CausalBayesianEvaluator                   │
        ├─────────────────────────────────────────────────────────────┤
        │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
        │  │ TemporalDecay│  │ AdaptiveThreshold │  │ CausalGraph │  │
        │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘       │  │
        │         │                │                │               │
        │  ┌──────▼──────────────▼──────────────▼──────┐           │
        │  │              Evidence Collection           │           │
        │  └──────────────────────┬────────────────────┘           │
        │                         │                                 │
        │  ┌──────────────────────▼────────────────────┐           │
        │  │  ┌─────────────┐    ┌─────────────────┐   │           │
        │  │  │ CopulaFusion│ +  │ Dempster-Shafer │   │           │
        │  │  └──────┬──────┘    └──────┬──────────┘   │           │
        │  │         └───────┬─────────┘               │           │
        │  └─────────────────┼─────────────────────────┘           │
        │                    │                                     │
        │  ┌─────────────────▼─────────────────┐                   │
        │  │     StateMachine (HMM)             │                   │
        │  └─────────────────┬─────────────────┘                   │
        │                    │                                     │
        │  ┌─────────────────▼─────────────────┐                   │
        │  │     DecisionExplainer              │                   │
        │  └─────────────────┬─────────────────┘                   │
        │                    │                                     │
        │                    ▼                                     │
        │            CompanyEvaluation                             │
        └─────────────────────────────────────────────────────────┘

    Example:
        >>> evaluator = CausalBayesianEvaluator()
        >>> result = evaluator.evaluate_company(
        ...     ts_code="000001.SZ",
        ...     trend_data=aggregated_trends,
        ...     company_info={"industry": "银行", "market_cap": 3000}
        ... )
    """

    def __init__(self, config: Optional[EvaluatorConfig] = None):
        self.config = config or EvaluatorConfig()

        # 配置文件目录
        config_dir = Path(__file__).parent / "config"

        # 初始化子模块
        # 注意: 时间衰减已由 trend 层完成，此处不再初始化 TemporalDecay

        # 从 YAML 加载自适应阈值配置
        threshold_config = config_dir / "adaptive_thresholds.yaml"
        if threshold_config.exists():
            self._threshold_engine = AdaptiveThresholdEngine.from_config(threshold_config)
            logger.info(f"Loaded adaptive thresholds from {threshold_config}")
        else:
            self._threshold_engine = AdaptiveThresholdEngine.with_defaults()
            logger.warning("adaptive_thresholds.yaml not found, using defaults")

        # 从 YAML 加载因果图配置
        if self.config.use_causal_inference:
            causal_config = config_dir / "causal_structure.yaml"
            if causal_config.exists():
                self._causal_graph = CausalGraph.from_config(causal_config)
                logger.info(f"Loaded causal graph from {causal_config}")
            else:
                self._causal_graph = create_financial_causal_graph()
                logger.warning("causal_structure.yaml not found, using defaults")
        else:
            self._causal_graph = None

        # 从 YAML 加载状态机配置
        if self.config.use_state_machine:
            state_config = config_dir / "state_machine.yaml"
            if state_config.exists():
                self._state_machine = CompanyStateMachine.from_config(state_config)
                logger.info(f"Loaded state machine from {state_config}")
            else:
                self._state_machine = get_default_state_machine()
                logger.warning("state_machine.yaml not found, using defaults")
        else:
            self._state_machine = None

        # 从 YAML 加载规则引擎
        rules_config = config_dir / "rules.yaml"
        if rules_config.exists():
            self._rule_engine = RuleEngine.from_config(rules_config)
            logger.info(f"Loaded rule engine from {rules_config}")
        else:
            self._rule_engine = RuleEngine.with_defaults()
            logger.warning("rules.yaml not found, using empty rule engine")

        self._copula_fusion = CopulaEvidenceFusion(
            default_correlation=self.config.evidence_correlation_default
        )

        # 存储每公司上次状态推断，用于 HMM 贝叶斯更新
        self._company_prior_states: Dict[str, CompanyState] = {}

        logger.info(f"CausalBayesianEvaluator initialized with config: {self.config}")

    def evaluate_company(
        self,
        ts_code: str,
        trend_data: Dict[str, pd.DataFrame],
        company_info: Optional[Dict[str, Any]] = None
    ) -> CompanyEvaluation:
        """
        评估单个公司（完整流程）

        处理流程：
        1. 提取 PDDA 特征
        2. 创建自适应上下文
        3. 【规则引擎】执行 veto/penalty/bonus 规则（首先检查是否被一票否决）
        4. 推断公司状态（HMM）
        5. 收集证据
        6. Copula 融合（处理证据相关性）
        7. Dempster-Shafer 融合（处理不确定性）
        8. 因果诊断（Pearl do-calculus）
        9. 计算综合评分（整合所有组件结果）
        10. 做出决策
        11. 生成解释

        Args:
            ts_code: 股票代码
            trend_data: 趋势分析数据，键为指标名（roic, roe, ...）
                       每个 DataFrame 来自 PDDA 聚合，每公司只有 1 行
            company_info: 公司信息（可选）
                - name: 公司名称
                - industry: 行业
                - market_cap: 市值（亿元）
                - market_cycle: 市场周期 (expansion/peak/contraction/trough)

        Returns:
            CompanyEvaluation 完整评估结果
        """
        company_info = company_info or {}

        # 提取该公司的趋势数据
        company_trends = self._extract_company_trends(ts_code, trend_data)

        if not company_trends:
            logger.warning(f"No trend data found for {ts_code}")
            return CompanyEvaluation(
                ts_code=ts_code,
                name=company_info.get("name"),
                decision=DecisionType.UNCERTAIN,
                confidence=0.0
            )

        # 1. 从 PDDA 单行输出直接提取特征
        features = self._extract_features_from_pdda(company_trends)

        # 2. 创建自适应上下文
        context = self._create_adaptive_context(company_info)

        # 3. 【关键修复】规则引擎评估（首先执行，检查是否被一票否决）
        rule_result = None
        vetoed = False
        veto_reason = ""

        if self.config.use_rule_engine and self._rule_engine:
            rule_result = self._run_rule_engine(features, context)
            if rule_result.vetoed and self.config.rule_veto_enabled:
                # 一票否决：直接返回
                vetoed = True
                veto_reason = rule_result.veto_reason
                logger.info(f"{ts_code} vetoed by rule engine: {veto_reason}")

                return CompanyEvaluation(
                    ts_code=ts_code,
                    name=company_info.get("name"),
                    industry=company_info.get("industry"),
                    score=0.0,
                    decision=DecisionType.VETO,
                    confidence=0.95,  # 规则引擎的否决具有高置信度
                    vetoed=True,
                    veto_reason=veto_reason,
                    rule_result=rule_result,
                    factors=[Factor(
                        name="veto_rule",
                        display_name="一票否决",
                        value=0.0,
                        contribution=-1.0,
                        direction="negative",
                        explanation=veto_reason
                    )],
                    trend_data=company_trends
                )

        # 4. 推断公司状态（HMM）
        state_inference = self._infer_company_state(features, ts_code=ts_code)

        # 5. 收集证据（充分利用 PDDA 的布尔特征）
        evidences = self._collect_evidences(features, context)

        # 6. 【关键修复】Copula 融合（处理证据相关性）
        copula_result = self._copula_fusion.fuse(evidences)

        # 7. 【关键修复】Dempster-Shafer 融合（使用动态 target）
        ds_result = self._ds_evaluate_with_dynamic_target(evidences, features)

        # 8. 【关键修复】因果诊断（Pearl do-calculus）
        causal_diagnosis = None
        causal_adjustment = 0.0
        if self._causal_graph and self.config.use_causal_inference:
            causal_diagnosis = self._run_causal_diagnosis(features)
            # 因果诊断影响评分
            causal_adjustment = self._compute_causal_adjustment(causal_diagnosis)

        # 9. 【关键修复】计算综合评分（整合所有组件）
        score, factors = self._compute_integrated_score(
            features=features,
            state_inference=state_inference,
            copula_result=copula_result,
            ds_result=ds_result,
            rule_result=rule_result,
            causal_adjustment=causal_adjustment,
            context=context
        )

        # 10. 做出决策（整合所有信号）
        decision, confidence = self._make_integrated_decision(
            score=score,
            ds_result=ds_result,
            state_inference=state_inference,
            copula_result=copula_result,
            rule_result=rule_result
        )

        # 11. 生成解释
        explanation = self._generate_explanation(
            ts_code, company_info, decision, confidence, factors,
            score, state_inference, causal_diagnosis
        )

        return CompanyEvaluation(
            ts_code=ts_code,
            name=company_info.get("name"),
            industry=company_info.get("industry"),
            score=score,
            decision=decision,
            confidence=confidence,
            company_state=state_inference.most_likely_state if state_inference else None,
            state_confidence=state_inference.confidence if state_inference else 0.0,
            factors=factors,
            ds_result=ds_result,
            copula_result=copula_result,
            causal_diagnosis=causal_diagnosis,
            rule_result=rule_result,
            vetoed=vetoed,
            veto_reason=veto_reason,
            explanation=explanation,
            trend_data=company_trends
        )

    def _extract_company_trends(
        self,
        ts_code: str,
        trend_data: Dict[str, pd.DataFrame]
    ) -> Dict[str, pd.DataFrame]:
        """提取单个公司的趋势数据"""
        company_trends = {}

        for metric, df in trend_data.items():
            if df is None or df.empty:
                continue

            # 假设 DataFrame 有 ts_code 列
            if "ts_code" in df.columns:
                company_df = df[df["ts_code"] == ts_code]
                if not company_df.empty:
                    company_trends[metric] = company_df
            else:
                # 如果没有 ts_code 列，假设整个 DataFrame 就是单个公司的
                company_trends[metric] = df

        return company_trends

    def _extract_features_from_pdda(
        self,
        company_trends: Dict[str, pd.DataFrame]
    ) -> Dict[str, Any]:
        """
        从 PDDA 单行聚合结果直接提取特征

        PDDA 输出格式：每公司每指标 1 行，包含 ~40 个预计算特征
        此方法直接映射这些特征，不做二次计算

        Args:
            company_trends: {metric_name: DataFrame} 每个 DataFrame 只有 1 行

        Returns:
            特征字典，包含所有指标的趋势、水平、波动、恶化等特征
        """
        features: Dict[str, Any] = {}
        C = PDDAColumns  # 列名常量

        for metric, df in company_trends.items():
            if df.empty:
                continue

            # PDDA 键为 business_key（如 gross_margin），列前缀为 output_prefix（如 grossprofit_margin）
            try:
                prefix = MetricRegistry.get_output_prefix(metric)
            except (ValueError, KeyError):
                prefix = metric

            # PDDA 输出每公司只有 1 行
            row = df.iloc[0]

            # ============ 趋势特征 ============
            # 使用 OLS 对数斜率（与 rules.yaml 阈值对齐），回退到线性斜率
            log_slope_col = C.col(prefix, C.LOG_SLOPE)
            slope_col = C.col(prefix, C.SLOPE)

            if log_slope_col in row.index and pd.notna(row[log_slope_col]):
                features[f"{metric}_trend"] = float(row[log_slope_col])
            elif slope_col in row.index and pd.notna(row[slope_col]):
                features[f"{metric}_trend"] = float(row[slope_col])
            else:
                features[f"{metric}_trend"] = 0.0

            # R² 拟合优度
            r2_col = C.col(prefix, C.R_SQUARED)
            features[f"{metric}_r_squared"] = (
                float(row[r2_col]) if r2_col in row.index and pd.notna(row[r2_col]) else 0.0
            )

            # Mann-Kendall tau（非参数趋势）
            mk_col = C.col(prefix, C.MK_TAU)
            features[f"{metric}_mk_tau"] = (
                float(row[mk_col]) if mk_col in row.index and pd.notna(row[mk_col]) else 0.0
            )

            # 趋势方向（PDDA 已判断）
            dir_col = C.col(prefix, C.TREND_DIRECTION)
            features[f"{metric}_direction"] = (
                str(row[dir_col]) if dir_col in row.index else "flat"
            )

            # 近3年趋势（短期）
            recent_col = C.col(prefix, C.RECENT_3Y_SLOPE)
            features[f"{metric}_recent_trend"] = (
                float(row[recent_col]) if recent_col in row.index and pd.notna(row[recent_col]) else 0.0
            )

            # ============ 水平特征 ============
            latest_col = C.col(prefix, C.LATEST_VALUE)
            features[f"{metric}_level"] = (
                float(row[latest_col]) if latest_col in row.index and pd.notna(row[latest_col]) else 0.0
            )

            weighted_col = C.col(prefix, C.WEIGHTED_AVG)
            features[f"{metric}_weighted_avg"] = (
                float(row[weighted_col]) if weighted_col in row.index and pd.notna(row[weighted_col]) else 0.0
            )

            # 最新值 vs 加权平均（衡量近期表现）
            ratio_col = C.col(prefix, C.LATEST_VS_WEIGHTED)
            features[f"{metric}_latest_vs_weighted"] = (
                float(row[ratio_col]) if ratio_col in row.index and pd.notna(row[ratio_col]) else 1.0
            )

            # ============ 波动特征 ============
            cv_col = C.col(prefix, C.CV)
            features[f"{metric}_volatility"] = (
                float(row[cv_col]) if cv_col in row.index and pd.notna(row[cv_col]) else 0.2
            )

            vol_type_col = C.col(prefix, C.VOLATILITY_TYPE)
            features[f"{metric}_volatility_type"] = (
                str(row[vol_type_col]) if vol_type_col in row.index else "moderate"
            )

            # ============ 恶化检测（布尔特征）============
            det_col = C.col(prefix, C.HAS_DETERIORATION)
            features[f"{metric}_has_deterioration"] = (
                bool(row[det_col]) if det_col in row.index else False
            )

            sev_col = C.col(prefix, C.DETERIORATION_SEVERITY)
            features[f"{metric}_deterioration_severity"] = (
                str(row[sev_col]) if sev_col in row.index else "none"
            )

            decline_col = C.col(prefix, C.TOTAL_DECLINE_PCT)
            # total_decline_pct 存储为百分点（如 -37.5），转换为绝对比率（0.375）以匹配 rules.yaml 阈值
            raw_decline = (
                float(row[decline_col]) if decline_col in row.index and pd.notna(row[decline_col]) else 0.0
            )
            features[f"{metric}_decline_pct"] = abs(raw_decline) / 100.0

            # ============ 拐点检测 ============
            infl_col = C.col(prefix, C.HAS_INFLECTION)
            features[f"{metric}_has_inflection"] = (
                bool(row[infl_col]) if infl_col in row.index else False
            )

            # ============ 周期性特征 ============
            cyc_col = C.col(prefix, C.IS_CYCLICAL)
            features[f"{metric}_is_cyclical"] = (
                bool(row[cyc_col]) if cyc_col in row.index else False
            )

            phase_col = C.col(prefix, C.CURRENT_PHASE)
            features[f"{metric}_cycle_phase"] = (
                str(row[phase_col]) if phase_col in row.index else ""
            )

            # ============ 加速/减速 ============
            acc_col = C.col(prefix, C.IS_ACCELERATING)
            features[f"{metric}_is_accelerating"] = (
                bool(row[acc_col]) if acc_col in row.index else False
            )

            dec_col = C.col(prefix, C.IS_DECELERATING)
            features[f"{metric}_is_decelerating"] = (
                bool(row[dec_col]) if dec_col in row.index else False
            )

            # ============ 结构断点 ============
            break_col = C.col(prefix, C.HAS_STRUCTURAL_BREAK)
            features[f"{metric}_has_break"] = (
                bool(row[break_col]) if break_col in row.index else False
            )

            regime_col = C.col(prefix, C.DATA_REGIME)
            features[f"{metric}_data_regime"] = (
                str(row[regime_col]) if regime_col in row.index else "stable"
            )

        return features

    def _create_adaptive_context(
        self,
        company_info: Dict[str, Any]
    ) -> AdaptiveContext:
        """创建自适应阈值上下文"""
        industry = company_info.get("industry", "default")
        market_cap = company_info.get("market_cap", 100.0)
        market_cycle = company_info.get("market_cycle", "expansion")

        return AdaptiveContext.from_company_info(
            industry_name=industry,
            market_cap=market_cap,
            current_cycle=market_cycle
        )

    def _infer_company_state(
        self,
        features: Dict[str, Any],
        ts_code: str = ""
    ) -> Optional[StateInference]:
        """
        推断公司状态（HMM）

        修复：统一使用 PDDA 提取的键名，传入先验状态用于贝叶斯更新
        """
        if not self._state_machine:
            return None

        # 准备状态机特征
        revenue_trend = features.get("revenue_trend", 0.0)

        # 状态机期望的是增长率形式，使用 latest_vs_weighted 作为代理
        revenue_growth_proxy = features.get("revenue_latest_vs_weighted", 1.0) - 1.0
        if abs(revenue_growth_proxy) < 0.01:
            revenue_growth_proxy = revenue_trend * 10

        state_features = {
            "revenue_growth": revenue_growth_proxy,
            "roic_level": features.get("roic_level", 10.0),
            "roic_trend": features.get("roic_trend", 0.0),
            "volatility": features.get("roic_volatility", 0.2),
            "roe_level": features.get("roe_level", 10.0),
            "gross_margin_level": features.get("gross_margin_level", 20.0),
        }

        # 【H4 修复】传入先验状态进行贝叶斯更新
        prior_state = self._company_prior_states.get(ts_code)

        inference = self._state_machine.infer_state(
            state_features,
            prior_state=prior_state
        )

        # 缓存当前推断状态，供下次评估使用
        if inference and ts_code:
            self._company_prior_states[ts_code] = inference.most_likely_state

        return inference

    def _collect_evidences(
        self,
        features: Dict[str, Any],
        context: AdaptiveContext
    ) -> List[Evidence]:
        """
        收集证据

        充分利用 PDDA 提供的丰富特征，包括：
        - 趋势斜率（连续值）
        - 恶化检测（布尔值）
        - 波动类型（分类值）
        - 周期性状态（分类值）
        """
        evidences = []

        # ============ 1. 趋势斜率证据（连续值 → 概率）============
        trend_configs = [
            # (特征键, 正向阈值, 基础置信度, 权重)
            ("roic_trend", 0.02, 0.9, 1.0),
            ("roe_trend", 0.01, 0.85, 0.9),
            ("revenue_trend", 0.05, 0.8, 0.8),
            ("gross_margin_trend", 0.0, 0.8, 0.85),
            ("net_margin_trend", 0.0, 0.8, 0.8),
            ("ocf_trend", 0.0, 0.75, 0.75),
            ("roiic_trend", 0.05, 0.7, 0.6),
            ("fcf_margin_trend", 0.0, 0.75, 0.65),
            ("asset_turnover_trend", 0.0, 0.70, 0.55),
        ]

        # 每个指标的 sigmoid 温度参数（控制转换灵敏度）
        sigmoid_temperatures = {
            "roic_trend": 0.03,
            "roe_trend": 0.04,
            "revenue_trend": 0.08,
            "gross_margin_trend": 0.05,
            "net_margin_trend": 0.05,
            "ocf_trend": 0.06,
            "roiic_trend": 0.10,
            "fcf_margin_trend": 0.06,
            "asset_turnover_trend": 0.04,
        }

        for metric_key, threshold, base_conf, weight in trend_configs:
            if metric_key in features:
                value = features[metric_key]

                # Sigmoid 转换：趋势值 → 质量概率（使用每指标温度）
                temperature = sigmoid_temperatures.get(metric_key, 0.05)
                z = (value - threshold) / temperature
                prob_quality = 1 / (1 + np.exp(-z))

                evidence = Evidence.from_probability(
                    name=metric_key,
                    value=value,
                    prob_positive=prob_quality,
                    confidence=base_conf * weight
                )
                evidences.append(evidence)

        # ============ 2. 恶化检测证据（布尔值 → 强证据）============
        deterioration_metrics = ["roic", "roe", "gross_margin", "net_margin"]

        for metric in deterioration_metrics:
            det_key = f"{metric}_has_deterioration"
            sev_key = f"{metric}_deterioration_severity"

            if det_key in features and features[det_key]:
                severity = features.get(sev_key, "moderate")

                # 根据严重程度设置证据强度
                severity_belief = {
                    "mild": 0.3,
                    "moderate": 0.5,
                    "severe": 0.8
                }.get(severity, 0.5)

                evidence = Evidence(
                    name=f"{metric}_deterioration",
                    value=severity,
                    belief=0.0,  # 不支持"质量"
                    disbelief=severity_belief,  # 支持"非质量"
                    uncertainty=1.0 - severity_belief
                )
                evidences.append(evidence)

        # ============ 3. 波动性证据（分类值 → 证据）============
        volatility_metrics = ["roic", "roe", "gross_margin"]

        for metric in volatility_metrics:
            vol_key = f"{metric}_volatility_type"

            if vol_key in features:
                vol_type = features[vol_key]

                # 波动性映射
                vol_evidence_map = {
                    "stable": (0.7, 0.1, 0.2),      # (belief, disbelief, uncertainty)
                    "moderate": (0.4, 0.2, 0.4),
                    "volatile": (0.1, 0.5, 0.4),
                    "high_volatility": (0.0, 0.7, 0.3)
                }

                if vol_type in vol_evidence_map:
                    b, d, u = vol_evidence_map[vol_type]
                    evidence = Evidence(
                        name=f"{metric}_stability",
                        value=vol_type,
                        belief=b,
                        disbelief=d,
                        uncertainty=u
                    )
                    evidences.append(evidence)

        # ============ 4. 周期性证据（影响评估策略）============
        for metric in ["roic", "revenue", "gross_margin"]:
            cyc_key = f"{metric}_is_cyclical"
            phase_key = f"{metric}_cycle_phase"

            if cyc_key in features and features[cyc_key]:
                phase = features.get(phase_key, "")

                # 周期底部是积极信号（困境反转）
                if phase in ["bottom", "rising"]:
                    evidence = Evidence(
                        name=f"{metric}_cycle_bottom",
                        value=phase,
                        belief=0.6,
                        disbelief=0.1,
                        uncertainty=0.3
                    )
                    evidences.append(evidence)
                elif phase in ["top", "falling"]:
                    # 顶部/下降是警告信号
                    evidence = Evidence(
                        name=f"{metric}_cycle_top",
                        value=phase,
                        belief=0.2,
                        disbelief=0.4,
                        uncertainty=0.4
                    )
                    evidences.append(evidence)

        # ============ 5. 结构断点证据 ============
        for metric in ["roic", "roe", "revenue"]:
            break_key = f"{metric}_has_break"

            if break_key in features and features[break_key]:
                # 结构断点增加不确定性
                evidence = Evidence(
                    name=f"{metric}_structural_break",
                    value=True,
                    belief=0.2,
                    disbelief=0.2,
                    uncertainty=0.6  # 高不确定性
                )
                evidences.append(evidence)

        # ============ 6. 近期表现证据（最新值 vs 加权平均）============
        for metric in ["roic", "roe", "gross_margin"]:
            ratio_key = f"{metric}_latest_vs_weighted"

            if ratio_key in features:
                ratio = features[ratio_key]

                # ratio > 1 表示近期表现优于历史平均
                if ratio > 1.2:
                    evidence = Evidence(
                        name=f"{metric}_improving",
                        value=ratio,
                        belief=0.7,
                        disbelief=0.1,
                        uncertainty=0.2
                    )
                    evidences.append(evidence)
                elif ratio < 0.8:
                    evidence = Evidence(
                        name=f"{metric}_declining",
                        value=ratio,
                        belief=0.1,
                        disbelief=0.6,
                        uncertainty=0.3
                    )
                    evidences.append(evidence)

        return evidences

    def _ds_evaluate(
        self,
        evidences: List[Evidence]
    ) -> DSEvaluationResult:
        """Dempster-Shafer 评估"""
        evaluator = DSEvidenceEvaluator(
            conflict_threshold=self.config.ds_conflict_threshold
        )

        for evidence in evidences:
            evaluator.add_evidence(
                name=evidence.name,
                target="quality",
                belief=evidence.belief,
                disbelief=evidence.disbelief,
                uncertainty=evidence.uncertainty
            )

        return evaluator.evaluate("quality")

    def _ds_evaluate_with_dynamic_target(
        self,
        evidences: List[Evidence],
        features: Dict[str, Any]
    ) -> DSEvaluationResult:
        """
        【修复】Dempster-Shafer 评估 - 使用动态 target

        根据证据的极性动态选择 target，而不是全部硬编码为 "quality"。
        这样可以正确利用 DS 的三分类能力。
        """
        evaluator = DSEvidenceEvaluator(
            conflict_threshold=self.config.ds_conflict_threshold
        )

        for evidence in evidences:
            # 【深化修复】保留原始极性信息，不再用 max/min 破坏
            if evidence.belief >= evidence.disbelief:
                # 正面证据：直接支持 "quality"
                evaluator.add_evidence(
                    name=evidence.name,
                    target="quality",
                    belief=evidence.belief,
                    disbelief=evidence.disbelief,
                    uncertainty=evidence.uncertainty
                )
            else:
                # 负面证据：支持 "poor"
                # belief/disbelief 交换映射到 "poor" target:
                #   "我相信这是 poor 的程度" = 原始 disbelief
                #   "我怀疑它是 poor 的程度" = 原始 belief
                evaluator.add_evidence(
                    name=evidence.name,
                    target="poor",
                    belief=evidence.disbelief,
                    disbelief=evidence.belief,
                    uncertainty=evidence.uncertainty
                )

        return evaluator.evaluate("quality")

    def _run_rule_engine(
        self,
        features: Dict[str, Any],
        context: AdaptiveContext
    ) -> RuleEngineResult:
        """
        【新增】运行规则引擎

        执行 rules.yaml 中定义的：
        - veto_rules: 一票否决规则
        - penalty_rules: 扣分规则
        - bonus_rules: 加分规则
        - strategies: 投资策略识别
        """
        if not self._rule_engine:
            return RuleEngineResult(
                base_score=100.0,
                final_score=100.0,
                grade="C"
            )

        # 获取自适应阈值
        thresholds = {}
        try:
            for metric_name in ["roic_level", "roe_level", "gross_margin", "net_margin"]:
                ts = self._threshold_engine.get_thresholds(metric_name, context)
                thresholds[f"{metric_name}_high"] = ts.excellent
                thresholds[f"{metric_name}_moat_min"] = ts.good
        except (ValueError, AttributeError):
            pass  # 使用默认阈值

        # 对每个核心指标执行规则评估
        # 【进化修复】多指标共识否决: 需要 ≥2 个指标同时触发否决规则
        combined_result = RuleEngineResult(
            base_score=100.0,
            final_score=100.0,
            grade="C"
        )

        veto_count = 0
        veto_reasons = []

        for metric in ["roic", "roe", "gross_margin", "net_margin"]:
            # 【C3 修复】为每个指标独立构建完整特征字典，避免跨指标覆盖
            metric_features: Dict[str, Any] = {}

            # 趋势斜率
            trend_key = f"{metric}_trend"
            if trend_key in features:
                metric_features["log_slope"] = features[trend_key]
                metric_features[trend_key] = features[trend_key]

            # R² 拟合优度
            r2_key = f"{metric}_r_squared"
            if r2_key in features:
                metric_features["r_squared"] = features[r2_key]

            # 变异系数
            cv_key = f"{metric}_volatility"
            if cv_key in features:
                metric_features["cv"] = features[cv_key]

            # 最新值
            level_key = f"{metric}_level"
            if level_key in features:
                metric_features["latest_value"] = features[level_key]

            # 恶化检测
            det_key = f"{metric}_has_deterioration"
            if det_key in features:
                metric_features["has_deterioration"] = features[det_key]

            sev_key = f"{metric}_deterioration_severity"
            if sev_key in features:
                metric_features["deterioration_severity"] = features[sev_key]

            # 下降幅度（恶化探针3年变化，用于 severe_deterioration 规则）
            decline_key = f"{metric}_decline_pct"
            if decline_key in features:
                metric_features["decline_pct"] = features[decline_key]

            # 峰值下跌（从加权平均估算峰值到最新值的跌幅，用于 peak_decline 规则）
            weighted_key = f"{metric}_weighted_avg"
            level_val = features.get(level_key, 0)
            weighted_val = features.get(weighted_key, 0)
            if weighted_val > 0 and level_val < weighted_val:
                metric_features["peak_decline_pct"] = (weighted_val - level_val) / weighted_val
            else:
                metric_features["peak_decline_pct"] = 0.0

            # 周期性
            cyc_key = f"{metric}_is_cyclical"
            if cyc_key in features:
                metric_features["is_cyclical"] = features[cyc_key]

            phase_key = f"{metric}_cycle_phase"
            if phase_key in features:
                metric_features["cycle_phase"] = features[phase_key]

            # 计算 max_value（近似）
            weighted_key = f"{metric}_weighted_avg"
            if weighted_key in features and level_key in features:
                level = features.get(level_key, 0)
                weighted = features.get(weighted_key, level)
                metric_features["max_value"] = max(level, weighted * 1.5)

            result = self._rule_engine.evaluate(
                features=metric_features,
                metric_name=metric,
                thresholds=thresholds
            )

            # 合并结果 - 多指标共识否决
            if result.vetoed:
                veto_count += 1
                veto_reasons.append(f"[{metric}] {result.veto_reason}")
                combined_result.veto_rules.extend(result.veto_rules)

            combined_result.penalty_rules.extend(result.penalty_rules)
            combined_result.bonus_rules.extend(result.bonus_rules)
            combined_result.strategies.extend(result.strategies)
            combined_result.total_penalty += result.total_penalty
            combined_result.total_bonus += result.total_bonus

        # 【多维共识否决】需要 ≥3 个指标同时触发否决规则
        # 避免 ROIC/ROE 高相关性导致 2 指标就淘汰的假阳性
        if veto_count >= 3:
            combined_result.vetoed = True
            combined_result.veto_reason = f"多指标共识否决({veto_count}个): " + "; ".join(veto_reasons)

        # 【C4 修复】跨指标累计后再做一次 cap，防止多指标累加超限
        max_penalty = self._rule_engine._max_penalty if hasattr(self._rule_engine, '_max_penalty') else 50.0
        max_bonus = self._rule_engine._max_bonus if hasattr(self._rule_engine, '_max_bonus') else 30.0
        combined_result.total_penalty = min(combined_result.total_penalty, max_penalty)
        combined_result.total_bonus = min(combined_result.total_bonus, max_bonus)

        # 计算最终分数
        if not combined_result.vetoed:
            combined_result.final_score = max(
                0,
                min(100, 100 - combined_result.total_penalty + combined_result.total_bonus)
            )
            combined_result.grade = self._score_to_grade(combined_result.final_score)

        return combined_result

    def _score_to_grade(self, score: float) -> str:
        """分数转等级"""
        if score >= 90:
            return "A"
        elif score >= 80:
            return "B"
        elif score >= 70:
            return "C"
        elif score >= 60:
            return "D"
        else:
            return "F"

    def _run_causal_diagnosis(
        self,
        features: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        运行因果诊断

        使用因果图对核心指标（roic_trend）进行诊断，
        识别其变化的主要原因和混淆因子。

        Args:
            features: 提取的特征字典

        Returns:
            诊断结果字典，包含 status, confidence, primary_causes 等
        """
        if not self._causal_graph:
            return None

        try:
            # 构建因果图需要的观测数据（仅保留数值型特征）
            observed_data = {}
            for key, value in features.items():
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    observed_data[key] = float(value)

            # 对核心指标 roic_trend 进行因果诊断
            diagnosis = self._causal_graph.diagnose(
                target_metric="roic_trend",
                observed_data=observed_data
            )

            return {
                "status": diagnosis.status,
                "confidence": diagnosis.confidence,
                "primary_causes": diagnosis.primary_causes,
                "confounders": diagnosis.confounders_detected,
                "suggestions": diagnosis.intervention_suggestions,
                "explanation": diagnosis.explanation,
            }
        except Exception as e:
            logger.warning(f"Causal diagnosis failed: {e}")
            return None

    def _compute_causal_adjustment(
        self,
        causal_diagnosis: Optional[Dict[str, Any]]
    ) -> float:
        """
        根据因果诊断计算评分调整

        如果因果诊断发现明确的恶化原因，降低评分；
        如果发现改善原因，提高评分。

        【M4 修复】增大调整量级，使因果诊断对最终评分有实质影响
        """
        if not causal_diagnosis:
            return 0.0

        status = causal_diagnosis.get("status", "stable")
        confidence = causal_diagnosis.get("confidence", 0.5)
        primary_causes = causal_diagnosis.get("primary_causes", [])

        adjustment = 0.0

        if status == "declining":
            # 恶化：根据置信度和原因数量扣分（最大 -10 分）
            base_penalty = -10.0
            cause_factor = min(len(primary_causes), 3) / 3
            adjustment = base_penalty * confidence * (0.5 + 0.5 * cause_factor)

        elif status == "improving":
            # 改善：根据置信度加分（最大 +5 分）
            base_bonus = 5.0
            adjustment = base_bonus * confidence

        return adjustment

    def _compute_integrated_score(
        self,
        features: Dict[str, Any],
        state_inference: Optional[StateInference],
        copula_result: CopulaFusionResult,
        ds_result: DSEvaluationResult,
        rule_result: Optional[RuleEngineResult],
        causal_adjustment: float,
        context: AdaptiveContext
    ) -> Tuple[float, List[Factor]]:
        """
        【关键修复】计算综合评分 - 整合所有组件

        整合：
        1. 基于趋势特征的基础分数
        2. 规则引擎的扣分/加分
        3. 状态机的调整
        4. 因果诊断的调整
        5. Copula 有效证据数对置信度的影响
        6. DS 冲突对确定性的影响
        """
        factors = []
        weighted_sum = 0.0
        total_weight = 0.0

        # ============ 1. 基于趋势特征的基础分数 ============
        for metric, weight in self.config.score_weights.items():
            if metric == "state_bonus":
                continue

            feature_key = metric
            if feature_key not in features:
                continue

            value = features[feature_key]

            # 获取自适应阈值
            grade = self._get_adaptive_grade(metric, value, context)

            # 等级转分数
            grade_scores = {
                "excellent": 95,
                "good": 80,
                "acceptable": 60,
                "poor": 40,
                "veto": 10
            }
            metric_score = grade_scores.get(grade, 50)

            # 计算方向
            direction = "positive" if value > 0.005 else ("negative" if value < -0.005 else "neutral")

            # 计算贡献度
            contribution = (metric_score - 50) / 50 * weight

            factor = Factor(
                name=metric,
                display_name=metric,
                value=value,
                contribution=contribution,
                direction=direction
            )
            factors.append(factor)

            weighted_sum += metric_score * weight
            total_weight += weight

        # 基础分数
        if total_weight > 0:
            base_score = weighted_sum / total_weight
        else:
            base_score = 50.0

        # ============ 2. 规则引擎调整 ============
        rule_adjustment = 0.0
        if rule_result and not rule_result.vetoed:
            # 规则引擎的扣分和加分
            rule_adjustment = -rule_result.total_penalty + rule_result.total_bonus

            if abs(rule_adjustment) > 0.1:
                factors.append(Factor(
                    name="rule_engine",
                    display_name="规则引擎调整",
                    value=rule_adjustment,
                    contribution=rule_adjustment / 100,
                    direction="positive" if rule_adjustment > 0 else "negative"
                ))

        # ============ 3. 状态机调整 ============
        state_adjustment = 0.0
        if state_inference and self._state_machine:
            state_adjustment = self._state_machine.get_quality_score_adjustment(
                state_inference.most_likely_state
            )

            if abs(state_adjustment) > 0.1:
                factors.append(Factor(
                    name="company_state",
                    display_name="公司状态",
                    value=state_adjustment,
                    contribution=state_adjustment / 100,
                    direction="positive" if state_adjustment > 0 else "negative"
                ))

        # ============ 4. 因果诊断调整 ============
        if abs(causal_adjustment) > 0.1:
            factors.append(Factor(
                name="causal_diagnosis",
                display_name="因果诊断",
                value=causal_adjustment,
                contribution=causal_adjustment / 100,
                direction="positive" if causal_adjustment > 0 else "negative"
            ))

        # ============ 5. Copula 有效证据数调整 ============
        # 当有效证据数远低于名义证据数时，增加不确定性惩罚
        copula_adjustment = 0.0
        if copula_result and copula_result.effective_evidence_count > 0:
            nominal_count = len(copula_result.individual_contributions)
            if nominal_count > 0:
                efficiency = copula_result.effective_evidence_count / nominal_count
                if efficiency < 0.5:
                    # 证据高度相关，有效信息不足
                    copula_adjustment = -(1 - efficiency) * 10 * self.config.copula_confidence_weight

                    factors.append(Factor(
                        name="evidence_correlation",
                        display_name="证据相关性惩罚",
                        value=efficiency,
                        contribution=copula_adjustment / 100,
                        direction="negative"
                    ))

        # ============ 6. DS 冲突调整 ============
        ds_adjustment = 0.0
        if ds_result.conflict > 0.5:
            # 高冲突降低确定性
            ds_adjustment = -(ds_result.conflict - 0.5) * 20

            factors.append(Factor(
                name="ds_conflict",
                display_name="证据冲突惩罚",
                value=ds_result.conflict,
                contribution=ds_adjustment / 100,
                direction="negative"
            ))

        # ============ 计算最终分数 ============
        # v3.4: 降低 DS/Copula 对评分的直接影响
        # 保留它们作为诊断信息, 但不让它们拉平分数
        final_score = (
            base_score
            + rule_adjustment
            + state_adjustment
            + causal_adjustment
            + copula_adjustment * 0.7   # v3.5: 保留 Copula 惩罚但降低幅度
            + ds_adjustment * 0.7       # v3.5: 保留 DS 惩罚但降低幅度
        )

        final_score = np.clip(final_score, 0, 100)

        return final_score, factors

    def _get_adaptive_grade(
        self,
        metric: str,
        value: float,
        context: AdaptiveContext
    ) -> str:
        """
        获取自适应等级

        v3.6 修复：基于 A 股实际分布校准阈值
        - 10 年 log_slope 分布: p25≈-0.08, p50≈-0.02, p75≈+0.02
        - 旧阈值: -0.025 就判"veto"(10分), 导致 68% 公司得 10 分
        - 新阈值: 基于百分位, 使评分有合理区分度
        - 评分仅用于 QUALITY/AVERAGE/POOR 排名, 不产生 VETO
        """
        try:
            thresholds = self._threshold_engine.get_thresholds(metric, context)
            return thresholds.get_grade(value, higher_is_better=True)
        except (ValueError, KeyError):
            # v3.6: 基于实际 A 股趋势斜率分布校准
            # 斜率分布: p10≈-0.15, p25≈-0.08, p50≈-0.02, p75≈+0.02, p90≈+0.06
            if value > 0.04:
                return "excellent"   # top ~15%
            elif value > 0.01:
                return "good"        # top ~30%
            elif value > -0.02:
                return "acceptable"  # 中位数附近
            elif value > -0.08:
                return "poor"        # bottom ~25%
            else:
                return "veto"        # bottom ~10%, 真正的长期恶化

    def _make_integrated_decision(
        self,
        score: float,
        ds_result: DSEvaluationResult,
        state_inference: Optional[StateInference],
        copula_result: CopulaFusionResult,
        rule_result: Optional[RuleEngineResult]
    ) -> Tuple[DecisionType, float]:
        """
        v3.6 决策函数 — 已通过规则引擎的公司仅用分数排名

        设计原则:
        - 到达此函数的公司已通过 step3 规则引擎否决检查 (≥3指标共识)
        - 规则引擎是唯一的硬否决网关，此函数不再产生 VETO
        - 此函数仅做 QUALITY / AVERAGE / POOR 三档排名
        - DS 信号作为辅助信号微调评分，不改变大方向

        改进 (v3.5→v3.6):
        - 移除 score < veto_threshold → VETO 路径（冗余且过度惩罚）
        - 保留 QUALITY / AVERAGE / POOR 三档纯排名
        """
        # 1. DS 信号辅助决策 (微调，不否决)
        ds_boost = 0
        if ds_result.decision == "accept" and ds_result.confidence > 0.6:
            ds_boost = 5  # DS支持时微升评分
        elif ds_result.decision == "reject" and ds_result.confidence > 0.85:
            ds_boost = -10  # DS强烈反对时降评

        adjusted_score = score + ds_boost

        # 2. 基于调整后分数的排名（不产生 VETO — 规则引擎已处理）
        if adjusted_score >= self.config.quality_threshold:
            decision = DecisionType.QUALITY
        elif adjusted_score >= self.config.average_threshold:
            decision = DecisionType.AVERAGE
        else:
            decision = DecisionType.POOR

        # 4. v3.4 置信度: 规则引擎结果 + 状态机 + 评分离散度
        # 规则引擎置信度: 触发越多规则, 置信度越高
        rule_conf = 0.50
        if rule_result:
            triggered_count = len(rule_result.penalty_rules) + len(rule_result.bonus_rules)
            if rule_result.strategies:
                rule_conf = min(0.95, 0.65 + 0.05 * len(rule_result.strategies))
            elif triggered_count > 3:
                rule_conf = 0.80
            elif triggered_count > 0:
                rule_conf = 0.65

        state_conf = state_inference.confidence if state_inference else 0.50

        # 评分离散度: 分数距离决策边界越远, 置信度越高
        boundary = self.config.quality_threshold if decision == DecisionType.QUALITY else self.config.average_threshold
        distance = abs(score - boundary) / 20.0  # 归一化
        score_conf = min(0.95, 0.50 + distance)

        confidence = (
            0.50 * rule_conf +
            0.25 * state_conf +
            0.25 * score_conf
        )

        return decision, confidence

    def _generate_explanation(
        self,
        ts_code: str,
        company_info: Dict[str, Any],
        decision: DecisionType,
        confidence: float,
        factors: List[Factor],
        score: float,
        state_inference: Optional[StateInference],
        causal_diagnosis: Optional[Dict[str, Any]]
    ) -> ExplanationResult:
        """生成解释"""
        company_name = company_info.get("name", ts_code)
        industry = company_info.get("industry")

        explainer = DecisionExplainer(
            company_name=company_name,
            industry=industry
        )

        state_info = None
        if state_inference:
            state_info = {
                "state": state_inference.most_likely_state.value,
                "confidence": state_inference.confidence
            }

        return explainer.explain(
            decision=decision,
            confidence=confidence,
            factors=factors,
            score=score,
            state_info=state_info,
            causal_diagnosis=causal_diagnosis
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Pipeline 集成
# ═══════════════════════════════════════════════════════════════════════════════

@register_method(
    component_type="business_engine",
    engine_type="evaluator",
    engine_name="causal_bayesian_evaluator"
)
def run_causal_bayesian_evaluator(
    aggregated_trends: Dict[str, pd.DataFrame],
    company_list: Optional[List[Dict[str, Any]]] = None,
    config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    运行因果贝叶斯评估器

    Pipeline 入口函数。

    Args:
        aggregated_trends: 来自 PDDA 的聚合趋势数据
            键: 指标名 (roic, roe, revenue, ...)
            值: DataFrame 包含所有公司的趋势分析结果
        company_list: 可选的公司信息列表
            [{"ts_code": "000001.SZ", "name": "平安银行", "industry": "银行"}, ...]
        config: 可选的配置覆盖

    Returns:
        {
            "evaluations": [CompanyEvaluation.to_dict(), ...],
            "summary": {...},
            "quality_companies": [...],
            "veto_companies": [...]
        }
    """
    logger.info(f"Starting Causal Bayesian Evaluator with {len(aggregated_trends)} metrics")

    # 解析配置
    eval_config = EvaluatorConfig()
    if config:
        for key, value in config.items():
            if hasattr(eval_config, key):
                setattr(eval_config, key, value)

    # 创建评估器
    evaluator = CausalBayesianEvaluator(eval_config)

    # 获取所有公司代码
    all_ts_codes = set()
    for df in aggregated_trends.values():
        if df is not None and "ts_code" in df.columns:
            all_ts_codes.update(df["ts_code"].unique())

    # 构建公司信息字典
    company_info_dict = {}
    if company_list:
        for info in company_list:
            ts_code = info.get("ts_code")
            if ts_code:
                company_info_dict[ts_code] = info

    # 从 PDDA 聚合数据中提取公司名称和行业（趋势分析已携带 name/industry 列）
    if not company_info_dict:
        for df in aggregated_trends.values():
            if df is not None and not df.empty and "name" in df.columns:
                for _, row in df[["ts_code", "name", "industry"]].drop_duplicates("ts_code").iterrows():
                    ts = row["ts_code"]
                    if ts not in company_info_dict:
                        company_info_dict[ts] = {
                            "ts_code": ts,
                            "name": str(row.get("name", "") or ""),
                            "industry": str(row.get("industry", "") or ""),
                        }
                break  # 任意一个 DataFrame 即可，无需遍历全部
        if company_info_dict:
            logger.info(f"Extracted {len(company_info_dict)} company names from aggregated_trends")

    # 评估每个公司
    evaluations = []
    quality_companies = []
    veto_companies = []

    for ts_code in all_ts_codes:
        company_info = company_info_dict.get(ts_code, {"ts_code": ts_code})

        try:
            result = evaluator.evaluate_company(
                ts_code=ts_code,
                trend_data=aggregated_trends,
                company_info=company_info
            )

            evaluations.append(result.to_dict())

            if result.decision == DecisionType.QUALITY:
                quality_companies.append(ts_code)
            elif result.decision == DecisionType.VETO:
                veto_companies.append(ts_code)

        except Exception as e:
            logger.error(f"Error evaluating {ts_code}: {e}")
            continue

    # 生成摘要
    summary = {
        "total_evaluated": len(evaluations),
        "quality_count": len(quality_companies),
        "veto_count": len(veto_companies),
        "average_score": np.mean([e["score"] for e in evaluations]) if evaluations else 0,
        "average_confidence": np.mean([e["confidence"] for e in evaluations]) if evaluations else 0
    }

    logger.info(f"Evaluation complete: {summary}")

    return {
        "evaluations": evaluations,
        "summary": summary,
        "quality_companies": quality_companies,
        "veto_companies": veto_companies
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 便捷函数
# ═══════════════════════════════════════════════════════════════════════════════

def evaluate_single_company(
    ts_code: str,
    trend_data: Dict[str, pd.DataFrame],
    company_name: Optional[str] = None,
    industry: Optional[str] = None,
    market_cap: Optional[float] = None
) -> CompanyEvaluation:
    """便捷函数：评估单个公司"""
    evaluator = CausalBayesianEvaluator()

    company_info = {
        "ts_code": ts_code,
        "name": company_name,
        "industry": industry,
        "market_cap": market_cap or 100.0
    }

    return evaluator.evaluate_company(ts_code, trend_data, company_info)
