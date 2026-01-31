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
# PDDA 列名映射（与 trend 层输出对齐）
# ═══════════════════════════════════════════════════════════════════════════════

class PDDAColumns:
    """
    PDDA 输出列名常量

    trend 层输出格式: {metric}_{feature}
    例如: roic_slope, roic_cv, roic_has_deterioration
    """
    # 趋势特征
    SLOPE = "slope"                    # OLS 斜率
    LOG_SLOPE = "log_slope"            # 对数斜率
    ROBUST_SLOPE = "robust_slope"      # Theil-Sen 稳健斜率
    R_SQUARED = "r_squared"            # 拟合优度
    CAGR = "cagr"                      # 复合增长率
    TREND_DIRECTION = "trend_direction"  # up/down/flat

    # 波动特征
    CV = "cv"                          # 变异系数
    STD_DEV = "std_dev"                # 标准差
    VOLATILITY_TYPE = "volatility_type"  # stable/moderate/volatile/high_volatility
    VOLATILITY_REGIME = "volatility_regime"  # 波动体制

    # 恶化检测
    HAS_DETERIORATION = "has_deterioration"      # 是否恶化
    DETERIORATION_SEVERITY = "deterioration_severity"  # none/mild/moderate/severe
    TOTAL_DECLINE_PCT = "total_decline_pct"      # 总下降百分比

    # 拐点检测
    HAS_INFLECTION = "has_inflection"    # 是否有拐点
    INFLECTION_TYPE = "inflection_type"  # 拐点类型

    # 周期性
    IS_CYCLICAL = "is_cyclical"          # 是否周期性
    CURRENT_PHASE = "current_phase"      # 当前周期阶段
    CYCLE_POSITION = "cycle_position"    # 周期位置

    # 加速/减速
    IS_ACCELERATING = "is_accelerating"  # 是否加速
    IS_DECELERATING = "is_decelerating"  # 是否减速

    # 滚动窗口
    RECENT_3Y_SLOPE = "recent_3y_slope"  # 近3年斜率
    MK_TAU = "mk_tau"                    # Mann-Kendall tau
    MK_P_VALUE = "mk_p_value"            # MK p值

    # 水平指标
    WEIGHTED_AVG = "weighted_avg"        # 加权平均值
    LATEST_VALUE = "latest_value"        # 最新值
    LATEST_VS_WEIGHTED = "latest_vs_weighted_ratio"  # 最新/加权比

    # 数据质量
    FULL_DATA_YEARS = "full_data_years"  # 完整数据年数
    TREND_WINDOW_YEARS = "trend_window_years"  # 趋势窗口年数

    # 结构断点
    HAS_STRUCTURAL_BREAK = "has_structural_break"  # 是否有结构断点
    BREAK_YEAR_INDEX = "break_year_index"  # 断点位置
    DATA_REGIME = "data_regime"          # 数据体制

    @classmethod
    def col(cls, metric: str, feature: str) -> str:
        """生成完整列名"""
        return f"{metric}_{feature}"


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

    # 状态机
    use_state_machine: bool = True
    state_config_path: Optional[str] = None

    # 证据融合
    evidence_correlation_default: float = 0.3
    ds_conflict_threshold: float = 0.7

    # 评分权重
    score_weights: Dict[str, float] = field(default_factory=lambda: {
        "roic_trend": 0.20,
        "roe_trend": 0.15,
        "revenue_trend": 0.15,
        "gross_margin_trend": 0.12,
        "net_margin_trend": 0.10,
        "ocf_trend": 0.13,
        "roiic_trend": 0.10,
        "state_bonus": 0.05
    })

    # 决策阈值
    quality_threshold: float = 70.0
    average_threshold: float = 50.0
    veto_threshold: float = 30.0


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

        # 初始化子模块
        # 注意: 时间衰减已由 trend 层完成，此处不再初始化 TemporalDecay

        self._threshold_engine = AdaptiveThresholdEngine.with_defaults()

        self._causal_graph = (
            create_financial_causal_graph()
            if self.config.use_causal_inference
            else None
        )

        self._state_machine = (
            get_default_state_machine()
            if self.config.use_state_machine
            else None
        )

        self._copula_fusion = CopulaEvidenceFusion(
            default_correlation=self.config.evidence_correlation_default
        )

        self._explainer = DecisionExplainer()

        logger.info(f"CausalBayesianEvaluator initialized with config: {self.config}")

    def evaluate_company(
        self,
        ts_code: str,
        trend_data: Dict[str, pd.DataFrame],
        company_info: Optional[Dict[str, Any]] = None
    ) -> CompanyEvaluation:
        """
        评估单个公司

        Args:
            ts_code: 股票代码
            trend_data: 趋势分析数据，键为指标名（roic, roe, ...）
                       每个 DataFrame 来自 PDDA 聚合，每公司只有 1 行
            company_info: 公司信息（可选）
                - name: 公司名称
                - industry: 行业
                - market_cap: 市值（亿元）

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

        # 1. 从 PDDA 单行输出直接提取特征（不做时间衰减，trend 层已处理）
        features = self._extract_features_from_pdda(company_trends)

        # 2. 创建自适应上下文
        context = self._create_adaptive_context(company_info)

        # 3. 推断公司状态
        state_inference = self._infer_company_state(features)

        # 4. 收集证据（充分利用 PDDA 的布尔特征）
        evidences = self._collect_evidences(features, context)

        # 5. Copula 融合
        copula_result = self._copula_fusion.fuse(evidences)

        # 6. Dempster-Shafer 融合
        ds_result = self._ds_evaluate(evidences)

        # 7. 因果诊断（如果启用）
        causal_diagnosis = None
        if self._causal_graph:
            causal_diagnosis = self._run_causal_diagnosis(features)

        # 8. 计算综合评分
        score, factors = self._compute_score(
            features, state_inference, copula_result, ds_result, context
        )

        # 9. 做出决策
        decision, confidence = self._make_decision(score, ds_result, state_inference)

        # 10. 生成解释
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

            # PDDA 输出每公司只有 1 行
            row = df.iloc[0]

            # ============ 趋势特征 ============
            # 优先使用稳健斜率（Theil-Sen），回退到 OLS 斜率
            robust_col = C.col(metric, C.ROBUST_SLOPE)
            slope_col = C.col(metric, C.SLOPE)

            if robust_col in row.index and pd.notna(row[robust_col]):
                features[f"{metric}_trend"] = float(row[robust_col])
            elif slope_col in row.index and pd.notna(row[slope_col]):
                features[f"{metric}_trend"] = float(row[slope_col])
            else:
                features[f"{metric}_trend"] = 0.0

            # R² 拟合优度
            r2_col = C.col(metric, C.R_SQUARED)
            features[f"{metric}_r_squared"] = (
                float(row[r2_col]) if r2_col in row.index and pd.notna(row[r2_col]) else 0.0
            )

            # Mann-Kendall tau（非参数趋势）
            mk_col = C.col(metric, C.MK_TAU)
            features[f"{metric}_mk_tau"] = (
                float(row[mk_col]) if mk_col in row.index and pd.notna(row[mk_col]) else 0.0
            )

            # 趋势方向（PDDA 已判断）
            dir_col = C.col(metric, C.TREND_DIRECTION)
            features[f"{metric}_direction"] = (
                str(row[dir_col]) if dir_col in row.index else "flat"
            )

            # 近3年趋势（短期）
            recent_col = C.col(metric, C.RECENT_3Y_SLOPE)
            features[f"{metric}_recent_trend"] = (
                float(row[recent_col]) if recent_col in row.index and pd.notna(row[recent_col]) else 0.0
            )

            # ============ 水平特征 ============
            latest_col = C.col(metric, C.LATEST_VALUE)
            features[f"{metric}_level"] = (
                float(row[latest_col]) if latest_col in row.index and pd.notna(row[latest_col]) else 0.0
            )

            weighted_col = C.col(metric, C.WEIGHTED_AVG)
            features[f"{metric}_weighted_avg"] = (
                float(row[weighted_col]) if weighted_col in row.index and pd.notna(row[weighted_col]) else 0.0
            )

            # 最新值 vs 加权平均（衡量近期表现）
            ratio_col = C.col(metric, C.LATEST_VS_WEIGHTED)
            features[f"{metric}_latest_vs_weighted"] = (
                float(row[ratio_col]) if ratio_col in row.index and pd.notna(row[ratio_col]) else 1.0
            )

            # ============ 波动特征 ============
            cv_col = C.col(metric, C.CV)
            features[f"{metric}_volatility"] = (
                float(row[cv_col]) if cv_col in row.index and pd.notna(row[cv_col]) else 0.2
            )

            vol_type_col = C.col(metric, C.VOLATILITY_TYPE)
            features[f"{metric}_volatility_type"] = (
                str(row[vol_type_col]) if vol_type_col in row.index else "moderate"
            )

            # ============ 恶化检测（布尔特征）============
            det_col = C.col(metric, C.HAS_DETERIORATION)
            features[f"{metric}_has_deterioration"] = (
                bool(row[det_col]) if det_col in row.index else False
            )

            sev_col = C.col(metric, C.DETERIORATION_SEVERITY)
            features[f"{metric}_deterioration_severity"] = (
                str(row[sev_col]) if sev_col in row.index else "none"
            )

            decline_col = C.col(metric, C.TOTAL_DECLINE_PCT)
            features[f"{metric}_decline_pct"] = (
                float(row[decline_col]) if decline_col in row.index and pd.notna(row[decline_col]) else 0.0
            )

            # ============ 拐点检测 ============
            infl_col = C.col(metric, C.HAS_INFLECTION)
            features[f"{metric}_has_inflection"] = (
                bool(row[infl_col]) if infl_col in row.index else False
            )

            # ============ 周期性特征 ============
            cyc_col = C.col(metric, C.IS_CYCLICAL)
            features[f"{metric}_is_cyclical"] = (
                bool(row[cyc_col]) if cyc_col in row.index else False
            )

            phase_col = C.col(metric, C.CURRENT_PHASE)
            features[f"{metric}_cycle_phase"] = (
                str(row[phase_col]) if phase_col in row.index else ""
            )

            # ============ 加速/减速 ============
            acc_col = C.col(metric, C.IS_ACCELERATING)
            features[f"{metric}_is_accelerating"] = (
                bool(row[acc_col]) if acc_col in row.index else False
            )

            dec_col = C.col(metric, C.IS_DECELERATING)
            features[f"{metric}_is_decelerating"] = (
                bool(row[dec_col]) if dec_col in row.index else False
            )

            # ============ 结构断点 ============
            break_col = C.col(metric, C.HAS_STRUCTURAL_BREAK)
            features[f"{metric}_has_break"] = (
                bool(row[break_col]) if break_col in row.index else False
            )

            regime_col = C.col(metric, C.DATA_REGIME)
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

        return AdaptiveContext.from_company_info(
            industry_name=industry,
            market_cap=market_cap,
            current_cycle="expansion"  # 可以从外部传入
        )

    def _infer_company_state(
        self,
        features: Dict[str, Any]
    ) -> Optional[StateInference]:
        """推断公司状态"""
        if not self._state_machine:
            return None

        # 准备状态机特征（使用 PDDA 提取的正确键名）
        state_features = {
            "revenue_growth": features.get("revenue_trend", 0.0),
            "roic_level": features.get("roic_level", 10.0),
            "roic_trend": features.get("roic_trend", 0.0),
            "volatility": features.get("roic_volatility", 0.2)
        }

        return self._state_machine.infer_state(state_features)

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
        ]

        for metric_key, threshold, base_conf, weight in trend_configs:
            if metric_key in features:
                value = features[metric_key]

                # Sigmoid 转换：趋势值 → 质量概率
                z = (value - threshold) / 0.05
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

    def _run_causal_diagnosis(
        self,
        features: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """运行因果诊断"""
        if not self._causal_graph:
            return None

        # 准备因果图观测数据
        observed_data = {
            "revenue_trend": features.get("revenue_trend", 0.0),
            "gross_margin_trend": features.get("gross_margin_trend", 0.0),
            "net_margin_trend": features.get("net_margin_trend", 0.0),
            "roic_trend": features.get("roic_trend", 0.0),
            "roe_trend": features.get("roe_trend", 0.0),
            "ocf_trend": features.get("ocf_trend", 0.0),
        }

        # 诊断 ROIC 趋势
        diagnosis = self._causal_graph.diagnose(
            target_metric="roic_trend",
            observed_data=observed_data
        )

        return {
            "target": diagnosis.target_metric,
            "status": diagnosis.status,
            "primary_causes": diagnosis.primary_causes,
            "explanation": diagnosis.explanation,
            "confidence": diagnosis.confidence
        }

    def _compute_score(
        self,
        features: Dict[str, Any],
        state_inference: Optional[StateInference],
        copula_result: CopulaFusionResult,
        ds_result: DSEvaluationResult,
        context: AdaptiveContext
    ) -> Tuple[float, List[Factor]]:
        """计算综合评分"""
        factors = []
        weighted_sum = 0.0
        total_weight = 0.0

        # 从特征计算各维度分数
        metric_scores = []

        for metric, weight in self.config.score_weights.items():
            if metric == "state_bonus":
                continue  # 状态加分单独处理

            feature_key = metric
            if feature_key not in features:
                continue

            value = features[feature_key]

            # 获取自适应阈值
            try:
                thresholds = self._threshold_engine.get_thresholds(metric, context)
                grade = thresholds.get_grade(value, higher_is_better=True)
            except ValueError:
                # 使用默认评分逻辑
                if value > 0.02:
                    grade = "excellent"
                elif value > 0:
                    grade = "good"
                elif value > -0.02:
                    grade = "acceptable"
                else:
                    grade = "poor"

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

            # 计算贡献度（归一化的加权分数偏离中位数的程度）
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

        # 状态加分/减分
        state_adjustment = 0.0
        if state_inference and self._state_machine:
            state_adjustment = self._state_machine.get_quality_score_adjustment(
                state_inference.most_likely_state
            )

            # 状态因素
            factors.append(Factor(
                name="company_state",
                display_name="公司状态",
                value=state_adjustment,
                contribution=state_adjustment / 100,
                direction="positive" if state_adjustment > 0 else "negative"
            ))

        # DS 置信度调整
        # 高冲突时降低分数确定性
        if ds_result.conflict > 0.5:
            confidence_penalty = (ds_result.conflict - 0.5) * 20
            base_score = base_score * (1 - confidence_penalty / 100)

        # 最终分数
        final_score = np.clip(base_score + state_adjustment, 0, 100)

        return final_score, factors

    def _make_decision(
        self,
        score: float,
        ds_result: DSEvaluationResult,
        state_inference: Optional[StateInference]
    ) -> Tuple[DecisionType, float]:
        """做出决策"""
        # 检查是否被一票否决
        if ds_result.decision == "reject" and ds_result.confidence > 0.7:
            return DecisionType.VETO, ds_result.confidence

        # 基于分数的决策
        if score >= self.config.quality_threshold:
            decision = DecisionType.QUALITY
        elif score >= self.config.average_threshold:
            decision = DecisionType.AVERAGE
        elif score >= self.config.veto_threshold:
            decision = DecisionType.POOR
        else:
            decision = DecisionType.VETO

        # 计算置信度
        # 综合 DS 置信度和状态置信度
        ds_conf = ds_result.confidence
        state_conf = state_inference.confidence if state_inference else 0.5

        confidence = 0.6 * ds_conf + 0.4 * state_conf

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
