"""T.R.U.T.H. 六维因子计算 - 专业级基因测序实现

实现设计文档中定义的六维基因测序：
    - α (Cyclicality): 周期性 - 业绩对宏观经济的敏感弹性
    - β (Heaviness): 资本密度 - 赚取下一块钱利润所需的"重"度
    - γ (Growth): 原始动能 - 业务表面上的扩张加速度
    - δ_fraud (Fraud Entropy): 欺诈熵 - 财务报表的物理真实性 (熔断项)
    - δ_decay (Decay Entropy): 衰退熵 - 商业模式的恶化趋势 (惩罚项)
    - V (Verification): 真相验证 - 成长的含金量 (照妖镜)

数据依赖:
    - 所有因子从 ProbeInput.features 获取趋势特征
    - β/δ_fraud 因子需要 financial_context 探针提供原始财务结构

架构说明:
    - 使用鸭子类型 (duck typing)，不依赖 ABC 继承
    - 实现 FactorProtocol (typing.Protocol) 即可作为因子
    - 每个因子提供 explain() 方法生成人类可读解释

版本: 3.3.0
日期: 2026-01-06
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

from .models import FactorId, FactorResult, ProbeInput, TruthWarning, WarningLevel
from .config import (
    TruthConfig,
    AlphaFactorConfig,
    BetaFactorConfig,
    GammaFactorConfig,
    LambdaFactorConfig,
    DeltaFraudFactorConfig,
    DeltaDecayFactorConfig,
    VerificationFactorConfig,
)


# ============================================================================
# 辅助函数
# ============================================================================

def normalize_score(value: float, method: str = "minmax",
                    vmin: float = 0.0, vmax: float = 1.0,
                    clip: bool = True) -> float:
    """归一化分数到 [0, 1]

    Args:
        value: 原始值
        method: 归一化方法 (minmax/tanh/sigmoid)
        vmin: 最小值 (用于 minmax)
        vmax: 最大值 (用于 minmax)
        clip: 是否裁剪到 [0, 1]
    """
    if math.isnan(value) or math.isinf(value):
        return 0.5

    if method == "none":
        result = value
    elif method == "tanh":
        result = 0.5 * (math.tanh(value) + 1.0)
    elif method == "sigmoid":
        result = 1.0 / (1.0 + math.exp(-value))
    elif method == "minmax":
        if vmax == vmin:
            result = 0.5
        else:
            result = (value - vmin) / (vmax - vmin)
    else:
        result = value

    if clip:
        result = max(0.0, min(1.0, result))

    return result


def get_feature(probes: Sequence[ProbeInput],
                feature_suffix: str,
                metric_filter: Optional[str] = None) -> Optional[float]:
    """从探针中提取特征值

    Args:
        probes: 探针列表
        feature_suffix: 特征后缀 (如 "cv", "slope", "cagr")
        metric_filter: 指标过滤 (如 "roic", "roe")
    """
    for probe in probes:
        if metric_filter and probe.probe_name != metric_filter:
            continue
        for fname, fval in probe.features.items():
            if fname.endswith(f"_{feature_suffix}") or fname == feature_suffix:
                if not math.isnan(fval) and not math.isinf(fval):
                    return fval
    return None


def get_financial_context(probes: Sequence[ProbeInput],
                          field_name: str,
                          default: float = 0.0) -> float:
    """从 financial_context 探针获取财务上下文字段

    专用于 β/δ_fraud 因子，从 FinancialContextProbe 输出中提取字段。

    Args:
        probes: 探针列表
        field_name: 字段名 (如 "ratio_hard_asset", "flag_goodwill_risk")
        default: 默认值

    Returns:
        字段值，如果不存在则返回默认值

    Example:
        >>> hard_asset_ratio = get_financial_context(probes, "ratio_hard_asset")
        >>> goodwill_risk = get_financial_context(probes, "flag_goodwill_risk")
    """
    for probe in probes:
        if probe.probe_name == "financial_context":
            val = probe.features.get(field_name)
            if val is not None and not math.isnan(val) and not math.isinf(val):
                return float(val)
    return default


def has_financial_context(probes: Sequence[ProbeInput]) -> bool:
    """检查是否存在 financial_context 探针"""
    return any(p.probe_name == "financial_context" for p in probes)


def aggregate_feature(probes: Sequence[ProbeInput],
                      feature_suffix: str,
                      agg: str = "mean",
                      metrics: Optional[List[str]] = None) -> Optional[float]:
    """跨多个指标聚合特征值

    Args:
        probes: 探针列表
        feature_suffix: 特征后缀
        agg: 聚合方式 (mean/max/min/median)
        metrics: 限定的指标列表 (None = 全部)
    """
    values = []
    for probe in probes:
        if metrics and probe.probe_name not in metrics:
            continue
        for fname, fval in probe.features.items():
            if fname.endswith(f"_{feature_suffix}") or fname == feature_suffix:
                if not math.isnan(fval) and not math.isinf(fval):
                    values.append(fval)

    if not values:
        return None

    if agg == "mean":
        return sum(values) / len(values)
    elif agg == "max":
        return max(values)
    elif agg == "min":
        return min(values)
    elif agg == "median":
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        if n % 2 == 0:
            return (sorted_vals[n//2 - 1] + sorted_vals[n//2]) / 2
        return sorted_vals[n//2]

    return sum(values) / len(values)


# ============================================================================
# 因子基类 (鸭子类型 - 实现 FactorProtocol 即可)
# ============================================================================

# 不再使用 ABC 继承，改用 Protocol 鸭子类型
# 任何实现了 factor_id, evaluate(), explain() 的类都是有效因子

# NOTE: 以下是实现 FactorProtocol 的约定:
#   - factor_id: FactorId  # 因子标识
#   - def evaluate(ts_code, probes, config) -> Tuple[FactorResult, List[TruthWarning]]
#   - def explain(result: FactorResult) -> str  # 人类可读解释


# ============================================================================
# α 因子: 周期性 (Cyclicality)
# ============================================================================

@dataclass
class AlphaFactor:
    """α 因子: 周期性 - 业绩对宏观经济的敏感弹性

    高 α = 高周期性 (如钢铁、有色)
    低 α = 低周期性 (如医药、消费)

    计算组件:
        1. detrended_cv: 去趋势变异系数 (高 = 周期性强)
        2. r_squared_inverse: 1 - R² (低R² = 趋势不明 = 周期性)
        3. cv: 原始变异系数
        4. is_cyclical: 周期性标志
        5. hurst_exponent: Hurst指数 (H<0.5 = 均值回归 = 真周期)
    """

    factor_id: FactorId = FactorId.ALPHA

    def evaluate(self,
                 ts_code: str,
                 probes: Sequence[ProbeInput],
                 config: TruthConfig) -> Tuple[FactorResult, List[TruthWarning]]:

        warnings: List[TruthWarning] = []
        components: Dict[str, float] = {}
        conf = config.alpha_config
        weights = conf.component_weights

        score = 0.0
        total_weight = 0.0
        confidence_factors = []

        # 1. 去趋势变异系数 (detrended_cv)
        detrended_cv = aggregate_feature(probes, "detrended_cv", "mean")
        if detrended_cv is None:
            detrended_cv = aggregate_feature(probes, "cv", "mean")

        if detrended_cv is not None:
            # CV 归一化: 0-1 映射到 0-1 (高CV = 高周期性)
            normalized = normalize_score(detrended_cv, "minmax", 0.0, 1.0)
            w = weights.get("detrended_cv", 0.35)
            score += w * normalized
            total_weight += w
            components["detrended_cv"] = detrended_cv
            confidence_factors.append(1.0)
        else:
            confidence_factors.append(0.3)

        # 2. R² 反向 (低R² = 周期性)
        r_squared = aggregate_feature(probes, "r_squared", "mean")
        if r_squared is not None:
            r_squared = max(0.0, min(1.0, r_squared))
            r_squared_inverse = 1.0 - r_squared
            w = weights.get("r_squared_inverse", 0.25)
            score += w * r_squared_inverse
            total_weight += w
            components["r_squared"] = r_squared
            components["r_squared_inverse"] = r_squared_inverse
            confidence_factors.append(1.0)
        else:
            confidence_factors.append(0.3)

        # 3. 原始 CV
        cv = aggregate_feature(probes, "cv", "mean")
        if cv is not None:
            normalized = normalize_score(cv, "minmax", 0.0, 2.0)
            w = weights.get("cv", 0.20)
            score += w * normalized
            total_weight += w
            components["cv"] = cv

        # 4. 周期性标志
        is_cyclical = aggregate_feature(probes, "is_cyclical", "max")
        if is_cyclical is not None:
            cyclical_score = 1.0 if is_cyclical > 0.5 else 0.0
            w = weights.get("is_cyclical", 0.15)
            score += w * cyclical_score
            total_weight += w
            components["is_cyclical"] = is_cyclical

        # 5. Hurst 指数 (H < 0.5 = 均值回归 = 真周期)
        hurst = aggregate_feature(probes, "hurst_exponent", "mean")
        if hurst is not None:
            # H < 0.45 表示均值回归 (真周期), 得分高
            hurst_score = 1.0 - normalize_score(hurst, "minmax", 0.3, 0.7)
            w = weights.get("hurst_exponent", 0.05)
            score += w * hurst_score
            total_weight += w
            components["hurst_exponent"] = hurst

        # 计算最终分数
        if total_weight > 0:
            score = score / total_weight
        else:
            score = 0.5  # 默认中性

        # 计算置信度
        confidence = sum(confidence_factors) / len(confidence_factors) if confidence_factors else 0.5

        # 生成警告
        if score > 0.8:
            warnings.append(TruthWarning(
                code="ALPHA_HIGH",
                level=WarningLevel.INFO,
                title="高周期性",
                message=f"α={score:.2f}，业绩波动大，适合周期底部布局",
                source="alpha_factor",
                values={"alpha": score},
            ))

        return FactorResult(
            factor_id=self.factor_id,
            ts_code=ts_code,
            score=score,
            confidence=confidence,
            components=components,
            details={"total_weight": total_weight},
        ), warnings

    def explain(self, result: FactorResult) -> str:
        """生成人类可读的解释文本"""
        score = result.score or 0.5
        components = result.components or {}

        # 周期性分类
        if score > 0.7:
            cycle_type = "强周期 (如钢铁、有色)"
        elif score > 0.5:
            cycle_type = "中等周期"
        elif score > 0.3:
            cycle_type = "弱周期"
        else:
            cycle_type = "非周期 (如消费、医药)"

        # 构建解释
        parts = [f"α={score:.2f} ({cycle_type})"]

        if "detrended_cv" in components:
            parts.append(f"变异系数{components['detrended_cv']:.2f}")
        if "r_squared" in components:
            parts.append(f"R²={components['r_squared']:.2f}")

        return "，".join(parts)


# ============================================================================
# β 因子: 资本密度 (Heaviness)
# ============================================================================

@dataclass
class BetaFactor:
    """β 因子: 资本密度 - 赚取下一块钱利润所需的"重"度

    高 β = 重资产 (钢铁、航空、公用事业)
    低 β = 轻资产 (软件、互联网、品牌消费)

    数据源 (来自 FinancialContextProbe，必需):
        - ratio_hard_asset: (固定资产+在建工程) / 总资产 (权重45%)
        - ratio_nca: 非流动资产 / 总资产 (权重25%)
        - ratio_intang_asset: 无形资产 / 总资产 (权重15%，反向)
        - ratio_working_capital: 营运资本 / 总资产 (权重15%，反向)

    注意: 本因子必须依赖 financial_context 探针，无数据时返回低置信度默认值
    """

    factor_id: FactorId = FactorId.BETA

    def evaluate(self,
                 ts_code: str,
                 probes: Sequence[ProbeInput],
                 config: TruthConfig) -> Tuple[FactorResult, List[TruthWarning]]:

        warnings: List[TruthWarning] = []
        components: Dict[str, float] = {}
        conf = config.beta_config
        weights = conf.component_weights

        score = 0.0
        total_weight = 0.0

        # ================================================================
        # 数据源: financial_context 探针 (必需)
        # ================================================================
        if not has_financial_context(probes):
            # 无数据时返回低置信度默认值
            warnings.append(TruthWarning(
                code="BETA_NO_CONTEXT",
                level=WarningLevel.CRITICAL,
                title="β因子数据缺失",
                message="financial_context探针不可用，无法计算资本密度",
                source="beta_factor",
            ))
            return FactorResult(
                factor_id=self.factor_id,
                ts_code=ts_code,
                score=0.5,  # 中性默认值
                confidence=0.1,  # 极低置信度
                components={},
                details={"asset_type": "unknown", "data_available": False},
            ), warnings

        # 1. 硬资产比率 (核心指标)
        hard_asset_ratio = get_financial_context(probes, "ratio_hard_asset", -1.0)
        if hard_asset_ratio >= 0:
            # 硬资产比率直接就是 β 分数: 高硬资产 = 高β = 重资产
            w = weights.get("hard_asset_ratio", 0.45)
            score += w * hard_asset_ratio
            total_weight += w
            components["hard_asset_ratio"] = hard_asset_ratio

        # 2. 非流动资产比率
        nca_ratio = get_financial_context(probes, "ratio_nca", -1.0)
        if nca_ratio >= 0:
            w = weights.get("nca_ratio", 0.25)
            score += w * nca_ratio
            total_weight += w
            components["nca_ratio"] = nca_ratio

        # 3. 无形资产比率 (轻资产特征，反向)
        intang_ratio = get_financial_context(probes, "ratio_intang_asset", -1.0)
        if intang_ratio >= 0:
            # 高无形资产 = 轻资产 = 低β
            beta_from_intang = 1.0 - min(1.0, intang_ratio * 2)
            w = weights.get("intang_ratio", 0.15)
            score += w * beta_from_intang
            total_weight += w
            components["intang_ratio"] = intang_ratio

        # 4. 营运资本比率 (轻资产特征，反向)
        working_capital_ratio = get_financial_context(probes, "ratio_working_capital", -1.0)
        if working_capital_ratio >= 0:
            # 高营运资本比率 = 轻资产 = 低β
            beta_from_wc = 1.0 - min(1.0, working_capital_ratio)
            w = weights.get("working_capital_ratio", 0.15)
            score += w * beta_from_wc
            total_weight += w
            components["working_capital_ratio"] = working_capital_ratio

        # ================================================================
        # 计算最终分数
        # ================================================================
        if total_weight > 0:
            score = score / total_weight
        else:
            score = 0.5

        score = max(0.0, min(1.0, score))

        # 置信度: 基于可用数据量
        confidence = min(0.95, 0.5 + total_weight * 0.5)

        # 标记轻/重资产
        if score < 0.35:
            asset_type = "light"  # 轻资产
        elif score > 0.65:
            asset_type = "heavy"  # 重资产
        else:
            asset_type = "moderate"  # 中等

        return FactorResult(
            factor_id=self.factor_id,
            ts_code=ts_code,
            score=score,
            confidence=confidence,
            components=components,
            details={
                "asset_type": asset_type,
                "data_available": True,
                "total_weight": total_weight,
            },
        ), warnings

    def explain(self, result: FactorResult) -> str:
        """生成人类可读的解释文本"""
        score = result.score or 0.5
        components = result.components or {}
        details = result.details or {}

        # 资产类型
        asset_type = details.get("asset_type", "unknown")
        asset_label = {
            "light": "轻资产 (如软件、互联网)",
            "heavy": "重资产 (如钢铁、航空)",
            "moderate": "中等资产",
            "unknown": "未知",
        }.get(asset_type, "未知")

        parts = [f"β={score:.2f} ({asset_label})"]

        if "hard_asset_ratio" in components:
            parts.append(f"硬资产比率{components['hard_asset_ratio']:.1%}")
        if "nca_ratio" in components:
            parts.append(f"非流动资产{components['nca_ratio']:.1%}")

        return "，".join(parts)


# ============================================================================
# γ 因子: 成长动能 (Growth)
# ============================================================================

@dataclass
class GammaFactor:
    """γ 因子: 成长动能 - 业务扩张的加速度

    高 γ = 高成长 (营收/利润快速增长)
    低 γ = 低成长/停滞

    多维度评估:
        1. CAGR: 复合年增长率 (基础指标)
        2. log_slope: 对数斜率 (增长加速度)
        3. recent_3y_slope: 近3年斜率 (动量)
        4. robust_slope: Theil-Sen 稳健斜率 (去噪)
        5. R² 惩罚: 不稳定增长打折
    """

    factor_id: FactorId = FactorId.GAMMA

    def evaluate(self,
                 ts_code: str,
                 probes: Sequence[ProbeInput],
                 config: TruthConfig) -> Tuple[FactorResult, List[TruthWarning]]:

        warnings: List[TruthWarning] = []
        components: Dict[str, float] = {}
        conf = config.gamma_config
        weights = conf.component_weights

        score = 0.0
        total_weight = 0.0

        # 成长指标列表 (营收和利润)
        growth_metrics = ["revenue", "profit"]

        # 1. CAGR (复合增长率)
        cagr = aggregate_feature(probes, "cagr", "mean", growth_metrics)
        if cagr is None:
            cagr = aggregate_feature(probes, "cagr_approx", "mean", growth_metrics)

        if cagr is not None:
            # CAGR: 使用 tanh 压缩极端值
            # -50% ~ +50% 映射到 0 ~ 1
            normalized = normalize_score(cagr / 50.0, "tanh")
            w = weights.get("cagr", 0.35)
            score += w * normalized
            total_weight += w
            components["cagr"] = cagr

        # 2. 对数斜率 (增长加速度)
        log_slope = aggregate_feature(probes, "log_slope", "mean", growth_metrics)
        if log_slope is not None:
            normalized = normalize_score(log_slope / 0.3, "tanh")  # 30% 年化增速归一化
            w = weights.get("log_slope", 0.25)
            score += w * normalized
            total_weight += w
            components["log_slope"] = log_slope

        # 3. 近3年斜率 (动量)
        recent_slope = aggregate_feature(probes, "recent_3y_slope", "mean", growth_metrics)
        if recent_slope is not None:
            normalized = normalize_score(recent_slope / 0.3, "tanh")
            w = weights.get("recent_3y_slope", 0.20)
            score += w * normalized
            total_weight += w
            components["recent_3y_slope"] = recent_slope

        # 4. 稳健斜率 (Theil-Sen)
        robust_slope = aggregate_feature(probes, "robust_slope", "mean", growth_metrics)
        if robust_slope is not None:
            normalized = normalize_score(robust_slope / 0.3, "tanh")
            w = weights.get("robust_slope", 0.15)
            score += w * normalized
            total_weight += w
            components["robust_slope"] = robust_slope

        # 5. R² 惩罚 (不稳定增长打折)
        r_squared = aggregate_feature(probes, "r_squared", "mean", growth_metrics)
        if r_squared is not None:
            # R² < 0.5 时惩罚
            r2_penalty = max(0, 0.5 - r_squared)
            w = weights.get("r_squared_penalty", 0.05)
            score -= w * r2_penalty  # 注意是减分
            components["r_squared"] = r_squared
            components["r2_penalty"] = r2_penalty

        # 计算最终分数
        if total_weight > 0:
            score = score / total_weight
        else:
            score = 0.5

        # 确保在 [0, 1] 范围内
        score = max(0.0, min(1.0, score))

        # 置信度
        confidence = min(1.0, total_weight / 0.8)

        # 成长分类
        # v4.1.1 修复: CAGR 为小数形式 (0.15=15%), 直接与 config 阈值比较
        if cagr is not None:
            if cagr > conf.high_growth_threshold:
                growth_type = "high_growth"
            elif cagr > conf.moderate_growth_threshold:
                growth_type = "moderate_growth"
            elif cagr > 0:
                growth_type = "low_growth"
            else:
                growth_type = "decline"
        else:
            growth_type = "unknown"

        # 警告
        if growth_type == "decline":
            warnings.append(TruthWarning(
                code="GAMMA_DECLINE",
                level=WarningLevel.WARNING,
                title="负增长",
                message=f"CAGR={cagr:.1f}%，营收/利润处于下滑趋势",
                source="gamma_factor",
                values={"cagr": cagr if cagr else 0},
            ))

        return FactorResult(
            factor_id=self.factor_id,
            ts_code=ts_code,
            score=score,
            confidence=confidence,
            components=components,
            details={"growth_type": growth_type},
        ), warnings

    def explain(self, result: FactorResult) -> str:
        """生成人类可读的解释文本"""
        score = result.score or 0.5
        components = result.components or {}
        details = result.details or {}

        growth_type = details.get("growth_type", "unknown")
        growth_label = {
            "high_growth": "高成长",
            "moderate_growth": "中等成长",
            "low_growth": "低成长",
            "decline": "负增长",
            "unknown": "未知",
        }.get(growth_type, "未知")

        parts = [f"γ={score:.2f} ({growth_label})"]

        if "cagr" in components:
            parts.append(f"CAGR={components['cagr']:.1f}%")
        if "log_slope" in components:
            parts.append(f"斜率={components['log_slope']:.2f}")

        return "，".join(parts)


# ============================================================================
# λ 因子: 杠杆强度 (Leverage Strength) — v4.1 新增
# ============================================================================

@dataclass
class LambdaFactor:
    """λ 因子: 杠杆强度 - 偿债安全边际与资本结构健康度

    填补 Altman Z-Score (3/5 指标涉及杠杆) 和 AQR QMJ Safety 维度的空白。
    高 λ = 高杠杆 = 高风险 (与 δ_fraud/δ_decay 同为负向因子)

    数据源:
        - financial_context 探针: ratio_debt_to_assets
        - 趋势探针: 负债率变动趋势
        - financial_context 探针: ratio_cash_to_assets (现金覆盖)

    学术参考:
        - Altman Z-Score (1968): Working Capital/TA, RE/TA, EBIT/TA
        - AQR Quality Minus Junk: Safety = low leverage + low β + low ROE vol
    """

    factor_id: FactorId = FactorId.LAMBDA

    def evaluate(self,
                 ts_code: str,
                 probes: Sequence[ProbeInput],
                 config: TruthConfig) -> Tuple[FactorResult, List[TruthWarning]]:

        warnings: List[TruthWarning] = []
        components: Dict[str, float] = {}
        conf = config.lambda_config
        weights = conf.component_weights

        leverage_score = 0.0
        total_weight = 0.0

        # ================================================================
        # 数据源: financial_context 探针
        # ================================================================
        if not has_financial_context(probes):
            # 无数据时返回中性低置信度
            warnings.append(TruthWarning(
                code="LAMBDA_NO_CONTEXT",
                level=WarningLevel.WARNING,
                title="λ因子数据缺失",
                message="financial_context探针不可用，杠杆强度使用默认值",
                source="lambda_factor",
            ))
            return FactorResult(
                factor_id=self.factor_id,
                ts_code=ts_code,
                score=0.3,  # 默认偏低杠杆 (保守)
                confidence=0.1,
                components={},
                details={"leverage_level": "unknown", "data_available": False},
            ), warnings

        # 1. 资产负债率 (核心指标 — Altman Z-Score 核心)
        debt_to_assets = get_financial_context(probes, "ratio_debt_to_assets", -1.0)
        if debt_to_assets >= 0:
            # 线性映射: 0% → 0.0, 50% → 0.5, 75% → 0.85, 100% → 1.0
            # 使用 sigmoid 使高负债区域惩罚更明显
            if debt_to_assets <= conf.safe_debt_ratio:
                debt_score = debt_to_assets / conf.safe_debt_ratio * 0.5
            else:
                # 超过安全线, 快速上升
                excess = (debt_to_assets - conf.safe_debt_ratio) / (conf.danger_debt_ratio - conf.safe_debt_ratio)
                debt_score = 0.5 + 0.5 * min(1.0, excess)

            w = weights.get("debt_to_assets", 0.35)
            leverage_score += w * debt_score
            total_weight += w
            components["debt_to_assets"] = debt_to_assets
            components["debt_score"] = debt_score

            # 高杠杆警告
            if debt_to_assets > conf.danger_debt_ratio:
                warnings.append(TruthWarning(
                    code="LAMBDA_HIGH_LEVERAGE",
                    level=WarningLevel.CRITICAL,
                    title="⚠️ 高杠杆风险",
                    message=f"资产负债率={debt_to_assets:.1%}，超过{conf.danger_debt_ratio:.0%}警戒线",
                    source="lambda_factor",
                    values={"debt_to_assets": debt_to_assets},
                ))

        # 2. 负债率变动趋势 (杠杆是否在恶化?)
        # 从趋势探针中近似: 如果多个效率指标恶化 + 负债率本身高, 杠杆趋势恶化
        decay_score_val = aggregate_feature(probes, "has_deterioration", "mean")
        negative_slopes = aggregate_feature(probes, "log_slope", "min")
        if decay_score_val is not None and debt_to_assets >= 0:
            # 如果基本面恶化+高杠杆 → 趋势更差
            trend_risk = 0.0
            if decay_score_val > 0.5:  # 有恶化信号
                trend_risk += 0.4
            if negative_slopes is not None and negative_slopes < -0.05:
                trend_risk += 0.3
            if debt_to_assets > conf.safe_debt_ratio:
                trend_risk += 0.3
            trend_risk = min(1.0, trend_risk)

            w = weights.get("debt_trend", 0.25)
            leverage_score += w * trend_risk
            total_weight += w
            components["debt_trend_risk"] = trend_risk

        # 3. 现金覆盖度 (现金是否足以应对短期债务)
        cash_to_assets = get_financial_context(probes, "ratio_cash_to_assets", -1.0)
        if cash_to_assets >= 0 and debt_to_assets > 0:
            # 现金/负债 比率: 高 = 安全
            cash_coverage = cash_to_assets / max(debt_to_assets, 0.01)
            # 反向: 低覆盖 = 高杠杆风险
            coverage_risk = max(0, 1.0 - cash_coverage)
            w = weights.get("cash_coverage", 0.20)
            leverage_score += w * coverage_risk
            total_weight += w
            components["cash_coverage_ratio"] = cash_coverage
            components["coverage_risk"] = coverage_risk

        # 4. 权益乘数 (总资产/股东权益)
        assets_to_equity = get_financial_context(probes, "ratio_equity_multiplier", -1.0)
        if assets_to_equity < 0:
            # 从 debt_to_assets 近似: EM = 1/(1-D/A)
            if debt_to_assets >= 0 and debt_to_assets < 0.95:
                assets_to_equity = 1.0 / (1.0 - debt_to_assets)
                components["equity_multiplier_source"] = "derived"

        if assets_to_equity > 0:
            # 权益乘数 2 = 50%负债(正常), 4 = 75%负债(危险), 10 = 90%负债(极度)
            em_score = min(1.0, max(0, (assets_to_equity - 1.0) / 4.0))
            w = weights.get("equity_multiplier", 0.20)
            leverage_score += w * em_score
            total_weight += w
            components["equity_multiplier"] = assets_to_equity

        # ================================================================
        # 计算最终分数
        # ================================================================
        if total_weight > 0:
            score = leverage_score / total_weight
        else:
            score = 0.3

        score = max(0.0, min(1.0, score))

        # 置信度
        confidence = min(0.95, 0.4 + total_weight * 0.6)

        # 杠杆等级
        if score < 0.25:
            leverage_level = "conservative"   # 保守型 (低杠杆)
        elif score < 0.45:
            leverage_level = "moderate"        # 适度杠杆
        elif score < 0.65:
            leverage_level = "elevated"        # 偏高杠杆
        else:
            leverage_level = "dangerous"       # 危险杠杆

        return FactorResult(
            factor_id=self.factor_id,
            ts_code=ts_code,
            score=score,
            confidence=confidence,
            components=components,
            details={
                "leverage_level": leverage_level,
                "data_available": True,
            },
        ), warnings

    def explain(self, result: FactorResult) -> str:
        """生成人类可读的解释文本"""
        score = result.score or 0.3
        components = result.components or {}
        details = result.details or {}

        level = details.get("leverage_level", "unknown")
        level_label = {
            "conservative": "低杠杆 (安全)",
            "moderate": "适度杠杆",
            "elevated": "偏高杠杆 ⚠️",
            "dangerous": "危险杠杆 🚨",
            "unknown": "未知",
        }.get(level, "未知")

        parts = [f"λ={score:.2f} ({level_label})"]

        if "debt_to_assets" in components:
            parts.append(f"资产负债率={components['debt_to_assets']:.1%}")
        if "equity_multiplier" in components:
            parts.append(f"权益乘数={components['equity_multiplier']:.1f}x")

        return "，".join(parts)


# ============================================================================
# δ_fraud 因子: 欺诈熵 (Fraud Entropy)
# ============================================================================

@dataclass
class DeltaFraudFactor:
    """δ_fraud 因子: 欺诈熵 - 财务报表的物理真实性

    这是熔断项: δ_fraud > 0.58 直接触发警报

    v3.1 升级: 整合 financial_context 探针进行真实风险检测

    一级风险信号 (来自 FinancialContextProbe，硬杀):
        1. 商誉爆雷: goodwill/equity > 0.4
        2. 存贷双高: cash/assets > 0.3 且 debt/assets > 0.6
        3. 应收占比过高: receivables/revenue > 0.5

    二级风险信号 (来自趋势探针):
        4. 利润-现金流背离: OCF增速 << 利润增速
        5. 麦道夫特征: 业绩太平滑 (CV < 1%)
        6. 太完美: R² > 99%
        7. 交叉验证失败: 营收增长但效率下降
    """

    factor_id: FactorId = FactorId.DELTA_FRAUD

    def evaluate(self,
                 ts_code: str,
                 probes: Sequence[ProbeInput],
                 config: TruthConfig) -> Tuple[FactorResult, List[TruthWarning]]:

        warnings: List[TruthWarning] = []
        components: Dict[str, float] = {}
        conf = config.delta_fraud_config
        weights = conf.component_weights

        fraud_signals = 0.0
        total_weight = 0.0
        hard_kill_triggered = False

        # ================================================================
        # 一级风险: financial_context 硬杀检测
        # ================================================================
        if has_financial_context(probes):
            # 1. 商誉爆雷风险 (硬杀)
            goodwill_risk = get_financial_context(probes, "flag_goodwill_risk", 0.0)
            goodwill_to_equity = get_financial_context(probes, "ratio_goodwill_to_equity", 0.0)

            if goodwill_risk > 0.5 or goodwill_to_equity > 0.4:
                # 商誉风险触发熔断
                hard_kill_triggered = True
                goodwill_score = min(1.0, goodwill_to_equity / 0.4)  # >0.4 满分
                w = 0.35  # 最高权重
                fraud_signals += w * goodwill_score
                total_weight += w
                components["goodwill_risk"] = goodwill_score
                components["goodwill_to_equity"] = goodwill_to_equity

                warnings.append(TruthWarning(
                    code="FRAUD_GOODWILL_BOMB",
                    level=WarningLevel.FATAL,
                    title="🚨 商誉爆雷风险",
                    message=f"商誉/权益={goodwill_to_equity:.1%}，超过40%警戒线",
                    source="delta_fraud_factor",
                    values={"goodwill_to_equity": goodwill_to_equity},
                ))

            # 2. 存贷双高检测 (硬杀)
            cash_loan_anomaly = get_financial_context(probes, "flag_cash_loan_anomaly", 0.0)
            cash_ratio = get_financial_context(probes, "ratio_cash_to_assets", 0.0)
            debt_ratio = get_financial_context(probes, "ratio_debt_to_assets", 0.0)

            if cash_loan_anomaly > 0.5:
                # 存贷双高触发熔断
                hard_kill_triggered = True
                w = 0.30
                fraud_signals += w * 1.0  # 满分
                total_weight += w
                components["cash_loan_anomaly"] = 1.0
                components["cash_ratio"] = cash_ratio
                components["debt_ratio"] = debt_ratio

                warnings.append(TruthWarning(
                    code="FRAUD_CASH_LOAN_DOUBLE_HIGH",
                    level=WarningLevel.FATAL,
                    title="🚨 存贷双高",
                    message=f"货币资金占比{cash_ratio:.1%}，负债率{debt_ratio:.1%}，存贷双高",
                    source="delta_fraud_factor",
                    values={"cash_ratio": cash_ratio, "debt_ratio": debt_ratio},
                ))

            # 3. 应收账款过高 (严重警告)
            high_receivable = get_financial_context(probes, "flag_high_receivable", 0.0)
            receivable_to_revenue = get_financial_context(probes, "ratio_receivable_to_revenue", 0.0)

            if high_receivable > 0.5 or receivable_to_revenue > 0.5:
                receivable_score = min(1.0, receivable_to_revenue / 0.5)
                w = 0.15
                fraud_signals += w * receivable_score
                total_weight += w
                components["high_receivable"] = receivable_score

                warnings.append(TruthWarning(
                    code="FRAUD_HIGH_RECEIVABLE",
                    level=WarningLevel.CRITICAL,
                    title="应收账款过高",
                    message=f"应收/营收={receivable_to_revenue:.1%}，可能存在收入确认问题",
                    source="delta_fraud_factor",
                    values={"receivable_to_revenue": receivable_to_revenue},
                ))

        # ================================================================
        # 一级风险+: Beneish M-Score 近似信号 (基于 financial_context)
        # 原始 M-Score 需要连续两年数据; 这里用可得比率近似
        # ================================================================

        if has_financial_context(probes):
            # Beneish-1: DSRI 近似 — 应收周转恶化
            # 高 ratio_receivable_to_revenue = 应收周转慢 = DSRI 升高
            recv_to_rev = get_financial_context(probes, "ratio_receivable_to_revenue", 0.0)
            if recv_to_rev > 0.25:
                # 正常 ~0.1-0.2, >0.25 开始异常, >0.5 严重
                dsri_score = min(1.0, (recv_to_rev - 0.25) / 0.35)
                w = 0.10
                fraud_signals += w * dsri_score
                total_weight += w
                components["beneish_dsri_proxy"] = dsri_score

            # Beneish-2: AQI 近似 — 资产质量恶化
            # 高无形资产占比 + 低有形资产 = 资产质量差
            intang_ratio = get_financial_context(probes, "ratio_intang_asset", 0.0)
            hard_asset = get_financial_context(probes, "ratio_hard_asset", 0.5)
            if intang_ratio > 0.3 and hard_asset < 0.3:
                aqi_score = min(1.0, (intang_ratio - 0.3) / 0.4)
                w = 0.08
                fraud_signals += w * aqi_score
                total_weight += w
                components["beneish_aqi_proxy"] = aqi_score

            # Beneish-3: TATA 近似 — 总应计占总资产
            # 用 valuation_cash_conversion 的倒数近似:
            # 低现金转化 = 高应计 = 高操纵概率
            cash_conv = get_financial_context(probes, "valuation_cash_conversion", float('nan'))
            if not math.isnan(cash_conv) and cash_conv < 0.5:
                # cash_conv < 0.5 说明利润中现金占比低 = 高应计
                tata_score = min(1.0, (0.5 - cash_conv) / 0.5)
                w = 0.08
                fraud_signals += w * tata_score
                total_weight += w
                components["beneish_tata_proxy"] = tata_score

                if cash_conv < 0.2:
                    warnings.append(TruthWarning(
                        code="FRAUD_HIGH_ACCRUALS",
                        level=WarningLevel.WARNING,
                        title="高应计异常",
                        message=f"现金转化率={cash_conv:.2f}，利润现金含量极低",
                        source="delta_fraud_factor",
                        values={"cash_conversion": cash_conv},
                    ))

        # ================================================================
        # 二级风险: 趋势探针信号
        # ================================================================

        # 4. 利润-现金流背离
        ocf_cagr = get_feature(probes, "cagr", "ocf")
        profit_cagr = get_feature(probes, "cagr", "profit")

        if ocf_cagr is not None and profit_cagr is not None:
            # 利润增长但现金流不跟 = 危险信号
            # v4.1.1 修复: CAGR 为小数形式 (0.05 = 5%), 阈值从 5 → 0.05
            if profit_cagr > 0.05 and ocf_cagr < profit_cagr * 0.3:
                divergence_score = min(1.0, (profit_cagr - ocf_cagr) / 0.30)
                w = weights.get("ocf_profit_divergence", 0.25)
                fraud_signals += w * divergence_score
                total_weight += w
                components["ocf_profit_divergence"] = divergence_score

                warnings.append(TruthWarning(
                    code="FRAUD_OCF_DIVERGENCE",
                    level=WarningLevel.CRITICAL,
                    title="利润-现金流背离",
                    message=f"利润增速{profit_cagr:.1f}%，但现金流仅{ocf_cagr:.1f}%",
                    source="delta_fraud_factor",
                    metrics=("profit", "ocf"),
                    values={"profit_cagr": profit_cagr, "ocf_cagr": ocf_cagr},
                ))

        # 5. 毛利率太平滑 ("麦道夫特征")
        margin_cv = get_feature(probes, "cv", "gross_margin")
        if margin_cv is not None:
            if margin_cv < conf.too_smooth_cv_threshold:
                smoothness_score = 1.0 - margin_cv / conf.too_smooth_cv_threshold
                w = weights.get("margin_smoothness", 0.15)
                fraud_signals += w * smoothness_score
                total_weight += w
                components["margin_smoothness"] = smoothness_score

                warnings.append(TruthWarning(
                    code="FRAUD_TOO_SMOOTH",
                    level=WarningLevel.WARNING,
                    title="业绩过于平滑",
                    message=f"毛利率CV仅{margin_cv:.3f}，异常平稳",
                    source="delta_fraud_factor",
                    values={"margin_cv": margin_cv},
                ))

        # 6. R² 太高 ("太完美")
        revenue_r2 = get_feature(probes, "r_squared", "revenue")
        if revenue_r2 is not None:
            if revenue_r2 > conf.too_perfect_r2_threshold:
                perfect_score = (revenue_r2 - conf.too_perfect_r2_threshold) / (1.0 - conf.too_perfect_r2_threshold)
                w = weights.get("revenue_r_squared", 0.10)
                fraud_signals += w * perfect_score
                total_weight += w
                components["revenue_r2_too_high"] = perfect_score

        # 7. 交叉验证: 营收增长但ROIC下降 = 质量恶化
        revenue_slope = get_feature(probes, "log_slope", "revenue")
        roic_slope = get_feature(probes, "log_slope", "roic")

        if revenue_slope is not None and roic_slope is not None:
            if revenue_slope > 0.05 and roic_slope < -0.02:
                # 营收增长但效率下降
                cross_val_score = min(1.0, (revenue_slope - roic_slope) / 0.2)
                w = weights.get("cross_validation", 0.10)
                fraud_signals += w * cross_val_score
                total_weight += w
                components["cross_validation_fail"] = cross_val_score

        # ================================================================
        # 计算最终分数
        # v4.1.1 修复: 用所有可能信号的最大权重之和作为分母
        # 旧逻辑: score = fraud_signals / total_weight (仅触发信号的权重和)
        # 问题: 单个信号触发时分母极小, 任何 raw_score > 0.58 直接触发熔断
        # 新逻辑: 用固定分母 (所有信号权重之和), 只有多信号并发才能触发熔断
        # ================================================================
        # 所有可能的信号权重总和:
        # 一级: goodwill=0.35 + cash_loan=0.30 + receivable=0.15 = 0.80
        # Beneish: DSRI=0.10 + AQI=0.08 + TATA=0.08 = 0.26
        # 二级(from config): ocf_divergence + smoothness + r_squared + cross_val ≈ 0.80
        # 总计 ≈ 1.86, 这里用保守估计 1.0 作为分母基底
        max_possible_weight = max(1.0, total_weight)  # 至少为 1.0
        if total_weight > 0:
            score = fraud_signals / max_possible_weight
        else:
            score = 0.0  # 无信号 = 无欺诈嫌疑

        # 硬杀时确保分数超过熔断阈值
        if hard_kill_triggered:
            score = max(score, conf.meltdown_threshold + 0.05)

        score = max(0.0, min(1.0, score))

        # 熔断检测
        is_meltdown = score > conf.meltdown_threshold

        if is_meltdown and not hard_kill_triggered:
            # 非硬杀触发的熔断
            warnings.append(TruthWarning(
                code="FRAUD_MELTDOWN",
                level=WarningLevel.FATAL,
                title="🚨 欺诈熔断",
                message=f"δ_fraud={score:.2f} > {conf.meltdown_threshold}，触发熔断",
                source="delta_fraud_factor",
                values={"delta_fraud": score, "threshold": conf.meltdown_threshold},
            ))

        # 置信度
        confidence = 0.9 if has_financial_context(probes) else 0.6

        return FactorResult(
            factor_id=self.factor_id,
            ts_code=ts_code,
            score=score,
            confidence=confidence,
            components=components,
            details={
                "is_meltdown": is_meltdown,
                "hard_kill_triggered": hard_kill_triggered,
                "has_financial_context": has_financial_context(probes),
            },
        ), warnings

    def explain(self, result: FactorResult) -> str:
        """生成人类可读的解释文本"""
        score = result.score or 0.0
        components = result.components or {}
        details = result.details or {}

        is_meltdown = details.get("is_meltdown", False)
        hard_kill = details.get("hard_kill_triggered", False)

        if is_meltdown:
            parts = [f"🚨 δ_fraud={score:.2f} (熔断触发!)"]
            if hard_kill:
                if "goodwill_risk" in components:
                    parts.append("商誉爆雷风险")
                if "cash_loan_anomaly" in components:
                    parts.append("存贷双高")
                if "high_receivable" in components:
                    parts.append("应收过高")
        elif score > 0.4:
            parts = [f"⚠️ δ_fraud={score:.2f} (风险偏高)"]
        else:
            parts = [f"δ_fraud={score:.2f} (风险可控)"]

        if "ocf_profit_divergence" in components:
            parts.append("利润现金流背离")

        return "，".join(parts)


# ============================================================================
# δ_decay 因子: 衰退熵 (Decay Entropy)
# ============================================================================

@dataclass
class DeltaDecayFactor:
    """δ_decay 因子: 衰退熵 - 商业模式的恶化趋势

    这是惩罚项: 用于调低评分

    衰退信号:
        1. has_deterioration: 是否存在恶化
        2. consecutive_decline: 连续下跌年数
        3. total_decline_pct: 总下跌百分比
        4. deterioration_acceleration: 恶化加速度
        5. negative_slope: 负斜率惩罚
    """

    factor_id: FactorId = FactorId.DELTA_DECAY

    def evaluate(self,
                 ts_code: str,
                 probes: Sequence[ProbeInput],
                 config: TruthConfig) -> Tuple[FactorResult, List[TruthWarning]]:

        warnings: List[TruthWarning] = []
        components: Dict[str, float] = {}
        conf = config.delta_decay_config
        weights = conf.component_weights

        decay_score = 0.0
        total_weight = 0.0

        # 效率指标 (ROIC, ROE, 毛利率)
        efficiency_metrics = ["roic", "roe", "gross_margin"]

        # 1. 是否存在恶化
        has_deterioration = aggregate_feature(probes, "has_deterioration", "max", efficiency_metrics)
        if has_deterioration is not None:
            det_score = 1.0 if has_deterioration > 0.5 else 0.0
            w = weights.get("has_deterioration", 0.25)
            decay_score += w * det_score
            total_weight += w
            components["has_deterioration"] = has_deterioration

        # 2. 连续下跌年数
        consecutive = aggregate_feature(probes, "consecutive_decline_years", "max", efficiency_metrics)
        if consecutive is not None:
            # 3年以上连跌 = 危险
            consec_score = min(1.0, consecutive / 5.0)
            w = weights.get("consecutive_decline", 0.25)
            decay_score += w * consec_score
            total_weight += w
            components["consecutive_decline_years"] = consecutive

            if consecutive >= conf.consecutive_years_threshold:
                warnings.append(TruthWarning(
                    code="DECAY_CONSECUTIVE",
                    level=WarningLevel.WARNING,
                    title="连续下跌",
                    message=f"效率指标连续{int(consecutive)}年下跌",
                    source="delta_decay_factor",
                    values={"consecutive_years": consecutive},
                ))

        # 3. 总下跌百分比
        total_decline = aggregate_feature(probes, "total_decline_pct", "min", efficiency_metrics)
        if total_decline is not None:
            # 下跌幅度 (负数表示下跌)
            decline_pct = abs(min(0, total_decline))  # total_decline 为百分数 (如 -30 表示跌30%)
            # v4.1.1 修复: severe_decline_threshold=0.30 (30%), 乘 100 转为百分数单位匹配
            decline_score = min(1.0, decline_pct / (conf.severe_decline_threshold * 100))
            w = weights.get("total_decline_pct", 0.20)
            decay_score += w * decline_score
            total_weight += w
            components["total_decline_pct"] = total_decline

        # 4. 恶化加速度
        det_accel = aggregate_feature(probes, "deterioration_acceleration", "max")
        if det_accel is not None and det_accel > 0:
            accel_score = min(1.0, det_accel / 0.1)
            w = weights.get("deterioration_acceleration", 0.15)
            decay_score += w * accel_score
            total_weight += w
            components["deterioration_acceleration"] = det_accel

        # 5. 负斜率惩罚
        log_slope = aggregate_feature(probes, "log_slope", "mean", efficiency_metrics)
        if log_slope is not None and log_slope < 0:
            slope_penalty = min(1.0, abs(log_slope) / 0.1)
            w = weights.get("negative_slope", 0.15)
            decay_score += w * slope_penalty
            total_weight += w
            components["negative_slope_penalty"] = slope_penalty

        # 计算最终分数
        if total_weight > 0:
            score = decay_score / total_weight
        else:
            score = 0.0

        score = max(0.0, min(1.0, score))

        # 衰退严重程度
        if score > 0.7:
            decay_severity = "severe"
        elif score > 0.4:
            decay_severity = "moderate"
        elif score > 0.1:
            decay_severity = "mild"
        else:
            decay_severity = "none"

        return FactorResult(
            factor_id=self.factor_id,
            ts_code=ts_code,
            score=score,
            confidence=min(1.0, total_weight / 0.6),
            components=components,
            details={"decay_severity": decay_severity},
        ), warnings

    def explain(self, result: FactorResult) -> str:
        """生成人类可读的解释文本"""
        score = result.score or 0.0
        components = result.components or {}
        details = result.details or {}

        severity = details.get("decay_severity", "unknown")
        severity_label = {
            "severe": "严重衰退",
            "moderate": "中度衰退",
            "mild": "轻微衰退",
            "none": "无衰退",
        }.get(severity, "未知")

        parts = [f"δ_decay={score:.2f} ({severity_label})"]

        if "consecutive_decline_years" in components:
            years = int(components["consecutive_decline_years"])
            if years > 0:
                parts.append(f"连跌{years}年")

        if "total_decline_pct" in components:
            decline = components["total_decline_pct"]
            if decline < 0:
                parts.append(f"累计下跌{abs(decline):.1f}%")

        return "，".join(parts)


# ============================================================================
# V 因子: 真相验证 (Verification)
# ============================================================================

@dataclass
class VerificationFactor:
    """V 因子: 真相验证 - 成长的含金量 (照妖镜)

    核心公式: V = OCF增速 / 营收增速

    V > 1.0: 现金流增速超过营收 = 真成长 (含金量高)
    V = 1.0: 正常
    V < 1.0: 营收增长但现金流不跟 = 假成长/应收账款堆积
    V < 0.3: 危险信号

    这是"照妖镜"：揭示成长的真实质量
    """

    factor_id: FactorId = FactorId.VERIFICATION

    def evaluate(self,
                 ts_code: str,
                 probes: Sequence[ProbeInput],
                 config: TruthConfig) -> Tuple[FactorResult, List[TruthWarning]]:

        warnings: List[TruthWarning] = []
        components: Dict[str, float] = {}
        conf = config.verification_config
        weights = conf.component_weights

        # v4.6 BUG FIX: 0.5 初始值 + 仅 sloan(w=0.15) 触发时 → 0.5/0.15=3.33 → 截断为1.00
        # 导致 74.8% 的 A/A+ 公司 V=1.00 (虚假满分), 置信度仅 21%
        # 修复: 从 0 开始累加, 无数据时 fallback 仍为 0.5
        score = 0.0
        total_weight = 0.0

        # 1. OCF增速 / 营收增速
        ocf_cagr = get_feature(probes, "cagr", "ocf")
        if ocf_cagr is None:
            ocf_cagr = get_feature(probes, "cagr_approx", "ocf")

        revenue_cagr = get_feature(probes, "cagr", "revenue")
        if revenue_cagr is None:
            revenue_cagr = get_feature(probes, "cagr_approx", "revenue")

        v_ratio_revenue = None
        if ocf_cagr is not None and revenue_cagr is not None:
            components["ocf_cagr"] = ocf_cagr
            components["revenue_cagr"] = revenue_cagr

            # 计算 V 比率 (CAGR 为小数: 5%=0.05)
            if abs(revenue_cagr) < 0.01:
                # 营收几乎不变(<1%)，看 OCF 绝对值
                v_ratio_revenue = 1.0 if ocf_cagr >= 0 else 0.5
            elif revenue_cagr < 0:
                # 营收下降
                if ocf_cagr > revenue_cagr:
                    v_ratio_revenue = 1.2  # 现金流比营收抗跌 = 好
                else:
                    v_ratio_revenue = 0.7
            else:
                # 正常情况
                v_ratio_revenue = ocf_cagr / revenue_cagr

            # 归一化到 [0, 1]
            v_normalized = normalize_score(v_ratio_revenue, "minmax", 0.0, 1.5)
            w = weights.get("ocf_revenue_ratio", 0.50)
            score += v_normalized * w  # v4.6: += 而非 = (与其他组件一致)
            total_weight += w
            components["v_ratio_revenue"] = v_ratio_revenue

        # 2. OCF增速 / 利润增速
        profit_cagr = get_feature(probes, "cagr", "profit")
        if profit_cagr is None:
            profit_cagr = get_feature(probes, "cagr_approx", "profit")

        v_ratio_profit = None
        if ocf_cagr is not None and profit_cagr is not None:
            components["profit_cagr"] = profit_cagr

            if abs(profit_cagr) < 0.01:
                # 利润几乎不变(<1%)，看 OCF 绝对值
                v_ratio_profit = 1.0 if ocf_cagr >= 0 else 0.5
            elif profit_cagr < 0:
                v_ratio_profit = 1.2 if ocf_cagr > profit_cagr else 0.7
            else:
                v_ratio_profit = ocf_cagr / profit_cagr

            v_normalized = normalize_score(v_ratio_profit, "minmax", 0.0, 1.5)
            w = weights.get("ocf_profit_ratio", 0.30)
            score += v_normalized * w
            total_weight += w
            components["v_ratio_profit"] = v_ratio_profit

        # 3. 一致性检验: 两个 V 比率是否一致
        if v_ratio_revenue is not None and v_ratio_profit is not None:
            consistency = 1.0 - abs(v_ratio_revenue - v_ratio_profit) / max(v_ratio_revenue, v_ratio_profit, 0.01)
            w = weights.get("consistency", 0.20)
            score += max(0, consistency) * w
            total_weight += w
            components["consistency"] = consistency

        # 4. Sloan Accruals Ratio (Sloan 1996) — 应计质量
        # Accruals = (ΔCA - ΔCash) - (ΔCL - ΔSTD - ΔTP) - Dep&Amort
        # 简化版: 用 OCF/净利润 比率的倒数近似
        # 高应计 (低 OCF/利润) = 盈余质量差, 未来回报低
        if has_financial_context(probes):
            # 从 financial_context 取现金转化效率
            cash_conversion = get_financial_context(probes, "valuation_cash_conversion", float('nan'))
            if not math.isnan(cash_conversion):
                # cash_conversion = FCFF / |EPS|, 范围 [-2, 3]
                # 映射到 [0, 1]: 高现金转化 = 低应计 = 好
                # cash_conversion > 1.0: 优秀 (现金流超过利润)
                # cash_conversion 0.5~1.0: 正常
                # cash_conversion < 0.5: 应计质量差
                if cash_conversion >= 1.0:
                    sloan_score = 1.0  # 优秀: 经营现金流超过利润
                elif cash_conversion >= 0.0:
                    sloan_score = cash_conversion  # 线性映射
                else:
                    sloan_score = 0.0  # 负现金转化 = 最差

                w = weights.get("sloan_accruals", 0.15)
                score += sloan_score * w
                total_weight += w
                components["sloan_accruals"] = sloan_score
                components["cash_conversion_raw"] = cash_conversion

                if cash_conversion < 0.3:
                    warnings.append(TruthWarning(
                        code="V_HIGH_ACCRUALS",
                        level=WarningLevel.WARNING,
                        title="高应计比率",
                        message=f"现金转化率={cash_conversion:.2f}，盈余质量堪忧",
                        source="verification_factor",
                        values={"cash_conversion": cash_conversion},
                    ))

        # 归一化
        if total_weight > 0:
            score = score / total_weight
        else:
            score = 0.5

        score = max(0.0, min(1.0, score))

        # 置信度
        confidence = min(1.0, total_weight / 0.7)

        # 成长质量判定
        if v_ratio_revenue is not None:
            if v_ratio_revenue >= conf.true_growth_threshold:
                growth_quality = "true_growth"
            elif v_ratio_revenue >= 0.5:
                growth_quality = "moderate_quality"
            elif v_ratio_revenue >= conf.fake_growth_threshold:
                growth_quality = "low_quality"
            else:
                growth_quality = "fake_growth"
                warnings.append(TruthWarning(
                    code="V_FAKE_GROWTH",
                    level=WarningLevel.CRITICAL,
                    title="假成长信号",
                    message=f"V={v_ratio_revenue:.2f}，现金流严重落后于营收",
                    source="verification_factor",
                    values={"v_ratio": v_ratio_revenue},
                ))
        else:
            growth_quality = "unknown"

        return FactorResult(
            factor_id=self.factor_id,
            ts_code=ts_code,
            score=score,
            confidence=confidence,
            components=components,
            details={"growth_quality": growth_quality},
        ), warnings

    def explain(self, result: FactorResult) -> str:
        """生成人类可读的解释文本"""
        score = result.score or 0.5
        components = result.components or {}
        details = result.details or {}

        quality = details.get("growth_quality", "unknown")
        quality_label = {
            "true_growth": "真成长 ✓",
            "moderate_quality": "中等质量",
            "low_quality": "低质量",
            "fake_growth": "假成长 ✗",
            "unknown": "未知",
        }.get(quality, "未知")

        parts = [f"V={score:.2f} ({quality_label})"]

        if "v_ratio_revenue" in components:
            v_ratio = components["v_ratio_revenue"]
            parts.append(f"OCF/营收增速={v_ratio:.2f}")
        if "ocf_cagr" in components and "revenue_cagr" in components:
            parts.append(f"OCF增速{components['ocf_cagr']:.1f}% vs 营收{components['revenue_cagr']:.1f}%")

        return "，".join(parts)


# ============================================================================
# 类型别名 (兼容性)
# ============================================================================

# TruthFactor 现在是一个协议类型别名，保持向后兼容
# 任何实现了 factor_id, evaluate(), explain() 的类都是有效因子
from typing import Union
TruthFactor = Union[AlphaFactor, BetaFactor, GammaFactor, LambdaFactor, DeltaFraudFactor, DeltaDecayFactor, VerificationFactor]


# ============================================================================
# 工厂函数
# ============================================================================

def get_all_factors() -> List[TruthFactor]:
    """获取所有因子实例"""
    return [
        AlphaFactor(),
        BetaFactor(),
        GammaFactor(),
        LambdaFactor(),
        DeltaFraudFactor(),
        DeltaDecayFactor(),
        VerificationFactor(),
    ]


def get_factor_by_id(factor_id: FactorId) -> TruthFactor:
    """根据ID获取因子"""
    mapping = {
        FactorId.ALPHA: AlphaFactor(),
        FactorId.BETA: BetaFactor(),
        FactorId.GAMMA: GammaFactor(),
        FactorId.LAMBDA: LambdaFactor(),
        FactorId.DELTA_FRAUD: DeltaFraudFactor(),
        FactorId.DELTA_DECAY: DeltaDecayFactor(),
        FactorId.VERIFICATION: VerificationFactor(),
    }
    return mapping.get(factor_id, AlphaFactor())


# ============================================================================
# 导出
# ============================================================================

__all__ = [
    # 类型别名 (兼容性)
    "TruthFactor",
    # 因子实现
    "AlphaFactor",
    "BetaFactor",
    "GammaFactor",
    "LambdaFactor",
    "DeltaFraudFactor",
    "DeltaDecayFactor",
    "VerificationFactor",
    # 工厂函数
    "get_all_factors",
    "get_factor_by_id",
    # 辅助函数
    "normalize_score",
    "get_feature",
    "get_financial_context",
    "has_financial_context",
    "aggregate_feature",
]
