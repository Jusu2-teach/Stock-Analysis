"""T.R.U.T.H. 八维因子计算 - 专业级基因测序实现

实现设计文档中定义的八维基因测序：
    - α (Cyclicality): 周期性 - 业绩对宏观经济的敏感弹性
    - β (Heaviness): 资本密度 - 赚取下一块钱利润所需的"重"度
    - γ (Growth): 原始动能 - 业务表面上的扩张加速度
    - π (Profitability): 盈利质量 - GP/Assets (Novy-Marx 最强单因子)
    - λ (Leverage): 杠杆风险 - 资本结构健康度
    - δ_fraud (Fraud Entropy): 欺诈熵 - 财务报表的物理真实性 (熔断项)
    - δ_decay (Decay Entropy): 衰退熵 - 商业模式的恶化趋势 (惩罚项)
    - V (Verification): 真相验证 - 成长的含金量 (照妖镜)

数据依赖:
    - 所有因子从 ProbeInput.features 获取趋势特征
    - β/δ_fraud 因子需要 financial_context 探针提供原始财务结构

架构说明:
    - 使用鸭子类型 (duck typing)，不依赖 ABC 继承
    - 实现 FactorProtocol (typing.Protocol) 即可作为因子
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

from .models import FactorId, FactorResult, ProbeInput, TruthWarning, WarningLevel
from .config import (
    TruthConfig,
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

        # v4.8: ROIIC 是衍生指标 (Δ利润/Δ资本), 天然有极端波动性 (cv=7-30),
        # 不反映真实业务周期性. 排除 ROIIC 以避免α虚高.
        _ALPHA_METRICS = ["roic", "roe", "revenue", "profit",
                          "gross_margin", "net_margin", "ocf"]

        # v5.0: 去共线性重构 — 移除原始cv(与detrended_cv r>0.9), 重新分配权重
        # 学术依据: 多重共线性导致权重语义失效(VIF>5不可接受)
        # 重构后: detrended_cv(0.40) + R²反向(0.30) + 周期标志(0.20) + Hurst(0.10)

        # 1. 去趋势变异系数 (主波动率度量, 吸收原cv权重)
        detrended_cv = aggregate_feature(probes, "detrended_cv", "mean",
                                         metrics=_ALPHA_METRICS)
        if detrended_cv is None:
            detrended_cv = aggregate_feature(probes, "cv", "mean",
                                             metrics=_ALPHA_METRICS)

        if detrended_cv is not None:
            normalized = normalize_score(detrended_cv, "minmax", 0.0, 1.0)
            w = weights.get("detrended_cv", 0.40)
            score += w * normalized
            total_weight += w
            components["detrended_cv"] = detrended_cv
            confidence_factors.append(1.0)
        else:
            confidence_factors.append(0.3)

        # 2. R² 反向 (低R² = 趋势不明确 = 周期性)
        r_squared = aggregate_feature(probes, "r_squared", "mean",
                                       metrics=_ALPHA_METRICS)
        if r_squared is not None:
            r_squared = max(0.0, min(1.0, r_squared))
            r_squared_inverse = 1.0 - r_squared
            w = weights.get("r_squared_inverse", 0.30)
            score += w * r_squared_inverse
            total_weight += w
            components["r_squared"] = r_squared
            components["r_squared_inverse"] = r_squared_inverse
            confidence_factors.append(1.0)
        else:
            confidence_factors.append(0.3)

        # v5.0: 移除原始cv组件 — 与detrended_cv共线(r>0.9), 双重计数

        # 3. 周期性标志 (行业分类信号)
        is_cyclical = aggregate_feature(probes, "is_cyclical", "max",
                                          metrics=_ALPHA_METRICS)
        if is_cyclical is not None:
            cyclical_score = 1.0 if is_cyclical > 0.5 else 0.0
            w = weights.get("is_cyclical", 0.20)
            score += w * cyclical_score
            total_weight += w
            components["is_cyclical"] = is_cyclical

        # 4. Hurst 指数 (H < 0.5 = 均值回归 = 真周期)
        hurst = aggregate_feature(probes, "hurst_exponent", "mean",
                                    metrics=_ALPHA_METRICS)
        if hurst is not None:
            hurst_score = 1.0 - normalize_score(hurst, "minmax", 0.3, 0.7)
            w = weights.get("hurst_exponent", 0.10)
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

        # v5.0: 去共线性重构 — 移除nca_ratio(hard_asset是其子集, r>0.8)
        # 重构后: hard_asset(0.50) + intang反向(0.25) + working_capital反向(0.25)

        # 1. 硬资产比率 (核心指标, 吸收原nca权重)
        hard_asset_ratio = get_financial_context(probes, "ratio_hard_asset", -1.0)
        if hard_asset_ratio >= 0:
            w = weights.get("hard_asset_ratio", 0.50)
            score += w * hard_asset_ratio
            total_weight += w
            components["hard_asset_ratio"] = hard_asset_ratio

        # v5.0: 移除nca_ratio — 固定资产是非流动资产子集, 高度共线(VIF>5)

        # 2. 无形资产比率 (轻资产特征，反向)
        intang_ratio = get_financial_context(probes, "ratio_intang_asset", -1.0)
        if intang_ratio >= 0:
            beta_from_intang = 1.0 - min(1.0, intang_ratio * 2)
            w = weights.get("intang_ratio", 0.25)
            score += w * beta_from_intang
            total_weight += w
            components["intang_ratio"] = intang_ratio

        # 3. 营运资本比率 (轻资产特征，反向)
        working_capital_ratio = get_financial_context(probes, "ratio_working_capital", -1.0)
        if working_capital_ratio >= 0:
            beta_from_wc = 1.0 - min(1.0, working_capital_ratio)
            w = weights.get("working_capital_ratio", 0.25)
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

        # v5.0: CAGR归一化致命BUG修复 + 去共线性重构
        #   BUG: cagr/50.0 → CAGR是小数(0.17=17%), 除以50后值域[-0.01,+0.01]
        #        tanh(0.003)≈0.003 → 归一化后全部压在0.500±0.005, 零区分度!
        #   修复: cagr/0.50 → ±50%增速映射到±1, tanh(±1)=±0.76, 区分度充分
        #   去共线: 移除robust_slope(与log_slope r>0.95), 重新分配权重
        #   新权重: CAGR(0.45) + log_slope(0.30) + recent_3y(0.20) + R²惩罚(0.05)

        if cagr is not None:
            # v5.0修复: cagr/0.50 (±50%增速→±1), NOT cagr/50.0
            normalized = normalize_score(cagr / 0.50, "tanh")
            w = weights.get("cagr", 0.45)
            score += w * normalized
            total_weight += w
            components["cagr"] = cagr

        # 2. 对数斜率 (增长加速度)
        log_slope = aggregate_feature(probes, "log_slope", "mean", growth_metrics)
        if log_slope is not None:
            normalized = normalize_score(log_slope / 0.3, "tanh")
            w = weights.get("log_slope", 0.30)
            score += w * normalized
            total_weight += w
            components["log_slope"] = log_slope

        # 3. 近3年斜率 (动量 — 捕捉近期加速/减速)
        recent_slope = aggregate_feature(probes, "recent_3y_slope", "mean", growth_metrics)
        if recent_slope is not None:
            normalized = normalize_score(recent_slope / 0.3, "tanh")
            w = weights.get("recent_3y_slope", 0.20)
            score += w * normalized
            total_weight += w
            components["recent_3y_slope"] = recent_slope

        # v5.0: 移除robust_slope — 与log_slope共线(r>0.95), 双重计数

        # 4. R² 惩罚 (不稳定增长打折)
        r_squared = aggregate_feature(probes, "r_squared", "mean", growth_metrics)
        if r_squared is not None:
            r2_penalty = max(0, 0.5 - r_squared)
            w = weights.get("r_squared_penalty", 0.05)
            score -= w * r2_penalty
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
                message=f"CAGR={cagr:.1%}，营收/利润处于下滑趋势",
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

# ============================================================================
# π 因子: 盈利能力水平 (Profitability Level) — v7.0 新增
# ============================================================================

@dataclass
class PiFactor:
    """π 因子: 盈利能力水平 - 当前资本回报绝对水平

    v7.0 新增: 填补 AQR QMJ / MSCI Quality / Novy-Marx 三大框架的核心维度缺失。

    学术依据:
        - Novy-Marx (2013): GP/Assets 是最强单一质量因子
          (Gross Profitability = GP/TA, 独立于 BM 和 momentum predict returns)
        - AQR QMJ: Profitability = GPOA + ROE + ROA + CFOA (四支柱首位)
        - MSCI Quality: ROE level 是三大成分之一
        - DuPont分解: ROE = Margin × Turnover × Leverage (本因子覆盖前两项)

    数据源:
        - financial_context 探针: profitability_gp_assets, profitability_assets_turn,
          profitability_roic_level, profitability_roe_level
        - 趋势探针: roic_latest_value, roe_latest_value (fallback)

    组件 (权重设计):
        1. GP/Assets (0.35): Novy-Marx 核心 — 用总资产归一化比净利更robust (不受杠杆/税影响)
        2. ROIC Level (0.30): 投入资本回报 — GMO Quality 核心, 含杠杆信息
        3. ROE Level (0.20): 股东权益回报 — MSCI Quality 核心, 反映股东价值创造
        4. Asset Turnover (0.15): DuPont分解 — 资本效率维度, 覆盖 Piotroski F-Score #9

    评分极性: **正向** (高π = 高盈利 = 高质量)
    """

    factor_id: FactorId = FactorId.PI

    def evaluate(self,
                 ts_code: str,
                 probes: Sequence[ProbeInput],
                 config: TruthConfig) -> Tuple[FactorResult, List[TruthWarning]]:

        warnings: List[TruthWarning] = []
        components: Dict[str, float] = {}
        conf = config.pi_config
        weights = conf.component_weights

        score = 0.0
        total_weight = 0.0

        # ================================================================
        # 数据源 1: financial_context 探针 (首选 — 最新年度数据)
        # ================================================================
        gp_assets = get_financial_context(probes, "profitability_gp_assets", -1.0)
        roic_level = get_financial_context(probes, "profitability_roic_level", float('nan'))
        roe_level = get_financial_context(probes, "profitability_roe_level", float('nan'))
        assets_turn = get_financial_context(probes, "profitability_assets_turn", -1.0)

        # ================================================================
        # Fallback: 从趋势探针获取 latest_value (时间序列最新值)
        # ================================================================
        if math.isnan(roic_level):
            roic_latest = get_feature(probes, "latest_value", metric_filter="roic")
            if roic_latest is not None:
                roic_level = roic_latest
        if math.isnan(roe_level):
            roe_latest = get_feature(probes, "latest_value", metric_filter="roe")
            if roe_latest is not None:
                roe_level = roe_latest

        # ================================================================
        # 1. GP/Assets — Novy-Marx (2013) 核心信号 (权重 0.35)
        #
        # GP/A = Gross Profit / Total Assets
        # = (Gross Margin) × (Revenue / Assets)
        # = grossprofit_margin/100 × assets_turn
        #
        # Sigmoid 归一化:
        #   GP/A = 0.05 (低) → ~0.27
        #   GP/A = 0.15 (中) → 0.50
        #   GP/A = 0.25 (高) → ~0.73
        #   GP/A = 0.40 (极高) → ~0.92
        # ================================================================
        if gp_assets >= 0:
            centered = (gp_assets - conf.gpa_center) / conf.gpa_scale
            gpa_score = 1.0 / (1.0 + math.exp(-centered))
            w = weights.get("gp_assets", 0.35)
            score += w * gpa_score
            total_weight += w
            components["gp_assets"] = gp_assets
            components["gp_assets_score"] = gpa_score

        # ================================================================
        # 2. ROIC Level — GMO Quality 核心 (权重 0.30)
        #
        # 投入资本回报率: 衡量公司用全部投入资本 (debt + equity) 创造价值的能力
        # 比 ROE 更全面 (不受杠杆扭曲), 是 Gravity solver 的核心输入
        #
        # Sigmoid 归一化 (center=10%, scale=5%):
        #   ROIC = 0% → ~0.12
        #   ROIC = 5% → ~0.27
        #   ROIC = 10% → 0.50
        #   ROIC = 15% → ~0.73
        #   ROIC = 25% → ~0.95
        # ================================================================
        if not math.isnan(roic_level):
            centered = (roic_level - conf.roic_center) / conf.roic_scale
            roic_score = 1.0 / (1.0 + math.exp(-centered))
            w = weights.get("roic_level", 0.30)
            score += w * roic_score
            total_weight += w
            components["roic_level"] = roic_level
            components["roic_level_score"] = roic_score

        # ================================================================
        # 3. ROE Level — MSCI Quality 核心 (权重 0.20)
        #
        # 股东权益回报率: 直接反映股东价值创造能力
        # 与 ROIC 部分相关但捕获杠杆效应信号 (DuPont ROE = margin×turn×leverage)
        #
        # Sigmoid 归一化 (center=12%, scale=6%):
        #   ROE = 0% → ~0.12
        #   ROE = 6% → ~0.27
        #   ROE = 12% → 0.50
        #   ROE = 18% → ~0.73
        #   ROE = 30% → ~0.95
        # ================================================================
        if not math.isnan(roe_level):
            centered = (roe_level - conf.roe_center) / conf.roe_scale
            roe_score = 1.0 / (1.0 + math.exp(-centered))
            w = weights.get("roe_level", 0.20)
            score += w * roe_score
            total_weight += w
            components["roe_level"] = roe_level
            components["roe_level_score"] = roe_score

        # ================================================================
        # 4. Asset Turnover — DuPont分解, Piotroski #9 (权重 0.15)
        #
        # 资产周转率 = Revenue / Total Assets
        # 高周转 = 资本效率高 (轻资产模式, 存货管理优秀)
        # 与 β 因子反向互补: β 度量"重度", π.AT 度量"效率"
        #
        # 归一化 (min-max):
        #   AT = 0.1 (极低) → ~0.10
        #   AT = 0.5 (中等) → ~0.45
        #   AT = 1.0 (高效) → ~0.75
        #   AT = 1.5 (极高) → ~1.00
        # ================================================================
        if assets_turn > 0:
            at_score = min(1.0, max(0.0, (assets_turn - 0.05) / 1.45))
            w = weights.get("asset_turnover", 0.15)
            score += w * at_score
            total_weight += w
            components["asset_turnover"] = assets_turn
            components["asset_turnover_score"] = at_score

        # ================================================================
        # 最终分数计算
        # ================================================================
        if total_weight > 0:
            score = score / total_weight
        else:
            score = 0.5  # 无数据时中性

        score = max(0.0, min(1.0, score))

        # 置信度: 基于可用组件数
        n_components = len([k for k in components if k.endswith("_score")])
        confidence = min(0.95, 0.3 + n_components * 0.18)

        # 盈利能力分类
        if score > 0.70:
            profitability_type = "high_profitability"
        elif score > 0.50:
            profitability_type = "moderate_profitability"
        elif score > 0.30:
            profitability_type = "low_profitability"
        else:
            profitability_type = "poor_profitability"

        # 警告: 高 ROIC 但低 GP/A → 可能是杠杆或税收扭曲
        if ("roic_level" in components and "gp_assets" in components
                and components.get("roic_level", 0) > 15
                and components.get("gp_assets", 1) < 0.08):
            warnings.append(TruthWarning(
                code="PI_ROIC_GPA_DIVERGENCE",
                level=WarningLevel.WARNING,
                title="ROIC与GP/A背离",
                message=f"ROIC={components['roic_level']:.1f}%高但GP/A={components['gp_assets']:.3f}低，"
                        f"可能存在杠杆扭曲或非经常性损益",
                source="pi_factor",
                values={
                    "roic_level": components.get("roic_level", 0),
                    "gp_assets": components.get("gp_assets", 0),
                },
            ))

        return FactorResult(
            factor_id=self.factor_id,
            ts_code=ts_code,
            score=score,
            confidence=confidence,
            components=components,
            details={
                "profitability_type": profitability_type,
                "n_components": n_components,
            },
        ), warnings

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
        # v5.0: 吸收原equity_multiplier权重(0.20), 现金覆盖权重0.20→0.35
        cash_to_assets = get_financial_context(probes, "ratio_cash_to_assets", -1.0)
        if cash_to_assets >= 0 and debt_to_assets > 0:
            cash_coverage = cash_to_assets / max(debt_to_assets, 0.01)
            coverage_risk = max(0, 1.0 - cash_coverage)
            w = weights.get("cash_coverage", 0.35)
            leverage_score += w * coverage_risk
            total_weight += w
            components["cash_coverage_ratio"] = cash_coverage
            components["coverage_risk"] = coverage_risk

        # v5.0: 移除equity_multiplier — EM=1/(1-D/A), 是debt_to_assets的单调变换
        # 两者共线(r=1.0), VIF→∞, 权重0.55实质是对同一变量的双重计数

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
                    message=f"利润增速{profit_cagr:.1%}，但现金流仅{ocf_cagr:.1%}",
                    source="delta_fraud_factor",
                    metrics=("profit", "ocf"),
                    values={"profit_cagr": profit_cagr, "ocf_cagr": ocf_cagr},
                ))

        # 5. 毛利率太平滑 ("麦道夫特征")
        # v4.7: 增加卓越稳定公司豁免
        # 原版问题: 迈瑞医疗毛利率极其稳定(CV<0.03)被标记为欺诈特征
        # 修复: 如果 ROIC 水平卓越(>15%) + 毛利率卓越(>30%) → 这是真实稳定, 不是造假
        margin_cv = get_feature(probes, "cv", "gross_margin")
        roic_for_smooth = get_feature(probes, "latest_value", "roic")
        gm_for_smooth = get_feature(probes, "latest_value", "gross_margin")
        is_genuinely_stable = (
            roic_for_smooth is not None and roic_for_smooth > 15.0
            and gm_for_smooth is not None and gm_for_smooth > 30.0
        )
        if margin_cv is not None:
            if margin_cv < conf.too_smooth_cv_threshold and not is_genuinely_stable:
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
            elif margin_cv < conf.too_smooth_cv_threshold and is_genuinely_stable:
                # 卓越稳定公司: 记录但不惩罚
                components["margin_smoothness"] = 0.0  # 豁免

        # 6. R² 太高 ("太完美")
        # v4.8: 多指标一致性高增长豁免 — 如果 revenue, profit, ROIC 均呈显著正趋势
        # 且 R² 都较高, 说明是真实的持续增长而非数据造假
        revenue_r2 = get_feature(probes, "r_squared", "revenue")
        if revenue_r2 is not None:
            if revenue_r2 > conf.too_perfect_r2_threshold:
                # 检查多指标一致性: ROIC 和 profit 也应显示一致的正向趋势
                roic_r2 = get_feature(probes, "r_squared", "roic")
                profit_r2 = get_feature(probes, "r_squared", "profit")
                roic_slope_val = get_feature(probes, "log_slope", "roic")
                revenue_slope_val = get_feature(probes, "log_slope", "revenue")

                is_multi_metric_consistent = (
                    roic_r2 is not None and roic_r2 > 0.80
                    and profit_r2 is not None and profit_r2 > 0.80
                    and roic_slope_val is not None and roic_slope_val > 0
                    and revenue_slope_val is not None and revenue_slope_val > 0
                )

                if is_multi_metric_consistent:
                    # 多指标一致性高增长: 真实的卓越企业, 豁免 R² 惩罚
                    components["revenue_r2_too_high"] = 0.0  # 记录但不惩罚
                else:
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

        # ═══════════════════════════════════════════════════════════════════
        # v8.0: 双层衰退监测 (Two-Tier Decay Detection)
        # ═══════════════════════════════════════════════════════════════════
        #
        # 传统方法仅监控3个效率指标 (ROIC/ROE/毛利率)，存在致命盲区:
        #   当营收崩塌但利润率暂时维持时（典型衰退初期），系统完全失聪。
        #
        # Howard Marks 第一多米诺骨牌原理:
        #   需求↓ → 营收↓ → 产能利用率↓ → 单位成本↑ → 利润率压缩 → ROIC↓
        #   等到效率指标恶化时,商业模式衰退已进入中后期。
        #
        # 设计: Tier 1 (效率层 65%) + Tier 2 (量价层 35%)
        #   效率衰退 = 竞争优势侵蚀 (护城河正在失守)
        #   量价衰退 = 需求/增长恶化 (先导信号, 提前预警)
        #
        # Monotonic Amplification 原则:
        #   量价层只能放大衰退信号, 永远不能稀释效率层信号。
        #   这确保扩展后的模型严格不弱于原始模型。
        # ═══════════════════════════════════════════════════════════════════
        efficiency_metrics = ["roic", "roe", "gross_margin"]          # Tier 1: 核心质量
        volume_metrics = ["revenue", "profit", "net_margin", "ocf"]   # Tier 2: 先导信号
        _EFF_W, _VOL_W = 0.65, 0.35

        def _tiered_agg(feature: str, agg_fn: str) -> Optional[float]:
            """双层加权聚合 + 单调放大约束

            量价层可以发现效率层遗漏的衰退信号,
            但在任何情况下都不会稀释已检测到的效率层衰退。
            """
            eff = aggregate_feature(probes, feature, agg_fn, efficiency_metrics)
            vol = aggregate_feature(probes, feature, agg_fn, volume_metrics)
            if eff is not None and vol is not None:
                blend = eff * _EFF_W + vol * _VOL_W
                # Monotonic Amplification: 取"更恶化方向"的值
                if agg_fn == "max":   # 高 = 更恶化 (has_deterioration, consecutive)
                    return max(eff, blend)
                else:                 # 低 = 更恶化 (total_decline, log_slope)
                    return min(eff, blend)
            return eff if eff is not None else vol

        # 1. 是否存在恶化
        has_deterioration = _tiered_agg("has_deterioration", "max")
        if has_deterioration is not None:
            # v8.0: 阶梯式衰退评分 (替代硬阈值0.5的二元判断)
            # 纯效率衰退: val≥1.0 → score=1.0 (不变)
            # 纯量价衰退: val=0.35 → score=0.54 (适度预警)
            # 双层衰退:   val=1.0 → score=1.0 (最强信号)
            det_score = min(1.0, has_deterioration / _EFF_W) if has_deterioration > 0.2 else 0.0
            w = weights.get("has_deterioration", 0.25)
            decay_score += w * det_score
            total_weight += w
            components["has_deterioration"] = has_deterioration

        # 2. 连续下跌年数
        consecutive = _tiered_agg("consecutive_decline_years", "max")
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
                    message=f"核心指标连续{int(consecutive)}年下跌",
                    source="delta_decay_factor",
                    values={"consecutive_years": consecutive},
                ))

        # 3. 总下跌百分比
        total_decline = _tiered_agg("total_decline_pct", "min")
        if total_decline is not None:
            # 下跌幅度 (负数表示下跌)
            decline_pct = abs(min(0, total_decline))  # total_decline 为百分数 (如 -30 表示跌30%)
            # v4.1.1 修复: severe_decline_threshold=0.30 (30%), 乘 100 转为百分数单位匹配
            decline_score = min(1.0, decline_pct / (conf.severe_decline_threshold * 100))
            w = weights.get("total_decline_pct", 0.20)
            decay_score += w * decline_score
            total_weight += w
            components["total_decline_pct"] = total_decline

        # 4. 恶化加速度 (已全指标聚合, 无需分层)
        det_accel = aggregate_feature(probes, "deterioration_acceleration", "max")
        if det_accel is not None and det_accel > 0:
            accel_score = min(1.0, det_accel / 0.1)
            w = weights.get("deterioration_acceleration", 0.15)
            decay_score += w * accel_score
            total_weight += w
            components["deterioration_acceleration"] = det_accel

        # 5. 负斜率惩罚
        log_slope = _tiered_agg("log_slope", "mean")
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

        # v4.7→v13.2: 渐进式绝对水平折扣 — 高基数轻微下降 ≠ 严重衰退
        # ROIC 从 32%→30% (还是世界级) 不应与 ROIC 从 8%→6% 获得相同惩罚
        # 原理: 高水平维持是竞争优势的体现, 微小波动是正常的
        #
        # v13.2 改进: 从二元阶梯 (0.50/0.70) 改为连续渐进式折扣
        # 旧版问题: ROIC 35%→21% 的骤降被 ×0.50 过度减免
        # 新版: discount = max(0.50, 1.0 - (ROIC - 15) × 0.025)
        #   ROIC=15%: ×1.00 (无折扣)
        #   ROIC=20%: ×0.875 (温和折扣)
        #   ROIC=25%: ×0.75
        #   ROIC=35%: ×0.50 (最大折扣, 仅真正的世界级公司)
        roic_level = get_feature(probes, "latest_value", "roic")
        if roic_level is not None and roic_level > 15.0 and score > 0.0:
            discount = max(0.50, 1.0 - (roic_level - 15.0) * 0.025)
            score *= discount
            score = max(0.0, min(1.0, score))

        # v5.0: 移除v4.9成长轨迹折扣 — 循环论证!
        # γ因子已使用revenue/profit CAGR评估成长性
        # δ_decay再用同一CAGR减免衰退 = 同一信号被两次利用抬高分数
        # 保留ROIC绝对水平折扣(使用ROIC水平而非增速, 不存在循环)

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
        # v4.8: 当 CAGR 因 OCF 极端波动 (cv>1, R²<0.2) 而为 NaN 时,
        #        回退到 log_slope 作为 CAGR 代理, 避免 V 因子空值
        ocf_cagr = get_feature(probes, "cagr", "ocf")
        if ocf_cagr is None:
            ocf_cagr = get_feature(probes, "cagr_approx", "ocf")
        if ocf_cagr is None:
            # fallback: log_slope ≈ CAGR 在小值范围内的合理近似
            ocf_cagr = get_feature(probes, "log_slope", "ocf")

        revenue_cagr = get_feature(probes, "cagr", "revenue")
        if revenue_cagr is None:
            revenue_cagr = get_feature(probes, "cagr_approx", "revenue")
        if revenue_cagr is None:
            revenue_cagr = get_feature(probes, "log_slope", "revenue")

        v_ratio_revenue = None
        if ocf_cagr is not None and revenue_cagr is not None:
            components["ocf_cagr"] = ocf_cagr
            components["revenue_cagr"] = revenue_cagr

            # 计算 V 比率 (CAGR 为小数: 5%=0.05)
            # v13.2 改进: 营收下降时保留连续信息 (替代固定 1.2/0.7)
            # 旧版: 营收负增长→固定值, 丢失 OCF 下降幅度的连续信号
            # 新版: 两者同为负数时直接比较 (OCF跌得少=好), 保留连续性
            if abs(revenue_cagr) < 0.01:
                # 营收几乎不变(<1%)，看 OCF 绝对值
                v_ratio_revenue = 1.0 if ocf_cagr >= 0 else 0.5
            elif revenue_cagr < 0:
                # 营收下降: OCF 抗跌程度的连续度量
                # ocf_cagr > revenue_cagr (跌得少或不跌) → 比率 > 1.0
                # ocf_cagr < revenue_cagr (跌得更惨) → 比率 < 1.0
                # clamp 到 [0.3, 1.5] 防止极端值
                if abs(revenue_cagr) > 0.005:
                    v_ratio_revenue = min(1.5, max(0.3, ocf_cagr / revenue_cagr))
                else:
                    v_ratio_revenue = 1.0 if ocf_cagr >= 0 else 0.5
            else:
                # 正常情况
                v_ratio_revenue = ocf_cagr / revenue_cagr

            # 归一化到 [0, 1]
            v_normalized = normalize_score(v_ratio_revenue, "minmax", 0.0, 1.5)
            w = weights.get("ocf_revenue_ratio", 0.55)  # v5.1: ↑ 0.50→0.55
            score += v_normalized * w  # v4.6: += 而非 = (与其他组件一致)
            total_weight += w
            components["v_ratio_revenue"] = v_ratio_revenue

        # 2. v5.1: 营收/利润一致性 (替代 ocf_profit_ratio 以消除与δ_fraud重叠)
        # OCF/利润背离已由δ_fraud.ocf_profit_divergence专门检测，
        # V因子改为检测 revenue growth vs profit growth 一致性 —— 不同的信号。
        profit_cagr = get_feature(probes, "cagr", "profit")
        if profit_cagr is None:
            profit_cagr = get_feature(probes, "cagr_approx", "profit")
        if profit_cagr is None:
            profit_cagr = get_feature(probes, "log_slope", "profit")

        if revenue_cagr is not None and profit_cagr is not None:
            components["profit_cagr"] = profit_cagr
            # 利润增速应跟随营收增速; 利润增速 > 营收增速 = 经营杠杆/效率提升 = 好
            # 利润增速 << 营收增速 = 成本失控 = 差
            if abs(revenue_cagr) < 0.01:
                rev_profit_consistency = 1.0 if profit_cagr >= 0 else 0.4
            elif revenue_cagr > 0:
                ratio = profit_cagr / revenue_cagr
                rev_profit_consistency = normalize_score(ratio, "minmax", 0.0, 2.0)
            else:
                # 营收下降: 利润下降更少 = 好
                rev_profit_consistency = 0.8 if profit_cagr > revenue_cagr else 0.3
            w = weights.get("rev_profit_consistency", 0.25)  # v5.1 new signal
            score += rev_profit_consistency * w
            total_weight += w
            components["rev_profit_consistency"] = rev_profit_consistency

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

                w = weights.get("sloan_accruals", 0.20)  # v5.1: ↑ 0.15→0.20
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

# ============================================================================
# 类型别名 (兼容性)
# ============================================================================

# TruthFactor 现在是一个协议类型别名，保持向后兼容
# 任何实现了 factor_id, evaluate(), explain() 的类都是有效因子
from typing import Union
TruthFactor = Union[AlphaFactor, BetaFactor, GammaFactor, PiFactor, LambdaFactor, DeltaFraudFactor, DeltaDecayFactor, VerificationFactor]


# ============================================================================
# 工厂函数
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
    "PiFactor",
    # 工厂函数
    # 辅助函数
    "normalize_score",
    "get_feature",
    "get_financial_context",
    "has_financial_context",
    "aggregate_feature",
]
