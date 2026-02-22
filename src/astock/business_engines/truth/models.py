"""T.R.U.T.H. 领域模型 - 专业级六维基因量化系统

设计理念：
    1. 语义化命名：每个枚举/类名直接表达业务含义
    2. 不可变数据：frozen dataclass 确保线程安全
    3. 完整输出：包含signal/grade/warnings/thresholds
    4. 物理隐喻：求解器输出阈值而非分数

版本: 3.2.0
日期: 2026-01-06
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Mapping, Optional, Sequence, Tuple, Any


# ============================================================================
# 六维因子标识 (语义化命名)
# ============================================================================

class FactorId(str, Enum):
    """七维基因因子标识

    T.R.U.T.H. 系统的核心：彻底去标签化，用数据驱动的七维基因描述公司特征

    属性基因（描述公司特质）:
        - ALPHA (α): 周期性 - 业绩对宏观经济的敏感弹性
        - BETA (β): 资本密度 - 赚取下一块钱利润所需的"重"度
        - GAMMA (γ): 成长动能 - 业务表面上的扩张加速度
        - LAMBDA (λ): 杠杆强度 - 偿债安全边际与资本结构健康度

    风险基因（识别隐患）:
        - DELTA_FRAUD (δ_fraud): 欺诈熵 - 财务报表的物理真实性 (熔断项)
        - DELTA_DECAY (δ_decay): 衰退熵 - 商业模式的恶化趋势 (惩罚项)

    验证基因（照妖镜）:
        - VERIFICATION (V): 真相验证 - 成长的含金量
    """
    ALPHA = "alpha"                   # α: 周期性
    BETA = "beta"                     # β: 资本密度
    GAMMA = "gamma"                   # γ: 成长动能
    LAMBDA = "lambda_leverage"        # λ: 杠杆强度 (v4.1 新增)
    DELTA_FRAUD = "delta_fraud"       # δ_fraud: 欺诈熵 (熔断项)
    DELTA_DECAY = "delta_decay"       # δ_decay: 衰退熵 (惩罚项)
    VERIFICATION = "verification"     # V: 真相验证

    @property
    def display_name(self) -> str:
        """返回中文显示名"""
        names = {
            "alpha": "α 周期性",
            "beta": "β 资本密度",
            "gamma": "γ 成长动能",
            "lambda_leverage": "λ 杠杆强度",
            "delta_fraud": "δ_fraud 欺诈熵",
            "delta_decay": "δ_decay 衰退熵",
            "verification": "V 验证因子",
        }
        return names.get(self.value, self.value)

    @property
    def category(self) -> str:
        """因子类别"""
        if self.value in ("alpha", "beta", "gamma", "lambda_leverage"):
            return "attribute"  # 属性基因
        elif self.value in ("delta_fraud", "delta_decay"):
            return "risk"  # 风险基因
        else:
            return "verification"  # 验证基因


class SolverId(str, Enum):
    """三大物理求解器标识

    物理隐喻：
        - GRAVITY: 重力求解器 - ROIC/ROE 必须克服"资金重力"
        - VELOCITY: 速度求解器 - 增长必须克服"GDP摩擦力"
        - STRUCTURE: 结构求解器 - 护城河对抗"熵增"

    每个求解器输出的是动态阈值，而非简单分数
    """
    GRAVITY = "gravity"       # 重力求解器: 输出 ROIC/ROE 动态阈值
    VELOCITY = "velocity"     # 速度求解器: 输出 增长率边界
    STRUCTURE = "structure"   # 结构求解器: 输出 护城河宽度

    @property
    def display_name(self) -> str:
        names = {
            "gravity": "🌍 重力求解器",
            "velocity": "🚀 速度求解器",
            "structure": "🧬 结构求解器",
        }
        return names.get(self.value, self.value)


class TruthSignal(str, Enum):
    """TRUTH 投资信号"""
    STRONG_BUY = "strong_buy"     # 强烈推荐
    BUY = "buy"                   # 推荐
    HOLD = "hold"                 # 持有/观望
    CAUTION = "caution"           # 谨慎
    SELL = "sell"                 # 回避
    FRAUD_ALERT = "fraud_alert"   # 欺诈预警 (熔断)

    @property
    def emoji(self) -> str:
        emojis = {
            "strong_buy": "🚀",
            "buy": "✅",
            "hold": "⏸️",
            "caution": "⚠️",
            "sell": "❌",
            "fraud_alert": "🚨",
        }
        return emojis.get(self.value, "")

    @property
    def score_range(self) -> Tuple[float, float]:
        """对应的分数区间"""
        ranges = {
            "strong_buy": (80, 100),
            "buy": (65, 80),
            "hold": (50, 65),
            "caution": (35, 50),
            "sell": (0, 35),
            "fraud_alert": (0, 0),
        }
        return ranges.get(self.value, (0, 100))


class TruthGrade(str, Enum):
    """TRUTH 综合评级"""
    A_PLUS = "A+"    # 顶级优质
    A = "A"          # 优质
    B_PLUS = "B+"    # 良好偏上
    B = "B"          # 良好
    C = "C"          # 一般
    D = "D"          # 较差
    F = "F"          # 不及格/熔断

    @property
    def score_range(self) -> Tuple[float, float]:
        """对应的分数区间"""
        ranges = {
            "A+": (90, 100),
            "A": (80, 90),
            "B+": (70, 80),
            "B": (60, 70),
            "C": (50, 60),
            "D": (40, 50),
            "F": (0, 40),
        }
        return ranges.get(self.value, (0, 100))


class WarningLevel(str, Enum):
    """预警级别"""
    INFO = "info"           # 信息提示
    WARNING = "warning"     # 警告
    CRITICAL = "critical"   # 严重
    FATAL = "fatal"         # 致命 (触发熔断)


# ============================================================================
# 输入数据结构
# ============================================================================

@dataclass(frozen=True)
class ProbeInput:
    """单只股票、单一指标的探针输入

    Attributes:
        ts_code: 股票代码
        probe_name: 标准化指标名 (roic/roe/revenue/ocf/financial_context/...)
        features: 探针特征字典 (计算得出的趋势/波动指标)

    Note:
        β/δ_fraud 因子所需的原始财务数据通过 probe_name="financial_context"
        的专用探针传递，其 features 包含 ratio_* 和 flag_* 字段
    """
    ts_code: str
    probe_name: str
    features: Mapping[str, float] = field(default_factory=dict)

    def get_feature(self, name: str, default: float = 0.0) -> float:
        """安全获取特征值"""
        return self.features.get(name, default)


# ============================================================================
# 因子输出结构
# ============================================================================

@dataclass(frozen=True)
class FactorResult:
    """单因子计算结果

    Attributes:
        factor_id: 因子标识
        ts_code: 股票代码
        score: 因子分数 [0, 1]
        confidence: 置信度 [0, 1]，基于数据质量和一致性
        components: 分项得分 (用于解释)
        details: 额外详情
    """
    factor_id: FactorId
    ts_code: str
    score: float
    confidence: float = 1.0
    components: Mapping[str, float] = field(default_factory=dict)
    details: Mapping[str, float] = field(default_factory=dict)

    @property
    def is_valid(self) -> bool:
        """是否为有效结果"""
        return 0 <= self.score <= 1 and self.confidence > 0

    @property
    def weighted_score(self) -> float:
        """置信度加权分数"""
        return self.score * self.confidence


# ============================================================================
# 求解器输出结构
# ============================================================================

@dataclass(frozen=True)
class DynamicThreshold:
    """动态阈值 - T.R.U.T.H. 核心创新

    阈值由六维基因决定，而非静态配置
    """
    name: str                        # 阈值名称
    value: float                     # 阈值值
    lower_bound: float = 0.0         # 置信区间下界
    upper_bound: float = 0.0         # 置信区间上界
    confidence: float = 0.85         # 置信度
    unit: str = "percent"            # 单位
    description: str = ""            # 描述
    actual_value: Optional[float] = None  # 实际观测值（用于 passed 判断）

    @property
    def passed(self) -> bool:
        """实际值是否达标（超过阈值）

        如果未设置 actual_value，默认返回 False。
        """
        if self.actual_value is None:
            return False
        return self.actual_value >= self.value


@dataclass(frozen=True)
class SolverResult:
    """求解器输出

    每个求解器输出一组动态阈值，而非简单分数
    """
    solver_id: SolverId
    ts_code: str

    # 动态阈值输出
    thresholds: Mapping[str, DynamicThreshold] = field(default_factory=dict)

    # 综合评分 (基于阈值通过情况)
    score: float = 0.5
    confidence: float = 1.0

    # 计算组件 (用于调试和解释)
    components: Mapping[str, float] = field(default_factory=dict)

    # 标签
    label: str = ""

    # 详细信息
    details: Mapping[str, Any] = field(default_factory=dict)

    @property
    def all_passed(self) -> bool:
        """所有阈值是否都通过"""
        if not self.thresholds:
            return False
        return all(t.passed for t in self.thresholds.values())

    @property
    def pass_rate(self) -> float:
        """阈值通过率"""
        if not self.thresholds:
            return 0.5
        return sum(1 for t in self.thresholds.values() if t.passed) / len(self.thresholds)


# ============================================================================
# 预警结构
# ============================================================================

@dataclass(frozen=True)
class TruthWarning:
    """TRUTH 预警"""
    code: str                        # 预警代码 (如 FRAUD_001)
    level: WarningLevel              # 预警级别
    title: str                       # 标题
    message: str                     # 详细信息
    source: str = ""                 # 来源 (factor/solver)
    metrics: Tuple[str, ...] = ()    # 相关指标
    values: Mapping[str, float] = field(default_factory=dict)

    @property
    def is_fatal(self) -> bool:
        """是否为致命预警"""
        return self.level == WarningLevel.FATAL


# ============================================================================
# 最终输出结构
# ============================================================================

@dataclass(frozen=True)
class TruthProfile:
    """单只股票的 TRUTH 完整画像

    这是 TRUTH 系统的最终输出，包含：
    1. 六维基因向量
    2. 三大求解器结果 (含动态阈值)
    3. 综合评分/评级/信号
    4. 预警列表
    """
    ts_code: str
    name: str = ""
    industry: str = ""

    # 因子和求解器
    factors: Mapping[FactorId, FactorResult] = field(default_factory=dict)
    solvers: Mapping[SolverId, SolverResult] = field(default_factory=dict)

    # 综合评估
    final_score: float = 0.0         # 综合得分 [0, 100]
    signal: TruthSignal = TruthSignal.HOLD
    grade: TruthGrade = TruthGrade.C

    # 预警
    warnings: Tuple[TruthWarning, ...] = ()

    # 元数据
    confidence: float = 1.0          # 整体置信度
    data_quality: str = "good"       # 数据质量评估

    @property
    def is_fraud_alert(self) -> bool:
        """是否触发欺诈熔断"""
        return self.signal == TruthSignal.FRAUD_ALERT

    @property
    def has_critical_warnings(self) -> bool:
        """是否有严重预警"""
        return any(w.level in (WarningLevel.CRITICAL, WarningLevel.FATAL)
                   for w in self.warnings)

    def get_factor(self, factor_id: FactorId) -> Optional[FactorResult]:
        """获取指定因子结果"""
        return self.factors.get(factor_id)

    def get_solver(self, solver_id: SolverId) -> Optional[SolverResult]:
        """获取指定求解器结果"""
        return self.solvers.get(solver_id)

    def get_threshold(self, metric: str) -> Optional[DynamicThreshold]:
        """获取指定指标的动态阈值"""
        for solver in self.solvers.values():
            if metric in solver.thresholds:
                return solver.thresholds[metric]
        return None

    def get_genome_vector(self) -> Dict[str, float]:
        """获取六维基因向量"""
        return {fid.value: fr.score for fid, fr in self.factors.items()}

    def get_all_thresholds(self) -> Dict[str, DynamicThreshold]:
        """获取所有动态阈值"""
        thresholds = {}
        for solver in self.solvers.values():
            thresholds.update(solver.thresholds)
        return thresholds


@dataclass(frozen=True)
class TruthRunResult:
    """TRUTH 批量运行结果"""
    profiles: Sequence[TruthProfile] = ()
    algo_version: str = "3.3.0"

    def __len__(self) -> int:
        return len(self.profiles)

    def get_profile(self, ts_code: str) -> Optional[TruthProfile]:
        for p in self.profiles:
            if p.ts_code == ts_code:
                return p
        return None

    def filter_by_signal(self, signal: TruthSignal) -> List[TruthProfile]:
        return [p for p in self.profiles if p.signal == signal]

    @property
    def fraud_alerts(self) -> List[TruthProfile]:
        return [p for p in self.profiles if p.is_fraud_alert]


# ============================================================================
# 导出
# ============================================================================

__all__ = [
    "FactorId",
    "SolverId",
    "TruthSignal",
    "TruthGrade",
    "WarningLevel",
    "ProbeInput",
    "FactorResult",
    "DynamicThreshold",
    "SolverResult",
    "TruthWarning",
    "TruthProfile",
    "TruthRunResult",
]
