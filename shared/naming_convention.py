"""
==============================================================================
AStock-Analysis 统一命名规范系统 (Unified Naming Convention System)
==============================================================================

版本: 1.0.0
创建日期: 2025-12-25
作者: AStock Team

设计原则:
---------
1. 单一真相源 (Single Source of Truth)
   - 所有指标配置集中定义，全局共享
   - 消除多处重复定义导致的不同步问题

2. 三层命名体系 (Three-Layer Naming)
   - 业务层 (business_key): 面向用户的友好名称，如 'roic', 'revenue'
   - 数据层 (source_column): 原始数据列名，如 'roic', 'total_revenue_ps'
   - 输出层 (output_prefix): 分析结果列名前缀，如 'roic', 'total_revenue_ps'

3. 列名格式规范 (Column Naming Format)
   - 原始数据: {source_column}
   - 分析结果: {output_prefix}_{field_name}
   - 示例: roic_slope, roic_cagr, roic_cv

使用方式:
---------
from shared.naming_convention import MetricRegistry, FieldRegistry, ColumnBuilder

# 获取指标配置
metric = MetricRegistry.get('roic')
print(metric.source_column)  # 'roic'
print(metric.display_name)   # 'ROIC'

# 构建列名
col = ColumnBuilder.analysis_column('roic', 'slope')  # 'roic_slope'
col = ColumnBuilder.analysis_column('revenue', 'cagr')  # 'total_revenue_ps_cagr'

# 获取字段配置
field = FieldRegistry.get('slope')
print(field.description)  # '线性回归斜率'
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from enum import Enum
import re


# ==============================================================================
# 1. 枚举定义
# ==============================================================================

class MetricCategory(Enum):
    """指标分类"""
    EFFICIENCY = "efficiency"      # 效率指标 (ROIC, ROE, ROIIC)
    PROFITABILITY = "profitability"  # 盈利指标 (毛利率, 净利率)
    GROWTH = "growth"              # 增长指标 (营收, 利润)
    CASHFLOW = "cashflow"          # 现金流指标 (OCF)
    DERIVED = "derived"            # 派生指标 (ROIIC)


class DataStage(Enum):
    """数据阶段"""
    RAW = "raw"                    # 原始数据 (从 Tushare 获取)
    INTERMEDIATE = "intermediate"  # 中间数据 (趋势分析结果)
    FINAL = "final"               # 最终数据 (报告输出)


class FieldCategory(Enum):
    """字段分类"""
    CORE_TREND = "core_trend"          # 核心趋势 (slope, r_squared)
    VOLATILITY = "volatility"          # 波动性 (cv, std_dev)
    DETERIORATION = "deterioration"    # 恶化检测
    INFLECTION = "inflection"          # 拐点检测
    CYCLICAL = "cyclical"              # 周期性
    ROLLING = "rolling"                # 滚动分析
    ROBUST = "robust"                  # 稳健统计
    MULTI_HORIZON = "multi_horizon"    # 多时间窗口
    VALUE = "value"                    # 数值统计
    METADATA = "metadata"              # 元数据


# ==============================================================================
# 2. 数据类定义
# ==============================================================================

@dataclass(frozen=True)
class MetricConfig:
    """
    指标配置 (不可变)

    Attributes:
        business_key: 业务层键名，用于代码引用和 workflow 配置
        source_column: 数据层列名，原始数据中的实际列名
        output_prefix: 输出层前缀，分析结果列名的前缀
        display_name: 显示名称，用于报告和 UI
        category: 指标分类
        description: 描述说明
        reference_metrics: 交叉验证参考指标
        is_derived: 是否为派生指标
    """
    business_key: str
    source_column: str
    output_prefix: str
    display_name: str
    category: MetricCategory
    description: str = ""
    reference_metrics: tuple = field(default_factory=tuple)
    is_derived: bool = False

    def __post_init__(self):
        """验证配置有效性"""
        if not self.business_key or not self.source_column:
            raise ValueError(f"business_key 和 source_column 不能为空")


@dataclass(frozen=True)
class FieldConfig:
    """
    字段配置 (不可变)

    Attributes:
        name: 字段名 (不含前缀)
        category: 字段分类
        data_type: 数据类型
        description: 描述说明
        default_value: 默认值
        nullable: 是否可为空
    """
    name: str
    category: FieldCategory
    data_type: str  # 'float', 'int', 'str', 'bool'
    description: str = ""
    default_value: Any = None
    nullable: bool = True


# ==============================================================================
# 3. 指标注册表 (Metric Registry)
# ==============================================================================

class MetricRegistry:
    """
    指标注册表 - 单一真相源

    所有指标配置的中央管理器，提供统一的指标查询和映射功能。

    Usage:
        >>> metric = MetricRegistry.get('roic')
        >>> metric.source_column  # 'roic'
        >>> metric.display_name   # 'ROIC'

        >>> MetricRegistry.get_source_column('revenue')  # 'total_revenue_ps'
        >>> MetricRegistry.get_output_prefix('gross_margin')  # 'grossprofit_margin'
    """

    # =========================================================================
    # 别名映射: 步骤名/非标准命名 -> 标准 business_key
    # 将分散的硬编码别名统一管理，实现真正的零硬编码
    # =========================================================================
    _ALIASES: Dict[str, str] = {
        # 步骤名中可能出现的非标准命名
        'grossmargin': 'gross_margin',
        'netmargin': 'net_margin',
        'gross': 'gross_margin',
        'net': 'net_margin',
        # 常见缩写
        'gm': 'gross_margin',
        'nm': 'net_margin',
        'rev': 'revenue',
        # 可能的拼写变体
        'gross_profit_margin': 'gross_margin',
        'net_profit_margin': 'net_margin',
    }

    # =========================================================================
    # 核心配置: 所有指标的统一定义
    # =========================================================================
    _METRICS: Dict[str, MetricConfig] = {
        # ----- 效率指标 -----
        "roic": MetricConfig(
            business_key="roic",
            source_column="roic",
            output_prefix="roic",
            display_name="ROIC",
            category=MetricCategory.EFFICIENCY,
            description="投入资本回报率，核心效率指标",
            reference_metrics=("roe", "roiic"),
        ),
        "roe": MetricConfig(
            business_key="roe",
            source_column="roe",
            output_prefix="roe",
            display_name="ROE",
            category=MetricCategory.EFFICIENCY,
            description="股东权益回报率",
            reference_metrics=("netprofit_margin", "roic"),
        ),
        "roiic": MetricConfig(
            business_key="roiic",
            source_column="roiic",
            output_prefix="roiic",
            display_name="ROIIC",
            category=MetricCategory.EFFICIENCY,
            description="增量投入资本回报率",
            reference_metrics=("roic",),
            is_derived=True,
        ),

        # ----- 盈利指标 -----
        "gross_margin": MetricConfig(
            business_key="gross_margin",
            source_column="grossprofit_margin",
            output_prefix="grossprofit_margin",
            display_name="毛利率",
            category=MetricCategory.PROFITABILITY,
            description="毛利率，护城河指标",
            reference_metrics=("netprofit_margin",),
        ),
        "net_margin": MetricConfig(
            business_key="net_margin",
            source_column="netprofit_margin",
            output_prefix="netprofit_margin",
            display_name="净利率",
            category=MetricCategory.PROFITABILITY,
            description="净利率，综合盈利能力",
            reference_metrics=("grossprofit_margin", "ocfps"),
        ),

        # ----- 增长指标 -----
        "revenue": MetricConfig(
            business_key="revenue",
            source_column="total_revenue_ps",
            output_prefix="total_revenue_ps",
            display_name="营收",
            category=MetricCategory.GROWTH,
            description="每股营业收入",
            reference_metrics=("roe", "eps"),
        ),
        "profit": MetricConfig(
            business_key="profit",
            source_column="eps",
            output_prefix="eps",
            display_name="利润",
            category=MetricCategory.GROWTH,
            description="每股收益",
            reference_metrics=("ocfps",),
        ),

        # ----- 现金流指标 -----
        "ocf": MetricConfig(
            business_key="ocf",
            source_column="ocfps",
            output_prefix="ocfps",
            display_name="经营现金流",
            category=MetricCategory.CASHFLOW,
            description="每股经营现金流",
            reference_metrics=("eps",),
        ),
    }

    # =========================================================================
    # 类方法
    # =========================================================================

    @classmethod
    def get(cls, business_key: str) -> MetricConfig:
        """
        获取指标配置

        Args:
            business_key: 业务层键名 (如 'roic', 'revenue')

        Returns:
            MetricConfig 对象

        Raises:
            KeyError: 如果指标不存在
        """
        if business_key not in cls._METRICS:
            raise KeyError(
                f"未知指标: '{business_key}'。"
                f"可用指标: {list(cls._METRICS.keys())}"
            )
        return cls._METRICS[business_key]

    @classmethod
    def get_source_column(cls, business_key: str) -> str:
        """获取原始数据列名"""
        return cls.get(business_key).source_column

    @classmethod
    def get_output_prefix(cls, business_key: str) -> str:
        """获取输出列名前缀"""
        return cls.get(business_key).output_prefix

    @classmethod
    def get_display_name(cls, business_key: str) -> str:
        """获取显示名称"""
        return cls.get(business_key).display_name

    @classmethod
    def all_keys(cls) -> List[str]:
        """获取所有业务键名"""
        return list(cls._METRICS.keys())

    @classmethod
    def all_metrics(cls) -> List[MetricConfig]:
        """获取所有指标配置"""
        return list(cls._METRICS.values())

    @classmethod
    def by_category(cls, category: MetricCategory) -> List[MetricConfig]:
        """按分类获取指标"""
        return [m for m in cls._METRICS.values() if m.category == category]

    @classmethod
    def source_to_business(cls, source_column: str) -> Optional[str]:
        """从原始列名反查业务键名"""
        for key, metric in cls._METRICS.items():
            if metric.source_column == source_column:
                return key
        return None

    @classmethod
    def prefix_to_business(cls, output_prefix: str) -> Optional[str]:
        """从输出前缀反查业务键名"""
        for key, metric in cls._METRICS.items():
            if metric.output_prefix == output_prefix:
                return key
        return None

    # =========================================================================
    # 🌟 核心方法: 智能解析 (统一入口)
    # =========================================================================

    @classmethod
    def resolve(cls, identifier: str) -> MetricConfig:
        """
        智能解析任意指标标识符，统一返回 MetricConfig

        这是推荐的统一入口方法，支持多种输入格式：
        - business_key: 'revenue', 'gross_margin', 'ocf'
        - alias: 'grossmargin', 'netmargin', 'gm' (别名)
        - source_column: 'total_revenue_ps', 'grossprofit_margin', 'ocfps'
        - display_name: '营收', '毛利率', '经营现金流'

        Args:
            identifier: 任意格式的指标标识符

        Returns:
            MetricConfig 对象

        Raises:
            ValueError: 无法识别的标识符

        Usage:
            >>> config = MetricRegistry.resolve('revenue')
            >>> config = MetricRegistry.resolve('total_revenue_ps')  # 也能解析
            >>> config = MetricRegistry.resolve('grossmargin')  # 别名也支持
            >>> config.business_key  # 'gross_margin'
        """
        # 0. 标准化: 转小写并去除空格
        normalized = identifier.lower().strip()

        # 1. 尝试 business_key (最优先)
        if normalized in cls._METRICS:
            return cls._METRICS[normalized]

        # 2. 🆕 尝试别名映射 (集中管理的别名)
        if normalized in cls._ALIASES:
            canonical_key = cls._ALIASES[normalized]
            return cls._METRICS[canonical_key]

        # 3. 尝试 source_column 反向查找
        for key, config in cls._METRICS.items():
            if config.source_column.lower() == normalized:
                return config

        # 4. 尝试 output_prefix 反向查找 (通常与 source_column 相同)
        for key, config in cls._METRICS.items():
            if config.output_prefix.lower() == normalized:
                return config

        # 5. 尝试 display_name 反向查找
        for key, config in cls._METRICS.items():
            if config.display_name == identifier:  # display_name 保持原始大小写
                return config

        # 6. 无法识别，抛出详细错误
        available_keys = list(cls._METRICS.keys())
        available_aliases = list(cls._ALIASES.keys())
        available_sources = [c.source_column for c in cls._METRICS.values()]
        available_displays = [c.display_name for c in cls._METRICS.values()]

        raise ValueError(
            f"无法识别的指标标识符: '{identifier}'\n"
            f"  可用的 business_key: {available_keys}\n"
            f"  可用的 alias: {available_aliases}\n"
            f"  可用的 source_column: {available_sources}\n"
            f"  可用的 display_name: {available_displays}"
        )

    @classmethod
    def resolve_alias(cls, alias: str) -> Optional[str]:
        """
        解析别名到标准 business_key

        Args:
            alias: 别名 (如 'grossmargin', 'netmargin')

        Returns:
            标准的 business_key，如果不是别名则返回 None
        """
        normalized = alias.lower().strip()
        return cls._ALIASES.get(normalized)

    @classmethod
    def get_all_aliases(cls) -> Dict[str, str]:
        """
        获取所有别名映射

        Returns:
            {alias: business_key, ...}
        """
        return dict(cls._ALIASES)

    @classmethod
    def resolve_safe(cls, identifier: str) -> Optional[MetricConfig]:
        """
        安全版本的 resolve，无法识别时返回 None 而非抛出异常

        适用于需要容错处理的场景
        """
        try:
            return cls.resolve(identifier)
        except ValueError:
            return None

    @classmethod
    def validate_metric_name(cls, metric_name: str) -> tuple:
        """
        验证 metric_name 是否规范，并给出建议

        Args:
            metric_name: YAML 配置中的 metric_name 值

        Returns:
            (is_valid, is_recommended, message, suggested_key)
            - is_valid: 是否可以解析
            - is_recommended: 是否使用了推荐的 business_key
            - message: 验证消息
            - suggested_key: 建议使用的 business_key (如果需要改进)

        Usage:
            >>> valid, recommended, msg, suggestion = MetricRegistry.validate_metric_name('total_revenue_ps')
            >>> print(msg)  # "⚠️ 可用但建议使用 business_key 'revenue'"
            >>> print(suggestion)  # 'revenue'
        """
        try:
            config = cls.resolve(metric_name)

            if metric_name == config.business_key:
                return (True, True, f"✅ 规范: 使用 business_key '{metric_name}'", None)
            else:
                return (
                    True,
                    False,
                    f"⚠️ 可用但建议使用 business_key '{config.business_key}' 替代 '{metric_name}'",
                    config.business_key
                )
        except ValueError as e:
            return (False, False, f"❌ 无效: {str(e)}", None)

    @classmethod
    def get_canonical_name(cls, identifier: str) -> str:
        """
        获取标准化的 business_key

        无论输入什么格式，都返回标准的 business_key

        Args:
            identifier: 任意格式的指标标识符

        Returns:
            标准的 business_key

        Usage:
            >>> MetricRegistry.get_canonical_name('total_revenue_ps')  # 'revenue'
            >>> MetricRegistry.get_canonical_name('revenue')  # 'revenue'
            >>> MetricRegistry.get_canonical_name('营收')  # 'revenue'
        """
        return cls.resolve(identifier).business_key

    @classmethod
    def is_valid_identifier(cls, identifier: str) -> bool:
        """检查标识符是否有效"""
        return cls.resolve_safe(identifier) is not None

    @classmethod
    def list_all_identifiers(cls) -> dict:
        """
        列出所有可用的标识符映射

        Returns:
            {
                'business_keys': [...],
                'source_columns': [...],
                'output_prefixes': [...],
                'display_names': [...]
            }
        """
        return {
            'business_keys': [c.business_key for c in cls._METRICS.values()],
            'source_columns': [c.source_column for c in cls._METRICS.values()],
            'output_prefixes': [c.output_prefix for c in cls._METRICS.values()],
            'display_names': [c.display_name for c in cls._METRICS.values()],
        }

    @classmethod
    def prefix_to_business_old(cls, output_prefix: str) -> Optional[str]:
        """从输出前缀反查业务键名 (旧版兼容)"""
        for key, metric in cls._METRICS.items():
            if metric.output_prefix == output_prefix:
                return key
        return None

    @classmethod
    def get_reference_metrics(cls, business_key: str) -> List[str]:
        """获取交叉验证参考指标"""
        return list(cls.get(business_key).reference_metrics)

    @classmethod
    def validate_source_columns(cls, df_columns: List[str]) -> Dict[str, bool]:
        """验证 DataFrame 是否包含必要的原始数据列"""
        result = {}
        for key, metric in cls._METRICS.items():
            if not metric.is_derived:
                result[key] = metric.source_column in df_columns
        return result


# ==============================================================================
# 4. 字段注册表 (Field Registry)
# ==============================================================================

class FieldRegistry:
    """
    字段注册表 - 分析结果字段的统一定义

    定义所有趋势分析输出字段的名称、类型和描述。

    Usage:
        >>> field = FieldRegistry.get('slope')
        >>> field.description  # '线性回归斜率'
        >>> field.data_type    # 'float'

        >>> FieldRegistry.get_fields_by_category(FieldCategory.CORE_TREND)
    """

    _FIELDS: Dict[str, FieldConfig] = {
        # ----- 核心趋势字段 -----
        "slope": FieldConfig("slope", FieldCategory.CORE_TREND, "float", "线性回归斜率"),
        "log_slope": FieldConfig("log_slope", FieldCategory.CORE_TREND, "float", "对数回归斜率"),
        "r_squared": FieldConfig("r_squared", FieldCategory.CORE_TREND, "float", "R²决定系数", 0.0),
        "p_value": FieldConfig("p_value", FieldCategory.CORE_TREND, "float", "P值显著性", 1.0),
        "cagr": FieldConfig("cagr", FieldCategory.CORE_TREND, "float", "复合年均增长率"),
        "trend_direction": FieldConfig("trend_direction", FieldCategory.CORE_TREND, "str", "趋势方向", "flat"),

        # ----- 波动性字段 -----
        "cv": FieldConfig("cv", FieldCategory.VOLATILITY, "float", "变异系数", 0.0),
        "std_dev": FieldConfig("std_dev", FieldCategory.VOLATILITY, "float", "标准差", 0.0),
        "volatility_type": FieldConfig("volatility_type", FieldCategory.VOLATILITY, "str", "波动类型", "stable"),
        "volatility_regime": FieldConfig("volatility_regime", FieldCategory.VOLATILITY, "str", "波动体制", "normal"),

        # ----- 恶化检测字段 -----
        "has_deterioration": FieldConfig("has_deterioration", FieldCategory.DETERIORATION, "bool", "是否存在恶化", False),
        "deterioration_severity": FieldConfig("deterioration_severity", FieldCategory.DETERIORATION, "str", "恶化严重程度", "none"),
        "total_decline_pct": FieldConfig("total_decline_pct", FieldCategory.DETERIORATION, "float", "总下降百分比", 0.0),

        # ----- 拐点检测字段 -----
        "has_inflection": FieldConfig("has_inflection", FieldCategory.INFLECTION, "bool", "是否存在拐点", False),
        "inflection_type": FieldConfig("inflection_type", FieldCategory.INFLECTION, "str", "拐点类型", "none"),

        # ----- 周期性字段 -----
        "is_cyclical": FieldConfig("is_cyclical", FieldCategory.CYCLICAL, "bool", "是否周期性", False),
        "current_phase": FieldConfig("current_phase", FieldCategory.CYCLICAL, "str", "当前周期阶段", "unknown"),
        "cycle_position": FieldConfig("cycle_position", FieldCategory.CYCLICAL, "float", "周期位置", 0.5),

        # ----- 滚动分析字段 -----
        "is_accelerating": FieldConfig("is_accelerating", FieldCategory.ROLLING, "bool", "是否加速", False),
        "is_decelerating": FieldConfig("is_decelerating", FieldCategory.ROLLING, "bool", "是否减速", False),
        "recent_3y_slope": FieldConfig("recent_3y_slope", FieldCategory.ROLLING, "float", "近3年斜率"),

        # ----- 稳健统计字段 -----
        "robust_slope": FieldConfig("robust_slope", FieldCategory.ROBUST, "float", "稳健斜率(Theil-Sen)"),
        "mk_tau": FieldConfig("mk_tau", FieldCategory.ROBUST, "float", "Mann-Kendall tau"),
        "mk_p_value": FieldConfig("mk_p_value", FieldCategory.ROBUST, "float", "Mann-Kendall P值"),

        # ----- 数值统计字段 -----
        "weighted_avg": FieldConfig("weighted_avg", FieldCategory.VALUE, "float", "加权平均值"),
        "latest_value": FieldConfig("latest_value", FieldCategory.VALUE, "float", "最新值"),
        "latest_vs_weighted_ratio": FieldConfig("latest_vs_weighted_ratio", FieldCategory.VALUE, "float", "最新值/加权比"),

        # ----- 多时间窗口字段 -----
        "full_data_years": FieldConfig("full_data_years", FieldCategory.MULTI_HORIZON, "int", "完整数据年数"),
        "trend_window_years": FieldConfig("trend_window_years", FieldCategory.MULTI_HORIZON, "int", "趋势窗口年数"),
        "has_structural_break": FieldConfig("has_structural_break", FieldCategory.MULTI_HORIZON, "bool", "是否有结构断点", False),
        "break_year_index": FieldConfig("break_year_index", FieldCategory.MULTI_HORIZON, "int", "断点年份索引"),
        "data_regime": FieldConfig("data_regime", FieldCategory.MULTI_HORIZON, "str", "数据体制", "stable"),

        # ----- 元数据字段 -----
        "metric_name": FieldConfig("metric_name", FieldCategory.METADATA, "str", "指标名称"),
    }

    @classmethod
    def get(cls, field_name: str) -> FieldConfig:
        """获取字段配置"""
        if field_name not in cls._FIELDS:
            raise KeyError(f"未知字段: '{field_name}'")
        return cls._FIELDS[field_name]

    @classmethod
    def all_names(cls) -> List[str]:
        """获取所有字段名"""
        return list(cls._FIELDS.keys())

    @classmethod
    def by_category(cls, category: FieldCategory) -> List[FieldConfig]:
        """按分类获取字段"""
        return [f for f in cls._FIELDS.values() if f.category == category]

    @classmethod
    def get_default_value(cls, field_name: str) -> Any:
        """获取字段默认值"""
        return cls.get(field_name).default_value


# ==============================================================================
# 5. 列名构建器 (Column Builder)
# ==============================================================================

class ColumnBuilder:
    """
    列名构建器 - 统一的列名生成工具

    确保整个系统使用一致的列名格式。

    Usage:
        >>> ColumnBuilder.analysis_column('roic', 'slope')
        'roic_slope'

        >>> ColumnBuilder.analysis_column('revenue', 'cagr')
        'total_revenue_ps_cagr'

        >>> ColumnBuilder.parse_column('roic_slope')
        ('roic', 'slope')
    """

    SEPARATOR = "_"

    @classmethod
    def analysis_column(
        cls,
        metric_key: str,
        field_name: str,
        suffix: str = ""
    ) -> str:
        """
        构建分析结果列名

        Args:
            metric_key: 业务层指标键名 (如 'roic', 'revenue')
            field_name: 字段名 (如 'slope', 'cagr')
            suffix: 可选后缀

        Returns:
            完整列名，格式: {output_prefix}_{field_name}{suffix}
        """
        prefix = MetricRegistry.get_output_prefix(metric_key)
        col = f"{prefix}{cls.SEPARATOR}{field_name}"
        if suffix:
            col += suffix
        return col

    @classmethod
    def batch_columns(
        cls,
        metric_key: str,
        field_names: List[str]
    ) -> List[str]:
        """批量构建列名"""
        return [cls.analysis_column(metric_key, f) for f in field_names]

    @classmethod
    def all_analysis_columns(cls, metric_key: str) -> List[str]:
        """获取某指标的所有分析列名"""
        return cls.batch_columns(metric_key, FieldRegistry.all_names())

    @classmethod
    def parse_column(cls, column_name: str) -> Optional[tuple]:
        """
        解析列名，提取前缀和字段名

        Args:
            column_name: 列名 (如 'roic_slope')

        Returns:
            (业务键名, 字段名) 或 None
        """
        for key in MetricRegistry.all_keys():
            prefix = MetricRegistry.get_output_prefix(key)
            pattern = f"^{re.escape(prefix)}{cls.SEPARATOR}(.+)$"
            match = re.match(pattern, column_name)
            if match:
                return (key, match.group(1))
        return None

    @classmethod
    def validate_column(cls, column_name: str) -> bool:
        """验证列名是否符合规范"""
        parsed = cls.parse_column(column_name)
        if parsed is None:
            return False
        _, field_name = parsed
        return field_name in FieldRegistry.all_names()


# ==============================================================================
# 6. 数据路径规范
# ==============================================================================

class PathConvention:
    """
    路径命名规范

    定义数据文件的存储路径和命名规则。
    """

    # 基础目录
    DATA_ROOT = "data"

    # 子目录
    RAW_DIR = "polars"           # 原始整合数据
    INTERMEDIATE_DIR = "filter_middle"  # 中间分析结果
    OUTPUT_DIR = "."             # 最终输出 (data/ 根目录)

    # 文件命名模板
    TREND_ANALYSIS_TEMPLATE = "{metric}_trend_analysis.csv"
    COMPREHENSIVE_REPORT = "comprehensive_analysis_report.md"
    TRUTH_REPORT_MD = "truth_analysis_report.md"
    TRUTH_REPORT_CSV = "truth_analysis_report.csv"
    TRUTH_REPORT_JSON = "truth_analysis_report.json"

    @classmethod
    def raw_data_path(cls, duration_years: int = 10) -> str:
        """获取原始数据路径"""
        return f"{cls.DATA_ROOT}/{cls.RAW_DIR}/{duration_years}yd_final_industry.csv"

    @classmethod
    def trend_analysis_path(cls, metric_key: str) -> str:
        """获取趋势分析结果路径"""
        # 使用业务键名作为文件名 (更友好)
        filename = cls.TREND_ANALYSIS_TEMPLATE.format(metric=metric_key)
        return f"{cls.DATA_ROOT}/{cls.INTERMEDIATE_DIR}/{filename}"

    @classmethod
    def comprehensive_report_path(cls) -> str:
        """获取综合报告路径"""
        return f"{cls.DATA_ROOT}/{cls.COMPREHENSIVE_REPORT}"

    @classmethod
    def truth_report_path(cls, format: str = "md") -> str:
        """获取 T.R.U.T.H. 报告路径"""
        templates = {
            "md": cls.TRUTH_REPORT_MD,
            "csv": cls.TRUTH_REPORT_CSV,
            "json": cls.TRUTH_REPORT_JSON,
        }
        return f"{cls.DATA_ROOT}/{templates.get(format, cls.TRUTH_REPORT_MD)}"


# ==============================================================================
# 7. 便捷访问接口
# ==============================================================================

# 常用映射的快捷访问
METRIC_SOURCE_MAP = {
    key: MetricRegistry.get_source_column(key)
    for key in MetricRegistry.all_keys()
}

METRIC_PREFIX_MAP = {
    key: MetricRegistry.get_output_prefix(key)
    for key in MetricRegistry.all_keys()
}

METRIC_DISPLAY_MAP = {
    key: MetricRegistry.get_display_name(key)
    for key in MetricRegistry.all_keys()
}

# 反向映射
SOURCE_TO_BUSINESS_MAP = {
    v: k for k, v in METRIC_SOURCE_MAP.items()
}

PREFIX_TO_BUSINESS_MAP = {
    v: k for k, v in METRIC_PREFIX_MAP.items()
}


# ==============================================================================
# 8. 兼容性适配器
# ==============================================================================

class LegacyAdapter:
    """
    兼容性适配器

    用于适配旧代码中的命名方式，逐步迁移到新规范。
    """

    # 旧字段名 -> 新字段名 映射
    FIELD_ALIASES = {
        "cagr_approx": "cagr",       # 旧版使用 cagr_approx
        "mann_kendall_tau": "mk_tau",
        "mann_kendall_p_value": "mk_p_value",
    }

    @classmethod
    def normalize_field_name(cls, field_name: str) -> str:
        """将旧字段名转换为新字段名"""
        return cls.FIELD_ALIASES.get(field_name, field_name)

    @classmethod
    def normalize_column_name(cls, column_name: str) -> str:
        """规范化列名"""
        parsed = ColumnBuilder.parse_column(column_name)
        if parsed:
            metric_key, field_name = parsed
            normalized_field = cls.normalize_field_name(field_name)
            return ColumnBuilder.analysis_column(metric_key, normalized_field)
        return column_name


# ==============================================================================
# 9. 导出
# ==============================================================================

__all__ = [
    # 枚举
    "MetricCategory",
    "DataStage",
    "FieldCategory",

    # 数据类
    "MetricConfig",
    "FieldConfig",

    # 注册表
    "MetricRegistry",
    "FieldRegistry",

    # 工具类
    "ColumnBuilder",
    "PathConvention",
    "LegacyAdapter",

    # 快捷映射
    "METRIC_SOURCE_MAP",
    "METRIC_PREFIX_MAP",
    "METRIC_DISPLAY_MAP",
    "SOURCE_TO_BUSINESS_MAP",
    "PREFIX_TO_BUSINESS_MAP",
]


# ==============================================================================
# 10. 自检与验证
# ==============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("AStock-Analysis 命名规范系统自检 (增强版)")
    print("=" * 70)

    print("\n📊 指标配置:")
    for key in MetricRegistry.all_keys():
        metric = MetricRegistry.get(key)
        print(f"  {key:15} -> source: {metric.source_column:20} prefix: {metric.output_prefix}")

    print("\n📋 字段配置 (核心趋势):")
    for field in FieldRegistry.by_category(FieldCategory.CORE_TREND):
        print(f"  {field.name:20} ({field.data_type:6}) - {field.description}")

    print("\n🔧 列名构建示例:")
    examples = [
        ("roic", "slope"),
        ("roic", "cagr"),
        ("revenue", "cagr"),
        ("gross_margin", "cv"),
    ]
    for metric, field in examples:
        col = ColumnBuilder.analysis_column(metric, field)
        print(f"  {metric}/{field} -> {col}")

    print("\n📁 路径规范:")
    print(f"  原始数据: {PathConvention.raw_data_path()}")
    print(f"  ROIC分析: {PathConvention.trend_analysis_path('roic')}")
    print(f"  综合报告: {PathConvention.comprehensive_report_path()}")

    print("\n" + "=" * 70)
    print("🌟 智能解析 (MetricRegistry.resolve) 测试")
    print("=" * 70)

    test_cases = [
        # (输入, 预期 business_key)
        ("roic", "roic"),
        ("revenue", "revenue"),
        ("total_revenue_ps", "revenue"),     # source_column 反向解析
        ("grossprofit_margin", "gross_margin"),  # source_column 反向解析
        ("eps", "profit"),                   # source_column 反向解析
        ("ocfps", "ocf"),                    # source_column 反向解析
        ("营收", "revenue"),                  # display_name 反向解析
        ("毛利率", "gross_margin"),           # display_name 反向解析
    ]

    for identifier, expected in test_cases:
        try:
            config = MetricRegistry.resolve(identifier)
            status = "✅" if config.business_key == expected else "❌"
            print(f"  {status} resolve('{identifier}') -> business_key='{config.business_key}'")
        except ValueError as e:
            print(f"  ❌ resolve('{identifier}') -> ERROR: {e}")

    print("\n" + "=" * 70)
    print("🔍 YAML metric_name 验证测试")
    print("=" * 70)

    yaml_test_cases = [
        "roic",               # ✅ 标准 business_key
        "revenue",            # ✅ 标准 business_key
        "total_revenue_ps",   # ⚠️ 可用但应使用 business_key
        "grossprofit_margin", # ⚠️ 可用但应使用 business_key
        "invalid_metric",     # ❌ 无效
    ]

    for metric_name in yaml_test_cases:
        is_valid, is_recommended, msg, suggestion = MetricRegistry.validate_metric_name(metric_name)
        print(f"  {msg}")
        if suggestion:
            print(f"      → 建议改为: metric_name: '{suggestion}'")

    print("\n" + "=" * 70)
    print("📋 标准化名称获取测试")
    print("=" * 70)

    canonical_tests = [
        "total_revenue_ps",
        "grossprofit_margin",
        "netprofit_margin",
        "eps",
        "ocfps",
    ]

    for identifier in canonical_tests:
        canonical = MetricRegistry.get_canonical_name(identifier)
        print(f"  get_canonical_name('{identifier}') -> '{canonical}'")
