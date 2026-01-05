"""
PGCS 专业演示 - 展示框架的通用性和扩展性

这个演示展示了：
1. PGCS 框架本身是干净、通用的 - 没有任何业务逻辑
2. 用户可以在自己的业务层定义自己的领域概念
3. 框架只提供抽象和机制，不提供策略
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from typing import Literal
from enum import Enum, auto
from shared.contracts import (
    # 核心 - 干净的抽象
    Field, Schema,
    # 验证 - 通用机制
    required, range_check, custom,
    # 注册 - 通用服务
    get_registry, CompatibilityMode,
    # 路由 - 参数化解析
    Router, RouteParser, DelimiterParser,
    # 元数据 - 通用存储
    Metadata, Lineage,
    # 工具
    fingerprint,
)

print("="*60)
print("PGCS 专业演示 - 用户自定义业务概念")
print("="*60)

# ============================================================
# 用户定义自己的业务概念（框架完全不知道这些）
# ============================================================

# 用户定义的基因类型（框架不 hardcode 这个）
class GeneTarget(Enum):
    """用户在业务层定义的基因目标 - 框架完全不知道这个"""
    ALPHA = auto()   # 增长能力基因
    BETA = auto()    # 稳定性基因
    GAMMA = auto()   # 盈利质量基因
    DELTA = auto()   # 风险预警基因


# 用户定义的字段工厂（使用框架的通用 Field + metadata）
def gene_field(
    target: GeneTarget,
    probe_source: str = "",
    weight: float = 1.0,
    **kwargs
) -> Field:
    """用户自己的业务字段工厂"""
    return Field(
        **kwargs,
        metadata={
            "gene_target": target.name,      # 用户的业务概念
            "probe_source": probe_source,    # 用户的业务概念
            "weight": weight,                # 用户的业务概念
            "system": "TRUTH",               # 用户的标识
        }
    )


# ============================================================
# 用户定义业务 Schema（使用框架的通用 Schema 机制）
# ============================================================

class GeneAnalysisRecord(Schema):
    """用户的业务 Schema - 框架只提供元类机制"""

    # 基础标识
    ts_code = Field(str, description="股票代码")
    name = Field(str, description="股票名称")

    # 基因维度字段 - 用户用自己的工厂函数
    alpha_gene = gene_field(
        GeneTarget.ALPHA,
        field_type=float,
        probe_source="growth_probes",
        weight=0.3,
        validators=[range_check(0, 1)],
        description="增长能力基因评分"
    )

    beta_gene = gene_field(
        GeneTarget.BETA,
        field_type=float,
        probe_source="stability_probes",
        weight=0.25,
        validators=[range_check(0, 1)],
        description="稳定性基因评分"
    )

    gamma_gene = gene_field(
        GeneTarget.GAMMA,
        field_type=float,
        probe_source="quality_probes",
        weight=0.25,
        validators=[range_check(0, 1)],
        description="盈利质量基因评分"
    )

    delta_gene = gene_field(
        GeneTarget.DELTA,
        field_type=float,
        probe_source="risk_probes",
        weight=0.2,
        validators=[range_check(0, 1)],
        description="风险预警基因评分"
    )


# ============================================================
# 演示 1: Schema 自省能力
# ============================================================
print("\n【演示1】Schema 自省能力")
print("-"*40)

schema_info = GeneAnalysisRecord.__schema_info__
fields_dict = GeneAnalysisRecord.__fields__
print(f"Schema: {schema_info.name}")
print(f"版本: {schema_info.version}")
print(f"字段数: {len(fields_dict)}")

print("\n字段元数据:")
for fname, fdef in fields_dict.items():
    # 获取字段描述符
    descriptor = fdef._descriptor
    gene_info = descriptor.metadata.get('gene_target', 'N/A')
    type_info = descriptor.type_info
    type_name = type_info.python_type.__name__ if type_info else 'Any'
    print(f"  {fname}: gene={gene_info}, type={type_name}")


# ============================================================
# 演示 2: 用户自定义验证器
# ============================================================
print("\n【演示2】用户自定义验证器")
print("-"*40)

# 用户定义自己的业务验证器
def gene_sum_validator(data: dict) -> bool:
    """用户定义的业务验证：所有基因评分加权和应接近 1.0"""
    total = (
        data.get('alpha_gene', 0) * 0.3 +
        data.get('beta_gene', 0) * 0.25 +
        data.get('gamma_gene', 0) * 0.25 +
        data.get('delta_gene', 0) * 0.2
    )
    return 0.1 <= total <= 1.0

# 创建实例并验证
record = GeneAnalysisRecord()
record.ts_code = "000001.SZ"
record.name = "平安银行"
record.alpha_gene = 0.8
record.beta_gene = 0.7
record.gamma_gene = 0.6
record.delta_gene = 0.3

# 使用类方法验证
is_valid, errors = GeneAnalysisRecord.validate_data(record.to_dict())
print(f"内置验证: {'✓ 通过' if is_valid else f'✗ 失败: {errors}'}")

# 用户自己的业务验证
is_valid = gene_sum_validator(record.to_dict())
print(f"业务验证 (基因和检查): {'✓ 通过' if is_valid else '✗ 失败'}")


# ============================================================
# 演示 3: 通用路由系统（用户定义自己的路由模式）
# ============================================================
print("\n【演示3】通用路由系统")
print("-"*40)

# 框架提供通用路由，用户定义自己的模式
router = Router()

# 用户注册自己的业务路由处理器
def gene_handler(gene_type: str):
    return f"处理基因: {gene_type}"

def probe_handler(gene_type: str, probe_name: str):
    return f"探针: {gene_type}.{probe_name}"

# 添加路由模式并注册处理器
router.add_pattern("gene/{gene_type}", name="gene")
router.register_handler("gene", gene_handler)

router.add_pattern("probe/{gene_type}/{probe_name}", name="probe")
router.register_handler("probe", probe_handler)

# 路由匹配
result = router.match("gene/alpha")
if result:
    print(f"路由匹配: gene/alpha → {gene_handler(result.params['gene_type'])}")

result = router.match("probe/gamma/trend")
if result:
    print(f"路由匹配: probe/gamma/trend → {probe_handler(**result.params)}")


# ============================================================
# 演示 4: 注册中心（版本管理和兼容性）
# ============================================================
print("\n【演示4】Schema 注册中心")
print("-"*40)

# 注册中心是单例
registry = get_registry()
print(f"注册中心实例: {type(registry).__name__}")
print(f"兼容性模式: {registry._compatibility_mode.name}")


# ============================================================
# 演示 5: 数据血缘追踪
# ============================================================
print("\n【演示5】数据血缘追踪")
print("-"*40)

lineage = Lineage()

# 用户跟踪自己的业务流程
lineage.add_node(
    node_id="raw_financial_data",
    name="原始财务数据",
    source="tushare",
    type="财务指标"
)
lineage.add_node(
    node_id="trend_analysis",
    name="趋势分析",
    probes=8,
    type="趋势分析"
)
lineage.add_node(
    node_id="gene_scores",
    name="基因评分",
    dimensions=4,
    type="基因评分"
)

lineage.connect("raw_financial_data", "trend_analysis")
lineage.connect("trend_analysis", "gene_scores")

print("血缘图:")
for node_id, node in lineage._nodes.items():
    upstream = lineage.get_upstream(node_id)
    upstream_ids = [n.id for n in upstream]
    print(f"  {node_id}: upstream={upstream_ids}, name={node.name}")


# ============================================================
# 演示 6: 指纹和缓存键
# ============================================================
print("\n【演示6】Schema 指纹")
print("-"*40)

# 计算 schema 指纹（用于缓存和版本控制）
from dataclasses import asdict
fp = fingerprint(str(asdict(schema_info)))
print(f"Schema 指纹: {fp[:16]}...")


# ============================================================
# 演示 7: 序列化（多格式）
# ============================================================
print("\n【演示7】序列化")
print("-"*40)

data_dict = record.to_dict()
print(f"to_dict(): {list(data_dict.keys())}")

# JSON 格式
import json
json_str = json.dumps(data_dict, ensure_ascii=False, indent=2)
print(f"JSON 片段: {json_str[:100]}...")


# ============================================================
# 总结
# ============================================================
print("\n" + "="*60)
print("总结: PGCS 的设计原则")
print("="*60)
print("""
✓ 框架层 (shared/contracts/):
  - 纯抽象: Field, Schema, Validator, Serializer...
  - 零业务逻辑: 没有 GeneTarget, 没有 alpha/beta/gamma
  - 通用机制: metadata 字典、组合验证器、参数化路由

✓ 用户层 (业务代码):
  - 自定义枚举: GeneTarget, ProbeType...
  - 自定义工厂: gene_field(), alpha_field()...
  - 自定义 Schema: GeneAnalysisRecord...
  - 自定义验证: gene_sum_validator()...

✓ 分层清晰:
  框架不知道业务 → 业务使用框架机制 → 完全解耦
""")

print("\nPGCS 测试完成!")
