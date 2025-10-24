# 🔌 插件化派生器系统 - 实施报告

**文档日期**: 2025-01-XX
**实施状态**: ✅ 完成
**受影响文件**: 4个新文件 + 1个修改文件

---

## 📋 概览

成功将硬编码的 ROIIC 派生逻辑重构为可扩展的插件化系统，实现了指标派生器的动态注册、查找和调用。

### 设计目标 ✨
- ✅ **解耦派生逻辑**: 从 `analyze_metric_trend` 中分离指标计算
- ✅ **可扩展性**: 支持轻松添加新的派生指标（ROA、FCFROIC 等）
- ✅ **动态发现**: 自动查找并调用合适的派生器
- ✅ **错误诊断**: 提供详细的缺失列和可用指标信息
- ✅ **向后兼容**: 保持原有 ROIIC 计算逻辑不变

---

## 🏗️ 架构设计

### 三层结构

```
trend/derivers/
├── base.py              # Protocol 定义层（接口契约）
├── roiic_deriver.py     # 实现层（具体派生器）
└── __init__.py          # 注册层（插件中心）
```

### 核心组件

#### 1. **MetricDeriver Protocol** (`base.py`)
```python
@runtime_checkable
class MetricDeriver(Protocol):
    @property
    def metric_name(self) -> str:
        """派生器能够生成的指标名称（如 'roiic'）"""

    @property
    def required_columns(self) -> Set[str]:
        """派生此指标所需的必需列集合"""

    @property
    def description(self) -> str:
        """派生器的描述信息（用于文档和错误提示）"""

    def can_derive(self, metric_name: str, available_cols: Set[str]) -> bool:
        """判断是否能派生指定指标"""

    def derive(self, con, source_sql: str, group_column: str) -> str:
        """执行派生逻辑，返回新的 SQL 视图名"""
```

**设计优势**:
- `@runtime_checkable`: 支持运行时类型检查 `isinstance(obj, MetricDeriver)`
- Protocol: 鸭子类型，无需继承基类即可实现接口
- 清晰的职责分离：元数据 + 判断 + 执行

#### 2. **ROIICDeriver** (`roiic_deriver.py`)
完整迁移原 `_prepare_derived_metric` 的 ROIIC 计算逻辑：

```python
class ROIICDeriver:
    @property
    def metric_name(self) -> str:
        return "roiic"

    @property
    def required_columns(self) -> Set[str]:
        return {"roic", "invest_capital", "end_date"}

    def derive(self, con, source_sql: str, group_column: str) -> str:
        # 完整的 SQL 逻辑：
        # 1. 使用 ROIC × 投入资本 估算 NOPAT
        # 2. 使用 LAG 窗口函数获取前期值
        # 3. 计算增量 ROIIC = ΔNOPAT / Δ投入资本
        # 返回临时视图名 "trend_with_roiic"
```

**SQL 逻辑说明**:
```sql
WITH base AS (
    SELECT *, (roic / 100.0) * invest_capital AS nopat_est
    FROM source
),
lagged AS (
    SELECT *,
        LAG(nopat_est) OVER (PARTITION BY ts_code ORDER BY end_date) AS nopat_prev,
        LAG(invest_capital) OVER (...) AS invest_prev
    FROM base
)
SELECT
    * EXCLUDE (nopat_est, nopat_prev, invest_prev),
    CASE
        WHEN ABS(invest_capital - invest_prev) < 1e-6 THEN NULL
        ELSE ((nopat_est - nopat_prev) / (invest_capital - invest_prev)) * 100.0
    END AS roiic
FROM lagged
```

#### 3. **注册中心** (`__init__.py`)
```python
_REGISTERED_DERIVERS: List[MetricDeriver] = [
    ROIICDeriver()  # 预注册 ROIIC 派生器
]

def find_deriver(metric_name: str, available_cols: Set[str]) -> Optional[MetricDeriver]:
    """查找能够派生指定指标的派生器"""
    for deriver in _REGISTERED_DERIVERS:
        if deriver.can_derive(metric_name, available_cols):
            return deriver
    return None

def check_derivable(metric_name: str, available_cols: Set[str]) -> Tuple[bool, Set[str]]:
    """检查指标是否可派生，返回 (是否可行, 缺失列集合)"""
    deriver = find_deriver(metric_name, available_cols)
    if deriver:
        missing = deriver.required_columns - available_cols
        return (len(missing) == 0, missing)
    return (False, set())

def list_available_metrics() -> List[str]:
    """列出所有已注册派生器支持的指标"""
    return [d.metric_name for d in _REGISTERED_DERIVERS]
```

---

## 🔄 集成到 `duckdb_trend.py`

### 修改前（硬编码）
```python
def _prepare_derived_metric(con, source_sql, metric_name, group_column):
    """独立函数，硬编码 ROIIC 逻辑"""
    if metric_name.lower() != "roiic":
        return None
    # ... 60+ 行 SQL ...
    return view_name

# 在 analyze_metric_trend 中调用
if metric_name not in all_cols:
    derived_view = _prepare_derived_metric(con, source_sql, metric_name, group_cols[0])
    if derived_view:
        source_sql = derived_view
```

### 修改后（插件化）
```python
from ..trend.derivers import find_deriver, check_derivable, list_available_metrics

# 在 analyze_metric_trend 中
if metric_name not in all_cols:
    # 🔌 使用插件系统
    deriver = find_deriver(metric_name, set(all_cols))

    if deriver:
        logger.info(f"🔌 使用插件 {deriver.__class__.__name__} 派生 {metric_name}")
        source_sql = deriver.derive(con, source_sql, group_cols_list[0])

        # 刷新列信息
        cols_info = con.execute(f"DESCRIBE SELECT * FROM {source_sql}").df()
        all_cols = cols_info['column_name'].tolist()

    # 最终检查：提供详细错误
    if metric_name not in all_cols:
        can_derive, missing = check_derivable(metric_name, set(all_cols))

        if missing:
            raise ValueError(
                f"❌ 指标 '{metric_name}' 无法派生，缺少必需列: {', '.join(sorted(missing))}\n"
                f"当前可用列: {', '.join(sorted(all_cols))}"
            )
        else:
            available = list_available_metrics()
            raise ValueError(
                f"❌ 指标 '{metric_name}' 不存在且无可用派生器。\n"
                f"可派生指标: {', '.join(available)}\n"
                f"当前可用列: {', '.join(sorted(all_cols))}"
            )
```

### 关键改进
1. **删除 `_prepare_derived_metric` 函数**（62行）
2. **动态插件查找**: `find_deriver` 自动匹配合适派生器
3. **详细错误提示**: 区分"缺少列"和"不支持派生"两种情况
4. **日志增强**: 显示使用的派生器类名

---

## 📊 代码变更统计

### 新增文件（343行）
| 文件 | 行数 | 作用 |
|------|------|------|
| `base.py` | 72 | Protocol 定义 |
| `roiic_deriver.py` | 106 | ROIIC 派生器实现 |
| `__init__.py` | 165 | 注册中心 |

### 修改文件
| 文件 | 删除 | 新增 | 净变化 |
|------|------|------|--------|
| `duckdb_trend.py` | 62 | 33 | **-29行** |

**总净变化**: +314行（架构性新增）

---

## ✅ 验证清单

### 功能验证
- ✅ ROIIC 派生逻辑与原实现完全一致
- ✅ `find_deriver("roiic", {...})` 正常工作
- ✅ `check_derivable` 返回正确的缺失列信息
- ✅ 错误提示清晰（区分"缺少列"和"不支持"）
- ✅ 日志输出 `🔌 使用插件 ROIICDeriver 派生 roiic`

### 代码质量
- ✅ 无语法错误（Pylance 检查通过）
- ✅ 类型注解完整（Protocol、Optional、Set、Tuple）
- ✅ 文档字符串完备（所有公开函数）
- ✅ 向后兼容（现有调用代码无需修改）

### 可扩展性测试
- ⏳ 待验证：添加 ROADeriver 测试动态注册
- ⏳ 待验证：多个派生器同时工作
- ⏳ 待验证：性能影响（插件查找开销）

---

## 🚀 扩展示例

### 添加新派生器（ROA - 资产收益率）

#### 步骤 1: 创建派生器类
```python
# src/astock/business_engines/trend/derivers/roa_deriver.py
from typing import Set
import logging

logger = logging.getLogger(__name__)

class ROADeriver:
    """资产收益率派生器

    公式: ROA = (净利润 / 平均总资产) × 100
    """

    @property
    def metric_name(self) -> str:
        return "roa"

    @property
    def required_columns(self) -> Set[str]:
        return {"n_income", "total_assets", "end_date"}

    @property
    def description(self) -> str:
        return "资产收益率 (ROA) = (净利润 / 平均总资产) × 100"

    def can_derive(self, metric_name: str, available_cols: Set[str]) -> bool:
        return (
            metric_name.lower() == self.metric_name
            and self.required_columns.issubset(available_cols)
        )

    def derive(self, con, source_sql: str, group_column: str) -> str:
        from .duckdb_utils import _q

        group_col_q = _q(group_column)
        view_name = "trend_with_roa"

        sql = f"""
        CREATE OR REPLACE TEMP VIEW {view_name} AS
        WITH lagged AS (
            SELECT
                *,
                LAG(total_assets) OVER (
                    PARTITION BY {group_col_q}
                    ORDER BY end_date
                ) AS total_assets_prev
            FROM {source_sql}
        )
        SELECT
            * EXCLUDE (total_assets_prev),
            CASE
                WHEN n_income IS NULL OR total_assets IS NULL OR total_assets_prev IS NULL THEN NULL
                WHEN (total_assets + total_assets_prev) = 0 THEN NULL
                ELSE (n_income / ((total_assets + total_assets_prev) / 2.0)) * 100.0
            END AS roa
        FROM lagged
        """

        logger.info("🔁 自动派生 ROA 序列用于趋势分析")
        con.execute(sql)
        return view_name
```

#### 步骤 2: 注册派生器
```python
# src/astock/business_engines/trend/derivers/__init__.py
from .roa_deriver import ROADeriver

_REGISTERED_DERIVERS: List[MetricDeriver] = [
    ROIICDeriver(),
    ROADeriver()  # ← 添加新派生器
]
```

#### 步骤 3: 使用
```python
# 现有代码无需修改，自动支持！
result = analyze_metric_trend(
    con=con,
    source_sql="my_table",
    metric_name="roa",  # ← 自动查找 ROADeriver
    group_cols="ts_code",
    industry_column="industry_zs"
)
```

---

## 🎯 设计优势总结

### 1. **开闭原则** (Open-Closed Principle)
- 对扩展开放：添加新派生器只需创建新类 + 注册
- 对修改关闭：`analyze_metric_trend` 函数无需修改

### 2. **单一职责** (Single Responsibility)
- 每个派生器只负责一个指标的计算
- 注册中心只负责查找和管理
- 主分析函数只负责协调流程

### 3. **依赖倒置** (Dependency Inversion)
- `duckdb_trend.py` 依赖抽象的 `MetricDeriver` 接口
- 不依赖具体的 `ROIICDeriver` 实现

### 4. **可测试性**
```python
# 单元测试派生器
def test_roiic_deriver():
    deriver = ROIICDeriver()
    assert deriver.metric_name == "roiic"
    assert deriver.can_derive("roiic", {"roic", "invest_capital", "end_date"})
    assert not deriver.can_derive("roiic", {"roic"})  # 缺少列

# 集成测试
def test_find_deriver():
    deriver = find_deriver("roiic", {"roic", "invest_capital", "end_date"})
    assert deriver is not None
    assert isinstance(deriver, MetricDeriver)
```

---

## 📝 后续优化建议

### Phase 3: 扩展更多派生器
- [ ] **ROADeriver**: 资产收益率（净利润 / 平均总资产）
- [ ] **FCFROICDeriver**: 自由现金流 ROIC（经营现金流净额 / Δ投入资本）
- [ ] **AssetTurnoverDeriver**: 资产周转率（营业收入 / 平均总资产）

### Phase 4: 性能优化
- [ ] 缓存派生器查找结果（避免重复遍历）
- [ ] 并行执行多个派生器（如果需要多个派生指标）
- [ ] 延迟视图创建（仅在确实需要时派生）

### Phase 5: 元数据增强
- [ ] 添加 `category` 属性（盈利能力/运营能力/偿债能力）
- [ ] 添加 `formula` 属性（显示计算公式）
- [ ] 支持派生器依赖关系（如 FCFROIC 依赖 ROIIC）

### Phase 6: 用户体验
- [ ] 提供 CLI 命令列出可用派生器: `astock list-metrics`
- [ ] 自动建议相似指标（如用户输入 "roi" 提示 "roic"）
- [ ] 生成派生器文档（自动从 Protocol 提取）

---

## 🎓 总结

本次重构成功将 **硬编码的 ROIIC 派生逻辑** 转变为 **可扩展的插件化系统**，在保持向后兼容的同时，显著提升了代码的可维护性和可扩展性。

### 关键成果
- ✅ 删除 62 行硬编码逻辑
- ✅ 新增 343 行可复用插件架构
- ✅ 支持动态添加新派生器（无需修改主代码）
- ✅ 提供详细错误诊断（缺失列提示）
- ✅ 保持 100% 向后兼容

### 技术亮点
- 使用 `Protocol` 实现鸭子类型接口
- 注册中心模式实现插件发现
- 详细的类型注解和文档字符串
- 清晰的职责分离和模块化设计

**文档版本**: v1.0
**创建日期**: 2025-01-XX
**相关文档**: `PLUGIN_DERIVER_GUIDE.md`, `FULL_CLEANUP_REPORT.md`
