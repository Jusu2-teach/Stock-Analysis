# 插件化派生器实战示例
> 从硬编码到插件化的完整重构指南

## 🎯 场景：股票分析系统需要支持多个派生指标

### 当前问题
- 只支持 ROIIC（增量资本回报率）
- 未来需要支持：ROA、FCFROIC、资产周转率等 10+ 个指标
- 每次加新指标都要改 `analyze_metric_trend` 核心代码

---

## 🔴 方案 A：硬编码（当前做法）

### 代码演化过程

#### 阶段 1：只有 ROIIC
```python
def _prepare_derived_metric(con, source_sql, metric_name, group_column):
    if metric_name.lower() != "roiic":
        return None

    # 60行 SQL 代码...
    return view_name
```

#### 阶段 2：加入 ROA
```python
def _prepare_derived_metric(con, source_sql, metric_name, group_column):
    if metric_name.lower() == "roiic":
        # 60行 ROIIC 代码...
        return "trend_with_roiic"

    elif metric_name.lower() == "roa":
        # 30行 ROA 代码...
        return "trend_with_roa"

    return None
```

#### 阶段 3：加入更多指标（灾难开始）
```python
def _prepare_derived_metric(con, source_sql, metric_name, group_column):
    metric_lower = metric_name.lower()

    if metric_lower == "roiic":
        # 60行 ROIIC 代码...
    elif metric_lower == "roa":
        # 30行 ROA 代码...
    elif metric_lower == "fcfroic":
        # 50行 FCFROIC 代码...
    elif metric_lower == "asset_turnover":
        # 40行代码...
    elif metric_lower == "working_capital_ratio":
        # 35行代码...
    elif metric_lower == "debt_equity_adjusted_roe":
        # 55行代码...
    # ... 越来越长 ...

    return None

# 函数已经 300+ 行，难以维护！
```

### ❌ 问题
1. **函数过长**：单个函数 300+ 行
2. **难以测试**：无法单独测试 ROIIC 逻辑
3. **修改风险高**：加 ROA 时可能破坏 ROIIC
4. **代码重复**：很多指标有相似的 LAG 窗口逻辑

---

## 🟢 方案 B：插件化（推荐做法）

### 目录结构

```
src/astock/business_engines/
└── trend/
    └── derivers/
        ├── __init__.py          # 插件注册中心
        ├── base.py              # 插件接口定义
        ├── roiic_deriver.py     # ROIIC 插件
        ├── roa_deriver.py       # ROA 插件
        ├── fcfroic_deriver.py   # FCFROIC 插件
        └── ...                  # 未来的插件
```

### 实现步骤

#### Step 1: 定义插件接口

```python
# trend/derivers/base.py
from typing import Protocol, Set
from abc import abstractmethod

class MetricDeriver(Protocol):
    """
    指标派生器插件接口

    每个插件负责将基础指标转换为派生指标
    例如：ROIIC = ΔNOPAT / Δ投入资本
    """

    @property
    @abstractmethod
    def metric_name(self) -> str:
        """返回派生指标名称（如 'roiic'）"""
        pass

    @property
    @abstractmethod
    def required_columns(self) -> Set[str]:
        """返回依赖的源列（如 {'roic', 'invest_capital'}）"""
        pass

    @property
    def description(self) -> str:
        """插件描述"""
        return f"{self.metric_name} 派生器"

    def can_derive(self, metric_name: str, available_cols: Set[str]) -> bool:
        """
        判断是否能派生此指标

        Args:
            metric_name: 请求的指标名
            available_cols: 数据中可用的列

        Returns:
            True 如果可以派生
        """
        if metric_name.lower() != self.metric_name.lower():
            return False
        return self.required_columns.issubset(available_cols)

    @abstractmethod
    def derive(
        self,
        con,              # DuckDB 连接
        source_sql: str,  # 源数据 SQL 视图
        group_column: str # 分组列（如 ts_code）
    ) -> str:
        """
        执行派生逻辑

        Returns:
            新视图的名称
        """
        pass
```

#### Step 2: 实现 ROIIC 插件

```python
# trend/derivers/roiic_deriver.py
import logging
from typing import Set
from .base import MetricDeriver

logger = logging.getLogger(__name__)

class ROIICDeriver:
    """
    ROIIC (Return on Incremental Invested Capital) 派生器

    公式：ROIIC = ΔNOPAT / Δ投入资本
    其中：NOPAT ≈ ROIC × 投入资本

    依赖列：
    - roic: 投入资本回报率
    - invest_capital: 投入资本
    - end_date: 时间列（用于计算增量）
    """

    @property
    def metric_name(self) -> str:
        return "roiic"

    @property
    def required_columns(self) -> Set[str]:
        return {"roic", "invest_capital", "end_date"}

    @property
    def description(self) -> str:
        return "增量资本回报率(ROIIC): 衡量新增投资的回报效率"

    def derive(self, con, source_sql: str, group_column: str) -> str:
        from ..engines.duckdb_utils import _q

        group_col_q = _q(group_column)
        view_name = "trend_with_roiic"

        # 完整的 ROIIC 派生 SQL（从原代码迁移）
        sql = f"""
            CREATE OR REPLACE TEMP VIEW {view_name} AS
            WITH base AS (
                SELECT *,
                    CASE
                        WHEN roic IS NULL OR invest_capital IS NULL THEN NULL
                        ELSE (roic / 100.0) * invest_capital
                    END AS nopat_est
                FROM {source_sql}
            ),
            lagged AS (
                SELECT base.*,
                    LAG(nopat_est) OVER (
                        PARTITION BY {group_col_q}
                        ORDER BY end_date
                    ) AS nopat_prev,
                    LAG(invest_capital) OVER (
                        PARTITION BY {group_col_q}
                        ORDER BY end_date
                    ) AS invest_prev
                FROM base
            )
            SELECT
                lagged.* EXCLUDE (nopat_est, nopat_prev, invest_prev),
                CASE
                    WHEN nopat_est IS NULL OR nopat_prev IS NULL THEN NULL
                    WHEN invest_prev IS NULL OR invest_capital IS NULL THEN NULL
                    WHEN ABS(invest_capital - invest_prev) < 1e-6 THEN NULL
                    ELSE ((nopat_est - nopat_prev) /
                          (invest_capital - invest_prev)) * 100.0
                END AS roiic
            FROM lagged
        """

        logger.info("🔌 ROIIC插件: 派生 ROIIC = ΔNOPAT / Δ投入资本")
        con.execute(sql)
        return view_name


# 单元测试（独立测试这个插件）
def test_roiic_deriver():
    deriver = ROIICDeriver()

    # 测试元数据
    assert deriver.metric_name == "roiic"
    assert "roic" in deriver.required_columns
    assert "invest_capital" in deriver.required_columns

    # 测试能力判断
    assert deriver.can_derive("roiic", {"roic", "invest_capital", "end_date"})
    assert deriver.can_derive("ROIIC", {"roic", "invest_capital", "end_date"})  # 大小写
    assert not deriver.can_derive("roa", {"roic", "invest_capital", "end_date"})  # 错误指标
    assert not deriver.can_derive("roiic", {"roic"})  # 缺少列
```

#### Step 3: 实现更多插件（轻而易举）

```python
# trend/derivers/roa_deriver.py
class ROADeriver:
    """ROA (Return on Assets) 派生器

    公式：ROA = 净利润 / 总资产 × 100%
    """

    @property
    def metric_name(self) -> str:
        return "roa"

    @property
    def required_columns(self) -> Set[str]:
        return {"n_income", "total_assets"}

    def derive(self, con, source_sql: str, group_column: str) -> str:
        view_name = "trend_with_roa"
        sql = f"""
            CREATE OR REPLACE TEMP VIEW {view_name} AS
            SELECT *,
                CASE
                    WHEN total_assets IS NULL OR total_assets = 0 THEN NULL
                    ELSE (n_income / total_assets) * 100.0
                END AS roa
            FROM {source_sql}
        """
        con.execute(sql)
        return view_name


# trend/derivers/fcfroic_deriver.py
class FCFROICDeriver:
    """FCFROIC (Free Cash Flow Return on Invested Capital) 派生器

    公式：FCFROIC = 自由现金流 / 投入资本 × 100%
    """

    @property
    def metric_name(self) -> str:
        return "fcfroic"

    @property
    def required_columns(self) -> Set[str]:
        return {"free_cash_flow", "invest_capital"}

    def derive(self, con, source_sql: str, group_column: str) -> str:
        view_name = "trend_with_fcfroic"
        sql = f"""
            CREATE OR REPLACE TEMP VIEW {view_name} AS
            SELECT *,
                CASE
                    WHEN invest_capital IS NULL OR invest_capital = 0 THEN NULL
                    ELSE (free_cash_flow / invest_capital) * 100.0
                END AS fcfroic
            FROM {source_sql}
        """
        con.execute(sql)
        return view_name
```

#### Step 4: 插件注册中心

```python
# trend/derivers/__init__.py
from typing import List, Optional, Set
from .base import MetricDeriver
from .roiic_deriver import ROIICDeriver
from .roa_deriver import ROADeriver
from .fcfroic_deriver import FCFROICDeriver

# 🔌 全局插件注册表
_REGISTERED_DERIVERS: List[MetricDeriver] = [
    ROIICDeriver(),
    ROADeriver(),
    FCFROICDeriver(),
    # 未来添加新插件只需一行！
]

def get_registered_derivers() -> List[MetricDeriver]:
    """获取所有已注册的派生器"""
    return list(_REGISTERED_DERIVERS)

def list_available_metrics() -> List[str]:
    """列出所有可派生的指标"""
    return [d.metric_name for d in _REGISTERED_DERIVERS]

def find_deriver(
    metric_name: str,
    available_cols: Set[str]
) -> Optional[MetricDeriver]:
    """
    查找能派生指定指标的派生器

    Args:
        metric_name: 目标指标名
        available_cols: 数据中可用的列

    Returns:
        匹配的派生器，如果没有则返回 None
    """
    for deriver in _REGISTERED_DERIVERS:
        if deriver.can_derive(metric_name, available_cols):
            return deriver
    return None

def get_deriver_info(metric_name: str) -> Optional[dict]:
    """获取派生器的详细信息"""
    for deriver in _REGISTERED_DERIVERS:
        if deriver.metric_name.lower() == metric_name.lower():
            return {
                "name": deriver.metric_name,
                "description": deriver.description,
                "required_columns": list(deriver.required_columns),
                "plugin_class": deriver.__class__.__name__,
            }
    return None
```

#### Step 5: 在核心代码中使用插件

```python
# duckdb_trend.py（大幅简化）
from ..trend.derivers import find_deriver, list_available_metrics

def analyze_metric_trend(...):
    con, source_sql = _init_duckdb_and_source(data)

    # 检查指标列是否存在
    cols_info = con.execute(f"DESCRIBE SELECT * FROM {source_sql}").df()
    all_cols = set(cols_info['column_name'].tolist())

    if metric_name not in all_cols:
        # 🔌 使用插件系统自动派生
        deriver = find_deriver(metric_name, all_cols)

        if deriver:
            logger.info(
                f"🔌 使用插件 {deriver.__class__.__name__} "
                f"派生 {metric_name}"
            )
            source_sql = deriver.derive(con, source_sql, group_cols_list[0])

            # 刷新列信息
            cols_info = con.execute(f"DESCRIBE SELECT * FROM {source_sql}").df()
            all_cols = set(cols_info['column_name'].tolist())

        if metric_name not in all_cols:
            available = list_available_metrics()
            raise ValueError(
                f"指标 '{metric_name}' 不存在且无法派生。"
                f"可用派生指标: {available}"
            )

    # 后续趋势分析逻辑完全不变...
```

---

## 📊 对比效果

### 添加新指标 ROE

#### 硬编码方式
```python
# ❌ 需要修改核心代码（风险高）
def _prepare_derived_metric(...):
    # ... 已有 300 行代码 ...
    elif metric_lower == "roe":  # ← 新加 40 行
        # 计算 ROE 逻辑...
        return "trend_with_roe"
    # ...
```

#### 插件化方式
```python
# ✅ 仅新增独立文件（零风险）
# trend/derivers/roe_deriver.py
class ROEDeriver:
    @property
    def metric_name(self) -> str:
        return "roe"

    @property
    def required_columns(self) -> Set[str]:
        return {"n_income", "total_equity"}

    def derive(self, con, source_sql: str, group_column: str) -> str:
        view_name = "trend_with_roe"
        sql = f"""
            CREATE OR REPLACE TEMP VIEW {view_name} AS
            SELECT *,
                CASE
                    WHEN total_equity = 0 THEN NULL
                    ELSE (n_income / total_equity) * 100.0
                END AS roe
            FROM {source_sql}
        """
        con.execute(sql)
        return view_name

# trend/derivers/__init__.py
_REGISTERED_DERIVERS = [
    ROIICDeriver(),
    ROADeriver(),
    FCFROICDeriver(),
    ROEDeriver(),  # ← 仅此一行！
]
```

---

## 🎁 插件化的额外好处

### 1. 自动文档生成

```python
# 自动生成派生指标文档
def generate_deriver_docs():
    for deriver in get_registered_derivers():
        print(f"### {deriver.metric_name.upper()}")
        print(f"- 描述: {deriver.description}")
        print(f"- 依赖列: {', '.join(deriver.required_columns)}")
        print()

# 输出:
# ### ROIIC
# - 描述: 增量资本回报率(ROIIC): 衡量新增投资的回报效率
# - 依赖列: roic, invest_capital, end_date
#
# ### ROA
# - 描述: 总资产回报率(ROA): 衡量资产利用效率
# - 依赖列: n_income, total_assets
```

### 2. 运行时查询可用指标

```python
# CLI 命令
$ python -m astock.trend list-derivers

可派生指标:
  roiic     - 增量资本回报率 (需要: roic, invest_capital)
  roa       - 总资产回报率 (需要: n_income, total_assets)
  fcfroic   - 自由现金流回报率 (需要: free_cash_flow, invest_capital)
  roe       - 净资产回报率 (需要: n_income, total_equity)
```

### 3. 智能错误提示

```python
# 当前:
ValueError: 指标 'roiic' 不存在于数据中，并且无法派生

# 插件化后:
ValueError: 指标 'roiic' 不存在且无法派生
原因: 缺少依赖列 ['invest_capital']
可用数据列: ['roic', 'roe', 'roa', 'end_date']
建议: 请确保数据包含 invest_capital 列，或选择其他指标
可派生指标: ['roa', 'roe']
```

---

## 🚀 迁移路线图

### Phase 1: 准备（1天）
- [ ] 创建 `trend/derivers/` 目录
- [ ] 定义 `base.py` 接口
- [ ] 编写插件注册中心

### Phase 2: 迁移 ROIIC（1天）
- [ ] 创建 `roiic_deriver.py`
- [ ] 将现有逻辑迁移
- [ ] 编写单元测试
- [ ] 更新 `duckdb_trend.py` 使用插件

### Phase 3: 验证（0.5天）
- [ ] 运行完整 workflow
- [ ] 对比输出结果（应该完全一致）
- [ ] 性能测试

### Phase 4: 扩展（按需）
- [ ] 添加 ROA 插件
- [ ] 添加 FCFROIC 插件
- [ ] ...

---

需要我帮你实施 Phase 1-2 吗？
