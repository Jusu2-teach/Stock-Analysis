"""
探针协议重构设计
================

目标：统一所有探针的接口、命名和参数规范。

当前问题分析
------------

1. **命名混乱**:
   - LogTrendCalculator, VolatilityCalculator (Calculator)
   - CyclicalPatternDetector, DeteriorationDetector, InflectionDetector (Detector)
   - RobustTrendProbe (Probe)

2. **方法名不统一**:
   - calculate() - 计算类
   - detect() - 检测类
   - compute() - 计算类

3. **参数签名不统一**:
   - `(values)` - InflectionDetector
   - `(values, **thresholds)` - 大多数
   - `(values, context)` - RobustTrendProbe (强制需要 context)

4. **core 目录结构混乱**:
   - interfaces.py: IAnalyzer/IScorer 从未使用
   - duckdb_utils.py: 与 probe 无关的工具函数

重构方案
--------

### 1. 统一探针协议

所有探针类都应该：
- 命名为 `XxxProbe` (不是 Calculator/Detector)
- 实现 `compute(values, **kwargs)` 方法
- 实现 `default()` 方法
- 有 `name` 属性

```python
class ProbeProtocol(Protocol):
    name: str

    def compute(self, values: List[float], **kwargs) -> Any:
        '''执行计算，kwargs 接收所有可选参数'''
        ...

    def default(self) -> Any:
        '''返回默认结果'''
        ...
```

### 2. 重命名探针类

| 当前名称 | 建议名称 | 方法 |
|---------|---------|------|
| LogTrendCalculator | LogTrendProbe | compute |
| VolatilityCalculator | VolatilityProbe | compute |
| CyclicalPatternDetector | CyclicalProbe | compute |
| DeteriorationDetector | DeteriorationProbe | compute |
| RollingTrendCalculator | RollingProbe | compute |
| RobustTrendProbe | RobustProbe | compute |
| InflectionDetector | InflectionProbe | compute |

### 3. 统一参数签名

```python
def compute(self, values: List[float], **kwargs) -> XxxResult:
    '''
    所有可选参数通过 kwargs 传递:
    - thresholds (dict): 阈值配置
    - context (ProbeContext): 上下文信息 (可选)
    - options (dict): 其他选项
    '''
```

### 4. 重构 core 目录

```
core/
├── __init__.py           # 导出
├── probe_engine/         # 探针引擎 (保留)
│   ├── unified.py        # 统一引擎
│   ├── specs.py          # 探针规格
│   └── ...
└── protocols.py          # 统一协议定义 (新)

# 移除或迁移:
# - interfaces.py → 删除 (未使用)
# - duckdb_utils.py → 移到 src/astock/utils/
```

### 5. 探针重构示例

**重构前 (RobustTrendProbe)**:
```python
class RobustTrendProbe:
    name = "robust"

    def compute(self, values: List[float], context: MetricProbeContext) -> RobustTrendResult:
        # context 是必需参数，破坏了统一性
        ...
```

**重构后**:
```python
class RobustProbe:
    name = "robust"

    def compute(self, values: List[float], **kwargs) -> RobustTrendResult:
        # context 是可选的，默认为空
        context = kwargs.get('context')
        group_key = context.group_key if context else "unknown"
        ...

    def default(self) -> RobustTrendResult:
        return RobustTrendResult(...)
```

实施步骤
--------

1. 创建统一协议 `core/protocols.py`
2. 重构所有探针类：
   - 重命名为 XxxProbe
   - 方法统一为 compute()
   - 参数统一为 (values, **kwargs)
   - 添加 default() 方法
3. 更新 specs.py 配置
4. 移除 interfaces.py
5. 迁移 duckdb_utils.py 到 utils/

预期收益
--------

1. **统一性**: 所有探针遵循相同的协议
2. **可扩展性**: 新增探针无需修改框架代码
3. **可维护性**: 单一职责，清晰的边界
4. **测试友好**: 统一的接口便于 mock 和测试
"""
