# AStock Orchestrator (v4)

精简、模块化、无隐式魔法的注册与调度层。

**📍 位置**: 根目录 `orchestrator/`，与 `pipeline/` 平级，作为独立的编排系统。

## 目录结构
```
orchestrator/
  __init__.py          # 对外暴露 AStockOrchestrator / register_method (已移除 get_registry 兼容函数)
  orchestrator.py      # Facade + 动态组件访问
  models.py            # MethodRegistration 数据模型
  errors.py            # 自定义异常
  config.py            # RegistryConfig (冲突策略 / 基础包)
  utils_version.py     # 版本字符串解析
  decorators/
    register.py        # @register_method 装饰器
  registry/
    registry.py        # Registry 单例：注册 / 选择 / 执行 / 统计 / 刷新
    index.py           # 分层索引 component -> method -> engine
    strategies.py      # 策略模式实现 (default/latest/stable/priority/engine override)
    metrics.py         # 执行指标收集
    hooks.py           # HookBus 事件分发
    loader.py          # 模块发现与导入
    executor.py        # MethodExecutor + 输入风格校验
```

## 设计原则
- 显式优先：无参数别名注入、无隐式推断
- 单一职责：策略 / 度量 / 加载 / 执行 解耦
- 可观测：metrics + hooks 支持后续对接监控
- 可扩展：新增策略 / 新增组件目录无需改核心

## 关键概念
| 概念 | 说明 |
|------|------|
| component_type | 逻辑组件类别 (data_engines / datahub / business_engines ...) |
| engine_type    | 具体实现（pandas / polars / tushare / duckdb 等）|
| engine_name    | 对外暴露调用的方法名 |
| full_key       | `component_type::engine_type::engine_name` 唯一标识 |

## 注册一个方法
```python
# orchestrator 已移至根目录
from orchestrator import register_method

@register_method(
    component_type="data_engines",
    engine_type="polars",
    engine_name="clean_data",
    version="1.2.0",
    priority=10,
    description="示例清洗"
)
def clean_data(df):
    return df
```

## 执行调用
```python
# orchestrator 已移至根目录
from orchestrator import AStockOrchestrator

o = AStockOrchestrator(auto_discover=True)
res = o.data_engines.clean_data(my_df)
```

## 策略选择
```python
o.execute("data_engines", "clean_data", _strategy="latest")
o.execute("data_engines", "clean_data", _engine_type="polars")
```

## 输入风格校验 (环境变量)
| 变量 | 默认 | 说明 |
|------|------|------|
| ASTOCK_INPUT_STYLE | strict_single | strict_single / allow_list / enforce_list |

strict_single: 禁止用单元素 list 伪装单对象输入 (除非函数首参注解为 Iterable)

enforce_list: 强制第一个位置参数为 list/tuple

## 已移除内容
| 删除 | 原因 |
|------|------|
| core.py | 旧包装/兼容层，冗余且增加噪音 |
| intelligent_registry.py | 早期单文件“智能”实现，功能被模块化 Registry 取代 |
| 隐式参数别名 (data/df/dataset) | 不可预测副作用 |
| registry.methods / callable_method | 兼容层清理，统一走 index + registration.callable |

## Hook 事件 (预留)
| 事件名 | 触发时机 |
|--------|----------|
| after_method_registered | 每次方法成功注册 |
| after_registry_refresh  | refresh 全量重建结束 |

## 后续扩展建议
- metrics 导出 Prometheus collector
- hook -> 插件系统 (链路追踪, tracing)
- 版本/标签筛选复合策略 (e.g., latest_non_deprecated)
- schema 校验与策略（失败可 soft / hard）

## 最小故障排查
| 问题 | 排查 |
|------|------|
| 方法找不到 | 确认 component_type/engine_type/engine_name 拼写；查看 `o.list_methods()` |
| 参数校验错误 | 检查 ASTOCK_INPUT_STYLE；核对函数签名 |
| 版本未切换 | 检查 priority / version 是否满足策略 |

## 许可
内部项目 / 未公开发布。
