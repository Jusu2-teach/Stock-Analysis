# PGCS Router 集成示例

## 概述

本文档演示如何将 `shared/contracts/router` 集成到 Pipeline 的引用解析系统中。

---

## 当前实现分析

### 现状：硬编码正则 ([context.py](../pipeline/core/context.py#L22))

```python
# pipeline/core/context.py
REF_PATTERN = re.compile(r"^steps\.(?P<step>[^.]+)\.outputs\.parameters\.(?P<param>[^.]+)$")
```

**问题**:
1. 模式写死，不可配置
2. 只支持一种引用格式
3. 无法扩展新的引用类型

---

## 集成方案

### Step 1: 定义 Pipeline 路由模式

```python
# pipeline/core/routes.py (新建)
"""Pipeline 路由模式定义"""
from shared.contracts import Router, RoutePattern

# 定义引用解析路由
PIPELINE_ROUTES = Router()

# 模式1: 步骤输出引用 steps.{step}.outputs.parameters.{param}
PIPELINE_ROUTES.register(RoutePattern(
    template='steps.{step}.outputs.parameters.{param}',
    handler='step_output',
    description='引用其他步骤的输出参数'
))

# 模式2: 全局配置引用 config.{section}.{key} (可扩展)
PIPELINE_ROUTES.register(RoutePattern(
    template='config.{section}.{key}',
    handler='config_value',
    description='引用全局配置值'
))

# 模式3: 环境变量引用 env.{var_name} (可扩展)
PIPELINE_ROUTES.register(RoutePattern(
    template='env.{var_name}',
    handler='env_var',
    description='引用环境变量'
))


def resolve_reference(ref_string: str) -> dict | None:
    """解析引用字符串，返回匹配结果

    Returns:
        dict: {'handler': 'step_output', 'step': 'Load_Data', 'param': 'Raw_Data'}
        None: 未匹配任何模式
    """
    route = PIPELINE_ROUTES.match(ref_string)
    if route and route.is_matched:
        return {'handler': route.handler, **route.params}
    return None
```

### Step 2: 修改 context.py

```python
# pipeline/core/context.py (修改)
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

# 移除硬编码正则，改用 Router
# REF_PATTERN = re.compile(...)  # 删除

# 导入 Router 解析函数
from .routes import resolve_reference


@dataclass
class PipelineContext:
    """Pipeline 执行上下文"""

    def resolve_ref(self, ref_string: str) -> Any:
        """解析引用字符串

        Args:
            ref_string: 'steps.Load_Data.outputs.parameters.Raw_Data'

        Returns:
            解析后的值，或 None
        """
        result = resolve_reference(ref_string)
        if not result:
            return None

        handler = result.get('handler')

        if handler == 'step_output':
            step = result.get('step')
            param = result.get('param')
            ref_key = f"steps.{step}.outputs.parameters.{param}"
            return self.reference_values.get(ref_key)

        elif handler == 'config_value':
            section = result.get('section')
            key = result.get('key')
            return self.config.get(section, {}).get(key)

        elif handler == 'env_var':
            import os
            return os.environ.get(result.get('var_name'))

        return None
```

### Step 3: 修改 kedro_engine.py

```python
# pipeline/engines/kedro_engine.py (修改)

# 移除旧的正则引用
# from pipeline.core.context import REF_PATTERN  # 删除

# 使用新的路由解析
from pipeline.core.routes import resolve_reference


def _resolve_refs_via_catalog(obj):
    """使用 Router 解析引用"""

    def walk(v):
        if isinstance(v, dict) and '__ref__' in v:
            ref = v['__ref__']

            # 使用 Router 解析
            result = resolve_reference(ref)

            if result and result.get('handler') == 'step_output':
                step_id = result['step']
                out_id = result['param']
                ds_name = f"{step_id}__{out_id}".replace('-', '_')

                if ds_name in self.global_catalog:
                    return self.global_catalog[ds_name]

                raise ValueError(f"catalog 中未找到数据集: {ds_name}")

        if isinstance(v, list):
            return [walk(x) for x in v]
        if isinstance(v, dict):
            return {k: walk(val) for k, val in v.items() if k != '__ref__'}
        return v

    return {k: walk(val) for k, val in obj.items()}
```

---

## 收益

| 维度 | 旧方案 | 新方案 |
|------|--------|--------|
| **可扩展性** | 只支持一种模式 | 支持多种路由模式 |
| **可配置性** | 硬编码 | 可动态注册 |
| **可测试性** | 测试正则 | 测试路由匹配 |
| **可读性** | 正则不直观 | 模板语法清晰 |

---

## 测试用例

```python
# tests/pipeline/test_routes.py
from pipeline.core.routes import resolve_reference, PIPELINE_ROUTES

def test_step_output_reference():
    result = resolve_reference('steps.Load_Data.outputs.parameters.Raw_Data')
    assert result == {
        'handler': 'step_output',
        'step': 'Load_Data',
        'param': 'Raw_Data'
    }

def test_config_reference():
    result = resolve_reference('config.database.host')
    assert result == {
        'handler': 'config_value',
        'section': 'database',
        'key': 'host'
    }

def test_invalid_reference():
    result = resolve_reference('invalid.reference.format')
    assert result is None
```

---

## 兼容性说明

- **渐进式迁移**: 可保留 `REF_PATTERN` 作为 fallback
- **配置驱动**: 可通过 YAML 配置启用/禁用新路由
- **向后兼容**: 现有 YAML 工作流无需修改
