# AStock Pipeline 架构指南 (最新版)

> 状态: Active
> 架构核心: 显式步骤 (steps) + 延迟引擎绑定 (MethodHandle) + Prefect 调度 + Kedro 节点执行 + 签名级缓存
> 关键词: 显式、智能、可观测、可扩展、最小魔法

---
## 目录
1. 设计原则
2. 总体组件示意
3. 演进关键点 (相对旧版本差异)
4. 核心执行链路
5. 配置模型 (steps YAML)
6. 引擎解析: MethodHandle 机制
7. 缓存签名与预测指纹
8. 输入/输出 & 引用解析
9. 指标 (metrics) 与血缘 (lineage)
10. 失败快照 与 resume
11. Hook 与插件体系
12. 服务分层与职责
13. 扩展点: 新方法 / 新引擎 / 新插件
14. 已移除/废弃特性列表 (Clean Sheet)
15. 常用运行示例 (Cheat Sheet)
16. 未来增强 (Roadmap)

---
## 1. 设计原则
- 显式优先: 取消隐式自动多输入推断、取消 primary_policy，方法参数与 step.parameters 一一对应。
- 智能抽象: 仅在 engine=auto 场景使用 MethodHandle 做延迟引擎决策，保持固定引擎零额外开销。
- 轻量可观测: 缓存签名、人类可读差异日志、metrics/lineage 可导出。
- 分层清晰: Orchestrator -> ExecuteManager -> Services -> Engines -> 注册方法。
- 可渐进扩展: MethodHandle 保留 explain / fastpath / invalidate 钩子，为未来性能学习与多实现竞速留接口。
- 稳定缓存策略: 通过 method 链 + predicted implementation 指纹 + 参数值 + 上游指纹 组合生成节点签名。

---
## 2. 总体组件示意
```
CLI (pipeline.main)
  | run / status / metrics / cache
  v
ExecuteManager  (聚合 + 生命周期 + 插件加载)
  |-- ConfigService (解析 steps -> 建 auto kedro nodes)
  |-- RuntimeParamService (运行期参数引用/变量解析)
  |-- FlowExecutor (Prefect Flow 构建与触发)
  |-- ResultAssembler (汇总 lineage / metrics)
  |-- CacheStatsService (缓存命中统计)
  |-- HookManager (事件总线)
  |-- (MethodHandle 列表由 ConfigService 注入 nodes)

PrefectEngine (节点级任务拓扑调度) --> KedroEngine (单节点执行 + 缓存 + 指纹 + 方法链)
```

---
## 3. 演进关键点
| 方向 | 旧状态 | 新状态 |
|------|--------|--------|
| 自动输入聚合 | InputInferenceService 自动推断 | 已删除 (完全显式 inputs 参数) |
| primary_policy | 输出/输入裁剪 | 移除，保留完整输入上下文 |
| 引擎绑定 | 提前固定或直接指定 | 引入 MethodHandle 延迟解析 (engine=auto) |
| metadata_provider | 外部元数据构建签名 | 使用 predict_signature 内部预测指纹 |
| 多方法执行 | 简单串行 | 统一通过方法链 + per-method handle (预测+解析)|
| 缓存签名 | 方法链 + 参数 + 上游指纹 | 增加实现预测指纹 (engine:version:priority) |
| 可观察 | 零散日志 | 结构化 metrics/lineage + cache diff 警示 |

---
## 4. 核心执行链路
1. 读取 YAML (`-c pipeline/configs/xxx.yaml`).
2. ConfigService: 解析 steps → 引用扫描 → 拓扑排序 → 生成 auto nodes (含 handles)。
3. PrefectEngine: 根据 granularity=node 构建 Flow (每 step 一个 Prefect task)。
4. 执行单节点：
   - 解析参数引用（reference → 上游输出/参数）
   - 预测每个方法的实现签名 `predict_signature()`
   - 组装节点签名 (methods|predicted|params|upstream_fps)
   - 缓存命中则跳过，miss 则执行：对方法链逐个：
     * 若 engine=auto：`MethodHandle.resolve()` 解析实际引擎
     * 绑定参数 → 调用 orchestrator.execute_with_engine()
   - 捕获输出 → 注册 global_catalog → 记录 lineage/node_metrics
5. Flow 结束 → 汇总 metrics / lineage → 输出结果结构。

---
## 5. 配置模型 (示例)
```yaml
pipeline:
  name: Demo
  orchestration:
    granularity: node
    task_runner: concurrent
    max_workers: 4
    soft_fail: true
  steps:
    - name: load_base
      component: data_engine
      engine: pandas        # 或 auto
      methods: [load_raw]
      parameters:
        path: data/raw.csv
    - name: clean
      component: data_engine
      engine: auto          # 由 handle 延迟挑选真实引擎
      methods: [clean_basic, enrich]
      parameters:
        source_ref: {__ref__: steps.load_base.outputs.parameters.raw_df}
    - name: aggregate
      component: business_engine
      engine: duckdb
      methods: [aggregate_kpi]
      parameters:
        df: {__ref__: steps.clean.outputs.parameters.enriched}
```
注意：
- methods 可为单字符串或列表。
- outputs 可省略：系统根据下游引用自动补全。
- 参数引用统一结构 `{__ref__: <ref_string>}`。

---
## 6. 引擎解析: MethodHandle 机制
针对每个 (component, method)，当该 step 配置为 `engine=auto`：
- 创建 `MethodHandle(component, method, prefer='auto')`。
- 执行时：
  1. `predict_signature()`：描述候选（describe）→ 过滤 deprecated → priority/版本排序 → 选中候选 → 组成指纹；不写入 `_resolved_engine`。
  2. 真实执行前：`resolve()` 再次（或使用 fastpath）确定 `engine_type`，生成 explain 结构。
- 支持快速路径：若最近预测时间在 TTL/5 秒内，可直接采用预测结果 (env: `ASTOCK_HANDLE_PREDICT_FASTPATH=0` 可关闭)。
- `invalidate()` 可用于动态注册后人工失效。

Explain 结构示例：
```json
{
  "component": "data_engine",
  "method": "clean_basic",
  "strategy": "default_priority_version",
  "selected": {"engine_type": "pandas", "version": "1.0", "priority": 10, "reason": "rule=priority_version"},
  "candidates": [...],
  "ts": 1730000000.123
}
```

---
## 7. 缓存签名与预测指纹
节点签名构成：
```
<method_chain_joined>|<method_meta_joined>|<sorted(param_items)>|<sorted(upstream_name:fingerprint)>
```
其中 `method_meta_joined` 由每个方法的 `predict_signature()` 产物拼接：
```
method@engine:version:priority;method2@engine:version:priority
```
缓存命中规则：
1. 所有计划数据集输出已存在。
2. 旧签名 == 新签名。
3. (可选) TTL 未过期 (step 可设置 cache_ttl)。

签名差异检测：若输出存在但签名变化，日志输出差异片段（methods/params/upstream）。

---
## 8. 输入/输出 & 引用解析
- 引用语法：`steps.<step>.outputs.parameters.<param_name>`。
- 在参数结构中以 `{"__ref__": "steps.load.outputs.parameters.raw"}` 表达。
- ConfigService 扫描所有参数值 → 构建依赖图。
- 若下游引用的输出上游未显式声明 outputs，将自动补全。
- IOManager (内部) 负责最终参数绑定与输出捕获；已去除 primary_policy / 自动输入推断逻辑。

---
## 9. 指标与血缘
`node_metrics[step]`：
```
{
  duration_sec, cached, signature, outputs: [{name, type, shape/len/...}]
}
```
`lineage[step]`：
```
{
  inputs, outputs, primary_output, cached, duration_sec, signature
}
```
缓存统计：hit / miss / hit_rate 由 CacheStatsService 汇总。

---
## 10. 失败快照 与 resume
- 失败时生成 `.pipeline/failures/<step>.json`。
- `--resume`：读取失败列表，重建需要的上游子图（基于 step 依赖）。
- `soft_fail: true`：节点失败不终止 Flow，后续依赖节点被标记跳过。

---
## 11. Hook 与插件
事件：`before_flow` `after_flow` `before_node` `after_node` `on_cache_hit` `on_failure`。
插件：位于 `pipeline/plugins/`，定义 `register(hooks)` 函数。
禁用：`PIPELINE_DISABLE_PLUGINS=plugin_a,plugin_b` 或 `.pipeline_disable_plugins` 文件。

---
## 12. 服务分层
| 服务 | 作用 |
|------|------|
| ExecuteManager | 生命周期与聚合中枢 |
| ConfigService | 解析 steps / 依赖图 / 生成 nodes |
| FlowExecutor | Prefect 流构建与运行 |
| KedroEngine | 节点执行 + 缓存 + 指纹 + 方法链 orchestrate |
| RuntimeParamService | 运行期动态参数解析 |
| ResultAssembler | 汇总 lineage / metrics / 输出注册 |
| CacheStatsService | 缓存统计汇总 |
| HookManager | 事件广播 |

---
## 13. 扩展点
### 新增方法
1. 在 `src/astock/<domain>/engines/<engine>.py` 增加函数。
2. Orchestrator 自动扫描注册（约定式导入）。
3. YAML 中引用 component + engine + methods。

### 新增引擎实现
- 同一 component 下增加新 `<engine>.py`，注册函数。
- 提升优先级：在注册装饰器里设置 `priority` 更高或 version 更新。

### 新增插件
`pipeline/plugins/my_plugin.py`:
```python
def register(hooks):
    def after_node(step, ctx, metrics):
        ...
    hooks.register('after_node', after_node)
```

### MethodHandle 高级用法 (调试)
```python
for node in em.config['pipeline']['kedro_pipelines']['__auto__']['nodes']:
    for h in node.get('handles', []):
        try:
            h.resolve(em.orchestrator)
            print(h.method, h.explain())
        except: pass
```

---
## 14. 已移除 / 废弃特性
| 特性 | 状态 | 替代 | 原因 |
|------|------|------|------|
| InputInferenceService | 已删除 | 显式参数列表 | 不可预测 / 难调试 |
| primary_policy | 已删除 | 全量输入 | 简化心智模型 |
| metadata_provider | 已删除 | MethodHandle.predict_signature | 去中心化 + 减少外部依赖 |
| dataset_primary_map | 已删除 | primary_output 字段 | 统一输出模型 |
| auto_inputs env 系列 | 废弃 | 无 | 移除隐式魔法 |

---
## 15. 常用运行示例
| 任务 | 命令 |
|------|------|
| 执行 | `python -m pipeline.main run -c pipeline/configs/pipeline.yaml` |
| 仅部分步骤 | `... run -c cfg.yaml --only stepA,stepB` |
| 排除步骤 | `... run -c cfg.yaml --exclude stepX` |
| 查看指标 | `python -m pipeline.main metrics -c cfg.yaml` |
| JSON 指标 | `... metrics -c cfg.yaml --format json` |
| Markdown 指标 | `... metrics -c cfg.yaml --format markdown` |
| 缓存计划 | `python -m pipeline.main cache plan -c cfg.yaml` |
| 预热缓存 | `python -m pipeline.main cache warm -c cfg.yaml` |
| 清理缓存 | `python -m pipeline.main cache clear` |
| 查看引擎 | `python -m pipeline.main engines` |
| 系统状态 | `python -m pipeline.main status` |

---
## 16. 未来增强 (Roadmap)
| 项目 | 描述 |
|------|------|
| Fastpath 统计 | 观察预测快速路径命中率 |
| Explain 导出 | handles_explain.json 自动生成 |
| 更细粒度缓存 | 方法级局部缓存（链内复用） |
| OpenTelemetry | Trace 节点 span + 属性注入 |
| Metrics 推送 | Prometheus / OTLP 双输出 |
| 并行策略 | 根据拓扑 & 历史耗时自适应调度权重 |
| 扩展安全 | 可插拔输出序列化策略 (parquet/arrow) |
| 失败策略 | 更多 classify（可重试 vs 不可重试） |

---
若引入新概念：
1. 先更新本 README
2. 补充示例 YAML
3. 增加最小测试/运行验证
4. 保持签名兼容或版本化迁移策略

> Keep it explicit. Keep it explainable. Make smart optional but safe.
