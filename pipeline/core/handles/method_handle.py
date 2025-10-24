"""MethodHandle (Phase 2)

扩展点:
1. 延迟解析改为基于 orchestrator.describe() 的候选集合策略选择
2. 策略 (默认): 过滤 deprecated -> priority 最大 -> version 高 -> 若 self.prefer 指定且存在则优先
3. 短期缓存解析 (TTL 可配) - env: ASTOCK_HANDLE_RESOLVE_TTL (秒, 默认 5)
4. 提供 explain() 返回决策详情 (candidates / selected / reason / ts)
5. 保持 fixed_engine 直达路径; prefer 预留未来策略扩展

暂未实现(下一阶段可扩展):
- 回退(fallback) / 多实现竞速 / 自适应性能学习 / 并行探测
"""
from __future__ import annotations
from typing import Optional, Any, Dict, List
import time
import os


def _parse_version(v: str) -> tuple:
    if not v:
        return (0,)
    parts = []
    for seg in str(v).split('.'):
        try:
            parts.append(int(seg))
        except ValueError:
            # 提取前缀数字部分
            num = ''
            for ch in seg:
                if ch.isdigit():
                    num += ch
                else:
                    break
            parts.append(int(num) if num else 0)
    return tuple(parts or [0])

class MethodHandle:
    __slots__ = (
        "component", "method", "prefer", "fixed_engine",
        "_resolved_engine", "_resolved_at", "_explain", "_ttl",
        "_lock", "_last_prediction"
    )

    def __init__(self, component: str, method: str, *, prefer: str = 'auto', fixed_engine: str | None = None):
        """创建方法句柄。

        参数:
            component: 组件类型 (如 data / indicator)
            method:    方法名
            prefer:    'auto' / 'fixed' / 预留其它策略关键字
            fixed_engine: 显式绑定引擎（非 auto 场景）

        设计约束:
        - 不做昂贵操作 (不在 __init__ 里触发 describe / resolve)
        - 线程安全: 通过内部锁避免并发重复解析
        - 支持快速路径: 若刚刚做过 predict 且允许快速复用，可跳过再次 describe
        """
        if not component or not method:
            raise ValueError("component 与 method 不能为空")
        self.component = component
        self.method = method
        self.prefer = prefer  # 未来扩展使用
        self.fixed_engine = fixed_engine
        self._resolved_engine: Optional[str] = None
        self._resolved_at: Optional[float] = None
        self._explain: Optional[Dict[str, Any]] = None
        # 最近一次 predict 的结果 (不等同于真实解析)
        self._last_prediction: Optional[Dict[str, Any]] = None
        # TTL 解析缓存 (秒)
        try:
            self._ttl = float(os.getenv('ASTOCK_HANDLE_RESOLVE_TTL', '5'))
        except ValueError:
            self._ttl = 5.0
        # 并发锁 (避免多线程同时触发耗时解析)
        try:
            from threading import RLock  # 优先 RLock 便于递归调用场景
            self._lock = RLock()
        except Exception:  # 极端环境 fallback
            class _Dummy:
                def __enter__(self_inner): return None
                def __exit__(self_inner, *a): return False
            self._lock = _Dummy()

    # ------------------ public API ------------------
    def resolve(self, orchestrator) -> str:
        """最终解析引擎 (可能利用预测快速路径)。"""
        with self._lock:
            # 固定引擎: 直接返回
            if self.fixed_engine:
                self._resolved_engine = self.fixed_engine
                if not self._explain:
                    self._explain = {
                        'component': self.component,
                        'method': self.method,
                        'selected': {
                            'engine_type': self.fixed_engine,
                            'reason': 'fixed_engine'
                        },
                        'candidates': [],
                        'ts': time.time(),
                        'strategy': 'fixed'
                    }
                return self.fixed_engine
            # 缓存有效
            if self._is_cache_valid():
                return self._resolved_engine  # type: ignore
            # 预测快速路径 (可通过 env 关闭)
            fast_enabled = os.getenv('ASTOCK_HANDLE_PREDICT_FASTPATH', '1') == '1'
            if fast_enabled and self._last_prediction:
                pred = self._last_prediction
                # 允许使用最近预测 (5 秒内 或 小于 TTL) 作为快速解析
                pred_ts = pred.get('ts') or 0
                if (time.time() - pred_ts) < min(self._ttl, 5):
                    self._resolved_engine = pred.get('engine_type')
                    self._resolved_at = time.time()
                    self._explain = {
                        'component': self.component,
                        'method': self.method,
                        'strategy': 'predicted_fastpath',
                        'candidates': pred.get('candidates', []),  # 预测时可附带
                        'selected': {
                            'engine_type': pred.get('engine_type'),
                            'version': pred.get('version'),
                            'priority': pred.get('priority'),
                            'reason': 'fastpath'
                        },
                        'ts': self._resolved_at
                    }
                    return self._resolved_engine  # type: ignore
            # 正常解析
            self._perform_resolution(orchestrator)
            if self._resolved_engine is None:
                raise ValueError(f"MethodHandle 未能解析引擎: {self.component}.{self.method}")
            return self._resolved_engine

    def explain(self) -> Dict[str, Any]:
        return self._explain or {
            'component': self.component,
            'method': self.method,
            'selected': None,
            'candidates': [],
            'ts': None,
            'strategy': None
        }

    def identity(self) -> str:
        base = f"{self.component}.{self.method}"
        if self.fixed_engine:
            return base + f"@fixed:{self.fixed_engine}"
        if self._resolved_engine:
            return base + f"@auto:{self._resolved_engine}"
        return base + "@unresolved"

    def predict_signature(self, orchestrator) -> str:
        """预测用于缓存签名的实现指纹 (不强制提前持久 resolve)。

        结构: method@engine:version:priority
        - 与 resolve() 选择逻辑保持一致: describe -> priority/version -> fallback resolve_engine
        - 不修改 _resolved_engine (保持延迟绑定), 但可利用已缓存的 explain 信息 (TTL 未过期时)
        - 若信息缺失, 使用 unknown:0 占位
        """
        # 如果已解析且缓存有效, 直接用 explain 里的 selected
        if self._is_cache_valid() and self._explain and self._explain.get('selected'):
            sel = self._explain['selected']
            eng = sel.get('engine_type') or 'unknown'
            ver = sel.get('version') or 'unknown'
            pri = sel.get('priority') if sel.get('priority') is not None else 0
            return f"{self.method}@{eng}:{ver}:{pri}"
        # fixed_engine 直接查 registry 取 version/priority
        if self.fixed_engine:
            try:
                reg = getattr(orchestrator, 'registry', None)
                if reg is not None:
                    key = f"{self.component}_{self.fixed_engine}_{self.method}"
                    reg_obj = reg.methods.get(key)
                    if reg_obj:
                        ver = getattr(reg_obj, 'version', 'unknown') or 'unknown'
                        pri = getattr(reg_obj, 'priority', 0) or 0
                        # 不缓存 last_prediction（固定值意义不大）
                        return f"{self.method}@{self.fixed_engine}:{ver}:{pri}"
                return f"{self.method}@{self.fixed_engine}:unknown:0"
            except Exception:
                return f"{self.method}@{self.fixed_engine}:unknown:0"
        # 尝试使用 describe 进行同 resolve 逻辑 (不落盘)
        try:
            desc = None
            if hasattr(orchestrator, 'describe'):
                desc = orchestrator.describe(self.component, self.method)
            if not desc or desc.get('status') != 'ok':
                # fallback: 使用 resolve_engine 获取 engine, 再取 registry 补充元信息
                eng = orchestrator.resolve_engine(self.component, self.method)
                ver = 'unknown'
                pri = 0
                try:
                    reg = getattr(orchestrator, 'registry', None)
                    if reg is not None:
                        key = f"{self.component}_{eng}_{self.method}"
                        reg_obj = reg.methods.get(key)
                        if reg_obj:
                            ver = getattr(reg_obj, 'version', 'unknown') or 'unknown'
                            pri = getattr(reg_obj, 'priority', 0) or 0
                except Exception:
                    pass
                return f"{self.method}@{eng}:{ver}:{pri}"
            impls = desc.get('implementations', []) or []
            candidates = [
                {
                    'engine_type': i.get('engine_type'),
                    'version': i.get('version'),
                    'priority': i.get('priority', 0),
                    'deprecated': i.get('deprecated', False)
                } for i in impls
            ]
            active = [c for c in candidates if not c['deprecated']]
            if not active:
                active = candidates
            chosen = None
            # prefer 逻辑复用
            if self.prefer and self.prefer not in ('auto', 'AUTO'):
                chosen = next((c for c in active if c['engine_type'] == self.prefer), None)
            if chosen is None:
                active_sorted = sorted(active, key=lambda x: (x.get('priority', 0), _parse_version(x.get('version'))), reverse=True)
                if active_sorted:
                    chosen = active_sorted[0]
            if not chosen:
                return f"{self.method}@unknown:unknown:0"
            # 记录预测 (供 fastpath 使用，不写入 _resolved_engine)
            try:
                self._last_prediction = {
                    'engine_type': chosen.get('engine_type'),
                    'version': chosen.get('version') or 'unknown',
                    'priority': chosen.get('priority', 0),
                    'candidates': candidates,
                    'ts': time.time()
                }
            except Exception:
                pass
            return f"{self.method}@{chosen.get('engine_type')}:{chosen.get('version') or 'unknown'}:{chosen.get('priority', 0)}"
        except Exception:
            return f"{self.method}@unknown:unknown:0"

    def as_dict(self) -> dict:
        d = {
            'component': self.component,
            'method': self.method,
            'prefer': self.prefer,
            'fixed_engine': self.fixed_engine,
            'resolved': self._resolved_engine,
            'resolved_at': self._resolved_at,
            'ttl': self._ttl
        }
        if self._explain:
            d['selected_reason'] = self._explain.get('selected', {}).get('reason')
        return d

    # ------------------ internal helpers ------------------
    def _is_cache_valid(self) -> bool:
        if self._resolved_engine is None or self._resolved_at is None:
            return False
        if self._ttl <= 0:
            return False
        return (time.time() - self._resolved_at) < self._ttl

    def _perform_resolution(self, orchestrator) -> None:
        ts = time.time()
        reason = []
        try:
            desc = None
            if hasattr(orchestrator, 'describe'):
                desc = orchestrator.describe(self.component, self.method)
            if not desc or desc.get('status') != 'ok':
                # fallback 使用 resolve_engine (旧逻辑)
                engine_type = orchestrator.resolve_engine(self.component, self.method)
                self._resolved_engine = engine_type
                self._resolved_at = ts
                self._explain = {
                    'component': self.component,
                    'method': self.method,
                    'strategy': 'fallback_resolve_engine',
                    'candidates': [],
                    'selected': {'engine_type': engine_type, 'reason': 'fallback'},
                    'ts': ts
                }
                return
            impls: List[Dict[str, Any]] = desc.get('implementations', [])  # type: ignore
            candidates = []
            for impl in impls:
                # copy subset for explain
                candidates.append({
                    'engine_type': impl.get('engine_type'),
                    'version': impl.get('version'),
                    'priority': impl.get('priority', 0),
                    'deprecated': impl.get('deprecated', False),
                })
            active = [c for c in candidates if not c.get('deprecated')]
            if not active:
                reason.append('no_active_candidates_use_all')
                active = candidates[:]  # 全部都 deprecated 仍然可选
            # prefer engine (self.prefer 仅当不是 'auto' 且存在)
            if self.prefer and self.prefer not in ('auto', 'AUTO'):
                pref_hit = next((c for c in active if c['engine_type'] == self.prefer), None)
                if pref_hit:
                    chosen = pref_hit
                    reason.append(f"prefer_engine={self.prefer}")
                else:
                    chosen = None
                    reason.append(f"prefer_missing={self.prefer}")
            else:
                chosen = None
            if chosen is None:
                # 排序: priority -> version
                active_sorted = sorted(active, key=lambda x: (x.get('priority', 0), _parse_version(x.get('version'))), reverse=True)
                if active_sorted:
                    chosen = active_sorted[0]
                    reason.append('rule=priority_version')
            if not chosen:
                raise ValueError('无可选择实现')
            self._resolved_engine = chosen['engine_type']
            self._resolved_at = ts
            self._explain = {
                'component': self.component,
                'method': self.method,
                'strategy': 'default_priority_version',
                'candidates': candidates,
                'selected': {
                    'engine_type': chosen['engine_type'],
                    'version': chosen.get('version'),
                    'priority': chosen.get('priority'),
                    'deprecated': chosen.get('deprecated'),
                    'reason': '+'.join(reason) if reason else 'selected'
                },
                'ts': ts
            }
        except Exception as e:
            self._resolved_engine = None
            self._resolved_at = ts
            self._explain = {
                'component': self.component,
                'method': self.method,
                'strategy': 'error',
                'error': str(e),
                'candidates': [],
                'selected': None,
                'ts': ts
            }

    # ------------------ maintenance utilities ------------------
    def invalidate(self):
        """手动失效当前解析与预测缓存 (用于热更新 / 动态注册后)."""
        with self._lock:
            self._resolved_engine = None
            self._resolved_at = None
            # 保留 explain 历史以便追踪，可选: 置空 -> 这里选择保留
            self._last_prediction = None


__all__ = ["MethodHandle"]
