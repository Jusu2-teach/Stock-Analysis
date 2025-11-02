"""MethodHandle - 方法句柄（延迟绑定）

核心功能：
1. 延迟解析：配置时创建，运行时才解析引擎
2. 策略选择：优先级 > 版本 > 非废弃
3. 短期缓存：TTL=5秒（环境变量 ASTOCK_HANDLE_RESOLVE_TTL）
4. 线程安全：RLock 避免并发解析

设计原则：
- 轻量初始化：__init__ 不执行耗时操作
- 智能缓存：避免重复查询 orchestrator
- 职责单一：只负责引擎选择
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
        "_resolved_engine", "_resolved_at", "_explain", "_ttl", "_lock"
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
        self._ttl = float(os.getenv('ASTOCK_HANDLE_RESOLVE_TTL', '5'))

        # 并发锁（避免多线程同时解析）
        from threading import RLock
        self._lock = RLock()

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
        """预测缓存签名（格式: method@engine:version:priority）"""
        # 使用缓存的解析结果
        if self._is_cache_valid() and self._explain:
            sel = self._explain.get('selected')
            if sel:
                eng = sel.get('engine_type', 'unknown')
                ver = sel.get('version', 'unknown')
                pri = sel.get('priority', 0)
                return f"{self.method}@{eng}:{ver}:{pri}"

        # 固定引擎：直接返回
        if self.fixed_engine:
            return f"{self.method}@{self.fixed_engine}:unknown:0"

        # 执行选择逻辑（不持久化到 _resolved_engine）
        try:
            result = self._select_best_implementation(orchestrator)
            chosen = result['chosen']
            return f"{self.method}@{chosen['engine_type']}:{chosen['version']}:{chosen['priority']}"
        except Exception:
            return f"{self.method}@unknown:unknown:0"

    def as_dict(self) -> dict:
        """返回句柄状态（调试用）"""
        return {
            'component': self.component,
            'method': self.method,
            'prefer': self.prefer,
            'fixed_engine': self.fixed_engine,
            'resolved': self._resolved_engine,
            'resolved_at': self._resolved_at
        }

    # ------------------ internal helpers ------------------
    def _is_cache_valid(self) -> bool:
        if self._resolved_engine is None or self._resolved_at is None:
            return False
        if self._ttl <= 0:
            return False
        return (time.time() - self._resolved_at) < self._ttl

    def _select_best_implementation(self, orchestrator) -> Dict[str, Any]:
        """选择最佳实现（通用逻辑）"""
        desc = orchestrator.describe(self.component, self.method)
        if desc.get('status') != 'ok':
            raise ValueError(f'Method not found: {self.component}.{self.method}')

        implementations = desc.get('implementations', [])
        if not implementations:
            raise ValueError(f'No implementations found for {self.component}.{self.method}')

        # 过滤废弃的实现
        candidates = [
            {
                'engine_type': impl['engine_type'],
                'version': impl.get('version', 'unknown'),
                'priority': impl.get('priority', 0),
                'deprecated': impl.get('deprecated', False)
            }
            for impl in implementations
        ]

        active = [c for c in candidates if not c['deprecated']] or candidates

        # 如果指定了偏好引擎，优先使用
        if self.prefer and self.prefer not in ('auto', 'AUTO'):
            preferred = next((c for c in active if c['engine_type'] == self.prefer), None)
            if preferred:
                return {'chosen': preferred, 'candidates': candidates, 'reason': f'prefer={self.prefer}'}

        # 按优先级和版本排序，选择最优
        sorted_impls = sorted(
            active,
            key=lambda x: (x['priority'], _parse_version(x['version'])),
            reverse=True
        )

        return {
            'chosen': sorted_impls[0],
            'candidates': candidates,
            'reason': 'priority_version'
        }

    def _perform_resolution(self, orchestrator) -> None:
        """执行解析并缓存结果"""
        ts = time.time()
        try:
            result = self._select_best_implementation(orchestrator)
            chosen = result['chosen']

            self._resolved_engine = chosen['engine_type']
            self._resolved_at = ts
            self._explain = {
                'component': self.component,
                'method': self.method,
                'strategy': 'default',
                'candidates': result['candidates'],
                'selected': chosen,
                'reason': result['reason'],
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
        """手动失效缓存（用于热更新）"""
        with self._lock:
            self._resolved_engine = None
            self._resolved_at = None
            self._explain = None


__all__ = ["MethodHandle"]
