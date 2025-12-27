"""
Pipeline Services: Cache Service
================================

缓存管理服务，从 KedroEngine 提取。

职责:
- 计算节点签名
- 管理签名持久化
- 判断缓存命中
"""

from __future__ import annotations

import json
import hashlib
import pickle
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from shared.contracts.store import DataStore


class CacheService:
    """
    缓存管理服务

    从 KedroEngine 提取的缓存相关逻辑。

    Example:
        cache = CacheService(cache_dir=Path('.pipeline/cache'))

        # 计算签名
        signature = cache.compute_signature(
            step='Load_Data',
            methods=['load_csv'],
            params={'path': 'data.csv'},
            upstream_fingerprints=['input1:abc123'],
        )

        # 检查缓存命中
        if cache.is_cached('Load_Data', signature, ['Load_Data__Raw']):
            # 使用缓存
            pass

        # 保存签名
        cache.save_signature('Load_Data', signature)
    """

    def __init__(
        self,
        cache_dir: Path,
        store: Optional['DataStore'] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """
        初始化缓存服务

        Args:
            cache_dir: 缓存目录
            store: 数据存储（可选，用于检查数据是否存在）
            logger: 日志器
        """
        self._cache_dir = Path(cache_dir)
        self._store = store
        self._logger = logger or logging.getLogger(__name__)

        # 内存中的签名缓存
        self._signatures: Dict[str, str] = {}

        # 确保目录存在
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    # ==================== 签名计算 ====================

    def compute_signature(
        self,
        step: str,
        methods: List[str],
        params: Dict[str, Any],
        upstream_fingerprints: List[str],
        method_meta: str = '',
    ) -> str:
        """
        计算节点执行签名

        签名组成:
        1. 方法链
        2. 方法元信息（版本、优先级等）
        3. 参数
        4. 上游数据指纹

        Args:
            step: 步骤名称
            methods: 方法列表
            params: 参数字典
            upstream_fingerprints: 上游数据指纹列表，格式 ["input:fingerprint", ...]
            method_meta: 方法元信息字符串

        Returns:
            签名字符串
        """
        # 排序参数确保一致性
        param_items = sorted(params.items(), key=lambda x: x[0])
        param_str = str(param_items)

        # 排序上游指纹
        upstream_str = "|".join(sorted(upstream_fingerprints))

        # 组合签名
        components = [
            "|".join(methods),
            method_meta or 'default',
            param_str,
            upstream_str,
        ]

        return "#".join(components)

    def compute_fingerprint(self, data: Any) -> str:
        """
        计算数据指纹

        Args:
            data: 数据对象

        Returns:
            指纹字符串
        """
        try:
            # 尝试使用 contracts 的 fingerprint
            from shared.contracts import fingerprint
            return fingerprint(data, length=16)
        except ImportError:
            # 回退到简单实现
            return self._simple_fingerprint(data)

    def _simple_fingerprint(self, data: Any) -> str:
        """简单的指纹计算"""
        try:
            # 尝试 pickle
            content = pickle.dumps(data)
        except Exception:
            # 回退到 str
            content = str(data).encode()

        return hashlib.sha256(content).hexdigest()[:16]

    # ==================== 缓存判定 ====================

    def is_cached(
        self,
        step: str,
        signature: str,
        outputs: List[str],
        ttl: Optional[float] = None,
    ) -> bool:
        """
        判断是否缓存命中

        条件:
        1. 所有输出都存在
        2. 签名匹配
        3. 未超过 TTL（如果设置了）

        Args:
            step: 步骤名称
            signature: 当前签名
            outputs: 预期输出列表
            ttl: 缓存有效期（秒），None 表示不限制

        Returns:
            是否命中缓存
        """
        # 检查签名
        last_sig = self._signatures.get(step)
        if last_sig != signature:
            return False

        # 检查输出是否存在
        if self._store:
            for output in outputs:
                if not self._store.has(output):
                    return False

        # 检查 TTL
        if ttl is not None and ttl > 0:
            if self._is_ttl_expired(step, ttl):
                return False

        return True

    def _is_ttl_expired(self, step: str, ttl: float) -> bool:
        """检查 TTL 是否过期"""
        sig_file = self._cache_dir / 'node_signatures.json'
        if not sig_file.exists():
            return True

        try:
            import time
            age = time.time() - sig_file.stat().st_mtime
            return age > ttl
        except Exception:
            return True

    # ==================== 持久化 ====================

    def save_signature(self, step: str, signature: str):
        """
        保存节点签名

        Args:
            step: 步骤名称
            signature: 签名
        """
        self._signatures[step] = signature
        self._persist_signatures()

    def load_signatures(self) -> int:
        """
        从磁盘加载签名

        Returns:
            加载的签名数量
        """
        sig_file = self._cache_dir / 'node_signatures.json'
        if not sig_file.exists():
            return 0

        try:
            data = json.loads(sig_file.read_text(encoding='utf-8'))
            self._signatures = data.get('signatures', {})
            return len(self._signatures)
        except Exception as e:
            self._logger.warning(f"签名加载失败: {e}")
            return 0

    def _persist_signatures(self):
        """持久化签名到磁盘"""
        sig_file = self._cache_dir / 'node_signatures.json'
        try:
            data = {
                'signatures': self._signatures,
                'updated_at': datetime.now().isoformat(),
            }
            sig_file.write_text(
                json.dumps(data, indent=2, ensure_ascii=False),
                encoding='utf-8',
            )
        except Exception as e:
            self._logger.warning(f"签名持久化失败: {e}")

    def get_signature(self, step: str) -> Optional[str]:
        """获取步骤签名"""
        return self._signatures.get(step)

    def clear_signature(self, step: str):
        """清除步骤签名"""
        self._signatures.pop(step, None)
        self._persist_signatures()

    def clear_all(self):
        """清除所有签名"""
        self._signatures.clear()
        sig_file = self._cache_dir / 'node_signatures.json'
        if sig_file.exists():
            sig_file.unlink()

    # ==================== 差异分析 ====================

    def analyze_signature_diff(
        self,
        step: str,
        old_signature: str,
        new_signature: str,
    ) -> Dict[str, Any]:
        """
        分析签名差异

        Args:
            step: 步骤名称
            old_signature: 旧签名
            new_signature: 新签名

        Returns:
            差异分析结果
        """
        old_parts = old_signature.split('#')
        new_parts = new_signature.split('#')

        diff = {
            'step': step,
            'changed': old_signature != new_signature,
            'components': {},
        }

        labels = ['methods', 'method_meta', 'params', 'upstream']
        for i, label in enumerate(labels):
            old_val = old_parts[i] if i < len(old_parts) else ''
            new_val = new_parts[i] if i < len(new_parts) else ''
            if old_val != new_val:
                diff['components'][label] = {
                    'old': old_val[:100],  # 截断避免过长
                    'new': new_val[:100],
                }

        return diff
