"""
PGCS Utils: Fingerprint
=======================

内容指纹生成工具。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Optional


def fingerprint(
    data: Any,
    algorithm: str = 'sha256',
    length: int = 12,
) -> str:
    """
    生成数据指纹

    Args:
        data: 要计算指纹的数据
        algorithm: 哈希算法 ('md5', 'sha256', 'sha1')
        length: 指纹长度

    Returns:
        十六进制指纹字符串

    Example:
        fp = fingerprint({'name': 'test', 'value': 123})
        # 'a1b2c3d4e5f6'
    """
    # 序列化数据
    if isinstance(data, str):
        content = data
    elif isinstance(data, bytes):
        content = data.decode('utf-8', errors='replace')
    elif isinstance(data, dict):
        content = json.dumps(data, sort_keys=True, default=str)
    elif hasattr(data, 'to_dict'):
        content = json.dumps(data.to_dict(), sort_keys=True, default=str)
    elif hasattr(data, '__dict__'):
        content = json.dumps(data.__dict__, sort_keys=True, default=str)
    else:
        content = str(data)

    # 计算哈希
    if algorithm == 'md5':
        h = hashlib.md5(content.encode())
    elif algorithm == 'sha1':
        h = hashlib.sha1(content.encode())
    else:
        h = hashlib.sha256(content.encode())

    return h.hexdigest()[:length]


def content_hash(content: str, algorithm: str = 'sha256') -> str:
    """
    计算内容哈希

    Args:
        content: 字符串内容
        algorithm: 哈希算法

    Returns:
        完整哈希值
    """
    if algorithm == 'md5':
        return hashlib.md5(content.encode()).hexdigest()
    elif algorithm == 'sha1':
        return hashlib.sha1(content.encode()).hexdigest()
    else:
        return hashlib.sha256(content.encode()).hexdigest()


def dict_fingerprint(
    data: Dict[str, Any],
    include_keys: Optional[list] = None,
    exclude_keys: Optional[list] = None,
    length: int = 12,
) -> str:
    """
    计算字典指纹

    Args:
        data: 字典数据
        include_keys: 只包含这些键
        exclude_keys: 排除这些键
        length: 指纹长度

    Returns:
        指纹字符串
    """
    filtered = {}

    for key, value in data.items():
        if include_keys is not None and key not in include_keys:
            continue
        if exclude_keys is not None and key in exclude_keys:
            continue
        filtered[key] = value

    return fingerprint(filtered, length=length)


__all__ = [
    'fingerprint',
    'content_hash',
    'dict_fingerprint',
]
