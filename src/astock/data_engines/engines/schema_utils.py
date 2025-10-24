"""轻量级 Schema 校验工具 (不引入外部依赖)

提供简单的输入列校验与输出字典键校验：
@ensure_columns(required_columns=[...], output_keys=[...])

- 对输入 DataFrame 或 inputs(list[DataFrame]) 检查列是否存在
- 对返回的 dict 检查是否包含指定键
- 所有校验以 WARNING 形式提示，不强制报错（可配置 strict=True 使其报错）
"""
from __future__ import annotations
from typing import List, Callable, Any, Dict
from functools import wraps
import logging
import pandas as pd

logger = logging.getLogger(__name__)


def ensure_columns(required_columns: List[str] | None = None,
                   output_keys: List[str] | None = None,
                   strict: bool = False) -> Callable:
    required_columns = required_columns or []
    output_keys = output_keys or []

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            # -------- 输入校验 --------
            dfs: List[pd.DataFrame] = []
            # 1) 直接参数中的 DataFrame
            for a in args:
                if isinstance(a, pd.DataFrame):
                    dfs.append(a)
            # 2) kwargs 中的 DataFrame 或 inputs list
            for v in kwargs.values():
                if isinstance(v, pd.DataFrame):
                    dfs.append(v)
                elif isinstance(v, (list, tuple)) and v and all(isinstance(x, pd.DataFrame) for x in v):
                    dfs.extend(v)
            if required_columns and dfs:
                checked = 0
                for df in dfs[:2]:  # 只检查前两个主要输入以避免噪音
                    missing = [c for c in required_columns if c not in df.columns]
                    if missing:
                        msg = f"Schema 校验: 缺少列 {missing} (期望: {required_columns})"
                        if strict:
                            raise ValueError(msg)
                        else:
                            logger.warning(msg)
                    else:
                        checked += 1
                if checked == 0:
                    logger.warning("Schema 校验: 未找到可用于列校验的 DataFrame")

            result = func(*args, **kwargs)

            # -------- 输出校验 --------
            if output_keys and isinstance(result, dict):
                missing_out = [k for k in output_keys if k not in result]
                if missing_out:
                    msg = f"输出字典缺少键: {missing_out} (期望: {output_keys})"
                    if strict:
                        raise ValueError(msg)
                    else:
                        logger.warning(msg)
            return result
        # 附加 schema 元信息，便于后续 introspection（不影响 wraps 保留签名）
        wrapper.__schema__ = {
            'required_columns': required_columns,
            'output_keys': output_keys,
            'strict': strict
        }
        return wrapper
    return decorator

__all__ = ["ensure_columns"]
