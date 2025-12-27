"""
PGCS Store: Data Storage
========================

通用数据存储组件。

设计原则:
- 零业务耦合：不包含任何领域特定概念
- 可复用：适用于任何 key-value + 多视图索引场景
- 复用 contracts 现有组件：fingerprint, Metadata, Lineage

核心组件:
- DataEntry: 数据条目
- DataStore: 数据存储
- ReferenceResolver: 引用解析器

Example:
    from shared.contracts.store import DataStore, ReferenceResolver

    # 创建存储
    store = DataStore()

    # 存储数据
    store.put(
        key='my_key',
        value=my_data,
        ref='custom.path.to.data',
    )

    # 获取数据
    data = store.get('my_key')
    data = store.get_by_ref('custom.path.to.data')

    # 引用解析
    resolver = ReferenceResolver(store)
    resolved = resolver.resolve({
        'input': {'__ref__': 'custom.path.to.data'}
    })
"""

from .data_entry import DataEntry
from .data_store import DataStore, SingletonDataStore
from .reference import ReferenceResolver, ReferenceNotFoundError, BatchResolver

__all__ = [
    'DataEntry',
    'DataStore',
    'SingletonDataStore',
    'ReferenceResolver',
    'ReferenceNotFoundError',
    'BatchResolver',
]
