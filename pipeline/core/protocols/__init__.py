"""Pipeline Core Protocols - 接口抽象层"""

from .method_handle_protocol import IMethodHandle, create_method_handle

__all__ = ['IMethodHandle', 'create_method_handle']
