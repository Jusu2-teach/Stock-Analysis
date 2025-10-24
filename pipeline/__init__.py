#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AStock Pipeline System
=====================

Configuration-driven workflow execution system

Core components:
- ExecuteManager (hybrid orchestration builder)
- PrefectEngine (internal hybrid runtime wrapper around Kedro)
- CLI (pipeline/main.py)

Usage example:
    from pipeline import ExecuteManager, create_pipeline

    # Basic usage
    manager = ExecuteManager()
    manager.load_config('pipeline.yaml')
    result = manager.execute_pipeline()

Author: AStock Team
Version: 1.0.0
"""

from .core.execute_manager import ExecuteManager

from .engines.prefect_engine import PrefectEngine  # hybrid engine

PREFECT_AVAILABLE = True  # always true in trimmed hybrid mode

__version__ = "1.0.0"
__author__ = "AStock Team"

# Export main classes and functions
__all__ = [
    'ExecuteManager',
    'PrefectEngine',
    'get_system_info'
]


def create_pipeline(config_path: str, **kwargs) -> ExecuteManager:  # backward name kept
    mgr = ExecuteManager(config_path)
    mgr.load_config(config_path)
    return mgr


def get_system_info() -> dict:
    """Get system information"""
    return {
        'version': __version__,
        'author': __author__,
        'engines': {'hybrid_prefect_kedro': True}
    }


# validate_config helper removed (legacy API)