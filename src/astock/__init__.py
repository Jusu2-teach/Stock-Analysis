"""
AStock主模块 - 智能股票分析系统
===============================

基于智能Orchestrator架构：

🧠 核心组件：
- DataHub: 数据资源管理器 (akshare等数据源)
- DataEngines: 数据处理引擎 (pandas数据清理、验证、异常检测)
- BusinessEngines: 业务逻辑引擎 (财务分析、风险评估、投资评级)

� 架构说明：
- Orchestrator 已移至根目录，与 pipeline 平级
- 如需使用 Orchestrator，请使用: from orchestrator import AStockOrchestrator

🚀 核心特性：
- 完全动态组件发现
- 自动注册和管理
- 零硬编码架构
- 热插拔组件支持
- 企业级设计
"""

# 版本信息（与 pyproject.toml 对齐）
__version__ = "1.0.0"
__author__ = "Your Name"

# 公开接口
__all__ = [
    # 组件包
    'datahub',
    'data_engines',
    'business_engines',
]

# 说明：Orchestrator 已独立
# 使用方式: from orchestrator import AStockOrchestrator, register_method