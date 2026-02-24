"""
OOS Validation Framework v1.0
Out-of-Sample 验证框架: 独立、通用、可反复运行

验证策略:
    1. Monte Carlo Parameter Perturbation — 参数扰动稳定性
    2. Company Bootstrap Resampling     — 公司自举稳定性
    3. Factor / Weight Ablation         — 因子消融影响分析
    4. Cross-Engine Consistency          — 双引擎一致性深度分析

运行:
    python -m oos_validation               # 完整运行 (默认配置)
    python -m oos_validation --fast        # 快速模式 (减少迭代)
    python -m oos_validation --only perturb  # 仅运行扰动策略
"""

__version__ = "1.0.0"
