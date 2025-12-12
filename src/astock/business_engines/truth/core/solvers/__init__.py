"""
T.R.U.T.H. System - Solvers Module
==================================

三大物理求解器，每个求解器独立文件：

- gravity_solver.py: 重力求解器（ROIC/ROE阈值）- 上帝方程 I
- velocity_solver.py: 速度求解器（营收/利润增速阈值）- 上帝方程 II
- structure_solver.py: 结构求解器（毛利率趋势阈值）- 上帝方程 III

使用方法：
```python
from .solvers import gravity_solver, velocity_solver, structure_solver
from .solvers import create_gravity_result, create_velocity_result, create_structure_result
```
"""

from .gravity_solver import (
    gravity_solver,
    GravitySolverResult,
    create_gravity_result,
)
from .velocity_solver import (
    velocity_solver,
    VelocitySolverResult,
    create_velocity_result,
)
from .structure_solver import (
    structure_solver,
    StructureSolverResult,
    create_structure_result,
)

__all__ = [
    # 求解器函数
    "gravity_solver",
    "velocity_solver",
    "structure_solver",

    # 工厂函数
    "create_gravity_result",
    "create_velocity_result",
    "create_structure_result",

    # 结果类
    "GravitySolverResult",
    "VelocitySolverResult",
    "StructureSolverResult",
]
