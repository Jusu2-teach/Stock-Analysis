# AStock Analysis System

![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![Architecture](https://img.shields.io/badge/Architecture-Modular-green)
![Engine](https://img.shields.io/badge/Engine-DuckDB%20%7C%20Polars-orange)

AStock Analysis 是一个现代化、高性能的股票数据分析系统。它采用 **Orchestrator (调度器) + Pipeline (编排器) + Business Plugins (业务插件)** 的分层架构，旨在提供极致的灵活性、解耦性和扩展性。

---

## 🌟 核心架构

系统经过深度重构，由三大独立且松耦合的模块组成：

### 1. Orchestrator (核心调度层)
系统的“大脑”和服务总线。
-   **自动发现 (Auto-Discovery)**: 引入智能 `Scanner`，业务代码无需手动注册，遵循约定即可自动挂载。
-   **统一门面 (Facade)**: 提供统一的 API 调用接口，屏蔽底层实现细节。
-   **策略路由**: 支持多种执行策略（如优先使用最新版本、特定引擎等）。

### 2. Pipeline (流程编排层)
系统的“骨架”和执行引擎。
-   **混合引擎 (Hybrid Engine)**: 结合了 **Prefect** 的任务调度能力和 **Kedro** 的数据流管理能力。
-   **配置驱动**: 通过 YAML 文件定义复杂的工作流 (`workflow/*.yaml`)。
-   **健壮性**: 支持断点续传 (`--resume`)、缓存机制、依赖自动解析和详细的执行指标。

### 3. AStock Business Engines (业务逻辑层)
系统的“血肉”，纯净的业务实现。
-   **零侵入性**: 业务代码（如 `src/astock`）完全独立，不依赖框架代码。
-   **高性能**: 内置 DuckDB 和 Polars 引擎，支持 SQL 级和向量化的高速计算。
-   **即插即用**: 编写普通 Python 函数即可被系统识别为业务能力。

---

## 🚀 快速开始

### 环境要求
- Python 3.9+
- 依赖包: 见 `requirements.txt`

### 安装
```bash
pip install -r requirements.txt
```

### 常用命令

CLI 入口统一为 `pipeline/main.py`。

#### 1. 查看系统状态
检查组件加载情况和可用方法数量。
```bash
python pipeline/main.py status
```

#### 2. 运行工作流
执行定义在 YAML 中的数据分析流程。
```bash
python pipeline/main.py run -c workflow/duckdb_screen.yaml
```

#### 3. 查看可用引擎
列出所有已注册的业务方法及其对应的引擎实现。
```bash
python pipeline/main.py engines
```

#### 4. 性能指标与缓存
查看执行性能分析或管理缓存。
```bash
python pipeline/main.py metrics -c workflow/duckdb_screen.yaml
python pipeline/main.py cache clear
```

---

## 💻 开发指南

### 如何添加新的业务逻辑？

得益于 **"约定优于配置"** 的设计，添加新功能极其简单：

1.  **编写函数**: 在 `src/astock/business_engines/engines/` 下新建文件（例如 `my_logic.py`），编写普通的 Python 函数。
    ```python
    # src/astock/business_engines/engines/my_logic.py
    def calculate_alpha(df):
        """计算 Alpha 因子"""
        return df['close'] - df['open']
    ```

2.  **自动扫描**: 在包的 `__init__.py` 中配置扫描。
    ```python
    # src/astock/business_engines/__init__.py
    from orchestrator import Registry
    from .engines import my_logic

    Registry.get().scan(
        module=my_logic,
        component_type="business_engine",
        engine_type="duckdb"
    )
    ```

3.  **完成！** 系统会自动发现并注册 `calculate_alpha` 方法，你可以在 Pipeline YAML 中直接使用。

### Pipeline 配置示例

```yaml
pipeline:
  name: "My Analysis Flow"
  steps:
    - name: load_data
      method: business_engine.duckdb.load_file  # 调用自动注册的方法
      parameters:
        path: "data/source.parquet"
      outputs:
        - raw_data

    - name: compute_alpha
      method: business_engine.duckdb.calculate_alpha
      inputs:
        df: raw_data
```

---

## 📂 目录结构

```text
AStock-Analysis/
├── orchestrator/       # [Core] 调度与注册中心 (Scanner, Registry)
├── pipeline/           # [Core] 流程执行引擎 (CLI, Prefect/Kedro)
├── src/
│   └── astock/         # [Plugin] 业务逻辑插件包
│       └── business_engines/
│           ├── engines/    # 具体算法实现 (DuckDB/Polars)
│           └── __init__.py # 自动扫描配置
├── workflow/           # [Config] YAML 工作流定义
├── data/               # 数据目录
└── requirements.txt    # 项目依赖
```

## 📄 License

MIT License
