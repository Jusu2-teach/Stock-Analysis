# Shared 独立模块架构文档

## 概览

`shared/` 目录包含 5 个完全独立的模块系统，遵循开源最佳实践设计。

```
shared/
├── __init__.py           # 主入口 (v7.0)
├── event_bus/            # 事件总线 (已存在 v6.0)
├── contracts/            # PGCS 契约系统 (已存在)
├── naming_convention.py  # 命名规范 (已存在)
│
├── errors/               # ✨ 新增: 统一错误系统
├── logging/              # ✨ 新增: 结构化日志系统
├── cache/                # ✨ 新增: 多层缓存系统
├── config/               # ✨ 新增: 配置管理系统
└── io/                   # ✨ 新增: 统一 I/O 系统
```

## 1. 错误系统 (shared.errors)

**参考设计**: Django, FastAPI, requests, Rich, Sentry

### 文件结构
```
shared/errors/
├── __init__.py       # 模块入口
├── codes.py          # 错误码枚举 (REG-0xx, PIP-1xx, VAL-2xx, DAT-3xx, CFG-4xx)
├── base.py           # AStockError 基类
├── registry.py       # 注册表相关错误
├── pipeline.py       # Pipeline 相关错误
├── validation.py     # 验证相关错误
├── data.py           # 数据相关错误
└── handlers.py       # 错误处理器链
```

### 使用示例
```python
from shared.errors import (
    AStockError,
    ErrorCode,
    MethodNotFoundError,
    DataLoadError,
    error_handler,
)

# 抛出错误
raise MethodNotFoundError("my_method", available=["load", "save"])

# 错误处理装饰器
@error_handler(retry_on=[DataLoadError], max_retries=3)
def load_data():
    ...

# 错误链
try:
    ...
except IOError as e:
    raise DataLoadError("file.csv").with_cause(e)
```

---

## 2. 日志系统 (shared.logging)

**参考设计**: structlog, loguru, Python logging, Sentry

### 文件结构
```
shared/logging/
├── __init__.py       # 模块入口
├── context.py        # 结构化上下文 (contextvars)
├── formatters.py     # 格式化器 (Console, JSON, Colored)
├── handlers.py       # 处理器 (File, Rotating, EventBus, Async)
├── logger.py         # AStockLogger 核心类
├── decorators.py     # @log_call, @timed, LogScope
└── config.py         # 预设配置 (development, production)
```

### 使用示例
```python
from shared.logging import get_logger, configure_logging, log_call, timed

# 配置日志
configure_logging(level="DEBUG", format="colored")

# 获取日志器
logger = get_logger(__name__)
logger.info("Processing", stock="000001", year=2024)

# 装饰器
@log_call(level="INFO")
@timed
def process_data(df):
    ...

# 上下文绑定
with logger.bind(step="Load_Data"):
    logger.info("Loading...")
```

---

## 3. 缓存系统 (shared.cache)

**参考设计**: cachetools, diskcache, joblib, Redis

### 文件结构
```
shared/cache/
├── __init__.py       # 模块入口
├── core.py           # Cache 核心类, CacheKey, CacheStats
├── backends.py       # Memory, Disk, Tiered, Null 后端
├── strategies.py     # LRU, TTL, LFU, Size 策略
├── serializers.py    # Pickle, JSON, Parquet, DataFrame 序列化
├── decorators.py     # @cached, @cached_property
└── config.py         # 预设配置
```

### 使用示例
```python
from shared.cache import Cache, cached, CacheRegion

# 创建缓存
cache = Cache(backend="memory", maxsize=1000)
cache.set("key", value, ttl=3600)
value = cache.get("key")

# 装饰器缓存
@cached(ttl=3600, key_prefix="analysis")
def analyze_stock(code: str, year: int):
    ...

# 区域缓存
region = CacheRegion("analysis", ttl=3600)

@region.cache_on_arguments()
def compute_roic(stock_code):
    ...
```

---

## 4. 配置系统 (shared.config)

**参考设计**: pydantic-settings, dynaconf, hydra, python-dotenv

### 文件结构
```
shared/config/
├── __init__.py       # 模块入口
├── core.py           # Config 核心类, ConfigValue
├── settings.py       # BaseSettings, Field, SecretStr
├── loaders.py        # YAML, JSON, TOML, Env, DotEnv 加载器
├── environment.py    # Environment, EnvironmentType
├── validators.py     # 配置验证器
└── providers.py      # 配置提供者 (Default, File, Env, Chain)
```

### 使用示例
```python
from shared.config import Config, Settings, Field, get_environment

# 简单配置
config = Config.from_yaml("config.yaml")
db_host = config.get("database.host", default="localhost")

# 类型安全配置
class AppSettings(Settings):
    debug: bool = Field(default=False)
    db_url: str = Field(env="DATABASE_URL")
    api_key: SecretStr = Field(env="API_KEY")

    class Config:
        env_prefix = "ASTOCK_"

settings = AppSettings()
print(settings.debug)

# 环境感知
env = get_environment()
if env.is_development:
    config = Config.from_yaml("config.dev.yaml")
```

---

## 5. I/O 系统 (shared.io)

**参考设计**: kedro DataCatalog, fsspec, pandas, polars

### 文件结构
```
shared/io/
├── __init__.py       # 模块入口
├── core.py           # DataSet, DataCatalog 核心类
├── readers.py        # CSV, Parquet, JSON, Excel, SQL 读取器
├── writers.py        # 多格式写入器
├── datasets.py       # 数据集实现 (CSV, Parquet, Polars, DuckDB)
└── paths.py          # PathManager, AStockPaths
```

### 使用示例
```python
from shared.io import (
    read_csv, read_parquet, write_csv,
    DataCatalog, CSVDataSet, ParquetDataSet,
    PathManager, AStockPaths,
)

# 快捷读写
df = read_csv("data/input.csv", engine="polars")
write_parquet(df, "data/output.parquet")

# DataCatalog (kedro 风格)
catalog = DataCatalog()
catalog.register("raw_data", CSVDataSet("data/raw/input.csv"))
catalog.register("processed", ParquetDataSet("data/processed/output.parquet"))

df = catalog.load("raw_data")
catalog.save("processed", df)

# AStock 专用路径
paths = AStockPaths()
raw_10y = paths.raw_10y("20241231_fina_indicator.csv")
middle = paths.filter_middle("roic_trend_analysis.csv")
```

---

## 设计原则

### 1. 零依赖原则
每个模块可独立使用，不强制依赖其他模块。

### 2. 渐进增强
```python
# 最简用法
from shared.logging import get_logger
logger = get_logger(__name__)

# 进阶用法
from shared.logging import configure_logging, PRESET_PRODUCTION
configure_logging(PRESET_PRODUCTION)
```

### 3. 与 EventBus 集成
所有模块都支持与 EventBus 集成：
```python
# 日志事件
logger.add_handler(EventBusHandler())

# 错误事件
error.emit_to_eventbus()

# 缓存事件
cache.on("hit", handler)
cache.on("miss", handler)
```

### 4. 类型安全
所有模块都提供完整的类型注解。

### 5. 预设配置
```python
from shared.logging.config import PRESET_DEVELOPMENT, PRESET_PRODUCTION
from shared.cache.config import PRESET_DEVELOPMENT, PRESET_PRODUCTION
```

---

## 迁移指南

### 替换现有 logging
```python
# 旧代码
import logging
logger = logging.getLogger(__name__)
logger.info("Processing %s", stock_code)

# 新代码
from shared.logging import get_logger
logger = get_logger(__name__)
logger.info("Processing", stock_code=stock_code)
```

### 替换现有错误
```python
# 旧代码
raise ValueError(f"Method {name} not found")

# 新代码
from shared.errors import MethodNotFoundError
raise MethodNotFoundError(name)
```

### 替换现有 I/O
```python
# 旧代码
import pandas as pd
df = pd.read_csv("data/input.csv")
df.to_parquet("data/output.parquet")

# 新代码
from shared.io import read_csv, write_parquet
df = read_csv("data/input.csv")
write_parquet(df, "data/output.parquet")
```

---

## 版本历史

- **v7.0.0**: 新增 errors, logging, cache, config, io 五大独立模块
- **v6.0.0**: EventBus 增强 (HookSpec, DeadLetter, Middleware, Async)
- **v5.0.0**: 命名规范系统 (MetricRegistry, FieldRegistry)
- **v4.0.0**: PGCS 契约系统
