"""Pipeline Config - YAML Loader
================================

YAML 配置加载和解析。

版本: 2.0.0
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Set

import yaml

from ..core.spec import (
    FlowSpec,
    TaskSpec,
    TaskInputSpec,
    TaskOutputSpec,
    TaskPolicies,
    FlowDefaults,
    FlowOrchestration,
)
from ..core.policy import (
    RetryPolicy,
    CachePolicy,
    TimeoutPolicy,
    FailurePolicy,
    FailureStrategy,
    AggregationPolicy,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Schema 验证
# =============================================================================

class ConfigSchemaError(ValueError):
    """配置 Schema 验证错误"""


class ConfigSchemaValidator:
    """配置 Schema 验证器

    验证 YAML 配置的结构和必填字段。

    支持的验证:
    - 必填字段检查
    - 字段类型检查
    - 未知字段警告
    - 枚举值验证
    """

    # 顶层必填字段
    REQUIRED_TOP_LEVEL: Set[str] = {'steps'}

    # 顶层允许的字段
    ALLOWED_TOP_LEVEL: Set[str] = {
        'name', 'pipeline', 'steps', 'tasks', 'defaults', 'orchestration',
        'description', 'version', 'metadata',
    }

    # 步骤必填字段 (name 必需, method 强烈建议但某些场景可省略)
    REQUIRED_STEP_FIELDS: Set[str] = {'name'}
    RECOMMENDED_STEP_FIELDS: Set[str] = {'method'}  # 警告但不报错

    # 步骤允许的字段
    ALLOWED_STEP_FIELDS: Set[str] = {
        'name', 'component', 'engine', 'method', 'inputs', 'outputs',
        'depends_on', 'policies', 'tags', 'description', 'metadata', 'parameters',
    }

    # orchestration.granularity 允许的值 (与 FlowOrchestration 保持一致)
    ALLOWED_GRANULARITY: Set[str] = {'task', 'layer'}

    # orchestration.task_runner 允许的值 (与 FlowOrchestration 保持一致)
    ALLOWED_TASK_RUNNER: Set[str] = {'sequential', 'threaded', 'async'}

    # policies.failure.strategy 允许的值
    ALLOWED_FAILURE_STRATEGY: Set[str] = {'fail_flow', 'skip_downstream', 'continue'}

    # cache.backend 允许的值
    ALLOWED_CACHE_BACKEND: Set[str] = {'memory', 'file', 'tiered', 'redis', 'none'}

    def __init__(self, strict: bool = True, warn_unknown: bool = True):
        """
        Args:
            strict: 严格模式，缺少必填字段时抛出异常
            warn_unknown: 发现未知字段时输出警告
        """
        self._strict = strict
        self._warn_unknown = warn_unknown
        self._errors: List[str] = []
        self._warnings: List[str] = []

    def validate(self, data: Dict[str, Any], source: str = "") -> bool:
        """验证配置

        Args:
            data: 解析后的 YAML 数据
            source: 配置来源（用于错误消息）

        Returns:
            验证是否通过

        Raises:
            ConfigSchemaError: 严格模式下验证失败
        """
        self._errors = []
        self._warnings = []

        # 支持 pipeline: 包装格式
        if 'pipeline' in data and isinstance(data['pipeline'], dict):
            data = data['pipeline']

        # 验证顶层
        self._validate_top_level(data)

        # 验证 steps
        steps = data.get('steps', data.get('tasks', []))
        if not steps:
            self._errors.append("Config must have at least one step/task")
        else:
            for i, step in enumerate(steps):
                self._validate_step(step, i)

        # 验证 orchestration
        if 'orchestration' in data:
            self._validate_orchestration(data['orchestration'])

        # 验证 defaults
        if 'defaults' in data:
            self._validate_defaults(data['defaults'])

        # 处理结果
        for warning in self._warnings:
            logger.warning(f"[Config Validation] {warning}")

        if self._errors:
            error_msg = f"Config validation failed ({source}):\n" + "\n".join(
                f"  - {e}" for e in self._errors
            )
            if self._strict:
                raise ConfigSchemaError(error_msg)
            else:
                logger.error(error_msg)
                return False

        return True

    def _validate_top_level(self, data: Dict[str, Any]) -> None:
        """验证顶层字段"""
        # 检查必填字段
        has_steps = 'steps' in data or 'tasks' in data
        if not has_steps:
            self._errors.append("Missing required field: 'steps' or 'tasks'")

        # 检查未知字段
        if self._warn_unknown:
            unknown = set(data.keys()) - self.ALLOWED_TOP_LEVEL
            for field in unknown:
                self._warnings.append(f"Unknown top-level field: '{field}'")

    def _validate_step(self, step: Dict[str, Any], index: int) -> None:
        """验证单个步骤"""
        step_name = step.get('name', f'step[{index}]')

        # 检查必填字段
        for field in self.REQUIRED_STEP_FIELDS:
            if field not in step:
                self._errors.append(f"Step '{step_name}': missing required field '{field}'")

        # 检查推荐字段 (警告但不报错)
        for field in self.RECOMMENDED_STEP_FIELDS:
            if field not in step:
                self._warnings.append(
                    f"Step '{step_name}': missing recommended field '{field}'"
                )

        # 验证步骤策略
        self._validate_step_policies(step, step_name)

        # 检查未知字段（参数字段除外）
        if self._warn_unknown:
            # 允许任意 parameters 字段和直接写在 step 下的参数（简写语法）
            known = self.ALLOWED_STEP_FIELDS | set(step.get('parameters', {}).keys())
            unknown = set(step.keys()) - known - {'parameters'}

            # 过滤掉可能是简写参数的字段（非保留字段名）
            # 简写语法允许直接在 step 下写参数，如 metric_name: 'roic'
            for field in unknown:
                # 如果字段名不像是配置字段（不含下划线前缀的特殊字段），可能是参数
                if not field.startswith('_'):
                    self._warnings.append(
                        f"Step '{step_name}': unknown field '{field}' "
                        f"(may be a shorthand parameter)"
                    )

    def _validate_step_policies(self, step: Dict[str, Any], step_name: str) -> None:
        """验证步骤策略配置"""
        policies = step.get('policies', {})

        # 验证 retry 策略
        if 'retry' in policies:
            retry = policies['retry']
            if 'max_attempts' in retry:
                if not isinstance(retry['max_attempts'], int) or retry['max_attempts'] < 1:
                    self._errors.append(
                        f"Step '{step_name}': retry.max_attempts must be a positive integer"
                    )
            if 'backoff' in retry:
                allowed_backoff = {'none', 'linear', 'exponential', 'fibonacci'}
                if retry['backoff'] not in allowed_backoff:
                    self._errors.append(
                        f"Step '{step_name}': invalid retry.backoff '{retry['backoff']}'. "
                        f"Allowed: {allowed_backoff}"
                    )

        # 验证 cache 策略
        if 'cache' in policies:
            cache = policies['cache']
            if 'backend' in cache:
                if cache['backend'] not in self.ALLOWED_CACHE_BACKEND:
                    self._errors.append(
                        f"Step '{step_name}': invalid cache.backend '{cache['backend']}'. "
                        f"Allowed: {self.ALLOWED_CACHE_BACKEND}"
                    )

        # 验证 failure 策略
        if 'failure' in policies:
            failure = policies['failure']
            if 'strategy' in failure:
                if failure['strategy'] not in self.ALLOWED_FAILURE_STRATEGY:
                    self._errors.append(
                        f"Step '{step_name}': invalid failure.strategy '{failure['strategy']}'. "
                        f"Allowed: {self.ALLOWED_FAILURE_STRATEGY}"
                    )

    def _validate_orchestration(self, orch: Dict[str, Any]) -> None:
        """验证编排配置"""
        if 'granularity' in orch:
            if orch['granularity'] not in self.ALLOWED_GRANULARITY:
                self._errors.append(
                    f"Invalid orchestration.granularity: '{orch['granularity']}'. "
                    f"Allowed: {self.ALLOWED_GRANULARITY}"
                )

        if 'task_runner' in orch:
            if orch['task_runner'] not in self.ALLOWED_TASK_RUNNER:
                self._errors.append(
                    f"Invalid orchestration.task_runner: '{orch['task_runner']}'. "
                    f"Allowed: {self.ALLOWED_TASK_RUNNER}"
                )

        if 'max_parallelism' in orch:
            if not isinstance(orch['max_parallelism'], int) or orch['max_parallelism'] < 1:
                self._errors.append(
                    "orchestration.max_parallelism must be a positive integer"
                )

    def _validate_defaults(self, defaults: Dict[str, Any]) -> None:
        """验证默认配置"""
        if 'cache' in defaults:
            cache = defaults['cache']
            if 'backend' in cache:
                if cache['backend'] not in self.ALLOWED_CACHE_BACKEND:
                    self._errors.append(
                        f"Invalid defaults.cache.backend: '{cache['backend']}'. "
                        f"Allowed: {self.ALLOWED_CACHE_BACKEND}"
                    )

        if 'failure' in defaults:
            failure = defaults['failure']
            if 'strategy' in failure:
                if failure['strategy'] not in self.ALLOWED_FAILURE_STRATEGY:
                    self._errors.append(
                        f"Invalid defaults.failure.strategy: '{failure['strategy']}'. "
                        f"Allowed: {self.ALLOWED_FAILURE_STRATEGY}"
                    )


# =============================================================================
# 环境变量解析
# =============================================================================

_ENV_PATTERN = re.compile(r'\$\{([^}]+)\}')


def resolve_env_vars(value: str) -> str:
    """解析环境变量

    支持格式: ${VAR_NAME} 或 ${VAR_NAME:default}
    """
    def replacer(match):
        var_expr = match.group(1)
        if ':' in var_expr:
            var_name, default = var_expr.split(':', 1)
        else:
            var_name, default = var_expr, ''

        return os.environ.get(var_name, default)

    return _ENV_PATTERN.sub(replacer, value)


def resolve_env_in_dict(data: Any) -> Any:
    """递归解析字典中的环境变量"""
    if isinstance(data, str):
        return resolve_env_vars(data)
    elif isinstance(data, dict):
        return {k: resolve_env_in_dict(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [resolve_env_in_dict(v) for v in data]
    return data


# =============================================================================
# YAML 解析器
# =============================================================================

@dataclass
class LoaderConfig:
    """加载器配置"""
    resolve_env: bool = True
    strict_mode: bool = True  # 严格模式：缺少必填字段报错
    validate_schema: bool = True  # 是否进行 Schema 验证
    warn_unknown_fields: bool = True  # 是否警告未知字段


class YAMLLoader:
    """YAML 配置加载器

    从 YAML 文件加载并解析为 FlowSpec。

    特性：
    - 环境变量解析 (${VAR_NAME} 或 ${VAR_NAME:default})
    - Schema 验证（可配置严格/宽松模式）
    - 支持 pipeline: 包装格式
    - 支持简写语法

    Example:
        loader = YAMLLoader()
        flow_spec = loader.load("workflow/analysis.yaml")
    """

    def __init__(self, config: Optional[LoaderConfig] = None):
        self._config = config or LoaderConfig()
        self._validator = ConfigSchemaValidator(
            strict=self._config.strict_mode,
            warn_unknown=self._config.warn_unknown_fields,
        )

    def load(self, path: Union[str, Path]) -> FlowSpec:
        """加载 YAML 文件

        Args:
            path: YAML 文件路径

        Returns:
            FlowSpec 对象
        """
        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        with open(path, 'r', encoding='utf-8') as f:
            raw_data = yaml.safe_load(f)

        # 解析环境变量
        if self._config.resolve_env:
            raw_data = resolve_env_in_dict(raw_data)

        # Schema 验证
        if self._config.validate_schema:
            self._validator.validate(raw_data, source=str(path))

        # 解析为 FlowSpec
        return self._parse_flow(raw_data, source_file=str(path))

    def load_string(self, yaml_content: str) -> FlowSpec:
        """从字符串加载

        Args:
            yaml_content: YAML 内容

        Returns:
            FlowSpec 对象
        """
        raw_data = yaml.safe_load(yaml_content)

        if self._config.resolve_env:
            raw_data = resolve_env_in_dict(raw_data)

        # Schema 验证
        if self._config.validate_schema:
            self._validator.validate(raw_data, source="<string>")

        return self._parse_flow(raw_data)

    def _parse_flow(
        self,
        data: Dict[str, Any],
        source_file: str = "",
    ) -> FlowSpec:
        """解析流程配置"""
        # 支持 pipeline: 包装格式
        if 'pipeline' in data and isinstance(data['pipeline'], dict):
            data = data['pipeline']

        # 获取流程名称
        name = data.get('name', data.get('metadata', {}).get('name', 'unnamed'))

        # 解析默认配置
        defaults = self._parse_defaults(data.get('defaults', {}))

        # 解析编排配置
        orchestration = self._parse_orchestration(data.get('orchestration', {}))

        # 解析任务 - 'steps' 是首选键名，'tasks' 已弃用
        if 'tasks' in data and 'steps' not in data:
            import warnings
            warnings.warn(
                "Using 'tasks' key is deprecated, please use 'steps' instead. "
                "This will be removed in a future version.",
                DeprecationWarning,
                stacklevel=3,
            )
            steps_data = data.get('tasks', [])
        else:
            steps_data = data.get('steps', [])

        tasks = []
        for step_data in steps_data:
            task_spec = self._parse_task(step_data, defaults)
            tasks.append(task_spec)

        return FlowSpec(
            name=name,
            tasks=tuple(tasks),
            defaults=defaults,
            orchestration=orchestration,
            description=data.get('description', ''),
            version=data.get('version', '1.0.0'),
            metadata={
                'source_file': source_file,
                **data.get('metadata', {}),
            },
        )

    def _parse_task(
        self,
        data: Dict[str, Any],
        defaults: FlowDefaults,
    ) -> TaskSpec:
        """解析任务配置"""
        name = data.get('name', '')

        if not name:
            raise ValueError("Task name is required")

        # 解析输入
        inputs = []
        for inp_data in data.get('inputs', []):
            if isinstance(inp_data, dict):
                inputs.append(TaskInputSpec(
                    name=inp_data.get('name', ''),
                    source=inp_data.get('source'),
                    type_hint=inp_data.get('type', 'Any'),
                    required=inp_data.get('required', True),
                    default=inp_data.get('default'),
                ))
            elif isinstance(inp_data, str):
                inputs.append(TaskInputSpec(name=inp_data))

        # 解析输出
        outputs = []
        for out_data in data.get('outputs', {}).get('parameters', []):
            if isinstance(out_data, dict):
                outputs.append(TaskOutputSpec(
                    name=out_data.get('name', ''),
                    type_hint=out_data.get('type', 'Any'),
                    primary=out_data.get('primary', False),
                ))
            elif isinstance(out_data, str):
                outputs.append(TaskOutputSpec(name=out_data))

        # 解析参数 - 从 data 本身获取，排除保留字段
        reserved_keys = {
            'name', 'component', 'engine', 'method', 'inputs', 'outputs',
            'depends_on', 'policies', 'tags', 'description', 'metadata',
        }
        parameters = {
            k: v for k, v in data.get('parameters', {}).items()
        }

        # 支持直接在任务中写参数（简写语法）
        for k, v in data.items():
            if k not in reserved_keys and k != 'parameters':
                if k not in parameters:
                    parameters[k] = v

        # 解析策略
        policies = self._parse_policies(data.get('policies', {}), defaults)

        # 解析依赖
        depends_on = data.get('depends_on', [])
        if isinstance(depends_on, str):
            depends_on = [depends_on]

        # 方法名可能是列表或字符串
        method = data.get('method', '')
        if isinstance(method, list):
            if len(method) > 1:
                logger.warning(
                    f"Step '{name}': method is a list with {len(method)} items, "
                    f"only the first one ('{method[0]}') will be used"
                )
            method = method[0] if method else ''

        return TaskSpec(
            name=name,
            component=data.get('component', 'business_engine'),
            engine=data.get('engine', ''),
            method=method,
            inputs=tuple(inputs),
            outputs=tuple(outputs),
            parameters=parameters,
            depends_on=frozenset(depends_on),
            policies=policies,
            tags=frozenset(data.get('tags', [])),
            description=data.get('description', ''),
            metadata=data.get('metadata', {}),
        )

    def _parse_defaults(self, data: Dict[str, Any]) -> FlowDefaults:
        """解析默认配置"""
        return FlowDefaults(
            retry=self._parse_retry_policy(data.get('retry', {})),
            cache=self._parse_cache_policy(data.get('cache', {})),
            timeout=self._parse_timeout_policy(data.get('timeout', {})),
            failure=self._parse_failure_policy(data.get('failure', {})),
        )

    def _parse_orchestration(self, data: Dict[str, Any]) -> FlowOrchestration:
        """解析编排配置"""
        return FlowOrchestration(
            granularity=data.get('granularity', 'task'),
            soft_fail=data.get('soft_fail', False),
            max_parallelism=data.get('max_parallelism', data.get('max_workers', 4)),
            task_runner=data.get('task_runner', 'sequential'),
        )

    def _parse_policies(
        self,
        data: Dict[str, Any],
        defaults: FlowDefaults,
    ) -> TaskPolicies:
        """解析任务策略

        改进: 跟踪哪些策略被显式配置，确保用户设置不被默认值覆盖。
        """
        # 收集显式配置的策略名称
        configured_policies = []

        if 'retry' in data:
            configured_policies.append('retry')
            retry = self._parse_retry_policy(data['retry'])
        else:
            retry = defaults.retry

        if 'cache' in data:
            configured_policies.append('cache')
            cache = self._parse_cache_policy(data['cache'])
        else:
            cache = defaults.cache

        if 'timeout' in data:
            configured_policies.append('timeout')
            timeout = self._parse_timeout_policy(data['timeout'])
        else:
            timeout = defaults.timeout

        if 'failure' in data:
            configured_policies.append('failure')
            failure = self._parse_failure_policy(data['failure'])
        else:
            failure = defaults.failure

        if 'aggregation' in data:
            configured_policies.append('aggregation')
            aggregation = self._parse_aggregation_policy(data['aggregation'])
        else:
            from ..core.policy import AggregationPolicy
            aggregation = AggregationPolicy()

        return TaskPolicies(
            retry=retry,
            cache=cache,
            timeout=timeout,
            failure=failure,
            aggregation=aggregation,
            _configured_policies=frozenset(configured_policies),
        )

    def _parse_retry_policy(self, data: Dict[str, Any]) -> RetryPolicy:
        """解析重试策略"""
        return RetryPolicy(
            max_attempts=data.get('max_attempts', 1),
            delay_seconds=data.get('delay_seconds', data.get('delay', 0.0)),
            backoff=data.get('backoff', 'none'),
            backoff_multiplier=data.get('backoff_multiplier', 2.0),
            max_delay_seconds=data.get('max_delay_seconds', 300.0),
            jitter_seconds=data.get('jitter_seconds', 0.0),
        )

    def _parse_cache_policy(self, data: Dict[str, Any]) -> CachePolicy:
        """解析缓存策略"""
        return CachePolicy(
            enabled=data.get('enabled', False),
            ttl_seconds=data.get('ttl_seconds', data.get('ttl')),
            backend=data.get('backend', 'memory'),
            key_prefix=data.get('key_prefix', ''),
            include_params=tuple(data.get('include_params', [])) or None,
            exclude_params=tuple(data.get('exclude_params', [])),
            invalidate_on_failure=data.get('invalidate_on_failure', False),
        )

    def _parse_timeout_policy(self, data: Dict[str, Any]) -> TimeoutPolicy:
        """解析超时策略"""
        return TimeoutPolicy(
            timeout_seconds=data.get('timeout_seconds', data.get('seconds')),
            soft_timeout=data.get('soft_timeout', False),
        )

    def _parse_failure_policy(self, data: Dict[str, Any]) -> FailurePolicy:
        """解析失败策略"""
        strategy_str = data.get('strategy', 'fail_flow')
        strategy_map = {
            'fail_flow': FailureStrategy.FAIL_FLOW,
            'skip_downstream': FailureStrategy.SKIP_DOWNSTREAM,
            'continue': FailureStrategy.CONTINUE,
        }

        return FailurePolicy(
            strategy=strategy_map.get(strategy_str, FailureStrategy.FAIL_FLOW),
            allowed_failures=data.get('allowed_failures', 0),
        )

    def _parse_aggregation_policy(self, data: Dict[str, Any]) -> AggregationPolicy:
        """解析聚合策略"""
        return AggregationPolicy(
            enabled=data.get('enabled', True),
            namespace=data.get('namespace', 'default'),
            collect_as_producer=data.get('collect_as_producer', True),
            inject_as_consumer=data.get('inject_as_consumer', True),
            consumer_param_name=data.get('consumer_param_name', 'aggregated_data'),
        )


# =============================================================================
# 便捷函数
# =============================================================================

def load_flow(path: Union[str, Path]) -> FlowSpec:
    """加载流程配置"""
    return YAMLLoader().load(path)


def load_flow_string(yaml_content: str) -> FlowSpec:
    """从字符串加载流程配置"""
    return YAMLLoader().load_string(yaml_content)
