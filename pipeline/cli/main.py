"""Pipeline CLI - Command Line Interface
========================================

Pipeline 2.0 命令行工具。

版本: 2.0.0
"""

from __future__ import annotations

import argparse
import importlib
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Type

from ..protocols import MethodResolverProtocol

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%H:%M:%S',
)

logger = logging.getLogger(__name__)


# =============================================================================
# MethodResolver 插件加载机制
# =============================================================================

# 默认 resolver 类路径（可通过环境变量覆盖）
DEFAULT_METHOD_RESOLVER = "orchestrator.adapters.RegistryMethodResolver"
ENV_METHOD_RESOLVER = "PIPELINE_METHOD_RESOLVER"


def load_method_resolver() -> MethodResolverProtocol:
    """动态加载 MethodResolver 实现

    加载顺序（优先级从高到低）：
    1. 环境变量 PIPELINE_METHOD_RESOLVER 指定的类路径
    2. 默认实现 orchestrator.adapters.RegistryMethodResolver

    类路径格式: "module.path.ClassName"

    Returns:
        MethodResolverProtocol 实现实例

    Raises:
        ImportError: 无法导入指定的模块
        AttributeError: 模块中找不到指定的类
        TypeError: 类不符合 MethodResolverProtocol 协议

    Example:
        # 使用默认实现
        resolver = load_method_resolver()

        # 使用自定义实现（通过环境变量）
        # export PIPELINE_METHOD_RESOLVER=mypackage.resolvers.CustomResolver
        resolver = load_method_resolver()
    """
    class_path = os.environ.get(ENV_METHOD_RESOLVER, DEFAULT_METHOD_RESOLVER)

    try:
        module_path, class_name = class_path.rsplit(".", 1)
        module = importlib.import_module(module_path)
        resolver_class = getattr(module, class_name)
        resolver = resolver_class()

        # 运行时协议检查
        if not isinstance(resolver, MethodResolverProtocol):
            raise TypeError(
                f"Loaded resolver '{class_path}' does not implement MethodResolverProtocol. "
                f"Required methods: resolve(), list_methods()"
            )

        logger.debug(f"Loaded MethodResolver: {class_path}")
        return resolver

    except ImportError as e:
        raise ImportError(
            f"Cannot import MethodResolver from '{class_path}'. "
            f"Ensure the module is installed and the path is correct. "
            f"Original error: {e}"
        ) from e
    except AttributeError as e:
        raise AttributeError(
            f"Module '{module_path}' does not have class '{class_name}'. "
            f"Check the class name in {ENV_METHOD_RESOLVER} or use the default."
        ) from e


def create_parser() -> argparse.ArgumentParser:
    """创建命令行解析器"""
    parser = argparse.ArgumentParser(
        prog='pipeline',
        description='Pipeline 2.0 - Enterprise Workflow Engine',
    )

    parser.add_argument(
        '--version', '-V',
        action='version',
        version='Pipeline 2.0.0',
    )

    parser.add_argument(
        '--verbose', '-v',
        action='count',
        default=0,
        help='Increase verbosity (can be repeated)',
    )

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # run 命令
    run_parser = subparsers.add_parser('run', help='Execute a workflow')
    run_parser.add_argument(
        '-c', '--config',
        required=True,
        help='Path to workflow YAML config',
    )
    run_parser.add_argument(
        '--only',
        nargs='+',
        default=argparse.SUPPRESS,
        help='Only run these tasks (and their dependencies)',
    )
    run_parser.add_argument(
        '--exclude',
        nargs='+',
        default=argparse.SUPPRESS,
        help='Exclude these tasks from execution',
    )
    run_parser.add_argument(
        '--resume-from',
        dest='resume_from',
        default=argparse.SUPPRESS,
        help='Resume from a specific task (run that task and its downstream only)',
    )
    run_parser.add_argument(
        '--soft-fail',
        action='store_true',
        default=argparse.SUPPRESS,
        help='Continue execution even if tasks fail',
    )
    run_parser.add_argument(
        '--parallel',
        action='store_true',
        default=argparse.SUPPRESS,
        help='Enable parallel execution',
    )
    run_parser.add_argument(
        '--workers',
        type=int,
        default=argparse.SUPPRESS,
        help='Number of parallel workers (default: 4)',
    )
    run_parser.add_argument(
        '--dry-run',
        action='store_true',
        default=argparse.SUPPRESS,
        help='Validate config without executing',
    )

    run_parser.add_argument(
        '--no-skip-cached',
        dest='no_skip_cached',
        action='store_true',
        default=argparse.SUPPRESS,
        help='Do not skip execution on cache hits (still allows writing cache)',
    )
    run_parser.add_argument(
        '--output', '-o',
        help='Output file for run results (JSON)',
    )

    # validate 命令
    validate_parser = subparsers.add_parser('validate', help='Validate workflow config')
    validate_parser.add_argument(
        '-c', '--config',
        required=True,
        help='Path to workflow YAML config',
    )
    validate_parser.add_argument(
        '--strict',
        action='store_true',
        help='Enable strict validation',
    )

    # graph 命令
    graph_parser = subparsers.add_parser('graph', help='Visualize dependency graph')
    graph_parser.add_argument(
        '-c', '--config',
        required=True,
        help='Path to workflow YAML config',
    )
    graph_parser.add_argument(
        '--format',
        choices=['ascii', 'dot', 'json'],
        default='ascii',
        help='Output format (default: ascii)',
    )
    graph_parser.add_argument(
        '--output', '-o',
        help='Output file',
    )

    # engines 命令
    engines_parser = subparsers.add_parser('engines', help='List registered methods')
    engines_parser.add_argument(
        '--format',
        choices=['table', 'json'],
        default='table',
        help='Output format',
    )

    # cache 命令
    cache_parser = subparsers.add_parser('cache', help='Cache management')
    cache_parser.add_argument(
        '--clear',
        action='store_true',
        help='Clear all cached data',
    )
    cache_parser.add_argument(
        '--stats',
        action='store_true',
        help='Show cache statistics',
    )

    return parser


def cmd_run(args: argparse.Namespace) -> int:
    """执行工作流"""
    from ..config import load_flow
    from ..execution import FlowRunner, RunnerConfig
    from ..core.container import Container, get_container

    try:
        # 动态加载 MethodResolver（支持通过环境变量配置）
        method_resolver = load_method_resolver()

        # 加载配置
        logger.info(f"Loading workflow: {args.config}")
        flow_spec = load_flow(args.config)

        if getattr(args, 'dry_run', False):
            logger.info(f"Dry run: {flow_spec.name}")
            logger.info(f"  Tasks: {len(flow_spec.tasks)}")
            for task in flow_spec.tasks:
                logger.info(f"    - {task.name}: {task.method}")
            return 0

        # 配置运行器：仅当 CLI 显式提供时才覆盖 YAML orchestration。
        # 优先级由 FlowRunner 统一解析并打印：CLI/代码 > YAML orchestration > defaults。
        config_kwargs: Dict[str, Any] = {}

        if hasattr(args, 'parallel') and args.parallel:
            config_kwargs['execution_mode'] = 'parallel'

        if hasattr(args, 'workers'):
            config_kwargs['max_workers'] = args.workers

        if hasattr(args, 'soft_fail'):
            config_kwargs['soft_fail'] = args.soft_fail

        if hasattr(args, 'dry_run'):
            config_kwargs['dry_run'] = args.dry_run

        if hasattr(args, 'no_skip_cached') and args.no_skip_cached:
            config_kwargs['skip_cached'] = False

        if hasattr(args, 'resume_from'):
            config_kwargs['resume_from'] = args.resume_from

        if hasattr(args, 'only'):
            config_kwargs['only_tasks'] = set(args.only) if args.only else set()

        if hasattr(args, 'exclude'):
            config_kwargs['exclude_tasks'] = set(args.exclude) if args.exclude else set()

        config = RunnerConfig(**config_kwargs)

        # 创建运行器 (使用依赖注入)
        container = get_container()

        runner = FlowRunner(
            container=container,
            method_resolver=method_resolver,
            config=config,
        )

        # 执行
        logger.info(f"Starting workflow: {flow_spec.name}")
        flow_run = runner.run(flow_spec)

        # 输出结果
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(flow_run.to_dict(), f, indent=2)
            logger.info(f"Results written to: {args.output}")

        # 返回码
        if flow_run.state.name == 'SUCCESS':
            return 0
        elif flow_run.state.name == 'PARTIAL_SUCCESS':
            return 1
        else:
            return 2

    except Exception as e:
        logger.error(f"Failed to run workflow: {e}", exc_info=args.verbose > 0)
        return 1

def cmd_validate(args: argparse.Namespace) -> int:
    """验证工作流配置"""
    from ..config import load_flow
    from ..core.dag import DAG

    try:
        logger.info(f"Validating: {args.config}")

        # 加载配置
        flow_spec = load_flow(args.config)

        # 构建 DAG (检测循环依赖)
        dag = DAG.from_flow_spec(flow_spec)
        plan = dag.get_execution_plan()

        logger.info(f"✓ Config valid: {flow_spec.name}")
        logger.info(f"  Tasks: {plan.total_tasks}")
        logger.info(f"  Layers: {plan.total_layers}")
        logger.info(f"  Critical path: {' → '.join(plan.critical_path)}")

        return 0

    except Exception as e:
        logger.error(f"✗ Validation failed: {e}", exc_info=args.verbose > 0)
        return 1


def cmd_graph(args: argparse.Namespace) -> int:
    """可视化依赖图"""
    from ..config import load_flow
    from ..core.dag import DAG

    try:
        flow_spec = load_flow(args.config)
        dag = DAG.from_flow_spec(flow_spec)

        if args.format == 'ascii':
            output = dag.visualize_ascii()
        elif args.format == 'json':
            output = json.dumps(dag.to_dict(), indent=2)
        elif args.format == 'dot':
            # Graphviz DOT 格式
            lines = ['digraph G {', '  rankdir=LR;']
            for task in flow_spec.tasks:
                for dep in dag.get_dependencies(task.name):
                    lines.append(f'  "{dep}" -> "{task.name}";')
            lines.append('}')
            output = '\n'.join(lines)

        if args.output:
            with open(args.output, 'w') as f:
                f.write(output)
            logger.info(f"Graph written to: {args.output}")
        else:
            print(output)

        return 0

    except Exception as e:
        logger.error(f"Failed to generate graph: {e}", exc_info=args.verbose > 0)
        return 1


def cmd_engines(args: argparse.Namespace) -> int:
    """列出已注册的方法"""
    try:
        # 尝试从 orchestrator 加载注册表
        try:
            from orchestrator.registry import get_registry
            registry = get_registry()
            methods = registry.list_methods()
        except ImportError:
            methods = {}
            logger.warning("Orchestrator registry not available")

        if args.format == 'json':
            print(json.dumps(methods, indent=2))
        else:
            print("\nRegistered Methods:")
            print("=" * 60)
            for name, info in methods.items():
                print(f"  {name}")
                if isinstance(info, dict):
                    for k, v in info.items():
                        print(f"    {k}: {v}")
            print()

        return 0

    except Exception as e:
        logger.error(f"Failed to list engines: {e}")
        return 1


def cmd_cache(args: argparse.Namespace) -> int:
    """缓存管理"""
    from ..cache import create_cache_backend

    try:
        cache = create_cache_backend('tiered')

        if args.clear:
            count = cache.clear()
            logger.info(f"Cleared {count} cached entries")

        if args.stats:
            stats = cache.get_stats()
            print(json.dumps(stats, indent=2))

        return 0

    except Exception as e:
        logger.error(f"Cache operation failed: {e}")
        return 1


def main(argv: Optional[List[str]] = None) -> int:
    """CLI 入口点"""
    parser = create_parser()
    args = parser.parse_args(argv)

    # 设置日志级别
    if args.verbose >= 2:
        logging.getLogger().setLevel(logging.DEBUG)
    elif args.verbose >= 1:
        logging.getLogger().setLevel(logging.INFO)

    # 分发命令
    if args.command == 'run':
        return cmd_run(args)
    elif args.command == 'validate':
        return cmd_validate(args)
    elif args.command == 'graph':
        return cmd_graph(args)
    elif args.command == 'engines':
        return cmd_engines(args)
    elif args.command == 'cache':
        return cmd_cache(args)
    else:
        parser.print_help()
        return 0


if __name__ == '__main__':
    sys.exit(main())
