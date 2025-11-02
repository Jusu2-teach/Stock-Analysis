#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AStock Pipeline - Main CLI
=========================

Intelligent configuration-driven workflow system
Pure Prefect+Kedro hybrid architecture

Author: AStock Team
Version: 2.0.0
"""

import argparse
import sys
import json
import os
from pathlib import Path
from typing import Dict, Any, Optional

# --- Robust path/bootstrap handling ---
# Goal: allow BOTH of these invocations without ImportError:
#   1) python -m pipeline.main run -c pipeline/configs/xxx.yaml
#   2) python pipeline/main.py run -c pipeline/configs/xxx.yaml

THIS_FILE = Path(__file__)
PIPELINE_DIR = THIS_FILE.parent               # .../pipeline
PROJECT_ROOT = PIPELINE_DIR.parent            # project root
SRC_DIR = PROJECT_ROOT / 'src'

def _ensure_path(p: Path):
    sp = str(p)
    if sp not in sys.path:
        sys.path.insert(0, sp)

# 优先确保项目根目录和 src 在 sys.path 前列
_ensure_path(PROJECT_ROOT)
if SRC_DIR.exists():
    _ensure_path(SRC_DIR)

# 避免把 pipeline 自身目录直接放在 sys.path 前列（那样会导致顶级包名解析失败）
# 如果用户单文件执行, Python 会自动把项目根目录加入 sys.path[0], 足够找到 pipeline 包。

ExecuteManager = None  # type: ignore
_import_error = None
try:
    # 优先绝对导入 (包形式)
    from pipeline.core.execute_manager import ExecuteManager  # type: ignore
except Exception as e_abs:
    _import_error = e_abs
    # 尝试相对导入 (仅在模块方式执行时有效)
    try:  # pragma: no cover
        from .core.execute_manager import ExecuteManager  # type: ignore
        _import_error = None
    except Exception as e_rel:
        if _import_error is None:
            _import_error = e_rel

if ExecuteManager is None:
    print(f"[ERROR] IMPORT ERROR: {repr(_import_error)}")
    print("📍 解决建议:")
    print("  1) 在项目根目录执行: python -m pipeline.main run -c pipeline/configs/tushare_fina.yaml")
    print("  2) 或确认存在: pipeline/__init__.py (已存在则忽略)")
    print("  3) 不要在其它目录用相对路径调用 (cwd 必须是项目根)")
    sys.exit(1)


class AStockCLI:
    """[AI] AStock Pipeline CLI - Pure Intelligence System"""
    # manager 属性在 __init__ 中动态赋值

    def __init__(self):
        self.manager = None

    def _init_manager(self, config_path: Optional[str] = None) -> None:
        """Initialize execution manager with consistent error handling"""
        try:
            self.manager = ExecuteManager(config_path)
            if config_path:
                self.manager.load_config(config_path)
            print("[OK] SUCCESS: Pipeline manager initialized")
        except Exception as e:
            print(f"[ERROR] ERROR: Manager initialization failed: {e}")
            sys.exit(1)

    def _handle_error(self, operation: str, error: Exception, debug: bool = False) -> None:
        """Unified error handling"""
        print(f"[ERROR] ERROR: {operation} failed: {error}")
        if debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)

    def cmd_run(self, args) -> None:
        """[LAUNCH] Execute pipeline (Hybrid Prefect+Kedro only)"""
        print(f"[RUN] Running Hybrid Pipeline: {args.config}")

        self._init_manager(args.config)

        try:
            # ===== Development Mode Enhancements =====
            # 统一调试与热刷新开关：
            #   ASTOCK_DEBUG=1       -> 所有日志提升到DEBUG
            #   ASTOCK_HOT_RELOAD=1  -> 每次执行前热刷新全部组件（重新扫描+注册）
            debug_on = os.getenv("ASTOCK_DEBUG") == "1"
            hot_reload_on = os.getenv("ASTOCK_HOT_RELOAD") == "1"

            if debug_on:
                import logging
                logging.getLogger().setLevel(logging.DEBUG)
                print("[DEV] ASTOCK_DEBUG=1 -> Root logger set to DEBUG")

            if hot_reload_on:
                try:
                    # 直接使用已初始化的 orchestrator 实例中的 registry
                    if self.manager and self.manager.orchestrator:
                        self.manager.orchestrator.registry.refresh(hot_reload=True)
                        print("[DEV] ASTOCK_HOT_RELOAD=1 -> Components hot reloaded")
                except Exception as hr_err:
                    print(f"[DEV] Hot reload failed: {hr_err}")
            # ==========================================

            # Step filtering: only/exclude/resume must happen before building auto nodes
            steps_list = self.manager.ctx.config.get('pipeline', {}).get('steps', [])
            if getattr(args, 'only', None):
                only_set = {s.strip() for s in args.only.split(',') if s.strip()}
                steps_list = [s for s in steps_list if isinstance(s, dict) and s.get('name') in only_set]
                self.manager.ctx.config['pipeline']['steps'] = steps_list
                print(f"[CFG] Applied --only -> {len(steps_list)} steps")
            if getattr(args, 'exclude', None):
                exclude_set = {s.strip() for s in args.exclude.split(',') if s.strip()}
                steps_list = [s for s in steps_list if isinstance(s, dict) and s.get('name') not in exclude_set]
                self.manager.ctx.config['pipeline']['steps'] = steps_list
                print(f"[CFG] Applied --exclude -> {len(steps_list)} steps")
            if getattr(args, 'resume', None):
                fail_dir = Path('.pipeline') / 'failures'
                failed = []
                if fail_dir.is_dir():
                    for f in fail_dir.glob('*.json'):
                        try:
                            data = json.loads(f.read_text(encoding='utf-8'))
                            step_name = data.get('step')
                            if step_name:
                                failed.append(step_name)
                        except Exception:
                            pass
                if failed:
                    # 依赖感知：补齐失败节点的所有上游链（根据原始 steps 拓扑）
                    original_steps = {s['name']: s for s in steps_list if isinstance(s, dict) and s.get('name')}
                    # 构建依赖图 (基于参数引用已在 manager.steps 解析前，这里只使用 depends_on 字段的显式声明作为近似)
                    # 为准确, 先暂存当前配置, 稍后用 rebuild_after_filter 构建真实拓扑再截取
                    # 简化策略: 临时解析原 steps depends_on 字段
                    dep_map = {n: set(s.get('depends_on', []) or []) for n, s in original_steps.items()}
                    include = set(failed)
                    changed = True
                    while changed:
                        changed = False
                        for node, pres in dep_map.items():
                            if node in include:
                                for p in pres:
                                    if p not in include and p in original_steps:
                                        include.add(p)
                                        changed = True
                    steps_list = [s for s in steps_list if isinstance(s, dict) and s.get('name') in include]
                    self.manager.ctx.config['pipeline']['steps'] = steps_list
                    print(f"[CFG] Resume(dep-aware) -> failed={failed} total_included={len(steps_list)}")
                else:
                    print('[CFG] Resume: no failure snapshots found – running all steps.')

            # 过滤后重建拓扑
            self.manager.rebuild_after_filter()

            # Override granularity if provided
            if getattr(args, 'granularity', None):
                try:
                    pipe_block = self.manager.ctx.config.setdefault('pipeline', {})
                    orch_block = pipe_block.setdefault('orchestration', {})
                    orch_block['granularity'] = args.granularity
                    print(f"[CFG] Override orchestration.granularity={args.granularity}")
                except Exception as _e:
                    print(f"[WARN] Unable to apply granularity override: {_e}")

            # 始终使用混合引擎
            result = self.manager.execute_pipeline()

            print("🎉 SUCCESS: Pipeline execution completed!")
            print(f"[DATA] Status: {result['status']} (mode={result.get('mode','hybrid')})")
            # 修正: 使用 executed_steps 统计真实执行的步骤数量
            step_count = len(result.get('executed_steps', []))
            print(f"📈 Steps: {step_count}")
            # 附加: 展示已注册输出对象数量（调试/观察数据血缘）
            if 'outputs' in result and isinstance(result['outputs'], dict):
                reg_size = result['outputs'].get('registry_size')
                if reg_size is not None:
                    print(f"🧬 Registered Outputs: {reg_size}")

            # Save results if requested
            if args.output:
                output_path = Path(args.output)
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(result, f, ensure_ascii=False, indent=2, default=str)
                print(f"💾 Results saved: {output_path}")

        except Exception as e:
            self._handle_error("Pipeline execution", e, args.debug)

    # （已彻底移除 validate / deprecated 命令实现）

    def cmd_cache(self, args) -> None:
        from core.execute_manager import ExecuteManager
        if args.action == 'clear':
            ExecuteManager.clear_cache()
            print('[CACHE] 已清理缓存目录')
        elif args.action == 'warm':
            if not getattr(args, 'config', None):
                print('[CACHE] warm 需要 -c 配置文件')
                sys.exit(1)
            ExecuteManager.clear_cache()
            self._init_manager(args.config)
            self.manager.execute_pipeline()
            print('[CACHE] 预热完成')
        elif args.action == 'plan':
            if not getattr(args, 'config', None):
                print('[CACHE] plan 需要 -c 配置文件')
                sys.exit(1)
            self._init_manager(args.config)
            auto_nodes_info = self.manager._build_auto_kedro_config()
            from engines.kedro_engine import KedroEngine
            ke = KedroEngine(self.manager)
            # 不执行 create_pipeline（那会创建真实节点），仅加载持久化签名 & datasets 即可判断
            plan = []
            for node_cfg in auto_nodes_info['nodes']:
                step_name = node_cfg['name']
                outputs = node_cfg.get('outputs', []) or []
                sig = ke.node_signatures.get(step_name)
                all_outputs_cached = bool(outputs) and all(o in ke.global_catalog for o in outputs)
                predicted_hit = all_outputs_cached and sig is not None
                plan.append({
                    'step': step_name,
                    'outputs': outputs,
                    'signature_cached': bool(sig),
                    'all_outputs_cached': all_outputs_cached,
                    'predicted_action': 'skip (cache hit)' if predicted_hit else 'execute'
                })
            print('[CACHE PLAN] 预测执行计划:')
            for item in plan:
                print(f" - {item['step']}: {item['predicted_action']} (outputs={len(item['outputs'])} cached={item['all_outputs_cached']} sig={item['signature_cached']})")
            print(f"[CACHE PLAN] total steps={len(plan)} to_execute={sum(1 for x in plan if x['predicted_action']=='execute')} cache_hits={sum(1 for x in plan if x['predicted_action'].startswith('skip'))}")
            sys.exit(0)
        else:
            print(f'[CACHE] 未知操作: {args.action}')
            sys.exit(1)
        sys.exit(0)

    def cmd_metrics(self, args) -> None:
        # 运行一次（或读取缓存）后输出指标与关键路径
        self._init_manager(args.config)
        result = self.manager.execute_pipeline()
        cache_metrics = result.get('metrics', {}).get('cache', {})
        lineage = result.get('lineage', {}) or {}
        # 关键路径: 简单按累计 duration 拓扑上游最长
        durations = {k: v.get('duration_sec', 0.0) for k, v in lineage.items()}
        # 构建依赖图: lineage.inputs -> node
        graph = {k: set(lineage[k].get('inputs', [])) for k in lineage}
        memo = {}
        def longest(n):
            if n in memo:
                return memo[n]
            if not graph.get(n):
                memo[n] = (durations.get(n,0.0), [n])
                return memo[n]
            best = (durations.get(n,0.0), [n])
            for pre in graph[n]:
                if pre in durations:
                    acc_path = longest(pre)
                    cand = (acc_path[0] + durations.get(n,0.0), acc_path[1] + [n])
                    if cand[0] > best[0]:
                        best = cand
            memo[n] = best
            return best
        critical_path = None
        if durations:
            critical_path = max((longest(n) for n in durations), key=lambda x: x[0])
        total_time = round(sum(durations.values()),4)
        avg_time = round(total_time/len(durations),4) if durations else 0.0
        top_n = args.top if getattr(args, 'top', None) else 5
        top_nodes = sorted(durations.items(), key=lambda x: x[1], reverse=True)[:top_n]
        if args.format == 'text':
            print('[METRICS] Cache:', cache_metrics)
            if critical_path:
                print(f"[METRICS] CriticalPathDuration={round(critical_path[0],4)} path={' -> '.join(critical_path[1])}")
            print(f"[METRICS] Nodes={len(durations)} Steps={len(result.get('executed_steps', []))} TotalTime={total_time}s AvgNode={avg_time}s")
            print(f"[METRICS] Top{top_n} Nodes:")
            for name, dur in top_nodes:
                pct = f"{(dur/total_time*100):.1f}%" if total_time else '0%'
                cached_flag = lineage.get(name, {}).get('cached')
                print(f"  - {name}: {round(dur,4)}s ({pct}) cached={cached_flag}")
        elif args.format == 'json':
            payload = {
                'cache': cache_metrics,
                'critical_path': {
                    'duration_sec': round(critical_path[0],4) if critical_path else None,
                    'nodes': critical_path[1] if critical_path else []
                },
                'summary': {
                    'node_count': len(durations),
                    'step_count': len(result.get('executed_steps', [])),
                    'total_time_sec': total_time,
                    'avg_node_time_sec': avg_time
                },
                'top_nodes': [
                    {
                        'name': name,
                        'duration_sec': round(dur,4),
                        'percent': (dur/total_time) if total_time else 0,
                        'cached': lineage.get(name, {}).get('cached')
                    } for name, dur in top_nodes
                ]
            }
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        elif args.format == 'markdown':
            lines = []
            lines.append(f"## Pipeline Metrics\n")
            lines.append(f"**Cache**: hits={cache_metrics.get('hits')} miss={cache_metrics.get('miss')} hit_rate={cache_metrics.get('hit_rate')}\n")
            if critical_path:
                lines.append(f"**Critical Path** ({round(critical_path[0],4)}s): {' → '.join(critical_path[1])}\n")
            lines.append(f"**Summary**: nodes={len(durations)} steps={len(result.get('executed_steps', []))} total={total_time}s avg={avg_time}s\n")
            lines.append(f"### Top {top_n} Slow Nodes\n")
            lines.append("| Node | Duration(s) | % | Cached |\n|------|-------------|----|--------|")
            for name, dur in top_nodes:
                pct = f"{(dur/total_time*100):.1f}%" if total_time else '0%'
                cached_flag = lineage.get(name, {}).get('cached')
                lines.append(f"| {name} | {round(dur,4)} | {pct} | {cached_flag} |")
            print("\n".join(lines))
        if args.export:
            out = Path(args.export)
            out.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
            print(f"[METRICS] Full result exported -> {out}")
        sys.exit(0)

    # ---- minimal status / engines (保留用户可用) ----
    def cmd_status(self, args) -> None:
        try:
            self._init_manager()
            engines = self.manager.get_available_engines()
            comp_cnt = len(engines.get('components', []))
            method_cnt = len(engines.get('methods', {}))
            print(f"[STATUS] components={comp_cnt} methods={method_cnt} mode=hybrid-only")
        except Exception as e:
            print(f"[STATUS] error: {e}")
            sys.exit(1)

    def cmd_engines(self, args) -> None:
        try:
            self._init_manager()
            engines = self.manager.get_available_engines()
            for k,v in engines.get('methods', {}).items():
                print(f"• {k} -> {v.get('engine_type')}")
        except Exception as e:
            print(f"[ENGINES] error: {e}")
            sys.exit(1)


def create_parser() -> argparse.ArgumentParser:
    """Create enhanced argument parser"""
    parser = argparse.ArgumentParser(
        description='[AI] AStock Pipeline - Pure Intelligence System',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s run -c config.yaml              # Execute pipeline
  %(prog)s cache warm -c config.yaml       # Warm cache
  %(prog)s cache clear                     # Clear cache
        """
    )
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # run command - simplified and focused
    run_parser = subparsers.add_parser('run', help='[LAUNCH] Execute intelligent pipeline')
    run_parser.add_argument('--config', '-c', required=True, help='Configuration file path')
    run_parser.add_argument('--output', '-o', help='Save results to file')
    run_parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    run_parser.add_argument('--granularity', choices=['pipeline', 'node'], help='Override orchestration granularity (pipeline|node)')
    run_parser.add_argument('--resume', action='store_true', help='Resume from failed steps only')
    run_parser.add_argument('--only', help='Comma separated step names to include')
    run_parser.add_argument('--exclude', help='Comma separated step names to exclude')

    # status command - comprehensive
    status_parser = subparsers.add_parser('status', help='[DATA] System status')

    # engines command - detailed
    engines_parser = subparsers.add_parser('engines', help='[TOOL] Available engines')
    engines_parser.add_argument('--verbose', '-v', action='store_true', help='Detailed information')


    # cache command - new utility
    cache_parser = subparsers.add_parser('cache', help='cache ops: clear | warm | plan')
    cache_parser.add_argument('action', choices=['clear','warm','plan'])
    cache_parser.add_argument('-c','--config')

    metrics_parser = subparsers.add_parser('metrics', help='Show execution & cache metrics')
    metrics_parser.add_argument('-c','--config', required=True)
    metrics_parser.add_argument('--export', help='Export full result JSON')
    metrics_parser.add_argument('--top', type=int, help='Show top N slow nodes (default 5)')
    metrics_parser.add_argument('--format', choices=['text','json','markdown'], default='text', help='Output format (text|json|markdown)')

    return parser


def main() -> None:
    """[LAUNCH] Main entry point for AStock Pipeline"""
    parser = create_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    # Initialize CLI
    cli = AStockCLI()

    # Command dispatch
    try:
        command_map = {
            'run': cli.cmd_run,
            'status': cli.cmd_status,
            'engines': cli.cmd_engines,
            'cache': cli.cmd_cache,
            'metrics': cli.cmd_metrics,
        }

        if args.command in command_map:
            command_map[args.command](args)
        else:
            print(f"[ERROR] ERROR: Unknown command: {args.command}")
            parser.print_help()
            sys.exit(1)

    except KeyboardInterrupt:
        print("\n⏹️  Operation interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"[ERROR] FATAL ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()