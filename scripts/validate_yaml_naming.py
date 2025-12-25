#!/usr/bin/env python3
"""
YAML 配置命名规范验证工具
========================

验证 workflow YAML 文件中的 metric_name 是否符合统一命名规范。

使用方法:
    python scripts/validate_yaml_naming.py workflow/analysis.yaml

输出示例:
    ✅ [Analyze_ROIC_Trend] metric_name='roic' - 规范
    ⚠️ [Analyze_Revenue_Trend] metric_name='total_revenue_ps' - 建议使用 'revenue'
    ❌ [Analyze_Custom_Metric] metric_name='invalid' - 无效指标
"""

import sys
import yaml
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from shared.naming_convention import MetricRegistry


def validate_yaml_metrics(yaml_path: str) -> dict:
    """
    验证 YAML 配置中的 metric_name

    Args:
        yaml_path: YAML 文件路径

    Returns:
        {
            'valid': [...],      # 完全规范的配置
            'warnings': [...],   # 可用但建议改进
            'errors': [...],     # 无效配置
        }
    """
    with open(yaml_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    results = {
        'valid': [],
        'warnings': [],
        'errors': [],
    }

    steps = config.get('pipeline', {}).get('steps', [])

    for step in steps:
        step_name = step.get('name', 'Unknown')
        params = step.get('parameters', {})

        if 'metric_name' not in params:
            continue

        metric_name = params['metric_name']
        is_valid, is_recommended, msg, suggestion = MetricRegistry.validate_metric_name(metric_name)

        item = {
            'step': step_name,
            'metric_name': metric_name,
            'message': msg,
            'suggestion': suggestion,
        }

        if not is_valid:
            results['errors'].append(item)
        elif not is_recommended:
            results['warnings'].append(item)
        else:
            results['valid'].append(item)

    return results


def print_results(results: dict, verbose: bool = False):
    """打印验证结果"""

    total = len(results['valid']) + len(results['warnings']) + len(results['errors'])

    print("\n" + "=" * 70)
    print("📋 YAML metric_name 命名规范验证报告")
    print("=" * 70)

    # 错误
    if results['errors']:
        print(f"\n❌ 无效配置 ({len(results['errors'])} 项):")
        for item in results['errors']:
            print(f"   [{item['step']}] metric_name='{item['metric_name']}'")
            print(f"      → {item['message']}")

    # 警告
    if results['warnings']:
        print(f"\n⚠️ 建议改进 ({len(results['warnings'])} 项):")
        for item in results['warnings']:
            print(f"   [{item['step']}] metric_name='{item['metric_name']}'")
            print(f"      → 建议改为: metric_name: '{item['suggestion']}'")

    # 通过
    if verbose and results['valid']:
        print(f"\n✅ 规范配置 ({len(results['valid'])} 项):")
        for item in results['valid']:
            print(f"   [{item['step']}] metric_name='{item['metric_name']}'")

    # 总结
    print("\n" + "-" * 70)
    print(f"总计: {total} 个 metric_name 配置")
    print(f"  ✅ 规范: {len(results['valid'])}")
    print(f"  ⚠️ 建议改进: {len(results['warnings'])}")
    print(f"  ❌ 无效: {len(results['errors'])}")

    if results['warnings']:
        print("\n💡 建议: 将所有 metric_name 统一为 business_key 格式")
        print("   例如: 'total_revenue_ps' → 'revenue'")
        print("         'grossprofit_margin' → 'gross_margin'")

    return len(results['errors']) == 0


def generate_fix_suggestions(results: dict) -> str:
    """生成修复建议的 YAML 片段"""

    if not results['warnings']:
        return ""

    lines = [
        "\n# ========== 建议的 metric_name 修改 ==========",
        "# 将以下配置替换为推荐的 business_key 格式:",
        ""
    ]

    for item in results['warnings']:
        lines.append(f"# 步骤: {item['step']}")
        lines.append(f"# 原始: metric_name: '{item['metric_name']}'")
        lines.append(f"# 建议: metric_name: '{item['suggestion']}'")
        lines.append("")

    return "\n".join(lines)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("用法: python validate_yaml_naming.py <yaml_file>")
        print("示例: python validate_yaml_naming.py workflow/analysis.yaml")
        sys.exit(1)

    yaml_path = sys.argv[1]
    verbose = '--verbose' in sys.argv or '-v' in sys.argv

    if not Path(yaml_path).exists():
        print(f"错误: 文件不存在: {yaml_path}")
        sys.exit(1)

    results = validate_yaml_metrics(yaml_path)
    success = print_results(results, verbose)

    # 生成修复建议
    suggestions = generate_fix_suggestions(results)
    if suggestions:
        print(suggestions)

    sys.exit(0 if success else 1)
