"""
基因可视化模块 (Genome Visualizer)
====================================

根据设计文档第十三章Phase 4要求实现：
1. plot_genome_radar() - 6维基因雷达图
2. generate_genome_interpretation() - 基因文字解读
3. export_to_report() - 导出到报告

设计文档参考：
- 决策6：6维基因可视化
- 可解释性(Explainability)：让用户理解为什么公司被筛选/淘汰
- 异常检测：雷达图形状异常一眼可见

作者: AStock Analysis System
日期: 2025-12-10
"""

import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from pathlib import Path
import base64
import io

# 可视化库 - 使用懒加载避免导入错误
_plt = None
_HAS_MATPLOTLIB = None


def _get_plt():
    """懒加载matplotlib"""
    global _plt, _HAS_MATPLOTLIB
    if _HAS_MATPLOTLIB is None:
        try:
            import matplotlib.pyplot as plt
            import matplotlib
            matplotlib.use('Agg')  # 非交互式后端
            plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
            plt.rcParams['axes.unicode_minus'] = False
            _plt = plt
            _HAS_MATPLOTLIB = True
        except ImportError:
            _HAS_MATPLOTLIB = False
    return _plt if _HAS_MATPLOTLIB else None


from .models import CompanyGenome, TruthResult


# =============================================================================
# 基因雷达图生成
# =============================================================================

@dataclass
class GenomeVisualization:
    """基因可视化结果"""
    ts_code: str
    company_name: str

    # 雷达图数据
    radar_figure: Optional[Any] = None  # matplotlib Figure对象
    radar_base64: Optional[str] = None  # Base64编码的图片
    radar_svg: Optional[str] = None  # SVG格式

    # 文字解读
    interpretation: str = ""

    # 摘要指标
    genome_summary: Dict[str, Any] = None


def plot_genome_radar(
    genome: CompanyGenome,
    company_name: str,
    save_path: Optional[str] = None,
    return_base64: bool = True
) -> GenomeVisualization:
    """
    生成6维基因雷达图

    根据设计文档决策6实现：
    - 可解释性：直观展示公司6维基因
    - 异常检测：形状异常一眼可见

    Args:
        genome: 公司基因组
        company_name: 公司名称
        save_path: 可选保存路径
        return_base64: 是否返回Base64编码

    Returns:
        GenomeVisualization: 包含图表和解读的完整可视化结果
    """
    plt = _get_plt()

    result = GenomeVisualization(
        company_code=genome.ts_code,
        company_name=company_name,
        genome_summary={}
    )

    # 提取6维基因数据
    categories = ['α 周期性', 'β 资本密度', 'γ 成长动能',
                  'δf 欺诈风险', 'δd 衰退风险', 'V 真相验证']
    values = [
        genome.alpha,           # α 周期性
        genome.beta,            # β 资本密度
        genome.gamma,           # γ 成长动能
        genome.delta_fraud,     # δf 欺诈风险
        genome.delta_decay,     # δd 衰退风险
        genome.verification,    # V 真相验证
    ]

    # 存储摘要
    result.genome_summary = {
        'alpha': genome.alpha,
        'beta': genome.beta,
        'gamma': genome.gamma,
        'delta_fraud': genome.delta_fraud,
        'delta_decay': genome.delta_decay,
        'verification': genome.verification,
    }

    if plt is None:
        # 无matplotlib时返回文本可视化
        result.interpretation = generate_genome_interpretation(genome)
        result.radar_svg = _generate_text_radar(categories, values, company_name)
        return result

    # 创建雷达图
    angles = np.linspace(0, 2 * np.pi, len(categories), endpoint=False).tolist()
    values_plot = values + values[:1]  # 闭合
    angles += angles[:1]

    fig, ax = plt.subplots(figsize=(10, 10), subplot_kw=dict(polar=True))

    # 填充区域 - 使用渐变色
    ax.fill(angles, values_plot, color='steelblue', alpha=0.25, label='基因值')
    ax.plot(angles, values_plot, color='steelblue', linewidth=2)

    # 添加数据点标记
    for angle, value, cat in zip(angles[:-1], values, categories):
        ax.scatter([angle], [value], c='steelblue', s=80, zorder=5)

        # 标注具体数值
        offset = 0.12 if value < 0.9 else -0.12
        ax.annotate(f'{value:.2f}', xy=(angle, value),
                    xytext=(angle, value + offset),
                    ha='center', va='center', fontsize=10, fontweight='bold')

    # 添加基准线 - 行业中性值(0.5)
    reference_values = [0.5] * (len(categories) + 1)
    ax.plot(angles, reference_values, color='gray', linewidth=1,
            linestyle='--', alpha=0.5, label='中性基准(0.5)')

    # 设置刻度和标签
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(categories, fontsize=12, fontweight='bold')
    ax.set_ylim(0, 1.1)
    ax.set_yticks([0.2, 0.4, 0.6, 0.8, 1.0])
    ax.set_yticklabels(['0.2', '0.4', '0.6', '0.8', '1.0'], fontsize=9)

    # 标题和图例
    ax.set_title(f'{company_name} ({genome.ts_code})\n六维基因图谱',
                 fontsize=16, fontweight='bold', pad=20)
    ax.legend(loc='upper right', bbox_to_anchor=(1.15, 1.1))

    # 添加风险标识
    _add_risk_annotations(ax, genome, angles)

    plt.tight_layout()

    # 保存或转换
    result.radar_figure = fig

    if save_path:
        fig.savefig(save_path, dpi=150, bbox_inches='tight',
                    facecolor='white', edgecolor='none')

    if return_base64:
        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=150, bbox_inches='tight',
                    facecolor='white', edgecolor='none')
        buf.seek(0)
        result.radar_base64 = base64.b64encode(buf.read()).decode('utf-8')
        buf.close()

    # 生成解读
    result.interpretation = generate_genome_interpretation(genome)

    plt.close(fig)

    return result


def _add_risk_annotations(ax, genome: CompanyGenome, angles: List[float]):
    """添加风险标识到雷达图"""
    # 欺诈风险熔断警示
    if genome.delta_fraud >= 0.58:
        ax.annotate('⚠️ 熔断!', xy=(angles[3], genome.delta_fraud),
                    xytext=(angles[3] + 0.3, genome.delta_fraud + 0.15),
                    fontsize=12, color='red', fontweight='bold',
                    arrowprops=dict(arrowstyle='->', color='red'))

    # 高衰退风险警示
    if genome.delta_decay > 0.7:
        ax.annotate('⚠️ 高衰退', xy=(angles[4], genome.delta_decay),
                    xytext=(angles[4] - 0.3, genome.delta_decay + 0.1),
                    fontsize=10, color='orange', fontweight='bold')

    # 验证异常警示
    if genome.verification < 0.3:
        ax.annotate('❌ 验证失败', xy=(angles[5], genome.verification),
                    xytext=(angles[5] + 0.2, genome.verification + 0.15),
                    fontsize=10, color='red', fontweight='bold')


def _generate_text_radar(categories: List[str], values: List[float], company_name: str) -> str:
    """无matplotlib时生成文本雷达图"""
    lines = [
        f"📊 {company_name} 六维基因图谱",
        "=" * 50,
        ""
    ]

    max_bar_len = 30
    for cat, val in zip(categories, values):
        bar_len = int(val * max_bar_len)
        bar = "█" * bar_len + "░" * (max_bar_len - bar_len)
        lines.append(f"{cat:12} [{bar}] {val:.2f}")

    lines.append("")
    lines.append("=" * 50)

    return "\n".join(lines)


# =============================================================================
# 基因文字解读
# =============================================================================

def generate_genome_interpretation(genome: CompanyGenome) -> str:
    """
    生成基因解读文字

    根据设计文档决策6实现：
    - 周期性解读
    - 资本密度解读
    - 成长性解读
    - 风险因子解读
    - 验证状态解读

    Args:
        genome: 公司基因组

    Returns:
        str: 完整的基因解读文字
    """
    lines = []
    lines.append(f"### {genome.ts_code} 基因解读")
    lines.append("")

    # === α 周期性解读 ===
    lines.append("#### 🔄 周期性特征 (α)")
    if genome.alpha > 0.7:
        lines.append(f"- **α = {genome.alpha:.2f}** → ⚡ **强周期特征**")
        lines.append("- 业绩波动大，受经济周期影响显著")
        lines.append("- 投资建议：需关注周期位置，避免在周期顶部买入")
        lines.append("- 阈值调整：T.R.U.T.H.系统自动应用周期豁免，降低ROIC要求")
    elif genome.alpha > 0.4:
        lines.append(f"- **α = {genome.alpha:.2f}** → 📊 **中等周期特征**")
        lines.append("- 业绩有一定波动，但相对可预测")
        lines.append("- 投资建议：关注行业景气度变化")
    else:
        lines.append(f"- **α = {genome.alpha:.2f}** → 🛡️ **防御特征**")
        lines.append("- 业绩稳定，受经济周期影响小")
        lines.append("- 投资建议：适合长期持有，可作为底仓配置")
    lines.append("")

    # === β 资本密度解读 ===
    lines.append("#### 🏭 资本密度 (β)")
    if genome.beta > 0.7:
        lines.append(f"- **β = {genome.beta:.2f}** → 🏭 **重资产模式**")
        lines.append("- 固定成本高，经营杠杆大")
        lines.append("- 投资建议：需关注产能利用率和折旧压力")
        lines.append("- 阈值调整：T.R.U.T.H.系统施加资本密度惩罚")
    elif genome.beta > 0.4:
        lines.append(f"- **β = {genome.beta:.2f}** → ⚖️ **混合资产模式**")
        lines.append("- 资产结构平衡，具有一定灵活性")
        lines.append("- 投资建议：关注资产周转效率")
    else:
        lines.append(f"- **β = {genome.beta:.2f}** → 💰 **轻资产模式**")
        lines.append("- 现金流优质，具有'印钞机'属性")
        lines.append("- 投资建议：优先考虑，ROE/ROIC通常较高")
    lines.append("")

    # === γ 成长动能解读 ===
    lines.append("#### 🚀 成长动能 (γ)")
    if genome.gamma > 0.7:
        lines.append(f"- **γ = {genome.gamma:.2f}** → 🚀 **高成长期**")
        lines.append("- 营收/利润快速扩张")
        lines.append("- 投资建议：需验证现金流是否跟上利润增长")
        lines.append("- 阈值调整：T.R.U.T.H.系统保持中性（不额外奖励，防追涨）")
    elif genome.gamma > 0.4:
        lines.append(f"- **γ = {genome.gamma:.2f}** → 📈 **稳健成长期**")
        lines.append("- 保持健康增长节奏")
        lines.append("- 投资建议：理想的成长股候选")
    elif genome.gamma > 0.2:
        lines.append(f"- **γ = {genome.gamma:.2f}** → 📊 **成熟期**")
        lines.append("- 增长放缓，进入稳态")
        lines.append("- 投资建议：关注分红和回购政策")
    else:
        lines.append(f"- **γ = {genome.gamma:.2f}** → 📉 **衰退/转型期**")
        lines.append("- 增长明显放缓或下滑")
        lines.append("- 投资建议：除非有反转信号，否则谨慎")
        lines.append("- 阈值调整：T.R.U.T.H.系统施加成长衰退惩罚")
    lines.append("")

    # === δ_fraud 欺诈风险解读 ===
    lines.append("#### 🚨 欺诈风险 (δ_fraud)")
    if genome.delta_fraud >= 0.58:
        lines.append(f"- **δ_fraud = {genome.delta_fraud:.2f}** → 🔴 **熔断触发！**")
        lines.append("- **⚠️ 严重警告：存在显著财务异常**")
        lines.append("- 特征：OCF与利润严重背离，或M-Score异常")
        lines.append("- 投资建议：**立即回避，不参与**")
        lines.append("- T.R.U.T.H.处理：直接淘汰，不参与任何筛选")
    elif genome.delta_fraud > 0.4:
        lines.append(f"- **δ_fraud = {genome.delta_fraud:.2f}** → 🟡 **中等风险**")
        lines.append("- 存在一定财务异常信号")
        lines.append("- 投资建议：需深入研究财报，理解异常原因")
    else:
        lines.append(f"- **δ_fraud = {genome.delta_fraud:.2f}** → 🟢 **低风险**")
        lines.append("- 财务数据一致性良好")
        lines.append("- 投资建议：基本面可信")
    lines.append("")

    # === δ_decay 衰退风险解读 ===
    lines.append("#### 📉 衰退风险 (δ_decay)")
    if genome.delta_decay > 0.7:
        lines.append(f"- **δ_decay = {genome.delta_decay:.2f}** → 🔴 **高衰退风险**")
        lines.append("- 业务持续恶化，多项指标连续下滑")
        lines.append("- 投资建议：除非判断是周期底部，否则回避")
        lines.append("- 阈值调整：T.R.U.T.H.系统施加衰退熵惩罚")
    elif genome.delta_decay > 0.4:
        lines.append(f"- **δ_decay = {genome.delta_decay:.2f}** → 🟡 **中等衰退风险**")
        lines.append("- 部分指标出现恶化迹象")
        lines.append("- 投资建议：密切关注后续财报")
    else:
        lines.append(f"- **δ_decay = {genome.delta_decay:.2f}** → 🟢 **低衰退风险**")
        lines.append("- 业务健康，无明显恶化信号")
        lines.append("- 投资建议：可放心持有")
    lines.append("")

    # === V 验证因子解读 ===
    lines.append("#### 🔍 真相验证 (V)")
    if genome.verification > 0.8:
        lines.append(f"- **V = {genome.verification:.2f}** → 🟢 **强验证通过**")
        lines.append("- OCF与利润高度一致，盈利质量优秀")
        lines.append("- 投资建议：优先考虑，真金白银")
    elif genome.verification > 0.5:
        lines.append(f"- **V = {genome.verification:.2f}** → 🟡 **基本验证通过**")
        lines.append("- 现金流与利润基本匹配")
        lines.append("- 投资建议：可接受，继续关注")
    elif genome.verification > 0.3:
        lines.append(f"- **V = {genome.verification:.2f}** → 🟠 **验证不充分**")
        lines.append("- 现金流与利润存在一定差异")
        lines.append("- 投资建议：需分析差异原因（应收账款？存货？）")
    else:
        lines.append(f"- **V = {genome.verification:.2f}** → 🔴 **验证失败**")
        lines.append("- 现金流无法支撑利润，可能存在'纸面富贵'")
        lines.append("- 投资建议：**高度警惕**，可能是财务陷阱")
    lines.append("")

    # === 综合评估 ===
    lines.append("#### 📊 综合评估")
    lines.append("")

    # 计算综合评分
    positive_score = genome.gamma + genome.verification
    negative_score = genome.delta_fraud + genome.delta_decay
    net_score = positive_score - negative_score

    if genome.delta_fraud >= 0.58:
        lines.append("**⛔ 综合判断：熔断淘汰**")
        lines.append("- 欺诈风险触发熔断，直接淘汰")
    elif net_score > 0.5 and genome.verification > 0.5:
        lines.append("**✅ 综合判断：优质标的**")
        lines.append("- 正面因子明显强于负面因子")
        lines.append("- 盈利质量通过验证")
    elif net_score > 0 and genome.verification > 0.3:
        lines.append("**🟡 综合判断：可关注**")
        lines.append("- 整体可接受，但需持续跟踪")
    else:
        lines.append("**⚠️ 综合判断：谨慎**")
        lines.append("- 风险因子较高或验证不足")
        lines.append("- 建议等待更多积极信号")

    return "\n".join(lines)


# =============================================================================
# 导出到报告
# =============================================================================

def export_genome_section_markdown(
    genomes: List[CompanyGenome],
    results: List[TruthResult] = None,
    top_n: int = 10,
    include_radar: bool = True
) -> str:
    """
    导出基因分析到Markdown报告章节

    Args:
        genomes: 公司基因组列表
        results: 对应的T.R.U.T.H.结果列表
        top_n: 展示前N个公司
        include_radar: 是否包含雷达图

    Returns:
        str: Markdown格式的报告章节
    """
    lines = []

    lines.append("## 🧬 T.R.U.T.H. 基因分析报告")
    lines.append("")
    lines.append("> **Trend-Reality Unified Truth Hashing System** - 六维基因测序分析")
    lines.append("")

    # 汇总统计
    lines.append("### 📊 基因组统计概览")
    lines.append("")

    if genomes:
        alphas = [g.alpha for g in genomes]
        betas = [g.beta for g in genomes]
        gammas = [g.gamma for g in genomes]
        frauds = [g.delta_fraud for g in genomes]
        decays = [g.delta_decay for g in genomes]
        verifs = [g.verification for g in genomes]

        lines.append("| 基因维度 | 平均值 | 中位数 | 最小值 | 最大值 | 说明 |")
        lines.append("|----------|--------|--------|--------|--------|------|")
        lines.append(f"| α 周期性 | {np.mean(alphas):.2f} | {np.median(alphas):.2f} | {np.min(alphas):.2f} | {np.max(alphas):.2f} | 越高越强周期 |")
        lines.append(f"| β 资本密度 | {np.mean(betas):.2f} | {np.median(betas):.2f} | {np.min(betas):.2f} | {np.max(betas):.2f} | 越高越重资产 |")
        lines.append(f"| γ 成长动能 | {np.mean(gammas):.2f} | {np.median(gammas):.2f} | {np.min(gammas):.2f} | {np.max(gammas):.2f} | 越高增长越快 |")
        lines.append(f"| δf 欺诈风险 | {np.mean(frauds):.2f} | {np.median(frauds):.2f} | {np.min(frauds):.2f} | {np.max(frauds):.2f} | ≥0.58熔断 |")
        lines.append(f"| δd 衰退风险 | {np.mean(decays):.2f} | {np.median(decays):.2f} | {np.min(decays):.2f} | {np.max(decays):.2f} | 越高风险越大 |")
        lines.append(f"| V 真相验证 | {np.mean(verifs):.2f} | {np.median(verifs):.2f} | {np.min(verifs):.2f} | {np.max(verifs):.2f} | 越高越可信 |")
        lines.append("")

        # 熔断统计
        fuse_count = sum(1 for f in frauds if f >= 0.58)
        if fuse_count > 0:
            lines.append(f"⚠️ **熔断公司数量：{fuse_count}** （δ_fraud ≥ 0.58）")
            lines.append("")

    # 通过筛选的公司
    if results:
        passed = [r for r in results if r.passes_screen]
        lines.append(f"### ✅ 通过T.R.U.T.H.筛选的公司 ({len(passed)}/{len(results)})")
        lines.append("")

        if passed:
            lines.append("| 代码 | 名称 | ROIC阈值 | 实际ROIC | 裕度 | 主要基因特征 |")
            lines.append("|------|------|----------|----------|------|--------------|")

            # 按裕度排序
            passed_sorted = sorted(passed, key=lambda x: x.excess_return or 0, reverse=True)[:top_n]

            for r in passed_sorted:
                genome = r.genome
                # 主要特征
                chars = []
                if genome.alpha > 0.6:
                    chars.append("周期")
                if genome.beta < 0.4:
                    chars.append("轻资产")
                if genome.gamma > 0.6:
                    chars.append("高成长")
                if genome.verification > 0.7:
                    chars.append("高验证")

                chars_str = "、".join(chars) if chars else "均衡"

                # 获取阈值和实际ROIC
                threshold_val = r.threshold.final_threshold if r.threshold else 0.08
                actual_roic = r.rep_roic.final_value if r.rep_roic else 0.0
                margin = r.excess_return or 0.0

                lines.append(f"| {r.ts_code} | {r.company_name or '-'} | "
                           f"{threshold_val:.2%} | {actual_roic:.2%} | "
                           f"{margin:+.2%} | {chars_str} |")

            lines.append("")

    # 单个公司详细分析
    lines.append("### 🔬 精选公司基因详解")
    lines.append("")

    # 选取有代表性的公司
    display_genomes = genomes[:min(top_n, len(genomes))]

    for genome in display_genomes:
        lines.append(generate_genome_interpretation(genome))
        lines.append("")
        lines.append("---")
        lines.append("")

    return "\n".join(lines)


def export_genome_table_markdown(genomes: List[CompanyGenome]) -> str:
    """
    导出基因数据为Markdown表格

    Args:
        genomes: 公司基因组列表

    Returns:
        str: Markdown表格
    """
    lines = []
    lines.append("| 代码 | 名称 | α周期 | β资本 | γ成长 | δf欺诈 | δd衰退 | V验证 | 状态 |")
    lines.append("|------|------|-------|-------|-------|--------|--------|-------|------|")

    for g in genomes:
        # 状态判断
        if g.delta_fraud >= 0.58:
            status = "🔴熔断"
        elif g.verification < 0.3:
            status = "🟠验证差"
        elif g.delta_decay > 0.7:
            status = "🟡高衰退"
        elif g.gamma > 0.6 and g.verification > 0.6:
            status = "🟢优质"
        else:
            status = "⚪正常"

        lines.append(f"| {g.ts_code} | {g.company_name or '-'} | "
                    f"{g.alpha:.2f} | {g.beta:.2f} | {g.gamma:.2f} | "
                    f"{g.delta_fraud:.2f} | {g.delta_decay:.2f} | {g.verification:.2f} | "
                    f"{status} |")

    return "\n".join(lines)


# =============================================================================
# 批量可视化
# =============================================================================

def batch_generate_visualizations(
    genomes: List[CompanyGenome],
    output_dir: Optional[str] = None,
    top_n: int = 20
) -> List[GenomeVisualization]:
    """
    批量生成基因可视化

    Args:
        genomes: 公司基因组列表
        output_dir: 输出目录
        top_n: 生成图表的公司数量

    Returns:
        List[GenomeVisualization]: 可视化结果列表
    """
    results = []

    # 按某种标准排序（比如验证分数）
    sorted_genomes = sorted(
        genomes,
        key=lambda g: g.verification - g.delta_fraud - g.delta_decay,
        reverse=True
    )

    for i, genome in enumerate(sorted_genomes[:top_n]):
        save_path = None
        if output_dir:
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            save_path = str(output_path / f"genome_{genome.ts_code}.png")

        viz = plot_genome_radar(
            genome=genome,
            company_name=genome.company_name or genome.ts_code,
            save_path=save_path,
            return_base64=True
        )
        results.append(viz)

    return results
