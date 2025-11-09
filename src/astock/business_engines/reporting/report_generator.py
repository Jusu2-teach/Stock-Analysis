"""
报告生成器核心 (简化版)
======================

使用模板模式重构,将669行简化为~200行:
1. ReportTemplate - 报告模板基类
2. TrendReportGenerator - 趋势报告生成器
3. 插件化的Section生成器

优势:
- 模板化,易扩展
- 代码复用率高
- 减少70%重复代码
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Callable
from datetime import datetime
from pathlib import Path
import pandas as pd


class ReportSection(ABC):
    """报告章节基类"""

    def __init__(self, title: str, level: int = 2):
        self.title = title
        self.level = level

    @abstractmethod
    def generate(self, df: pd.DataFrame, col: Callable[[str], str]) -> List[str]:
        """生成章节内容

        Args:
            df: 数据DataFrame
            col: 列名构建函数

        Returns:
            Markdown行列表
        """
        pass

    def _header(self) -> str:
        """生成章节标题"""
        return f"\n{'#' * self.level} {self.title}\n"


class SummarySection(ReportSection):
    """摘要章节"""

    def __init__(self):
        super().__init__("执行摘要", level=2)

    def generate(self, df: pd.DataFrame, col: Callable[[str], str]) -> List[str]:
        lines = [self._header()]

        total = len(df)

        # 基础统计
        lines.append(f"- **总企业数**: {total}家")
        lines.append(f"- **分析日期**: {datetime.now().strftime('%Y-%m-%d')}")

        # ROIC分布
        if 'roic_weighted' in df.columns:
            avg_roic = df['roic_weighted'].mean()
            lines.append(f"- **平均ROIC**: {avg_roic:.1f}%")

            high_roic = (df['roic_weighted'] >= 15).sum()
            lines.append(f"- **高ROIC企业** (≥15%): {high_roic}家 ({high_roic/total*100:.1f}%)")

            # 列出高ROIC企业名称
            if high_roic > 0:
                high_names = df[df['roic_weighted'] >= 15]['name'].tolist()
                lines.append(f"  - {', '.join(high_names[:5])}")  # 最多5家

        # 趋势分布
        if 'roic_trend_score' in df.columns:
            strong = (df['roic_trend_score'] >= 70).sum()
            moderate = ((df['roic_trend_score'] >= 40) & (df['roic_trend_score'] < 70)).sum()
            weak = (df['roic_trend_score'] < 40).sum()

            lines.append(f"\n**趋势分布**:")
            lines.append(f"- 强趋势 (≥70): {strong}家")
            lines.append(f"- 中等趋势 (40-70): {moderate}家")
            lines.append(f"- 弱趋势 (<40): {weak}家")

        return lines


class OpportunitySection(ReportSection):
    """投资机会章节"""

    def __init__(self):
        super().__init__("投资机会识别", level=2)

    def generate(self, df: pd.DataFrame, col: Callable[[str], str]) -> List[str]:
        lines = [self._header()]

        # 高质量改善型
        if 'roic_weighted' in df.columns and 'roic_trend_score' in df.columns:
            improving = df[
                (df['roic_weighted'] >= 15) &
                (df['roic_trend_score'] >= 70)
            ]

            if len(improving) > 0:
                lines.append("### 1. 高质量强趋势型")
                lines.append(f"**标准**: ROIC≥15% 且 趋势分≥70")
                lines.append(f"**数量**: {len(improving)}家\n")

                # 列出前5家
                top5 = improving.nlargest(5, 'roic_trend_score')

                for _, row in top5.iterrows():
                    name = row.get('name', 'N/A')
                    code = row.get('ts_code', 'N/A')
                    roic = row['roic_weighted']
                    trend = row['roic_trend_score']

                    lines.append(f"- **{name}** ({code}): ROIC={roic:.1f}%, 趋势分={trend:.0f}")

        return lines


class RiskSection(ReportSection):
    """风险警示章节"""

    def __init__(self):
        super().__init__("风险警示", level=2)

    def generate(self, df: pd.DataFrame, col: Callable[[str], str]) -> List[str]:
        lines = [self._header()]

        # 严重问题企业
        if 'roic_penalty' in df.columns:
            severe = df[df['roic_penalty'] >= 15]

            if len(severe) > 0:
                lines.append("### 1. 高风险企业 (趋势重罚)")
                lines.append(f"**标准**: 趋势罚分≥15")
                lines.append(f"**数量**: {len(severe)}家\n")

                # 列出企业
                for _, row in severe.iterrows():
                    name = row.get('name', 'N/A')
                    code = row.get('ts_code', 'N/A')
                    penalty = row['roic_penalty']
                    roic = row.get('roic_weighted', 0)

                    lines.append(f"- **{name}** ({code}): 罚分={penalty:.0f}, ROIC={roic:.1f}%")

        # 低ROIC企业
        if 'roic_weighted' in df.columns:
            low_roic = df[df['roic_weighted'] < 8]

            if len(low_roic) > 0:
                lines.append("\n### 2. 低质量企业 (ROIC<8%)")
                lines.append(f"**数量**: {len(low_roic)}家")
                lines.append(f"**建议**: 谨慎关注")

        return lines


class ReportTemplate:
    """报告模板基类

    使用模板模式组织报告结构
    """

    def __init__(self, sections: List[ReportSection]):
        self.sections = sections

    def generate_header(self, df: pd.DataFrame) -> List[str]:
        """生成报告头部"""
        lines = [
            "# 趋势分析报告\n",
            f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"**分析企业数**: {len(df)}家\n",
            "---\n",
        ]
        return lines

    def generate(
        self,
        df: pd.DataFrame,
        col: Callable[[str], str]
    ) -> str:
        """生成完整报告

        Args:
            df: 数据DataFrame
            col: 列名构建函数

        Returns:
            Markdown格式报告字符串
        """
        lines = []

        # 1. 头部
        lines.extend(self.generate_header(df))

        # 2. 各章节
        for section in self.sections:
            lines.extend(section.generate(df, col))

        # 3. 尾部
        lines.append("\n---")
        lines.append(f"*Report generated by AStock Analysis System*")

        return '\n'.join(lines)


class TrendReportGenerator:
    """趋势报告生成器

    简化版,使用模板模式
    """

    def __init__(
        self,
        metric_prefix: str = 'roic',
        metric_suffix: str = ''
    ):
        self.metric_prefix = metric_prefix
        self.metric_suffix = metric_suffix

        # 构建报告章节
        self.sections = [
            SummarySection(),
            OpportunitySection(),
            RiskSection(),
        ]

        self.template = ReportTemplate(self.sections)

    def col(self, field: str) -> str:
        """构建列名"""
        return f"{self.metric_prefix}_{field}{self.metric_suffix}"

    def generate(
        self,
        input_csv: str,
        output_path: str
    ) -> None:
        """生成报告

        Args:
            input_csv: 输入CSV路径
            output_path: 输出Markdown路径
        """
        print("=" * 80)
        print("生成趋势分析报告".center(80))
        print("=" * 80)

        # 读取数据
        print(f"\n读取数据: {input_csv}")
        df = pd.read_csv(input_csv)
        print(f"成功加载 {len(df)} 家企业数据")

        # 生成报告
        report_content = self.template.generate(df, self.col)

        # 写入文件
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report_content)

        print(f"\n报告生成成功: {output_path}")
