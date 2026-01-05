"""
T.R.U.T.H. 数据驱动报告生成器
==============================

纯数据驱动分析，无预设阈值。

与 comprehensive_generator.py (规则驱动) 独立，方便对比两种模式的准确性。

架构：
    探针分析结果(DataFrame)
        → T.R.U.T.H. 六大基因计算
        → 三大物理求解器
        → 动态阈值报告

数据输入：直接接收探针分析结果 DataFrame

每个指标都经过 T.R.U.T.H. 系统：
- ROIC: 核心盈利能力
- ROE: 股东回报
- ROIIC: 增量资本效率
- 毛利率: 护城河
- 净利率: 盈利质量
- 营收: 规模增长
- 利润: 盈利增长
- OCF: 现金流验证

作者: AStock Analysis System
日期: 2025-01
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime
import json
import logging

logger = logging.getLogger(__name__)

# 导入 T.R.U.T.H. 系统
try:
    from ..truth import (
        TruthEngine,
        TruthConfig,
        CompanyGenome,
        TruthResult,
        get_default_truth_config,
        ProbeAdapter,
        ProbeOutputs,
        MultiIndicatorProbeOutputs,
        compute_genome_from_probes,
        # 求解器
        gravity_solver,
        velocity_solver,
        structure_solver,
        # 可视化
        generate_genome_interpretation,
        export_genome_table_markdown,
    )
    from ..truth.core.solvers.gravity_solver import create_gravity_result
    from ..truth.core.solvers.velocity_solver import create_velocity_result
    from ..truth.core.solvers.structure_solver import create_structure_result
    HAS_TRUTH = True
except ImportError as e:
    logger.error(f"T.R.U.T.H. 系统导入失败: {e}")
    HAS_TRUTH = False


class TruthReportGenerator:
    """
    T.R.U.T.H. 数据驱动报告生成器

    特点：
    - 无预设阈值，完全由数据驱动
    - 每个指标独立分析，多维度评价
    - 六大基因 + 三大求解器 + 动态阈值
    """

    # 指标配置：每个指标的前缀和权重
    METRICS_CONFIG = {
        "roic": {
            "prefix": "roic",
            "name": "ROIC",
            "description": "投入资本回报率 - 核心盈利能力",
            "weight": 0.20,
        },
        "roe": {
            "prefix": "roe",
            "name": "ROE",
            "description": "净资产收益率 - 股东回报",
            "weight": 0.15,
        },
        "roiic": {
            "prefix": "roiic",
            "name": "ROIIC",
            "description": "增量投入资本回报率 - 增量效率",
            "weight": 0.15,
        },
        "gross_margin": {
            "prefix": "grossprofit_margin",
            "name": "毛利率",
            "description": "毛利率 - 护城河指标",
            "weight": 0.10,
        },
        "net_margin": {
            "prefix": "netprofit_margin",
            "name": "净利率",
            "description": "净利率 - 盈利质量",
            "weight": 0.10,
        },
        "revenue": {
            "prefix": "total_revenue_ps",
            "name": "营收",
            "description": "营收增长 - 规模扩张",
            "weight": 0.10,
        },
        "profit": {
            "prefix": "eps",
            "name": "利润",
            "description": "利润增长 - 盈利扩张",
            "weight": 0.10,
        },
        "ocf": {
            "prefix": "ocfps",
            "name": "经营现金流",
            "description": "经营现金流 - 盈利验证",
            "weight": 0.10,
        },
    }

    def __init__(
        self,
        truth_processed: Dict[str, Any],
        probe_data: Dict[str, pd.DataFrame] = None,
    ):
        """初始化 T.R.U.T.H. 报告生成器（仅支持基于 TruthProcessor 的新架构）

        Args:
            truth_processed: TruthProcessor 处理后的结果
            probe_data: 可选的探针数据字典，用于补充明细（通常来自 truth_processed['probe_data']）
        """
        self.metrics_data: Dict[str, pd.DataFrame] = {}
        self.company_genomes: Dict[str, CompanyGenome] = {}
        self.company_results: Dict[str, Dict[str, Any]] = {}
        self.truth_processed = truth_processed
        self.use_professional_mode = True

        if not HAS_TRUTH:
            raise RuntimeError("T.R.U.T.H. 系统不可用")

        self.truth_engine = TruthEngine()
        self.truth_config = get_default_truth_config()
        self.probe_adapter = ProbeAdapter()

        # 加载数据（必须有 truth_processed，probe_data 仅作为补充）
        if truth_processed is None:
            raise ValueError("truth_processed 不能为空")

        # 如果 truth_processed 自带 probe_data，则优先使用
        embedded_probe_data = None
        if isinstance(truth_processed, dict):
            embedded_probe_data = truth_processed.get('probe_data')

        effective_probe_data = embedded_probe_data or probe_data

        self._load_from_truth_processed(truth_processed)

        if effective_probe_data and any(v is not None for v in effective_probe_data.values()):
            self._load_from_probe_data(effective_probe_data)

        logger.info("🧬 使用专业基因-指标映射模式（来自 TruthProcessor）")

    def _load_from_truth_processed(self, truth_processed: Dict[str, Any]) -> None:
        """从 TruthProcessor 处理结果加载数据"""
        # 提取处理结果
        batch_result = truth_processed.get('processed_results')
        results_df = truth_processed.get('results_df')
        probe_data = truth_processed.get('probe_data', {})

        # 加载探针数据（用于详细报告）
        if probe_data:
            for metric_key, df in probe_data.items():
                if df is not None and not df.empty:
                    if 'ts_code' in df.columns and df.index.name != 'ts_code':
                        df = df.set_index('ts_code')
                    self.metrics_data[metric_key] = df

        # 从处理结果加载基因组
        if batch_result is not None:
            for result in batch_result.results:
                if result.genome:
                    self.company_genomes[result.ts_code] = result.genome
                    self.company_results[result.ts_code] = {
                        'genome': result.genome,
                        'gene_extractions': result.gene_extractions,
                        'solver_results': result.solver_results,
                        'causal_validation': result.causal_validation,
                        'final_score': result.final_score,
                        'signal': result.signal,
                        'grade': result.grade,
                        'warnings': result.warnings,
                    }

        logger.info(f"  ✓ 已加载 {len(self.company_genomes)} 家公司的专业基因组数据")

    def _load_from_probe_data(self, probe_data: Dict[str, pd.DataFrame]) -> None:
        """从探针数据字典加载数据"""
        logger.info("直接使用探针分析结果（无需读取文件）")

        for metric_key, df in probe_data.items():
            if df is not None and not df.empty:
                # 确保有索引
                if 'ts_code' in df.columns and df.index.name != 'ts_code':
                    df = df.set_index('ts_code')
                self.metrics_data[metric_key] = df
                config = self.METRICS_CONFIG.get(metric_key, {})
                name = config.get('name', metric_key)
                logger.info(f"  ✓ {name}: {len(df)} 公司")

    def load_all_metrics(self) -> Dict[str, pd.DataFrame]:
        """返回已加载的探针数据"""
        return self.metrics_data

    def _safe_get(self, row: pd.Series, col: str, default: float = 0.0) -> float:
        """安全获取值"""
        if col not in row.index:
            return default
        val = row[col]
        if pd.isna(val):
            return default
        return float(val)

    def _extract_single_metric_genome(
        self,
        ts_code: str,
        metric_key: str,
        row: pd.Series,
    ) -> Dict[str, float]:
        """
        从单个指标提取基因特征

        返回六大基因的贡献值
        """
        prefix = self.METRICS_CONFIG[metric_key]["prefix"]

        # α 周期性: 来自 detrended_cv, cyclical_confidence
        detrended_cv = self._safe_get(row, f'{prefix}_detrended_cv', 0.1)
        cyclical_confidence = self._safe_get(row, f'{prefix}_cyclical_confidence', 0.0)
        is_cyclical = self._safe_get(row, f'{prefix}_is_cyclical', 0)

        alpha_raw = min(detrended_cv * 2, 1.0)
        if cyclical_confidence > 0.7:
            alpha_raw = max(alpha_raw, 0.6)

        # β 资本密度: 来自 cv, dol_ratio (利润/营收波动比)
        cv = self._safe_get(row, f'{prefix}_cv', 0.2)

        # γ 成长动能: 来自 cagr, trend_acceleration, r_squared
        cagr = self._safe_get(row, f'{prefix}_cagr', 0.0)
        r_squared = self._safe_get(row, f'{prefix}_r_squared', 0.5)
        trend_acceleration = self._safe_get(row, f'{prefix}_trend_acceleration', 0.0)

        gamma_raw = cagr * 5  # 20% CAGR → γ=1.0
        # R² 惩罚
        if r_squared < 0.5:
            gamma_raw *= r_squared * 2

        # δ_fraud 欺诈熵: 来自 CV太低(麦道夫), R²太高
        fraud_score = 0.0
        if cv < 0.02:  # 太光滑
            fraud_score += 0.3
        if r_squared > 0.98:  # 太完美
            fraud_score += 0.2

        # δ_decay 衰退熵: 来自 deterioration_probability, has_inflection
        deterioration_prob = self._safe_get(row, f'{prefix}_deterioration_probability', 0.0)
        has_inflection = self._safe_get(row, f'{prefix}_has_inflection', 0)
        inflection_type = row.get(f'{prefix}_inflection_type', 'none')
        recent_slope = self._safe_get(row, f'{prefix}_recent_slope', 0.0)

        decay_score = deterioration_prob
        if has_inflection and inflection_type == 'peak':
            decay_score = max(decay_score, 0.4)
        if recent_slope < -0.05:
            decay_score = max(decay_score, 0.3)

        # V 验证: 来自数据质量
        data_quality = row.get(f'{prefix}_data_quality', '5')
        v_score = 1.0 if data_quality in ['5', '4'] else 0.7

        return {
            'alpha': max(0, min(1, alpha_raw)),
            'beta': max(0, min(1, cv * 2)),
            'gamma': max(0, min(1, gamma_raw)),
            'delta_fraud': max(0, min(1, fraud_score)),
            'delta_decay': max(0, min(1, decay_score)),
            'verification': max(0, min(1, v_score)),
        }

    def _aggregate_company_genome(
        self,
        ts_code: str,
    ) -> CompanyGenome:
        """
        聚合公司所有指标的基因特征

        加权平均各指标的基因贡献
        """
        # 收集所有指标的基因
        metric_genes = {}
        total_weight = 0.0

        for metric_key, config in self.METRICS_CONFIG.items():
            if metric_key not in self.metrics_data:
                continue

            df = self.metrics_data[metric_key]
            if ts_code not in df.index:
                continue

            row = df.loc[ts_code]
            genes = self._extract_single_metric_genome(ts_code, metric_key, row)
            metric_genes[metric_key] = genes
            total_weight += config['weight']

        if not metric_genes:
            # 没有数据，返回中性基因
            return CompanyGenome(
                ts_code=ts_code,
                company_name=ts_code,
                alpha=0.5, beta=0.5, gamma=0.5,
                delta_fraud=0.0, delta_decay=0.0, verification=0.5,
            )

        # 加权聚合
        aggregated = {
            'alpha': 0.0, 'beta': 0.0, 'gamma': 0.0,
            'delta_fraud': 0.0, 'delta_decay': 0.0, 'verification': 0.0,
        }

        for metric_key, genes in metric_genes.items():
            weight = self.METRICS_CONFIG[metric_key]['weight'] / total_weight
            for gene_name, value in genes.items():
                aggregated[gene_name] += value * weight

        # 获取公司名称
        company_name = ts_code
        for metric_key in ['roic', 'roe', 'revenue']:
            if metric_key in self.metrics_data and ts_code in self.metrics_data[metric_key].index:
                row = self.metrics_data[metric_key].loc[ts_code]
                if 'name' in row.index:
                    company_name = row['name']
                    break

        return CompanyGenome(
            ts_code=ts_code,
            company_name=company_name,
            alpha=aggregated['alpha'],
            beta=aggregated['beta'],
            gamma=aggregated['gamma'],
            delta_fraud=aggregated['delta_fraud'],
            delta_decay=aggregated['delta_decay'],
            verification=aggregated['verification'],
        )

    def _run_solvers(self, genome: CompanyGenome) -> Dict[str, Any]:
        """运行三大物理求解器"""
        gravity = create_gravity_result(genome)
        velocity = create_velocity_result(genome)
        structure = create_structure_result(genome)

        return {
            'gravity': gravity,
            'velocity': velocity,
            'structure': structure,
            'thresholds': {
                'T_threshold': gravity.threshold.final_threshold if gravity.threshold else 0.08,
                'T_growth_bound': velocity.growth_bound.max_sustainable_growth if velocity.growth_bound else 0.05,
                'T_slope': structure.slope.expected_slope if structure.slope else 0.0,
            },
            'gates': {
                'v_gate_passed': genome.verification >= 0.4,
                'fraud_alert': genome.delta_fraud > 0.5,
                'decay_alert': genome.delta_decay > 0.6,
            },
            'interpretations': [
                gravity.interpretation,
                velocity.interpretation,
                structure.interpretation,
            ],
        }

    def analyze_all_companies(self) -> Dict[str, Dict]:
        """
        分析所有公司

        在专业模式下，直接使用 TruthProcessor 的结果
        在兼容模式下，使用简化的基因提取
        """
        # 专业模式：数据已经在 __init__ 中加载
        if self.use_professional_mode and self.company_results:
            logger.info(f"🧬 专业模式：使用预处理的 {len(self.company_results)} 家公司数据")
            return self.company_results

        # 兼容模式：从探针数据计算
        if not self.metrics_data:
            self.load_all_metrics()

        # 获取所有公司代码
        all_codes = set()
        for df in self.metrics_data.values():
            all_codes.update(df.index.tolist())

        logger.info(f"📊 兼容模式：开始分析 {len(all_codes)} 家公司...")

        for ts_code in all_codes:
            try:
                # 1. 聚合基因组
                genome = self._aggregate_company_genome(ts_code)
                self.company_genomes[ts_code] = genome

                # 2. 运行求解器
                solver_results = self._run_solvers(genome)

                # 3. 生成综合信号
                signal = self._generate_signal(genome, solver_results)

                self.company_results[ts_code] = {
                    'genome': genome,
                    'solvers': solver_results,
                    'signal': signal,
                }

            except Exception as e:
                logger.debug(f"分析失败 {ts_code}: {e}")

        logger.info(f"完成分析 {len(self.company_results)} 家公司")
        return self.company_results

    def _build_professional_report_item(
        self,
        ts_code: str,
        data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        构建专业模式的报告项

        从 TruthProcessor 的结果构建报告数据
        """
        genome = data['genome']
        solver_results = data.get('solver_results', {})
        gene_extractions = data.get('gene_extractions', {})
        causal_validation = data.get('causal_validation')

        # 提取求解器阈值
        gravity_result = solver_results.get('gravity')
        velocity_result = solver_results.get('velocity')
        structure_result = solver_results.get('structure', {})

        # 获取动态阈值
        t_threshold = 0.08
        t_growth_bound = 0.05
        t_slope = 0.0

        if gravity_result and hasattr(gravity_result, 'result'):
            t_threshold = getattr(gravity_result.result, 'final_threshold', 0.08)

        if velocity_result and hasattr(velocity_result, 'result'):
            growth_bound = getattr(velocity_result.result, 'growth_bound', None)
            if growth_bound:
                t_growth_bound = getattr(growth_bound, 'max_sustainable_growth', 0.05)

        # 构建基因来源信息
        gene_sources = {}
        for gene_name, extraction in gene_extractions.items():
            gene_sources[gene_name] = {
                'value': round(extraction.value, 3),
                'sources': extraction.source_indicators,
                'aggregation': extraction.aggregation_method,
            }

        # 构建报告项
        report_item = {
            'ts_code': ts_code,
            'company_name': genome.company_name,
            'genome': {
                'alpha': round(genome.alpha, 3),
                'beta': round(genome.beta, 3),
                'gamma': round(genome.gamma, 3),
                'delta_fraud': round(genome.delta_fraud, 3),
                'delta_decay': round(genome.delta_decay, 3),
                'verification': round(genome.verification, 3),
            },
            'gene_sources': gene_sources,  # 专业模式独有：基因来源追溯
            'dynamic_thresholds': {
                'T_threshold': round(t_threshold, 4),
                'T_growth_bound': round(t_growth_bound, 4),
                'T_slope': round(t_slope, 4),
            },
            'gates': {
                'v_gate_passed': genome.verification >= 0.4,
                'fraud_alert': genome.delta_fraud > 0.5,
                'decay_alert': genome.delta_decay > 0.6,
            },
            'signal': data.get('signal', '📊 中性'),
            'recommendation': self._get_recommendation_from_signal(data.get('signal', 'NEUTRAL')),
            'risk_level': self._get_risk_level_from_signal(data.get('signal', 'NEUTRAL')),
            'grade': data.get('grade', 'B'),
            'final_score': round(data.get('final_score', 0.5), 3),
            'warnings': data.get('warnings', []),
        }

        # 添加因果验证结果
        if causal_validation:
            report_item['causal_validation'] = {
                'overall_score': round(causal_validation.overall_score, 3),
                'revenue_profit_consistent': causal_validation.revenue_profit_consistent,
                'profit_ocf_consistent': causal_validation.profit_ocf_consistent,
                'roe_roic_consistent': causal_validation.roe_roic_consistent,
                'warnings': causal_validation.warnings,
            }

        # 构建解释
        interpretations = []
        for solver_name, solver_result in solver_results.items():
            if hasattr(solver_result, 'interpretation'):
                interpretations.append(solver_result.interpretation)
        report_item['interpretations'] = interpretations

        return report_item

    def _get_recommendation_from_signal(self, signal: str) -> str:
        """从信号获取推荐"""
        signal_map = {
            'STRONG_BUY': '强烈买入',
            'BUY': '买入',
            'NEUTRAL': '观察',
            'SELL': '卖出',
            'FRAUD_RISK': '回避',
            'DECAY_WARNING': '谨慎',
        }
        return signal_map.get(signal, '观察')

    def _get_risk_level_from_signal(self, signal: str) -> str:
        """从信号获取风险等级"""
        risk_map = {
            'STRONG_BUY': '低',
            'BUY': '中低',
            'NEUTRAL': '中',
            'SELL': '中高',
            'FRAUD_RISK': '高',
            'DECAY_WARNING': '高',
        }
        return risk_map.get(signal, '中')

    def _generate_signal(
        self,
        genome: CompanyGenome,
        solver_results: Dict,
    ) -> Dict[str, str]:
        """生成综合投资信号"""
        gates = solver_results['gates']

        if not gates['v_gate_passed']:
            return {
                'signal': '⚠️ 现金流验证未通过',
                'recommendation': '回避',
                'risk_level': '高',
            }

        if gates['fraud_alert']:
            return {
                'signal': '🚨 欺诈风险较高',
                'recommendation': '回避',
                'risk_level': '高',
            }

        if gates['decay_alert']:
            return {
                'signal': '📉 衰退信号明显',
                'recommendation': '减持',
                'risk_level': '中高',
            }

        if genome.gamma > 0.6 and genome.delta_decay < 0.3:
            threshold = solver_results['thresholds']['T_threshold']
            if threshold < 0.15:
                return {
                    'signal': '🚀 成长动能强劲',
                    'recommendation': '关注',
                    'risk_level': '中低',
                }
            else:
                return {
                    'signal': '🚀 成长动能强劲',
                    'recommendation': '谨慎关注',
                    'risk_level': '中',
                }

        if genome.alpha > 0.6:
            return {
                'signal': '🔄 周期性特征明显',
                'recommendation': '择时',
                'risk_level': '中',
            }

        return {
            'signal': '📊 中性',
            'recommendation': '观察',
            'risk_level': '中',
        }

    def generate_report(self, output_path: str = "data/truth_analysis_report.md") -> str:
        """
        生成 T.R.U.T.H. 报告

        根据 output_path 扩展名自动选择主格式：
        - .md: Markdown 为主，同时生成 JSON 和 CSV
        - .json: JSON 为主，同时生成 CSV 和 Markdown

        输出: JSON + CSV + Markdown
        """
        if not self.company_results:
            self.analyze_all_companies()

        # 准备报告数据
        reports = []
        for ts_code, data in self.company_results.items():
            genome = data['genome']

            # 处理专业模式和兼容模式的数据结构差异
            if self.use_professional_mode:
                # 专业模式：数据来自 TruthProcessor
                report_item = self._build_professional_report_item(ts_code, data)
            else:
                # 兼容模式：使用旧的数据结构
                solvers = data.get('solvers', {})
                signal = data.get('signal', {})
                report_item = {
                    'ts_code': ts_code,
                    'company_name': genome.company_name,
                    'genome': {
                        'alpha': round(genome.alpha, 3),
                        'beta': round(genome.beta, 3),
                        'gamma': round(genome.gamma, 3),
                        'delta_fraud': round(genome.delta_fraud, 3),
                        'delta_decay': round(genome.delta_decay, 3),
                        'verification': round(genome.verification, 3),
                    },
                    'dynamic_thresholds': {
                        'T_threshold': round(solvers.get('thresholds', {}).get('T_threshold', 0.08), 4),
                        'T_growth_bound': round(solvers.get('thresholds', {}).get('T_growth_bound', 0.05), 4),
                        'T_slope': round(solvers.get('thresholds', {}).get('T_slope', 0.0), 4),
                    },
                    'gates': solvers.get('gates', {}),
                    'signal': signal.get('signal', '📊 中性'),
                    'recommendation': signal.get('recommendation', '观察'),
                    'risk_level': signal.get('risk_level', '中'),
                    'interpretations': solvers.get('interpretations', []),
                }

            reports.append(report_item)

        # 确保目录存在
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # 根据扩展名确定基础路径
        base_path = str(output_file).rsplit('.', 1)[0]

        # 导出所有格式
        json_path = f"{base_path}.json"
        csv_path = f"{base_path}.csv"
        md_path = f"{base_path}.md"

        # 导出 JSON
        mode_tag = "Professional Gene-Indicator Mapping" if self.use_professional_mode else "Compatible Mode"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump({
                'version': '3.2',
                'system': 'T.R.U.T.H. (Threshold Rendering Using True History)',
                'mode': mode_tag,
                'generated_at': datetime.now().isoformat(),
                'total_companies': len(reports),
                'metrics_analyzed': list(self.metrics_data.keys()),
                'reports': reports,
            }, f, ensure_ascii=False, indent=2)
        logger.info(f"导出 JSON: {json_path}")

        # 导出 CSV
        self._export_csv(reports, json_path)

        # 导出 Markdown
        self._export_markdown(reports, json_path)

        # 返回主输出路径
        return output_path

    def _export_csv(self, reports: List[Dict], output_path: str):
        """导出 CSV"""
        rows = []
        for r in reports:
            rows.append({
                'ts_code': r['ts_code'],
                'company_name': r['company_name'],
                'alpha': r['genome']['alpha'],
                'beta': r['genome']['beta'],
                'gamma': r['genome']['gamma'],
                'delta_fraud': r['genome']['delta_fraud'],
                'delta_decay': r['genome']['delta_decay'],
                'verification': r['genome']['verification'],
                'T_threshold': r['dynamic_thresholds']['T_threshold'],
                'T_growth_bound': r['dynamic_thresholds']['T_growth_bound'],
                'T_slope': r['dynamic_thresholds']['T_slope'],
                'signal': r['signal'],
                'recommendation': r['recommendation'],
                'risk_level': r['risk_level'],
            })

        df = pd.DataFrame(rows)
        csv_path = output_path.replace('.json', '.csv')
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        logger.info(f"导出 CSV: {csv_path}")

    def _export_markdown(self, reports: List[Dict], output_path: str):
        """导出 Markdown"""
        lines = [
            "# T.R.U.T.H. 数据驱动分析报告",
            "",
            f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"**分析公司数**: {len(reports)}",
            f"**分析指标**: {', '.join(self.metrics_data.keys())}",
            "",
            "---",
            "",
            "## 报告说明",
            "",
            "本报告基于 **T.R.U.T.H. 系统** 生成，完全数据驱动，无预设阈值。",
            "",
            "### 六大基因含义",
            "",
            "| 基因 | 含义 | 高值解读 |",
            "|-----|------|---------|",
            "| α 周期性 | 业务周期波动 | 周期性强，需择时 |",
            "| β 资本密度 | 资产重度 | 重资产，回报慢 |",
            "| γ 成长动能 | 增长势能 | 成长强劲 |",
            "| δ_fraud | 欺诈熵 | 财务异常风险 |",
            "| δ_decay | 衰退熵 | 业务衰退风险 |",
            "| V 验证 | 现金流验证 | 盈利质量高 |",
            "",
            "### 三大动态阈值",
            "",
            "| 阈值 | 含义 | 求解器 |",
            "|-----|------|-------|",
            "| T_threshold | ROIC门槛 | gravity_solver |",
            "| T_growth | 增长上限 | velocity_solver |",
            "| T_slope | 趋势斜率 | structure_solver |",
            "",
            "---",
            "",
        ]

        # 按推荐分组
        groups = {}
        for r in reports:
            rec = r['recommendation']
            if rec not in groups:
                groups[rec] = []
            groups[rec].append(r)

        lines.append("## 分类汇总")
        lines.append("")

        for rec, group in sorted(groups.items()):
            lines.append(f"### {rec} ({len(group)}家)")
            lines.append("")
            lines.append("| 代码 | 名称 | 信号 | T_threshold | T_growth | V验证 |")
            lines.append("|------|------|------|-------------|----------|-------|")

            for r in group[:20]:
                v_status = "✓" if r['gates']['v_gate_passed'] else "✗"
                name = r['company_name'][:6] if len(r['company_name']) > 6 else r['company_name']
                signal = r['signal'][:10] if len(r['signal']) > 10 else r['signal']
                lines.append(
                    f"| {r['ts_code']} | {name} | "
                    f"{signal} | "
                    f"{r['dynamic_thresholds']['T_threshold']:.2%} | "
                    f"{r['dynamic_thresholds']['T_growth_bound']:.2%} | "
                    f"{v_status} |"
                )

            if len(group) > 20:
                lines.append(f"| ... | 还有{len(group)-20}家 | ... | ... | ... | ... |")

            lines.append("")

        md_path = output_path.replace('.json', '.md')
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))

        logger.info(f"导出 Markdown: {md_path}")

    def generate_single_stock_report(
        self,
        stock_code: str,
        output_path: str = None,
    ) -> str:
        """
        生成单只股票的 T.R.U.T.H. 深度分析报告

        Args:
            stock_code: 股票代码
            output_path: 输出路径（可选）

        Returns:
            生成的报告内容 (Markdown)
        """
        if not self.metrics_data:
            self.load_all_metrics()

        # 获取基因组
        genome = self._aggregate_company_genome(stock_code)
        solver_results = self._run_solvers(genome)
        signal = self._generate_signal(genome, solver_results)

        # 收集各指标详情
        metric_details = []
        for metric_key, config in self.METRICS_CONFIG.items():
            if metric_key not in self.metrics_data:
                continue
            df = self.metrics_data[metric_key]
            if stock_code not in df.index:
                continue

            row = df.loc[stock_code]
            genes = self._extract_single_metric_genome(stock_code, metric_key, row)

            prefix = config['prefix']
            metric_details.append({
                'name': config['name'],
                'description': config['description'],
                'genes': genes,
                'raw_data': {
                    'cagr': self._safe_get(row, f'{prefix}_cagr', None),
                    'cv': self._safe_get(row, f'{prefix}_cv', None),
                    'r_squared': self._safe_get(row, f'{prefix}_r_squared', None),
                    'recent_slope': self._safe_get(row, f'{prefix}_recent_slope', None),
                },
            })

        # 生成报告
        lines = [
            f"# T.R.U.T.H. 深度分析: {stock_code}",
            "",
            f"**公司名称**: {genome.company_name}",
            f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "---",
            "",
            "## 综合信号",
            "",
            f"- **信号**: {signal['signal']}",
            f"- **建议**: {signal['recommendation']}",
            f"- **风险等级**: {signal['risk_level']}",
            "",
            "---",
            "",
            "## 六维基因图谱",
            "",
            "| 基因 | 值 | 解读 |",
            "|-----|-----|------|",
            f"| α 周期性 | {genome.alpha:.3f} | {'强周期' if genome.alpha > 0.5 else '弱周期'} |",
            f"| β 资本密度 | {genome.beta:.3f} | {'重资产' if genome.beta > 0.5 else '轻资产'} |",
            f"| γ 成长动能 | {genome.gamma:.3f} | {'高成长' if genome.gamma > 0.5 else '低成长'} |",
            f"| δ_fraud | {genome.delta_fraud:.3f} | {'⚠️ 关注' if genome.delta_fraud > 0.3 else '正常'} |",
            f"| δ_decay | {genome.delta_decay:.3f} | {'⚠️ 关注' if genome.delta_decay > 0.4 else '正常'} |",
            f"| V 验证 | {genome.verification:.3f} | {'✓ 通过' if genome.verification > 0.4 else '✗ 未通过'} |",
            "",
            "---",
            "",
            "## 动态阈值",
            "",
            f"- **T_threshold (ROIC门槛)**: {solver_results['thresholds']['T_threshold']:.2%}",
            f"- **T_growth_bound (增长上限)**: {solver_results['thresholds']['T_growth_bound']:.2%}",
            f"- **T_slope (趋势斜率)**: {solver_results['thresholds']['T_slope']:.4f}",
            "",
            "---",
            "",
            "## 各指标详情",
            "",
        ]

        for detail in metric_details:
            lines.append(f"### {detail['name']}")
            lines.append(f"_{detail['description']}_")
            lines.append("")
            lines.append("| 基因 | 值 |")
            lines.append("|-----|-----|")
            for gene_name, value in detail['genes'].items():
                lines.append(f"| {gene_name} | {value:.3f} |")
            lines.append("")

            raw = detail['raw_data']
            if raw['cagr'] is not None:
                lines.append(f"- CAGR: {raw['cagr']:.2%}")
            if raw['cv'] is not None:
                lines.append(f"- CV: {raw['cv']:.3f}")
            if raw['r_squared'] is not None:
                lines.append(f"- R²: {raw['r_squared']:.3f}")
            if raw['recent_slope'] is not None:
                lines.append(f"- 近期斜率: {raw['recent_slope']:.4f}")
            lines.append("")

        lines.append("---")
        lines.append("")
        lines.append("## 求解器解读")
        lines.append("")
        for interp in solver_results['interpretations']:
            if interp:
                lines.append(f"- {interp}")

        report_content = '\n'.join(lines)

        if output_path:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(report_content)
            logger.info(f"导出单股报告: {output_path}")

        return report_content
