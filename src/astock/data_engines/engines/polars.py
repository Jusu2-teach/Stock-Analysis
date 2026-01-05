"""
Polars数据引擎 - 高性能数据处理引擎
提供存储、字段映射和去重功能
"""
from pathlib import Path
import polars as pl
import yaml
from typing import Optional, Dict, Any, Union, List
import logging
import re
import fnmatch

logger = logging.getLogger(__name__)

def _load_mapping() -> Dict[str, str]:
    """加载财务指标映射配置"""
    try:
        config_path = Path(__file__).parent / "config.yaml"
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            return config.get('financial_indicators', {})
    except Exception as e:
        logger.warning(f"配置文件加载失败: {e}, 使用空映射")
        return {}


def _infer_format(path_obj: Path, pattern: str | None) -> str:
    inferred = "csv"
    if path_obj.is_file():
        suffix = path_obj.suffix.lower()
        if suffix in (".csv", ".parquet", ".json"):
            inferred = suffix.removeprefix('.')
    elif pattern and any(pattern.endswith(ext) for ext in (".csv", ".parquet", ".json")):
        for ext in ("parquet", "csv", "json"):
            if pattern.endswith(ext):
                inferred = ext
                break
    return inferred

def _build_file_list(path_obj: Path, read_format: str, pattern: str | None, recursive: bool) -> tuple[list[Path], str]:
    if pattern:
        glob_pattern = pattern
    else:
        ext_map = {'csv': '*.csv', 'parquet': '*.parquet', 'json': '*.json'}
        glob_pattern = ext_map.get(read_format, '*')
    if recursive:
        files = list(path_obj.rglob(glob_pattern))
    else:
        files = list(path_obj.glob(glob_pattern))
    return [f for f in files if f.is_file()], glob_pattern

def _apply_exclude(files: list[Path], exclude: Union[str, list[str], tuple[str, ...], None]) -> list[Path]:
    if not exclude:
        return files
    patterns: list[str] = []
    if isinstance(exclude, str):
        if ',' in exclude:
            patterns.extend([p.strip() for p in exclude.split(',') if p.strip()])
        else:
            patterns.append(exclude.strip())
    else:
        patterns.extend([p.strip() for p in list(exclude) if isinstance(p, str) and p.strip()])
    if not patterns:
        return files
    before = len(files)
    filtered = [f for f in files if not any(fnmatch.fnmatch(f.name, pat) for pat in patterns)]
    removed = before - len(filtered)
    if removed:
        logger.info(f"文件排除: patterns={patterns} 移除={removed}")
    return filtered


def _read_single_file(p: Path, fmt: str, lazy: bool, kwargs: dict[str, Any], log_each: bool, ignore_errors: bool) -> Optional[Union[pl.DataFrame, pl.LazyFrame]]:
    try:
        if lazy and fmt in {"csv", "parquet"}:
            df_local = pl.scan_csv(p, **kwargs) if fmt == "csv" else pl.scan_parquet(p, **kwargs)
        elif fmt == "csv":
            df_local = pl.read_csv(p, **kwargs)
        elif fmt == "parquet":
            df_local = pl.read_parquet(p, **kwargs)
        elif fmt == "json":
            if lazy:
                logger.warning("json 不支持 scan，退回 eager 读取")
            df_local = pl.read_json(p, **kwargs)
        else:
            logger.error(f"不支持的格式: {fmt}")
            return None
        if log_each and not isinstance(df_local, pl.LazyFrame):
            logger.info(f"读取: {p.name} -> {df_local.height}行 x {df_local.width}列")
        elif log_each:
            logger.info(f"扫描: {p.name} -> LazyFrame")
        return df_local
    except Exception as ex:
        if ignore_errors:
            logger.warning(f"读取失败已跳过 {p}: {ex}")
            return None
        raise

def _unify_schema(dfs: list[Union[pl.DataFrame, pl.LazyFrame]], fill_value, strict: bool) -> list[Union[pl.DataFrame, pl.LazyFrame]]:
    if not dfs:
        return dfs
    try:
        all_cols = []
        col_set = set()
        # 保持第一次出现顺序
        for d in dfs:
            for c in d.columns:
                if c not in col_set:
                    col_set.add(c)
                    all_cols.append(c)
        base_cols = dfs[0].columns
        ordered = list(base_cols) + [c for c in all_cols if c not in base_cols]
        aligned = []
        for d in dfs:
            d_cols = set(d.columns)
            missing = [c for c in ordered if c not in d_cols]
            if missing and strict:
                raise ValueError(f"strict_schema=True 列不一致: 缺失 {missing}")
            if missing:
                d = d.with_columns([pl.lit(fill_value).alias(m) for m in missing])
            d = d.select([pl.col(c) for c in ordered])
            aligned.append(d)
        return aligned
    except Exception as ex:
        logger.warning(f"Schema 对齐失败(忽略 unify_schema): {ex}")
        return dfs

def _normalize_sort_spec(spec: Union[str, list[str], tuple[str, ...], None]) -> list[str]:
    if not spec:
        return []
    if isinstance(spec, str):
        return [p.strip() for p in spec.split(',') if p.strip()]
    return [p.strip() for p in list(spec) if isinstance(p, str) and p.strip()]

def _parse_sort_tokens(tokens: list[str]) -> tuple[list[str], list[bool]]:
    cols: list[str] = []
    descs: list[bool] = []
    for token in tokens:
        desc = False
        raw = token
        if raw.startswith('-') and len(raw) > 1:
            desc = True
            raw = raw[1:]
        if ':' in raw:
            name, mode = [x.strip() for x in raw.split(':', 1)]
            raw = name
            if mode.lower() in ("desc", "descending", "d"):
                desc = True
            elif mode.lower() in ("asc", "ascending", "a"):
                desc = False
        cols.append(raw)
        descs.append(desc)
    return cols, descs

def _apply_sort(merged: Union[pl.DataFrame, pl.LazyFrame],
                sort_by,
                sort_desc,
                coerce_dates: bool) -> tuple[Union[pl.DataFrame, pl.LazyFrame], list[str]]:
    tokens = _normalize_sort_spec(sort_by)
    if not tokens:
        logger.debug("未指定排序列或排序列表为空, 跳过排序")
        return merged, []
    cols, descs = _parse_sort_tokens(tokens)
    # sort_desc 显式覆盖
    if cols:
        if isinstance(sort_desc, bool):
            descs = [bool(sort_desc)] * len(cols)
        else:
            tmp = list(sort_desc)
            if len(tmp) < len(cols):
                tmp += [False] * (len(cols) - len(tmp))
            descs = [bool(x) for x in tmp[:len(cols)]]
    existing_map = {c: c for c in merged.columns}
    kept_cols: list[str] = []
    kept_desc: list[bool] = []
    missing: list[str] = []
    for c, d in zip(cols, descs):
        if c in existing_map:
            kept_cols.append(existing_map[c])
            kept_desc.append(d)
        else:
            missing.append(c)
    if not kept_cols:
        logger.warning(f"排序: 所有指定列不存在 -> {cols} (跳过排序)")
        return merged, []
    if coerce_dates:
        date_targets = [c for c in kept_cols if 'date' in c.lower()]
        casts = []
        for coln in date_targets:
            try:
                if merged.schema.get(coln) == pl.Utf8:
                    casts.append(pl.col(coln).str.strptime(pl.Date, strict=False).alias(coln))
            except Exception as ce:
                logger.debug(f"日期解析跳过 {coln}: {ce}")
        if casts:
            try:
                merged = merged.with_columns(casts)
            except Exception as ce2:
                logger.debug(f"日期列批量解析失败(忽略): {ce2}")
    try:
        merged = merged.sort(by=kept_cols, descending=kept_desc)
        logger.info(
            f"排序完成: 列={kept_cols} 降序={kept_desc} 缺失列={missing if missing else 'None'}"
        )
        if missing:
            logger.warning(f"排序: 以下列未找到 -> {missing}")
    except Exception as ex:
        logger.warning(f"排序失败(忽略): {ex}")
        logger.debug(f"失败上下文 sort_cols={kept_cols} desc={kept_desc}")
    return merged, kept_cols

def load_data(data: Optional[pl.DataFrame] = None,
              file_path: str = None,
              pattern: str | None = None,
              exclude: Union[str, list[str], tuple[str, ...], None] = None,
              lazy: bool = False,
              sort_by: Union[str, list[str], tuple[str, ...], None] = "end_date",
              sort_desc: Union[bool, list[bool], tuple[bool, ...]] = False,
              deduplicate: bool = True,
              dedup_keys: Union[list[str], tuple[str, ...]] = ("ts_code", "end_date"),
              dedup_strategy: str = "first"  # first|last|latest_ann_date
              ) -> Optional[Union[pl.DataFrame, pl.LazyFrame]]:
    """数据加载入口 (支持单文件 / 目录多文件)。内部已拆分多步骤函数以提升可维护性。

    参数说明与原版本一致，功能等价。
    """
    # 内部配置（后续可抽象为 dataclass 配置）
    FORMAT: str | None = None
    RECURSIVE = False
    LIMIT = 0
    COERCE_DATES = True
    UNIFY_SCHEMA = True
    FILL_MISSING_WITH = None
    STRICT_SCHEMA = False
    IGNORE_ERRORS = True  # 是否忽略单文件读取错误
    LOG_EACH = True

    kwargs: dict[str, Any] = {}
    if not file_path:
        logger.error("必须指定 file_path")
        return None
    path_obj = Path(file_path)
    if not path_obj.exists():
        logger.error(f"路径不存在: {file_path}")
        return None

    read_format = (FORMAT or _infer_format(path_obj, pattern)).lower()

    # 单文件直接读取
    if path_obj.is_file():
        df = _read_single_file(path_obj, read_format, lazy, kwargs, LOG_EACH, IGNORE_ERRORS)
        if df is not None and not isinstance(df, pl.LazyFrame):
            logger.info(f"成功加载数据: {path_obj} ({df.height}行, {df.width}列)")
        return df
    if not path_obj.is_dir():
        logger.error(f"file_path 既不是文件也不是目录: {file_path}")
        return None

    files, glob_pattern = _build_file_list(path_obj, read_format, pattern, RECURSIVE)
    files = _apply_exclude(files, exclude)
    if LIMIT and LIMIT > 0:
        files = files[:LIMIT]
    if not files:
        logger.warning(f"目录未匹配到任何文件: {file_path} pattern={glob_pattern}")
        return None
    original_count = len(files)
    logger.info(f"目录模式: 待读取文件数={len(files)} (原始={original_count}) pattern={glob_pattern} format={read_format} lazy={lazy}")

    # 读取
    dfs: list[Union[pl.DataFrame, pl.LazyFrame]] = []
    for fp in files:
        dfl = _read_single_file(fp, read_format, lazy, kwargs, LOG_EACH, IGNORE_ERRORS)
        if dfl is not None:
            dfs.append(dfl)
    if not dfs:
        logger.error("所有文件读取失败或为空")
        return None

    # Schema 对齐
    if UNIFY_SCHEMA:
        dfs = _unify_schema(dfs, FILL_MISSING_WITH, STRICT_SCHEMA)

    try:
        merged = pl.concat(dfs, how="vertical_relaxed")
    except Exception as ex:
        logger.error(f"合并失败: {ex}")
        return None

    # 去重（如果配置）在排序前执行
    if deduplicate and dedup_keys:
        missing_keys = [k for k in dedup_keys if k not in merged.columns]
        if missing_keys:
            logger.warning(f"去重跳过: 缺失关键列 {missing_keys}")
        else:
            try:
                merged = _remove_duplicates(merged, list(dedup_keys), dedup_strategy)
            except Exception as de:
                logger.warning(f"内部去重失败(忽略): {de}")

    merged, used_sort_cols = _apply_sort(merged, sort_by, sort_desc, COERCE_DATES)

    if isinstance(merged, pl.LazyFrame):
        logger.info(f"合并完成 (LazyFrame): 文件数={len(dfs)} 列数(估计)={len(merged.columns)} sort_by={used_sort_cols if used_sort_cols else 'N/A'}")
        if not lazy:
            collected = merged.collect()
            logger.info(f"已 collect -> 行数={collected.height} 列数={collected.width}")
            return collected
        return merged
    else:
        logger.info(f"合并完成: 文件数={len(dfs)} 总行数={merged.height} 列数={merged.width} (排序列={used_sort_cols if used_sort_cols else 'N/A'}) lazy={lazy}")
        return merged

def store(data: Optional[Union[pl.DataFrame, Any]] = None,
          file_path: str = None,
          format: str = "parquet",
          **kwargs) -> Optional[pl.DataFrame]:
    """
    纯 Polars 数据写出函数。

    参数:
        data: 必须是 pl.DataFrame
        file_path: 目标文件路径
        format: parquet | csv | json
        **kwargs: 传递给底层写出方法的附加参数

    返回:
        原始 pl.DataFrame (便于链式调用) 或 None
    """
    if data is None:
        logger.warning("没有数据需要存储")
        return None

    # Support for AnalysisResult/ScoreResult objects from business engines
    if hasattr(data, 'data') and (hasattr(data, 'metric_name') or hasattr(data, 'score_col')):
        logger.info(f"store: Detected Result object ({type(data).__name__}), extracting .data")
        data = data.data

    # 兼容 pandas.DataFrame 自动转换
    if not isinstance(data, pl.DataFrame):
        try:
            import pandas as pd  # type: ignore
            if isinstance(data, pd.DataFrame):
                logger.info("store: 检测到 pandas.DataFrame, 自动转换为 Polars")
                try:
                    # 先尝试直接转换
                    data = pl.from_pandas(data)
                except Exception as conv_e:
                    logger.warning(f"直接转换失败 ({conv_e}), 尝试清理数据类型...")
                    try:
                        # 清理 pandas DataFrame 中的不兼容类型
                        df_clean = data.copy()
                        for col in df_clean.columns:
                            # 处理 object 类型列，转换为 string
                            if df_clean[col].dtype == 'object':
                                df_clean[col] = df_clean[col].astype(str)
                            # 处理 nullable Int64 等类型
                            elif str(df_clean[col].dtype).startswith('Int') or str(df_clean[col].dtype).startswith('UInt'):
                                df_clean[col] = df_clean[col].astype(float)
                        data = pl.from_pandas(df_clean)
                        logger.info("数据类型清理后转换成功")
                    except Exception as clean_e:
                        logger.error(f"pandas->Polars 转换失败: {clean_e}")
                        return None
            else:
                logger.error("store 仅支持 Polars 或 pandas DataFrame")
                return None
        except Exception as ie:
            logger.error(f"pandas 兼容转换失败: {ie}")
            return None
    if file_path is None:
        logger.error("必须指定文件路径")
        return None

    try:
        dst = Path(file_path)
        dst.parent.mkdir(parents=True, exist_ok=True)
        fmt = format.lower()
        if fmt == "parquet":
            data.write_parquet(dst, **kwargs)
        elif fmt == "csv":
            data.write_csv(dst, **kwargs)
        elif fmt == "json":
            data.write_json(dst, **kwargs)
        else:
            logger.error(f"不支持的格式: {format}")
            return None
        logger.info(f"数据已保存到: {dst} (format={fmt}) rows={data.height}")
        return data
    except Exception as e:
        logger.error(f"保存数据失败: {e}")
        return None

def filter_mapped_columns(data: Optional[pl.DataFrame] = None,
                          include_all_mapped: bool = True,  # 预留参数，当前仅筛选存在列
                          show_schema: bool = True,
                          **kwargs) -> Optional[pl.DataFrame]:
    """
    根据配置映射筛选列，只保留映射中定义的字段，并把中文字段名行作为第一条数据行插入。

    Args:
        data: 输入的Polars DataFrame
        include_all_mapped: 是否包含所有映射的列（即使数据中不存在）
        show_schema: 是否显示字段的中文含义说明
        **kwargs: 其他参数

    Returns:
    Optional[pl.DataFrame]: 筛选后的DataFrame（写出CSV后: 第1行为英文表头, 第2行为中文字段名, 第3+行为数据）
    """
    if data is None:
        logger.warning("没有输入数据")
        return None

    mapping = _load_mapping()
    if not mapping:
        logger.warning("没有加载到映射配置，返回原数据")
        return data

    try:
        # 获取映射中定义的字段
        mapped_columns = list(mapping.keys())
        logger.info(f"🔍 配置文件中的映射字段: {mapped_columns}")

        # 筛选数据中存在的映射字段
        existing_columns = [col for col in mapped_columns if col in data.columns]
        logger.info(f"🎯 数据中存在的映射字段: {existing_columns}")

        if not existing_columns:
            logger.warning("数据中没有找到任何映射字段")
            return data

        # 筛选列
        filtered_data = data.select(existing_columns)
        logger.info(f"🔧 筛选后数据维度: {filtered_data.height}行 x {filtered_data.width}列")

        # 将所有列转为Utf8，避免插入中文行时类型冲突
        try:
            filtered_data = filtered_data.select([
                pl.col(c).cast(pl.Utf8).alias(c) for c in filtered_data.columns
            ])
            logger.debug("列类型统一为Utf8便于插入中文说明行")
        except Exception as cast_err:
            logger.warning(f"列类型转换Utf8失败(不影响继续): {cast_err}")

        # 如果第一条数据行已经是中文(检测含中文字符) 则不重复插入
        already_chinese = False
        if filtered_data.height > 0:
            first_val = str(filtered_data[0, existing_columns[0]])
            if re.search(r'[\u4e00-\u9fff]', first_val):
                already_chinese = True
        if not already_chinese:
            chinese_names = [mapping.get(col, col) for col in existing_columns]
            chinese_row_dict = {col: cname for col, cname in zip(existing_columns, chinese_names)}
            logger.info(f"插入中文字段名行: {chinese_row_dict}")
            chinese_row = pl.DataFrame([chinese_row_dict])
            filtered_data = pl.concat([chinese_row, filtered_data], how="vertical_relaxed")
            logger.info("中文字段名行插入完成 (作为文件第二行)")
        else:
            logger.debug("检测到已有中文说明行，跳过插入")

        logger.info(f"共保留 {len(existing_columns)} 个映射字段: {existing_columns}")

        if show_schema:
            logger.info("字段映射说明 (英文 -> 中文):")
            for col in existing_columns:
                chinese_name = mapping.get(col, "未知")
                logger.info(f"  {col} -> {chinese_name}")

        return filtered_data
    except Exception as e:
        logger.error(f"字段筛选失败: {e}")
        return data

def _remove_duplicates(
    df: pl.DataFrame,
    key_columns: list,
    strategy: str = "first"
) -> pl.DataFrame:
    if df is None or df.height == 0:
        return df
    miss = [c for c in key_columns if c not in df.columns]
    if miss:
        raise ValueError(f"关键字段缺失: {miss}")
    if strategy == "latest_ann_date":
        if "ann_date" not in df.columns:
            raise ValueError("latest_ann_date 策略需要 ann_date 列")
        try:
            df = df.sort(by=key_columns + ["ann_date"], descending=[False]*len(key_columns) + [True])
        except Exception as se:
            logger.debug(f"latest_ann_date 预排序失败(忽略): {se}")
        keep = "first"
    elif strategy in ("first", "last"):
        keep = strategy
    else:
        raise ValueError(f"未知去重策略: {strategy}")
    before = df.height
    try:
        try:
            df_u = df.unique(subset=key_columns, keep=keep, maintain_order=True)
        except TypeError:
            df_u = df.unique(subset=key_columns, keep=keep)
        try:
            df_u = df_u.sort(by=key_columns, descending=[False]*len(key_columns))
        except Exception:
            pass
        removed = before - df_u.height
        if removed > 0:
            logger.info(f"内部去重: strategy={strategy} keys={key_columns} 移除={removed} 剩余={df_u.height}")
        else:
            logger.info(f"内部去重: 无重复 strategy={strategy}")
        return df_u
    except Exception as e:
        logger.warning(f"内部去重失败(返回原数据): {e}")
        return df


def _load_filter_industries() -> List[str]:
    """加载 config.yaml 中的 filter_industry 列表"""
    try:
        config_path = Path(__file__).parent / "config.yaml"
        with open(config_path, 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f) or {}
            lst = cfg.get('filter_industry') or []
            if not isinstance(lst, list):
                logger.warning("filter_industry 不是列表类型，忽略")
                return []
            return [str(x).strip() for x in lst if str(x).strip()]
    except Exception as e:
        logger.warning(f"加载行业过滤列表失败: {e}")
        return []


def _read_table_auto(path_str: str,
                     strict: bool = False,
                     label: str = "数据文件") -> Optional[pl.DataFrame]:
    """统一的表格读取逻辑 (csv/parquet 自动尝试)

    Args:
        path_str: 文件路径
        strict: 严格模式, True 时失败抛出异常
        label: 日志中显示的标签 (例如: 基础信息 / 财务数据)

    Returns:
        pl.DataFrame or None
    """
    p = Path(path_str)
    if not p.exists():
        msg = f"{label}文件不存在: {path_str}"
        logger.error(msg)
        if strict:
            raise RuntimeError(msg)
        return None
    try:
        suf = p.suffix.lower()
        if suf == '.parquet':
            return pl.read_parquet(p)
        if suf == '.csv':
            return pl.read_csv(p)
        # 未知后缀: 依次尝试 parquet -> csv
        try:
            return pl.read_parquet(p)
        except Exception:
            return pl.read_csv(p)
    except Exception as e:
        msg = f"读取{label}失败: {e}"
        logger.error(msg)
        if strict:
            raise RuntimeError(msg)
        return None


def filter_industry(data: Optional[pl.DataFrame] = None,
                    financial_path: str = "data/polars/final_concat.parquet",
                    basic_info_path: str = "data/stack_basic.csv",
                    industries: Optional[List[str]] = None,
                    join_type: str = "inner",
                    keep_financial: bool = True,
                    keep_columns: Optional[List[str]] = None,
                    strict: bool = False) -> Optional[pl.DataFrame]:
    """
    Args:
        data: 可选，若传入则不再从 financial_path 读取；应包含 ts_code 列
    financial_path: 全量合并财务数据路径 (parquet/csv 皆可)
    basic_info_path: stock_basic 导出的基础文件 (CSV/Parquet 均可, 含 ts_code,name,industry)
        industries: 直接指定行业白名单；若为 None 则从 config.yaml 的 filter_industry 读取
        join_type: 连接类型 (inner / left)
        keep_financial: 是否保留财务数据的其余列；False 时仅输出 ts_code,name,industry
    keep_columns: 可选手动指定最终列顺序（若为空自动推断）

    Returns:
        过滤后的 Polars DataFrame

    strict: 为 True 时，关键文件缺失或读取失败直接抛出 RuntimeError，防止后续节点误用旧数据
    """
    # 1. 行业白名单
    if industries is None:
        industries = _load_filter_industries()
    if not industries:
        logger.warning("未获取到任何行业白名单(industries)，返回空")
        return None
    industries_set = set(industries)
    logger.info(f"行业白名单: {industries}")

    # 2. 读取基础信息映射
    basic_df = _read_table_auto(basic_info_path, strict=strict, label="基础信息")
    if basic_df is None:
        return None
    expected_cols = {"ts_code", "name", "industry"}
    missing_basic = expected_cols - set(basic_df.columns)
    if missing_basic:
        logger.warning(f"基础信息缺失列 {missing_basic}，可用列: {basic_df.columns}")
    # 过滤行业
    if "industry" in basic_df.columns:
        basic_df = basic_df.filter(pl.col("industry").is_in(list(industries_set)))
    else:
        msg = "缺少行业列 'industry'，无法按行业过滤，返回空"
        logger.warning(msg)
        if strict:
            raise RuntimeError(msg)
        return None
    if basic_df.is_empty():
        logger.warning("按行业过滤后基础信息为空")
        return basic_df
    logger.info(f"基础信息行业过滤后行数: {basic_df.height}")

    # 2.1 永久性排除 *ST 等特殊处理公司（退市/风险预警）
    if "name" in basic_df.columns:
        before_st = basic_df.height
        try:
            basic_df = basic_df.filter(~pl.col("name").str.contains(r"\\*ST"))
            removed_st = before_st - basic_df.height
            if removed_st > 0:
                logger.info(f"排除 *ST 公司: 移除 {removed_st} 行 (剩余 {basic_df.height})")
        except Exception as ste:
            logger.warning(f"排除 *ST 步骤失败(忽略): {ste}")

    # 3. 财务数据读取/准备
    if data is None:
        fin_df = _read_table_auto(financial_path, strict=strict, label="财务数据")
        if fin_df is None:
            return None
    else:
        fin_df = data
    if "ts_code" not in fin_df.columns:
        msg = "财务数据缺少 ts_code 列"
        logger.error(msg)
        if strict:
            raise RuntimeError(msg)
        return None

    # 4. 生成需要连接的唯一 ts_code 集合 (提升 join 性能)
    uniq_codes = fin_df.select("ts_code").unique()
    logger.info(f"财务数据唯一股票数: {uniq_codes.height}")

    # 5. 连接基础信息 (预先过滤行业后的 basic_df)
    how = "inner" if join_type not in ("left", "outer") else "left"
    join_df = uniq_codes.join(basic_df, on="ts_code", how=how)
    logger.info(f"行业映射 join 后行数: {join_df.height}")
    if join_df.is_empty():
        logger.warning("Join 后无匹配股票，返回空")
        return join_df

    # 6. 合并回财务数据（可选）
    if keep_financial:
        merged = join_df.join(fin_df, on="ts_code", how="inner")
    else:
        merged = join_df

    # 7. 列顺序整理: ts_code, name, industry 放最前
    front_cols = [c for c in ["ts_code", "name", "industry"] if c in merged.columns]
    other_cols = [c for c in merged.columns if c not in front_cols]
    final_cols = keep_columns if keep_columns else front_cols + other_cols
    merged = merged.select(final_cols)
    logger.info(f"最终过滤后行数={merged.height} 列数={merged.width}")

    return merged


def compute_hand_metrics(data: Optional[pl.DataFrame] = None,
                         tax_rate: float = 0.25,
                         cast_double: bool = True,
                         inplace: bool = False,
                         strict: bool = False) -> Optional[pl.DataFrame]:
    """为输入数据追加 5 个手工计算指标列:

    1. roe_cal = profit_dedt / bps
    2. roa_cal = netprofit_margin * assets_turn
    3. roic_cal = op_income / (bps * assets_to_eqt)
    4. fcf_margin_ps = fcff_ps / total_revenue_ps
    5. roic_tax = ebit * (1 - tax_rate) / invest_capital

    仅在依赖列全部存在时才追加该列；缺失时跳过并记录日志。

    参数:
        data: 输入 Polars DataFrame
        tax_rate: 税率 (用于 roic_tax)
        cast_double: 是否将参与计算的列统一尝试为 Float64 (防止字符串列导致结果 NULL)
        inplace: True 时原地在同一 DataFrame 上追加并返回；False 时复制一份
        strict: True 时若所有手工列均未生成则抛出异常
    """
    if data is None:
        logger.warning("没有输入数据 data=None")
        return None
    if not isinstance(data, pl.DataFrame):
        logger.error("compute_hand_metrics 仅支持 Polars DataFrame")
        return None
    df = data if inplace else data.clone()

    existing = set(df.columns)
    deps = {
        'roe_cal': ['profit_dedt', 'bps'],
        'roa_cal': ['netprofit_margin', 'assets_turn'],
        'roic_cal': ['op_income', 'bps', 'assets_to_eqt'],
        'fcf_margin_ps': ['fcff_ps', 'total_revenue_ps'],
        'roic_tax': ['ebit', 'invest_capital'],
    }
    produced = []
    def all_exists(cols: list[str]) -> bool:
        return all(c in existing for c in cols)
    def col_expr(name: str):
        c = pl.col(name)
        return c.cast(pl.Float64) if cast_double else c

    try:
        if all_exists(deps['roe_cal']):
            df = df.with_columns(
                (pl.when(col_expr('bps') > 0)
                 .then(col_expr('profit_dedt') / pl.when(col_expr('bps') == 0).then(None).otherwise(col_expr('bps')))
                 .otherwise(None)
                 ).alias('roe_cal')
            )
            produced.append('roe_cal')
        else:
            logger.info("跳过 roe_cal: 缺失列 %s", [c for c in deps['roe_cal'] if c not in existing])

        if all_exists(deps['roa_cal']):
            df = df.with_columns((col_expr('netprofit_margin') * col_expr('assets_turn')).alias('roa_cal'))
            produced.append('roa_cal')
        else:
            logger.info("跳过 roa_cal: 缺失列 %s", [c for c in deps['roa_cal'] if c not in existing])

        if all_exists(deps['roic_cal']):
            df = df.with_columns(
                (pl.when((col_expr('bps') > 0) & (col_expr('assets_to_eqt') != 0))
                 .then(col_expr('op_income') / pl.when((col_expr('bps') * col_expr('assets_to_eqt')) == 0).then(None).otherwise(col_expr('bps') * col_expr('assets_to_eqt')))
                 .otherwise(None)
                 ).alias('roic_cal')
            )
            produced.append('roic_cal')
        else:
            logger.info("跳过 roic_cal: 缺失列 %s", [c for c in deps['roic_cal'] if c not in existing])

        if all_exists(deps['fcf_margin_ps']):
            df = df.with_columns(
                (pl.when((col_expr('total_revenue_ps').is_not_null()) & (col_expr('total_revenue_ps') != 0))
                 .then(col_expr('fcff_ps') / pl.when(col_expr('total_revenue_ps') == 0).then(None).otherwise(col_expr('total_revenue_ps')))
                 .otherwise(None)
                 ).alias('fcf_margin_ps')
            )
            produced.append('fcf_margin_ps')
        else:
            logger.info("跳过 fcf_margin_ps: 缺失列 %s", [c for c in deps['fcf_margin_ps'] if c not in existing])

        if all_exists(deps['roic_tax']):
            df = df.with_columns(
                (pl.when(col_expr('invest_capital') > 0)
                 .then(col_expr('ebit') * (1 - tax_rate) / pl.when(col_expr('invest_capital') == 0).then(None).otherwise(col_expr('invest_capital')))
                 .otherwise(None)
                 ).alias('roic_tax')
            )
            produced.append('roic_tax')
        else:
            logger.info("跳过 roic_tax: 缺失列 %s", [c for c in deps['roic_tax'] if c not in existing])
    except Exception as e:
        logger.error(f"手工指标计算过程异常: {e}")
        if strict:
            raise

    if not produced:
        msg = "未生成任何手工指标列"
        if strict:
            raise RuntimeError(msg)
        logger.warning(msg)
    else:
        logger.info(f"追加手工指标列: {produced}")
    return df


def add_size_classification(
    data: Optional[pl.DataFrame] = None,
    capital_col: str = "invest_capital",
    size_col: str = "size_class",
    unit_scale: float = 1e8,
    thresholds: Optional[Dict[str, tuple]] = None,
    **kwargs
) -> Optional[pl.DataFrame]:
    """
    根据投入资本计算公司规模分类

    规模分类标准 (按投入资本，单位：亿元):
    - micro: < 10亿 (微型) - 流动性差，风险极高
    - small: 10-50亿 (小型) - 成长空间大，但波动剧烈
    - mid: 50-200亿 (中型) - 相对稳健，机构关注度提升
    - large: 200-1000亿 (大型) - 行业龙头，流动性好
    - mega: > 1000亿 (超大型) - 蓝筹白马，稳定性最高

    Args:
        data: 输入的Polars DataFrame
        capital_col: 投入资本列名，默认 "invest_capital"
        size_col: 输出的规模分类列名，默认 "size_class"
        unit_scale: 单位换算因子，默认 1e8 (转换为亿元)
        thresholds: 自定义阈值，格式 {"micro": (0, 10), "small": (10, 50), ...}
        **kwargs: 其他参数

    Returns:
        添加了 size_class 列的 DataFrame
    """
    if data is None:
        logger.warning("add_size_classification: 没有输入数据")
        return None

    if capital_col not in data.columns:
        logger.error(f"add_size_classification: 列 '{capital_col}' 不存在")
        return data

    # 默认阈值 (单位：亿元)
    if thresholds is None:
        thresholds = {
            "micro": (0, 10),       # 微型: < 10亿
            "small": (10, 50),      # 小型: 10-50亿
            "mid": (50, 200),       # 中型: 50-200亿
            "large": (200, 1000),   # 大型: 200-1000亿
            "mega": (1000, float('inf'))  # 超大型: > 1000亿
        }

    try:
        # 先转换为亿元
        capital_yi = pl.col(capital_col) / unit_scale

        # 使用 when-then-otherwise 链式判断
        size_expr = (
            pl.when(capital_yi < thresholds["micro"][1])
            .then(pl.lit("micro"))
            .when(capital_yi < thresholds["small"][1])
            .then(pl.lit("small"))
            .when(capital_yi < thresholds["mid"][1])
            .then(pl.lit("mid"))
            .when(capital_yi < thresholds["large"][1])
            .then(pl.lit("large"))
            .otherwise(pl.lit("mega"))
        )

        df = data.with_columns(size_expr.alias(size_col))

        # 统计各规模分布
        size_dist = df.group_by(size_col).agg(pl.len().alias("count")).sort("count", descending=True)
        dist_str = ", ".join([f"{row[size_col]}={row['count']}" for row in size_dist.iter_rows(named=True)])
        logger.info(f"✅ 规模分类完成: {dist_str}")

        return df

    except Exception as e:
        logger.error(f"add_size_classification 失败: {e}")
        return data

