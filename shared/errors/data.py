"""
数据错误 (Data Errors)
=======================

数据加载、处理、存储相关的错误定义。
"""
from pathlib import Path
from typing import Any, Dict, List, Optional

from .base import AStockError
from .codes import ErrorCode


class DataError(AStockError):
    """数据处理基础错误"""
    default_code = ErrorCode.DATA_LOAD_FAILED


class FileNotFoundError(DataError):
    """文件未找到错误

    注意：此类覆盖内置 FileNotFoundError，在 AStock 上下文中提供更丰富的信息。
    """
    default_code = ErrorCode.DATA_FILE_NOT_FOUND

    def __init__(
        self,
        file_path: str | Path,
        *,
        searched_paths: Optional[List[str]] = None,
        expected_extension: Optional[str] = None,
        **kwargs
    ):
        path = Path(file_path)
        message = f"File not found: '{path}'"

        if expected_extension:
            message += f" (expected extension: {expected_extension})"

        super().__init__(message, **kwargs)

        self.with_context(
            file_path=str(path),
            file_name=path.name,
            directory=str(path.parent),
            searched_paths=searched_paths,
            expected_extension=expected_extension,
        )


class FileFormatError(DataError):
    """文件格式错误"""
    default_code = ErrorCode.DATA_FORMAT_ERROR

    def __init__(
        self,
        file_path: str | Path,
        expected_format: str,
        *,
        actual_format: Optional[str] = None,
        reason: str = "",
        **kwargs
    ):
        path = Path(file_path)
        message = f"Invalid file format for '{path.name}': expected {expected_format}"
        if actual_format:
            message += f", got {actual_format}"
        if reason:
            message += f" ({reason})"

        super().__init__(message, **kwargs)

        self.with_context(
            file_path=str(path),
            expected_format=expected_format,
            actual_format=actual_format,
            reason=reason,
        )


class DataLoadError(DataError):
    """数据加载错误"""
    default_code = ErrorCode.DATA_LOAD_FAILED

    def __init__(
        self,
        source: str,
        *,
        reason: str = "",
        row_count: Optional[int] = None,
        column_count: Optional[int] = None,
        **kwargs
    ):
        message = f"Failed to load data from '{source}'"
        if reason:
            message += f": {reason}"

        super().__init__(message, **kwargs)

        self.with_context(
            source=source,
            reason=reason,
            row_count=row_count,
            column_count=column_count,
        )


class DataTransformError(DataError):
    """数据转换错误"""
    default_code = ErrorCode.DATA_TRANSFORM_FAILED

    def __init__(
        self,
        operation: str,
        *,
        input_shape: Optional[tuple] = None,
        output_shape: Optional[tuple] = None,
        column_name: Optional[str] = None,
        reason: str = "",
        **kwargs
    ):
        message = f"Data transformation failed during '{operation}'"
        if column_name:
            message += f" on column '{column_name}'"
        if reason:
            message += f": {reason}"

        super().__init__(message, **kwargs)

        self.with_context(
            operation=operation,
            input_shape=input_shape,
            output_shape=output_shape,
            column_name=column_name,
            reason=reason,
        )


class ColumnNotFoundError(DataError):
    """列未找到错误"""
    default_code = ErrorCode.DATA_COLUMN_NOT_FOUND

    def __init__(
        self,
        column_name: str | List[str],
        *,
        available_columns: Optional[List[str]] = None,
        dataframe_name: Optional[str] = None,
        **kwargs
    ):
        if isinstance(column_name, list):
            columns = column_name
            message = f"Columns not found: {columns}"
        else:
            columns = [column_name]
            message = f"Column not found: '{column_name}'"

        if dataframe_name:
            message += f" in DataFrame '{dataframe_name}'"

        super().__init__(message, **kwargs)

        self.with_context(
            missing_columns=columns,
            available_columns=available_columns,
            dataframe_name=dataframe_name,
        )


class EmptyDataError(DataError):
    """空数据错误"""
    default_code = ErrorCode.DATA_EMPTY

    def __init__(
        self,
        source: str,
        *,
        expected_min_rows: Optional[int] = None,
        filter_applied: Optional[str] = None,
        **kwargs
    ):
        message = f"Empty data received from '{source}'"
        if expected_min_rows:
            message += f" (expected at least {expected_min_rows} rows)"
        if filter_applied:
            message += f" after filter: {filter_applied}"

        super().__init__(message, **kwargs)

        self.with_context(
            source=source,
            expected_min_rows=expected_min_rows,
            filter_applied=filter_applied,
        )


class DataIntegrityError(DataError):
    """数据完整性错误"""
    default_code = ErrorCode.DATA_INTEGRITY_ERROR

    def __init__(
        self,
        check_name: str,
        *,
        expected: Optional[Any] = None,
        actual: Optional[Any] = None,
        affected_rows: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
        **kwargs
    ):
        message = f"Data integrity check failed: '{check_name}'"
        if expected is not None and actual is not None:
            message += f" (expected {expected}, got {actual})"
        if affected_rows:
            message += f" ({affected_rows} rows affected)"

        super().__init__(message, **kwargs)

        self.with_context(
            check_name=check_name,
            expected=expected,
            actual=actual,
            affected_rows=affected_rows,
            details=details or {},
        )


class DatabaseError(DataError):
    """数据库错误"""
    default_code = ErrorCode.DATA_LOAD_FAILED

    def __init__(
        self,
        operation: str,
        *,
        database: str = "duckdb",
        query: Optional[str] = None,
        reason: str = "",
        **kwargs
    ):
        message = f"Database operation '{operation}' failed"
        if database:
            message += f" (database: {database})"
        if reason:
            message += f": {reason}"

        super().__init__(message, **kwargs)

        # 注意：SQL 查询可能包含敏感信息，生产环境应考虑脱敏
        self.with_context(
            operation=operation,
            database=database,
            query=query[:500] if query and len(query) > 500 else query,
            reason=reason,
        )


class SerializationError(DataError):
    """序列化错误"""
    default_code = ErrorCode.DATA_TRANSFORM_FAILED

    def __init__(
        self,
        operation: str,  # "serialize" or "deserialize"
        format_type: str,  # "json", "csv", "parquet", "pickle"
        *,
        reason: str = "",
        file_path: Optional[str] = None,
        **kwargs
    ):
        message = f"Failed to {operation} data as {format_type}"
        if file_path:
            message += f" for file '{file_path}'"
        if reason:
            message += f": {reason}"

        super().__init__(message, **kwargs)

        self.with_context(
            operation=operation,
            format_type=format_type,
            file_path=file_path,
            reason=reason,
        )
