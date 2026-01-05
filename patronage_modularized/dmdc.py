"""patronage_modularized.dmdc

DMDC export generation for the Patronage pipeline.

Responsibilities:
- Determine if today is a DMDC transfer day (Wed/Fri).
- Query eligible active Patronage records within the incremental extraction
    window.
- Build fixed-width DMDC records (via `build_dmdc_record_sql`).
- Write a newline-delimited text file to DBFS.
- Record a checkpoint row with timestamp, filename, record_count, and query.

Operational note (important for reliability in Jobs):
- pandas and standard Python file APIs expect local filesystem paths.
- Databricks provides a DBFS FUSE mount at `/dbfs/...`.
- Therefore we convert `dbfs:/...` paths into `/dbfs/...` before writing.
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timedelta, timezone
from typing import Tuple

from databricks.sdk.runtime import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import asc, col

from .config import (
    DMDC_CHECKPOINT_TABLE_NAME,
    DMDC_ELIGIBILITY_FILTER,
    DMDC_EXPORT_DIR,
    PY_DATE_COMPACT_FORMAT,
    PATRONAGE_TABLE_NAME,
    SOURCE_TYPE_CG,
    SOURCE_TYPE_SCD,
    build_dmdc_record_sql,
    log_message,
    to_local_fuse_path,
    write_unix_text_file_no_blank_eof,
)


def is_dmdc_transfer_day() -> bool:
    """Return True if today is a scheduled DMDC transfer day.

    Transfer days are Wednesday and Friday.
    """
    return datetime.now(timezone.utc).date().weekday() in [2, 4]  # Wed=2, Fri=4


def _format_spark_timestamp_utc(dt: datetime) -> str:
    """Format a datetime as a Spark SQL TIMESTAMP literal in UTC.

    Spark often returns naive Python datetimes for TIMESTAMP columns.
    We treat naive values as UTC (this is correct when Spark session timezone
    is pinned to UTC, which the orchestrator now enforces).

    Returns:
        Timestamp formatted as `YYYY-MM-DD HH:MM:SS`.
    """
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _to_local_fuse_path(dbfs_path: str) -> str:
    """Backward-compatible wrapper (prefer `config.to_local_fuse_path`)."""
    return to_local_fuse_path(dbfs_path)


def _get_dmdc_last_run_date() -> datetime:
    """Get the most recent DMDC export checkpoint timestamp.

    Returns:
        The max `checkpoint_timestamp` in the checkpoint table, or "yesterday"
        if the table is empty or cannot be queried.
    """
    try:
        result = spark.sql(f"SELECT max(checkpoint_timestamp) FROM {DMDC_CHECKPOINT_TABLE_NAME}").first()
        last_run = result[0] if result and result[0] else (datetime.now(timezone.utc) - timedelta(days=1))
        log_message(f"Last DMDC export run date: {last_run}", level="DEBUG", depth=2)
        return last_run
    except Exception as e:
        log_message(f"Could not get last run date, using yesterday. Error: {e}", level="WARN", depth=2)
        return datetime.now(timezone.utc) - timedelta(days=1)


def _query_dmdc_records(last_run_date: datetime, today_start_time: datetime) -> Tuple[DataFrame, str]:
    """Query eligible Patronage records to send to DMDC.

    Args:
        last_run_date: Lower bound (inclusive) from checkpoint table.
        today_start_time: Upper bound (exclusive), typically UTC midnight.

    Returns:
        A tuple of:
        - DataFrame containing DMDC record payload + sort keys.
        - The SQL string used (stored in checkpoint for auditability).

    Notes:
        - Filters to active records (`RecordStatus = true`).
        - Only records with non-null EDIPI are exportable.
        - Ordering is stable by `(Batch_CD, edipi)`.
    """
    last_run_literal = _format_spark_timestamp_utc(last_run_date)
    today_start_literal = _format_spark_timestamp_utc(today_start_time)
    query = f"""
        SELECT 
            {build_dmdc_record_sql('edipi')} as DMDC_Record,
            Batch_CD,
            edipi
        FROM {PATRONAGE_TABLE_NAME}
        WHERE RecordStatus = true
        AND SDP_Event_Created_Timestamp >= '{last_run_literal}'
        AND SDP_Event_Created_Timestamp < '{today_start_literal}'
        AND edipi IS NOT NULL
        AND ({DMDC_ELIGIBILITY_FILTER})
    """
    log_message("Executing DMDC export query...", level="DEBUG", depth=1)
    dmdc_df = spark.sql(query).orderBy(asc("Batch_CD"), asc("edipi"))

    return dmdc_df, query


def _write_dmdc_file(dmdc_df: DataFrame, record_count: int) -> str:
    """Write the DMDC export file to DBFS.

    Args:
        dmdc_df: DataFrame containing a `DMDC_Record` string column.
        record_count: Precomputed count for logging.

    Returns:
        The DBFS output path where the file was written.

    Side effects:
        Creates the export directory (if needed) and writes a newline-delimited
        file.
    """
    current_date_str = datetime.now(timezone.utc).date().strftime(PY_DATE_COMPACT_FORMAT)
    filename = f"PATRONAGE_{current_date_str}.txt"
    output_path = f"{DMDC_EXPORT_DIR}/{filename}"

    log_message(f"Total number of records to be sent to DMDC: {record_count:,}", depth=1)
    log_message(f"Filename: {filename}", depth=1)
    log_message(f"Writing {record_count:,} records to {output_path}", depth=1)

    dbutils.fs.mkdirs(DMDC_EXPORT_DIR)

    pandas_df = dmdc_df.select("DMDC_Record").toPandas()
    records = pandas_df["DMDC_Record"].astype(str).tolist() if "DMDC_Record" in pandas_df.columns else []
    write_unix_text_file_no_blank_eof(output_path, records, encoding="utf-8")

    local_path = _to_local_fuse_path(output_path)

    from . import config

    if config.LOGGING_VERBOSE:
        try:
            log_message(f"File size: {os.path.getsize(local_path) / 1024:.2f} KB", depth=2)
        except Exception:
            pass

    return output_path


def _update_dmdc_checkpoint(today_start_time: datetime, output_path: str, record_count: int, query: str) -> None:
    """Record a checkpoint row for the DMDC export.

    Args:
        today_start_time: The checkpoint timestamp (typically UTC midnight).
        output_path: Path of the written export file.
        record_count: Number of exported records.
        query: The SQL query used to generate the export.

    Notes:
        We escape single quotes to store the query as a string literal.
    """
    try:
        # Store a normalized one-line query for easier auditing/grep.
        # (Do not change the executed SQL; only the stored string.)
        normalized_query = re.sub(r"\s+", " ", query).strip()
        escaped_query = normalized_query.replace("'", "\\'")
        checkpoint_literal = _format_spark_timestamp_utc(today_start_time)
        spark.sql(
            f"""
            INSERT INTO {DMDC_CHECKPOINT_TABLE_NAME} (checkpoint_timestamp, filename, record_count, generate_query)
            VALUES ('{checkpoint_literal}', '{output_path}', {record_count}, '{escaped_query}')
        """
        )
        log_message("DMDC checkpoint table updated.", depth=2)
    except Exception as e:
        log_message(f"Failed to update DMDC checkpoint table. Error: {e}", level="ERROR", depth=2)


def generate_dmdc_transfer_file() -> None:
    """Generate and checkpoint the DMDC export file for the current run window.

    Workflow:
        1) Determine last run timestamp from checkpoint table.
        2) Compute today's UTC midnight for end-of-window.
        3) Query eligible records.
        4) Write file to DBFS and insert checkpoint row.

    Raises:
        Propagates exceptions from Spark/pandas writes after logging.
    """
    log_message("Starting DMDC Data Transfer File Generation...")

    last_run_date = _get_dmdc_last_run_date()
    today_start_time = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    log_message(f"Extraction window: {last_run_date} to {today_start_time}", depth=1)

    dmdc_df, query = _query_dmdc_records(last_run_date, today_start_time)
    record_count = dmdc_df.count()
    log_message(f"Found {record_count:,} records to export.", depth=1)

    if record_count == 0:
        log_message("No new records to send to DMDC.", depth=1)
        return

    from . import config

    if config.LOGGING_VERBOSE:
        cg_count = dmdc_df.filter(col("Batch_CD") == SOURCE_TYPE_CG).count()
        scd_count = dmdc_df.filter(col("Batch_CD") == SOURCE_TYPE_SCD).count()
        log_message(f"     Number of Caregivers records: {cg_count}")
        log_message(f"     Number of SCD records: {scd_count}")

    output_path = _write_dmdc_file(dmdc_df, record_count)
    _update_dmdc_checkpoint(today_start_time, output_path, record_count, query)

    log_message("DMDC Data Transfer File Generation complete.", depth=1)
