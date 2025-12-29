"""patronage_modularized.discovery

File discovery logic for the Patronage pipeline.

This module is responsible for determining which inbound source files should be
processed on a given run.

Key behaviors:
- Two modes:
    - "rebuild": process from the configured beginning date up to an end-of-window
        boundary.
    - "update": process from the last processed timestamp recorded in the target
        table for that source type.
- File eligibility is based on file modification time and filename pattern.

Databricks assumptions:
- Requires `spark` and `dbutils` provided by Databricks Runtime.
- Paths in `PIPELINE_CONFIG` are expected to be readable by `dbutils.fs.ls`.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

from databricks.sdk.runtime import *

from pyspark.sql.functions import col, lit

from .config import (
    PIPELINE_CONFIG,
    PY_DATE_FORMAT,
    PY_DATETIME_FORMAT,
    PATRONAGE_TABLE_NAME,
    SOURCE_TYPE_CG,
    SOURCE_TYPE_SCD,
    log_message,
)


def _get_last_processed_timestamp(source_type: str) -> Optional[datetime]:
    """Fetch the last processed timestamp for a given source from the target table.

    The Patronage target table stores `SDP_Event_Created_Timestamp` for each
    processed record. For update mode, we use the maximum value for the
    specified `Batch_CD` (source type) as the lower bound.

    Args:
        source_type: Source identifier (e.g., `SOURCE_TYPE_SCD`, `SOURCE_TYPE_CG`).

    Returns:
        A timezone-aware UTC datetime if available; otherwise `None`.

    Notes:
        - If the query fails or returns NULL, `None` is returned to allow the
          caller to fall back to configured beginning dates.
        - Databricks may return naive timestamps depending on settings; we
          normalize to UTC when tzinfo is missing.
    """
    try:
        query = f"SELECT MAX(SDP_Event_Created_Timestamp) FROM {PATRONAGE_TABLE_NAME} WHERE Batch_CD = '{source_type}'"
        result = spark.sql(query).collect()[0][0]
        if result:
            log_message(f"{source_type}: Last processed timestamp from table: {result}", level="DEBUG", depth=1)
            if result.tzinfo is None:
                return result.replace(tzinfo=timezone.utc)
            return result
    except Exception as e:
        log_message(f"{source_type}: Could not get last processed timestamp: {e}. Will process all files.", depth=1)

    return None


def _calculate_time_boundary(processing_mode: str) -> datetime:
    """Compute the exclusive upper bound for file discovery.

    Args:
        processing_mode: Either "rebuild" or "update".

    Returns:
        A timezone-aware UTC datetime used as an exclusive upper bound.

    Behavior:
        - rebuild: boundary is the last second of the previous month relative to
          the current month.
        - update: boundary is midnight (start of day) UTC for the current day.
    """
    current_time_utc = datetime.now(timezone.utc)
    if processing_mode == "rebuild":
        first_day_of_month = current_time_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return (first_day_of_month - timedelta(days=1)).replace(hour=23, minute=59, second=59)

    return current_time_utc.replace(hour=0, minute=0, second=0, microsecond=0)


def discover_unprocessed_files(processing_mode: str) -> Dict[str, List[Tuple[str, str]]]:
    """Discover unprocessed inbound files per configured source.

    For each source in `PIPELINE_CONFIG`, this scans the configured directory and
    returns files whose modification time is within the computed time window and
    whose filename matches the expected prefix/extension.

    Args:
        processing_mode: Either "rebuild" or "update".

    Returns:
        Dict mapping source type -> list of tuples: (file_path, modification_time_iso).

    Notes:
        - We sort results by modification time ascending for deterministic
          processing order.
        - This function is intentionally conservative: if a start time cannot be
          determined for a source, it logs and skips the source.
    """
    log_message(f"Discovering unprocessed sources ({processing_mode} mode)...")
    unprocessed_files: Dict[str, List[Tuple[str, str]]] = {key: [] for key in PIPELINE_CONFIG.keys()}

    time_boundary_utc = _calculate_time_boundary(processing_mode)
    log_message(f"Global time boundary: {time_boundary_utc}", level="DEBUG")

    for source_type, config in PIPELINE_CONFIG.items():
        log_message(f"Checking source: {source_type}", depth=1)

        start_time_utc = None
        if processing_mode == "update":
            start_time_utc = _get_last_processed_timestamp(source_type)

        if start_time_utc is None and "beginning_date" in config:
            date_format = PY_DATE_FORMAT if source_type == SOURCE_TYPE_SCD else PY_DATETIME_FORMAT
            start_time_naive = datetime.strptime(config["beginning_date"], date_format)
            start_time_utc = start_time_naive.replace(tzinfo=timezone.utc)

        if not start_time_utc:
            log_message(f"Warning: No start time could be determined for {source_type}. Skipping.", depth=2)
            continue

        log_message(
            f"Time window for {source_type}: {start_time_utc} -> {time_boundary_utc}", level="DEBUG", depth=2
        )

        try:
            for file_info in dbutils.fs.ls(config["path"]):
                mod_time_utc = datetime.fromtimestamp(file_info.modificationTime / 1000, tz=timezone.utc)

                if not (start_time_utc < mod_time_utc < time_boundary_utc):
                    continue

                is_match = file_info.name.lower().startswith(config["matching_characters"].lower()) and file_info.name.lower().endswith(
                    f".{config['file_extension'].lower()}"
                )

                if is_match:
                    unprocessed_files[source_type].append((file_info.path, mod_time_utc.isoformat()))

        except Exception as e:
            log_message(
                f"Warning: Could not access {source_type} files at {config['path']}: {str(e)}", depth=2
            )

        num_files = len(unprocessed_files[source_type])
        if num_files > 0:
            unprocessed_files[source_type].sort(key=lambda x: x[1])
            log_message(f"Found {num_files} unprocessed files for {source_type}.", depth=1)
        else:
            log_message(f"No unprocessed files found for {source_type}.", depth=1)

    return unprocessed_files
