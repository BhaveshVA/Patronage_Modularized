"""patronage_modularized.orchestrator

VA Patronage Data Pipeline (Modularized)

This module is the primary entrypoint that wires together the Patronage pipeline:
- table initialization (target table + DMDC checkpoint table)
- identity correlation loading (ICN <-> EDIPI/participant_id)
- inbound file discovery (SCD + CG)
- transformations and SCD Type 2 merges into the Patronage Delta table
- scheduled tasks (EDIPI backfill, DMDC export)

Execution environment
--------------------
- Designed to run in Databricks where `spark` and `dbutils` are available.
- Uses Delta Lake (`delta.tables.DeltaTable`) and Spark SQL.
- Spark session timezone is pinned to UTC (defensive) to avoid DST boundary drift.
- Python-side date logic is UTC-anchored for the same reason.

BUSINESS CONTEXT
================
This pipeline supports the VA's coordination with the Department of Defense (DoD)
for Veteran benefits administration.

ABBREVIATIONS
=============
- VA: Department of Veterans Affairs
- DoD: Department of Defense
- DMDC: Defense Manpower Data Center
- SCD: Service-Connected Disability (source feed)
- CG: Caregiver (source feed)
- PT: Permanent & Total (100% disability rating, permanent)
- ICN: Integration Control Number (VA's master person identifier)
- EDIPI: Electronic Data Interchange Person Identifier (DoD's person ID)
- VBA: Veterans Benefits Administration
- VADIR: VA/DoD Identity Repository
- CARMA: Caregiver Record Management Application
- MVI: Master Veteran Index (source for identity correlation)
- SCD Type 2 / SCD2: Slowly Changing Dimension Type 2 (historical versioning)
- Delta / Delta Lake: Databricks Delta format used for the target table and merges
- DBFS: Databricks File System (dbfs:/ vs /dbfs path semantics)

WHY THIS EXISTS:
- Veterans with Service-Connected Disabilities may be eligible for commissary /
    exchange benefits at military installations.
- Caregivers of eligible Veterans may also qualify for these benefits.
- VA shares eligibility data with DoD's Defense Manpower Data Center (DMDC).
- This pipeline processes eligibility updates and generates DMDC transfer files.

KEY BUSINESS RULES (high level)
===============================
1) PT "Sticky Y" rule:
     Once PT (Permanent & Total) is 'Y' for a Veteran, it remains 'Y' forever.
     The pipeline must never downgrade PT from 'Y' back to 'N'.

2) SCD Type 2 history:
     We preserve a full change history. When a tracked field changes:
     - the old active record is expired (RecordStatus=false)
     - a new active version is inserted (RecordStatus=true)

3) Dual source processing:
     - SCD: Service Connected Disability updates (Veteran eligibility signals).
     - CG: Caregiver eligibility updates.

4) Deterministic deduplication:
     Source data is deduplicated by business keys before merging to prevent
     multiple active rows for the same entity.

DATA FLOW (conceptual)
======================
Inbound files (SCD + CG) --> Transform --> Patronage Delta table --> DMDC exports
                                                                     |
                                                        Identity correlation
                                                    (ICN <-> EDIPI/participant_id)

TABLE OF CONTENTS
=================
SECTION 1: Table initialization & shared state
SECTION 2: Identity correlation lookup
SECTION 3: File discovery (rebuild vs update)
SECTION 4: Data transformation (SCD + CG)
SECTION 5: SCD Type 2 merge semantics (Delta Lake)
SECTION 6: Scheduled tasks (EDIPI backfill + DMDC export)
SECTION 7: Monitoring & freshness checks (operational)
SECTION 8: Operational notes (timezones, DBFS paths, troubleshooting)

SECTION 1: TABLE INITIALIZATION & SHARED STATE
==============================================
Entry points:
- initialize_all_tables()

Responsibilities:
- Ensure Patronage target Delta table exists (schema + location).
- Ensure DMDC checkpoint table exists (incremental export watermarking).
- Initialize in-memory run state in `patronage_modularized.state`.

SECTION 2: IDENTITY CORRELATION LOOKUP
======================================
Entry points:
- identity.initialize_identity_lookup()

Responsibilities:
- Load/refresh identity correlation used to map identifiers such as:
    ICN, EDIPI, and participant_id.
- This lookup is required to populate EDIPI reliably for DMDC export, and to
    support backfills when identifiers arrive asynchronously.

SECTION 3: FILE DISCOVERY (REBUILD VS UPDATE)
=============================================
Entry points:
- discovery.discover_unprocessed_files(processing_mode)

Modes:
- rebuild:
    Processes historical files from configured "beginning_date" up to an
    end-of-window boundary.
- update:
    Processes files newer than the last processed watermark in the Patronage
    target table, per source type.

Notes:
- File eligibility is based on DBFS modification timestamps and filename patterns.
- Discovery boundaries are treated as UTC to avoid DST issues.

SECTION 4: DATA TRANSFORMATION (SCD + CG)
=========================================
Entry points (high level):
- transforms.create_and_transform_source_dataframe(...)
- transforms.get_pt_data_for_mode(...) for SCD PT lookup rules
- transforms.initialize_caregiver_seed_data() for CG seed handling

SCD transformation scenarios (easy reference)
---------------------------------------------
The SCD transformation logic conceptually handles these scenarios:

Scenario 1: New SCD record (no PT correlation)
- A Veteran is new to the target and PT is not found (or not asserted).
- Result: insert new row, PT defaults to 'N'.

Scenario 2: New SCD record (PT found / asserted)
- A Veteran is new to the target and PT is known as 'Y' from PT source.
- Result: insert new row with PT='Y'.

Scenario 3: Existing record update (Disability % and PT change)
- Veteran exists and both disability percentage and PT value change (commonly N->Y).
- Result: expire old row + insert new row.

Scenario 4: Existing record update (Disability % change only)
- Veteran exists and disability percentage changes but PT does not change.
- Result: expire old row + insert new row.

Scenario 5: Existing record update (PT change only)
- Veteran exists and PT changes (typically N->Y) without a disability % change.
- Result: expire old row + insert new row.

Scenario 6: PT backfill for records NOT in today’s SCD file
- PT source indicates a Veteran should be PT='Y', but they are not present in the
    day’s inbound SCD file.
- Result: create PT-only changes for affected Veterans so PT is corrected to 'Y'
    while respecting "Sticky Y".

Scenario 7: Edge/fallback identifier resolution
- Correlation for participant_id/ICN/EDIPI may be incomplete for some inbound rows
    (or arrives later than SCD data).
- Result: use safe fallback logic (from target table where possible) to avoid
    dropping records and to keep exports consistent.

CG transformation and seed initialization (why seed matters)
------------------------------------------------------------
CG has a one-time seed behavior during rebuild:
- In rebuild mode, if the target table is empty, CG seed initialization loads an
    initial “baseline” caregiver population.
- This matters because CG eligibility is not always fully reconstructible from
    only incremental “daily update” files. Without seeding, a rebuild could produce
    a target table missing eligible caregivers until they happen to appear again in
    future inbound updates.
- Operationally: CG is evaluated first in rebuild to ensure seed logic can run
    while the table is still empty.

SECTION 5: SCD TYPE 2 MERGE (DELTA LAKE)
========================================
Entry points:
- scd2.execute_scd_pipeline_for_source(transformed_df, source_type)

Responsibilities:
- Insert brand-new records.
- Expire prior active records when a change is detected.
- Insert new active versions representing the latest state.
- Preserve historical rows for auditability and replay reasoning.

SECTION 6: SCHEDULED TASKS
==========================
1) Monthly EDIPI backfill (last Friday of month)
     Entry points:
     - scheduled.is_last_friday_of_month()
     - scheduled.run_edipi_backfill()

     Purpose:
     - Identify active Patronage rows missing EDIPI.
     - Join to identity correlation to find EDIPI candidates.
     - Generate a DMDC-format export file for the backfilled population.
     - Update the Patronage table to persist EDIPI.

2) DMDC export (Wednesday / Friday)
     Entry points:
     - dmdc.is_dmdc_transfer_day()
     - dmdc.generate_dmdc_transfer_file()

     Purpose:
     - Export eligible active Patronage rows for DMDC consumption.
     - Use a checkpoint table to implement incremental extraction windows.

SECTION 7: MONITORING & FRESHNESS CHECKS (OPERATIONAL)
======================================================
Monitoring is implemented as a Databricks job that runs a dedicated monitoring
notebook. The monitoring notebook depends on the runner notebook (or runner-like
logic) as the source of truth for configuration, discovery, and logging behavior.

Typical monitoring responsibilities:
- Confirm inbound directories contain recent files (SCD, CG, DMDC exports).
- Confirm Delta tables are present and recently updated.
- Enforce freshness SLAs (daily/weekly/monthly as appropriate per source).
- Emit a clear PASS/FAIL signal (and fail the job on critical freshness gaps).

Notes:
- Keeping monitoring logic in a separate notebook makes it safe to run frequently
    without mutating production tables (except for reading metadata).
- Monitoring should use UTC boundaries for consistency with pipeline execution.

SECTION 8: OPERATIONAL NOTES
============================
Timezones / DST:
- Spark session timezone is pinned to UTC to avoid DST-related boundary drift.
- Python-side date logic uses UTC so scheduling gates and filenames do not depend
    on driver OS timezone.

DBFS paths:
- Use `dbfs:/...` with `dbutils.fs.*` operations (ls/mkdirs).
- Convert to `/dbfs/...` for Python file I/O (pandas writes, os.path, etc.).

Troubleshooting themes:
- Misconfigured DBFS paths (dbfs vs /dbfs vs dbfs/mnt).
- Missing permissions to read source mounts or write export directories.
- Empty/missing checkpoint tables for DMDC export.
"""

from __future__ import annotations
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from databricks.sdk.runtime import *
from delta.tables import DeltaTable
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from . import config, state
from .config import (
    DMDC_CHECKPOINT_TABLE_NAME,
    DUP_IDENTITY_TABLE_NAME,
    IDENTITY_TABLE_NAME,
    PATRONAGE_TABLE_CLUSTER_COLUMNS,
    PATRONAGE_TABLE_NAME,
    PATRONAGE_TABLE_PATH,
    PATRONAGE_TABLE_SCHEMA,
    PATRONAGE_PIPELINE_LOG_SCHEMA,
    PATRONAGE_PIPELINE_LOG_TABLE_NAME,
    PIPELINE_CONFIG,
    SOURCE_TYPE_CG,
    SOURCE_TYPE_SCD,
    PY_DATE_FORMAT,
    PY_DATETIME_FORMAT,
    log_message,
    optimize_delta_table,
)
from .discovery import _calculate_time_boundary, _get_last_processed_timestamp, discover_unprocessed_files
from .dmdc import generate_dmdc_transfer_file, is_dmdc_transfer_day
from .identity import initialize_identity_lookup
from .scd2 import execute_scd_pipeline_for_source
from .scheduled import is_first_day_of_month, is_last_friday_of_month, run_edipi_backfill, run_monthly_backups
from .transforms import create_and_transform_source_dataframe, get_pt_data_for_mode, initialize_caregiver_seed_data


def reset_patronage_table() -> None:
    """Fully reset the Patronage Delta table (metadata + underlying files).

    This is a destructive operation intended for "rebuild" mode only.

    Notes:
        - Drops the SQL table if it exists.
        - Deletes the underlying Delta storage path (recursive).
        - Does not modify the DMDC checkpoint table.
    """
    log_message("REBUILD: FULL RESET of Patronage target table requested.", level="WARN", depth=1)
    log_message(f"Dropping table if exists: {PATRONAGE_TABLE_NAME}", level="WARN", depth=2)
    spark.sql(f"DROP TABLE IF EXISTS {PATRONAGE_TABLE_NAME}")

    # Clear any cached handle so downstream code must re-initialize.
    state.target_delta_table = None

    log_message(f"Deleting Delta files at: {PATRONAGE_TABLE_PATH}", level="WARN", depth=2)
    try:
        dbutils.fs.rm(PATRONAGE_TABLE_PATH, True)
    except Exception as e:
        message = str(e)
        if "FileNotFoundException" in message or "does not exist" in message or "No such file" in message:
            log_message("Delta path not found; skipping delete.", level="DEBUG", depth=3)
        else:
            log_message(f"Failed to delete Patronage Delta path: {e}", level="ERROR", depth=2)
            raise


def initialize_all_tables() -> None:
    """Create required Delta tables if they do not already exist.

    Side effects:
        - Ensures the Patronage target table exists at `PATRONAGE_TABLE_PATH`.
        - Initializes `state.target_delta_table` for downstream merges.
        - Ensures the DMDC checkpoint table exists.

    Notes:
        Cluster/partitioning choices are configured in `config.py`.
    """
    log_message("Initializing all pipeline tables...")

    log_message(f"Initializing table: {PATRONAGE_TABLE_NAME}", depth=1)
    patronage_create_sql = f"""
        CREATE TABLE IF NOT EXISTS {PATRONAGE_TABLE_NAME} (
            {PATRONAGE_TABLE_SCHEMA}
        ) CLUSTER BY ({PATRONAGE_TABLE_CLUSTER_COLUMNS}) LOCATION '{PATRONAGE_TABLE_PATH}'
    """
    spark.sql(patronage_create_sql)
    state.target_delta_table = DeltaTable.forPath(spark, PATRONAGE_TABLE_PATH)
    log_message(f"Table '{PATRONAGE_TABLE_NAME}' is ready.", level="DEBUG", depth=1)

    log_message(f"Initializing table: {DMDC_CHECKPOINT_TABLE_NAME}", depth=1)
    checkpoint_create_sql = f"""
        CREATE TABLE IF NOT EXISTS {DMDC_CHECKPOINT_TABLE_NAME} (
            checkpoint_timestamp TIMESTAMP,
            filename STRING,
            record_count LONG,
            generate_query STRING
        )
    """
    spark.sql(checkpoint_create_sql)
    log_message(f"Table '{DMDC_CHECKPOINT_TABLE_NAME}' is ready.", level="DEBUG", depth=1)

    log_message(f"Initializing table: {PATRONAGE_PIPELINE_LOG_TABLE_NAME}", depth=1)
    pipeline_log_create_sql = f"""
        CREATE TABLE IF NOT EXISTS {PATRONAGE_PIPELINE_LOG_TABLE_NAME} (
            {PATRONAGE_PIPELINE_LOG_SCHEMA}
        )
    """
    spark.sql(pipeline_log_create_sql)
    log_message(f"Table '{PATRONAGE_PIPELINE_LOG_TABLE_NAME}' is ready.", level="DEBUG", depth=1)


def process_patronage_data(
    processing_mode: str, verbose_logging: bool = False
) -> Tuple[bool, Dict[str, int], Dict[str, Any]]:
    """Run the core Patronage processing for SCD/CG sources.

    Args:
        processing_mode: "rebuild" or "update".
                        - "rebuild" is destructive: it drops the Patronage target table and deletes the underlying Delta files
                            before reprocessing historical inputs.
        verbose_logging: Enables additional debug metrics and counts.

    Returns:
        Tuple of:
            - True if any new source files were processed; otherwise False.
            - Dict of raw/processed row counts by source.
            - File discovery detail snapshot (start/end window + file counts).

    High-level workflow:
        1) Initialize Delta tables and shared state.
        2) Refresh/load identity lookup (daily rebuild if needed).
        3) Discover inbound files per source.
        4) Transform and merge:
           - SCD update mode: process files individually.
           - Otherwise: process files in a single batch per source.

    Cleanup:
        Unpersists cached PT data at the end of the run.
    """
    config.LOGGING_VERBOSE = verbose_logging
    log_message(f"Starting Patronage Pipeline in {processing_mode.upper()} mode")
    pipeline_timer = config.Stopwatch()

    raw_processed_counts: Dict[str, int] = {
        "raw_cg_rows": 0,
        "processed_cg_rows": 0,
        "raw_scd_rows": 0,
        "processed_scd_rows": 0,
    }

    try:
        if processing_mode == "rebuild":
            reset_patronage_table()

        initialize_all_tables()
        if state.target_delta_table is None:
            raise RuntimeError("Target Delta table was not initialized.")
        initialize_identity_lookup()
        unprocessed_files = discover_unprocessed_files(processing_mode)
        file_discovery_detail_snapshot = _build_file_discovery_detail(processing_mode, unprocessed_files)

        for source_type in PIPELINE_CONFIG.keys():
            if unprocessed_files[source_type]:
                log_message(f"Processing '{source_type}' source...")
                source_timer = config.Stopwatch()

                pt_data_for_source = None
                if source_type == SOURCE_TYPE_SCD:
                    pt_data_for_source = get_pt_data_for_mode(processing_mode)

                # CG seed processing for rebuild of an empty table
                if (
                    source_type == SOURCE_TYPE_CG
                    and processing_mode == "rebuild"
                    and not state.target_delta_table.toDF().count() > 0
                ):
                    log_message("Processing CG seed data for initial table population...", depth=1)
                    transformed_cg_seed_df = initialize_caregiver_seed_data()
                    cg_seed_count = transformed_cg_seed_df.count()
                    raw_processed_counts["raw_cg_rows"] += cg_seed_count
                    raw_processed_counts["processed_cg_rows"] += cg_seed_count
                    execute_scd_pipeline_for_source(transformed_cg_seed_df, SOURCE_TYPE_CG)

                if processing_mode == "update" and source_type == SOURCE_TYPE_SCD:
                    log_message(
                        f"Processing {len(unprocessed_files[source_type])} SCD file(s) individually for UPDATE mode...",
                        depth=1,
                    )
                    for i, (file_path, mod_time) in enumerate(unprocessed_files[source_type]):
                        file_timer = config.Stopwatch()
                        log_message(
                            f"Processing file {i+1}/{len(unprocessed_files[source_type])}: {file_path.split('/')[-1]}",
                            depth=2,
                        )
                        transformed_df, raw_count = create_and_transform_source_dataframe(
                            source_type,
                            [(file_path, mod_time)],
                            processing_mode,
                            pt_data=pt_data_for_source,
                        )
                        raw_processed_counts["raw_scd_rows"] += raw_count
                        if transformed_df is not None:
                            raw_processed_counts["processed_scd_rows"] += transformed_df.count()
                        execute_scd_pipeline_for_source(transformed_df, source_type)
                        log_message(
                            f"Finished file {i+1} in {file_timer.format()}.",
                            depth=2,
                        )
                else:
                    batch_timer = config.Stopwatch()
                    log_message(
                        f"Processing {len(unprocessed_files[source_type])} {source_type} file(s) in a single batch...",
                        depth=1,
                    )
                    transformed_df, raw_count = create_and_transform_source_dataframe(
                        source_type,
                        unprocessed_files[source_type],
                        processing_mode,
                        pt_data=pt_data_for_source,
                    )
                    if source_type == SOURCE_TYPE_CG:
                        raw_processed_counts["raw_cg_rows"] += raw_count
                        if transformed_df is not None:
                            raw_processed_counts["processed_cg_rows"] += transformed_df.count()
                    else:
                        raw_processed_counts["raw_scd_rows"] += raw_count
                        if transformed_df is not None:
                            raw_processed_counts["processed_scd_rows"] += transformed_df.count()
                    execute_scd_pipeline_for_source(transformed_df, source_type)
                    log_message(
                        f"Finished processing all '{source_type}' files in {batch_timer.format()}.",
                        depth=2,
                    )

                log_message(f"Finished processing all '{source_type}' data in {source_timer.format()}.")

        if not any(unprocessed_files.values()):
            log_message("No unprocessed SCD or CG files found. Skipping main processing.")

        has_processed = any(unprocessed_files.values())
        return has_processed, raw_processed_counts, file_discovery_detail_snapshot

    except Exception as e:
        log_message(f"An error occurred during data processing: {e}", level="ERROR")
        raise

    finally:
        if state.pt_data_cache is not None:
            state.pt_data_cache.unpersist()
            state.pt_data_cache = None

        log_message(f"Patronage Pipeline ({processing_mode.upper()}) finished in {pipeline_timer.format()}.")


def _safe_table_count(table_name: str) -> Optional[int]:
    try:
        return spark.table(table_name).count()
    except Exception:
        return None


def _safe_scalar(query: str) -> Optional[Any]:
    try:
        result = spark.sql(query).collect()
        return result[0][0] if result else None
    except Exception:
        return None


def _to_iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def _build_file_discovery_detail(processing_mode: str, unprocessed_files: Dict[str, Any]) -> Dict[str, Any]:
    time_boundary_utc = _calculate_time_boundary(processing_mode)
    scd_start_time = None
    cg_start_time = None

    for source_type, cfg in PIPELINE_CONFIG.items():
        start_time_utc = None
        if processing_mode == "update":
            start_time_utc = _get_last_processed_timestamp(source_type)

        if start_time_utc is None and "beginning_date" in cfg:
            date_format = PY_DATE_FORMAT if source_type == SOURCE_TYPE_SCD else PY_DATETIME_FORMAT
            start_time_naive = datetime.strptime(cfg["beginning_date"], date_format)
            start_time_utc = start_time_naive.replace(tzinfo=timezone.utc)

        if source_type == SOURCE_TYPE_SCD:
            scd_start_time = start_time_utc
        elif source_type == SOURCE_TYPE_CG:
            cg_start_time = start_time_utc

    return {
        "scd_start_date": _to_iso(scd_start_time),
        "cg_start_date": _to_iso(cg_start_time),
        "end_date": _to_iso(time_boundary_utc),
        "total_cg_files": len(unprocessed_files.get(SOURCE_TYPE_CG, [])),
        "total_scd_files": len(unprocessed_files.get(SOURCE_TYPE_SCD, [])),
    }


def _build_output_row_counts_by_batch() -> Dict[str, Any]:
    result = {
        SOURCE_TYPE_SCD: {"active": 0, "new": 0, "expired": 0},
        SOURCE_TYPE_CG: {"active": 0, "new": 0, "expired": 0},
    }

    try:
        active_rows = spark.sql(
            f"""
            SELECT Batch_CD, COUNT(*) AS cnt
            FROM {PATRONAGE_TABLE_NAME}
            WHERE RecordStatus = true
            GROUP BY Batch_CD
            """
        ).collect()
        for row in active_rows:
            if row["Batch_CD"] in result:
                result[row["Batch_CD"]]["active"] = row["cnt"]

        new_rows = spark.sql(
            f"""
            SELECT Batch_CD, COUNT(*) AS cnt
            FROM {PATRONAGE_TABLE_NAME}
            WHERE RecordChangeStatus = 'New Record'
            GROUP BY Batch_CD
            """
        ).collect()
        for row in new_rows:
            if row["Batch_CD"] in result:
                result[row["Batch_CD"]]["new"] = row["cnt"]

        expired_rows = spark.sql(
            f"""
            SELECT Batch_CD, COUNT(*) AS cnt
            FROM {PATRONAGE_TABLE_NAME}
            WHERE RecordChangeStatus = 'Expired Record'
            GROUP BY Batch_CD
            """
        ).collect()
        for row in expired_rows:
            if row["Batch_CD"] in result:
                result[row["Batch_CD"]]["expired"] = row["cnt"]
    except Exception:
        pass

    return result


def run_pipeline(processing_mode: str, verbose_logging: bool = False) -> None:
    """Run the full pipeline including scheduled post-processing tasks.

    Args:
        processing_mode: "rebuild" or "update".
        verbose_logging: Enables additional debug logging.

    Behavior:
        - Runs `process_patronage_data` first.
        - Runs `OPTIMIZE` on the Patronage table only if new data was processed.
        - Applies scheduling gates:
          - Last Friday of month: EDIPI backfill.
          - Wed/Fri: DMDC export.
                    - First day of month: monthly backups.
    """
    run_id = str(uuid.uuid4())
    run_start_utc = datetime.now(timezone.utc)
    status = "SUCCESS"
    error_message = None

    patronage_rows_before = _safe_table_count(PATRONAGE_TABLE_NAME)

    raw_processed_counts: Dict[str, int] = {
        "raw_cg_rows": 0,
        "processed_cg_rows": 0,
        "raw_scd_rows": 0,
        "processed_scd_rows": 0,
    }

    dmdc_export_stats: Dict[str, Any] = {"triggered": False, "record_count": 0, "filename": None}
    edipi_backfill_stats: Dict[str, Any] = {"triggered": False, "record_count": 0, "filename": None}
    backup_stats: Dict[str, Any] = {"triggered": False, "status": None}

    file_discovery_detail_snapshot: Dict[str, Any] = {}

    try:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        log_message("Pinned spark.sql.session.timeZone to UTC", level="DEBUG", depth=1)

        has_processed, raw_processed_counts, file_discovery_detail_snapshot = process_patronage_data(
            processing_mode, verbose_logging
        )
        if has_processed:
            optimize_delta_table(PATRONAGE_TABLE_NAME)

        log_message("Checking for scheduled tasks...")

        if is_last_friday_of_month():
            log_message("Task triggered: EDIPI Backfill", depth=1)
            edipi_backfill_stats = run_edipi_backfill()
        else:
            log_message("Skipping EDIPI Backfill (not last Friday of the month).", level="DEBUG", depth=1)

        if is_dmdc_transfer_day():
            log_message("Task triggered: DMDC Export", depth=1)
            dmdc_export_stats = generate_dmdc_transfer_file()
        else:
            log_message("Skipping DMDC Export (not a transfer day).", level="DEBUG", depth=1)

        if is_first_day_of_month():
            log_message("Task triggered: Monthly Backups", depth=1)
            backup_stats["triggered"] = True
            run_monthly_backups()
            backup_stats["status"] = "SUCCESS"
        else:
            log_message("Skipping Monthly Backups (not first day of the month).", level="DEBUG", depth=1)

    except Exception as e:
        status = "FAILED"
        error_message = str(e)
        if backup_stats.get("triggered"):
            backup_stats["status"] = "FAILED"
        log_message(f"FATAL: An error occurred during the main pipeline execution: {e}", level="ERROR")
        raise

    finally:
        run_end_utc = datetime.now(timezone.utc)
        duration_seconds = (run_end_utc - run_start_utc).total_seconds()

        patronage_rows_after = _safe_table_count(PATRONAGE_TABLE_NAME)
        patronage_table_detail = {
            "table_name": PATRONAGE_TABLE_NAME,
            "rows_before": patronage_rows_before,
            "rows_after": patronage_rows_after,
            "rows_added": None
            if patronage_rows_before is None or patronage_rows_after is None
            else patronage_rows_after - patronage_rows_before,
        }

        identity_total = _safe_table_count(IDENTITY_TABLE_NAME)
        duplicate_total = _safe_table_count(DUP_IDENTITY_TABLE_NAME)
        identity_correlation_detail = {
            "table_name": IDENTITY_TABLE_NAME,
            "total_rows": identity_total,
            "duplicate_table_name": DUP_IDENTITY_TABLE_NAME,
            "duplicate_rows": duplicate_total,
        }

        file_discovery_detail = file_discovery_detail_snapshot

        input_watermarks = {
            "SCD_last_processed_ts": _to_iso(_get_last_processed_timestamp(SOURCE_TYPE_SCD)),
            "CG_last_processed_ts": _to_iso(_get_last_processed_timestamp(SOURCE_TYPE_CG)),
        }

        output_row_counts_by_batch = _build_output_row_counts_by_batch()

        scheduled_tasks_detail = {
            "dmdc_export_triggered": bool(dmdc_export_stats.get("triggered")),
            "edipi_backfill_triggered": bool(edipi_backfill_stats.get("triggered")),
            "backup_triggered": bool(backup_stats.get("triggered")),
        }

        log_row = {
            "run_id": run_id,
            "run_timestamp_utc": run_start_utc,
            "processing_mode": processing_mode,
            "status": status,
            "error_message": error_message,
            "duration_seconds": duration_seconds,
            "patronage_table_detail": json.dumps(patronage_table_detail),
            "identity_correlation_detail": json.dumps(identity_correlation_detail),
            "file_discovery_detail": json.dumps(file_discovery_detail),
            "records_processed_detail": json.dumps(raw_processed_counts),
            "scheduled_tasks_detail": json.dumps(scheduled_tasks_detail),
            "input_watermarks": json.dumps(input_watermarks),
            "output_row_counts_by_batch": json.dumps(output_row_counts_by_batch),
            "dmdc_export_stats": json.dumps(dmdc_export_stats),
            "edipi_backfill_stats": json.dumps(edipi_backfill_stats),
            "backup_stats": json.dumps(backup_stats),
        }

        try:
            string_fields = [
                "run_id",
                "processing_mode",
                "status",
                "error_message",
                "patronage_table_detail",
                "identity_correlation_detail",
                "file_discovery_detail",
                "records_processed_detail",
                "scheduled_tasks_detail",
                "input_watermarks",
                "output_row_counts_by_batch",
                "dmdc_export_stats",
                "edipi_backfill_stats",
                "backup_stats",
            ]
            coerced_log_row = {
                **log_row,
                **{key: (None if log_row.get(key) is None else str(log_row.get(key))) for key in string_fields},
            }

            log_schema = StructType(
                [
                    StructField("run_id", StringType(), True),
                    StructField("run_timestamp_utc", TimestampType(), True),
                    StructField("processing_mode", StringType(), True),
                    StructField("status", StringType(), True),
                    StructField("error_message", StringType(), True),
                    StructField("duration_seconds", DoubleType(), True),
                    StructField("patronage_table_detail", StringType(), True),
                    StructField("identity_correlation_detail", StringType(), True),
                    StructField("file_discovery_detail", StringType(), True),
                    StructField("records_processed_detail", StringType(), True),
                    StructField("scheduled_tasks_detail", StringType(), True),
                    StructField("input_watermarks", StringType(), True),
                    StructField("output_row_counts_by_batch", StringType(), True),
                    StructField("dmdc_export_stats", StringType(), True),
                    StructField("edipi_backfill_stats", StringType(), True),
                    StructField("backup_stats", StringType(), True),
                ]
            )

            spark.createDataFrame([coerced_log_row], schema=log_schema).write.insertInto(
                PATRONAGE_PIPELINE_LOG_TABLE_NAME
            )
        except Exception as log_error:
            log_message(f"Failed to insert pipeline log row: {log_error}", level="ERROR", depth=1)
