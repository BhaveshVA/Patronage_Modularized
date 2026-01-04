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
from typing import Any

from databricks.sdk.runtime import *
from delta.tables import DeltaTable

from . import config, state
from .config import (
    DMDC_CHECKPOINT_TABLE_NAME,
    PATRONAGE_TABLE_CLUSTER_COLUMNS,
    PATRONAGE_TABLE_NAME,
    PATRONAGE_TABLE_PATH,
    PATRONAGE_TABLE_SCHEMA,
    PIPELINE_CONFIG,
    SOURCE_TYPE_CG,
    SOURCE_TYPE_SCD,
    log_message,
    optimize_delta_table,
)
from .discovery import discover_unprocessed_files
from .dmdc import generate_dmdc_transfer_file, is_dmdc_transfer_day
from .identity import initialize_identity_lookup
from .scd2 import execute_scd_pipeline_for_source
from .scheduled import is_last_friday_of_month, run_edipi_backfill
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


def process_patronage_data(processing_mode: str, verbose_logging: bool = False) -> bool:
    """Run the core Patronage processing for SCD/CG sources.

    Args:
        processing_mode: "rebuild" or "update".
                        - "rebuild" is destructive: it drops the Patronage target table and deletes the underlying Delta files
                            before reprocessing historical inputs.
        verbose_logging: Enables additional debug metrics and counts.

    Returns:
        True if any new source files were processed; otherwise False.

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

    try:
        if processing_mode == "rebuild":
            reset_patronage_table()

        initialize_all_tables()
        if state.target_delta_table is None:
            raise RuntimeError("Target Delta table was not initialized.")
        initialize_identity_lookup()
        unprocessed_files = discover_unprocessed_files(processing_mode)

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
                        transformed_df = create_and_transform_source_dataframe(
                            source_type,
                            [(file_path, mod_time)],
                            processing_mode,
                            pt_data=pt_data_for_source,
                        )
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
                    transformed_df = create_and_transform_source_dataframe(
                        source_type,
                        unprocessed_files[source_type],
                        processing_mode,
                        pt_data=pt_data_for_source,
                    )
                    execute_scd_pipeline_for_source(transformed_df, source_type)
                    log_message(
                        f"Finished processing all '{source_type}' files in {batch_timer.format()}.",
                        depth=2,
                    )

                log_message(f"Finished processing all '{source_type}' data in {source_timer.format()}.")

        if not any(unprocessed_files.values()):
            log_message("No unprocessed SCD or CG files found. Skipping main processing.")

        has_processed = any(unprocessed_files.values())
        return has_processed

    except Exception as e:
        log_message(f"An error occurred during data processing: {e}", level="ERROR")
        raise

    finally:
        if state.pt_data_cache is not None:
            state.pt_data_cache.unpersist()
            state.pt_data_cache = None

        log_message(f"Patronage Pipeline ({processing_mode.upper()}) finished in {pipeline_timer.format()}.")


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
    """
    try:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        log_message("Pinned spark.sql.session.timeZone to UTC", level="DEBUG", depth=1)

        has_processed = process_patronage_data(processing_mode, verbose_logging)
        if has_processed:
            optimize_delta_table(PATRONAGE_TABLE_NAME)

        log_message("Checking for scheduled tasks...")

        if is_last_friday_of_month():
            log_message("Task triggered: EDIPI Backfill", depth=1)
            run_edipi_backfill()
        else:
            log_message("Skipping EDIPI Backfill (not last Friday of the month).", level="DEBUG", depth=1)

        if is_dmdc_transfer_day():
            log_message("Task triggered: DMDC Export", depth=1)
            generate_dmdc_transfer_file()
        else:
            log_message("Skipping DMDC Export (not a transfer day).", level="DEBUG", depth=1)

    except Exception as e:
        log_message(f"FATAL: An error occurred during the main pipeline execution: {e}", level="ERROR")
        raise
