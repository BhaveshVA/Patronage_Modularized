"""patronage_modularized.scheduled

Scheduled/periodic tasks that run after the main Patronage processing.

Currently implemented:
- Monthly EDIPI backfill (runs on the last Friday of the month).

The EDIPI backfill process:
1) Finds active patronage records with NULL EDIPI.
2) Joins them to the identity correlation table to find a candidate EDIPI.
3) Generates a DMDC-format export file for just the backfilled population.
4) Updates the patronage Delta table in-place to set `edipi`.

Databricks assumptions:
- Requires `spark` runtime and Delta Lake (`delta.tables.DeltaTable`).
- Uses `_to_local_fuse_path` so pandas can write reliably via DBFS FUSE.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Optional

from databricks.sdk.runtime import *
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from .config import (
    DMDC_ELIGIBILITY_FILTER,
    DMDC_EXPORT_DIR,
    IDENTITY_TABLE_NAME,
    PIPELINE_CONFIG,
    PATRONAGE_TABLE_NAME,
    PY_DATE_COMPACT_FORMAT,
    SOURCE_TYPE_CG,
    SOURCE_TYPE_SCD,
    build_dmdc_record_sql,
    log_message,
)
from .dmdc import _to_local_fuse_path


def is_last_friday_of_month(run_date: Optional[date] = None) -> bool:
    """Return True if `run_date` is the last Friday of its month.

    Args:
        run_date: Date to test. Defaults to `date.today()`.

    Returns:
        True if `run_date` is a Friday and adding 7 days crosses into the next
        month; otherwise False.

    Notes:
        This helper is used to gate the EDIPI backfill so it runs only once
        per month.
    """
    if run_date is None:
        run_date = datetime.now(timezone.utc).date()

    if run_date.weekday() != 4:
        return False

    return (run_date + timedelta(days=7)).month != run_date.month


def _get_edipi_backfill_candidates() -> Optional[DataFrame]:
    """Identify active Patronage records eligible for EDIPI backfill.

    Returns:
        A DataFrame containing the active records needing an EDIPI plus the
        lookup EDIPI as `lu_edipi`, or `None` if no candidates exist.

    Implementation notes:
        - Pulls lookup rows from the identity table where EDIPI is present.
        - Targets only active (`RecordStatus = true`) Patronage rows.
        - Only backfills where target.edipi is NULL and lookup EDIPI is not NULL.
    """
    log_message("Loading Identity Correlations...", depth=1)
    identity_correlations_df = (
        spark.table(IDENTITY_TABLE_NAME)
        .filter(col("edipi").isNotNull())
        .selectExpr("MVIPersonICN as lu_ICN", "edipi as lu_edipi")
    )

    from . import config

    if config.LOGGING_VERBOSE:
        log_message(
            f"Found {identity_correlations_df.count():,} records with an EDIPI in the lookup table.",
            level="DEBUG",
            depth=2,
        )

    log_message("Finding records with NULL EDIPI in the patronage table...", depth=1)
    null_edipi_records = spark.table(PATRONAGE_TABLE_NAME).filter(col("RecordStatus") & col("edipi").isNull())
    null_count = null_edipi_records.count()
    log_message(f"Found {null_count:,} active records with a NULL EDIPI.", depth=2)

    if null_count == 0:
        log_message("No records with NULL EDIPI found.", depth=1)
        return None

    log_message("Joining NULL records with identity correlations to find backfill candidates...", depth=1)
    backfill_candidates = (
        null_edipi_records.alias("target")
        .join(identity_correlations_df.alias("corr"), col("target.ICN") == col("corr.lu_ICN"), "inner")
        .select(col("target.*"), col("corr.lu_edipi"))
        .filter(col("target.edipi").isNull() & col("corr.lu_edipi").isNotNull())
    )

    backfill_count = backfill_candidates.count()
    log_message(f"Identified {backfill_count:,} records that can be backfilled with an EDIPI.", depth=2)

    if backfill_count == 0:
        log_message("No backfill candidates found after joining with identity table.", depth=1)
        return None

    return backfill_candidates


def _generate_edipi_backfill_file(backfill_candidates: DataFrame) -> None:
    """Write a DMDC-format export file for the backfilled EDIPI population.

    Args:
        backfill_candidates: DataFrame from `_get_edipi_backfill_candidates()`.

    Side effects:
        Writes a text file to `DMDC_EXPORT_DIR`.

    Notes:
        - We use `_to_local_fuse_path` to convert DBFS URIs into `/dbfs/...`
          paths for pandas writes (reliability in Jobs).
        - Output ordering is stable by `(Batch_CD, lu_edipi)`.
    """
    log_message("Generating DMDC export file for backfilled records...", depth=1)

    dmdc_query = f"""
        SELECT {build_dmdc_record_sql('lu_edipi')} as record, Batch_CD, lu_edipi
        FROM backfill_candidates_view
        WHERE {DMDC_ELIGIBILITY_FILTER}
        ORDER BY Batch_CD, lu_edipi
    """

    backfill_candidates.createOrReplaceTempView("backfill_candidates_view")

    today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    date_str = today.strftime(PY_DATE_COMPACT_FORMAT)
    output_filename = f"BACKFILL_EDIPI_PATRONAGE_{date_str}.txt"
    output_path = f"{DMDC_EXPORT_DIR}/{output_filename}"

    pandas_df = spark.sql(dmdc_query)[["record"]].toPandas()

    if not pandas_df.empty:
        local_path = _to_local_fuse_path(output_path)
        pandas_df.to_csv(local_path, header=False, index=False, sep="\t", encoding="utf-8", lineterminator="\n")
        log_message(f"Successfully generated DMDC backfill file: {output_path}", depth=2)

        from . import config

        if config.LOGGING_VERBOSE:
            log_message(f"DMDC backfill data:\n{pandas_df.to_string()}", level="DEBUG", depth=2)
    else:
        log_message("No data to write for DMDC backfill file.", level="DEBUG", depth=2)


def _update_patronage_with_backfilled_edipis(backfill_candidates: DataFrame) -> int:
    """Update the Patronage table by setting EDIPI on eligible active rows.

    Args:
        backfill_candidates: DataFrame containing source keys + `lu_edipi`.

    Returns:
        Total number of candidate rows merged/updated.

    Notes:
                - Iterates source types in `PIPELINE_CONFIG.keys()` order.
        - Merge keys differ by source type (CG uses additional business keys).
    """
    log_message("Executing table updates to backfill EDIPIs...", depth=1)
    delta_table = DeltaTable.forName(spark, PATRONAGE_TABLE_NAME)
    total_updated = 0

    # Loop over PIPELINE_CONFIG.keys() order.
    for source_type in PIPELINE_CONFIG.keys():
        candidates = backfill_candidates.filter(col("Batch_CD") == source_type)
        count = candidates.count()

        if count > 0:
            log_message(f"Updating {count:,} {source_type} records...", depth=2)

            if source_type == SOURCE_TYPE_CG:
                merge_key = (
                    "target.ICN = source.ICN AND target.Veteran_ICN = source.Veteran_ICN "
                    "AND target.Applicant_Type = source.Applicant_Type AND target.RecordStatus = true"
                )
            else:
                merge_key = "target.ICN = source.ICN AND target.RecordStatus = true"

            (
                delta_table.alias("target")
                .merge(candidates.alias("source"), merge_key)
                .whenMatchedUpdate(set={"edipi": "source.lu_edipi"})
                .execute()
            )

            total_updated += count

    return total_updated


def run_edipi_backfill() -> None:
    """Run the full EDIPI backfill workflow.

    Workflow:
        1) Load candidates (active Patronage rows missing EDIPI).
        2) Generate a DMDC export file for the backfill population.
        3) Merge updates into the Patronage Delta table.

    Raises:
        Propagates any exception after logging a high-level error message.
    """
    log_message("Starting EDIPI Backfill Process...")
    backfill_candidates = None

    try:
        backfill_candidates = _get_edipi_backfill_candidates()

        if backfill_candidates is None:
            log_message("EDIPI Backfill process complete - no candidates found.", depth=1)
            return

        backfill_candidates = backfill_candidates.persist()
        _ = backfill_candidates.count()

        _generate_edipi_backfill_file(backfill_candidates)

        total_updated = _update_patronage_with_backfilled_edipis(backfill_candidates)
        log_message(
            f"EDIPI Backfill process completed successfully. Total records updated: {total_updated:,}",
            depth=1,
        )

    except Exception as e:
        log_message(f"An error occurred during the EDIPI backfill process: {e}", level="ERROR")
        raise

    finally:
        if backfill_candidates is not None and backfill_candidates.is_cached:
            backfill_candidates.unpersist()
